from __future__ import annotations

import json
import sqlite3

from .dust import (
    DUST_TRACKING_LOT_STATE,
    OPEN_EXPOSURE_LOT_STATE,
    DustClassification,
    DustDisplayContext,
    DustState,
    build_dust_display_context,
    is_strictly_below_min_qty,
)
from .markets import parse_user_market_input

OPEN_POSITION_STATE = OPEN_EXPOSURE_LOT_STATE
DUST_TRACKING_STATE = DUST_TRACKING_LOT_STATE


_ENTRY_DECISION_FALLBACK_LOOKBACK_MS = 15 * 60 * 1000
# BUY fill attribution states are persisted in entry_decision_linkage.
# The order below matters: direct linked decision takes precedence over
# fallback classification, and fallback classification must stay specific
# enough to explain why the BUY fill was or was not linked.
ENTRY_DECISION_LINKAGE_DIRECT = "direct"
ENTRY_DECISION_LINKAGE_STRICT_SINGLE_FALLBACK = "fallback_strict_match"
ENTRY_DECISION_LINKAGE_AMBIGUOUS_MULTI_CANDIDATE = "ambiguous_multi_candidate"
ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH = "unattributed_no_strict_match"
ENTRY_DECISION_LINKAGE_UNATTRIBUTED_MISSING_STRATEGY = "unattributed_missing_strategy"
ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED = "degraded_recovery_unattributed"


def _row_value(row: object, key: str, index: int) -> object | None:
    """Read a SQLite result row by key when available, otherwise by position.

    Lifecycle paths can be called with either ``sqlite3.Row`` or plain tuples
    depending on how the connection was created. Keep those callers working
    without forcing a global row-factory policy here.
    """

    if row is None:
        return None
    if hasattr(row, "keys"):
        try:
            return row[key]  # type: ignore[index]
        except (KeyError, IndexError, TypeError):
            pass
    try:
        return row[index]  # type: ignore[index]
    except (IndexError, KeyError, TypeError):
        return None


def _load_strategy_for_decision_id(conn: sqlite3.Connection, *, decision_id: int) -> str | None:
    row = conn.execute(
        """
        SELECT strategy_name
        FROM strategy_decisions
        WHERE id=?
        LIMIT 1
        """,
        (int(decision_id),),
    ).fetchone()
    strategy_name = _row_value(row, "strategy_name", 0)
    if strategy_name is None:
        return None
    return str(strategy_name)


def _extract_pair_from_context(context: object) -> str | None:
    if not isinstance(context, dict):
        return None

    candidate_paths = (
        ("pair",),
        ("market",),
        ("position_state", "normalized_exposure", "pair"),
        ("position_state", "pair"),
    )
    for path in candidate_paths:
        current: object = context
        for key in path:
            if not isinstance(current, dict) or key not in current:
                current = None
                break
            current = current[key]
        if current is None:
            continue
        text = str(current).strip()
        if text:
            return text
    return None


def _normalize_pair_for_match(pair: object) -> str | None:
    text = str(pair or "").strip()
    if not text:
        return None
    try:
        return parse_user_market_input(text)
    except Exception:
        return text.upper()


def _find_entry_decision(
    conn: sqlite3.Connection,
    *,
    fill_ts: int,
    pair: str,
    strategy_name: str | None,
) -> tuple[int | None, str | None, str]:
    """Resolve the BUY decision that should be attributed to a fill.

    Precedence is intentionally narrow and stable:

    1. direct linked decision: an explicit ``entry_decision_id`` always wins.
    2. strict single fallback: when no direct link exists, filter by
       ``strategy_name`` + ``signal='BUY'`` + ``decision_ts <= fill_ts`` within
       the fallback window, then require a strict pair match in the decision
       context.
    3. ambiguous multi candidate: more than one strict pair match in the window.
    4. unattributed no strict match: no strict pair match in the window.

    The pair is the final strict gate. ``strategy_name``/``signal``/``decision_ts``
    form the coarse candidate pool, and ``fill_ts`` is the upper bound so a BUY
    fill never attaches to a later decision.
    """
    if strategy_name is None or not str(strategy_name).strip():
        return None, None, ENTRY_DECISION_LINKAGE_UNATTRIBUTED_MISSING_STRATEGY

    lower_ts = max(0, int(fill_ts) - _ENTRY_DECISION_FALLBACK_LOOKBACK_MS)
    rows = conn.execute(
        """
        SELECT id, strategy_name, context_json
        FROM strategy_decisions
        WHERE signal='BUY'
          AND strategy_name=?
          AND decision_ts BETWEEN ? AND ?
        ORDER BY decision_ts DESC, id DESC
        """,
        (str(strategy_name), lower_ts, int(fill_ts)),
    ).fetchall()

    if not rows:
        return None, str(strategy_name), ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH

    normalized_pair = _normalize_pair_for_match(pair)
    strict_rows: list[sqlite3.Row] = []
    for row in rows:
        try:
            context = json.loads(str(_row_value(row, "context_json", 2) or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(context, dict):
            continue
        candidate_pair = _normalize_pair_for_match(_extract_pair_from_context(context))
        if candidate_pair is None or normalized_pair is None:
            continue
        if candidate_pair == normalized_pair:
            strict_rows.append(row)
            if len(strict_rows) > 1:
                break

    if not strict_rows:
        return None, str(strategy_name), ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH
    if len(strict_rows) > 1:
        return None, str(strategy_name), ENTRY_DECISION_LINKAGE_AMBIGUOUS_MULTI_CANDIDATE

    row = strict_rows[0]
    return (
        int(_row_value(row, "id", 0) or 0),
        str(_row_value(row, "strategy_name", 1) or ""),
        ENTRY_DECISION_LINKAGE_STRICT_SINGLE_FALLBACK,
    )


def apply_fill_lifecycle(
    conn: sqlite3.Connection,
    *,
    side: str,
    pair: str,
    trade_id: int,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    strategy_name: str | None = None,
    entry_decision_id: int | None = None,
    exit_decision_id: int | None = None,
    exit_reason: str | None = None,
    exit_rule_name: str | None = None,
    allow_entry_decision_fallback: bool = True,
) -> None:
    if side == "BUY":
        # BUY fills always create the real position lot; dust_tracking is a
        # downstream operator-only state used only when harmless dust is later
        # reclassified.
        resolved_entry_decision_id = entry_decision_id
        resolved_strategy_name = strategy_name
        resolved_entry_decision_linkage = (
            ENTRY_DECISION_LINKAGE_DIRECT
            if resolved_entry_decision_id is not None
            else ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH
        )
        if resolved_entry_decision_id is not None and resolved_strategy_name is None:
            resolved_strategy_name = _load_strategy_for_decision_id(conn, decision_id=int(resolved_entry_decision_id))
        if resolved_entry_decision_id is None and allow_entry_decision_fallback:
            lookup_decision_id, lookup_strategy_name, lookup_linkage = _find_entry_decision(
                conn,
                fill_ts=int(fill_ts),
                pair=str(pair),
                strategy_name=resolved_strategy_name,
            )
            resolved_entry_decision_id = lookup_decision_id
            if resolved_strategy_name is None:
                resolved_strategy_name = lookup_strategy_name
            resolved_entry_decision_linkage = lookup_linkage
        elif resolved_entry_decision_id is None and not allow_entry_decision_fallback:
            resolved_entry_decision_linkage = ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_fill_id,
                entry_ts,
                entry_price,
                qty_open,
                position_state,
                entry_fee_total,
                strategy_name,
                entry_decision_id,
                entry_decision_linkage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pair),
                int(trade_id),
                str(client_order_id),
                fill_id,
                int(fill_ts),
                float(price),
                float(qty),
                OPEN_EXPOSURE_LOT_STATE,
                float(fee),
                resolved_strategy_name,
                resolved_entry_decision_id,
                resolved_entry_decision_linkage,
            ),
        )
        return

    if side != "SELL":
        raise RuntimeError(f"unsupported lifecycle side: {side}")

    # SELL lifecycle consumes only the sellable open_exposure path.
    # dust_tracking lots remain operator evidence and are never matched here.
    rows = _fetch_sellable_open_exposure_lots(conn, pair=str(pair))

    remaining = float(qty)
    if remaining <= 0:
        return

    total_exit_qty = float(qty)
    eps = 1e-12
    for row in rows:
        if remaining <= eps:
            break

        lot = row
        lot_qty = float(_row_value(lot, "qty_open", 6) or 0.0)
        matched_qty = min(lot_qty, remaining)
        if matched_qty <= eps:
            continue

        entry_fee_total = float(_row_value(lot, "entry_fee_total", 8) or 0.0)
        entry_fee_alloc = (entry_fee_total * (matched_qty / lot_qty)) if lot_qty > eps else 0.0
        exit_fee_alloc = float(fee) * (matched_qty / total_exit_qty)

        gross_pnl = (float(price) - float(_row_value(lot, "entry_price", 5) or 0.0)) * matched_qty
        fee_total = entry_fee_alloc + exit_fee_alloc
        net_pnl = gross_pnl - fee_total
        holding_time_seconds = max(0.0, (int(fill_ts) - int(_row_value(lot, "entry_ts", 4) or 0)) / 1000.0)

        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                pair,
                entry_trade_id,
                exit_trade_id,
                entry_client_order_id,
                exit_client_order_id,
                entry_fill_id,
                exit_fill_id,
                entry_ts,
                exit_ts,
                matched_qty,
                entry_price,
                exit_price,
                gross_pnl,
                fee_total,
                net_pnl,
                holding_time_sec,
                strategy_name,
                entry_decision_id,
                entry_decision_linkage,
                exit_decision_id,
                exit_reason,
                exit_rule_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pair),
                int(_row_value(lot, "entry_trade_id", 1) or 0),
                int(trade_id),
                str(_row_value(lot, "entry_client_order_id", 2) or ""),
                str(client_order_id),
                _row_value(lot, "entry_fill_id", 3),
                fill_id,
                int(_row_value(lot, "entry_ts", 4) or 0),
                int(fill_ts),
                float(matched_qty),
                float(_row_value(lot, "entry_price", 5) or 0.0),
                float(price),
                float(gross_pnl),
                float(fee_total),
                float(net_pnl),
                float(holding_time_seconds),
                strategy_name or _row_value(lot, "strategy_name", 9),
                entry_decision_id if entry_decision_id is not None else _row_value(lot, "entry_decision_id", 10),
                (
                    ENTRY_DECISION_LINKAGE_DIRECT
                    if entry_decision_id is not None
                    else str(_row_value(lot, "entry_decision_linkage", 11) or "")
                ),
                exit_decision_id,
                exit_reason,
                exit_rule_name,
            ),
        )

        qty_open_after = max(0.0, lot_qty - matched_qty)
        fee_remaining = max(0.0, entry_fee_total - entry_fee_alloc)
        conn.execute(
            """
            UPDATE open_position_lots
            SET qty_open=?, entry_fee_total=?
            WHERE id=?
            """,
            (qty_open_after, fee_remaining, int(_row_value(lot, "id", 0) or 0)),
        )

        remaining -= matched_qty

    if remaining > 1e-9:
        fallback_exit_fee = float(fee) * (remaining / total_exit_qty)
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                pair,
                entry_trade_id,
                exit_trade_id,
                entry_client_order_id,
                exit_client_order_id,
                entry_fill_id,
                exit_fill_id,
                entry_ts,
                exit_ts,
                matched_qty,
                entry_price,
                exit_price,
                gross_pnl,
                fee_total,
                net_pnl,
                holding_time_sec,
                strategy_name,
                entry_decision_id,
                entry_decision_linkage,
                exit_decision_id,
                exit_reason,
                exit_rule_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pair),
                0,
                int(trade_id),
                "__unknown_entry__",
                str(client_order_id),
                None,
                fill_id,
                int(fill_ts),
                int(fill_ts),
                float(remaining),
                float(price),
                float(price),
                0.0,
                float(fallback_exit_fee),
                float(-fallback_exit_fee),
                0.0,
                strategy_name,
                entry_decision_id,
                "unattributed_unknown_entry",
                exit_decision_id,
                exit_reason,
                exit_rule_name,
            ),
        )

    conn.execute(
        "DELETE FROM open_position_lots WHERE pair=? AND position_state=? AND qty_open <= ?",
        (str(pair), OPEN_EXPOSURE_LOT_STATE, eps),
    )


def mark_harmless_dust_positions(
    conn: sqlite3.Connection,
    *,
    pair: str,
    dust_metadata: DustDisplayContext | DustClassification | str | dict[str, object] | None,
) -> int:
    dust_context = (
        dust_metadata
        if isinstance(dust_metadata, DustDisplayContext)
        else build_dust_display_context(dust_metadata)
    )
    dust = dust_context.classification
    if not (
        dust.present
        and dust.classification == DustState.HARMLESS_DUST.value
        and dust_context.effective_flat_due_to_harmless_dust
    ):
        return 0

    min_qty = max(0.0, float(dust.min_qty))
    if min_qty <= 0.0:
        return 0

    candidate_rows = conn.execute(
        """
        SELECT id, qty_open
        FROM open_position_lots
        WHERE pair=?
          AND position_state=?
          AND qty_open > 1e-12
        ORDER BY entry_ts ASC, id ASC
        """,
        (
            str(pair),
            OPEN_EXPOSURE_LOT_STATE,
        ),
    ).fetchall()

    updated_count = 0
    for row in candidate_rows:
        # The boundary is strict: qty_open == min_qty stays open_exposure.
        # Only strict sub-min residues are reclassified to dust_tracking.
        if not is_strictly_below_min_qty(qty_open=float(_row_value(row, "qty_open", 1) or 0.0), min_qty=min_qty):
            continue
        conn.execute(
            """
            UPDATE open_position_lots
            SET position_state=?
            WHERE id=?
            """,
            (
                DUST_TRACKING_LOT_STATE,
                int(_row_value(row, "id", 0) or 0),
            ),
        )
        updated_count += 1
    return updated_count


def _fetch_sellable_open_exposure_lots(
    conn: sqlite3.Connection,
    *,
    pair: str,
) -> list[sqlite3.Row]:
    """Return lots that can actually be sold.

    Only `open_exposure` lots are eligible. `dust_tracking` lots are operator
    evidence and must not be counted as sellable inventory.
    """

    return conn.execute(
        """
        SELECT
            id,
            entry_trade_id,
            entry_client_order_id,
            entry_fill_id,
            entry_ts,
            entry_price,
            qty_open,
            position_state,
            entry_fee_total,
            strategy_name,
            entry_decision_id,
            entry_decision_linkage
        FROM open_position_lots
        WHERE pair=? AND position_state=? AND qty_open > 0
        ORDER BY entry_ts ASC, id ASC
        """,
        (str(pair), OPEN_EXPOSURE_LOT_STATE),
    ).fetchall()
