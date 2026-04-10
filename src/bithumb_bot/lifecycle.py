from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .config import settings
from .dust import (
    DUST_TRACKING_LOT_STATE,
    OPEN_EXPOSURE_LOT_STATE,
    DustClassification,
    DustDisplayContext,
    DustState,
    ExecutableLot,
    build_dust_display_context,
    build_executable_lot,
    is_strictly_below_min_qty,
)
from .lot_model import build_market_lot_rules, lot_count_to_qty, qty_to_executable_lot_count
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


@dataclass(frozen=True)
class PositionLotSnapshot:
    """Recovery-facing lot summary with explicit lot-native exposure counts.

    The executable semantic authority is the lot state/count layer. The qty
    fields remain available as raw or compatibility quantities for accounting,
    reporting, and broker reconciliation.
    """

    raw_open_exposure_qty: float
    executable_open_exposure_qty: float
    dust_tracking_qty: float
    raw_total_asset_qty: float
    open_lot_count: int
    dust_tracking_lot_count: int
    effective_min_trade_qty: float
    exit_non_executable_reason: str
    position_semantic_basis: str

    @property
    def total_holdings_qty(self) -> float:
        return float(self.raw_total_asset_qty)

    @property
    def executable_exposure_qty(self) -> float:
        return float(self.executable_open_exposure_qty)

    @property
    def tracked_dust_qty(self) -> float:
        return float(self.dust_tracking_qty)

    @property
    def semantic_basis(self) -> str:
        return str(self.position_semantic_basis or "lot-native")

    def as_dict(self) -> dict[str, float | int | str]:
        return {
            "semantic_basis": self.semantic_basis,
            "position_semantic_basis": self.semantic_basis,
            "raw_open_exposure_qty": float(self.raw_open_exposure_qty),
            "raw_total_asset_qty": float(self.raw_total_asset_qty),
            "total_holdings_qty": float(self.total_holdings_qty),
            "executable_open_exposure_qty": float(self.executable_open_exposure_qty),
            "executable_exposure_qty": float(self.executable_exposure_qty),
            "dust_tracking_qty": float(self.dust_tracking_qty),
            "tracked_dust_qty": float(self.tracked_dust_qty),
            "open_exposure_lot_count": int(self.open_lot_count),
            "open_lot_count": int(self.open_lot_count),
            "dust_tracking_lot_count": int(self.dust_tracking_lot_count),
            "effective_min_trade_qty": float(self.effective_min_trade_qty),
            "exit_non_executable_reason": self.exit_non_executable_reason,
        }


def _build_fill_lot_rules(*, pair: str, market_price: float) -> object:
    """Build deterministic lot rules for fill lifecycle accounting.

    The lifecycle layer must not depend on a live order-rules fetch to split or
    consume lot-native exposure. Use the local configuration fallback inputs so
    ledger semantics stay stable even in offline tests and recovery flows.
    """

    fallback_rules = type(
        "_LifecycleLotRules",
        (object,),
        {
            "min_qty": float(settings.LIVE_MIN_ORDER_QTY),
            "qty_step": float(settings.LIVE_ORDER_QTY_STEP),
            "min_notional_krw": float(settings.MIN_ORDER_NOTIONAL_KRW),
            "max_qty_decimals": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        },
    )()
    return build_market_lot_rules(
        market_id=str(pair),
        market_price=float(market_price),
        rules=fallback_rules,
        source_mode="ledger",
    )


def _row_executable_lot_count(row: object, *, qty_open: float, lot_rules: object) -> int:
    raw_count = int(_row_value(row, "executable_lot_count", 7) or 0)
    if raw_count > 0:
        return raw_count
    # Do not infer executable-lot authority from qty alone. Legacy rows that
    # lack an executable lot count must fail closed rather than silently
    # recreating executable exposure semantics.
    return 0


def _row_dust_tracking_lot_count(row: object, *, qty_open: float) -> int:
    raw_count = int(_row_value(row, "dust_tracking_lot_count", 8) or 0)
    if raw_count > 0:
        return raw_count
    # Dust tracking is operator evidence only; qty without explicit dust state
    # is not authoritative enough to recreate dust semantics.
    return 0


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
        # BUY fills are persisted as lot-native exposure plus explicit dust.
        # The stored executable quantity is the exact executable lot multiple;
        # the non-executable remainder is tracked separately as dust evidence.
        lot_rules = _build_fill_lot_rules(pair=pair, market_price=price)
        fill_lot = build_executable_lot(
            qty=float(qty),
            market_price=float(price),
            min_qty=float(lot_rules.lot_size),
            qty_step=float(lot_rules.lot_size),
            min_notional_krw=float(lot_rules.min_notional_krw),
            max_qty_decimals=int(lot_rules.max_qty_decimals),
        )
        executable_lot_count = int(
            qty_to_executable_lot_count(qty=float(fill_lot.executable_qty), lot_rules=lot_rules)
        )
        dust_lot_count = 1 if fill_lot.dust_qty > 1e-12 else 0
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
        total_fill_qty = max(0.0, float(qty))
        executable_qty = max(0.0, float(fill_lot.executable_qty))
        dust_qty = max(0.0, float(fill_lot.dust_qty))
        if executable_qty > 1e-12:
            executable_fee = float(fee) * (executable_qty / total_fill_qty) if total_fill_qty > 1e-12 else float(fee)
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
                    executable_lot_count,
                    dust_tracking_lot_count,
                    position_semantic_basis,
                    position_state,
                    entry_fee_total,
                    strategy_name,
                    entry_decision_id,
                    entry_decision_linkage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(pair),
                    int(trade_id),
                    str(client_order_id),
                    fill_id,
                    int(fill_ts),
                    float(price),
                    executable_qty,
                    executable_lot_count,
                    0,
                    "lot-native",
                    OPEN_EXPOSURE_LOT_STATE,
                    float(executable_fee),
                    resolved_strategy_name,
                    resolved_entry_decision_id,
                    resolved_entry_decision_linkage,
                ),
            )
        if dust_qty > 1e-12:
            dust_fee = float(fee) - (float(fee) * (executable_qty / total_fill_qty) if total_fill_qty > 1e-12 else float(fee))
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
                    executable_lot_count,
                    dust_tracking_lot_count,
                    position_semantic_basis,
                    position_state,
                    entry_fee_total,
                    strategy_name,
                    entry_decision_id,
                    entry_decision_linkage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(pair),
                    int(trade_id),
                    str(client_order_id),
                    fill_id,
                    int(fill_ts),
                    float(price),
                    dust_qty,
                    0,
                    dust_lot_count or 1,
                    "lot-native",
                    DUST_TRACKING_LOT_STATE,
                    float(dust_fee),
                    resolved_strategy_name,
                    resolved_entry_decision_id,
                    resolved_entry_decision_linkage,
                ),
            )
        if executable_qty <= 1e-12 and dust_qty <= 1e-12:
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
                    executable_lot_count,
                    dust_tracking_lot_count,
                    position_semantic_basis,
                    position_state,
                    entry_fee_total,
                    strategy_name,
                    entry_decision_id,
                    entry_decision_linkage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(pair),
                    int(trade_id),
                    str(client_order_id),
                    fill_id,
                    int(fill_ts),
                    float(price),
                    0.0,
                    0,
                    1,
                    "lot-native",
                    DUST_TRACKING_LOT_STATE,
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
    lot_rules = _build_fill_lot_rules(pair=pair, market_price=price)
    rows = _fetch_sellable_open_exposure_lots(conn, pair=str(pair))

    remaining_lots = int(qty_to_executable_lot_count(qty=float(qty), lot_rules=lot_rules))
    if remaining_lots <= 0:
        return

    total_exit_qty = lot_count_to_qty(lot_count=remaining_lots, lot_size=float(lot_rules.lot_size))
    eps = 1e-12
    for row in rows:
        if remaining_lots <= 0:
            break

        lot = row
        lot_qty = float(_row_value(lot, "qty_open", 6) or 0.0)
        lot_count = _row_executable_lot_count(lot, qty_open=lot_qty, lot_rules=lot_rules)
        if lot_count <= 0:
            continue
        matched_lots = min(lot_count, remaining_lots)
        if matched_lots <= 0:
            continue
        matched_qty = lot_count_to_qty(lot_count=matched_lots, lot_size=float(lot_rules.lot_size))

        entry_fee_total = float(_row_value(lot, "entry_fee_total", 10) or 0.0)
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
                strategy_name or _row_value(lot, "strategy_name", 11),
                entry_decision_id if entry_decision_id is not None else _row_value(lot, "entry_decision_id", 12),
                (
                    ENTRY_DECISION_LINKAGE_DIRECT
                    if entry_decision_id is not None
                    else str(_row_value(lot, "entry_decision_linkage", 13) or "")
                ),
                exit_decision_id,
                exit_reason,
                exit_rule_name,
            ),
        )

        remaining_lot_count = max(0, lot_count - matched_lots)
        qty_open_after = lot_count_to_qty(lot_count=remaining_lot_count, lot_size=float(lot_rules.lot_size))
        fee_remaining = max(0.0, entry_fee_total - entry_fee_alloc)
        conn.execute(
            """
            UPDATE open_position_lots
            SET qty_open=?, executable_lot_count=?, entry_fee_total=?
            WHERE id=?
            """,
            (
                qty_open_after,
                remaining_lot_count,
                fee_remaining,
                int(_row_value(lot, "id", 0) or 0),
            ),
        )

        remaining_lots -= matched_lots

    if remaining_lots > 0:
        remaining_qty = lot_count_to_qty(lot_count=remaining_lots, lot_size=float(lot_rules.lot_size))
        fallback_exit_fee = float(fee) * (remaining_qty / total_exit_qty) if total_exit_qty > eps else 0.0
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
                float(remaining_qty),
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
        """
        DELETE FROM open_position_lots
        WHERE pair=?
          AND position_state=?
          AND qty_open <= ?
          AND COALESCE(executable_lot_count, 0) <= 0
        """,
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
            SET position_state=?,
                position_semantic_basis='lot-native',
                executable_lot_count=0,
                dust_tracking_lot_count=CASE
                    WHEN COALESCE(executable_lot_count, 0) > 0 THEN executable_lot_count
                    ELSE 1
                END
            WHERE id=?
            """,
            (
                DUST_TRACKING_LOT_STATE,
                int(_row_value(row, "id", 0) or 0),
            ),
        )
        updated_count += 1
    return updated_count


def summarize_position_lots(
    conn: sqlite3.Connection,
    *,
    pair: str,
    executable_lot: ExecutableLot | None = None,
) -> PositionLotSnapshot:
    try:
        open_row = conn.execute(
            """
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(executable_lot_count, 0) > 0 THEN qty_open
                            WHEN COALESCE(position_semantic_basis, '') = 'lot-native'
                                 AND COALESCE(executable_lot_count, 0) = 0
                                 AND COALESCE(dust_tracking_lot_count, 0) = 0 THEN qty_open
                            ELSE 0.0
                        END
                    ),
                    0.0
                ),
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(executable_lot_count, 0) > 0 THEN executable_lot_count
                            WHEN COALESCE(position_semantic_basis, '') = 'lot-native'
                                 AND COALESCE(executable_lot_count, 0) = 0
                                 AND COALESCE(dust_tracking_lot_count, 0) = 0 THEN 1
                            ELSE 0
                        END
                    ),
                    0
                )
            FROM open_position_lots
            WHERE pair=? AND position_state=?
            """,
            (str(pair), OPEN_EXPOSURE_LOT_STATE),
        ).fetchone()
        dust_row = conn.execute(
            """
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(dust_tracking_lot_count, 0) > 0 THEN qty_open
                            WHEN COALESCE(position_semantic_basis, '') = 'lot-native'
                                 AND COALESCE(executable_lot_count, 0) = 0
                                 AND COALESCE(dust_tracking_lot_count, 0) = 0 THEN qty_open
                            ELSE 0.0
                        END
                    ),
                    0.0
                ),
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(dust_tracking_lot_count, 0) > 0 THEN dust_tracking_lot_count
                            WHEN COALESCE(position_semantic_basis, '') = 'lot-native'
                                 AND COALESCE(executable_lot_count, 0) = 0
                                 AND COALESCE(dust_tracking_lot_count, 0) = 0 THEN 1
                            ELSE 0
                        END
                    ),
                    0
                )
            FROM open_position_lots
            WHERE pair=? AND position_state=?
            """,
            (str(pair), DUST_TRACKING_LOT_STATE),
        ).fetchone()
    except sqlite3.OperationalError:
        try:
            open_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(qty_open), 0.0),
                    COALESCE(SUM(CASE WHEN COALESCE(executable_lot_count, 0) > 0 THEN executable_lot_count ELSE 0 END), 0)
                FROM open_position_lots
                WHERE pair=? AND position_state=? AND COALESCE(executable_lot_count, 0) > 0
                """,
                (str(pair), OPEN_EXPOSURE_LOT_STATE),
            ).fetchone()
            dust_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(qty_open), 0.0),
                    COALESCE(SUM(CASE WHEN COALESCE(dust_tracking_lot_count, 0) > 0 THEN dust_tracking_lot_count ELSE 0 END), 0)
                FROM open_position_lots
                WHERE pair=? AND position_state=? AND COALESCE(dust_tracking_lot_count, 0) > 0
                """,
                (str(pair), DUST_TRACKING_LOT_STATE),
            ).fetchone()
        except (sqlite3.OperationalError, AssertionError):
            open_row = (0.0, 0)
            dust_row = (0.0, 0)
    except AssertionError:
        open_row = (0.0, 0)
        dust_row = (0.0, 0)
    raw_open_qty = max(0.0, float(open_row[0] if open_row is not None else 0.0))
    tracked_dust_qty = max(0.0, float(dust_row[0] if dust_row is not None else 0.0))
    open_lot_count = max(0, int(open_row[1] if open_row is not None else 0))
    dust_lot_count = max(0, int(dust_row[1] if dust_row is not None else 0))
    if executable_lot is None:
        executable_qty = 0.0
        effective_min_trade_qty = 0.0
        if open_lot_count > 0:
            exit_non_executable_reason = "none"
        elif dust_lot_count > 0:
            exit_non_executable_reason = "dust_only_remainder"
        else:
            exit_non_executable_reason = "no_executable_open_lots"
    else:
        executable_qty = float(executable_lot.executable_qty)
        effective_min_trade_qty = float(executable_lot.effective_min_trade_qty)
        exit_non_executable_reason = str(executable_lot.exit_non_executable_reason)
    return PositionLotSnapshot(
        raw_open_exposure_qty=raw_open_qty,
        executable_open_exposure_qty=float(executable_qty),
        dust_tracking_qty=max(0.0, tracked_dust_qty + (0.0 if executable_lot is None else float(executable_lot.dust_qty))),
        raw_total_asset_qty=max(0.0, raw_open_qty + tracked_dust_qty),
        open_lot_count=max(0, open_lot_count),
        dust_tracking_lot_count=max(0, dust_lot_count),
        effective_min_trade_qty=float(effective_min_trade_qty),
        exit_non_executable_reason=exit_non_executable_reason,
        position_semantic_basis="lot-native",
    )


def summarize_reserved_exit_qty(
    conn: sqlite3.Connection,
    *,
    pair: str,
) -> float:
    """Return remaining qty already reserved by unresolved SELL orders."""

    try:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(MAX(qty_req - qty_filled, 0.0)), 0.0) AS reserved_exit_qty
            FROM orders
            WHERE symbol=?
              AND side='SELL'
              AND status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
            """,
            (str(pair),),
        ).fetchone()
    except sqlite3.OperationalError:
        return 0.0
    if row is None:
        return 0.0
    try:
        value = row["reserved_exit_qty"] if hasattr(row, "keys") else row[0]
        return max(0.0, float(value or 0.0))
    except (TypeError, ValueError, IndexError, KeyError):
        return 0.0


def reclassify_non_executable_open_exposure(
    conn: sqlite3.Connection,
    *,
    pair: str,
    executable_lot: ExecutableLot,
) -> int:
    if executable_lot.executable_qty > 1e-12:
        return 0
    if executable_lot.raw_qty <= 1e-12:
        return 0
    result = conn.execute(
        """
        UPDATE open_position_lots
        SET position_state=?,
            position_semantic_basis='lot-native',
            dust_tracking_lot_count=CASE
                WHEN COALESCE(executable_lot_count, 0) > 0 THEN executable_lot_count
                ELSE 1
            END,
            executable_lot_count=0
        WHERE pair=?
          AND position_state=?
          AND qty_open > 1e-12
        """,
        (DUST_TRACKING_LOT_STATE, str(pair), OPEN_EXPOSURE_LOT_STATE),
    )
    return int(result.rowcount or 0)


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
            executable_lot_count,
            dust_tracking_lot_count,
            position_state,
            entry_fee_total,
            strategy_name,
            entry_decision_id,
            entry_decision_linkage
        FROM open_position_lots
        WHERE pair=? AND position_state=? AND COALESCE(executable_lot_count, 0) > 0
        ORDER BY entry_ts ASC, id ASC
        """,
        (str(pair), OPEN_EXPOSURE_LOT_STATE),
    ).fetchall()
