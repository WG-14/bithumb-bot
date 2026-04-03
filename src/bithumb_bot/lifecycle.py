from __future__ import annotations

import sqlite3


_ENTRY_DECISION_FALLBACK_LOOKBACK_MS = 15 * 60 * 1000


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
    if row is None or row["strategy_name"] is None:
        return None
    return str(row["strategy_name"])


def _find_entry_decision(
    conn: sqlite3.Connection,
    *,
    fill_ts: int,
    pair: str,
    strategy_name: str | None,
) -> tuple[int | None, str | None, str]:
    if strategy_name is None or not str(strategy_name).strip():
        return None, None, "unattributed_missing_strategy"

    lower_ts = max(0, int(fill_ts) - _ENTRY_DECISION_FALLBACK_LOOKBACK_MS)
    rows = conn.execute(
        """
        SELECT id, strategy_name
        FROM strategy_decisions
        WHERE signal='BUY'
          AND strategy_name=?
          AND decision_ts BETWEEN ? AND ?
          AND json_extract(context_json, '$.pair')=?
        ORDER BY decision_ts DESC, id DESC
        LIMIT 2
        """,
        (str(strategy_name), lower_ts, int(fill_ts), str(pair)),
    ).fetchall()

    if not rows:
        return None, str(strategy_name), "unattributed_no_strict_match"
    if len(rows) > 1:
        return None, str(strategy_name), "ambiguous_multi_candidate"

    row = rows[0]
    return int(row["id"]), str(row["strategy_name"]), "fallback_strict_match"


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
        resolved_entry_decision_id = entry_decision_id
        resolved_strategy_name = strategy_name
        resolved_entry_decision_linkage = "direct" if resolved_entry_decision_id is not None else "unattributed"
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
            resolved_entry_decision_linkage = "degraded_recovery_unattributed"
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
                entry_fee_total,
                strategy_name,
                entry_decision_id,
                entry_decision_linkage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pair),
                int(trade_id),
                str(client_order_id),
                fill_id,
                int(fill_ts),
                float(price),
                float(qty),
                float(fee),
                resolved_strategy_name,
                resolved_entry_decision_id,
                resolved_entry_decision_linkage,
            ),
        )
        return

    if side != "SELL":
        raise RuntimeError(f"unsupported lifecycle side: {side}")

    rows = conn.execute(
        """
        SELECT
            id,
            entry_trade_id,
            entry_client_order_id,
            entry_fill_id,
            entry_ts,
            entry_price,
            qty_open,
            entry_fee_total,
            strategy_name,
            entry_decision_id,
            entry_decision_linkage
        FROM open_position_lots
        WHERE pair=? AND qty_open > 0
        ORDER BY entry_ts ASC, id ASC
        """,
        (str(pair),),
    ).fetchall()

    remaining = float(qty)
    if remaining <= 0:
        return

    total_exit_qty = float(qty)
    eps = 1e-12
    for row in rows:
        if remaining <= eps:
            break

        lot = row
        lot_qty = float(lot["qty_open"])
        matched_qty = min(lot_qty, remaining)
        if matched_qty <= eps:
            continue

        entry_fee_total = float(lot["entry_fee_total"])
        entry_fee_alloc = (entry_fee_total * (matched_qty / lot_qty)) if lot_qty > eps else 0.0
        exit_fee_alloc = float(fee) * (matched_qty / total_exit_qty)

        gross_pnl = (float(price) - float(lot["entry_price"])) * matched_qty
        fee_total = entry_fee_alloc + exit_fee_alloc
        net_pnl = gross_pnl - fee_total
        holding_time_seconds = max(0.0, (int(fill_ts) - int(lot["entry_ts"])) / 1000.0)

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
                int(lot["entry_trade_id"]),
                int(trade_id),
                str(lot["entry_client_order_id"]),
                str(client_order_id),
                lot["entry_fill_id"],
                fill_id,
                int(lot["entry_ts"]),
                int(fill_ts),
                float(matched_qty),
                float(lot["entry_price"]),
                float(price),
                float(gross_pnl),
                float(fee_total),
                float(net_pnl),
                float(holding_time_seconds),
                strategy_name or lot["strategy_name"],
                entry_decision_id if entry_decision_id is not None else lot["entry_decision_id"],
                ("direct" if entry_decision_id is not None else lot["entry_decision_linkage"]),
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
            (qty_open_after, fee_remaining, int(lot["id"])),
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
        "DELETE FROM open_position_lots WHERE pair=? AND qty_open <= ?",
        (str(pair), eps),
    )
