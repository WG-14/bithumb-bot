from __future__ import annotations

import sqlite3
from typing import Any


def _row_value(row: sqlite3.Row | tuple[Any, ...] | None, key: str, index: int) -> Any:
    if row is None:
        return None
    return row[key] if hasattr(row, "keys") else row[index]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _fill_row(conn: sqlite3.Connection, client_order_id: str | None) -> sqlite3.Row | tuple[Any, ...] | None:
    if not client_order_id or not _table_exists(conn, "fills"):
        return None
    return conn.execute(
        """
        SELECT id, fill_id, qty
        FROM fills
        WHERE client_order_id=?
        ORDER BY fill_ts DESC, id DESC
        LIMIT 1
        """,
        (client_order_id,),
    ).fetchone()


def _latest_asset_after(conn: sqlite3.Connection) -> float:
    if not _table_exists(conn, "trades"):
        return 0.0
    row = conn.execute("SELECT asset_after FROM trades ORDER BY ts DESC, id DESC LIMIT 1").fetchone()
    return float(_row_value(row, "asset_after", 0) or 0.0)


def _portfolio_asset_qty(conn: sqlite3.Connection) -> float:
    if not _table_exists(conn, "portfolio"):
        return 0.0
    row = conn.execute(
        """
        SELECT COALESCE(asset_available,0) + COALESCE(asset_locked,0) AS asset_qty
        FROM portfolio
        WHERE id=1
        """
    ).fetchone()
    return float(_row_value(row, "asset_qty", 0) or 0.0)


def build_h74_execution_path_probe_report(
    conn: sqlite3.Connection,
    probe_run_id: str,
) -> dict[str, object]:
    probe_id = str(probe_run_id or "").strip()
    buy_order = conn.execute(
        """
        SELECT id, client_order_id, cycle_id, entry_decision_id, authority_hash,
               h74_position_ownership_contract_hash, h74_entry_plan_client_order_id,
               h74_position_ownership_contract
        FROM orders
        WHERE probe_run_id=?
          AND side='BUY'
          AND strategy_name='daily_participation_sma'
        ORDER BY created_ts ASC, id ASC
        LIMIT 1
        """,
        (probe_id,),
    ).fetchone()
    buy_client_order_id = str(_row_value(buy_order, "client_order_id", 1) or "")
    cycle_id = str(_row_value(buy_order, "cycle_id", 2) or "")
    buy_fill = _fill_row(conn, buy_client_order_id)

    cycle = None
    if cycle_id and _table_exists(conn, "h74_cycle_state"):
        cycle = conn.execute(
            """
            SELECT cycle_id, state, acquired_qty, sold_qty, locked_exit_qty, contract_hash,
                   h74_entry_plan_client_order_id
            FROM h74_cycle_state
            WHERE cycle_id=?
            """,
            (cycle_id,),
        ).fetchone()
    remaining_qty = max(
        0.0,
        float(_row_value(cycle, "acquired_qty", 2) or 0.0)
        - float(_row_value(cycle, "sold_qty", 3) or 0.0)
        - float(_row_value(cycle, "locked_exit_qty", 4) or 0.0),
    )
    open_lot = None
    if buy_client_order_id and _table_exists(conn, "open_position_lots"):
        open_lot = conn.execute(
            """
            SELECT id
            FROM open_position_lots
            WHERE entry_client_order_id=?
            ORDER BY id ASC
            LIMIT 1
            """,
            (buy_client_order_id,),
        ).fetchone()

    manual_sell_exists = False
    if _table_exists(conn, "orders"):
        row = conn.execute(
            """
            SELECT 1
            FROM orders
            WHERE probe_run_id=?
              AND side='SELL'
              AND COALESCE(decision_reason,'') IN ('manual_flatten','operator_closeout')
            LIMIT 1
            """,
            (probe_id,),
        ).fetchone()
        manual_sell_exists = row is not None
    sell_order = conn.execute(
        """
        SELECT id, client_order_id, exit_decision_id
        FROM orders
        WHERE probe_run_id=?
          AND side='SELL'
          AND strategy_name='daily_participation_sma'
          AND cycle_id=?
          AND COALESCE(decision_reason,'') NOT IN ('manual_flatten','operator_closeout')
        ORDER BY created_ts ASC, id ASC
        LIMIT 1
        """,
        (probe_id, cycle_id),
    ).fetchone() if cycle_id else None
    sell_client_order_id = str(_row_value(sell_order, "client_order_id", 1) or "")
    sell_fill = _fill_row(conn, sell_client_order_id)
    lifecycle = None
    if sell_client_order_id and _table_exists(conn, "trade_lifecycles"):
        lifecycle = conn.execute(
            """
            SELECT id, operator_intervention
            FROM trade_lifecycles
            WHERE exit_client_order_id=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (sell_client_order_id,),
        ).fetchone()
    operator_closeout = bool(int(_row_value(lifecycle, "operator_intervention", 1) or 0))
    manual_intervention = manual_sell_exists or operator_closeout
    portfolio_flat = abs(_portfolio_asset_qty(conn)) <= 1e-12
    accounting_flat = abs(_latest_asset_after(conn)) <= 1e-12
    cycle_closed = str(_row_value(cycle, "state", 1) or "") == "CLOSED"

    report: dict[str, object] = {
        "artifact_type": "h74_execution_path_probe_report",
        "probe_run_id": probe_id,
        "buy_order_filled": buy_fill is not None,
        "h74_cycle_ownership_created": cycle is not None,
        "h74_cycle_id": cycle_id,
        "h74_remaining_cycle_qty_before_sell": float(_row_value(buy_fill, "qty", 2) or 0.0),
        "sell_order_submitted": sell_order is not None,
        "sell_order_filled": sell_fill is not None,
        "h74_cycle_state_closed": cycle_closed,
        "portfolio_flat": portfolio_flat,
        "accounting_flat": accounting_flat,
        "manual_intervention": manual_intervention,
        "manual_sell": manual_sell_exists,
        "operator_closeout": operator_closeout,
        "h74_exit_authority_ready": 1 if remaining_qty > 1e-12 else 0,
        "h74_remaining_cycle_qty": remaining_qty,
        "h74_cycle_contract_hash": str(_row_value(cycle, "contract_hash", 5) or ""),
        "h74_exit_authority_not_ready_reason": "none" if remaining_qty > 1e-12 else "h74_cycle_closed_or_missing",
        "buy_decision_id": _row_value(buy_order, "entry_decision_id", 3),
        "buy_execution_plan_id": _row_value(buy_order, "authority_hash", 4),
        "buy_order_id": _row_value(buy_order, "id", 0),
        "buy_client_order_id": buy_client_order_id,
        "buy_fill_id": _row_value(buy_fill, "id", 0),
        "buy_order_h74_entry_plan_client_order_id": _row_value(
            buy_order, "h74_entry_plan_client_order_id", 6
        ),
        "buy_order_h74_position_ownership_contract": _row_value(
            buy_order, "h74_position_ownership_contract", 7
        ),
        "cycle_h74_entry_plan_client_order_id": _row_value(
            cycle, "h74_entry_plan_client_order_id", 6
        ),
        "open_lot_id": _row_value(open_lot, "id", 0),
        "sell_decision_id": _row_value(sell_order, "exit_decision_id", 2),
        "sell_execution_plan_id": cycle_id if sell_order is not None else None,
        "sell_order_id": _row_value(sell_order, "id", 0),
        "sell_client_order_id": sell_client_order_id,
        "sell_fill_id": _row_value(sell_fill, "id", 0),
        "lifecycle_id": _row_value(lifecycle, "id", 0),
        "buy_leg": {
            "decision_id": _row_value(buy_order, "entry_decision_id", 3),
            "execution_plan_id": _row_value(buy_order, "authority_hash", 4),
            "order_id": _row_value(buy_order, "id", 0),
            "client_order_id": buy_client_order_id,
            "fill_id": _row_value(buy_fill, "id", 0),
            "open_lot_id": _row_value(open_lot, "id", 0),
        },
        "sell_leg": {
            "decision_id": _row_value(sell_order, "exit_decision_id", 2),
            "execution_plan_id": cycle_id if sell_order is not None else None,
            "order_id": _row_value(sell_order, "id", 0),
            "client_order_id": sell_client_order_id,
            "fill_id": _row_value(sell_fill, "id", 0),
            "lifecycle_id": _row_value(lifecycle, "id", 0),
        },
        "accounting": {"validated": portfolio_flat and accounting_flat},
        "final_flat_or_documented_dust": portfolio_flat and accounting_flat,
    }
    pass_ready = (
        bool(report["buy_order_filled"])
        and bool(report["h74_cycle_ownership_created"])
        and bool(report["buy_order_h74_entry_plan_client_order_id"])
        and bool(report["buy_order_h74_position_ownership_contract"])
        and report["buy_order_h74_entry_plan_client_order_id"]
        == report["cycle_h74_entry_plan_client_order_id"]
        and bool(report["sell_order_submitted"])
        and bool(report["sell_order_filled"])
        and bool(report["lifecycle_id"])
        and cycle_closed
        and portfolio_flat
        and accounting_flat
        and not manual_intervention
    )
    report["execution_path_probe_status"] = "PASS" if pass_ready else (
        "PARTIAL_PASS" if bool(report["buy_order_filled"]) else "INCOMPLETE"
    )
    return report


__all__ = ["build_h74_execution_path_probe_report"]
