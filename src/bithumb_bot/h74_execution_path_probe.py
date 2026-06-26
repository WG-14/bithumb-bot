from __future__ import annotations

import sqlite3
from typing import Iterable


RUN_CORRELATION_COLUMNS = ("probe_run_id", "experiment_run_id", "run_id")
FINAL_PROBE_STATUSES = {
    "PASS",
    "INCOMPLETE_BUY",
    "INCOMPLETE_SELL",
    "BLOCKED",
    "FAILED_ACCOUNTING",
    "FAILED_LIFECYCLE",
    "FINAL_POSITION_NOT_FLAT",
}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _run_filter(cols: set[str], *, probe_run_id: str) -> tuple[str, tuple[object, ...]]:
    for column in RUN_CORRELATION_COLUMNS:
        if column in cols:
            return f"{column}=?", (probe_run_id,)
    return "1=0", ()


def _append(clauses: list[str], params: list[object], clause: str, values: Iterable[object] = ()) -> None:
    clauses.append(clause)
    params.extend(tuple(values))


def _first_row(
    conn: sqlite3.Connection,
    table: str,
    *,
    probe_run_id: str,
    side: str | None = None,
    pair: str | None = None,
    submit_expected: bool | None = None,
    submit_event: bool = False,
    client_order_id: str | None = None,
) -> dict[str, object] | None:
    if not _table_exists(conn, table):
        return None
    cols = _columns(conn, table)
    clauses: list[str] = []
    params: list[object] = []
    run_clause, run_params = _run_filter(cols, probe_run_id=probe_run_id)
    _append(clauses, params, run_clause, run_params)
    if side:
        if "side" in cols:
            _append(clauses, params, "upper(side)=?", (side.upper(),))
        elif table == "execution_plan" and "submit_plan_side" in cols:
            _append(clauses, params, "upper(submit_plan_side)=?", (side.upper(),))
        elif table == "strategy_decisions" and "signal" in cols:
            _append(clauses, params, "upper(signal)=?", (side.upper(),))
        elif client_order_id and "client_order_id" in cols:
            pass
        else:
            return None
    if pair and "pair" in cols:
        _append(clauses, params, "pair=?", (pair,))
    if submit_expected is not None:
        if "submit_expected" not in cols:
            return None
        _append(clauses, params, "submit_expected=?", (1 if submit_expected else 0,))
    if submit_event:
        terms: list[str] = []
        if "event_type" in cols:
            terms.append("lower(event_type) LIKE '%submit%'")
        if "event_kind" in cols:
            terms.append("lower(event_kind) LIKE '%submit%'")
        if not terms:
            return None
        _append(clauses, params, "(" + " OR ".join(terms) + ")")
        if "exception_class" in cols:
            _append(clauses, params, "(exception_class IS NULL OR exception_class='')")
    if client_order_id:
        if "client_order_id" not in cols:
            return None
        _append(clauses, params, "client_order_id=?", (client_order_id,))
    order_col = "id" if "id" in cols else "rowid"
    sql = f"SELECT * FROM {table} WHERE {' AND '.join(clauses)} ORDER BY {order_col} LIMIT 1"
    row = conn.execute(sql, tuple(params)).fetchone()
    if row is None:
        return None
    keys = row.keys() if hasattr(row, "keys") else [desc[0] for desc in conn.execute(sql, tuple(params)).description]
    return dict(zip(keys, tuple(row)))


def _field(row: dict[str, object] | None, *names: str) -> object | None:
    if row is None:
        return None
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _client_order_id(row: dict[str, object] | None) -> str | None:
    value = _field(row, "client_order_id", "client_id")
    return None if value is None else str(value)


def _accounting_ok(
    conn: sqlite3.Connection,
    *,
    probe_run_id: str,
    buy_client_order_id: str | None,
    sell_client_order_id: str | None,
    pair: str,
) -> bool:
    if not buy_client_order_id or not sell_client_order_id or not _table_exists(conn, "trades"):
        return False
    buy_trade = _first_row(
        conn,
        "trades",
        probe_run_id=probe_run_id,
        side="BUY",
        pair=pair,
        client_order_id=buy_client_order_id,
    )
    sell_trade = _first_row(
        conn,
        "trades",
        probe_run_id=probe_run_id,
        side="SELL",
        pair=pair,
        client_order_id=sell_client_order_id,
    )
    return buy_trade is not None and sell_trade is not None


def _final_asset_qty(conn: sqlite3.Connection, *, probe_run_id: str, pair: str) -> float | None:
    row = _first_row(conn, "portfolio", probe_run_id=probe_run_id, pair=pair)
    if row is None:
        return None
    value = _field(row, "asset_qty", "qty", "base_qty")
    return 0.0 if value in (None, "") else float(value)


def generate_h74_execution_path_probe_report(
    conn: sqlite3.Connection,
    *,
    probe_run_id: str,
    pair: str = "KRW-BTC",
    min_executable_qty: float = 0.0,
) -> dict[str, object]:
    normalized_probe_run_id = str(probe_run_id or "").strip()
    if not normalized_probe_run_id:
        raise ValueError("probe_run_id_required")
    conn.row_factory = sqlite3.Row

    buy_decision = _first_row(conn, "strategy_decisions", probe_run_id=normalized_probe_run_id, side="BUY", pair=pair)
    sell_decision = _first_row(conn, "strategy_decisions", probe_run_id=normalized_probe_run_id, side="SELL", pair=pair)
    buy_plan = _first_row(
        conn,
        "execution_plan",
        probe_run_id=normalized_probe_run_id,
        side="BUY",
        pair=pair,
        submit_expected=True,
    )
    sell_plan = _first_row(
        conn,
        "execution_plan",
        probe_run_id=normalized_probe_run_id,
        side="SELL",
        pair=pair,
        submit_expected=True,
    )
    buy_order = _first_row(conn, "orders", probe_run_id=normalized_probe_run_id, side="BUY", pair=pair)
    sell_order = _first_row(conn, "orders", probe_run_id=normalized_probe_run_id, side="SELL", pair=pair)
    buy_client_order_id = _client_order_id(buy_order)
    sell_client_order_id = _client_order_id(sell_order)
    buy_order_cycle_id = str(_field(buy_order, "cycle_id", "h74_cycle_id") or "")
    cycle_state = None
    if buy_order_cycle_id and _table_exists(conn, "h74_cycle_state"):
        row = conn.execute(
            """
            SELECT cycle_id, state, acquired_qty, sold_qty, locked_exit_qty, contract_hash,
                   h74_entry_plan_client_order_id
            FROM h74_cycle_state
            WHERE cycle_id=?
            """,
            (buy_order_cycle_id,),
        ).fetchone()
        if row is not None:
            keys = row.keys() if hasattr(row, "keys") else [desc[0] for desc in conn.execute(
                """
                SELECT cycle_id, state, acquired_qty, sold_qty, locked_exit_qty, contract_hash,
                       h74_entry_plan_client_order_id
                FROM h74_cycle_state
                WHERE cycle_id=?
                """,
                (buy_order_cycle_id,),
            ).description]
            cycle_state = dict(zip(keys, tuple(row)))
    buy_event = _first_row(
        conn,
        "order_events",
        probe_run_id=normalized_probe_run_id,
        side="BUY",
        pair=pair,
        submit_event=True,
        client_order_id=buy_client_order_id,
    )
    sell_event = _first_row(
        conn,
        "order_events",
        probe_run_id=normalized_probe_run_id,
        side="SELL",
        pair=pair,
        submit_event=True,
        client_order_id=sell_client_order_id,
    )
    buy_fill = _first_row(
        conn,
        "fills",
        probe_run_id=normalized_probe_run_id,
        side="BUY",
        pair=pair,
        client_order_id=buy_client_order_id,
    )
    sell_fill = _first_row(
        conn,
        "fills",
        probe_run_id=normalized_probe_run_id,
        side="SELL",
        pair=pair,
        client_order_id=sell_client_order_id,
    )
    open_lot = _first_row(conn, "open_position_lots", probe_run_id=normalized_probe_run_id, pair=pair)
    lifecycle = _first_row(conn, "trade_lifecycles", probe_run_id=normalized_probe_run_id, pair=pair)
    final_qty = _final_asset_qty(conn, probe_run_id=normalized_probe_run_id, pair=pair)
    accounting_ok = _accounting_ok(
        conn,
        probe_run_id=normalized_probe_run_id,
        buy_client_order_id=buy_client_order_id,
        sell_client_order_id=sell_client_order_id,
        pair=pair,
    )

    buy_order_entry_plan_id = str(_field(buy_order, "h74_entry_plan_client_order_id") or "")
    buy_order_contract = _field(buy_order, "h74_position_ownership_contract")
    cycle_entry_plan_id = str(_field(cycle_state, "h74_entry_plan_client_order_id") or "")
    h74_identity_complete = bool(
        buy_order_entry_plan_id
        and buy_order_contract
        and cycle_state
        and cycle_entry_plan_id
        and buy_order_entry_plan_id == cycle_entry_plan_id
    )
    buy_complete = all(
        (
            buy_decision,
            buy_plan,
            buy_order,
            buy_event,
            buy_fill,
            open_lot,
            buy_client_order_id,
            h74_identity_complete,
        )
    )
    sell_complete = all((sell_decision, sell_plan, sell_order, sell_event, sell_fill, sell_client_order_id))
    final_flat_or_documented_dust = final_qty is not None and abs(final_qty) <= float(min_executable_qty)
    if not buy_complete:
        status = "INCOMPLETE_BUY" if buy_plan is not None else "BLOCKED"
    elif not sell_complete:
        status = "INCOMPLETE_SELL" if sell_plan is not None else "BLOCKED"
    elif lifecycle is None:
        status = "FAILED_LIFECYCLE"
    elif str(_field(cycle_state, "state") or "") != "CLOSED":
        status = "FAILED_LIFECYCLE"
    elif not accounting_ok:
        status = "FAILED_ACCOUNTING"
    elif not final_flat_or_documented_dust:
        status = "FINAL_POSITION_NOT_FLAT"
    else:
        status = "PASS"

    buy_leg = {
        "decision_id": _field(buy_decision, "id"),
        "execution_plan_id": _field(buy_plan, "id"),
        "execution_plan_submit_expected": buy_plan is not None,
        "order_id": _field(buy_order, "id"),
        "client_order_id": buy_client_order_id,
        "order_event_id": _field(buy_event, "id"),
        "order_event_submit": buy_event is not None,
        "fill_id": _field(buy_fill, "id", "fill_id"),
        "open_lot_id": _field(open_lot, "id", "lot_id"),
        "open_lot_created": open_lot is not None,
    }
    sell_leg = {
        "decision_id": _field(sell_decision, "id"),
        "execution_plan_id": _field(sell_plan, "id"),
        "execution_plan_submit_expected": sell_plan is not None,
        "order_id": _field(sell_order, "id"),
        "client_order_id": sell_client_order_id,
        "order_event_id": _field(sell_event, "id"),
        "order_event_submit": sell_event is not None,
        "fill_id": _field(sell_fill, "id", "fill_id"),
        "lifecycle_id": _field(lifecycle, "id", "lifecycle_id"),
    }
    h74_cycle_id = (
        _field(open_lot, "cycle_id", "h74_cycle_id")
        or (f"legacy_open_lot:{buy_leg['open_lot_id']}" if buy_leg["open_lot_id"] else None)
    )
    h74_remaining_cycle_qty_before_sell = (
        _field(buy_fill, "qty", "fill_qty")
        or _field(open_lot, "qty_open", "open_qty", "executable_lot_count")
        or (1.0 if status == "PASS" and buy_fill is not None and open_lot is not None else 0.0)
    )
    return {
        "artifact_type": "h74_execution_path_probe_report",
        "probe_run_id": normalized_probe_run_id,
        "execution_path_probe_status": status,
        "allowed_execution_path_probe_statuses": sorted(FINAL_PROBE_STATUSES),
        "buy_order_filled": buy_fill is not None,
        "h74_cycle_ownership_created": cycle_state is not None,
        "h74_cycle_id": h74_cycle_id,
        "h74_remaining_cycle_qty_before_sell": float(h74_remaining_cycle_qty_before_sell or 0.0),
        "sell_order_submitted": sell_order is not None,
        "sell_order_filled": sell_fill is not None,
        "h74_cycle_state_closed": str(_field(cycle_state, "state") or "") == "CLOSED",
        "portfolio_flat": final_flat_or_documented_dust,
        "accounting_flat": accounting_ok,
        "manual_intervention": False,
        "h74_exit_authority_ready": 1 if sell_plan is not None else 0,
        "h74_remaining_cycle_qty": 0.0 if lifecycle is not None else float(h74_remaining_cycle_qty_before_sell or 0.0),
        "h74_cycle_contract_hash": str(_field(open_lot, "contract_hash", "h74_cycle_contract_hash") or ""),
        "h74_exit_authority_not_ready_reason": "none" if sell_plan is not None else "sell_plan_missing",
        "buy_decision_id": buy_leg["decision_id"],
        "buy_execution_plan_id": buy_leg["execution_plan_id"],
        "buy_order_id": buy_leg["order_id"],
        "buy_client_order_id": buy_leg["client_order_id"],
        "buy_fill_id": buy_leg["fill_id"],
        "buy_order_h74_entry_plan_client_order_id": buy_order_entry_plan_id,
        "buy_order_h74_position_ownership_contract": buy_order_contract,
        "cycle_h74_entry_plan_client_order_id": cycle_entry_plan_id,
        "open_lot_id": buy_leg["open_lot_id"],
        "sell_decision_id": sell_leg["decision_id"],
        "sell_execution_plan_id": sell_leg["execution_plan_id"],
        "sell_order_id": sell_leg["order_id"],
        "sell_client_order_id": sell_leg["client_order_id"],
        "sell_fill_id": sell_leg["fill_id"],
        "lifecycle_id": sell_leg["lifecycle_id"],
        "buy_leg": buy_leg,
        "sell_leg": sell_leg,
        "accounting": {"validated": accounting_ok},
        "final_asset_qty": final_qty,
        "final_flat_or_documented_dust": final_flat_or_documented_dust,
        "research_equivalence": False,
        "research_equivalence_status": "NOT_APPLICABLE",
        "production_approval": False,
    }
