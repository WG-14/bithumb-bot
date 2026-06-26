from __future__ import annotations

import sqlite3

from bithumb_bot.h74_execution_path_probe import generate_h74_execution_path_probe_report


_CONTRACT_HASH = "sha256:" + "1" * 64
_CONTRACT_JSON = (
    '{"authority_hash":"sha256:a","contract_hash":"'
    + _CONTRACT_HASH
    + '","cycle_id":"cycle-1","entry_plan_id":"probe-entry-plan",'
    '"entry_side":"BUY","h74_cycle_id":"cycle-1","hold_policy":"hold_acquired_fill_qty_until_max_holding_exit",'
    '"pair":"KRW-BTC","position_mode":"fixed_fill_qty_until_exit","probe_run_id":"probe-1",'
    '"strategy_instance_id":"h74-source-observation"}'
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE strategy_decisions(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, signal TEXT);
        CREATE TABLE execution_plan(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, side TEXT, submit_expected INTEGER);
        CREATE TABLE orders(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, client_order_id TEXT, side TEXT,
            cycle_id TEXT, h74_entry_plan_client_order_id TEXT,
            h74_position_ownership_contract_hash TEXT, h74_position_ownership_contract TEXT
        );
        CREATE TABLE order_events(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, client_order_id TEXT, side TEXT, event_type TEXT, exception_class TEXT);
        CREATE TABLE fills(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, client_order_id TEXT, side TEXT);
        CREATE TABLE trades(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, client_order_id TEXT, side TEXT);
        CREATE TABLE open_position_lots(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, cycle_id TEXT);
        CREATE TABLE h74_cycle_state(
            cycle_id TEXT PRIMARY KEY, probe_run_id TEXT, state TEXT,
            acquired_qty REAL DEFAULT 0, sold_qty REAL DEFAULT 0, locked_exit_qty REAL DEFAULT 0,
            contract_hash TEXT, h74_entry_plan_client_order_id TEXT
        );
        CREATE TABLE trade_lifecycles(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT);
        CREATE TABLE portfolio(id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, asset_qty REAL);
        """
    )
    return conn


def _seed_buy(conn: sqlite3.Connection, run_id: str, *, open_lot: bool = True) -> None:
    conn.execute("INSERT INTO strategy_decisions(probe_run_id, pair, signal) VALUES(?, 'KRW-BTC', 'BUY')", (run_id,))
    conn.execute("INSERT INTO execution_plan(probe_run_id, pair, side, submit_expected) VALUES(?, 'KRW-BTC', 'BUY', 1)", (run_id,))
    conn.execute(
        """
        INSERT INTO orders(
            probe_run_id, pair, client_order_id, side, cycle_id, h74_entry_plan_client_order_id,
            h74_position_ownership_contract_hash, h74_position_ownership_contract
        )
        VALUES(?, 'KRW-BTC', ?, 'BUY', 'cycle-1', 'probe-entry-plan', ?, ?)
        """,
        (run_id, f"{run_id}-buy", _CONTRACT_HASH, _CONTRACT_JSON),
    )
    conn.execute("INSERT INTO order_events(probe_run_id, pair, client_order_id, side, event_type, exception_class) VALUES(?, 'KRW-BTC', ?, 'BUY', 'submit', '')", (run_id, f"{run_id}-buy"))
    conn.execute("INSERT INTO fills(probe_run_id, pair, client_order_id, side) VALUES(?, 'KRW-BTC', ?, 'BUY')", (run_id, f"{run_id}-buy"))
    conn.execute("INSERT INTO trades(probe_run_id, pair, client_order_id, side) VALUES(?, 'KRW-BTC', ?, 'BUY')", (run_id, f"{run_id}-buy"))
    conn.execute(
        """
        INSERT INTO h74_cycle_state(
            cycle_id, probe_run_id, state, acquired_qty, sold_qty, locked_exit_qty,
            contract_hash, h74_entry_plan_client_order_id
        )
        VALUES('cycle-1', ?, 'CLOSED', 0.001, 0.001, 0.0, ?, 'probe-entry-plan')
        ON CONFLICT(cycle_id) DO UPDATE SET probe_run_id=excluded.probe_run_id
        """,
        (run_id, _CONTRACT_HASH),
    )
    if open_lot:
        conn.execute("INSERT INTO open_position_lots(probe_run_id, pair, cycle_id) VALUES(?, 'KRW-BTC', 'cycle-1')", (run_id,))


def _seed_sell(conn: sqlite3.Connection, run_id: str, *, trade: bool = True) -> None:
    conn.execute("INSERT INTO strategy_decisions(probe_run_id, pair, signal) VALUES(?, 'KRW-BTC', 'SELL')", (run_id,))
    conn.execute("INSERT INTO execution_plan(probe_run_id, pair, side, submit_expected) VALUES(?, 'KRW-BTC', 'SELL', 1)", (run_id,))
    conn.execute(
        "INSERT INTO orders(probe_run_id, pair, client_order_id, side, cycle_id) VALUES(?, 'KRW-BTC', ?, 'SELL', 'cycle-1')",
        (run_id, f"{run_id}-sell"),
    )
    conn.execute("INSERT INTO order_events(probe_run_id, pair, client_order_id, side, event_type, exception_class) VALUES(?, 'KRW-BTC', ?, 'SELL', 'submit', '')", (run_id, f"{run_id}-sell"))
    conn.execute("INSERT INTO fills(probe_run_id, pair, client_order_id, side) VALUES(?, 'KRW-BTC', ?, 'SELL')", (run_id, f"{run_id}-sell"))
    if trade:
        conn.execute("INSERT INTO trades(probe_run_id, pair, client_order_id, side) VALUES(?, 'KRW-BTC', ?, 'SELL')", (run_id, f"{run_id}-sell"))


def _seed_pass(conn: sqlite3.Connection, run_id: str) -> None:
    _seed_buy(conn, run_id)
    _seed_sell(conn, run_id)
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES(?, 'KRW-BTC')", (run_id,))
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES(?, 'KRW-BTC', 0)", (run_id,))


def test_probe_report_filters_by_probe_run_id() -> None:
    conn = _conn()
    _seed_buy(conn, "run-a")
    _seed_sell(conn, "run-b")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('run-b', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('run-b', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="run-a")

    assert report["execution_path_probe_status"] != "PASS"
    assert report["sell_order_id"] is None


def test_probe_report_requires_run_id() -> None:
    conn = _conn()

    try:
        generate_h74_execution_path_probe_report(conn, probe_run_id="")
    except ValueError as exc:
        assert str(exc) == "probe_run_id_required"
    else:
        raise AssertionError("expected probe_run_id_required")


def test_probe_report_without_matching_run_id_does_not_pass() -> None:
    conn = _conn()
    _seed_pass(conn, "other-run")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] != "PASS"


def test_probe_report_buy_only_does_not_pass() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] != "PASS"
    assert report["sell_order_id"] is None


def test_probe_report_sell_only_does_not_pass() -> None:
    conn = _conn()
    _seed_sell(conn, "probe-1")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] != "PASS"
    assert report["buy_order_id"] is None


def test_probe_report_orders_and_fills_mixed_run_ids_do_not_pass() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    _seed_sell(conn, "probe-1")
    conn.execute("UPDATE fills SET probe_run_id='other-run'")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] != "PASS"
    assert report["buy_fill_id"] is None


def test_probe_report_accounting_mixed_run_id_does_not_pass() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    _seed_sell(conn, "probe-1")
    conn.execute("UPDATE trades SET probe_run_id='other-run'")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] == "FAILED_ACCOUNTING"


def test_probe_report_lifecycle_mixed_run_id_does_not_pass() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    _seed_sell(conn, "probe-1")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('other-run', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] == "FAILED_LIFECYCLE"


def test_probe_report_final_flat_mixed_run_id_does_not_pass() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    _seed_sell(conn, "probe-1")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('other-run', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] == "FINAL_POSITION_NOT_FLAT"
    assert report["final_flat_or_documented_dust"] is False


def test_probe_report_missing_final_flat_evidence_does_not_pass() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    _seed_sell(conn, "probe-1")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] == "FINAL_POSITION_NOT_FLAT"


def test_probe_report_final_non_flat_evidence_does_not_pass() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    _seed_sell(conn, "probe-1")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0.001)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] == "FINAL_POSITION_NOT_FLAT"


def test_uncorrelated_legacy_rows_do_not_satisfy_probe_report() -> None:
    conn = _conn()
    _seed_pass(conn, "probe-1")
    for table in (
        "strategy_decisions",
        "execution_plan",
        "orders",
        "order_events",
        "fills",
        "trades",
        "open_position_lots",
        "h74_cycle_state",
        "trade_lifecycles",
        "portfolio",
    ):
        conn.execute(f"UPDATE {table} SET probe_run_id=NULL")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] != "PASS"


def test_probe_report_requires_open_lot_created() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1", open_lot=False)
    _seed_sell(conn, "probe-1")
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] != "PASS"
    assert report["buy_leg"]["open_lot_created"] is False


def test_probe_report_classifies_failed_accounting() -> None:
    conn = _conn()
    _seed_buy(conn, "probe-1")
    _seed_sell(conn, "probe-1", trade=False)
    conn.execute("INSERT INTO trade_lifecycles(probe_run_id, pair) VALUES('probe-1', 'KRW-BTC')")
    conn.execute("INSERT INTO portfolio(probe_run_id, pair, asset_qty) VALUES('probe-1', 'KRW-BTC', 0)")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] == "FAILED_ACCOUNTING"


def test_pass_report_contains_client_order_and_fill_identifiers() -> None:
    conn = _conn()
    _seed_pass(conn, "probe-1")

    report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-1")

    assert report["execution_path_probe_status"] == "PASS"
    for key in (
        "buy_decision_id",
        "buy_execution_plan_id",
        "buy_order_id",
        "buy_client_order_id",
        "buy_fill_id",
        "open_lot_id",
        "sell_decision_id",
        "sell_execution_plan_id",
        "sell_order_id",
        "sell_client_order_id",
        "sell_fill_id",
        "lifecycle_id",
    ):
        assert report[key]
