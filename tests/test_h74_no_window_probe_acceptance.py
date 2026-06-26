from __future__ import annotations

import sqlite3

from bithumb_bot.h74_execution_path_probe import generate_h74_execution_path_probe_report
from bithumb_bot.h74_probe_acceptance import evaluate_h74_execution_path_probe_acceptance
from tests.test_h74_live_roundtrip import _acceptance_report


def _probe_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE strategy_decisions(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, signal TEXT, pair TEXT, decision_ts INTEGER
        );
        CREATE TABLE execution_plan(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, submit_plan_side TEXT, pair TEXT,
            submit_expected INTEGER
        );
        CREATE TABLE orders(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, side TEXT, pair TEXT, client_order_id TEXT,
            cycle_id TEXT, h74_entry_plan_client_order_id TEXT, h74_position_ownership_contract TEXT
        );
        CREATE TABLE order_events(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, side TEXT, pair TEXT, client_order_id TEXT,
            event_type TEXT
        );
        CREATE TABLE fills(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, side TEXT, pair TEXT, client_order_id TEXT,
            fill_id TEXT, qty REAL
        );
        CREATE TABLE open_position_lots(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, entry_client_order_id TEXT,
            cycle_id TEXT, qty_open REAL, contract_hash TEXT
        );
        CREATE TABLE h74_cycle_state(
            cycle_id TEXT PRIMARY KEY, state TEXT, acquired_qty REAL, sold_qty REAL, locked_exit_qty REAL,
            contract_hash TEXT, h74_entry_plan_client_order_id TEXT
        );
        CREATE TABLE trade_lifecycles(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, exit_client_order_id TEXT
        );
        CREATE TABLE portfolio(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, pair TEXT, asset_qty REAL
        );
        CREATE TABLE trades(
            id INTEGER PRIMARY KEY, probe_run_id TEXT, side TEXT, pair TEXT, client_order_id TEXT
        );
        """
    )
    for row_id, side in ((1, "BUY"), (2, "SELL")):
        conn.execute(
            "INSERT INTO strategy_decisions(id, probe_run_id, signal, pair, decision_ts) VALUES (?, ?, ?, ?, ?)",
            (row_id, "probe-run-1", side, "KRW-BTC", row_id),
        )
        conn.execute(
            "INSERT INTO execution_plan(id, probe_run_id, submit_plan_side, pair, submit_expected) VALUES (?, ?, ?, ?, 1)",
            (row_id, "probe-run-1", side, "KRW-BTC"),
        )
        client_order_id = "buy-1" if side == "BUY" else "sell-1"
        conn.execute(
            """
            INSERT INTO orders(
                id, probe_run_id, side, pair, client_order_id, cycle_id,
                h74_entry_plan_client_order_id, h74_position_ownership_contract
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row_id,
                "probe-run-1",
                side,
                "KRW-BTC",
                client_order_id,
                "cycle-1",
                "h74-entry-plan-1",
                '{"entry_plan_id":"h74-entry-plan-1"}',
            ),
        )
        conn.execute(
            "INSERT INTO order_events(id, probe_run_id, side, pair, client_order_id, event_type) VALUES (?, ?, ?, ?, ?, ?)",
            (row_id, "probe-run-1", side, "KRW-BTC", client_order_id, "submit_attempt_preflight"),
        )
        conn.execute(
            "INSERT INTO fills(id, probe_run_id, side, pair, client_order_id, fill_id, qty) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row_id, "probe-run-1", side, "KRW-BTC", client_order_id, f"{side.lower()}-fill", 0.0008),
        )
        conn.execute(
            "INSERT INTO trades(id, probe_run_id, side, pair, client_order_id) VALUES (?, ?, ?, ?, ?)",
            (row_id, "probe-run-1", side, "KRW-BTC", client_order_id),
        )
    conn.execute(
        """
        INSERT INTO open_position_lots(id, probe_run_id, pair, entry_client_order_id, cycle_id, qty_open, contract_hash)
        VALUES (1, 'probe-run-1', 'KRW-BTC', 'buy-1', 'cycle-1', 0, 'sha256:contract')
        """
    )
    conn.execute(
        """
        INSERT INTO h74_cycle_state(
            cycle_id, state, acquired_qty, sold_qty, locked_exit_qty, contract_hash,
            h74_entry_plan_client_order_id
        )
        VALUES ('cycle-1', 'CLOSED', 0.0008, 0.0008, 0, 'sha256:contract', 'h74-entry-plan-1')
        """
    )
    conn.execute(
        "INSERT INTO trade_lifecycles(id, probe_run_id, pair, exit_client_order_id) VALUES (1, 'probe-run-1', 'KRW-BTC', 'sell-1')"
    )
    conn.execute("INSERT INTO portfolio(id, probe_run_id, pair, asset_qty) VALUES (1, 'probe-run-1', 'KRW-BTC', 0)")
    return conn


def test_no_window_probe_acceptance_rejects_buy_without_cycle_state() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(
        _acceptance_report(
            h74_cycle_ownership_created=False,
            h74_cycle_state_closed=False,
            cycle_h74_entry_plan_client_order_id="",
        )
    )

    assert result["execution_path_probe_status"] != "PASS"
    assert "h74_cycle_ownership_created" in result["missing_evidence"]
    assert "cycle_h74_entry_plan_client_order_id" in result["missing_evidence"]


def test_no_window_probe_acceptance_rejects_buy_without_sell() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(
        _acceptance_report(
            sell_order_submitted=False,
            sell_order_filled=False,
            sell_order_id=None,
            sell_fill_id=None,
            lifecycle_id=None,
            sell_leg={
                "decision_id": None,
                "execution_plan_id": None,
                "order_id": None,
                "client_order_id": None,
                "fill_id": None,
                "lifecycle_id": None,
            },
        )
    )

    assert result["execution_path_probe_status"] != "PASS"
    assert "sell_order_submitted" in result["missing_evidence"]
    assert "sell_order_filled" in result["missing_evidence"]


def test_no_window_probe_acceptance_rejects_entry_plan_mismatch() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(
        _acceptance_report(cycle_h74_entry_plan_client_order_id="different-entry")
    )

    assert result["execution_path_probe_status"] != "PASS"
    assert "h74_entry_plan_identity_match" in result["missing_evidence"]


def test_no_window_probe_report_generator_requires_buy_sell_cycle_close_flat() -> None:
    conn = _probe_conn()
    try:
        report = generate_h74_execution_path_probe_report(conn, probe_run_id="probe-run-1")
        acceptance = evaluate_h74_execution_path_probe_acceptance(report)
    finally:
        conn.close()

    assert report["execution_path_probe_status"] == "PASS"
    assert report["h74_cycle_ownership_created"] is True
    assert report["h74_cycle_state_closed"] is True
    assert report["sell_order_filled"] is True
    assert report["portfolio_flat"] is True
    assert acceptance["execution_path_probe_status"] == "PASS"
