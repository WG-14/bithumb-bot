from __future__ import annotations

import sqlite3

from bithumb_bot.db_core import ensure_schema, init_portfolio
from bithumb_bot.h74_cycle_state import ensure_h74_cycle_schema
from bithumb_bot.h74_probe_acceptance import evaluate_h74_execution_path_probe_acceptance
from bithumb_bot.h74_probe_report import build_h74_execution_path_probe_report


_CONTRACT_HASH = "sha256:" + "1" * 64
_CONTRACT_JSON = (
    '{"authority_hash":"sha256:a","contract_hash":"'
    + _CONTRACT_HASH
    + '","cycle_id":"cycle-1","entry_plan_id":"probe-entry-plan",'
    '"entry_side":"BUY","h74_cycle_id":"cycle-1","hold_policy":"hold_acquired_fill_qty_until_max_holding_exit",'
    '"pair":"KRW-BTC","position_mode":"fixed_fill_qty_until_exit","probe_run_id":"probe-1",'
    '"strategy_instance_id":"h74-source-observation"}'
)


def _pass_report() -> dict[str, object]:
    return {
        "artifact_type": "h74_execution_path_probe_report",
        "probe_run_id": "probe-1",
        "execution_path_probe_status": "PASS",
        "buy_order_filled": True,
        "h74_cycle_ownership_created": True,
        "h74_cycle_id": "cycle-1",
        "h74_remaining_cycle_qty_before_sell": 0.0008,
        "sell_order_submitted": True,
        "sell_order_filled": True,
        "h74_cycle_state_closed": True,
        "portfolio_flat": True,
        "accounting_flat": True,
        "manual_intervention": False,
        "h74_exit_authority_ready": 1,
        "h74_remaining_cycle_qty": 0.0008,
        "h74_cycle_contract_hash": "sha256:contract",
        "h74_exit_authority_not_ready_reason": "none",
        "buy_decision_id": 1,
        "buy_execution_plan_id": 2,
        "buy_order_id": 3,
        "buy_client_order_id": "buy-1",
        "buy_fill_id": 4,
        "buy_order_h74_entry_plan_client_order_id": "probe-entry-plan",
        "buy_order_h74_position_ownership_contract": {"entry_plan_id": "probe-entry-plan"},
        "cycle_h74_entry_plan_client_order_id": "probe-entry-plan",
        "open_lot_id": 5,
        "sell_decision_id": 6,
        "sell_execution_plan_id": 7,
        "sell_order_id": 8,
        "sell_client_order_id": "sell-1",
        "sell_fill_id": 9,
        "lifecycle_id": 10,
        "buy_leg": {
            "decision_id": 1,
            "execution_plan_id": 2,
            "order_id": 3,
            "client_order_id": "buy-1",
            "fill_id": 4,
            "open_lot_id": 5,
        },
        "sell_leg": {
            "decision_id": 6,
            "execution_plan_id": 7,
            "order_id": 8,
            "client_order_id": "sell-1",
            "fill_id": 9,
            "lifecycle_id": 10,
        },
        "accounting": {"validated": True},
        "final_flat_or_documented_dust": True,
        "research_equivalence": False,
        "research_equivalence_status": "NOT_APPLICABLE",
        "production_approval": False,
    }


def test_acceptance_consumes_probe_report_schema() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(_pass_report())
    assert result["execution_path_probe_status"] == "PASS"
    assert result["acceptance_track"] == "execution_path_probe"
    assert result["sell_order_filled"] is True
    assert result["h74_cycle_state_closed"] is True
    assert result["portfolio_flat"] is True
    assert result["accounting_flat"] is True


def test_acceptance_rejects_report_without_lifecycle_id() -> None:
    report = _pass_report()
    report["lifecycle_id"] = None
    report["sell_leg"]["lifecycle_id"] = None
    result = evaluate_h74_execution_path_probe_acceptance(report)
    assert result["execution_path_probe_status"] != "PASS"
    assert "lifecycle_id" in result["missing_evidence"]


def test_acceptance_artifact_never_enables_research_or_production() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(_pass_report())
    assert result["research_equivalence"] is False
    assert result["research_equivalence_status"] == "NOT_APPLICABLE"
    assert result["production_approval"] is False
    assert result["promotion_grade"] is False


def test_h74_buy_only_does_not_pass_roundtrip_acceptance() -> None:
    report = _pass_report()
    report["sell_order_submitted"] = False
    report["sell_order_filled"] = False
    report["sell_order_id"] = None
    report["sell_leg"]["order_id"] = None

    result = evaluate_h74_execution_path_probe_acceptance(report)

    assert result["execution_path_probe_status"] != "PASS"
    assert "sell_order_submitted" in result["missing_evidence"]
    assert "sell_order_filled" in result["missing_evidence"]


def test_no_window_probe_acceptance_requires_buy_fill_cycle_sell_close() -> None:
    test_h74_buy_only_does_not_pass_roundtrip_acceptance()


def test_h74_manual_sell_does_not_count_as_automated_sell_success() -> None:
    report = _pass_report()
    report["manual_sell"] = True

    result = evaluate_h74_execution_path_probe_acceptance(report)

    assert result["execution_path_probe_status"] != "PASS"
    assert "automated_sell_required" in result["missing_evidence"]


def test_no_window_probe_acceptance_rejects_manual_flatten_as_sell_success() -> None:
    test_h74_manual_sell_does_not_count_as_automated_sell_success()


def test_no_window_probe_acceptance_rejects_missing_h74_cycle_state() -> None:
    report = _pass_report()
    report["h74_cycle_ownership_created"] = False
    report["h74_cycle_state_closed"] = False
    report["h74_cycle_id"] = ""

    result = evaluate_h74_execution_path_probe_acceptance(report)

    assert result["execution_path_probe_status"] != "PASS"
    assert "h74_cycle_ownership_created" in result["missing_evidence"]
    assert "h74_cycle_state_closed" in result["missing_evidence"]


def test_acceptance_uses_runtime_built_probe_report() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    ensure_h74_cycle_schema(conn)
    init_portfolio(conn)
    conn.execute(
        """
        INSERT INTO orders(
            probe_run_id, client_order_id, status, side, pair, price, qty_req,
            qty_filled, strategy_name, strategy_instance_id, cycle_id, authority_hash,
            h74_entry_plan_client_order_id, h74_position_ownership_contract_hash,
            h74_position_ownership_contract, entry_decision_id, created_ts, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "probe-1",
            "buy-1",
            "FILLED",
            "BUY",
            "KRW-BTC",
            100_000_000.0,
            0.0008,
            0.0008,
            "daily_participation_sma",
            "h74-source-observation",
            "cycle-1",
            "sha256:a",
            "probe-entry-plan",
            _CONTRACT_HASH,
            _CONTRACT_JSON,
            1,
            1,
            1,
        ),
    )
    conn.execute(
        """
        INSERT INTO orders(
            probe_run_id, client_order_id, status, side, pair, price, qty_req,
            qty_filled, strategy_name, strategy_instance_id, cycle_id, authority_hash,
            h74_position_ownership_contract_hash, exit_decision_id, decision_reason,
            created_ts, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "probe-1",
            "sell-1",
            "FILLED",
            "SELL",
            "KRW-BTC",
            100_000_000.0,
            0.0008,
            0.0008,
            "daily_participation_sma",
            "h74-source-observation",
            "cycle-1",
            "sha256:a",
            "sha256:contract",
            2,
            "max_holding_time",
            2,
            2,
        ),
    )
    conn.execute(
        """
        INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee)
        VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)
        """,
        (
            "buy-1",
            "buy-fill",
            1,
            100_000_000.0,
            0.0008,
            32.0,
            "sell-1",
            "sell-fill",
            2,
            100_000_000.0,
            0.0008,
            32.0,
        ),
    )
    conn.execute(
        """
        INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, client_order_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "KRW-BTC",
            "1m",
            "BUY",
            100_000_000.0,
            0.0008,
            32.0,
            920_000.0,
            0.0008,
            "buy-1",
            2,
            "KRW-BTC",
            "1m",
            "SELL",
            100_000_000.0,
            0.0008,
            32.0,
            999_936.0,
            0.0,
            "sell-1",
        ),
    )
    conn.execute(
        """
        INSERT INTO h74_cycle_state(
            cycle_id, authority_hash, strategy_instance_id, pair, state,
            entry_client_order_id, exit_client_order_id, acquired_qty, sold_qty,
            locked_exit_qty, contract_hash, h74_entry_plan_client_order_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "cycle-1",
            "sha256:a",
            "h74-source-observation",
            "KRW-BTC",
            "CLOSED",
            "buy-1",
            "sell-1",
            0.0008,
            0.0008,
            0.0,
            _CONTRACT_HASH,
            "probe-entry-plan",
        ),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair, entry_trade_id, entry_client_order_id, entry_ts, entry_price,
            qty_open, executable_lot_count, position_semantic_basis, position_state
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("KRW-BTC", 1, "buy-1", 1, 100_000_000.0, 0.0, 0, "lot-native", "open_exposure"),
    )
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            pair, entry_trade_id, exit_trade_id, entry_client_order_id,
            exit_client_order_id, entry_ts, exit_ts, matched_qty, entry_price,
            exit_price, gross_pnl, fee_total, net_pnl, holding_time_sec
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("KRW-BTC", 1, 2, "buy-1", "sell-1", 1, 2, 0.0008, 100_000_000.0, 100_000_000.0, 0.0, 64.0, -64.0, 1.0),
    )

    report = build_h74_execution_path_probe_report(conn, "probe-1")
    result = evaluate_h74_execution_path_probe_acceptance(report)

    assert report["execution_path_probe_status"] == "PASS"
    assert result["execution_path_probe_status"] == "PASS"
