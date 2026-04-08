from __future__ import annotations

import json
import os

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.broker import order_rules
from bithumb_bot.db_core import (
    ensure_db,
    init_portfolio,
    record_external_cash_adjustment,
    record_strategy_decision,
)
from bithumb_bot.config import PATH_MANAGER
from bithumb_bot.engine import evaluate_startup_safety_gate
from bithumb_bot.reporting import _sell_failure_category_from_observability, cmd_ops_report
from bithumb_bot.oms import record_order_suppression
from bithumb_bot.utils_time import kst_str
from bithumb_bot.reason_codes import (
    DUST_RESIDUAL_SUPPRESSED,
    DUST_RESIDUAL_UNSELLABLE,
    SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
    SELL_FAILURE_CATEGORY_DUST_RESIDUAL_UNSELLABLE,
    SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
    SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH,
)


def test_ops_report_with_strategy_and_trade_data(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr(
        "bithumb_bot.reporting.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5500.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
            ),
            source={
                "min_qty": "local_fallback",
                "qty_step": "local_fallback",
                "min_notional_krw": "local_fallback",
                "max_qty_decimals": "local_fallback",
                "bid_min_total_krw": "chance_doc",
                "ask_min_total_krw": "chance_doc",
                "bid_price_unit": "chance_doc",
                "ask_price_unit": "chance_doc",
            },
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_balance_snapshot": lambda self: None,
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "accounts_v1_rest_snapshot",
                    "reason": "ok",
                    "failure_category": "none",
                    "stale": False,
                    "last_success_ts_ms": 1710000000000,
                    "last_asset_ts_ms": 1710000000000,
                },
            },
        )(),
    )

    conn = ensure_db()
    try:
        init_portfolio(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "remote_open_order_found": 0,
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 0,
                "dust_policy_reason": "dangerous_dust_operator_review_required",
                "dust_residual_summary": "broker_qty=0.00009000 local_qty=0.00009000 min_qty=0.00010000",
                "dust_broker_qty": 0.00009,
                "dust_local_qty": 0.00009,
                "dust_delta_qty": 0.0,
                "dust_min_qty": 0.0001,
                "dust_min_notional_krw": 5000.0,
                "dust_broker_qty_is_dust": 1,
                "dust_local_qty_is_dust": 1,
                "dust_broker_notional_is_dust": 0,
                "dust_local_notional_is_dust": 0,
                "dust_qty_gap_small": 1,
            },
            now_epoch_sec=1000.0,
        )
        conn.execute(
            """
            INSERT INTO order_intent_dedup(
                intent_key, symbol, side, strategy_context, intent_type, intent_ts, qty,
                client_order_id, order_status, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                "intent-1",
                "BTC_KRW",
                "BUY",
                "paper:sma_cross:1m",
                "ENTRY",
                1,
                0.001,
                "coid-1",
                "FILLED",
                1,
                1,
            ),
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-1", "ex-1", "FILLED", "BUY", 100000000.0, 0.001, 0.001, 1, 2),
        )
        conn.execute(
            """
            INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("coid-1", "fill-1", 3, 100000000.0, 0.001, 50.0),
        )
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, side, qty, price, submission_reason_code, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("coid-1", "submit_attempt_recorded", 4, "FILLED", "BUY", 0.001, 100000000.0, "SIGNAL_BUY", "submit ok"),
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("dust-sell-1", None, "FAILED", "SELL", 100000000.0, 0.00009, 0.0, 6, 6),
        )
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, side, qty, price, submission_reason_code, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "dust-sell-1",
                "submit_attempt_recorded",
                6,
                "FAILED",
                "SELL",
                0.00009,
                100000000.0,
                "DUST_RESIDUAL_UNSELLABLE",
                "state=EXIT_PARTIAL_LEFT_DUST;operator_action=MANUAL_DUST_REVIEW_REQUIRED;position_qty=0.000090000000",
            ),
        )
        conn.execute(
            """
            INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (5, "BTC_KRW", "1m", "BUY", 100000000.0, 0.001, 50.0, 900000.0, 0.001, "paper fill"),
        )
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
                entry_ts, exit_ts, matched_qty, entry_price, exit_price, gross_pnl, fee_total, net_pnl,
                holding_time_sec, strategy_name, entry_decision_id, entry_decision_linkage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "KRW-BTC",
                1,
                2,
                "coid-1",
                "coid-2",
                900_000,
                1_001_000,
                0.001,
                100000000.0,
                100100000.0,
                100.0,
                50.0,
                50.0,
                60.0,
                "strategy_ops",
                None,
                "degraded_recovery_submit_unknown",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=5)
    out = capsys.readouterr().out

    assert "[OPS-REPORT]" in out
    assert "market=KRW-BTC" in out
    assert f"db_path={db_path}" in out
    assert "paper:sma_cross:1m,1,1,100000.00,0.00,50.00,-100050.00" in out
    assert "order_events_qty=" in out
    assert "submit_payload_qty=" in out
    assert "event=submit_attempt_recorded" in out
    assert "reason=DUST_RESIDUAL_UNSELLABLE" in out
    assert "sell_failure_category=dust_residual_unsellable" in out
    assert "EXIT_PARTIAL_LEFT_DUST" in out
    assert "note=paper fill" in out
    assert "[ORDER-RULE-SNAPSHOT]" in out
    assert "BUY(min_total_krw=5500.0 (source=chance_doc), price_unit=10.0 (source=chance_doc))" in out
    assert "balance_source=accounts_v1_rest_snapshot" in out
    assert "category=none stale=False execution_mode=- quote_currency=- base_currency=-" in out
    assert "unresolved_open_order_count=0 recovery_required_count=0 dust_state=blocking_dust" in out
    assert "dust_action=manual_review_before_resume" in out
    assert "dust_new_orders_allowed=0 dust_resume_allowed=0 dust_treat_as_flat=0" in out
    assert "dust_broker_qty=0.00009000 dust_local_qty=0.00009000 dust_delta_qty=0.00000000" in out
    assert "dust_min_qty=0.00010000 dust_min_notional_krw=5000.0" in out
    assert "raw_holdings_state=blocking_dust" in out
    assert "accounts_flat_start_allowed=None" in out
    assert "unresolved_attribution_count=1 recent_recovery_derived_trade_count=1" in out

    payload = json.loads(PATH_MANAGER.ops_report_path().read_text(encoding="utf-8"))
    assert payload["recovery_attribution_quality_signals"]["unresolved_attribution_count"] == 1
    assert payload["recovery_attribution_quality_signals"]["recent_recovery_derived_trade_count"] == 1
    assert payload["recovery_attribution_quality_signals"]["ambiguous_linkage_after_recent_reconcile"] is False
    assert payload["operator_recovery_summary"]["dust_state"] == "blocking_dust"
    assert payload["operator_recovery_summary"]["dust_new_orders_allowed"] is False
    assert payload["operator_recovery_summary"]["raw_holdings"]["classification"] == "blocking_dust"
    assert payload["operator_recovery_summary"]["normalized_exposure"]["normalized_exposure_active"] is False
    assert payload["operator_recovery_summary"]["operator_diagnostics"]["state"] == "blocking_dust"
    assert "recent_external_cash_adjustment=none" in out


def test_ops_report_shows_recent_external_cash_adjustment_summary(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report-adjustment.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr(
        "bithumb_bot.reporting.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5500.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
            ),
            source={
                "min_qty": "local_fallback",
                "qty_step": "local_fallback",
                "min_notional_krw": "local_fallback",
                "max_qty_decimals": "local_fallback",
                "bid_min_total_krw": "chance_doc",
                "ask_min_total_krw": "chance_doc",
                "bid_price_unit": "chance_doc",
                "ask_price_unit": "chance_doc",
            },
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_balance_snapshot": lambda self: None,
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "accounts_v1_rest_snapshot",
                    "reason": "ok",
                    "failure_category": "none",
                    "stale": False,
                    "last_success_ts_ms": 1710000000000,
                    "last_asset_ts_ms": 1710000000000,
                },
            },
        )(),
    )

    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_external_cash_adjustment(
            conn,
            event_ts=1710000000000,
            currency="KRW",
            delta_amount=123.0,
            source="legacy_balance_api",
            reason="reconcile_cash_drift",
            broker_snapshot_basis={"cash_available": 1000000.0},
            note="ops test adjustment",
            adjustment_key="ops-report-adjustment-1",
        )
    finally:
        conn.close()

    cmd_ops_report(limit=1)
    out = capsys.readouterr().out

    expected_last_event = kst_str(1710000000000)
    assert "recent_external_cash_adjustment=count=1 total=123.000" in out
    assert f"last_event={expected_last_event}" in out
    assert "source=legacy_balance_api" in out
    assert "reason=reconcile_cash_drift" in out


def test_ops_report_uses_env_db_path_without_hardcoded_path(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "env-db.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr(
        "bithumb_bot.reporting.get_effective_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("rules unavailable")),
    )
    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_balance_snapshot": lambda self: None,
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "myasset_ws_private_stream",
                    "reason": "myAsset stream stale",
                    "failure_category": "stale_source",
                    "stale": True,
                },
            },
        )(),
    )

    conn = ensure_db()
    conn.close()

    assert os.path.exists(db_path)
    cmd_ops_report(limit=1)
    out = capsys.readouterr().out
    assert "market=KRW-BTC" in out
    assert f"db_path={db_path}" in out
    assert "no strategy_context rows" in out
    assert "failed_to_load=RuntimeError: rules unavailable" in out
    assert "balance_source=myasset_ws_private_stream" in out
    assert "category=stale_source stale=True execution_mode=- quote_currency=- base_currency=-" in out
    assert "accounts_flat_start_allowed=None" in out


def test_ops_report_surfaces_resume_safe_dust_without_hiding_unresolved_open_orders(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report-dust-unresolved.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr(
        "bithumb_bot.reporting.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5500.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
            ),
            source={},
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_balance_snapshot": lambda self: None,
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "accounts_v1_rest_snapshot",
                    "reason": "ok",
                    "failure_category": "none",
                    "stale": False,
                },
            },
        )(),
    )

    conn = ensure_db()
    try:
        init_portfolio(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECENT_FILL_APPLIED",
            metadata={
                "remote_open_order_found": 1,
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 1,
                "dust_policy_reason": "matched_harmless_dust_resume_allowed",
                "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000",
                "dust_broker_qty": 0.00009629,
                "dust_local_qty": 0.00009629,
                "dust_delta_qty": 0.0,
                "dust_min_qty": 0.0001,
                "dust_min_notional_krw": 5000.0,
                "dust_broker_qty_is_dust": 1,
                "dust_local_qty_is_dust": 1,
                "dust_broker_notional_is_dust": 1,
                "dust_local_notional_is_dust": 1,
                "dust_qty_gap_small": 1,
            },
            now_epoch_sec=1000.0,
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("open-dust-1", "ex-open-1", "NEW", "SELL", 100000000.0, 0.00009, 0.0, 10, 10),
        )
        conn.commit()
    finally:
        conn.close()

    evaluate_startup_safety_gate()
    cmd_ops_report(limit=3)
    out = capsys.readouterr().out

    assert "unresolved_open_order_count=1 recovery_required_count=0" in out
    assert "dust_state=harmless_dust" in out
    assert "dust_new_orders_allowed=1 dust_resume_allowed=1" in out

    payload = json.loads(PATH_MANAGER.ops_report_path().read_text(encoding="utf-8"))
    assert payload["operator_recovery_summary"]["unresolved_open_order_count"] == 1
    assert payload["operator_recovery_summary"]["dust_resume_allowed_by_policy"] is True


def test_ops_report_surfaces_dangerous_dust_alongside_unresolved_open_orders(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report-dangerous-dust-unresolved.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr(
        "bithumb_bot.reporting.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5500.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
            ),
            source={},
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_balance_snapshot": lambda self: None,
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "accounts_v1_rest_snapshot",
                    "reason": "ok",
                    "failure_category": "none",
                    "stale": False,
                },
            },
        )(),
    )

    conn = ensure_db()
    try:
        init_portfolio(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "remote_open_order_found": 1,
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 0,
                "dust_policy_reason": "dangerous_dust_operator_review_required",
                "dust_residual_summary": (
                    "broker_qty=0.00009900 local_qty=0.00001000 delta=0.00008900 "
                    "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=0 "
                    "classification=blocking_dust harmless_dust=0 broker_local_match=0 "
                    "allow_resume=0 effective_flat=0 policy_reason=dangerous_dust_operator_review_required"
                ),
                "dust_broker_qty": 0.000099,
                "dust_local_qty": 0.00001,
                "dust_delta_qty": 0.000089,
                "dust_min_qty": 0.0001,
                "dust_min_notional_krw": 5000.0,
                "dust_broker_qty_is_dust": 1,
                "dust_local_qty_is_dust": 1,
                "dust_broker_notional_is_dust": 1,
                "dust_local_notional_is_dust": 1,
                "dust_qty_gap_small": 0,
            },
            now_epoch_sec=1000.0,
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("open-dangerous-dust-1", "ex-open-dangerous-1", "NEW", "SELL", 100000000.0, 0.00009, 0.0, 10, 10),
        )
        conn.commit()
    finally:
        conn.close()

    evaluate_startup_safety_gate()
    cmd_ops_report(limit=3)
    out = capsys.readouterr().out

    assert "unresolved_open_order_count=1 recovery_required_count=0" in out
    assert "dust_state=blocking_dust" in out
    assert "dust_action=manual_review_before_resume" in out
    assert "dust_new_orders_allowed=0 dust_resume_allowed=0 dust_treat_as_flat=0" in out
    assert "dust_broker_local_match=0" in out
    assert "dust_qty_below_min=broker=1 local=1" in out
    assert "dust_notional_below_min=broker=1 local=1" in out

    payload = json.loads(PATH_MANAGER.ops_report_path().read_text(encoding="utf-8"))
    summary = payload["operator_recovery_summary"]
    assert summary["unresolved_open_order_count"] == 1
    assert summary["dust_state"] == "blocking_dust"
    assert summary["dust_broker_local_match"] is False
    assert summary["dust_resume_allowed_by_policy"] is False
    assert summary["dust_treat_as_flat"] is False


def test_ops_report_keeps_dust_detail_when_reconcile_metadata_is_trimmed(tmp_path, monkeypatch, capsys):
    expected_qty = 0.00009193
    expected_summary = (
        "broker_qty=0.00009193 local_qty=0.00009193 delta=0.00000000 "
        "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
        "classification=harmless_dust harmless_dust=1 broker_local_match=1 allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
    )
    db_path = str(tmp_path / "ops-report-dust-trimmed.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    monkeypatch.setattr(
        "bithumb_bot.reporting.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5500.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
            ),
            source={},
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_balance_snapshot": lambda self: None,
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "accounts_v1_rest_snapshot",
                    "reason": "ok",
                    "failure_category": "none",
                    "stale": False,
                    "flat_start_reason": (
                        "flat_start_requires_operator_review("
                        "state=blocking_dust broker_qty=0.00000000 "
                        "local_qty=0.00009193 delta_qty=-0.00009193 min_qty=0.00010000 "
                        "min_notional_krw=5000.0 qty_below_min(broker=0 local=1) "
                        "notional_below_min(broker=0 local=0) broker_local_match=0 "
                        "operator_action=manual_review_before_resume new_orders_allowed=0 "
                        "resume_allowed=0 treat_as_flat=0)"
                    ),
                },
            },
        )(),
    )

    conn = ensure_db()
    try:
        init_portfolio(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "remote_open_order_found": 0,
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 1,
                "dust_policy_reason": "matched_harmless_dust_resume_allowed",
                "dust_residual_summary": expected_summary,
                "dust_broker_qty": expected_qty,
                "dust_local_qty": expected_qty,
                "dust_delta_qty": 0.0,
                "dust_min_qty": 0.0001,
                "dust_min_notional_krw": 5000.0,
                "dust_latest_price": 100000000.0,
                "dust_broker_qty_is_dust": 1,
                "dust_local_qty_is_dust": 1,
                "dust_broker_notional_is_dust": 0,
                "dust_local_notional_is_dust": 0,
                "dust_qty_gap_small": 1,
                "oversized_debug_blob": "x" * 5000,
            },
            now_epoch_sec=1000.0,
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=1)
    out = capsys.readouterr().out

    assert f"broker_qty={expected_qty:.8f}" in expected_summary
    assert f"local_qty={expected_qty:.8f}" in expected_summary
    assert "allow_resume=1" in expected_summary
    assert "policy_reason=matched_harmless_dust_resume_allowed" in expected_summary
    assert "dust_state=harmless_dust" in out
    assert "dust_action=harmless_dust_tracked_resume_allowed" in out
    assert "dust_new_orders_allowed=1 dust_resume_allowed=1 dust_treat_as_flat=1" in out
    assert "raw_holdings_state=harmless_dust" in out
    assert "entry_allowed=1 normalized_exposure_active=0" in out
    assert (
        f"dust_broker_qty={expected_qty:.8f} dust_local_qty={expected_qty:.8f} "
        "dust_delta_qty=0.00000000 dust_min_qty=0.00010000 dust_min_notional_krw=5000.0"
    ) in out
    assert "dust_broker_local_match=1" in out
    assert "dust_qty_below_min=broker=1 local=1" in out
    assert "dust_notional_below_min=broker=0 local=0" in out
    assert "accounts_flat_start_allowed=True" in out
    assert "accounts_flat_start_reason=flat_start_effective_flat(" in out
    payload = json.loads(PATH_MANAGER.ops_report_path().read_text(encoding="utf-8"))
    summary = payload["operator_recovery_summary"]
    assert summary["dust_state"] == "harmless_dust"
    assert summary["dust_operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert summary["dust_new_orders_allowed"] is True
    assert summary["dust_resume_allowed_by_policy"] is True
    assert summary["dust_treat_as_flat"] is True
    assert summary["dust_effective_flat"] is True
    assert summary["effective_flat_due_to_harmless_dust"] is True
    assert summary["raw_holdings"]["classification"] == "harmless_dust"
    assert summary["normalized_exposure"]["normalized_exposure_active"] is False
    assert summary["operator_diagnostics"]["resume_allowed"] is True
    assert summary["dust_broker_qty"] == pytest.approx(expected_qty)
    assert summary["dust_local_qty"] == pytest.approx(expected_qty)
    assert summary["dust_broker_local_match"] is True
    assert summary["dust_broker_qty_below_min"] is True
    assert summary["dust_local_qty_below_min"] is True
    assert summary["dust_broker_notional_below_min"] is False
    assert summary["dust_local_notional_below_min"] is False
    assert payload["balance_source_diagnostics"]["flat_start_reason"] == (
        "flat_start_effective_flat("
        "state=harmless_dust broker_qty=0.00009193 local_qty=0.00009193 "
        "delta_qty=0.00000000 min_qty=0.00010000 min_notional_krw=5000.0 "
        "qty_below_min(broker=1 local=1) notional_below_min(broker=0 local=0) "
        "broker_local_match=1 operator_action=harmless_dust_tracked_resume_allowed "
        "new_orders_allowed=1 resume_allowed=1 treat_as_flat=1)"
    )


def test_ops_report_includes_recent_decision_flow_truth_sources(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report-decision-flow.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=3,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=3,
            market_price=102_500_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "effective_flat": True,
                "raw_qty_open": 0.00009629,
                "raw_total_asset_qty": 0.00019192,
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009563,
                "submit_qty_source": "position_state.normalized_exposure.open_exposure_qty",
                "position_state_source": "context.raw_qty_open",
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.00009629,
                        "raw_total_asset_qty": 0.00019192,
                        "open_exposure_qty": 0.00009629,
                        "dust_tracking_qty": 0.00009563,
                        "submit_qty_source": "position_state.normalized_exposure.open_exposure_qty",
                        "position_state_source": "context.raw_qty_open",
                        "entry_allowed": True,
                        "effective_flat": True,
                        "normalized_exposure_active": False,
                        "normalized_exposure_qty": 0.0,
                    }
                },
            },
        )
        record_strategy_decision(
            conn,
            decision_ts=2,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="position held: no exit rule triggered",
            candle_ts=2,
            market_price=102_400_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "filtered entry: gap",
                "blocked_filters": ["gap"],
                "filter_blocked": True,
                "position_gate": {
                    "effective_flat_due_to_harmless_dust": False,
                    "raw_qty_open": 0.00009629,
                },
            },
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=5)
    out = capsys.readouterr().out

    assert "[RECENT-STRATEGY-DECISION-FLOW]" in out
    assert "flow=BUY_SUBMIT" in out
    assert "flow=BUY_BLOCKED" in out
    assert "submit_qty_source=position_state.normalized_exposure.open_exposure_qty" in out
    assert "sell_submit_qty_source=position_state.normalized_exposure.open_exposure_qty" in out
    assert "sell_normalized_exposure_qty=0.00009629" in out
    assert "position_qty=0.00009629" in out
    assert "submit_payload_qty=0.00000000" in out or "submit_payload_qty=0" in out
    assert "position_state_source=context.raw_qty_open" in out
    assert "raw_total_asset_qty=0.00019192" in out
    assert "open_exposure_qty=0.00009629" in out
    assert "dust_tracking_qty=0.00009563" in out
    assert "entry_allowed_truth_source=context.entry_allowed" in out
    assert "effective_flat_truth_source=context.effective_flat" in out or "effective_flat_truth_source=position_gate.effective_flat_due_to_harmless_dust" in out


def test_ops_report_surfaces_top_level_position_state_truth_sources(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report-position-state-top-level.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=4,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=4,
            market_price=102_500_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "position_state": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.0,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.0,
                },
            },
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=5)
    out = capsys.readouterr().out

    assert "raw_qty_open_truth_source=position_state.raw_qty_open" in out
    assert "raw_total_asset_qty_truth_source=position_state.raw_total_asset_qty" in out
    assert "open_exposure_qty_truth_source=position_state.open_exposure_qty" in out
    assert "dust_tracking_qty_truth_source=position_state.dust_tracking_qty" in out
    assert "position_state_source_truth_source=context.position_state_source" in out




def test_sell_failure_category_from_observability_flags_boundary_below_min_from_message():
    category = _sell_failure_category_from_observability(
        submission_reason_code=DUST_RESIDUAL_UNSELLABLE,
        message="state=EXIT_PARTIAL_LEFT_DUST;qty_below_min=1;normalized_below_min=0;notional_below_min=0",
        submit_evidence=None,
    )

    assert category == SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN


def test_sell_failure_category_from_observability_flags_qty_step_mismatch_from_message():
    category = _sell_failure_category_from_observability(
        submission_reason_code="SUBMIT_FAILED",
        message="state=EXIT_PARTIAL_LEFT_DUST;qty_step=0.0001;normalized_qty=0.00009",
        submit_evidence=None,
    )

    assert category == "qty_step_mismatch"


def test_sell_failure_category_from_observability_prefers_boundary_kind_over_unsafe_mismatch():
    category = _sell_failure_category_from_observability(
        submission_reason_code=DUST_RESIDUAL_UNSELLABLE,
        message="state=EXIT_PARTIAL_LEFT_DUST;operator_action=MANUAL_DUST_REVIEW_REQUIRED",
        submit_evidence=None,
        dust_details={
            "sell_qty_boundary_kind": "min_qty",
            "sell_failure_category": "unsafe_dust_mismatch_dust",
            "qty_below_min": 1,
            "dust_qty_gap_small": 1,
            "summary": "sell_qty_boundary_kind=min_qty;qty_below_min=1;dust_qty_gap_small=1",
        },
    )

    assert category == SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN


def test_sell_failure_category_from_observability_prefers_qty_step_boundary_kind():
    category = _sell_failure_category_from_observability(
        submission_reason_code=DUST_RESIDUAL_UNSELLABLE,
        message="state=EXIT_PARTIAL_LEFT_DUST;operator_action=MANUAL_DUST_REVIEW_REQUIRED",
        submit_evidence=None,
        dust_details={
            "sell_qty_boundary_kind": "qty_step",
            "summary": "sell_qty_boundary_kind=qty_step;qty_step=0.0001",
        },
    )

    assert category == SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH


def test_sell_failure_category_from_observability_flags_unsafe_mismatch_from_evidence():
    category = _sell_failure_category_from_observability(
        submission_reason_code=DUST_RESIDUAL_UNSELLABLE,
        message="state=EXIT_PARTIAL_LEFT_DUST;operator_action=MANUAL_DUST_REVIEW_REQUIRED",
        submit_evidence=json.dumps(
            {
                "dust_qty_gap_small": 1,
                "dust_broker_qty_is_dust": 1,
                "dust_local_qty_is_dust": 1,
                "summary": "dust_qty_gap_small=1 dust_broker_qty_is_dust=1 dust_local_qty_is_dust=1",
            }
        ),
    )

    assert category == SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH


def test_sell_failure_category_from_observability_flags_submission_halt_from_message():
    category = _sell_failure_category_from_observability(
        submission_reason_code="RISKY_ORDER_BLOCK",
        message="runtime halted: code=KILL_SWITCH reason=operator stop",
        submit_evidence=None,
    )

    assert category == "submission_halt"


def test_sell_failure_category_from_observability_flags_unresolved_risk_gate_from_message():
    category = _sell_failure_category_from_observability(
        submission_reason_code="RISKY_ORDER_BLOCK",
        message="category=unresolved_risk_gate;code=open_order_timeout;reason=unresolved",
        submit_evidence=None,
    )

    assert category == "unresolved_risk_gate"


def test_ops_report_classifies_unresolved_risk_gate_in_recent_flow(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report-unresolved-risk-gate.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-risk-gate", "ex-risk-gate", "FAILED", "SELL", 100000000.0, 0.0002, 0.0, 10, 10),
        )
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, side, qty, price, submission_reason_code, message, submit_evidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "coid-risk-gate",
                "submit_attempt_recorded",
                11,
                "FAILED",
                "SELL",
                0.0002,
                100000000.0,
                "RISKY_ORDER_BLOCK",
                "category=unresolved_risk_gate;reason_detail_code=open_order_timeout;reason=unresolved order gate",
                json.dumps(
                    {
                        "sell_failure_category": "none",
                        "sell_failure_detail": "none",
                        "submit_qty_source": "position_state.normalized_exposure.open_exposure_qty",
                        "sell_submit_qty_source": "position_state.normalized_exposure.open_exposure_qty",
                        "sell_qty_boundary_kind": "none",
                        "sell_qty_basis_qty": 0.0002,
                        "sell_qty_basis_source": "position_state.normalized_exposure.open_exposure_qty",
                    }
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=5)
    out = capsys.readouterr().out

    assert "[RECENT-STRATEGY-ORDER-FILL-FLOW]" in out
    assert "sell_failure_category=unresolved_risk_gate" in out
    assert "sell_failure_detail=unresolved_risk_gate" in out
    assert "reason=RISKY_ORDER_BLOCK" in out
    assert "reason_detail_code=open_order_timeout" in out


def test_ops_report_includes_sell_suppression_category(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report-sell-suppression.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_order_suppression(
            suppression_key="sell-suppression-1",
            event_kind="sell_dust_exit",
            mode="live",
            strategy_context="live:sma_with_filter:1m",
            strategy_name="sma_with_filter",
            signal="SELL",
            side="SELL",
            reason_code=DUST_RESIDUAL_SUPPRESSED,
            reason="category=dust_suppression;decision_suppressed:harmless_dust_exit",
            requested_qty=0.0002,
            normalized_qty=0.00009629,
            market_price=102_500_000.0,
            context={
                "submit_qty_source": "position_state.normalized_exposure.open_exposure_qty",
                "sell_submit_qty_source": "position_state.normalized_exposure.open_exposure_qty",
                "submit_qty_source_truth_source": "context.submit_qty_source",
                "sell_submit_qty_source_truth_source": "context.submit_qty_source",
                "position_state_source_truth_source": "context.position_state_source",
                "raw_qty_open_truth_source": "position_state.raw_qty_open",
                "raw_total_asset_qty_truth_source": "position_state.raw_total_asset_qty",
                "open_exposure_qty_truth_source": "position_state.open_exposure_qty",
                "dust_tracking_qty_truth_source": "position_state.dust_tracking_qty",
                "entry_allowed_truth_source": "position_gate.entry_allowed",
                "effective_flat_truth_source": "position_gate.effective_flat_due_to_harmless_dust",
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009563,
                "sell_open_exposure_qty": 0.00009629,
                "sell_dust_tracking_qty": 0.00009563,
                "sell_failure_category": "dust_suppression",
                "sell_failure_detail": "dust_suppression",
                "operator_action": "harmless_dust_tracked_resume_allowed",
                "dust_action": "harmless_dust_tracked_resume_allowed",
            },
            dust_present=True,
            dust_allow_resume=True,
            dust_effective_flat=True,
            dust_state="harmless_dust",
            dust_action="harmless_dust_tracked_resume_allowed",
            summary="state=harmless_dust;operator_action=harmless_dust_tracked_resume_allowed",
            conn=conn,
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=5)
    out = capsys.readouterr().out

    assert "[RECENT-SELL-SUPPRESSIONS]" in out
    assert "reason=DUST_RESIDUAL_SUPPRESSED" in out
    assert "sell_failure_category=dust_suppression" in out
    assert "sell_failure_detail=dust_suppression" in out
    assert "submit_qty_source=position_state.normalized_exposure.open_exposure_qty" in out
    assert "submit_qty_source_truth_source=context.submit_qty_source" in out
    assert "sell_submit_qty_source=position_state.normalized_exposure.open_exposure_qty" in out
    assert "sell_submit_qty_source_truth_source=context.submit_qty_source" in out
    assert "sell_qty_basis_qty=0.00009629" in out
    assert "sell_qty_basis_source=position_state.normalized_exposure.open_exposure_qty" in out
    assert "operator_action=harmless_dust_tracked_resume_allowed" in out
    assert "open_exposure_qty=0.00009629" in out
    assert "open_exposure_qty_truth_source=position_state.open_exposure_qty" in out
    assert "dust_tracking_qty=0.00009563" in out
    assert "dust_tracking_qty_truth_source=position_state.dust_tracking_qty" in out
    assert "harmless_dust_tracked_resume_allowed" in out
    assert "entry_allowed_truth_source=position_gate.entry_allowed" in out
    assert "effective_flat_truth_source=position_gate.effective_flat_due_to_harmless_dust" in out

    payload = json.loads(PATH_MANAGER.ops_report_path().read_text(encoding="utf-8"))
    sell_suppression = payload["recent_sell_suppressions"][0]
    assert sell_suppression["operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert sell_suppression["sell_submit_qty_source_truth_source"] == "context.submit_qty_source"
    assert sell_suppression["sell_qty_basis_qty"] == pytest.approx(0.00009629)
    assert sell_suppression["sell_qty_basis_source"] == "position_state.normalized_exposure.open_exposure_qty"


def test_ops_report_prefers_boundary_below_min_over_unsafe_dust_mismatch_in_recent_flow(
    tmp_path,
    monkeypatch,
    capsys,
):
    db_path = str(tmp_path / "ops-report-boundary-precedence.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-boundary", "ex-boundary", "FAILED", "SELL", 100000000.0, 0.00009999, 0.0, 10, 10),
        )
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, side, qty, price, submission_reason_code, message, submit_evidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "coid-boundary",
                "submit_attempt_recorded",
                11,
                "FAILED",
                "SELL",
                0.00009999,
                100000000.0,
                DUST_RESIDUAL_UNSELLABLE,
                "category=unsafe_dust_mismatch_dust;detail=unsafe_dust_mismatch_dust;qty_below_min=1",
                json.dumps(
                    {
                        "sell_failure_category": "unsafe_dust_mismatch_dust",
                        "sell_failure_detail": "unsafe_dust_mismatch_dust",
                        "sell_qty_boundary_kind": "min_qty",
                        "qty_below_min": 1,
                        "normalized_below_min": 0,
                        "notional_below_min": 0,
                        "summary": "sell_qty_boundary_kind=min_qty qty_below_min=1 dust_qty_gap_small=1",
                    }
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=5)
    out = capsys.readouterr().out

    assert "sell_failure_category=boundary_below_min" in out
    assert "sell_failure_detail=unsafe_dust_mismatch_dust" in out
