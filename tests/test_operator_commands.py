from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import pytest

import bithumb_bot.app as app_module
from bithumb_bot import runtime_state
from bithumb_bot.app import main as app_main
from bithumb_bot.app import (
    _load_recovery_report,
    cmd_cash_drift_report,
    cmd_broker_diagnose,
    cmd_health,
    cmd_pause,
    cmd_panic_stop,
    cmd_flatten_position,
    cmd_cancel_open_orders,
    cmd_reconcile,
    cmd_repair_plan,
    cmd_recover_order,
    cmd_recovery_report,
    cmd_risk_report,
    cmd_restart_checklist,
    cmd_rebuild_position_authority,
    cmd_resume,
)
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.broker.balance_source import BalanceSnapshot
from bithumb_bot.broker import order_rules
from bithumb_bot.config import settings
from bithumb_bot.db_core import (
    ensure_db,
    get_fee_gap_accounting_repair_summary,
    get_fee_pending_accounting_repair_summary,
    get_portfolio_breakdown,
    init_portfolio,
    record_external_cash_adjustment,
    record_manual_flat_accounting_repair,
    set_portfolio_breakdown,
)
from bithumb_bot.engine import (
    ResumeBlocker,
    build_resume_guidance,
    evaluate_restart_readiness,
    evaluate_resume_eligibility,
    evaluate_startup_safety_gate,
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.oms import add_fill, set_exchange_order_id, set_status
from bithumb_bot.position_authority_repair import (
    apply_position_authority_rebuild,
    build_position_authority_rebuild_preview,
)
from bithumb_bot.public_api_orderbook import BestQuote
from bithumb_bot.reason_codes import DUST_RESIDUAL_UNSELLABLE
from bithumb_bot.recovery import reconcile_with_broker
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot
from bithumb_bot.risk import evaluate_daily_loss_state
from bithumb_bot.utils_time import kst_str


@pytest.fixture(autouse=True)
def _restore_settings_state(monkeypatch: pytest.MonkeyPatch):
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_start_cash = settings.START_CASH_KRW
    original_db_path = settings.DB_PATH
    original_live_fill_fee_alert_min_notional_krw = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW

    monkeypatch.setenv("MODE", "paper")
    object.__setattr__(settings, "MODE", "paper")

    try:
        yield
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(
            settings,
            "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW",
            original_live_fill_fee_alert_min_notional_krw,
        )
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)


def _set_tmp_db(tmp_path, monkeypatch: pytest.MonkeyPatch | None = None):
    db_path = str((tmp_path / "operator.sqlite").resolve())
    if monkeypatch is not None:
        monkeypatch.setenv("DB_PATH", db_path)
    else:
        os.environ["DB_PATH"] = db_path
    roots = {
        "ENV_ROOT": (tmp_path / "env").resolve(),
        "RUN_ROOT": (tmp_path / "run").resolve(),
        "DATA_ROOT": (tmp_path / "data").resolve(),
        "LOG_ROOT": (tmp_path / "logs").resolve(),
        "BACKUP_ROOT": (tmp_path / "backup").resolve(),
    }
    for key, value in roots.items():
        if monkeypatch is not None:
            monkeypatch.setenv(key, str(value))
        else:
            os.environ[key] = str(value)
    run_lock_path = str((roots["RUN_ROOT"] / "live" / "bithumb-bot.lock").resolve())
    if monkeypatch is not None:
        monkeypatch.setenv("RUN_LOCK_PATH", run_lock_path)
    else:
        os.environ["RUN_LOCK_PATH"] = run_lock_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(app_module.settings, "DB_PATH", db_path)


def _patch_cash_drift_broker(monkeypatch: pytest.MonkeyPatch, *, cash_available: float, cash_locked: float = 0.0):
    class _CashDriftBroker:
        def get_balance_snapshot(self):
            return BalanceSnapshot(
                source_id="accounts_v1_rest_snapshot",
                observed_ts_ms=1710000000000,
                asset_ts_ms=1710000000000,
                balance=BrokerBalance(
                    cash_available=cash_available,
                    cash_locked=cash_locked,
                    asset_available=0.0,
                    asset_locked=0.0,
                ),
            )

    monkeypatch.setattr("bithumb_bot.reporting.BithumbBroker", lambda: _CashDriftBroker())


def _seed_manual_flat_accounting_candidate(tmp_path, monkeypatch: pytest.MonkeyPatch, *, cleaned_lots: bool = True) -> None:
    _set_tmp_db(tmp_path, monkeypatch)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="manual_flat_buy",
            side="BUY",
            qty_req=0.01,
            price=100.0,
            ts_ms=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="manual_flat_buy",
            side="BUY",
            fill_id="manual_flat_fill_1",
            fill_ts=2,
            price=100.0,
            qty=0.01,
            fee=0.0,
        )
        set_status("manual_flat_buy", "FILLED", conn=conn)
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        if cleaned_lots:
            conn.execute("DELETE FROM open_position_lots")
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"balance_split_mismatch_count": 0},
    )


def _seed_fee_gap_accounting_candidate(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    manual_flat_repaired: bool = True,
) -> None:
    _set_tmp_db(tmp_path, monkeypatch)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 1_000.0)

    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="fee_gap_buy",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=1_700_000_000_000,
            status="NEW",
        )
        add_fill(
            conn=conn,
            client_order_id="fee_gap_buy",
            fill_id="fee_gap_buy_fill_1",
            fill_ts=1_700_000_000_100,
            price=100_000_000.0,
            qty=0.001,
            fee=0.0,
        )
        set_status("fee_gap_buy", "FILLED", conn=conn)
        record_external_cash_adjustment(
            conn,
            event_ts=1_700_000_000_200,
            currency="KRW",
            delta_amount=-50.0,
            source="accounts_v1_rest_snapshot",
            reason="reconcile_fee_gap_cash_drift",
            broker_snapshot_basis={
                "balance_source": "accounts_v1_rest_snapshot",
                "observed_ts_ms": 1_700_000_000_200,
                "broker_cash_total": 999_950.0,
                "local_cash_total": 1_000_000.0,
            },
            correlation_metadata={"fee_gap_recovery_required": 1},
            note="cash drift inferred from reconcile balance snapshot; material zero-fee fill history present",
            adjustment_key="fee-gap-adjustment-1",
        )
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW - 50.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        if manual_flat_repaired:
            record_manual_flat_accounting_repair(
                conn,
                event_ts=1_700_000_000_300,
                asset_qty_delta=-0.001,
                cash_delta=100_000.0,
                source="manual_flat_recovery",
                reason="manual_flat_accounting_repair",
                repair_basis={
                    "event_type": "manual_flat_accounting_repair",
                    "portfolio_cash_basis": settings.START_CASH_KRW - 50.0,
                    "portfolio_qty_basis": 0.0,
                },
                note="flat position confirmed before fee-gap repair",
            )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="FEE_GAP_RECOVERY_REQUIRED",
        metadata={
            "balance_split_mismatch_count": 0,
            "material_zero_fee_fill_count": 1,
            "material_zero_fee_fill_latest_ts": 1_700_000_000_100,
            "fee_gap_adjustment_count": 1,
            "fee_gap_adjustment_total_krw": -50.0,
            "fee_gap_adjustment_latest_event_ts": 1_700_000_000_200,
            "fee_gap_recovery_required": 1,
            "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
        },
        now_epoch_sec=0.0,
    )


def test_cash_drift_report_shows_explained_delta_from_external_adjustments(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    _patch_cash_drift_broker(monkeypatch, cash_available=settings.START_CASH_KRW + 120.0)

    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_external_cash_adjustment(
            conn,
            event_ts=1710000000000,
            currency="KRW",
            delta_amount=120.0,
            source="legacy_balance_api",
            reason="reconcile_cash_drift",
            broker_snapshot_basis={"cash_available": settings.START_CASH_KRW},
            note="cash drift audit",
            adjustment_key="cash-drift-explained-1",
        )
    finally:
        conn.close()

    cmd_cash_drift_report(recent_limit=5)
    out = capsys.readouterr().out

    assert "[CASH-DRIFT-REPORT]" in out
    assert f"broker_cash={settings.START_CASH_KRW + 120.0:,.3f}" in out
    assert f"local_cash={settings.START_CASH_KRW + 120.0:,.3f}" in out
    assert f"ledger_cash_without_adjustments={settings.START_CASH_KRW:,.3f}" in out
    assert "external_cash_adjustment_total=120.000" in out
    assert "explained_delta=120.000" in out
    assert "unexplained_residual_delta=0.000" in out
    assert "recent_adjustments:" in out
    assert "reason=reconcile_cash_drift" in out


def test_cash_drift_report_shows_unexplained_residual_delta(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    _patch_cash_drift_broker(monkeypatch, cash_available=settings.START_CASH_KRW + 75.0)

    conn = ensure_db()
    conn.close()

    app_main(["cash-drift-report", "--recent-limit", "5"])
    out = capsys.readouterr().out

    assert "[CASH-DRIFT-REPORT]" in out
    assert f"broker_cash={settings.START_CASH_KRW + 75.0:,.3f}" in out
    assert f"local_cash={settings.START_CASH_KRW:,.3f}" in out
    assert f"ledger_cash_without_adjustments={settings.START_CASH_KRW:,.3f}" in out
    assert "external_cash_adjustment_total=0.000" in out
    assert "explained_delta=0.000" in out
    assert "unexplained_residual_delta=75.000" in out


def test_rebuild_position_authority_preview_shows_full_projection_gate_details(monkeypatch, capsys):
    class _DummyConn:
        def close(self):
            return None

    monkeypatch.setattr(app_module, "ensure_db", lambda: _DummyConn())
    monkeypatch.setattr(
        app_module,
        "get_position_authority_repair_summary",
        lambda _conn: {"repair_count": 1},
    )
    monkeypatch.setattr(
        app_module,
        "build_position_authority_rebuild_preview",
        lambda _conn, full_projection_rebuild=False: {
            "needs_rebuild": True,
            "safe_to_apply": False,
            "recovery_stage": "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING",
            "repair_mode": "full_projection_rebuild",
            "eligibility_reason": "remote_open_orders=1",
            "portfolio_qty": 0.00099986,
            "accounted_buy_qty": 0.00059998,
            "accounted_buy_fill_count": 1,
            "sell_trade_count": 0,
            "open_lot_count": 0,
            "dust_tracking_lot_count": 14,
            "existing_lot_rows": 14,
            "position_authority_assessment": {"incident_class": "HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT"},
            "target_lot_provenance_kind": "portfolio_anchor_projection_lot",
            "target_lot_source_modes": ["full_projection_rebuild_portfolio_anchor"],
            "portfolio_anchor_missing_evidence": ["portfolio_anchor_projection_attestation_missing"],
            "manual_projection_missing_evidence": [],
            "manual_db_update_unsafe": True,
            "broker_qty_known": True,
            "broker_qty": 0.00099986,
            "remote_open_order_count": 1,
            "projection_converged": False,
            "projected_total_qty": 0.001788,
            "projected_qty_excess": 0.00078814,
            "lot_row_count": 14,
            "other_active_qty": 0.001788,
            "portfolio_projection_publication_present": False,
            "portfolio_projection_repair_event_status": "recorded_but_not_current_state_proof",
            "needs_full_projection_rebuild": True,
            "accounting_projection_ok": True,
            "broker_portfolio_converged": True,
            "unresolved_open_order_count": 0,
            "pending_submit_count": 0,
            "submit_unknown_count": 0,
            "unresolved_fee_pending": False,
            "next_required_action": "review_position_authority_evidence",
            "recommended_command": "uv run python bot.py rebuild-position-authority --full-projection-rebuild",
            "full_projection_rebuild_gate_report": {"reasons": ["remote_open_orders=1"]},
        },
    )

    cmd_rebuild_position_authority(full_projection_rebuild=True)
    out = capsys.readouterr().out

    assert "projection_converged=0" in out
    assert "projected_total_qty=0.001788000000" in out
    assert "projected_qty_excess=0.000788140000" in out
    assert "lot_row_count=14" in out
    assert "other_active_qty=0.001788000000" in out
    assert "target_lot_provenance_kind=portfolio_anchor_projection_lot" in out
    assert "provenance_missing_evidence=portfolio_anchor_projection_attestation_missing" in out
    assert "manual_db_update_unsafe=1" in out
    assert "portfolio_projection_publication_present=0" in out
    assert "portfolio_projection_repair_event_status=recorded_but_not_current_state_proof" in out
    assert "needs_full_projection_rebuild=1" in out
    assert "unresolved_fee_pending=0" in out
    assert "full_projection_rebuild_gate_reasons=remote_open_orders=1" in out


def test_cash_drift_report_handles_no_adjustment_edge_case(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    _patch_cash_drift_broker(monkeypatch, cash_available=settings.START_CASH_KRW)

    conn = ensure_db()
    conn.close()

    cmd_cash_drift_report(recent_limit=5)
    out = capsys.readouterr().out

    assert "[CASH-DRIFT-REPORT]" in out
    assert "recent_adjustments=none" in out
    assert "external_cash_adjustment_total=0.000" in out
    assert "unexplained_residual_delta=0.000" in out


def test_record_external_cash_adjustment_command_creates_event_without_trade(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    ensure_db().close()

    app_main(
        [
            "record-external-cash-adjustment",
            "--event-ts",
            "1710000000000",
            "--delta-amount",
            "77.5",
            "--source",
            "manual_deposit",
            "--reason",
            "operator_correction",
            "--broker-snapshot-basis",
            "{\"balance_source\":\"manual\",\"broker_cash_total\":1000077.5,\"local_cash_total\":1000000.0}",
            "--correlation-metadata",
            "{\"ticket\":\"ops-77\"}",
            "--note",
            "manual cash top-up",
            "--adjustment-key",
            "manual_deposit:ops-77",
            "--yes",
        ]
    )
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        adjustment = conn.execute(
            "SELECT adjustment_key, event_ts, currency, delta_amount, source, reason, note FROM external_cash_adjustments"
        ).fetchone()
        portfolio = conn.execute(
            "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
        ).fetchone()
        trade_count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    finally:
        conn.close()

    assert "[EXTERNAL-CASH-ADJUSTMENT]" in out
    assert "created=1" in out
    assert adjustment is not None
    assert adjustment["adjustment_key"] == "manual_deposit:ops-77"
    assert int(adjustment["event_ts"]) == 1710000000000
    assert adjustment["currency"] == "KRW"
    assert float(adjustment["delta_amount"]) == pytest.approx(77.5)
    assert adjustment["source"] == "manual_deposit"
    assert adjustment["reason"] == "operator_correction"
    assert adjustment["note"] == "manual cash top-up"
    assert float(portfolio["cash_krw"]) == pytest.approx(settings.START_CASH_KRW + 77.5)
    assert float(portfolio["cash_available"]) == pytest.approx(settings.START_CASH_KRW + 77.5)
    assert float(portfolio["cash_locked"]) == pytest.approx(0.0)
    assert trade_count == 0


def test_manual_flat_accounting_repair_preview_is_non_mutating(tmp_path, monkeypatch, capsys):
    _seed_manual_flat_accounting_candidate(tmp_path, monkeypatch)

    app_main(["manual-flat-accounting-repair"])
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        repair_count = conn.execute("SELECT COUNT(*) FROM manual_flat_accounting_repairs").fetchone()[0]
    finally:
        conn.close()

    assert "[MANUAL-FLAT-ACCOUNTING-REPAIR] preview" in out
    assert "needs_repair=1" in out
    assert "safe_to_apply=1" in out
    assert "[MANUAL-FLAT-ACCOUNTING-REPAIR] dry-run: no changes applied" in out
    assert repair_count == 0


def test_manual_flat_accounting_repair_requires_explicit_confirmation(tmp_path, monkeypatch, capsys):
    _seed_manual_flat_accounting_candidate(tmp_path, monkeypatch)

    with pytest.raises(SystemExit) as exc:
        app_main(["manual-flat-accounting-repair", "--apply"])
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        repair_count = conn.execute("SELECT COUNT(*) FROM manual_flat_accounting_repairs").fetchone()[0]
    finally:
        conn.close()

    assert exc.value.code == 1
    assert "confirmation required" in out
    assert repair_count == 0


def test_manual_flat_accounting_repair_refuses_when_lot_residue_remains(tmp_path, monkeypatch, capsys):
    _seed_manual_flat_accounting_candidate(tmp_path, monkeypatch, cleaned_lots=False)

    with pytest.raises(SystemExit) as exc:
        app_main(["manual-flat-accounting-repair", "--apply", "--yes"])
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        repair_count = conn.execute("SELECT COUNT(*) FROM manual_flat_accounting_repairs").fetchone()[0]
    finally:
        conn.close()

    assert exc.value.code == 1
    assert "unsafe repair request" in out
    assert "lot_residue_present=" in out
    assert repair_count == 0


def test_manual_flat_accounting_repair_converges_recovery_surfaces(tmp_path, monkeypatch, capsys):
    _seed_manual_flat_accounting_candidate(tmp_path, monkeypatch)
    monkeypatch.setattr("bithumb_bot.app._safe_recent_broker_orders_snapshot", lambda limit=100: ([], None))
    monkeypatch.setattr(
        "bithumb_bot.app.build_broker_with_auth_diagnostics",
        lambda **_kwargs: (SimpleNamespace(get_accounts_validation_diagnostics=lambda: {}), {}),
    )

    cmd_recovery_report()
    report_before = capsys.readouterr().out
    assert "blocker=MANUAL_FLAT_ACCOUNTING_REPAIR_REQUIRED" in report_before

    app_main(["manual-flat-accounting-repair", "--apply", "--yes", "--note", "broker-side manual flat"])
    apply_out = capsys.readouterr().out
    assert "[MANUAL-FLAT-ACCOUNTING-REPAIR] applied" in apply_out
    assert "remaining_needs_repair=0" in apply_out

    app_main(["audit-ledger"])
    audit_out = capsys.readouterr().out
    assert "manual_flat_accounting_repair_count=1" in audit_out
    assert "[AUDIT-LEDGER] OK" in audit_out

    cmd_health()
    health_out = capsys.readouterr().out
    assert "manual_flat_accounting_repair_needed=0" in health_out
    assert "manual_flat_accounting_repair_safe_to_apply=0" in health_out

    cmd_recovery_report()
    report_after = capsys.readouterr().out
    assert "[P3.0c] manual_flat_accounting_repair" in report_after
    assert "needed=0" in report_after
    assert "repair_count=1" in report_after
    assert "MANUAL_FLAT_ACCOUNTING_REPAIR_REQUIRED" not in report_after

    cmd_restart_checklist()
    checklist_out = capsys.readouterr().out
    assert "PASS    manual-flat accounting repair:" in checklist_out
    assert "safe_to_resume=1" in checklist_out


def test_fee_pending_accounting_repair_cli_applies_observed_fill(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 2_000_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
            VALUES ('fee_pending_cli','ex_fee_pending_cli','RECOVERY_REQUIRED','BUY',NULL,0.01,0,1000,1000,'fee pending')
            """
        )
        conn.execute(
            """
            INSERT INTO broker_fill_observations(
                event_ts, client_order_id, exchange_order_id, fill_id, fill_ts, side,
                price, qty, fee, fee_status, accounting_status, source, parse_warnings, raw_payload
            ) VALUES (1001, 'fee_pending_cli', 'ex_fee_pending_cli', 'fill_fee_pending_cli', 1001,
                'BUY', 100000000.0, 0.01, NULL, 'missing', 'fee_pending',
                'test_fixture', 'missing_fee_field', '{"uuid":"fill_fee_pending_cli"}')
            """
        )
        conn.commit()
    finally:
        conn.close()
    runtime_state.refresh_open_order_health(now_epoch_sec=2.0)

    app_main(
        [
            "fee-pending-accounting-repair",
            "--client-order-id",
            "fee_pending_cli",
            "--fill-id",
            "fill_fee_pending_cli",
            "--fee",
            "500",
            "--fee-provenance",
            "operator_checked_bithumb_trade_history",
            "--apply",
            "--yes",
        ]
    )
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        order = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='fee_pending_cli'"
        ).fetchone()
        repair_summary = get_fee_pending_accounting_repair_summary(conn)
        lot_count = conn.execute(
            "SELECT COALESCE(SUM(executable_lot_count), 0) FROM open_position_lots WHERE pair=?",
            (settings.PAIR,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert "[FEE-PENDING-ACCOUNTING-REPAIR] applied" in out
    assert order["status"] == "FILLED"
    assert float(order["qty_filled"]) == pytest.approx(0.01)
    assert order["last_error"] is None
    assert repair_summary["repair_count"] == 1
    assert lot_count > 0


def test_fee_gap_accounting_repair_preview_is_non_mutating(tmp_path, monkeypatch, capsys):
    _seed_fee_gap_accounting_candidate(tmp_path, monkeypatch)

    app_main(["fee-gap-accounting-repair"])
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        repair_count = conn.execute("SELECT COUNT(*) FROM fee_gap_accounting_repairs").fetchone()[0]
    finally:
        conn.close()

    assert "[FEE-GAP-ACCOUNTING-REPAIR] preview" in out
    assert "needs_repair=1" in out
    assert "safe_to_apply=1" in out
    assert "[FEE-GAP-ACCOUNTING-REPAIR] dry-run: no changes applied" in out
    assert repair_count == 0


def test_fee_gap_accounting_repair_requires_explicit_confirmation(tmp_path, monkeypatch, capsys):
    _seed_fee_gap_accounting_candidate(tmp_path, monkeypatch)

    with pytest.raises(SystemExit) as exc:
        app_main(["fee-gap-accounting-repair", "--apply"])
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        repair_count = conn.execute("SELECT COUNT(*) FROM fee_gap_accounting_repairs").fetchone()[0]
    finally:
        conn.close()

    assert exc.value.code == 1
    assert "confirmation required" in out
    assert repair_count == 0


def test_fee_gap_accounting_repair_refuses_when_manual_flat_is_still_pending(tmp_path, monkeypatch, capsys):
    _seed_fee_gap_accounting_candidate(tmp_path, monkeypatch, manual_flat_repaired=False)

    with pytest.raises(SystemExit) as exc:
        app_main(["fee-gap-accounting-repair", "--apply", "--yes"])
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        repair_count = conn.execute("SELECT COUNT(*) FROM fee_gap_accounting_repairs").fetchone()[0]
    finally:
        conn.close()

    assert exc.value.code == 1
    assert "unsafe repair request" in out
    assert "manual_flat_accounting_repair_pending=" in out
    assert repair_count == 0


def test_fee_gap_accounting_repair_converges_recovery_surfaces(tmp_path, monkeypatch, capsys):
    _seed_fee_gap_accounting_candidate(tmp_path, monkeypatch)
    monkeypatch.setattr("bithumb_bot.app._safe_recent_broker_orders_snapshot", lambda limit=100: ([], None))
    monkeypatch.setattr(
        "bithumb_bot.app.build_broker_with_auth_diagnostics",
        lambda **_kwargs: (SimpleNamespace(get_accounts_validation_diagnostics=lambda: {}), {}),
    )

    cmd_recovery_report()
    report_before = capsys.readouterr().out
    assert "blocker=FEE_GAP_RECOVERY_REQUIRED" in report_before
    assert "action=manual_fee_gap_recovery_required" in report_before

    app_main(["fee-gap-accounting-repair", "--apply", "--yes", "--note", "reviewed historical zero-fee fills"])
    apply_out = capsys.readouterr().out
    assert "[FEE-GAP-ACCOUNTING-REPAIR] applied" in apply_out
    assert "remaining_needs_repair=0" in apply_out

    conn = ensure_db()
    try:
        readiness_after_repair = compute_runtime_readiness_snapshot(conn)
        preview_after_repair = app_module.build_fee_gap_accounting_repair_preview(conn)
    finally:
        conn.close()
    assert readiness_after_repair.fee_gap_recovery_required is True
    assert readiness_after_repair.fee_gap_incident.incident_kind == "historical_fee_gap_repaired"
    assert readiness_after_repair.fee_gap_incident.incident_scope == "historical_context"
    assert readiness_after_repair.fee_gap_incident.resolution_state == "repaired"
    assert readiness_after_repair.fee_gap_incident.active_issue is False
    assert readiness_after_repair.fee_gap_incident.policy.resume_blocking is False
    assert preview_after_repair["fee_gap_recovery_required"] == 1
    assert preview_after_repair["incident_kind"] == "historical_fee_gap_repaired"
    assert preview_after_repair["incident_scope"] == "historical_context"
    assert preview_after_repair["resolution_state"] == "repaired"
    assert preview_after_repair["active_issue"] is False

    app_main(["audit-ledger"])
    audit_out = capsys.readouterr().out
    assert "fee_gap_accounting_repair_count=1" in audit_out
    assert "[AUDIT-LEDGER] OK" in audit_out

    cmd_health()
    health_out = capsys.readouterr().out
    assert "fee_gap_accounting_repair_needed=0" in health_out
    assert "fee_gap_accounting_repair_incident=kind=historical_fee_gap_repaired scope=historical_context resolution=repaired active_issue=0" in health_out
    assert "fee_gap_accounting_repair_safe_to_apply=0" in health_out
    assert "blocker_reason_codes=none" in health_out
    assert "blockers=none" in health_out

    report_json_after = _load_recovery_report()
    assert report_json_after["runtime_readiness"]["fee_gap_incident"]["incident_kind"] == "historical_fee_gap_repaired"
    assert report_json_after["runtime_readiness"]["fee_gap_incident"]["active_issue"] is False
    assert report_json_after["fee_gap_accounting_repair_preview"]["incident_scope"] == "historical_context"
    assert report_json_after["operator_next_action"] == "resume_now"

    cmd_recovery_report()
    report_after = capsys.readouterr().out
    assert "[P3.0d] fee_gap_accounting_repair" in report_after
    assert "incident_kind=historical_fee_gap_repaired" in report_after
    assert "incident_scope=historical_context" in report_after
    assert "active_issue=0" in report_after
    assert "needed=0" in report_after
    assert "repair_count=1" in report_after
    assert "blocker_reason_codes=none" in report_after
    assert "blockers=none" in report_after

    app_module.cmd_ops_report(limit=1)
    ops_out = capsys.readouterr().out
    assert "fee_gap_incident_kind=historical_fee_gap_repaired" in ops_out
    assert "fee_gap_incident_scope=historical_context" in ops_out
    assert "fee_gap_resolution_state=repaired" in ops_out
    assert "fee_gap_active_issue=0" in ops_out

    cmd_restart_checklist()
    checklist_out = capsys.readouterr().out
    assert "PASS    fee-gap accounting repair:" in checklist_out
    assert "incident_kind=historical_fee_gap_repaired" in checklist_out
    assert "active_issue=0" in checklist_out
    assert "safe_to_resume=1" in checklist_out

    conn = ensure_db()
    try:
        summary = get_fee_gap_accounting_repair_summary(conn)
    finally:
        conn.close()
    assert summary["repair_count"] == 1
    assert summary["last_reason"] == "fee_gap_accounting_repair"


def _insert_order(
    *,
    status: str,
    client_order_id: str,
    created_ts: int,
    side: str = "BUY",
    qty_req: float = 0.01,
    price: float | None = None,
) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            )
            VALUES (?, NULL, ?, ?, ?, ?, 0.0, ?, ?, NULL)
            """,
            (client_order_id, status, side, price, qty_req, created_ts, created_ts),
        )
        conn.commit()
    finally:
        conn.close()


def _set_last_error(*, client_order_id: str, last_error: str) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            "UPDATE orders SET last_error=? WHERE client_order_id=?",
            (last_error, client_order_id),
        )
        conn.commit()
    finally:
        conn.close()


class _ResumeFilledReplayBroker:
    def __init__(self, *, balance: BrokerBalance) -> None:
        self._balance = balance

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-resume-filled", "BUY", "FILLED", 100.0, 1.0, 1.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="ex-resume-filled:aggregate:201",
                fill_ts=201,
                price=100.0,
                qty=0.4,
                fee=0.0,
                exchange_order_id="ex-resume-filled",
            )
        ]

    def get_balance(self) -> BrokerBalance:
        return self._balance


def _insert_order_event(
    *,
    client_order_id: str,
    event_type: str,
    event_ts: int,
    submit_attempt_id: str | None = None,
    intent_ts: int | None = None,
    submit_ts: int | None = None,
    timeout_flag: int | None = None,
    exchange_order_id_obtained: int | None = None,
    order_status: str | None = None,
) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, submit_attempt_id,
                intent_ts, submit_ts, timeout_flag, exchange_order_id_obtained, side, qty
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'BUY', 0.01)
            """,
            (
                client_order_id,
                event_type,
                event_ts,
                order_status,
                submit_attempt_id,
                intent_ts,
                submit_ts,
                timeout_flag,
                exchange_order_id_obtained,
            ),
        )
        conn.commit()
    finally:
        conn.close()


class _RecoverSuccessBroker:
    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id,
            "BUY",
            "FILLED",
            None,
            0.01,
            0.01,
            1,
            1,
        )

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id=str(client_order_id or ""),
                fill_id="recover_fill_1",
                fill_ts=1000,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id=exchange_order_id,
            )
        ]

    def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=0.0,
            cash_locked=0.0,
            asset_available=0.01,
            asset_locked=0.0,
        )


class _RecoverAmbiguousBroker(_RecoverSuccessBroker):
    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id,
            "BUY",
            "NEW",
            None,
            0.01,
            0.0,
            1,
            1,
        )


class _RecoverUnresolvedHighConfidenceTerminalBroker(_RecoverSuccessBroker):
    def __init__(self, *, recent_orders: list[BrokerOrder] | None = None, remote_status: str = "FILLED") -> None:
        self._recent_orders = list(recent_orders or [])
        self._remote_status = remote_status

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return list(self._recent_orders)

    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex-unresolved-1",
            "BUY",
            self._remote_status,
            100.0,
            0.01,
            0.01 if self._remote_status == "FILLED" else 0.0,
            1,
            1,
        )


class _RecoveryReportMissingPriceBroker(_RecoverSuccessBroker):
    def __init__(self) -> None:
        self.recent_orders = [
            BrokerOrder(
                "live_1775658600000_sell_ae61703f",
                "C0101000002903202695",
                "SELL",
                "FILLED",
                None,
                0.0001,
                0.0001,
                1775658600000,
                1775658605000,
            )
        ]

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return list(self.recent_orders)[:limit]

    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "C0101000002903202695",
            "SELL",
            "FILLED",
            105950000.0,
            0.0001,
            0.0001,
            1775658600000,
            1775658605000,
        )

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id=str(client_order_id or "live_1775658600000_sell_ae61703f"),
                fill_id="C0101000002903202695:trade:1",
                fill_ts=1775658605000,
                price=105950000.0,
                qty=0.0001,
                fee=4.23,
                exchange_order_id=str(exchange_order_id or "C0101000002903202695"),
            )
        ]

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=1_010_590.77,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )

class _SubmitUnknownRecoveredByRecentFillBroker:
    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex-submit-unknown-1",
            "BUY",
            "FILLED",
            100.0,
            0.01,
            0.01,
            1,
            1,
        )

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list[BrokerFill]:
        return []

    def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return [
            BrokerFill(
                client_order_id="ambiguous_resume_case",
                fill_id="ambiguous_submit_fill_1",
                fill_ts=1000,
                price=100.0,
                qty=0.01,
                fee=0.0,
                exchange_order_id="ex-submit-unknown-1",
            )
        ]

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=0.0,
            cash_locked=0.0,
            asset_available=0.01,
            asset_locked=0.0,
        )


class _RecoveryReportCandidateBroker:
    def __init__(self, recent_orders: list[BrokerOrder]):
        self._recent_orders = recent_orders

    def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return self._recent_orders[:limit]

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise NotImplementedError

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None):
        return []

    def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(0.0, 0.0, 0.0, 0.0)


class _CancelOpenOrdersSafetyBroker:
    def __init__(self, open_orders: list[BrokerOrder]):
        self._open_orders = list(open_orders)
        self.cancel_calls: list[dict[str, str | None]] = []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return list(self._open_orders)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None):
        self.cancel_calls.append(
            {"client_order_id": client_order_id, "exchange_order_id": exchange_order_id}
        )

        class _CancelResult:
            status = "CANCELED"

        return _CancelResult()

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None):
        class _Order:
            status = "CANCELED"

        return _Order()

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return list(self._open_orders)[:limit]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list[BrokerFill]:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(0.0, 0.0, 0.0, 0.0)


def test_pause_disables_trading_via_persistent_runtime_state(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    cmd_pause()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason == "manual operator pause"
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "MANUAL_PAUSE"
    assert state.halt_state_unresolved is False


def test_manual_pause_then_resume_success_path(tmp_path):
    _set_tmp_db(tmp_path)

    cmd_pause()
    cmd_resume(force=False)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None


def test_pause_resume_state_transition_contract(tmp_path, capsys):
    _set_tmp_db(tmp_path)

    cmd_pause()
    pause_out = capsys.readouterr().out

    cmd_resume(force=False)
    resume_out = capsys.readouterr().out

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None
    assert "[PAUSE] precondition=" in pause_out
    assert "postcondition=trading_enabled=0" in pause_out
    assert "[RESUME] precondition=" in resume_out
    assert "postcondition=trading_enabled=1" in resume_out


def test_pause_resume_state_transition_reports_resume_gate_summary(tmp_path, capsys):
    _set_tmp_db(tmp_path)

    cmd_pause()
    pause_out = capsys.readouterr().out

    cmd_resume(force=False)
    resume_out = capsys.readouterr().out

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None
    assert "resume_allowed=1" in pause_out
    assert "resume_blockers=none" in pause_out
    assert "resume_blocker_reason_codes=none" in pause_out
    assert "force_override=0" in resume_out
    assert "resume_gate_blocked=0" in resume_out
    assert "resume_blockers=none" in resume_out


def test_resume_refuses_when_blockers_remain_after_pause(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="resume_blocked_after_pause",
        created_ts=now_ms,
    )

    cmd_pause()
    capsys.readouterr()

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    state = runtime_state.snapshot()

    assert exc.value.code == 1
    assert "[RESUME] refused:" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "recovery_required_orders=1" in out
    assert state.trading_enabled is False
    assert state.resume_gate_blocked is True


def test_resume_live_recent_fill_replay_does_not_fail_with_filled_to_partial(tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    conn = ensure_db()
    try:
        record_order_if_missing(
            conn,
            client_order_id="resume_filled_replay",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PENDING_SUBMIT",
        )
        set_exchange_order_id("resume_filled_replay", "ex-resume-filled", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="resume_filled_replay",
            side="BUY",
            fill_id="resume-fill-existing",
            fill_ts=120,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("resume_filled_replay", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="resume_flatten",
            side="SELL",
            qty_req=0.4,
            price=110.0,
            ts_ms=130,
            status="PENDING_SUBMIT",
        )
        set_exchange_order_id("resume_flatten", "ex-resume-flat", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="resume_flatten",
            side="SELL",
            fill_id="resume-flat-fill",
            fill_ts=140,
            price=110.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("resume_flatten", "FILLED", conn=conn)

        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
        conn.commit()
    finally:
        conn.close()

    broker = _ResumeFilledReplayBroker(
        balance=BrokerBalance(
            cash_available=cash_available,
            cash_locked=cash_locked,
            asset_available=asset_available,
            asset_locked=asset_locked,
        )
    )

    try:
        cmd_pause()
        cmd_resume(
            force=False,
            broker_factory=lambda: broker,
            reconcile_fn=reconcile_with_broker,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.last_reconcile_status in {"ok", "success"}

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='resume_filled_replay'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(0.4)


def test_resume_live_accepts_injected_reconcile_dependencies(tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    broker = object()
    calls: list[object] = []

    def _broker_factory():
        calls.append("factory")
        return broker

    def _reconcile(candidate):
        calls.append(("reconcile", candidate))

    try:
        cmd_pause()
        cmd_resume(
            force=False,
            broker_factory=_broker_factory,
            reconcile_fn=_reconcile,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert calls == ["factory", ("reconcile", broker)]
    state = runtime_state.snapshot()
    assert state.trading_enabled is True


def test_resume_refuses_when_unresolved_state_exists_without_force(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "STARTUP_SAFETY_GATE_BLOCKED" in state.resume_gate_reason


def test_resume_refused_when_ambiguous_submit_only_weakly_matches_recent_fill(
    monkeypatch, tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="SUBMIT_UNKNOWN",
        client_order_id="ambiguous_resume_case",
        created_ts=now_ms,
    )

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit):
        cmd_resume(force=False)

    state_blocked = runtime_state.snapshot()
    assert state_blocked.resume_gate_blocked is True
    assert "STARTUP_SAFETY_GATE_BLOCKED" in str(state_blocked.resume_gate_reason)
    assert "submit_unknown_orders=1" in str(state_blocked.resume_gate_reason)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _SubmitUnknownRecoveredByRecentFillBroker(),
    )
    try:
        cmd_reconcile()

        with pytest.raises(SystemExit) as exc:
            cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert exc.value.code == 1
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "recovery_required_orders=1" in out
    assert "LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS" in out

    state_after = runtime_state.snapshot()
    assert state_after.trading_enabled is False
    assert state_after.resume_gate_blocked is True
    assert state_after.resume_gate_reason is not None
    assert "STARTUP_SAFETY_GATE_BLOCKED" in str(state_after.resume_gate_reason)
    assert "recovery_required_orders=1" in str(state_after.resume_gate_reason)
    assert "LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS" in str(
        state_after.resume_gate_reason
    )

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT status, last_error
            FROM orders
            WHERE client_order_id='ambiguous_resume_case'
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "manual recovery required" in str(row["last_error"])


def test_resume_runs_preflight_reconcile_and_refuses_when_recovery_required(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    calls = {"n": 0}

    def _reconcile(_broker):
        calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", _reconcile)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1
    assert calls["n"] == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is False
    assert state.startup_gate_reason is not None
    assert "recovery_required_orders=1" in str(state.startup_gate_reason)


def test_extension_invariant_unresolved_or_recovery_required_state_never_auto_resumes(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="still_open",
        created_ts=now_ms,
    )
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="still_needs_recovery",
        created_ts=now_ms,
    )

    runtime_state.enable_trading()
    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is False
    assert {blocker.code for blocker in blockers} >= {"STARTUP_SAFETY_GATE_BLOCKED"}
    state = runtime_state.snapshot()
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "recovery_required_orders=1" in str(state.resume_gate_reason)


def test_resume_refuses_when_reconcile_has_balance_split_mismatch(
    monkeypatch, tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 2,
            "balance_split_mismatch_summary": (
                "cash_available(local=1000000,broker=900000,delta=-100000) "
                "asset_available(local=0.500000000000,broker=0.450000000000,delta=-0.050000000000)"
            ),
            "external_cash_adjustment_count": 1,
        },
    )

    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", lambda _broker: None)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())

    try:
        with pytest.raises(SystemExit) as exc:
            cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "code=BALANCE_SPLIT_MISMATCH" in out
    assert "balance split mismatch detected after reconcile" in out
    assert exc.value.code == 1


def test_recovery_report_classifies_general_balance_split_mismatch_blocker(tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 2,
            "balance_split_mismatch_summary": (
                "cash_available(local=1000000,broker=900000,delta=-100000) "
                "asset_available(local=0.500000000000,broker=0.450000000000,delta=-0.050000000000)"
            ),
            "external_cash_adjustment_count": 1,
            "external_cash_adjustment_delta_krw": -100000.0,
            "external_cash_adjustment_total_krw": -100000.0,
        },
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert report["can_resume"] is False
    assert report["resume_blockers"] == ["BALANCE_SPLIT_MISMATCH"]
    assert report["resume_blocker_reason_codes"] == ["PORTFOLIO_BROKER_CASH_MISMATCH"]
    assert report["blocker_summary_view"][0]["reason_code"] == "PORTFOLIO_BROKER_CASH_MISMATCH"
    assert report["blocker_summary_view"][0]["summary"] == "portfolio cash split does not match broker snapshot"


def test_recovery_report_classifies_external_cash_adjustment_missing_blocker(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.app._safe_recent_broker_orders_snapshot",
        lambda *, limit=100: ([], "stubbed broker snapshot"),
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 1,
            "balance_split_mismatch_summary": (
                "cash_available(local=1000000,broker=999950,delta=-50)"
            ),
            "external_cash_adjustment_count": 0,
            "external_cash_adjustment_delta_krw": 0.0,
            "external_cash_adjustment_total_krw": 0.0,
        },
    )

    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert report["can_resume"] is False
    assert report["resume_blockers"] == ["EXTERNAL_CASH_ADJUSTMENT_REQUIRED"]
    assert report["resume_blocker_reason_codes"] == ["BROKER_CASH_DELTA_UNEXPLAINED"]
    assert report["blocker_summary_view"][0]["reason_code"] == "BROKER_CASH_DELTA_UNEXPLAINED"
    assert report["blocker_summary_view"][0]["summary"] == "cash mismatch requires external cash adjustment evidence"
    assert report["blocker_summary_view"][0]["delta_krw"] == pytest.approx(-50.0)
    assert report["blocker_summary_view"][0]["recent_external_cash_adjustment_present"] is False
    assert "delta_krw=-50.000" in report["blocker_summary_view"][0]["evidence"]
    assert "recent_external_cash_adjustment_present=0" in report["blocker_summary_view"][0]["evidence"]


def test_recovery_report_classifies_trade_fill_unresolved_blocker(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="trade_fill_unresolved", created_ts=now_ms - 5_000)

    report = _load_recovery_report()

    assert report["can_resume"] is False
    assert report["resume_blockers"][0] == "STARTUP_SAFETY_GATE_BLOCKED"
    assert report["resume_blocker_reason_codes"][0] == "TRADE_FILL_UNRESOLVED"
    assert report["blocker_summary_view"][0]["reason_code"] == "TRADE_FILL_UNRESOLVED"
    assert report["blocker_summary_view"][0]["summary"] == "trade/fill state remains unresolved"




def test_resume_refuses_when_kill_switch_halt_has_open_position(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.5, 900000.0, 100000.0, 0.5, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON; emergency cancellation attempted; risk_open_exposure_remains(open_orders=0,position=1)",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=HALT_RISK_OPEN_POSITION" in out
    assert "open exposure" in out
    assert "position_present=1" in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "HALT_RISK_OPEN_POSITION" in state.resume_gate_reason


def test_resume_blocks_risk_halt_when_only_matched_dust_policy_review_remains(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON; flatten submitted; dust residual only",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_effective_flat": 1,
            "submit_unknown_count": 1,
            "dust_policy_reason": "matched_harmless_dust_operator_review_required",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 classification=harmless_dust harmless_dust=1 broker_local_match=1 min_qty=0.00010000 submit_unknown_count=1 allow_resume=0 effective_flat=1 policy_reason=matched_harmless_dust_operator_review_required",
        },
    )
    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", lambda _broker: None)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    state = runtime_state.snapshot()
    assert exc.value.code == 1
    assert "code=HARMLESS_DUST_POLICY_REVIEW_REQUIRED" in out
    assert state.trading_enabled is False
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "HARMLESS_DUST_POLICY_REVIEW_REQUIRED" in state.resume_gate_reason


def test_resume_refuses_when_dust_residual_policy_requires_review(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON; flatten submitted but attribution inconsistent",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00020000 delta=-0.00010371 min_qty=0.00010000 min_notional_krw=5000.0",
        },
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=HALT_RISK_OPEN_POSITION" in out
    assert "dust_policy=dangerous_dust_operator_review_required" in out
    assert exc.value.code == 1




def test_resume_allows_risk_halt_when_exposure_is_flat(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=False,
    )

    cmd_resume(force=False)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None


def test_resume_live_clears_post_trade_reconcile_halt_after_flatten(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path, monkeypatch)
    old_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.0, 1000000.0, 0.0, 0.0, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="post trade reconcile failed (RuntimeError): duplicate fill replay",
        reason_code="POST_TRADE_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    class _SafeFlatBroker:
        def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
            return BrokerOrder(client_order_id, exchange_order_id or "ex-safe-flat", "BUY", "FILLED", 100.0, 1.0, 1.0, 1, 1)

        def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
            return []

        def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
            return []

        def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
            return []

        def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
            return []

        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=1000000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _SafeFlatBroker())

    try:
        cmd_resume(force=False)

        state = runtime_state.snapshot()
        assert state.trading_enabled is True
        assert state.halt_new_orders_blocked is False
        assert state.halt_state_unresolved is False
        assert state.halt_reason_code is None
    finally:
        object.__setattr__(settings, "MODE", old_mode)


def test_resume_non_risk_halt_with_open_exposure_message_is_unchanged(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.25, 900000.0, 100000.0, 0.25, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        halt_new_orders_blocked=True,
        unresolved=False,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=HALT_RISK_OPEN_POSITION" in out
    assert "risk halt resume rejected until exposure is flattened/resolved first" not in out
    assert exc.value.code == 1


def _set_stale_initial_reconcile_halt_with_clean_reconcile() -> None:
    runtime_state.disable_trading_until(
        float("inf"),
        reason=(
            "initial reconcile failed (BrokerRejectError): "
            "bithumb private /info/orders rejected with http status=400"
        ),
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 0,
            "balance_split_mismatch_summary": "none",
            "remote_open_order_found": 0,
        },
    )
    runtime_state.refresh_open_order_health()


def test_resume_refuses_when_halt_state_unresolved_even_without_open_orders(
    tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): boom",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=HALT_STATE_UNRESOLVED" in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_state_unresolved is True


def test_resume_refuses_when_last_reconcile_failed(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    runtime_state.record_reconcile_result(
        success=False,
        error="boom",
        reason_code="PERIODIC_RECONCILE_FAILED",
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=LAST_RECONCILE_FAILED" in out
    assert "PERIODIC_RECONCILE_FAILED" in out
    assert exc.value.code == 1

    state = runtime_state.snapshot()
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "LAST_RECONCILE_FAILED" in state.resume_gate_reason


def test_resume_force_refuses_when_last_reconcile_failed(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    runtime_state.record_reconcile_result(
        success=False,
        error="boom",
        reason_code="PERIODIC_RECONCILE_FAILED",
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=LAST_RECONCILE_FAILED" in out
    assert "overridable=0" in out
    assert "reason_code=PERIODIC_RECONCILE_FAILED" in out
    assert exc.value.code == 1


def test_resume_force_refuses_when_startup_safety_gate_blocked(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "overridable=0" in out
    assert "recovery_required_orders=1" in out
    assert exc.value.code == 1


def test_resume_force_rejects_startup_blocker_before_clearing_manual_pause(
    tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="still_open_on_startup",
        created_ts=now_ms,
    )
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "unresolved_open_orders=1" in out
    assert "manual operator pause" not in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False


def test_resume_force_refuses_when_halt_state_unresolved(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): boom",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=HALT_STATE_UNRESOLVED" in out
    assert "overridable=0" in out
    assert exc.value.code == 1


def test_resume_force_rejects_initial_reconcile_failure_with_operator_readable_reason(
    tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): broker timeout",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=HALT_STATE_UNRESOLVED" in out
    assert "INITIAL_RECONCILE_FAILED" in out
    assert "broker timeout" in out
    assert exc.value.code == 1


def test_resume_auto_clears_stale_initial_reconcile_halt_after_clean_reconcile(tmp_path):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is True
    assert blockers == []
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False
    assert state.halt_reason_code is None
    assert state.last_disable_reason is None


def test_resume_force_enables_for_safe_manual_pause(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        unresolved=False,
    )
    cmd_resume(force=True)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.retry_at_epoch_sec is None
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None
    assert state.halt_state_unresolved is False


def test_cancel_open_orders_persists_runtime_state(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(app_module.settings, "LIVE_DRY_RUN", False)

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())
    monkeypatch.setattr(
        "bithumb_bot.app.cancel_open_orders_with_broker",
        lambda _broker: {
            "remote_open_count": 2,
            "cancel_accepted_count": 1,
            "canceled_count": 1,
            "cancel_confirm_pending_count": 0,
            "matched_local_count": 1,
            "stray_canceled_count": 0,
            "failed_count": 1,
            "stray_messages": [],
            "error_messages": ["cancel failed: order_2"],
        },
    )

    try:
        from bithumb_bot.app import cmd_cancel_open_orders

        cmd_cancel_open_orders()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "LIVE_DRY_RUN", original_live_dry_run)

    out = capsys.readouterr().out
    state = runtime_state.snapshot()
    assert state.last_cancel_open_orders_trigger == "operator-command"
    assert state.last_cancel_open_orders_status == "partial"
    assert state.last_cancel_open_orders_epoch_sec is not None
    assert state.last_cancel_open_orders_summary is not None
    assert '"failed_count": 1' in state.last_cancel_open_orders_summary
    assert "[CANCEL-OPEN-ORDERS] precondition=" in out
    assert "[CANCEL-OPEN-ORDERS] warning=" in out
    assert "postcondition=status=partial" in out


def test_cancel_open_orders_refuses_in_live_dry_run(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(app_module.settings, "LIVE_DRY_RUN", True)

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)
    broker_created = {"called": False}

    def _broker_factory():
        broker_created["called"] = True
        raise AssertionError("broker should not be created in LIVE_DRY_RUN mode")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _broker_factory)

    try:
        with pytest.raises(SystemExit) as exc:
            app_module.cmd_cancel_open_orders()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "LIVE_DRY_RUN", original_live_dry_run)

    out = capsys.readouterr().out
    assert exc.value.code == 1
    assert "LIVE_DRY_RUN=true would only simulate cancel" in out
    assert broker_created["called"] is False


def test_cancel_open_orders_skips_stray_remote_orders_and_reports_resume_gate(
    monkeypatch, tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(app_module.settings, "LIVE_DRY_RUN", False)

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="local_cancel_1", created_ts=now_ms - 10_000)
    set_exchange_order_id("local_cancel_1", "ex-local-cancel-1")

    matched_order = BrokerOrder(
        client_order_id="local_cancel_1",
        exchange_order_id="ex-local-cancel-1",
        side="BUY",
        status="NEW",
        price=100.0,
        qty_req=0.01,
        qty_filled=0.0,
        created_ts=now_ms - 9_000,
        updated_ts=now_ms - 9_000,
    )
    stray_order = BrokerOrder(
        client_order_id="stray_remote_1",
        exchange_order_id="ex-stray-cancel-1",
        side="BUY",
        status="NEW",
        price=100.0,
        qty_req=0.02,
        qty_filled=0.0,
        created_ts=now_ms - 8_000,
        updated_ts=now_ms - 8_000,
    )
    broker = _CancelOpenOrdersSafetyBroker([matched_order, stray_order])
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)

    try:
        cmd_cancel_open_orders()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "LIVE_DRY_RUN", original_live_dry_run)

    out = capsys.readouterr().out
    state = runtime_state.snapshot()

    assert len(broker.cancel_calls) == 1
    assert broker.cancel_calls[0]["exchange_order_id"] == "ex-local-cancel-1"
    assert "cancel skipped for remote open order without local unresolved mapping" in out
    assert "[CANCEL-OPEN-ORDERS] warning=" in out
    assert "resume_gate_blocked=" in out
    assert state.last_cancel_open_orders_trigger == "operator-command"
    assert state.last_cancel_open_orders_status == "ok"
    assert state.last_cancel_open_orders_summary is not None
    assert '"canceled_count": 1' in state.last_cancel_open_orders_summary


class _PanicStopBroker:
    def __init__(self, *, open_orders: list[BrokerOrder], balance: BrokerBalance) -> None:
        self._open_orders = list(open_orders)
        self._balance = balance
        self.cancel_calls: list[dict[str, str | None]] = []
        self.place_order_calls: list[dict[str, str | float | None]] = []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return list(self._open_orders)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None):
        self.cancel_calls.append(
            {"client_order_id": client_order_id, "exchange_order_id": exchange_order_id}
        )

        class _CancelResult:
            def __init__(self, *, exchange_order_id: str | None) -> None:
                self.status = "CANCELED"
                self.exchange_order_id = exchange_order_id

        return _CancelResult(exchange_order_id=exchange_order_id)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None):
        class _Order:
            def __init__(self, *, exchange_order_id: str | None) -> None:
                self.status = "CANCELED"
                self.exchange_order_id = exchange_order_id

        return _Order(exchange_order_id=exchange_order_id)

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return list(self._open_orders)[:limit]

    def get_balance(self) -> BrokerBalance:
        return self._balance

    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract=None,
    ):
        self.place_order_calls.append(
            {
                "client_order_id": client_order_id,
                "side": side,
                "qty": qty,
                "price": price,
            }
        )

        class _Order:
            exchange_order_id = "ex-panic-stop"
            status = "NEW"

        return _Order()


def test_panic_stop_blocks_new_orders_and_cancels_open_orders_without_flatten(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="panic_open_1", created_ts=now_ms - 5_000, side="BUY", price=100.0)
    set_exchange_order_id("panic_open_1", "ex-panic-1")
    runtime_state.record_flatten_position_result(status="no_position", summary={"status": "no_position"})

    broker = _PanicStopBroker(
        open_orders=[
            BrokerOrder(
                client_order_id="panic_open_1",
                exchange_order_id="ex-panic-1",
                side="BUY",
                status="NEW",
                price=100.0,
                qty_req=0.01,
                qty_filled=0.0,
                created_ts=now_ms - 5_000,
                updated_ts=now_ms - 5_000,
            )
        ],
        balance=BrokerBalance(cash_available=100_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)

    try:
        cmd_panic_stop(flatten=False)
        from bithumb_bot.broker.live import live_execute_signal

        result = live_execute_signal(broker, "BUY", now_ms, 100.0)
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)

    assert result is None
    assert len(broker.cancel_calls) == 1
    assert len(broker.place_order_calls) == 0
    out = capsys.readouterr().out
    assert "flatten_requested=0" in out
    assert "resume_allowed=1" in out
    assert "resume_precondition=clear" in out
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_policy_auto_liquidate_positions is False
    assert state.last_cancel_open_orders_status == "ok"
    assert state.last_flatten_position_status == "skipped"
    assert state.last_flatten_position_summary is not None
    assert '"status": "skipped"' in state.last_flatten_position_summary
    assert "flatten_status=skipped" in str(state.last_disable_reason)


def test_panic_stop_with_flatten_attempts_sell_after_cancelling_open_orders(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_step = settings.LIVE_ORDER_QTY_STEP
    original_max_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.000001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 6)
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)
    monkeypatch.setattr(
        "bithumb_bot.flatten.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="panic_flatten_1", created_ts=now_ms - 5_000, side="BUY", price=100.0)
    set_exchange_order_id("panic_flatten_1", "ex-panic-flat-1")
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.05, 1000000.0, 0.0, 0.05, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "panic_flatten_lot", now_ms - 10_000, 100_000_000.0, 0.05, 1, 0, "lot-native", "open_exposure"),
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_flatten_position_result(status="no_position", summary={"status": "no_position"})

    broker = _PanicStopBroker(
        open_orders=[
            BrokerOrder(
                client_order_id="panic_flatten_1",
                exchange_order_id="ex-panic-flat-1",
                side="BUY",
                status="NEW",
                price=100.0,
                qty_req=0.01,
                qty_filled=0.0,
                created_ts=now_ms - 5_000,
                updated_ts=now_ms - 5_000,
            )
        ],
        balance=BrokerBalance(cash_available=100_000.0, cash_locked=0.0, asset_available=1.0, asset_locked=0.0),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)

    try:
        cmd_panic_stop(flatten=True)
        monkeypatch.setattr("bithumb_bot.app._run_live_reconcile", lambda **_kwargs: None)
        with pytest.raises(SystemExit) as exc:
            cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_step)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_max_decimals)

    assert exc.value.code == 1
    assert len(broker.cancel_calls) == 1
    assert len(broker.place_order_calls) == 1
    assert broker.place_order_calls[0]["side"] == "SELL"
    conn = ensure_db()
    try:
        flatten_row = conn.execute(
            """
            SELECT client_order_id, exchange_order_id, status, side, qty_req, strategy_name, local_intent_state
            FROM orders
            WHERE client_order_id LIKE 'flatten_%'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        assert flatten_row is not None
        assert flatten_row["exchange_order_id"] == "ex-panic-stop"
        assert flatten_row["status"] == "NEW"
        assert flatten_row["side"] == "SELL"
        assert flatten_row["strategy_name"] == "operator_flatten"
        assert flatten_row["local_intent_state"] == "NEW"
        event_types = {
            str(row["event_type"])
            for row in conn.execute(
                """
                SELECT event_type
                FROM order_events
                WHERE client_order_id=?
                """,
                (flatten_row["client_order_id"],),
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"intent_created", "submit_started", "submit_attempt_preflight", "submit_attempt_acknowledged"} <= event_types
    out = capsys.readouterr().out
    assert "flatten_requested=1" in out
    assert "resume_allowed=0" in out
    assert "code=HALT_RISK_OPEN_POSITION" in out
    assert "resume_precondition=blocked" in out
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_policy_auto_liquidate_positions is True
    assert state.last_flatten_position_status == "submitted"
    assert state.last_cancel_open_orders_status == "ok"
    assert "flatten_status=submitted" in str(state.last_disable_reason)
    assert state.halt_state_unresolved is True


def test_panic_stop_cli_dispatches_to_command(monkeypatch):
    called = {"flatten": None}

    monkeypatch.setattr("bithumb_bot.app.validate_mode_or_raise", lambda _mode: None)
    monkeypatch.setattr(
        "bithumb_bot.app.cmd_panic_stop",
        lambda *, flatten=False: called.__setitem__("flatten", flatten),
    )

    rc = app_module.main(["panic-stop", "--flatten"])

    assert rc == 0
    assert called["flatten"] is True


def test_broker_diagnose_success_output(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('diag_live_1','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_max_market_slippage_bps = settings.MAX_MARKET_SLIPPAGE_BPS
    original_live_price_protection_max_slippage_bps = settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1200000.0, 10000.0, 0.12, 0.01)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return [
                BrokerOrder("a", "ex1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1),
                BrokerOrder("b", "ex2", "SELL", "PARTIAL", 110.0, 0.1, 0.05, 1, 1),
            ]

        def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return [
                BrokerOrder("", "ex3", "BUY", "FILLED", 120.0, 0.2, 0.2, 1, 2),
                BrokerOrder("", "ex4", "SELL", "CANCELED", 121.0, 0.2, 0.0, 1, 2),
            ][:limit]

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "order_types": ("limit",),
                        "bid_types": ("limit", "price"),
                        "ask_types": ("limit", "market"),
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "bid_price_unit": 0.0,
                        "ask_price_unit": 0.0,
                    },
                )(),
                "source": {
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            },
        )(),
    )

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", original_max_market_slippage_bps)
        object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", original_live_price_protection_max_slippage_bps)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)

    out = capsys.readouterr().out
    assert "[BROKER-READINESS]" in out
    assert "overall=PASS" in out
    assert "[PASS] config/env loaded" in out
    assert "[PASS] broker authentication" in out
    assert "[PASS] balance query" in out
    assert "[PASS] live execution mode: MODE=live LIVE_DRY_RUN=True armed=False" in out
    assert "[PASS] order submit routing: price=None => /v2/orders market/price order, price set => /v2/orders limit order" in out
    assert "[PASS] order lookup path: get_order reads /v1/order directly; open/recent snapshots use /v1/orders" in out
    assert "[PASS] open order query: known_unresolved_count=2" in out
    assert "[PASS] symbol/order rule query" in out
    assert "[PASS] BUY price=None chance resolution:" in out
    assert "raw_bid_types=['limit', 'price']" in out
    assert "raw_order_types=['limit']" in out
    assert "raw_buy_supported_types=['limit', 'price']" in out
    assert "support_source=bid_types" in out
    assert "resolved_contract=validation_order_type=price exchange_order_type=price submit_field=price" in out
    assert "contract_id=" in out
    assert "resolved_order_type=price submit_field=price allowed=True decision_outcome=pass decision_basis=raw alias_used=False alias_policy=market_to_price_alias_disabled block_reason=-" in out
    assert "overall=WARN" not in out
    assert "[PASS] accounts snapshot(/v1/accounts) validation diagnostic: reason=ok" in out
    assert "execution_mode=- quote_currency=- base_currency=-" in out
    assert "base_currency_missing_policy=- preflight_outcome=-" in out
    assert "bid_min_total_krw=0.0 (source=chance_doc)" in out
    assert "ask_price_unit=0.0 (source=chance_doc)" in out
    assert "min_qty=0.0001 (source=local_fallback)" in out
    assert "[PASS] DB writable" in out


def test_broker_diagnose_surfaces_blocked_buy_price_none_resolution(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    import bithumb_bot.app as app_module

    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1200000.0, 10000.0, 0.12, 0.01)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

        def get_accounts_validation_diagnostics(self):
            return {}

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "order_types": ("limit", "market"),
                        "bid_types": ("market",),
                        "ask_types": ("limit", "market"),
                        "bid_min_total_krw": 5000.0,
                        "ask_min_total_krw": 5000.0,
                        "bid_price_unit": 1.0,
                        "ask_price_unit": 1.0,
                    },
                )(),
                "source": {
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            },
        )(),
    )

    with pytest.raises(SystemExit, match="1"):
        cmd_broker_diagnose()
    object.__setattr__(settings, "MODE", original_mode)
    object.__setattr__(app_module.settings, "MODE", original_mode)
    object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)

    out = capsys.readouterr().out
    assert "overall=FAIL" in out
    assert "overall=PASS" not in out
    assert "[FAIL] BUY price=None chance resolution:" in out
    assert "raw_bid_types=['market']" in out
    assert "raw_order_types=['limit', 'market']" in out
    assert "raw_buy_supported_types=['market']" in out
    assert "support_source=bid_types" in out
    assert "resolved_contract=validation_order_type=price exchange_order_type=price submit_field=price" in out
    assert "contract_id=" in out
    assert "resolved_order_type=price submit_field=price allowed=False decision_outcome=block decision_basis=raw alias_used=False alias_policy=market_to_price_alias_disabled" in out
    assert "block_reason=buy_price_none_requires_explicit_price_support" in out


def test_broker_diagnose_fails_market_only_buy_price_none(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    import bithumb_bot.app as app_module

    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1200000.0, 10000.0, 0.12, 0.01)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

        def get_accounts_validation_diagnostics(self):
            return {}

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.DerivedOrderConstraints(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=1.0,
                ask_price_unit=1.0,
                order_types=("limit", "market"),
                bid_types=("market",),
                ask_types=("limit", "market"),
                order_sides=("bid", "ask"),
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

    try:
        with pytest.raises(SystemExit, match="1"):
            cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)

    out = capsys.readouterr().out
    assert "overall=PASS" not in out
    assert "overall=FAIL" in out
    assert "[FAIL] BUY price=None chance resolution:" in out
    assert "raw_buy_supported_types=['market']" in out
    assert "resolved_contract=validation_order_type=price exchange_order_type=price submit_field=price" in out
    assert (
        "allowed=False decision_outcome=block decision_basis=raw "
        "alias_used=False alias_policy=market_to_price_alias_disabled "
        "block_reason=buy_price_none_requires_explicit_price_support"
    ) in out


def test_broker_diagnose_fails_when_tracked_chance_contract_change_is_detected(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    import bithumb_bot.app as app_module

    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1200000.0, 10000.0, 0.12, 0.01)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

        def get_accounts_validation_diagnostics(self):
            return {}

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.DerivedOrderConstraints(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=1.0,
                ask_price_unit=1.0,
                order_types=("limit", "market"),
                bid_types=("market",),
                ask_types=("limit", "market"),
                order_sides=("bid", "ask"),
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
            chance_contract_change=order_rules.ChanceContractChange(
                detected=True,
                changed_fields={
                    "bid_types": {
                        "previous": ("limit", "price"),
                        "current": ("market",),
                    }
                },
                previous_snapshot={
                    "order_types": ("limit",),
                    "bid_types": ("limit", "price"),
                    "ask_types": ("limit", "market"),
                    "order_sides": ("bid", "ask"),
                },
                current_snapshot={
                    "order_types": ("limit", "market"),
                    "bid_types": ("market",),
                    "ask_types": ("limit", "market"),
                    "order_sides": ("bid", "ask"),
                },
                previous_fetched_ts=1710000000000,
            ),
        ),
    )

    with pytest.raises(SystemExit, match="1"):
        cmd_broker_diagnose()
    object.__setattr__(settings, "MODE", original_mode)
    object.__setattr__(app_module.settings, "MODE", original_mode)
    object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)

    out = capsys.readouterr().out
    assert "overall=FAIL" in out
    assert "[FAIL] chance contract drift canary:" in out
    assert "change_detected=1" in out
    assert "changed_fields=bid_types:['limit', 'price']->['market']" in out


def test_broker_diagnose_partial_failure(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('diag_live_2','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_max_market_slippage_bps = settings.MAX_MARKET_SLIPPAGE_BPS
    original_live_price_protection_max_slippage_bps = settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagPartialBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            raise RuntimeError("open orders timeout")

        def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagPartialBroker())
    monkeypatch.setattr("bithumb_bot.app.get_effective_order_rules", lambda _pair: (_ for _ in ()).throw(RuntimeError("rules api down")))

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", original_max_market_slippage_bps)
        object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", original_live_price_protection_max_slippage_bps)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)

    out = capsys.readouterr().out
    assert "overall=WARN" in out
    assert "[PASS] live execution mode: MODE=live LIVE_DRY_RUN=True armed=False" in out
    assert "[WARN] symbol/order rule query" in out
    assert "[WARN] open order query" in out


def test_broker_diagnose_accounts_policy_context_is_operator_readable(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_live_real_order_armed = settings.LIVE_REAL_ORDER_ARMED
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(self, **_kwargs):
            return []

        def get_accounts_validation_diagnostics(self):
            return {
                "reason": "ok",
                "failure_category": "none",
                "row_count": 1,
                "currencies": ["KRW"],
                "missing_required_currencies": [],
                "duplicate_currencies": [],
                "execution_mode": "live_dry_run_unarmed",
                "quote_currency": "KRW",
                "base_currency": "BTC",
                "base_currency_missing_policy": "allow_zero_position_start_in_dry_run",
                "preflight_outcome": "pass_no_position_allowed",
                "last_success_reason": "ok",
                "last_failure_reason": "required currency missing",
            }

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("skip rule detail")),
    )
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", original_live_real_order_armed)

    out = capsys.readouterr().out
    assert "execution_mode=live_dry_run_unarmed quote_currency=KRW base_currency=BTC" in out
    assert "base_currency_missing_policy=allow_zero_position_start_in_dry_run" in out
    assert "preflight_outcome=pass_no_position_allowed" in out


def test_broker_diagnose_accounts_policy_context_shows_real_order_block(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_live_real_order_armed = settings.LIVE_REAL_ORDER_ARMED
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(self, **_kwargs):
            return []

        def get_accounts_validation_diagnostics(self):
            return {
                "reason": "required currency missing",
                "failure_category": "schema_mismatch",
                "row_count": 1,
                "currencies": ["KRW"],
                "missing_required_currencies": ["BTC"],
                "duplicate_currencies": [],
                "execution_mode": "live_real_order_path",
                "quote_currency": "KRW",
                "base_currency": "BTC",
                "base_currency_missing_policy": "block_when_base_currency_row_missing",
                "preflight_outcome": "fail_real_order_blocked",
                "last_success_reason": "ok",
                "last_failure_reason": "required currency missing",
            }

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("skip rule detail")),
    )
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", original_live_real_order_armed)

    out = capsys.readouterr().out
    assert "execution_mode=live_real_order_path quote_currency=KRW base_currency=BTC" in out
    assert "base_currency_missing_policy=block_when_base_currency_row_missing" in out
    assert "preflight_outcome=fail_real_order_blocked" in out


def test_broker_diagnose_config_failure_is_critical(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_api_key = settings.BITHUMB_API_KEY
    original_api_secret = settings.BITHUMB_API_SECRET
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(app_module.settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)
    object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", 0)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(app_module.settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(app_module.settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")
    object.__setattr__(app_module.settings, "BITHUMB_API_SECRET", "")

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        app_module,
        "validate_live_mode_preflight",
        lambda _cfg: (_ for _ in ()).throw(
            app_module.LiveModeValidationError(
                "live mode preflight validation failed: MAX_ORDER_KRW must be > 0"
            )
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "bid_price_unit": 0.0,
                        "ask_price_unit": 0.0,
                    },
                )(),
                "source": {"min_qty": "local_fallback"},
            },
        )(),
    )

    try:
        with pytest.raises(SystemExit):
            app_module.cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(app_module.settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "BITHUMB_API_KEY", original_api_key)
        object.__setattr__(app_module.settings, "BITHUMB_API_KEY", original_api_key)
        object.__setattr__(settings, "BITHUMB_API_SECRET", original_api_secret)
        object.__setattr__(app_module.settings, "BITHUMB_API_SECRET", original_api_secret)

    out = capsys.readouterr().out
    assert "overall=FAIL" in out
    assert "[FAIL] config/env loaded" in out


def test_broker_diagnose_never_calls_place_order(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_max_market_slippage_bps = settings.MAX_MARKET_SLIPPAGE_BPS
    original_live_price_protection_max_slippage_bps = settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)
    place_calls = {"n": 0}

    class _NoTradeBroker:
        def place_order(self, **_kwargs):
            place_calls["n"] += 1
            raise AssertionError("place_order must not be called")

        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

        def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            raise NotImplementedError

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _NoTradeBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "order_types": ("limit", "price"),
                        "bid_types": ("price",),
                        "ask_types": ("limit", "market"),
                        "order_sides": ("bid", "ask"),
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "bid_price_unit": 0.0,
                        "ask_price_unit": 0.0,
                    },
                )(),
                "source": {
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "order_types": "chance_doc",
                    "bid_types": "chance_doc",
                    "ask_types": "chance_doc",
                    "order_sides": "chance_doc",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            },
        )(),
    )

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", original_max_market_slippage_bps)
        object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", original_live_price_protection_max_slippage_bps)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)

    assert place_calls["n"] == 0


def test_recovery_report_summarizes_unresolved_and_recovery_required(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="open_1", created_ts=now_ms - 30_000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="open_2",
        created_ts=now_ms - 20_000,
    )

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"remote_open_order_found": 2},
        now_epoch_sec=time.time() - 3,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="periodic reconcile failed",
        reason_code="PERIODIC_RECONCILE_FAILED",
        unresolved=True,
    )

    report = _load_recovery_report()

    assert int(report["unresolved_count"]) == 2
    assert int(report["recovery_required_count"]) == 1
    assert report["oldest_unresolved_age_sec"] is not None
    assert float(report["oldest_unresolved_age_sec"]) >= 20.0
    assert "status=ok" in str(report["last_reconcile_summary"])
    assert "reason_code=RECONCILE_OK" in str(report["last_reconcile_summary"])
    assert "code=PERIODIC_RECONCILE_FAILED" in str(report["recent_halt_reason"])
    assert int(report["unprocessed_remote_open_orders"]) == 2
    oldest_orders = report["oldest_orders"]
    assert isinstance(oldest_orders, list)
    assert len(oldest_orders) == 2
    assert oldest_orders[0]["client_order_id"] == "open_1"
    assert oldest_orders[0]["status"] == "NEW"
    assert oldest_orders[1]["client_order_id"] == "open_2"


def test_recovery_report_shows_defaults_when_empty(tmp_path):
    _set_tmp_db(tmp_path)

    report = _load_recovery_report()

    assert int(report["unresolved_count"]) == 0
    assert int(report["recovery_required_count"]) == 0
    assert report["oldest_unresolved_age_sec"] is None
    assert report["oldest_orders"] == []
    assert report["last_reconcile_summary"] == "none"
    assert report["recent_halt_reason"] == "none"
    assert int(report["unprocessed_remote_open_orders"]) == 0


def test_recovery_report_candidate_no_match(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="rr_none", created_ts=now_ms - 20_000)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("remote_x", "ex_x", "SELL", "NEW", None, 5.0, 0.0, now_ms - 1_000_000, now_ms - 900_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_none"]
    assert len(entries) == 1
    assert entries[0]["attempted_locally"] is False
    assert entries[0]["request_likely_sent"] == "unknown"
    assert entries[0]["candidate_outcome"] == "no_candidate"
    assert entries[0]["candidates"] == []


def test_recovery_report_candidate_single_plausible(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="rr_one", created_ts=now_ms - 30_000, price=100.0)
    _insert_order_event(
        client_order_id="rr_one",
        event_type="submit_attempt_recorded",
        event_ts=now_ms - 29_000,
        submit_attempt_id="attempt_one",
        submit_ts=now_ms - 29_000,
        timeout_flag=1,
        exchange_order_id_obtained=0,
        order_status="SUBMIT_UNKNOWN",
    )

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("remote_1", "ex_match", "BUY", "PARTIAL", 100.05, 0.01, 0.003, now_ms - 32_000, now_ms - 10_000),
                BrokerOrder("remote_2", "ex_weak", "BUY", "NEW", None, 0.02, 0.0, now_ms - 3_600_000, now_ms - 3_500_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_one"]
    assert len(entries) == 1
    assert entries[0]["candidate_outcome"] == "single_plausible_candidate"
    assert entries[0]["likely_broker_match"] is True
    assert entries[0]["likely_broker_exchange_order_id"] == "ex_match"
    assert entries[0]["attempted_locally"] is True
    assert entries[0]["request_likely_sent"] == "yes"
    assert int(entries[0]["plausible_candidate_count"]) == 1
    assert entries[0]["candidates"][0]["exchange_order_id"] == "ex_match"
    assert float(entries[0]["candidates"][0]["time_gap_sec"]) < 90.0
    assert float(entries[0]["candidates"][0]["qty_gap_pct"]) < 1.0
    assert float(entries[0]["candidates"][0]["price_gap_pct"]) < 0.2


def test_recovery_report_candidate_multiple_plausible(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="rr_many", created_ts=now_ms - 40_000, price=100.0)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("rr_many", "ex_m1", "BUY", "NEW", 100.05, 0.0101, 0.0, now_ms - 42_000, now_ms - 20_000),
                BrokerOrder("rr_many", "ex_m2", "BUY", "PARTIAL", 99.95, 0.0099, 0.005, now_ms - 41_000, now_ms - 19_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_many"]
    assert len(entries) == 1
    assert entries[0]["candidate_outcome"] == "multiple_plausible_candidates"
    assert entries[0]["likely_broker_match"] is False
    assert int(entries[0]["plausible_candidate_count"]) == 2
    assert all("same client_order_id" in c["match_reason"] for c in entries[0]["candidates"][:2])


def test_recovery_report_candidate_weak_only(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="rr_weak", created_ts=now_ms - 30_000, side="BUY", qty_req=0.01)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("remote_weak", "ex_weak", "BUY", "NEW", None, 0.01025, 0.0, now_ms - 580_000, now_ms - 20_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_weak"]
    assert len(entries) == 1
    assert entries[0]["candidate_outcome"] == "weak_candidates_only"
    assert entries[0]["likely_broker_match"] is False
    assert int(entries[0]["plausible_candidate_count"]) == 0
    assert entries[0]["candidates"][0]["exchange_order_id"] == "ex_weak"



def test_recovery_report_shows_concise_oldest_order_list(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    for i in range(6):
        _insert_order(
            status="RECOVERY_REQUIRED" if i % 2 == 0 else "NEW",
            client_order_id=f"open_{i}",
            created_ts=now_ms - (60_000 - i * 1_000),
        )
    _set_last_error(
        client_order_id="open_0",
        last_error=(
            "timeout while polling exchange status endpoint due to transient "
            "error and retry budget exceeded"
        ),
    )

    cmd_recovery_report()
    out = capsys.readouterr().out

    assert "[RECOVERY-REPORT]" in out
    assert "[P0] blocker_summary_view" in out
    assert "blocker=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "recommended_next_action=uv run python bot.py recover-order --client-order-id <id>" in out
    assert "[P1] order_recovery_status" in out
    assert "unresolved_count=6" in out
    assert "recovery_required_count=3" in out
    assert "[RUN-LOCK]" in out
    assert "[P2] resume_eligibility" in out
    assert "active_blocker_summary=" in out
    assert "risk_level=high" in out
    assert "[P3] balance_mismatch" in out
    assert "summary=none" in out
    assert "[P4] last_reconcile_summary" in out
    assert "[P5] recent_halt_reason" in out
    assert "[P6] operator_next_action" in out
    assert "action=manual_recovery_required" in out
    assert (
        "recommended_next_action="
        "Recover RECOVERY_REQUIRED orders before attempting resume."
    ) in out
    assert (
        "resume_blocked_reason=resume blocked by RECOVERY_REQUIRED orders" in out
    )
    assert (
        "command=uv run python bot.py recover-order --client-order-id <id>" in out
    )
    assert "[P7] unprocessed_remote_open_orders" in out
    assert "resume_allowed=0" in out
    assert "can_resume=false" in out
    assert "blockers=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "force_resume_allowed=0" in out
    assert "blocker_summary=total=" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "overridable=0" in out
    assert "recent_external_cash_adjustment=none" in out
    assert "oldest_unresolved_orders(top 5):" in out
    assert "recovery_required_orders(top 3):" in out
    assert "client_order_id=open_0" in out
    assert "client_order_id=open_4" in out
    assert "reason=timeout while polling exchange status endpoint due to transi..." in out
    assert (
        "last_error=timeout while polling exchange status endpoint due to transi..."
        in out
    )


def test_recovery_report_shows_recent_external_cash_adjustment_summary(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_external_cash_adjustment(
            conn,
            event_ts=1710000000000,
            currency="KRW",
            delta_amount=250.0,
            source="legacy_balance_api",
            reason="reconcile_cash_drift",
            broker_snapshot_basis={"cash_available": 1000000.0},
            note="recovery test adjustment",
            adjustment_key="recovery-report-adjustment-1",
        )
    finally:
        conn.close()

    cmd_recovery_report()
    out = capsys.readouterr().out

    expected_last_event = kst_str(1710000000000)
    assert "[RECOVERY-REPORT]" in out
    assert "recent_external_cash_adjustment=count=1 total=250.000" in out
    assert "last_delta=250.000" in out
    assert f"last_event={expected_last_event}" in out
    assert "present=1" in out
    assert "source=legacy_balance_api" in out
    assert "reason=reconcile_cash_drift" in out


def test_recovery_report_distinguishes_adjusted_cash_only_mismatch_fallback(
    tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_external_cash_adjustment(
            conn,
            event_ts=1710000000000,
            currency="KRW",
            delta_amount=50.0,
            source="legacy_balance_api",
            reason="reconcile_cash_drift",
            broker_snapshot_basis={"cash_available": 1000.0},
            note="adjusted cash-only mismatch",
            adjustment_key="recovery-report-adjusted-cash-only-mismatch",
        )
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 1,
            "balance_split_mismatch_summary": "cash_available(local=1000,broker=1050,delta=50)",
            "external_cash_adjustment_count": 1,
            "external_cash_adjustment_delta_krw": 50.0,
            "external_cash_adjustment_total_krw": 50.0,
        },
        now_epoch_sec=time.time() - 1,
    )

    try:
        cmd_recovery_report()
        out = capsys.readouterr().out
        expected_last_event = kst_str(1710000000000)
        assert "recent_external_cash_adjustment=count=1 total=50.000" in out
        assert "last_delta=50.000" in out
        assert f"last_event={expected_last_event}" in out
        assert "present=1" in out
        assert "blocker=BALANCE_SPLIT_MISMATCH" in out
        assert (
            "summary=cash split mismatch persists after external cash adjustment was recorded"
            in out
        )
        assert "action=reconcile_after_external_adjustment" in out
        assert "command=uv run python bot.py reconcile" in out

        report = _load_recovery_report()
        assert report["resume_blockers"] == ["BALANCE_SPLIT_MISMATCH"]
        assert report["resume_blocker_reason_codes"] == ["PORTFOLIO_BROKER_CASH_MISMATCH"]
        assert report["blocker_summary_view"][0]["recent_external_cash_adjustment_present"] is True
        assert report["blocker_summary_view"][0]["recent_external_cash_adjustment_count"] == 1
    finally:
        object.__setattr__(settings, "MODE", original_mode)


def test_recovery_report_includes_recent_order_lifecycle_block(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="o_submit_unknown", created_ts=now_ms - 20_000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="o_recovery", created_ts=now_ms - 10_000)

    conn = ensure_db()
    try:
        conn.execute(
            "UPDATE orders SET submit_attempt_id=?, exchange_order_id=NULL WHERE client_order_id=?",
            ("attempt_a", "o_submit_unknown"),
        )
        conn.execute(
            "UPDATE orders SET submit_attempt_id=?, exchange_order_id=? WHERE client_order_id=?",
            ("attempt_b", "ex-123", "o_recovery"),
        )
        conn.commit()
    finally:
        conn.close()

    _insert_order_event(
        client_order_id="o_submit_unknown",
        event_type="intent_created",
        event_ts=now_ms - 20_000,
        submit_attempt_id="attempt_a",
        intent_ts=now_ms - 20_000,
    )
    _insert_order_event(
        client_order_id="o_submit_unknown",
        event_type="submit_attempt_preflight",
        event_ts=now_ms - 19_500,
        submit_attempt_id="attempt_a",
    )
    _insert_order_event(
        client_order_id="o_submit_unknown",
        event_type="submit_attempt_recorded",
        event_ts=now_ms - 19_000,
        submit_attempt_id="attempt_a",
        submit_ts=now_ms - 19_000,
        timeout_flag=1,
        exchange_order_id_obtained=0,
        order_status="SUBMIT_UNKNOWN",
    )

    cmd_recovery_report()
    out = capsys.readouterr().out

    assert "[P8] recent_order_lifecycle(top 2):" in out
    assert "client_order_id=o_submit_unknown" in out
    assert "submit_ts=" in out
    assert "correlation=attempt=attempt_a meta=1 timeout=1" in out
    assert "mapping=submit_no_mapping" in out
    assert "state=SUBMIT_UNKNOWN unresolved=1" in out
    assert "client_order_id=o_recovery" in out
    assert "mapping=mapped" in out
    assert "state=RECOVERY_REQUIRED unresolved=1" in out


def test_health_prints_risk_snapshot_for_operator_visibility(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "bid_min_total_krw": 5500.0,
                        "ask_min_total_krw": 5000.0,
                        "bid_price_unit": 10.0,
                        "ask_price_unit": 1.0,
                    },
                )(),
                "source": {
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            },
        )(),
    )
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 2.0,
            "error_count": 0,
            "trading_enabled": False,
            "retry_at_epoch_sec": 1200.0,
            "unresolved_open_order_count": 4,
            "oldest_unresolved_order_age_sec": 95.0,
            "recovery_required_count": 2,
            "last_reconcile_epoch_sec": 1000.0,
            "last_reconcile_status": "error",
            "last_reconcile_error": "timeout",
            "last_reconcile_reason_code": "RECONCILE_TIMEOUT",
            "last_reconcile_metadata": None,
            "last_disable_reason": "periodic reconcile failed",
            "halt_new_orders_blocked": True,
            "halt_reason_code": "PERIODIC_RECONCILE_FAILED",
            "halt_state_unresolved": True,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (False, []))

    cmd_health()
    out = capsys.readouterr().out

    assert "[RISK-SNAPSHOT]" in out
    assert "[HALT-RECOVERY-STATUS]" in out
    assert "state=halted trading_enabled=0 halt_new_orders_blocked=1" in out
    assert "can_resume=false" in out
    assert "blockers=none" in out
    assert "resume_safety=unsafe" in out
    assert "unresolved_open_order_count=4 recovery_required_count=2 submit_unknown_count=0" in out
    assert "recovery_required_present=1" in out
    assert "run_lock=path=" in out
    assert "current_halt_reason=code=PERIODIC_RECONCILE_FAILED reason=periodic reconcile failed" in out
    assert "reconcile_latest=epoch_sec=1000.0 status=error reason_code=RECONCILE_TIMEOUT" in out
    assert (
        "unresolved_attribution_count=0 recent_recovery_derived_trade_count=0 "
        "ambiguous_linkage_after_recent_reconcile=False"
    ) in out
    assert "[CRITICAL-OPERATOR-SUMMARY]" in out
    assert "halt_reason=PERIODIC_RECONCILE_FAILED unresolved_order_count=4" in out
    assert "open_order_count=0" in out
    assert "position=flat" in out
    assert "next_commands=uv run python bot.py recover-order --client-order-id <id> | uv run python bot.py recovery-report" in out
    assert "[ORDER-RULE-SNAPSHOT]" in out
    assert "BUY(min_total_krw=5500.0 (source=chance_doc), price_unit=10.0 (source=chance_doc))" in out


def test_health_reports_order_rule_fallback_risk_when_autosync_degrades(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 2.0,
            "error_count": 0,
            "trading_enabled": True,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": None,
            "halt_new_orders_blocked": False,
            "halt_reason_code": None,
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (True, []))
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.DerivedOrderConstraints(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=0.0,
                ask_price_unit=0.0,
            ),
            source={
                "min_qty": "local_fallback",
                "qty_step": "local_fallback",
                "min_notional_krw": "local_fallback",
                "max_qty_decimals": "local_fallback",
                "bid_min_total_krw": "local_fallback",
                "ask_min_total_krw": "local_fallback",
                "bid_price_unit": "local_fallback",
                "ask_price_unit": "local_fallback",
                "ruleset": "merged",
            },
            fallback_used=True,
            fallback_reason_code="AUTH_QUERY_HASH_MISMATCH",
            fallback_reason_summary="JWT query_hash mismatch; GET query string/body hash must match transmitted params",
            fallback_reason_detail="BrokerRejectError: invalid_query_payload",
            fallback_risk=(
                "order-rule auto-sync unavailable; side minimum totals, fees, and tick-size normalization "
                "may stay on local fallback until /v1/orders/chance succeeds again"
            ),
        ),
    )

    cmd_health()
    out = capsys.readouterr().out

    assert "order_rules_autosync=FALLBACK" in out
    assert "reason_code=AUTH_QUERY_HASH_MISMATCH" in out
    assert "risk=order-rule auto-sync unavailable" in out


def test_risk_report_prints_recent_evaluation_with_provenance(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(app_module.settings, "MODE", "paper")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(app_module.settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 30_000.0)
    object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", 30_000.0)

    conn = ensure_db()
    try:
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        now_ms = int(time.time() * 1000)
        evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms,
            price=100_000_000.0,
            mark_price_source="test_seed",
            evaluation_origin="test_seed",
        )
        set_portfolio_breakdown(
            conn,
            cash_available=954_734.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms + 1,
            price=100_000_000.0,
            mark_price_source="test_mark",
            evaluation_origin="test_breach",
        )
    finally:
        conn.close()

    cmd_risk_report(limit=5)
    out = capsys.readouterr().out

    assert "[RISK-REPORT]" in out
    assert "baseline_origin=seeded_on_first_verified_eval" in out
    assert "reason_code=DAILY_LOSS_LIMIT" in out
    assert "current_source=local_portfolio" in out


def test_health_surfaces_supported_buy_price_none_resolution(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 2.0,
            "last_candle_status": "ok",
            "last_candle_sync_epoch_sec": 1.0,
            "last_candle_ts_ms": 1000,
            "last_candle_status_detail": "ok",
            "error_count": 0,
            "trading_enabled": True,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": None,
            "halt_new_orders_blocked": False,
            "halt_reason_code": None,
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (True, []))
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.DerivedOrderConstraints(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5500.0,
                ask_min_total_krw=5500.0,
                bid_price_unit=10.0,
                ask_price_unit=10.0,
                order_types=("limit",),
                bid_types=("limit", "price"),
                ask_types=("limit", "market"),
                order_sides=("bid", "ask"),
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
        "bithumb_bot.app.build_broker_with_auth_diagnostics",
        lambda **_kwargs: (SimpleNamespace(get_accounts_validation_diagnostics=lambda: {}), {}),
    )

    cmd_health()
    out = capsys.readouterr().out

    assert "buy_price_none_resolution=" in out
    assert "raw_bid_types=['limit', 'price']" in out
    assert "raw_order_types=['limit']" in out
    assert "raw_buy_supported_types=['limit', 'price']" in out
    assert "support_source=bid_types" in out
    assert "resolved_contract=validation_order_type=price exchange_order_type=price submit_field=price" in out
    assert "contract_id=" in out
    assert "resolved_order_type=price" in out
    assert "submit_field=price" in out
    assert "allowed=True" in out
    assert "decision_basis=raw" in out
    assert "alias_used=False" in out
    assert "block_reason=-" in out


def test_health_surfaces_blocked_buy_price_none_resolution(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 2.0,
            "last_candle_status": "ok",
            "last_candle_sync_epoch_sec": 1.0,
            "last_candle_ts_ms": 1000,
            "last_candle_status_detail": "ok",
            "error_count": 0,
            "trading_enabled": True,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": None,
            "halt_new_orders_blocked": False,
            "halt_reason_code": None,
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (True, []))
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.DerivedOrderConstraints(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
                bid_min_total_krw=5500.0,
                ask_min_total_krw=5500.0,
                bid_price_unit=10.0,
                ask_price_unit=10.0,
                order_types=("limit", "market"),
                bid_types=("market",),
                ask_types=("limit", "market"),
                order_sides=("bid", "ask"),
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
        "bithumb_bot.app.build_broker_with_auth_diagnostics",
        lambda **_kwargs: (SimpleNamespace(get_accounts_validation_diagnostics=lambda: {}), {}),
    )

    cmd_health()
    out = capsys.readouterr().out

    assert "buy_price_none_resolution=" in out
    assert "allowed=True" not in out
    assert "raw_bid_types=['market']" in out
    assert "raw_order_types=['limit', 'market']" in out
    assert "raw_buy_supported_types=['market']" in out
    assert "support_source=bid_types" in out
    assert "resolved_contract=validation_order_type=price exchange_order_type=price submit_field=price" in out
    assert "contract_id=" in out
    assert "resolved_order_type=price" in out
    assert "submit_field=price" in out
    assert "allowed=False" in out
    assert "decision_outcome=block" in out
    assert "decision_basis=raw" in out
    assert "alias_used=False" in out
    assert "alias_policy=market_to_price_alias_disabled" in out
    assert "block_reason=buy_price_none_requires_explicit_price_support" in out


def test_health_summary_shows_paused_state(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": None,
            "last_candle_status": "waiting_first_sync",
            "last_candle_sync_epoch_sec": None,
            "last_candle_ts_ms": None,
            "last_candle_status_detail": "startup warming up",
            "error_count": 0,
            "trading_enabled": False,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": "manual operator pause",
            "halt_new_orders_blocked": False,
            "halt_reason_code": None,
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (True, []))

    cmd_health()
    out = capsys.readouterr().out

    assert "[HALT-RECOVERY-STATUS]" in out
    assert "state=paused trading_enabled=0 halt_new_orders_blocked=0" in out
    assert "reason=code=- reason=manual operator pause" in out
    assert "can_resume=true" in out
    assert "blockers=none" in out
    assert "resume_safety=safe" in out
    assert "last_candle_age_sec=None (status=waiting_first_sync" in out
    assert "last_candle_status_detail=startup warming up" in out


def test_health_includes_balance_source_diagnostics(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 1.0,
            "last_candle_status": "ok",
            "last_candle_sync_epoch_sec": 1.0,
            "last_candle_ts_ms": 1000,
            "last_candle_status_detail": "ok",
            "error_count": 0,
            "trading_enabled": True,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": None,
            "halt_new_orders_blocked": False,
            "halt_reason_code": None,
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (True, []))
    monkeypatch.setattr(
        "bithumb_bot.app.DEFAULT_BITHUMB_BROKER_CLASS",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "myasset_ws_private_stream",
                    "reason": "myAsset stream stale",
                    "failure_category": "stale_source",
                    "stale": True,
                    "last_success_ts_ms": 1710000000000,
                    "last_observed_ts_ms": 1710000005000,
                    "last_asset_ts_ms": 1710000000000,
                }
            },
        )(),
    )

    cmd_health()
    out = capsys.readouterr().out

    assert "balance_source=myasset_ws_private_stream" in out
    assert "diag_category=stale_source stale=True" in out
    assert "diag_execution_mode=- quote_currency=- base_currency=- base_missing_policy=- preflight_outcome=-" in out
    assert "balance_source_last_asset_ts_ms=1710000000000" in out


def test_health_shows_recent_external_cash_adjustment_summary(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_external_cash_adjustment(
            conn,
            event_ts=1710000000000,
            currency="KRW",
            delta_amount=125.0,
            source="legacy_balance_api",
            reason="reconcile_cash_drift",
            broker_snapshot_basis={"cash_available": 1000.0},
            note="health test adjustment",
            adjustment_key="health-report-adjustment-1",
        )
    finally:
        conn.close()

    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.build_broker_with_auth_diagnostics",
        lambda **kwargs: (
            type(
                "_HealthDiagBroker",
                (),
                {
                    "get_accounts_validation_diagnostics": lambda self: {
                        "source": "accounts_v1_rest_snapshot",
                        "reason": "ok",
                        "failure_category": "none",
                        "stale": False,
                    }
                },
            )(),
            {"env": {}, "chance_auth": {}},
        ),
    )

    cmd_health()
    out = capsys.readouterr().out

    expected_last_event = kst_str(1710000000000)
    assert "recent_external_cash_adjustment=count=1 total=125.000" in out
    assert "last_delta=125.000" in out
    assert f"last_event={expected_last_event}" in out
    assert "present=1" in out


def test_health_prints_accounts_preflight_outcome_context(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 1.0,
            "last_candle_status": "ok",
            "last_candle_sync_epoch_sec": 1.0,
            "last_candle_ts_ms": 1000,
            "last_candle_status_detail": "ok",
            "error_count": 0,
            "trading_enabled": False,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": "accounts preflight blocked",
            "halt_new_orders_blocked": True,
            "halt_reason_code": "PRECHECK_FAILED",
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (False, []))
    monkeypatch.setattr(
        "bithumb_bot.app.DEFAULT_BITHUMB_BROKER_CLASS",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "accounts_v1_rest_snapshot",
                    "reason": "required currency missing",
                    "failure_category": "schema_mismatch",
                    "stale": False,
                    "execution_mode": "live_real_order_path",
                    "quote_currency": "KRW",
                    "base_currency": "BTC",
                    "base_currency_missing_policy": "block_when_base_currency_row_missing",
                    "preflight_outcome": "fail_real_order_blocked",
                }
            },
        )(),
    )

    cmd_health()
    out = capsys.readouterr().out

    assert "diag_execution_mode=live_real_order_path quote_currency=KRW base_currency=BTC" in out
    assert "base_missing_policy=block_when_base_currency_row_missing preflight_outcome=fail_real_order_blocked" in out
    assert "balance_source_preflight_outcome=fail_real_order_blocked" in out


def test_health_summary_flags_unresolved_orders_as_resume_unsafe(capsys, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="unsafe_resume_1", created_ts=now_ms - 15_000)
    runtime_state.refresh_open_order_health(now_epoch_sec=now_ms / 1000)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    cmd_health()
    out = capsys.readouterr().out

    assert "state=paused" in out
    assert "unresolved_open_order_count=1" in out
    assert "can_resume=false" in out
    assert "blockers=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "resume_safety=unsafe (STARTUP_SAFETY_GATE_BLOCKED)" in out



def test_health_auto_clears_stale_initial_reconcile_halt(capsys, tmp_path):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    cmd_health()
    out = capsys.readouterr().out

    assert "halt_new_orders_blocked=0" in out
    assert "can_resume=true" in out
    assert "blockers=none" in out
    assert "resume_safety=safe" in out
    assert "halt_state_unresolved=False" in out
    assert "halt_reason_code=None" in out




def test_recovery_report_includes_submit_unknown_count(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="su_1", created_ts=now_ms - 10_000)

    report = _load_recovery_report()

    assert int(report["submit_unknown_count"]) == 1


def test_resume_refusal_prints_explicit_blocking_reasons_header(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit):
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "[RESUME] refused:" in out
    assert "blocking_reasons:" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out

def test_recovery_report_json_snapshot_schema_is_stable(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="open_1", created_ts=now_ms - 40_000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="recovery_1",
        created_ts=now_ms - 30_000,
    )

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"remote_open_order_found": 1},
        now_epoch_sec=time.time() - 2,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="periodic reconcile failed",
        reason_code="PERIODIC_RECONCILE_FAILED",
        unresolved=True,
    )

    cmd_recovery_report(as_json=True)
    out = capsys.readouterr().out
    payload = json.loads(out.strip().splitlines()[-1])

    assert set(payload.keys()) == {
        "mode",
        "broker_recent_orders_snapshot_error",
        "balance_split_mismatch_summary",
        "dust_classification",
        "dust_effective_flat",
        "dust_residual_present",
        "dust_residual_allow_resume",
        "dust_policy_reason",
        "dust_residual_summary",
        "dust_state",
        "dust_display_scope",
        "broker_dust_signal_state",
        "broker_dust_signal_message",
        "dust_tradeability_consistent",
        "dust_state_label",
        "dust_operator_action",
        "dust_operator_message",
        "dust_broker_local_match",
        "dust_new_orders_allowed",
        "dust_resume_allowed_by_policy",
        "dust_treat_as_flat",
        "dust_broker_qty",
        "dust_local_qty",
        "dust_delta_qty",
        "dust_min_qty",
        "dust_min_notional_krw",
        "dust_broker_qty_below_min",
        "dust_local_qty_below_min",
        "dust_broker_notional_below_min",
        "dust_local_notional_below_min",
        "effective_flat_due_to_harmless_dust",
        "residue_policy_scope",
        "residue_policy_state",
        "residue_policy_message",
        "residue_blocks_new_entry",
        "residue_blocks_closeout",
        "strategy_tradeability_state",
        "entry_policy_state",
        "closeout_policy_state",
        "tradeability_operator_message",
            "recent_dust_unsellable_event",
            "recent_external_cash_adjustment",
            "external_position_accounting_repair_preview",
            "external_position_adjustment_summary",
            "manual_flat_accounting_repair_preview",
            "manual_flat_accounting_repair_summary",
        "fee_gap_accounting_repair_preview",
        "fee_gap_accounting_repair_summary",
        "fee_pending_accounting_repair_summary",
        "fee_rate_drift_diagnostics",
            "fill_accounting_incident_projection",
        "fill_accounting_root_cause",
        "recovery_policy",
        "position_authority_rebuild_preview",
            "position_authority_repair_summary",
                "broker_fill_observation_summary",
        "runtime_readiness",
        "pending_fee_count",
        "auto_recovery_count",
        "operator_review_required_count",
        "accounting_projection_ok",
        "broker_portfolio_converged",
        "broker_qty_known",
            "broker_qty",
            "portfolio_qty",
            "lot_projection_converged",
            "live_ready",
            "blocking_incident_class",
            "recovery_stage",
            "recovery_blocker_categories",
        "active_blocker_summary",
        "blocker_summary",
        "blocker_summary_view",
        "blockers",
        "force_resume_allowed",
        "can_resume",
        "resume_blockers",
        "last_reconcile_summary",
        "oldest_orders",
        "oldest_unresolved_age_sec",
        "operator_next_action",
        "recommended_next_action",
        "non_overridable_blockers",
        "primary_blocker_code",
        "primary_blocker_reason_code",
        "recent_halt_reason",
        "recommended_command",
        "recent_order_lifecycle",
        "recovery_required_count",
        "recovery_required_summary",
        "submit_unknown_count",
        "resume_blocked_reason",
        "resume_blocker_reason_codes",
        "resume_allowed",
        "risk_level",
        "trading_enabled",
        "trading_state",
        "trading_blocked",
        "hard_halt_reason",
        "emergency_flatten_blocked",
        "emergency_flatten_block_reason",
        "recovery_candidates",
        "remote_known_unresolved_verification_summary",
        "unprocessed_remote_open_orders",
        "unresolved_count",
        "unresolved_summary",
    }


def test_recovery_report_json_snapshot_has_required_fields(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="open_1", created_ts=now_ms - 50_000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="recovery_1",
        created_ts=now_ms - 20_000,
    )

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"remote_open_order_found": 3},
        now_epoch_sec=time.time() - 1,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        unresolved=True,
    )

    cmd_recovery_report(as_json=True)
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert payload["trading_enabled"] is False
    assert "code=" in payload["recent_halt_reason"]
    assert payload["recent_halt_reason"] != "none"
    assert payload["unresolved_count"] >= 1
    assert isinstance(payload["unresolved_summary"], list)
    assert payload["unresolved_summary"]
    assert payload["unresolved_summary"][0]["client_order_id"]
    assert payload["recovery_required_count"] >= 1
    assert isinstance(payload["recovery_required_summary"], list)
    assert payload["recovery_required_summary"]
    assert payload["primary_blocker_code"] != "-"
    assert payload["recovery_required_summary"][0]["client_order_id"]
    assert payload["last_reconcile_summary"] != "none"
    assert "status=" in payload["last_reconcile_summary"]
    assert payload["resume_allowed"] is False
    assert payload["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in payload["resume_blockers"]
    assert payload["force_resume_allowed"] is False
    assert isinstance(payload["blockers"], list)
    assert payload["blockers"]
    assert payload["blockers"][0]["code"]
    assert isinstance(payload["blockers"][0]["overridable"], bool)
    assert "total=" in payload["blocker_summary"]
    assert "non_overridable=" in payload["blocker_summary"]
    assert payload["active_blocker_summary"]
    assert payload["risk_level"] in {"low", "medium", "high"}
    assert isinstance(payload["non_overridable_blockers"], list)
    assert payload["operator_next_action"] in {
        "resume_now",
        "review_and_force_resume",
        "manual_recovery_required",
        "investigate_blockers",
    }
    assert payload["recommended_next_action"]
    assert payload["resume_blocked_reason"]
    assert payload["recommended_command"]
    assert payload["dust_display_scope"] == "broker_reconcile_signal"
    assert payload["residue_policy_scope"] == "lot_native_tradeability"
    assert payload["residue_policy_state"]
    assert isinstance(payload["dust_tradeability_consistent"], bool)
    assert payload["tradeability_operator_message"]




def test_recovery_report_blocker_summary_view_for_submit_unknown(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="summary_submit_unknown", created_ts=now_ms - 5_000)

    report = _load_recovery_report()

    view = report["blocker_summary_view"]
    assert view
    assert view[0]["blocker"] == "STARTUP_SAFETY_GATE_BLOCKED"
    assert view[0]["reason_code"] == "SUBMIT_UNKNOWN_RECOVERY_REQUIRED"
    assert "submit_unknown=1" in view[0]["evidence"]
    assert view[0]["recommended_next_action"] == "uv run python bot.py reconcile"


def test_build_resume_guidance_prefers_manual_recovery_required_over_dust_review() -> None:
    blockers = [
        ResumeBlocker(
            code="HARMLESS_DUST_POLICY_REVIEW_REQUIRED",
            detail="harmless dust still needs review",
            reason_code="DUST_RESIDUAL_BLOCK",
            summary="harmless dust still needs policy review",
            overridable=False,
        )
    ]

    guidance = build_resume_guidance(
        resume_allowed=False,
        blockers=blockers,
        unresolved_count=0,
        recovery_required_count=1,
        submit_unknown_count=0,
    )

    assert guidance.operator_next_action == "manual_recovery_required"
    assert guidance.recommended_command == "uv run python bot.py recover-order --client-order-id <id>"
    assert guidance.blocker_summary_view[0]["recommended_next_action"] == "uv run python bot.py recovery-report --json"


def test_recovery_report_blocker_summary_view_for_recovery_required(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="summary_recovery_required", created_ts=now_ms - 5_000)

    report = _load_recovery_report()

    view = report["blocker_summary_view"]
    assert view
    assert view[0]["blocker"] == "STARTUP_SAFETY_GATE_BLOCKED"
    assert view[0]["reason_code"] == "SUBMIT_UNKNOWN_RECOVERY_REQUIRED"
    assert "recovery_required=1" in view[0]["evidence"]
    assert (
        view[0]["recommended_next_action"]
        == "uv run python bot.py recover-order --client-order-id <id>"
    )


def test_recovery_report_exposes_invalid_fill_price_recovery_reason(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="invalid_price_recovery",
        created_ts=now_ms - 5_000,
    )
    _set_last_error(
        client_order_id="invalid_price_recovery",
        last_error="recent fill has missing/invalid execution price; exchange_order_id=ex-sell-1; manual recovery required",
    )

    report = _load_recovery_report()
    rows = report["recovery_required_summary"]

    assert rows
    assert rows[0]["client_order_id"] == "invalid_price_recovery"
    assert "missing/invalid execution price" in rows[0]["last_error"]


def test_recovery_report_blocker_summary_view_for_persistent_halt(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    report = _load_recovery_report()

    view = report["blocker_summary_view"]
    assert view
    assert view[0]["blocker"] == "HALT_STATE_UNRESOLVED"
    assert "halt unresolved:" in view[0]["evidence"]
    assert view[0]["recommended_next_action"] == "uv run python bot.py restart-checklist"
def test_recovery_report_auto_clears_stale_initial_reconcile_halt(tmp_path):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []
    assert report["recent_halt_reason"] == "none"
    state = runtime_state.snapshot()
    assert state.halt_state_unresolved is False
    assert state.halt_reason_code is None


def test_recovery_report_auto_clears_stale_locked_post_trade_reconcile_halt_when_safe(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason=(
            "reconcile failed (OperationalError): database is locked"
        ),
        reason_code="POST_TRADE_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "recent_fill_applied": 1,
            "balance_split_mismatch_count": 0,
            "balance_split_mismatch_summary": "none",
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
            "startup_gate_blocked": 0,
            "source_conflict_halt": 0,
        },
    )
    runtime_state.refresh_open_order_health()

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []
    assert report["recent_halt_reason"] == "none"
    assert report["last_reconcile_summary"] != "none"
    assert "status=ok" in str(report["last_reconcile_summary"])
    assert "reason_code=RECENT_FILL_APPLIED" in str(report["last_reconcile_summary"])
    state = runtime_state.snapshot()
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False
    assert state.halt_reason_code is None


def test_recovery_report_can_resume_clean_state(tmp_path):
    _set_tmp_db(tmp_path)

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []


def test_recovery_report_can_resume_false_for_unresolved_recovery_state(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="resume_blocked_rr", created_ts=now_ms)

    report = _load_recovery_report()

    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]


def test_recovery_report_can_resume_false_for_risk_halt_with_non_flat_position(tmp_path):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.25, 1000000.0, 0.0, 0.25, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="kill switch engaged",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=False,
    )

    report = _load_recovery_report()

    assert report["can_resume"] is False
    assert "HALT_RISK_OPEN_POSITION" in report["resume_blockers"]


def test_recovery_report_can_resume_true_again_after_risk_halt_is_flat(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="kill switch engaged",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=False,
    )
    runtime_state.enable_trading()

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []


def test_recovery_report_and_restart_checklist_distinguish_harmless_dust_only_from_open_exposure(
    tmp_path,
):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="kill switch engaged",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=False,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_classification": "harmless_dust",
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "dust_broker_qty": 0.00009629,
            "dust_local_qty": 0.00009629,
            "dust_effective_flat": 1,
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
        },
    )

    report = _load_recovery_report()
    checklist = app_module._load_restart_safety_checklist()
    normalized_position_item = next(item for item in checklist if item[0] == "normalized position state")

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []
    assert report["runtime_readiness"]["fee_gap_incident"]["incident_kind"] == "none"
    assert report["runtime_readiness"]["fee_gap_incident"]["active_issue"] is False
    assert report["fee_gap_accounting_repair_preview"]["incident_kind"] == "none"
    assert report["dust_state"] == "harmless_dust"
    assert normalized_position_item[1] is True
    assert "terminal_state=dust_only" in normalized_position_item[2]
    assert "has_executable_exposure=0" in normalized_position_item[2]
    assert "has_dust_only_remainder=1" in normalized_position_item[2]
    assert "dust_resume_allowed=1" in normalized_position_item[2]


def test_resume_eligibility_allows_matched_harmless_dust_when_policy_marks_it_tracked_only(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is True
    assert blockers == []


def test_resume_eligibility_keeps_unresolved_open_order_block_even_when_dust_is_resume_safe(tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(
        status="NEW",
        client_order_id="open_dust_guard",
        created_ts=int(time.time() * 1000),
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000 min_notional_krw=5000.0",
            "remote_open_order_found": 1,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is False
    blocker_codes = [b.code for b in blockers]
    assert "STARTUP_SAFETY_GATE_BLOCKED" in blocker_codes
    assert "BLOCKING_DUST_REVIEW_REQUIRED" not in blocker_codes


def test_resume_eligibility_blocks_fee_gap_recovery_required_state(tmp_path):
    _set_tmp_db(tmp_path)
    object.__setattr__(settings, "MODE", "live")
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="FEE_GAP_RECOVERY_REQUIRED",
        metadata={
            "balance_split_mismatch_count": 0,
            "material_zero_fee_fill_count": 2,
            "fee_gap_adjustment_count": 1,
            "fee_gap_recovery_required": 1,
        },
    )

    eligible, blockers = evaluate_resume_eligibility()
    state = runtime_state.snapshot()

    assert eligible is False
    assert [b.code for b in blockers] == ["STARTUP_SAFETY_GATE_BLOCKED", "FEE_GAP_RECOVERY_REQUIRED"]
    assert [b.reason_code for b in blockers] == ["FEE_GAP_RECOVERY_REQUIRED", "FEE_GAP_RECOVERY_REQUIRED"]
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "FEE_GAP_RECOVERY_REQUIRED" in state.resume_gate_reason


def test_recovery_report_surfaces_fee_gap_contamination_as_distinct_blocker(tmp_path):
    _set_tmp_db(tmp_path)
    object.__setattr__(settings, "MODE", "live")
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="FEE_GAP_RECOVERY_REQUIRED",
        metadata={
            "balance_split_mismatch_count": 0,
            "material_zero_fee_fill_count": 2,
            "fee_gap_adjustment_count": 1,
            "fee_gap_recovery_required": 1,
        },
    )

    report = _load_recovery_report()

    assert report["resume_allowed"] is False
    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]
    assert "FEE_GAP_RECOVERY_REQUIRED" in report["resume_blocker_reason_codes"]
    assert report["primary_blocker_reason_code"] == "FEE_GAP_RECOVERY_REQUIRED"
    assert report["operator_next_action"] == "manual_fee_gap_recovery_required"
    assert report["resume_blocked_reason"] == "resume blocked by fee-related accounting inconsistency"
    assert report["runtime_readiness"]["fee_gap_incident"]["incident_kind"] == "active_fee_gap_unrepaired"
    assert report["runtime_readiness"]["fee_gap_incident"]["incident_scope"] == "active_blocking"
    assert report["runtime_readiness"]["fee_gap_incident"]["resolution_state"] == "unresolved"
    assert report["runtime_readiness"]["fee_gap_incident"]["active_issue"] is True
    assert report["fee_gap_accounting_repair_preview"]["incident_kind"] == "active_fee_gap_unrepaired"


def test_recovery_report_surfaces_position_authority_gap_as_distinct_blocker(tmp_path):
    _set_tmp_db(tmp_path)
    object.__setattr__(settings, "MODE", "live")

    conn = ensure_db()
    try:
        init_portfolio(conn)
        conn.execute(
            """
            UPDATE portfolio
            SET asset_qty=?, asset_available=?, asset_locked=0.0
            WHERE id=1
            """,
            (0.0008, 0.0008),
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 0,
            "dust_state": "no_dust",
            "dust_policy_reason": "no_dust_residual",
            "dust_broker_qty": 0.0008,
            "dust_local_qty": 0.0008,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5000.0,
            "dust_broker_qty_is_dust": 0,
            "dust_local_qty_is_dust": 0,
            "dust_qty_gap_small": 1,
        },
    )

    report = _load_recovery_report()

    assert report["resume_allowed"] is False
    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]
    assert "POSITION_AUTHORITY_RECOVERY_REQUIRED" in report["resume_blocker_reason_codes"]
    assert report["primary_blocker_reason_code"] == "POSITION_AUTHORITY_RECOVERY_REQUIRED"
    assert report["operator_next_action"] == "manual_position_authority_recovery_required"
    assert report["resume_blocked_reason"] == "resume blocked by missing lot authority"
    assert report["runtime_readiness"]["recovery_stage"] == "AUTHORITY_REBUILD_PENDING"
    assert report["runtime_readiness"]["fee_gap_incident"]["incident_kind"] == "none"
    assert report["runtime_readiness"]["fee_gap_incident"]["active_issue"] is False
    assert report["position_authority_rebuild_preview"]["needs_rebuild"] is True


def test_authority_rebuild_then_fee_gap_progression_is_staged_not_deadlocked(tmp_path):
    _set_tmp_db(tmp_path)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 2_000_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(client_order_id, exchange_order_id, status, side, price,
                               qty_req, qty_filled, created_ts, updated_ts)
            VALUES ('authority_gap_buy','ex-authority-gap','FILLED','BUY',NULL,0.01,0.0,1000,1000)
            """
        )
        apply_fill_and_trade(
            conn,
            client_order_id="authority_gap_buy",
            side="BUY",
            fill_id="authority-gap-fill",
            fill_ts=1100,
            price=100000000.0,
            qty=0.01,
            fee=500.0,
            allow_entry_decision_fallback=False,
        )
        conn.execute("DELETE FROM open_position_lots")
        conn.commit()
    finally:
        conn.close()

    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="FEE_GAP_RECOVERY_REQUIRED",
        metadata={
            "balance_split_mismatch_count": 0,
            "material_zero_fee_fill_count": 1,
            "material_zero_fee_fill_latest_ts": 1100,
            "fee_gap_adjustment_count": 1,
            "fee_gap_adjustment_total_krw": 500.0,
            "fee_gap_adjustment_latest_event_ts": 1200,
            "fee_gap_recovery_required": 1,
            "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
        },
    )

    conn = ensure_db()
    try:
        snapshot = compute_runtime_readiness_snapshot(conn)
        authority_preview = build_position_authority_rebuild_preview(conn)
        fee_gap_preview = app_module.build_fee_gap_accounting_repair_preview(conn)
    finally:
        conn.close()

    assert snapshot.recovery_stage == "AUTHORITY_REBUILD_PENDING"
    assert snapshot.blocker_categories == ("executable_authority",)
    assert authority_preview["safe_to_apply"] is True
    assert fee_gap_preview["needs_repair"] is True
    assert fee_gap_preview["safe_to_apply"] is False
    assert fee_gap_preview["blocked_by_authority_rebuild"] is True
    assert fee_gap_preview["next_required_action"] == "rebuild_position_authority"

    conn = ensure_db()
    try:
        result = apply_position_authority_rebuild(conn, note="test staged rebuild")
        conn.commit()
        post_snapshot = compute_runtime_readiness_snapshot(conn)
        post_fee_gap_preview = app_module.build_fee_gap_accounting_repair_preview(conn)
    finally:
        conn.close()

    assert result["repair"]["created"] is True
    assert int(result["lot_snapshot_after"]["open_lot_count"]) > 0
    assert post_snapshot.recovery_stage == "RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT"
    assert post_snapshot.resume_ready is True
    assert post_snapshot.resume_blockers == ()
    assert post_snapshot.blocker_categories == ("advisory_historical_debt",)
    assert post_snapshot.fee_gap_incident.incident_kind == "active_fee_gap_unrepaired"
    assert post_snapshot.fee_gap_incident.incident_scope == "active_advisory"
    assert post_snapshot.fee_gap_incident.resolution_state == "unresolved"
    assert post_snapshot.fee_gap_incident.policy.resume_blocking is False
    assert post_fee_gap_preview["blocked_by_authority_rebuild"] is False
    assert post_fee_gap_preview["incident_scope"] == "active_advisory"
    assert post_fee_gap_preview["repair_eligibility_state"] == "blocked_until_flattened"
    assert post_fee_gap_preview["resume_policy"] == "defer_for_open_position_management"
    assert post_fee_gap_preview["resume_blocking"] is False
    assert post_fee_gap_preview["closeout_blocking"] is True
    assert post_fee_gap_preview["next_required_action"] == "manage_open_position_until_flat_then_apply_fee_gap_repair"

    startup_reason = evaluate_startup_safety_gate()
    resume_allowed, resume_blockers = evaluate_resume_eligibility()

    assert startup_reason is None
    assert resume_allowed is True
    assert resume_blockers == []


def test_open_position_fee_gap_debt_is_consistent_across_operator_surfaces(tmp_path, monkeypatch, capsys):
    test_authority_rebuild_then_fee_gap_progression_is_staged_not_deadlocked(tmp_path)
    monkeypatch.setattr("bithumb_bot.app._safe_recent_broker_orders_snapshot", lambda limit=100: ([], None))
    monkeypatch.setattr(
        "bithumb_bot.app.build_broker_with_auth_diagnostics",
        lambda **_kwargs: (SimpleNamespace(get_accounts_validation_diagnostics=lambda: {}), {}),
    )

    report = _load_recovery_report()
    cmd_health()
    health_out = capsys.readouterr().out
    cmd_recovery_report()
    recovery_out = capsys.readouterr().out
    app_module.cmd_ops_report(limit=1)
    ops_out = capsys.readouterr().out
    cmd_restart_checklist()
    checklist_out = capsys.readouterr().out
    resume_allowed, resume_blockers = evaluate_resume_eligibility()

    assert report["runtime_readiness"]["recovery_stage"] == "RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT"
    assert report["recovery_blocker_categories"] == ["advisory_historical_debt"]
    assert report["resume_allowed"] is True
    assert report["can_resume"] is True
    assert report["operator_next_action"] == "resume_manage_open_position_then_repair_fee_gap_after_flatten"
    assert report["fee_gap_accounting_repair_preview"]["resume_policy"] == "defer_for_open_position_management"
    assert "recovery_stage=RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT" in health_out
    assert "fee_gap_accounting_repair_resume_blocking=0" in health_out
    assert "recovery_stage=RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT" in recovery_out
    assert "recovery_blocker_categories=advisory_historical_debt" in recovery_out
    assert "resume_policy=defer_for_open_position_management" in recovery_out
    assert "recovery_stage=RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT" in ops_out
    assert "PASS    normalized position state:" in checklist_out
    assert "PASS    fee-gap accounting repair:" in checklist_out
    assert "safe_to_resume=1" in checklist_out
    assert resume_allowed is True
    assert resume_blockers == []


def test_runtime_readiness_is_consistent_across_reports_for_authority_gap(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    object.__setattr__(settings, "MODE", "live")
    conn = ensure_db()
    try:
        init_portfolio(conn)
        conn.execute(
            """
            UPDATE portfolio
            SET asset_qty=0.0008, asset_available=0.0008, asset_locked=0.0
            WHERE id=1
            """
        )
        conn.commit()
        snapshot = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 0,
            "dust_policy_reason": "no_dust_residual",
        },
    )

    report = _load_recovery_report()
    cmd_health()
    health_out = capsys.readouterr().out
    app_module.cmd_ops_report(limit=1)
    ops_out = capsys.readouterr().out
    checklist = evaluate_restart_readiness()

    assert snapshot.recovery_stage == "AUTHORITY_REBUILD_PENDING"
    assert report["runtime_readiness"]["recovery_stage"] == "AUTHORITY_REBUILD_PENDING"
    assert "recovery_stage=AUTHORITY_REBUILD_PENDING" in health_out
    assert "recovery_stage=AUTHORITY_REBUILD_PENDING" in ops_out
    normalized_position_item = next(item for item in checklist if item[0] == "normalized position state")
    assert normalized_position_item[1] is False
    assert "terminal_state=open_exposure" in normalized_position_item[2]


def test_recovery_report_blocks_resume_now_when_dust_requires_operator_review(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "matched_harmless_dust_operator_review_required",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000",
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
            "submit_unknown_count": 1,
            "balance_split_mismatch_count": 0,
        },
    )

    report = _load_recovery_report()

    assert report["resume_allowed"] is False
    assert report["can_resume"] is False
    assert "HARMLESS_DUST_POLICY_REVIEW_REQUIRED" in report["resume_blockers"]
    assert "DUST_RESIDUAL_BLOCK" in report["resume_blocker_reason_codes"]
    assert report["operator_next_action"] != "resume_now"
    assert report["operator_next_action"] == "review_harmless_dust_policy"
    assert report["dust_state"] == "harmless_dust"
    assert report["dust_new_orders_allowed"] is False
    assert report["dust_resume_allowed_by_policy"] is False


def test_recovery_report_allows_resume_now_for_matched_harmless_dust_when_tracked_only(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    report = _load_recovery_report()

    assert report["resume_allowed"] is True
    assert report["can_resume"] is True
    assert report["resume_blockers"] == []
    assert report["operator_next_action"] == "resume_now"
    assert report["dust_state"] == "harmless_dust"
    assert report["dust_new_orders_allowed"] is True
    assert report["dust_resume_allowed_by_policy"] is True


def test_recovery_report_prioritizes_dangerous_dust_when_unresolved_order_also_blocks_resume(tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(
        status="NEW",
        client_order_id="open_dangerous_dust_report_guard",
        created_ts=int(time.time() * 1000),
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": (
                "broker_qty=0.00009900 local_qty=0.00001000 delta=0.00008900 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=0 "
                "classification=blocking_dust harmless_dust=0 broker_local_match=0 "
                "allow_resume=0 effective_flat=0 policy_reason=dangerous_dust_operator_review_required"
            ),
            "remote_open_order_found": 1,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    report = _load_recovery_report()

    assert report["dust_state"] == "blocking_dust"
    assert report["dust_broker_local_match"] is False
    assert report["dust_resume_allowed_by_policy"] is False
    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]
    assert "BLOCKING_DUST_REVIEW_REQUIRED" in report["resume_blockers"]
    assert "DUST_RESIDUAL_BLOCK" in report["resume_blocker_reason_codes"]
    assert report["operator_next_action"] == "manual_dust_review_required"


def test_recovery_report_keeps_effective_flat_dust_visible_when_unresolved_order_also_blocks_resume(tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(
        status="NEW",
        client_order_id="open_dust_report_guard",
        created_ts=int(time.time() * 1000),
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000",
            "remote_open_order_found": 1,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    report = _load_recovery_report()

    assert report["dust_state"] == "harmless_dust"
    assert report["dust_resume_allowed_by_policy"] is True
    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]
    assert "BLOCKING_DUST_REVIEW_REQUIRED" not in report["resume_blockers"]
    assert report["operator_next_action"] == "investigate_blockers"


def test_resume_eligibility_clears_stale_lock_halt_after_successful_reconcile_evidence(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="reconcile failed (OperationalError): database is locked",
        reason_code="POST_TRADE_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "recent_fill_applied": 1,
            "balance_split_mismatch_count": 0,
        },
    )
    runtime_state.refresh_open_order_health()

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is True
    assert blockers == []
    state = runtime_state.snapshot()
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False


def test_reconcile_recovery_report_and_resume_clear_terminal_recent_order_with_missing_price(
    tmp_path, monkeypatch, capsys
):
    _set_tmp_db(tmp_path)
    now_ms = 1775658600000
    conn = ensure_db()
    try:
        init_portfolio(conn)
        conn.execute(
            """
            UPDATE portfolio
            SET cash_krw=1000000.0,
                asset_qty=0.0001,
                cash_available=1000000.0,
                cash_locked=0.0,
                asset_available=0.0001,
                asset_locked=0.0
            WHERE id=1
            """
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, 'NEW', 'SELL', ?, ?, 0.0, ?, ?, NULL)
            """,
            (
                "live_1775658600000_sell_ae61703f",
                "C0101000002903202695",
                105950000.0,
                0.0001,
                now_ms,
                now_ms,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="BrokerRejectError: missing required numeric field 'price'",
        reason_code="LIVE_EXECUTION_BROKER_ERROR",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    broker = _RecoveryReportMissingPriceBroker()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    try:
        cmd_reconcile(broker_factory=lambda: broker, reconcile_fn=reconcile_with_broker)
        report = _load_recovery_report()
        state_after_report = runtime_state.snapshot()

        assert report["unresolved_count"] == 0
        assert report["recovery_required_count"] == 0
        assert report["can_resume"] is True
        assert report["resume_blockers"] == []
        assert "no local unresolved identifiers available for broker snapshot" in str(
            report["broker_recent_orders_snapshot_error"]
        )
        assert state_after_report.halt_reason_code is None
        assert state_after_report.halt_state_unresolved is False
        assert state_after_report.resume_gate_reason is None

        cmd_resume(force=False, broker_factory=lambda: broker, reconcile_fn=reconcile_with_broker)
        out = capsys.readouterr().out
        state_after_resume = runtime_state.snapshot()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert "[RECONCILE] completed one live reconciliation pass" in out
    assert "[RESUME] trading enabled" in out
    assert state_after_resume.trading_enabled is True
    assert state_after_resume.halt_reason_code is None
    assert state_after_resume.resume_gate_reason is None


def test_reconcile_skips_in_non_live_mode(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "paper")
    try:
        cmd_reconcile()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[RECONCILE] skipped" in out


def test_reconcile_live_accepts_injected_dependencies(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    broker = object()
    calls: list[object] = []

    def _broker_factory():
        calls.append("factory")
        return broker

    def _reconcile(candidate):
        calls.append(("reconcile", candidate))

    try:
        cmd_reconcile(broker_factory=_broker_factory, reconcile_fn=_reconcile)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[RECONCILE] completed one live reconciliation pass" in out
    assert calls == ["factory", ("reconcile", broker)]


def test_reconcile_live_updates_state_and_reports_contract(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    broker = object()
    calls: list[object] = []

    def _broker_factory():
        calls.append("factory")
        return broker

    def _reconcile(candidate):
        calls.append(("reconcile", candidate))
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "remote_open_order_found": 0,
                "balance_split_mismatch_count": 0,
            },
        )

    try:
        cmd_reconcile(broker_factory=_broker_factory, reconcile_fn=_reconcile)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    state = runtime_state.snapshot()

    assert "[RECONCILE] precondition=" in out
    assert "[RECONCILE] completed one live reconciliation pass" in out
    assert "postcondition=last_reconcile_status=ok" in out
    assert calls == ["factory", ("reconcile", broker)]
    assert state.last_reconcile_status == "ok"
    assert state.last_reconcile_reason_code == "RECONCILE_OK"


def test_reconcile_live_command_reports_resume_gate_contract(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    broker = object()

    def _broker_factory():
        return broker

    def _reconcile(candidate):
        assert candidate is broker
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "remote_open_order_found": 0,
                "balance_split_mismatch_count": 0,
            },
        )

    try:
        cmd_reconcile(broker_factory=_broker_factory, reconcile_fn=_reconcile)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    state = runtime_state.snapshot()

    assert "[RECONCILE] warning=" in out
    assert "resume_allowed=1" in out
    assert "resume_blockers=none" in out
    assert "resume_blocker_reason_codes=none" in out
    assert "unresolved_open_order_count=0" in out
    assert "recovery_required_count=0" in out
    assert state.last_reconcile_status == "ok"
    assert state.last_reconcile_reason_code == "RECONCILE_OK"
    assert state.resume_gate_blocked is False


def test_recover_order_success_for_known_exchange_order_id(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db()
    try:
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
    finally:
        conn.close()
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )

    try:
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_manual_1",
            confirm=True,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT status, exchange_order_id, qty_filled
            FROM orders
            WHERE client_order_id='needs_recovery'
            """
        ).fetchone()
        fills = conn.execute(
            """
            SELECT fill_id, qty, fee
            FROM fills
            WHERE client_order_id='needs_recovery'
            """
        ).fetchall()
        trades = conn.execute(
            "SELECT side, qty, fee FROM trades WHERE note LIKE 'manual recovery%'"
        ).fetchall()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex_manual_1"
    assert row["qty_filled"] == pytest.approx(0.01)
    assert len(fills) == 1
    assert fills[0]["fill_id"] == "recover_fill_1"
    assert fills[0]["qty"] == pytest.approx(0.01)
    assert len(trades) == 1
    assert trades[0]["side"] == "BUY"


def test_recover_order_failure_keeps_recovery_required_and_exits_non_zero(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverAmbiguousBroker(),
    )

    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="needs_recovery",
                exchange_order_id="ex_manual_2",
                confirm=True,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    assert exc.value.code == 1

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT status, exchange_order_id, last_error
            FROM orders
            WHERE client_order_id='needs_recovery'
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] == "ex_manual_2"
    assert "manual recovery failed" in str(row["last_error"])


def test_recover_order_dry_run_prints_preview_and_makes_no_changes(capsys, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    try:
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_preview_1",
            dry_run=True,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[RECOVER-ORDER] preview" in out
    assert "target_order_id=needs_recovery exchange_order_id=ex_preview_1" in out
    assert "current_known_state=status=RECOVERY_REQUIRED" in out
    assert "proposed_recovery_action=manual_recover_with_exchange_id" in out
    assert "[RECOVER-ORDER] dry-run: no changes applied" in out

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='needs_recovery'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None


def test_recover_order_requires_explicit_confirmation(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker_calls = {"n": 0}

    def _unexpected_broker():
        broker_calls["n"] += 1
        return _RecoverSuccessBroker()

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _unexpected_broker)
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="needs_recovery",
                exchange_order_id="ex_confirm_needed",
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1
    assert broker_calls["n"] == 0


def test_recover_order_refuses_when_order_not_recovery_required(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="FILLED",
        client_order_id="already_filled",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker_calls = {"n": 0}

    def _unexpected_broker():
        broker_calls["n"] += 1
        return _RecoverSuccessBroker()

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _unexpected_broker)
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="already_filled",
                exchange_order_id="ex_should_refuse",
                confirm=True,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1
    assert broker_calls["n"] == 0


def test_recover_order_allows_new_unresolved_when_single_high_confidence_terminal_match(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="new_unresolved_recoverable",
        created_ts=now_ms,
        price=100.0,
    )

    conn = ensure_db()
    try:
        set_exchange_order_id("new_unresolved_recoverable", "ex-unresolved-1", conn=conn)
        conn.commit()
    finally:
        conn.close()

    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    broker = _RecoverUnresolvedHighConfidenceTerminalBroker(
        recent_orders=[
            BrokerOrder(
                "new_unresolved_recoverable",
                "ex-unresolved-1",
                "BUY",
                "FILLED",
                100.0,
                0.01,
                0.01,
                now_ms,
                now_ms,
            )
        ],
        remote_status="FILLED",
    )

    try:
        cmd_recover_order(
            client_order_id="new_unresolved_recoverable",
            exchange_order_id="ex-unresolved-1",
            confirm=True,
            broker_factory=lambda: broker,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='new_unresolved_recoverable'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex-unresolved-1"


def test_recover_order_refuses_when_unresolved_has_multiple_high_confidence_candidates(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="new_unresolved_ambiguous",
        created_ts=now_ms,
        price=100.0,
    )

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker = _RecoverUnresolvedHighConfidenceTerminalBroker(
        recent_orders=[
            BrokerOrder("new_unresolved_ambiguous", "ex-a", "BUY", "FILLED", 100.0, 0.01, 0.01, now_ms, now_ms),
            BrokerOrder("new_unresolved_ambiguous", "ex-b", "BUY", "FILLED", 100.0, 0.01, 0.01, now_ms, now_ms),
        ],
        remote_status="FILLED",
    )
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="new_unresolved_ambiguous",
                exchange_order_id="ex-a",
                confirm=True,
                broker_factory=lambda: broker,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1


def test_recover_order_refuses_when_unresolved_candidate_is_not_terminal(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="new_unresolved_non_terminal",
        created_ts=now_ms,
        price=100.0,
    )
    conn = ensure_db()
    try:
        set_exchange_order_id("new_unresolved_non_terminal", "ex-non-terminal", conn=conn)
        conn.commit()
    finally:
        conn.close()

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker = _RecoverUnresolvedHighConfidenceTerminalBroker(
        recent_orders=[
            BrokerOrder(
                "new_unresolved_non_terminal",
                "ex-non-terminal",
                "BUY",
                "NEW",
                100.0,
                0.01,
                0.0,
                now_ms,
                now_ms,
            )
        ],
        remote_status="NEW",
    )
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="new_unresolved_non_terminal",
                exchange_order_id="ex-non-terminal",
                confirm=True,
                broker_factory=lambda: broker,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1


def test_recover_order_does_not_auto_resume_trading(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db()
    try:
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
    finally:
        conn.close()
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )

    runtime_state.enable_trading()
    try:
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_manual_3",
            confirm=True,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")


def test_resume_succeeds_after_manual_recovery_clears_recovery_required(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db()
    try:
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
    finally:
        conn.close()
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )

    try:
        runtime_state.disable_trading_until(float("inf"), reason="startup recovery gate")
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_manual_4",
            confirm=True,
        )
        cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    report = _load_recovery_report()
    state = runtime_state.snapshot()
    assert int(report["recovery_required_count"]) == 0
    assert int(report["unresolved_count"]) == 0
    assert state.trading_enabled is True


def test_halt_resume_flow_requires_manual_recover_order_before_resume(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="halt_resume_recovery",
        created_ts=now_ms,
    )

    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): broker timeout",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit):
        cmd_resume(force=False)

    state_blocked = runtime_state.snapshot()
    assert state_blocked.halt_state_unresolved is True

    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db()
    try:
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
    finally:
        conn.close()
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )
    try:
        cmd_recover_order(
            client_order_id="halt_resume_recovery",
            exchange_order_id="ex_halt_resume_1",
            confirm=True,
        )
        cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    state_after = runtime_state.snapshot()
    assert state_after.halt_state_unresolved is False
    assert state_after.trading_enabled is True


def test_cmd_run_notifies_run_lock_conflict(monkeypatch):
    from bithumb_bot.app import cmd_run
    from bithumb_bot.run_lock import RunLockError

    notifications: list[str] = []
    run_loop_calls = {"n": 0}
    monkeypatch.setattr("bithumb_bot.app.notify", lambda msg: notifications.append(msg))
    monkeypatch.setattr(
        "bithumb_bot.engine.run_loop",
        lambda *_args, **_kwargs: run_loop_calls.__setitem__(
            "n", run_loop_calls["n"] + 1
        ),
    )

    class _RaiseOnEnter:
        def __enter__(self):
            raise RunLockError("another bot run loop is already running")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("bithumb_bot.run_lock.acquire_run_lock", lambda *_args, **_kwargs: _RaiseOnEnter())

    with pytest.raises(SystemExit) as exc:
        cmd_run(5, 20)

    assert exc.value.code == 1
    assert run_loop_calls["n"] == 0
    assert any("event=run_lock_conflict" in n for n in notifications)
    assert any("reason_code=RUN_LOCK_CONFLICT" in n for n in notifications)
    assert any("client_order_id=-" in n for n in notifications)
    assert any("submit_attempt_id=-" in n for n in notifications)
    assert any("exchange_order_id=-" in n for n in notifications)


def test_cmd_run_blocks_before_lock_when_live_preflight_fails(monkeypatch):
    from bithumb_bot.app import cmd_run

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.app.notify", lambda msg: notifications.append(msg))

    def _raise_preflight(_cfg):
        raise app_module.LiveModeValidationError("live startup guard failed")

    def _fail_lock(*_args, **_kwargs):
        raise AssertionError("run lock must not be acquired when live preflight fails")

    monkeypatch.setattr("bithumb_bot.app.validate_live_run_startup_contract", _raise_preflight)
    monkeypatch.setattr("bithumb_bot.run_lock.acquire_run_lock", _fail_lock)

    with pytest.raises(SystemExit) as exc:
        cmd_run(5, 20)

    assert exc.value.code == 1
    assert any("event=startup_gate_blocked" in n for n in notifications)
    assert any("reason_code=LIVE_STARTUP_GUARD" in n for n in notifications)


def test_main_pre_dispatch_blocks_live_run_without_startup_contract(monkeypatch, capsys):
    object.__setattr__(settings, "MODE", "live")
    notifications: list[str] = []
    calls = {"startup": 0}

    def _raise_startup(_cfg):
        calls["startup"] += 1
        raise app_module.LiveModeValidationError("central startup guard failed")

    monkeypatch.setattr(app_module, "validate_live_run_startup_contract", _raise_startup)
    monkeypatch.setattr(app_module, "log_live_execution_contract", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(app_module, "notify", lambda msg: notifications.append(msg))
    monkeypatch.setattr(app_module, "cmd_run", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("cmd_run bypassed guard")))

    with pytest.raises(SystemExit) as exc:
        app_module.main(["run"])

    out = capsys.readouterr().out
    assert exc.value.code == 1
    assert calls == {"startup": 1}
    assert "[LIVE-COMMAND-GUARD]" in out
    assert any("reason_code=LIVE_STARTUP_GUARD" in n for n in notifications)


def test_main_pre_dispatch_blocks_live_write_command_without_preflight(monkeypatch, capsys):
    object.__setattr__(settings, "MODE", "live")
    calls = {"preflight": 0}

    def _raise_preflight(_cfg):
        calls["preflight"] += 1
        raise app_module.LiveModeValidationError("central preflight failed")

    monkeypatch.setattr(app_module, "validate_live_mode_preflight", _raise_preflight)
    monkeypatch.setattr(app_module, "log_live_execution_contract", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(app_module, "cmd_panic_stop", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("panic-stop bypassed guard")))

    with pytest.raises(SystemExit) as exc:
        app_module.main(["panic-stop"])

    out = capsys.readouterr().out
    assert exc.value.code == 1
    assert calls == {"preflight": 1}
    assert "[LIVE-COMMAND-GUARD]" in out


def test_restart_checklist_blocks_when_restart_risks_exist(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    _insert_order(status="NEW", client_order_id="open_order", created_ts=now_ms)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.25, 1000000.0, 0.0, 0.25, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=False,
        error="timeout",
        reason_code="RECONCILE_TIMEOUT",
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    cmd_restart_checklist()
    out = capsys.readouterr().out

    assert "[RESTART-SAFETY-CHECKLIST]" in out
    assert "BLOCKED unresolved/recovery-required orders" in out
    assert "BLOCKED open orders" in out
    assert "BLOCKED normalized position state" in out
    assert "terminal_state=open_exposure" in out
    assert "BLOCKED halt state" in out
    assert "BLOCKED last reconcile" in out
    assert "safe_to_resume=0" in out


def test_evaluate_restart_readiness_uses_lot_native_dust_authority(tmp_path):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_classification": "harmless_dust",
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "dust_broker_qty": 0.00009629,
            "dust_local_qty": 0.00009629,
            "dust_effective_flat": 1,
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
        },
    )

    checklist = evaluate_restart_readiness()
    normalized_position_item = next(item for item in checklist if item[0] == "normalized position state")
    halt_item = next(item for item in checklist if item[0] == "halt state")

    assert normalized_position_item[1] is True
    assert "terminal_state=dust_only" in normalized_position_item[2]
    assert "has_executable_exposure=0" in normalized_position_item[2]
    assert halt_item[1] is True


def test_restart_checklist_auto_clears_stale_initial_reconcile_halt(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    cmd_restart_checklist()
    out = capsys.readouterr().out

    assert "PASS    halt state" in out
    assert "safe_to_resume=1" in out


def test_restart_checklist_passes_when_safe_to_resume(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()

    cmd_restart_checklist()
    out = capsys.readouterr().out

    assert "PASS    unresolved/recovery-required orders" in out
    assert "PASS    open orders" in out
    assert "PASS    normalized position state" in out
    assert "PASS    halt state" in out
    assert "PASS    last reconcile" in out
    assert "safe_to_resume=1" in out


def test_restart_checklist_scopes_safe_to_resume_when_sub_min_tracked_dust_allows_entry(
    tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    conn = ensure_db()
    try:
        init_portfolio(conn)
        residual_qty = 0.00019996
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, ?, 1000000.0, 0.0, ?, 0.0)
            """,
            (residual_qty, residual_qty),
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                1,
                "live_1776602640000_buy_ae184595",
                "incident-buy-fill",
                1_776_602_640_000,
                7_050_000.0,
                residual_qty,
                0,
                1,
                1,
                0.0004,
                0.0002,
                0.0001,
                0.0,
                8,
                "ledger",
                "lot-native",
                "dust_tracking",
                0.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 0,
            "dust_state": "no_dust",
            "dust_policy_reason": "no_dust_residual",
        },
        now_epoch_sec=1000.0,
    )

    cmd_restart_checklist()
    out = capsys.readouterr().out

    assert "safe_to_resume=1" in out
    assert "resume_scope=process_loop_only" in out
    assert "run_loop_allowed=1 trading_allowed=1 new_entry_allowed=1 closeout_allowed=0" in out
    assert "operator_action_required=0" in out
    assert "canonical_state=DUST_ONLY_TRACKED residual_class=HARMLESS_DUST_TREAT_AS_FLAT" in out
    assert "trading_block_reason=closeout_blocked:dust_only_remainder" in out
    assert "authoritative internal lot boundary" in out


def test_repair_completed_with_no_blockers_auto_clears_pause(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path, monkeypatch)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"balance_split_mismatch_count": 0},
    )

    auto_cleared = app_module._finalize_repair_runtime_policy(
        reason_code="FEE_PENDING_ACCOUNTING_REPAIR_COMPLETED",
        metadata={
            "fee_pending_auto_recovering": 0,
            "fee_pending_fill_count": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    state = runtime_state.snapshot()
    report = _load_recovery_report()

    assert auto_cleared is True
    assert state.trading_enabled is True
    assert report["can_resume"] is True
    assert report["auto_recovery_count"] == 0


def test_audit_fails_on_terminal_order_with_pending_local_intent_state(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    conn = ensure_db()
    try:
        record_order_if_missing(
            conn,
            client_order_id="audit_bad_terminal",
            side="BUY",
            qty_req=0.01,
            price=100.0,
            ts_ms=1,
            status="FILLED",
            local_intent_state="PENDING_SUBMIT",
        )
        conn.execute(
            "UPDATE orders SET qty_filled=? WHERE client_order_id='audit_bad_terminal'",
            (0.01,),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(SystemExit) as exc:
        app_main(["audit"])

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "terminal order retained pending local intent state" in out


class _FlattenBrokerSuccess:
    def __init__(self):
        self.calls: list[dict[str, str | float | None]] = []
        self.balance = BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=10.0, asset_locked=0.0)

    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract=None,
        submit_plan=None,
    ):
        assert submit_plan is not None
        self.calls.append({"client_order_id": client_order_id, "side": side, "qty": qty, "price": price})

        class _Order:
            exchange_order_id = "ex-flat-1"
            status = "NEW"

        return _Order()

    def get_balance(self) -> BrokerBalance:
        return self.balance


def _stub_flatten_submit_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    rules = order_rules.DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        order_types=("limit", "price", "market"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: SimpleNamespace(rules=rules),
    )


def test_flatten_position_no_position_safe_noop(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    cmd_flatten_position(dry_run=False)
    out = capsys.readouterr().out

    assert "no position to flatten" in out
    state = runtime_state.snapshot()
    assert state.last_flatten_position_status == "no_position"
    assert state.last_flatten_position_summary is not None
    assert '"status": "no_position"' in state.last_flatten_position_summary


def test_flatten_position_dust_only_remainder_is_not_treated_as_executable_position(
    monkeypatch,
    tmp_path,
    capsys,
):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_classification": "harmless_dust",
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "dust_broker_qty": 0.00009629,
            "dust_local_qty": 0.00009629,
            "dust_effective_flat": 1,
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
        },
    )

    broker = _FlattenBrokerSuccess()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    cmd_flatten_position(dry_run=False)
    out = capsys.readouterr().out

    assert "no position to flatten" in out
    assert broker.calls == []
    state = runtime_state.snapshot()
    assert state.last_flatten_position_status == "no_position"
    assert state.last_flatten_position_summary is not None
    assert '"terminal_state": "dust_only"' in state.last_flatten_position_summary
    assert '"raw_total_asset_qty": 9.629e-05' in state.last_flatten_position_summary or '"raw_total_asset_qty": 0.00009629' in state.last_flatten_position_summary
    assert '"executable_exposure_qty": 0.0' in state.last_flatten_position_summary
    assert '"tracked_dust_qty": 9.629e-05' in state.last_flatten_position_summary or '"tracked_dust_qty": 0.00009629' in state.last_flatten_position_summary


def test_flatten_position_recorded_buy_below_effective_min_qty_is_normal_noop(
    monkeypatch,
    tmp_path,
    capsys,
):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    buy_qty = 0.00009629
    conn = ensure_db()
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="entry_below_min",
            side="BUY",
            qty_req=buy_qty,
            submit_attempt_id="attempt_entry_below_min",
            price=100_000_000.0,
            ts_ms=1_700_000_000_000,
            status="FILLED",
        )
        trade = apply_fill_and_trade(
            conn,
            client_order_id="entry_below_min",
            side="BUY",
            fill_id="fill_entry_below_min",
            fill_ts=1_700_000_000_000,
            price=100_000_000.0,
            qty=buy_qty,
            fee=0.0,
            strategy_name="sma_with_filter",
            entry_decision_id=101,
        )
        conn.execute(
            """
            UPDATE portfolio
            SET cash_available=cash_krw, asset_available=asset_qty, asset_locked=0.0
            WHERE id=1
            """
        )
        conn.commit()
        sell_orders_before = conn.execute("SELECT COUNT(*) AS n FROM orders WHERE side='SELL'").fetchone()
    finally:
        conn.close()

    assert trade is not None
    assert sell_orders_before is not None
    assert int(sell_orders_before["n"]) == 0

    broker = _FlattenBrokerSuccess()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)
    monkeypatch.setattr(
        "bithumb_bot.flatten.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    cmd_flatten_position(dry_run=False)
    out = capsys.readouterr().out

    conn = ensure_db()
    try:
        sell_orders_after = conn.execute("SELECT COUNT(*) AS n FROM orders WHERE side='SELL'").fetchone()
    finally:
        conn.close()

    assert "no position to flatten" in out
    assert broker.calls == []
    assert sell_orders_after is not None
    assert int(sell_orders_after["n"]) == 0
    state = runtime_state.snapshot()
    assert state.last_flatten_position_status == "no_position"
    assert state.last_flatten_position_summary is not None
    assert '"status": "no_position"' in state.last_flatten_position_summary
    assert '"terminal_state": "dust_only"' in state.last_flatten_position_summary
    assert '"executable_exposure_qty": 0.0' in state.last_flatten_position_summary
    assert '"tracked_dust_qty": 9.629e-05' in state.last_flatten_position_summary or '"tracked_dust_qty": 0.00009629' in state.last_flatten_position_summary


def test_flatten_position_submits_sell_when_position_exists(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    _stub_flatten_submit_rules(monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    prev_step = settings.LIVE_ORDER_QTY_STEP
    prev_max_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.000001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 6)
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.12345678, 1000000.0, 0.0, 0.12345678, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.12345678, 1234, 0, "lot-native", "open_exposure"),
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FlattenBrokerSuccess()

    class _BrokerFactory:
        def __call__(self):
            return broker

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _BrokerFactory())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    try:
        cmd_flatten_position(dry_run=False)
        out = capsys.readouterr().out

        assert "submitted" in out
        assert len(broker.calls) == 1
        assert broker.calls[0]["side"] == "SELL"
        assert abs(float(broker.calls[0]["qty"]) - 0.123456) < 1e-12
        conn = ensure_db()
        try:
            flatten_order = conn.execute(
                """
                SELECT client_order_id, exchange_order_id, status, side, qty_req, strategy_name
                FROM orders
                WHERE client_order_id LIKE 'flatten_%'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            assert flatten_order is not None
            assert flatten_order["exchange_order_id"] == "ex-flat-1"
            assert flatten_order["status"] == "NEW"
            assert flatten_order["side"] == "SELL"
            assert flatten_order["strategy_name"] == "operator_flatten"
            assert abs(float(flatten_order["qty_req"]) - 0.123456) < 1e-12
            event_rows = conn.execute(
                """
                SELECT event_type, submit_phase, broker_response_summary, exchange_order_id_obtained
                FROM order_events
                WHERE client_order_id=?
                ORDER BY id
                """,
                (flatten_order["client_order_id"],),
            ).fetchall()
        finally:
            conn.close()
        event_types = {str(row["event_type"]) for row in event_rows}
        assert {"intent_created", "submit_started", "submit_attempt_preflight", "submit_attempt_acknowledged"} <= event_types
        assert any(str(row["submit_phase"]) == "operator_pre_submit" for row in event_rows)
        assert any(int(row["exchange_order_id_obtained"] or 0) == 1 for row in event_rows)
        state = runtime_state.snapshot()
        assert state.last_flatten_position_status == "submitted"
    finally:
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", prev_step)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", prev_max_decimals)


@pytest.mark.lot_native_regression_gate
def test_flatten_position_qty_only_portfolio_does_not_restore_sell_authority(
    monkeypatch,
    tmp_path,
    capsys,
):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.12345678, 1000000.0, 0.0, 0.12345678, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FlattenBrokerSuccess()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    cmd_flatten_position(dry_run=False)
    out = capsys.readouterr().out

    assert "no position to flatten" in out
    assert broker.calls == []
    state = runtime_state.snapshot()
    assert state.last_flatten_position_status == "no_position"
    assert state.last_flatten_position_summary is not None
    assert '"raw_total_asset_qty": 0.12345678' in state.last_flatten_position_summary
    assert '"executable_exposure_qty": 0.0' in state.last_flatten_position_summary


@pytest.mark.lot_native_regression_gate
def test_flatten_sell_authority_boundary_reads_canonical_snapshot_without_local_override(monkeypatch) -> None:
    from bithumb_bot.flatten import _resolve_flatten_sell_authority

    canonical_exposure = SimpleNamespace(
        sellable_executable_lot_count=2,
        reserved_exit_lot_count=0,
        exit_allowed=True,
        exit_block_reason="none",
    )

    monkeypatch.setattr(
        "bithumb_bot.flatten.resolve_canonical_position_exposure_snapshot",
        lambda _payload: canonical_exposure,
    )

    class _DummyPositionState:
        def as_dict(self) -> dict[str, object]:
            return {"normalized_exposure": {"sellable_executable_lot_count": 0}}

    resolved_exposure, sellable_lot_count, exit_allowed, exit_block_reason = _resolve_flatten_sell_authority(
        position_state=_DummyPositionState(),
    )

    assert resolved_exposure is canonical_exposure
    assert sellable_lot_count == 2
    assert exit_allowed is True
    assert exit_block_reason == "none"


@pytest.mark.lot_native_regression_gate
def test_flatten_position_reserved_exit_qty_does_not_bypass_canonical_sell_authority(
    monkeypatch,
    tmp_path,
    capsys,
):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.0002, 1000000.0, 0.0, 0.0002, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0002, 2, 0, "lot-native", "open_exposure"),
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "reserved-exit-1",
                "ex-reserved-exit-1",
                "NEW",
                "SELL",
                100_000_000.0,
                0.0002,
                0.0,
                1_700_000_000_000,
                1_700_000_000_000,
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FlattenBrokerSuccess()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)
    monkeypatch.setattr(
        "bithumb_bot.flatten.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)
    out = capsys.readouterr().out

    assert exc.value.code == 1
    assert "blocked" in out
    assert "reason=unresolved_orders_present" in out
    assert broker.calls == []


def test_flatten_position_submit_failure_persisted(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    _stub_flatten_submit_rules(monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.01, 1000000.0, 0.0, 0.01, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "flatten_submit_failure", 1_700_000_000_000, 100_000_000.0, 0.01, 1, 0, "lot-native", "open_exposure"),
        )
        conn.commit()
    finally:
        conn.close()

    class _FailBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=10.0, asset_locked=0.0)

        def place_order(
            self,
            *,
            client_order_id: str,
            side: str,
            qty: float,
            price: float | None = None,
            buy_price_none_submit_contract=None,
            submit_plan=None,
        ):
            raise RuntimeError("submit boom")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _FailBroker())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed" in out
    state = runtime_state.snapshot()
    assert state.last_flatten_position_status == "failed"
    assert state.last_flatten_position_summary is not None
    assert "submit boom" in state.last_flatten_position_summary
    conn = ensure_db()
    try:
        flatten_order = conn.execute(
            """
            SELECT client_order_id, status, side, last_error
            FROM orders
            WHERE client_order_id LIKE 'flatten_%'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        assert flatten_order is not None
        assert flatten_order["status"] == "SUBMIT_UNKNOWN"
        assert flatten_order["side"] == "SELL"
        assert "operator flatten submit outcome unknown" in str(flatten_order["last_error"])
        event_types = {
            str(row["event_type"])
            for row in conn.execute(
                "SELECT event_type FROM order_events WHERE client_order_id=?",
                (flatten_order["client_order_id"],),
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"intent_created", "submit_started", "submit_attempt_preflight", "status_transition", "submit_timeout"} <= event_types


def test_flatten_position_validation_failure_blocks_submission(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.015, 1000000.0, 0.0, 0.015, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "flatten_validation_failure", 1_700_000_000_000, 100_000_000.0, 0.015, 1, 0, "lot-native", "open_exposure"),
        )
        conn.commit()
    finally:
        conn.close()

    class _LowAssetBroker:
        def __init__(self):
            self.place_order_calls = 0

        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)

        def place_order(
            self,
            *,
            client_order_id: str,
            side: str,
            qty: float,
            price: float | None = None,
            buy_price_none_submit_contract=None,
            submit_plan=None,
        ):
            self.place_order_calls += 1
            raise AssertionError("place_order must not be called when pretrade fails")

    broker = _LowAssetBroker()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed: ValueError: insufficient available asset" in out
    assert broker.place_order_calls == 0


def test_flatten_position_blocks_on_invalid_best_quote(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.015, 1000000.0, 0.0, 0.015, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (settings.PAIR, 1, "flatten_invalid_quote", 1_700_000_000_000, 100_000_000.0, 0.015, 1, 0, "lot-native", "open_exposure"),
        )
        conn.commit()
    finally:
        conn.close()

    class _NoSubmitBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=10.0, asset_locked=0.0)

        def place_order(
            self,
            *,
            client_order_id: str,
            side: str,
            qty: float,
            price: float | None = None,
            buy_price_none_submit_contract=None,
            submit_plan=None,
        ):
            raise AssertionError("place_order must not be called when best quote is invalid")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _NoSubmitBroker())
    monkeypatch.setattr(
        "bithumb_bot.flatten.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_010_000.0, ask_price=100_000_000.0),
    )

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed: RuntimeError: orderbook top invalid quote" in out


def test_flatten_position_blocks_on_live_preflight_failure(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")

    def _raise_preflight(_cfg):
        raise app_module.LiveModeValidationError("preflight boom")

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", _raise_preflight)

    class _BrokerFactory:
        def __call__(self):
            raise AssertionError("broker should not be constructed when preflight fails")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _BrokerFactory())

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed: live mode preflight" not in out
    assert "failed: preflight boom" in out


def test_flatten_position_blocks_when_live_unarmed(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)

    def _armed_gate(_cfg):
        raise app_module.LiveModeValidationError("LIVE_REAL_ORDER_ARMED=true is required")

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", _armed_gate)

    class _BrokerFactory:
        def __call__(self):
            raise AssertionError("broker should not be constructed when live mode is unarmed")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _BrokerFactory())

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "LIVE_REAL_ORDER_ARMED=true is required" in out


def test_flatten_position_blocks_when_unapplied_principal_pending(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.00059998, 1000000.0, 0.0, 0.00059998, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                1,
                "incident_open_buy",
                1_777_104_360_321,
                115_465_000.0,
                0.00059998,
                1,
                0,
                "lot-native",
                "open_exposure",
            ),
        )
        conn.execute(
            """
            INSERT INTO broker_fill_observations(
                event_ts, client_order_id, exchange_order_id, fill_id, fill_ts, side,
                price, qty, fee, fee_status, accounting_status, source, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1_777_104_360_500,
                "live_1777104360000_buy_aee4c564",
                "C0101000002949768709",
                "C0101000000983820316",
                1_777_104_360_321,
                "BUY",
                115_465_000.0,
                0.00059998,
                27.71,
                "order_level_candidate",
                "fee_pending",
                "incident_fixture_fee_pending",
                '{"fixture":"incident_shape_unapplied_principal"}',
            ),
        )
        conn.commit()
    finally:
        conn.close()

    class _NoSubmitBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=10.0, asset_locked=0.0)

        def place_order(self, **_kwargs):
            raise AssertionError("flatten must not submit while principal is unapplied")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _NoSubmitBroker())

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "blocked" in out
    assert "reason=unapplied_principal_pending" in out
    assert "recommended_command=uv run python bot.py recovery-report" in out


def test_resume_blocked_when_emergency_flatten_unresolved(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_flatten_position_result(
        status="failed",
        summary={"status": "failed", "error": "submit boom", "trigger": "position-loss-halt"},
    )

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())
    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", lambda broker: None)

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    assert exc.value.code == 1
    report = _load_recovery_report()
    assert report["can_resume"] is False
    assert "EMERGENCY_FLATTEN_UNRESOLVED" in report["resume_blockers"]


def test_health_and_recovery_report_expose_emergency_flatten_blocker(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_flatten_position_result(
        status="failed",
        summary={"status": "failed", "error": "submit boom", "trigger": "kill-switch"},
    )

    cmd_health()
    health_out = capsys.readouterr().out
    assert "emergency_flatten_blocked=True" in health_out
    assert "emergency_flatten_block_reason=emergency flatten unresolved" in health_out
    assert "blockers=STARTUP_SAFETY_GATE_BLOCKED, EMERGENCY_FLATTEN_UNRESOLVED" in health_out
    assert "blocker_reason_codes=" in health_out
    assert "EMERGENCY_FLATTEN_UNRESOLVED" in health_out

    cmd_recovery_report(as_json=False)
    report_out = capsys.readouterr().out
    assert "emergency_flatten_blocked=1" in report_out
    assert "emergency_flatten_block_reason=emergency flatten unresolved" in report_out
    assert "EMERGENCY_FLATTEN_UNRESOLVED" in report_out


def test_health_recovery_report_and_restart_checklist_expose_fee_rate_drift(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    runtime_state.enable_trading()
    monkeypatch.setattr("bithumb_bot.app.write_json_atomic", lambda *_args, **_kwargs: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO broker_fill_observations(
                event_ts, client_order_id, exchange_order_id, fill_id, fill_ts, side,
                price, qty, fee, fee_status, fee_source, fee_confidence, accounting_status, source,
                fee_provenance, fee_validation_reason, fee_validation_checks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1_777_104_360_500,
                "live_1777104360000_buy_aee4c564",
                "C0101000002949768709",
                "C0101000000983820316",
                1_777_104_360_321,
                "BUY",
                115_465_000.0,
                0.00059998,
                27.71,
                "validated_order_level_paid_fee",
                "order_level_paid_fee",
                "validated",
                "accounting_complete",
                "live_application_fee_rate_warning",
                "order_level_paid_fee_validated_single_fill_fee_rate_warning",
                "order_level_paid_fee_validated_single_fill_expected_fee_rate_mismatch",
                json.dumps(
                    {
                        "single_fill": True,
                        "paid_fee_present": True,
                        "executed_volume_match": True,
                        "executed_funds_match": True,
                        "expected_fee_rate_match": False,
                        "expected_fee_rate_warning": True,
                        "identifiers_match": True,
                        "material_notional_suspicious": True,
                    },
                    sort_keys=True,
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_health()
    health_out = capsys.readouterr().out
    assert "configured_fee_rate=0.002500" in health_out
    assert "fee_rate_drift=configured_fee_rate=0.002500 configured_fee_rate_estimate=0.002500" in health_out
    assert "configured_fee_bps=25.000" in health_out
    assert "observed_fee_bps_median=4.000" in health_out
    assert "observed_fee_sample_count=1" in health_out
    assert "fee_rate_deviation_pct=525.02" in health_out
    assert "recent_expected_fee_rate_mismatch_count=1" in health_out
    assert "expected_fee_rate_warning_count=1" in health_out
    assert "fee_pending_count=0" in health_out
    assert "position_authority_repair_count=0" in health_out
    assert "diagnostic_only_vs_startup_blocking=diagnostic_only" in health_out
    assert "startup_impact=diagnostic_only_without_active_fee_pending" in health_out
    assert "operator_action=review_fee_diagnostics" in health_out
    assert "fee_rate_drift_summary=diagnostic_only configured_fee_bps=25.000 observed_fee_bps_median=4.000" in health_out
    assert "action=review_fee_diagnostics" in health_out
    assert "recommended_command=uv run python bot.py fee-diagnostics" in health_out

    cmd_recovery_report(as_json=False)
    report_out = capsys.readouterr().out
    assert "[P3.0e1] fee_rate_drift" in report_out
    assert "configured_fee_rate=0.002500" in report_out
    assert "configured_fee_rate_estimate=0.002500" in report_out
    assert "configured_fee_bps=25.000" in report_out
    assert "observed_fee_bps_median=4.000" in report_out
    assert "observed_fee_sample_count=1" in report_out
    assert "fee_rate_deviation_pct=525.02" in report_out
    assert "recent_expected_fee_rate_mismatch_count=1" in report_out
    assert "expected_fee_rate_warning_count=1" in report_out
    assert "fee_pending_count=0" in report_out
    assert "fee_pending_accounting_repair_count=0" in report_out
    assert "position_authority_repair_count=0" in report_out
    assert "diagnostic_only_vs_startup_blocking=diagnostic_only" in report_out
    assert "startup_impact=diagnostic_only_without_active_fee_pending" in report_out
    assert "operator_action=review_fee_diagnostics" in report_out
    assert "fee_rate_drift_summary=diagnostic_only configured_fee_bps=25.000 observed_fee_bps_median=4.000" in report_out
    assert "action=review_fee_diagnostics" in report_out
    assert "recommended_command=uv run python bot.py fee-diagnostics" in report_out

    cmd_restart_checklist()
    checklist_out = capsys.readouterr().out
    assert "configured_fee_rate=0.002500" in checklist_out
    assert "configured_fee_rate_estimate=0.002500" in checklist_out
    assert "configured_fee_bps=25.000" in checklist_out
    assert "observed_fee_bps_median=4.000" in checklist_out
    assert "observed_fee_sample_count=1" in checklist_out
    assert "fee_rate_deviation_pct=525.02" in checklist_out
    assert "recent_expected_fee_rate_mismatch_count=1" in checklist_out
    assert "expected_fee_rate_warning_count=1" in checklist_out
    assert "fee_pending_count=0" in checklist_out
    assert "position_authority_repair_count=0" in checklist_out
    assert "diagnostic_only_vs_startup_blocking=diagnostic_only" in checklist_out
    assert "startup_impact=diagnostic_only_without_active_fee_pending" in checklist_out
    assert "operator_action=review_fee_diagnostics" in checklist_out
    assert "fee_rate_drift_summary=diagnostic_only configured_fee_bps=25.000 observed_fee_bps_median=4.000" in checklist_out
    assert "action=review_fee_diagnostics" in checklist_out
    assert "recommended_command=uv run python bot.py fee-diagnostics" in checklist_out


def test_recovery_report_and_restart_checklist_use_forensic_accounting_mode_for_active_accounting_root_cause(
    tmp_path, monkeypatch, capsys
):
    _set_tmp_db(tmp_path)
    object.__setattr__(settings, "MODE", "live")
    runtime_state.enable_trading()
    monkeypatch.setattr("bithumb_bot.app.write_json_atomic", lambda *_args, **_kwargs: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO broker_fill_observations(
                event_ts, client_order_id, exchange_order_id, fill_id, fill_ts, side,
                price, qty, fee, fee_status, fee_source, fee_confidence, accounting_status, source,
                fee_provenance, fee_validation_reason, fee_validation_checks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1_777_104_360_500,
                "live_pending_buy",
                "C0101000002949768709",
                "C0101000000983820316",
                1_777_104_360_321,
                "BUY",
                115_465_000.0,
                0.00059998,
                None,
                "order_level_candidate",
                "order_level_paid_fee",
                "ambiguous",
                "fee_pending",
                "live_application_fee_pending",
                "order_level_paid_fee_candidate",
                "pending_fee_validation",
                json.dumps({"single_fill": True}, sort_keys=True),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    report = _load_recovery_report()
    policy = report["recovery_policy"]
    assert policy["primary_incident_class"] == "ACCOUNTING_ROOT_CAUSE"
    assert policy["recommended_mode"] == "forensic_accounting"
    assert policy["accounting_root_cause_unresolved"] is True
    assert policy["accounting_evidence_reliable"] is False
    assert policy["additional_orders_allowed"] is False
    assert policy["flatten_primary_recommendation"] is False
    assert policy["flatten_not_primary"] is True
    assert policy["recommended_action"] == "collect_broker_fill_evidence_and_build_repair_plan"
    assert policy["recommended_command"] == "uv run python bot.py repair-plan"
    assert "fill_accounting_incident_active" in policy["incident_reasons"]
    assert report["fill_accounting_root_cause"]["flatten_as_primary_response"] is False

    cmd_recovery_report(as_json=False)
    report_out = capsys.readouterr().out
    assert "[P3.0e4] recovery_policy" in report_out
    assert "primary_incident_class=ACCOUNTING_ROOT_CAUSE" in report_out
    assert "recommended_mode=forensic_accounting" in report_out
    assert "accounting_root_cause_unresolved=1" in report_out
    assert "accounting_evidence_reliable=0" in report_out
    assert "actual_executable_exposure=0" in report_out
    assert "additional_orders_allowed=0" in report_out
    assert "flatten_primary_recommendation=0" in report_out
    assert "flatten_not_primary=1" in report_out
    assert "recommended_action=collect_broker_fill_evidence_and_build_repair_plan" in report_out
    assert "recommended_command=uv run python bot.py repair-plan" in report_out
    assert "incident_reasons=fill_accounting_incident_active" in report_out

    cmd_restart_checklist()
    checklist_out = capsys.readouterr().out
    assert "primary_incident_class=ACCOUNTING_ROOT_CAUSE" in checklist_out
    assert "recommended_mode=forensic_accounting" in checklist_out
    assert "accounting_root_cause_unresolved=1" in checklist_out
    assert "accounting_evidence_reliable=0" in checklist_out
    assert "actual_executable_exposure=0" in checklist_out
    assert "additional_orders_allowed=0" in checklist_out
    assert "flatten_primary_recommendation=0" in checklist_out
    assert "flatten_not_primary=1" in checklist_out
    assert "recommended_action=collect_broker_fill_evidence_and_build_repair_plan" in checklist_out
    assert "recommended_command=uv run python bot.py repair-plan" in checklist_out
    assert "incident_reasons=fill_accounting_incident_active" in checklist_out

    cmd_health()
    health_out = capsys.readouterr().out
    assert "primary_incident_class=ACCOUNTING_ROOT_CAUSE" in health_out
    assert "recommended_mode=forensic_accounting" in health_out
    assert "accounting_root_cause_unresolved=1" in health_out
    assert "actual_executable_exposure=0" in health_out
    assert "additional_orders_allowed=0" in health_out
    assert "flatten_primary_recommendation=0" in health_out
    assert "flatten_not_primary=1" in health_out
    assert "recommended_action=collect_broker_fill_evidence_and_build_repair_plan" in health_out
    assert "recommended_command=uv run python bot.py repair-plan" in health_out
    assert "incident_reasons=fill_accounting_incident_active" in health_out
    assert "next_commands=uv run python bot.py repair-plan" in health_out


def test_repair_plan_preview_is_non_mutating_and_lists_accounting_candidates(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    object.__setattr__(settings, "MODE", "live")
    runtime_state.enable_trading()

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO broker_fill_observations(
                event_ts, client_order_id, exchange_order_id, fill_id, fill_ts, side,
                price, qty, fee, fee_status, fee_source, fee_confidence, accounting_status, source,
                fee_provenance, fee_validation_reason, fee_validation_checks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1_777_104_360_500,
                "live_pending_buy",
                "C0101000002949768709",
                "C0101000000983820316",
                1_777_104_360_321,
                "BUY",
                115_465_000.0,
                0.00059998,
                None,
                "order_level_candidate",
                "order_level_paid_fee",
                "ambiguous",
                "fee_pending",
                "live_application_fee_pending",
                "order_level_paid_fee_candidate",
                "pending_fee_validation",
                json.dumps({"single_fill": True}, sort_keys=True),
            ),
        )
        before = {
            "fee_pending_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM fee_pending_accounting_repairs").fetchone()["cnt"],
            "fee_gap_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM fee_gap_accounting_repairs").fetchone()["cnt"],
            "manual_flat_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM manual_flat_accounting_repairs").fetchone()["cnt"],
            "position_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()["cnt"],
            "external_position_adjustments": conn.execute("SELECT COUNT(*) AS cnt FROM external_position_adjustments").fetchone()["cnt"],
        }
        conn.commit()
    finally:
        conn.close()

    cmd_repair_plan(as_json=False)
    out = capsys.readouterr().out
    assert "[REPAIR-PLAN]" in out
    assert "preview_mode=read_only_non_mutating" in out
    assert "plan_id=" in out
    assert "primary_incident_class=ACCOUNTING_ROOT_CAUSE" in out
    assert "recommended_mode=forensic_accounting" in out
    assert "accounting_root_cause_unresolved=1" in out
    assert "accounting_evidence_reliable=0" in out
    assert "actual_executable_exposure=0" in out
    assert "additional_orders_allowed=0" in out
    assert "flatten_primary_recommendation=0" in out
    assert "flatten_not_primary=1" in out
    assert "non_mutating_preview=1" in out
    assert "incident_reasons=fill_accounting_incident_active" in out
    assert "[POSITION-PROJECTION]" in out
    assert "source_of_truth=fills+trades+fee_adjustments+external_adjustments+repair_events" in out
    assert "projection_kind=open_position_lots" in out
    assert "rebuildable=1" in out
    assert "candidate_repairs:" in out
    assert "name=fee-pending-accounting-repair needed=1 active_issue=1 safe_to_apply=0" in out
    assert "name=fee-gap-accounting-repair" in out
    assert "name=manual-flat-accounting-repair" in out
    assert "name=external-position-accounting-repair" in out
    assert "name=rebuild-position-authority" in out
    assert "preconditions=" in out
    assert "touched_tables=" in out
    assert "expected_after=" in out
    assert "idempotency_key=" in out
    assert "rollback_or_backup=" in out
    assert "why_safe=" in out

    conn = ensure_db()
    try:
        after = {
            "fee_pending_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM fee_pending_accounting_repairs").fetchone()["cnt"],
            "fee_gap_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM fee_gap_accounting_repairs").fetchone()["cnt"],
            "manual_flat_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM manual_flat_accounting_repairs").fetchone()["cnt"],
            "position_repairs": conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()["cnt"],
            "external_position_adjustments": conn.execute("SELECT COUNT(*) AS cnt FROM external_position_adjustments").fetchone()["cnt"],
        }
    finally:
        conn.close()
    assert after == before


def test_health_and_recovery_report_include_dust_residual_metadata(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "oversized_debug_blob": "x" * 5000,
        },
    )

    cmd_health()
    health_out = capsys.readouterr().out
    assert "dust_residual_present=1" in health_out
    assert "dust_residual_allow_resume=1" in health_out
    assert "dust_policy_reason=matched_harmless_dust_resume_allowed" in health_out
    assert "dust_state=harmless_dust" in health_out
    assert "dust_operator_action=harmless_dust_tracked_resume_allowed" in health_out
    assert "dust_resume_allowed_by_policy=1" in health_out
    assert "dust_treat_as_flat=1" in health_out
    assert "dust_observed_broker_qty=0.00009629" in health_out
    assert "dust_observed_local_qty=0.00009629" in health_out
    assert "dust_broker_local_match=1" in health_out
    assert "dust_qty_below_min=broker=1 local=1" in health_out
    assert "dust_notional_below_min=broker=0 local=0" in health_out
    assert "position=dust_only_qty=0.00009629 entry_allowed=1" in health_out
    assert "effective_flat_due_to_harmless_dust=1" in health_out
    assert "tracked_dust_qty=0.00009629" in health_out
    assert "dust_effective_flat=1" in health_out
    assert "balance_source_flat_start_allowed=True" in health_out
    assert "balance_source_flat_start_reason=flat_start_effective_flat(" in health_out

    cmd_recovery_report(as_json=False)
    report_out = capsys.readouterr().out
    assert "[P3.0] dust_residual" in report_out
    assert "present=1" in report_out
    assert "allow_resume=1" in report_out
    assert "policy_reason=matched_harmless_dust_resume_allowed" in report_out
    assert "state=harmless_dust" in report_out
    assert "operator_action=harmless_dust_tracked_resume_allowed" in report_out
    assert "resume_allowed_by_policy=1" in report_out
    assert "treat_as_flat=1" in report_out
    assert "dust_effective_flat=1" in report_out
    assert "entry_allowed=1" in report_out
    assert "effective_flat_due_to_harmless_dust=1" in report_out
    assert (
        "observed_broker_qty=0.00009629 observed_local_qty=0.00009629 delta_qty=0.00000000 "
        "min_qty=0.00010000 min_notional_krw=5000.0"
    ) in report_out
    assert (
        "summary=observed_broker_qty=0.00009629 observed_local_qty=0.00009629 delta=0.00000000 "
        "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 classification=harmless_dust"
    ) in report_out
    assert "qty_below_min=broker=1 local=1 notional_below_min=broker=0 local=0" in report_out
    assert "broker_local_match=1" in report_out
    assert "[P3.1] lot_exposure" in report_out
    assert "raw_total_asset_qty=0.00009629 open_exposure_qty=0.00000000 dust_tracking_qty=0.00009629" in report_out
    assert "open_lot_count=0" in report_out
    assert "dust_tracking_lot_count=1" in report_out
    assert "sellable_executable_lot_count=0" in report_out
    assert "sellable_executable_qty=0.00000000" in report_out
    assert "terminal_state=dust_only" in report_out
    assert "exit_block_reason=dust_only_remainder" in report_out


def test_recovery_report_uses_lot_basis_for_order_summary(tmp_path):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        record_order_if_missing(
            conn,
            client_order_id="lot-basis-1",
            side="BUY",
            qty_req=0.001,
            price=100000000.0,
            ts_ms=10,
            status="NEW",
            intended_lot_count=1,
            executable_lot_count=1,
            final_intended_qty=0.001,
            final_submitted_qty=0.0004,
        )
        conn.commit()
    finally:
        conn.close()

    report = _load_recovery_report()
    item = next(entry for entry in report["recovery_candidates"] if entry["client_order_id"] == "lot-basis-1")

    assert item["requested_qty"] == pytest.approx(0.001)
    assert item["local_qty"] == pytest.approx(0.0004)
    assert item["local_qty_source"] == "final_submitted_qty"
    assert item["requested_lot_count"] == 1
    assert item["executable_lot_count"] == 1


def test_recovery_report_includes_recent_dust_unsellable_sell_event(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="FAILED",
        client_order_id="dust_exit_1",
        created_ts=now_ms,
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, side, qty, price, submission_reason_code, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "dust_exit_1",
                "submit_attempt_recorded",
                now_ms,
                "FAILED",
                "SELL",
                0.00009,
                100000000.0,
                DUST_RESIDUAL_UNSELLABLE,
                "state=EXIT_PARTIAL_LEFT_DUST;operator_action=MANUAL_DUST_REVIEW_REQUIRED;position_qty=0.000090000000",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_recovery_report(as_json=False)
    out = capsys.readouterr().out

    assert "[P3.0a] recent_dust_unsellable_event" in out
    assert f"reason_code={DUST_RESIDUAL_UNSELLABLE}" in out
    assert "EXIT_PARTIAL_LEFT_DUST" in out
    assert "MANUAL_DUST_REVIEW_REQUIRED" in out
