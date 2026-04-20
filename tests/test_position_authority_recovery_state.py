from __future__ import annotations

import json

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, record_broker_fill_observation, set_portfolio_breakdown
from bithumb_bot.engine import (
    evaluate_restart_readiness,
    evaluate_resume_eligibility,
    evaluate_startup_safety_gate,
    get_health_status,
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.fee_gap_repair import apply_fee_gap_accounting_repair, build_fee_gap_accounting_repair_preview
from bithumb_bot.fee_pending_repair import (
    apply_fee_pending_accounting_repair,
    build_fee_pending_accounting_repair_preview,
)
from bithumb_bot.lifecycle import summarize_position_lots
from bithumb_bot.oms import set_status
from bithumb_bot.position_authority_repair import (
    apply_position_authority_rebuild,
    build_position_authority_rebuild_preview,
)
from bithumb_bot.position_authority_state import build_position_authority_assessment
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot


FILL_QTY = 0.00059996
LOT_SIZE = 0.0004
PRICE = 7_050_000.0


@pytest.fixture
def recovery_db(tmp_path, monkeypatch):
    db_path = tmp_path / "authority-recovery.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0003)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_startup_gate_reason(None)
    runtime_state.record_reconcile_result(success=True, reason_code="RECONCILE_OK", metadata={}, now_epoch_sec=0.0)
    return db_path


def _record_fee_pending_buy(conn, *, client_order_id: str = "incident_buy", fill_id: str = "fill-23") -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side="BUY",
        qty_req=FILL_QTY,
        price=PRICE,
        ts_ms=1_700_000_000_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0002,
        qty_step=0.0001,
        min_notional_krw=0.0,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    record_broker_fill_observation(
        conn,
        event_ts=1_700_000_000_100,
        client_order_id=client_order_id,
        exchange_order_id="ex-61",
        fill_id=fill_id,
        fill_ts=1_700_000_000_050,
        side="BUY",
        price=PRICE,
        qty=FILL_QTY,
        fee=None,
        fee_status="missing",
        accounting_status="fee_pending",
        source="test_reconcile_fee_pending",
        raw_payload={"fixture": "incident"},
    )


def _apply_fee_pending_buy(conn, *, client_order_id: str = "incident_buy", fill_id: str = "fill-23") -> None:
    _record_fee_pending_buy(conn, client_order_id=client_order_id, fill_id=fill_id)
    result = apply_fee_pending_accounting_repair(
        conn,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fee=4.23,
        fee_provenance="operator_fixture",
    )
    assert result["applied_fill"] is not None
    conn.commit()


def _corrupt_latest_buy_lot_as_incident(conn, *, client_order_id: str = "incident_buy") -> None:
    trade = conn.execute(
        "SELECT id, ts FROM trades WHERE client_order_id=? AND side='BUY'",
        (client_order_id,),
    ).fetchone()
    assert trade is not None
    conn.execute("DELETE FROM open_position_lots WHERE entry_trade_id=?", (int(trade["id"]),))
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
            int(trade["id"]),
            client_order_id,
            "fill-23",
            int(trade["ts"]),
            PRICE,
            FILL_QTY,
            0,
            1,
            1,
            FILL_QTY,
            0.0003,
            0.0001,
            0.0,
            8,
            "ledger",
            "lot-native",
            "dust_tracking",
            4.23,
        ),
    )
    conn.commit()


def _record_historical_sell_history(conn) -> None:
    record_order_if_missing(
        conn,
        client_order_id="historical_buy",
        side="BUY",
        qty_req=LOT_SIZE,
        price=PRICE,
        ts_ms=1_699_999_000_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="historical_buy",
        side="BUY",
        fill_id="historical-buy-fill",
        fill_ts=1_699_999_000_100,
        price=PRICE,
        qty=LOT_SIZE,
        fee=1.0,
    )
    set_status("historical_buy", "FILLED", conn=conn)
    record_order_if_missing(
        conn,
        client_order_id="historical_sell",
        side="SELL",
        qty_req=LOT_SIZE,
        price=PRICE,
        ts_ms=1_699_999_100_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="historical_sell",
        side="SELL",
        fill_id="historical-sell-fill",
        fill_ts=1_699_999_100_100,
        price=PRICE,
        qty=LOT_SIZE,
        fee=1.0,
    )
    set_status("historical_sell", "FILLED", conn=conn)
    conn.commit()


def _apply_fee_pending_sell(conn, *, client_order_id: str = "incident_sell", fill_id: str = "sell-fill-9") -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side="SELL",
        qty_req=LOT_SIZE,
        price=PRICE,
        ts_ms=1_700_000_100_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0002,
        qty_step=0.0001,
        min_notional_krw=0.0,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    record_broker_fill_observation(
        conn,
        event_ts=1_700_000_100_100,
        client_order_id=client_order_id,
        exchange_order_id="ex-sell-71",
        fill_id=fill_id,
        fill_ts=1_700_000_100_050,
        side="SELL",
        price=PRICE,
        qty=LOT_SIZE,
        fee=None,
        fee_status="missing",
        accounting_status="fee_pending",
        source="test_reconcile_fee_pending",
        raw_payload={"fixture": "incident-sell"},
    )
    result = apply_fee_pending_accounting_repair(
        conn,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fee=17.73,
        fee_provenance="operator_fixture",
    )
    assert result["applied_fill"] is not None
    conn.commit()


def test_fee_pending_repaired_buy_materializes_consistent_lot_authority(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            ORDER BY id ASC
            """
        ).fetchall()
        fill = conn.execute("SELECT intended_lot_count, executable_lot_count, internal_lot_size FROM fills").fetchone()
        order = conn.execute("SELECT status, qty_filled, executable_lot_count, internal_lot_size FROM orders").fetchone()
        summary = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert fill["executable_lot_count"] == 1
    assert fill["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert order["status"] == "FILLED"
    assert order["qty_filled"] == pytest.approx(FILL_QTY)
    assert rows[0]["position_state"] == "open_exposure"
    assert rows[0]["qty_open"] == pytest.approx(LOT_SIZE)
    assert rows[0]["executable_lot_count"] == 1
    assert rows[1]["position_state"] == "dust_tracking"
    assert rows[1]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert summary.open_lot_count == 1
    assert summary.dust_tracking_lot_count == 1


def test_authority_correction_repairs_incident_dust_row_with_historical_sell_history(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)

        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn)

        assert assessment["needs_correction"] is True
        assert assessment["safe_to_correct"] is True
        assert preview["repair_mode"] == "correction"
        assert preview["safe_to_apply"] is True
        assert preview["sell_trade_count"] == 1

        result = apply_position_authority_rebuild(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            WHERE entry_client_order_id='incident_buy'
            ORDER BY id ASC
            """
        ).fetchall()
        repair = conn.execute("SELECT reason, repair_basis FROM position_authority_repairs").fetchone()
    finally:
        conn.close()

    assert result["repair"]["reason"] == "accounted_buy_fill_authority_correction"
    assert repair["reason"] == "accounted_buy_fill_authority_correction"
    assert json.loads(repair["repair_basis"])["event_type"] == "position_authority_correction"
    assert rows[0]["position_state"] == "open_exposure"
    assert rows[0]["qty_open"] == pytest.approx(LOT_SIZE)
    assert rows[0]["executable_lot_count"] == 1
    assert rows[0]["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert rows[1]["position_state"] == "dust_tracking"
    assert rows[1]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert rows[1]["dust_tracking_lot_count"] == 1


def test_fee_gap_deadlock_reports_authority_correction_as_next_stage(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "fee_gap_recovery_required": 1,
                "material_zero_fee_fill_count": 1,
                "material_zero_fee_fill_latest_ts": 1_699_999_000_100,
                "fee_gap_adjustment_count": 1,
                "fee_gap_adjustment_total_krw": 4.23,
                "fee_gap_adjustment_latest_event_ts": 1_700_000_010_000,
                "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
            },
            now_epoch_sec=1.0,
        )

        readiness = compute_runtime_readiness_snapshot(conn)
        fee_gap = build_fee_gap_accounting_repair_preview(conn)
        startup_reason = evaluate_startup_safety_gate()
        resume_allowed, resume_blockers = evaluate_resume_eligibility()
    finally:
        conn.close()

    assert readiness.recovery_stage == "AUTHORITY_CORRECTION_PENDING"
    assert readiness.resume_blockers == ("POSITION_AUTHORITY_CORRECTION_REQUIRED",)
    assert fee_gap["needs_repair"] is True
    assert fee_gap["safe_to_apply"] is False
    assert fee_gap["blocked_by_authority_correction"] is True
    assert fee_gap["next_required_action"] == "rebuild_position_authority"
    assert "position_authority_correction_required=" in str(startup_reason)
    assert resume_allowed is False
    assert any(blocker.reason_code == "POSITION_AUTHORITY_CORRECTION_REQUIRED" for blocker in resume_blockers)


def test_fee_pending_and_authority_repair_resume_open_position_with_deferred_fee_gap(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="FEE_GAP_RECOVERY_REQUIRED",
            metadata={
                "fee_gap_recovery_required": 1,
                "material_zero_fee_fill_count": 1,
                "material_zero_fee_fill_latest_ts": 1_699_999_000_100,
                "fee_gap_adjustment_count": 1,
                "fee_gap_adjustment_total_krw": 4.23,
                "fee_gap_adjustment_latest_event_ts": 1_700_000_010_000,
                "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
            },
            now_epoch_sec=1.0,
        )

        before = compute_runtime_readiness_snapshot(conn)
        repair = apply_position_authority_rebuild(conn)
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
        fee_gap = build_fee_gap_accounting_repair_preview(conn)
        lots = summarize_position_lots(conn, pair=settings.PAIR)
        startup_reason = evaluate_startup_safety_gate()
        resume_allowed, resume_blockers = evaluate_resume_eligibility()
    finally:
        conn.close()

    assert before.recovery_stage == "AUTHORITY_CORRECTION_PENDING"
    assert repair["repair"]["reason"] == "accounted_buy_fill_authority_correction"
    assert lots.open_lot_count >= 1
    assert lots.dust_tracking_lot_count == 1
    assert after.recovery_stage == "RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT"
    assert after.resume_ready is True
    assert after.resume_blockers == ()
    assert after.blocker_categories == ("advisory_historical_debt",)
    assert fee_gap["needs_repair"] is True
    assert fee_gap["safe_to_apply"] is False
    assert fee_gap["repair_eligibility_state"] == "blocked_until_flattened"
    assert fee_gap["resume_policy"] == "defer_for_open_position_management"
    assert fee_gap["resume_blocking"] is False
    assert fee_gap["closeout_blocking"] is True
    assert fee_gap["blocked_by_open_exposure"] is True
    assert fee_gap["blocked_by_dust_residue"] is True
    assert startup_reason is None
    assert resume_allowed is True
    assert resume_blockers == []


def test_partial_close_residual_normalization_replays_buy_and_sell_authority(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        apply_position_authority_rebuild(conn)
        _apply_fee_pending_sell(conn)

        before = compute_runtime_readiness_snapshot(conn)
        conn.commit()
        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn)
        result = apply_position_authority_rebuild(conn)
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            WHERE entry_client_order_id='incident_buy'
            ORDER BY id ASC
            """
        ).fetchall()
        repair = conn.execute(
            """
            SELECT reason, repair_basis
            FROM position_authority_repairs
            ORDER BY event_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        startup_reason = evaluate_startup_safety_gate()
        resume_allowed, resume_blockers = evaluate_resume_eligibility()
        restart = evaluate_restart_readiness()
        health = get_health_status()
    finally:
        conn.close()

    assert before.recovery_stage == "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING"
    assert assessment["needs_residual_normalization"] is True
    assert assessment["safe_to_normalize_residual"] is True
    assert assessment["expected_residual_qty"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert preview["repair_mode"] == "residual_normalization"
    assert preview["safe_to_apply"] is True
    assert result["repair"]["reason"] == "partial_close_residual_authority_normalization"
    assert repair["reason"] == "partial_close_residual_authority_normalization"
    basis = json.loads(repair["repair_basis"])
    assert basis["event_type"] == "partial_close_residual_authority_normalization"
    assert basis["target_trade_id"] == assessment["target_trade_id"]
    assert basis["expected_residual_qty"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert len(rows) == 1
    assert rows[0]["position_state"] == "dust_tracking"
    assert rows[0]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert rows[0]["executable_lot_count"] == 0
    assert rows[0]["dust_tracking_lot_count"] == 1
    assert after.recovery_stage == "RESUME_READY"
    assert after.resume_ready is True
    assert startup_reason is None
    assert resume_allowed is True
    assert resume_blockers == []
    assert health["startup_gate_reason"] is None
    assert health["resume_gate_blocked"] is False
    assert all(ok for _label, ok, _detail in restart)


def test_dust_only_fee_gap_deadlock_converges_through_canonical_execution_flat_state(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        apply_position_authority_rebuild(conn)
        _apply_fee_pending_sell(conn)
        apply_position_authority_rebuild(conn)
        conn.commit()
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="FEE_GAP_RECOVERY_REQUIRED",
            metadata={
                "fee_gap_recovery_required": 1,
                "material_zero_fee_fill_count": 1,
                "material_zero_fee_fill_latest_ts": 1_700_000_100_050,
                "fee_gap_adjustment_count": 1,
                "fee_gap_adjustment_total_krw": 17.73,
                "fee_gap_adjustment_latest_event_ts": 1_700_000_200_000,
                "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
            },
            now_epoch_sec=1.0,
        )

        readiness = compute_runtime_readiness_snapshot(conn)
        fee_gap = build_fee_gap_accounting_repair_preview(conn)
        resume_allowed_before, resume_blockers_before = evaluate_resume_eligibility()
        repair = apply_fee_gap_accounting_repair(conn)
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
        fee_gap_after = build_fee_gap_accounting_repair_preview(conn)
    finally:
        conn.close()

    assert readiness.canonical_state == "DUST_ONLY_TRACKED"
    assert readiness.execution_flat is True
    assert readiness.accounting_flat is False
    assert readiness.recovery_stage == "HISTORICAL_FEE_GAP_PENDING"
    assert fee_gap["canonical_state"] == "DUST_ONLY_TRACKED"
    assert fee_gap["execution_flat"] is True
    assert fee_gap["accounting_flat"] is False
    assert fee_gap["needs_repair"] is True
    assert fee_gap["safe_to_apply"] is True
    assert fee_gap["repair_eligibility_state"] == "safe_to_apply_with_tracked_dust"
    assert fee_gap["next_required_action"] == "apply_fee_gap_accounting_repair"
    assert resume_allowed_before is False
    assert any(blocker.reason_code == "FEE_GAP_RECOVERY_REQUIRED" for blocker in resume_blockers_before)
    assert repair["repair"]["created"] is True
    assert after.recovery_stage == "RESUME_READY"
    assert after.resume_ready is True
    assert fee_gap_after["needs_repair"] is False
    assert fee_gap_after["already_repaired"] is True


def test_recovery_policy_cross_module_consistency_for_representative_states(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        flat = compute_runtime_readiness_snapshot(conn)
        flat_fee_gap = build_fee_gap_accounting_repair_preview(conn)

        _apply_fee_pending_buy(conn, client_order_id="open_buy", fill_id="open-fill")
        open_readiness = compute_runtime_readiness_snapshot(conn)
        open_fee_gap = build_fee_gap_accounting_repair_preview(conn)

        _apply_fee_pending_sell(conn, client_order_id="open_sell", fill_id="open-sell-fill")
        apply_position_authority_rebuild(conn)
        conn.commit()
        dust_readiness = compute_runtime_readiness_snapshot(conn)
        dust_fee_gap = build_fee_gap_accounting_repair_preview(conn)

        conn.execute("DELETE FROM open_position_lots")
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.123,
            asset_locked=0.0,
        )
        conn.commit()
        non_exec_readiness = compute_runtime_readiness_snapshot(conn)
        non_exec_fee_gap = build_fee_gap_accounting_repair_preview(conn)
    finally:
        conn.close()

    cases = [
        (flat, flat_fee_gap, "FLAT", True, True),
        (open_readiness, open_fee_gap, "OPEN_EXECUTABLE", False, False),
        (dust_readiness, dust_fee_gap, "DUST_ONLY_TRACKED", True, False),
        (non_exec_readiness, non_exec_fee_gap, "AUTHORITY_MISSING", False, False),
    ]
    for readiness, fee_gap, canonical_state, execution_flat, accounting_flat in cases:
        assert readiness.canonical_state == canonical_state
        assert readiness.execution_flat is execution_flat
        assert readiness.accounting_flat is accounting_flat
        assert fee_gap["canonical_state"] == canonical_state
        assert fee_gap["execution_flat"] is execution_flat
        assert fee_gap["accounting_flat"] is accounting_flat


def test_fee_pending_repair_remains_applicable_when_fill_exists_but_fee_incomplete(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_002_000_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            side="BUY",
            fill_id="fee-incomplete-fill",
            fill_ts=1_700_002_000_100,
            price=PRICE,
            qty=LOT_SIZE,
            fee=0.0,
        )
        set_status("fee_incomplete_existing_fill", "FILLED", conn=conn)
        record_broker_fill_observation(
            conn,
            event_ts=1_700_002_000_200,
            client_order_id="fee_incomplete_existing_fill",
            exchange_order_id="ex-fee-incomplete",
            fill_id="fee-incomplete-fill",
            fill_ts=1_700_002_000_100,
            side="BUY",
            price=PRICE,
            qty=LOT_SIZE,
            fee=None,
            fee_status="missing",
            accounting_status="fee_pending",
            source="test_existing_fill_fee_pending",
            raw_payload={"fixture": "existing-fill-fee-pending"},
        )
        conn.commit()
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="FILL_FEE_PENDING_RECOVERY_REQUIRED",
            metadata={"fee_pending_recovery_required": 1},
            now_epoch_sec=1.0,
        )

        readiness = compute_runtime_readiness_snapshot(conn)
        preview = build_fee_pending_accounting_repair_preview(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            fill_id="fee-incomplete-fill",
            fee=3.21,
            fee_provenance="operator_checked_bithumb_trade_history",
        )
        result = apply_fee_pending_accounting_repair(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            fill_id="fee-incomplete-fill",
            fee=3.21,
            fee_provenance="operator_checked_bithumb_trade_history",
        )
        conn.commit()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt, SUM(fee) AS fee_total FROM fills WHERE client_order_id='fee_incomplete_existing_fill'"
        ).fetchone()
        complete_observation = conn.execute(
            """
            SELECT fee_status, accounting_status
            FROM broker_fill_observations
            WHERE client_order_id='fee_incomplete_existing_fill'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert readiness.fee_pending_count == 1
    assert preview["needs_repair"] is True
    assert preview["safe_to_apply"] is True
    assert preview["repair_mode"] == "complete_existing_fill_fee"
    assert "fill_already_accounted" not in preview["eligibility_reason"]
    assert result["applied_fill"]["repair_mode"] == "complete_existing_fill_fee"
    assert fill_count["cnt"] == 1
    assert fill_count["fee_total"] == pytest.approx(3.21)
    assert complete_observation["fee_status"] == "operator_confirmed"
    assert complete_observation["accounting_status"] == "accounting_complete"
