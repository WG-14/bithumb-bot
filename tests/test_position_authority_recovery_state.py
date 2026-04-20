from __future__ import annotations

import json

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, record_broker_fill_observation
from bithumb_bot.engine import evaluate_resume_eligibility, evaluate_startup_safety_gate
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.fee_gap_repair import build_fee_gap_accounting_repair_preview
from bithumb_bot.fee_pending_repair import apply_fee_pending_accounting_repair
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


def test_authority_correction_fails_closed_when_target_buy_was_later_sold(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        record_order_if_missing(
            conn,
            client_order_id="later_sell",
            side="SELL",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_100_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        conn.execute(
            """
            INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, client_order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1_700_000_100_100, settings.PAIR, settings.INTERVAL, "SELL", PRICE, LOT_SIZE, 1.0, 0.0, 0.0, "later_sell"),
        )
        set_status("later_sell", "FILLED", conn=conn)
        conn.commit()
        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn)
    finally:
        conn.close()

    assert assessment["needs_correction"] is True
    assert assessment["safe_to_correct"] is False
    assert "sell_after_target_buy=1" in assessment["blockers"]
    assert preview["safe_to_apply"] is False
    assert "sell_after_target_buy=1" in preview["eligibility_reason"]
