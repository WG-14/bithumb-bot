from __future__ import annotations

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, set_portfolio_breakdown
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.lifecycle import summarize_position_lots
from bithumb_bot.oms import set_status
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot, evaluate_clean_account_gate


BUY_QTY = 0.00099996
SELL_QTY = 0.00090000
RESIDUAL_QTY = 0.00009996
LOT_SIZE = 0.0003
MIN_QTY = 0.0001
MIN_NOTIONAL = 5000.0
PRICE = 96_688_675.47018808


@pytest.fixture
def incident_db(tmp_path, monkeypatch):
    original = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "PAIR": settings.PAIR,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "RESIDUAL_INVENTORY_MODE": settings.RESIDUAL_INVENTORY_MODE,
    }
    db_path = tmp_path / "residual-incident.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", MIN_QTY)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", MIN_QTY)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", MIN_NOTIONAL)
    object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", "track")
    runtime_state.enable_trading()
    try:
        yield db_path
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)
        runtime_state.enable_trading()


def _apply_incident(conn) -> None:
    record_order_if_missing(
        conn,
        client_order_id="incident_buy",
        side="BUY",
        qty_req=BUY_QTY,
        price=PRICE,
        ts_ms=1_700_000_000_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=MIN_QTY,
        qty_step=MIN_QTY,
        min_notional_krw=MIN_NOTIONAL,
        intended_lot_count=3,
        executable_lot_count=3,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="incident_buy",
        side="BUY",
        fill_id="incident_buy_fill",
        fill_ts=1_700_000_000_100,
        price=PRICE,
        qty=BUY_QTY,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=101,
        note="incident buy",
    )
    set_status("incident_buy", "FILLED", conn=conn)
    after_buy = summarize_position_lots(conn, pair=settings.PAIR)
    assert after_buy.raw_open_exposure_qty == pytest.approx(SELL_QTY)
    assert after_buy.dust_tracking_qty == pytest.approx(RESIDUAL_QTY)

    record_order_if_missing(
        conn,
        client_order_id="incident_sell",
        side="SELL",
        qty_req=SELL_QTY,
        price=PRICE,
        ts_ms=1_700_000_000_200,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=MIN_QTY,
        qty_step=MIN_QTY,
        min_notional_krw=MIN_NOTIONAL,
        intended_lot_count=3,
        executable_lot_count=3,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="incident_sell",
        side="SELL",
        fill_id="incident_sell_fill",
        fill_ts=1_700_000_000_300,
        price=PRICE,
        qty=SELL_QTY,
        fee=0.0,
        strategy_name="sma_with_filter",
        exit_decision_id=201,
        note="incident sell",
    )
    set_status("incident_sell", "FILLED", conn=conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=RESIDUAL_QTY,
        asset_locked=0.0,
    )
    conn.commit()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_observed_ts_ms": 1_700_000_000_400,
            "base_currency": "BTC",
            "quote_currency": "KRW",
            "broker_asset_qty": RESIDUAL_QTY,
            "broker_asset_available": RESIDUAL_QTY,
            "broker_asset_locked": 0.0,
        },
        now_epoch_sec=0.0,
    )


def test_buy_00099996_sell_00090000_leaves_sub_min_tracked_dust(incident_db):
    conn = ensure_db(str(incident_db))
    try:
        _apply_incident(conn)
        summary = summarize_position_lots(conn, pair=settings.PAIR)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert summary.raw_open_exposure_qty == pytest.approx(0.0)
    assert summary.dust_tracking_qty == pytest.approx(RESIDUAL_QTY)
    assert readiness.position_state.normalized_exposure.sellable_executable_lot_count == 0
    assert readiness.residual_inventory.exchange_sellable is False
    assert readiness.residual_disposition.disposition == "TRACKED_NON_EXECUTABLE"


def test_incident_residual_does_not_block_clean_account_gate(incident_db):
    conn = ensure_db(str(incident_db))
    try:
        _apply_incident(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        gate = evaluate_clean_account_gate(readiness)
    finally:
        conn.close()

    assert gate.allowed is True
    assert gate.reason_code != "sellable_residual_clean_account_required"
    assert readiness.run_loop_allowed is True


def test_incident_residual_does_not_create_sellable_residual_candidate(incident_db):
    conn = ensure_db(str(incident_db))
    try:
        _apply_incident(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert readiness.residual_sell_candidate is None
    assert readiness.residual_inventory.residual_qty == pytest.approx(RESIDUAL_QTY)
    assert readiness.residual_inventory.exchange_sellable is False
