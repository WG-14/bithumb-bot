from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, set_portfolio_breakdown
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.lifecycle import rebuild_lifecycle_projections_from_trades
from bithumb_bot.oms import set_status
from bithumb_bot.position_authority_state import build_position_authority_assessment


BUY_QTY = 0.00099996
SELL_QTY = 0.00090000
RESIDUAL_QTY = 0.00009996
LOT_SIZE = 0.0003


@pytest.fixture
def authority_db(tmp_path, monkeypatch):
    original = {
        "DB_PATH": settings.DB_PATH,
        "PAIR": settings.PAIR,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
    }
    db_path = tmp_path / "projection-replay-residual.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    try:
        yield db_path
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def _seed_partial_close(conn, *, portfolio_qty: float = RESIDUAL_QTY) -> None:
    record_order_if_missing(
        conn,
        client_order_id="projection_buy",
        side="BUY",
        qty_req=BUY_QTY,
        price=100_000_000.0,
        ts_ms=1,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        intended_lot_count=3,
        executable_lot_count=3,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="projection_buy",
        side="BUY",
        fill_id="projection_buy_fill",
        fill_ts=2,
        price=100_000_000.0,
        qty=BUY_QTY,
        fee=0.0,
    )
    set_status("projection_buy", "FILLED", conn=conn)
    record_order_if_missing(
        conn,
        client_order_id="projection_sell",
        side="SELL",
        qty_req=SELL_QTY,
        price=100_000_000.0,
        ts_ms=3,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        intended_lot_count=3,
        executable_lot_count=3,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="projection_sell",
        side="SELL",
        fill_id="projection_sell_fill",
        fill_ts=4,
        price=100_000_000.0,
        qty=SELL_QTY,
        fee=0.0,
    )
    set_status("projection_sell", "FILLED", conn=conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=float(portfolio_qty),
        asset_locked=0.0,
    )
    rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
    conn.commit()


def _required_fields(assessment: dict[str, object]) -> None:
    for key in (
        "incident_class",
        "partial_close_residual_candidate",
        "residual_state_converged",
        "expected_residual_qty",
        "projection_convergence",
        "blockers",
    ):
        assert key in assessment


def test_partial_close_residual_converged_does_not_require_authority_correction(authority_db):
    conn = ensure_db(str(authority_db))
    try:
        _seed_partial_close(conn)
        assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
    finally:
        conn.close()

    _required_fields(assessment)
    assert assessment["incident_class"] != "POSITION_AUTHORITY_CORRECTION_REQUIRED"
    assert assessment["partial_close_residual_candidate"] is True
    assert assessment["residual_state_converged"] is True
    assert assessment["expected_residual_qty"] == pytest.approx(RESIDUAL_QTY)
    assert assessment["projection_convergence"]["converged"] is True
    assert "POSITION_AUTHORITY_CORRECTION_REQUIRED" not in assessment["blockers"]


def test_projection_mismatch_requires_authority_repair_not_manual_closeout(authority_db):
    conn = ensure_db(str(authority_db))
    try:
        _seed_partial_close(conn, portfolio_qty=RESIDUAL_QTY + 0.0001)
        assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
    finally:
        conn.close()

    _required_fields(assessment)
    assert assessment["incident_class"] in {
        "PROJECTION_PORTFOLIO_DIVERGENCE",
        "PROJECTION_RESIDUAL_DIVERGENCE",
        "LOT_AUTHORITY_CONFLICT",
    }
    assert assessment["expected_residual_qty"] == pytest.approx(RESIDUAL_QTY)
    assert assessment["projection_convergence"]["converged"] is False
    assert assessment["recommended_action"] == "review_recovery_report"
    assert "manual_exchange_closeout_or_rule_update" not in str(assessment)
    assert assessment["blockers"] != []


def test_rebuild_projection_is_deterministic_for_incident_fixture(authority_db):
    conn = ensure_db(str(authority_db))
    try:
        _seed_partial_close(conn)
        first = build_position_authority_assessment(conn, pair=settings.PAIR)
        rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        second = build_position_authority_assessment(conn, pair=settings.PAIR)
    finally:
        conn.close()

    _required_fields(first)
    _required_fields(second)
    for key in (
        "incident_class",
        "partial_close_residual_candidate",
        "residual_state_converged",
        "expected_residual_qty",
        "projection_convergence",
        "blockers",
    ):
        assert second[key] == first[key]
