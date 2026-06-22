from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.lifecycle import rebuild_lifecycle_projections_from_trades, summarize_position_lots
from bithumb_bot.oms import set_status


BUY_QTY = 0.00099996
SELL_QTY = 0.00090000
RESIDUAL_QTY = 0.00009996
LOT_SIZE = 0.0003


@pytest.fixture
def projection_replay_db(tmp_path, monkeypatch):
    original = {
        "DB_PATH": settings.DB_PATH,
        "PAIR": settings.PAIR,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
    }
    db_path = tmp_path / "projection-replay-invariants.sqlite"
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


def _seed(conn) -> None:
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
    conn.commit()


def _rows(conn):
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT entry_client_order_id, position_state, qty_open,
                   executable_lot_count, dust_tracking_lot_count
            FROM open_position_lots
            ORDER BY id
            """
        ).fetchall()
    ]


def test_rebuild_projection_is_idempotent_for_open_and_dust_lots(projection_replay_db):
    conn = ensure_db(str(projection_replay_db))
    try:
        _seed(conn)
        rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        first = _rows(conn)
        rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        second = _rows(conn)
    finally:
        conn.close()

    assert second == first


def test_projection_qty_equals_open_plus_dust_after_rebuild(projection_replay_db):
    conn = ensure_db(str(projection_replay_db))
    try:
        _seed(conn)
        rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        summary = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert summary.raw_total_asset_qty == pytest.approx(
        summary.raw_open_exposure_qty + summary.dust_tracking_qty
    )
    assert summary.raw_open_exposure_qty == pytest.approx(0.0)


def test_projection_rebuild_preserves_incident_residual_qty(projection_replay_db):
    conn = ensure_db(str(projection_replay_db))
    try:
        _seed(conn)
        before = summarize_position_lots(conn, pair=settings.PAIR)
        rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        after = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert before.dust_tracking_qty == pytest.approx(RESIDUAL_QTY)
    assert after.dust_tracking_qty == pytest.approx(RESIDUAL_QTY)
    assert after.open_lot_count == 0
