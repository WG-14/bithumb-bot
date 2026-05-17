from __future__ import annotations

import pytest

from bithumb_bot.orderbook_depth_store import build_orderbook_depth_snapshot
from bithumb_bot.research.execution_model import DepthWalkExecutionModel, ExecutionRequest


def _snapshot():
    return build_orderbook_depth_snapshot(
        ts=1_700_000_000_100,
        pair="KRW-BTC",
        bid_levels=[(99.0, 1.0), (98.0, 2.0)],
        ask_levels=[(101.0, 1.0), (102.0, 2.0)],
        source="bithumb_public_v1_orderbook",
    )


def _request(**overrides):
    payload = {
        "signal_ts": 1,
        "decision_ts": 1_700_000_000_000,
        "submit_ts_assumption": 1_700_000_000_100,
        "fill_reference_ts": 1_700_000_000_100,
        "side": "BUY",
        "reference_price": 101.0,
        "fee_rate": 0.001,
        "requested_notional": 101.0,
        "best_bid": 99.0,
        "best_ask": 101.0,
        "spread_bps": 200.0,
    }
    payload.update(overrides)
    return ExecutionRequest(**payload)


def test_depth_walk_buy_full_fill_walks_asks_and_computes_vwap() -> None:
    fill = DepthWalkExecutionModel(fee_rate=0.001, depth_snapshot=_snapshot()).simulate(_request())

    assert fill.fill_status == "filled"
    assert fill.filled_qty == pytest.approx(1.0)
    assert fill.filled_notional == pytest.approx(101.0)
    assert fill.avg_fill_price == pytest.approx(101.0)
    assert fill.depth_levels_consumed == 1
    assert fill.depth_available is True
    assert fill.depth_sufficient is True
    assert fill.queue_position_mode == "unavailable"
    assert fill.market_impact_mode == "unavailable"


def test_depth_walk_buy_partial_fill_walks_multiple_ask_levels() -> None:
    fill = DepthWalkExecutionModel(fee_rate=0.0, depth_snapshot=_snapshot()).simulate(
        _request(reference_price=100.0, requested_notional=500.0)
    )

    assert fill.fill_status == "partial"
    assert fill.filled_qty == pytest.approx(3.0)
    assert fill.filled_notional == pytest.approx(101.0 + 204.0)
    assert fill.remaining_qty == pytest.approx(2.0)
    assert fill.avg_fill_price == pytest.approx(305.0 / 3.0)
    assert fill.slippage_bps == pytest.approx(((305.0 / 3.0) - 100.0) / 100.0 * 10_000.0)
    assert fill.depth_levels_consumed == 2
    assert fill.depth_sufficient is False
    assert "insufficient_depth_liquidity" in fill.execution_realism_limitations


def test_depth_walk_sell_full_fill_walks_bids() -> None:
    fill = DepthWalkExecutionModel(fee_rate=0.0, depth_snapshot=_snapshot()).simulate(
        _request(side="SELL", reference_price=99.0, requested_qty=2.0, requested_notional=None)
    )

    assert fill.fill_status == "filled"
    assert fill.filled_qty == pytest.approx(2.0)
    assert fill.filled_notional == pytest.approx(99.0 + 98.0)
    assert fill.avg_fill_price == pytest.approx(197.0 / 2.0)
    assert fill.slippage_bps == pytest.approx((99.0 - (197.0 / 2.0)) / 99.0 * 10_000.0)
    assert fill.depth_levels_consumed == 2


def test_depth_walk_no_fill_records_unfilled() -> None:
    fill = DepthWalkExecutionModel(fee_rate=0.0, depth_snapshot=_snapshot()).simulate(
        _request(side="SELL", requested_qty=0.0, requested_notional=None)
    )

    assert fill.fill_status == "unfilled"
    assert fill.filled_qty == 0.0
    assert fill.avg_fill_price is None
    assert fill.depth_sufficient is False
    assert "depth_unavailable_for_requested_side" in fill.execution_realism_limitations
