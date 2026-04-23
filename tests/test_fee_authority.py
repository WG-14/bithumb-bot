from __future__ import annotations

from types import SimpleNamespace

import pytest

from bithumb_bot.broker import order_rules
from bithumb_bot.config import settings
from bithumb_bot.fee_authority import (
    FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON,
    build_fee_authority_snapshot,
)
from bithumb_bot.order_sizing import build_buy_execution_sizing


pytestmark = pytest.mark.fast_regression


@pytest.fixture(autouse=True)
def _restore_settings():
    old = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "LIVE_FEE_RATE_ESTIMATE": settings.LIVE_FEE_RATE_ESTIMATE,
        "BUY_FRACTION": settings.BUY_FRACTION,
        "MAX_ORDER_KRW": settings.MAX_ORDER_KRW,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": settings.STRATEGY_ENTRY_SLIPPAGE_BPS,
        "ENTRY_EDGE_BUFFER_RATIO": settings.ENTRY_EDGE_BUFFER_RATIO,
    }
    yield
    for key, value in old.items():
        object.__setattr__(settings, key, value)


def _rules(**overrides):
    values = {
        "market_id": "KRW-BTC",
        "bid_min_total_krw": 5000.0,
        "ask_min_total_krw": 5000.0,
        "bid_price_unit": 1.0,
        "ask_price_unit": 1.0,
        "order_types": ("limit", "price", "market"),
        "bid_types": ("price",),
        "ask_types": ("limit", "market"),
        "order_sides": ("bid", "ask"),
        "bid_fee": 0.001,
        "ask_fee": 0.002,
        "maker_bid_fee": 0.0007,
        "maker_ask_fee": 0.0008,
        "min_qty": 0.0001,
        "qty_step": 0.0001,
        "min_notional_krw": 5000.0,
        "max_qty_decimals": 8,
    }
    values.update(overrides)
    return order_rules.DerivedOrderConstraints(**values)


def _chance_source():
    return {
        "bid_fee": "chance_doc",
        "ask_fee": "chance_doc",
        "maker_bid_fee": "chance_doc",
        "maker_ask_fee": "chance_doc",
    }


def test_exchange_fee_authority_preserves_chance_provenance() -> None:
    resolution = SimpleNamespace(
        rules=_rules(),
        source=_chance_source(),
        source_mode="merged",
        fallback_used=False,
        retrieved_at_sec=100.0,
        expires_at_sec=400.0,
        stale=False,
        snapshot_persisted=True,
        is_stale=lambda *, now_sec=None: False,
    )

    authority = build_fee_authority_snapshot(resolution, now_sec=200.0)

    assert authority.fee_source == "chance_doc"
    assert authority.degraded is False
    assert authority.bid_fee == authority.taker_bid_fee_rate
    assert float(authority.taker_roundtrip_fee_rate) == pytest.approx(0.003)
    assert authority.as_dict()["live_entry_allowed"] is True


def test_config_fee_estimate_is_degraded_fallback_not_parallel_authority() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.99)
    resolution = SimpleNamespace(
        rules=_rules(bid_fee=0.001, ask_fee=0.002),
        source=_chance_source(),
        source_mode="merged",
        fallback_used=False,
        retrieved_at_sec=100.0,
        expires_at_sec=400.0,
        stale=False,
        snapshot_persisted=False,
        is_stale=lambda *, now_sec=None: False,
    )

    authority = build_fee_authority_snapshot(resolution, now_sec=200.0)

    assert authority.fee_source == "chance_doc"
    assert float(authority.bid_fee) == pytest.approx(0.001)
    assert float(authority.ask_fee) == pytest.approx(0.002)

    fallback_resolution = SimpleNamespace(
        rules=_rules(bid_fee=0.0, ask_fee=0.0),
        source={
            "bid_fee": "unsupported_by_doc",
            "ask_fee": "unsupported_by_doc",
            "maker_bid_fee": "unsupported_by_doc",
            "maker_ask_fee": "unsupported_by_doc",
        },
        source_mode="local_fallback",
        fallback_used=True,
        retrieved_at_sec=100.0,
        expires_at_sec=400.0,
        stale=False,
        snapshot_persisted=False,
        is_stale=lambda *, now_sec=None: False,
    )
    fallback = build_fee_authority_snapshot(
        fallback_resolution,
        now_sec=200.0,
        config_fallback_fee_rate=0.004,
    )

    assert fallback.fee_source == "config_estimate_degraded"
    assert fallback.degraded is True
    assert float(fallback.bid_fee) == pytest.approx(0.004)
    assert "fee_source_not_chance_doc" in fallback.degraded_reason


def test_persisted_snapshot_fee_authority_is_operator_visible_degraded() -> None:
    resolution = SimpleNamespace(
        rules=_rules(),
        source=_chance_source(),
        source_mode="merged",
        fallback_used=False,
        retrieved_at_sec=100.0,
        expires_at_sec=0.0,
        stale=False,
        snapshot_persisted=True,
        is_stale=lambda *, now_sec=None: False,
    )

    authority = build_fee_authority_snapshot(resolution, now_sec=200.0)

    assert authority.fee_source == "chance_doc"
    assert authority.snapshot_derived is True
    assert authority.degraded is True
    assert "persisted_snapshot_fee_authority" in authority.degraded_reason
    assert authority.as_dict()["live_entry_block_reason"] == FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON


def test_degraded_fee_authority_blocks_live_armed_buy_sizing(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 10_000.0)

    fallback_resolution = SimpleNamespace(
        rules=_rules(bid_fee=0.0, ask_fee=0.0),
        source={
            "bid_fee": "unsupported_by_doc",
            "ask_fee": "unsupported_by_doc",
            "maker_bid_fee": "unsupported_by_doc",
            "maker_ask_fee": "unsupported_by_doc",
        },
        source_mode="local_fallback",
        fallback_used=True,
        retrieved_at_sec=100.0,
        expires_at_sec=400.0,
        stale=False,
        snapshot_persisted=False,
        is_stale=lambda *, now_sec=None: False,
    )
    monkeypatch.setattr("bithumb_bot.order_sizing.get_effective_order_rules", lambda _pair: fallback_resolution)

    plan = build_buy_execution_sizing(
        pair="KRW-BTC",
        cash_krw=10_000.0,
        market_price=10_000_000.0,
    )

    assert plan.allowed is False
    assert plan.block_reason == FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON
    assert plan.fee_authority_degraded is True
    assert plan.fee_authority_source == "config_estimate_degraded"


def test_decimal_boundary_sizing_uses_chance_fee_without_float_fee_budget_drift(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.50)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 5001.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    resolution = SimpleNamespace(
        rules=_rules(
            bid_fee=0.001,
            ask_fee=0.0,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
        ),
        source=_chance_source(),
        source_mode="merged",
        fallback_used=False,
        retrieved_at_sec=100.0,
        expires_at_sec=400.0,
        stale=False,
        snapshot_persisted=False,
        is_stale=lambda *, now_sec=None: False,
    )
    monkeypatch.setattr("bithumb_bot.order_sizing.get_effective_order_rules", lambda _pair: resolution)

    plan = build_buy_execution_sizing(
        pair="KRW-BTC",
        cash_krw=5001.0,
        market_price=10_000_000.0,
    )

    assert plan.allowed is True
    assert plan.requested_qty == pytest.approx(0.0005001)
    assert plan.fee_rate_used == pytest.approx(0.001)
    assert plan.fee_authority_source == "chance_doc"
