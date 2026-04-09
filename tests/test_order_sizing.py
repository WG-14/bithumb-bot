from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.order_sizing import build_buy_execution_sizing, build_sell_execution_sizing


@pytest.fixture
def sizing_rule_overrides():
    original = {
        "BUY_FRACTION": float(settings.BUY_FRACTION),
        "MAX_ORDER_KRW": float(settings.MAX_ORDER_KRW),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
        "LIVE_FEE_RATE_ESTIMATE": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
    }
    object.__setattr__(settings, "BUY_FRACTION", 0.5)
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    try:
        yield
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_buy_execution_sizing_finalizes_order_qty_from_entry_budget(sizing_rule_overrides) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=20_000_000.0,
    )

    assert plan.side == "BUY"
    assert plan.allowed is True
    assert plan.qty_source == "entry.intent_budget_krw"
    assert plan.budget_krw == pytest.approx(10000.0)
    assert plan.requested_qty == pytest.approx(0.0005)
    assert plan.executable_qty == pytest.approx(0.0005)


def test_buy_execution_sizing_consumes_entry_intent_and_still_finalizes_qty_in_sizing(
    sizing_rule_overrides,
) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=20_000_000.0,
        entry_intent={
            "pair": "BTC_KRW",
            "intent": "enter_open_exposure",
            "budget_model": "cash_fraction_capped_by_max_order_krw",
            "budget_fraction_of_cash": 0.25,
            "max_budget_krw": 4000.0,
            "requires_execution_sizing": True,
        },
    )

    assert plan.allowed is False
    assert plan.budget_krw == pytest.approx(4000.0)
    assert plan.requested_qty == pytest.approx(0.0002)
    assert plan.executable_qty == pytest.approx(0.0)
    assert plan.block_reason == "no_executable_exit_lot"


def test_buy_execution_sizing_does_not_reserve_fee_budget_before_qty_rounding(sizing_rule_overrides) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=10_000.0,
        fee_rate=0.1,
    )

    assert plan.side == "BUY"
    assert plan.allowed is True
    assert plan.budget_krw == pytest.approx(10000.0)
    assert plan.requested_qty == pytest.approx(10000.0 / 10_000.0)
    assert plan.executable_qty <= plan.requested_qty
    assert plan.executable_qty == pytest.approx(plan.requested_qty)


def test_sell_execution_sizing_finalizes_order_qty_from_sellable_inventory(sizing_rule_overrides) -> None:
    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        sellable_qty=0.12345678,
        exit_allowed=True,
        exit_block_reason="none",
    )

    assert plan.side == "SELL"
    assert plan.allowed is True
    assert plan.qty_source == "position_state.normalized_exposure.sellable_executable_qty"
    assert plan.requested_qty == pytest.approx(0.12345678)
    assert plan.executable_qty == pytest.approx(0.1234)
