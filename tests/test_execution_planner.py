from __future__ import annotations

from types import SimpleNamespace

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker import order_rules
from bithumb_bot.execution_models import OrderIntent
from bithumb_bot.execution_planner import build_submit_plan
from bithumb_bot.public_api_orderbook import BestQuote


pytestmark = pytest.mark.fast_regression


def test_planner_builds_buy_market_notional_submit_plan() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )
    intent = OrderIntent(
        client_order_id="cid-planner-buy",
        market="KRW-BTC",
        side="BUY",
        normalized_side="bid",
        qty=0.0008,
        price=None,
        created_ts=1_700_000_000_000,
        submit_contract=order_rules.build_buy_price_none_submit_contract(
            rules=rules,
            resolution=order_rules.resolve_buy_price_none_resolution(rules=rules),
        ),
    )

    plan = build_submit_plan(
        intent=intent,
        rules=rules,
        fetch_order_rules=lambda _market: SimpleNamespace(rules=rules),
        fetch_top_of_book=lambda _market: BestQuote(
            market="KRW-BTC",
            bid_price=99_900_000.0,
            ask_price=100_000_000.0,
        ),
        resolve_best_ask=lambda _quote, _market: 100_000_000.0,
        truncate_volume=lambda qty: qty,
    )

    assert plan.intent is intent
    assert plan.chance_validation_order_type == "price"
    assert plan.exchange_submit_field == "price"
    assert plan.exchange_order_type == "price"
    assert plan.exchange_submit_price == pytest.approx(80_000.0)
    assert plan.exchange_submit_volume is None
    assert plan.exchange_submit_notional_krw == pytest.approx(80_000.0)
    assert plan.submit_contract_context["buy_price_none_decision_outcome"] == "pass"
    assert plan.submit_contract_context["exchange_order_type"] == "price"


def test_planner_builds_sell_market_qty_submit_plan() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price", "market"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )
    intent = OrderIntent(
        client_order_id="cid-planner-sell",
        market="KRW-BTC",
        side="SELL",
        normalized_side="ask",
        qty=0.0008,
        price=None,
        created_ts=1_700_000_000_000,
    )

    plan = build_submit_plan(
        intent=intent,
        rules=rules,
        fetch_order_rules=lambda _market: SimpleNamespace(rules=rules),
        fetch_top_of_book=lambda _market: BestQuote(
            market="KRW-BTC",
            bid_price=99_900_000.0,
            ask_price=100_000_000.0,
        ),
        resolve_best_ask=lambda _quote, _market: 100_000_000.0,
        truncate_volume=lambda qty: qty,
    )

    assert plan.intent is intent
    assert plan.chance_validation_order_type == "market"
    assert plan.exchange_submit_field == "volume"
    assert plan.exchange_order_type == "market"
    assert plan.exchange_submit_price is None
    assert plan.exchange_submit_volume == pytest.approx(0.0008)
    assert plan.buy_price_none_submit_contract is None
    assert plan.submit_contract_context["exchange_order_type"] == "market"


def test_planner_reports_buy_market_policy_block_reason() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "market"),
        bid_types=("market",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=5000.0,
        ask_min_total_krw=0.0,
        min_notional_krw=0.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=10.0,
        ask_price_unit=1.0,
    )
    intent = OrderIntent(
        client_order_id="cid-planner-blocked",
        market="KRW-BTC",
        side="BUY",
        normalized_side="bid",
        qty=0.001,
        price=None,
        created_ts=1_700_000_000_000,
        submit_contract=order_rules.build_buy_price_none_submit_contract(
            rules=rules,
            resolution=order_rules.resolve_buy_price_none_resolution(rules=rules),
        ),
    )

    with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support"):
        build_submit_plan(
            intent=intent,
            rules=rules,
            fetch_order_rules=lambda _market: SimpleNamespace(rules=rules),
            fetch_top_of_book=lambda _market: BestQuote(
                market="KRW-BTC",
                bid_price=99_900_000.0,
                ask_price=100_000_000.0,
            ),
            resolve_best_ask=lambda _quote, _market: 100_000_000.0,
            truncate_volume=lambda qty: qty,
        )
