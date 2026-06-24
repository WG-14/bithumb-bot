from __future__ import annotations

import pytest

from bithumb_bot.broker import order_rules
from bithumb_bot.broker.order_payloads import build_order_payload_from_plan
from bithumb_bot.execution_models import OrderIntent
from bithumb_bot.execution_planner import build_submit_plan
from bithumb_bot.execution_service import ExecutionSubmitPlan, H74SubmitSemantics


pytestmark = pytest.mark.fast_regression


def _rules() -> order_rules.DerivedOrderConstraints:
    return order_rules.DerivedOrderConstraints(
        order_types=("limit", "price", "market"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        min_notional_krw=5000.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )


def _plan_h74_quote_buy():
    rules = _rules()
    return build_submit_plan(
        intent=OrderIntent(
            client_order_id="h74-quote-buy",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=100_000.0 / 100_000_120.0,
            price=None,
            created_ts=1,
            submit_contract=order_rules.build_buy_price_none_submit_contract(
                rules=rules,
                resolution=order_rules.resolve_buy_price_none_resolution(rules=rules),
            ),
            quote_notional_krw=100_000.0,
            quote_notional_authority="h74_fixed_fill_quote_notional_buy",
            submit_semantics="quote_notional_market_buy",
            submit_semantics_authority="h74_fixed_fill_quote_notional_buy",
            market_price_hint=100_000_120.0,
        ),
        rules=rules,
        fetch_order_rules=lambda _market: type("Resolution", (), {"rules": rules})(),
        fetch_top_of_book=lambda _market: None,
        resolve_best_ask=lambda _quote, _market: 100_000_120.0,
        truncate_volume=lambda qty: qty,
    )


def test_h74_fixed_fill_buy_preserves_quote_notional_100000_at_high_btc_price() -> None:
    plan = _plan_h74_quote_buy()
    payload = build_order_payload_from_plan(plan=plan).payload

    assert plan.exchange_submit_notional_krw == pytest.approx(100_000.0)
    assert plan.quote_notional_krw == pytest.approx(100_000.0)
    assert plan.exchange_order_type == "price"
    assert plan.exchange_submit_field == "price"
    assert plan.exchange_submit_volume is None
    assert plan.submit_qty_authority == "non_authoritative_preview"
    assert payload["order_type"] == "price"
    assert payload["price"] == "100000"
    assert "volume" not in payload


def test_h74_fixed_fill_buy_does_not_floor_quote_notional_to_90000() -> None:
    plan = _plan_h74_quote_buy()

    assert plan.exchange_constrained_qty == pytest.approx(0.0009)
    assert plan.exchange_submit_notional_krw != pytest.approx(90_000.108)
    assert plan.exchange_submit_notional_krw == pytest.approx(100_000.0)


def test_h74_fixed_fill_buy_payload_is_price_order_without_volume() -> None:
    payload = build_order_payload_from_plan(plan=_plan_h74_quote_buy()).payload

    assert payload == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "price",
        "price": "100000",
        "client_order_id": "h74-quote-buy",
    }


def test_h74_buy_submit_plan_requires_quote_notional_semantics() -> None:
    plan = _plan_h74_quote_buy()

    assert plan.submit_semantics == "quote_notional_market_buy"
    assert plan.quote_notional_krw == pytest.approx(100_000.0)
    assert plan.exchange_submit_field == "price"


def test_execution_submit_plan_as_final_payload_includes_h74_typed_fields() -> None:
    plan = ExecutionSubmitPlan(
        side="BUY",
        source="h74_source_observation",
        authority="h74_fixed_fill_quote_notional_buy",
        final_action="REBALANCE_TO_TARGET",
        qty=0.0009,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="h74-final",
        h74_submit_semantics=H74SubmitSemantics(
            sizing_mode="quote_notional",
            quote_notional_krw=100_000.0,
            submit_semantics="quote_notional_market_buy",
            fill_qty_authority="broker_fill",
            position_mode="fixed_fill_qty_until_exit",
            exchange_order_type="price",
            exchange_submit_field="price",
            exchange_submit_notional_krw=100_000.0,
            exchange_submit_qty=None,
            quote_notional_authority="h74_fixed_fill_quote_notional_buy",
            submit_semantics_authority="h74_fixed_fill_quote_notional_buy",
        ),
    )

    payload = plan.as_final_payload()

    assert payload["submit_semantics"] == "quote_notional_market_buy"
    assert payload["quote_notional_krw"] == pytest.approx(100_000.0)
    assert payload["fill_qty_authority"] == "broker_fill"
    assert payload["position_mode"] == "fixed_fill_qty_until_exit"
    assert payload["exchange_order_type"] == "price"
    assert payload["exchange_submit_field"] == "price"
    assert payload["exchange_submit_notional_krw"] == pytest.approx(100_000.0)
    assert payload["exchange_submit_qty"] is None
    assert payload["content_hash"].startswith("sha256:")


def test_h74_buy_submit_plan_rejects_base_qty_semantics() -> None:
    plan = _plan_h74_quote_buy()

    assert plan.submit_semantics != "base_qty"
    assert plan.exchange_submit_volume is None


def test_general_target_delta_buy_keeps_floor_sizing() -> None:
    rules = _rules()
    plan = build_submit_plan(
        intent=OrderIntent(
            client_order_id="general-floor-buy",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=100_000.0 / 100_000_120.0,
            price=None,
            created_ts=1,
            submit_contract=order_rules.build_buy_price_none_submit_contract(
                rules=rules,
                resolution=order_rules.resolve_buy_price_none_resolution(rules=rules),
            ),
            market_price_hint=100_000_120.0,
        ),
        rules=rules,
        fetch_order_rules=lambda _market: type("Resolution", (), {"rules": rules})(),
        fetch_top_of_book=lambda _market: None,
        resolve_best_ask=lambda _quote, _market: 100_000_120.0,
        truncate_volume=lambda qty: qty,
    )

    assert plan.exchange_constrained_qty == pytest.approx(0.0009)
    assert plan.exchange_submit_notional_krw == pytest.approx(90_000.0)
    assert plan.submit_qty_authority == "submit_plan.exchange_constraints"
