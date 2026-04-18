from __future__ import annotations

from types import SimpleNamespace

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.order_submit import build_place_order_submission_flow, execute_place_order
from bithumb_bot.broker import order_rules
from bithumb_bot.config import settings
from bithumb_bot.execution_models import (
    OrderConfirmation,
    OrderIntent,
    SignedOrderRequest,
    SubmissionRecord,
    SubmitPlan,
    SubmitPriceTickPolicy,
)
from bithumb_bot.public_api_orderbook import BestQuote


pytestmark = pytest.mark.fast_regression


def _configure_live_settings() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)


def test_execution_contract_models_are_instantiable() -> None:
    intent = OrderIntent(
        client_order_id="cid-model",
        market="KRW-BTC",
        side="BUY",
        normalized_side="bid",
        qty=0.001,
        price=None,
        created_ts=1_700_000_000_000,
        submit_contract={"shape": "explicit"},
    )
    plan = SubmitPlan(
        intent=intent,
        rules=SimpleNamespace(name="rules"),
        chance_validation_order_type="price",
        chance_supported_order_types=("price",),
        exchange_submit_field_hint="price",
        submit_contract_context={"market": "KRW-BTC"},
        submit_price_tick_policy=SubmitPriceTickPolicy(applies=True, price_unit=1.0, reason="test"),
        effective_market_price=100_000_000.0,
        lot_rules=SimpleNamespace(lot_size=0.001),
        qty_split=SimpleNamespace(lot_count=1, dust_qty=0.0),
        internal_lot_qty=0.001,
        exchange_submit_qty=0.001,
        buy_price_none_submit_contract={"kind": "market-buy"},
    )
    signed_request = SignedOrderRequest(
        intent=intent,
        plan=plan,
        payload={"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "100000"},
        submit_contract_context={"market": "KRW-BTC"},
        exchange_submit_field="price",
        exchange_submit_notional_krw=100_000.0,
        exchange_submit_qty=0.001,
        internal_lot_qty=0.001,
        canonical_payload="market=KRW-BTC",
    )
    submission = SubmissionRecord(
        intent=intent,
        plan=plan,
        signed_request=signed_request,
        request_ts=intent.created_ts,
    )
    confirmation = OrderConfirmation(
        submission=submission,
        client_order_id=intent.client_order_id,
        exchange_order_id="ex-model",
        side=intent.side,
        status="NEW",
        price=intent.price,
        qty=plan.internal_lot_qty,
        filled_qty=0.0,
        created_ts=intent.created_ts,
        updated_ts=intent.created_ts,
        raw={"order_id": "ex-model"},
        submit_contract_context={"market": "KRW-BTC"},
    )

    assert plan.intent is intent
    assert signed_request.plan is plan
    assert submission.signed_request is signed_request
    assert confirmation.submission is submission


def test_live_submit_flow_creates_explicit_execution_contract_models(monkeypatch) -> None:
    _configure_live_settings()

    resolved_rules = order_rules.DerivedOrderConstraints(
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
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=resolved_rules),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _market: BestQuote(market="KRW-BTC", bid_price=99_900_000.0, ask_price=100_000_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.validated_best_quote_ask_price",
        lambda _quote, requested_market: 100_000_000.0,
    )

    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda _endpoint, payload, *, retry_safe=False: {
            "status": "0000",
            "data": {"order_id": "ex-contract-flow", "client_order_id": payload["client_order_id"]},
        },
    )
    submit_contract = order_rules.build_buy_price_none_submit_contract(
        rules=resolved_rules,
        resolution=order_rules.resolve_buy_price_none_resolution(rules=resolved_rules),
    )

    flow = build_place_order_submission_flow(
        broker,
        validated_client_order_id="cid-contract-flow",
        side="BUY",
        qty=0.0008,
        price=None,
        buy_price_none_submit_contract=submit_contract,
        now=1_700_000_000_000,
    )
    confirmation = execute_place_order(
        broker,
        plan=flow.plan,
        signed_request=flow.signed_request,
        now=flow.intent.created_ts,
    )

    assert isinstance(flow.intent, OrderIntent)
    assert isinstance(flow.plan, SubmitPlan)
    assert isinstance(flow.signed_request, SignedOrderRequest)
    assert isinstance(confirmation.submission, SubmissionRecord)
    assert isinstance(confirmation, OrderConfirmation)
    assert confirmation.client_order_id == "cid-contract-flow"
    assert confirmation.exchange_order_id == "ex-contract-flow"
    assert confirmation.submit_contract_context["exchange_order_type"] == submit_contract.exchange_order_type
