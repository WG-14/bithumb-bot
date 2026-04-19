from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.bithumb_adapter import build_signed_order_request, build_submission_flow
from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.bithumb_client import submit_signed_order_request
from bithumb_bot.broker import bithumb_client
from bithumb_bot.broker.bithumb_execution import execute_signed_order_request
from bithumb_bot.broker.bithumb_read_models import parse_order_confirmation
from bithumb_bot.broker import order_rules
from bithumb_bot.broker.order_submit import plan_place_order
from bithumb_bot.config import settings
from bithumb_bot.execution_models import OrderConfirmation, OrderIntent, SignedOrderRequest
from bithumb_bot.public_api_orderbook import BestQuote


pytestmark = pytest.mark.fast_regression


def _configure_live() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)


def _resolved_rules():
    return order_rules.DerivedOrderConstraints(
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


def _patch_planning(monkeypatch, rules) -> None:
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _market: SimpleNamespace(rules=rules),
    )
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")
    monkeypatch.setattr(
        "bithumb_bot.broker.order_submit.fetch_orderbook_top",
        lambda _market: BestQuote(market="KRW-BTC", bid_price=99_900_000.0, ask_price=100_000_000.0),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_submit.validated_best_quote_ask_price",
        lambda _quote, requested_market: 100_000_000.0,
    )


def test_adapter_builds_plan_and_signed_request(monkeypatch) -> None:
    _configure_live()
    rules = _resolved_rules()
    _patch_planning(monkeypatch, rules)
    broker = BithumbBroker()
    plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-module-flow",
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
            market_price_hint=100_000_000.0,
            trace_id="cid-module-flow",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )

    flow = build_submission_flow(
        broker,
        plan=plan,
    )
    signed_request = build_signed_order_request(broker, plan=flow.plan)

    assert flow.plan.intent.client_order_id == "cid-module-flow"
    assert isinstance(signed_request, SignedOrderRequest)
    assert signed_request.payload["order_type"] == "price"


def test_client_submits_signed_order_request(monkeypatch) -> None:
    _configure_live()
    rules = _resolved_rules()
    _patch_planning(monkeypatch, rules)
    broker = BithumbBroker()
    calls: list[dict[str, object]] = []

    def _fake_submit_order(*, signed_request, retry_safe=False, response_excerpt=None):
        calls.append(
            {
                "payload": dict(signed_request.payload),
                "retry_safe": retry_safe,
            }
        )
        return {
            "status": "0000",
            "data": {"order_id": "ex-client", "client_order_id": signed_request.payload["client_order_id"]},
        }

    monkeypatch.setattr(broker._private_api, "submit_order", _fake_submit_order)
    plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-module-client",
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
            market_price_hint=100_000_000.0,
            trace_id="cid-module-client",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    flow = build_submission_flow(
        broker,
        plan=plan,
    )

    data = submit_signed_order_request(broker, signed_request=flow.signed_request)

    assert data["data"]["order_id"] == "ex-client"
    assert calls == [
        {
            "payload": flow.signed_request.payload,
            "retry_safe": False,
        }
    ]


def test_direct_signed_request_submit_is_rejected_for_armed_live(monkeypatch) -> None:
    _configure_live()
    rules = _resolved_rules()
    _patch_planning(monkeypatch, rules)
    broker = BithumbBroker()
    plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-module-direct-submit",
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
            market_price_hint=100_000_000.0,
            trace_id="cid-module-direct-submit",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    signed_request = build_signed_order_request(broker, plan=plan)

    with pytest.raises(BrokerRejectError, match="validated place_order flow authority"):
        submit_signed_order_request(broker, signed_request=signed_request)


def test_execution_and_read_models_confirm_response(monkeypatch) -> None:
    _configure_live()
    rules = _resolved_rules()
    _patch_planning(monkeypatch, rules)
    broker = BithumbBroker()
    plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-module-confirm",
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
            market_price_hint=100_000_000.0,
            trace_id="cid-module-confirm",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    flow = build_submission_flow(
        broker,
        plan=plan,
    )
    response = {
        "status": "0000",
        "data": {"order_id": "ex-confirm", "client_order_id": flow.plan.intent.client_order_id},
    }
    confirmation = parse_order_confirmation(
        broker,
        plan=flow.plan,
        signed_request=flow.signed_request,
        submission_record=SimpleNamespace(),
        response_data=response,
        now=1_700_000_000_000,
    )
    monkeypatch.setattr(
        broker._private_api,
        "submit_order",
        lambda *, signed_request, retry_safe=False, response_excerpt=None: response,
    )
    executed = execute_signed_order_request(
        broker,
        plan=flow.plan,
        signed_request=flow.signed_request,
        now=1_700_000_000_000,
    )

    assert isinstance(confirmation, OrderConfirmation)
    assert confirmation.exchange_order_id == "ex-confirm"
    assert isinstance(executed, OrderConfirmation)
    assert executed.exchange_order_id == "ex-confirm"


def test_submit_ignores_broker_post_private_override(monkeypatch) -> None:
    _configure_live()
    rules = _resolved_rules()
    _patch_planning(monkeypatch, rules)
    broker = BithumbBroker()
    post_override_called = False

    def _post_override(_endpoint, _payload, *, retry_safe=False):
        nonlocal post_override_called
        post_override_called = True
        return {"status": "0000", "data": {"order_id": "override"}}

    monkeypatch.setattr(broker, "_post_private", _post_override)
    monkeypatch.setattr(
        broker._private_api,
        "submit_order",
        lambda *, signed_request, retry_safe=False, response_excerpt=None: {
            "status": "0000",
            "data": {"order_id": "canonical", "client_order_id": signed_request.payload["client_order_id"]},
        },
    )
    plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id="cid-module-single-path",
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
            market_price_hint=100_000_000.0,
            trace_id="cid-module-single-path",
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    flow = build_submission_flow(broker, plan=plan)

    data = submit_signed_order_request(broker, signed_request=flow.signed_request)

    assert data["data"]["order_id"] == "canonical"
    assert post_override_called is False


def test_submit_client_cannot_reintroduce_raw_private_request_bypass() -> None:
    source = inspect.getsource(bithumb_client.submit_validated_order_payload)

    assert "._private_api.submit_order" in source
    assert "._private_api.request" not in source
    assert "\"/v2/orders\"" not in source
