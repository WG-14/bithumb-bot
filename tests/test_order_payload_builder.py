from __future__ import annotations

import pytest

from bithumb_bot.broker import order_rules
from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.order_payloads import build_order_payload, build_order_payload_from_plan, validate_client_order_id
from bithumb_bot.execution_models import OrderIntent
from bithumb_bot.execution_planner import build_submit_plan


pytestmark = pytest.mark.fast_regression


def test_build_limit_order_payload_uses_doc_fields() -> None:
    payload = build_order_payload(
        market="KRW-BTC",
        side="buy",
        ord_type="limit",
        volume="0.1",
        price="10000",
    )
    assert payload == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "limit",
        "volume": "0.1",
        "price": "10000",
    }


def test_build_market_buy_payload_uses_doc_fields() -> None:
    payload = build_order_payload(
        market="KRW-BTC",
        side="bid",
        ord_type="price",
        price="10000",
    )
    assert payload == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "price",
        "price": "10000",
    }


def test_build_market_sell_payload_uses_doc_fields() -> None:
    payload = build_order_payload(
        market="KRW-BTC",
        side="sell",
        ord_type="market",
        volume="0.1",
    )
    assert payload == {
        "market": "KRW-BTC",
        "side": "ask",
        "order_type": "market",
        "volume": "0.1",
    }


def test_build_payload_rejects_unsupported_side() -> None:
    with pytest.raises(BrokerRejectError, match="unsupported order side"):
        build_order_payload(market="KRW-BTC", side="hold", ord_type="limit", volume="0.1", price="10000")


def test_build_payload_rejects_unsupported_order_type() -> None:
    with pytest.raises(BrokerRejectError, match="unsupported order_type"):
        build_order_payload(market="KRW-BTC", side="buy", ord_type="ioc", volume="0.1", price="10000")


def test_validate_client_order_id_accepts_documented_characters() -> None:
    assert validate_client_order_id("abcXYZ_123-xyz") == "abcXYZ_123-xyz"


def test_validate_client_order_id_rejects_too_long_value() -> None:
    with pytest.raises(BrokerRejectError, match="at most 36"):
        validate_client_order_id("a" * 37)


def test_validate_client_order_id_rejects_invalid_characters() -> None:
    with pytest.raises(BrokerRejectError, match="contains invalid characters"):
        validate_client_order_id("cid.bad")


def test_validate_client_order_id_rejects_empty_or_whitespace() -> None:
    with pytest.raises(BrokerRejectError, match="must not be empty"):
        validate_client_order_id("")
    with pytest.raises(BrokerRejectError, match="must not be empty"):
        validate_client_order_id("   ")


def test_build_order_payload_from_submit_plan_uses_planned_fields() -> None:
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
        client_order_id="cid-plan-payload",
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
        fetch_order_rules=lambda _market: type("Resolution", (), {"rules": rules})(),
        fetch_top_of_book=lambda _market: None,
        resolve_best_ask=lambda _quote, _market: 100_000_000.0,
        truncate_volume=lambda qty: qty,
    )

    payload_plan = build_order_payload_from_plan(
        plan=plan,
        decimal_from_value=lambda value: __import__("decimal").Decimal(str(value)),
        format_krw_amount=lambda value: format(value, "f").rstrip("0").rstrip("."),
        format_volume=lambda value: format(value, ".8f").rstrip("0").rstrip("."),
    )

    assert payload_plan.payload == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "price",
        "price": "80000",
        "client_order_id": "cid-plan-payload",
    }
