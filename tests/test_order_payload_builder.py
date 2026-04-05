from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.order_payloads import build_order_payload, validate_client_order_id


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
        "ord_type": "limit",
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
        "ord_type": "price",
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
        "ord_type": "market",
        "volume": "0.1",
    }


def test_build_payload_rejects_unsupported_side() -> None:
    with pytest.raises(BrokerRejectError, match="unsupported order side"):
        build_order_payload(market="KRW-BTC", side="hold", ord_type="limit", volume="0.1", price="10000")


def test_build_payload_rejects_unsupported_order_type() -> None:
    with pytest.raises(BrokerRejectError, match="unsupported ord_type"):
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
