from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.order_payloads import build_order_payload


def test_build_limit_order_payload_uses_doc_fields() -> None:
    payload = build_order_payload(
        market="KRW-BTC",
        side="buy",
        ord_type="limit",
        volume="0.1",
        price="10000",
        client_order_id="cid-1",
    )
    assert payload == {
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "limit",
        "volume": "0.1",
        "price": "10000",
        "client_order_id": "cid-1",
    }


def test_build_market_buy_payload_uses_doc_fields() -> None:
    payload = build_order_payload(
        market="KRW-BTC",
        side="bid",
        ord_type="price",
        price="10000",
        client_order_id="cid-2",
    )
    assert payload == {
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "price",
        "price": "10000",
        "client_order_id": "cid-2",
    }


def test_build_market_sell_payload_uses_doc_fields() -> None:
    payload = build_order_payload(
        market="KRW-BTC",
        side="sell",
        ord_type="market",
        volume="0.1",
        client_order_id="cid-3",
    )
    assert payload == {
        "market": "KRW-BTC",
        "side": "ask",
        "ord_type": "market",
        "volume": "0.1",
        "client_order_id": "cid-3",
    }


def test_build_payload_rejects_unsupported_side() -> None:
    with pytest.raises(BrokerRejectError, match="unsupported order side"):
        build_order_payload(market="KRW-BTC", side="hold", ord_type="limit", volume="0.1", price="10000")


def test_build_payload_rejects_unsupported_order_type() -> None:
    with pytest.raises(BrokerRejectError, match="unsupported ord_type"):
        build_order_payload(market="KRW-BTC", side="buy", ord_type="ioc", volume="0.1", price="10000")


def test_build_payload_rejects_empty_client_order_id() -> None:
    with pytest.raises(BrokerRejectError, match="client_order_id must be a non-empty string"):
        build_order_payload(market="KRW-BTC", side="buy", ord_type="limit", volume="0.1", price="10000", client_order_id="  ")
