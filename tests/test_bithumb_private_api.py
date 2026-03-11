from __future__ import annotations

import httpx
import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.base import BrokerRejectError, BrokerTemporaryError
from bithumb_bot.config import settings


class _SequencedClient:
    actions: list[object] = []
    calls = 0

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, endpoint: str, data: dict[str, str], headers: dict[str, str]):
        type(self).calls += 1
        action = type(self).actions.pop(0)
        if isinstance(action, Exception):
            raise action
        return action



def _mk_response(status_code: int, payload: dict) -> httpx.Response:
    req = httpx.Request("POST", "https://api.bithumb.com/private")
    return httpx.Response(status_code, json=payload, request=req)


def _configure_live():
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")


def test_private_timeout_is_temporary_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [httpx.ReadTimeout("timeout")]
    _SequencedClient.calls = 0
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerTemporaryError):
        broker._post_private("/info/balance", {"currency": "BTC"}, retry_safe=False)


def test_private_business_reject_is_reject_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"status": "5600", "message": "invalid"})]
    _SequencedClient.calls = 0
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError):
        broker._post_private("/info/balance", {"currency": "BTC"}, retry_safe=False)


def test_private_safe_call_retries_temporary_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        httpx.ConnectError("down"),
        _mk_response(200, {"status": "0000", "data": {}}),
    ]
    _SequencedClient.calls = 0
    sleeps: list[float] = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda sec: sleeps.append(sec))

    broker = BithumbBroker()
    data = broker._post_private("/info/balance", {"currency": "BTC"}, retry_safe=True)

    assert data["status"] == "0000"
    assert _SequencedClient.calls == 2
    assert sleeps == [0.2]


def test_balance_parses_available_and_locked(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {
            "status": "0000",
            "data": {
                "available_krw": "1000",
                "in_use_krw": "25",
                "available_btc": "0.1",
                "in_use_btc": "0.02",
            },
        },
    )

    bal = broker.get_balance()

    assert bal.cash_available == 1000.0
    assert bal.cash_locked == 25.0
    assert bal.asset_available == 0.1
    assert bal.asset_locked == 0.02


def test_place_order_market_buy_routes_to_market_endpoint(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"status": "0000", "data": {"order_id": "mkt-1"}}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-1", side="BUY", qty=0.1234, price=None)

    assert order.exchange_order_id == "mkt-1"
    assert call["endpoint"] == "/trade/market_buy"
    assert call["retry_safe"] is False
    assert call["payload"] == {
        "order_currency": "BTC",
        "payment_currency": "KRW",
        "units": "0.1234000000000000",
    }


def test_place_order_limit_sell_still_uses_trade_place(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"status": "0000", "data": {"order_id": "lmt-1"}}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-2", side="SELL", qty=0.5, price=150000000)

    assert order.exchange_order_id == "lmt-1"
    assert call["endpoint"] == "/trade/place"
    assert call["retry_safe"] is False
    assert call["payload"] == {
        "order_currency": "BTC",
        "payment_currency": "KRW",
        "units": "0.5000000000000000",
        "type": "sell",
        "price": "150000000",
    }


def test_recent_orders_includes_filled_history_not_in_open_orders(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    open_order_payload = {
        "order_id": "open-1",
        "type": "buy",
        "price": "150000000",
        "units": "0.0200",
        "units_remaining": "0.0200",
    }
    tx_rows = [
        {
            "order_id": "filled-1",
            "search": "sell",
            "price": "151000000",
            "units_traded": "0.0100",
            "transfer_date": "1710000001000",
        },
        {
            "order_id": "filled-1",
            "search": "sell",
            "price": "152000000",
            "units_traded": "0.0050",
            "transfer_date": "1710000002000",
        },
    ]

    monkeypatch.setattr(
        broker,
        "get_open_orders",
        lambda: [broker._broker_order_from_open_row(open_order_payload, now_ts=1710000000000)],
    )
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {"status": "0000", "data": tx_rows},
    )

    recent = broker.get_recent_orders(limit=10)

    by_id = {str(order.exchange_order_id): order for order in recent}
    assert by_id["open-1"].status == "NEW"
    assert by_id["filled-1"].status == "FILLED"
    assert by_id["filled-1"].qty_filled == pytest.approx(0.015)
    assert by_id["filled-1"].qty_req == pytest.approx(0.015)
    assert by_id["filled-1"].side == "SELL"


def test_recent_orders_falls_back_to_open_orders_when_history_unavailable(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    open_orders = [
        broker._broker_order_from_open_row(
            {
                "order_id": "open-1",
                "type": "buy",
                "price": "150000000",
                "units": "0.0200",
                "units_remaining": "0.0100",
            },
            now_ts=1710000000000,
        )
    ]
    monkeypatch.setattr(broker, "get_open_orders", lambda: open_orders)

    def _raise_history(*_args, **_kwargs):
        raise BrokerTemporaryError("history unavailable")

    monkeypatch.setattr(broker, "_post_private", _raise_history)

    recent = broker.get_recent_orders(limit=10)

    assert recent == open_orders
    assert recent[0].status == "PARTIAL"


def test_open_order_lookup_still_uses_open_orders_endpoint(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    calls: list[str] = []

    def _fake_post_private(endpoint, payload, retry_safe=False):
        calls.append(endpoint)
        if endpoint == "/info/orders":
            return {
                "status": "0000",
                "data": [
                    {
                        "order_id": "open-1",
                        "type": "buy",
                        "price": "150000000",
                        "units": "0.0200",
                        "units_remaining": "0.0200",
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    open_orders = broker.get_open_orders()

    assert calls == ["/info/orders"]
    assert len(open_orders) == 1
    assert open_orders[0].exchange_order_id == "open-1"


def test_get_order_uses_open_orders_as_primary_status_source(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    calls: list[str] = []

    def _fake_post_private(endpoint, payload, retry_safe=False):
        calls.append(endpoint)
        if endpoint == "/info/orders":
            return {
                "status": "0000",
                "data": [
                    {
                        "order_id": "open-1",
                        "type": "buy",
                        "price": "150000000",
                        "units": "0.0200",
                        "units_remaining": "0.0050",
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.get_order(client_order_id="cid-1", exchange_order_id="open-1")

    assert calls == ["/info/orders"]
    assert order.status == "PARTIAL"
    assert order.qty_req == pytest.approx(0.02)
    assert order.qty_filled == pytest.approx(0.015)


def test_get_order_maps_non_open_partial_to_canceled(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    calls: list[str] = []

    def _fake_post_private(endpoint, payload, retry_safe=False):
        calls.append(endpoint)
        if endpoint == "/info/orders":
            return {"status": "0000", "data": []}
        if endpoint == "/info/order_detail":
            return {
                "status": "0000",
                "data": [
                    {
                        "order_id": "closed-1",
                        "type": "sell",
                        "price": "151000000",
                        "units": "0.1000",
                        "units_remaining": "0.0400",
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.get_order(client_order_id="cid-2", exchange_order_id="closed-1")

    assert calls == ["/info/orders", "/info/order_detail"]
    assert order.status == "CANCELED"
    assert order.qty_req == pytest.approx(0.1)
    assert order.qty_filled == pytest.approx(0.06)


def test_get_order_maps_non_open_filled_to_filled(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_post_private(endpoint, payload, retry_safe=False):
        if endpoint == "/info/orders":
            return {"status": "0000", "data": []}
        if endpoint == "/info/order_detail":
            return {
                "status": "0000",
                "data": [
                    {
                        "order_id": "filled-1",
                        "type": "buy",
                        "price": "149000000",
                        "units": "0.0500",
                        "units_remaining": "0.0000",
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.get_order(client_order_id="cid-3", exchange_order_id="filled-1")

    assert order.status == "FILLED"
    assert order.qty_req == pytest.approx(0.05)
    assert order.qty_filled == pytest.approx(0.05)


def test_get_order_rejects_ambiguous_closed_lookup(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_post_private(endpoint, payload, retry_safe=False):
        if endpoint == "/info/orders":
            return {"status": "0000", "data": []}
        if endpoint == "/info/order_detail":
            return {
                "status": "0000",
                "data": [
                    {
                        "order_id": "ambig-1",
                        "type": "buy",
                        "price": "149000000",
                        "units": "0",
                        "units_remaining": "0",
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    with pytest.raises(BrokerRejectError, match="ambiguous"):
        broker.get_order(client_order_id="cid-4", exchange_order_id="ambig-1")
