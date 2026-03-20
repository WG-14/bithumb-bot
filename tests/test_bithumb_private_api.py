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


def test_place_order_market_sell_routes_to_market_endpoint(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"status": "0000", "data": {"order_id": "mkt-2"}}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-2", side="SELL", qty=0.4321, price=None)

    assert order.exchange_order_id == "mkt-2"
    assert call["endpoint"] == "/trade/market_sell"
    assert call["retry_safe"] is False
    assert call["payload"] == {
        "order_currency": "BTC",
        "payment_currency": "KRW",
        "units": "0.4321000000000000",
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


def test_place_order_limit_buy_still_uses_trade_place(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"status": "0000", "data": {"order_id": "lmt-2"}}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-3", side="BUY", qty=0.4, price=149500000)

    assert order.exchange_order_id == "lmt-2"
    assert call["endpoint"] == "/trade/place"
    assert call["retry_safe"] is False
    assert call["payload"] == {
        "order_currency": "BTC",
        "payment_currency": "KRW",
        "units": "0.4000000000000000",
        "type": "buy",
        "price": "149500000",
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


def test_recent_orders_skips_malformed_transaction_rows(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    open_order = broker._broker_order_from_open_row(
        {
            "order_id": "open-1",
            "type": "buy",
            "price": "150000000",
            "units": "0.0200",
            "units_remaining": "0.0200",
        },
        now_ts=1710000000000,
    )
    monkeypatch.setattr(broker, "get_open_orders", lambda: [open_order])
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {
            "status": "0000",
            "data": [
                "bad-row",
                123,
                {
                    "order_id": "filled-bad-ts",
                    "search": "sell",
                    "price": "152000000",
                    "units_traded": "0.0050",
                    "transfer_date": "not-a-timestamp",
                },
                {
                    "order_id": "filled-bad-qty",
                    "search": "sell",
                    "price": "152000000",
                    "units_traded": object(),
                    "transfer_date": "1710000001500",
                },
                {
                    "order_id": "filled-bad-price",
                    "search": "sell",
                    "price": "not-a-price",
                    "units_traded": "0.0050",
                    "transfer_date": "1710000001600",
                },
                {
                    "order_id": "filled-good",
                    "search": "sell",
                    "price": "152000000",
                    "units_traded": "0.0050",
                    "transfer_date": "1710000002000",
                },
            ],
        },
    )

    recent = broker.get_recent_orders(limit=10)

    by_id = {str(order.exchange_order_id): order for order in recent}
    assert set(by_id) == {"open-1", "filled-good"}
    assert by_id["open-1"].status == "NEW"
    assert by_id["filled-good"].status == "FILLED"
    assert by_id["filled-good"].qty_filled == pytest.approx(0.005)
    assert by_id["filled-good"].price == pytest.approx(152000000.0)


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


def test_get_order_maps_open_order_without_fills_to_new(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_post_private(endpoint, payload, retry_safe=False):
        if endpoint == "/info/orders":
            return {
                "status": "0000",
                "data": [
                    {
                        "order_id": "open-new-1",
                        "type": "buy",
                        "price": "150000000",
                        "units": "0.0200",
                        "units_remaining": "0.0200",
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.get_order(client_order_id="cid-new-1", exchange_order_id="open-new-1")

    assert order.status == "NEW"
    assert order.qty_req == pytest.approx(0.02)
    assert order.qty_filled == pytest.approx(0.0)


def test_get_order_maps_open_order_with_partial_fills_to_partial(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_post_private(endpoint, payload, retry_safe=False):
        if endpoint == "/info/orders":
            return {
                "status": "0000",
                "data": [
                    {
                        "order_id": "open-partial-1",
                        "type": "sell",
                        "price": "151000000",
                        "units": "0.1000",
                        "units_remaining": "0.0300",
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.get_order(client_order_id="cid-partial-1", exchange_order_id="open-partial-1")

    assert order.status == "PARTIAL"
    assert order.qty_req == pytest.approx(0.1)
    assert order.qty_filled == pytest.approx(0.07)


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


def test_get_order_rejects_incomplete_closed_lookup_missing_quantity_fields(monkeypatch):
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
                        "order_id": "ambig-2",
                        "type": "sell",
                        "price": "149000000",
                        # Broker occasionally omits quantity fields for stale/invalid lookups.
                    }
                ],
            }
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    with pytest.raises(BrokerRejectError, match="ambiguous"):
        broker.get_order(client_order_id="cid-5", exchange_order_id="ambig-2")


def test_read_journal_summary_masks_sensitive_balance_fields(monkeypatch):
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
                "api_nonce": "123",
                "api_key": "secret",
                "sign": "sig",
            },
        },
    )

    broker.get_balance()
    summary = broker.get_read_journal_summary()

    assert "/info/balance" in summary
    assert "available_krw" in summary["/info/balance"]
    assert "in_use_krw" in summary["/info/balance"]
    assert "api_nonce" not in summary["/info/balance"]
    assert "api_key" not in summary["/info/balance"]
    assert "sign" not in summary["/info/balance"]


def test_recent_orders_journal_summary_captures_sample_order_ids(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(broker, "get_open_orders", lambda: [])
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {
            "status": "0000",
            "data": [
                {"order_id": "filled-1", "search": "buy", "price": "100", "units_traded": "0.1", "transfer_date": "1"},
                {"order_id": "filled-2", "search": "sell", "price": "101", "units_traded": "0.2", "transfer_date": "2"},
            ],
        },
    )

    broker.get_recent_orders(limit=10)
    summary = broker.get_read_journal_summary()

    assert "/info/user_transactions(recent_orders)" in summary
    assert "sample_order_ids" in summary["/info/user_transactions(recent_orders)"]
    assert "filled-1" in summary["/info/user_transactions(recent_orders)"]
