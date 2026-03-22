from __future__ import annotations

import base64
import hashlib
import json
from urllib.parse import urlencode

import httpx
import pytest

from bithumb_bot.broker.bithumb import BithumbBroker, BithumbPrivateAPI
from bithumb_bot.broker.base import BrokerRejectError, BrokerTemporaryError
from bithumb_bot.config import settings
from decimal import Decimal

_HTTPX_TIMEOUT = getattr(httpx, "ReadTimeout", getattr(httpx, "RequestError"))
_HTTPX_CONNECT = getattr(httpx, "ConnectError", getattr(httpx, "RequestError"))


class _SequencedClient:
    actions: list[object] = []
    calls = 0
    requests: list[dict[str, object]] = []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def request(self, method: str, endpoint: str, headers: dict[str, str] | None = None, **kwargs):
        type(self).calls += 1
        type(self).requests.append(
            {"method": method, "endpoint": endpoint, "headers": headers or {}, **kwargs}
        )
        action = type(self).actions.pop(0)
        if isinstance(action, Exception):
            raise action
        return action



def _mk_response(status_code: int, payload: dict | list) -> httpx.Response:
    req = httpx.Request("GET", "https://api.bithumb.com/private")
    return httpx.Response(status_code, json=payload, request=req)



def _decode_jwt(token: str) -> dict[str, object]:
    _header, payload, _signature = token.split(".")
    padded = payload + "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode()).decode())



def _configure_live():
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")



def test_private_timeout_is_temporary_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_HTTPX_TIMEOUT("timeout")]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerTemporaryError):
        broker._get_private("/v1/accounts", {}, retry_safe=False)



def test_private_http_error_includes_sanitized_response_body(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        _mk_response(
            400,
            {
                "error": {"message": "bad request"},
                "api_key": "should-not-leak",
                "nonce": "12345",
            },
        )
    ]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError) as excinfo:
        broker._get_private("/v1/orders", {"market": "KRW-BTC"}, retry_safe=False)

    message = str(excinfo.value)
    assert "status=400" in message
    assert "bad request" in message
    assert "api_key" not in message
    assert "nonce" not in message



def test_private_safe_call_retries_temporary_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        _HTTPX_CONNECT("down"),
        _mk_response(200, [{"currency": "KRW", "balance": "1000", "locked": "0"}]),
    ]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    sleeps: list[float] = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda sec: sleeps.append(sec))

    broker = BithumbBroker()
    data = broker._get_private("/v1/accounts", {}, retry_safe=True)

    assert isinstance(data, list)
    assert _SequencedClient.calls == 2
    assert sleeps == [0.2]



def test_query_string_for_order_chance_market_is_exact():
    assert BithumbPrivateAPI._query_string({"market": "KRW-BTC"}) == "market=KRW-BTC"
    claims = BithumbPrivateAPI._query_hash_claims({"market": "KRW-BTC"})
    assert claims == {
        "query_hash": "b749dfc2e17f75e5b46c8161f97fe7c9298ed4167ea21c5c94d16573efd8a801351470c0ff1a9a3f1e763f8249968218c04c571c8b45aa80cd4588e6c4be0738",
        "query_hash_alg": "SHA512",
    }


def test_query_string_preserves_bithumb_array_brackets():
    assert BithumbPrivateAPI._query_string({"uuids": ["order-1", "order-2"], "state": "wait"}) == (
        "uuids[]=order-1&uuids[]=order-2&state=wait"
    )


def test_canonical_payload_for_query_hash_matches_order_submit_payload():
    payload = {
        "market": "KRW-BTC",
        "side": "bid",
        "price": "9999",
        "ord_type": "price",
    }

    assert BithumbPrivateAPI._canonical_payload_for_query_hash(payload) == (
        "market=KRW-BTC&side=bid&price=9999&ord_type=price"
    )
    assert BithumbPrivateAPI._query_string(payload) == "market=KRW-BTC&side=bid&price=9999&ord_type=price"


def test_order_submit_query_hash_matches_official_urlencode_sha512_rule():
    payload = {
        "market": "KRW-BTC",
        "side": "bid",
        "price": "9998",
        "ord_type": "price",
    }

    official_query = urlencode(
        [
            ("market", "KRW-BTC"),
            ("side", "bid"),
            ("price", "9998"),
            ("ord_type", "price"),
        ],
        doseq=False,
        safe="[]",
    )
    official_hash = hashlib.sha512(official_query.encode("utf-8")).hexdigest()

    assert BithumbPrivateAPI._canonical_payload_for_query_hash(payload) == official_query
    assert BithumbPrivateAPI._query_hash_from_canonical_payload(official_query) == {
        "query_hash": official_hash,
        "query_hash_alg": "SHA512",
    }


def test_build_order_rules_market_uses_v1_symbol():
    from bithumb_bot.broker.order_rules import build_order_rules_market

    assert build_order_rules_market("BTC") == "KRW-BTC"


def test_private_jwt_headers_include_query_hash_for_get(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, [])]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    broker._get_private("/v1/orders", {"market": "KRW-BTC", "state": "wait"}, retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))

    assert claims["access_key"] == "k"
    assert "nonce" in claims
    assert "timestamp" in claims
    assert "query_hash" in claims
    assert call["params"] == {"market": "KRW-BTC", "state": "wait"}



def test_private_jwt_headers_include_query_hash_for_post_and_form_body(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    broker._post_private("/v2/orders", {"market": "KRW-BTC", "side": "ask", "volume": "0.1", "ord_type": "market"}, retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))

    assert claims["access_key"] == "k"
    assert "query_hash" in claims
    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert call["content"] == b"market=KRW-BTC&side=ask&volume=0.1&ord_type=market"
    assert "json" not in call



def test_balance_parses_available_and_locked(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": "1000", "locked": "25"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ],
    )

    bal = broker.get_balance()

    assert bal.cash_available == 1000.0
    assert bal.cash_locked == 25.0
    assert bal.asset_available == 0.1
    assert bal.asset_locked == 0.02



def test_order_chance_uses_private_v1_endpoint(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["params"] = params
        call["retry_safe"] = retry_safe
        return {"market": {"bid": {"min_total": "5000"}}}

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    payload = broker.get_order_chance(market="KRW-BTC")

    assert payload["market"]["bid"]["min_total"] == "5000"
    assert call == {
        "endpoint": "/v1/orders/chance",
        "params": {"market": "KRW-BTC"},
        "retry_safe": True,
    }



def test_place_order_market_buy_routes_to_v2_price_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "mkt-1"}

    monkeypatch.setattr("bithumb_bot.broker.bithumb.fetch_orderbook_top", lambda _pair: (149_000_000.0, 150_000_000.0))
    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-1", side="BUY", qty=0.1234, price=None)

    assert order.exchange_order_id == "mkt-1"
    assert call["endpoint"] == "/v2/orders"
    assert call["retry_safe"] is False
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "price": str(int(Decimal("150000000.0") * Decimal("0.1234"))),
        "ord_type": "price",
    }



def test_place_order_market_sell_routes_to_v2_market_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "mkt-2"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-2", side="SELL", qty=0.4321, price=None)

    assert order.exchange_order_id == "mkt-2"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "ask",
        "volume": "0.4321",
        "ord_type": "market",
    }



def test_place_order_limit_buy_uses_v2_limit_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "lmt-2"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-3", side="BUY", qty=0.4, price=149500000)

    assert order.exchange_order_id == "lmt-2"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "volume": "0.4",
        "price": "149500000",
        "ord_type": "limit",
    }



def test_recent_orders_includes_done_and_cancel_states(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint, params, retry_safe=False):
        assert endpoint == "/v1/orders"
        state = params["state"]
        if state == "wait":
            return [{"uuid": "open-1", "side": "bid", "price": "150000000", "volume": "0.02", "remaining_volume": "0.02", "state": "wait"}]
        if state == "done":
            return [{"uuid": "filled-1", "side": "ask", "price": "151000000", "volume": "0.01", "remaining_volume": "0", "executed_volume": "0.01", "state": "done"}]
        if state == "cancel":
            return [{"uuid": "cancel-1", "side": "bid", "price": "149000000", "volume": "0.03", "remaining_volume": "0.02", "executed_volume": "0.01", "state": "cancel"}]
        raise AssertionError(state)

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    recent = broker.get_recent_orders(limit=10)

    by_id = {str(order.exchange_order_id): order for order in recent}
    assert by_id["open-1"].status == "NEW"
    assert by_id["filled-1"].status == "FILLED"
    assert by_id["cancel-1"].status == "CANCELED"
    assert by_id["filled-1"].side == "SELL"



def test_get_open_orders_uses_wait_state(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["params"] = params
        return [{"uuid": "open-1", "side": "bid", "price": "150000000", "volume": "0.02", "remaining_volume": "0.02", "state": "wait"}]

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    open_orders = broker.get_open_orders()

    assert call == {
        "endpoint": "/v1/orders",
        "params": {"market": "KRW-BTC", "state": "wait", "limit": 100},
    }
    assert len(open_orders) == 1
    assert open_orders[0].exchange_order_id == "open-1"
    assert open_orders[0].side == "BUY"



def test_get_order_uses_v1_order_lookup(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["params"] = params
        return {
            "uuid": "filled-1",
            "side": "bid",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "state": "done",
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    order = broker.get_order(client_order_id="cid-3", exchange_order_id="filled-1")

    assert call == {"endpoint": "/v1/order", "params": {"uuid": "filled-1"}}
    assert order.status == "FILLED"
    assert order.qty_req == pytest.approx(0.05)
    assert order.qty_filled == pytest.approx(0.05)



def test_get_fills_prefers_embedded_trade_rows(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-1",
            "price": "149000000",
            "volume": "0.05",
            "executed_volume": "0.05",
            "state": "done",
            "trades": [
                {"uuid": "t1", "price": "149000000", "volume": "0.02", "fee": "10", "created_at": "2024-01-01T00:00:00+00:00"},
                {"uuid": "t2", "price": "149500000", "volume": "0.03", "fee": "12", "created_at": "2024-01-01T00:00:01+00:00"},
            ],
        },
    )

    fills = broker.get_fills(client_order_id="cid-1", exchange_order_id="filled-1")

    assert [fill.fill_id for fill in fills] == ["t1", "t2"]
    assert fills[0].qty == pytest.approx(0.02)
    assert fills[1].fee == pytest.approx(12.0)



def test_read_journal_summary_masks_sensitive_balance_fields(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": "1000", "locked": "25", "api_nonce": "123", "api_key": "secret", "authorization": "sig"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ],
    )

    broker.get_balance()
    summary = broker.get_read_journal_summary()

    assert "/v1/accounts" in summary
    assert "api_nonce" not in summary["/v1/accounts"]
    assert "api_key" not in summary["/v1/accounts"]
    assert "authorization" not in summary["/v1/accounts"]



def test_recent_orders_journal_summary_captures_sample_order_ids(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint, params, retry_safe=False):
        if params["state"] == "wait":
            return []
        if params["state"] == "done":
            return [{"uuid": "filled-1", "side": "bid", "price": "100", "volume": "0.1", "executed_volume": "0.1", "state": "done"}]
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    broker.get_recent_orders(limit=10)
    summary = broker.get_read_journal_summary()

    assert "/v1/orders(done)" in summary
    assert "sample_order_ids" in summary["/v1/orders(done)"]
    assert "filled-1" in summary["/v1/orders(done)"]


def test_private_non_order_posts_keep_json_body(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"ok": True})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    api.request("POST", "/v2/orders/cancel", json_body={"order_id": "abc123"}, retry_safe=False)

    call = _SequencedClient.requests[0]
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["json"] == {"order_id": "abc123"}
    assert "content" not in call



def test_order_submit_uses_dedicated_auth_builder(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    calls: list[dict[str, object]] = []
    original = api._order_submit_auth_context

    def _spy(payload, *, nonce=None, timestamp=None):
        calls.append({"payload": payload, "nonce": nonce, "timestamp": timestamp})
        return original(payload, nonce=nonce, timestamp=timestamp)

    monkeypatch.setattr(api, "_order_submit_auth_context", _spy)

    payload = {"market": "KRW-BTC", "side": "ask", "volume": "0.1", "ord_type": "market"}
    api.request("POST", "/v2/orders", json_body=payload, retry_safe=False)

    assert len(calls) == 2
    assert calls[0]["payload"] == payload
    assert calls[1]["payload"] == payload
    assert calls[0]["nonce"] == calls[1]["nonce"]
    assert calls[0]["timestamp"] == calls[1]["timestamp"]


def test_order_submit_auth_context_matches_official_claim_contract(monkeypatch):
    _configure_live()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.uuid.uuid4", lambda: "nonce-fixed")
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.time", lambda: 1712230310.689)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    payload = {"market": "KRW-BTC", "side": "bid", "price": "9998", "ord_type": "price"}

    context = api._order_submit_auth_context(payload)

    assert context["canonical_payload"] == "market=KRW-BTC&side=bid&price=9998&ord_type=price"
    assert context["request_content"] == b"market=KRW-BTC&side=bid&price=9998&ord_type=price"
    assert context["query_hash_claims"] == {
        "query_hash": hashlib.sha512(context["request_content"]).hexdigest(),
        "query_hash_alg": "SHA512",
    }
    assert context["claims"] == {
        "access_key": "k",
        "nonce": "nonce-fixed",
        "timestamp": 1712230310689,
        "query_hash": hashlib.sha512(context["request_content"]).hexdigest(),
        "query_hash_alg": "SHA512",
    }
    assert context["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert context["headers"]["Authorization"].startswith("Bearer ")
    assert context["request_kwargs"] == {
        "content": b"market=KRW-BTC&side=bid&price=9998&ord_type=price",
    }



def test_non_order_post_does_not_use_order_submit_auth_builder(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"ok": True})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)

    def _boom(*args, **kwargs):
        raise AssertionError("order submit auth builder should not be used")

    monkeypatch.setattr(api, "_order_submit_auth_context", _boom)

    api.request("POST", "/v2/orders/cancel", json_body={"order_id": "abc123"}, retry_safe=False)

    call = _SequencedClient.requests[0]
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["json"] == {"order_id": "abc123"}


@pytest.mark.parametrize(
    ("payload", "expected_content"),
    [
        (
            {"market": "KRW-BTC", "side": "bid", "price": "9999", "ord_type": "price"},
            b"market=KRW-BTC&side=bid&price=9999&ord_type=price",
        ),
        (
            {"market": "KRW-BTC", "side": "ask", "volume": "0.1", "ord_type": "market"},
            b"market=KRW-BTC&side=ask&volume=0.1&ord_type=market",
        ),
        (
            {"market": "KRW-BTC", "side": "bid", "volume": "0.4", "price": "149500000", "ord_type": "limit"},
            b"market=KRW-BTC&side=bid&volume=0.4&price=149500000&ord_type=limit",
        ),
    ],
)
def test_order_submit_uses_form_encoded_body_consistently(monkeypatch, payload, expected_content):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    broker._post_private("/v2/orders", payload, retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))

    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert call["content"] == expected_content
    assert "json" not in call
    assert claims["query_hash"] == BithumbPrivateAPI._query_hash_claims(payload)["query_hash"]





def test_order_submit_jwt_uses_same_canonical_payload_nonce_and_timestamp(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.uuid.uuid4", lambda: "nonce-fixed")
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.time", lambda: 1712230310.689)

    payload = {"market": "KRW-BTC", "side": "bid", "price": "10002", "ord_type": "price"}
    broker = BithumbBroker()
    broker._post_private("/v2/orders", payload, retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))
    canonical_payload = call["content"].decode()

    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert canonical_payload == "market=KRW-BTC&side=bid&price=10002&ord_type=price"
    assert claims["nonce"] == "nonce-fixed"
    assert claims["timestamp"] == 1712230310689
    assert claims["query_hash"] == BithumbPrivateAPI._query_hash_from_canonical_payload(canonical_payload)["query_hash"]
    assert claims["query_hash_alg"] == "SHA512"


def test_order_http_debug_request_logs_matching_signed_and_transmitted_payload(monkeypatch, caplog):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        broker._post_private(
            "/v2/orders",
            {"market": "KRW-BTC", "side": "bid", "price": "9999", "ord_type": "price"},
            retry_safe=False,
        )

    order_logs = [record.message for record in caplog.records if "[ORDER_HTTP_DEBUG] request" in record.message]
    assert order_logs
    assert "content_type=application/x-www-form-urlencoded" in order_logs[-1]
    assert "signed_payload_repr='market=KRW-BTC&side=bid&price=9999&ord_type=price'" in order_logs[-1]
    assert "transmitted_payload_repr='market=KRW-BTC&side=bid&price=9999&ord_type=price'" in order_logs[-1]


def test_order_http_debug_response_body_masks_sensitive_fields(monkeypatch, caplog):
    _configure_live()
    _SequencedClient.actions = [_mk_response(400, {"error": {"message": "Invalid request"}, "api_key": "leak-me"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        with pytest.raises(BrokerRejectError):
            broker._post_private("/v2/orders", {"market": "KRW-BTC", "side": "ask", "volume": "0.1"}, retry_safe=False)

    order_logs = [record.message for record in caplog.records if "[ORDER_HTTP_DEBUG] response" in record.message]
    assert order_logs
    assert "Invalid request" in order_logs[-1]
    assert "api_key" not in order_logs[-1]


def test_order_submit_live_failure_regression_uses_canonical_form_bytes_and_matching_hash(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(401, {"error": {"name": "invalid_query_payload"}})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.uuid.uuid4", lambda: "nonce-fixed")
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.time", lambda: 1712230310.689)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    payload = {"market": "KRW-BTC", "side": "bid", "price": "9998", "ord_type": "price"}

    with pytest.raises(BrokerRejectError) as excinfo:
        api.request("POST", "/v2/orders", json_body=payload, retry_safe=False)

    assert "status=401" in str(excinfo.value)
    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))
    canonical_payload = "market=KRW-BTC&side=bid&price=9998&ord_type=price"

    assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert call["content"] == canonical_payload.encode("utf-8")
    assert "json" not in call
    assert claims == {
        "access_key": "k",
        "nonce": "nonce-fixed",
        "timestamp": 1712230310689,
        "query_hash": hashlib.sha512(canonical_payload.encode("utf-8")).hexdigest(),
        "query_hash_alg": "SHA512",
    }
