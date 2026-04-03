from __future__ import annotations

import base64
import hashlib
import json
import logging
from urllib.parse import urlencode

import httpx
import pytest

from bithumb_bot.broker.bithumb import BithumbBroker, BithumbPrivateAPI, classify_private_api_error
from bithumb_bot.broker.base import (
    BrokerIdentifierMismatchError,
    BrokerRejectError,
    BrokerSchemaError,
    BrokerTemporaryError,
)
from bithumb_bot.config import settings
from bithumb_bot.public_api_orderbook import BestQuote
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



@pytest.fixture(autouse=True)
def _stub_canonical_market(monkeypatch):
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")
    monkeypatch.setattr("bithumb_bot.broker.order_rules.canonical_market_id", lambda _market: "KRW-BTC")


@pytest.fixture(autouse=True)
def _stub_order_rules(monkeypatch):
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "bid_min_total_krw": 5000.0,
                        "ask_min_total_krw": 5000.0,
                        "bid_price_unit": 1.0,
                        "ask_price_unit": 1.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )



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


def test_classify_private_api_error_categories_cover_v1_orders_contract_failures() -> None:
    code, summary = classify_private_api_error(BrokerRejectError("/v1/orders schema mismatch: unknown state 'halted'"))
    assert code == "DOC_SCHEMA"
    assert "schema mismatch" in summary

    code, summary = classify_private_api_error(
        BrokerRejectError("open order lookup requires identifiers; broad /v1/orders market/state scans are disabled")
    )
    assert code == "RECOVERY_REQUIRED"
    assert "identifier-based lookup" in summary

    code, summary = classify_private_api_error(BrokerRejectError("rejected with http status=401 body=invalid jwt"))
    assert code == "AUTH_SIGN"

    code, summary = classify_private_api_error(BrokerTemporaryError("bithumb private /v1/orders transport error"))
    assert code == "TEMPORARY"

    code, summary = classify_private_api_error(BrokerIdentifierMismatchError("order lookup response exchange_order_id mismatch"))
    assert code == "IDENTIFIER_MISMATCH"
    assert "identifiers conflict" in summary

    code, summary = classify_private_api_error(BrokerSchemaError("order lookup response schema mismatch: expected object payload"))
    assert code == "DOC_SCHEMA"



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



def test_private_jwt_headers_include_query_hash_for_post_and_json_body(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"order_id": "created-1"})]
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
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["content"] == b'{"market":"KRW-BTC","side":"ask","volume":"0.1","ord_type":"market"}'
    assert "json" not in call



def test_accounts_rest_balance_parses_available_and_locked(monkeypatch):
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


def test_accounts_rest_balance_uses_split_accounts_layers(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    calls: list[str] = []

    monkeypatch.setattr(
        broker,
        "fetch_accounts_raw",
        lambda: [
            {"currency": "KRW", "balance": "1000", "locked": "25"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ],
    )

    from bithumb_bot.broker.accounts_v1 import AccountRow, PairBalances, ParsedAccounts

    def _fake_parse(data):
        calls.append("parse")
        assert isinstance(data, list)
        return ParsedAccounts(
            rows=(
                AccountRow(currency="KRW", balance=Decimal("1000"), locked=Decimal("25")),
                AccountRow(currency="BTC", balance=Decimal("0.1"), locked=Decimal("0.02")),
            ),
            balances={
                "KRW": (Decimal("1000"), Decimal("25")),
                "BTC": (Decimal("0.1"), Decimal("0.02")),
            },
            row_count=2,
            currencies=("BTC", "KRW"),
            duplicate_currencies=(),
        )

    def _fake_select(accounts, *, order_currency, payment_currency, allow_missing_base=False):
        calls.append("select")
        assert order_currency == "BTC"
        assert payment_currency == "KRW"
        assert allow_missing_base is False
        return PairBalances(
            cash_balance=accounts.balances["KRW"][0],
            cash_locked=accounts.balances["KRW"][1],
            asset_balance=accounts.balances["BTC"][0],
            asset_locked=accounts.balances["BTC"][1],
        )

    monkeypatch.setattr("bithumb_bot.broker.bithumb.parse_accounts_response", _fake_parse)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.select_pair_balances", _fake_select)

    bal = broker.get_balance()

    assert calls == ["parse", "select"]
    assert bal.cash_available == 1000.0
    assert bal.asset_available == 0.1


def test_accounts_rest_balance_rejects_non_array_payload(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(broker, "_get_private", lambda endpoint, params, retry_safe=False: {"currency": "KRW"})

    with pytest.raises(BrokerRejectError, match=r"/v1/accounts schema mismatch: expected array payload"):
        broker.get_balance()


def test_accounts_rest_balance_rejects_non_object_row(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [{"currency": "KRW", "balance": "1000", "locked": "0"}, "bad-row"],
    )

    with pytest.raises(BrokerRejectError, match=r"/v1/accounts\[1\] schema mismatch: expected object row"):
        broker.get_balance()


@pytest.mark.parametrize("currency", [None, "", "   "])
def test_accounts_rest_balance_rejects_missing_or_empty_currency(monkeypatch, currency):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": currency, "balance": "1000", "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.01"},
        ],
    )

    with pytest.raises(BrokerRejectError, match="missing required text field 'currency'"):
        broker.get_balance()


@pytest.mark.parametrize("field", ["balance", "locked"])
def test_accounts_rest_balance_rejects_missing_required_numeric_fields(monkeypatch, field):
    _configure_live()
    broker = BithumbBroker()
    krw_row = {"currency": "KRW", "balance": "1000", "locked": "25"}
    del krw_row[field]
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [krw_row, {"currency": "BTC", "balance": "0.1", "locked": "0.02"}],
    )

    with pytest.raises(BrokerRejectError, match=rf"missing required numeric field '{field}'"):
        broker.get_balance()


@pytest.mark.parametrize("bad_value", ["abc", object()])
def test_accounts_rest_balance_rejects_non_numeric_values(monkeypatch, bad_value):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": bad_value, "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ],
    )

    with pytest.raises(BrokerRejectError, match="invalid numeric field 'balance'"):
        broker.get_balance()


@pytest.mark.parametrize("bad_value", ["-1", "-0.0001", "NaN", "Infinity", "-Infinity"])
def test_accounts_rest_balance_rejects_negative_or_non_finite_values(monkeypatch, bad_value):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": bad_value, "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ],
    )

    with pytest.raises(BrokerRejectError, match="schema mismatch: (negative|non-finite) numeric field 'balance'"):
        broker.get_balance()


def test_accounts_rest_balance_rejects_duplicate_currency_rows(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": "1000", "locked": "0"},
            {"currency": "KRW", "balance": "2000", "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ],
    )

    with pytest.raises(BrokerRejectError, match="duplicate currency row 'KRW'"):
        broker.get_balance()


def test_accounts_rest_balance_rejects_missing_pair_currency_rows(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [{"currency": "KRW", "balance": "1000", "locked": "0"}],
    )

    with pytest.raises(BrokerRejectError, match="missing base currency row 'BTC'"):
        broker.get_balance()


def test_accounts_rest_balance_rejects_missing_quote_currency_row(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [{"currency": "BTC", "balance": "0.1", "locked": "0.02"}],
    )

    with pytest.raises(BrokerRejectError, match="missing quote currency row 'KRW'"):
        broker.get_balance()


def test_accounts_rest_balance_records_v1_accounts_diag_on_success(monkeypatch):
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

    broker.get_balance()
    diag = broker.get_accounts_validation_diagnostics()

    assert diag["reason"] == "ok"
    assert diag["row_count"] == 2
    assert diag["currencies"] == ["BTC", "KRW"]
    assert diag["missing_required_currencies"] == []
    assert diag["duplicate_currencies"] == []
    assert diag["last_success_reason"] == "ok"
    assert diag["execution_mode"] == "live_real_order_path"
    assert diag["quote_currency"] == "KRW"
    assert diag["base_currency"] == "BTC"
    assert diag["preflight_outcome"] == "pass"


def test_accounts_rest_balance_records_v1_accounts_diag_on_required_currency_missing(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": "1000", "locked": "25"},
        ],
    )

    with pytest.raises(BrokerRejectError, match="missing base currency row 'BTC'"):
        broker.get_balance()

    diag = broker.get_accounts_validation_diagnostics()
    assert diag["reason"] == "required currency missing"
    assert diag["row_count"] == 1
    assert diag["currencies"] == ["KRW"]
    assert diag["missing_required_currencies"] == ["BTC"]
    assert diag["duplicate_currencies"] == []
    assert diag["last_failure_reason"] == "required currency missing"
    assert diag["execution_mode"] == "live_real_order_path"
    assert diag["preflight_outcome"] == "fail_real_order_blocked"


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


def test_order_chance_rejects_noncanonical_market_before_request(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: calls.append(
            {"endpoint": endpoint, "params": params, "retry_safe": retry_safe}
        ),
    )

    with pytest.raises(BrokerRejectError, match="canonical QUOTE-BASE"):
        broker.get_order_chance(market="BTC_KRW")
    assert calls == []


def test_order_chance_keeps_market_param_and_auth_query_hash(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"market": {"id": "KRW-BTC"}})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    broker.get_order_chance(market="KRW-BTC")

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))

    assert call["endpoint"] == "/v1/orders/chance"
    assert call["params"] == {"market": "KRW-BTC"}
    assert claims["query_hash"] == hashlib.sha512(b"market=KRW-BTC").hexdigest()
    assert claims["query_hash_alg"] == "SHA512"



def test_place_order_market_buy_routes_to_v2_price_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "mkt-1"}

    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=150_000_000.0),
    )
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
        "client_order_id": "cid-1",
    }


def test_place_order_accepts_valid_client_order_id_format(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}
    valid_client_order_id = "Abc_123-xyz_456-7890_ABC-def-ghi-jkl"

    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=150_000_000.0),
    )

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        return {"uuid": "mkt-valid-cid-1", "client_order_id": payload["client_order_id"]}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id=valid_client_order_id, side="BUY", qty=0.1234, price=None)

    assert order.client_order_id == valid_client_order_id
    assert call["payload"]["client_order_id"] == valid_client_order_id


def test_place_order_rejects_empty_client_order_id():
    _configure_live()
    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="must not be empty"):
        broker.place_order(client_order_id="", side="BUY", qty=0.1, price=149000000)


def test_place_order_rejects_overlength_client_order_id():
    _configure_live()
    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="at most 36 characters"):
        broker.place_order(client_order_id=("a" * 37), side="BUY", qty=0.1, price=149000000)


def test_place_order_rejects_invalid_characters_in_client_order_id():
    _configure_live()
    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="contains invalid characters"):
        broker.place_order(client_order_id="cid invalid", side="BUY", qty=0.1, price=149000000)


def test_place_order_market_buy_blocks_invalid_ask_quote(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=0.0),
    )

    with pytest.raises(BrokerTemporaryError, match="failed to load validated best ask"):
        broker.place_order(client_order_id="cid-invalid-ask", side="BUY", qty=0.1234, price=None)


def test_place_order_market_buy_blocks_on_quote_fetch_failure(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("orderbook offline")),
    )

    with pytest.raises(BrokerTemporaryError, match="failed to load validated best ask"):
        broker.place_order(client_order_id="cid-quote-fail", side="BUY", qty=0.1234, price=None)



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
        "client_order_id": "cid-2",
    }



def test_place_order_limit_buy_uses_v2_limit_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {
            "uuid": "lmt-2",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "client_order_id": "cid-3",
        }

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
        "client_order_id": "cid-3",
    }
    assert order.raw == {
        "market": "KRW-BTC",
        "ord_type": "limit",
        "client_order_id": "cid-3",
    }


def test_place_order_preserves_local_client_order_id_when_response_omits_it(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {
            "uuid": "lmt-omit-client-id",
            "market": "KRW-BTC",
            "ord_type": "limit",
        }

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    order = broker.place_order(client_order_id="cid-omit", side="BUY", qty=0.4, price=149500000)

    assert order.exchange_order_id == "lmt-omit-client-id"
    assert call["payload"]["client_order_id"] == "cid-omit"
    assert order.raw == {
        "market": "KRW-BTC",
        "ord_type": "limit",
        "client_order_id": "cid-omit",
    }


def test_place_order_rejects_when_response_client_order_id_mismatches(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda *_args, **_kwargs: {
            "uuid": "lmt-client-mismatch",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "client_order_id": "cid-other",
        },
    )

    with pytest.raises(BrokerRejectError, match="order submit response client_order_id mismatch"):
        broker.place_order(client_order_id="cid-local", side="BUY", qty=0.4, price=149500000)


def test_place_order_accepts_coid_alias_when_client_order_id_missing(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda *_args, **_kwargs: {
            "uuid": "lmt-coid-only",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "coid": "cid-coid-only",
        },
    )

    order = broker.place_order(client_order_id="cid-coid-only", side="BUY", qty=0.4, price=149500000)

    assert order.exchange_order_id == "lmt-coid-only"
    assert order.client_order_id == "cid-coid-only"


def test_place_order_rejects_when_client_order_id_and_coid_conflict(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda *_args, **_kwargs: {
            "uuid": "lmt-coid-mismatch",
            "client_order_id": "cid-primary",
            "coid": "cid-other",
        },
    )

    with pytest.raises(BrokerRejectError, match="client identifier mismatch"):
        broker.place_order(client_order_id="cid-primary", side="BUY", qty=0.4, price=149500000)


def test_place_order_limit_rejects_price_not_aligned_with_side_price_unit(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "bid_min_total_krw": 5000.0,
                        "ask_min_total_krw": 5000.0,
                        "bid_price_unit": 10.0,
                        "ask_price_unit": 1.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )

    with pytest.raises(BrokerRejectError, match="limit price does not match side price_unit"):
        broker.place_order(client_order_id="cid-lmt-unit", side="BUY", qty=0.01, price=149500001)


def test_place_order_limit_rejects_when_side_min_total_not_met(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "bid_min_total_krw": 5000.0,
                        "ask_min_total_krw": 7000.0,
                        "bid_price_unit": 1.0,
                        "ask_price_unit": 1.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )

    with pytest.raises(BrokerRejectError, match="order notional below side minimum for limit order"):
        broker.place_order(client_order_id="cid-lmt-min", side="SELL", qty=0.00001, price=100000000)



def test_recent_orders_includes_done_and_cancel_states(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint, params, retry_safe=False):
        assert endpoint == "/v1/orders"
        state = params["state"]
        if state == "wait":
            return [{"uuid": "open-1", "market": "KRW-BTC", "ord_type": "limit", "side": "bid", "price": "150000000", "volume": "0.02", "remaining_volume": "0.02", "executed_volume": "0", "state": "wait", "created_at": "2024-01-01T00:00:00+00:00", "updated_at": "2024-01-01T00:00:00+00:00"}]
        if state == "done":
            return [{"uuid": "filled-1", "market": "KRW-BTC", "ord_type": "limit", "side": "ask", "price": "151000000", "volume": "0.01", "remaining_volume": "0", "executed_volume": "0.01", "state": "done", "created_at": "2024-01-01T00:00:00+00:00", "updated_at": "2024-01-01T00:01:00+00:00"}]
        if state == "cancel":
            return [{"uuid": "cancel-1", "market": "KRW-BTC", "ord_type": "limit", "side": "bid", "price": "149000000", "volume": "0.03", "remaining_volume": "0.02", "executed_volume": "0.01", "state": "cancel", "created_at": "2024-01-01T00:00:00+00:00", "updated_at": "2024-01-01T00:00:30+00:00"}]
        raise AssertionError(state)

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    recent = broker.get_recent_orders(limit=10, exchange_order_ids=["open-1", "filled-1", "cancel-1"])

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
        return [{"uuid": "open-1", "market": "KRW-BTC", "ord_type": "limit", "side": "bid", "price": "150000000", "volume": "0.02", "remaining_volume": "0.02", "executed_volume": "0", "state": "wait", "created_at": "2024-01-01T00:00:00+00:00", "updated_at": "2024-01-01T00:00:00+00:00"}]

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    open_orders = broker.get_open_orders(exchange_order_ids=["open-1"])

    assert call == {
        "endpoint": "/v1/orders",
        "params": {"uuids": ["open-1"], "state": "wait", "page": 1, "order_by": "desc"},
    }
    assert len(open_orders) == 1
    assert open_orders[0].exchange_order_id == "open-1"
    assert open_orders[0].side == "BUY"


def test_v1_orders_broad_scan_is_rejected_without_identifiers() -> None:
    _configure_live()
    broker = BithumbBroker()

    with pytest.raises(BrokerRejectError, match="requires identifiers"):
        broker.get_open_orders()

    with pytest.raises(BrokerRejectError, match="requires identifiers"):
        broker.get_recent_orders(limit=5)


def test_get_fills_rejects_broad_scan_without_identifiers() -> None:
    _configure_live()
    broker = BithumbBroker()

    with pytest.raises(BrokerRejectError, match="requires identifiers"):
        broker.get_fills()


def test_get_recent_fills_is_explicitly_unsupported() -> None:
    _configure_live()
    broker = BithumbBroker()

    with pytest.raises(BrokerRejectError, match="unsupported"):
        broker.get_recent_fills(limit=5)


def test_cancel_order_uses_v2_orders_cancel_with_order_id_and_client_order_id(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-1",
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.05",
                "state": "wait",
            },
            client_order_id=client_order_id,
        ),
    )

    def _fake_post(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"order_id": payload["order_id"], "client_order_id": payload["client_order_id"]}

    monkeypatch.setattr(broker, "_post_private", _fake_post)

    order = broker.cancel_order(client_order_id="cid-cancel", exchange_order_id="cancel-1")

    assert order.exchange_order_id == "cancel-1"
    assert order.status == "CANCEL_REQUESTED"
    assert call == {
        "endpoint": "/v2/orders/cancel",
        "payload": {"order_id": "cancel-1", "client_order_id": "cid-cancel"},
        "retry_safe": False,
    }


def test_cancel_order_accepts_client_order_id_only_response(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "client_order_id": client_order_id,
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.05",
                "state": "wait",
            },
            client_order_id=client_order_id,
        ),
    )

    def _fake_post(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        return {"client_order_id": payload["client_order_id"]}

    monkeypatch.setattr(broker, "_post_private", _fake_post)

    order = broker.cancel_order(client_order_id="cid-cancel-only", exchange_order_id=None)

    assert call == {
        "endpoint": "/v2/orders/cancel",
        "payload": {"client_order_id": "cid-cancel-only"},
    }
    assert order.client_order_id == "cid-cancel-only"
    assert order.exchange_order_id in ("", None, "dry_cid-cancel-only")
    assert order.status == "CANCEL_REQUESTED"


def test_cancel_order_maps_already_canceled_reject_to_canceled(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-1",
                "client_order_id": client_order_id,
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.05",
                "state": "wait",
            },
            client_order_id=client_order_id,
        ),
    )

    def _reject(*_args, **_kwargs):
        raise BrokerRejectError("order already canceled")

    monkeypatch.setattr(broker, "_post_private", _reject)
    order = broker.cancel_order(client_order_id="cid-cancel", exchange_order_id="cancel-1")
    assert order.status == "CANCELED"


def test_cancel_order_raises_on_order_id_mismatch(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-1",
                "client_order_id": client_order_id,
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.05",
                "state": "wait",
            },
            client_order_id=client_order_id,
        ),
    )
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {
            "order_id": "different-order-id",
            "client_order_id": payload["client_order_id"],
        },
    )

    with pytest.raises(BrokerRejectError, match="cancel response order_id mismatch"):
        broker.cancel_order(client_order_id="cid-cancel", exchange_order_id="cancel-1")


def test_cancel_order_accepts_coid_alias_when_client_order_id_missing(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-coid-1",
                "client_order_id": client_order_id,
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.05",
                "state": "wait",
            },
            client_order_id=client_order_id,
        ),
    )
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {
            "order_id": payload["order_id"],
            "coid": payload["client_order_id"],
        },
    )

    order = broker.cancel_order(client_order_id="cid-cancel-coid", exchange_order_id="cancel-coid-1")

    assert order.exchange_order_id == "cancel-coid-1"
    assert order.client_order_id == "cid-cancel-coid"


def test_cancel_order_rejects_when_client_order_id_and_coid_conflict(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-conflict-1",
                "client_order_id": client_order_id,
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.05",
                "state": "wait",
            },
            client_order_id=client_order_id,
        ),
    )
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {
            "order_id": payload["order_id"],
            "client_order_id": payload["client_order_id"],
            "coid": "different-cid",
        },
    )

    with pytest.raises(BrokerRejectError, match="client identifier mismatch"):
        broker.cancel_order(client_order_id="cid-cancel-conflict", exchange_order_id="cancel-conflict-1")


def test_get_order_uses_v1_order_lookup(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["params"] = params
        return {
            "uuid": "filled-1",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "client_order_id": "exchange-cid-3",
            "side": "bid",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "done",
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    order = broker.get_order(client_order_id="cid-3", exchange_order_id="filled-1")

    assert call == {"endpoint": "/v1/order", "params": {"uuid": "filled-1"}}
    assert order.status == "FILLED"
    assert order.qty_req == pytest.approx(0.05)
    assert order.qty_filled == pytest.approx(0.05)
    assert order.raw is not None
    assert order.raw["market"] == "KRW-BTC"
    assert order.raw["ord_type"] == "limit"
    assert order.raw["client_order_id"] == "exchange-cid-3"
    assert order.raw["uuid"] == "filled-1"


def test_get_order_prefers_uuid_when_client_order_id_is_invalid(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["params"] = params
        return {
            "uuid": "filled-priority-1",
            "client_order_id": "cid-priority",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "bid",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "done",
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    order = broker.get_order(client_order_id="bad id with space", exchange_order_id="filled-priority-1")

    assert call == {"endpoint": "/v1/order", "params": {"uuid": "filled-priority-1"}}
    assert order.exchange_order_id == "filled-priority-1"


def test_get_order_lookup_identifier_priority_prefers_uuid_over_client_order_id(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["params"] = params
        return {
            "uuid": "filled-priority-2",
            "client_order_id": "cid-priority-2",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "bid",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "done",
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    order = broker.get_order(client_order_id="cid-priority-2", exchange_order_id="filled-priority-2")

    assert call == {"endpoint": "/v1/order", "params": {"uuid": "filled-priority-2"}}
    assert order.exchange_order_id == "filled-priority-2"


def test_get_order_supports_client_order_id_lookup(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["params"] = params
        return {
            "uuid": "filled-by-client-1",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "client_order_id": "cid-client-only",
            "side": "ask",
            "price": "150000000",
            "volume": "0.02",
            "remaining_volume": "0.01",
            "executed_volume": "0.01",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "wait",
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    order = broker.get_order(client_order_id="cid-client-only")

    assert call == {"endpoint": "/v1/order", "params": {"client_order_id": "cid-client-only"}}
    assert order.exchange_order_id == "filled-by-client-1"
    assert order.client_order_id == "cid-client-only"
    assert order.raw is not None
    assert order.raw["market"] == "KRW-BTC"
    assert order.raw["ord_type"] == "limit"
    assert order.raw["client_order_id"] == "cid-client-only"
    assert order.raw["uuid"] == "filled-by-client-1"


def test_get_order_requires_at_least_one_identifier():
    _configure_live()
    broker = BithumbBroker()

    with pytest.raises(ValueError, match="requires exchange_order_id\\(uuid\\) or client_order_id"):
        broker.get_order(client_order_id=None, exchange_order_id=None)


def test_get_order_raises_on_response_identifier_mismatch(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {"state": "wait", "side": "bid", "volume": "0.1"},
    )

    with pytest.raises(BrokerRejectError, match="response schema mismatch"):
        broker.get_order(client_order_id="cid-identifier-missing", exchange_order_id="ex-identifier-missing")


def test_get_order_rejects_legacy_alias_identifier_only_response(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "order_id": "order-id-only-1",
            "coid": "cid-order-id-only",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "bid",
            "price": "149000000",
            "volume": "0.03",
            "remaining_volume": "0.03",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "wait",
        },
    )

    with pytest.raises(BrokerRejectError, match="missing both uuid and client_order_id"):
        broker.get_order(client_order_id="cid-order-id-only")


def test_get_order_rejects_unknown_state(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-unknown-state-1",
            "client_order_id": "cid-unknown-state",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "bid",
            "price": "149000000",
            "volume": "0.03",
            "remaining_volume": "0.03",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "mystery",
        },
    )

    with pytest.raises(BrokerRejectError, match="unknown state"):
        broker.get_order(client_order_id="cid-unknown-state")


def test_get_order_rejects_invalid_numeric_fields(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-bad-number-1",
            "client_order_id": "cid-bad-number",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "ask",
            "price": "bad-price",
            "volume": "0.03",
            "remaining_volume": "0.03",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "wait",
        },
    )

    with pytest.raises(BrokerRejectError, match="invalid numeric field"):
        broker.get_order(client_order_id="cid-bad-number")


def test_get_order_rejects_invalid_executed_funds_numeric(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-bad-executed-funds-1",
            "client_order_id": "cid-bad-executed-funds",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "bid",
            "price": "149000000",
            "volume": "0.03",
            "remaining_volume": "0.00",
            "executed_volume": "0.03",
            "executed_funds": "not-a-number",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "done",
        },
    )

    with pytest.raises(BrokerRejectError, match="invalid numeric field 'executed_funds'"):
        broker.get_order(client_order_id="cid-bad-executed-funds")


def test_get_order_rejects_non_list_trades(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-bad-trades-1",
            "client_order_id": "cid-bad-trades",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "ask",
            "price": "149000000",
            "volume": "0.03",
            "remaining_volume": "0.01",
            "executed_volume": "0.02",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "wait",
            "trades": {"uuid": "invalid"},
        },
    )

    with pytest.raises(BrokerRejectError, match="trades must be a list"):
        broker.get_order(client_order_id="cid-bad-trades")


@pytest.mark.parametrize(
    ("created_at", "expected_error"),
    [
        ("not-a-timestamp", "invalid timestamp field 'created_at'"),
        (None, "missing required timestamp field 'created_at'"),
    ],
)
def test_get_order_rejects_invalid_or_missing_created_at_timestamp(monkeypatch, created_at, expected_error):
    _configure_live()
    broker = BithumbBroker()

    payload = {
        "uuid": "filled-bad-ts-1",
        "client_order_id": "cid-bad-ts",
        "market": "KRW-BTC",
        "ord_type": "limit",
        "side": "ask",
        "price": "149000000",
        "volume": "0.03",
        "remaining_volume": "0.03",
        "state": "wait",
    }
    if created_at is not None:
        payload["created_at"] = created_at

    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: payload,
    )

    with pytest.raises(BrokerRejectError, match=expected_error):
        broker.get_order(client_order_id="cid-bad-ts")


@pytest.mark.parametrize("executed_funds", [None, "4470000"])
def test_get_order_accepts_optional_executed_funds(monkeypatch, executed_funds):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint, params, retry_safe=False):
        payload = {
            "uuid": "filled-executed-funds-1",
            "client_order_id": "cid-executed-funds",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "ask",
            "price": "149000000",
            "volume": "0.03",
            "remaining_volume": "0.0",
            "executed_volume": "0.03",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "done",
        }
        if executed_funds is not None:
            payload["executed_funds"] = executed_funds
        return payload

    monkeypatch.setattr(broker, "_get_private", _fake_get)
    order = broker.get_order(client_order_id="cid-executed-funds")
    assert order.status == "FILLED"


def test_get_order_allows_optional_documented_field_expansion(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-optional-1",
            "client_order_id": "cid-optional",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "ask",
            "price": "151000000",
            "volume": "0.02",
            "remaining_volume": "0.00",
            "executed_volume": "0.02",
            "created_at": "2024-01-01T00:00:00+00:00",
            "updated_at": "2024-01-01T00:01:00+00:00",
            "state": "done",
            "paid_fee": "123",
            "locked": "0",
            "trades_count": "2",
            "extra_future_field": "ignored",
        },
    )

    order = broker.get_order(client_order_id="cid-optional")

    assert order.status == "FILLED"
    assert order.raw is not None
    assert order.raw["paid_fee"] == "123"
    assert order.raw["locked"] == "0"
    assert order.raw["trades_count"] == "2"
    assert "extra_future_field" not in order.raw


def test_get_open_orders_preserves_raw_market_and_ord_type(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {
                "uuid": "open-raw-1",
                "market": "KRW-BTC",
                "ord_type": "price",
                "client_order_id": "exchange-open-1",
                "side": "bid",
                "price": "150000000",
                "volume": "0.02",
                "remaining_volume": "0.02",
                "executed_volume": "0",
                "state": "wait",
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    rows = broker.get_open_orders(exchange_order_ids=["open-raw-1"])

    assert len(rows) == 1
    assert rows[0].raw is not None
    assert rows[0].raw["market"] == "KRW-BTC"
    assert rows[0].raw["ord_type"] == "price"
    assert rows[0].raw["client_order_id"] == "exchange-open-1"
    assert rows[0].client_order_id == "exchange-open-1"


def test_recent_orders_maps_exchange_and_client_identifiers_consistently(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint, params, retry_safe=False):
        state = params["state"]
        if state == "wait":
            return [
                {
                    "uuid": "open-consistent-1",
                    "client_order_id": "coid-open-1",
                    "coid": "legacy-open-1",
                    "side": "bid",
                    "price": "100",
                    "volume": "0.1",
                    "remaining_volume": "0.1",
                    "executed_volume": "0",
                    "state": "wait",
                    "market": "KRW-BTC",
                    "ord_type": "limit",
                    "created_at": "2024-01-01T00:00:00+00:00",
                    "updated_at": "2024-01-01T00:00:00+00:00",
                }
            ]
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    rows = broker.get_recent_orders(limit=5, exchange_order_ids=["open-consistent-1"])

    assert len(rows) == 1
    assert rows[0].exchange_order_id == "open-consistent-1"
    assert rows[0].client_order_id == "coid-open-1"
    assert rows[0].raw is not None
    assert rows[0].raw["client_order_id"] == "coid-open-1"



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


def test_get_fills_uses_uuid_lookup_when_exchange_order_id_present(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    calls: list[dict[str, object]] = []

    def _fake_get(endpoint, params, retry_safe=False):
        calls.append({"endpoint": endpoint, "params": params})
        return {
            "uuid": "filled-uuid-1",
            "client_order_id": "cid-uuid-1",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "state": "done",
            "created_at": "2024-01-01T00:00:00+00:00",
            "trades": [],
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)
    broker.get_fills(client_order_id="cid-uuid-1", exchange_order_id="filled-uuid-1")

    assert calls[0] == {"endpoint": "/v1/order", "params": {"uuid": "filled-uuid-1"}}


def test_get_fills_uses_client_order_id_lookup_when_only_client_order_id_present(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    calls: list[dict[str, object]] = []

    def _fake_get(endpoint, params, retry_safe=False):
        calls.append({"endpoint": endpoint, "params": params})
        return {
            "uuid": "filled-client-1",
            "client_order_id": "cid-client-1",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "state": "done",
            "created_at": "2024-01-01T00:00:00+00:00",
            "trades": [],
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)
    broker.get_fills(client_order_id="cid-client-1", exchange_order_id=None)

    assert calls[0] == {"endpoint": "/v1/order", "params": {"client_order_id": "cid-client-1"}}
    assert len(calls) == 1


@pytest.mark.parametrize("trades_value", [None, []])
def test_get_fills_v1_order_handles_missing_or_empty_trades_with_aggregate_fill(monkeypatch, trades_value):
    _configure_live()
    broker = BithumbBroker()

    payload = {
        "uuid": "filled-agg-1",
        "client_order_id": "cid-agg-1",
        "price": "149000000",
        "volume": "0.05",
        "executed_volume": "0.05",
        "state": "done",
        "created_at": "2024-01-01T00:00:00+00:00",
    }
    if trades_value is not None:
        payload["trades"] = trades_value

    monkeypatch.setattr(broker, "_get_private", lambda endpoint, params, retry_safe=False: payload)

    fills = broker.get_fills(client_order_id="cid-agg-1", exchange_order_id="filled-agg-1")

    assert len(fills) == 1
    assert fills[0].exchange_order_id == "filled-agg-1"
    assert fills[0].qty == pytest.approx(0.05)


def test_get_fills_rejects_when_direct_lookup_has_no_usable_fill_and_scan_is_disabled(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    calls: list[dict[str, object]] = []

    def _fake_get(endpoint, params, retry_safe=False):
        calls.append({"endpoint": endpoint, "params": params})
        if endpoint == "/v1/order":
            return {
                "uuid": "filled-fallback-1",
                "client_order_id": "cid-fallback-1",
                "price": "",
                "volume": "0.05",
                "remaining_volume": "0.00",
                "executed_volume": "0.05",
                "state": "done",
                "created_at": "2024-01-01T00:00:00+00:00",
                "trades": [],
            }
        return [
            {
                "uuid": "filled-fallback-1",
                "client_order_id": "cid-fallback-1",
                "market": "KRW-BTC",
                "ord_type": "limit",
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.00",
                "executed_volume": "0.05",
                "state": "done",
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:01+00:00",
            }
        ]

    monkeypatch.setattr(broker, "_get_private", _fake_get)
    with pytest.raises(BrokerRejectError, match="broad /v1/orders done scan fallback is disabled"):
        broker.get_fills(client_order_id="cid-fallback-1", exchange_order_id="filled-fallback-1")

    assert calls[0]["endpoint"] == "/v1/order"
    assert len(calls) == 1


def test_get_fills_maps_identifiers_from_trade_and_order_rows(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-id-1",
            "client_order_id": "cid-parent",
            "price": "149000000",
            "volume": "0.05",
            "executed_volume": "0.05",
            "state": "done",
            "trades": [
                {
                    "uuid": "t-parent-1",
                    "price": "149000000",
                    "volume": "0.02",
                    "fee": "10",
                    "created_at": "2024-01-01T00:00:00+00:00",
                }
            ],
        },
    )

    fills = broker.get_fills(client_order_id=None, exchange_order_id="filled-id-1")

    assert len(fills) == 1
    assert fills[0].client_order_id == "cid-parent"
    assert fills[0].exchange_order_id == "filled-id-1"


def test_get_fills_rejects_client_order_id_lookup_mismatch(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-client-mismatch-1",
            "client_order_id": "other-client-id",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "state": "done",
            "created_at": "2024-01-01T00:00:00+00:00",
            "trades": [],
        },
    )

    with pytest.raises(BrokerRejectError, match="client_order_id mismatch"):
        broker.get_fills(client_order_id="cid-client-mismatch", exchange_order_id=None)


def test_get_fills_rejects_non_list_trades_for_v1_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-id-1",
            "client_order_id": "cid-parent",
            "state": "done",
            "trades": {"uuid": "bad"},
        },
    )

    with pytest.raises(BrokerRejectError, match="trades must be a list"):
        broker.get_fills(client_order_id=None, exchange_order_id="filled-id-1")


def test_get_fills_rejects_unknown_state_for_v1_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-unknown-state-1",
            "client_order_id": "cid-unknown-state-1",
            "state": "mystery",
            "side": "ask",
            "volume": "0.01",
            "remaining_volume": "0.01",
            "created_at": "2024-01-01T00:00:00+00:00",
        },
    )

    with pytest.raises(BrokerRejectError, match="unknown state"):
        broker.get_fills(client_order_id="cid-unknown-state-1", exchange_order_id="filled-unknown-state-1")


@pytest.mark.parametrize(
    ("created_at", "expected_error"),
    [
        ("not-a-timestamp", "invalid timestamp field 'created_at'"),
        (None, "missing required timestamp field 'created_at'"),
    ],
)
def test_get_fills_rejects_invalid_or_missing_trade_timestamp(monkeypatch, created_at, expected_error):
    _configure_live()
    broker = BithumbBroker()
    trade = {
        "uuid": "t-bad-ts",
        "price": "149000000",
        "volume": "0.02",
        "fee": "1.0",
    }
    if created_at is not None:
        trade["created_at"] = created_at
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-bad-ts",
            "client_order_id": "cid-bad-ts",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "created_at": "2024-01-01T00:00:00+00:00",
            "trades": [trade],
        },
    )

    with pytest.raises(BrokerRejectError, match=expected_error):
        broker.get_fills(client_order_id="cid-bad-ts", exchange_order_id="filled-bad-ts")


def test_get_fills_accepts_paid_fee_when_fee_field_is_absent(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    trade = {
        "uuid": "t-paid-fee-only",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        "paid_fee": "2.22",
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-paid-fee-only",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    fills = broker.get_fills(client_order_id="cid-paid-fee-only", exchange_order_id="filled-paid-fee-only")
    assert len(fills) == 1
    assert fills[0].fee == pytest.approx(2.22)


def test_get_fills_client_order_id_path_does_not_regress_to_done_scan(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    endpoints: list[str] = []

    def _fake_get(endpoint, params, retry_safe=False):
        endpoints.append(endpoint)
        if endpoint == "/v1/orders":
            pytest.fail("unexpected /v1/orders fallback scan for successful client_order_id direct lookup")
        return {
            "uuid": "filled-client-regression-1",
            "client_order_id": "cid-client-regression-1",
            "price": "149000000",
            "volume": "0.05",
            "remaining_volume": "0.00",
            "executed_volume": "0.05",
            "state": "done",
            "created_at": "2024-01-01T00:00:00+00:00",
            "trades": [],
        }

    monkeypatch.setattr(broker, "_get_private", _fake_get)
    fills = broker.get_fills(client_order_id="cid-client-regression-1", exchange_order_id=None)

    assert len(fills) == 1
    assert endpoints == ["/v1/order"]


def test_get_order_rejects_exchange_order_id_mismatch(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "different-exid",
            "client_order_id": "cid-1",
            "state": "wait",
            "side": "bid",
            "volume": "0.1",
            "remaining_volume": "0.1",
            "created_at": "2024-01-01T00:00:00+00:00",
        },
    )

    with pytest.raises(BrokerRejectError, match="exchange_order_id mismatch"):
        broker.get_order(client_order_id="cid-1", exchange_order_id="requested-exid")


@pytest.mark.parametrize(
    ("trade_row", "expected_fee"),
    [
        ({"fee": "0.1234"}, 0.1234),
        ({"paid_fee": "0.1234"}, 0.1234),
        ({"commission": 0.1234}, 0.1234),
        ({"trade_fee": "1,234.56"}, 1234.56),
    ],
)
def test_get_fills_normalizes_fee_from_supported_keys(monkeypatch, trade_row, expected_fee):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t1",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        **trade_row,
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-1",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    fills = broker.get_fills(client_order_id="cid-1", exchange_order_id="filled-1")

    assert len(fills) == 1
    assert fills[0].fee == pytest.approx(expected_fee)


@pytest.mark.parametrize(
    ("trade_row", "expected_error"),
    [
        ({"fee": ""}, "invalid fee field"),
        ({"fee": None}, "invalid fee field"),
        ({}, "missing required fee field"),
    ],
)
def test_get_fills_rejects_missing_or_invalid_trade_fee(monkeypatch, trade_row, expected_error):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t1",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        **trade_row,
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-1",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    with pytest.raises(BrokerRejectError, match=expected_error):
        broker.get_fills(client_order_id="cid-1", exchange_order_id="filled-1")


def test_get_fills_allows_trade_zero_fee_value(monkeypatch, caplog):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t-zero-fee",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        "fee": "0",
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-zero-fee",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    with caplog.at_level(logging.WARNING, logger="bithumb_bot.run"):
        fills = broker.get_fills(client_order_id="cid-zero-fee", exchange_order_id="filled-zero-fee")

    assert len(fills) == 1
    assert fills[0].fee == pytest.approx(0.0)
    assert any("resolved zero fee" in rec.message for rec in caplog.records)


@pytest.mark.parametrize("fee_key", ["fee", "paid_fee", "commission", "trade_fee"])
def test_get_fills_fee_key_regression_parses_numeric_string(monkeypatch, fee_key):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t-fee-key",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        fee_key: "25.03",
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-fee-key",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    fills = broker.get_fills(client_order_id="cid-fee-key", exchange_order_id="filled-fee-key")

    assert len(fills) == 1
    assert fills[0].fee == pytest.approx(25.03)


@pytest.mark.parametrize("fee_key", ["fee", "paid_fee", "commission", "trade_fee"])
def test_get_fills_fee_key_regression_parses_numeric_type(monkeypatch, fee_key):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t-fee-number",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        fee_key: 25.03,
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-fee-number",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    fills = broker.get_fills(client_order_id="cid-fee-number", exchange_order_id="filled-fee-number")

    assert len(fills) == 1
    assert fills[0].fee == pytest.approx(25.03)


@pytest.mark.parametrize(
    ("fee_value", "expected_error"),
    [
        ("", "invalid fee field"),
        (None, "invalid fee field"),
        ("not-a-number", "invalid fee field"),
        ("nan", "invalid fee field"),
        ("inf", "invalid fee field"),
    ],
)
def test_get_fills_fee_parsing_regression_rejects_invalid_values(monkeypatch, fee_value, expected_error):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t-invalid-fee",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        "fee": fee_value,
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-invalid-fee",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    with pytest.raises(BrokerRejectError, match=expected_error):
        broker.get_fills(client_order_id="cid-invalid-fee", exchange_order_id="filled-invalid-fee")


def test_get_fills_fee_key_regression_prioritizes_fee_over_other_fee_keys(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t-fee-priority",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        "fee": "1.11",
        "paid_fee": "2.22",
        "commission": "3.33",
        "trade_fee": "4.44",
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-fee-priority",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    fills = broker.get_fills(client_order_id="cid-fee-priority", exchange_order_id="filled-fee-priority")

    assert len(fills) == 1
    assert fills[0].fee == pytest.approx(1.11)


def test_get_fills_fee_key_regression_rejects_when_fee_field_is_present_but_invalid(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t-fee-fallback",
        "price": "149000000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        "fee": "not-a-number",
        "paid_fee": "2.22",
        "commission": "3.33",
        "trade_fee": "4.44",
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-fee-fallback",
            "price": "149000000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    with pytest.raises(BrokerRejectError, match="invalid fee field 'fee'"):
        broker.get_fills(client_order_id="cid-fee-fallback", exchange_order_id="filled-fee-fallback")


def test_get_fills_skips_aggregate_fill_when_price_missing(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-1",
            "client_order_id": "cid-1",
            "side": "ask",
            "price": "",
            "volume": "0.05",
            "executed_volume": "0.05",
            "state": "done",
        },
    )

    with pytest.raises(BrokerRejectError, match="done scan fallback is disabled"):
        broker.get_fills(client_order_id="cid-1", exchange_order_id=None)


def test_get_fills_skips_aggregate_fill_without_timestamps_even_when_avg_price_exists(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-2",
            "client_order_id": "cid-2",
            "side": "ask",
            "price": "",
            "avg_price": "151000000",
            "volume": "0.03",
            "executed_volume": "0.03",
            "state": "done",
        },
    )

    with pytest.raises(BrokerRejectError, match="done scan fallback is disabled"):
        broker.get_fills(client_order_id="cid-2", exchange_order_id=None)



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
    assert "currencies" in summary["/v1/accounts"]
    assert "KRW" in summary["/v1/accounts"]
    assert "BTC" in summary["/v1/accounts"]
    assert "1000" not in summary["/v1/accounts"]


def test_accounts_validation_diagnostic_records_schema_mismatch_reason(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(broker, "_get_private", lambda endpoint, params, retry_safe=False: [{"currency": "KRW", "locked": "0"}])

    with pytest.raises(BrokerRejectError, match="schema mismatch"):
        broker.get_balance()

    diag = broker.get_accounts_validation_diagnostics()
    assert diag["reason"] == "schema mismatch"
    assert diag["row_count"] == 1
    assert diag["currencies"] == ["KRW"]


def test_accounts_validation_diagnostic_records_missing_currency_reason(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [{"currency": "KRW", "balance": "1000", "locked": "0"}],
    )

    with pytest.raises(BrokerRejectError, match="missing base currency row 'BTC'"):
        broker.get_balance()

    diag = broker.get_accounts_validation_diagnostics()
    assert diag["reason"] == "required currency missing"
    assert diag["missing_required_currencies"] == ["BTC"]
    assert "1000" not in str(diag)



def test_recent_orders_journal_summary_captures_sample_order_ids(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint, params, retry_safe=False):
        if params["state"] == "wait":
            return []
        if params["state"] == "done":
            return [{"uuid": "filled-1", "market": "KRW-BTC", "ord_type": "limit", "side": "bid", "price": "100", "volume": "0.1", "remaining_volume": "0", "executed_volume": "0.1", "state": "done", "created_at": "2024-01-01T00:00:00+00:00", "updated_at": "2024-01-01T00:00:01+00:00"}]
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    broker.get_recent_orders(limit=10, exchange_order_ids=["filled-1"])
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
    assert context["request_body_text"] == '{"market":"KRW-BTC","side":"bid","price":"9998","ord_type":"price"}'
    assert context["request_content"] == b'{"market":"KRW-BTC","side":"bid","price":"9998","ord_type":"price"}'
    assert context["query_hash_claims"] == {
        "query_hash": hashlib.sha512(context["canonical_payload"].encode("utf-8")).hexdigest(),
        "query_hash_alg": "SHA512",
    }
    assert context["claims"] == {
        "access_key": "k",
        "nonce": "nonce-fixed",
        "timestamp": 1712230310689,
        "query_hash": hashlib.sha512(context["canonical_payload"].encode("utf-8")).hexdigest(),
        "query_hash_alg": "SHA512",
    }
    assert context["headers"]["Content-Type"] == "application/json"
    assert context["headers"]["Authorization"].startswith("Bearer ")
    assert context["request_kwargs"] == {
        "content": b'{"market":"KRW-BTC","side":"bid","price":"9998","ord_type":"price"}',
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
    ("payload", "expected_content", "expected_query"),
    [
        (
            {"market": "KRW-BTC", "side": "bid", "price": "9999", "ord_type": "price"},
            b'{"market":"KRW-BTC","side":"bid","price":"9999","ord_type":"price"}',
            "market=KRW-BTC&side=bid&price=9999&ord_type=price",
        ),
        (
            {"market": "KRW-BTC", "side": "ask", "volume": "0.1", "ord_type": "market"},
            b'{"market":"KRW-BTC","side":"ask","volume":"0.1","ord_type":"market"}',
            "market=KRW-BTC&side=ask&volume=0.1&ord_type=market",
        ),
        (
            {"market": "KRW-BTC", "side": "bid", "volume": "0.4", "price": "149500000", "ord_type": "limit"},
            b'{"market":"KRW-BTC","side":"bid","volume":"0.4","price":"149500000","ord_type":"limit"}',
            "market=KRW-BTC&side=bid&volume=0.4&price=149500000&ord_type=limit",
        ),
    ],
)
def test_order_submit_uses_json_body_with_query_hash_from_canonical_payload(monkeypatch, payload, expected_content, expected_query):
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

    assert call["headers"]["Content-Type"] == "application/json"
    assert call["content"] == expected_content
    assert "json" not in call
    assert claims["query_hash"] == BithumbPrivateAPI._query_hash_from_canonical_payload(expected_query)["query_hash"]





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
    request_body_text = call["content"].decode()

    assert call["headers"]["Content-Type"] == "application/json"
    assert request_body_text == '{"market":"KRW-BTC","side":"bid","price":"10002","ord_type":"price"}'
    assert claims["nonce"] == "nonce-fixed"
    assert claims["timestamp"] == 1712230310689
    canonical_query = "market=KRW-BTC&side=bid&price=10002&ord_type=price"
    assert claims["query_hash"] == BithumbPrivateAPI._query_hash_from_canonical_payload(canonical_query)["query_hash"]
    assert claims["query_hash_alg"] == "SHA512"


def test_order_http_debug_request_logs_query_hash_and_json_body(monkeypatch, caplog):
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
    assert "content_type=application/json" in order_logs[-1]
    assert "canonical_query_string=market=KRW-BTC&side=bid&price=9999&ord_type=price" in order_logs[-1]
    assert "query_hash_alg=SHA512" in order_logs[-1]
    assert "nonce_present=1" in order_logs[-1]
    assert "timestamp_present=1" in order_logs[-1]
    assert "signed_payload_repr='market=KRW-BTC&side=bid&price=9999&ord_type=price'" in order_logs[-1]
    assert 'transmitted_payload_repr=\'{"market":"KRW-BTC","side":"bid","price":"9999","ord_type":"price"}\'' in order_logs[-1]


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


def test_order_submit_live_failure_regression_uses_json_body_and_matching_query_hash(monkeypatch):
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

    assert call["headers"]["Content-Type"] == "application/json"
    assert call["content"] == b'{"market":"KRW-BTC","side":"bid","price":"9998","ord_type":"price"}'
    assert "json" not in call
    assert claims == {
        "access_key": "k",
        "nonce": "nonce-fixed",
        "timestamp": 1712230310689,
        "query_hash": hashlib.sha512(canonical_payload.encode("utf-8")).hexdigest(),
        "query_hash_alg": "SHA512",
    }


def test_get_recent_orders_logs_parser_failure_context_for_v1_orders(monkeypatch, caplog):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint: str, params: dict[str, object], retry_safe: bool = False):
        assert endpoint == "/v1/orders"
        if params.get("state") == "wait":
            return [
                {
                    "uuid": "ex-log-1",
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ord_type": "limit",
                    "state": "mystery",
                    "price": "100",
                    "volume": "0.1",
                    "remaining_volume": "0.1",
                    "executed_volume": "0",
                    "created_at": "2024-01-01T00:00:00+00:00",
                    "updated_at": "2024-01-01T00:00:00+00:00",
                }
            ]
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    with caplog.at_level(logging.ERROR, logger="bithumb_bot.run"):
        with pytest.raises(BrokerRejectError, match="/v1/orders schema mismatch: unknown state"):
            broker.get_recent_orders(limit=5, exchange_order_ids=["ex-log-1"])

    assert "[V1_ORDERS_PARSE_FAIL]" in caplog.text
    assert "endpoint=/v1/orders" in caplog.text
    assert "state=wait" in caplog.text
    assert "exchange_ids_count=1" in caplog.text
    assert "uuid_present=1" in caplog.text
    assert "parser_failure_reason=" in caplog.text
    assert "unknown state 'mystery'" in caplog.text


def test_get_order_logs_myorder_lookup_failure_context_on_identifier_mismatch(monkeypatch, caplog):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "other-exid",
            "client_order_id": "cid-lookup-1",
            "side": "bid",
            "volume": "0.1",
            "remaining_volume": "0.1",
            "created_at": "2024-01-01T00:00:00+00:00",
            "state": "wait",
        },
    )

    with caplog.at_level(logging.ERROR, logger="bithumb_bot.run"):
        with pytest.raises(BrokerRejectError, match="exchange_order_id mismatch"):
            broker.get_order(client_order_id="cid-lookup-1", exchange_order_id="requested-exid")

    assert "[V1_MYORDER_LOOKUP_FAIL]" in caplog.text
    assert "stage=get_order" in caplog.text
    assert "requested_exchange_order_id=requested-exid" in caplog.text
    assert "response_exchange_order_id=other-exid" in caplog.text
    assert 'reason="IDENTIFIER_MISMATCH:' in caplog.text


def test_get_fills_logs_myorder_lookup_failure_context_on_schema_mismatch(monkeypatch, caplog):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(broker, "_get_private", lambda endpoint, params, retry_safe=False: [])

    with caplog.at_level(logging.ERROR, logger="bithumb_bot.run"):
        with pytest.raises(BrokerRejectError, match="expected object payload actual=list"):
            broker.get_fills(client_order_id="cid-fill-log-1", exchange_order_id="filled-log-1")

    assert "[V1_MYORDER_LOOKUP_FAIL]" in caplog.text
    assert "stage=get_fills" in caplog.text
    assert "requested_client_order_id=cid-fill-log-1" in caplog.text
    assert 'reason="DOC_SCHEMA:' in caplog.text

def test_balance_source_diagnostics_include_source_and_observed_timestamp(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "MODE", "live")
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": "1000", "locked": "25"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ],
    )

    broker.get_balance()
    diag = broker.get_accounts_validation_diagnostics()

    assert diag["source"] == "accounts_v1_rest_snapshot"
    assert int(diag["last_observed_ts_ms"]) > 0


def test_balance_source_injection_uses_dry_run_source_when_dryrun_enabled(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "START_CASH_KRW", 12345.0)

    broker = BithumbBroker()

    # dry source should not call private accounts endpoint when dry-run is enabled
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: (_ for _ in ()).throw(RuntimeError("should not call private API")),
    )

    bal = broker.get_balance()
    diag = broker.get_accounts_validation_diagnostics()

    assert bal.cash_available == 12345.0
    assert broker.get_balance_source_id() == "dry_run_static"
    assert diag["reason"] == "not_applicable"


def test_balance_source_feature_flag_off_keeps_accounts_v1_snapshot_path(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_WS_MYASSET_ENABLED", False)
    broker = BithumbBroker()

    called: list[str] = []

    def _fake_get_private(endpoint, params, retry_safe=False):
        called.append(str(endpoint))
        return [
            {"currency": "KRW", "balance": "1000", "locked": "25"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.02"},
        ]

    monkeypatch.setattr(broker, "_get_private", _fake_get_private)
    snapshot = broker.get_balance_snapshot()

    assert snapshot.source_id == "accounts_v1_rest_snapshot"
    assert called == ["/v1/accounts"]
