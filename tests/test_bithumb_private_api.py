from __future__ import annotations

import base64
import hashlib
import json
import logging
from types import SimpleNamespace
from urllib.parse import urlencode

import httpx
import pytest

from bithumb_bot.broker.bithumb import (
    BithumbBroker,
    BithumbPrivateAPI,
    BithumbOrderNotReadyError,
    BithumbRateLimitError,
    _resolve_submit_price_tick_policy,
    _documented_private_error_descriptor,
    classify_private_api_error,
    classify_private_api_failure,
)
from bithumb_bot.broker.base import (
    BrokerIdentifierMismatchError,
    BrokerRejectError,
    BrokerSchemaError,
    BrokerTemporaryError,
)
from bithumb_bot.broker.order_list_v1 import build_order_list_params, parse_v1_order_list_row
from bithumb_bot.broker.order_payloads import build_order_payload, validate_order_submit_payload
from bithumb_bot.config import settings
from bithumb_bot.lot_model import build_market_lot_rules, lot_count_to_qty
from bithumb_bot.public_api_orderbook import BestQuote
from decimal import Decimal, ROUND_DOWN

from bithumb_bot.broker import order_rules

_HTTPX_TIMEOUT = getattr(httpx, "ReadTimeout", getattr(httpx, "RequestError"))
_HTTPX_CONNECT = getattr(httpx, "ConnectError", getattr(httpx, "RequestError"))

pytestmark = pytest.mark.fast_regression


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


def _signed_order_request(payload: dict[str, object], *, authority: str = "validated_place_order_flow"):
    return SimpleNamespace(
        payload=dict(payload),
        canonical_payload=BithumbPrivateAPI._query_string(payload),
        dispatch_authority=authority,
    )



def _configure_live():
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")


def _set_buy_price_none_submit_contract(
    broker: BithumbBroker,
    *,
    rules: object,
    market: str = "KRW-BTC",
) -> order_rules.BuyPriceNoneSubmitContract:
    contract = order_rules.build_buy_price_none_submit_contract(rules=rules)
    return contract


def _exact_lot_qty(
    *,
    market_price: float | None,
    lot_count: int = 1,
    bid_min_total_krw: float = 5000.0,
    ask_min_total_krw: float = 5000.0,
    bid_price_unit: float = 1.0,
    ask_price_unit: float = 1.0,
    min_notional_krw: float = 5000.0,
) -> float:
    rules = SimpleNamespace(
        bid_min_total_krw=bid_min_total_krw,
        ask_min_total_krw=ask_min_total_krw,
        bid_price_unit=bid_price_unit,
        ask_price_unit=ask_price_unit,
        min_notional_krw=min_notional_krw,
    )
    lot_rules = build_market_lot_rules(
        market_id="KRW-BTC",
        market_price=market_price,
        rules=rules,
        exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
        source_mode="exchange",
    )
    return lot_count_to_qty(lot_count=lot_count, lot_size=lot_rules.lot_size)



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
                        "order_types": ("limit",),
                        "bid_types": ("limit", "price"),
                        "ask_types": ("limit", "market"),
                        "order_sides": ("bid", "ask"),
                    },
                )(),
            },
        )(),
    )


@pytest.fixture(autouse=True)
def _restore_live_mode_related_settings():
    snapshot = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "BITHUMB_API_KEY": settings.BITHUMB_API_KEY,
        "BITHUMB_API_SECRET": settings.BITHUMB_API_SECRET,
    }
    yield
    for key, value in snapshot.items():
        object.__setattr__(settings, key, value)


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
    monkeypatch.setattr("bithumb_bot.broker.bithumb._REQUEST_THROTTLER.acquire", lambda **_kwargs: 0.0)

    broker = BithumbBroker()
    data = broker._get_private("/v1/accounts", {}, retry_safe=True)

    assert isinstance(data, list)
    assert _SequencedClient.calls == 2
    assert sleeps == [0.15]


def test_private_rate_limit_retry_penalizes_bucket_and_uses_endpoint_aware_backoff(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        _mk_response(429, {"error": {"name": "too_many_requests", "message": "slow down"}}),
        _mk_response(200, [{"currency": "KRW", "balance": "1000", "locked": "0"}]),
    ]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    sleeps: list[float] = []
    penalties: list[tuple[str, float]] = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda sec: sleeps.append(sec))
    monkeypatch.setattr("bithumb_bot.broker.bithumb._REQUEST_THROTTLER.acquire", lambda **_kwargs: 0.0)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb._REQUEST_THROTTLER.penalize",
        lambda *, bucket, delay_sec: penalties.append((bucket, delay_sec)),
    )

    broker = BithumbBroker()
    data = broker._get_private("/v1/orders", {"market": "KRW-BTC"}, retry_safe=True)

    assert isinstance(data, list)
    assert penalties == [("order", 0.35)]
    assert sleeps == [0.35]


def test_private_request_uses_fresh_nonce_per_retry(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        _HTTPX_TIMEOUT("timeout"),
        _mk_response(200, {"status": "0000", "data": []}),
    ]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda _sec: None)
    monkeypatch.setattr("bithumb_bot.broker.bithumb._REQUEST_THROTTLER.acquire", lambda **_kwargs: 0.0)

    broker = BithumbBroker()
    broker._get_private("/v1/accounts", {}, retry_safe=True)

    auth_headers = [req["headers"]["Authorization"] for req in _SequencedClient.requests]
    assert len(auth_headers) == 2
    assert auth_headers[0] != auth_headers[1]


def test_private_order_submit_uses_utf8_json_content_type(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "order-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb._REQUEST_THROTTLER.acquire", lambda **_kwargs: 0.0)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    api.request(
        "POST",
        "/v2/orders",
        json_body={
            "market": "KRW-BTC",
            "side": "bid",
            "order_type": "price",
            "price": "1000",
            "client_order_id": "cid-json-1",
        },
    )

    assert str(_SequencedClient.requests[0]["headers"]["Content-Type"]).startswith("application/json")


def test_market_buy_chance_contract_log_includes_supported_types_and_submit_field(monkeypatch, caplog):
    _configure_live()
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _market: object(),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.validated_best_quote_ask_price",
        lambda _quote, requested_market: 100_000_000.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb._REQUEST_THROTTLER.acquire",
        lambda **_kwargs: 0.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                market_id="KRW-BTC",
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=1.0,
                ask_price_unit=1.0,
                order_types=("limit", "price", "market"),
                order_sides=("ask", "bid"),
                bid_fee=0.0025,
                ask_fee=0.0025,
                maker_bid_fee=0.0025,
                maker_ask_fee=0.0025,
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
            )
        ),
    )

    broker = BithumbBroker()
    monkeypatch.setattr(broker, "_market", lambda: "KRW-BTC")
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda *args, **kwargs: (_ for _ in ()).throw(BrokerRejectError("forced stop after logging")),
    )
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.DerivedOrderConstraints(
            order_types=("limit", "price", "market"),
            bid_types=("price",),
            ask_types=("limit", "market"),
            order_sides=("ask", "bid"),
            bid_min_total_krw=5000.0,
            ask_min_total_krw=5000.0,
            bid_price_unit=1.0,
            ask_price_unit=1.0,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=8,
        ),
    )

    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        with pytest.raises(BrokerRejectError, match="forced stop after logging"):
            broker.place_order(
                client_order_id="cid-buy-contract-log",
                side="BUY",
                qty=0.0004,
                price=None,
                buy_price_none_submit_contract=submit_contract,
            )

    assert "chance_validation_order_type=price" in caplog.text
    assert "supported_order_types=price" in caplog.text
    assert "submit_field=price" in caplog.text


def test_market_buy_chance_contract_log_surfaces_blocked_market_only_support(monkeypatch, caplog):
    _configure_live()
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _market: object(),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.validated_best_quote_ask_price",
        lambda _quote, requested_market: 100_000_000.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb._REQUEST_THROTTLER.acquire",
        lambda **_kwargs: 0.0,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                market_id="KRW-BTC",
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=1.0,
                ask_price_unit=1.0,
                order_types=("limit", "market"),
                order_sides=("ask", "bid"),
                bid_fee=0.0025,
                ask_fee=0.0025,
                maker_bid_fee=0.0025,
                maker_ask_fee=0.0025,
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
            )
        ),
    )

    broker = BithumbBroker()
    monkeypatch.setattr(broker, "_market", lambda: "KRW-BTC")
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda *args, **kwargs: (_ for _ in ()).throw(BrokerRejectError("forced stop after logging")),
    )
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.DerivedOrderConstraints(
            order_types=("limit", "market"),
            bid_types=("market",),
            ask_types=("limit", "market"),
            order_sides=("ask", "bid"),
            bid_min_total_krw=5000.0,
            ask_min_total_krw=5000.0,
            bid_price_unit=1.0,
            ask_price_unit=1.0,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=8,
        ),
    )

    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support"):
            broker.place_order(
                client_order_id="cid-buy-contract-market-alias-log",
                side="BUY",
                qty=0.0004,
                price=None,
                buy_price_none_submit_contract=submit_contract,
            )

    assert "chance_validation_order_type=price" in caplog.text
    assert "supported_order_types=market" in caplog.text
    assert "buy_price_none_allowed=0" in caplog.text
    assert "buy_price_none_alias_used=0" in caplog.text
    assert "buy_price_none_block_reason=buy_price_none_requires_explicit_price_support" in caplog.text
    assert "submit_field=price" in caplog.text


def test_classify_private_api_error_categories_cover_v1_orders_contract_failures() -> None:
    code, summary = classify_private_api_error(BrokerRejectError("/v1/orders schema mismatch: unknown state 'halted'"))
    assert code == "DOC_SCHEMA"
    assert "schema mismatch" in summary

    code, summary = classify_private_api_error(
        BrokerRejectError(
            "open order lookup is identifier-scoped by bot policy; /v1/orders broad market/state scans are reserved for recovery"
        )
    )
    assert code == "RECOVERY_REQUIRED"
    assert "identifier-based lookup" in summary

    code, summary = classify_private_api_error(BrokerRejectError("rejected with http status=401 body=invalid jwt"))
    assert code == "AUTH_SIGN"

    code, summary = classify_private_api_error(BrokerRejectError("rejected with http status=401 error_name=invalid_query_payload body={...}"))
    assert code == "AUTH_QUERY_HASH_MISMATCH"
    assert "query_hash" in summary

    code, summary = classify_private_api_error(BrokerRejectError("rejected with http status=401 error_name=invalid_access_key body={...}"))
    assert code == "AUTH_INVALID_ACCESS_KEY"
    assert "access key" in summary

    code, summary = classify_private_api_error(BrokerRejectError("status=409 error_name=duplicate_client_order_id body={}"))
    assert code == "DUPLICATE_CLIENT_ORDER_ID"
    assert "duplicate client order" in summary

    code, summary = classify_private_api_error(BrokerRejectError("status=400 error_name=bank_account_required body={}"))
    assert code == "ACCOUNT_SETUP_REQUIRED"

    code, summary = classify_private_api_error(BrokerRejectError("status=500 error_name=server_error body={}"))
    assert code == "SERVER_INTERNAL_FAILURE"
    assert "server/internal failure" in summary

    code, summary = classify_private_api_error(BrokerRejectError("status=403 error_name=blocked_member_id body={}"))
    assert code == "ACCOUNT_RESTRICTED"

    code, summary = classify_private_api_error(BrokerRejectError("unexpected broker response shape for private request"))
    assert code == "AUTH_RESPONSE_UNEXPECTED"

    code, summary = classify_private_api_error(BrokerTemporaryError("bithumb private /v1/orders transport error"))
    assert code == "TEMPORARY"

    code, summary = classify_private_api_error(BrokerTemporaryError("bithumb private /v1/orders server error status=503 body={}"))
    assert code == "SERVER_INTERNAL_FAILURE"
    assert "server/internal failure" in summary


def test_classify_private_api_error_uses_documented_error_names() -> None:
    assert classify_private_api_error(BrokerRejectError("status=401 error_name=jwt_verification body={}"))[0] == "AUTH_JWT_VERIFICATION"
    assert classify_private_api_error(BrokerRejectError("status=401 error_name=expired_jwt body={}"))[0] == "AUTH_JWT_EXPIRED"
    assert classify_private_api_error(BrokerRejectError("status=401 error_name=NotAllowIP body={}"))[0] == "AUTH_IP_DENIED"
    assert classify_private_api_error(BrokerRejectError("status=400 body=currency does not have a valid value"))[0] == "INVALID_PARAMETER"
    assert classify_private_api_error(BrokerRejectError("status=400 error_name=invalid_price body={}"))[0] == "INVALID_PRICE"
    assert classify_private_api_error(BrokerRejectError("status=400 error_name=under_price_limit_ask body={}"))[0] == "UNDER_PRICE_LIMIT"
    assert classify_private_api_error(BrokerRejectError("status=500 error_name=server_error body={}"))[0] == "SERVER_INTERNAL_FAILURE"
    assert classify_private_api_error(BrokerRejectError("status=404 error_name=order_not_found body={}"))[0] == "ORDER_NOT_FOUND"
    assert classify_private_api_error(BrokerRejectError("status=422 error_name=order_not_ready body={}"))[0] == "ORDER_NOT_READY"
    assert classify_private_api_error(BrokerRejectError("status=404 error_name=deposit_not_found body={}"))[0] == "LOOKUP_NOT_FOUND"
    assert classify_private_api_error(BrokerRejectError("status=404 error_name=withdraw_not_found body={}"))[0] == "LOOKUP_NOT_FOUND"
    assert classify_private_api_error(BrokerRejectError("status=400 error_name=cross_trading body={}"))[0] == "CROSS_TRADING"
    assert _documented_private_error_descriptor("order_not_found") is not None
    assert _documented_private_error_descriptor("duplicate_client_order_id") is None

    code, summary = classify_private_api_error(BrokerIdentifierMismatchError("order lookup response exchange_order_id mismatch"))
    assert code == "IDENTIFIER_MISMATCH"
    assert "identifiers conflict" in summary

    code, summary = classify_private_api_error(BrokerSchemaError("order lookup response schema mismatch: expected object payload"))
    assert code == "DOC_SCHEMA"


def test_classify_private_api_failure_new_buckets() -> None:
    classification = classify_private_api_failure(BrokerRejectError("invalid_parameter: malformed market"))
    assert classification.category == "INVALID_REQUEST"
    assert classification.summary

    classification = classify_private_api_failure(BrokerRejectError("status=400 error_name=invalid_price body={}"))
    assert classification.category == "INVALID_REQUEST"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=400 error_name=under_price_limit_ask body={}"))
    assert classification.category == "PRETRADE_GUARD"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=400 error_name=cross_trading body={}"))
    assert classification.category == "EXCHANGE_RULE_VIOLATION"
    assert classification.disable_trading is False

    classification = classify_private_api_failure(BrokerRejectError("status=400 error_name=under_min_total body={}"))
    assert classification.category == "PRETRADE_GUARD"
    assert classification.summary

    classification = classify_private_api_failure(BrokerRejectError("status=400 error_name=deposit_not_found body={}"))
    assert classification.category == "NOT_FOUND_NEEDS_RECONCILE"
    assert classification.needs_reconcile is True

    classification = classify_private_api_failure(BrokerRejectError("status=400 error_name=withdraw_not_found body={}"))
    assert classification.category == "NOT_FOUND_NEEDS_RECONCILE"
    assert classification.needs_reconcile is True

    classification = classify_private_api_failure(BithumbRateLimitError("bithumb private /v1/orders throttled with http status=429"))
    assert classification.category == "THROTTLED_BACKOFF"
    assert classification.should_retry is True

    classification = classify_private_api_failure(BithumbOrderNotReadyError("bithumb private /v2/order rejected with http status=422 error_name=order_not_ready"))
    assert classification.category == "ORDER_NOT_READY"
    assert classification.should_retry is True

    classification = classify_private_api_failure(BrokerTemporaryError("bithumb private /v1/orders transport error"))
    assert classification.category == "RETRYABLE_TRANSIENT"
    assert classification.should_retry is True

    classification = classify_private_api_failure(BrokerTemporaryError("bithumb private /v1/orders server error status=500 body={}"))
    assert classification.category == "SERVER_INTERNAL_FAILURE"
    assert classification.should_retry is True

    classification = classify_private_api_failure(BrokerRejectError("status=500 error_name=server_error body={}"))
    assert classification.category == "SERVER_INTERNAL_FAILURE"
    assert classification.should_retry is True

    classification = classify_private_api_failure(BrokerRejectError("order_not_found"))
    assert classification.category == "NOT_FOUND_NEEDS_RECONCILE"
    assert classification.needs_reconcile is True

    classification = classify_private_api_failure(BrokerRejectError("cancel_not_allowed"))
    assert classification.category == "CANCEL_NOT_ALLOWED"

    classification = classify_private_api_failure(BrokerRejectError("invalid_query_payload"))
    assert classification.category == "INVALID_REQUEST"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=400 error_name=bank_account_required body={}"))
    assert classification.category == "PREFLIGHT_BLOCKED"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=401 error_name=invalid_query_payload body={}"))
    assert classification.category == "INVALID_REQUEST"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=401 error_name=jwt_verification body={}"))
    assert classification.category == "AUTHENTICATION"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=401 error_name=expired_jwt body={}"))
    assert classification.category == "AUTHENTICATION"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=401 error_name=NotAllowIP body={}"))
    assert classification.category == "PERMISSION_SCOPE"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=403 error_name=blocked_member_id body={}"))
    assert classification.category == "PERMISSION_SCOPE"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=403 error_name=out_of_scope body={}"))
    assert classification.category == "PERMISSION_SCOPE"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=400 body=currency does not have a valid value"))
    assert classification.category == "INVALID_REQUEST"
    assert classification.disable_trading is True

    classification = classify_private_api_failure(BrokerRejectError("status=404 error_name=order_not_found body={}"))
    assert classification.category == "NOT_FOUND_NEEDS_RECONCILE"
    assert classification.needs_reconcile is True

    classification = classify_private_api_failure(BrokerRejectError("status=422 error_name=order_not_ready body={}"))
    assert classification.category == "ORDER_NOT_READY"
    assert classification.should_retry is True

    classification = classify_private_api_failure(BrokerRejectError("status=500 error_name=server_error body={}"))
    assert classification.category == "SERVER_INTERNAL_FAILURE"
    assert classification.should_retry is True


def test_classify_private_api_failure_uses_documented_error_table() -> None:
    classification = classify_private_api_failure(
        BrokerRejectError("status=404 error_name=order_not_found body={}")
    )

    assert classification.category == "NOT_FOUND_NEEDS_RECONCILE"
    assert classification.needs_reconcile is True
    assert classification.should_retry is False

    classification = classify_private_api_failure(
        BrokerRejectError("status=404 error_name=deposit_not_found body={}")
    )
    assert classification.category == "NOT_FOUND_NEEDS_RECONCILE"
    assert classification.needs_reconcile is True



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
        "order_type": "price",
        "price": "9999",
    }

    assert BithumbPrivateAPI._canonical_payload_for_query_hash(payload) == (
        "market=KRW-BTC&side=bid&order_type=price&price=9999"
    )
    assert BithumbPrivateAPI._query_string(payload) == "market=KRW-BTC&side=bid&order_type=price&price=9999"


def test_order_submit_query_hash_matches_official_urlencode_sha512_rule():
    payload = {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "price",
        "price": "9998",
    }

    official_query = urlencode(
        [
            ("market", "KRW-BTC"),
            ("side", "bid"),
            ("order_type", "price"),
            ("price", "9998"),
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


def test_validate_order_submit_payload_matches_documented_limit_body_shape():
    payload = validate_order_submit_payload(
        {
            "market": "KRW-BTC",
            "side": "bid",
            "order_type": "limit",
            "price": 80000000,
            "volume": "0.001",
            "client_order_id": "cid-limit-1",
        }
    )

    assert payload == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "limit",
        "price": "80000000",
        "volume": "0.001",
        "client_order_id": "cid-limit-1",
    }
    assert BithumbPrivateAPI._query_string(payload) == (
        "market=KRW-BTC&side=bid&order_type=limit&price=80000000&volume=0.001&client_order_id=cid-limit-1"
    )


def test_build_order_payload_supports_limit_price_and_market_shapes():
    assert build_order_payload(
        market="KRW-BTC",
        side="BUY",
        ord_type="limit",
        price="80000000",
        volume="0.001",
        client_order_id="cid-limit-shape",
    ) == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "limit",
        "price": "80000000",
        "volume": "0.001",
        "client_order_id": "cid-limit-shape",
    }
    assert build_order_payload(
        market="KRW-BTC",
        side="SELL",
        ord_type="market",
        volume="0.1",
        client_order_id="cid-market-shape",
    ) == {
        "market": "KRW-BTC",
        "side": "ask",
        "order_type": "market",
        "volume": "0.1",
        "client_order_id": "cid-market-shape",
    }


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {"market": "KRW-BTC", "side": "bid", "order_type": "limit", "price": "1000"},
            "limit order requires both price and volume",
        ),
        (
            {"market": "KRW-BTC", "side": "ask", "order_type": "market", "price": "1000", "volume": "0.1"},
            "order_type=market must not include price",
        ),
        (
            {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "1000", "volume": "0.1"},
            "order_type=price must not include volume",
        ),
        (
            {"market": "KRW-BTC", "side": "bid", "ord_type": "price", "price": "1000"},
            "must use documented key 'order_type'",
        ),
    ],
)
def test_validate_order_submit_payload_blocks_invalid_request_shapes(payload, message):
    with pytest.raises(BrokerRejectError, match=message):
        validate_order_submit_payload(payload)


def test_broker_raw_order_submit_bypass_is_disabled_before_http(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "should-not-send"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="raw /v2/orders submit bypass is disabled"):
        broker._post_private("/v2/orders", {"market": "KRW-BTC", "side": "bid", "ord_type": "price", "price": "9999"}, retry_safe=False)

    with pytest.raises(BrokerRejectError, match="raw /v2/orders submit bypass is disabled"):
        broker._request_private(
            "POST",
            "/v2/orders",
            json_body={"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9999"},
            retry_safe=False,
        )

    assert _SequencedClient.calls == 0


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
    assert "params" not in call
    assert call["endpoint"] == "/v1/orders?market=KRW-BTC&state=wait"


def test_private_get_orders_transmitted_query_matches_jwt_query_hash(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, [])]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    payload = {
        "page": 1,
        "order_by": "desc",
        "uuids": ["open-1"],
        "client_order_ids": ["live_123_buy_abc"],
        "state": "wait",
        "limit": 100,
    }
    broker._get_private("/v1/orders", payload, retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))
    transmitted_query = call["endpoint"].split("?", 1)[1]

    assert transmitted_query == (
        "page=1&order_by=desc&uuids[]=open-1&client_order_ids[]=live_123_buy_abc&state=wait&limit=100"
    )
    assert claims["query_hash"] == hashlib.sha512(transmitted_query.encode("utf-8")).hexdigest()
    assert claims["query_hash_alg"] == "SHA512"


def test_private_get_orders_chance_401_invalid_query_payload_is_reported_with_error_name(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        _mk_response(
            401,
            {
                "error": {
                    "name": "invalid_query_payload",
                    "message": "Jwt query validation failed",
                }
            },
        )
    ]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError) as excinfo:
        broker._get_private("/v1/orders/chance", {"market": "KRW-BTC"}, retry_safe=False)

    message = str(excinfo.value)
    assert "status=401" in message
    assert "error_name=invalid_query_payload" in message
    code, summary = classify_private_api_error(excinfo.value)
    assert code == "AUTH_QUERY_HASH_MISMATCH"
    assert "query string/body hash" in summary


def test_private_request_rejects_missing_api_key_before_http(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    _SequencedClient.actions = [_mk_response(200, [])]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="missing API key"):
        broker._get_private("/v1/accounts", {}, retry_safe=False)

    assert _SequencedClient.calls == 0


def test_private_request_rejects_missing_api_secret_before_http(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")
    _SequencedClient.actions = [_mk_response(200, [])]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError, match="missing API secret"):
        broker._get_private("/v1/accounts", {}, retry_safe=False)

    assert _SequencedClient.calls == 0



def test_private_jwt_headers_include_query_hash_for_post_and_json_body(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"order_id": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    payload = {"market": "KRW-BTC", "side": "ask", "order_type": "market", "volume": "0.1"}
    api.submit_order(signed_request=_signed_order_request(payload), retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))

    assert claims["access_key"] == "k"
    assert "query_hash" in claims
    assert str(call["headers"]["Content-Type"]).startswith("application/json")
    assert call["content"] == b'{"market":"KRW-BTC","side":"ask","order_type":"market","volume":"0.1"}'
    assert "json" not in call


def test_private_jwt_headers_include_query_hash_for_delete(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"order_id": "cancel-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    broker._delete_private("/v2/order", {"order_id": "cancel-1", "client_order_id": "cid-cancel"}, retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))

    assert claims["access_key"] == "k"
    assert claims["query_hash"]
    assert call["endpoint"] == "/v2/order?order_id=cancel-1&client_order_id=cid-cancel"


def test_private_safe_get_retries_on_rate_limit(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        _mk_response(429, {"error": {"name": "too_many_requests", "message": "slow down"}}),
        _mk_response(200, [{"currency": "KRW", "balance": "1000", "locked": "0"}]),
    ]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    sleeps: list[float] = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda sec: sleeps.append(sec))
    monkeypatch.setattr("bithumb_bot.broker.bithumb._REQUEST_THROTTLER.acquire", lambda **_kwargs: 0.0)

    broker = BithumbBroker()
    data = broker._get_private("/v1/accounts", {}, retry_safe=True)

    assert isinstance(data, list)
    assert _SequencedClient.calls == 2
    assert sleeps == [0.2]


def test_private_request_classifies_rate_limit_and_order_not_ready_errors(monkeypatch):
    _configure_live()
    rate_limit = BithumbRateLimitError("bithumb private /v1/accounts throttled with http status=429 body={}")
    order_not_ready = BithumbOrderNotReadyError("bithumb private /v2/order rejected with http status=422 error_name=order_not_ready")

    rate_code, rate_summary = classify_private_api_error(rate_limit)
    ready_code, ready_summary = classify_private_api_error(order_not_ready)

    assert rate_code == "RATE_LIMITED"
    assert "rate limit" in rate_summary
    assert ready_code == "ORDER_NOT_READY"
    assert "not ready" in ready_summary


def test_private_request_bucket_limit_defaults_match_official_documented_limits(monkeypatch):
    monkeypatch.setattr("bithumb_bot.broker.bithumb.settings", SimpleNamespace())

    assert BithumbPrivateAPI._request_bucket_limit("order") == pytest.approx(10.0)
    assert BithumbPrivateAPI._request_bucket_limit("private") == pytest.approx(140.0)

def test_private_api_dry_run_allows_read_only_get_requests(monkeypatch):
    _SequencedClient.actions = [_mk_response(200, [{"currency": "KRW", "balance": "1000", "locked": "0"}])]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(
        api_key="k",
        api_secret="s",
        base_url="https://api.bithumb.com",
        dry_run=True,
    )
    data = api.request("GET", "/v1/accounts", params={}, retry_safe=False)

    assert isinstance(data, list)
    assert _SequencedClient.calls == 1
    call = _SequencedClient.requests[0]
    assert call["endpoint"] == "/v1/accounts"
    assert str(call["headers"].get("Authorization", "")).startswith("Bearer ")


def test_private_api_dry_run_blocks_private_write_requests(monkeypatch):
    _SequencedClient.actions = [_mk_response(200, {"status": "0000"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(
        api_key="k",
        api_secret="s",
        base_url="https://api.bithumb.com",
        dry_run=True,
    )
    with pytest.raises(BrokerRejectError, match="LIVE_DRY_RUN=true"):
        payload = {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9999"}
        api.submit_order(signed_request=_signed_order_request(payload), retry_safe=False)

    assert _SequencedClient.calls == 0


def test_private_api_rejects_direct_v2_orders_request_bypass(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "should-not-send"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)

    with pytest.raises(BrokerRejectError, match="direct /v2/orders private request is disabled"):
        api.request(
            "POST",
            "/v2/orders",
            json_body={"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9999"},
            retry_safe=False,
        )

    assert _SequencedClient.calls == 0


def test_private_api_rejects_forged_v2_orders_authority_token(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "should-not-send"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)

    with pytest.raises(BrokerRejectError, match="direct /v2/orders private request is disabled"):
        api.request(
            "POST",
            "/v2/orders",
            json_body={"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9999"},
            retry_safe=False,
            _order_submit_authority="validated_place_order_flow",
        )

    assert _SequencedClient.calls == 0


def test_submit_order_rejects_signed_request_payload_drift(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "should-not-send"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    payload = {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9999"}
    signed_request = _signed_order_request(payload)
    signed_request.payload["price"] = "10000"

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)

    with pytest.raises(BrokerRejectError, match="canonical payload mismatch"):
        api.submit_order(signed_request=signed_request, retry_safe=False)

    assert _SequencedClient.calls == 0


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


def test_accounts_rest_balance_allows_missing_base_on_live_armed_flat_start(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    monkeypatch.setattr("bithumb_bot.broker.balance_source._default_flat_start_safety_check", lambda: (True, "flat_start_safe"))
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": "1000", "locked": "25"},
        ],
    )

    balance = broker.get_balance()
    diag = broker.get_accounts_validation_diagnostics()

    assert balance.asset_available == 0.0
    assert balance.asset_locked == 0.0
    assert diag["preflight_outcome"] == "pass_no_position_allowed"
    assert diag["base_currency_missing_policy"] == "allow_flat_start_when_no_open_or_unresolved_exposure"
    assert diag["flat_start_allowed"] is True


def test_accounts_rest_balance_allows_missing_base_on_live_unarmed_flat_recovery_path(monkeypatch):
    _configure_live()
    monkeypatch.setattr(
        "bithumb_bot.broker.balance_source._default_flat_start_safety_check",
        lambda: (True, "flat_start_safe"),
    )
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {"currency": "KRW", "balance": "1000", "locked": "25"},
        ],
    )

    balance = broker.get_balance()
    diag = broker.get_accounts_validation_diagnostics()

    assert balance.asset_available == 0.0
    assert balance.asset_locked == 0.0
    assert diag["preflight_outcome"] == "pass_no_position_allowed"
    assert diag["base_currency_missing_policy"] == "allow_flat_start_when_no_open_or_unresolved_exposure"
    assert diag["flat_start_allowed"] is True


def test_accounts_rest_balance_blocks_missing_base_on_live_armed_when_unresolved_exists(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    monkeypatch.setattr(
        "bithumb_bot.broker.balance_source._default_flat_start_safety_check",
        lambda: (False, "local_unresolved_or_open_orders=1"),
    )
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
    assert diag["flat_start_allowed"] is False
    assert diag["flat_start_reason"] == "local_unresolved_or_open_orders=1"


def test_accounts_rest_balance_blocks_missing_base_on_live_unarmed_when_not_flat(monkeypatch):
    _configure_live()
    monkeypatch.setattr(
        "bithumb_bot.broker.balance_source._default_flat_start_safety_check",
        lambda: (False, "local_position_present=0.100000000000"),
    )
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
    assert diag["flat_start_allowed"] is False
    assert diag["flat_start_reason"] == "local_position_present=0.100000000000"


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

    assert call["endpoint"] == "/v1/orders/chance?market=KRW-BTC"
    assert claims["query_hash"] == hashlib.sha512(b"market=KRW-BTC").hexdigest()
    assert claims["query_hash_alg"] == "SHA512"



def test_place_order_market_buy_routes_to_v2_price_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )

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
    monkeypatch.setattr(
        broker,
        "_submit_validated_order_payload",
        lambda *, payload_plan, retry_safe=False: _fake_post_private(
            "/v2/orders",
            payload_plan.payload,
            retry_safe=retry_safe,
        ),
    )

    qty = _exact_lot_qty(market_price=150_000_000.0)
    order = broker.place_order(
        client_order_id="cid-1",
        side="BUY",
        qty=qty,
        price=None,
        buy_price_none_submit_contract=submit_contract,
    )

    assert order.exchange_order_id == "mkt-1"
    assert call["endpoint"] == "/v2/orders"
    assert call["retry_safe"] is False
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "price",
        "price": broker._format_krw_amount(Decimal("150000000.0") * Decimal(str(qty))),
        "client_order_id": "cid-1",
    }
    assert order.submit_contract_context is not None
    assert order.submit_contract_context["buy_price_none_decision_outcome"] == "pass"
    assert order.submit_contract_context["buy_price_none_decision_basis"] == "raw"
    assert order.submit_contract_context["buy_price_none_resolved_order_type"] == "price"
    assert order.submit_contract_context["buy_price_none_raw_supported_types"] == ["limit", "price"]


def test_place_order_accepts_valid_client_order_id_format(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )
    call: dict[str, object] = {}
    valid_client_order_id = "Abc_123-xyz_456-7890_ABC-def-ghi-jkl"

    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=150_000_000.0),
    )

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        return {"uuid": "mkt-valid-cid-1"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    qty = _exact_lot_qty(market_price=150_000_000.0)
    order = broker.place_order(
        client_order_id=valid_client_order_id,
        side="BUY",
        qty=qty,
        price=None,
        buy_price_none_submit_contract=submit_contract,
    )

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
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )

    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=0.0),
    )

    with pytest.raises(BrokerTemporaryError, match="failed to load validated best ask"):
        broker.place_order(
            client_order_id="cid-invalid-ask",
            side="BUY",
            qty=_exact_lot_qty(market_price=150_000_000.0),
            price=None,
            buy_price_none_submit_contract=submit_contract,
        )


def test_place_order_market_buy_blocks_on_quote_fetch_failure(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )

    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("orderbook offline")),
    )

    with pytest.raises(BrokerTemporaryError, match="failed to load validated best ask"):
        broker.place_order(
            client_order_id="cid-quote-fail",
            side="BUY",
            qty=_exact_lot_qty(market_price=150_000_000.0),
            price=None,
            buy_price_none_submit_contract=submit_contract,
        )



def test_place_order_market_sell_routes_to_v2_market_order(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    call: dict[str, object] = {}

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "mkt-2"}

    monkeypatch.setattr(
        broker,
        "_submit_validated_order_payload",
        lambda *, payload_plan, retry_safe=False: _fake_post_private(
            "/v2/orders",
            payload_plan.payload,
            retry_safe=retry_safe,
        ),
    )

    order = broker.place_order(client_order_id="cid-2", side="SELL", qty=0.4320, price=None)

    assert order.exchange_order_id == "mkt-2"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "ask",
        "order_type": "market",
        "volume": "0.432",
        "client_order_id": "cid-2",
    }


def test_build_order_list_params_preserves_supported_documented_contract_fields() -> None:
    params = build_order_list_params(
        market="KRW-BTC",
        uuids=("open-1",),
        client_order_ids=("cid-1",),
        state="wait",
        page=3,
        order_by="asc",
        limit=25,
    )

    assert params == {
        "market": "KRW-BTC",
        "uuids": ["open-1"],
        "client_order_ids": ["cid-1"],
        "state": "wait",
        "page": 3,
        "order_by": "asc",
        "limit": 25,
    }


def test_build_order_list_params_keeps_watch_separate_from_recovery_states() -> None:
    params = build_order_list_params(
        market="KRW-BTC",
        states=("watch",),
        page=1,
        order_by="desc",
        limit=10,
        allow_broad_scan=True,
    )

    assert params == {
        "market": "KRW-BTC",
        "states": ["watch"],
        "page": 1,
        "order_by": "desc",
        "limit": 10,
    }


def test_place_order_rejects_unsupported_side_or_type_from_chance_rules(monkeypatch):
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
                        "bid_price_unit": 1.0,
                        "ask_price_unit": 1.0,
                        "min_notional_krw": 5000.0,
                        "order_sides": ("bid",),
                        "order_types": ("limit",),
                    },
                )(),
            },
        )(),
    )

    with pytest.raises(BrokerRejectError, match="rejected order side before submit"):
        broker.place_order(client_order_id="cid-side", side="SELL", qty=0.4320, price=None)

    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )
    with pytest.raises(BrokerRejectError, match="buy_price_none_unsupported"):
        broker.place_order(
            client_order_id="cid-type",
            side="BUY",
            qty=0.4320,
            price=None,
            buy_price_none_submit_contract=submit_contract,
        )


def test_place_order_accepts_buy_market_notional_from_side_specific_chance_types(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

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
                        "order_sides": ("bid", "ask"),
                        "order_types": ("limit",),
                        "bid_types": ("limit", "price"),
                        "ask_types": ("limit", "market"),
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=150_000_000.0),
    )

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        return {"uuid": "mkt-chance-ok"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )

    order = broker.place_order(
        client_order_id="cid-chance-ok",
        side="BUY",
        qty=_exact_lot_qty(market_price=150_000_000.0),
        price=None,
        buy_price_none_submit_contract=submit_contract,
    )

    assert order.exchange_order_id == "mkt-chance-ok"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"]["order_type"] == "price"


def test_place_order_blocks_buy_market_notional_when_chance_only_advertises_market(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

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
                        "order_sides": ("bid", "ask"),
                        "order_types": ("limit", "market"),
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=150_000_000.0),
    )

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        return {"uuid": "mkt-chance-market-ok"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )

    with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support"):
        broker.place_order(
            client_order_id="cid-chance-market-ok",
            side="BUY",
            qty=_exact_lot_qty(market_price=150_000_000.0),
            price=None,
            buy_price_none_submit_contract=submit_contract,
        )

    assert call == {}


def test_buy_price_none_resolution_allows_explicit_price_only_support():
    rules = order_rules.DerivedOrderConstraints(
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        min_notional_krw=5000.0,
        order_sides=("bid", "ask"),
        order_types=("price",),
        bid_types=("price",),
    )

    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)

    assert resolution.allowed is True
    assert resolution.resolved_order_type == "price"
    assert resolution.block_reason == ""
    assert resolution.raw_supported_types == ("price",)
    assert resolution.support_source == "bid_types"

    order_rules.validate_order_chance_support(
        rules=rules,
        side="BUY",
        order_type="price",
        buy_price_none_resolution=resolution,
    )


def test_buy_price_none_resolution_blocks_limit_only_support():
    rules = order_rules.DerivedOrderConstraints(
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        min_notional_krw=5000.0,
        order_sides=("bid", "ask"),
        order_types=("limit",),
        bid_types=("limit",),
    )

    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)

    assert resolution.allowed is False
    assert resolution.resolved_order_type == "price"
    assert resolution.block_reason == "buy_price_none_unsupported"
    assert resolution.raw_supported_types == ("limit",)
    assert resolution.support_source == "bid_types"

    with pytest.raises(BrokerRejectError, match="buy_price_none_unsupported"):
        order_rules.validate_order_chance_support(
            rules=rules,
            side="BUY",
            order_type="price",
            buy_price_none_resolution=resolution,
        )


def test_buy_price_none_blocked_exception_context_matches_shared_diagnostic_fields(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    rules = order_rules.DerivedOrderConstraints(
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        min_notional_krw=5000.0,
        order_sides=("bid", "ask"),
        order_types=("limit", "market"),
        bid_types=("market",),
        ask_types=("limit", "market"),
    )
    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)
    diagnostic_fields = order_rules.build_buy_price_none_diagnostic_fields(
        rules=rules,
        resolution=resolution,
    )

    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: SimpleNamespace(rules=rules),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.resolve_buy_price_none_resolution",
        lambda *, rules: resolution,
    )
    submit_contract = _set_buy_price_none_submit_contract(broker, rules=rules)

    with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support") as excinfo:
        broker.place_order(
            client_order_id="cid-buy-price-none-market-only",
            side="BUY",
            qty=_exact_lot_qty(market_price=150_000_000.0),
            price=None,
            buy_price_none_submit_contract=submit_contract,
        )

    context = getattr(excinfo.value, "submit_contract_context", None)
    assert context is not None
    assert context["buy_price_none_allowed"] == diagnostic_fields["allowed"]
    assert context["buy_price_none_decision_outcome"] == "block"
    assert context["buy_price_none_decision_basis"] == diagnostic_fields["decision_basis"]
    assert context["buy_price_none_alias_used"] == diagnostic_fields["alias_used"]
    assert context["buy_price_none_alias_policy"] == diagnostic_fields["alias_policy"]
    assert context["buy_price_none_block_reason"] == "buy_price_none_requires_explicit_price_support"
    assert context["buy_price_none_support_source"] == diagnostic_fields["support_source"]
    assert context["buy_price_none_raw_supported_types"] == diagnostic_fields["raw_buy_supported_types"]
    assert context["buy_price_none_resolved_order_type"] == diagnostic_fields["resolved_order_type"]
    assert context["chance_validation_order_type"] == "price"
    assert context["exchange_order_type"] == "price"
    assert context["exchange_submit_field"] == "price"


@pytest.mark.parametrize(
    ("order_types", "bid_types", "allowed", "block_reason"),
    (
        (("price",), ("price",), True, ""),
        (("limit",), ("limit", "price"), True, ""),
        (("limit",), ("limit",), False, "buy_price_none_unsupported"),
        (("limit", "market"), (), False, "buy_price_none_requires_explicit_price_support"),
    ),
)
def test_buy_price_none_validation_and_submit_routing_share_same_resolution(
    monkeypatch,
    order_types,
    bid_types,
    allowed,
    block_reason,
):
    _configure_live()
    broker = BithumbBroker()
    rules = order_rules.DerivedOrderConstraints(
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        min_notional_krw=5000.0,
        order_sides=("bid", "ask"),
        order_types=order_types,
        bid_types=bid_types,
    )
    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)
    observed: dict[str, object] = {}
    call: dict[str, object] = {}

    assert resolution.allowed is allowed
    assert resolution.resolved_order_type == "price"
    assert resolution.block_reason == block_reason

    if allowed:
        order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="price")
    else:
        with pytest.raises(BrokerRejectError, match=block_reason):
            order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="price")

    original_validate = order_rules.validate_order_chance_support

    def _capture_validate(**kwargs):
        observed["buy_price_none_resolution"] = kwargs.get("buy_price_none_resolution")
        return original_validate(**kwargs)

    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: SimpleNamespace(rules=rules),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.resolve_buy_price_none_resolution",
        lambda *, rules: resolution,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.validate_order_chance_support",
        _capture_validate,
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=149_000_000.0, ask_price=150_000_000.0),
    )

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "mkt-shared-resolution"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)

    if allowed:
        submit_contract = _set_buy_price_none_submit_contract(broker, rules=rules)
        order = broker.place_order(
            client_order_id="cid-chance-shared-resolution",
            side="BUY",
            qty=_exact_lot_qty(market_price=150_000_000.0),
            price=None,
            buy_price_none_submit_contract=submit_contract,
        )
        assert order.submit_contract_context is not None
        assert order.submit_contract_context["buy_price_none_allowed"] is True
        assert order.submit_contract_context["buy_price_none_decision_outcome"] == "pass"
        assert order.submit_contract_context["buy_price_none_decision_basis"] == "raw"
        assert order.submit_contract_context["chance_validation_order_type"] == resolution.resolved_order_type
        assert order.submit_contract_context["buy_price_none_alias_used"] is False
        assert order.submit_contract_context["buy_price_none_alias_policy"] == resolution.alias_policy
        assert order.submit_contract_context["buy_price_none_block_reason"] == ""
        assert order.submit_contract_context["buy_price_none_raw_supported_types"] == list(
            resolution.raw_supported_types
        )
        assert order.submit_contract_context["buy_price_none_support_source"] == resolution.support_source
        assert order.submit_contract_context["buy_price_none_resolved_order_type"] == resolution.resolved_order_type
        assert order.submit_contract_context["exchange_order_type"] == resolution.resolved_order_type
        assert call["endpoint"] == "/v2/orders"
        assert call["retry_safe"] is False
        assert call["payload"]["order_type"] == resolution.resolved_order_type
    else:
        submit_contract = _set_buy_price_none_submit_contract(broker, rules=rules)
        with pytest.raises(BrokerRejectError, match=block_reason) as excinfo:
            broker.place_order(
                client_order_id="cid-chance-shared-resolution",
                side="BUY",
                qty=_exact_lot_qty(market_price=150_000_000.0),
                price=None,
                buy_price_none_submit_contract=submit_contract,
            )
        context = getattr(excinfo.value, "submit_contract_context", None)
        assert context is not None
        assert context["buy_price_none_allowed"] is False
        assert context["buy_price_none_decision_outcome"] == "block"
        assert context["buy_price_none_decision_basis"] == "raw"
        assert context["buy_price_none_alias_used"] is False
        assert context["buy_price_none_alias_policy"] == resolution.alias_policy
        assert context["buy_price_none_block_reason"] == block_reason
        assert context["buy_price_none_raw_supported_types"] == list(resolution.raw_supported_types)
        assert context["buy_price_none_support_source"] == resolution.support_source
        assert context["buy_price_none_resolved_order_type"] == resolution.resolved_order_type
        assert context["chance_validation_order_type"] == resolution.resolved_order_type
        assert call == {}

    assert observed["buy_price_none_resolution"] is resolution


def test_place_order_rejects_buy_price_none_precomputed_contract_mismatch(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    rules = order_rules.DerivedOrderConstraints(
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        min_notional_krw=5000.0,
        order_sides=("bid", "ask"),
        order_types=("limit",),
        bid_types=("price",),
        ask_types=("limit", "market"),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: SimpleNamespace(rules=rules),
    )
    with pytest.raises(BrokerRejectError, match="BUY price=None submit contract invalid before broker dispatch") as excinfo:
        broker.place_order(
            client_order_id="cid-buy-price-none-mismatch",
            side="BUY",
            qty=_exact_lot_qty(market_price=150_000_000.0),
            price=None,
            buy_price_none_submit_contract={
                **order_rules.build_buy_price_none_submit_contract_context(rules=rules),
                "market": "KRW-BTC",
                "order_side": "BUY",
                "buy_price_none_allowed": False,
                "buy_price_none_decision_outcome": "block",
                "buy_price_none_block_reason": "forced_mismatch",
            },
        )

    context = getattr(excinfo.value, "submit_contract_context", None)
    assert context == {}


def test_place_order_rejects_buy_price_none_missing_precomputed_contract(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    rules = order_rules.DerivedOrderConstraints(
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
        min_notional_krw=5000.0,
        order_sides=("bid", "ask"),
        order_types=("limit",),
        bid_types=("price",),
        ask_types=("limit", "market"),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.order_rules.get_effective_order_rules",
        lambda _pair: SimpleNamespace(rules=rules),
    )

    with pytest.raises(BrokerRejectError, match="BUY price=None submit contract missing before broker dispatch"):
        broker.place_order(
            client_order_id="cid-buy-price-none-missing-contract",
            side="BUY",
            qty=_exact_lot_qty(market_price=150_000_000.0),
            price=None,
        )


def test_place_order_blocks_volume_that_would_be_silently_truncated():
    _configure_live()
    broker = BithumbBroker()

    with pytest.raises(BrokerRejectError, match="qty requires explicit .*normalization before submit"):
        broker.place_order(client_order_id="cid-truncate", side="SELL", qty=0.123456789, price=None)


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

    qty = _exact_lot_qty(market_price=149500000.0)
    order = broker.place_order(client_order_id="cid-3", side="BUY", qty=qty, price=149500000)

    assert order.exchange_order_id == "lmt-2"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "limit",
        "price": "149500000",
        "volume": broker._format_volume(qty),
        "client_order_id": "cid-3",
    }
    assert order.raw == {
        "market": "KRW-BTC",
        "order_type": "limit",
        "ord_type": "limit",
        "client_order_id": "cid-3",
    }
    assert order.submit_contract_context is not None
    assert order.submit_contract_context["exchange_order_type"] == "limit"
    assert order.submit_contract_context["exchange_submit_field"] == "volume"


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

    qty = _exact_lot_qty(market_price=149500000.0)
    order = broker.place_order(client_order_id="cid-omit", side="BUY", qty=qty, price=149500000)

    assert order.exchange_order_id == "lmt-omit-client-id"
    assert call["payload"]["client_order_id"] == "cid-omit"
    assert order.raw == {
        "market": "KRW-BTC",
        "order_type": "limit",
        "ord_type": "limit",
        "client_order_id": "cid-omit",
    }
    assert order.submit_contract_context is not None
    assert order.submit_contract_context["exchange_order_type"] == "limit"
    assert order.submit_contract_context["exchange_submit_field"] == "volume"


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
        broker.place_order(client_order_id="cid-local", side="BUY", qty=_exact_lot_qty(market_price=149500000.0), price=149500000)


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

    order = broker.place_order(client_order_id="cid-coid-only", side="BUY", qty=_exact_lot_qty(market_price=149500000.0), price=149500000)

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
        broker.place_order(client_order_id="cid-primary", side="BUY", qty=_exact_lot_qty(market_price=149500000.0), price=149500000)


def test_place_order_limit_buy_normalizes_off_tick_price_at_execution_boundary(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

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
                        "order_types": ("limit",),
                        "bid_types": ("limit", "price"),
                        "ask_types": ("limit", "market"),
                        "order_sides": ("bid", "ask"),
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
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
                        "ask_price_unit": 10.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
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
                        "ask_price_unit": 10.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: call.update(
            {"endpoint": endpoint, "payload": payload, "retry_safe": retry_safe}
        ) or {"uuid": "buy-off-tick-1"},
    )

    qty = _exact_lot_qty(market_price=149500001.0)
    order = broker.place_order(client_order_id="cid-lmt-unit", side="BUY", qty=qty, price=149500001)

    assert order.exchange_order_id == "buy-off-tick-1"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "limit",
        "volume": broker._format_volume(qty),
        "price": broker._format_krw_amount(Decimal("149500000")),
        "client_order_id": "cid-lmt-unit",
    }
    assert call["payload"]["side"] == "bid"
    assert call["payload"]["volume"] == broker._format_volume(qty)
    assert call["payload"]["client_order_id"] == "cid-lmt-unit"


def test_place_order_limit_sell_normalizes_off_tick_price_at_execution_boundary(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

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
                        "ask_price_unit": 10.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: call.update(
            {"endpoint": endpoint, "payload": payload, "retry_safe": retry_safe}
        ) or {"uuid": "sell-off-tick-1"},
    )

    qty = _exact_lot_qty(market_price=149500001.0)
    order = broker.place_order(client_order_id="cid-sell-unit", side="SELL", qty=qty, price=149500001)

    assert order.exchange_order_id == "sell-off-tick-1"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "ask",
        "order_type": "limit",
        "volume": broker._format_volume(qty),
        "price": broker._format_krw_amount(Decimal("149500010")),
        "client_order_id": "cid-sell-unit",
    }
    assert call["payload"]["side"] == "ask"
    assert call["payload"]["volume"] == broker._format_volume(qty)
    assert call["payload"]["client_order_id"] == "cid-sell-unit"


def test_place_order_limit_sell_tick_normalization_preserves_lot_native_submit_qty(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

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
                        "ask_price_unit": 10.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: call.update(
            {"endpoint": endpoint, "payload": payload, "retry_safe": retry_safe}
        ) or {"uuid": "sell-lot-authority-1"},
    )

    sell_qty = _exact_lot_qty(market_price=149_500_001.0, lot_count=2)

    order = broker.place_order(
        client_order_id="cid-sell-lot-authority",
        side="SELL",
        qty=sell_qty,
        price=149_500_001,
    )

    assert order.exchange_order_id == "sell-lot-authority-1"
    assert call["endpoint"] == "/v2/orders"
    assert call["payload"]["price"] == broker._format_krw_amount(Decimal("149500010"))
    assert call["payload"]["volume"] == broker._format_volume(sell_qty)
    assert call["payload"]["volume"] != broker._format_volume(_exact_lot_qty(market_price=149_500_001.0, lot_count=1))
    assert float(order.qty_req) == pytest.approx(sell_qty)


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
        broker.place_order(client_order_id="cid-lmt-min", side="SELL", qty=_exact_lot_qty(market_price=13000000.0), price=13000000)



def test_place_order_market_buy_normalizes_total_to_bid_price_unit(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

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
                        "order_types": ("limit",),
                        "bid_types": ("limit", "price"),
                        "ask_types": ("limit", "market"),
                        "order_sides": ("bid", "ask"),
                    },
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=99_999_990.0, ask_price=99_999_990.0),
    )

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "mkt-unit-1"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)
    submit_contract = _set_buy_price_none_submit_contract(
        broker,
        rules=order_rules.get_effective_order_rules("KRW-BTC").rules,
    )

    qty = _exact_lot_qty(market_price=99_999_990.0)
    broker.place_order(
        client_order_id="cid-mkt-unit",
        side="BUY",
        qty=qty,
        price=None,
        buy_price_none_submit_contract=submit_contract,
    )

    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "order_type": "price",
        "price": broker._format_krw_amount(
            (Decimal("99999990.0") * Decimal(str(qty)) / Decimal("10")).to_integral_value(rounding=ROUND_DOWN)
            * Decimal("10")
        ),
        "client_order_id": "cid-mkt-unit",
    }


def test_place_order_market_sell_marks_price_tick_non_applicable_at_execution_boundary(monkeypatch, caplog):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

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
                        "ask_price_unit": 7.0,
                        "min_notional_krw": 5000.0,
                    },
                )(),
            },
        )(),
    )

    def _fake_post_private(endpoint, payload, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = payload
        call["retry_safe"] = retry_safe
        return {"uuid": "mkt-sell-unit-1"}

    monkeypatch.setattr(broker, "_post_private", _fake_post_private)
    monkeypatch.setattr(broker, "_truncate_volume", lambda value: float(value))

    qty = _exact_lot_qty(market_price=100_000_000.0, lot_count=2)
    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        broker.place_order(client_order_id="cid-mkt-sell-unit", side="SELL", qty=qty, price=None)

    assert call["endpoint"] == "/v2/orders"
    assert call["payload"] == {
        "market": "KRW-BTC",
        "side": "ask",
        "order_type": "market",
        "volume": broker._format_volume(qty),
        "client_order_id": "cid-mkt-sell-unit",
    }
    assert "submit_price_tick_applies=0" in caplog.text
    assert "submit_price_tick_unit=0.0" in caplog.text
    assert "submit_price_tick_reason=market_sell_price_tick_non_applicable" in caplog.text


def test_submit_price_tick_policy_marks_market_sell_non_applicable() -> None:
    policy = _resolve_submit_price_tick_policy(
        order_side="SELL",
        price=None,
        rules=SimpleNamespace(bid_price_unit=10.0, ask_price_unit=7.0),
    )

    assert policy.applies is False
    assert policy.price_unit == pytest.approx(0.0)
    assert policy.reason == "market_sell_price_tick_non_applicable"


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

    with pytest.raises(BrokerRejectError, match="identifier-scoped by bot policy"):
        broker.get_open_orders()

    with pytest.raises(BrokerRejectError, match="identifier-scoped by bot policy"):
        broker.get_recent_orders(limit=5)


def test_build_order_list_params_requires_explicit_broad_scan_for_recovery_queries() -> None:
    with pytest.raises(ValueError, match="use build_recovery_order_list_params"):
        build_order_list_params(market="KRW-BTC", states=("wait", "done", "cancel"))

    params = build_order_list_params(
        market="KRW-BTC",
        states=("wait", "done", "cancel"),
        page=2,
        order_by="desc",
        limit=25,
        allow_broad_scan=True,
    )

    assert params == {
        "market": "KRW-BTC",
        "states": ["wait", "done", "cancel"],
        "page": 2,
        "order_by": "desc",
        "limit": 25,
    }


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

    def _fake_delete(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = params
        call["retry_safe"] = retry_safe
        return {"order_id": params["order_id"], "client_order_id": params["client_order_id"]}

    monkeypatch.setattr(broker, "_delete_private", _fake_delete)

    order = broker.cancel_order(client_order_id="cid-cancel", exchange_order_id="cancel-1")

    assert order.exchange_order_id == "cancel-1"
    assert order.status == "CANCEL_REQUESTED"
    assert call == {
        "endpoint": "/v2/order",
        "payload": {"order_id": "cancel-1", "client_order_id": "cid-cancel"},
        "retry_safe": False,
    }


def test_request_cancel_order_uses_documented_order_id_field(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    monkeypatch.setattr(
        broker,
        "_delete_private",
        lambda endpoint, params, retry_safe=False: call.update({"endpoint": endpoint, "payload": params, "retry_safe": retry_safe}) or {"order_id": params["order_id"], "client_order_id": params["client_order_id"]},
    )

    order = broker.request_cancel_order(client_order_id="cid-cancel", order_id="cancel-1")

    assert order.exchange_order_id == "cancel-1"
    assert call == {
        "endpoint": "/v2/order",
        "payload": {"order_id": "cancel-1", "client_order_id": "cid-cancel"},
        "retry_safe": False,
    }


def test_request_cancel_order_prefers_order_id_when_both_identifiers_are_supplied(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    call: dict[str, object] = {}

    monkeypatch.setattr(
        broker,
        "_delete_private",
        lambda endpoint, params, retry_safe=False: call.update({"endpoint": endpoint, "payload": params, "retry_safe": retry_safe}) or {
            "order_id": params["order_id"],
            "client_order_id": params["client_order_id"],
        },
    )

    order = broker.request_cancel_order(
        client_order_id="cid-cancel",
        order_id="cancel-1",
        exchange_order_id="cancel-2",
    )

    assert order.exchange_order_id == "cancel-1"
    assert call == {
        "endpoint": "/v2/order",
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

    def _fake_delete(endpoint, params, retry_safe=False):
        call["endpoint"] = endpoint
        call["payload"] = params
        return {"client_order_id": params["client_order_id"]}

    monkeypatch.setattr(broker, "_delete_private", _fake_delete)

    order = broker.cancel_order(client_order_id="cid-cancel-only", exchange_order_id=None)

    assert call == {
        "endpoint": "/v2/order",
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

    monkeypatch.setattr(broker, "_delete_private", _reject)
    order = broker.cancel_order(client_order_id="cid-cancel", exchange_order_id="cancel-1")
    assert order.status == "CANCELED"


def test_cancel_order_maps_order_not_found_reject_to_reconcile_needed(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-404-1",
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
        "_delete_private",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            BrokerRejectError("status=404 error_name=order_not_found body={}")
        ),
    )

    with pytest.raises(BrokerRejectError, match="NOT_FOUND_NEEDS_RECONCILE"):
        broker.cancel_order(client_order_id="cid-cancel-404", exchange_order_id="cancel-404-1")


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
        "_delete_private",
        lambda endpoint, params, retry_safe=False: {
            "order_id": "different-order-id",
            "client_order_id": params["client_order_id"],
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
        "_delete_private",
        lambda endpoint, params, retry_safe=False: {
            "order_id": params["order_id"],
            "coid": params["client_order_id"],
        },
    )

    order = broker.cancel_order(client_order_id="cid-cancel-coid", exchange_order_id="cancel-coid-1")

    assert order.exchange_order_id == "cancel-coid-1"
    assert order.client_order_id == "cid-cancel-coid"
    assert order.status == "CANCEL_REQUESTED"


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
        "_delete_private",
        lambda endpoint, params, retry_safe=False: {
            "order_id": params["order_id"],
            "client_order_id": params["client_order_id"],
            "coid": "different-cid",
        },
    )

    with pytest.raises(BrokerRejectError, match="client identifier mismatch"):
        broker.cancel_order(client_order_id="cid-cancel-conflict", exchange_order_id="cancel-conflict-1")


def test_cancel_order_validates_client_order_id_even_when_exchange_id_is_present(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    with pytest.raises(BrokerRejectError, match="contains invalid characters"):
        broker.cancel_order(client_order_id="bad id", exchange_order_id="cancel-1")


def test_cancel_order_retries_when_order_not_ready_then_succeeds(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "BITHUMB_CANCEL_RETRY_ATTEMPTS", 3)
    object.__setattr__(settings, "BITHUMB_CANCEL_RETRY_BACKOFF_SEC", 0.01)
    broker = BithumbBroker()
    calls: dict[str, int] = {"delete": 0, "lookup": 0}
    sleeps: list[float] = []

    def _fake_get_order(*, client_order_id, exchange_order_id=None):
        calls["lookup"] += 1
        return broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-ready-1",
                "client_order_id": client_order_id,
                "side": "bid",
                "price": "149000000",
                "volume": "0.05",
                "remaining_volume": "0.05" if calls["lookup"] == 1 else "0.00",
                "executed_volume": "0.00" if calls["lookup"] == 1 else "0.05",
                "state": "wait" if calls["lookup"] == 1 else "done",
            },
            client_order_id=client_order_id,
        )

    def _fake_delete(endpoint, params, retry_safe=False):
        calls["delete"] += 1
        if calls["delete"] == 1:
            raise BrokerRejectError("order_not_ready")
        return {"order_id": params["order_id"], "client_order_id": params["client_order_id"], "state": "cancel"}

    monkeypatch.setattr(broker, "get_order", _fake_get_order)
    monkeypatch.setattr(broker, "_delete_private", _fake_delete)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda sec: sleeps.append(sec))

    order = broker.cancel_order(client_order_id="cid-cancel-ready", exchange_order_id="cancel-ready-1")

    assert calls == {"delete": 2, "lookup": 2}
    assert sleeps == [0.01]
    assert order.status == "FILLED"
    assert order.exchange_order_id == "cancel-ready-1"


def test_cancel_order_exhausts_retry_budget_for_order_not_ready(monkeypatch):
    _configure_live()
    object.__setattr__(settings, "BITHUMB_CANCEL_RETRY_ATTEMPTS", 2)
    object.__setattr__(settings, "BITHUMB_CANCEL_RETRY_BACKOFF_SEC", 0.01)
    broker = BithumbBroker()
    sleeps: list[float] = []

    monkeypatch.setattr(
        broker,
        "get_order",
        lambda client_order_id, exchange_order_id=None: broker._order_from_v2_row(
            {
                "order_id": exchange_order_id or "cancel-limit-1",
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
    monkeypatch.setattr(broker, "_delete_private", lambda *_args, **_kwargs: (_ for _ in ()).throw(BrokerRejectError("order_not_ready")))
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda sec: sleeps.append(sec))

    with pytest.raises(BrokerTemporaryError, match="cancel retry exhausted"):
        broker.cancel_order(client_order_id="cid-cancel-limit", exchange_order_id="cancel-limit-1")

    assert sleeps == [0.01]


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


def test_get_order_rejects_invalid_client_order_id_even_when_uuid_is_present(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    with pytest.raises(BrokerRejectError, match="contains invalid characters"):
        broker.get_order(client_order_id="bad id with space", exchange_order_id="filled-priority-1")


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


def test_get_recent_orders_for_recovery_uses_market_scoped_scan_without_identifiers(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    calls: list[dict[str, object]] = []

    def _fake_get(endpoint, params, retry_safe=False):
        calls.append({"endpoint": endpoint, "params": params, "retry_safe": retry_safe})
        states = tuple(params["states"])
        if states == ("wait", "done", "cancel"):
            return [
                {
                    "uuid": "recover-1",
                    "market": params["market"],
                    "ord_type": "limit",
                    "side": "bid",
                    "price": "149000000",
                    "volume": "0.05",
                    "remaining_volume": "0.05",
                    "executed_volume": "0.00",
                    "state": "wait",
                    "created_at": "2024-01-01T00:00:00+00:00",
                    "updated_at": "2024-01-01T00:00:00+00:00",
                }
            ]
        if states == ("watch",):
            return [
                {
                    "uuid": "recover-watch-1",
                    "market": params["market"],
                    "ord_type": "limit",
                    "side": "ask",
                    "price": "150000000",
                    "volume": "0.02",
                    "remaining_volume": "0.01",
                    "executed_volume": "0.01",
                    "state": "watch",
                    "created_at": "2024-01-01T00:01:00+00:00",
                    "updated_at": "2024-01-01T00:02:00+00:00",
                }
            ]
        raise AssertionError(states)

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    orders = broker.get_recent_orders_for_recovery(limit=5, market="KRW-BTC", page_size=2)

    assert [call["endpoint"] for call in calls] == ["/v1/orders", "/v1/orders"]
    assert calls[0]["params"]["market"] == "KRW-BTC"
    assert calls[0]["params"]["states"] == ["wait", "done", "cancel"]
    assert "uuids" not in calls[0]["params"]
    assert "client_order_ids" not in calls[0]["params"]
    assert calls[0]["retry_safe"] is True
    assert calls[1]["params"]["states"] == ["watch"]
    assert len(orders) == 2
    assert {order.exchange_order_id for order in orders} == {"recover-1", "recover-watch-1"}


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


def test_get_order_tolerates_missing_volume_when_derivable(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-derive-volume-1",
            "client_order_id": "cid-derive-volume-1",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "bid",
            "price": "149000000",
            "volume": "",
            "remaining_volume": "0.01",
            "executed_volume": "0.01",
            "created_at": "2024-01-01T00:00:00+00:00",
            "updated_at": "2024-01-01T00:00:01+00:00",
            "state": "wait",
        },
    )

    order = broker.get_order(client_order_id="cid-derive-volume-1")

    assert order.qty_req == pytest.approx(0.02)
    assert order.qty_filled == pytest.approx(0.01)


def test_get_order_done_tolerates_missing_volume_with_executed_funds(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "done-derived-volume-1",
            "client_order_id": "cid-done-derived-volume-1",
            "market": "KRW-BTC",
            "ord_type": "limit",
            "side": "ask",
            "price": "149000000",
            "volume": "",
            "remaining_volume": "",
            "executed_volume": "",
            "executed_funds": "1490000",
            "state": "done",
            "created_at": "2024-01-01T00:00:00+00:00",
            "updated_at": "2024-01-01T00:01:00+00:00",
        },
    )

    order = broker.get_order(client_order_id="cid-done-derived-volume-1")
    assert order.qty_req == pytest.approx(0.01)
    assert order.qty_filled == pytest.approx(0.01)
    assert order.status == "FILLED"


def test_get_open_orders_tolerates_missing_volume_with_alias_fields(monkeypatch):
    _configure_live()
    broker = BithumbBroker()
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: [
            {
                "uuid": "open-alias-1",
                "client_order_id": "cid-open-alias-1",
                "market": "KRW-BTC",
                "ord_type": "limit",
                "side": "bid",
                "price": "149000000",
                "volume": "",
                "remaining_volume": "",
                "executed_volume": "",
                "units": "0.05",
                "units_remaining": "0.05",
                "filled_volume": "0",
                "state": "wait",
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    orders = broker.get_open_orders(exchange_order_ids=["open-alias-1"])

    assert len(orders) == 1
    assert orders[0].qty_req == pytest.approx(0.05)
    assert orders[0].qty_filled == pytest.approx(0.0)


def test_get_recent_orders_tolerates_done_missing_volume_and_fee_variants(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint, params, retry_safe=False):
        state = str(params.get("state"))
        if state == "wait":
            return []
        if state == "done":
            return [
                {
                    "uuid": "done-missing-volume-1",
                    "client_order_id": "cid-done-missing-volume-1",
                    "market": "KRW-BTC",
                    "ord_type": "limit",
                    "side": "bid",
                    "price": "149000000",
                    "volume": "",
                    "remaining_volume": "",
                    "executed_volume": "0.02",
                    "paid_fee": "",
                    "reserved_fee": None,
                    "state": "done",
                    "created_at": "2024-01-01T00:00:00+00:00",
                    "updated_at": "2024-01-01T00:00:01+00:00",
                    "trades_count": 1,
                    "executed_funds": "2980000",
                }
            ]
        if state == "cancel":
            return []
        raise AssertionError(state)

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    orders = broker.get_recent_orders(limit=10, exchange_order_ids=["done-missing-volume-1"])
    assert len(orders) == 1
    order = orders[0]
    assert order.exchange_order_id == "done-missing-volume-1"
    assert order.qty_req == pytest.approx(0.02)
    assert order.qty_filled == pytest.approx(0.02)
    assert order.status == "FILLED"


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
        ({"fee": "not-a-number"}, "invalid fee field"),
        ({"fee": "nan"}, "invalid fee field"),
        ({"fee": "inf"}, "invalid fee field"),
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


@pytest.mark.parametrize(
    ("trade_row", "expected_error"),
    [
        ({"fee": ""}, "empty fee field 'fee' for materially sized fill"),
        ({"fee": None}, "empty fee field 'fee' for materially sized fill"),
        ({}, "missing fee field for materially sized fill"),
    ],
)
def test_get_fills_rejects_materially_sized_missing_or_empty_trade_fee(monkeypatch, trade_row, expected_error):
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


def test_get_fills_rejects_materially_sized_trade_zero_fee_value(monkeypatch):
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

    with pytest.raises(BrokerRejectError, match="zero fee field 'fee' for materially sized fill"):
        broker.get_fills(client_order_id="cid-zero-fee", exchange_order_id="filled-zero-fee")


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


@pytest.mark.parametrize("fee_value", ["", None])
def test_get_fills_fee_parsing_regression_tolerates_empty_values(monkeypatch, fee_value):
    _configure_live()
    broker = BithumbBroker()

    trade = {
        "uuid": "t-empty-fee",
        "price": "1000",
        "volume": "0.02",
        "created_at": "2024-01-01T00:00:00+00:00",
        "fee": fee_value,
    }
    monkeypatch.setattr(
        broker,
        "_get_private",
        lambda endpoint, params, retry_safe=False: {
            "uuid": "filled-empty-fee",
            "price": "1000",
            "volume": "0.02",
            "executed_volume": "0.02",
            "state": "done",
            "trades": [trade],
        },
    )

    fills = broker.get_fills(client_order_id="cid-empty-fee", exchange_order_id="filled-empty-fee")
    assert len(fills) == 1
    assert fills[0].fee == pytest.approx(0.0)


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
    assert str(call["headers"]["Content-Type"]).startswith("application/json")
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

    payload = {"market": "KRW-BTC", "side": "ask", "order_type": "market", "volume": "0.1"}
    api.submit_order(signed_request=_signed_order_request(payload), retry_safe=False)

    assert len(calls) == 1
    assert calls[0]["payload"] == payload
    assert calls[0]["nonce"] is not None
    assert calls[0]["timestamp"] is not None


def test_order_submit_auth_context_matches_official_claim_contract(monkeypatch):
    _configure_live()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.uuid.uuid4", lambda: "nonce-fixed")
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.time", lambda: 1712230310.689)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    payload = {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9998"}

    context = api._order_submit_auth_context(payload)

    assert context["canonical_payload"] == "market=KRW-BTC&side=bid&order_type=price&price=9998"
    assert context["request_body_text"] == '{"market":"KRW-BTC","side":"bid","order_type":"price","price":"9998"}'
    assert context["request_content"] == b'{"market":"KRW-BTC","side":"bid","order_type":"price","price":"9998"}'
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
    assert str(context["headers"]["Content-Type"]).startswith("application/json")
    assert context["headers"]["Authorization"].startswith("Bearer ")
    assert context["request_kwargs"] == {
        "content": b'{"market":"KRW-BTC","side":"bid","order_type":"price","price":"9998"}',
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
    assert str(call["headers"]["Content-Type"]).startswith("application/json")
    assert call["json"] == {"order_id": "abc123"}


@pytest.mark.parametrize(
    ("payload", "expected_content", "expected_query"),
    [
        (
            {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9999"},
            b'{"market":"KRW-BTC","side":"bid","order_type":"price","price":"9999"}',
            "market=KRW-BTC&side=bid&order_type=price&price=9999",
        ),
        (
            {"market": "KRW-BTC", "side": "ask", "order_type": "market", "volume": "0.1"},
            b'{"market":"KRW-BTC","side":"ask","order_type":"market","volume":"0.1"}',
            "market=KRW-BTC&side=ask&order_type=market&volume=0.1",
        ),
        (
            {"market": "KRW-BTC", "side": "bid", "order_type": "limit", "price": "149500000", "volume": "0.4"},
            b'{"market":"KRW-BTC","side":"bid","order_type":"limit","price":"149500000","volume":"0.4"}',
            "market=KRW-BTC&side=bid&order_type=limit&price=149500000&volume=0.4",
        ),
    ],
)
def test_order_submit_uses_json_body_with_query_hash_from_canonical_payload(monkeypatch, payload, expected_content, expected_query):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    api.submit_order(signed_request=_signed_order_request(payload), retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))

    assert str(call["headers"]["Content-Type"]).startswith("application/json")
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

    payload = {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "10002"}
    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    api.submit_order(signed_request=_signed_order_request(payload), retry_safe=False)

    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))
    request_body_text = call["content"].decode()

    assert str(call["headers"]["Content-Type"]).startswith("application/json")
    assert request_body_text == '{"market":"KRW-BTC","side":"bid","order_type":"price","price":"10002"}'
    assert claims["nonce"] == "nonce-fixed"
    assert claims["timestamp"] == 1712230310689
    canonical_query = "market=KRW-BTC&side=bid&order_type=price&price=10002"
    assert claims["query_hash"] == BithumbPrivateAPI._query_hash_from_canonical_payload(canonical_query)["query_hash"]
    assert claims["query_hash_alg"] == "SHA512"


def test_order_http_debug_request_logs_query_hash_and_json_body(monkeypatch, caplog):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"uuid": "created-1"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    api = BithumbPrivateAPI(api_key="k", api_secret="s", base_url="https://api.bithumb.com", dry_run=False)
    payload = {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9999"}
    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        api.submit_order(signed_request=_signed_order_request(payload), retry_safe=False)

    order_logs = [record.message for record in caplog.records if "[ORDER_HTTP_DEBUG] request" in record.message]
    assert order_logs
    assert "content_type=\"application/json" in order_logs[-1]
    assert "canonical_query_string=market=KRW-BTC&side=bid&order_type=price&price=9999" in order_logs[-1]
    assert "query_hash_alg=SHA512" in order_logs[-1]
    assert "nonce_present=1" in order_logs[-1]
    assert "timestamp_present=1" in order_logs[-1]
    assert "signed_payload_repr='market=KRW-BTC&side=bid&order_type=price&price=9999'" in order_logs[-1]
    assert 'transmitted_payload_repr=\'{"market":"KRW-BTC","side":"bid","order_type":"price","price":"9999"}\'' in order_logs[-1]


def test_order_http_debug_response_body_masks_sensitive_fields(monkeypatch, caplog):
    _configure_live()
    _SequencedClient.actions = [_mk_response(400, {"error": {"message": "Invalid request"}, "api_key": "leak-me"})]
    _SequencedClient.calls = 0
    _SequencedClient.requests = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        with pytest.raises(BrokerRejectError):
            broker.place_order(
                client_order_id="cid-mask-1",
                side="SELL",
                qty=0.1,
                price=None,
            )

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
    payload = {"market": "KRW-BTC", "side": "bid", "order_type": "price", "price": "9998"}

    with pytest.raises(BrokerRejectError) as excinfo:
        api.submit_order(signed_request=_signed_order_request(payload), retry_safe=False)

    assert "status=401" in str(excinfo.value)
    call = _SequencedClient.requests[0]
    auth = str(call["headers"]["Authorization"])
    claims = _decode_jwt(auth.removeprefix("Bearer "))
    canonical_payload = "market=KRW-BTC&side=bid&order_type=price&price=9998"

    assert str(call["headers"]["Content-Type"]).startswith("application/json")
    assert call["content"] == b'{"market":"KRW-BTC","side":"bid","order_type":"price","price":"9998"}'
    assert "json" not in call
    assert claims == {
        "access_key": "k",
        "nonce": "nonce-fixed",
        "timestamp": 1712230310689,
        "query_hash": hashlib.sha512(canonical_payload.encode("utf-8")).hexdigest(),
        "query_hash_alg": "SHA512",
    }




def test_get_recent_orders_tolerates_done_row_missing_updated_at_for_identifier_scoped_reconcile(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint: str, params: dict[str, object], retry_safe: bool = False):
        assert endpoint == "/v1/orders"
        state = params.get("state")
        if state == "wait":
            return []
        if state == "done":
            return [
                {
                    "uuid": "done-missing-updated-1",
                    "client_order_id": "live_1712230310689_buy_abcd1234",
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ord_type": "price",
                    "state": "done",
                    "price": "149000000",
                    "volume": "",
                    "remaining_volume": "",
                    "executed_volume": "0.01",
                    "created_at": "2024-04-04T13:45:10+09:00",
                    "updated_at": "",
                    "executed_funds": "1490000",
                    "fee": "745",
                }
            ]
        if state == "cancel":
            return []
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    orders = broker.get_recent_orders(limit=10, exchange_order_ids=["done-missing-updated-1"])

    assert len(orders) == 1
    order = orders[0]
    assert order.exchange_order_id == "done-missing-updated-1"
    assert order.client_order_id == "live_1712230310689_buy_abcd1234"
    assert order.status == "FILLED"
    assert order.updated_ts == order.created_ts


def test_get_recent_orders_tolerates_done_row_missing_price_with_avg_price_fallback(monkeypatch, caplog):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint: str, params: dict[str, object], retry_safe: bool = False):
        assert endpoint == "/v1/orders"
        state = params.get("state")
        if state == "wait":
            return []
        if state == "done":
            return [
                {
                    "uuid": "done-missing-price-avg-1",
                    "client_order_id": "live_1775658600000_sell_ae61703f",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "",
                    "avg_price": "105950000",
                    "volume": "0.0001",
                    "remaining_volume": "",
                    "executed_volume": "0.0001",
                    "executed_funds": "10595",
                    "created_at": "2024-04-04T13:45:10+09:00",
                    "updated_at": "2024-04-04T13:45:10+09:00",
                }
            ]
        if state == "cancel":
            return []
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        orders = broker.get_recent_orders(limit=10, exchange_order_ids=["done-missing-price-avg-1"])

    assert len(orders) == 1
    order = orders[0]
    assert order.exchange_order_id == "done-missing-price-avg-1"
    assert order.status == "FILLED"
    assert order.price == pytest.approx(105_950_000.0)
    assert "[V1_ORDERS_PRICE_RESOLUTION]" in caplog.text
    assert "price_source=avg_price" in caplog.text
    assert "price_missing=0" in caplog.text


def test_get_recent_orders_tolerates_done_row_missing_price_with_executed_funds_fallback(
    monkeypatch, caplog
):
    _configure_live()
    broker = BithumbBroker()

    def _fake_get(endpoint: str, params: dict[str, object], retry_safe: bool = False):
        assert endpoint == "/v1/orders"
        state = params.get("state")
        if state == "wait":
            return []
        if state == "done":
            return [
                {
                    "uuid": "done-missing-price-funds-1",
                    "client_order_id": "live_1775658600000_sell_ae61703f",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "",
                    "volume": "0.0001",
                    "remaining_volume": "",
                    "executed_volume": "0.0001",
                    "executed_funds": "10595",
                    "created_at": "2024-04-04T13:45:10+09:00",
                    "updated_at": "2024-04-04T13:45:10+09:00",
                }
            ]
        if state == "cancel":
            return []
        return []

    monkeypatch.setattr(broker, "_get_private", _fake_get)

    with caplog.at_level(logging.INFO, logger="bithumb_bot.run"):
        orders = broker.get_recent_orders(limit=10, exchange_order_ids=["done-missing-price-funds-1"])

    assert len(orders) == 1
    order = orders[0]
    assert order.exchange_order_id == "done-missing-price-funds-1"
    assert order.status == "FILLED"
    assert order.price == pytest.approx(105_950_000.0)
    assert "[V1_ORDERS_PRICE_RESOLUTION]" in caplog.text
    assert "price_source=executed_funds/executed_volume" in caplog.text
    assert "price_missing=0" in caplog.text


def test_parse_v1_order_list_row_tolerates_terminal_price_missing_confirmation_only() -> None:
    parsed = parse_v1_order_list_row(
        {
            "uuid": "done-missing-price-terminal-1",
            "client_order_id": "live_1775658600000_sell_ae61703f",
            "market": "KRW-BTC",
            "side": "ask",
            "ord_type": "limit",
            "state": "done",
            "price": "",
            "volume": "",
            "remaining_volume": "",
            "executed_volume": "",
            "created_at": "2024-04-04T13:45:10+09:00",
            "updated_at": "2024-04-04T13:45:10+09:00",
        }
    )

    assert parsed.state == "done"
    assert parsed.price is None
    assert parsed.price_missing is True
    assert parsed.price_source == "terminal_confirmation_only"
    assert "price:missing_terminal_confirmation_only" in parsed.degraded_fields

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
