from __future__ import annotations

import json

import httpx
import pytest

from bithumb_bot.markets import (
    MarketCatalogClient,
    MarketCatalogError,
    MarketRegistry,
    UnsupportedMarketError,
    normalize_market_id,
    normalize_market_id_with_registry,
)
from bithumb_bot.public_api import (
    PublicApiRequestError,
    PublicApiResponseError,
    PublicApiSchemaError,
    extract_api_error,
)


class _FakeResponse:
    def __init__(self, payload=None, *, status_code: int = 200, text: str = ""):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _FakeClient:
    payload = []
    status_code = 200
    text = ""
    raise_request_error: Exception | None = None
    requests = []

    def __init__(self, *, base_url: str, timeout: float) -> None:
        self.base_url = base_url
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, path: str, params=None):
        self.__class__.requests.append({"path": path, "params": params})
        if self.__class__.raise_request_error is not None:
            raise self.__class__.raise_request_error
        return _FakeResponse(self.__class__.payload, status_code=self.__class__.status_code, text=self.__class__.text)


def _reset_fake_client() -> None:
    _FakeClient.payload = []
    _FakeClient.status_code = 200
    _FakeClient.text = ""
    _FakeClient.raise_request_error = None
    _FakeClient.requests = []


def test_normalize_market_id_aliases() -> None:
    assert normalize_market_id("BTC_KRW") == "KRW-BTC"
    assert normalize_market_id("btc_krw") == "KRW-BTC"
    assert normalize_market_id("KRW-BTC") == "KRW-BTC"
    assert normalize_market_id(" BTC_KRW ") == "KRW-BTC"
    assert normalize_market_id("BTC") == "KRW-BTC"


def test_normalize_market_id_with_registry_rejects_unsupported() -> None:
    registry = MarketRegistry([])
    with pytest.raises(UnsupportedMarketError, match="unsupported market"):
        normalize_market_id_with_registry("ETH_KRW", registry=registry)


def test_catalog_fetch_parses_market_all(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = [
        {"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin", "market_warning": "NONE"}
    ]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    items = MarketCatalogClient().fetch_markets()

    assert len(items) == 1
    assert items[0].market == "KRW-BTC"
    assert items[0].english_name == "Bitcoin"
    assert _FakeClient.requests[0] == {"path": "/v1/market/all", "params": {"isDetails": "false"}}


def test_catalog_fetch_raises_on_empty_payload(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = []
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(MarketCatalogError, match="empty"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_raises_on_invalid_payload_type(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = {"market": "KRW-BTC"}
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(MarketCatalogError, match="payload type"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_supports_is_details_true(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = [{"market": "KRW-BTC"}]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    MarketCatalogClient().fetch_markets(is_details=True)

    assert _FakeClient.requests[0] == {"path": "/v1/market/all", "params": {"isDetails": "true"}}


def test_catalog_fetch_raises_on_http_error_with_api_error_body(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.status_code = 400
    _FakeClient.payload = {"error": {"name": "invalid_parameter", "message": "isDetails must be true/false"}}
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(PublicApiResponseError, match="invalid_parameter"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_raises_on_non_json_response(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = json.JSONDecodeError("Expecting value", "<html>", 0)
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(PublicApiResponseError, match="invalid json"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_raises_on_network_error(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.raise_request_error = httpx.ConnectError("network down")
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(PublicApiRequestError, match="request failed"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_raises_on_schema_mismatch_field_type(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = [{"market": "KRW-BTC", "english_name": 123}]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(PublicApiSchemaError, match="english_name"):
        MarketCatalogClient().fetch_markets()


def test_extract_api_error_parses_known_shape() -> None:
    assert extract_api_error({"error": {"name": "invalid_param", "message": "bad request"}}) == (
        "invalid_param",
        "bad request",
    )
