from __future__ import annotations

import pytest

from bithumb_bot.markets import (
    MarketCatalogClient,
    MarketCatalogError,
    MarketRegistry,
    UnsupportedMarketError,
    normalize_market_id,
    normalize_market_id_with_registry,
)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeClient:
    payload = []
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
        return _FakeResponse(self.__class__.payload)


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
    _FakeClient.payload = [
        {"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin", "market_warning": "NONE"}
    ]
    _FakeClient.requests = []
    monkeypatch.setattr("httpx.Client", _FakeClient)

    items = MarketCatalogClient().fetch_markets()

    assert len(items) == 1
    assert items[0].market == "KRW-BTC"
    assert items[0].english_name == "Bitcoin"
    assert _FakeClient.requests[0] == {"path": "/v1/market/all", "params": {"isDetails": "false"}}


def test_catalog_fetch_raises_on_empty_payload(monkeypatch) -> None:
    _FakeClient.payload = []
    _FakeClient.requests = []
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(MarketCatalogError, match="empty"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_raises_on_invalid_payload_type(monkeypatch) -> None:
    _FakeClient.payload = {"market": "KRW-BTC"}
    _FakeClient.requests = []
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(MarketCatalogError, match="payload type"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_supports_is_details_true(monkeypatch) -> None:
    _FakeClient.payload = [{"market": "KRW-BTC"}]
    _FakeClient.requests = []
    monkeypatch.setattr("httpx.Client", _FakeClient)

    MarketCatalogClient().fetch_markets(is_details=True)

    assert _FakeClient.requests[0] == {"path": "/v1/market/all", "params": {"isDetails": "true"}}
