from __future__ import annotations

import json

import httpx
import pytest

from bithumb_bot.markets import (
    MarketCatalogClient,
    MarketCatalogError,
    MarketInfo,
    MarketRegistry,
    UnsupportedMarketError,
    canonical_market_with_raw,
    normalize_market_id,
    normalize_market_id_with_registry,
    validate_exchange_market_id,
    canonical_market_id,
    get_market_registry,
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


def test_normalize_market_id_is_not_naive_string_reverse() -> None:
    # 회귀 계약: canonical 형태(QUOTE-BASE)는 추가 뒤집기 없이 그대로 유지한다.
    assert normalize_market_id("BTC-KRW") == "BTC-KRW"
    assert normalize_market_id("USDT-ETH") == "USDT-ETH"


def test_normalize_market_id_with_registry_rejects_unsupported() -> None:
    registry = MarketRegistry([])
    with pytest.raises(UnsupportedMarketError, match="unsupported market"):
        normalize_market_id_with_registry("ETH_KRW", registry=registry)


def test_validate_exchange_market_id_requires_canonical_quote_base_without_implicit_default_quote() -> None:
    registry = MarketRegistry([MarketInfo(market="KRW-BTC")])
    assert validate_exchange_market_id("KRW-BTC", registry=registry) == "KRW-BTC"
    with pytest.raises(ValueError, match="canonical QUOTE-BASE"):
        validate_exchange_market_id("BTC", registry=registry)


def test_canonical_market_with_raw_tracks_noncanonical_input() -> None:
    canonical, raw_symbol = canonical_market_with_raw("BTC_KRW")
    assert canonical == "KRW-BTC"
    assert raw_symbol == "BTC_KRW"

    canonical2, raw_symbol2 = canonical_market_with_raw("KRW-BTC")
    assert canonical2 == "KRW-BTC"
    assert raw_symbol2 is None


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
    _FakeClient.payload = [
        {"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin", "market_warning": "NONE"}
    ]
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


def test_catalog_fetch_raises_when_row_is_not_dict(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = ["KRW-BTC"]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(MarketCatalogError, match="row type"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_raises_when_required_market_field_missing(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = [{"english_name": "Bitcoin"}]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(MarketCatalogError, match="required field missing"):
        MarketCatalogClient().fetch_markets()


def test_catalog_fetch_raises_when_detail_fields_missing_with_is_details_true(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = [{"market": "KRW-BTC"}]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    with pytest.raises(MarketCatalogError, match="required field missing"):
        MarketCatalogClient().fetch_markets(is_details=True)


def test_catalog_fetch_allows_optional_detail_fields_with_is_details_false(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = [{"market": "KRW-BTC"}]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    items = MarketCatalogClient().fetch_markets(is_details=False)

    assert items[0].market == "KRW-BTC"
    assert items[0].korean_name is None
    assert items[0].english_name is None
    assert items[0].market_warning is None


def test_extract_api_error_parses_known_shape() -> None:
    assert extract_api_error({"error": {"name": "invalid_param", "message": "bad request"}}) == (
        "invalid_param",
        "bad request",
    )


def test_canonical_market_id_requires_catalog_membership(monkeypatch) -> None:
    registry = MarketRegistry([])
    monkeypatch.setattr("bithumb_bot.markets.get_market_registry", lambda: registry)

    with pytest.raises(UnsupportedMarketError, match="unsupported market"):
        canonical_market_id("BTC_KRW")


def test_canonical_market_id_rejects_flipped_legacy_alias_even_when_tokens_look_valid(monkeypatch) -> None:
    registry = MarketRegistry([MarketInfo(market="KRW-BTC")])
    monkeypatch.setattr("bithumb_bot.markets.get_market_registry", lambda: registry)

    with pytest.raises(UnsupportedMarketError, match="canonical=BTC-KRW"):
        canonical_market_id("KRW_BTC")


def test_get_market_registry_uses_cache(monkeypatch) -> None:
    _reset_fake_client()
    _FakeClient.payload = [{"market": "KRW-BTC"}]
    monkeypatch.setattr("httpx.Client", _FakeClient)

    first = get_market_registry(refresh=True)
    second = get_market_registry()

    assert first is second
    assert len(_FakeClient.requests) == 1


def test_get_market_registry_refreshes_when_ttl_expires(monkeypatch) -> None:
    import bithumb_bot.markets as markets_mod

    calls = {"count": 0}
    monotonic = {"value": 100.0}

    def _fake_from_catalog(*, client=None, is_details=False):
        del client, is_details
        calls["count"] += 1
        suffix = "BTC" if calls["count"] == 1 else "ETH"
        return MarketRegistry([MarketInfo(market=f"KRW-{suffix}")])

    monkeypatch.setattr(markets_mod.MarketRegistry, "from_catalog", _fake_from_catalog)
    monkeypatch.setattr(markets_mod.time, "monotonic", lambda: monotonic["value"])
    monkeypatch.setattr(markets_mod, "_market_registry_cache_by_detail", {})
    monkeypatch.setattr(markets_mod, "_market_registry_cached_at_monotonic_by_detail", {})

    first = get_market_registry(ttl_seconds=10)
    monotonic["value"] = 109.0
    second = get_market_registry(ttl_seconds=10)
    monotonic["value"] = 120.0
    third = get_market_registry(ttl_seconds=10)

    assert first is second
    assert third is not second
    assert calls["count"] == 2


def test_get_market_registry_forces_refresh_when_requested(monkeypatch) -> None:
    import bithumb_bot.markets as markets_mod

    calls = {"count": 0}

    def _fake_from_catalog(*, client=None, is_details=False):
        del client, is_details
        calls["count"] += 1
        return MarketRegistry([MarketInfo(market=f"KRW-FAKE{calls['count']}")])

    monkeypatch.setattr(markets_mod.MarketRegistry, "from_catalog", _fake_from_catalog)
    monkeypatch.setattr(markets_mod, "_market_registry_cache_by_detail", {})
    monkeypatch.setattr(markets_mod, "_market_registry_cached_at_monotonic_by_detail", {})

    first = get_market_registry(ttl_seconds=300)
    second = get_market_registry(refresh=True, ttl_seconds=300)

    assert second is not first
    assert calls["count"] == 2


def test_get_market_registry_ttl_zero_disables_reuse(monkeypatch) -> None:
    import bithumb_bot.markets as markets_mod

    calls = {"count": 0}

    def _fake_from_catalog(*, client=None, is_details=False):
        del client, is_details
        calls["count"] += 1
        return MarketRegistry([MarketInfo(market=f"KRW-FAKE{calls['count']}")])

    monkeypatch.setattr(markets_mod.MarketRegistry, "from_catalog", _fake_from_catalog)
    monkeypatch.setattr(markets_mod, "_market_registry_cache_by_detail", {})
    monkeypatch.setattr(markets_mod, "_market_registry_cached_at_monotonic_by_detail", {})

    first = get_market_registry(ttl_seconds=0)
    second = get_market_registry(ttl_seconds=0)

    assert second is not first
    assert calls["count"] == 2


def test_get_market_registry_caches_details_and_non_details_separately(monkeypatch) -> None:
    import bithumb_bot.markets as markets_mod

    calls: list[bool] = []

    def _fake_from_catalog(*, client=None, is_details=False):
        del client
        calls.append(bool(is_details))
        suffix = "DETAIL" if is_details else "BASIC"
        return MarketRegistry([MarketInfo(market=f"KRW-{suffix}")])

    monkeypatch.setattr(markets_mod.MarketRegistry, "from_catalog", _fake_from_catalog)
    monkeypatch.setattr(markets_mod, "_market_registry_cache_by_detail", {})
    monkeypatch.setattr(markets_mod, "_market_registry_cached_at_monotonic_by_detail", {})

    first_basic = get_market_registry(is_details=False, ttl_seconds=300)
    first_detail = get_market_registry(is_details=True, ttl_seconds=300)
    second_basic = get_market_registry(is_details=False, ttl_seconds=300)
    second_detail = get_market_registry(is_details=True, ttl_seconds=300)

    assert first_basic is second_basic
    assert first_detail is second_detail
    assert first_basic is not first_detail
    assert calls == [False, True]
