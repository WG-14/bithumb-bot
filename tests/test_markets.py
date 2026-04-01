from __future__ import annotations

import json

import httpx
import pytest

from bithumb_bot.markets import (
    ExchangeMarketCodeError,
    MarketCatalogClient,
    MarketCatalogError,
    MarketContractDriftError,
    MarketInfo,
    MarketRegistry,
    UnsupportedMarketError,
    canonical_market_id,
    canonical_market_with_raw,
    get_market_registry,
    parse_documented_market_code,
    parse_exchange_market_response_code,
    parse_user_market_input,
    validate_exchange_market_code,
)
from bithumb_bot.public_api import PublicApiResponseError, PublicApiSchemaError, PublicApiTransientError


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
    raise_request_errors: list[Exception | None] | None = None
    status_codes: list[int] | None = None
    payloads: list[object] | None = None
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
        if self.__class__.raise_request_errors:
            req_err = self.__class__.raise_request_errors.pop(0)
            if req_err is not None:
                raise req_err
        elif self.__class__.raise_request_error is not None:
            raise self.__class__.raise_request_error

        status_code = self.__class__.status_codes.pop(0) if self.__class__.status_codes else self.__class__.status_code
        payload = self.__class__.payloads.pop(0) if self.__class__.payloads else self.__class__.payload
        return _FakeResponse(payload, status_code=status_code, text=self.__class__.text)


def _reset_fake_client() -> None:
    _FakeClient.payload = []
    _FakeClient.status_code = 200
    _FakeClient.text = ""
    _FakeClient.raise_request_error = None
    _FakeClient.raise_request_errors = None
    _FakeClient.status_codes = None
    _FakeClient.payloads = None
    _FakeClient.requests = []


class TestUserInputCompatibility:
    def test_accepts_legacy_alias_and_bare_symbol_for_user_input_layer(self) -> None:
        assert parse_user_market_input("BTC_KRW") == "KRW-BTC"
        assert parse_user_market_input("btc_krw") == "KRW-BTC"
        assert parse_user_market_input(" BTC_KRW ") == "KRW-BTC"
        assert parse_user_market_input("BTC") == "KRW-BTC"

    def test_canonical_market_with_raw_tracks_noncanonical_input(self) -> None:
        canonical, raw_symbol = canonical_market_with_raw("BTC_KRW")
        assert canonical == "KRW-BTC"
        assert raw_symbol == "BTC_KRW"

        canonical2, raw_symbol2 = canonical_market_with_raw("KRW-BTC")
        assert canonical2 == "KRW-BTC"
        assert raw_symbol2 is None


class TestExchangeContractValidation:
    def test_canonical_allowed_but_bare_and_legacy_rejected_at_exchange_boundary(self) -> None:
        registry = MarketRegistry([MarketInfo(market="KRW-BTC")])

        assert validate_exchange_market_code("KRW-BTC", registry=registry) == "KRW-BTC"

        with pytest.raises(ExchangeMarketCodeError, match="canonical QUOTE-BASE"):
            validate_exchange_market_code("BTC", registry=registry)
        with pytest.raises(ExchangeMarketCodeError, match="canonical QUOTE-BASE"):
            validate_exchange_market_code("BTC_KRW", registry=registry)

    def test_parse_documented_market_code_accepts_only_canonical_quote_base(self) -> None:
        assert parse_documented_market_code("krw-btc") == "KRW-BTC"

        with pytest.raises(ExchangeMarketCodeError):
            parse_documented_market_code("BTC")
        with pytest.raises(ExchangeMarketCodeError):
            parse_documented_market_code("BTC_KRW")

    def test_exchange_response_market_mismatch_is_hard_failure(self) -> None:
        assert parse_exchange_market_response_code("KRW-BTC", requested_market="KRW-BTC") == "KRW-BTC"

        with pytest.raises(MarketContractDriftError, match="mismatch"):
            parse_exchange_market_response_code("KRW-ETH", requested_market="KRW-BTC")

    def test_exchange_response_non_documented_shape_is_contract_drift(self) -> None:
        with pytest.raises(MarketContractDriftError, match="drift"):
            parse_exchange_market_response_code("BTC_KRW", requested_market="KRW-BTC")

    def test_canonical_market_id_requires_catalog_membership(self, monkeypatch) -> None:
        registry = MarketRegistry([])
        monkeypatch.setattr("bithumb_bot.markets.get_market_registry", lambda: registry)

        with pytest.raises(UnsupportedMarketError, match="unsupported market"):
            canonical_market_id("BTC_KRW")


class TestMarketCatalogSchemaContract:
    def test_fetch_market_all_uses_documented_params(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.payload = [
            {"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin", "market_warning": "NONE"}
        ]
        monkeypatch.setattr("httpx.Client", _FakeClient)

        items = MarketCatalogClient().fetch_markets(is_details=True)

        assert len(items) == 1
        assert items[0].market == "KRW-BTC"
        assert _FakeClient.requests[0] == {"path": "/v1/market/all", "params": {"isDetails": "true"}}

    def test_rejects_payload_type_drift(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.payload = {"market": "KRW-BTC"}
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(MarketCatalogError, match="payload type"):
            MarketCatalogClient().fetch_markets()

    def test_rejects_empty_payload(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.payload = []
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(MarketCatalogError, match="empty"):
            MarketCatalogClient().fetch_markets()

    def test_rejects_legacy_market_format_from_market_all(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.payload = [{"market": "BTC_KRW", "korean_name": "비트코인", "english_name": "Bitcoin"}]
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(MarketCatalogError, match="invalid market code format"):
            MarketCatalogClient().fetch_markets()

    def test_rejects_missing_required_fields(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.payload = [{"market": "KRW-BTC"}]
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(MarketCatalogError, match="missing required field: korean_name"):
            MarketCatalogClient().fetch_markets()

    def test_market_warning_policy_requires_string_in_details_mode(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.payload = [
            {"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin", "market_warning": 1}
        ]
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(MarketCatalogError, match="market_warning"):
            MarketCatalogClient().fetch_markets(is_details=True)

    def test_retries_transient_error_but_not_schema_error(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.status_codes = [503, 200]
        _FakeClient.payloads = [
            {"error": {"name": "temporarily_unavailable", "message": "retry"}},
            [{"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin"}],
        ]
        monkeypatch.setattr("httpx.Client", _FakeClient)

        items = MarketCatalogClient(max_retries=1, base_backoff_sec=0.0, max_backoff_sec=0.0, jitter_sec=0.0).fetch_markets()

        assert [item.market for item in items] == ["KRW-BTC"]
        assert len(_FakeClient.requests) == 2

        _reset_fake_client()
        _FakeClient.payload = [{"market": "KRW-BTC", "korean_name": "비트코인", "english_name": 123}]
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(PublicApiSchemaError, match="english_name"):
            MarketCatalogClient(max_retries=3, base_backoff_sec=0.0, max_backoff_sec=0.0, jitter_sec=0.0).fetch_markets()
        assert len(_FakeClient.requests) == 1

    def test_propagates_http_json_and_network_errors(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.status_code = 400
        _FakeClient.payload = {"error": {"name": "invalid_parameter", "message": "isDetails must be true/false"}}
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(PublicApiResponseError, match="invalid_parameter"):
            MarketCatalogClient().fetch_markets()

        _reset_fake_client()
        _FakeClient.payload = json.JSONDecodeError("Expecting value", "<html>", 0)
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(PublicApiResponseError, match="invalid json"):
            MarketCatalogClient().fetch_markets()

        _reset_fake_client()
        _FakeClient.raise_request_error = httpx.ConnectError("network down")
        monkeypatch.setattr("httpx.Client", _FakeClient)

        with pytest.raises(PublicApiTransientError, match="transient failure after retries"):
            MarketCatalogClient().fetch_markets()


class TestRegistryCacheRegression:
    def test_get_market_registry_uses_cache(self, monkeypatch) -> None:
        _reset_fake_client()
        _FakeClient.payload = [{"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin"}]
        monkeypatch.setattr("httpx.Client", _FakeClient)

        first = get_market_registry(refresh=True)
        second = get_market_registry()

        assert first is second
        assert len(_FakeClient.requests) == 1
    def test_get_market_registry_refresh_and_ttl_policies(self, monkeypatch) -> None:
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
