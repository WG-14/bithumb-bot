from __future__ import annotations

import httpx
import pytest

from bithumb_bot.public_api import PublicApiResponseError, PublicApiSchemaError
from bithumb_bot.public_api_ticker import (
    fetch_ticker_batch,
    fetch_ticker_single,
    fetch_ticker,
    normalize_single_ticker_market,
    normalize_ticker_markets,
    parse_ticker_lite_payload,
    parse_ticker_payload,
)


def _sample_ticker() -> dict[str, object]:
    return {
        "market": "KRW-BTC",
        "trade_date": "20260331",
        "trade_time": "120000",
        "trade_date_kst": "20260331",
        "trade_time_kst": "210000",
        "trade_timestamp": 1_743_379_200_000,
        "opening_price": 100.0,
        "high_price": 120.0,
        "low_price": 90.0,
        "trade_price": 110.0,
        "prev_closing_price": 99.0,
        "change": "RISE",
        "change_price": 11.0,
        "signed_change_price": 11.0,
        "change_rate": 0.111,
        "signed_change_rate": 0.111,
        "acc_trade_price": 123_456.0,
        "acc_trade_price_24h": 223_456.0,
        "acc_trade_volume": 7.89,
        "acc_trade_volume_24h": 8.9,
    }


def test_parse_ticker_lite_payload_success_with_required_subset_fields() -> None:
    tickers = parse_ticker_lite_payload([_sample_ticker()])
    assert len(tickers) == 1
    assert tickers[0].market == "KRW-BTC"
    assert tickers[0].trade_price == 110.0


def test_normalize_ticker_markets_accepts_comma_string() -> None:
    assert normalize_ticker_markets("krw-btc, KRW-ETH") == "KRW-BTC,KRW-ETH"


def test_normalize_ticker_markets_accepts_iterable_and_dedupes() -> None:
    assert normalize_ticker_markets(["krw-btc", "KRW-BTC", "krw-eth"]) == "KRW-BTC,KRW-ETH"


def test_normalize_ticker_markets_rejects_noncanonical_format() -> None:
    with pytest.raises(ValueError, match="canonical QUOTE-BASE"):
        normalize_ticker_markets(["BTC_KRW"])


def test_normalize_ticker_markets_rejects_bare_symbol() -> None:
    with pytest.raises(ValueError, match="canonical QUOTE-BASE"):
        normalize_ticker_markets(["BTC"])


def test_normalize_single_ticker_market_requires_exactly_one_market() -> None:
    assert normalize_single_ticker_market("krw-btc") == "KRW-BTC"


def test_parse_ticker_lite_payload_fails_when_required_field_missing() -> None:
    payload = _sample_ticker()
    del payload["trade_price"]
    with pytest.raises(PublicApiSchemaError, match="missing_fields=trade_price"):
        parse_ticker_lite_payload([payload])


def test_parse_ticker_lite_payload_fails_on_type_mismatch() -> None:
    payload = _sample_ticker()
    payload["acc_trade_volume_24h"] = "not-number"
    with pytest.raises(PublicApiSchemaError, match="field=acc_trade_volume_24h"):
        parse_ticker_lite_payload([payload])


def test_parse_ticker_payload_backward_compat_alias() -> None:
    tickers = parse_ticker_payload([_sample_ticker()])
    assert tickers[0].market == "KRW-BTC"


def test_fetch_ticker_raises_on_non_json_response() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>not-json</html>")

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        with pytest.raises(PublicApiResponseError, match="invalid json response"):
            fetch_ticker(client, markets=["KRW-BTC"])


def test_fetch_ticker_raises_on_http_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": {"name": "server_error", "message": "retry"}})

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        with pytest.raises(PublicApiResponseError, match="status=500"):
            fetch_ticker(client, markets="KRW-BTC")


def test_fetch_ticker_sends_markets_param() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/ticker"
        assert request.url.params.get("markets") == "KRW-BTC,KRW-ETH"
        assert "page" not in request.url.params
        assert "cursor" not in request.url.params
        assert request.headers.get("Authorization") is None
        assert request.headers.get("Api-Key") is None
        assert request.headers.get("X-Auth-Token") is None
        return httpx.Response(200, json=[_sample_ticker(), _sample_ticker() | {"market": "KRW-ETH"}])

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        tickers = fetch_ticker(client, markets=["krw-btc", "krw-eth"])
    assert len(tickers) == 2


def test_fetch_ticker_batch_returns_mapping_by_requested_order_when_response_order_changes() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_sample_ticker() | {"market": "KRW-ETH"}, _sample_ticker()])

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        snapshots_by_market = fetch_ticker_batch(client, markets=["KRW-BTC", "KRW-ETH"])

    assert list(snapshots_by_market.keys()) == ["KRW-BTC", "KRW-ETH"]
    assert snapshots_by_market["KRW-BTC"].market == "KRW-BTC"
    assert snapshots_by_market["KRW-ETH"].market == "KRW-ETH"


def test_fetch_ticker_single_enforces_single_market_contract() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get("markets") == "KRW-BTC"
        return httpx.Response(200, json=[_sample_ticker()])

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        snapshot = fetch_ticker_single(client, market="krw-btc")

    assert snapshot.market == "KRW-BTC"


def test_fetch_ticker_rejects_response_market_mismatch() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_sample_ticker() | {"market": "KRW-ETH"}])

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        with pytest.raises(PublicApiSchemaError, match="ticker response market mismatch"):
            fetch_ticker(client, markets=["KRW-BTC"])


def test_fetch_ticker_batch_rejects_response_market_mismatch_strict() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_sample_ticker() | {"market": "KRW-XRP"}])

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        with pytest.raises(PublicApiSchemaError, match="ticker response market mismatch"):
            fetch_ticker_batch(client, markets=["KRW-BTC"])


def test_ticker_contract_uses_markets_csv_string_input_without_private_auth() -> None:
    """문서 계약: /v1/ticker는 markets CSV를 사용하고 public 호출이어야 한다."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/ticker"
        assert request.url.params.get("markets") == "KRW-BTC,KRW-ETH"
        assert sorted(request.url.params.keys()) == ["markets"]
        assert request.headers.get("Authorization") is None
        assert request.headers.get("Api-Key") is None
        assert request.headers.get("X-Auth-Token") is None
        return httpx.Response(200, json=[_sample_ticker(), _sample_ticker() | {"market": "KRW-ETH"}])

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        snapshots = fetch_ticker(client, markets="KRW-BTC, KRW-ETH")

    assert [item.market for item in snapshots] == ["KRW-BTC", "KRW-ETH"]
