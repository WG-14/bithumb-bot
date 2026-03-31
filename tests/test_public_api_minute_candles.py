from __future__ import annotations

import httpx
import pytest

from bithumb_bot.public_api import PublicApiResponseError, PublicApiSchemaError
from bithumb_bot.public_api_minute_candles import (
    fetch_minute_candles,
    interval_to_minute_unit,
    parse_minute_candles,
)


def _sample_candle() -> dict[str, object]:
    return {
        "market": "KRW-BTC",
        "candle_date_time_utc": "2026-03-31T00:00:00",
        "candle_date_time_kst": "2026-03-31T09:00:00",
        "opening_price": 100.0,
        "high_price": 120.0,
        "low_price": 90.0,
        "trade_price": 110.0,
        "timestamp": 1_743_379_200_000,
        "candle_acc_trade_price": 12345.67,
        "candle_acc_trade_volume": 0.1234,
    }


def test_parse_minute_candles_success_with_object_list() -> None:
    candles = parse_minute_candles([_sample_candle()])

    assert len(candles) == 1
    assert candles[0].market == "KRW-BTC"
    assert candles[0].timestamp == 1_743_379_200_000


def test_parse_minute_candles_accepts_empty_list() -> None:
    assert parse_minute_candles([]) == []


def test_parse_minute_candles_fails_when_required_field_missing() -> None:
    payload = _sample_candle()
    del payload["trade_price"]

    with pytest.raises(PublicApiSchemaError, match="missing_fields=trade_price"):
        parse_minute_candles([payload])


def test_parse_minute_candles_fails_when_field_type_is_invalid() -> None:
    payload = _sample_candle()
    payload["timestamp"] = "not-int"

    with pytest.raises(PublicApiSchemaError, match="field=timestamp"):
        parse_minute_candles([payload])


def test_fetch_minute_candles_raises_on_http_4xx() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": {"name": "bad_request", "message": "invalid"}})

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        with pytest.raises(PublicApiResponseError, match="http error"):
            fetch_minute_candles(client, market="KRW-BTC", minute_unit=1, count=5)


def test_fetch_minute_candles_raises_on_http_5xx() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": {"name": "server_error", "message": "retry"}})

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        with pytest.raises(PublicApiResponseError, match="status=500"):
            fetch_minute_candles(client, market="KRW-BTC", minute_unit=1, count=5)


def test_fetch_minute_candles_raises_on_non_json_response() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>not-json</html>")

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        with pytest.raises(PublicApiResponseError, match="invalid json response"):
            fetch_minute_candles(client, market="KRW-BTC", minute_unit=1, count=5)


def test_fetch_minute_candles_sends_unit_in_path_and_query_params() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/candles/minutes/3"
        assert request.url.params.get("market") == "KRW-BTC"
        assert request.url.params.get("count") == "7"
        assert request.url.params.get("to") == "2026-03-31T09:00:00"
        return httpx.Response(200, json=[_sample_candle()])

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.bithumb.com") as client:
        candles = fetch_minute_candles(
            client,
            market="KRW-BTC",
            minute_unit=3,
            count=7,
            to="2026-03-31T09:00:00",
        )

    assert len(candles) == 1


@pytest.mark.parametrize(
    ("interval", "expected_unit"),
    [
        ("1m", 1),
        ("3m", 3),
        ("5m", 5),
        ("10m", 10),
        ("15m", 15),
        ("30m", 30),
        ("60m", 60),
        ("240m", 240),
    ],
)
def test_interval_to_minute_unit_supported_values(interval: str, expected_unit: int) -> None:
    assert interval_to_minute_unit(interval) == expected_unit


@pytest.mark.parametrize("interval", ["", "1", "2m", "1h", "day", "-1m"])
def test_interval_to_minute_unit_fails_for_unsupported_values(interval: str) -> None:
    with pytest.raises(ValueError, match="unsupported minute interval"):
        interval_to_minute_unit(interval)
