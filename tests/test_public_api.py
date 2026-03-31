from __future__ import annotations

import httpx
import pytest

from bithumb_bot.public_api import (
    PublicApiRequestError,
    PublicApiResponseError,
    PublicApiTransientError,
    decode_json_response,
    get_public_json,
    get_public_json_with_retry,
    mask_params,
    raise_for_http_error,
)


def _httpx_response(*, status_code: int = 200, payload: object | None = None) -> httpx.Response:
    request = httpx.Request("GET", "https://api.bithumb.com/v1/market/all")
    if payload is None:
        return httpx.Response(status_code, request=request)
    return httpx.Response(status_code, json=payload, request=request)


def test_mask_params_redacts_sensitive_tokens_but_keeps_market_fields() -> None:
    masked = mask_params(
        {
            "market": "KRW-BTC",
            "api_key": "secret-key",
            "auth_header": "Bearer token",
            "isDetails": "true",
        }
    )
    assert masked == {
        "market": "KRW-BTC",
        "api_key": "***",
        "auth_header": "***",
        "isDetails": "true",
    }


def test_raise_for_http_error_includes_api_error_details_when_present() -> None:
    response = _httpx_response(
        status_code=400,
        payload={"error": {"name": "invalid_parameter", "message": "isDetails must be true/false"}},
    )

    with pytest.raises(PublicApiResponseError, match="invalid_parameter"):
        raise_for_http_error(response=response, endpoint="/v1/market/all", params={"api_key": "k"})


def test_decode_json_response_raises_schema_error_on_invalid_json() -> None:
    request = httpx.Request("GET", "https://api.bithumb.com/v1/market/all")
    response = httpx.Response(status_code=200, content=b"<html>not-json</html>", request=request)

    with pytest.raises(PublicApiResponseError, match="invalid json response"):
        decode_json_response(response=response, endpoint="/v1/market/all")


def test_get_public_json_wraps_network_failures() -> None:
    transport = httpx.MockTransport(lambda _: (_ for _ in ()).throw(httpx.ConnectError("offline")))
    with httpx.Client(transport=transport, base_url="https://api.bithumb.com", timeout=1.0) as client:
        with pytest.raises(PublicApiRequestError, match="request failed"):
            get_public_json(client, "/v1/market/all")


def test_get_public_json_with_retry_succeeds_after_transient_status(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    responses = iter(
        [
            _httpx_response(status_code=503, payload={"error": {"name": "temporary", "message": "retry"}}),
            _httpx_response(status_code=200, payload=[{"ok": True}]),
        ]
    )

    def _handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    transport = httpx.MockTransport(_handler)
    with httpx.Client(transport=transport, base_url="https://api.bithumb.com", timeout=1.0) as client:
        payload = get_public_json_with_retry(
            client,
            "/v1/market/all",
            params={"market": "KRW-BTC"},
            max_retries=2,
            sleep_fn=lambda sec: sleeps.append(sec),
            random_fn=lambda _a, _b: 0.0,
        )

    assert payload == [{"ok": True}]
    assert len(sleeps) == 1


def test_get_public_json_with_retry_raises_transient_error_after_retry_budget() -> None:
    transport = httpx.MockTransport(lambda _request: _httpx_response(status_code=503, payload={"error": "busy"}))
    with httpx.Client(transport=transport, base_url="https://api.bithumb.com", timeout=1.0) as client:
        with pytest.raises(PublicApiTransientError, match="transient failure after retries"):
            get_public_json_with_retry(
                client,
                "/v1/market/all",
                params={"market": "KRW-BTC"},
                max_retries=1,
                sleep_fn=lambda _sec: None,
                random_fn=lambda _a, _b: 0.0,
            )
