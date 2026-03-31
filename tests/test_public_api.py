from __future__ import annotations

import httpx
import pytest

from bithumb_bot.public_api import (
    PublicApiRequestError,
    PublicApiResponseError,
    decode_json_response,
    get_public_json,
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
