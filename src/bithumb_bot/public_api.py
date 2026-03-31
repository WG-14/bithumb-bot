from __future__ import annotations

import json
from typing import Any

import httpx


_SENSITIVE_PARAM_TOKENS = ("secret", "token", "key", "auth", "sign", "jwt", "password")


class PublicApiError(RuntimeError):
    """Base error for Bithumb public API handling."""


class PublicApiRequestError(PublicApiError):
    """Raised when a public API call fails before an HTTP response is received."""


class PublicApiResponseError(PublicApiError):
    """Raised when a public API returns an HTTP/API-level failure response."""


class PublicApiSchemaError(PublicApiError):
    """Raised when a public API response shape is incompatible with expected schema."""


def mask_params(params: dict[str, Any] | None) -> dict[str, Any] | None:
    if params is None:
        return None
    masked: dict[str, Any] = {}
    for key, value in params.items():
        lowered = str(key).lower()
        if any(token in lowered for token in _SENSITIVE_PARAM_TOKENS):
            masked[key] = "***"
        else:
            masked[key] = value
    return masked


def extract_api_error(payload: object) -> tuple[str | None, str | None] | None:
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if isinstance(error, dict):
        name = error.get("name")
        message = error.get("message")
        return (
            str(name).strip() if isinstance(name, str) and name.strip() else None,
            str(message).strip() if isinstance(message, str) and message.strip() else None,
        )
    if isinstance(error, str) and error.strip():
        return None, error.strip()
    return None


def decode_json_response(*, response: httpx.Response, endpoint: str, params: dict[str, Any] | None = None) -> object:
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise PublicApiResponseError(
            "public api invalid json response "
            f"endpoint={endpoint} params={mask_params(params)} status={response.status_code} "
            f"error={type(exc).__name__}: {exc}"
        ) from exc


def raise_for_http_error(*, response: httpx.Response, endpoint: str, params: dict[str, Any] | None = None) -> None:
    if 200 <= response.status_code < 300:
        return

    api_error_name: str | None = None
    api_error_message: str | None = None
    try:
        payload = decode_json_response(response=response, endpoint=endpoint, params=params)
        api_error = extract_api_error(payload)
        if api_error is not None:
            api_error_name, api_error_message = api_error
    except PublicApiResponseError:
        api_error_message = None

    raise PublicApiResponseError(
        "public api http error "
        f"endpoint={endpoint} params={mask_params(params)} status={response.status_code} "
        f"api_error_name={api_error_name or '-'} api_error_message={api_error_message or '-'}"
    )


def get_public_json(
    client: httpx.Client,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
) -> object:
    try:
        response = client.get(endpoint, params=params)
    except httpx.RequestError as exc:
        raise PublicApiRequestError(
            "public api request failed "
            f"endpoint={endpoint} params={mask_params(params)} error={type(exc).__name__}: {exc}"
        ) from exc

    raise_for_http_error(response=response, endpoint=endpoint, params=params)
    return decode_json_response(response=response, endpoint=endpoint, params=params)
