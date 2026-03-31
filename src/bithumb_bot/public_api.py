from __future__ import annotations

import json
import random
import time
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


class PublicApiTransientError(PublicApiError):
    """Raised when retryable public API failures exceed retry budget."""


RETRYABLE_HTTP_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


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


def _sleep_backoff(
    *,
    attempt: int,
    base_backoff_sec: float,
    max_backoff_sec: float,
    jitter_sec: float,
    sleep_fn: Any,
    random_fn: Any,
) -> None:
    delay = min(max_backoff_sec, base_backoff_sec * (2 ** attempt))
    delay += max(0.0, float(random_fn(0.0, max(0.0, float(jitter_sec)))))
    sleep_fn(max(0.0, delay))


def get_public_json_with_retry(
    client: httpx.Client,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    max_retries: int = 3,
    base_backoff_sec: float = 0.2,
    max_backoff_sec: float = 2.0,
    jitter_sec: float = 0.1,
    sleep_fn: Any = time.sleep,
    random_fn: Any = random.uniform,
) -> object:
    retries = max(0, int(max_retries))
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = client.get(endpoint, params=params)
        except httpx.RequestError as exc:
            last_error = exc
            if attempt >= retries:
                break
            _sleep_backoff(
                attempt=attempt,
                base_backoff_sec=base_backoff_sec,
                max_backoff_sec=max_backoff_sec,
                jitter_sec=jitter_sec,
                sleep_fn=sleep_fn,
                random_fn=random_fn,
            )
            continue

        if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
            if attempt >= retries:
                last_error = PublicApiResponseError(
                    "public api retryable http status exhausted "
                    f"endpoint={endpoint} params={mask_params(params)} "
                    f"status={response.status_code} attempts={retries + 1}"
                )
                break
            _sleep_backoff(
                attempt=attempt,
                base_backoff_sec=base_backoff_sec,
                max_backoff_sec=max_backoff_sec,
                jitter_sec=jitter_sec,
                sleep_fn=sleep_fn,
                random_fn=random_fn,
            )
            continue

        raise_for_http_error(response=response, endpoint=endpoint, params=params)
        return decode_json_response(response=response, endpoint=endpoint, params=params)

    raise PublicApiTransientError(
        "public api transient failure after retries "
        f"endpoint={endpoint} params={mask_params(params)} attempts={retries + 1} "
        f"last_error={type(last_error).__name__ if last_error else '-'}: {last_error}"
    ) from last_error
