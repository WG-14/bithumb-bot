from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import logging
import os
import re
import time
import uuid
import threading
from decimal import Decimal, ROUND_DOWN
import math
from urllib.parse import urlencode
from dataclasses import dataclass
from typing import TypedDict

import httpx

from ..config import settings
from ..marketdata import fetch_orderbook_top, validated_best_quote_ask_price
from ..markets import ExchangeMarketCodeError, canonical_market_id, parse_documented_market_code
from ..observability import format_log_kv
from .base import (
    BrokerBalance,
    BrokerFill,
    BrokerIdentifierMismatchError,
    BrokerOrder,
    BrokerRejectError,
    BrokerSchemaError,
    BrokerTemporaryError,
)
from .balance_source import AccountsV1BalanceSource, BalanceSnapshot, BalanceSource, DryRunBalanceSource
from .accounts_v1 import (
    parse_accounts_response,
    select_pair_balances,
    to_broker_balance,
)
from .myasset_ws import MyAssetWsBalanceSource
from .myorder_events import NormalizedMyOrderEvent, normalize_myorder_event_payload
from .myorder_runtime import MyOrderIngestResult, ingest_myorder_event
from ..execution_models import OrderIntent
from .order_lookup_v1 import (
    V1NormalizedOrder,
    build_lookup_params as build_v1_order_lookup_params,
    build_cancel_order_params,
    ensure_identifier_consistency as ensure_v1_identifier_consistency,
    require_order_payload_dict as require_v1_order_payload_dict,
    require_known_state as require_v1_known_state,
    resolve_requested_identifiers as resolve_v1_requested_identifiers,
    resolve_identifiers as resolve_v1_order_identifiers,
    status_from_state as v1_status_from_state,
)
from .bithumb_adapter import build_signed_order_request
from .bithumb_order_queries import (
    get_fills as load_bithumb_fills,
    get_open_orders as load_bithumb_open_orders,
    get_order as load_bithumb_order,
    get_recent_orders as load_bithumb_recent_orders,
    get_recent_orders_for_recovery as load_bithumb_recent_orders_for_recovery,
)
from .order_list_v1 import build_order_list_params, parse_v1_order_list_row
from .order_list_v1 import build_recovery_order_list_params
from .order_list_v1 import V1ListNormalizedOrder
from .order_read_models import (
    normalize_order_side as normalize_order_row_side,
    normalize_v1_order_row_lenient_for_fills,
    normalize_v1_order_row_strict,
    normalize_v2_order_row,
    parse_ts,
    strict_parse_ts,
)
from .order_payloads import (
    normalize_order_side,
    validate_client_order_id,
    validate_order_submit_payload,
)
from .order_serialization import decimal_from_value, format_krw_amount, format_volume, truncate_volume
from .order_submit import (
    PlaceOrderSubmissionFlow,
    build_place_order_submission_flow,
    plan_place_order,
    resolve_submit_price_tick_policy as _resolve_submit_price_tick_policy,
    run_place_order_submission_flow,
)

_jwt = importlib.import_module("jwt") if importlib.util.find_spec("jwt") else importlib.import_module("bithumb_bot.broker.jwt_compat")


_HTTPX_TRANSIENT_ERRORS = tuple(
    cls
    for cls in (
        getattr(httpx, "TimeoutException", None),
        getattr(httpx, "TransportError", None),
        getattr(httpx, "RequestError", None),
    )
    if isinstance(cls, type)
)
RUN_LOG = logging.getLogger("bithumb_bot.run")
CANCEL_REQUESTED_STATUS = "CANCEL_REQUESTED"
_PRIVATE_REQUEST_RATE_LIMIT_BUCKET = "private"
_ORDER_REQUEST_RATE_LIMIT_BUCKET = "order"
_ORDER_RATE_LIMIT_ENDPOINTS = {
    "/v1/order",
    "/v1/orders",
    "/v1/orders/chance",
    "/v2/order",
    "/v2/orders",
}
_ERROR_NAME_RE = re.compile(r"\berror_name=([A-Za-z0-9_]+)")
_AUTH_ERROR_NAMES = frozenset({"invalid_query_payload", "jwt_verification", "expired_jwt", "invalid_access_key", "notallowip", "out_of_scope"})
_RATE_LIMIT_ERROR_NAMES = frozenset({"too_many_requests"})
_SERVER_ERROR_ERROR_NAMES = frozenset({"server_error", "internal_server_error", "service_unavailable", "gateway_timeout"})
_PARAM_ERROR_NAMES = frozenset({"invalid_parameter", "invalid_price", "invalid_price_ask", "invalid_price_bid", "under_price_limit_ask", "under_price_limit_bid"})
_ORDER_RULE_ERROR_NAMES = frozenset({"cross_trading", "under_min_total", "too_many_orders"})
_NOT_FOUND_ERROR_NAMES = frozenset({"order_not_found", "deposit_not_found", "withdraw_not_found"})
_ORDER_NOT_READY_ERROR_NAMES = frozenset({"order_not_ready"})
_DOCUMENTED_PRIVATE_ERROR_MESSAGES: dict[str, tuple[str, str, str, bool, bool, bool]] = {
    "currency does not have a valid value": (
        "INVALID_PARAMETER",
        "unsupported currency or market code supplied",
        "INVALID_REQUEST",
        False,
        True,
        False,
    ),
}


class BithumbAuthError(BrokerRejectError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


class BithumbRateLimitError(BrokerTemporaryError):
    pass


class BithumbOrderNotReadyError(BrokerTemporaryError):
    pass


@dataclass(frozen=True)
class PrivateApiFailureClassification:
    category: str
    summary: str
    should_retry: bool = False
    disable_trading: bool = False
    needs_reconcile: bool = False


@dataclass
class _BucketThrottleState:
    next_allowed_at: float = 0.0
    penalty_until: float = 0.0


class _RequestThrottleCoordinator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state_by_bucket: dict[str, _BucketThrottleState] = {}

    def acquire(self, *, bucket: str, limit_per_sec: float) -> float:
        limit = float(limit_per_sec)
        if not math.isfinite(limit) or limit <= 0:
            return 0.0

        interval = 1.0 / limit
        now = time.monotonic()
        with self._lock:
            state = self._state_by_bucket.setdefault(bucket, _BucketThrottleState())
            gate_until = max(state.next_allowed_at, state.penalty_until)
            wait = max(0.0, gate_until - now)
            state.next_allowed_at = max(now, gate_until) + interval
        if wait > 0:
            time.sleep(wait)
        return wait

    def penalize(self, *, bucket: str, delay_sec: float) -> None:
        normalized_delay = max(0.0, float(delay_sec))
        if normalized_delay <= 0.0:
            return
        now = time.monotonic()
        with self._lock:
            state = self._state_by_bucket.setdefault(bucket, _BucketThrottleState())
            state.penalty_until = max(state.penalty_until, now + normalized_delay)


_REQUEST_THROTTLER = _RequestThrottleCoordinator()
_OFFICIAL_PRIVATE_RPS_LIMIT = 140.0
_OFFICIAL_ORDER_RPS_LIMIT = 10.0

_DOCUMENTED_PRIVATE_ERROR_CODES: dict[str, tuple[str, str, str, bool, bool, bool]] = {
    "invalid_query_payload": ("AUTH_QUERY_HASH_MISMATCH", "JWT query_hash mismatch; GET query string/body hash must match transmitted params", "INVALID_REQUEST", False, True, False),
    "jwt_verification": ("AUTH_JWT_VERIFICATION", "JWT verification failed", "AUTHENTICATION", False, True, False),
    "expired_jwt": ("AUTH_JWT_EXPIRED", "JWT expired before broker accepted the request", "AUTHENTICATION", False, True, False),
    "invalid_access_key": ("AUTH_INVALID_ACCESS_KEY", "API access key rejected by broker", "AUTHENTICATION", False, True, False),
    "notallowip": ("AUTH_IP_DENIED", "client IP is not allowed for this API key", "PERMISSION_SCOPE", False, True, False),
    "out_of_scope": ("PERMISSION", "API key scope/permission denied", "PERMISSION_SCOPE", False, True, False),
    "invalid_parameter": ("INVALID_PARAMETER", "invalid parameter provided to broker request", "INVALID_REQUEST", False, True, False),
    "invalid_price": ("INVALID_PRICE", "order price is invalid for the requested side or tick", "INVALID_REQUEST", False, True, False),
    "invalid_price_ask": ("INVALID_PRICE", "order price is invalid for the requested side or tick", "INVALID_REQUEST", False, True, False),
    "invalid_price_bid": ("INVALID_PRICE", "order price is invalid for the requested side or tick", "INVALID_REQUEST", False, True, False),
    "under_price_limit_ask": ("UNDER_PRICE_LIMIT", "ask price is below the documented side minimum", "PRETRADE_GUARD", False, True, False),
    "under_price_limit_bid": ("UNDER_PRICE_LIMIT", "bid price is below the documented side minimum", "PRETRADE_GUARD", False, True, False),
    "under_min_total": ("UNDER_MIN_TOTAL", "order notional is below the documented minimum", "PRETRADE_GUARD", False, False, False),
    "too_many_orders": ("TOO_MANY_ORDERS", "order limit has been reached", "PRETRADE_GUARD", False, False, False),
    "too_many_requests": ("RATE_LIMITED", "private API rate limit or overload encountered", "THROTTLED_BACKOFF", True, False, False),
    "bank_account_required": ("ACCOUNT_SETUP_REQUIRED", "real-name deposit/withdrawal account registration is required", "PREFLIGHT_BLOCKED", False, True, False),
    "two_factor_auth_required": ("AUTH_CHANNEL_REQUIRED", "valid authentication channel is required", "AUTHENTICATION", False, True, False),
    "blocked_member_id": ("ACCOUNT_RESTRICTED", "service usage restricted by operational policy", "PERMISSION_SCOPE", False, True, False),
    "withdraw_insufficient_balance": ("WITHDRAW_LIMIT_BLOCKED", "withdrawal limit exceeded", "PREFLIGHT_BLOCKED", False, True, False),
    "server_error": ("SERVER_INTERNAL_FAILURE", "server/internal failure reported by broker", "SERVER_INTERNAL_FAILURE", True, False, False),
    "order_not_found": ("ORDER_NOT_FOUND", "order lookup/cancel target not found", "NOT_FOUND_NEEDS_RECONCILE", False, False, True),
    "deposit_not_found": ("LOOKUP_NOT_FOUND", "deposit lookup target not found", "NOT_FOUND_NEEDS_RECONCILE", False, False, True),
    "withdraw_not_found": ("LOOKUP_NOT_FOUND", "withdraw lookup target not found", "NOT_FOUND_NEEDS_RECONCILE", False, False, True),
    "order_not_ready": ("ORDER_NOT_READY", "order not ready yet; refresh and retry later", "ORDER_NOT_READY", True, False, False),
    "cross_trading": ("CROSS_TRADING", "exchange rejected self-crossing order", "EXCHANGE_RULE_VIOLATION", False, False, False),
}

_FALLBACK_PRIVATE_ERROR_CODES: dict[str, tuple[str, str, str, bool, bool, bool]] = {
    "too_many_requests": ("RATE_LIMITED", "private API rate limit or overload encountered", "THROTTLED_BACKOFF", True, False, False),
    "duplicate_client_order_id": ("DUPLICATE_CLIENT_ORDER_ID", "duplicate client order id or identifier conflict", "DUPLICATE_CLIENT_ORDER_ID", False, False, False),
    "id_conflict": ("DUPLICATE_CLIENT_ORDER_ID", "duplicate client order id or identifier conflict", "DUPLICATE_CLIENT_ORDER_ID", False, False, False),
    "cancel_not_allowed": ("CANCEL_NOT_ALLOWED", "cancel not allowed in current state", "CANCEL_NOT_ALLOWED", False, False, False),
}


def _request_bucket_for_endpoint(*, method: str, endpoint: str) -> str:
    normalized_endpoint = str(endpoint or "").split("?", 1)[0]
    if normalized_endpoint in _ORDER_RATE_LIMIT_ENDPOINTS:
        return _ORDER_REQUEST_RATE_LIMIT_BUCKET
    normalized_method = str(method or "").strip().upper()
    if normalized_method in {"POST", "DELETE"} and normalized_endpoint.startswith("/v2/"):
        return _ORDER_REQUEST_RATE_LIMIT_BUCKET
    return _PRIVATE_REQUEST_RATE_LIMIT_BUCKET


def _private_error_name(detail: str) -> str:
    match = _ERROR_NAME_RE.search(str(detail or ""))
    return str(match.group(1) if match else "").strip().lower()


def _is_rate_limit_indicator(*, status_code: int, error_name: str, error_message: str, body: str) -> bool:
    detail = " ".join(token.lower() for token in (error_name, error_message, body) if token)
    if status_code == 429:
        return True
    if str(error_name or "").strip().lower() in _RATE_LIMIT_ERROR_NAMES:
        return True
    if any(token in detail for token in ("too many requests", "rate limit", "throttl", "throttle", "overload", "overloaded")):
        return True
    return False


def _is_order_not_ready_indicator(*, status_code: int, error_name: str, error_message: str, body: str) -> bool:
    detail = " ".join(token.lower() for token in (error_name, error_message, body) if token)
    if status_code != 422:
        return False
    return str(error_name or "").strip().lower() in _ORDER_NOT_READY_ERROR_NAMES or "order_not_ready" in detail or "not ready" in detail

def _is_server_error_indicator(*, status_code: int, error_name: str, error_message: str, body: str) -> bool:
    detail = " ".join(token.lower() for token in (error_name, error_message, body) if token)
    if 500 <= status_code <= 599:
        return True
    if str(error_name or "").strip().lower() in _SERVER_ERROR_ERROR_NAMES:
        return True
    return any(token in detail for token in ("server error", "internal server error", "service unavailable", "gateway timeout"))


def _documented_private_error_descriptor(error_name: str) -> tuple[str, str, str, bool, bool, bool] | None:
    return _DOCUMENTED_PRIVATE_ERROR_CODES.get(str(error_name or "").strip().lower())


def _documented_private_error_descriptor_from_detail(
    *,
    error_name: str,
    detail: str,
) -> tuple[str, str, str, bool, bool, bool] | None:
    descriptor = _documented_private_error_descriptor(error_name)
    if descriptor is not None:
        return descriptor
    normalized_detail = str(detail or "").strip().lower()
    for message, candidate in _DOCUMENTED_PRIVATE_ERROR_MESSAGES.items():
        if message in normalized_detail:
            return candidate
    return None


def _fallback_private_error_descriptor(error_name: str) -> tuple[str, str, str, bool, bool, bool] | None:
    return _FALLBACK_PRIVATE_ERROR_CODES.get(str(error_name or "").strip().lower())


def _retry_backoff_delay(*, attempt: int, bucket: str, reason: str) -> float:
    normalized_attempt = max(0, int(attempt))
    if reason == "rate_limit":
        base = 0.35 if bucket == _ORDER_REQUEST_RATE_LIMIT_BUCKET else 0.2
        return min(base * (2 ** normalized_attempt), 4.0)
    base = 0.2 if bucket == _ORDER_REQUEST_RATE_LIMIT_BUCKET else 0.15
    return min(base * (2 ** normalized_attempt), 1.0)


def classify_private_api_failure(exc: Exception) -> PrivateApiFailureClassification:
    detail = str(exc).lower()
    error_name = _private_error_name(detail)
    documented = _documented_private_error_descriptor_from_detail(error_name=error_name, detail=detail)
    fallback = _fallback_private_error_descriptor(error_name)
    if isinstance(exc, BithumbAuthError):
        return PrivateApiFailureClassification(
            category="AUTH_OR_CONFIG_ERROR",
            summary=exc.reason_code,
            disable_trading=True,
        )
    if documented is not None:
        _code, summary, category, should_retry, disable_trading, needs_reconcile = documented
        return PrivateApiFailureClassification(
            category=category,
            summary=summary,
            should_retry=should_retry,
            disable_trading=disable_trading,
            needs_reconcile=needs_reconcile,
        )
    if fallback is not None:
        _code, summary, category, should_retry, disable_trading, needs_reconcile = fallback
        return PrivateApiFailureClassification(
            category=category,
            summary=summary,
            should_retry=should_retry,
            disable_trading=disable_trading,
            needs_reconcile=needs_reconcile,
        )
    if isinstance(exc, BithumbRateLimitError) or error_name in _RATE_LIMIT_ERROR_NAMES or any(
        token in detail for token in ("rate limit", "too many requests", "throttl", "throttle", "overload", "overloaded")
    ):
        return PrivateApiFailureClassification(
            category="THROTTLED_BACKOFF",
            summary="private API rate limit or overload encountered",
            should_retry=True,
        )
    if isinstance(exc, BithumbOrderNotReadyError) or error_name in _ORDER_NOT_READY_ERROR_NAMES or "order_not_ready" in detail:
        return PrivateApiFailureClassification(
            category="ORDER_NOT_READY",
            summary="order is not ready for the requested transition yet",
            should_retry=True,
        )
    if error_name in _ORDER_RULE_ERROR_NAMES or "cross_trading" in detail:
        return PrivateApiFailureClassification(
            category="EXCHANGE_RULE_VIOLATION",
            summary="exchange rejected order by documented rule",
        )
    if isinstance(exc, BrokerTemporaryError) and any(token in detail for token in ("server error", "internal server error", "service unavailable", "gateway timeout", "status=5")):
        return PrivateApiFailureClassification(
            category="SERVER_INTERNAL_FAILURE",
            summary="server/internal failure reported by broker",
            should_retry=True,
        )
    if isinstance(exc, BrokerTemporaryError) or any(token in detail for token in ("transport error", "timeout", "timed out", "temporar")):
        return PrivateApiFailureClassification(
            category="RETRYABLE_TRANSIENT",
            summary="temporary network/transport error",
            should_retry=True,
        )
    if isinstance(exc, BrokerIdentifierMismatchError) or "identifier mismatch" in detail:
        return PrivateApiFailureClassification(
            category="RECOVERY_REQUIRED",
            summary="request/response identifiers conflict",
            disable_trading=True,
            needs_reconcile=True,
        )
    if isinstance(exc, BrokerSchemaError) or "schema mismatch" in detail:
        return PrivateApiFailureClassification(
            category="RECOVERY_REQUIRED",
            summary="documented response schema mismatch",
            disable_trading=True,
            needs_reconcile=True,
        )
    if any(token in detail for token in ("too many requests", "rate limit", "throttl", "throttle", "overload", "overloaded")):
        return PrivateApiFailureClassification(
            category="THROTTLED_BACKOFF",
            summary="private API rate limit or overload encountered",
            should_retry=True,
        )
    if error_name == "invalid_query_payload" or any(token in detail for token in ("invalid_query_payload", "query hash mismatch", "query_hash mismatch")):
        return PrivateApiFailureClassification(
            category="INVALID_REQUEST",
            summary="JWT query_hash mismatch or invalid private request payload",
            disable_trading=True,
        )
    if error_name in {"jwt_verification", "expired_jwt", "invalid_access_key"} or any(token in detail for token in ("invalid_access_key", "jwt_verification", "expired_jwt", "invalid jwt", "signature")):
        return PrivateApiFailureClassification(
            category="AUTHENTICATION",
            summary="authentication/token signing failure",
            disable_trading=True,
        )
    if error_name in {"notallowip", "out_of_scope"} or "status=403" in detail or "permission" in detail:
        return PrivateApiFailureClassification(
            category="PERMISSION_SCOPE",
            summary="API key scope, permission, or IP denied",
            disable_trading=True,
        )
    if error_name in {"bank_account_required", "withdraw_insufficient_balance"} or any(token in detail for token in ("bank_account_required", "withdraw_insufficient_balance", "deposit/withdrawal account", "withdrawal limit")):
        return PrivateApiFailureClassification(
            category="PREFLIGHT_BLOCKED",
            summary="account setup or withdrawal limits blocked the request",
            disable_trading=True,
        )
    if error_name == "two_factor_auth_required" or "two_factor_auth_required" in detail:
        return PrivateApiFailureClassification(
            category="AUTHENTICATION",
            summary="valid authentication channel required",
            disable_trading=True,
        )
    if error_name == "blocked_member_id" or "blocked_member_id" in detail:
        return PrivateApiFailureClassification(
            category="PERMISSION_SCOPE",
            summary="service usage restricted by operational policy",
            disable_trading=True,
        )
    if any(token in detail for token in ("unexpected broker response", "unexpected /v1/orders/chance payload type", "unexpected /v1/accounts payload type")):
        return PrivateApiFailureClassification(
            category="RECOVERY_REQUIRED",
            summary="private broker returned an unexpected response shape",
            disable_trading=True,
            needs_reconcile=True,
        )
    if "order_not_ready" in detail:
        return PrivateApiFailureClassification(
            category="ORDER_NOT_READY",
            summary="order is not ready for the requested transition yet",
            should_retry=True,
        )
    if any(
        token in detail
        for token in (
            "broad /v1/orders",
            "requires identifiers",
            "fallback is disabled",
            "identifier-scoped by bot policy",
            "recovery-only market/state scans",
        )
    ):
        return PrivateApiFailureClassification(
            category="RECOVERY_REQUIRED",
            summary="startup/recovery path requires identifier-based lookup only",
            disable_trading=True,
            needs_reconcile=True,
        )
    if error_name in {"under_min_total", "too_many_orders"}:
        return PrivateApiFailureClassification(
            category="PRETRADE_GUARD",
            summary="order notional or order-count guard failed",
        )
    if error_name in _PARAM_ERROR_NAMES or any(token in detail for token in ("invalid_parameter", "validation", "currency does not have a valid value")):
        return PrivateApiFailureClassification(
            category="INVALID_REQUEST",
            summary="market/order parameter validation failed",
        )
    if error_name in {"invalid_price", "invalid_price_ask", "invalid_price_bid", "under_price_limit_ask", "under_price_limit_bid"} or any(token in detail for token in ("invalid_price", "price out of range", "price unit", "under_price_limit")):
        return PrivateApiFailureClassification(
            category="INVALID_REQUEST",
            summary="price rule validation failed",
        )
    if error_name in {"deposit_not_found", "withdraw_not_found"}:
        return PrivateApiFailureClassification(
            category="NOT_FOUND_NEEDS_RECONCILE",
            summary="lookup target not found",
            needs_reconcile=True,
        )
    if error_name == "order_not_found" or any(token in detail for token in ("not found", "order_not_found", "no such order", "unknown order")):
        return PrivateApiFailureClassification(
            category="NOT_FOUND_NEEDS_RECONCILE",
            summary="order lookup or cancel returned not found",
            needs_reconcile=True,
        )
    if error_name in {"duplicate_client_order_id", "id_conflict"} or any(
        token in detail for token in ("duplicate_client_order_id", "id_conflict", "duplicate client order")
    ):
        return PrivateApiFailureClassification(
            category="DUPLICATE_CLIENT_ORDER_ID",
            summary="duplicate client order id or identifier conflict",
        )
    if any(token in detail for token in ("cancel_not_allowed", "already finalized", "terminal state")):
        return PrivateApiFailureClassification(
            category="CANCEL_NOT_ALLOWED",
            summary="cancel not allowed in current state",
        )
    if "422" in detail or "order state" in detail or "transition" in detail:
        return PrivateApiFailureClassification(
            category="ORDER_STATE_RACE",
            summary="order transition or terminal-state race",
            needs_reconcile=True,
        )
    return PrivateApiFailureClassification(
        category="RECOVERY_REQUIRED",
        summary="unclassified private API failure; operator investigation required",
        disable_trading=True,
        needs_reconcile=True,
    )


class _OrderSubmitAuthContext(TypedDict):
    canonical_payload: str
    request_content: bytes
    request_body_text: str
    query_hash_claims: dict[str, str]
    claims: dict[str, object]
    headers: dict[str, str]
    request_kwargs: dict[str, object]


class BithumbPrivateAPI:
    ORDER_SUBMIT_ENDPOINT = "/v2/orders"
    ORDER_SUBMIT_CONTENT_TYPE = "application/json; charset=utf-8"

    def __init__(self, *, api_key: str, api_secret: str, base_url: str, dry_run: bool) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.dry_run = dry_run

    @staticmethod
    def _is_read_only_private_request(method: str) -> bool:
        return str(method or "").strip().upper() == "GET"

    @staticmethod
    def _payload_items(payload: dict[str, object] | None) -> list[tuple[str, str]]:
        if not payload:
            return []
        items: list[tuple[str, str]] = []
        for key, value in payload.items():
            if value is None:
                continue
            key_text = str(key)
            if isinstance(value, (list, tuple)):
                array_key = key_text if key_text.endswith("[]") else f"{key_text}[]"
                for item in value:
                    items.append((array_key, str(item)))
                continue
            items.append((key_text, str(value)))
        return items

    @classmethod
    def _canonical_payload_for_query_hash(cls, payload: dict[str, object] | None) -> str:
        return urlencode(cls._payload_items(payload), doseq=False, safe="[]")

    @classmethod
    def _query_string(cls, payload: dict[str, object] | None) -> str:
        return cls._canonical_payload_for_query_hash(payload)

    @staticmethod
    def _query_hash_from_canonical_payload(query_string: str) -> dict[str, str]:
        if not query_string:
            return {}
        return {
            "query_hash": hashlib.sha512(query_string.encode("utf-8")).hexdigest(),
            "query_hash_alg": "SHA512",
        }

    @classmethod
    def _query_hash_claims(cls, payload: dict[str, object] | None) -> dict[str, str]:
        return cls._query_hash_from_canonical_payload(cls._query_string(payload))

    @staticmethod
    def _request_bucket_for(method: str, endpoint: str) -> str:
        return _request_bucket_for_endpoint(method=method, endpoint=endpoint)

    @staticmethod
    def _request_bucket_limit(bucket: str) -> float:
        if bucket == _ORDER_REQUEST_RATE_LIMIT_BUCKET:
            return float(getattr(settings, "BITHUMB_ORDER_RPS_LIMIT", _OFFICIAL_ORDER_RPS_LIMIT))
        # Fallbacks mirror the exchange-documented private/order limits rather
        # than conservative internal throttles.
        return float(getattr(settings, "BITHUMB_PRIVATE_RPS_LIMIT", _OFFICIAL_PRIVATE_RPS_LIMIT))

    def _acquire_request_slot(self, *, method: str, endpoint: str) -> tuple[str, float, float]:
        bucket = self._request_bucket_for(method, endpoint)
        limit_per_sec = self._request_bucket_limit(bucket)
        waited = _REQUEST_THROTTLER.acquire(bucket=bucket, limit_per_sec=limit_per_sec)
        return bucket, limit_per_sec, waited

    def _jwt_token_from_claims(self, claims: dict[str, object]) -> str:
        token = _jwt.encode(claims, self.api_secret, algorithm="HS256")
        return token if isinstance(token, str) else token.decode()

    def _base_jwt_claims(
        self,
        *,
        nonce: str | None = None,
        timestamp: int | None = None,
    ) -> dict[str, object]:
        if not str(self.api_key or "").strip():
            raise BithumbAuthError(
                "AUTH_KEY_MISSING",
                "private request rejected before signing: missing API key",
            )
        if not str(self.api_secret or "").strip():
            raise BithumbAuthError(
                "AUTH_SECRET_MISSING",
                "private request rejected before signing: missing API secret",
            )
        resolved_nonce = nonce or str(uuid.uuid4())
        resolved_timestamp = round(time.time() * 1000) if timestamp is None else int(timestamp)
        return {
            "access_key": self.api_key,
            "nonce": resolved_nonce,
            "timestamp": resolved_timestamp,
        }

    def _jwt_token(
        self,
        payload: dict[str, object] | None,
        *,
        canonical_payload: str | None = None,
        nonce: str | None = None,
        timestamp: int | None = None,
    ) -> str:
        query_hash_claims = self._query_hash_from_canonical_payload(canonical_payload) if canonical_payload is not None else self._query_hash_claims(payload)
        claims = {
            **self._base_jwt_claims(nonce=nonce, timestamp=timestamp),
            **query_hash_claims,
        }
        return self._jwt_token_from_claims(claims)

    def _headers(
        self,
        payload: dict[str, object] | None,
        *,
        content_type: str | None = None,
        canonical_payload: str | None = None,
        nonce: str | None = None,
        timestamp: int | None = None,
    ) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._jwt_token(payload, canonical_payload=canonical_payload, nonce=nonce, timestamp=timestamp)}",
            "Accept": "application/json",
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def _order_submit_auth_context(
        self,
        payload: dict[str, object],
        *,
        nonce: str | None = None,
        timestamp: int | None = None,
    ) -> _OrderSubmitAuthContext:
        canonical_payload = self._query_string(payload)
        request_body_text = self._json_body_text(payload)
        request_content = request_body_text.encode("utf-8")
        query_hash_claims = self._query_hash_from_canonical_payload(canonical_payload)
        claims = {
            **self._base_jwt_claims(nonce=nonce, timestamp=timestamp),
            **query_hash_claims,
        }
        headers = {
            "Authorization": f"Bearer {self._jwt_token_from_claims(claims)}",
            "Accept": "application/json",
            "Content-Type": self.ORDER_SUBMIT_CONTENT_TYPE,
        }
        request_kwargs: dict[str, object] = {"content": request_content}
        return {
            "canonical_payload": canonical_payload,
            "request_content": request_content,
            "request_body_text": request_body_text,
            "query_hash_claims": query_hash_claims,
            "claims": claims,
            "headers": headers,
            "request_kwargs": request_kwargs,
        }

    def _order_submit_request_parts(
        self,
        payload: dict[str, object],
        *,
        nonce: str | None = None,
        timestamp: int | None = None,
    ) -> tuple[dict[str, str], dict[str, object], str]:
        context = self._order_submit_auth_context(payload, nonce=nonce, timestamp=timestamp)
        return context["headers"], context["request_kwargs"], context["canonical_payload"]

    @staticmethod
    def _json_body_text(payload: dict[str, object]) -> str:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def _form_body_bytes(cls, payload: dict[str, object]) -> bytes:
        return cls._query_string(payload).encode("utf-8")

    def describe_request_auth(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, object] | None = None,
        json_body: dict[str, object] | None = None,
    ) -> dict[str, object]:
        normalized_method = str(method or "").strip().upper()
        is_read_only = self._is_read_only_private_request(normalized_method)
        auth_payload = params if normalized_method in {"GET", "DELETE"} else json_body
        canonical_payload = self._query_string(auth_payload) if auth_payload else ""
        query_hash_claims = self._query_hash_from_canonical_payload(canonical_payload)
        request_bucket = self._request_bucket_for(normalized_method, endpoint)
        request_bucket_limit = self._request_bucket_limit(request_bucket)
        if self.dry_run and not is_read_only:
            auth_branch = "dry_run_write_block"
        elif normalized_method in {"GET", "DELETE"}:
            auth_branch = "params_query_hash" if canonical_payload else "empty_params_no_query_hash"
        elif normalized_method == "POST" and endpoint == self.ORDER_SUBMIT_ENDPOINT and json_body:
            auth_branch = "order_submit_json_query_hash"
        else:
            auth_branch = "json_body_query_hash" if canonical_payload else "json_body_no_query_hash"

        payload_items = self._payload_items(auth_payload)
        return {
            "method": normalized_method,
            "endpoint": endpoint,
            "auth_mode": "jwt_hs256",
            "request_kind": "private_read" if is_read_only else "private_write",
            "auth_branch": auth_branch,
            "query_hash_included": bool(query_hash_claims.get("query_hash")),
            "query_hash_alg": query_hash_claims.get("query_hash_alg"),
            "query_hash_preview": self._mask_query_hash(str(query_hash_claims.get("query_hash") or "")),
            "throttle_bucket": request_bucket,
            "throttle_limit_per_sec": request_bucket_limit,
            "canonical_payload_present": bool(canonical_payload),
            "canonical_payload_length": len(canonical_payload),
            "payload_key_count": len(payload_items),
            "payload_keys": [key for key, _value in payload_items[:5]],
            "dry_run_write_blocked": bool(self.dry_run and not is_read_only),
            "fallback_branch_used": False,
            "content_type": (
                self.ORDER_SUBMIT_CONTENT_TYPE
                if normalized_method == "POST" and endpoint == self.ORDER_SUBMIT_ENDPOINT and json_body
                else ("application/json; charset=utf-8" if json_body else None)
            ),
            "api_key_present": bool(self.api_key),
            "api_key_length": len(self.api_key or ""),
            "api_secret_present": bool(self.api_secret),
            "api_secret_length": len(self.api_secret or ""),
        }

    @staticmethod
    def _mask_query_hash(query_hash: str) -> str:
        if len(query_hash) <= 24:
            return query_hash
        return f"{query_hash[:12]}...{query_hash[-12:]}"

    @staticmethod
    def _response_error_details(response: httpx.Response) -> tuple[str, str]:
        error_name = ""
        error_message = ""
        try:
            payload = response.json()
        except ValueError:
            return error_name, error_message

        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict):
                error_name = str(error.get("name") or "").strip()
                error_message = str(error.get("message") or "").strip()
            else:
                error_name = str(payload.get("name") or "").strip()
                error_message = str(payload.get("message") or "").strip()
        return error_name, error_message

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, object] | None = None,
        json_body: dict[str, object] | None = None,
        retry_safe: bool = False,
        response_excerpt: callable | None = None,
    ) -> dict | list:
        method = method.upper()
        request_endpoint = endpoint
        if self.dry_run and not self._is_read_only_private_request(method):
            # LIVE_DRY_RUN safety contract:
            # - allow read-only private diagnostics (GET) to reach exchange
            # - block private state-changing requests (POST/DELETE/...) from reaching exchange
            return {}
        is_order_submit = method == "POST" and endpoint == self.ORDER_SUBMIT_ENDPOINT and bool(json_body)
        if is_order_submit:
            json_body = validate_order_submit_payload(json_body or {})

        attempts = 3 if retry_safe else 1
        auth_payload = params if method in {"GET", "DELETE"} else json_body
        debug_order_submit = is_order_submit
        request_kwargs: dict[str, object] = {}
        canonical_payload = self._query_string(auth_payload) if auth_payload else ""
        signed_payload = canonical_payload if debug_order_submit else ""
        signed_payload_repr = repr(signed_payload) if debug_order_submit else ""
        transmitted_payload_repr = ""
        request_content_type: str | None = None
        if method in {"GET", "DELETE"} and params:
            request_endpoint = f"{endpoint}?{canonical_payload}"
        if json_body:
            if is_order_submit:
                request_content_type = self.ORDER_SUBMIT_CONTENT_TYPE
            else:
                request_content_type = "application/json; charset=utf-8"
                if debug_order_submit:
                    transmitted_payload_repr = repr(self._json_body_text(json_body))
                request_kwargs["json"] = json_body

        for attempt in range(attempts):
            request_bucket = self._request_bucket_for(method, endpoint)
            request_bucket_limit = self._request_bucket_limit(request_bucket)
            waited = _REQUEST_THROTTLER.acquire(bucket=request_bucket, limit_per_sec=request_bucket_limit)
            if waited > 0:
                RUN_LOG.debug(
                    format_log_kv(
                        "[PRIVATE_THROTTLE]",
                        bucket=request_bucket,
                        limit_per_sec=request_bucket_limit,
                        waited_sec=round(waited, 6),
                        method=method,
                        endpoint=endpoint,
                        attempt=attempt + 1,
                    )
                )
            attempt_nonce = str(uuid.uuid4())
            attempt_timestamp = round(time.time() * 1000)
            if is_order_submit:
                order_context = self._order_submit_auth_context(
                    json_body or {},
                    nonce=attempt_nonce,
                    timestamp=attempt_timestamp,
                )
                headers = order_context["headers"]
                canonical_payload = order_context["canonical_payload"]
                request_kwargs = dict(order_context["request_kwargs"])
                request_content_type = str(order_context["headers"].get("Content-Type") or self.ORDER_SUBMIT_CONTENT_TYPE)
                transmitted_payload_repr = repr(order_context["request_body_text"]) if debug_order_submit else ""
            else:
                headers = self._headers(
                    auth_payload,
                    content_type=request_content_type,
                    canonical_payload=canonical_payload,
                    nonce=attempt_nonce,
                    timestamp=attempt_timestamp,
                )
            if debug_order_submit:
                query_hash = str(order_context["query_hash_claims"].get("query_hash", ""))
                masked_query_hash = f"{query_hash[:12]}...{query_hash[-12:]}" if len(query_hash) > 24 else query_hash
                auth_header = headers.get("Authorization", "")
                auth_preview = ""
                if auth_header.startswith("Bearer "):
                    token = auth_header.removeprefix("Bearer ")
                    auth_preview = f"Bearer {token[:12]}...{token[-8:]}" if len(token) > 24 else "Bearer ***"
                RUN_LOG.info(
                    format_log_kv(
                        "[ORDER_HTTP_DEBUG] request",
                        method=method,
                        endpoint=endpoint,
                        content_type=headers.get("Content-Type"),
                        canonical_query_string=canonical_payload,
                        query_hash=masked_query_hash,
                        query_hash_alg=order_context["query_hash_claims"].get("query_hash_alg"),
                        nonce_present=bool(order_context["claims"].get("nonce")),
                        timestamp_present=bool(order_context["claims"].get("timestamp")),
                        authorization_preview=auth_preview,
                        signed_payload_repr=signed_payload_repr,
                        transmitted_payload_repr=transmitted_payload_repr,
                    )
                )
            try:
                with httpx.Client(base_url=self.base_url, timeout=10.0) as client:
                    res = client.request(method, request_endpoint, headers=headers, **request_kwargs)

                if debug_order_submit:
                    RUN_LOG.info(
                        format_log_kv(
                            "[ORDER_HTTP_DEBUG] response",
                            method=method,
                            endpoint=endpoint,
                            content_type=headers.get("Content-Type"),
                            status_code=res.status_code,
                            response_body=response_excerpt(res) if response_excerpt else "",
                        )
                    )

                if 500 <= res.status_code <= 599:
                    body = response_excerpt(res) if response_excerpt else ""
                    raise BrokerTemporaryError(
                        f"bithumb private {endpoint} server error status={res.status_code} body={body}"
                    )
                res.raise_for_status()
                response_content = getattr(res, "content", None)
                data = res.json() if response_content != b"" else {}
            except _HTTPX_TRANSIENT_ERRORS as exc:
                if attempt < attempts - 1:
                    time.sleep(
                        _retry_backoff_delay(
                            attempt=attempt,
                            bucket=request_bucket,
                            reason="transport",
                        )
                    )
                    continue
                raise BrokerTemporaryError(
                    f"bithumb private {endpoint} transport error: {type(exc).__name__}: {exc}"
                ) from exc
            except httpx.HTTPStatusError as exc:
                body = response_excerpt(exc.response) if response_excerpt else ""
                error_name, error_message = self._response_error_details(exc.response)
                if _is_order_not_ready_indicator(
                    status_code=exc.response.status_code,
                    error_name=error_name,
                    error_message=error_message,
                    body=body,
                ):
                    raise BithumbOrderNotReadyError(
                        f"bithumb private {endpoint} rejected with http status={exc.response.status_code} "
                        f"error_name={error_name or '-'} error_message={error_message or '-'} body={body}"
                    ) from exc
                if _is_rate_limit_indicator(
                    status_code=exc.response.status_code,
                    error_name=error_name,
                    error_message=error_message,
                    body=body,
                ):
                    delay = _retry_backoff_delay(
                        attempt=attempt,
                        bucket=request_bucket,
                        reason="rate_limit",
                    )
                    _REQUEST_THROTTLER.penalize(bucket=request_bucket, delay_sec=delay)
                    RUN_LOG.warning(
                        format_log_kv(
                            "[PRIVATE_RATE_LIMIT]",
                            bucket=request_bucket,
                            method=method,
                            endpoint=endpoint,
                            attempt=attempt + 1,
                            delay_sec=round(delay, 6),
                            status_code=exc.response.status_code,
                            error_name=error_name or "-",
                        )
                    )
                    if retry_safe and attempt < attempts - 1:
                        time.sleep(delay)
                        continue
                    raise BithumbRateLimitError(
                        f"bithumb private {endpoint} throttled with http status={exc.response.status_code} "
                        f"error_name={error_name or '-'} error_message={error_message or '-'} body={body}"
                    ) from exc
                if _is_server_error_indicator(
                    status_code=exc.response.status_code,
                    error_name=error_name,
                    error_message=error_message,
                    body=body,
                ):
                    if attempt < attempts - 1:
                        time.sleep(
                            _retry_backoff_delay(
                                attempt=attempt,
                                bucket=request_bucket,
                                reason="server",
                            )
                        )
                        continue
                    raise BrokerTemporaryError(
                        f"bithumb private {endpoint} server error status={exc.response.status_code} body={body}"
                    ) from exc
                detail_parts = [
                    f"bithumb private {endpoint} rejected with http status={exc.response.status_code}",
                ]
                if error_name:
                    detail_parts.append(f"error_name={error_name}")
                if error_message:
                    detail_parts.append(f"error_message={error_message}")
                if body:
                    detail_parts.append(f"body={body}")
                raise BrokerRejectError(
                    " ".join(detail_parts)
                ) from exc

            if isinstance(data, dict) and data.get("status") not in (None, "0000"):
                raise BrokerRejectError(f"bithumb private call rejected: {data}")
            return data

        raise BrokerTemporaryError(f"bithumb private {endpoint} failed after retries")


def _classify_cancel_reject(exc: BrokerRejectError) -> tuple[str, str]:
    detail = str(exc).lower()
    code, summary = classify_private_api_error(exc)
    if "order_not_ready" in detail or "not ready yet" in detail or code == "ORDER_NOT_READY":
        return "ORDER_NOT_READY", "order not ready yet"
    if any(token in detail for token in ("already canceled", "already cancelled", "already cancel", "state=cancel")):
        return "ALREADY_CANCELED", "order already canceled"
    if any(token in detail for token in ("already filled", "already executed", "fully executed", "state=done")):
        return "ALREADY_FILLED", "order already filled"
    if code in {"ORDER_NOT_FOUND", "LOOKUP_NOT_FOUND"} or any(token in detail for token in ("not found", "no such order", "unknown order")):
        return "NOT_FOUND", "order not found"
    if any(token in detail for token in ("cannot cancel", "not cancelable", "pending")):
        return "PENDING_NOT_CANCELABLE", "order not cancelable in current state"
    return code, summary


def classify_private_api_error(exc: Exception) -> tuple[str, str]:
    detail = str(exc).lower()
    error_name = _private_error_name(detail)
    documented = _documented_private_error_descriptor_from_detail(error_name=error_name, detail=detail)
    fallback = _fallback_private_error_descriptor(error_name)
    if isinstance(exc, BithumbAuthError):
        if exc.reason_code == "AUTH_KEY_MISSING":
            return "AUTH_KEY_MISSING", "API key missing before JWT signing"
        if exc.reason_code == "AUTH_SECRET_MISSING":
            return "AUTH_SECRET_MISSING", "API secret missing before JWT signing"
        return exc.reason_code, "private API auth material missing before signing"
    if isinstance(exc, BithumbRateLimitError):
        return "RATE_LIMITED", "private API rate limit or overload encountered"
    if isinstance(exc, BithumbOrderNotReadyError):
        return "ORDER_NOT_READY", "order not ready yet; refresh and retry later"
    if documented is not None:
        code, summary, _category, _should_retry, _disable_trading, _needs_reconcile = documented
        return code, summary
    if fallback is not None:
        code, summary, _category, _should_retry, _disable_trading, _needs_reconcile = fallback
        return code, summary
    if error_name in _RATE_LIMIT_ERROR_NAMES or any(token in detail for token in ("too many requests", "rate limit", "throttl", "throttle", "overload", "overloaded")):
        return "RATE_LIMITED", "private API rate limit or overload encountered"
    if error_name in _ORDER_NOT_READY_ERROR_NAMES or "order_not_ready" in detail:
        return "ORDER_NOT_READY", "order not ready yet; refresh and retry later"
    if isinstance(exc, BrokerTemporaryError) and any(token in detail for token in ("server error", "internal server error", "service unavailable", "gateway timeout", "status=5")):
        return "SERVER_INTERNAL_FAILURE", "server/internal failure reported by broker; retry with backoff"
    if isinstance(exc, BrokerTemporaryError) or any(
        token in detail for token in ("transport error", "timeout", "timed out", "temporar")
    ):
        return "TEMPORARY", "temporary network/transport error; retry with backoff"
    if isinstance(exc, BrokerIdentifierMismatchError) or "identifier mismatch" in detail:
        return "IDENTIFIER_MISMATCH", "request/response identifiers conflict; reject and investigate"
    if isinstance(exc, BrokerSchemaError) or "schema mismatch" in detail:
        return "DOC_SCHEMA", "documented response schema mismatch (/v1/orders or /v1/order)"
    if error_name == "invalid_query_payload" or any(token in detail for token in ("invalid_query_payload", "query hash mismatch", "query_hash mismatch")):
        return "AUTH_QUERY_HASH_MISMATCH", "JWT query_hash mismatch; GET query string/body hash must match transmitted params"
    if error_name == "jwt_verification":
        return "AUTH_JWT_VERIFICATION", "JWT verification failed"
    if error_name == "expired_jwt":
        return "AUTH_JWT_EXPIRED", "JWT expired before broker accepted the request"
    if error_name == "notallowip":
        return "AUTH_IP_DENIED", "client IP is not allowed for this API key"
    if error_name == "out_of_scope":
        return "PERMISSION", "API key scope/permission denied"
    if error_name == "bank_account_required" or "bank_account_required" in detail:
        return "ACCOUNT_SETUP_REQUIRED", "real-name deposit/withdrawal account registration is required"
    if error_name == "two_factor_auth_required" or "two_factor_auth_required" in detail:
        return "AUTH_CHANNEL_REQUIRED", "valid authentication channel is required"
    if error_name == "blocked_member_id" or "blocked_member_id" in detail:
        return "ACCOUNT_RESTRICTED", "service usage restricted by operational policy"
    if error_name == "withdraw_insufficient_balance" or "withdraw_insufficient_balance" in detail:
        return "WITHDRAW_LIMIT_BLOCKED", "withdrawal limit exceeded"
    if error_name == "invalid_access_key" or "invalid_access_key" in detail:
        return "AUTH_INVALID_ACCESS_KEY", "API access key rejected by broker"
    if error_name in {"duplicate_client_order_id", "id_conflict"} or any(
        token in detail for token in ("duplicate_client_order_id", "id_conflict", "duplicate client order")
    ):
        return "DUPLICATE_CLIENT_ORDER_ID", "duplicate client order id or identifier conflict"
    if any(token in detail for token in ("cancel_not_allowed", "already finalized", "terminal state")):
        return "CANCEL_NOT_ALLOWED", "cancel not allowed in current state"
    if "status=401" in detail or "unauthorized" in detail or "invalid jwt" in detail or "signature" in detail:
        return "AUTH_SIGN", "authentication/signature failed (401 / invalid JWT / key-secret mismatch)"
    if "status=403" in detail or "out_of_scope" in detail or "permission" in detail:
        return "PERMISSION", "API key scope/permission denied"
    if any(token in detail for token in ("unexpected broker response", "unexpected /v1/orders/chance payload type", "unexpected /v1/accounts payload type")):
        return "AUTH_RESPONSE_UNEXPECTED", "private broker returned an unexpected response shape"
    if any(
        token in detail
        for token in (
            "broad /v1/orders",
            "requires identifiers",
            "fallback is disabled",
            "identifier-scoped by bot policy",
            "recovery-only market/state scans",
        )
    ):
        return "RECOVERY_REQUIRED", "startup/recovery path requires identifier-based lookup only"
    if error_name == "cross_trading":
        return "CROSS_TRADING", "exchange rejected self-crossing order"
    if error_name in {"under_min_total", "too_many_orders"} or any(token in detail for token in ("under_min_total", "too_many_orders")):
        return "PRETRADE_GUARD", "order notional or order-count guard failed"
    if error_name in _PARAM_ERROR_NAMES or any(token in detail for token in ("market", "price", "volume", "ord_type", "validation", "currency does not have a valid value", "invalid_parameter")):
        return "INVALID_REQUEST", "market/order parameter validation failed"
    if error_name in {"deposit_not_found", "withdraw_not_found"}:
        return "LOOKUP_NOT_FOUND", "deposit/withdraw lookup target not found"
    if error_name == "order_not_found":
        return "ORDER_NOT_FOUND", "order lookup/cancel target not found"
    return "UNRECOVERABLE", "unclassified private API failure; operator investigation required"


def build_broker_with_auth_diagnostics(
    *,
    caller: str,
    env_summary: dict[str, object] | None = None,
    broker_factory=None,
) -> tuple[BithumbBroker, dict[str, object]]:
    factory = broker_factory or BithumbBroker
    broker = factory()
    diagnostics = getattr(broker, "get_auth_runtime_diagnostics", lambda **_kwargs: {})(
        caller=caller,
        env_summary=env_summary,
    )
    should_log = bool(getattr(broker, "auth_diagnostics_enabled", lambda: False)())
    if should_log:
        getattr(broker, "log_auth_runtime_diagnostics", lambda **_kwargs: None)(
            caller=caller,
            env_summary=env_summary,
        )
    return broker, diagnostics


class BithumbBroker:
    def __init__(self) -> None:
        self.api_key = settings.BITHUMB_API_KEY
        self.api_secret = settings.BITHUMB_API_SECRET
        self.base_url = settings.BITHUMB_API_BASE
        self.dry_run = settings.LIVE_DRY_RUN
        self._read_journal: dict[str, str] = {}
        self._private_api = BithumbPrivateAPI(
            api_key=self.api_key,
            api_secret=self.api_secret,
            base_url=self.base_url,
            dry_run=self.dry_run,
        )
        self._balance_source: BalanceSource | None = None

    def _mask_sensitive(self, data: dict[str, object]) -> dict[str, object]:
        redacted: dict[str, object] = {}
        for key, value in data.items():
            lowered = str(key).lower()
            if any(token in lowered for token in ("secret", "sign", "nonce", "api", "authorization", "token", "key", "jwt")):
                continue
            redacted[key] = value
        return redacted

    def _sanitize_debug_value(self, value: object) -> object:
        if isinstance(value, dict):
            return {k: self._sanitize_debug_value(v) for k, v in self._mask_sensitive(value).items()}
        if isinstance(value, list):
            return [self._sanitize_debug_value(item) for item in value[:5]]
        return value

    def _response_body_excerpt(self, response: httpx.Response, *, limit: int = 240) -> str:
        try:
            body: object = response.json()
        except ValueError:
            body = str(getattr(response, "text", "")).strip()

        if isinstance(body, (dict, list)):
            rendered = json.dumps(self._sanitize_debug_value(body), ensure_ascii=False, separators=(",", ":"))
        else:
            rendered = str(body)

        rendered = " ".join(rendered.split())
        if len(rendered) > limit:
            return rendered[: limit - 3] + "..."
        return rendered

    def _normalize_order_side(self, side: str | None, *, default: str = "BUY") -> str:
        return normalize_order_row_side(side, default=default)

    @staticmethod
    def _clean_identifier(value: object) -> str:
        text = str(value or "").strip()
        return text

    def _resolve_order_identifiers(
        self,
        row: dict[str, object],
        *,
        fallback_client_order_id: str | None = None,
        fallback_exchange_order_id: str | None = None,
        allow_coid_alias: bool = False,
        context: str = "order response",
    ) -> tuple[str, str]:
        exchange_order_id = ""
        for key in ("uuid", "order_id"):
            candidate = self._clean_identifier(row.get(key))
            if candidate:
                exchange_order_id = candidate
                break
        if not exchange_order_id:
            exchange_order_id = self._clean_identifier(fallback_exchange_order_id)

        client_order_id = self._clean_identifier(row.get("client_order_id"))
        coid_alias = self._clean_identifier(row.get("coid"))
        if allow_coid_alias:
            # v2/?ㅼ떆媛?MyOrder) 怨꾩뿴 payload?먯꽌??`coid`媛 client_order_id 蹂꾩묶?쇰줈 ?????덈떎.
            # REST v1 ?쎄린 怨꾩빟(uuid/client_order_id)怨?異⑸룎???쇳븯湲??꾪빐 alias??opt-in 寃쎈줈?먯꽌留??댁꽍?쒕떎.
            if client_order_id and coid_alias and client_order_id != coid_alias:
                raise BrokerRejectError(
                    f"{context} client identifier mismatch: client_order_id={client_order_id} coid={coid_alias}"
                )
            if not client_order_id:
                client_order_id = coid_alias
        if not client_order_id:
            client_order_id = self._clean_identifier(fallback_client_order_id)

        return client_order_id, exchange_order_id

    def _journal_read_summary(self, *, path: str, data: dict[str, object] | list[object] | None) -> None:
        payload = data or {}
        rows: object
        if isinstance(payload, dict):
            rows = payload.get("data", payload)
        else:
            rows = payload
        row_count = len(rows) if isinstance(rows, list) else (1 if isinstance(rows, dict) else 0)

        sample_order_ids: list[str] = []
        if isinstance(rows, list):
            for row in rows[:3]:
                if not isinstance(row, dict):
                    continue
                order_id = row.get("uuid") or row.get("order_id")
                if order_id:
                    sample_order_ids.append(str(order_id))

        summary: dict[str, object] = {
            "path": path,
            "status": str(payload.get("status", "0000")) if isinstance(payload, dict) else "0000",
            "row_count": row_count,
        }
        if sample_order_ids:
            summary["sample_order_ids"] = sample_order_ids
        if path == "/v1/accounts" and isinstance(rows, list):
            currencies: list[str] = []
            duplicate_currencies: list[str] = []
            seen: set[str] = set()
            for row in rows:
                if not isinstance(row, dict):
                    continue
                raw_currency = row.get("currency")
                currency = str(raw_currency).strip().upper() if raw_currency is not None else ""
                if not currency:
                    continue
                if currency in seen and currency not in duplicate_currencies:
                    duplicate_currencies.append(currency)
                seen.add(currency)
                currencies.append(currency)
            if currencies:
                summary["currencies"] = sorted(seen)
            if duplicate_currencies:
                summary["duplicate_currencies"] = sorted(duplicate_currencies)
        if isinstance(rows, dict):
            summary["keys"] = sorted(self._mask_sensitive(rows).keys())[:10]
        self._read_journal[path] = str(summary)

    def get_read_journal_summary(self) -> dict[str, str]:
        return dict(self._read_journal)

    @staticmethod
    def auth_diagnostics_enabled() -> bool:
        return str(os.getenv("BITHUMB_AUTH_DIAGNOSTICS", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }

    def get_auth_runtime_diagnostics(
        self,
        *,
        caller: str,
        env_summary: dict[str, object] | None = None,
    ) -> dict[str, object]:
        accounts_preview = self._private_api.describe_request_auth(
            "GET",
            "/v1/accounts",
            params={},
        )
        chance_preview = self._private_api.describe_request_auth(
            "GET",
            "/v1/orders/chance",
            params={"market": self._market()},
        )
        return {
            "caller": caller,
            "mode": settings.MODE,
            "market": self._market(),
            "base_url": self.base_url,
            "live_dry_run": bool(self.dry_run),
            "live_real_order_armed": bool(settings.LIVE_REAL_ORDER_ARMED),
            "ws_myasset_enabled": bool(settings.BITHUMB_WS_MYASSET_ENABLED),
            "balance_source_selected": self.get_balance_source_id(),
            "api_key_present": bool(self.api_key),
            "api_key_length": len(self.api_key or ""),
            "api_secret_present": bool(self.api_secret),
            "api_secret_length": len(self.api_secret or ""),
            "env": dict(env_summary or {}),
            "accounts_auth": accounts_preview,
            "chance_auth": chance_preview,
        }

    def log_auth_runtime_diagnostics(
        self,
        *,
        caller: str,
        env_summary: dict[str, object] | None = None,
        level: int = logging.INFO,
    ) -> dict[str, object]:
        diagnostics = self.get_auth_runtime_diagnostics(caller=caller, env_summary=env_summary)
        env = diagnostics.get("env") if isinstance(diagnostics.get("env"), dict) else {}
        accounts_auth = diagnostics.get("accounts_auth") if isinstance(diagnostics.get("accounts_auth"), dict) else {}
        chance_auth = diagnostics.get("chance_auth") if isinstance(diagnostics.get("chance_auth"), dict) else {}
        RUN_LOG.log(
            level,
            format_log_kv(
                "[AUTH_INIT_DIAG]",
                caller=caller,
                mode=diagnostics.get("mode"),
                market=diagnostics.get("market"),
                env_source_key=env.get("source_key"),
                env_file=env.get("env_file"),
                env_loaded=env.get("loaded"),
                env_exists=env.get("exists"),
                env_override=env.get("override"),
                api_key_present=diagnostics.get("api_key_present"),
                api_key_length=diagnostics.get("api_key_length"),
                api_secret_present=diagnostics.get("api_secret_present"),
                api_secret_length=diagnostics.get("api_secret_length"),
                live_dry_run=diagnostics.get("live_dry_run"),
                live_real_order_armed=diagnostics.get("live_real_order_armed"),
                ws_myasset_enabled=diagnostics.get("ws_myasset_enabled"),
                balance_source_selected=diagnostics.get("balance_source_selected"),
                accounts_auth_branch=accounts_auth.get("auth_branch"),
                accounts_query_hash_included=accounts_auth.get("query_hash_included"),
                chance_auth_branch=chance_auth.get("auth_branch"),
                chance_query_hash_included=chance_auth.get("query_hash_included"),
                chance_query_hash_preview=chance_auth.get("query_hash_preview"),
                chance_throttle_bucket=chance_auth.get("throttle_bucket"),
                chance_throttle_limit_per_sec=chance_auth.get("throttle_limit_per_sec"),
                chance_payload_keys=chance_auth.get("payload_keys"),
                fallback_branch_used=chance_auth.get("fallback_branch_used"),
            ),
        )
        return diagnostics

    def get_accounts_validation_diagnostics(self) -> dict[str, object]:
        source = self._balance_source
        if source is not None and hasattr(source, "get_validation_diagnostics"):
            return dict(source.get_validation_diagnostics())
        return {
            "reason": "not_applicable",
            "failure_category": "none",
            "row_count": 0,
            "currencies": [],
            "missing_required_currencies": [],
            "duplicate_currencies": [],
            "execution_mode": "unknown",
            "quote_currency": None,
            "base_currency": None,
            "base_currency_missing_policy": None,
            "allow_missing_base_currency": False,
            "flat_start_allowed": False,
            "flat_start_reason": "not_available",
            "preflight_outcome": "not_checked",
            "last_success_reason": None,
            "last_failure_reason": None,
            "source": self.get_balance_source_id(),
            "last_observed_ts_ms": None,
            "last_asset_ts_ms": None,
            "last_success_ts_ms": None,
            "last_failure_ts_ms": None,
            "stale": False,
        }

    def get_balance_source_id(self) -> str:
        source = self._balance_source
        if isinstance(source, AccountsV1BalanceSource):
            return AccountsV1BalanceSource.SOURCE_ID
        if isinstance(source, MyAssetWsBalanceSource):
            return MyAssetWsBalanceSource.SOURCE_ID
        if isinstance(source, DryRunBalanceSource):
            return "dry_run_static"
        if self.dry_run:
            return "dry_run_static"
        return MyAssetWsBalanceSource.SOURCE_ID if bool(settings.BITHUMB_WS_MYASSET_ENABLED) else AccountsV1BalanceSource.SOURCE_ID

    def _get_balance_source(self) -> BalanceSource:
        source = self._balance_source
        if source is None:
            source = self._build_balance_source()
            self._balance_source = source
        return source

    def _build_balance_source(self) -> BalanceSource:
        if self.dry_run:
            return DryRunBalanceSource()
        order_currency, payment_currency = self._pair()
        if bool(settings.BITHUMB_WS_MYASSET_ENABLED):
            return MyAssetWsBalanceSource(
                connection_factory=self._build_myasset_ws_connection,
                order_currency=order_currency,
                payment_currency=payment_currency,
                now_ms=lambda: int(time.time() * 1000),
                stale_after_ms=int(settings.BITHUMB_WS_MYASSET_STALE_AFTER_MS),
                recv_timeout_sec=float(settings.BITHUMB_WS_MYASSET_RECV_TIMEOUT_SEC),
                subscribe_ticket=str(settings.BITHUMB_WS_MYASSET_SUBSCRIBE_TICKET or "").strip() or None,
            )
        return AccountsV1BalanceSource(
            fetch_accounts_raw=lambda: self.fetch_accounts_raw(),
            order_currency=order_currency,
            payment_currency=payment_currency,
            now_ms=lambda: int(time.time() * 1000),
            parse_accounts_response=lambda payload: parse_accounts_response(payload),
            select_pair_balances=lambda accounts, **kwargs: select_pair_balances(accounts, **kwargs),
            to_broker_balance=lambda pair: to_broker_balance(pair),
        )

    def _build_myasset_ws_connection(self):
        raise BrokerTemporaryError(
            "myAsset websocket private stream adapter is not configured; "
            "provide broker._build_myasset_ws_connection override/injection for runtime"
        )

    def _request_private(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, object] | None = None,
        json_body: dict[str, object] | None = None,
        retry_safe: bool = False,
    ) -> dict | list:
        if str(method).upper() == "POST" and endpoint == BithumbPrivateAPI.ORDER_SUBMIT_ENDPOINT:
            raise BrokerRejectError(
                "raw /v2/orders submit bypass is disabled; use place_order() validated submit flow"
            )
        return self._private_api.request(
            method,
            endpoint,
            params=params,
            json_body=json_body,
            retry_safe=retry_safe,
            response_excerpt=self._response_body_excerpt,
        )

    def _submit_validated_order_payload(
        self,
        *,
        payload_plan,
        retry_safe: bool = False,
    ) -> dict | list:
        compat_post_private = getattr(self, "_post_private")
        default_post_private = getattr(type(self), "_post_private")
        compat_func = getattr(compat_post_private, "__func__", compat_post_private)
        default_func = getattr(default_post_private, "__func__", default_post_private)
        if compat_func is not default_func:
            return compat_post_private(
                BithumbPrivateAPI.ORDER_SUBMIT_ENDPOINT,
                payload_plan.payload,
                retry_safe=retry_safe,
            )
        return self._private_api.request(
            "POST",
            BithumbPrivateAPI.ORDER_SUBMIT_ENDPOINT,
            json_body=payload_plan.payload,
            retry_safe=retry_safe,
            response_excerpt=self._response_body_excerpt,
        )

    def _build_quantity_guard(
        self,
        *,
        side: str,
        qty: float,
        market_price: float | None,
        min_qty: float,
        qty_step: float,
        min_notional_krw: float,
        max_qty_decimals: int,
        exit_fee_ratio: float = 0.0,
        exit_slippage_bps: float = 0.0,
        exit_buffer_ratio: float = 0.0,
    ):
        from ..dust import build_executable_lot

        return build_executable_lot(
            qty=qty,
            market_price=market_price,
            min_qty=min_qty,
            qty_step=qty_step,
            min_notional_krw=min_notional_krw,
            max_qty_decimals=max_qty_decimals,
            exit_fee_ratio=exit_fee_ratio,
            exit_slippage_bps=exit_slippage_bps,
            exit_buffer_ratio=exit_buffer_ratio,
        )

    def _log_v1_orders_parse_failure(
        self,
        *,
        endpoint: str,
        state: str,
        exchange_ids_count: int,
        client_ids_count: int,
        row: dict[str, object],
        reason: str,
    ) -> None:
        RUN_LOG.error(
            format_log_kv(
                "[V1_ORDERS_PARSE_FAIL]",
                endpoint=endpoint,
                state=state,
                exchange_ids_count=exchange_ids_count,
                client_ids_count=client_ids_count,
                uuid_present=bool(self._clean_identifier(row.get("uuid"))),
                client_order_id_present=bool(self._clean_identifier(row.get("client_order_id"))),
                parser_failure_reason=reason,
            )
        )

    def _log_v1_orders_price_resolution(
        self,
        *,
        endpoint: str,
        state: str,
        exchange_ids_count: int,
        client_ids_count: int,
        row: dict[str, object],
        normalized: V1ListNormalizedOrder,
    ) -> None:
        if normalized.price_source == "price" and not normalized.price_missing:
            return
        present_fields = [
            key
            for key in (
                "price",
                "avg_price",
                "average_price",
                "avg_execution_price",
                "trade_price",
                "price_avg",
                "executed_volume",
                "executed_funds",
                "volume",
                "remaining_volume",
            )
            if row.get(key) not in (None, "")
        ]
        RUN_LOG.info(
            format_log_kv(
                "[V1_ORDERS_PRICE_RESOLUTION]",
                endpoint=endpoint,
                state=state,
                exchange_ids_count=exchange_ids_count,
                client_ids_count=client_ids_count,
                uuid_present=bool(self._clean_identifier(row.get("uuid"))),
                client_order_id_present=bool(self._clean_identifier(row.get("client_order_id"))),
                price_source=normalized.price_source or "missing",
                price_missing=int(normalized.price_missing),
                terminal_confirmation_only=int(normalized.price_missing and state in {"done", "cancel"}),
                present_fields=",".join(present_fields) if present_fields else "-",
                degraded_fields=",".join(normalized.degraded_fields) if normalized.degraded_fields else "-",
            )
        )

    def _log_v1_myorder_lookup_failure(
        self,
        *,
        stage: str,
        retry_safe: bool,
        requested_client_order_id: str,
        requested_exchange_order_id: str,
        response_client_order_id: str,
        response_exchange_order_id: str,
        reason: str,
    ) -> None:
        RUN_LOG.error(
            format_log_kv(
                "[V1_MYORDER_LOOKUP_FAIL]",
                stage=stage,
                retry_safe=int(retry_safe),
                retryable=int("temporary" in reason.lower()),
                requested_client_order_id=requested_client_order_id,
                requested_exchange_order_id=requested_exchange_order_id,
                response_client_order_id=response_client_order_id,
                response_exchange_order_id=response_exchange_order_id,
                reason=reason,
            )
        )

    def _get_private(self, endpoint: str, params: dict[str, object], *, retry_safe: bool = False) -> dict | list:
        return self._request_private("GET", endpoint, params=params, retry_safe=retry_safe)

    def _post_private(self, endpoint: str, payload: dict[str, object], *, retry_safe: bool = False) -> dict | list:
        if self.dry_run:
            return {"status": "0000", "data": {"uuid": f"dry_{payload.get('uuid', payload.get('market', 'order'))}"}}
        return self._request_private("POST", endpoint, json_body=payload, retry_safe=retry_safe)

    def _delete_private(self, endpoint: str, params: dict[str, object], *, retry_safe: bool = False) -> dict | list:
        return self._request_private("DELETE", endpoint, params=params, retry_safe=retry_safe)

    def _pair(self) -> tuple[str, str]:
        quote_currency, order_currency = canonical_market_id(settings.PAIR).split("-", 1)
        return order_currency, quote_currency

    def _market(self) -> str:
        return canonical_market_id(settings.PAIR)

    @staticmethod
    def _now_millis() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _decimal_from_value(value: object) -> Decimal:
        return decimal_from_value(value)

    @classmethod
    def _format_krw_amount(cls, value: object) -> str:
        return format_krw_amount(value)

    @classmethod
    def _format_volume(cls, qty: object, *, places: int = 8) -> str:
        return format_volume(qty, places=places)

    @classmethod
    def _truncate_volume(cls, qty: object, *, places: int = 8) -> float:
        return truncate_volume(qty, places=places)

    @classmethod
    def _validate_volume_constraints(
        cls,
        *,
        qty: object,
        volume_text: str,
        min_qty: object,
        qty_step: object,
        max_qty_decimals: int,
        context: str,
    ) -> None:
        requested_qty = cls._decimal_from_value(qty)
        serialized_qty = cls._decimal_from_value(volume_text)
        if serialized_qty != requested_qty:
            raise BrokerRejectError(
                f"{context} qty requires explicit normalization before submit: "
                f"requested={format(requested_qty, 'f')} serialized={format(serialized_qty, 'f')}"
            )

        minimum_qty = cls._decimal_from_value(min_qty) if min_qty not in (None, "") else Decimal("0")
        if minimum_qty > 0 and serialized_qty < minimum_qty:
            raise BrokerRejectError(
                f"{context} qty below minimum: "
                f"qty={format(serialized_qty, 'f')} min_qty={format(minimum_qty, 'f')}"
            )

        step = cls._decimal_from_value(qty_step) if qty_step not in (None, "") else Decimal("0")
        if step > 0 and (serialized_qty % step) != 0:
            raise BrokerRejectError(
                f"{context} qty does not match qty_step: "
                f"qty={format(serialized_qty, 'f')} qty_step={format(step, 'f')}"
            )

        decimals = len(volume_text.split(".", 1)[1]) if "." in volume_text else 0
        if max_qty_decimals > 0 and decimals > max_qty_decimals:
            raise BrokerRejectError(
                f"{context} qty exceeds max decimals: "
                f"qty={volume_text} decimals={decimals} max_qty_decimals={max_qty_decimals}"
            )

    @staticmethod
    def _parse_ts(raw: object) -> int:
        return parse_ts(raw)

    @staticmethod
    def _number(payload: dict[str, object], *keys: str) -> float:
        for key in keys:
            raw = payload.get(key)
            if raw in (None, ""):
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _required_number(payload: dict[str, object], key: str, *, context: str) -> float:
        raw = payload.get(key)
        if raw in (None, ""):
            raise BrokerRejectError(f"{context} schema mismatch: missing required numeric field '{key}'")
        try:
            parsed = float(raw)
        except (TypeError, ValueError) as exc:
            raise BrokerRejectError(f"{context} schema mismatch: invalid numeric field '{key}'={raw}") from exc
        if not math.isfinite(parsed):
            raise BrokerRejectError(f"{context} schema mismatch: non-finite numeric field '{key}'={raw}")
        return parsed

    @staticmethod
    def _strict_optional_number(payload: dict[str, object], key: str, *, context: str) -> float | None:
        raw = payload.get(key)
        if raw in (None, ""):
            return None
        try:
            parsed = float(raw)
        except (TypeError, ValueError) as exc:
            raise BrokerRejectError(f"{context} schema mismatch: invalid numeric field '{key}'={raw}") from exc
        if not math.isfinite(parsed):
            raise BrokerRejectError(f"{context} schema mismatch: non-finite numeric field '{key}'={raw}")
        return parsed

    def fetch_accounts_raw(self) -> object:
        """Fetch a balance snapshot via private REST `/v1/accounts`."""
        response = self._get_private("/v1/accounts", {}, retry_safe=True)
        self._journal_read_summary(path="/v1/accounts", data=response)
        return response

    @staticmethod
    def _strict_parse_ts(raw: object, *, field_name: str, context: str) -> int:
        return strict_parse_ts(raw, field_name=field_name, context=context)

    @staticmethod
    def _optional_number(payload: dict[str, object], *keys: str) -> float | None:
        for key in keys:
            raw = payload.get(key)
            if raw in (None, ""):
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _to_float(value: object, *, default: float | None = 0.0) -> float | None:
        if value is None:
            return default
        if isinstance(value, str):
            text = value.strip()
            if not text or text.lower() in {"null", "none", "nan", "inf", "-inf", "+inf"}:
                return default
            normalized = text.replace(",", "")
        else:
            normalized = value
        try:
            parsed = float(normalized)
        except (TypeError, ValueError):
            return default
        if not math.isfinite(parsed):
            return default
        return parsed

    def _extract_fill_fee(
        self,
        row: dict[str, object],
        *,
        context: str,
        qty: float | None,
        price: float | None,
        strict: bool = False,
    ) -> float:
        material_notional_threshold = max(0.0, float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW))
        fill_notional = 0.0
        if qty is not None and price is not None:
            fill_notional = max(0.0, float(qty)) * max(0.0, float(price))
        material_fee_validation_required = fill_notional >= material_notional_threshold > 0.0
        fee_keys = ("fee", "paid_fee", "commission", "trade_fee", "transaction_fee", "fee_amount")
        present_keys = [key for key in fee_keys if key in row]
        log_payload = self._sanitize_debug_value(
            {
                "context": context,
                "fill_hint_id": row.get("uuid") or row.get("id") or row.get("order_id"),
                "fill_notional": fill_notional,
                "material_fee_validation_required": material_fee_validation_required,
                "present_fee_keys": present_keys,
                "fee_values": {key: row.get(key) for key in present_keys},
            }
        )
        if not present_keys:
            if strict and material_fee_validation_required:
                raise BrokerRejectError(
                    f"/v1/order.{context} schema mismatch: missing fee field for materially sized fill"
                )
            RUN_LOG.warning(format_log_kv("[FILL_FEE] missing fee key", payload=log_payload))
            return 0.0

        for key in present_keys:
            raw = row.get(key)
            parsed = self._to_float(raw, default=None)
            if parsed is None:
                if raw in (None, "") or (isinstance(raw, str) and raw.strip().lower() in {"null", "none"}):
                    if strict and material_fee_validation_required:
                        raise BrokerRejectError(
                            f"/v1/order.{context} schema mismatch: empty fee field '{key}' for materially sized fill"
                        )
                    RUN_LOG.warning(format_log_kv("[FILL_FEE] empty fee value", payload=log_payload, fee_key=key))
                    continue
                if strict:
                    raise BrokerRejectError(f"/v1/order.{context} schema mismatch: invalid fee field '{key}'={raw}")
                else:
                    RUN_LOG.warning(format_log_kv("[FILL_FEE] invalid fee value", payload=log_payload, fee_key=key))
                continue
            fee = float(parsed)
            if fee < 0:
                raise BrokerRejectError(f"/v1/order.{context} schema mismatch: negative fee field '{key}'={raw}")
            if fee == 0.0 and (qty or 0.0) > 0 and price is not None and price > 0:
                if strict and material_fee_validation_required:
                    raise BrokerRejectError(
                        f"/v1/order.{context} schema mismatch: zero fee field '{key}' for materially sized fill"
                    )
                RUN_LOG.warning(format_log_kv("[FILL_FEE] resolved zero fee", payload=log_payload, fee_key=key))
            return fee

        if strict:
            has_non_empty_fee = any(
                row.get(key) not in (None, "") and not (isinstance(row.get(key), str) and str(row.get(key)).strip().lower() in {"null", "none"})
                for key in present_keys
            )
            if has_non_empty_fee or material_fee_validation_required:
                raise BrokerRejectError(f"/v1/order.{context} schema mismatch: unable to parse fee field")
        RUN_LOG.warning(format_log_kv("[FILL_FEE] unable to parse any fee value", payload=log_payload))
        return 0.0

    def _resolve_fill_price(
        self,
        payload: dict[str, object],
        *,
        normalized_row: dict[str, object] | None = None,
    ) -> float | None:
        candidates: tuple[float | None, ...] = (
            self._optional_number(payload, "price", "trade_price", "avg_price", "avg_execution_price"),
            self._optional_number(payload, "cont_price", "contract_price"),
            self._optional_number(payload, "order_price"),
            self._optional_number(payload, "price_avg"),
            (float(normalized_row["price"]) if normalized_row and normalized_row.get("price") is not None else None),
        )
        for candidate in candidates:
            if candidate is None:
                continue
            if candidate > 0:
                return float(candidate)
        return None

    def _normalize_v2_order_row(self, row: dict[str, object]) -> dict[str, object]:
        return normalize_v2_order_row(row)

    def _normalize_v1_order_row_lenient_for_fills(self, row: dict[str, object]) -> dict[str, object]:
        return normalize_v1_order_row_lenient_for_fills(row)

    def _normalize_v1_order_row_strict(self, row: dict[str, object]) -> V1NormalizedOrder:
        return normalize_v1_order_row_strict(row)

    @staticmethod
    def _raw_v2_order_fields(
        row: dict[str, object],
        *,
        fallback_client_order_id: str | None = None,
        fallback_exchange_order_id: str | None = None,
    ) -> dict[str, object]:
        raw: dict[str, object] = {}
        for key in ("market", "ord_type", "order_type", "client_order_id"):
            if row.get(key) not in (None, ""):
                raw[key] = row[key]
        if "ord_type" not in raw and raw.get("order_type") not in (None, ""):
            raw["ord_type"] = raw["order_type"]
        if row.get("order_id") not in (None, ""):
            raw["order_id"] = row["order_id"]
        if "client_order_id" not in raw and fallback_client_order_id:
            raw["client_order_id"] = fallback_client_order_id
        if "uuid" not in raw and fallback_exchange_order_id:
            raw["uuid"] = fallback_exchange_order_id
        return raw

    def _order_from_v2_row(
        self,
        row: dict[str, object],
        *,
        client_order_id: str = "",
        exchange_order_id: str = "",
    ) -> BrokerOrder:
        normalized = self._normalize_v2_order_row(row)
        resolved_client_order_id, resolved_exchange_order_id = self._resolve_order_identifiers(
            row,
            fallback_client_order_id=client_order_id,
            fallback_exchange_order_id=exchange_order_id or str(normalized["uuid"]),
            allow_coid_alias=True,
            context="/v2/orders",
        )
        state = str(normalized["state"])
        qty_req = float(normalized["volume"])
        qty_filled = float(normalized["executed_volume"])
        if state in {"wait", "watch"}:
            status = "PARTIAL" if qty_filled > 0 else "NEW"
        elif state == "done":
            status = "FILLED"
        elif state == "cancel":
            status = "FILLED" if qty_req > 0 and qty_filled >= qty_req else "CANCELED"
        else:
            status = "PARTIAL" if qty_filled > 0 and qty_filled < qty_req else "NEW"
        return BrokerOrder(
            resolved_client_order_id,
            resolved_exchange_order_id,
            str(normalized["side"]),
            status,
            float(normalized["price"]) if normalized["price"] is not None else None,
            qty_req,
            qty_filled,
            int(normalized["created_ts"]),
            int(normalized["updated_ts"]),
            self._raw_v2_order_fields(
                row,
                fallback_client_order_id=resolved_client_order_id,
            ),
        )

    @staticmethod
    def _raw_v1_order_fields(row: dict[str, object]) -> dict[str, object]:
        raw: dict[str, object] = {}
        for key in (
            "market",
            "ord_type",
            "uuid",
            "client_order_id",
            "state",
            "side",
            "price",
            "volume",
            "remaining_volume",
            "executed_volume",
            "executed_funds",
            "paid_fee",
            "reserved_fee",
            "remaining_fee",
            "fee",
            "trade_fee",
            "locked",
            "created_at",
            "updated_at",
            "trades_count",
        ):
            if row.get(key) not in (None, ""):
                raw[key] = row[key]
        if isinstance(row.get("trades"), list):
            raw["trades"] = row["trades"]
        return raw

    @staticmethod
    def normalize_myorder_event(payload: dict[str, object]) -> NormalizedMyOrderEvent:
        return normalize_myorder_event_payload(payload)

    @staticmethod
    def ingest_myorder_event_runtime(
        conn,
        *,
        payload: dict[str, object],
        strategy_name: str | None = None,
    ) -> MyOrderIngestResult:
        return ingest_myorder_event(conn, payload=payload, strategy_name=strategy_name)

    @staticmethod
    def _v1_list_quantities(normalized) -> tuple[float, float]:
        qty_filled = float(normalized.executed_volume or 0.0)
        if normalized.volume is not None:
            qty_req = float(normalized.volume)
        elif normalized.remaining_volume is not None:
            qty_req = max(0.0, float(normalized.remaining_volume) + qty_filled)
        elif normalized.state == "done":
            qty_req = qty_filled
        else:
            raise BrokerRejectError(
                "/v1/orders schema mismatch: missing required numeric fields for quantity reconciliation"
            )
        return qty_req, qty_filled

    def get_order_chance(self, *, market: str | None = None) -> dict[str, object]:
        try:
            requested_market = parse_documented_market_code(market or self._market())
        except ExchangeMarketCodeError as exc:
            raise BrokerRejectError(
                "/v1/orders/chance request market must use canonical QUOTE-BASE "
                f"(e.g., KRW-BTC), got {market!r}"
            ) from exc
        response = self._get_private(
            "/v1/orders/chance",
            {"market": requested_market},
            retry_safe=True,
        )
        if not isinstance(response, dict):
            raise BrokerRejectError(f"unexpected /v1/orders/chance payload type: {type(response).__name__}")
        self._journal_read_summary(path="/v1/orders/chance", data=response)
        return response

    def place_order(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None = None,
        buy_price_none_submit_contract: "BuyPriceNoneSubmitContract | None" = None,
        submit_plan=None,
    ) -> BrokerOrder:
        now = int(time.time() * 1000)
        validated_client_order_id = validate_client_order_id(client_order_id)
        if self.dry_run:
            return BrokerOrder(validated_client_order_id, f"dry_{validated_client_order_id}", side, "NEW", price, qty, 0.0, now, now)

        submit_contract_context: dict[str, object] = {}
        try:
            if submit_plan is None:
                if settings.MODE == "live":
                    raise BrokerRejectError("live broker submit requires explicit submit_plan before dispatch")
                submit_plan = self._plan_place_order_for_compat_dispatch(
                    client_order_id=validated_client_order_id,
                    side=side,
                    qty=float(qty),
                    price=price,
                    now=now,
                    buy_price_none_submit_contract=buy_price_none_submit_contract,
                )
            if validated_client_order_id != submit_plan.intent.client_order_id:
                raise BrokerRejectError(
                    "submit_plan client_order_id mismatch: "
                    f"requested={validated_client_order_id} planned={submit_plan.intent.client_order_id}"
                )
            if str(side).strip().upper() != str(submit_plan.intent.side).strip().upper():
                raise BrokerRejectError(
                    "submit_plan side mismatch: "
                    f"requested={side} planned={submit_plan.intent.side}"
                )
            if not math.isclose(float(qty), float(submit_plan.intent.qty), rel_tol=0.0, abs_tol=1e-12):
                raise BrokerRejectError(
                    "submit_plan qty mismatch: "
                    f"requested={float(qty):.12f} planned={float(submit_plan.intent.qty):.12f}"
                )
            if price != submit_plan.intent.price:
                raise BrokerRejectError(
                    "submit_plan price mismatch: "
                    f"requested={price} planned={submit_plan.intent.price}"
                )
            flow = build_place_order_submission_flow(
                self,
                plan=submit_plan,
            )
            submit_contract_context = dict(flow.signed_request.submit_contract_context)
            return run_place_order_submission_flow(self, flow=flow)
        except BrokerRejectError as exc:
            exc_submit_contract_context = getattr(exc, "submit_contract_context", None)
            setattr(
                exc,
                "submit_contract_context",
                dict(exc_submit_contract_context)
                if isinstance(exc_submit_contract_context, dict)
                else dict(submit_contract_context),
            )
            raise

    def _plan_place_order_for_compat_dispatch(
        self,
        *,
        client_order_id: str,
        side: str,
        qty: float,
        price: float | None,
        now: int,
        buy_price_none_submit_contract: "BuyPriceNoneSubmitContract | None",
    ):
        normalized_side = normalize_order_side(side)
        if (
            price is None
            and normalized_side == "bid"
            and buy_price_none_submit_contract is not None
        ):
            from . import order_rules as order_rules_module

            resolved_rules = order_rules_module.get_effective_order_rules(self._market()).rules
            buy_price_none_resolution = order_rules_module.resolve_buy_price_none_resolution(
                rules=resolved_rules,
            )
            contract_context = buy_price_none_submit_contract.as_context()
            supported_order_types = ",".join(
                str(item)
                for item in buy_price_none_submit_contract.chance_supported_order_types
            ) or "-"
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SUBMIT] buy submit contract preflight",
                    chance_validation_order_type=buy_price_none_submit_contract.chance_validation_order_type,
                    supported_order_types=supported_order_types,
                    buy_price_none_allowed=1 if bool(contract_context.get("buy_price_none_allowed")) else 0,
                    buy_price_none_alias_used=1 if bool(contract_context.get("buy_price_none_alias_used")) else 0,
                    buy_price_none_block_reason=str(contract_context.get("buy_price_none_block_reason") or "-"),
                    submit_field=str(contract_context.get("exchange_submit_field") or "-"),
                )
            )
            order_rules_module.validate_order_chance_support(
                rules=resolved_rules,
                side="BUY",
                order_type=buy_price_none_submit_contract.chance_validation_order_type,
                buy_price_none_resolution=buy_price_none_resolution,
            )
        return plan_place_order(
            self,
            intent=OrderIntent(
                client_order_id=client_order_id,
                market=self._market(),
                side=side,
                normalized_side=normalized_side,
                qty=float(qty),
                price=price,
                created_ts=now,
                submit_contract=buy_price_none_submit_contract,
                trace_id=client_order_id,
            ),
        )

    def request_cancel_order(
        self,
        *,
        client_order_id: str,
        order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> BrokerOrder:
        requested_client_order_id_raw = str(client_order_id or "").strip()
        requested_client_order_id = (
            validate_client_order_id(requested_client_order_id_raw)
            if requested_client_order_id_raw
            else ""
        )
        requested_order_id_raw = str(order_id or "").strip()
        requested_exchange_order_id_raw = str(exchange_order_id or "").strip()
        requested_order_id = requested_order_id_raw or requested_exchange_order_id_raw
        requested_exchange_order_id = requested_order_id_raw or requested_exchange_order_id_raw
        now = int(time.time() * 1000)
        if self.dry_run:
            return BrokerOrder(
                requested_client_order_id,
                requested_exchange_order_id or "",
                "BUY",
                CANCEL_REQUESTED_STATUS,
                None,
                0.0,
                0.0,
                now,
                now,
            )

        cancel_payload = build_cancel_order_params(order_id=requested_order_id, client_order_id=requested_client_order_id or None)

        response = self._delete_private("/v2/order", cancel_payload, retry_safe=False)
        if not isinstance(response, dict):
            raise BrokerRejectError(f"unexpected /v2/order payload type: {type(response).__name__}")
        response_row = response.get("data") if isinstance(response.get("data"), dict) else response
        resolved_client_order_id, resolved_exchange_order_id = self._resolve_order_identifiers(
            response_row if isinstance(response_row, dict) else {},
            fallback_client_order_id=str(response.get("client_order_id") or ""),
            fallback_exchange_order_id=str(response.get("order_id") or response.get("uuid") or ""),
            allow_coid_alias=True,
            context="/v2/order response",
        )
        if requested_exchange_order_id and resolved_exchange_order_id and resolved_exchange_order_id != requested_exchange_order_id:
            raise BrokerRejectError(
                f"cancel response order_id mismatch: requested={requested_exchange_order_id} response={resolved_exchange_order_id}"
            )
        if requested_client_order_id and resolved_client_order_id and resolved_client_order_id != requested_client_order_id:
            raise BrokerRejectError(
                f"cancel response client_order_id mismatch: requested={requested_client_order_id} response={resolved_client_order_id}"
            )
        if requested_client_order_id and not resolved_client_order_id:
            resolved_client_order_id = requested_client_order_id
        if requested_exchange_order_id and not resolved_exchange_order_id:
            resolved_exchange_order_id = requested_exchange_order_id
        raw = self._raw_v2_order_fields(
            response_row if isinstance(response_row, dict) else {},
            fallback_client_order_id=resolved_client_order_id or requested_client_order_id,
            fallback_exchange_order_id=resolved_exchange_order_id or requested_exchange_order_id,
        )
        raw.setdefault("market", response.get("market"))
        raw.setdefault("state", "cancel")
        raw.setdefault("order_type", response.get("order_type"))
        raw.setdefault("ord_type", response.get("order_type"))
        return BrokerOrder(
            resolved_client_order_id or requested_client_order_id,
            resolved_exchange_order_id or requested_exchange_order_id,
            str(response.get("side") or "BUY"),
            CANCEL_REQUESTED_STATUS,
            None,
            float(response.get("volume") or 0.0),
            float(response.get("executed_volume") or 0.0),
            now,
            now,
            raw,
        )

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        requested_client_order_id_raw = str(client_order_id or "").strip()
        requested_client_order_id = (
            validate_client_order_id(requested_client_order_id_raw)
            if requested_client_order_id_raw
            else ""
        )
        requested_exchange_order_id = str(exchange_order_id or "").strip()
        now = int(time.time() * 1000)
        if self.dry_run:
            return BrokerOrder(
                requested_client_order_id,
                requested_exchange_order_id or "",
                "BUY",
                "CANCELED",
                None,
                0.0,
                0.0,
                now,
                now,
            )
        max_attempts = max(1, int(getattr(settings, "BITHUMB_CANCEL_RETRY_ATTEMPTS", 3)))
        backoffs = (
            float(getattr(settings, "BITHUMB_CANCEL_RETRY_BACKOFF_SEC", 0.15)),
            float(getattr(settings, "BITHUMB_CANCEL_RETRY_BACKOFF_SEC_2", 0.3)),
        )

        for attempt in range(max_attempts):
            try:
                cancel_requested = self.request_cancel_order(
                    client_order_id=requested_client_order_id,
                    exchange_order_id=requested_exchange_order_id or None,
                )
            except BrokerTemporaryError as exc:
                code, summary = classify_private_api_error(exc)
                if code not in {"RATE_LIMITED", "ORDER_NOT_READY", "TEMPORARY", "SERVER_INTERNAL_FAILURE"} or attempt >= max_attempts - 1:
                    raise BrokerTemporaryError(
                        f"cancel retry exhausted category={code} summary={summary}: {exc}"
                    ) from exc
                RUN_LOG.warning(
                    format_log_kv(
                        "[CANCEL_RETRY_DEFERRED]",
                        client_order_id=requested_client_order_id,
                        exchange_order_id=requested_exchange_order_id or "-",
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        category=code,
                        summary=summary,
                    )
                )
                time.sleep(backoffs[min(attempt, len(backoffs) - 1)])
                try:
                    current = self.get_order(
                        client_order_id=requested_client_order_id,
                        exchange_order_id=requested_exchange_order_id or None,
                    )
                except Exception:
                    current = None
                if current is not None and current.status in {"CANCELED", "FILLED"}:
                    return current
                continue
            except BrokerRejectError as exc:
                category, description = _classify_cancel_reject(exc)
                if category == "ALREADY_CANCELED":
                    return BrokerOrder(
                        requested_client_order_id,
                        requested_exchange_order_id,
                        "BUY",
                        "CANCELED",
                        None,
                        0.0,
                        0.0,
                        now,
                        now,
                    )
                if category == "ALREADY_FILLED":
                    return BrokerOrder(
                        requested_client_order_id,
                        requested_exchange_order_id,
                        "BUY",
                        "FILLED",
                        None,
                        0.0,
                        0.0,
                        now,
                        now,
                    )
                if category == "ORDER_NOT_READY":
                    if attempt >= max_attempts - 1:
                        raise BrokerTemporaryError(
                            f"cancel retry exhausted category=ORDER_NOT_READY summary={description}: {exc}"
                        ) from exc
                    RUN_LOG.warning(
                        format_log_kv(
                            "[CANCEL_RETRY_DEFERRED]",
                            client_order_id=requested_client_order_id,
                            exchange_order_id=requested_exchange_order_id or "-",
                            attempt=attempt + 1,
                            max_attempts=max_attempts,
                            category=category,
                            summary=description,
                        )
                    )
                    time.sleep(backoffs[min(attempt, len(backoffs) - 1)])
                    try:
                        current = self.get_order(
                            client_order_id=requested_client_order_id,
                            exchange_order_id=requested_exchange_order_id or None,
                        )
                    except Exception:
                        current = None
                    if current is not None and current.status in {"CANCELED", "FILLED"}:
                        return current
                    continue
                if category == "NOT_FOUND":
                    try:
                        current = self.get_order(
                            client_order_id=requested_client_order_id or None,
                            exchange_order_id=requested_exchange_order_id or None,
                        )
                    except Exception as lookup_exc:
                        raise BrokerRejectError(
                            f"cancel rejected category=NOT_FOUND_NEEDS_RECONCILE description={description}: {exc}; "
                            f"lookup_failed={type(lookup_exc).__name__}: {lookup_exc}"
                        ) from exc
                    if current.status in {"CANCELED", "FILLED"}:
                        return current
                    raise BrokerRejectError(
                        f"cancel rejected category=NOT_FOUND_NEEDS_RECONCILE description={description}: "
                        f"lookup_status={current.status}"
                    ) from exc
                raise BrokerRejectError(f"cancel rejected category={category} description={description}: {exc}") from exc
            cancel_status = str(cancel_requested.status or CANCEL_REQUESTED_STATUS).strip() or CANCEL_REQUESTED_STATUS
            if cancel_status == CANCEL_REQUESTED_STATUS:
                try:
                    current = self.get_order(
                        client_order_id=requested_client_order_id or None,
                        exchange_order_id=requested_exchange_order_id or None,
                    )
                    if current.status in {"CANCELED", "FILLED"}:
                        return current
                    return BrokerOrder(
                        current.client_order_id,
                        current.exchange_order_id,
                        current.side,
                        CANCEL_REQUESTED_STATUS,
                        current.price,
                        current.qty_req,
                        current.qty_filled,
                        current.created_ts,
                        current.updated_ts,
                        current.raw,
                    )
                except Exception:
                    return cancel_requested
            return cancel_requested

        raise BrokerTemporaryError(
            f"cancel retry exhausted category=ORDER_NOT_READY summary=cancel request remained not ready after {max_attempts} attempts"
        )

    def get_order(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> BrokerOrder:
        return load_bithumb_order(
            self,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            classify_private_api_error=classify_private_api_error,
        )

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return load_bithumb_open_orders(
            self,
            exchange_order_ids=exchange_order_ids,
            client_order_ids=client_order_ids,
        )

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return load_bithumb_fills(
            self,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            classify_private_api_error=classify_private_api_error,
        )

    def get_balance(self) -> BrokerBalance:
        """Return broker balance via configured snapshot source abstraction."""
        return self._get_balance_source().fetch_snapshot().balance

    def get_balance_snapshot(self) -> BalanceSnapshot:
        return self._get_balance_source().fetch_snapshot()

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return load_bithumb_recent_orders(
            self,
            limit=limit,
            exchange_order_ids=exchange_order_ids,
            client_order_ids=client_order_ids,
        )

    def get_recent_orders_for_recovery(
        self,
        *,
        limit: int = 100,
        market: str | None = None,
        page_size: int | None = None,
    ) -> list[BrokerOrder]:
        if self.dry_run:
            return []

        lim = max(0, int(limit))
        if lim == 0:
            return []

        requested_market = parse_documented_market_code(market or self._market())
        return load_bithumb_recent_orders_for_recovery(
            self,
            limit=lim,
            market=requested_market,
            page_size=page_size,
        )

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        raise BrokerRejectError(
            "recent fill broad scan is unsupported: Bithumb MyOrder contract requires uuid/client_order_id lookups"
        )
