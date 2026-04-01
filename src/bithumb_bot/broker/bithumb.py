from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import math
from urllib.parse import urlencode
from typing import TypedDict

import httpx

from ..config import settings
from ..marketdata import fetch_orderbook_top, validated_best_quote_ask_price
from ..markets import canonical_market_id
from ..observability import format_log_kv
from .base import BrokerBalance, BrokerFill, BrokerOrder, BrokerRejectError, BrokerTemporaryError
from .accounts_v1 import parse_accounts_response, select_pair_balances, to_broker_balance
from .order_lookup_v1 import (
    V1NormalizedOrder,
    build_lookup_params as build_v1_order_lookup_params,
    require_known_state as require_v1_known_state,
    resolve_identifiers as resolve_v1_order_identifiers,
    status_from_state as v1_status_from_state,
)
from .order_list_v1 import build_order_list_params, parse_v1_order_list_row
from .order_payloads import build_order_payload, normalize_order_side, validate_client_order_id

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
    ORDER_SUBMIT_CONTENT_TYPE = "application/json"

    def __init__(self, *, api_key: str, api_secret: str, base_url: str, dry_run: bool) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.dry_run = dry_run

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

    def _jwt_token_from_claims(self, claims: dict[str, object]) -> str:
        token = _jwt.encode(claims, self.api_secret, algorithm="HS256")
        return token if isinstance(token, str) else token.decode()

    def _base_jwt_claims(
        self,
        *,
        nonce: str | None = None,
        timestamp: int | None = None,
    ) -> dict[str, object]:
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
        if self.dry_run:
            return {}
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
        if self.dry_run:
            return {}

        attempts = 3 if retry_safe else 1
        backoffs = (0.2, 0.5)
        method = method.upper()
        is_order_submit = method == "POST" and endpoint == self.ORDER_SUBMIT_ENDPOINT and bool(json_body)
        auth_payload = params if method in {"GET", "DELETE"} else json_body
        debug_order_submit = is_order_submit
        request_kwargs: dict[str, object] = {}
        canonical_payload = self._query_string(auth_payload) if auth_payload else ""
        signed_payload = canonical_payload if debug_order_submit else ""
        signed_payload_repr = repr(signed_payload) if debug_order_submit else ""
        transmitted_payload_repr = ""
        request_content_type: str | None = None
        fixed_nonce: str | None = None
        fixed_timestamp: int | None = None
        if params:
            request_kwargs["params"] = params
        if json_body:
            if is_order_submit:
                fixed_nonce = str(uuid.uuid4())
                fixed_timestamp = round(time.time() * 1000)
                order_context = self._order_submit_auth_context(
                    json_body,
                    nonce=fixed_nonce,
                    timestamp=fixed_timestamp,
                )
                request_content_type = order_context["headers"].get("Content-Type")
                request_kwargs.update(order_context["request_kwargs"])
                canonical_payload = order_context["canonical_payload"]
                transmitted_payload_repr = repr(order_context["request_body_text"]) if debug_order_submit else ""
            else:
                request_content_type = "application/json"
                if debug_order_submit:
                    transmitted_payload_repr = repr(self._json_body_text(json_body))
                request_kwargs["json"] = json_body

        for attempt in range(attempts):
            if is_order_submit:
                order_context = self._order_submit_auth_context(
                    json_body or {},
                    nonce=fixed_nonce,
                    timestamp=fixed_timestamp,
                )
                headers = order_context["headers"]
                canonical_payload = order_context["canonical_payload"]
            else:
                headers = self._headers(
                    auth_payload,
                    content_type=request_content_type,
                    canonical_payload=None,
                    nonce=fixed_nonce,
                    timestamp=fixed_timestamp,
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
                    res = client.request(method, endpoint, headers=headers, **request_kwargs)

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
                    time.sleep(backoffs[attempt])
                    continue
                raise BrokerTemporaryError(
                    f"bithumb private {endpoint} transport error: {type(exc).__name__}: {exc}"
                ) from exc
            except httpx.HTTPStatusError as exc:
                if 500 <= exc.response.status_code <= 599:
                    if attempt < attempts - 1:
                        time.sleep(backoffs[attempt])
                        continue
                    body = response_excerpt(exc.response) if response_excerpt else ""
                    raise BrokerTemporaryError(
                        f"bithumb private {endpoint} server error status={exc.response.status_code} body={body}"
                    ) from exc
                body = response_excerpt(exc.response) if response_excerpt else ""
                raise BrokerRejectError(
                    f"bithumb private {endpoint} rejected with http status={exc.response.status_code} body={body}"
                ) from exc

            if isinstance(data, dict) and data.get("status") not in (None, "0000"):
                raise BrokerRejectError(f"bithumb private call rejected: {data}")
            return data

        raise BrokerTemporaryError(f"bithumb private {endpoint} failed after retries")


def _classify_cancel_reject(exc: BrokerRejectError) -> tuple[str, str]:
    detail = str(exc).lower()
    if any(token in detail for token in ("already canceled", "already cancelled", "already cancel", "state=cancel", "취소")):
        return "ALREADY_CANCELED", "order already canceled"
    if any(token in detail for token in ("already filled", "already executed", "fully executed", "state=done", "체결")):
        return "ALREADY_FILLED", "order already filled"
    if any(token in detail for token in ("not found", "no such order", "unknown order", "주문이 존재하지")):
        return "NOT_FOUND", "order not found"
    if any(token in detail for token in ("cannot cancel", "not cancelable", "접수 중", "pending")):
        return "PENDING_NOT_CANCELABLE", "order not cancelable in current state"
    return "REJECTED", "unclassified cancel rejection"


def classify_private_api_error(exc: Exception) -> tuple[str, str]:
    detail = str(exc).lower()
    if isinstance(exc, BrokerTemporaryError) or any(
        token in detail for token in ("transport error", "server error", "timeout", "timed out", "temporar")
    ):
        return "TEMPORARY", "temporary network/server error; retry with backoff"
    if "schema mismatch" in detail:
        return "DOC_SCHEMA", "documented response schema mismatch (/v1/orders or /v1/order)"
    if "status=401" in detail or "unauthorized" in detail or "invalid jwt" in detail or "signature" in detail:
        return "AUTH_SIGN", "authentication/signature failed (401 / invalid JWT / key-secret mismatch)"
    if "status=403" in detail or "out_of_scope" in detail or "permission" in detail:
        return "PERMISSION", "API key scope/permission denied"
    if any(token in detail for token in ("broad /v1/orders", "requires identifiers", "fallback is disabled")):
        return "RECOVERY_REQUIRED", "startup/recovery path requires identifier-based lookup only"
    if any(token in detail for token in ("insufficient", "under_min_total", "too_many_orders", "balance")):
        return "FUNDS", "balance or orderable-funds check failed"
    if any(token in detail for token in ("market", "price", "volume", "ord_type", "validation")):
        return "PARAM", "market/order parameter validation failed"
    return "UNRECOVERABLE", "unclassified private API failure; operator investigation required"


class BithumbBroker:
    def __init__(self) -> None:
        self.api_key = settings.BITHUMB_API_KEY
        self.api_secret = settings.BITHUMB_API_SECRET
        self.base_url = settings.BITHUMB_API_BASE
        self.dry_run = settings.LIVE_DRY_RUN
        self._read_journal: dict[str, str] = {}
        self._accounts_validation_diag: dict[str, object] = {
            "reason": "not_checked",
            "row_count": 0,
            "currencies": [],
            "missing_required_currencies": [],
            "duplicate_currencies": [],
            "last_success_reason": None,
            "last_failure_reason": None,
        }
        self._private_api = BithumbPrivateAPI(
            api_key=self.api_key,
            api_secret=self.api_secret,
            base_url=self.base_url,
            dry_run=self.dry_run,
        )

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
        token = str(side or "").strip().lower()
        if token in {"buy", "bid"}:
            return "BUY"
        if token in {"sell", "ask"}:
            return "SELL"
        return default

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
    ) -> tuple[str, str]:
        exchange_order_id = ""
        for key in ("uuid", "order_id"):
            candidate = self._clean_identifier(row.get(key))
            if candidate:
                exchange_order_id = candidate
                break
        if not exchange_order_id:
            exchange_order_id = self._clean_identifier(fallback_exchange_order_id)

        client_order_id = ""
        for key in ("client_order_id",):
            candidate = self._clean_identifier(row.get(key))
            if candidate:
                client_order_id = candidate
                break
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

    def get_accounts_validation_diagnostics(self) -> dict[str, object]:
        return dict(self._accounts_validation_diag)

    def _request_private(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, object] | None = None,
        json_body: dict[str, object] | None = None,
        retry_safe: bool = False,
    ) -> dict | list:
        return self._private_api.request(
            method,
            endpoint,
            params=params,
            json_body=json_body,
            retry_safe=retry_safe,
            response_excerpt=self._response_body_excerpt,
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
    def _decimal_from_value(value: object) -> Decimal:
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise BrokerRejectError(f"invalid numeric value for order serialization: {value}") from exc
        if not decimal_value.is_finite():
            raise BrokerRejectError(f"invalid non-finite numeric value for order serialization: {value}")
        return decimal_value

    @classmethod
    def _format_krw_amount(cls, value: object) -> str:
        amount = cls._decimal_from_value(value)
        if amount <= 0:
            return "0"
        rounded = amount.quantize(Decimal("1"), rounding=ROUND_DOWN)
        return format(rounded, "f").split(".", 1)[0]

    @classmethod
    def _format_volume(cls, qty: object, *, places: int = 8) -> str:
        quantizer = Decimal("1").scaleb(-places)
        volume = cls._decimal_from_value(qty)
        if volume <= 0:
            return "0"
        rounded = volume.quantize(quantizer, rounding=ROUND_DOWN)
        return format(rounded, "f").rstrip("0").rstrip(".") or "0"

    @staticmethod
    def _parse_ts(raw: object) -> int:
        if raw in (None, ""):
            return int(time.time() * 1000)
        try:
            value = float(raw)
        except (TypeError, ValueError):
            text = str(raw).strip()
            try:
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                dt = datetime.fromisoformat(text)
            except ValueError:
                return int(time.time() * 1000)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        if value > 1_000_000_000_000:
            return int(value)
        if value > 1_000_000_000:
            return int(value * 1000)
        return int(value * 1000)

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
        response = self._get_private("/v1/accounts", {}, retry_safe=True)
        self._journal_read_summary(path="/v1/accounts", data=response)
        return response

    @staticmethod
    def _classify_accounts_validation_reason(exc: Exception) -> str:
        detail = str(exc).lower()
        if "duplicate currency row" in detail:
            return "duplicate currency"
        if "missing quote currency row" in detail or "missing base currency row" in detail:
            return "required currency missing"
        return "schema mismatch"

    @staticmethod
    def _strict_parse_ts(raw: object, *, field_name: str, context: str) -> int:
        if raw in (None, ""):
            raise BrokerRejectError(f"{context} schema mismatch: missing required timestamp field '{field_name}'")
        try:
            value = float(raw)
        except (TypeError, ValueError):
            text = str(raw).strip()
            try:
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                dt = datetime.fromisoformat(text)
            except ValueError as exc:
                raise BrokerRejectError(
                    f"{context} schema mismatch: invalid timestamp field '{field_name}'={raw}"
                ) from exc
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        if not math.isfinite(value):
            raise BrokerRejectError(f"{context} schema mismatch: non-finite timestamp field '{field_name}'={raw}")
        if value > 1_000_000_000_000:
            return int(value)
        return int(value * 1000)

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
        fee_keys = ("fee", "paid_fee", "commission", "trade_fee", "transaction_fee", "fee_amount")
        present_keys = [key for key in fee_keys if key in row]
        log_payload = self._sanitize_debug_value(
            {
                "context": context,
                "fill_hint_id": row.get("uuid") or row.get("id") or row.get("order_id"),
                "present_fee_keys": present_keys,
                "fee_values": {key: row.get(key) for key in present_keys},
            }
        )
        if not present_keys:
            if strict:
                raise BrokerRejectError(f"/v1/order.{context} schema mismatch: missing required fee field")
            RUN_LOG.warning(format_log_kv("[FILL_FEE] missing fee key", payload=log_payload))
            return 0.0

        for key in present_keys:
            raw = row.get(key)
            parsed = self._to_float(raw, default=None)
            if parsed is None:
                if strict:
                    raise BrokerRejectError(f"/v1/order.{context} schema mismatch: invalid fee field '{key}'={raw}")
                if raw in (None, "") or (isinstance(raw, str) and raw.strip().lower() in {"null", "none"}):
                    RUN_LOG.warning(format_log_kv("[FILL_FEE] empty fee value", payload=log_payload, fee_key=key))
                else:
                    RUN_LOG.warning(format_log_kv("[FILL_FEE] invalid fee value", payload=log_payload, fee_key=key))
                continue
            fee = float(parsed)
            if fee < 0:
                raise BrokerRejectError(f"/v1/order.{context} schema mismatch: negative fee field '{key}'={raw}")
            if fee == 0.0 and (qty or 0.0) > 0 and price is not None and price > 0:
                RUN_LOG.warning(format_log_kv("[FILL_FEE] resolved zero fee", payload=log_payload, fee_key=key))
            return fee

        if strict:
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
        """Lenient normalization for /v2/orders response rows.

        /v2 orders submit/cancel responses are not part of the strict /v1 read-schema
        contract and may omit fields. This helper is intentionally tolerant and is
        scoped to v2 response shaping only.
        """
        volume = self._number(row, "volume", "units")
        remaining = self._number(row, "remaining_volume", "units_remaining")
        executed = self._number(row, "executed_volume")
        if executed <= 0 and volume > 0 and remaining >= 0:
            executed = max(0.0, volume - remaining)
        return {
            "uuid": str(row.get("uuid") or row.get("order_id") or ""),
            "side": self._normalize_order_side(str(row.get("side") or row.get("type")), default="BUY"),
            "state": str(row.get("state") or ""),
            "price": self._resolve_fill_price(row),
            "volume": volume,
            "remaining_volume": remaining,
            "executed_volume": executed,
            "created_ts": self._parse_ts(row.get("created_at") or row.get("timestamp")),
            "updated_ts": self._parse_ts(row.get("updated_at") or row.get("created_at") or row.get("timestamp")),
            "trades": row.get("trades") if isinstance(row.get("trades"), list) else [],
        }

    def _normalize_v1_order_row_lenient_for_fills(self, row: dict[str, object]) -> dict[str, object]:
        """Compatibility fallback for /v1/order fill aggregation.

        Strict parsing is always preferred for /v1/order and /v1/orders.
        This lenient path exists only for legacy cases where `/v1/order` omits
        trade-level fills but still exposes aggregate executed fields.
        """
        volume = self._number(row, "volume")
        remaining = self._number(row, "remaining_volume")
        executed = self._number(row, "executed_volume")
        if executed <= 0 and volume > 0 and remaining >= 0:
            executed = max(0.0, volume - remaining)
        return {
            "uuid": str(row.get("uuid") or ""),
            "side": self._normalize_order_side(str(row.get("side")), default="BUY"),
            "state": str(row.get("state") or ""),
            "price": self._resolve_fill_price(row),
            "volume": volume,
            "remaining_volume": remaining,
            "executed_volume": executed,
            "created_ts": self._parse_ts(row.get("created_at")),
            "updated_ts": self._parse_ts(row.get("updated_at") or row.get("created_at")),
            "trades": row.get("trades") if isinstance(row.get("trades"), list) else [],
        }

    def _normalize_v1_order_row_strict(self, row: dict[str, object]) -> V1NormalizedOrder:
        context = "/v1/order"
        volume = self._required_number(row, "volume", context=context)
        remaining = self._required_number(row, "remaining_volume", context=context)
        executed = self._strict_optional_number(row, "executed_volume", context=context)
        if executed is None:
            executed = max(0.0, volume - remaining)

        price = self._strict_optional_number(row, "price", context=context)
        state = require_v1_known_state(row.get("state"), context=context)

        raw_trades = row.get("trades")
        if raw_trades is None:
            trades: list[object] = []
        elif isinstance(raw_trades, list):
            trades = raw_trades
        else:
            raise BrokerRejectError(f"{context} schema mismatch: trades must be a list when present")

        created_ts = self._strict_parse_ts(row.get("created_at"), field_name="created_at", context=context)
        updated_raw = row.get("updated_at")
        updated_ts = (
            self._strict_parse_ts(updated_raw, field_name="updated_at", context=context)
            if updated_raw not in (None, "")
            else created_ts
        )
        return V1NormalizedOrder(
            side=self._normalize_order_side(str(row.get("side")), default="BUY"),
            state=state,
            price=price,
            volume=volume,
            remaining_volume=remaining,
            executed_volume=executed,
            created_ts=created_ts,
            updated_ts=updated_ts,
            trades=trades,
            executed_funds=self._strict_optional_number(row, "executed_funds", context=context),
        )

    @staticmethod
    def _raw_v2_order_fields(
        row: dict[str, object],
        *,
        fallback_client_order_id: str | None = None,
        fallback_exchange_order_id: str | None = None,
    ) -> dict[str, object]:
        raw: dict[str, object] = {}
        for key in ("market", "ord_type", "client_order_id"):
            if row.get(key) not in (None, ""):
                raw[key] = row[key]
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

    def get_order_chance(self, *, market: str | None = None) -> dict[str, object]:
        response = self._get_private(
            "/v1/orders/chance",
            {"market": market or self._market()},
            retry_safe=True,
        )
        if not isinstance(response, dict):
            raise BrokerRejectError(f"unexpected /v1/orders/chance payload type: {type(response).__name__}")
        self._journal_read_summary(path="/v1/orders/chance", data=response)
        return response

    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        now = int(time.time() * 1000)
        validated_client_order_id = validate_client_order_id(client_order_id)
        if self.dry_run:
            return BrokerOrder(validated_client_order_id, f"dry_{validated_client_order_id}", side, "NEW", price, qty, 0.0, now, now)

        normalized_side = normalize_order_side(side)
        payload: dict[str, object]
        market = self._market()
        from .order_rules import (
            get_effective_order_rules,
            normalize_limit_price_for_side,
            side_min_total_krw,
            side_price_unit,
        )

        rules = get_effective_order_rules(market).rules
        order_side = "BUY" if normalized_side == "bid" else "SELL"
        volume_text = self._format_volume(qty)
        if price is None:
            if normalized_side == "bid":
                try:
                    quote = fetch_orderbook_top(market)
                    ask = validated_best_quote_ask_price(quote, requested_market=market)
                except Exception as exc:
                    raise BrokerTemporaryError(
                        "market buy blocked: failed to load validated best ask "
                        f"market={market} client_order_id={validated_client_order_id} cause={type(exc).__name__}: {exc}"
                    ) from exc
                notional = self._decimal_from_value(ask) * self._decimal_from_value(qty)
                min_total = side_min_total_krw(rules=rules, side=order_side)
                if min_total > 0 and notional < self._decimal_from_value(min_total):
                    raise BrokerRejectError(
                        "order notional below side minimum for market BUY: "
                        f"side={order_side} notional={format(notional, 'f')} min_total={min_total:.8f}"
                    )
                payload = build_order_payload(
                    market=market,
                    side=normalized_side,
                    ord_type="price",
                    price=self._format_krw_amount(notional),
                    client_order_id=validated_client_order_id,
                )
            else:
                payload = build_order_payload(
                    market=market,
                    side=normalized_side,
                    ord_type="market",
                    volume=volume_text,
                    client_order_id=validated_client_order_id,
                )
        else:
            requested_limit_price = self._decimal_from_value(price)
            if requested_limit_price <= 0:
                raise BrokerRejectError(f"limit price must be > 0 (got {price})")

            price_unit = side_price_unit(rules=rules, side=order_side)
            normalized_limit_price = self._decimal_from_value(
                normalize_limit_price_for_side(price=float(requested_limit_price), side=order_side, rules=rules)
            )
            if price_unit > 0 and normalized_limit_price != requested_limit_price:
                raise BrokerRejectError(
                    "limit price does not match side price_unit; explicit correction required: "
                    f"side={order_side} requested={format(requested_limit_price, 'f')} "
                    f"price_unit={price_unit:.8f} suggested={format(normalized_limit_price, 'f')}"
                )

            notional = requested_limit_price * self._decimal_from_value(qty)
            min_total = side_min_total_krw(rules=rules, side=order_side)
            if min_total > 0 and notional < self._decimal_from_value(min_total):
                raise BrokerRejectError(
                    "order notional below side minimum for limit order: "
                    f"side={order_side} notional={format(notional, 'f')} min_total={min_total:.8f}"
                )
            payload = build_order_payload(
                market=market,
                side=normalized_side,
                ord_type="limit",
                volume=volume_text,
                price=self._format_krw_amount(requested_limit_price),
                client_order_id=validated_client_order_id,
            )

        RUN_LOG.info(
            format_log_kv(
                "[ORDER_SUBMIT] broker payload",
                market=payload.get("market"),
                side=normalized_side,
                ord_type=payload.get("ord_type"),
                volume=payload.get("volume"),
                price=payload.get("price"),
                payload=payload,
                client_order_id=validated_client_order_id,
            )
        )

        data = self._post_private("/v2/orders", payload, retry_safe=False)
        if not isinstance(data, dict):
            raise BrokerRejectError(f"unexpected /v2/orders payload type: {type(data).__name__}")
        response_row = data.get("data") if isinstance(data.get("data"), dict) else data
        resolved_client_order_id, resolved_exchange_order_id = self._resolve_order_identifiers(
            response_row if isinstance(response_row, dict) else {},
            fallback_client_order_id=validated_client_order_id,
        )
        if not resolved_exchange_order_id:
            raise BrokerRejectError(f"missing order id from /v2/orders response: {data}")
        if resolved_client_order_id and resolved_client_order_id != validated_client_order_id:
            raise BrokerRejectError(
                "order submit response client_order_id mismatch: "
                f"requested={validated_client_order_id} response={resolved_client_order_id}"
            )
        raw = self._raw_v2_order_fields(
            response_row if isinstance(response_row, dict) else {},
            fallback_client_order_id=validated_client_order_id,
        )
        raw.setdefault("market", payload.get("market"))
        raw.setdefault("ord_type", payload.get("ord_type"))
        return BrokerOrder(validated_client_order_id, resolved_exchange_order_id, side, "NEW", price, qty, 0.0, now, now, raw)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        order = self.get_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)
        if self.dry_run:
            now = int(time.time() * 1000)
            return BrokerOrder(order.client_order_id, order.exchange_order_id, order.side, "CANCELED", order.price, order.qty_req, order.qty_filled, order.created_ts, now)

        cancel_payload: dict[str, object] = {}
        if order.exchange_order_id:
            cancel_payload["order_id"] = str(order.exchange_order_id)
        if order.client_order_id:
            cancel_payload["client_order_id"] = str(order.client_order_id)
        if not cancel_payload:
            raise BrokerRejectError("cancel requires order_id or client_order_id")

        try:
            response = self._post_private("/v2/orders/cancel", cancel_payload, retry_safe=False)
        except BrokerRejectError as exc:
            category, description = _classify_cancel_reject(exc)
            now = int(time.time() * 1000)
            if category == "ALREADY_CANCELED":
                return BrokerOrder(
                    order.client_order_id,
                    order.exchange_order_id,
                    order.side,
                    "CANCELED",
                    order.price,
                    order.qty_req,
                    order.qty_filled,
                    order.created_ts,
                    now,
                )
            if category == "ALREADY_FILLED":
                return BrokerOrder(
                    order.client_order_id,
                    order.exchange_order_id,
                    order.side,
                    "FILLED",
                    order.price,
                    order.qty_req,
                    order.qty_req,
                    order.created_ts,
                    now,
                )
            raise BrokerRejectError(f"cancel rejected category={category} description={description}: {exc}") from exc
        if not isinstance(response, dict):
            raise BrokerRejectError(f"unexpected /v2/orders/cancel payload type: {type(response).__name__}")
        response_row = response.get("data") if isinstance(response.get("data"), dict) else response
        resolved_client_order_id, resolved_exchange_order_id = self._resolve_order_identifiers(
            response_row if isinstance(response_row, dict) else {},
            fallback_client_order_id=str(response.get("client_order_id") or ""),
            fallback_exchange_order_id=str(response.get("order_id") or response.get("uuid") or ""),
        )
        requested_order_id = str(cancel_payload.get("order_id") or "")
        requested_client_order_id = str(cancel_payload.get("client_order_id") or "")
        if requested_order_id and resolved_exchange_order_id and resolved_exchange_order_id != requested_order_id:
            raise BrokerRejectError(
                "cancel response order_id mismatch: "
                f"requested={requested_order_id} response={resolved_exchange_order_id}"
            )
        if (
            requested_client_order_id
            and resolved_client_order_id
            and resolved_client_order_id != requested_client_order_id
            and not (requested_order_id and resolved_exchange_order_id)
        ):
            raise BrokerRejectError(
                "cancel response client_order_id mismatch: "
                f"requested={requested_client_order_id} response={resolved_client_order_id}"
            )

        if isinstance(response_row, dict) and str(response_row.get("state") or ""):
            return self._order_from_v2_row(
                response_row,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id or "",
            )

        now = int(time.time() * 1000)
        response_raw = self._raw_v2_order_fields(
            response_row if isinstance(response_row, dict) else {},
            fallback_client_order_id=order.client_order_id,
            fallback_exchange_order_id=resolved_exchange_order_id or order.exchange_order_id or "",
        )
        return BrokerOrder(
            order.client_order_id,
            resolved_exchange_order_id or order.exchange_order_id,
            order.side,
            CANCEL_REQUESTED_STATUS,
            order.price,
            order.qty_req,
            order.qty_filled,
            order.created_ts,
            now,
            response_raw,
        )

    def get_order(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> BrokerOrder:
        now = int(time.time() * 1000)
        requested_client_order_id = str(client_order_id or "").strip()
        requested_exchange_order_id = str(exchange_order_id or "").strip()
        params = build_v1_order_lookup_params(
            client_order_id=requested_client_order_id,
            exchange_order_id=requested_exchange_order_id,
        )

        exid = requested_exchange_order_id or f"dry_{requested_client_order_id}"
        if self.dry_run:
            return BrokerOrder(requested_client_order_id, exid, "BUY", "NEW", None, 0.0, 0.0, now, now)

        data = self._get_private("/v1/order", params, retry_safe=True)
        if not isinstance(data, dict):
            raise BrokerRejectError(
                "order lookup response schema mismatch: "
                f"unexpected /v1/order payload type={type(data).__name__}"
            )
        self._journal_read_summary(path="/v1/order", data=data)
        response_has_identifier = any(self._clean_identifier(data.get(key)) for key in ("uuid", "client_order_id"))
        resolved_ids = resolve_v1_order_identifiers(
            data,
            fallback_client_order_id=requested_client_order_id,
        )
        resolved_client_order_id = resolved_ids.client_order_id
        resolved_exchange_order_id = resolved_ids.exchange_order_id
        if requested_exchange_order_id and resolved_exchange_order_id and requested_exchange_order_id != resolved_exchange_order_id:
            raise BrokerRejectError(
                "order lookup response exchange_order_id mismatch: "
                f"requested={requested_exchange_order_id} response={resolved_exchange_order_id}"
            )
        if (
            requested_client_order_id
            and not requested_exchange_order_id
            and resolved_client_order_id
            and requested_client_order_id != resolved_client_order_id
        ):
            raise BrokerRejectError(
                "order lookup response client_order_id mismatch: "
                f"requested={requested_client_order_id} response={resolved_client_order_id}"
            )
        if not response_has_identifier:
            raise BrokerRejectError(
                "order lookup response schema mismatch: "
                "missing both uuid and client_order_id in response"
            )
        normalized = self._normalize_v1_order_row_strict(data)
        state = normalized.state
        qty_req = float(normalized.volume)
        qty_filled = float(normalized.executed_volume)
        status = v1_status_from_state(state=state, qty_req=qty_req, qty_filled=qty_filled)
        order_raw = self._raw_v1_order_fields(data)
        return BrokerOrder(
            client_order_id=resolved_client_order_id,
            exchange_order_id=resolved_exchange_order_id,
            side=str(normalized.side),
            status=status,
            price=float(normalized.price) if normalized.price is not None else None,
            qty_req=qty_req,
            qty_filled=qty_filled,
            created_ts=int(normalized.created_ts),
            updated_ts=int(normalized.updated_ts),
            raw=order_raw,
        )

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        if self.dry_run:
            return []
        if not exchange_order_ids and not client_order_ids:
            raise BrokerRejectError(
                "open order lookup requires identifiers; broad /v1/orders market/state scans are disabled"
            )
        data = self._get_private(
            "/v1/orders",
            build_order_list_params(
                uuids=exchange_order_ids,
                client_order_ids=client_order_ids,
                state="wait",
                page=1,
                order_by="desc",
            ),
            retry_safe=True,
        )
        self._journal_read_summary(path="/v1/orders(open_orders)", data=data)
        if not isinstance(data, list):
            raise BrokerRejectError(f"unexpected /v1/orders payload type: {type(data).__name__}")

        out: list[BrokerOrder] = []
        exchange_ids_count = len(exchange_order_ids or [])
        client_ids_count = len(client_order_ids or [])
        for row in data:
            if not isinstance(row, dict):
                raise BrokerRejectError("/v1/orders schema mismatch: each row must be object")
            try:
                normalized = parse_v1_order_list_row(row)
            except BrokerRejectError as exc:
                self._log_v1_orders_parse_failure(
                    endpoint="/v1/orders",
                    state="wait",
                    exchange_ids_count=exchange_ids_count,
                    client_ids_count=client_ids_count,
                    row=row,
                    reason=str(exc),
                )
                raise
            qty_req = float(normalized.volume)
            qty_filled = float(normalized.executed_volume)
            status = v1_status_from_state(state=normalized.state, qty_req=qty_req, qty_filled=qty_filled)
            out.append(
                BrokerOrder(
                    client_order_id=normalized.client_order_id,
                    exchange_order_id=normalized.uuid,
                    side=normalized.side,
                    status=status,
                    price=float(normalized.price),
                    qty_req=qty_req,
                    qty_filled=qty_filled,
                    created_ts=int(normalized.created_ts),
                    updated_ts=int(normalized.updated_ts),
                    raw=self._raw_v1_order_fields(row),
                )
            )
        return out

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if self.dry_run:
            return []

        requested_client_order_id = str(client_order_id or "").strip()
        requested_exchange_order_id = str(exchange_order_id or "").strip()

        if not (requested_exchange_order_id or requested_client_order_id):
            return []
        params = build_v1_order_lookup_params(
            client_order_id=requested_client_order_id,
            exchange_order_id=requested_exchange_order_id,
        )
        data = self._get_private("/v1/order", params, retry_safe=True)
        self._journal_read_summary(path="/v1/order(fills)", data=data)
        if not isinstance(data, dict):
            raise BrokerRejectError(
                "fill lookup response schema mismatch: "
                f"unexpected /v1/order payload type={type(data).__name__}"
            )
        response_ids = resolve_v1_order_identifiers(
            data,
            fallback_client_order_id=requested_client_order_id,
        )
        response_client_order_id = response_ids.client_order_id
        response_exchange_order_id = response_ids.exchange_order_id
        if (
            requested_exchange_order_id
            and response_exchange_order_id
            and requested_exchange_order_id != response_exchange_order_id
        ):
            raise BrokerRejectError(
                "fill lookup response exchange_order_id mismatch: "
                f"requested={requested_exchange_order_id} response={response_exchange_order_id}"
            )
        if (
            requested_client_order_id
            and response_client_order_id
            and requested_client_order_id != response_client_order_id
        ):
            raise BrokerRejectError(
                "fill lookup response client_order_id mismatch: "
                f"requested={requested_client_order_id} response={response_client_order_id}"
            )

        fills: list[BrokerFill] = []
        requires_removed_legacy_scan = False
        for row in [data]:
            if row.get("trades") not in (None, "") and not isinstance(row.get("trades"), list):
                raise BrokerRejectError("/v1/order schema mismatch: trades must be a list when present")
            require_v1_known_state(row.get("state"), context="/v1/order")
            normalized = self._normalize_v1_order_row_lenient_for_fills(row)
            trades = normalized["trades"] if isinstance(normalized["trades"], list) else []
            if trades:
                for index, trade in enumerate(trades):
                    if not isinstance(trade, dict):
                        continue
                    qty = self._strict_optional_number(trade, "volume", context="/v1/order.trades")
                    price = self._resolve_fill_price(trade, normalized_row=normalized)
                    if qty is None or qty <= 0:
                        continue
                    if price is None:
                        raise BrokerRejectError("/v1/order.trades schema mismatch: missing required numeric field 'price'")
                    fee = self._extract_fill_fee(
                        trade,
                        context="trade",
                        qty=qty,
                        price=price,
                        strict=True,
                    )
                    ts_raw = trade.get("created_at")
                    ts = self._strict_parse_ts(ts_raw, field_name="created_at", context="/v1/order.trades")
                    trade_client_order_id, _ = self._resolve_order_identifiers(
                        trade,
                        fallback_client_order_id=requested_client_order_id or row.get("client_order_id") or "",
                    )
                    fills.append(
                        BrokerFill(
                            client_order_id=trade_client_order_id,
                            fill_id=str(trade.get("uuid") or trade.get("id") or f"{row.get('uuid') or ''}:{index}:{ts}"),
                            fill_ts=ts,
                            price=float(price),
                            qty=float(qty),
                            fee=fee,
                            exchange_order_id=str(row.get("uuid") or ""),
                        )
                    )
                continue

            qty_filled = float(normalized["executed_volume"])
            if qty_filled <= 0:
                requires_removed_legacy_scan = True
                continue
            price = self._resolve_fill_price(row, normalized_row=normalized)
            if price is None:
                requires_removed_legacy_scan = True
                continue
            updated_raw = row.get("updated_at")
            created_raw = row.get("created_at")
            try:
                if updated_raw not in (None, ""):
                    ts = self._strict_parse_ts(updated_raw, field_name="updated_at", context="/v1/order")
                elif created_raw not in (None, ""):
                    ts = self._strict_parse_ts(created_raw, field_name="created_at", context="/v1/order")
                else:
                    raise BrokerRejectError("/v1/order schema mismatch: missing required timestamp field 'created_at'")
            except BrokerRejectError:
                requires_removed_legacy_scan = True
                continue
            fee = self._extract_fill_fee(
                row,
                context="aggregate",
                qty=qty_filled,
                price=price,
                strict=False,
            )
            aggregate_client_order_id, aggregate_exchange_order_id = self._resolve_order_identifiers(
                row,
                fallback_client_order_id=requested_client_order_id or "",
                fallback_exchange_order_id=str(normalized.get("uuid") or ""),
            )
            fills.append(
                BrokerFill(
                    client_order_id=aggregate_client_order_id,
                    fill_id=f"{row.get('uuid') or ''}:aggregate:{ts}",
                    fill_ts=ts,
                    price=float(price),
                    qty=qty_filled,
                    fee=fee,
                    exchange_order_id=aggregate_exchange_order_id,
                )
            )
        if not fills and requires_removed_legacy_scan:
            raise BrokerRejectError(
                "fill lookup requires /v1/order trade payload completeness; broad /v1/orders done scan fallback is disabled"
            )
        return fills

    def get_balance(self) -> BrokerBalance:
        if self.dry_run:
            return BrokerBalance(cash_available=settings.START_CASH_KRW, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)
        order_currency, payment_currency = self._pair()
        response = self.fetch_accounts_raw()
        row_count = len(response) if isinstance(response, list) else 0
        currencies: list[str] = []
        if isinstance(response, list):
            for row in response:
                if not isinstance(row, dict):
                    continue
                token = str(row.get("currency") or "").strip().upper()
                if token:
                    currencies.append(token)
        parsed_accounts = None
        try:
            parsed_accounts = parse_accounts_response(response)
            pair_balances = select_pair_balances(
                parsed_accounts,
                order_currency=order_currency,
                payment_currency=payment_currency,
            )
        except Exception as exc:
            reason = self._classify_accounts_validation_reason(exc)
            missing_required_currencies: list[str] = []
            error_text = str(exc)
            if "missing quote currency row '" in error_text:
                missing_required_currencies.append(payment_currency.upper())
            if "missing base currency row '" in error_text:
                missing_required_currencies.append(order_currency.upper())
            self._accounts_validation_diag = {
                "reason": reason,
                "row_count": row_count,
                "currencies": sorted(set(currencies)),
                "missing_required_currencies": missing_required_currencies,
                "duplicate_currencies": sorted({token for token in currencies if currencies.count(token) > 1}),
                "last_success_reason": self._accounts_validation_diag.get("last_success_reason"),
                "last_failure_reason": reason,
            }
            raise

        self._accounts_validation_diag = {
            "reason": "ok",
            "row_count": row_count,
            "currencies": sorted(parsed_accounts.balances.keys()) if parsed_accounts is not None else [],
            "missing_required_currencies": [],
            "duplicate_currencies": sorted({token for token in currencies if currencies.count(token) > 1}),
            "last_success_reason": "ok",
            "last_failure_reason": self._accounts_validation_diag.get("last_failure_reason"),
        }
        return to_broker_balance(pair_balances)

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        lim = max(0, int(limit))
        if lim == 0:
            return []
        if not exchange_order_ids and not client_order_ids:
            raise BrokerRejectError(
                "recent order lookup requires identifiers; broad /v1/orders market/state scans are disabled"
            )

        snapshots: dict[str, BrokerOrder] = {}
        exchange_ids_count = len(exchange_order_ids or [])
        client_ids_count = len(client_order_ids or [])
        for state, journal_path in (("wait", "/v1/orders(open_orders)"), ("done", "/v1/orders(done)"), ("cancel", "/v1/orders(cancel)")):
            data = self._get_private(
                "/v1/orders",
                build_order_list_params(
                    uuids=exchange_order_ids,
                    client_order_ids=client_order_ids,
                    state=state,
                    page=1,
                    order_by="desc",
                ),
                retry_safe=True,
            )
            self._journal_read_summary(path=journal_path, data=data)
            if not isinstance(data, list):
                raise BrokerRejectError(f"unexpected /v1/orders payload type: {type(data).__name__}")
            for row in data:
                if not isinstance(row, dict):
                    raise BrokerRejectError("/v1/orders schema mismatch: each row must be object")
                try:
                    normalized = parse_v1_order_list_row(row)
                except BrokerRejectError as exc:
                    self._log_v1_orders_parse_failure(
                        endpoint="/v1/orders",
                        state=state,
                        exchange_ids_count=exchange_ids_count,
                        client_ids_count=client_ids_count,
                        row=row,
                        reason=str(exc),
                    )
                    raise
                qty_req = float(normalized.volume)
                qty_filled = float(normalized.executed_volume)
                order = BrokerOrder(
                    client_order_id=normalized.client_order_id,
                    exchange_order_id=normalized.uuid,
                    side=normalized.side,
                    status=v1_status_from_state(state=normalized.state, qty_req=qty_req, qty_filled=qty_filled),
                    price=float(normalized.price),
                    qty_req=qty_req,
                    qty_filled=qty_filled,
                    created_ts=int(normalized.created_ts),
                    updated_ts=int(normalized.updated_ts),
                    raw=self._raw_v1_order_fields(row),
                )
                snapshot_key = str(order.exchange_order_id or order.client_order_id or "")
                if snapshot_key:
                    snapshots[snapshot_key] = order

        out = list(snapshots.values())
        out.sort(key=lambda order: int(order.updated_ts), reverse=True)
        return out[:lim]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        fills = self.get_fills(client_order_id=None, exchange_order_id=None)
        fills.sort(key=lambda f: int(f.fill_ts), reverse=True)
        return fills[: max(0, int(limit))]
