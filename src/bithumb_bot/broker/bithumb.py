from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx

from ..config import settings
from ..marketdata import fetch_orderbook_top, to_v1_market
from .base import BrokerBalance, BrokerFill, BrokerOrder, BrokerRejectError, BrokerTemporaryError

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


class BithumbPrivateAPI:
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
            if isinstance(value, (list, tuple)):
                for item in value:
                    items.append((str(key), str(item)))
                continue
            items.append((str(key), str(value)))
        return items

    @classmethod
    def _query_string(cls, payload: dict[str, object] | None) -> str:
        return urlencode(cls._payload_items(payload), doseq=True)

    @classmethod
    def _query_hash_claims(cls, payload: dict[str, object] | None) -> dict[str, str]:
        query_string = cls._query_string(payload)
        if not query_string:
            return {}
        return {
            "query_hash": hashlib.sha512(query_string.encode()).hexdigest(),
            "query_hash_alg": "SHA512",
        }

    def _jwt_token(self, payload: dict[str, object] | None) -> str:
        claims: dict[str, object] = {
            "access_key": self.api_key,
            "nonce": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
            **self._query_hash_claims(payload),
        }
        token = _jwt.encode(claims, self.api_secret, algorithm="HS256")
        return token if isinstance(token, str) else token.decode()

    def _headers(self, payload: dict[str, object] | None, *, has_json_body: bool) -> dict[str, str]:
        if self.dry_run:
            return {}
        headers = {
            "Authorization": f"Bearer {self._jwt_token(payload)}",
            "Accept": "application/json",
        }
        if has_json_body:
            headers["Content-Type"] = "application/json"
        return headers

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
        auth_payload = params if method in {"GET", "DELETE"} else json_body
        request_kwargs: dict[str, object] = {}
        if params:
            request_kwargs["params"] = params
        if json_body:
            request_kwargs["json"] = json_body

        for attempt in range(attempts):
            headers = self._headers(auth_payload, has_json_body=bool(json_body))
            try:
                with httpx.Client(base_url=self.base_url, timeout=10.0) as client:
                    res = client.request(method, endpoint, headers=headers, **request_kwargs)

                if 500 <= res.status_code <= 599:
                    body = response_excerpt(res) if response_excerpt else ""
                    raise BrokerTemporaryError(
                        f"bithumb private {endpoint} server error status={res.status_code} body={body}"
                    )
                res.raise_for_status()
                data = res.json() if res.content else {}
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


def classify_private_api_error(exc: Exception) -> tuple[str, str]:
    detail = str(exc).lower()
    if "status=401" in detail or "unauthorized" in detail or "invalid jwt" in detail:
        return "AUTH", "authentication failed (401 / invalid JWT / key-secret mismatch)"
    if "status=403" in detail or "out_of_scope" in detail or "permission" in detail:
        return "PERMISSION", "API key scope/permission denied"
    if any(token in detail for token in ("insufficient", "under_min_total", "too_many_orders", "balance")):
        return "FUNDS", "balance or orderable-funds check failed"
    if any(token in detail for token in ("market", "price", "volume", "ord_type", "validation")):
        return "PARAM", "market/order parameter validation failed"
    return "UNKNOWN", "unclassified private API failure"


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
        if isinstance(rows, dict):
            summary["keys"] = sorted(self._mask_sensitive(rows).keys())[:10]
        self._read_journal[path] = str(summary)

    def get_read_journal_summary(self) -> dict[str, str]:
        return dict(self._read_journal)

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

    def _get_private(self, endpoint: str, params: dict[str, object], *, retry_safe: bool = False) -> dict | list:
        return self._request_private("GET", endpoint, params=params, retry_safe=retry_safe)

    def _post_private(self, endpoint: str, payload: dict[str, object], *, retry_safe: bool = False) -> dict | list:
        if self.dry_run:
            return {"status": "0000", "data": {"uuid": f"dry_{payload.get('uuid', payload.get('market', 'order'))}"}}
        return self._request_private("POST", endpoint, json_body=payload, retry_safe=retry_safe)

    def _delete_private(self, endpoint: str, params: dict[str, object], *, retry_safe: bool = False) -> dict | list:
        return self._request_private("DELETE", endpoint, params=params, retry_safe=retry_safe)

    def _pair(self) -> tuple[str, str]:
        order_currency, payment_currency = settings.PAIR.split("_")
        return order_currency, payment_currency

    def _market(self) -> str:
        return to_v1_market(settings.PAIR)

    @staticmethod
    def _format_volume(qty: float) -> str:
        return f"{float(qty):.16f}".rstrip("0").rstrip(".") or "0"

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

    def _normalize_order_row(self, row: dict[str, object]) -> dict[str, object]:
        volume = self._number(row, "volume", "units")
        remaining = self._number(row, "remaining_volume", "units_remaining")
        executed = self._number(row, "executed_volume")
        if executed <= 0 and volume > 0 and remaining >= 0:
            executed = max(0.0, volume - remaining)
        return {
            "uuid": str(row.get("uuid") or row.get("order_id") or ""),
            "side": self._normalize_order_side(str(row.get("side") or row.get("type")), default="BUY"),
            "state": str(row.get("state") or ""),
            "price": self._number(row, "price") or None,
            "volume": volume,
            "remaining_volume": remaining,
            "executed_volume": executed,
            "created_ts": self._parse_ts(row.get("created_at") or row.get("timestamp")),
            "updated_ts": self._parse_ts(row.get("updated_at") or row.get("created_at") or row.get("timestamp")),
            "trades": row.get("trades") if isinstance(row.get("trades"), list) else [],
        }

    def _order_from_v2_row(self, row: dict[str, object], *, client_order_id: str) -> BrokerOrder:
        normalized = self._normalize_order_row(row)
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
            client_order_id,
            str(normalized["uuid"]),
            str(normalized["side"]),
            status,
            float(normalized["price"]) if normalized["price"] is not None else None,
            qty_req,
            qty_filled,
            int(normalized["created_ts"]),
            int(normalized["updated_ts"]),
        )

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
        if self.dry_run:
            return BrokerOrder(client_order_id, f"dry_{client_order_id}", side, "NEW", price, qty, 0.0, now, now)

        payload: dict[str, object] = {
            "market": self._market(),
            "side": "bid" if side.lower() == "buy" else "ask",
        }
        if price is None:
            if side.lower() == "buy":
                bid, ask = fetch_orderbook_top(settings.PAIR)
                payload.update({
                    "price": str(max(float(ask), 0.0) * float(qty)),
                    "ord_type": "price",
                })
            else:
                payload.update({
                    "volume": self._format_volume(qty),
                    "ord_type": "market",
                })
        else:
            payload.update({
                "volume": self._format_volume(qty),
                "price": str(price),
                "ord_type": "limit",
            })

        data = self._post_private("/v2/orders", payload, retry_safe=False)
        if not isinstance(data, dict):
            raise BrokerRejectError(f"unexpected /v2/orders payload type: {type(data).__name__}")
        exchange_order_id = str(data.get("uuid") or data.get("order_id") or data.get("data", {}).get("uuid") or "")
        if not exchange_order_id:
            raise BrokerRejectError(f"missing order id from /v2/orders response: {data}")
        return BrokerOrder(client_order_id, exchange_order_id, side, "NEW", price, qty, 0.0, now, now)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        order = self.get_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)
        if self.dry_run:
            now = int(time.time() * 1000)
            return BrokerOrder(order.client_order_id, order.exchange_order_id, order.side, "CANCELED", order.price, order.qty_req, order.qty_filled, order.created_ts, now)

        self._delete_private(
            "/v2/order",
            {"uuid": str(order.exchange_order_id)},
            retry_safe=False,
        )
        now = int(time.time() * 1000)
        return BrokerOrder(order.client_order_id, order.exchange_order_id, order.side, "CANCELED", order.price, order.qty_req, order.qty_filled, order.created_ts, now)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        now = int(time.time() * 1000)
        exid = exchange_order_id or f"dry_{client_order_id}"
        if self.dry_run:
            return BrokerOrder(client_order_id, exid, "BUY", "NEW", None, 0.0, 0.0, now, now)

        data = self._get_private("/v1/order", {"uuid": str(exid)}, retry_safe=True)
        if not isinstance(data, dict):
            raise BrokerRejectError(f"unexpected /v1/order payload type: {type(data).__name__}")
        self._journal_read_summary(path="/v1/order", data=data)
        return self._order_from_v2_row(data, client_order_id=client_order_id)

    def get_open_orders(self) -> list[BrokerOrder]:
        if self.dry_run:
            return []
        data = self._get_private(
            "/v1/orders",
            {
                "market": self._market(),
                "state": "wait",
                "limit": 100,
            },
            retry_safe=True,
        )
        self._journal_read_summary(path="/v1/orders(open_orders)", data=data)
        rows = data if isinstance(data, list) else []
        return [self._order_from_v2_row(row, client_order_id="") for row in rows if isinstance(row, dict)]

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if self.dry_run:
            return []

        rows: list[dict[str, object]] = []
        if exchange_order_id:
            data = self._get_private("/v1/order", {"uuid": str(exchange_order_id)}, retry_safe=True)
            self._journal_read_summary(path="/v1/order(fills)", data=data)
            if isinstance(data, dict):
                rows = [data]
        else:
            data = self._get_private(
                "/v1/orders",
                {"market": self._market(), "state": "done", "limit": 100},
                retry_safe=True,
            )
            self._journal_read_summary(path="/v1/orders(fills)", data=data)
            if isinstance(data, list):
                rows = [row for row in data if isinstance(row, dict)]

        fills: list[BrokerFill] = []
        for row in rows:
            normalized = self._normalize_order_row(row)
            trades = normalized["trades"] if isinstance(normalized["trades"], list) else []
            if trades:
                for index, trade in enumerate(trades):
                    if not isinstance(trade, dict):
                        continue
                    qty = self._number(trade, "volume", "qty", "units_traded")
                    price = self._number(trade, "price")
                    fee = self._number(trade, "fee")
                    ts = self._parse_ts(trade.get("created_at") or trade.get("timestamp") or normalized["updated_ts"])
                    fills.append(
                        BrokerFill(
                            client_order_id=client_order_id or "",
                            fill_id=str(trade.get("uuid") or trade.get("id") or f"{normalized['uuid']}:{index}:{ts}"),
                            fill_ts=ts,
                            price=price,
                            qty=qty,
                            fee=fee,
                            exchange_order_id=str(normalized["uuid"]),
                        )
                    )
                continue

            qty_filled = float(normalized["executed_volume"])
            if qty_filled <= 0:
                continue
            ts = int(normalized["updated_ts"])
            fills.append(
                BrokerFill(
                    client_order_id=client_order_id or "",
                    fill_id=f"{normalized['uuid']}:aggregate:{ts}",
                    fill_ts=ts,
                    price=float(normalized["price"] or 0.0),
                    qty=qty_filled,
                    fee=0.0,
                    exchange_order_id=str(normalized["uuid"]),
                )
            )
        return fills

    def get_balance(self) -> BrokerBalance:
        if self.dry_run:
            return BrokerBalance(cash_available=settings.START_CASH_KRW, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)
        order_currency, payment_currency = self._pair()
        data = self._get_private("/v1/accounts", {}, retry_safe=True)
        self._journal_read_summary(path="/v1/accounts", data=data)
        rows = data if isinstance(data, list) else []
        accounts: dict[str, dict[str, object]] = {
            str(row.get("currency") or "").upper(): row
            for row in rows
            if isinstance(row, dict)
        }
        cash = accounts.get(payment_currency.upper(), {})
        asset = accounts.get(order_currency.upper(), {})
        return BrokerBalance(
            cash_available=self._number(cash, "balance", "available_balance"),
            cash_locked=self._number(cash, "locked", "in_use", "locked_balance"),
            asset_available=self._number(asset, "balance", "available_balance"),
            asset_locked=self._number(asset, "locked", "in_use", "locked_balance"),
        )

    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        lim = max(0, int(limit))
        if lim == 0:
            return []

        snapshots: dict[str, BrokerOrder] = {}
        for state, journal_path in (("wait", "/v1/orders(open_orders)"), ("done", "/v1/orders(done)"), ("cancel", "/v1/orders(cancel)")):
            try:
                data = self._get_private(
                    "/v1/orders",
                    {"market": self._market(), "state": state, "limit": max(lim, 100)},
                    retry_safe=True,
                )
            except (BrokerRejectError, BrokerTemporaryError):
                if state == "wait":
                    raise
                break
            self._journal_read_summary(path=journal_path, data=data)
            rows = data if isinstance(data, list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                order = self._order_from_v2_row(row, client_order_id="")
                if order.exchange_order_id:
                    snapshots[str(order.exchange_order_id)] = order

        out = list(snapshots.values())
        out.sort(key=lambda order: int(order.updated_ts), reverse=True)
        return out[:lim]

    def _broker_order_from_open_row(self, row: dict, *, now_ts: int) -> BrokerOrder:
        normalized = self._normalize_order_row({**row, "created_at": row.get("created_at") or now_ts})
        return BrokerOrder(
            client_order_id="",
            exchange_order_id=str(normalized["uuid"]),
            side=str(normalized["side"]),
            status="PARTIAL" if float(normalized["executed_volume"]) > 0 else "NEW",
            price=float(normalized["price"]) if normalized["price"] is not None else None,
            qty_req=float(normalized["volume"]),
            qty_filled=float(normalized["executed_volume"]),
            created_ts=int(normalized["created_ts"] or now_ts),
            updated_ts=int(normalized["updated_ts"] or now_ts),
        )

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        fills = self.get_fills(client_order_id=None, exchange_order_id=None)
        fills.sort(key=lambda f: int(f.fill_ts), reverse=True)
        return fills[: max(0, int(limit))]
