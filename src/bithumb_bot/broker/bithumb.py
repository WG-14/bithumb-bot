from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

import httpx

from ..config import settings
from .base import BrokerBalance, BrokerFill, BrokerOrder, BrokerRejectError, BrokerTemporaryError

_HTTPX_TRANSIENT_ERRORS = tuple(
    cls
    for cls in (
        getattr(httpx, "TimeoutException", None),
        getattr(httpx, "TransportError", None),
        getattr(httpx, "RequestError", None),
    )
    if isinstance(cls, type)
)


class BithumbBroker:
    def __init__(self) -> None:
        self.api_key = settings.BITHUMB_API_KEY
        self.api_secret = settings.BITHUMB_API_SECRET
        self.base_url = settings.BITHUMB_API_BASE
        self.dry_run = settings.LIVE_DRY_RUN
        self._read_journal: dict[str, str] = {}

    def _mask_sensitive(self, data: dict[str, object]) -> dict[str, object]:
        redacted: dict[str, object] = {}
        for key, value in data.items():
            lowered = str(key).lower()
            if any(token in lowered for token in ("secret", "sign", "nonce", "api", "authorization", "token", "key")):
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

    def _private_order_type(self, side: str | None) -> str:
        token = str(side or "").strip().lower()
        if token in {"buy", "bid"}:
            return "bid"
        if token in {"sell", "ask"}:
            return "ask"
        return token

    def _journal_read_summary(self, *, path: str, data: dict[str, object] | None) -> None:
        payload = data or {}
        rows = payload.get("data") if isinstance(payload, dict) else None
        row_count = len(rows) if isinstance(rows, list) else (1 if isinstance(rows, dict) else 0)

        sample_order_ids: list[str] = []
        if isinstance(rows, list):
            for row in rows[:3]:
                if not isinstance(row, dict):
                    continue
                order_id = row.get("order_id")
                if order_id:
                    sample_order_ids.append(str(order_id))

        summary: dict[str, object] = {
            "path": path,
            "status": str(payload.get("status", "")) if isinstance(payload, dict) else "",
            "row_count": row_count,
        }
        if sample_order_ids:
            summary["sample_order_ids"] = sample_order_ids
        if isinstance(rows, dict):
            summary["keys"] = sorted(self._mask_sensitive(rows).keys())[:10]
        self._read_journal[path] = str(summary)

    def get_read_journal_summary(self) -> dict[str, str]:
        return dict(self._read_journal)

    def _nonce(self) -> str:
        return str(int(time.time() * 1_000_000))

    def _headers(self, endpoint: str, payload: dict[str, str]) -> dict[str, str]:
        if self.dry_run:
            return {}
        nonce = self._nonce()
        body = urlencode(payload)
        message = endpoint + "\0" + body + "\0" + nonce
        digest = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha512).hexdigest()
        sign = base64.b64encode(digest.encode()).decode()
        return {
            "Api-Key": self.api_key,
            "Api-Nonce": nonce,
            "Api-Sign": sign,
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def _post_private(self, endpoint: str, payload: dict[str, str], *, retry_safe: bool = False) -> dict:
        if self.dry_run:
            return {"status": "0000", "data": {"order_id": f"dry_{payload.get('order_id', payload.get('order_currency', 'order'))}"}}

        attempts = 3 if retry_safe else 1
        backoffs = (0.2, 0.5)

        for attempt in range(attempts):
            headers = self._headers(endpoint, payload)
            try:
                with httpx.Client(base_url=self.base_url, timeout=10.0) as client:
                    res = client.post(endpoint, data=payload, headers=headers)

                if 500 <= res.status_code <= 599:
                    raise BrokerTemporaryError(
                        f"bithumb private {endpoint} server error status={res.status_code} body={self._response_body_excerpt(res)}"
                    )
                res.raise_for_status()
                data = res.json()
            except _HTTPX_TRANSIENT_ERRORS as exc:
                if attempt < attempts - 1:
                    time.sleep(backoffs[attempt])
                    continue
                raise BrokerTemporaryError(f"bithumb private {endpoint} transport error: {type(exc).__name__}: {exc}") from exc
            except httpx.HTTPStatusError as exc:
                if 500 <= exc.response.status_code <= 599:
                    if attempt < attempts - 1:
                        time.sleep(backoffs[attempt])
                        continue
                    raise BrokerTemporaryError(
                        f"bithumb private {endpoint} server error status={exc.response.status_code} body={self._response_body_excerpt(exc.response)}"
                    ) from exc
                raise BrokerRejectError(
                    f"bithumb private {endpoint} rejected with http status={exc.response.status_code} body={self._response_body_excerpt(exc.response)}"
                ) from exc

            if str(data.get("status")) != "0000":
                raise BrokerRejectError(f"bithumb private call rejected: {data}")
            return data

        raise BrokerTemporaryError(f"bithumb private {endpoint} failed after retries")

    def _pair(self) -> tuple[str, str]:
        order_currency, payment_currency = settings.PAIR.split("_")
        return order_currency, payment_currency

    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        now = int(time.time() * 1000)
        if self.dry_run:
            return BrokerOrder(client_order_id, f"dry_{client_order_id}", side, "NEW", price, qty, 0.0, now, now)

        order_currency, payment_currency = self._pair()
        side_lower = side.lower()
        if price is None:
            endpoint = "/trade/market_buy" if side_lower == "buy" else "/trade/market_sell"
            payload = {
                "order_currency": order_currency,
                "payment_currency": payment_currency,
                "units": f"{qty:.16f}",
            }
        else:
            endpoint = "/trade/place"
            payload = {
                "order_currency": order_currency,
                "payment_currency": payment_currency,
                "units": f"{qty:.16f}",
                "type": side_lower,
                "price": str(price),
            }

        data = self._post_private(endpoint, payload, retry_safe=False)
        exchange_order_id = str(data["data"]["order_id"])
        return BrokerOrder(client_order_id, exchange_order_id, side, "NEW", price, qty, 0.0, now, now)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        order = self.get_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)
        if self.dry_run:
            now = int(time.time() * 1000)
            return BrokerOrder(order.client_order_id, order.exchange_order_id, order.side, "CANCELED", order.price, order.qty_req, order.qty_filled, order.created_ts, now)

        order_currency, payment_currency = self._pair()
        self._post_private(
            "/trade/cancel",
            {
                "order_id": str(order.exchange_order_id),
                "type": self._private_order_type(order.side),
                "order_currency": order_currency,
                "payment_currency": payment_currency,
            },
            retry_safe=False,
        )
        now = int(time.time() * 1000)
        return BrokerOrder(order.client_order_id, order.exchange_order_id, order.side, "CANCELED", order.price, order.qty_req, order.qty_filled, order.created_ts, now)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        now = int(time.time() * 1000)
        exid = exchange_order_id or f"dry_{client_order_id}"
        if self.dry_run:
            return BrokerOrder(client_order_id, exid, "BUY", "NEW", None, 0.0, 0.0, now, now)

        order_currency, payment_currency = self._pair()
        open_rows: list[dict] = []
        for order_type in ("bid", "ask"):
            open_data = self._post_private(
                "/info/orders",
                {
                    "count": "100",
                    "order_id": str(exid),
                    "type": order_type,
                    "order_currency": order_currency,
                    "payment_currency": payment_currency,
                },
                retry_safe=True,
            )
            self._journal_read_summary(path=f"/info/orders(get_order:{order_type})", data=open_data)
            open_rows.extend(open_data.get("data") or [])
        for row in open_rows:
            if str(row.get("order_id")) != str(exid):
                continue
            qty_req = float(row.get("units") or 0.0)
            qty_remain = float(row.get("units_remaining") or 0.0)
            qty_filled = max(0.0, qty_req - qty_remain)
            return BrokerOrder(
                client_order_id,
                str(exid),
                self._normalize_order_side(str(row.get("type")), default="BUY"),
                "PARTIAL" if qty_filled > 0 else "NEW",
                float(row.get("price")) if row.get("price") else None,
                qty_req,
                qty_filled,
                now,
                now,
            )

        # Fallback for non-open orders. /info/order_detail provides fill/remaining quantities,
        # but status inference is constrained to terminal states for restart safety.
        detail_data = self._post_private(
            "/info/order_detail",
            {
                "order_id": str(exid),
                "order_currency": order_currency,
                "payment_currency": payment_currency,
            },
            retry_safe=True,
        )
        self._journal_read_summary(path="/info/order_detail", data=detail_data)
        detail_rows = detail_data.get("data") or []
        if not detail_rows:
            raise BrokerRejectError(f"order lookup ambiguous for exchange_order_id={exid}: not open and no detail rows")

        row = detail_rows[0]
        qty_req = float(row.get("units") or 0.0)
        qty_remain = float(row.get("units_remaining") or 0.0)
        qty_filled = max(0.0, qty_req - qty_remain)

        if qty_req > 0 and qty_filled >= qty_req:
            status = "FILLED"
        elif qty_req > 0:
            status = "CANCELED"
        else:
            raise BrokerRejectError(
                f"order lookup ambiguous for exchange_order_id={exid}: non-open detail row missing quantity"
            )

        return BrokerOrder(
            client_order_id,
            str(exid),
            self._normalize_order_side(str(row.get("type")), default="BUY"),
            status,
            float(row.get("price")) if row.get("price") else None,
            qty_req,
            qty_filled,
            now,
            now,
        )

    def get_open_orders(self) -> list[BrokerOrder]:
        if self.dry_run:
            return []
        order_currency, payment_currency = self._pair()
        rows: list[dict] = []
        for order_type in ("bid", "ask"):
            data = self._post_private(
                "/info/orders",
                {
                    "count": "100",
                    "type": order_type,
                    "order_currency": order_currency,
                    "payment_currency": payment_currency,
                },
                retry_safe=True,
            )
            self._journal_read_summary(path=f"/info/orders(open_orders:{order_type})", data=data)
            rows.extend(data.get("data") or [])
        now = int(time.time() * 1000)
        return [self._broker_order_from_open_row(row, now_ts=now) for row in rows]

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if self.dry_run:
            return []
        order_currency, payment_currency = self._pair()
        payload = {
            "order_currency": order_currency,
            "payment_currency": payment_currency,
            "count": "100",
        }
        if exchange_order_id:
            payload["order_id"] = exchange_order_id
        data = self._post_private("/info/user_transactions", payload, retry_safe=True)
        self._journal_read_summary(path="/info/user_transactions(fills)", data=data)
        fills: list[BrokerFill] = []
        for row in data.get("data") or []:
            tms = int(float(row.get("transfer_date", 0)))
            fills.append(
                BrokerFill(
                    client_order_id=client_order_id or "",
                    fill_id=str(row.get("search")) + ":" + str(row.get("units_traded")) + ":" + str(tms),
                    fill_ts=tms,
                    price=float(row.get("price") or 0.0),
                    qty=float(row.get("units_traded") or 0.0),
                    fee=float(row.get("fee") or 0.0),
                    exchange_order_id=(str(row.get("order_id")) if row.get("order_id") else exchange_order_id),
                )
            )
        return fills

    def get_balance(self) -> BrokerBalance:
        if self.dry_run:
            return BrokerBalance(cash_available=settings.START_CASH_KRW, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)
        order_currency, payment_currency = self._pair()
        data = self._post_private(
            "/info/balance",
            {
                "currency": order_currency,
            },
            retry_safe=True,
        )
        self._journal_read_summary(path="/info/balance", data=data)
        d = data.get("data") or {}
        cash_available = float(d.get(f"available_{payment_currency.lower()}") or 0.0)
        asset_available = float(d.get(f"available_{order_currency.lower()}") or 0.0)
        # Bithumb balance payload may omit in-use values for some accounts.
        cash_locked = float(d.get(f"in_use_{payment_currency.lower()}") or 0.0)
        asset_locked = float(d.get(f"in_use_{order_currency.lower()}") or 0.0)
        return BrokerBalance(
            cash_available=cash_available,
            cash_locked=cash_locked,
            asset_available=asset_available,
            asset_locked=asset_locked,
        )


    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        lim = max(0, int(limit))
        if lim == 0:
            return []

        # Start from open orders so active exposure is always represented.
        open_orders = self.get_open_orders()
        by_exchange_id: dict[str, BrokerOrder] = {
            str(order.exchange_order_id): order
            for order in open_orders
            if order.exchange_order_id
        }

        # Bithumb does not expose a dedicated closed-order history endpoint in this adapter.
        # Best-effort: infer recently executed orders from transaction history.
        # Safe fallback: if history lookup fails, return open-order snapshot only.
        try:
            tx_payload = self._post_private(
                "/info/user_transactions",
                {
                    "order_currency": self._pair()[0],
                    "payment_currency": self._pair()[1],
                    "count": str(max(lim, 100)),
                },
                retry_safe=True,
            )
            self._journal_read_summary(path="/info/user_transactions(recent_orders)", data=tx_payload)
            tx_rows = tx_payload.get("data") or []
        except (BrokerRejectError, BrokerTemporaryError):
            return open_orders[:lim]

        if not isinstance(tx_rows, list):
            tx_rows = []

        aggregated: dict[str, dict[str, float | int | str | None]] = {}
        for row in tx_rows:
            if not isinstance(row, dict):
                continue

            exchange_order_id = str(row.get("order_id") or "")
            if not exchange_order_id:
                continue

            try:
                ts = int(float(row.get("transfer_date") or 0))
                qty = float(row.get("units_traded") or 0.0)
                price = float(row.get("price") or 0.0)
            except (TypeError, ValueError):
                continue

            slot = aggregated.setdefault(
                exchange_order_id,
                {
                    "exchange_order_id": exchange_order_id,
                    "side": str(row.get("search") or row.get("type") or "").upper() or "UNKNOWN",
                    "qty_filled": 0.0,
                    "notional": 0.0,
                    "created_ts": ts,
                    "updated_ts": ts,
                },
            )
            slot["qty_filled"] = float(slot["qty_filled"] or 0.0) + qty
            slot["notional"] = float(slot["notional"] or 0.0) + (qty * price)
            slot["created_ts"] = min(int(slot["created_ts"] or ts), ts)
            slot["updated_ts"] = max(int(slot["updated_ts"] or ts), ts)

        out: list[BrokerOrder] = list(open_orders)
        for exchange_order_id, snapshot in aggregated.items():
            if exchange_order_id in by_exchange_id:
                continue
            qty_filled = float(snapshot["qty_filled"] or 0.0)
            avg_price = (
                float(snapshot["notional"] or 0.0) / qty_filled
                if qty_filled > 0
                else None
            )
            out.append(
                BrokerOrder(
                    client_order_id="",
                    exchange_order_id=exchange_order_id,
                    side=str(snapshot["side"]),
                    status="FILLED" if qty_filled > 0 else "UNKNOWN",
                    price=avg_price,
                    # Transaction history does not expose original requested quantity.
                    # Use filled quantity as a conservative lower bound.
                    qty_req=qty_filled,
                    qty_filled=qty_filled,
                    created_ts=int(snapshot["created_ts"] or 0),
                    updated_ts=int(snapshot["updated_ts"] or 0),
                )
            )

        out.sort(key=lambda order: int(order.updated_ts), reverse=True)
        return out[:lim]

    def _broker_order_from_open_row(self, row: dict, *, now_ts: int) -> BrokerOrder:
        qty_req = float(row.get("units") or 0.0)
        qty_remain = float(row.get("units_remaining") or 0.0)
        qty_filled = max(0.0, qty_req - qty_remain)
        return BrokerOrder(
            client_order_id="",
            exchange_order_id=str(row.get("order_id")),
            side=self._normalize_order_side(str(row.get("type")), default="BUY"),
            status="PARTIAL" if qty_filled > 0 else "NEW",
            price=float(row.get("price")) if row.get("price") else None,
            qty_req=qty_req,
            qty_filled=qty_filled,
            created_ts=now_ts,
            updated_ts=now_ts,
        )

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        fills = self.get_fills(client_order_id=None, exchange_order_id=None)
        fills.sort(key=lambda f: int(f.fill_ts), reverse=True)
        return fills[: max(0, int(limit))]
