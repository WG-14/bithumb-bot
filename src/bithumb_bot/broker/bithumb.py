from __future__ import annotations

import base64
import hashlib
import hmac
import time
from urllib.parse import urlencode

import httpx

from ..config import settings
from .base import BrokerBalance, BrokerFill, BrokerOrder, BrokerRejectError, BrokerTemporaryError


class BithumbBroker:
    def __init__(self) -> None:
        self.api_key = settings.BITHUMB_API_KEY
        self.api_secret = settings.BITHUMB_API_SECRET
        self.base_url = settings.BITHUMB_API_BASE
        self.dry_run = settings.LIVE_DRY_RUN

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
                    raise BrokerTemporaryError(f"bithumb private {endpoint} server error status={res.status_code}")
                res.raise_for_status()
                data = res.json()
            except (httpx.TimeoutException, httpx.TransportError) as exc:
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
                        f"bithumb private {endpoint} server error status={exc.response.status_code}"
                    ) from exc
                raise BrokerRejectError(
                    f"bithumb private {endpoint} rejected with http status={exc.response.status_code}"
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
        payload = {
            "order_currency": order_currency,
            "payment_currency": payment_currency,
            "units": f"{qty:.16f}",
            "type": side.lower(),
        }
        if price is not None:
            payload["price"] = str(price)

        data = self._post_private("/trade/place", payload, retry_safe=False)
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
                "type": order.side.lower(),
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
        data = self._post_private(
            "/info/order_detail",
            {
                "order_id": str(exid),
                "order_currency": order_currency,
                "payment_currency": payment_currency,
            },
            retry_safe=True,
        )
        rows = data.get("data") or []
        if not rows:
            raise BrokerRejectError(f"order not found for exchange_order_id={exid}")
        row = rows[0]
        qty_req = float(row.get("units") or 0.0)
        qty_remain = float(row.get("units_remaining") or 0.0)
        qty_filled = max(0.0, qty_req - qty_remain)
        status = "FILLED" if qty_filled >= qty_req and qty_req > 0 else ("PARTIAL" if qty_filled > 0 else "NEW")
        return BrokerOrder(client_order_id, str(exid), str(row.get("type", "BUY")).upper(), status, float(row.get("price")) if row.get("price") else None, qty_req, qty_filled, now, now)

    def get_open_orders(self) -> list[BrokerOrder]:
        if self.dry_run:
            return []
        order_currency, payment_currency = self._pair()
        data = self._post_private(
            "/info/orders",
            {
                "count": "100",
                "order_currency": order_currency,
                "payment_currency": payment_currency,
            },
            retry_safe=True,
        )
        out: list[BrokerOrder] = []
        now = int(time.time() * 1000)
        for row in data.get("data") or []:
            qty_req = float(row.get("units") or 0.0)
            qty_remain = float(row.get("units_remaining") or 0.0)
            qty_filled = max(0.0, qty_req - qty_remain)
            out.append(
                BrokerOrder(
                    client_order_id="",
                    exchange_order_id=str(row.get("order_id")),
                    side=str(row.get("type", "buy")).upper(),
                    status="PARTIAL" if qty_filled > 0 else "NEW",
                    price=float(row.get("price")) if row.get("price") else None,
                    qty_req=qty_req,
                    qty_filled=qty_filled,
                    created_ts=now,
                    updated_ts=now,
                )
            )
        return out

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
        # Bithumb private API does not expose a separate closed-order history endpoint
        # for this bot, so reuse open-order snapshots conservatively.
        return self.get_open_orders()[: max(0, int(limit))]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        fills = self.get_fills(client_order_id=None, exchange_order_id=None)
        fills.sort(key=lambda f: int(f.fill_ts), reverse=True)
        return fills[: max(0, int(limit))]
