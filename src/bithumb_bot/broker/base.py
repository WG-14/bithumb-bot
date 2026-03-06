from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class BrokerOrder:
    client_order_id: str
    exchange_order_id: str | None
    side: str
    status: str
    price: float | None
    qty_req: float
    qty_filled: float
    created_ts: int
    updated_ts: int


@dataclass(frozen=True)
class BrokerFill:
    client_order_id: str
    fill_id: str
    fill_ts: int
    price: float
    qty: float
    fee: float


@dataclass(frozen=True)
class BrokerBalance:
    cash_krw: float
    asset_qty: float


class Broker(Protocol):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        ...

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        ...

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        ...

    def get_open_orders(self) -> list[BrokerOrder]:
        ...

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        ...

    def get_balance(self) -> BrokerBalance:
        ...
