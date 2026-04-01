from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class BrokerError(Exception):
    """Base broker exception."""


class BrokerTemporaryError(BrokerError):
    """Temporary/transient broker error (timeout, transport, 5xx)."""


class BrokerRejectError(BrokerError):
    """Exchange business reject (request was received but rejected)."""


class BrokerSchemaError(BrokerRejectError):
    """Broker/exchange response contract (schema) mismatch."""


class BrokerIdentifierMismatchError(BrokerRejectError):
    """Requested and response identifiers conflict."""


class BrokerSubmissionUnknownError(BrokerError):
    """Order submission state is unknown and requires reconciliation."""


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
    raw: dict[str, object] | None = None


@dataclass(frozen=True)
class BrokerFill:
    client_order_id: str
    fill_id: str
    fill_ts: int
    price: float
    qty: float
    fee: float
    exchange_order_id: str | None = None


@dataclass(frozen=True)
class BrokerBalance:
    cash_available: float
    cash_locked: float
    asset_available: float
    asset_locked: float

    @property
    def cash_krw(self) -> float:
        # Backward compatibility: historical callers used available cash only.
        return self.cash_available

    @property
    def asset_qty(self) -> float:
        # Backward compatibility: historical callers used available asset only.
        return self.asset_available


class Broker(Protocol):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        ...

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        ...

    def get_order(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> BrokerOrder:
        ...

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        ...

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        ...

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        ...

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        ...

    def get_balance(self) -> BrokerBalance:
        ...
