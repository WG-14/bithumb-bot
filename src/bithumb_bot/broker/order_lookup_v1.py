from __future__ import annotations

from dataclasses import dataclass

from .base import BrokerRejectError
from .order_payloads import validate_client_order_id

V1_ORDER_STATES = {"wait", "watch", "done", "cancel"}


@dataclass(frozen=True)
class V1OrderIdentifiers:
    client_order_id: str
    exchange_order_id: str


@dataclass(frozen=True)
class V1NormalizedOrder:
    side: str
    state: str
    price: float | None
    volume: float
    remaining_volume: float
    executed_volume: float
    created_ts: int
    updated_ts: int
    trades: list[object]
    executed_funds: float | None


def clean_identifier(value: object) -> str:
    return str(value or "").strip()


def resolve_identifiers(
    row: dict[str, object],
    *,
    fallback_client_order_id: str | None = None,
    fallback_exchange_order_id: str | None = None,
) -> V1OrderIdentifiers:
    exchange_order_id = clean_identifier(row.get("uuid")) or clean_identifier(fallback_exchange_order_id)
    client_order_id = clean_identifier(row.get("client_order_id")) or clean_identifier(fallback_client_order_id)
    return V1OrderIdentifiers(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )


def build_lookup_params(*, client_order_id: str | None, exchange_order_id: str | None) -> dict[str, str]:
    requested_exchange_order_id = clean_identifier(exchange_order_id)
    requested_client_order_id = clean_identifier(client_order_id)
    if requested_exchange_order_id:
        return {"uuid": requested_exchange_order_id}
    if requested_client_order_id:
        return {"client_order_id": validate_client_order_id(requested_client_order_id)}
    raise ValueError("order lookup requires exchange_order_id(uuid) or client_order_id")


def require_known_state(state: object, *, context: str) -> str:
    normalized = clean_identifier(state).lower()
    if normalized not in V1_ORDER_STATES:
        raise BrokerRejectError(f"{context} schema mismatch: unknown state '{state}'")
    return normalized


def status_from_state(*, state: str, qty_req: float, qty_filled: float) -> str:
    if state in {"wait", "watch"}:
        return "PARTIAL" if qty_filled > 0 else "NEW"
    if state == "done":
        return "FILLED"
    return "FILLED" if qty_req > 0 and qty_filled >= qty_req else "CANCELED"
