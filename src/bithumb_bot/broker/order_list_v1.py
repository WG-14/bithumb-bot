from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math

from .base import BrokerRejectError
from .order_lookup_v1 import V1_ORDER_STATES, clean_identifier
from .order_payloads import validate_client_order_id

_ORDER_BY_VALUES = {"asc", "desc"}
_MAX_IDENTIFIER_COUNT = 100
_MAX_PAGE = 10_000
_MAX_LIMIT = 100


@dataclass(frozen=True)
class OrderListQuery:
    uuids: tuple[str, ...] = ()
    client_order_ids: tuple[str, ...] = ()
    state: str | None = None
    page: int = 1
    order_by: str = "desc"
    limit: int | None = None

    def to_params(self) -> dict[str, object]:
        params: dict[str, object] = {
            "page": self.page,
            "order_by": self.order_by,
        }
        if self.uuids:
            params["uuids"] = list(self.uuids)
        if self.client_order_ids:
            params["client_order_ids"] = list(self.client_order_ids)
        if self.state is not None:
            params["state"] = self.state
        if self.limit is not None:
            params["limit"] = self.limit
        return params


@dataclass(frozen=True)
class V1ListNormalizedOrder:
    uuid: str
    client_order_id: str
    market: str
    side: str
    ord_type: str
    state: str
    price: float
    volume: float
    remaining_volume: float
    executed_volume: float
    created_ts: int
    updated_ts: int
    executed_funds: float | None


def _validate_identifier_list(values: list[str], *, field_name: str) -> tuple[str, ...]:
    if len(values) > _MAX_IDENTIFIER_COUNT:
        raise ValueError(f"{field_name} allows at most {_MAX_IDENTIFIER_COUNT} items")
    out: list[str] = []
    for raw in values:
        cleaned = clean_identifier(raw)
        if not cleaned:
            raise ValueError(f"{field_name} must not include empty identifiers")
        out.append(cleaned)
    return tuple(out)


def _required_text(row: dict[str, object], key: str, *, context: str) -> str:
    value = clean_identifier(row.get(key))
    if not value:
        raise BrokerRejectError(f"{context} schema mismatch: missing required field '{key}'")
    return value


def _required_number(row: dict[str, object], key: str, *, context: str) -> float:
    raw = row.get(key)
    if raw in (None, ""):
        raise BrokerRejectError(f"{context} schema mismatch: missing required numeric field '{key}'")
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise BrokerRejectError(f"{context} schema mismatch: invalid numeric field '{key}'={raw}") from exc
    if not math.isfinite(parsed):
        raise BrokerRejectError(f"{context} schema mismatch: non-finite numeric field '{key}'={raw}")
    return parsed


def _optional_number(row: dict[str, object], key: str, *, context: str) -> float | None:
    raw = row.get(key)
    if raw in (None, ""):
        return None
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise BrokerRejectError(f"{context} schema mismatch: invalid numeric field '{key}'={raw}") from exc
    if not math.isfinite(parsed):
        raise BrokerRejectError(f"{context} schema mismatch: non-finite numeric field '{key}'={raw}")
    return parsed


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
            raise BrokerRejectError(f"{context} schema mismatch: invalid timestamp field '{field_name}'={raw}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    if not math.isfinite(value):
        raise BrokerRejectError(f"{context} schema mismatch: non-finite timestamp field '{field_name}'={raw}")
    if value > 1_000_000_000_000:
        return int(value)
    return int(value * 1000)


def _normalize_side(side: object, *, context: str) -> str:
    normalized = clean_identifier(side).lower()
    if normalized in {"bid", "buy"}:
        return "BUY"
    if normalized in {"ask", "sell"}:
        return "SELL"
    raise BrokerRejectError(f"{context} schema mismatch: unknown side '{side}'")


def parse_v1_order_list_row(row: dict[str, object]) -> V1ListNormalizedOrder:
    context = "/v1/orders"
    uuid = clean_identifier(row.get("uuid"))
    client_order_id = clean_identifier(row.get("client_order_id"))
    if not uuid and not client_order_id:
        raise BrokerRejectError(f"{context} schema mismatch: missing both uuid and client_order_id")

    state = clean_identifier(row.get("state")).lower()
    if state not in V1_ORDER_STATES:
        raise BrokerRejectError(f"{context} schema mismatch: unknown state '{row.get('state')}'")

    return V1ListNormalizedOrder(
        uuid=uuid,
        client_order_id=client_order_id,
        market=_required_text(row, "market", context=context),
        side=_normalize_side(row.get("side"), context=context),
        ord_type=_required_text(row, "ord_type", context=context),
        state=state,
        price=_required_number(row, "price", context=context),
        volume=_required_number(row, "volume", context=context),
        remaining_volume=_required_number(row, "remaining_volume", context=context),
        executed_volume=_required_number(row, "executed_volume", context=context),
        created_ts=_strict_parse_ts(row.get("created_at"), field_name="created_at", context=context),
        updated_ts=_strict_parse_ts(row.get("updated_at"), field_name="updated_at", context=context),
        executed_funds=_optional_number(row, "executed_funds", context=context),
    )


def build_order_list_params(
    *,
    uuids: list[str] | tuple[str, ...] | None = None,
    client_order_ids: list[str] | tuple[str, ...] | None = None,
    state: str | None = None,
    page: int = 1,
    order_by: str = "desc",
    limit: int | None = None,
) -> dict[str, object]:
    uuid_values = _validate_identifier_list(list(uuids or []), field_name="uuids")
    client_values = _validate_identifier_list(
        [validate_client_order_id(value) for value in list(client_order_ids or [])],
        field_name="client_order_ids",
    )
    if not uuid_values and not client_values:
        raise ValueError("order list lookup requires uuids or client_order_ids")

    normalized_state = clean_identifier(state).lower() if state is not None else None
    if normalized_state is not None and normalized_state not in V1_ORDER_STATES:
        raise ValueError(f"state must be one of {sorted(V1_ORDER_STATES)}")

    normalized_page = int(page)
    if normalized_page < 1 or normalized_page > _MAX_PAGE:
        raise ValueError(f"page must be between 1 and {_MAX_PAGE}")

    normalized_order_by = clean_identifier(order_by).lower()
    if normalized_order_by not in _ORDER_BY_VALUES:
        raise ValueError(f"order_by must be one of {sorted(_ORDER_BY_VALUES)}")

    normalized_limit: int | None = None
    if limit is not None:
        normalized_limit = int(limit)
        if normalized_limit < 1 or normalized_limit > _MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {_MAX_LIMIT}")

    return OrderListQuery(
        uuids=uuid_values,
        client_order_ids=client_values,
        state=normalized_state,
        page=normalized_page,
        order_by=normalized_order_by,
        limit=normalized_limit,
    ).to_params()
