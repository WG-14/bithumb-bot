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
    volume: float | None
    remaining_volume: float | None
    executed_volume: float | None
    created_ts: int
    updated_ts: int
    executed_funds: float | None
    paid_fee: float | None
    degraded_fields: tuple[str, ...]


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


def _first_optional_number(
    row: dict[str, object],
    keys: tuple[str, ...],
    *,
    context: str,
) -> float | None:
    for key in keys:
        value = _optional_number(row, key, context=context)
        if value is not None:
            return value
    return None


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


def _try_parse_ts(raw: object, *, field_name: str, context: str) -> int | None:
    if raw in (None, ""):
        return None
    try:
        return _strict_parse_ts(raw, field_name=field_name, context=context)
    except BrokerRejectError:
        return None


def _trade_timestamp_fallback(row: dict[str, object], *, context: str) -> int | None:
    trades = row.get("trades")
    if not isinstance(trades, list):
        return None
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        for key in ("updated_at", "created_at", "ordered_at", "trade_timestamp", "timestamp", "trade_at"):
            parsed = _try_parse_ts(trade.get(key), field_name=f"trades.{key}", context=context)
            if parsed is not None:
                return parsed
    return None


def _resolve_order_timestamps(row: dict[str, object], *, context: str) -> tuple[int, int, tuple[str, ...]]:
    degraded: list[str] = []
    ordered_aliases = ("ordered_at", "order_at", "order_time", "order_timestamp")

    updated_ts = _try_parse_ts(row.get("updated_at"), field_name="updated_at", context=context)
    if updated_ts is None:
        created_as_updated = _try_parse_ts(row.get("created_at"), field_name="created_at", context=context)
        if created_as_updated is not None:
            updated_ts = created_as_updated
            degraded.append("updated_at:derived_from_created_at")

    if updated_ts is None:
        for key in ordered_aliases:
            alias_ts = _try_parse_ts(row.get(key), field_name=key, context=context)
            if alias_ts is not None:
                updated_ts = alias_ts
                degraded.append(f"updated_at:derived_from_{key}")
                break

    if updated_ts is None:
        trade_ts = _trade_timestamp_fallback(row, context=context)
        if trade_ts is not None:
            updated_ts = trade_ts
            degraded.append("updated_at:derived_from_trade_timestamp")

    created_ts = _try_parse_ts(row.get("created_at"), field_name="created_at", context=context)
    if created_ts is None:
        for key in ordered_aliases:
            alias_ts = _try_parse_ts(row.get(key), field_name=key, context=context)
            if alias_ts is not None:
                created_ts = alias_ts
                degraded.append(f"created_at:derived_from_{key}")
                break

    if created_ts is None and updated_ts is not None:
        created_ts = updated_ts
        degraded.append("created_at:derived_from_updated_at")

    if created_ts is None:
        trade_ts = _trade_timestamp_fallback(row, context=context)
        if trade_ts is not None:
            created_ts = trade_ts
            degraded.append("created_at:derived_from_trade_timestamp")

    if created_ts is None and updated_ts is None:
        created_ts = 0
        updated_ts = 0
        degraded.append("timestamps:missing_defaulted_zero")
    elif created_ts is None:
        created_ts = updated_ts or 0
        degraded.append("created_at:derived_from_updated_at")
    elif updated_ts is None:
        updated_ts = created_ts
        degraded.append("updated_at:derived_from_created_ts")

    return created_ts, updated_ts, tuple(degraded)


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

    degraded_fields: list[str] = []
    volume = _first_optional_number(row, ("volume", "units"), context=context)
    remaining_volume = _first_optional_number(row, ("remaining_volume", "units_remaining"), context=context)
    executed_volume = _first_optional_number(row, ("executed_volume", "filled_volume"), context=context)
    executed_funds = _optional_number(row, "executed_funds", context=context)
    paid_fee = _first_optional_number(
        row,
        ("paid_fee", "trade_fee", "fee", "reserved_fee", "remaining_fee"),
        context=context,
    )
    price = _required_number(row, "price", context=context)
    avg_price = _first_optional_number(row, ("avg_price", "average_price"), context=context)
    reference_price = avg_price if avg_price is not None and avg_price > 0 else price

    if remaining_volume is None and volume is not None and executed_volume is not None:
        remaining_volume = max(0.0, volume - executed_volume)
    elif remaining_volume is None:
        degraded_fields.append("remaining_volume")
    if executed_volume is None and volume is not None and remaining_volume is not None:
        executed_volume = max(0.0, volume - remaining_volume)
    elif executed_volume is None:
        degraded_fields.append("executed_volume")
    if volume is None and remaining_volume is not None and executed_volume is not None:
        volume = max(0.0, remaining_volume + executed_volume)
    elif volume is None:
        if state == "done" and executed_volume is not None:
            volume = max(0.0, executed_volume)
            degraded_fields.append("volume:derived_from_executed_volume")
        elif state == "done" and executed_funds is not None and reference_price > 0:
            volume = max(0.0, executed_funds / reference_price)
            if executed_volume is None:
                executed_volume = volume
            if remaining_volume is None:
                remaining_volume = 0.0
            degraded_fields.append("volume:derived_from_executed_funds")
        else:
            degraded_fields.append("volume")

    if executed_volume is None:
        if volume is not None and remaining_volume is not None:
            executed_volume = max(0.0, volume - remaining_volume)
        elif state == "done":
            executed_volume = max(0.0, volume or 0.0)
    if remaining_volume is None and state == "done":
        remaining_volume = 0.0

    if state in {"wait", "watch"} and remaining_volume is None:
        raise BrokerRejectError(f"{context} schema mismatch: missing required numeric field 'remaining_volume'")
    if state in {"wait", "watch"} and volume is None and executed_volume is None:
        raise BrokerRejectError(
            f"{context} schema mismatch: missing required numeric fields 'volume' and 'executed_volume'"
        )

    created_ts, updated_ts, degraded_ts_fields = _resolve_order_timestamps(row, context=context)
    degraded_fields.extend(degraded_ts_fields)

    return V1ListNormalizedOrder(
        uuid=uuid,
        client_order_id=client_order_id,
        market=_required_text(row, "market", context=context),
        side=_normalize_side(row.get("side"), context=context),
        ord_type=_required_text(row, "ord_type", context=context),
        state=state,
        price=price,
        volume=volume,
        remaining_volume=remaining_volume,
        executed_volume=executed_volume,
        created_ts=created_ts,
        updated_ts=updated_ts,
        executed_funds=executed_funds,
        paid_fee=paid_fee,
        degraded_fields=tuple(degraded_fields),
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
