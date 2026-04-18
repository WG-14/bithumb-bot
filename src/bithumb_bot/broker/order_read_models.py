from __future__ import annotations

import math
import time
from datetime import datetime, timezone

from .base import BrokerRejectError
from .order_lookup_v1 import V1NormalizedOrder, require_known_state as require_v1_known_state


def normalize_order_side(side: str | None, *, default: str = "BUY") -> str:
    token = str(side or "").strip().lower()
    if token in {"buy", "bid"}:
        return "BUY"
    if token in {"sell", "ask"}:
        return "SELL"
    return default


def parse_ts(raw: object) -> int:
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
    return int(value * 1000)


def strict_parse_ts(raw: object, *, field_name: str, context: str) -> int:
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


def number(payload: dict[str, object], *keys: str) -> float:
    for key in keys:
        raw = payload.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return 0.0


def strict_optional_number(payload: dict[str, object], key: str, *, context: str) -> float | None:
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


def optional_number(payload: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        raw = payload.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def resolve_fill_price(
    payload: dict[str, object],
    *,
    normalized_row: dict[str, object] | None = None,
) -> float | None:
    candidates: tuple[float | None, ...] = (
        optional_number(payload, "price", "trade_price", "avg_price", "avg_execution_price"),
        optional_number(payload, "cont_price", "contract_price"),
        optional_number(payload, "order_price"),
        optional_number(payload, "price_avg"),
        (float(normalized_row["price"]) if normalized_row and normalized_row.get("price") is not None else None),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        if candidate > 0:
            return float(candidate)
    return None


def normalize_v2_order_row(row: dict[str, object]) -> dict[str, object]:
    volume = number(row, "volume", "units")
    remaining = number(row, "remaining_volume", "units_remaining")
    executed = number(row, "executed_volume")
    if executed <= 0 and volume > 0 and remaining >= 0:
        executed = max(0.0, volume - remaining)
    return {
        "uuid": str(row.get("uuid") or row.get("order_id") or ""),
        "side": normalize_order_side(str(row.get("side") or row.get("type")), default="BUY"),
        "state": str(row.get("state") or ""),
        "price": resolve_fill_price(row),
        "volume": volume,
        "remaining_volume": remaining,
        "executed_volume": executed,
        "created_ts": parse_ts(row.get("created_at") or row.get("timestamp")),
        "updated_ts": parse_ts(row.get("updated_at") or row.get("created_at") or row.get("timestamp")),
        "trades": row.get("trades") if isinstance(row.get("trades"), list) else [],
    }


def normalize_v1_order_row_lenient_for_fills(row: dict[str, object]) -> dict[str, object]:
    volume = number(row, "volume")
    remaining = number(row, "remaining_volume")
    executed = number(row, "executed_volume")
    if executed <= 0 and volume > 0 and remaining >= 0:
        executed = max(0.0, volume - remaining)
    return {
        "uuid": str(row.get("uuid") or ""),
        "side": normalize_order_side(str(row.get("side")), default="BUY"),
        "state": str(row.get("state") or ""),
        "price": resolve_fill_price(row),
        "volume": volume,
        "remaining_volume": remaining,
        "executed_volume": executed,
        "created_ts": parse_ts(row.get("created_at")),
        "updated_ts": parse_ts(row.get("updated_at") or row.get("created_at")),
        "trades": row.get("trades") if isinstance(row.get("trades"), list) else [],
    }


def normalize_v1_order_row_strict(row: dict[str, object]) -> V1NormalizedOrder:
    context = "/v1/order"
    state = require_v1_known_state(row.get("state"), context=context)
    volume = strict_optional_number(row, "volume", context=context)
    if volume is None:
        volume = strict_optional_number(row, "units", context=context)

    remaining = strict_optional_number(row, "remaining_volume", context=context)
    if remaining is None:
        remaining = strict_optional_number(row, "units_remaining", context=context)

    executed = strict_optional_number(row, "executed_volume", context=context)
    if executed is None:
        executed = strict_optional_number(row, "filled_volume", context=context)

    if remaining is None and volume is not None and executed is not None:
        remaining = max(0.0, volume - executed)
    if executed is None and volume is not None and remaining is not None:
        executed = max(0.0, volume - remaining)
    if volume is None and remaining is not None and executed is not None:
        volume = max(0.0, remaining + executed)

    executed_funds = strict_optional_number(row, "executed_funds", context=context)
    price = strict_optional_number(row, "price", context=context)
    avg_price = strict_optional_number(row, "avg_price", context=context)
    reference_price = avg_price if avg_price is not None and avg_price > 0 else price
    if volume is None and state == "done":
        if executed is not None:
            volume = max(0.0, executed)
        elif executed_funds is not None and reference_price is not None and reference_price > 0:
            volume = max(0.0, executed_funds / reference_price)
            executed = volume
    if remaining is None and state == "done":
        remaining = 0.0
    if volume is None:
        raise BrokerRejectError(f"{context} schema mismatch: missing required numeric field 'volume'")
    if remaining is None:
        raise BrokerRejectError(f"{context} schema mismatch: missing required numeric field 'remaining_volume'")
    if executed is None:
        executed = max(0.0, volume - remaining)

    raw_trades = row.get("trades")
    if raw_trades is None:
        trades: list[object] = []
    elif isinstance(raw_trades, list):
        trades = raw_trades
    else:
        raise BrokerRejectError(f"{context} schema mismatch: trades must be a list when present")

    created_ts = strict_parse_ts(row.get("created_at"), field_name="created_at", context=context)
    updated_raw = row.get("updated_at")
    updated_ts = (
        strict_parse_ts(updated_raw, field_name="updated_at", context=context)
        if updated_raw not in (None, "")
        else created_ts
    )
    return V1NormalizedOrder(
        side=normalize_order_side(str(row.get("side")), default="BUY"),
        state=state,
        price=price,
        volume=volume,
        remaining_volume=remaining,
        executed_volume=executed,
        created_ts=created_ts,
        updated_ts=updated_ts,
        trades=trades,
        executed_funds=executed_funds,
    )
