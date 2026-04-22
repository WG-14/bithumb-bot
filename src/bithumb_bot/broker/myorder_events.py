from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import time
from typing import Any


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed


def _normalize_status(raw_status: str) -> str:
    token = raw_status.strip().lower()
    if token in {"wait", "open", "new", "pending"}:
        return "NEW"
    if token in {"done", "filled", "executed", "complete"}:
        return "FILLED"
    if token in {"cancel", "canceled", "cancelled", "requested_cancel", "cancel_requested"}:
        return "CANCEL_REQUESTED"
    if token in {"partial", "partially_filled", "partial_fill"}:
        return "PARTIAL"
    if token in {"fail", "failed", "rejected"}:
        return "FAILED"
    return raw_status.strip().upper() or "UNKNOWN"


def _normalize_event_type(raw_event_type: str, status: str) -> str:
    token = raw_event_type.strip().lower()
    if token:
        return token
    if status == "FILLED":
        return "order_filled"
    if status == "CANCEL_REQUESTED":
        return "order_cancel_requested"
    if status == "PARTIAL":
        return "order_partially_filled"
    return "order_update"


@dataclass(frozen=True)
class NormalizedMyOrderEvent:
    raw_payload: dict[str, Any]
    raw_event_type: str
    event_type: str
    event_ts_ms: int
    client_order_id: str
    exchange_order_id: str
    side: str
    order_type: str
    status: str
    qty: float | None
    price: float | None
    fee: float | None
    fee_status: str
    fee_warning: str | None
    fill_id: str
    dedupe_key: str


def normalize_myorder_event_payload(payload: dict[str, Any]) -> NormalizedMyOrderEvent:
    if not isinstance(payload, dict):
        raise TypeError(f"myorder payload must be dict, got {type(payload).__name__}")

    raw_event_type = _clean_text(
        payload.get("event_type")
        or payload.get("type")
        or payload.get("event")
        or payload.get("action")
    )
    raw_status = _clean_text(payload.get("state") or payload.get("status") or payload.get("order_state"))
    status = _normalize_status(raw_status)
    event_type = _normalize_event_type(raw_event_type, status)
    client_order_id = _clean_text(payload.get("client_order_id") or payload.get("coid"))
    exchange_order_id = _clean_text(payload.get("uuid") or payload.get("order_id"))
    side = _clean_text(payload.get("side") or payload.get("order_side") or payload.get("type") or "BUY").upper()
    order_type = _clean_text(payload.get("ord_type") or payload.get("order_type") or payload.get("kind"))
    qty = _optional_float(payload.get("executed_volume") or payload.get("volume") or payload.get("qty"))
    price = _optional_float(payload.get("price") or payload.get("trade_price") or payload.get("avg_price"))
    trade_level_fee_keys = ("fee", "commission", "trade_fee", "transaction_fee", "fee_amount")
    order_level_fee_keys = ("paid_fee", "reserved_fee", "remaining_fee")
    fee_keys = trade_level_fee_keys + order_level_fee_keys
    fee = None
    fee_status = "missing"
    fee_warning = "missing_fee_field"
    for key in fee_keys:
        if key not in payload:
            continue
        raw_fee = payload.get(key)
        if raw_fee in (None, ""):
            fee_status = "empty"
            fee_warning = f"empty_fee_field:{key}"
            break
        parsed_fee = _optional_float(raw_fee)
        if parsed_fee is None or parsed_fee < 0.0:
            fee_status = "invalid"
            fee_warning = f"invalid_fee_field:{key}"
            break
        fee = float(parsed_fee)
        if fee == 0.0:
            fee_status = "zero_reported"
            fee_warning = f"zero_fee_field:{key}"
        elif key in order_level_fee_keys:
            fee_status = "order_level_candidate"
            fee_warning = f"order_level_fee_candidate:{key}"
        else:
            fee_status = "complete"
            fee_warning = None
        break
    fill_id = _clean_text(payload.get("trade_id") or payload.get("fill_id") or payload.get("uuid") or payload.get("order_id"))
    event_ts_ms = _optional_int(
        payload.get("timestamp")
        or payload.get("ts")
        or payload.get("event_ts")
        or payload.get("created_at")
        or payload.get("updated_at")
    ) or int(time.time() * 1000)
    canonical_digest_payload = {
        "event_type": event_type,
        "client_order_id": client_order_id,
        "exchange_order_id": exchange_order_id,
        "status": status,
        "side": side,
        "order_type": order_type,
        "qty": qty,
        "price": price,
        "fee": fee,
        "fee_status": fee_status,
        "fill_id": fill_id,
        "event_ts_ms": event_ts_ms,
    }
    dedupe_key = hashlib.sha256(
        json.dumps(canonical_digest_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return NormalizedMyOrderEvent(
        raw_payload=dict(payload),
        raw_event_type=raw_event_type,
        event_type=event_type,
        event_ts_ms=event_ts_ms,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        side=side,
        order_type=order_type,
        status=status,
        qty=qty,
        price=price,
        fee=fee,
        fee_status=fee_status,
        fee_warning=fee_warning,
        fill_id=fill_id,
        dedupe_key=dedupe_key,
    )
