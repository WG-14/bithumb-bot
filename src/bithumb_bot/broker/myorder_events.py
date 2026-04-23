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
    canonicalization_version: str
    raw_payload: dict[str, Any]
    raw_event_type: str
    event_type: str
    exchange_state: str
    event_ts_ms: int
    trade_ts_ms: int | None
    order_ts_ms: int | None
    client_order_id: str
    exchange_order_id: str
    trade_uuid: str
    side: str
    ask_bid: str
    order_type: str
    status: str
    qty: float | None
    executed_volume: float | None
    remaining_volume: float | None
    executed_funds: float | None
    price: float | None
    fee: float | None
    fee_status: str
    fee_warning: str | None
    fill_id: str
    dedupe_key: str

    @property
    def is_fill_event(self) -> bool:
        return self.exchange_state == "trade"

    @property
    def fill_ts_ms(self) -> int:
        return int(self.trade_ts_ms if self.trade_ts_ms is not None else self.event_ts_ms)


def normalize_myorder_event_payload(payload: dict[str, Any]) -> NormalizedMyOrderEvent:
    if not isinstance(payload, dict):
        raise TypeError(f"myorder payload must be dict, got {type(payload).__name__}")

    canonicalization_version = "bithumb_myorder_v1"
    exchange_state = _clean_text(payload.get("state") or payload.get("status") or payload.get("order_state")).lower()
    raw_event_type = _clean_text(
        payload.get("event_type")
        or payload.get("type")
        or payload.get("event")
        or payload.get("action")
    )
    if exchange_state == "trade":
        status = "PARTIAL"
    elif exchange_state == "cancel":
        status = "CANCELED"
    else:
        status = _normalize_status(exchange_state)
    event_type = _normalize_event_type(raw_event_type, status)
    client_order_id = _clean_text(payload.get("client_order_id") or payload.get("coid"))
    exchange_order_id = _clean_text(payload.get("uuid") or payload.get("order_id"))
    trade_uuid = _clean_text(payload.get("trade_uuid"))
    ask_bid = _clean_text(payload.get("ask_bid")).lower()
    if ask_bid == "bid":
        side = "BUY"
    elif ask_bid == "ask":
        side = "SELL"
    else:
        side = _clean_text(payload.get("side") or payload.get("order_side") or "BUY").upper()
    order_type = _clean_text(payload.get("ord_type") or payload.get("order_type") or payload.get("kind"))
    volume = _optional_float(payload.get("volume") or payload.get("qty"))
    executed_volume = _optional_float(payload.get("executed_volume"))
    remaining_volume = _optional_float(payload.get("remaining_volume"))
    executed_funds = _optional_float(payload.get("executed_funds"))
    if exchange_state == "trade":
        qty = volume
    else:
        qty = executed_volume if executed_volume is not None else volume
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
    legacy_fill_id = _clean_text(payload.get("trade_id") or payload.get("fill_id"))
    fill_id = trade_uuid if trade_uuid else legacy_fill_id
    event_ts_ms = _optional_int(
        payload.get("timestamp")
        or payload.get("ts")
        or payload.get("event_ts")
        or payload.get("created_at")
        or payload.get("updated_at")
    ) or int(time.time() * 1000)
    trade_ts_ms = _optional_int(payload.get("trade_timestamp"))
    order_ts_ms = _optional_int(payload.get("order_timestamp"))
    canonical_digest_payload = {
        "canonicalization_version": canonicalization_version,
        "event_type": event_type,
        "exchange_state": exchange_state,
        "client_order_id": client_order_id,
        "exchange_order_id": exchange_order_id,
        "trade_uuid": trade_uuid,
        "status": status,
        "side": side,
        "ask_bid": ask_bid,
        "order_type": order_type,
        "qty": qty,
        "executed_volume": executed_volume,
        "remaining_volume": remaining_volume,
        "executed_funds": executed_funds,
        "price": price,
        "fee": fee,
        "fee_status": fee_status,
        "fill_id": fill_id,
        "event_ts_ms": event_ts_ms,
        "trade_ts_ms": trade_ts_ms,
        "order_ts_ms": order_ts_ms,
    }
    dedupe_key = hashlib.sha256(
        json.dumps(canonical_digest_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return NormalizedMyOrderEvent(
        canonicalization_version=canonicalization_version,
        raw_payload=dict(payload),
        raw_event_type=raw_event_type,
        event_type=event_type,
        exchange_state=exchange_state,
        event_ts_ms=event_ts_ms,
        trade_ts_ms=trade_ts_ms,
        order_ts_ms=order_ts_ms,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        trade_uuid=trade_uuid,
        side=side,
        ask_bid=ask_bid,
        order_type=order_type,
        status=status,
        qty=qty,
        executed_volume=executed_volume,
        remaining_volume=remaining_volume,
        executed_funds=executed_funds,
        price=price,
        fee=fee,
        fee_status=fee_status,
        fee_warning=fee_warning,
        fill_id=fill_id,
        dedupe_key=dedupe_key,
    )
