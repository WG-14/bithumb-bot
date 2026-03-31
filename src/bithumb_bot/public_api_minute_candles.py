from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from .public_api import PublicApiSchemaError, get_public_json


_ALLOWED_MINUTE_UNITS = frozenset({1, 3, 5, 10, 15, 30, 60, 240})
_REQUIRED_FIELDS = (
    "market",
    "candle_date_time_utc",
    "candle_date_time_kst",
    "opening_price",
    "high_price",
    "low_price",
    "trade_price",
    "timestamp",
    "candle_acc_trade_price",
    "candle_acc_trade_volume",
)


@dataclass(frozen=True)
class MinuteCandle:
    market: str
    candle_date_time_utc: str
    candle_date_time_kst: str
    opening_price: float
    high_price: float
    low_price: float
    trade_price: float
    timestamp: int
    candle_acc_trade_price: float
    candle_acc_trade_volume: float


def interval_to_minute_unit(interval: str) -> int:
    normalized = str(interval).strip().lower()
    if not normalized.endswith("m"):
        raise ValueError(f"unsupported minute interval: {interval}")

    minute_text = normalized[:-1]
    if not minute_text.isdigit():
        raise ValueError(f"unsupported minute interval: {interval}")

    minute_unit = int(minute_text)
    if minute_unit not in _ALLOWED_MINUTE_UNITS:
        raise ValueError(f"unsupported minute interval: {interval}")
    return minute_unit


def _require_number(*, candle: dict[str, Any], field: str) -> float:
    value = candle.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PublicApiSchemaError(f"minute candle schema mismatch field={field} expected=number actual={type(value).__name__}")
    return float(value)


def _require_int(*, candle: dict[str, Any], field: str) -> int:
    value = candle.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PublicApiSchemaError(f"minute candle schema mismatch field={field} expected=int actual={type(value).__name__}")
    return value


def _require_str(*, candle: dict[str, Any], field: str) -> str:
    value = candle.get(field)
    if not isinstance(value, str):
        raise PublicApiSchemaError(f"minute candle schema mismatch field={field} expected=str actual={type(value).__name__}")
    if not value.strip():
        raise PublicApiSchemaError(f"minute candle schema mismatch field={field} expected=non-empty str")
    return value


def parse_minute_candles(payload: object) -> list[MinuteCandle]:
    if not isinstance(payload, list):
        raise PublicApiSchemaError(
            "minute candle schema mismatch expected=list "
            f"actual={type(payload).__name__}"
        )

    candles: list[MinuteCandle] = []
    for item in payload:
        if not isinstance(item, dict):
            raise PublicApiSchemaError(
                "minute candle schema mismatch expected=list[dict] "
                f"actual_item={type(item).__name__}"
            )

        missing = [field for field in _REQUIRED_FIELDS if field not in item]
        if missing:
            raise PublicApiSchemaError(
                f"minute candle schema mismatch missing_fields={','.join(missing)}"
            )

        candles.append(
            MinuteCandle(
                market=_require_str(candle=item, field="market"),
                candle_date_time_utc=_require_str(candle=item, field="candle_date_time_utc"),
                candle_date_time_kst=_require_str(candle=item, field="candle_date_time_kst"),
                opening_price=_require_number(candle=item, field="opening_price"),
                high_price=_require_number(candle=item, field="high_price"),
                low_price=_require_number(candle=item, field="low_price"),
                trade_price=_require_number(candle=item, field="trade_price"),
                timestamp=_require_int(candle=item, field="timestamp"),
                candle_acc_trade_price=_require_number(candle=item, field="candle_acc_trade_price"),
                candle_acc_trade_volume=_require_number(candle=item, field="candle_acc_trade_volume"),
            )
        )

    return candles


def fetch_minute_candles(
    client: httpx.Client,
    *,
    market: str,
    minute_unit: int,
    count: int,
    to: str | None = None,
) -> list[MinuteCandle]:
    if minute_unit not in _ALLOWED_MINUTE_UNITS:
        raise ValueError(f"unsupported minute unit: {minute_unit}")

    endpoint = f"/v1/candles/minutes/{minute_unit}"
    params: dict[str, Any] = {
        "market": market,
        "count": count,
    }
    if to is not None:
        params["to"] = to

    payload = get_public_json(client, endpoint, params=params)
    return parse_minute_candles(payload)
