from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import httpx

from .markets import canonical_market_id
from .public_api import PublicApiSchemaError, get_public_json


_REQUIRED_FIELDS = (
    "market",
    "trade_price",
    "high_price",
    "low_price",
    "acc_trade_volume_24h",
)


@dataclass(frozen=True)
class TickerSnapshot:
    market: str
    trade_price: float
    high_price: float
    low_price: float
    acc_trade_volume_24h: float


def _require_number(*, row: dict[str, Any], field: str) -> float:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PublicApiSchemaError(
            f"ticker schema mismatch field={field} expected=number actual={type(value).__name__}"
        )
    return float(value)


def _require_str(*, row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str):
        raise PublicApiSchemaError(
            f"ticker schema mismatch field={field} expected=str actual={type(value).__name__}"
        )
    text = value.strip()
    if not text:
        raise PublicApiSchemaError(f"ticker schema mismatch field={field} expected=non-empty str")
    return text


def normalize_ticker_markets(markets: str | Iterable[str]) -> str:
    if isinstance(markets, str):
        raw_items = [item for item in markets.split(",")]
    else:
        raw_items = list(markets)

    if not raw_items:
        raise ValueError("markets must not be empty")

    normalized: list[str] = []
    seen: set[str] = set()
    for market in raw_items:
        canonical = canonical_market_id(str(market).strip())
        if canonical in seen:
            continue
        seen.add(canonical)
        normalized.append(canonical)

    if not normalized:
        raise ValueError("markets must include at least one non-empty market")
    return ",".join(normalized)


def parse_ticker_payload(payload: object) -> list[TickerSnapshot]:
    if not isinstance(payload, list):
        raise PublicApiSchemaError(f"ticker schema mismatch expected=list actual={type(payload).__name__}")

    snapshots: list[TickerSnapshot] = []
    for item in payload:
        if not isinstance(item, dict):
            raise PublicApiSchemaError(
                "ticker schema mismatch expected=list[dict] "
                f"actual_item={type(item).__name__}"
            )

        missing = [field for field in _REQUIRED_FIELDS if field not in item]
        if missing:
            raise PublicApiSchemaError(
                f"ticker schema mismatch missing_fields={','.join(missing)}"
            )

        snapshots.append(
            TickerSnapshot(
                market=_require_str(row=item, field="market"),
                trade_price=_require_number(row=item, field="trade_price"),
                high_price=_require_number(row=item, field="high_price"),
                low_price=_require_number(row=item, field="low_price"),
                acc_trade_volume_24h=_require_number(row=item, field="acc_trade_volume_24h"),
            )
        )

    return snapshots


def fetch_ticker(client: httpx.Client, *, markets: str | Iterable[str]) -> list[TickerSnapshot]:
    params = {"markets": normalize_ticker_markets(markets)}
    payload = get_public_json(client, "/v1/ticker", params=params)
    return parse_ticker_payload(payload)
