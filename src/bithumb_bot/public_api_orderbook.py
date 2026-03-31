from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from .public_api import PublicApiSchemaError, get_public_json


@dataclass(frozen=True)
class OrderbookTop:
    market: str
    bid_price: float
    ask_price: float


def _require_number(*, row: dict[str, Any], field: str) -> float:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise PublicApiSchemaError(
            f"orderbook schema mismatch field={field} expected=numeric actual={type(value).__name__}"
        )
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise PublicApiSchemaError(
            f"orderbook schema mismatch field={field} expected=numeric actual={value!r}"
        ) from exc


def _require_non_empty_list(*, payload: object, where: str) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        raise PublicApiSchemaError(
            f"orderbook schema mismatch expected=list actual={type(payload).__name__} where={where}"
        )
    if not payload:
        raise PublicApiSchemaError(f"orderbook schema mismatch expected=non-empty list where={where}")

    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise PublicApiSchemaError(
                f"orderbook schema mismatch expected=list[dict] actual_item={type(item).__name__} where={where}"
            )
        rows.append(item)
    return rows


def parse_orderbook_top(payload: object) -> list[OrderbookTop]:
    markets = _require_non_empty_list(payload=payload, where="root")

    snapshots: list[OrderbookTop] = []
    for market_item in markets:
        market = market_item.get("market")
        if not isinstance(market, str) or not market.strip():
            raise PublicApiSchemaError(
                "orderbook schema mismatch field=market expected=non-empty str"
            )

        units_payload = market_item.get("orderbook_units")
        units = _require_non_empty_list(payload=units_payload, where="orderbook_units")
        top = units[0]

        snapshots.append(
            OrderbookTop(
                market=market.strip(),
                bid_price=_require_number(row=top, field="bid_price"),
                ask_price=_require_number(row=top, field="ask_price"),
            )
        )

    return snapshots


def fetch_orderbook_top(
    client: httpx.Client,
    *,
    market: str,
) -> list[OrderbookTop]:
    market_text = str(market).strip()
    if not market_text:
        raise ValueError("market must not be empty")
    params = {"markets": market_text}
    payload = get_public_json(client, "/v1/orderbook", params=params)
    return parse_orderbook_top(payload)
