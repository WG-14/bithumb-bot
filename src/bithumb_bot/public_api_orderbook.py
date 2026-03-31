from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import httpx

from .markets import normalize_market_id
from .public_api import PublicApiSchemaError, get_public_json_with_retry


@dataclass(frozen=True)
class OrderbookUnit:
    bid_price: float
    ask_price: float


@dataclass(frozen=True)
class OrderbookSnapshot:
    market: str
    orderbook_units: tuple[OrderbookUnit, ...]


@dataclass(frozen=True)
class BestQuote:
    market: str
    bid_price: float
    ask_price: float
    observed_at_epoch_sec: float | None = None
    source: str | None = None


OrderbookTop = BestQuote


def _require_number(*, row: dict[str, Any], field: str) -> float:
    if field not in row:
        raise PublicApiSchemaError(f"orderbook schema mismatch field={field} expected=present")
    value = row[field]
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


def _require_row_dict(*, payload: object, where: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise PublicApiSchemaError(
            f"orderbook schema mismatch expected=dict actual={type(payload).__name__} where={where}"
        )
    return payload


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


def parse_orderbook_snapshots(payload: object) -> list[OrderbookSnapshot]:
    markets = _require_non_empty_list(payload=payload, where="root")

    snapshots: list[OrderbookSnapshot] = []
    for market_item in markets:
        market = market_item.get("market")
        if not isinstance(market, str) or not market.strip():
            raise PublicApiSchemaError(
                "orderbook schema mismatch field=market expected=non-empty str"
            )

        units_payload = market_item.get("orderbook_units")
        units = _require_non_empty_list(payload=units_payload, where="orderbook_units")
        parsed_units: list[OrderbookUnit] = []
        for idx, raw_unit in enumerate(units):
            unit = _require_row_dict(payload=raw_unit, where=f"orderbook_units[{idx}]")
            parsed_units.append(
                OrderbookUnit(
                    bid_price=_require_number(row=unit, field="bid_price"),
                    ask_price=_require_number(row=unit, field="ask_price"),
                )
            )

        snapshots.append(
            OrderbookSnapshot(
                market=market.strip(),
                orderbook_units=tuple(parsed_units),
            )
        )

    return snapshots


def extract_top_quote(snapshot: OrderbookSnapshot) -> BestQuote:
    if not snapshot.orderbook_units:
        raise PublicApiSchemaError(
            f"orderbook schema mismatch expected=non-empty orderbook_units market={snapshot.market!r}"
        )
    top = snapshot.orderbook_units[0]
    return BestQuote(market=snapshot.market, bid_price=top.bid_price, ask_price=top.ask_price)


def extract_top_quotes(snapshots: list[OrderbookSnapshot]) -> list[BestQuote]:
    return [extract_top_quote(snapshot) for snapshot in snapshots]


def parse_orderbook_top(payload: object) -> list[OrderbookTop]:
    return extract_top_quotes(parse_orderbook_snapshots(payload))


def _canonicalize_market_set(markets: Sequence[str]) -> set[str]:
    return {normalize_market_id(market) for market in markets}


def _canonicalize_requested_markets(markets: Sequence[str]) -> list[str]:
    if isinstance(markets, str):
        raise TypeError("markets must be a sequence of market identifiers, not a string")

    canonicalized: list[str] = []
    seen: set[str] = set()
    for market in markets:
        canonical = normalize_market_id(market)
        if canonical in seen:
            continue
        seen.add(canonical)
        canonicalized.append(canonical)

    if not canonicalized:
        raise ValueError("markets must not be empty")

    return canonicalized


def _validate_batch_market_response(
    *,
    requested_markets: Sequence[str],
    snapshots: list[OrderbookSnapshot],
    endpoint: str,
) -> list[OrderbookSnapshot]:
    requested_market_set = _canonicalize_market_set(requested_markets)
    returned_markets = [snapshot.market for snapshot in snapshots]
    returned_market_set = _canonicalize_market_set(returned_markets)

    if len(snapshots) != len(requested_market_set) or returned_market_set != requested_market_set:
        raise PublicApiSchemaError(
            "orderbook response market mismatch "
            f"endpoint={endpoint} "
            f"requested_markets={sorted(requested_market_set)} "
            f"returned_markets={sorted(returned_market_set)} "
            f"returned_count={len(snapshots)}"
        )

    return snapshots


def fetch_orderbook_snapshots(
    client: httpx.Client,
    *,
    markets: Sequence[str],
    max_retries: int = 3,
) -> list[OrderbookSnapshot]:
    requested_markets = _canonicalize_requested_markets(markets)
    params = {"markets": ",".join(requested_markets)}
    endpoint = "/v1/orderbook"
    payload = get_public_json_with_retry(
        client,
        endpoint,
        params=params,
        max_retries=max_retries,
    )
    try:
        snapshots = parse_orderbook_snapshots(payload)
        validated_snapshots = _validate_batch_market_response(
            requested_markets=requested_markets,
            snapshots=snapshots,
            endpoint=endpoint,
        )
    except PublicApiSchemaError as exc:
        raise PublicApiSchemaError(
            "orderbook schema validation failed "
            f"endpoint={endpoint} requested_markets={requested_markets} params={params} "
            f"detail={exc}"
        ) from exc
    return validated_snapshots


def fetch_orderbook_snapshot(
    client: httpx.Client,
    *,
    market: str,
    max_retries: int = 3,
) -> OrderbookSnapshot:
    snapshots = fetch_orderbook_snapshots(client, markets=[market], max_retries=max_retries)
    return snapshots[0]


def fetch_orderbook_tops(
    client: httpx.Client,
    *,
    markets: Sequence[str],
    max_retries: int = 3,
) -> list[OrderbookTop]:
    return extract_top_quotes(fetch_orderbook_snapshots(client, markets=markets, max_retries=max_retries))


def fetch_orderbook_top(
    client: httpx.Client,
    *,
    market: str,
    max_retries: int = 3,
) -> list[OrderbookTop]:
    return fetch_orderbook_tops(client, markets=[market], max_retries=max_retries)
