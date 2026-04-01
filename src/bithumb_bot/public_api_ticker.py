from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import httpx

from .markets import ExchangeMarketCodeError, parse_documented_market_code
from .public_api import PublicApiSchemaError, get_public_json


_DOCUMENTED_TICKER_FIELDS = (
    "market",
    "trade_date",
    "trade_time",
    "trade_date_kst",
    "trade_time_kst",
    "trade_timestamp",
    "opening_price",
    "high_price",
    "low_price",
    "trade_price",
    "prev_closing_price",
    "change",
    "change_price",
    "signed_change_price",
    "change_rate",
    "signed_change_rate",
    "acc_trade_price",
    "acc_trade_price_24h",
    "acc_trade_volume",
    "acc_trade_volume_24h",
)


@dataclass(frozen=True)
class TickerResponseRow:
    """Full documented /v1/ticker response row model.

    This keeps the official row shape intact so future strategy/reporting/alert layers
    can consume richer ticker context without re-parsing raw payloads.
    """

    market: str
    trade_date: str
    trade_time: str
    trade_date_kst: str
    trade_time_kst: str
    trade_timestamp: int
    opening_price: float
    high_price: float
    low_price: float
    trade_price: float
    prev_closing_price: float
    change: str
    change_price: float
    signed_change_price: float
    change_rate: float
    signed_change_rate: float
    acc_trade_price: float
    acc_trade_price_24h: float
    acc_trade_volume: float
    acc_trade_volume_24h: float


@dataclass(frozen=True)
class TickerLiteSnapshot:
    """Subset ticker model used by the bot's current market-data flow.

    This is intentionally *not* the full documented /v1/ticker response model.
    Only fields required by the current system are validated and retained.
    """

    market: str
    trade_price: float
    high_price: float
    low_price: float
    acc_trade_volume_24h: float

    @classmethod
    def from_response_row(cls, row: TickerResponseRow) -> "TickerLiteSnapshot":
        return cls(
            market=row.market,
            trade_price=row.trade_price,
            high_price=row.high_price,
            low_price=row.low_price,
            acc_trade_volume_24h=row.acc_trade_volume_24h,
        )


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


def _require_int(*, row: dict[str, Any], field: str) -> int:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PublicApiSchemaError(
            f"ticker schema mismatch field={field} expected=int actual={type(value).__name__}"
        )
    return value


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
        canonical_market = parse_documented_market_code(str(market).strip())
        if canonical_market in seen:
            continue
        seen.add(canonical_market)
        normalized.append(canonical_market)

    if not normalized:
        raise ValueError("markets must include at least one non-empty market")
    return ",".join(normalized)


def normalize_single_ticker_market(market: str) -> str:
    canonical_market = normalize_ticker_markets([market])
    if "," in canonical_market:
        raise ValueError(f"single market expected exactly one market actual={canonical_market!r}")
    return canonical_market


def parse_ticker_response_payload(payload: object) -> list[TickerResponseRow]:
    if not isinstance(payload, list):
        raise PublicApiSchemaError(f"ticker schema mismatch expected=list actual={type(payload).__name__}")

    rows: list[TickerResponseRow] = []
    for item in payload:
        if not isinstance(item, dict):
            raise PublicApiSchemaError(
                "ticker schema mismatch expected=list[dict] "
                f"actual_item={type(item).__name__}"
            )

        missing = [field for field in _DOCUMENTED_TICKER_FIELDS if field not in item]
        if missing:
            raise PublicApiSchemaError(
                f"ticker schema mismatch missing_fields={','.join(missing)}"
            )

        rows.append(
            TickerResponseRow(
                market=parse_documented_market_code(_require_str(row=item, field="market")),
                trade_date=_require_str(row=item, field="trade_date"),
                trade_time=_require_str(row=item, field="trade_time"),
                trade_date_kst=_require_str(row=item, field="trade_date_kst"),
                trade_time_kst=_require_str(row=item, field="trade_time_kst"),
                trade_timestamp=_require_int(row=item, field="trade_timestamp"),
                opening_price=_require_number(row=item, field="opening_price"),
                high_price=_require_number(row=item, field="high_price"),
                low_price=_require_number(row=item, field="low_price"),
                trade_price=_require_number(row=item, field="trade_price"),
                prev_closing_price=_require_number(row=item, field="prev_closing_price"),
                change=_require_str(row=item, field="change"),
                change_price=_require_number(row=item, field="change_price"),
                signed_change_price=_require_number(row=item, field="signed_change_price"),
                change_rate=_require_number(row=item, field="change_rate"),
                signed_change_rate=_require_number(row=item, field="signed_change_rate"),
                acc_trade_price=_require_number(row=item, field="acc_trade_price"),
                acc_trade_price_24h=_require_number(row=item, field="acc_trade_price_24h"),
                acc_trade_volume=_require_number(row=item, field="acc_trade_volume"),
                acc_trade_volume_24h=_require_number(row=item, field="acc_trade_volume_24h"),
            )
        )

    return rows


def parse_ticker_lite_payload(payload: object) -> list[TickerLiteSnapshot]:
    rows = parse_ticker_response_payload(payload)
    return [TickerLiteSnapshot.from_response_row(row) for row in rows]


def parse_ticker_payload(payload: object) -> list[TickerLiteSnapshot]:
    """Backward-compatible alias for the lite ticker payload parser."""

    return parse_ticker_lite_payload(payload)


def _validate_batch_market_response(
    *,
    requested_markets_csv: str,
    snapshots: list[TickerLiteSnapshot],
    endpoint: str,
) -> list[TickerLiteSnapshot]:
    requested_market_order = [
        parse_documented_market_code(token) for token in requested_markets_csv.split(",")
    ]
    requested_market_set = set(requested_market_order)
    returned_market_set = {parse_documented_market_code(snapshot.market) for snapshot in snapshots}
    if len(snapshots) != len(requested_market_set) or returned_market_set != requested_market_set:
        raise PublicApiSchemaError(
            "ticker response market mismatch "
            f"endpoint={endpoint} "
            f"requested_markets={sorted(requested_market_set)} "
            f"returned_markets={sorted(returned_market_set)} "
            f"returned_count={len(snapshots)}"
        )
    return snapshots


def _resolve_snapshots_by_market(
    *,
    requested_markets_csv: str,
    snapshots: list[TickerLiteSnapshot],
) -> dict[str, TickerLiteSnapshot]:
    requested_market_order = [
        parse_documented_market_code(token) for token in requested_markets_csv.split(",")
    ]
    snapshot_by_market = {
        parse_documented_market_code(snapshot.market): snapshot for snapshot in snapshots
    }
    return {market: snapshot_by_market[market] for market in requested_market_order}


def fetch_ticker_batch(
    client: httpx.Client, *, markets: str | Iterable[str]
) -> dict[str, TickerLiteSnapshot]:
    endpoint = "/v1/ticker"
    try:
        requested_markets_csv = normalize_ticker_markets(markets)
    except ExchangeMarketCodeError as exc:
        raise PublicApiSchemaError(
            f"ticker request market validation failed endpoint={endpoint} detail={exc}"
        ) from exc

    params = {"markets": requested_markets_csv}
    payload = get_public_json(client, endpoint, params=params)
    try:
        snapshots = parse_ticker_lite_payload(payload)
        _validate_batch_market_response(
            requested_markets_csv=requested_markets_csv,
            snapshots=snapshots,
            endpoint=endpoint,
        )
        return _resolve_snapshots_by_market(
            requested_markets_csv=requested_markets_csv,
            snapshots=snapshots,
        )
    except ExchangeMarketCodeError as exc:
        raise PublicApiSchemaError(
            "ticker response market validation failed "
            f"endpoint={endpoint} requested_markets={requested_markets_csv} detail={exc}"
        ) from exc


def fetch_ticker_single(client: httpx.Client, *, market: str) -> TickerLiteSnapshot:
    canonical_market = normalize_single_ticker_market(market)
    snapshots_by_market = fetch_ticker_batch(client, markets=[canonical_market])
    return snapshots_by_market[canonical_market]


def fetch_ticker(client: httpx.Client, *, markets: str | Iterable[str]) -> list[TickerLiteSnapshot]:
    """Backward-compatible alias for batch ticker fetch.

    Returns snapshots in the same order as requested markets.
    """

    return list(fetch_ticker_batch(client, markets=markets).values())


# Backward compatibility: keep historical public name while clarifying the contract.
TickerSnapshot = TickerLiteSnapshot
