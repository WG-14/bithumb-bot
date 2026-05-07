from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass

from .markets import parse_user_market_input
from .public_api_orderbook import BestQuote


ORDERBOOK_TOP_SOURCE = "bithumb_public_v1_orderbook"


@dataclass(frozen=True)
class OrderbookTopSnapshot:
    ts: int
    pair: str
    bid_price: float
    ask_price: float
    spread_bps: float
    source: str
    observed_at_epoch_sec: float | None = None

    def as_db_tuple(self) -> tuple[int, str, float, float, float, str, float | None]:
        return (
            self.ts,
            self.pair,
            self.bid_price,
            self.ask_price,
            self.spread_bps,
            self.source,
            self.observed_at_epoch_sec,
        )


def compute_spread_bps(*, bid_price: float, ask_price: float) -> float:
    bid = float(bid_price)
    ask = float(ask_price)
    _validate_bid_ask(bid=bid, ask=ask)
    mid = (bid + ask) / 2.0
    spread_bps = ((ask - bid) / mid) * 10_000.0
    if not math.isfinite(spread_bps) or spread_bps < 0.0:
        raise ValueError(f"invalid orderbook top spread_bps: {spread_bps!r}")
    return spread_bps


def build_orderbook_top_snapshot(
    *,
    ts: int,
    pair: str,
    bid_price: float,
    ask_price: float,
    source: str = ORDERBOOK_TOP_SOURCE,
    observed_at_epoch_sec: float | None = None,
) -> OrderbookTopSnapshot:
    if not str(source or "").strip():
        raise ValueError("orderbook top source is required")
    market = parse_user_market_input(pair)
    bid = float(bid_price)
    ask = float(ask_price)
    spread_bps = compute_spread_bps(bid_price=bid, ask_price=ask)
    observed = None if observed_at_epoch_sec is None else float(observed_at_epoch_sec)
    if observed is not None and not math.isfinite(observed):
        raise ValueError(f"invalid orderbook top observed_at_epoch_sec: {observed!r}")
    return OrderbookTopSnapshot(
        ts=int(ts),
        pair=market,
        bid_price=bid,
        ask_price=ask,
        spread_bps=spread_bps,
        source=str(source).strip(),
        observed_at_epoch_sec=observed,
    )


def snapshot_from_best_quote(*, ts: int, quote: BestQuote, requested_pair: str) -> OrderbookTopSnapshot:
    requested_market = parse_user_market_input(requested_pair)
    quote_market = parse_user_market_input(quote.market)
    if quote_market != requested_market:
        raise ValueError(
            "orderbook top market mismatch "
            f"requested_pair={requested_market!r} quote_market={quote_market!r}"
        )
    return build_orderbook_top_snapshot(
        ts=ts,
        pair=requested_market,
        bid_price=quote.bid_price,
        ask_price=quote.ask_price,
        source=quote.source or ORDERBOOK_TOP_SOURCE,
        observed_at_epoch_sec=quote.observed_at_epoch_sec,
    )


def upsert_orderbook_top_snapshot(conn: sqlite3.Connection, snapshot: OrderbookTopSnapshot) -> int:
    validated = build_orderbook_top_snapshot(
        ts=snapshot.ts,
        pair=snapshot.pair,
        bid_price=snapshot.bid_price,
        ask_price=snapshot.ask_price,
        source=snapshot.source,
        observed_at_epoch_sec=snapshot.observed_at_epoch_sec,
    )
    cur = conn.execute(
        """
        INSERT OR REPLACE INTO orderbook_top_snapshots(
            ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        validated.as_db_tuple(),
    )
    return int(cur.rowcount or 0)


def _validate_bid_ask(*, bid: float, ask: float) -> None:
    if not math.isfinite(bid) or not math.isfinite(ask) or bid <= 0.0 or ask <= 0.0:
        raise ValueError(f"invalid orderbook top quote: bid={bid!r} ask={ask!r}")
    if bid > ask:
        raise ValueError(f"crossed orderbook top quote: bid={bid!r} ask={ask!r}")
