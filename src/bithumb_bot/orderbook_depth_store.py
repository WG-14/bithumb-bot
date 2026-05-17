from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass

from .markets import parse_user_market_input
from .orderbook_top_store import ORDERBOOK_TOP_SOURCE
from .public_api_orderbook import OrderbookSnapshot


@dataclass(frozen=True)
class OrderbookDepthLevel:
    ts: int
    pair: str
    side: str
    level_index: int
    price: float
    size: float
    cumulative_size: float
    cumulative_notional: float
    source: str
    observed_at_epoch_sec: float | None = None

    def as_db_tuple(self) -> tuple[int, str, str, int, float, float, float, float, str, float | None]:
        return (
            self.ts,
            self.pair,
            self.side,
            self.level_index,
            self.price,
            self.size,
            self.cumulative_size,
            self.cumulative_notional,
            self.source,
            self.observed_at_epoch_sec,
        )


@dataclass(frozen=True)
class OrderbookDepthSnapshot:
    ts: int
    pair: str
    bids: tuple[OrderbookDepthLevel, ...]
    asks: tuple[OrderbookDepthLevel, ...]
    source: str
    observed_at_epoch_sec: float | None = None

    @property
    def has_depth(self) -> bool:
        return bool(self.bids and self.asks)

    def all_levels(self) -> tuple[OrderbookDepthLevel, ...]:
        return (*self.bids, *self.asks)

    def depth_ref(self) -> str:
        return f"{self.source}:{self.pair}:{self.ts}"


def build_orderbook_depth_snapshot(
    *,
    ts: int,
    pair: str,
    bid_levels: list[tuple[float, float]] | tuple[tuple[float, float], ...],
    ask_levels: list[tuple[float, float]] | tuple[tuple[float, float], ...],
    source: str = ORDERBOOK_TOP_SOURCE,
    observed_at_epoch_sec: float | None = None,
) -> OrderbookDepthSnapshot:
    if not str(source or "").strip():
        raise ValueError("orderbook depth source is required")
    market = parse_user_market_input(pair)
    observed = None if observed_at_epoch_sec is None else float(observed_at_epoch_sec)
    if observed is not None and not math.isfinite(observed):
        raise ValueError(f"invalid orderbook depth observed_at_epoch_sec: {observed!r}")
    bids = _build_side_levels(
        ts=int(ts),
        pair=market,
        side="bid",
        levels=tuple(bid_levels),
        source=str(source).strip(),
        observed_at_epoch_sec=observed,
    )
    asks = _build_side_levels(
        ts=int(ts),
        pair=market,
        side="ask",
        levels=tuple(ask_levels),
        source=str(source).strip(),
        observed_at_epoch_sec=observed,
    )
    _validate_depth_sides(bids=bids, asks=asks)
    return OrderbookDepthSnapshot(
        ts=int(ts),
        pair=market,
        bids=bids,
        asks=asks,
        source=str(source).strip(),
        observed_at_epoch_sec=observed,
    )


def depth_snapshot_from_orderbook_snapshot(
    *,
    ts: int,
    snapshot: OrderbookSnapshot,
    source: str = ORDERBOOK_TOP_SOURCE,
    observed_at_epoch_sec: float | None = None,
) -> OrderbookDepthSnapshot | None:
    if not snapshot.orderbook_units:
        return None
    if not all(unit.has_depth_size for unit in snapshot.orderbook_units):
        return None
    return build_orderbook_depth_snapshot(
        ts=ts,
        pair=snapshot.market,
        bid_levels=[(unit.bid_price, unit.bid_size or 0.0) for unit in snapshot.orderbook_units],
        ask_levels=[(unit.ask_price, unit.ask_size or 0.0) for unit in snapshot.orderbook_units],
        source=source,
        observed_at_epoch_sec=observed_at_epoch_sec,
    )


def upsert_orderbook_depth_snapshot(conn: sqlite3.Connection, snapshot: OrderbookDepthSnapshot) -> int:
    validated = build_orderbook_depth_snapshot(
        ts=snapshot.ts,
        pair=snapshot.pair,
        bid_levels=[(level.price, level.size) for level in snapshot.bids],
        ask_levels=[(level.price, level.size) for level in snapshot.asks],
        source=snapshot.source,
        observed_at_epoch_sec=snapshot.observed_at_epoch_sec,
    )
    conn.execute(
        """
        DELETE FROM orderbook_depth_levels
        WHERE ts=? AND pair=? AND source=?
        """,
        (validated.ts, validated.pair, validated.source),
    )
    count = 0
    for level in validated.all_levels():
        cur = conn.execute(
            """
            INSERT INTO orderbook_depth_levels(
                ts, pair, side, level_index, price, size,
                cumulative_size, cumulative_notional, source, observed_at_epoch_sec
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            level.as_db_tuple(),
        )
        count += int(cur.rowcount or 0)
    return count


def load_orderbook_depth_snapshot_after_or_equal(
    conn: sqlite3.Connection,
    *,
    pair: str,
    target_ts: int,
    max_wait_ms: int,
    source: str | None = None,
) -> OrderbookDepthSnapshot | None:
    market = parse_user_market_input(pair)
    params: list[object] = [market, int(target_ts), int(target_ts) + int(max_wait_ms)]
    source_predicate = ""
    if source is not None:
        source_predicate = "AND source=?"
        params.append(source)
    row = conn.execute(
        f"""
        SELECT ts, source, observed_at_epoch_sec
        FROM orderbook_depth_levels
        WHERE pair=? AND ts >= ? AND ts <= ? {source_predicate}
        GROUP BY ts, source, observed_at_epoch_sec
        HAVING SUM(CASE WHEN side='bid' THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN side='ask' THEN 1 ELSE 0 END) > 0
        ORDER BY ts ASC, source ASC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    level_rows = conn.execute(
        """
        SELECT side, level_index, price, size
        FROM orderbook_depth_levels
        WHERE ts=? AND pair=? AND source=?
        ORDER BY side ASC, level_index ASC
        """,
        (int(row[0]), market, str(row[1])),
    ).fetchall()
    bids = [(float(price), float(size)) for side, _idx, price, size in level_rows if str(side) == "bid"]
    asks = [(float(price), float(size)) for side, _idx, price, size in level_rows if str(side) == "ask"]
    return build_orderbook_depth_snapshot(
        ts=int(row[0]),
        pair=market,
        bid_levels=bids,
        ask_levels=asks,
        source=str(row[1]),
        observed_at_epoch_sec=(None if row[2] is None else float(row[2])),
    )


def has_orderbook_depth_evidence(
    conn: sqlite3.Connection,
    *,
    pair: str,
    start_ts: int | None = None,
    end_ts: int | None = None,
    source: str | None = None,
) -> bool:
    table = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_depth_levels'"
    ).fetchone()
    if table is None:
        return False
    market = parse_user_market_input(pair)
    clauses = ["pair=?"]
    params: list[object] = [market]
    if start_ts is not None:
        clauses.append("ts >= ?")
        params.append(int(start_ts))
    if end_ts is not None:
        clauses.append("ts <= ?")
        params.append(int(end_ts))
    if source is not None:
        clauses.append("source=?")
        params.append(source)
    where = " AND ".join(clauses)
    row = conn.execute(
        f"""
        SELECT 1
        FROM orderbook_depth_levels
        WHERE {where}
        GROUP BY ts, source
        HAVING SUM(CASE WHEN side='bid' THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN side='ask' THEN 1 ELSE 0 END) > 0
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    return row is not None


def _build_side_levels(
    *,
    ts: int,
    pair: str,
    side: str,
    levels: tuple[tuple[float, float], ...],
    source: str,
    observed_at_epoch_sec: float | None,
) -> tuple[OrderbookDepthLevel, ...]:
    if side not in {"bid", "ask"}:
        raise ValueError(f"invalid orderbook depth side: {side!r}")
    if not levels:
        raise ValueError(f"orderbook depth {side} levels are required")
    out: list[OrderbookDepthLevel] = []
    cumulative_size = 0.0
    cumulative_notional = 0.0
    previous_price: float | None = None
    for index, raw in enumerate(levels):
        price, size = float(raw[0]), float(raw[1])
        _validate_price_size(price=price, size=size, side=side, level_index=index)
        if previous_price is not None:
            if side == "bid" and price > previous_price:
                raise ValueError("bid depth levels must be sorted best-to-worse")
            if side == "ask" and price < previous_price:
                raise ValueError("ask depth levels must be sorted best-to-worse")
        previous_price = price
        cumulative_size += size
        cumulative_notional += price * size
        out.append(
            OrderbookDepthLevel(
                ts=ts,
                pair=pair,
                side=side,
                level_index=index,
                price=price,
                size=size,
                cumulative_size=cumulative_size,
                cumulative_notional=cumulative_notional,
                source=source,
                observed_at_epoch_sec=observed_at_epoch_sec,
            )
        )
    return tuple(out)


def _validate_price_size(*, price: float, size: float, side: str, level_index: int) -> None:
    if not math.isfinite(price) or price <= 0.0:
        raise ValueError(f"invalid orderbook depth price side={side} level_index={level_index}: {price!r}")
    if not math.isfinite(size) or size <= 0.0:
        raise ValueError(f"invalid orderbook depth size side={side} level_index={level_index}: {size!r}")


def _validate_depth_sides(
    *,
    bids: tuple[OrderbookDepthLevel, ...],
    asks: tuple[OrderbookDepthLevel, ...],
) -> None:
    if not bids or not asks:
        raise ValueError("orderbook depth requires both bid and ask levels")
    best_bid = float(bids[0].price)
    best_ask = float(asks[0].price)
    if best_bid > best_ask:
        raise ValueError(f"crossed orderbook depth: best_bid={best_bid!r} best_ask={best_ask!r}")
