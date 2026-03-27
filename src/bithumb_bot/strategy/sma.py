from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from ..config import settings
from .base import StrategyDecision


@dataclass(frozen=True)
class SmaCrossStrategy:
    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL

    name: str = "sma_cross"

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        if self.short_n >= self.long_n:
            raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

        need = self.long_n + 2
        query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
        params: list[object] = [self.pair, self.interval]
        if through_ts_ms is not None:
            query += " AND ts <= ?"
            params.append(int(through_ts_ms))
        query += " ORDER BY ts ASC"

        rows = conn.execute(query, tuple(params)).fetchall()

        if len(rows) < need:
            return None

        closes = [float(r[1]) for r in rows]
        ts_list = [int(r[0]) for r in rows]

        def sma(values: list[float], n: int, end: int) -> float:
            w = values[end - n : end]
            return sum(w) / n

        end_prev = len(closes) - 1
        end_curr = len(closes)

        prev_s = sma(closes, self.short_n, end_prev)
        prev_l = sma(closes, self.long_n, end_prev)
        curr_s = sma(closes, self.short_n, end_curr)
        curr_l = sma(closes, self.long_n, end_curr)

        signal = "HOLD"
        reason = "sma no crossover"
        if prev_s <= prev_l and curr_s > curr_l:
            signal = "BUY"
            reason = "sma golden cross"
        elif prev_s >= prev_l and curr_s < curr_l:
            signal = "SELL"
            reason = "sma dead cross"

        return StrategyDecision(
            signal=signal,
            reason=reason,
            context={
                "ts": ts_list[-1],
                "prev_s": prev_s,
                "prev_l": prev_l,
                "curr_s": curr_s,
                "curr_l": curr_l,
                "last_close": float(closes[-1]),
                "strategy": self.name,
            },
        )


def create_sma_strategy(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
) -> SmaCrossStrategy:
    return SmaCrossStrategy(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings.PAIR if pair is None else str(pair),
        interval=settings.INTERVAL if interval is None else str(interval),
    )


def compute_signal(
    conn: sqlite3.Connection,
    short_n: int,
    long_n: int,
    *,
    through_ts_ms: int | None = None,
) -> dict[str, Any] | None:
    decision = create_sma_strategy(short_n=short_n, long_n=long_n).decide(
        conn,
        through_ts_ms=through_ts_ms,
    )
    if decision is None:
        return None
    return decision.as_dict()
