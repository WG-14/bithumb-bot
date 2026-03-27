from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from statistics import fmean
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


@dataclass(frozen=True)
class SmaWithFilterStrategy:
    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL
    min_gap_ratio: float = settings.SMA_FILTER_GAP_MIN_RATIO
    volatility_window: int = settings.SMA_FILTER_VOL_WINDOW
    min_volatility_ratio: float = settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
    overextended_lookback: int = settings.SMA_FILTER_OVEREXT_LOOKBACK
    overextended_max_return_ratio: float = settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO

    name: str = "sma_with_filter"

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        if self.short_n >= self.long_n:
            raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

        min_rows = max(
            self.long_n + 2,
            int(self.volatility_window),
            int(self.overextended_lookback) + 1,
        )
        query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
        params: list[object] = [self.pair, self.interval]
        if through_ts_ms is not None:
            query += " AND ts <= ?"
            params.append(int(through_ts_ms))
        query += " ORDER BY ts ASC"

        rows = conn.execute(query, tuple(params)).fetchall()
        if len(rows) < min_rows:
            return None

        closes = [float(r[1]) for r in rows]
        ts_list = [int(r[0]) for r in rows]

        def sma(values: list[float], n: int, end: int) -> float:
            w = values[end - n : end]
            return sum(w) / n

        def safe_ratio(numerator: float, denominator: float) -> float:
            if denominator == 0:
                return 0.0
            return numerator / denominator

        end_prev = len(closes) - 1
        end_curr = len(closes)

        prev_s = sma(closes, self.short_n, end_prev)
        prev_l = sma(closes, self.long_n, end_prev)
        curr_s = sma(closes, self.short_n, end_curr)
        curr_l = sma(closes, self.long_n, end_curr)

        base_signal = "HOLD"
        base_reason = "sma no crossover"
        if prev_s <= prev_l and curr_s > curr_l:
            base_signal = "BUY"
            base_reason = "sma golden cross"
        elif prev_s >= prev_l and curr_s < curr_l:
            base_signal = "SELL"
            base_reason = "sma dead cross"

        gap_ratio = abs(safe_ratio(curr_s - curr_l, curr_l))

        vol_window = max(1, int(self.volatility_window))
        vol_closes = closes[-vol_window:]
        vol_mean = fmean(vol_closes)
        volatility_ratio = safe_ratio((max(vol_closes) - min(vol_closes)), vol_mean)

        overext_lookback = max(1, int(self.overextended_lookback))
        base_close = closes[-1 - overext_lookback]
        overextended_ratio = abs(safe_ratio(closes[-1] - base_close, base_close))

        gap_filter_enabled = float(self.min_gap_ratio) > 0
        volatility_filter_enabled = float(self.min_volatility_ratio) > 0
        overextended_filter_enabled = float(self.overextended_max_return_ratio) > 0

        gap_triggered = gap_filter_enabled and gap_ratio < float(self.min_gap_ratio)
        volatility_triggered = (
            volatility_filter_enabled and volatility_ratio < float(self.min_volatility_ratio)
        )
        overextended_triggered = (
            overextended_filter_enabled
            and overextended_ratio > float(self.overextended_max_return_ratio)
        )

        blocked_filters = []
        if gap_triggered:
            blocked_filters.append("gap")
        if volatility_triggered:
            blocked_filters.append("volatility")
        if overextended_triggered:
            blocked_filters.append("overextended")

        decision_signal = base_signal
        decision_reason = base_reason
        should_filter_entry = base_signal in ("BUY", "SELL")
        if should_filter_entry and blocked_filters:
            decision_signal = "HOLD"
            decision_reason = f"filtered entry: {', '.join(blocked_filters)}"

        context = {
            "ts": ts_list[-1],
            "last_close": float(closes[-1]),
            "strategy": self.name,
            "features": {
                "prev_s": prev_s,
                "prev_l": prev_l,
                "curr_s": curr_s,
                "curr_l": curr_l,
                "sma_gap_ratio": gap_ratio,
                "volatility_range_ratio": volatility_ratio,
                "overextended_abs_return_ratio": overextended_ratio,
                "base_signal": base_signal,
                "base_reason": base_reason,
            },
            "filters": {
                "gap": {
                    "enabled": gap_filter_enabled,
                    "passed": not gap_triggered,
                    "threshold": float(self.min_gap_ratio),
                    "value": gap_ratio,
                },
                "volatility": {
                    "enabled": volatility_filter_enabled,
                    "passed": not volatility_triggered,
                    "window": vol_window,
                    "threshold": float(self.min_volatility_ratio),
                    "value": volatility_ratio,
                },
                "overextended": {
                    "enabled": overextended_filter_enabled,
                    "passed": not overextended_triggered,
                    "lookback": overext_lookback,
                    "threshold": float(self.overextended_max_return_ratio),
                    "value": overextended_ratio,
                },
            },
            "filter_blocked": bool(should_filter_entry and blocked_filters),
            "blocked_filters": blocked_filters,
        }

        return StrategyDecision(
            signal=decision_signal,
            reason=decision_reason,
            context=context,
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


def create_sma_with_filter_strategy(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
    min_gap_ratio: float | None = None,
    volatility_window: int | None = None,
    min_volatility_ratio: float | None = None,
    overextended_lookback: int | None = None,
    overextended_max_return_ratio: float | None = None,
) -> SmaWithFilterStrategy:
    return SmaWithFilterStrategy(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings.PAIR if pair is None else str(pair),
        interval=settings.INTERVAL if interval is None else str(interval),
        min_gap_ratio=float(
            settings.SMA_FILTER_GAP_MIN_RATIO if min_gap_ratio is None else min_gap_ratio
        ),
        volatility_window=int(
            settings.SMA_FILTER_VOL_WINDOW if volatility_window is None else volatility_window
        ),
        min_volatility_ratio=float(
            settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
            if min_volatility_ratio is None
            else min_volatility_ratio
        ),
        overextended_lookback=int(
            settings.SMA_FILTER_OVEREXT_LOOKBACK
            if overextended_lookback is None
            else overextended_lookback
        ),
        overextended_max_return_ratio=float(
            settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO
            if overextended_max_return_ratio is None
            else overextended_max_return_ratio
        ),
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
