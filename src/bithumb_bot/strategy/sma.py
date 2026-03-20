# src/bithumb_bot/strategy/sma.py
from __future__ import annotations

import sqlite3
from typing import Any

from ..config import settings


def compute_signal(
    conn: sqlite3.Connection,
    short_n: int,
    long_n: int,
    *,
    through_ts_ms: int | None = None,
) -> dict[str, Any] | None:
    if short_n >= long_n:
        raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

    need = long_n + 2
    query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
    params: list[object] = [settings.PAIR, settings.INTERVAL]
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

    prev_s = sma(closes, short_n, end_prev)
    prev_l = sma(closes, long_n, end_prev)
    curr_s = sma(closes, short_n, end_curr)
    curr_l = sma(closes, long_n, end_curr)

    signal = "HOLD"
    if prev_s <= prev_l and curr_s > curr_l:
        signal = "BUY"
    elif prev_s >= prev_l and curr_s < curr_l:
        signal = "SELL"

    return {
        "ts": ts_list[-1],
        "prev_s": prev_s,
        "prev_l": prev_l,
        "curr_s": curr_s,
        "curr_l": curr_l,
        "signal": signal,
        "last_close": float(closes[-1]),
    }