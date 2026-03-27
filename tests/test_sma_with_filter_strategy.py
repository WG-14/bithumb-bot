from __future__ import annotations

import sqlite3

from bithumb_bot.strategy.sma import create_sma_strategy, create_sma_with_filter_strategy


def _build_candle_db(closes: list[float]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE candles (
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            close REAL NOT NULL
        )
        """
    )
    base_ts = 1_700_000_000_000
    for idx, close in enumerate(closes):
        conn.execute(
            "INSERT INTO candles(ts, pair, interval, close) VALUES (?, ?, ?, ?)",
            (base_ts + idx * 60_000, "BTC_KRW", "1m", close),
        )
    conn.commit()
    return conn


def test_filtered_sma_can_change_trade_signal_to_hold() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        plain = create_sma_strategy(short_n=2, long_n=3, pair="BTC_KRW", interval="1m").decide(conn)
        filtered = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.02,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert plain is not None
    assert filtered is not None
    assert plain.signal == "BUY"
    assert filtered.signal == "HOLD"
    assert filtered.reason.startswith("filtered entry")


def test_gap_filter_blocks_entry_and_writes_context() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.02,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert "gap" in decision.context["blocked_filters"]
    assert decision.context["filters"]["gap"]["passed"] is False
    assert decision.context["features"]["base_signal"] == "BUY"


def test_volatility_filter_blocks_low_range_entry() -> None:
    conn = _build_candle_db([100.0, 100.0, 100.0, 100.0, 100.01])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=5,
            min_volatility_ratio=0.001,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert "volatility" in decision.context["blocked_filters"]
    assert decision.context["filters"]["volatility"]["passed"] is False


def test_overextended_filter_blocks_chasing_entry() -> None:
    conn = _build_candle_db([100.0, 100.0, 100.0, 100.0, 130.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=2,
            overextended_max_return_ratio=0.1,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert "overextended" in decision.context["blocked_filters"]
    assert decision.context["filters"]["overextended"]["passed"] is False
