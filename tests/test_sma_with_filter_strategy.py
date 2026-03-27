from __future__ import annotations

import importlib
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


def test_cost_edge_filter_blocks_small_gap_entry_and_records_reason() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.02,
            entry_edge_buffer_ratio=0.005,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.reason.startswith("filtered entry")
    assert "cost_edge" in decision.context["blocked_filters"]
    assert decision.context["filters"]["cost_edge"]["passed"] is False
    assert decision.context["filters"]["cost_edge"]["cost_floor_ratio"] == 0.045


def test_cost_edge_filter_allows_entry_when_signal_clears_cost_floor() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert decision.context["filters"]["cost_edge"]["passed"] is True


def test_cost_edge_filter_becomes_more_conservative_when_fee_or_buffer_increase() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        permissive = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
        conservative = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.01,
            entry_edge_buffer_ratio=0.01,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert permissive is not None
    assert conservative is not None
    assert permissive.signal == "BUY"
    assert conservative.signal == "HOLD"
    assert conservative.context["filters"]["cost_edge"]["threshold"] > permissive.context["filters"][
        "cost_edge"
    ]["threshold"]


def test_filtered_strategy_default_thresholds_are_conservative_and_valid() -> None:
    strategy = create_sma_with_filter_strategy(short_n=2, long_n=3, pair="BTC_KRW", interval="1m")

    assert strategy.min_gap_ratio >= 0.001
    assert strategy.min_volatility_ratio >= 0.003
    assert strategy.overextended_max_return_ratio <= 0.02


def test_sma_cross_cost_edge_filter_blocks_weak_entry_and_records_context() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.02,
            entry_edge_buffer_ratio=0.005,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.reason == "filtered entry: cost_edge"
    assert decision.context["blocked_by_cost_filter"] is True
    assert decision.context["gap_ratio"] < decision.context["cost_floor_ratio"]
    assert decision.context["filters"]["cost_edge"]["passed"] is False


def test_sma_cross_cost_edge_filter_keeps_signal_when_edge_is_sufficient() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert decision.context["blocked_by_cost_filter"] is False
    assert decision.context["gap_ratio"] > decision.context["cost_floor_ratio"]
    assert decision.context["filters"]["cost_edge"]["passed"] is True


def test_strategy_entry_slippage_defaults_to_zero_when_env_values_are_unset(monkeypatch) -> None:
    monkeypatch.delenv("STRATEGY_ENTRY_SLIPPAGE_BPS", raising=False)
    monkeypatch.delenv("MAX_MARKET_SLIPPAGE_BPS", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)
    config_module = importlib.import_module("bithumb_bot.config")
    config_module = importlib.reload(config_module)
    try:
        assert config_module.settings.STRATEGY_ENTRY_SLIPPAGE_BPS == 0.0
    finally:
        importlib.reload(config_module)
