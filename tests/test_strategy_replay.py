from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.decision_attribution import DecisionAttributionSummary
from bithumb_bot.strategy_config import sma_strategy_config_from_settings
from bithumb_bot.strategy_replay import (
    CandleReplayDataset,
    StrategyReplayConfig,
    load_replay_candles,
    replay_sma_strategy_decisions,
    replay_sma_strategy_decisions_from_candles,
)


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


def _base_config(**overrides):
    config = replace(
        sma_strategy_config_from_settings(short_n=2, long_n=3),
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        strategy_min_expected_edge_ratio=0.0,
        buy_fraction=0.25,
        max_order_krw=50_000.0,
    )
    return replace(config, **overrides)


def test_replay_returns_empty_result_for_insufficient_candles() -> None:
    conn = _build_candle_db([10.0, 10.0, 11.0, 12.0])
    try:
        result = replay_sma_strategy_decisions(
            conn,
            StrategyReplayConfig(strategy_config=_base_config()),
        )
    finally:
        conn.close()

    assert result.decision_count == 0
    assert result.attribution_summary.sample_count == 0
    assert result.insufficient_candle_count == 4


def test_load_replay_candles_max_candles_returns_latest_n_ascending() -> None:
    conn = _build_candle_db([10.0, 11.0, 12.0, 13.0, 14.0])
    try:
        dataset = load_replay_candles(
            conn,
            pair="BTC_KRW",
            interval="1m",
            max_candles=3,
        )
    finally:
        conn.close()

    assert len(dataset.candles) == 3
    assert [close for _ts, close in dataset.candles] == [12.0, 13.0, 14.0]
    assert [ts for ts, _close in dataset.candles] == sorted(
        ts for ts, _close in dataset.candles
    )


def test_load_replay_candles_respects_from_to_and_through_bounds() -> None:
    conn = _build_candle_db([10.0, 11.0, 12.0, 13.0, 14.0])
    base_ts = 1_700_000_000_000
    try:
        dataset = load_replay_candles(
            conn,
            pair="BTC_KRW",
            interval="1m",
            from_ts_ms=base_ts + 60_000,
            to_ts_ms=base_ts + 4 * 60_000,
            through_ts_ms=base_ts + 3 * 60_000,
        )
    finally:
        conn.close()

    assert [close for _ts, close in dataset.candles] == [11.0, 12.0, 13.0]
    assert dataset.from_ts_ms == base_ts + 60_000
    assert dataset.to_ts_ms == base_ts + 4 * 60_000
    assert dataset.through_ts_ms == base_ts + 3 * 60_000


def test_replay_golden_cross_produces_raw_buy() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        result = replay_sma_strategy_decisions(
            conn,
            StrategyReplayConfig(strategy_config=_base_config()),
        )
    finally:
        conn.close()

    assert result.decision_count == 1
    assert result.attribution_summary.candidate_funnel["raw_BUY"] >= 1
    assert result.attribution_summary.raw_signal_counts["BUY"] >= 1


def test_replay_from_candles_matches_db_backed_wrapper_for_same_dataset() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0, 11.0])
    config = StrategyReplayConfig(strategy_config=_base_config(), max_candles=5)
    try:
        dataset = load_replay_candles(
            conn,
            pair="BTC_KRW",
            interval="1m",
            max_candles=5,
        )
        pure = replay_sma_strategy_decisions_from_candles(dataset, config)
        db_backed = replay_sma_strategy_decisions(conn, config)
    finally:
        conn.close()

    assert pure.config_id == db_backed.config_id
    assert pure.decision_count == db_backed.decision_count
    assert pure.attribution_summary.as_dict() == db_backed.attribution_summary.as_dict()


def test_replay_from_candles_does_not_need_sqlite_connection() -> None:
    dataset = CandleReplayDataset(
        pair="BTC_KRW",
        interval="1m",
        candles=tuple(
            (1_700_000_000_000 + idx * 60_000, close)
            for idx, close in enumerate([10.0, 10.0, 10.0, 10.0, 11.0])
        ),
        from_ts_ms=None,
        to_ts_ms=None,
        through_ts_ms=None,
        max_candles=None,
    )

    result = replay_sma_strategy_decisions_from_candles(
        dataset,
        StrategyReplayConfig(strategy_config=_base_config()),
    )

    assert result.decision_count == 1
    assert result.attribution_summary.candidate_funnel["raw_BUY"] >= 1


def test_replay_high_edge_buffer_turns_buy_candidate_into_cost_filtered_hold() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    low_buffer_conn = _build_candle_db(closes)
    high_buffer_conn = _build_candle_db(closes)
    try:
        low = replay_sma_strategy_decisions(
            low_buffer_conn,
            StrategyReplayConfig(strategy_config=_base_config(entry_edge_buffer_ratio=0.0)),
        )
        high = replay_sma_strategy_decisions(
            high_buffer_conn,
            StrategyReplayConfig(strategy_config=_base_config(entry_edge_buffer_ratio=0.02)),
        )
    finally:
        low_buffer_conn.close()
        high_buffer_conn.close()

    assert low.attribution_summary.candidate_funnel["raw_BUY"] >= 1
    assert high.attribution_summary.candidate_funnel["raw_BUY"] >= 1
    assert high.attribution_summary.candidate_funnel["final_BUY"] < high.attribution_summary.candidate_funnel["raw_BUY"]
    assert high.attribution_summary.filter_ratios["blocked_by_cost_filter_ratio"] > 0
    assert high.attribution_summary.block_reason_counts["strategy_filters.cost_edge"] >= 1
    assert high.attribution_summary.edge_stats["gap_lt_required_ratio"] is not None


def test_replay_is_deterministic_for_same_candles_and_config() -> None:
    conn_a = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0, 11.0])
    conn_b = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0, 11.0])
    config = StrategyReplayConfig(strategy_config=_base_config(entry_edge_buffer_ratio=0.01))
    try:
        first = replay_sma_strategy_decisions(conn_a, config)
        second = replay_sma_strategy_decisions(conn_b, config)
    finally:
        conn_a.close()
        conn_b.close()

    assert first.config_id == second.config_id
    assert first.decision_count == second.decision_count
    assert first.attribution_summary.as_dict() == second.attribution_summary.as_dict()


def test_replay_uses_decision_attribution_summary() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        result = replay_sma_strategy_decisions(
            conn,
            StrategyReplayConfig(strategy_config=_base_config()),
        )
    finally:
        conn.close()

    assert isinstance(result.attribution_summary, DecisionAttributionSummary)


def test_replay_is_read_only_for_trade_state_tables() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, marker TEXT)")
    conn.execute("CREATE TABLE fills (id INTEGER PRIMARY KEY, marker TEXT)")
    conn.execute("CREATE TABLE portfolio (id INTEGER PRIMARY KEY, marker TEXT)")
    conn.execute("CREATE TABLE open_position_lots (id INTEGER PRIMARY KEY, marker TEXT)")
    for table in ("orders", "fills", "portfolio", "open_position_lots"):
        conn.execute(f"INSERT INTO {table}(id, marker) VALUES (1, 'sentinel')")
    conn.commit()

    try:
        replay_sma_strategy_decisions(
            conn,
            StrategyReplayConfig(strategy_config=_base_config(entry_edge_buffer_ratio=0.02)),
        )
        for table in ("orders", "fills", "portfolio", "open_position_lots"):
            row = conn.execute(f"SELECT COUNT(*), MIN(marker), MAX(marker) FROM {table}").fetchone()
            assert row == (1, "sentinel", "sentinel")
    finally:
        conn.close()


def test_replay_cost_parameters_are_config_driven_without_mutating_settings() -> None:
    original_slippage = settings.STRATEGY_ENTRY_SLIPPAGE_BPS
    original_buffer = settings.ENTRY_EDGE_BUFFER_RATIO
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    conn_a = _build_candle_db(closes)
    conn_b = _build_candle_db(closes)
    try:
        loose = replay_sma_strategy_decisions(
            conn_a,
            StrategyReplayConfig(strategy_config=_base_config(entry_edge_buffer_ratio=0.0)),
        )
        strict = replay_sma_strategy_decisions(
            conn_b,
            StrategyReplayConfig(strategy_config=_base_config(entry_edge_buffer_ratio=0.02)),
        )
    finally:
        conn_a.close()
        conn_b.close()
        object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", original_slippage)
        object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", original_buffer)

    assert loose.attribution_summary.candidate_funnel["final_BUY"] == 1
    assert strict.attribution_summary.candidate_funnel["final_BUY"] == 0
    assert strict.attribution_summary.filter_ratios["blocked_by_cost_filter_ratio"] > 0
