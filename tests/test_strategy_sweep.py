from __future__ import annotations

import sqlite3
from dataclasses import asdict, fields, replace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.decision_attribution import DecisionAttributionSummary
from bithumb_bot.strategy_config import sma_strategy_config_from_settings
from bithumb_bot.strategy_replay import CandleReplayDataset, StrategyReplayResult
from bithumb_bot.strategy_sweep import (
    StrategySweepGrid,
    StrategySweepSummaryRow,
    build_strategy_sweep_execution_plan,
    run_sma_strategy_sweep,
    summarize_strategy_sweep_results,
)
import bithumb_bot.strategy_sweep as strategy_sweep_module


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


def _single_cross_grid(**overrides) -> StrategySweepGrid:
    grid = StrategySweepGrid(
        short_values=(2,),
        long_values=(3,),
        entry_edge_buffer_values=(0.0,),
        strategy_min_expected_edge_values=(0.0,),
        slippage_bps_values=(0.0,),
    )
    return replace(grid, **overrides)


@pytest.fixture
def settings_guard():
    original = {
        "ENTRY_EDGE_BUFFER_RATIO": settings.ENTRY_EDGE_BUFFER_RATIO,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": settings.STRATEGY_ENTRY_SLIPPAGE_BPS,
    }
    try:
        yield
    finally:
        for name, value in original.items():
            object.__setattr__(settings, name, value)


def test_sweep_filters_invalid_short_long_configs() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        results = run_sma_strategy_sweep(
            conn,
            base_config=_base_config(),
            grid=_single_cross_grid(short_values=(2, 3), long_values=(3,)),
        )
    finally:
        conn.close()

    assert [(result.strategy_config.short_n, result.strategy_config.long_n) for result in results] == [
        (2, 3)
    ]


def test_sweep_result_order_and_summary_rows_are_deterministic() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0, 11.0]
    grid = StrategySweepGrid(
        short_values=(2,),
        long_values=(3, 4),
        entry_edge_buffer_values=(0.0, 0.02),
        strategy_min_expected_edge_values=(0.0,),
        slippage_bps_values=(0.0, 5.0),
    )
    conn_a = _build_candle_db(closes)
    conn_b = _build_candle_db(closes)
    try:
        first = run_sma_strategy_sweep(conn_a, base_config=_base_config(), grid=grid)
        second = run_sma_strategy_sweep(conn_b, base_config=_base_config(), grid=grid)
    finally:
        conn_a.close()
        conn_b.close()

    assert [result.config_id for result in first] == [result.config_id for result in second]
    assert [asdict(row) for row in summarize_strategy_sweep_results(first)] == [
        asdict(row) for row in summarize_strategy_sweep_results(second)
    ]


def test_sweep_max_candles_produces_deterministic_rows() -> None:
    closes = [9.0, 10.0, 10.0, 10.0, 10.0, 11.0]
    conn_a = _build_candle_db(closes)
    conn_b = _build_candle_db(closes)
    try:
        first = run_sma_strategy_sweep(
            conn_a,
            base_config=_base_config(),
            grid=_single_cross_grid(),
            max_candles=5,
        )
        second = run_sma_strategy_sweep(
            conn_b,
            base_config=_base_config(),
            grid=_single_cross_grid(),
            max_candles=5,
        )
    finally:
        conn_a.close()
        conn_b.close()

    assert [asdict(row) for row in summarize_strategy_sweep_results(first)] == [
        asdict(row) for row in summarize_strategy_sweep_results(second)
    ]
    assert first[0].replay_result.candle_count == 5


def test_sweep_loads_candle_dataset_once(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:")
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
        max_candles=5,
    )
    calls = []

    def fake_loader(*args, **kwargs):
        calls.append((args, kwargs))
        return dataset

    monkeypatch.setattr(strategy_sweep_module, "load_replay_candles", fake_loader)
    try:
        results = run_sma_strategy_sweep(
            conn,
            base_config=_base_config(),
            grid=_single_cross_grid(entry_edge_buffer_values=(0.0, 0.02)),
            max_candles=5,
        )
    finally:
        conn.close()

    assert len(calls) == 1
    assert len(results) == 2


def test_sweep_results_use_replay_and_decision_attribution_summary() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        results = run_sma_strategy_sweep(
            conn,
            base_config=_base_config(),
            grid=_single_cross_grid(),
        )
    finally:
        conn.close()

    assert len(results) == 1
    assert isinstance(results[0].replay_result, StrategyReplayResult)
    assert isinstance(results[0].replay_result.attribution_summary, DecisionAttributionSummary)


def test_edge_buffer_changes_final_buy_and_cost_filter_metrics() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        results = run_sma_strategy_sweep(
            conn,
            base_config=_base_config(),
            grid=_single_cross_grid(entry_edge_buffer_values=(0.0, 0.02)),
        )
    finally:
        conn.close()

    rows = summarize_strategy_sweep_results(results)
    low_buffer, high_buffer = rows
    assert low_buffer.raw_buy == high_buffer.raw_buy
    assert high_buffer.final_buy < low_buffer.final_buy
    assert (
        high_buffer.blocked_by_cost_filter_ratio
        > low_buffer.blocked_by_cost_filter_ratio
    )


def test_sweep_does_not_mutate_base_config_or_settings(settings_guard) -> None:
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.77)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 88.0)
    base_config = _base_config(entry_edge_buffer_ratio=0.001, slippage_bps=2.0)
    original_config = base_config
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        run_sma_strategy_sweep(
            conn,
            base_config=base_config,
            grid=_single_cross_grid(
                entry_edge_buffer_values=(0.0, 0.02),
                slippage_bps_values=(0.0, 5.0),
            ),
        )
    finally:
        conn.close()

    assert base_config == original_config
    assert settings.ENTRY_EDGE_BUFFER_RATIO == pytest.approx(0.77)
    assert settings.STRATEGY_ENTRY_SLIPPAGE_BPS == pytest.approx(88.0)


def test_sweep_is_read_only_for_trade_state_tables() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, marker TEXT)")
    conn.execute("CREATE TABLE fills (id INTEGER PRIMARY KEY, marker TEXT)")
    conn.execute("CREATE TABLE portfolio (id INTEGER PRIMARY KEY, marker TEXT)")
    conn.execute("CREATE TABLE open_position_lots (id INTEGER PRIMARY KEY, marker TEXT)")
    for table in ("orders", "fills", "portfolio", "open_position_lots"):
        conn.execute(f"INSERT INTO {table}(id, marker) VALUES (1, 'sentinel')")
    conn.commit()

    try:
        run_sma_strategy_sweep(
            conn,
            base_config=_base_config(),
            grid=_single_cross_grid(entry_edge_buffer_values=(0.0, 0.02)),
        )
        for table in ("orders", "fills", "portfolio", "open_position_lots"):
            row = conn.execute(f"SELECT COUNT(*), MIN(marker), MAX(marker) FROM {table}").fetchone()
            assert row == (1, "sentinel", "sentinel")
    finally:
        conn.close()


def test_summary_rows_are_deterministic_and_attribution_based() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        results = run_sma_strategy_sweep(
            conn,
            base_config=_base_config(),
            grid=_single_cross_grid(entry_edge_buffer_values=(0.0, 0.02)),
        )
    finally:
        conn.close()

    rows = summarize_strategy_sweep_results(results)
    assert all(isinstance(row, StrategySweepSummaryRow) for row in rows)
    assert [asdict(row) for row in rows] == [asdict(row) for row in rows]
    row_fields = {field.name for field in fields(StrategySweepSummaryRow)}
    assert {
        "config_id",
        "short_n",
        "long_n",
        "entry_edge_buffer_ratio",
        "strategy_min_expected_edge_ratio",
        "slippage_bps",
        "decision_count",
        "raw_buy",
        "final_buy",
        "blocked_by_cost_filter_ratio",
        "gap_lt_required_ratio",
        "primary_issue",
    }.issubset(row_fields)
    assert not any("pnl" in field_name.lower() for field_name in row_fields)
    assert not any("drawdown" in field_name.lower() for field_name in row_fields)
    assert not any("fee_drag" in field_name.lower() for field_name in row_fields)


def test_sweep_execution_plan_counts_valid_grid_and_operations() -> None:
    grid = _single_cross_grid(short_values=(2, 3), long_values=(3, 4))

    plan = build_strategy_sweep_execution_plan(
        grid=grid,
        candle_count=5000,
        max_candles=5000,
        from_ts_ms=None,
        to_ts_ms=None,
        through_ts_ms=None,
        mode="live",
        allow_full_history=False,
    )

    assert plan.grid_count == 3
    assert plan.estimated_operations == 15_000
    assert plan.max_operations is None
    assert plan.allow_large_sweep is False
    assert plan.full_history is False
    assert plan.allowed is True


def test_sweep_execution_plan_blocks_operations_over_budget() -> None:
    plan = build_strategy_sweep_execution_plan(
        grid=_single_cross_grid(
            short_values=(2, 3),
            long_values=(3, 4),
            entry_edge_buffer_values=(0.0, 0.01),
        ),
        candle_count=100,
        max_candles=100,
        from_ts_ms=None,
        to_ts_ms=None,
        through_ts_ms=None,
        mode="live",
        allow_full_history=False,
        max_operations=500,
        allow_large_sweep=False,
    )

    assert plan.estimated_operations == 600
    assert plan.allowed is False
    assert plan.block_reason == "estimated_operations_exceeds_max_operations"
    assert plan.max_operations == 500


def test_sweep_execution_plan_allow_large_sweep_overrides_operation_budget() -> None:
    plan = build_strategy_sweep_execution_plan(
        grid=_single_cross_grid(
            short_values=(2, 3),
            long_values=(3, 4),
            entry_edge_buffer_values=(0.0, 0.01),
        ),
        candle_count=100,
        max_candles=100,
        from_ts_ms=None,
        to_ts_ms=None,
        through_ts_ms=None,
        mode="live",
        allow_full_history=False,
        max_operations=500,
        allow_large_sweep=True,
    )

    assert plan.estimated_operations == 600
    assert plan.allowed is True
    assert plan.allow_large_sweep is True


def test_sweep_execution_plan_blocks_live_unbounded_full_history() -> None:
    plan = build_strategy_sweep_execution_plan(
        grid=_single_cross_grid(),
        candle_count=100,
        max_candles=None,
        from_ts_ms=None,
        to_ts_ms=None,
        through_ts_ms=None,
        mode="live",
        allow_full_history=False,
    )

    assert plan.full_history is True
    assert plan.allowed is False
    assert plan.block_reason == "live_full_history_requires_window_or_max_candles"


def test_sweep_execution_plan_allows_live_bounded_and_non_live_full_history() -> None:
    live_bounded = build_strategy_sweep_execution_plan(
        grid=_single_cross_grid(),
        candle_count=100,
        max_candles=None,
        from_ts_ms=1,
        to_ts_ms=None,
        through_ts_ms=None,
        mode="live",
        allow_full_history=False,
    )
    paper_full = build_strategy_sweep_execution_plan(
        grid=_single_cross_grid(),
        candle_count=100,
        max_candles=None,
        from_ts_ms=None,
        to_ts_ms=None,
        through_ts_ms=None,
        mode="paper",
        allow_full_history=False,
    )

    assert live_bounded.allowed is True
    assert live_bounded.full_history is False
    assert paper_full.allowed is True
    assert paper_full.full_history is True
