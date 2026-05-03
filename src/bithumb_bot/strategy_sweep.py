from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Iterable

from .strategy_config import SmaStrategyConfig
from .strategy_replay import (
    CandleReplayDataset,
    StrategyReplayConfig,
    StrategyReplayResult,
    load_replay_candles,
    replay_sma_strategy_decisions_from_candles,
)


DEFAULT_STRATEGY_SWEEP_MAX_OPERATIONS = 300_000


@dataclass(frozen=True)
class StrategySweepGrid:
    short_values: tuple[int, ...]
    long_values: tuple[int, ...]
    entry_edge_buffer_values: tuple[float, ...]
    strategy_min_expected_edge_values: tuple[float, ...]
    slippage_bps_values: tuple[float, ...] = (0.0,)


@dataclass(frozen=True)
class StrategySweepResult:
    config_id: str
    strategy_config: SmaStrategyConfig
    replay_result: StrategyReplayResult


@dataclass(frozen=True)
class StrategySweepSummaryRow:
    config_id: str
    short_n: int
    long_n: int
    entry_edge_buffer_ratio: float
    strategy_min_expected_edge_ratio: float
    slippage_bps: float
    decision_count: int
    raw_buy: int
    final_buy: int
    submit_expected_buy: int
    raw_sell: int
    final_sell: int
    blocked_by_cost_filter_ratio: float
    gap_lt_required_ratio: float | None
    primary_issue: str


@dataclass(frozen=True)
class StrategySweepExecutionPlan:
    grid_count: int
    candle_count: int
    estimated_operations: int
    max_candles: int | None
    max_operations: int | None
    allow_large_sweep: bool
    full_history: bool
    allowed: bool
    block_reason: str | None


def _valid_grid_configs(
    *,
    base_config: SmaStrategyConfig,
    grid: StrategySweepGrid,
) -> tuple[SmaStrategyConfig, ...]:
    configs: list[SmaStrategyConfig] = []
    for short_n in grid.short_values:
        for long_n in grid.long_values:
            if int(short_n) >= int(long_n):
                continue
            for entry_edge_buffer_ratio in grid.entry_edge_buffer_values:
                for strategy_min_expected_edge_ratio in grid.strategy_min_expected_edge_values:
                    for slippage_bps in grid.slippage_bps_values:
                        configs.append(
                            replace(
                                base_config,
                                short_n=int(short_n),
                                long_n=int(long_n),
                                entry_edge_buffer_ratio=float(entry_edge_buffer_ratio),
                                strategy_min_expected_edge_ratio=float(
                                    strategy_min_expected_edge_ratio
                                ),
                                slippage_bps=float(slippage_bps),
                            )
                        )
    return tuple(configs)


def build_strategy_sweep_configs(
    *,
    base_config: SmaStrategyConfig,
    grid: StrategySweepGrid,
) -> tuple[SmaStrategyConfig, ...]:
    return _valid_grid_configs(base_config=base_config, grid=grid)


def build_strategy_sweep_execution_plan(
    *,
    grid: StrategySweepGrid,
    candle_count: int,
    max_candles: int | None,
    from_ts_ms: int | None,
    to_ts_ms: int | None,
    through_ts_ms: int | None,
    mode: str,
    allow_full_history: bool,
    max_operations: int | None = None,
    allow_large_sweep: bool = False,
) -> StrategySweepExecutionPlan:
    grid_count = sum(
        1
        for short_n in grid.short_values
        for long_n in grid.long_values
        if int(short_n) < int(long_n)
    )
    grid_count *= (
        len(grid.entry_edge_buffer_values)
        * len(grid.strategy_min_expected_edge_values)
        * len(grid.slippage_bps_values)
    )
    full_history = (
        from_ts_ms is None
        and to_ts_ms is None
        and through_ts_ms is None
        and max_candles is None
    )
    estimated_operations = int(grid_count) * int(candle_count)
    block_reason = None
    if str(mode) == "live" and full_history and not bool(allow_full_history):
        block_reason = "live_full_history_requires_window_or_max_candles"
    elif (
        max_operations is not None
        and int(max_operations) >= 0
        and estimated_operations > int(max_operations)
        and not bool(allow_large_sweep)
    ):
        block_reason = "estimated_operations_exceeds_max_operations"
    return StrategySweepExecutionPlan(
        grid_count=int(grid_count),
        candle_count=int(candle_count),
        estimated_operations=estimated_operations,
        max_candles=None if max_candles is None else int(max_candles),
        max_operations=None if max_operations is None else int(max_operations),
        allow_large_sweep=bool(allow_large_sweep),
        full_history=bool(full_history),
        allowed=block_reason is None,
        block_reason=block_reason,
    )


def run_sma_strategy_sweep(
    conn: sqlite3.Connection,
    *,
    base_config: SmaStrategyConfig,
    grid: StrategySweepGrid,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    through_ts_ms: int | None = None,
    max_candles: int | None = None,
) -> list[StrategySweepResult]:
    configs = build_strategy_sweep_configs(base_config=base_config, grid=grid)
    dataset = load_replay_candles(
        conn,
        pair=base_config.pair,
        interval=base_config.interval,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
        through_ts_ms=through_ts_ms,
        max_candles=max_candles,
    )
    return run_sma_strategy_sweep_from_candles(
        dataset,
        configs=configs,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
        through_ts_ms=through_ts_ms,
        max_candles=max_candles,
    )


def run_sma_strategy_sweep_from_candles(
    dataset: CandleReplayDataset,
    *,
    configs: Iterable[SmaStrategyConfig],
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    through_ts_ms: int | None = None,
    max_candles: int | None = None,
) -> list[StrategySweepResult]:
    results: list[StrategySweepResult] = []
    for strategy_config in configs:
        replay_result = replay_sma_strategy_decisions_from_candles(
            dataset,
            StrategyReplayConfig(
                strategy_config=strategy_config,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
                through_ts_ms=through_ts_ms,
                max_candles=max_candles,
            ),
        )
        results.append(
            StrategySweepResult(
                config_id=replay_result.config_id,
                strategy_config=strategy_config,
                replay_result=replay_result,
            )
        )
    return results


def summarize_strategy_sweep_results(
    results: Iterable[StrategySweepResult],
) -> list[StrategySweepSummaryRow]:
    rows: list[StrategySweepSummaryRow] = []
    for result in results:
        summary = result.replay_result.attribution_summary
        funnel = summary.candidate_funnel
        filter_ratios = summary.filter_ratios
        edge_stats = summary.edge_stats
        interpretation = summary.interpretation
        config = result.strategy_config
        rows.append(
            StrategySweepSummaryRow(
                config_id=result.config_id,
                short_n=int(config.short_n),
                long_n=int(config.long_n),
                entry_edge_buffer_ratio=float(config.entry_edge_buffer_ratio),
                strategy_min_expected_edge_ratio=float(
                    config.strategy_min_expected_edge_ratio
                ),
                slippage_bps=float(config.slippage_bps),
                decision_count=int(result.replay_result.decision_count),
                raw_buy=int(funnel.get("raw_BUY", 0)),
                final_buy=int(funnel.get("final_BUY", 0)),
                submit_expected_buy=int(funnel.get("submit_expected_BUY", 0)),
                raw_sell=int(funnel.get("raw_SELL", 0)),
                final_sell=int(funnel.get("final_SELL", 0)),
                blocked_by_cost_filter_ratio=float(
                    filter_ratios.get("blocked_by_cost_filter_ratio", 0.0)
                ),
                gap_lt_required_ratio=(
                    None
                    if edge_stats.get("gap_lt_required_ratio") is None
                    else float(edge_stats["gap_lt_required_ratio"])
                ),
                primary_issue=str(interpretation.get("primary_issue", "unknown")),
            )
        )
    return rows
