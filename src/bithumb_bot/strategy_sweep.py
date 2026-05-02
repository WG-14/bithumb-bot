from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Iterable

from .strategy_config import SmaStrategyConfig
from .strategy_replay import (
    StrategyReplayConfig,
    StrategyReplayResult,
    replay_sma_strategy_decisions,
)


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


def run_sma_strategy_sweep(
    conn: sqlite3.Connection,
    *,
    base_config: SmaStrategyConfig,
    grid: StrategySweepGrid,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    through_ts_ms: int | None = None,
) -> list[StrategySweepResult]:
    results: list[StrategySweepResult] = []
    for short_n in grid.short_values:
        for long_n in grid.long_values:
            if int(short_n) >= int(long_n):
                continue
            for entry_edge_buffer_ratio in grid.entry_edge_buffer_values:
                for strategy_min_expected_edge_ratio in grid.strategy_min_expected_edge_values:
                    for slippage_bps in grid.slippage_bps_values:
                        strategy_config = replace(
                            base_config,
                            short_n=int(short_n),
                            long_n=int(long_n),
                            entry_edge_buffer_ratio=float(entry_edge_buffer_ratio),
                            strategy_min_expected_edge_ratio=float(
                                strategy_min_expected_edge_ratio
                            ),
                            slippage_bps=float(slippage_bps),
                        )
                        replay_result = replay_sma_strategy_decisions(
                            conn,
                            StrategyReplayConfig(
                                strategy_config=strategy_config,
                                from_ts_ms=from_ts_ms,
                                to_ts_ms=to_ts_ms,
                                through_ts_ms=through_ts_ms,
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
