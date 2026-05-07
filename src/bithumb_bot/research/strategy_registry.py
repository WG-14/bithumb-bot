from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .backtest_engine import BacktestRun, run_sma_backtest
from .dataset_snapshot import DatasetSnapshot
from .execution_model import ExecutionModel


ResearchStrategyRunner = Callable[
    [DatasetSnapshot, dict[str, Any], float, float, float | None, ExecutionModel | None],
    BacktestRun,
]


class ResearchStrategyRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class ResearchStrategyDataRequirements:
    required_data: tuple[str, ...]
    optional_data: tuple[str, ...] = ()
    unsupported_without: tuple[str, ...] = ()


def research_strategy_data_requirements(strategy_name: str) -> ResearchStrategyDataRequirements:
    if strategy_name == "sma_with_filter":
        return ResearchStrategyDataRequirements(required_data=("candles",), optional_data=("top_of_book",))
    if strategy_name == "top_of_book_required_test":
        return ResearchStrategyDataRequirements(required_data=("candles", "top_of_book"))
    raise ResearchStrategyRegistryError(f"unsupported research strategy: {strategy_name}")


def resolve_research_strategy(strategy_name: str) -> ResearchStrategyRunner:
    if strategy_name in {"sma_with_filter", "top_of_book_required_test"}:
        return _run_sma_with_filter
    raise ResearchStrategyRegistryError(f"unsupported research strategy: {strategy_name}")


def _run_sma_with_filter(
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
) -> BacktestRun:
    _require_parameter(parameter_values, "SMA_SHORT")
    _require_parameter(parameter_values, "SMA_LONG")
    return run_sma_backtest(
        dataset=dataset,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
    )


def _require_parameter(parameter_values: dict[str, Any], key: str) -> None:
    if key not in parameter_values:
        raise ResearchStrategyRegistryError(f"sma_with_filter missing required parameter: {key}")
