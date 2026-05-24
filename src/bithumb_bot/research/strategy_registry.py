from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .backtest_engine import (
    BacktestRun,
    BacktestRunContext,
    run_buy_and_hold_baseline_backtest,
    run_noop_baseline_backtest,
    run_sma_backtest,
)
from .dataset_snapshot import DatasetSnapshot
from .execution_model import ExecutionModel
from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy
from .hashing import sha256_prefixed
from .strategy_spec import BUY_AND_HOLD_BASELINE_SPEC, NOOP_BASELINE_SPEC, SMA_WITH_FILTER_SPEC, StrategySpec


ResearchStrategyRunner = Callable[
    [
        DatasetSnapshot,
        dict[str, Any],
        float,
        float,
        float | None,
        ExecutionModel | None,
        ExecutionTimingPolicy | None,
        PortfolioPolicy | None,
        BacktestRunContext | None,
    ],
    BacktestRun,
]
RuntimeReplayBuilder = Callable[[dict[str, Any], dict[str, Any] | None], Any]
RuntimeEnvParameterExtractor = Callable[[dict[str, str]], dict[str, Any]]
RuntimeSettingsParameterExtractor = Callable[[object], dict[str, Any]]


class ResearchStrategyRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class ResearchStrategyDataRequirements:
    required_data: tuple[str, ...]
    optional_data: tuple[str, ...] = ()
    unsupported_without: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeParameterAdapter:
    from_env: RuntimeEnvParameterExtractor
    from_settings: RuntimeSettingsParameterExtractor


@dataclass(frozen=True)
class ResearchStrategyPlugin:
    name: str
    version: str
    spec: StrategySpec
    required_data: tuple[str, ...]
    optional_data: tuple[str, ...]
    runner: ResearchStrategyRunner
    runtime_replay_builder: RuntimeReplayBuilder | None
    runtime_parameter_adapter: RuntimeParameterAdapter | None
    decision_contract_version: str
    diagnostics_namespace: str

    def contract_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "strategy_spec_hash": self.spec.spec_hash(),
            "required_data": list(self.required_data),
            "optional_data": list(self.optional_data),
            "behavior_affecting_parameter_names": list(self.spec.behavior_affecting_parameter_names),
            "runner_module": self.runner.__module__,
            "runner_qualname": self.runner.__qualname__,
            "runtime_replay_supported": self.runtime_replay_builder is not None,
            "runtime_replay_builder_module": (
                self.runtime_replay_builder.__module__ if self.runtime_replay_builder is not None else None
            ),
            "runtime_replay_builder_qualname": (
                self.runtime_replay_builder.__qualname__ if self.runtime_replay_builder is not None else None
            ),
            "decision_contract_version": self.decision_contract_version,
            "diagnostics_namespace": self.diagnostics_namespace,
        }

    def contract_hash(self) -> str:
        return sha256_prefixed(self.contract_payload())


TEST_TOP_OF_BOOK_REQUIRED_STRATEGY = "__test_top_of_book_required__"


def research_strategy_data_requirements(strategy_name: str) -> ResearchStrategyDataRequirements:
    if strategy_name == TEST_TOP_OF_BOOK_REQUIRED_STRATEGY:
        return ResearchStrategyDataRequirements(required_data=("candles", "top_of_book"))
    plugin = resolve_research_strategy_plugin(strategy_name)
    return ResearchStrategyDataRequirements(
        required_data=plugin.required_data,
        optional_data=plugin.optional_data,
    )


def resolve_research_strategy_plugin(strategy_name: str) -> ResearchStrategyPlugin:
    try:
        return _RESEARCH_STRATEGY_PLUGINS[strategy_name]
    except KeyError as exc:
        raise ResearchStrategyRegistryError(f"unsupported research strategy: {strategy_name}") from exc


def resolve_research_strategy(strategy_name: str) -> ResearchStrategyRunner:
    if strategy_name == TEST_TOP_OF_BOOK_REQUIRED_STRATEGY:
        return _run_sma_with_filter
    return resolve_research_strategy_plugin(strategy_name).runner


def _build_sma_runtime_replay_strategy(
    profile: dict[str, Any],
    candidate_regime_policy: dict[str, Any] | None = None,
) -> Any:
    from bithumb_bot.config import settings
    from bithumb_bot.strategy.sma import create_sma_with_filter_strategy

    params = profile.get("strategy_parameters") if isinstance(profile.get("strategy_parameters"), dict) else {}
    cost = profile.get("cost_model") if isinstance(profile.get("cost_model"), dict) else {}
    return create_sma_with_filter_strategy(
        short_n=int(params.get("SMA_SHORT", settings.SMA_SHORT)),
        long_n=int(params.get("SMA_LONG", settings.SMA_LONG)),
        pair=str(profile.get("market") or settings.PAIR),
        interval=str(profile.get("interval") or settings.INTERVAL),
        min_gap_ratio=float(params.get("SMA_FILTER_GAP_MIN_RATIO", settings.SMA_FILTER_GAP_MIN_RATIO)),
        volatility_window=int(params.get("SMA_FILTER_VOL_WINDOW", settings.SMA_FILTER_VOL_WINDOW)),
        min_volatility_ratio=float(
            params.get("SMA_FILTER_VOL_MIN_RANGE_RATIO", settings.SMA_FILTER_VOL_MIN_RANGE_RATIO)
        ),
        overextended_lookback=int(
            params.get("SMA_FILTER_OVEREXT_LOOKBACK", settings.SMA_FILTER_OVEREXT_LOOKBACK)
        ),
        overextended_max_return_ratio=float(
            params.get("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO)
        ),
        cost_edge_enabled=_coerce_bool(params.get("SMA_COST_EDGE_ENABLED", settings.SMA_COST_EDGE_ENABLED)),
        cost_edge_min_ratio=float(params.get("SMA_COST_EDGE_MIN_RATIO", settings.SMA_COST_EDGE_MIN_RATIO)),
        entry_edge_buffer_ratio=float(params.get("ENTRY_EDGE_BUFFER_RATIO", settings.ENTRY_EDGE_BUFFER_RATIO)),
        slippage_bps=float(cost.get("slippage_bps", settings.STRATEGY_ENTRY_SLIPPAGE_BPS)),
        live_fee_rate_estimate=float(cost.get("fee_rate", settings.LIVE_FEE_RATE_ESTIMATE)),
        exit_rule_names=str(params.get("STRATEGY_EXIT_RULES", settings.STRATEGY_EXIT_RULES)).split(","),
        exit_stop_loss_ratio=float(
            params.get("STRATEGY_EXIT_STOP_LOSS_RATIO", settings.STRATEGY_EXIT_STOP_LOSS_RATIO)
        ),
        exit_max_holding_min=int(
            params.get("STRATEGY_EXIT_MAX_HOLDING_MIN", settings.STRATEGY_EXIT_MAX_HOLDING_MIN)
        ),
        exit_min_take_profit_ratio=float(
            params.get("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO)
        ),
        exit_small_loss_tolerance_ratio=float(
            params.get(
                "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
                settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO,
            )
        ),
        candidate_regime_policy=candidate_regime_policy,
    )


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _sma_runtime_parameters_from_env(env: dict[str, str]) -> dict[str, Any]:
    def _value(*keys: str, default: str = "") -> str:
        for key in keys:
            if env.get(key, "").strip() != "":
                return env[key]
        return default

    return {
        "SMA_SHORT": _value("SMA_SHORT", default="7"),
        "SMA_LONG": _value("SMA_LONG", default="30"),
        "SMA_FILTER_GAP_MIN_RATIO": _value("SMA_FILTER_GAP_MIN_RATIO", default="0.0012"),
        "SMA_FILTER_VOL_WINDOW": _value("SMA_FILTER_VOL_WINDOW", default="10"),
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": _value("SMA_FILTER_VOL_MIN_RANGE_RATIO", default="0.003"),
        "SMA_FILTER_OVEREXT_LOOKBACK": _value("SMA_FILTER_OVEREXT_LOOKBACK", default="3"),
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": _value("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", default="0.02"),
        "SMA_MARKET_REGIME_ENABLED": _value("SMA_MARKET_REGIME_ENABLED", default="true"),
        "SMA_COST_EDGE_ENABLED": _value("SMA_COST_EDGE_ENABLED", default="true"),
        "SMA_COST_EDGE_MIN_RATIO": _value(
            "SMA_COST_EDGE_MIN_RATIO",
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
            default="0",
        ),
        "ENTRY_EDGE_BUFFER_RATIO": _value("ENTRY_EDGE_BUFFER_RATIO", default="0.0005"),
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": _value("STRATEGY_MIN_EXPECTED_EDGE_RATIO", default="0"),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": _value(
            "STRATEGY_ENTRY_SLIPPAGE_BPS",
            "MAX_MARKET_SLIPPAGE_BPS",
            "SLIPPAGE_BPS",
            default="0",
        ),
        "LIVE_FEE_RATE_ESTIMATE": _value(
            "LIVE_FEE_RATE_ESTIMATE",
            "PAPER_FEE_RATE",
            "PAPER_FEE_RATE_ESTIMATE",
            "FEE_RATE",
            default="0.0004",
        ),
        "STRATEGY_EXIT_RULES": _value("STRATEGY_EXIT_RULES", default="stop_loss,opposite_cross,max_holding_time"),
        "STRATEGY_EXIT_STOP_LOSS_RATIO": _value("STRATEGY_EXIT_STOP_LOSS_RATIO", default="0"),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": _value("STRATEGY_EXIT_MAX_HOLDING_MIN", default="0"),
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": _value("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", default="0"),
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": _value(
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
            default="0",
        ),
    }


def _sma_runtime_parameters_from_settings(cfg: object) -> dict[str, Any]:
    return {
        "SMA_SHORT": int(getattr(cfg, "SMA_SHORT")),
        "SMA_LONG": int(getattr(cfg, "SMA_LONG")),
        "SMA_FILTER_GAP_MIN_RATIO": float(getattr(cfg, "SMA_FILTER_GAP_MIN_RATIO")),
        "SMA_FILTER_VOL_WINDOW": int(getattr(cfg, "SMA_FILTER_VOL_WINDOW")),
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": float(getattr(cfg, "SMA_FILTER_VOL_MIN_RANGE_RATIO")),
        "SMA_FILTER_OVEREXT_LOOKBACK": int(getattr(cfg, "SMA_FILTER_OVEREXT_LOOKBACK")),
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": float(getattr(cfg, "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO")),
        "SMA_MARKET_REGIME_ENABLED": bool(getattr(cfg, "SMA_MARKET_REGIME_ENABLED", True)),
        "SMA_COST_EDGE_ENABLED": bool(getattr(cfg, "SMA_COST_EDGE_ENABLED")),
        "SMA_COST_EDGE_MIN_RATIO": float(getattr(cfg, "SMA_COST_EDGE_MIN_RATIO")),
        "ENTRY_EDGE_BUFFER_RATIO": float(getattr(cfg, "ENTRY_EDGE_BUFFER_RATIO")),
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": float(getattr(cfg, "STRATEGY_MIN_EXPECTED_EDGE_RATIO")),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(getattr(cfg, "STRATEGY_ENTRY_SLIPPAGE_BPS")),
        "LIVE_FEE_RATE_ESTIMATE": float(getattr(cfg, "LIVE_FEE_RATE_ESTIMATE")),
        "STRATEGY_EXIT_RULES": str(getattr(cfg, "STRATEGY_EXIT_RULES")),
        "STRATEGY_EXIT_STOP_LOSS_RATIO": float(getattr(cfg, "STRATEGY_EXIT_STOP_LOSS_RATIO")),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": int(getattr(cfg, "STRATEGY_EXIT_MAX_HOLDING_MIN")),
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": float(getattr(cfg, "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO")),
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": float(
            getattr(cfg, "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO")
        ),
    }


def runtime_strategy_parameters_from_env(strategy_name: str, env: dict[str, str]) -> dict[str, Any]:
    plugin = resolve_research_strategy_plugin(strategy_name)
    if plugin.runtime_parameter_adapter is None:
        raise ResearchStrategyRegistryError(f"runtime parameter extraction unsupported: {plugin.name}")
    parameters = plugin.runtime_parameter_adapter.from_env(env)
    _assert_runtime_parameters_accepted(plugin=plugin, parameters=parameters)
    return parameters


def runtime_strategy_parameters_from_settings(strategy_name: str, cfg: object) -> dict[str, Any]:
    plugin = resolve_research_strategy_plugin(strategy_name)
    if plugin.runtime_parameter_adapter is None:
        raise ResearchStrategyRegistryError(f"runtime parameter extraction unsupported: {plugin.name}")
    parameters = plugin.runtime_parameter_adapter.from_settings(cfg)
    _assert_runtime_parameters_accepted(plugin=plugin, parameters=parameters)
    return parameters


def _assert_runtime_parameters_accepted(
    *,
    plugin: ResearchStrategyPlugin,
    parameters: dict[str, Any],
) -> None:
    unexpected = sorted(set(parameters) - set(plugin.spec.accepted_parameter_names))
    if unexpected:
        joined = ",".join(unexpected)
        raise ResearchStrategyRegistryError(f"runtime parameter extraction returned unsupported keys:{plugin.name}:{joined}")


def _run_sma_with_filter(
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
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
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _run_noop_baseline(
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    return run_noop_baseline_backtest(
        dataset=dataset,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _run_buy_and_hold_baseline(
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    return run_buy_and_hold_baseline_backtest(
        dataset=dataset,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _require_parameter(parameter_values: dict[str, Any], key: str) -> None:
    if key not in parameter_values:
        raise ResearchStrategyRegistryError(f"sma_with_filter missing required parameter: {key}")


_SMA_WITH_FILTER_PLUGIN = ResearchStrategyPlugin(
    name=SMA_WITH_FILTER_SPEC.strategy_name,
    version=SMA_WITH_FILTER_SPEC.strategy_version,
    spec=SMA_WITH_FILTER_SPEC,
    required_data=SMA_WITH_FILTER_SPEC.required_data,
    optional_data=SMA_WITH_FILTER_SPEC.optional_data,
    runner=_run_sma_with_filter,
    runtime_replay_builder=_build_sma_runtime_replay_strategy,
    runtime_parameter_adapter=RuntimeParameterAdapter(
        from_env=_sma_runtime_parameters_from_env,
        from_settings=_sma_runtime_parameters_from_settings,
    ),
    decision_contract_version=SMA_WITH_FILTER_SPEC.decision_contract_version,
    diagnostics_namespace="sma_with_filter",
)

_NOOP_BASELINE_PLUGIN = ResearchStrategyPlugin(
    name=NOOP_BASELINE_SPEC.strategy_name,
    version=NOOP_BASELINE_SPEC.strategy_version,
    spec=NOOP_BASELINE_SPEC,
    required_data=NOOP_BASELINE_SPEC.required_data,
    optional_data=NOOP_BASELINE_SPEC.optional_data,
    runner=_run_noop_baseline,
    runtime_replay_builder=None,
    runtime_parameter_adapter=None,
    decision_contract_version=NOOP_BASELINE_SPEC.decision_contract_version,
    diagnostics_namespace="noop_baseline",
)

_BUY_AND_HOLD_BASELINE_PLUGIN = ResearchStrategyPlugin(
    name=BUY_AND_HOLD_BASELINE_SPEC.strategy_name,
    version=BUY_AND_HOLD_BASELINE_SPEC.strategy_version,
    spec=BUY_AND_HOLD_BASELINE_SPEC,
    required_data=BUY_AND_HOLD_BASELINE_SPEC.required_data,
    optional_data=BUY_AND_HOLD_BASELINE_SPEC.optional_data,
    runner=_run_buy_and_hold_baseline,
    runtime_replay_builder=None,
    runtime_parameter_adapter=None,
    decision_contract_version=BUY_AND_HOLD_BASELINE_SPEC.decision_contract_version,
    diagnostics_namespace="buy_and_hold_baseline",
)


_RESEARCH_STRATEGY_PLUGINS: dict[str, ResearchStrategyPlugin] = {
    _SMA_WITH_FILTER_PLUGIN.name: _SMA_WITH_FILTER_PLUGIN,
    _NOOP_BASELINE_PLUGIN.name: _NOOP_BASELINE_PLUGIN,
    _BUY_AND_HOLD_BASELINE_PLUGIN.name: _BUY_AND_HOLD_BASELINE_PLUGIN,
}
