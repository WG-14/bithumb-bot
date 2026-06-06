from __future__ import annotations

from typing import Any

from bithumb_bot.research import sma_with_filter_plugin as runtime_contract
from bithumb_bot.research.backtest_types import BacktestRun, BacktestRunContext
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot
from bithumb_bot.research.execution_model import ExecutionModel
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy
from bithumb_bot.research.strategy_registry import (
    ResearchStrategyPlugin,
    ResearchStrategyRegistryError,
    RuntimeParameterAdapter,
)
from bithumb_bot.research.strategy_spec import SMA_WITH_FILTER_SPEC, materialize_strategy_parameters
from bithumb_bot.strategy_authoring import PromotionGradeStrategyExtension
from bithumb_bot.strategy_authoring import build_live_eligible_strategy_plugin
from bithumb_bot.strategy_authoring import research_plugin_from_event_builder
from bithumb_bot.strategy_plugins.sma_with_filter_contract import (
    SMA_DECISION_EVIDENCE_CONTRACT,
    sma_runtime_data_requirements,
)
from bithumb_bot.strategy_plugins.sma_with_filter_events import build_sma_with_filter_research_events


def materialize_sma_with_filter_research_parameters(
    *,
    plugin: ResearchStrategyPlugin,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    context: BacktestRunContext | None = None,
) -> dict[str, Any]:
    del context
    effective_parameters = materialize_strategy_parameters(
        plugin.name,
        parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    # Historical research compatibility: exploratory SMA tests intentionally ran
    # raw crosses unless a filter was explicitly supplied. Strict promotion and
    # runtime materialization remains owned by the assembly layer.
    legacy_disabled_filter_defaults = {
        "SMA_FILTER_GAP_MIN_RATIO": 0.0,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
        "SMA_COST_EDGE_ENABLED": False,
        "SMA_MARKET_REGIME_ENABLED": False,
    }
    for key, value in legacy_disabled_filter_defaults.items():
        if key not in parameter_values:
            effective_parameters[key] = value
    return effective_parameters


def run_sma_with_filter_backtest(
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
    from bithumb_bot.research.backtest_runner import run_plugin_backtest

    return run_plugin_backtest(
        plugin=SMA_WITH_FILTER_PLUGIN,
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


_SMA_WITH_FILTER_PROMOTION_EXTENSION = PromotionGradeStrategyExtension(
    runtime_replay_builder=runtime_contract.build_runtime_replay_strategy,
    runtime_parameter_adapter=RuntimeParameterAdapter(
        from_env=runtime_contract.runtime_parameters_from_env,
        from_settings=runtime_contract.runtime_parameters_from_settings,
        env_keys=(
            "SMA_SHORT",
            "SMA_LONG",
            "SMA_FILTER_GAP_MIN_RATIO",
            "SMA_FILTER_VOL_WINDOW",
            "SMA_FILTER_VOL_MIN_RANGE_RATIO",
            "SMA_FILTER_OVEREXT_LOOKBACK",
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
            "SMA_MARKET_REGIME_ENABLED",
            "SMA_COST_EDGE_ENABLED",
            "SMA_COST_EDGE_MIN_RATIO",
            "ENTRY_EDGE_BUFFER_RATIO",
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
            "STRATEGY_ENTRY_SLIPPAGE_BPS",
            "LIVE_FEE_RATE_ESTIMATE",
            "STRATEGY_EXIT_RULES",
            "STRATEGY_EXIT_STOP_LOSS_RATIO",
            "STRATEGY_EXIT_MAX_HOLDING_MIN",
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
        ),
    ),
    decision_payload_adapter=runtime_contract.decision_payload_adapter,
    exit_signal_context_builder=runtime_contract.exit_signal_context,
    exit_rule_factory=runtime_contract.exit_rule_factory,
    research_policy_decision_builder=runtime_contract.research_policy_decision_builder,
    research_export_normalizer=runtime_contract.research_export_normalizer,
    runtime_decision_adapter_factory=runtime_contract.runtime_decision_adapter_factory,
    runtime_feature_snapshot_builder=runtime_contract.runtime_feature_snapshot_builder,
    single_replay_bundle_builder=runtime_contract.single_replay_bundle_builder,
    policy_assembly_factory=runtime_contract.policy_assembly_factory,
    live_dry_run_allowed=True,
    live_real_order_allowed=True,
    approved_profile_required=True,
    fail_closed_reason="sma_with_filter_capability_missing",
    decision_evidence_contract=SMA_DECISION_EVIDENCE_CONTRACT,
    runtime_data_requirement_builder=sma_runtime_data_requirements,
)


_SMA_WITH_FILTER_RESEARCH_PLUGIN = research_plugin_from_event_builder(
    strategy_name=SMA_WITH_FILTER_SPEC.strategy_name,
    spec=SMA_WITH_FILTER_SPEC,
    version=SMA_WITH_FILTER_SPEC.strategy_version,
    required_data=SMA_WITH_FILTER_SPEC.required_data,
    optional_data=SMA_WITH_FILTER_SPEC.optional_data,
    build_research_events=build_sma_with_filter_research_events,
    diagnostics_namespace="sma_with_filter",
    research_parameter_materializer=materialize_sma_with_filter_research_parameters,
)


SMA_WITH_FILTER_PLUGIN = build_live_eligible_strategy_plugin(
    research=_SMA_WITH_FILTER_RESEARCH_PLUGIN,
    extension=_SMA_WITH_FILTER_PROMOTION_EXTENSION,
    runner=run_sma_with_filter_backtest,
).to_research_strategy_plugin()
