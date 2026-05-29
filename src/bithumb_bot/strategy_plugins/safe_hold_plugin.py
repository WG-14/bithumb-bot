from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.research.backtest_types import BacktestRun, BacktestRunContext
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot
from bithumb_bot.research.execution_model import ExecutionModel
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy
from bithumb_bot.research.strategy_registry import (
    ResearchStrategyPlugin,
    ResearchStrategyRegistryError,
    RuntimeParameterAdapter,
)
from bithumb_bot.research.strategy_spec import StrategySpec
from bithumb_bot.strategy_authoring import PromotionGradeStrategyExtension


SAFE_HOLD_STRATEGY_NAME = "safe_hold"
SAFE_HOLD_POLICY_CONTRACT_VERSION = "safe_hold_runtime_policy_v1"


def _safe_hold_runtime_decision_adapter_factory() -> Any:
    from bithumb_bot.runtime_adapters.safe_hold import SafeHoldRuntimeDecisionAdapter

    return SafeHoldRuntimeDecisionAdapter()


def _safe_hold_runtime_parameters_from_env(_env: dict[str, str]) -> dict[str, Any]:
    return {}


def _safe_hold_runtime_parameters_from_settings(_cfg: object) -> dict[str, Any]:
    return {}


@dataclass(frozen=True)
class SafeHoldPolicyAssembly:
    strategy_name: str = SAFE_HOLD_STRATEGY_NAME
    decision_contract_version: str = SAFE_HOLD_POLICY_CONTRACT_VERSION

    def materialize_parameters(self, raw: dict[str, Any]) -> dict[str, Any]:
        if raw:
            raise ValueError("safe_hold_parameters_unsupported")
        return {}


def _safe_hold_policy_assembly_factory() -> SafeHoldPolicyAssembly:
    return SafeHoldPolicyAssembly()


def run_safe_hold_research_placeholder(
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
    """Fail closed because safe_hold is a runtime fail-safe, not research parity."""
    del (
        dataset,
        parameter_values,
        fee_rate,
        slippage_bps,
        parameter_stability_score,
        execution_model,
        execution_timing_policy,
        portfolio_policy,
        context,
    )
    raise ResearchStrategyRegistryError("safe_hold is runtime fallback only and has no research runner")


SAFE_HOLD_SPEC = StrategySpec(
    strategy_name=SAFE_HOLD_STRATEGY_NAME,
    strategy_version=SAFE_HOLD_POLICY_CONTRACT_VERSION,
    accepted_parameter_names=(),
    required_parameter_names=(),
    behavior_affecting_parameter_names=(),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={},
    decision_contract_version=SAFE_HOLD_POLICY_CONTRACT_VERSION,
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={"schema_version": 1, "rules": (), "strategy_name": SAFE_HOLD_STRATEGY_NAME},
)


_SAFE_HOLD_PROMOTION_EXTENSION = PromotionGradeStrategyExtension(
    runtime_replay_builder=None,
    runtime_parameter_adapter=RuntimeParameterAdapter(
        from_env=_safe_hold_runtime_parameters_from_env,
        from_settings=_safe_hold_runtime_parameters_from_settings,
        env_keys=(),
    ),
    runtime_decision_adapter_factory=_safe_hold_runtime_decision_adapter_factory,
    policy_assembly_factory=_safe_hold_policy_assembly_factory,
    live_dry_run_allowed=False,
    live_real_order_allowed=False,
    approved_profile_required=False,
    fail_closed_reason="safe_hold_runtime_fallback_not_live_eligible",
)


SAFE_HOLD_PLUGIN = ResearchStrategyPlugin(
    name=SAFE_HOLD_SPEC.strategy_name,
    version=SAFE_HOLD_SPEC.strategy_version,
    spec=SAFE_HOLD_SPEC,
    required_data=SAFE_HOLD_SPEC.required_data,
    optional_data=SAFE_HOLD_SPEC.optional_data,
    runner=run_safe_hold_research_placeholder,
    research_event_builder=None,
    runtime_replay_builder=_SAFE_HOLD_PROMOTION_EXTENSION.runtime_replay_builder,
    runtime_parameter_adapter=_SAFE_HOLD_PROMOTION_EXTENSION.runtime_parameter_adapter,
    decision_contract_version=SAFE_HOLD_SPEC.decision_contract_version,
    diagnostics_namespace=SAFE_HOLD_STRATEGY_NAME,
    runtime_decision_adapter_factory=_SAFE_HOLD_PROMOTION_EXTENSION.runtime_decision_adapter_factory,
    policy_assembly_factory=_SAFE_HOLD_PROMOTION_EXTENSION.policy_assembly_factory,
    research_runnable=False,
    runtime_capabilities=_SAFE_HOLD_PROMOTION_EXTENSION.runtime_capabilities(),
    authoring_contract_kind="promotion_grade",
    promotion_extension_payload=_SAFE_HOLD_PROMOTION_EXTENSION.contract_payload(),
)
