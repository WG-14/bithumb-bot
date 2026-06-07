from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from bithumb_bot.research.backtest_types import BacktestRun, BacktestRunContext
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.execution_model import ExecutionModel
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy
from bithumb_bot.research.strategy_registry import (
    DecisionPayloadAdapter,
    ExitPolicyMaterializer,
    ExitRuleFactory,
    ExitSignalContextBuilder,
    ResearchEventBuilder,
    ResearchExportNormalizer,
    ResearchParameterMaterializer,
    ResearchPolicyDecisionBuilder,
    ResearchStrategyPlugin,
    RuntimeParameterAdapter,
    RuntimeReplayBuilder,
    RuntimeDataRequirementBuilder,
    RuntimeFeatureSnapshotBuilder,
    SingleReplayBundleBuilder,
    StrategyRuntimeCapabilities,
)
from bithumb_bot.research.strategy_spec import StrategySpec
from bithumb_bot.strategy_evidence_contract import DecisionEvidenceContract
from bithumb_bot.strategy_evidence_contract import GENERIC_DECISION_EVIDENCE_CONTRACT


RESEARCH_ONLY_FAIL_CLOSED_REASON = "promotion_extension_missing"
REPLAY_COMPATIBLE_FAIL_CLOSED_REASON = "replay_compatible_not_live_eligible"


@dataclass(frozen=True)
class ResearchOnlyStrategyPlugin:
    strategy_name: str
    version: str
    spec: StrategySpec
    required_data: tuple[str, ...]
    optional_data: tuple[str, ...] = ()
    research_event_builder: ResearchEventBuilder | None = None
    runner: Callable[..., BacktestRun] | None = None
    research_parameter_materializer: ResearchParameterMaterializer | None = None
    decision_contract_version: str | None = None
    diagnostics_namespace: str | None = None
    runtime_data_requirement_builder: RuntimeDataRequirementBuilder | None = None

    def __post_init__(self) -> None:
        name = str(self.strategy_name or "").strip().lower()
        if not name:
            raise ValueError("research-only strategy name must be non-empty")
        if self.research_event_builder is None:
            raise ValueError(f"research-only strategy event builder missing: {name}")
        object.__setattr__(self, "strategy_name", name)
        object.__setattr__(self, "required_data", tuple(self.required_data))
        object.__setattr__(self, "optional_data", tuple(self.optional_data))

    def to_research_strategy_plugin(self) -> ResearchStrategyPlugin:
        runner = self.runner or _runner_for_research_only_plugin(self)
        return ResearchStrategyPlugin(
            name=self.strategy_name,
            version=self.version,
            spec=self.spec,
            required_data=self.required_data,
            optional_data=self.optional_data,
            runner=runner,
            research_event_builder=self.research_event_builder,
            research_parameter_materializer=self.research_parameter_materializer,
            runtime_replay_builder=None,
            runtime_parameter_adapter=None,
            decision_contract_version=self.decision_contract_version or self.spec.decision_contract_version,
            diagnostics_namespace=self.diagnostics_namespace or self.strategy_name,
            runtime_capabilities=StrategyRuntimeCapabilities(
                promotion_runtime_decisions_supported=False,
                runtime_replay_supported=False,
                research_only=True,
                baseline_only=False,
                live_dry_run_allowed=False,
                live_real_order_allowed=False,
                approved_profile_required=False,
                fail_closed_reason=RESEARCH_ONLY_FAIL_CLOSED_REASON,
            ),
            authoring_contract_kind="research_only",
            promotion_extension_payload=None,
            decision_evidence_contract=GENERIC_DECISION_EVIDENCE_CONTRACT,
            runtime_data_requirement_builder=self.runtime_data_requirement_builder,
        )


@dataclass(frozen=True)
class PromotionGradeStrategyExtension:
    runtime_replay_builder: RuntimeReplayBuilder | None
    runtime_parameter_adapter: RuntimeParameterAdapter | None
    runtime_decision_adapter_factory: Callable[[], Any] | None
    policy_assembly_factory: Callable[[], Any] | None
    runtime_feature_snapshot_builder: RuntimeFeatureSnapshotBuilder | None = None
    research_export_normalizer: ResearchExportNormalizer | None = None
    decision_payload_adapter: DecisionPayloadAdapter | None = None
    exit_signal_context_builder: ExitSignalContextBuilder | None = None
    exit_rule_factory: ExitRuleFactory | None = None
    exit_policy_materializer: ExitPolicyMaterializer | None = None
    research_policy_decision_builder: ResearchPolicyDecisionBuilder | None = None
    single_replay_bundle_builder: SingleReplayBundleBuilder | None = None
    live_dry_run_allowed: bool = False
    live_real_order_allowed: bool = False
    approved_profile_required: bool = True
    fail_closed_reason: str = "promotion_grade_capability_missing"
    decision_evidence_contract: DecisionEvidenceContract = GENERIC_DECISION_EVIDENCE_CONTRACT
    runtime_data_requirement_builder: RuntimeDataRequirementBuilder | None = None

    def runtime_capabilities(self) -> StrategyRuntimeCapabilities:
        return StrategyRuntimeCapabilities(
            promotion_runtime_decisions_supported=True,
            runtime_replay_supported=self.runtime_replay_builder is not None,
            research_only=False,
            baseline_only=False,
            live_dry_run_allowed=bool(self.live_dry_run_allowed),
            live_real_order_allowed=bool(self.live_real_order_allowed),
            approved_profile_required=bool(self.approved_profile_required),
            fail_closed_reason=self.fail_closed_reason,
        )

    def contract_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "promotion_extension": True,
            "runtime_replay_supported": self.runtime_replay_builder is not None,
            "runtime_parameter_adapter_supported": self.runtime_parameter_adapter is not None,
            "runtime_decision_adapter_supported": self.runtime_decision_adapter_factory is not None,
            "runtime_feature_snapshot_builder_supported": self.runtime_feature_snapshot_builder is not None,
            "policy_assembly_supported": self.policy_assembly_factory is not None,
            "research_export_normalizer_supported": self.research_export_normalizer is not None,
            "single_replay_bundle_supported": self.single_replay_bundle_builder is not None,
            "exit_policy_materializer_supported": self.exit_policy_materializer is not None,
            "exit_policy_materializer_module": (
                self.exit_policy_materializer.__module__
                if self.exit_policy_materializer is not None
                else None
            ),
            "exit_policy_materializer_qualname": (
                self.exit_policy_materializer.__qualname__
                if self.exit_policy_materializer is not None
                else None
            ),
            "exit_policy_materializer_authority_scope": (
                "promotion_profile_runtime_live_authority"
                if self.exit_policy_materializer is not None
                else "unsupported"
            ),
            "approved_profile_required": bool(self.approved_profile_required),
            "live_dry_run_allowed": bool(self.live_dry_run_allowed),
            "live_real_order_allowed": bool(self.live_real_order_allowed),
            "fail_closed_reason": self.fail_closed_reason,
            "decision_evidence_contract": self.decision_evidence_contract.as_dict(),
            "runtime_data_requirement_builder_supported": self.runtime_data_requirement_builder is not None,
        }


@dataclass(frozen=True)
class ReplayCompatibleStrategyExtension:
    runtime_replay_builder: RuntimeReplayBuilder
    single_replay_bundle_builder: SingleReplayBundleBuilder | None = None
    research_export_normalizer: ResearchExportNormalizer | None = None
    decision_payload_adapter: DecisionPayloadAdapter | None = None
    exit_policy_materializer: ExitPolicyMaterializer | None = None
    research_policy_decision_builder: ResearchPolicyDecisionBuilder | None = None
    parameter_materializer: ResearchParameterMaterializer | None = None
    fail_closed_reason: str = REPLAY_COMPATIBLE_FAIL_CLOSED_REASON

    def runtime_capabilities(self) -> StrategyRuntimeCapabilities:
        return StrategyRuntimeCapabilities(
            promotion_runtime_decisions_supported=False,
            runtime_replay_supported=True,
            research_only=False,
            baseline_only=False,
            live_dry_run_allowed=False,
            live_real_order_allowed=False,
            approved_profile_required=False,
            fail_closed_reason=self.fail_closed_reason,
            replay_decisions_supported=True,
            promotion_export_supported=True,
            runtime_decision_supported=False,
        )

    def contract_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "replay_compatible_extension": True,
            "runtime_replay_supported": True,
            "single_replay_bundle_supported": self.single_replay_bundle_builder is not None,
            "research_export_normalizer_supported": self.research_export_normalizer is not None,
            "decision_payload_adapter_supported": self.decision_payload_adapter is not None,
            "exit_policy_materializer_supported": self.exit_policy_materializer is not None,
            "exit_policy_materializer_module": (
                self.exit_policy_materializer.__module__
                if self.exit_policy_materializer is not None
                else None
            ),
            "exit_policy_materializer_qualname": (
                self.exit_policy_materializer.__qualname__
                if self.exit_policy_materializer is not None
                else None
            ),
            "exit_policy_materializer_authority_scope": (
                "profile_export_replay_authority"
                if self.exit_policy_materializer is not None
                else "unsupported"
            ),
            "research_policy_decision_builder_supported": self.research_policy_decision_builder is not None,
            "approved_profile_required": False,
            "live_dry_run_allowed": False,
            "live_real_order_allowed": False,
            "fail_closed_reason": self.fail_closed_reason,
        }


@dataclass(frozen=True)
class ReplayCompatibleStrategyPlugin:
    research: ResearchOnlyStrategyPlugin
    extension: ReplayCompatibleStrategyExtension
    runner: Callable[..., BacktestRun] | None = None

    def to_research_strategy_plugin(self) -> ResearchStrategyPlugin:
        normalized = self.research.to_research_strategy_plugin()
        materializer = self.extension.parameter_materializer or normalized.research_parameter_materializer
        return ResearchStrategyPlugin(
            name=normalized.name,
            version=normalized.version,
            spec=normalized.spec,
            required_data=normalized.required_data,
            optional_data=normalized.optional_data,
            runner=self.runner or normalized.runner,
            research_event_builder=normalized.research_event_builder,
            research_parameter_materializer=materializer,
            runtime_replay_builder=self.extension.runtime_replay_builder,
            runtime_parameter_adapter=None,
            decision_contract_version=normalized.decision_contract_version,
            diagnostics_namespace=normalized.diagnostics_namespace,
            decision_payload_adapter=self.extension.decision_payload_adapter,
            exit_policy_materializer=self.extension.exit_policy_materializer,
            research_policy_decision_builder=self.extension.research_policy_decision_builder,
            research_export_normalizer=self.extension.research_export_normalizer,
            single_replay_bundle_builder=self.extension.single_replay_bundle_builder,
            runtime_capabilities=self.extension.runtime_capabilities(),
            authoring_contract_kind="replay_compatible",
            promotion_extension_payload=self.extension.contract_payload(),
            decision_evidence_contract=GENERIC_DECISION_EVIDENCE_CONTRACT,
            runtime_data_requirement_builder=normalized.runtime_data_requirement_builder,
        )


@dataclass(frozen=True)
class LiveEligibleStrategyPlugin:
    research: ResearchOnlyStrategyPlugin
    extension: PromotionGradeStrategyExtension
    runner: Callable[..., BacktestRun] | None = None

    def to_research_strategy_plugin(self) -> ResearchStrategyPlugin:
        return promotion_grade_plugin(
            research=self.research,
            extension=self.extension,
            runner=self.runner,
        )


def build_replay_compatible_strategy_plugin(
    *,
    research: ResearchOnlyStrategyPlugin,
    extension: ReplayCompatibleStrategyExtension,
    runner: Callable[..., BacktestRun] | None = None,
) -> ReplayCompatibleStrategyPlugin:
    return ReplayCompatibleStrategyPlugin(
        research=research,
        extension=extension,
        runner=runner,
    )


def build_live_eligible_strategy_plugin(
    *,
    research: ResearchOnlyStrategyPlugin,
    extension: PromotionGradeStrategyExtension,
    runner: Callable[..., BacktestRun] | None = None,
) -> LiveEligibleStrategyPlugin:
    return LiveEligibleStrategyPlugin(
        research=research,
        extension=extension,
        runner=runner,
    )


def research_plugin_from_event_builder(
    *,
    strategy_name: str,
    version: str,
    spec: StrategySpec,
    required_data: tuple[str, ...],
    optional_data: tuple[str, ...] = (),
    build_research_events: ResearchEventBuilder,
    diagnostics_namespace: str | None = None,
    research_parameter_materializer: ResearchParameterMaterializer | None = None,
    runtime_data_requirement_builder: RuntimeDataRequirementBuilder | None = None,
) -> ResearchOnlyStrategyPlugin:
    return ResearchOnlyStrategyPlugin(
        strategy_name=strategy_name,
        version=version,
        spec=spec,
        required_data=required_data,
        optional_data=optional_data,
        research_event_builder=build_research_events,
        diagnostics_namespace=diagnostics_namespace,
        research_parameter_materializer=research_parameter_materializer,
        runtime_data_requirement_builder=runtime_data_requirement_builder,
    )


def research_plugin_from_decide_snapshot(
    *,
    strategy_name: str,
    version: str,
    spec: StrategySpec,
    required_data: tuple[str, ...],
    decide_snapshot: Callable[..., dict[str, Any]],
    diagnostics_namespace: str | None = None,
    optional_data: tuple[str, ...] = (),
) -> ResearchOnlyStrategyPlugin:
    from bithumb_bot.research.execution_timing import candle_close_ts

    normalized_strategy_name = str(strategy_name or "").strip().lower()

    def build_research_events(
        *,
        dataset: DatasetSnapshot,
        parameter_values: dict[str, Any],
        fee_rate: float,
        slippage_bps: float,
        execution_timing_policy: ExecutionTimingPolicy,
        portfolio_policy: PortfolioPolicy,
        context: Any | None = None,
    ) -> tuple[ResearchDecisionEvent, ...]:
        del fee_rate, slippage_bps, portfolio_policy, context
        events: list[ResearchDecisionEvent] = []
        for candle_index, candle in enumerate(dataset.candles):
            decision = decide_snapshot(
                candle=candle,
                candle_index=candle_index,
                dataset=dataset,
                parameter_values=parameter_values,
            )
            signal = str(decision.get("signal") or decision.get("final_signal") or "HOLD").upper()
            reason = str(decision.get("reason") or "research_only_decision")
            decision_ts = candle_close_ts(candle, interval=dataset.interval) + int(
                execution_timing_policy.decision_guard_ms
            )
            feature_snapshot = dict(decision.get("feature_snapshot") or {})
            feature_snapshot.setdefault("candle_index", int(candle_index))
            feature_snapshot.setdefault("close", float(candle.close))
            diagnostics = dict(decision.get("strategy_diagnostics") or {})
            diagnostics.setdefault("schema_version", 1)
            events.append(
                ResearchDecisionEvent(
                    candle_ts=int(candle.ts),
                    decision_ts=int(decision_ts),
                    strategy_name=normalized_strategy_name,
                    strategy_version=version,
                    raw_signal=signal,
                    final_signal=signal,
                    reason=reason,
                    feature_snapshot=feature_snapshot,
                    strategy_diagnostics=diagnostics,
                    entry_signal=signal if signal == "BUY" else "HOLD",
                    exit_signal=signal if signal == "SELL" else "HOLD",
                    order_intent=(
                        dict(decision["order_intent"])
                        if isinstance(decision.get("order_intent"), dict)
                        else None
                    ),
                    exit_intent=(
                        dict(decision["exit_intent"])
                        if isinstance(decision.get("exit_intent"), dict)
                        else None
                    ),
                    extra_payload=dict(decision.get("extra_payload") or {}),
                )
            )
        return tuple(events)

    return research_plugin_from_event_builder(
        strategy_name=normalized_strategy_name,
        version=version,
        spec=spec,
        required_data=required_data,
        optional_data=optional_data,
        build_research_events=build_research_events,
        diagnostics_namespace=diagnostics_namespace,
    )


def promotion_grade_plugin(
    *,
    research: ResearchOnlyStrategyPlugin,
    extension: PromotionGradeStrategyExtension,
    runner: Callable[..., BacktestRun] | None = None,
) -> ResearchStrategyPlugin:
    normalized = research.to_research_strategy_plugin()
    return ResearchStrategyPlugin(
        name=normalized.name,
        version=normalized.version,
        spec=normalized.spec,
        required_data=normalized.required_data,
        optional_data=normalized.optional_data,
        runner=runner or normalized.runner,
        research_event_builder=normalized.research_event_builder,
        research_parameter_materializer=normalized.research_parameter_materializer,
        runtime_replay_builder=extension.runtime_replay_builder,
        runtime_parameter_adapter=extension.runtime_parameter_adapter,
        decision_contract_version=normalized.decision_contract_version,
        diagnostics_namespace=normalized.diagnostics_namespace,
        decision_payload_adapter=extension.decision_payload_adapter,
        exit_signal_context_builder=extension.exit_signal_context_builder,
        exit_rule_factory=extension.exit_rule_factory,
        exit_policy_materializer=extension.exit_policy_materializer,
        research_policy_decision_builder=extension.research_policy_decision_builder,
        research_export_normalizer=extension.research_export_normalizer,
        runtime_decision_adapter_factory=extension.runtime_decision_adapter_factory,
        runtime_feature_snapshot_builder=extension.runtime_feature_snapshot_builder,
        single_replay_bundle_builder=extension.single_replay_bundle_builder,
        policy_assembly_factory=extension.policy_assembly_factory,
        runtime_capabilities=extension.runtime_capabilities(),
        authoring_contract_kind="promotion_grade",
        promotion_extension_payload=extension.contract_payload(),
        decision_evidence_contract=extension.decision_evidence_contract,
        runtime_data_requirement_builder=(
            extension.runtime_data_requirement_builder
            or normalized.runtime_data_requirement_builder
        ),
    )


def _runner_for_research_only_plugin(authoring_plugin: ResearchOnlyStrategyPlugin) -> Callable[..., BacktestRun]:
    def run_research_only_backtest(
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
        from bithumb_bot.research.backtest_runner import run_plugin_backtest

        return run_plugin_backtest(
            plugin=authoring_plugin.to_research_strategy_plugin(),
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

    return run_research_only_backtest
