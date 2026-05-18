from __future__ import annotations

import json
import itertools
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.execution_reality_contract import (
    evaluate_execution_reality_policy,
    unsupported_capability_reasons,
)
from bithumb_bot.market_regime import RegimeAcceptanceGate

from .deployment_policy import DEPLOYMENT_TIERS, is_production_bound_target, normalize_deployment_tier
from .hashing import sha256_prefixed
from .audit_trail import AuditTrailPolicy as ResearchAuditTrailPolicy


class ManifestValidationError(ValueError):
    pass


@dataclass(frozen=True)
class DateRange:
    start: str
    end: str

    def start_ts_ms(self) -> int:
        return _date_start_ts_ms(self.start)

    def end_ts_ms(self) -> int:
        return _date_end_ts_ms(self.end)

    def as_dict(self) -> dict[str, str]:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True)
class DatasetSplit:
    train: DateRange
    validation: DateRange
    final_holdout: DateRange | None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "train": self.train.as_dict(),
            "validation": self.validation.as_dict(),
        }
        if self.final_holdout is not None:
            payload["final_holdout"] = self.final_holdout.as_dict()
        return payload


@dataclass(frozen=True)
class TopOfBookDatasetSpec:
    source: str = "sqlite_orderbook_top_snapshots"
    required: bool = False
    join_tolerance_ms: int = 3000
    missing_policy: str = "warn"
    quote_source: str | None = None
    min_coverage_pct: float = 100.0

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "source": self.source,
            "required": self.required,
            "join_tolerance_ms": self.join_tolerance_ms,
            "missing_policy": self.missing_policy,
            "min_coverage_pct": self.min_coverage_pct,
        }
        if self.quote_source is not None:
            payload["quote_source"] = self.quote_source
        return payload


@dataclass(frozen=True)
class DatasetSpec:
    source: str
    snapshot_id: str
    split: DatasetSplit
    top_of_book: TopOfBookDatasetSpec | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "source": self.source,
            "snapshot_id": self.snapshot_id,
            **self.split.as_dict(),
        }
        if self.top_of_book is not None:
            payload["top_of_book"] = self.top_of_book.as_dict()
        return payload


@dataclass(frozen=True)
class CostModel:
    fee_rate: float
    slippage_bps: tuple[float, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "fee_rate": self.fee_rate,
            "slippage_bps": list(self.slippage_bps),
        }


@dataclass(frozen=True)
class ScenarioCostAssumption:
    label: str
    role: str
    fee_rate: float
    fee_source: str
    fee_authority_policy: str
    slippage_bps: float
    slippage_source: str
    valid_for: dict[str, object] | None = None
    promotable_as_base: bool = False
    source: str = "execution_model"

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "label": self.label,
            "role": self.role,
            "fee_rate": self.fee_rate,
            "fee_source": self.fee_source,
            "fee_authority_policy": self.fee_authority_policy,
            "slippage_bps": self.slippage_bps,
            "slippage_source": self.slippage_source,
            "promotable_as_base": self.promotable_as_base,
            "source": self.source,
        }
        if self.valid_for is not None:
            payload["valid_for"] = dict(self.valid_for)
        return payload


@dataclass(frozen=True)
class ExecutionScenario:
    type: str
    fee_rate: float
    slippage_bps: float
    latency_ms: int = 0
    partial_fill_rate: float = 0.0
    order_failure_rate: float = 0.0
    market_order_extra_cost_bps: float = 0.0
    seed: int | None = None
    source: str = "execution_model"
    scenario_policy: str = "single_scenario"
    scenario_role: str = "base"
    scenario_role_source: str = "derived"
    cost_assumption: ScenarioCostAssumption | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "type": self.type,
            "fee_rate": self.fee_rate,
            "slippage_bps": self.slippage_bps,
            "latency_ms": self.latency_ms,
            "partial_fill_rate": self.partial_fill_rate,
            "order_failure_rate": self.order_failure_rate,
            "market_order_extra_cost_bps": self.market_order_extra_cost_bps,
            "stress_extra_cost_bps": self.market_order_extra_cost_bps,
            "market_order_extra_cost_semantics": "scalar_stress_extra_cost_not_depth_aware_market_impact",
            "market_impact_model_available": False,
            "seed": self.seed,
            "source": self.source,
            "scenario_policy": self.scenario_policy,
            "scenario_role": self.scenario_role,
            "scenario_role_source": self.scenario_role_source,
        }
        if self.cost_assumption is not None:
            payload["cost_assumption"] = self.cost_assumption.as_dict()
            payload["cost_assumption_label"] = self.cost_assumption.label
            payload["fee_source"] = self.cost_assumption.fee_source
            payload["fee_authority_policy"] = self.cost_assumption.fee_authority_policy
            payload["slippage_source"] = self.cost_assumption.slippage_source
            payload["promotable_as_base"] = self.cost_assumption.promotable_as_base
        return payload


@dataclass(frozen=True)
class ExecutionModelConfig:
    scenarios: tuple[ExecutionScenario, ...]
    source: str
    scenario_policy: str
    calibration_required: bool = False
    calibration_strictness: str = "fail"

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "scenario_policy": self.scenario_policy,
            "calibration_required": self.calibration_required,
            "calibration_strictness": self.calibration_strictness,
            "scenarios": [scenario.as_dict() for scenario in self.scenarios],
        }


@dataclass(frozen=True)
class ExecutionTimingPolicy:
    signal_basis: str = "closed_candle"
    decision_time: str = "candle_close"
    decision_guard_ms: int = 0
    fill_reference_policy: str = "candle_close_legacy"
    quote_selection: str = "first_after_or_equal"
    max_quote_wait_ms: int = 3000
    missing_quote_policy: str = "warn"
    allow_same_candle_close_fill: bool = True
    min_execution_reality_level_for_promotion: str | None = None
    depth_required: bool = False
    trade_tick_required: bool = False
    queue_position_required: bool = False
    market_impact_required: bool = False
    intra_candle_path_required: bool = False
    source: str = "legacy_default"

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "signal_basis": self.signal_basis,
            "decision_time": self.decision_time,
            "decision_guard_ms": self.decision_guard_ms,
            "fill_reference_policy": self.fill_reference_policy,
            "quote_selection": self.quote_selection,
            "max_quote_wait_ms": self.max_quote_wait_ms,
            "missing_quote_policy": self.missing_quote_policy,
            "allow_same_candle_close_fill": self.allow_same_candle_close_fill,
            "source": self.source,
        }
        if self.min_execution_reality_level_for_promotion is not None:
            payload["min_execution_reality_level_for_promotion"] = self.min_execution_reality_level_for_promotion
        payload["depth_required"] = self.depth_required
        payload["trade_tick_required"] = self.trade_tick_required
        payload["queue_position_required"] = self.queue_position_required
        payload["market_impact_required"] = self.market_impact_required
        payload["intra_candle_path_required"] = self.intra_candle_path_required
        return payload


@dataclass(frozen=True)
class AcceptanceGate:
    min_trade_count: int
    max_mdd_pct: float
    min_profit_factor: float
    oos_return_must_be_positive: bool
    parameter_stability_required: bool
    walk_forward_required: bool = False
    final_holdout_required_for_promotion: bool = True
    min_cagr_pct: float | None = None
    min_expectancy_per_trade_krw: float | None = None
    min_expectancy_per_trade_pct: float | None = None
    max_exposure_time_pct: float | None = None
    max_avg_holding_time_minutes: float | None = None
    max_fee_drag_ratio: float | None = None
    max_slippage_drag_ratio: float | None = None
    max_single_trade_dependency_score: float | None = None
    reject_open_position_at_end: bool = False
    metrics_contract_required: bool = False
    regime_acceptance_gate: RegimeAcceptanceGate = field(default_factory=RegimeAcceptanceGate)

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "min_trade_count": self.min_trade_count,
            "max_mdd_pct": self.max_mdd_pct,
            "min_profit_factor": self.min_profit_factor,
            "oos_return_must_be_positive": self.oos_return_must_be_positive,
            "parameter_stability_required": self.parameter_stability_required,
            "walk_forward_required": self.walk_forward_required,
            "final_holdout_required_for_promotion": self.final_holdout_required_for_promotion,
            "regime_acceptance_gate": self.regime_acceptance_gate.as_dict(),
        }
        optional_fields = {
            "min_cagr_pct": self.min_cagr_pct,
            "min_expectancy_per_trade_krw": self.min_expectancy_per_trade_krw,
            "min_expectancy_per_trade_pct": self.min_expectancy_per_trade_pct,
            "max_exposure_time_pct": self.max_exposure_time_pct,
            "max_avg_holding_time_minutes": self.max_avg_holding_time_minutes,
            "max_fee_drag_ratio": self.max_fee_drag_ratio,
            "max_slippage_drag_ratio": self.max_slippage_drag_ratio,
            "max_single_trade_dependency_score": self.max_single_trade_dependency_score,
        }
        payload.update(optional_fields)
        payload["reject_open_position_at_end"] = self.reject_open_position_at_end
        payload["metrics_contract_required"] = self.metrics_contract_required
        return payload


@dataclass(frozen=True)
class WalkForwardConfig:
    train_window_days: int
    test_window_days: int
    step_days: int
    min_windows: int

    def as_dict(self) -> dict[str, int]:
        return {
            "train_window_days": self.train_window_days,
            "test_window_days": self.test_window_days,
            "step_days": self.step_days,
            "min_windows": self.min_windows,
        }


@dataclass(frozen=True)
class ResearchArtifactPolicy:
    candidate_journal: bool = True
    failed_candidate_evidence: bool = True
    full_decisions_external_jsonl: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_journal": bool(self.candidate_journal),
            "failed_candidate_evidence": bool(self.failed_candidate_evidence),
            "full_decisions_external_jsonl": bool(self.full_decisions_external_jsonl),
        }


@dataclass(frozen=True)
class ResearchResourceLimits:
    max_runtime_s_per_candidate_split: float | None = 300.0
    max_decisions_retained: int | None = 0
    max_trades: int | None = 5000
    max_equity_points_retained: int | None = 0
    max_rss_mb: float | None = 1400.0

    def as_dict(self) -> dict[str, object]:
        return {
            "max_runtime_s_per_candidate_split": self.max_runtime_s_per_candidate_split,
            "max_decisions_retained": self.max_decisions_retained,
            "max_trades": self.max_trades,
            "max_equity_points_retained": self.max_equity_points_retained,
            "max_rss_mb": self.max_rss_mb,
        }


@dataclass(frozen=True)
class ResearchHeartbeatPolicy:
    interval_s: float | None = 10.0
    bar_interval: int | None = 10000

    def as_dict(self) -> dict[str, object]:
        return {
            "interval_s": self.interval_s,
            "bar_interval": self.bar_interval,
        }


@dataclass(frozen=True)
class ResearchRunPolicy:
    report_detail: str = "summary"
    artifact_policy: ResearchArtifactPolicy = field(default_factory=ResearchArtifactPolicy)
    audit_trail: ResearchAuditTrailPolicy = field(default_factory=ResearchAuditTrailPolicy)
    resource_limits: ResearchResourceLimits = field(default_factory=ResearchResourceLimits)
    heartbeat: ResearchHeartbeatPolicy = field(default_factory=ResearchHeartbeatPolicy)

    def as_dict(self) -> dict[str, object]:
        return {
            "report_detail": self.report_detail,
            "artifact_policy": self.artifact_policy.as_dict(),
            "audit_trail": self.audit_trail.as_dict(),
            "resource_limits": self.resource_limits.as_dict(),
            "heartbeat": self.heartbeat.as_dict(),
        }


@dataclass(frozen=True)
class StatisticalBootstrapConfig:
    method: str
    n_bootstrap: int
    block_length_policy: str
    seed_policy: str

    def as_dict(self) -> dict[str, object]:
        return {
            "method": self.method,
            "n_bootstrap": self.n_bootstrap,
            "block_length_policy": self.block_length_policy,
            "seed_policy": self.seed_policy,
        }


@dataclass(frozen=True)
class StatisticalValidationGates:
    max_reality_check_p_value: float
    max_spa_p_value: float | None = None
    min_deflated_sharpe_probability: float | None = None
    max_holdout_reuse_count: int = 0
    max_attempt_index_without_new_hypothesis: int = 1

    def as_dict(self) -> dict[str, object]:
        return {
            "max_reality_check_p_value": self.max_reality_check_p_value,
            "max_spa_p_value": self.max_spa_p_value,
            "min_deflated_sharpe_probability": self.min_deflated_sharpe_probability,
            "max_holdout_reuse_count": self.max_holdout_reuse_count,
            "max_attempt_index_without_new_hypothesis": self.max_attempt_index_without_new_hypothesis,
        }


@dataclass(frozen=True)
class StatisticalSelectionContract:
    required_for_promotion: bool
    benchmark: str
    primary_metric: str
    selection_universe: str
    multiple_testing_scope: str
    bootstrap: StatisticalBootstrapConfig
    gates: StatisticalValidationGates

    def as_dict(self) -> dict[str, object]:
        return {
            "required_for_promotion": self.required_for_promotion,
            "benchmark": self.benchmark,
            "primary_metric": self.primary_metric,
            "selection_universe": self.selection_universe,
            "multiple_testing_scope": self.multiple_testing_scope,
            "bootstrap": self.bootstrap.as_dict(),
            "gates": self.gates.as_dict(),
        }


@dataclass(frozen=True)
class StressTradeRemovalContract:
    top_n_by_net_pnl: tuple[int, ...]
    min_return_retention_pct: float | None = None
    max_mdd_multiplier: float | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"top_n_by_net_pnl": list(self.top_n_by_net_pnl)}
        if self.min_return_retention_pct is not None:
            payload["min_return_retention_pct"] = self.min_return_retention_pct
        if self.max_mdd_multiplier is not None:
            payload["max_mdd_multiplier"] = self.max_mdd_multiplier
        return payload


@dataclass(frozen=True)
class StressTradeOrderMonteCarloContract:
    iterations: int
    seed_policy: str
    min_survival_probability: float
    ruin_max_drawdown_pct: float
    min_closed_trades: int = 10

    def as_dict(self) -> dict[str, object]:
        return {
            "iterations": self.iterations,
            "seed_policy": self.seed_policy,
            "min_survival_probability": self.min_survival_probability,
            "ruin_max_drawdown_pct": self.ruin_max_drawdown_pct,
            "min_closed_trades": self.min_closed_trades,
        }


@dataclass(frozen=True)
class StressRiskAdjustedScoreContract:
    required_metrics: tuple[str, ...]
    ranking: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "required_metrics": list(self.required_metrics),
            "ranking": list(self.ranking),
        }


@dataclass(frozen=True)
class StressPeriodAblationContract:
    calendar_years: tuple[int, ...] | str
    min_pass_ratio: float = 0.8
    min_return_retention_pct: float = 50.0

    def as_dict(self) -> dict[str, object]:
        return {
            "calendar_years": self.calendar_years if self.calendar_years == "auto" else list(self.calendar_years),
            "min_pass_ratio": self.min_pass_ratio,
            "min_return_retention_pct": self.min_return_retention_pct,
        }


@dataclass(frozen=True)
class StressParameterPerturbationContract:
    relative_pct: tuple[float, ...]
    numeric_params_only: bool = True
    min_pass_ratio: float = 0.8

    def as_dict(self) -> dict[str, object]:
        return {
            "relative_pct": list(self.relative_pct),
            "numeric_params_only": self.numeric_params_only,
            "min_pass_ratio": self.min_pass_ratio,
        }


@dataclass(frozen=True)
class StressSuiteContract:
    required_for_promotion: bool
    trade_removal: StressTradeRemovalContract | None = None
    trade_order_monte_carlo: StressTradeOrderMonteCarloContract | None = None
    period_ablation: StressPeriodAblationContract | None = None
    parameter_perturbation: StressParameterPerturbationContract | None = None
    risk_adjusted_score: StressRiskAdjustedScoreContract | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "required_for_promotion": self.required_for_promotion,
        }
        if self.trade_removal is not None:
            payload["trade_removal"] = self.trade_removal.as_dict()
        if self.trade_order_monte_carlo is not None:
            payload["trade_order_monte_carlo"] = self.trade_order_monte_carlo.as_dict()
        if self.period_ablation is not None:
            payload["period_ablation"] = self.period_ablation.as_dict()
        if self.parameter_perturbation is not None:
            payload["parameter_perturbation"] = self.parameter_perturbation.as_dict()
        if self.risk_adjusted_score is not None:
            payload["risk_adjusted_score"] = self.risk_adjusted_score.as_dict()
        return payload


@dataclass(frozen=True)
class FinalSelectionMetricRule:
    metric: str
    order: str
    required: bool = True
    null_policy: str = "fail_if_required_else_worst_rank"

    def as_dict(self) -> dict[str, object]:
        return {
            "metric": self.metric,
            "order": self.order,
            "required": self.required,
            "null_policy": self.null_policy,
        }


@dataclass(frozen=True)
class FinalSelectionContract:
    schema_version: int
    required_for_promotion: bool
    candidate_universe: str
    must_pass: dict[str, object]
    selection_exposure_policy: dict[str, object]
    method: str
    null_metric_policy: str
    ranking: tuple[FinalSelectionMetricRule, ...]
    unsupported_metric_policy: dict[str, str]

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "required_for_promotion": self.required_for_promotion,
            "candidate_universe": self.candidate_universe,
            "must_pass": dict(self.must_pass),
            "selection_exposure_policy": dict(self.selection_exposure_policy),
            "method": self.method,
            "null_metric_policy": self.null_metric_policy,
            "ranking": [rule.as_dict() for rule in self.ranking],
            "unsupported_metric_policy": dict(self.unsupported_metric_policy),
        }


@dataclass(frozen=True)
class ExperimentManifest:
    experiment_id: str
    hypothesis: str
    strategy_name: str
    market: str
    interval: str
    dataset: DatasetSpec
    parameter_space: dict[str, tuple[object, ...]]
    cost_model: CostModel
    execution_model: ExecutionModelConfig
    execution_timing: ExecutionTimingPolicy
    deployment_tier: str
    acceptance_gate: AcceptanceGate
    statistical_validation: StatisticalSelectionContract | None
    stress_suite: StressSuiteContract | None
    final_selection: FinalSelectionContract | None
    walk_forward: WalkForwardConfig | None
    research_run: ResearchRunPolicy
    raw: dict[str, Any]

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "hypothesis": self.hypothesis,
            "strategy_name": self.strategy_name,
            "market": self.market,
            "interval": self.interval,
            "dataset": self.dataset.as_dict(),
            "parameter_space": {key: sorted(list(value), key=repr) for key, value in sorted(self.parameter_space.items())},
            "cost_model": self.cost_model.as_dict(),
            "execution_model": self.execution_model.as_dict(),
            "execution_timing": self.execution_timing.as_dict(),
            "deployment_tier": self.deployment_tier,
            "dataset_quality_policy": _canonical_dataset_quality_policy(self.raw.get("dataset_quality_policy")),
            "acceptance_gate": self.acceptance_gate.as_dict(),
            "statistical_validation": (
                self.statistical_validation.as_dict()
                if self.statistical_validation is not None
                else None
            ),
            "stress_suite": self.stress_suite.as_dict() if self.stress_suite is not None else None,
            "final_selection": (
                self.final_selection.as_dict()
                if self.final_selection is not None
                else None
            ),
            "walk_forward": self.walk_forward.as_dict() if self.walk_forward is not None else None,
            "research_run": self.research_run.as_dict(),
        }

    def manifest_hash(self) -> str:
        return sha256_prefixed(self.canonical_payload())


def load_manifest(path: str | Path) -> ExperimentManifest:
    manifest_path = Path(path).expanduser()
    with manifest_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return parse_manifest(payload)


def parse_manifest(payload: dict[str, Any]) -> ExperimentManifest:
    if not isinstance(payload, dict):
        raise ManifestValidationError("manifest must be a JSON object")

    experiment_id = _required_str(payload, "experiment_id")
    hypothesis = _required_str(payload, "hypothesis")
    strategy_name = _required_str(payload, "strategy_name")
    market = _required_str(payload, "market")
    interval = _required_str(payload, "interval")
    dataset_payload = _required_dict(payload, "dataset")
    dataset = _parse_dataset(dataset_payload)
    parameter_space = _parse_parameter_space(payload.get("parameter_space"))
    if payload.get("cost_model") is None and payload.get("execution_model") is None:
        raise ManifestValidationError("manifest requires cost_model or execution_model")
    cost_model = _parse_cost_model(payload.get("cost_model"))
    execution_model = _parse_execution_model(payload.get("execution_model"), cost_model)
    execution_timing_payload = payload.get("execution_timing")
    execution_timing = _parse_execution_timing(execution_timing_payload)
    _validate_execution_model_capability_policy(
        execution_model=execution_model,
        execution_timing=execution_timing,
    )
    _parse_dataset_quality_policy(payload.get("dataset_quality_policy"))
    deployment_tier = _parse_deployment_tier(payload.get("deployment_tier") or payload.get("promotion_target"))
    acceptance_gate = _parse_acceptance_gate(_required_dict(payload, "acceptance_gate"))
    if is_production_bound_target(deployment_tier) and acceptance_gate.max_single_trade_dependency_score is None:
        acceptance_gate = replace(acceptance_gate, max_single_trade_dependency_score=0.8)
    statistical_validation = _parse_statistical_validation(
        payload.get("statistical_validation"),
        deployment_tier=deployment_tier,
    )
    stress_suite = _parse_stress_suite(payload.get("stress_suite"), deployment_tier=deployment_tier)
    final_selection = _parse_final_selection(payload.get("final_selection"), deployment_tier=deployment_tier)
    walk_forward = _parse_walk_forward(payload.get("walk_forward"))
    research_run = _parse_research_run(payload.get("research_run"))
    if acceptance_gate.walk_forward_required and walk_forward is None:
        raise ManifestValidationError("walk_forward is required when acceptance_gate.walk_forward_required=true")
    _validate_execution_reality_manifest_policy(
        deployment_tier=deployment_tier,
        dataset=dataset,
        execution_timing=execution_timing,
        execution_timing_declared="execution_timing" in payload and execution_timing_payload is not None,
        execution_timing_declared_fields=(
            set(execution_timing_payload)
            if isinstance(execution_timing_payload, dict)
            else set()
        ),
    )

    _validate_split_order(dataset.split)
    cost_policy_reasons = production_cost_assumption_policy_reasons(
        deployment_tier=deployment_tier,
        execution_model=execution_model,
    )
    if cost_policy_reasons:
        raise ManifestValidationError(",".join(cost_policy_reasons))

    return ExperimentManifest(
        experiment_id=experiment_id,
        hypothesis=hypothesis,
        strategy_name=strategy_name,
        market=market,
        interval=interval,
        dataset=dataset,
        parameter_space=parameter_space,
        cost_model=cost_model,
        execution_model=execution_model,
        execution_timing=execution_timing,
        deployment_tier=deployment_tier,
        acceptance_gate=acceptance_gate,
        statistical_validation=statistical_validation,
        stress_suite=stress_suite,
        final_selection=final_selection,
        walk_forward=walk_forward,
        research_run=research_run,
        raw=dict(payload),
    )


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"manifest field {key!r} is required")
    return value.strip()


def _required_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ManifestValidationError(f"manifest field {key!r} must be an object")
    return value


def _parse_deployment_tier(value: Any) -> str:
    tier = normalize_deployment_tier(value)
    if value is not None and tier == "research_only" and str(value).strip().lower() not in DEPLOYMENT_TIERS:
        raise ManifestValidationError(
            "deployment_tier must be one of research_only, paper_candidate, live_dry_run_candidate, small_live_candidate"
        )
    return tier


def _parse_date_range(payload: dict[str, Any], key: str) -> DateRange:
    section = payload.get(key)
    if not isinstance(section, dict):
        raise ManifestValidationError(f"dataset.{key} must be an object")
    date_range = DateRange(start=_required_str(section, "start"), end=_required_str(section, "end"))
    if date_range.start_ts_ms() > date_range.end_ts_ms():
        raise ManifestValidationError(f"dataset.{key}.start must be earlier than or equal to end")
    return date_range


def _parse_dataset(payload: dict[str, Any]) -> DatasetSpec:
    allowed_fields = {"source", "snapshot_id", "train", "validation", "final_holdout", "top_of_book"}
    unknown = sorted(set(payload) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"dataset unsupported fields: {','.join(unknown)}")
    source = _required_str(payload, "source")
    if source != "sqlite_candles":
        raise ManifestValidationError("dataset.source currently supports only 'sqlite_candles'")
    split = DatasetSplit(
        train=_parse_date_range(payload, "train"),
        validation=_parse_date_range(payload, "validation"),
        final_holdout=(
            _parse_date_range(payload, "final_holdout")
            if isinstance(payload.get("final_holdout"), dict)
            else None
        ),
    )
    return DatasetSpec(
        source=source,
        snapshot_id=_required_str(payload, "snapshot_id"),
        split=split,
        top_of_book=_parse_top_of_book_dataset(payload.get("top_of_book")),
    )


def _parse_dataset_quality_policy(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, dict):
        raise ManifestValidationError("dataset_quality_policy must be an object")
    allowed_fields = {
        "dense_candles_required",
        "missing_candle_policy",
        "allow_classified_no_trade_missing",
        "require_retry_attempts_for_missing_ranges",
        "max_unclassified_missing_buckets",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"dataset_quality_policy unsupported fields: {','.join(unknown)}")
    missing_policy = str(value.get("missing_candle_policy") or "fail").strip().lower()
    if missing_policy not in {"fail", "diagnostic_only"}:
        raise ManifestValidationError("dataset_quality_policy.missing_candle_policy must be fail or diagnostic_only")
    max_unclassified = _positive_or_zero_int(
        value.get("max_unclassified_missing_buckets", 0),
        "dataset_quality_policy.max_unclassified_missing_buckets",
    )
    if bool(value.get("dense_candles_required", True)) and missing_policy != "fail":
        raise ManifestValidationError("dataset_quality_policy dense_candles_required=true requires missing_candle_policy=fail")
    if max_unclassified != 0 and missing_policy == "fail":
        raise ManifestValidationError("dataset_quality_policy fail mode requires max_unclassified_missing_buckets=0")


def _canonical_dataset_quality_policy(value: Any) -> dict[str, object]:
    if not isinstance(value, dict):
        return {
            "dense_candles_required": True,
            "missing_candle_policy": "fail",
            "allow_classified_no_trade_missing": False,
            "require_retry_attempts_for_missing_ranges": True,
            "max_unclassified_missing_buckets": 0,
        }
    return {
        "dense_candles_required": bool(value.get("dense_candles_required", True)),
        "missing_candle_policy": str(value.get("missing_candle_policy") or "fail").strip().lower(),
        "allow_classified_no_trade_missing": bool(value.get("allow_classified_no_trade_missing", False)),
        "require_retry_attempts_for_missing_ranges": bool(value.get("require_retry_attempts_for_missing_ranges", True)),
        "max_unclassified_missing_buckets": int(value.get("max_unclassified_missing_buckets", 0) or 0),
    }


def _parse_top_of_book_dataset(value: Any) -> TopOfBookDatasetSpec | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("dataset.top_of_book must be an object")
    allowed_fields = {
        "source",
        "required",
        "join_tolerance_ms",
        "missing_policy",
        "quote_source",
        "min_coverage_pct",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"dataset.top_of_book unsupported fields: {','.join(unknown)}")
    source = str(value.get("source") or "sqlite_orderbook_top_snapshots").strip()
    if source != "sqlite_orderbook_top_snapshots":
        raise ManifestValidationError("dataset.top_of_book.source must be sqlite_orderbook_top_snapshots")
    join_tolerance_ms = _positive_int(value.get("join_tolerance_ms", 3000), "dataset.top_of_book.join_tolerance_ms")
    missing_policy = str(value.get("missing_policy") or "warn").strip().lower()
    if missing_policy not in {"warn", "fail"}:
        raise ManifestValidationError("dataset.top_of_book.missing_policy must be warn or fail")
    required = bool(value.get("required", False))
    quote_source = value.get("quote_source")
    parsed_quote_source = None
    if quote_source is not None:
        parsed_quote_source = str(quote_source).strip()
        if not parsed_quote_source:
            raise ManifestValidationError("dataset.top_of_book.quote_source must be non-empty when supplied")
    min_coverage_pct = _finite_non_negative_float(
        value.get("min_coverage_pct", 100.0),
        "dataset.top_of_book.min_coverage_pct",
    )
    if min_coverage_pct > 100.0:
        raise ManifestValidationError("dataset.top_of_book.min_coverage_pct must be <= 100")
    return TopOfBookDatasetSpec(
        source=source,
        required=required,
        join_tolerance_ms=join_tolerance_ms,
        missing_policy=missing_policy,
        quote_source=parsed_quote_source,
        min_coverage_pct=min_coverage_pct,
    )


def _parse_parameter_space(value: Any) -> dict[str, tuple[object, ...]]:
    if not isinstance(value, dict) or not value:
        raise ManifestValidationError("parameter_space must be a non-empty object")
    out: dict[str, tuple[object, ...]] = {}
    for key, raw_values in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ManifestValidationError("parameter_space keys must be non-empty strings")
        if not isinstance(raw_values, list) or len(raw_values) == 0:
            raise ManifestValidationError(f"parameter_space.{key} must be a non-empty array")
        out[key.strip()] = tuple(raw_values)
    return out


def _parse_cost_model(payload: Any) -> CostModel:
    if payload is None:
        return CostModel(fee_rate=0.0, slippage_bps=(0.0,))
    if not isinstance(payload, dict):
        raise ManifestValidationError("manifest field 'cost_model' must be an object")
    fee_rate = _finite_non_negative_float(payload.get("fee_rate"), "cost_model.fee_rate")
    slippage = payload.get("slippage_bps")
    if not isinstance(slippage, list) or not slippage:
        raise ManifestValidationError("cost_model.slippage_bps must be a non-empty array")
    return CostModel(
        fee_rate=fee_rate,
        slippage_bps=tuple(_finite_non_negative_float(value, "cost_model.slippage_bps") for value in slippage),
    )


def _parse_execution_model(value: Any, cost_model: CostModel) -> ExecutionModelConfig:
    if value is None:
        scenarios = tuple(
            ExecutionScenario(
                type="fixed_bps",
                fee_rate=cost_model.fee_rate,
                slippage_bps=float(slippage),
                source="legacy_cost_model",
                scenario_policy="legacy_cost_model_single_pass",
                scenario_role="base",
                scenario_role_source="legacy_cost_model",
                cost_assumption=ScenarioCostAssumption(
                    label="legacy_cost_model",
                    role="base",
                    fee_rate=cost_model.fee_rate,
                    fee_source="legacy_cost_model",
                    fee_authority_policy="unspecified_legacy",
                    slippage_bps=float(slippage),
                    slippage_source="legacy_cost_model",
                    promotable_as_base=False,
                    source="legacy_cost_model",
                ),
            )
            for slippage in cost_model.slippage_bps
        )
        return ExecutionModelConfig(
            scenarios=scenarios,
            source="legacy_cost_model",
            scenario_policy="legacy_cost_model_single_pass",
        )
    if not isinstance(value, dict):
        raise ManifestValidationError("execution_model must be an object")
    allowed_fields = {
        "type",
        "fee_rate",
        "slippage_bps",
        "latency_ms",
        "partial_fill_rate",
        "order_failure_rate",
        "market_order_extra_cost_bps",
        "scenario_policy",
        "scenario_role",
        "seed",
        "calibration_required",
        "calibration_strictness",
        "scenarios",
        "label",
        "fee_source",
        "fee_authority_policy",
        "slippage_source",
        "valid_for",
        "promotable_as_base",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"execution_model unsupported fields: {','.join(unknown)}")
    explicit_scenario_policy = value.get("scenario_policy")
    scenario_policy = str(explicit_scenario_policy or "").strip()
    if scenario_policy and scenario_policy not in {
        "single_scenario",
        "must_pass_base_and_survive_stress",
    }:
        raise ManifestValidationError(
            "execution_model.scenario_policy must be single_scenario or must_pass_base_and_survive_stress"
        )
    strictness = str(value.get("calibration_strictness") or "fail").strip().lower()
    if strictness not in {"fail", "warn"}:
        raise ManifestValidationError("execution_model.calibration_strictness must be fail or warn")
    explicit_scenarios = value.get("scenarios")
    if explicit_scenarios is not None:
        if not isinstance(explicit_scenarios, list) or not explicit_scenarios:
            raise ManifestValidationError("execution_model.scenarios must be a non-empty array")
        scenarios = [
            _parse_explicit_execution_scenario(
                raw,
                index=index,
                parent=value,
                scenario_policy=scenario_policy or "pending_default",
            )
            for index, raw in enumerate(explicit_scenarios)
        ]
    else:
        model_type = _required_str(value, "type")
        if model_type not in {"fixed_bps", "stress", "depth_walk"}:
            raise ManifestValidationError("execution_model.type must be fixed_bps, stress, or depth_walk")
        scenario_role = _optional_scenario_role(value.get("scenario_role"))
        scenario_role_source = "manifest" if scenario_role is not None else "derived"
        fees = _number_array(value, "fee_rate", default=(cost_model.fee_rate,))
        slippages = _number_array(value, "slippage_bps", default=cost_model.slippage_bps)
        latencies = _int_array(value, "latency_ms", default=(0,))
        partial_rates = _number_array(value, "partial_fill_rate", default=(0.0,))
        failure_rates = _number_array(value, "order_failure_rate", default=(0.0,))
        market_extra = _number_array(value, "market_order_extra_cost_bps", default=(0.0,))
        seed = value.get("seed")
        parsed_seed = None if seed is None else int(seed)
        scenarios = []
        for index, (fee, slippage, latency, partial, failure, extra) in enumerate(itertools.product(
            fees, slippages, latencies, partial_rates, failure_rates, market_extra
        )):
            active_role = scenario_role or _derived_scenario_role(index)
            scenarios.append(
                ExecutionScenario(
                    type=model_type,
                    fee_rate=float(fee),
                    slippage_bps=float(slippage),
                    latency_ms=int(latency),
                    partial_fill_rate=float(partial),
                    order_failure_rate=float(failure),
                    market_order_extra_cost_bps=float(extra),
                    seed=parsed_seed,
                    source="execution_model",
                    scenario_policy=scenario_policy or "pending_default",
                    scenario_role=active_role,
                    scenario_role_source=scenario_role_source,
                    cost_assumption=_scenario_cost_assumption(
                        label=str(value.get("label") or "").strip(),
                        role=active_role,
                        fee_rate=float(fee),
                        fee_source=str(value.get("fee_source") or "").strip(),
                        fee_authority_policy=str(value.get("fee_authority_policy") or "").strip(),
                        slippage_bps=float(slippage),
                        slippage_source=str(value.get("slippage_source") or "").strip(),
                        valid_for=value.get("valid_for"),
                        promotable_as_base=value.get("promotable_as_base"),
                        source="execution_model",
                    ),
                )
            )
    if not scenarios:
        raise ManifestValidationError("execution_model produced no scenarios")
    if not scenario_policy:
        scenario_policy = "single_scenario" if len(scenarios) == 1 else "must_pass_base_and_survive_stress"
        scenarios = [
            ExecutionScenario(
                type=scenario.type,
                fee_rate=scenario.fee_rate,
                slippage_bps=scenario.slippage_bps,
                latency_ms=scenario.latency_ms,
                partial_fill_rate=scenario.partial_fill_rate,
                order_failure_rate=scenario.order_failure_rate,
                market_order_extra_cost_bps=scenario.market_order_extra_cost_bps,
                seed=scenario.seed,
                source=scenario.source,
                scenario_policy=scenario_policy,
                scenario_role=scenario.scenario_role,
                scenario_role_source=scenario.scenario_role_source,
                cost_assumption=scenario.cost_assumption,
            )
            for scenario in scenarios
        ]
    _validate_scenario_policy_role_consistency(
        explicit_scenario_policy=explicit_scenario_policy,
        scenario_policy=scenario_policy,
        scenarios=tuple(scenarios),
    )
    return ExecutionModelConfig(
        scenarios=tuple(scenarios),
        source="execution_model",
        scenario_policy=scenario_policy,
        calibration_required=bool(value.get("calibration_required", False)),
        calibration_strictness=strictness,
    )


def _parse_explicit_execution_scenario(
    raw: Any,
    *,
    index: int,
    parent: dict[str, Any],
    scenario_policy: str,
) -> ExecutionScenario:
    if not isinstance(raw, dict):
        raise ManifestValidationError("execution_model.scenarios entries must be objects")
    allowed_fields = {
        "type",
        "scenario_role",
        "label",
        "fee_rate",
        "fee_source",
        "fee_authority_policy",
        "slippage_bps",
        "slippage_source",
        "valid_for",
        "promotable_as_base",
        "latency_ms",
        "partial_fill_rate",
        "order_failure_rate",
        "market_order_extra_cost_bps",
        "seed",
    }
    unknown = sorted(set(raw) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"execution_model.scenarios unsupported fields: {','.join(unknown)}")
    model_type = str(raw.get("type") or parent.get("type") or "fixed_bps").strip()
    if model_type not in {"fixed_bps", "stress", "depth_walk"}:
        raise ManifestValidationError("execution_model.scenarios.type must be fixed_bps, stress, or depth_walk")
    role = _optional_scenario_role(raw.get("scenario_role"))
    if role is None:
        raise ManifestValidationError("execution_model.scenarios.scenario_role must be base or stress")
    fee = _finite_non_negative_float(raw.get("fee_rate"), "execution_model.scenarios.fee_rate")
    slippage = _finite_non_negative_float(raw.get("slippage_bps"), "execution_model.scenarios.slippage_bps")
    return ExecutionScenario(
        type=model_type,
        fee_rate=fee,
        slippage_bps=slippage,
        latency_ms=_positive_or_zero_int(raw.get("latency_ms", parent.get("latency_ms", 0)), "execution_model.scenarios.latency_ms"),
        partial_fill_rate=_finite_non_negative_float(
            raw.get("partial_fill_rate", parent.get("partial_fill_rate", 0.0)),
            "execution_model.scenarios.partial_fill_rate",
        ),
        order_failure_rate=_finite_non_negative_float(
            raw.get("order_failure_rate", parent.get("order_failure_rate", 0.0)),
            "execution_model.scenarios.order_failure_rate",
        ),
        market_order_extra_cost_bps=_finite_non_negative_float(
            raw.get("market_order_extra_cost_bps", parent.get("market_order_extra_cost_bps", 0.0)),
            "execution_model.scenarios.market_order_extra_cost_bps",
        ),
        seed=None if raw.get("seed", parent.get("seed")) is None else int(raw.get("seed", parent.get("seed"))),
        source="execution_model",
        scenario_policy=scenario_policy,
        scenario_role=role,
        scenario_role_source="manifest",
        cost_assumption=_scenario_cost_assumption(
            label=str(raw.get("label") or "").strip(),
            role=role,
            fee_rate=fee,
            fee_source=str(raw.get("fee_source") or "").strip(),
            fee_authority_policy=str(raw.get("fee_authority_policy") or "").strip(),
            slippage_bps=slippage,
            slippage_source=str(raw.get("slippage_source") or "").strip(),
            valid_for=raw.get("valid_for"),
            promotable_as_base=raw.get("promotable_as_base"),
            source="execution_model",
        ),
    )


def _scenario_cost_assumption(
    *,
    label: str,
    role: str,
    fee_rate: float,
    fee_source: str,
    fee_authority_policy: str,
    slippage_bps: float,
    slippage_source: str,
    valid_for: Any,
    promotable_as_base: Any,
    source: str,
) -> ScenarioCostAssumption:
    valid_for_payload = None
    if valid_for is not None:
        if not isinstance(valid_for, dict):
            raise ManifestValidationError("cost assumption valid_for must be an object when supplied")
        valid_for_payload = dict(valid_for)
    return ScenarioCostAssumption(
        label=label,
        role=role,
        fee_rate=fee_rate,
        fee_source=fee_source,
        fee_authority_policy=fee_authority_policy or "runtime_fee_authority_or_config_fallback",
        slippage_bps=slippage_bps,
        slippage_source=slippage_source,
        valid_for=valid_for_payload,
        promotable_as_base=(
            bool(promotable_as_base)
            if promotable_as_base is not None
            else bool(role == "base" and label and fee_source and slippage_source and source != "legacy_cost_model")
        ),
        source=source,
    )


def production_cost_assumption_policy_reasons(
    *,
    deployment_tier: str,
    execution_model: ExecutionModelConfig,
) -> list[str]:
    if not is_production_bound_target(deployment_tier):
        return []
    reasons: list[str] = []
    if execution_model.source == "legacy_cost_model":
        reasons.append("production_legacy_cost_model_not_promotable")
    scenarios = list(execution_model.scenarios)
    base_assumptions = [
        scenario.cost_assumption
        for scenario in scenarios
        if scenario.scenario_role == "base" and scenario.cost_assumption is not None
    ]
    if not base_assumptions:
        reasons.append("production_base_cost_assumption_required")
    if scenarios and all(scenario.scenario_role == "stress" for scenario in scenarios):
        reasons.append("production_stress_only_cost_model_not_promotable")
    for assumption in base_assumptions:
        if not assumption.label:
            reasons.append("production_cost_assumption_label_required")
        if not assumption.fee_source or assumption.fee_source in {"legacy_cost_model", "stress_assumption"}:
            reasons.append("production_cost_assumption_source_required")
        if not assumption.slippage_source:
            reasons.append("production_cost_assumption_source_required")
        if assumption.role == "stress" or not assumption.promotable_as_base:
            reasons.append("production_stress_only_cost_model_not_promotable")
    return sorted(set(reasons))


def _parse_execution_timing(value: Any) -> ExecutionTimingPolicy:
    if value is None:
        return ExecutionTimingPolicy()
    if not isinstance(value, dict):
        raise ManifestValidationError("execution_timing must be an object")
    allowed_fields = {
        "signal_basis",
        "decision_time",
        "decision_guard_ms",
        "fill_reference_policy",
        "quote_selection",
        "max_quote_wait_ms",
        "missing_quote_policy",
        "allow_same_candle_close_fill",
        "min_execution_reality_level_for_promotion",
        "depth_required",
        "trade_tick_required",
        "queue_position_required",
        "market_impact_required",
        "intra_candle_path_required",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"execution_timing unsupported fields: {','.join(unknown)}")
    signal_basis = str(value.get("signal_basis") or "closed_candle").strip().lower()
    if signal_basis != "closed_candle":
        raise ManifestValidationError("execution_timing.signal_basis must be closed_candle")
    decision_time = str(value.get("decision_time") or "candle_close").strip().lower()
    if decision_time not in {"candle_close", "candle_close_plus_guard"}:
        raise ManifestValidationError("execution_timing.decision_time must be candle_close or candle_close_plus_guard")
    guard_ms = _positive_or_zero_int(value.get("decision_guard_ms", 0), "execution_timing.decision_guard_ms")
    if decision_time == "candle_close" and guard_ms:
        decision_time = "candle_close_plus_guard"
    fill_policy = str(value.get("fill_reference_policy") or "next_candle_open").strip().lower()
    if fill_policy not in {
        "candle_close_legacy",
        "next_candle_open",
        "first_orderbook_after_decision",
        "latency_adjusted_orderbook",
    }:
        raise ManifestValidationError("execution_timing.fill_reference_policy is unsupported")
    quote_selection = str(value.get("quote_selection") or "first_after_or_equal").strip().lower()
    if quote_selection != "first_after_or_equal":
        raise ManifestValidationError("execution_timing.quote_selection must be first_after_or_equal")
    max_wait = _positive_or_zero_int(value.get("max_quote_wait_ms", 3000), "execution_timing.max_quote_wait_ms")
    missing_quote_policy = str(value.get("missing_quote_policy") or "warn").strip().lower()
    if missing_quote_policy not in {"fail", "skip", "warn"}:
        raise ManifestValidationError("execution_timing.missing_quote_policy must be fail, skip, or warn")
    explicit_allow = value.get("allow_same_candle_close_fill")
    allow_same = bool(explicit_allow) if explicit_allow is not None else fill_policy == "candle_close_legacy"
    min_level_raw = value.get("min_execution_reality_level_for_promotion")
    min_level = None if min_level_raw is None else str(min_level_raw).strip()
    if min_level is not None and min_level not in {
        "candle_close_optimistic",
        "candle_next_open",
        "top_of_book_after_decision",
        "latency_adjusted_top_of_book",
        "l2_depth_walk_no_queue",
    }:
        raise ManifestValidationError("execution_timing.min_execution_reality_level_for_promotion is unsupported")
    depth_required = bool(value.get("depth_required", False))
    trade_tick_required = bool(value.get("trade_tick_required", False))
    queue_position_required = bool(value.get("queue_position_required", False))
    market_impact_required = bool(value.get("market_impact_required", False))
    intra_candle_path_required = bool(value.get("intra_candle_path_required", False))
    unsupported = unsupported_capability_reasons(
        {
            "depth_required": False,
            "trade_tick_required": trade_tick_required,
            "queue_position_required": queue_position_required,
            "market_impact_required": market_impact_required,
            "intra_candle_path_required": intra_candle_path_required,
            "depth_available": False,
            "trade_ticks_available": False,
            "queue_position_available": False,
            "market_impact_model_available": False,
            "intra_candle_path_available": False,
        }
    )
    if unsupported:
        raise ManifestValidationError(",".join(unsupported))
    return ExecutionTimingPolicy(
        signal_basis=signal_basis,
        decision_time=decision_time,
        decision_guard_ms=guard_ms,
        fill_reference_policy=fill_policy,
        quote_selection=quote_selection,
        max_quote_wait_ms=max_wait,
        missing_quote_policy=missing_quote_policy,
        allow_same_candle_close_fill=allow_same,
        min_execution_reality_level_for_promotion=min_level,
        depth_required=depth_required,
        trade_tick_required=trade_tick_required,
        queue_position_required=queue_position_required,
        market_impact_required=market_impact_required,
        intra_candle_path_required=intra_candle_path_required,
        source="manifest",
    )


def _validate_execution_reality_manifest_policy(
    *,
    deployment_tier: str,
    dataset: DatasetSpec,
    execution_timing: ExecutionTimingPolicy,
    execution_timing_declared: bool,
    execution_timing_declared_fields: set[str],
) -> None:
    evaluation = evaluate_execution_reality_policy(
        production_bound=is_production_bound_target(deployment_tier),
        execution_timing=execution_timing,
        execution_timing_declared=execution_timing_declared,
        execution_timing_declared_fields=execution_timing_declared_fields,
        dataset_top_of_book=dataset.top_of_book,
        context="manifest",
    )
    reasons = [str(reason) for reason in evaluation.get("reasons") or []]
    if reasons:
        raise ManifestValidationError(",".join(reasons))


def _validate_execution_model_capability_policy(
    *,
    execution_model: ExecutionModelConfig,
    execution_timing: ExecutionTimingPolicy,
) -> None:
    has_depth_walk = any(scenario.type == "depth_walk" for scenario in execution_model.scenarios)
    if execution_timing.depth_required and not has_depth_walk:
        raise ManifestValidationError("execution_depth_required_but_unavailable_without_depth_walk_scenario")
    if (
        execution_timing.min_execution_reality_level_for_promotion == "l2_depth_walk_no_queue"
        and not has_depth_walk
    ):
        raise ManifestValidationError("execution_l2_depth_walk_required_but_depth_walk_scenario_missing")


def _optional_scenario_role(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        raise ManifestValidationError("execution_model.scenario_role must be a scalar base or stress value")
    role = str(value).strip()
    if role not in {"base", "stress"}:
        raise ManifestValidationError("execution_model.scenario_role must be base or stress")
    return role


def _derived_scenario_role(index: int) -> str:
    return "base" if index == 0 else "stress"


def _validate_scenario_policy_role_consistency(
    *,
    explicit_scenario_policy: Any,
    scenario_policy: str,
    scenarios: tuple[ExecutionScenario, ...],
) -> None:
    if str(explicit_scenario_policy or "").strip() != "must_pass_base_and_survive_stress":
        return
    if scenario_policy != "must_pass_base_and_survive_stress" or len(scenarios) <= 1:
        return
    if not all(scenario.scenario_role_source == "manifest" for scenario in scenarios):
        return
    roles = {scenario.scenario_role for scenario in scenarios}
    if roles in ({"base"}, {"stress"}):
        raise ManifestValidationError(
            "execution_model.scenario_role conflicts with must_pass_base_and_survive_stress"
        )


def _number_array(payload: dict[str, Any], key: str, *, default: tuple[float, ...]) -> tuple[float, ...]:
    if key not in payload:
        return tuple(float(item) for item in default)
    value = payload.get(key)
    raw_values = value if isinstance(value, list) else [value]
    if not raw_values:
        raise ManifestValidationError(f"execution_model.{key} must not be empty")
    return tuple(_finite_non_negative_float(item, f"execution_model.{key}") for item in raw_values)


def _int_array(payload: dict[str, Any], key: str, *, default: tuple[int, ...]) -> tuple[int, ...]:
    if key not in payload:
        return tuple(int(item) for item in default)
    value = payload.get(key)
    raw_values = value if isinstance(value, list) else [value]
    if not raw_values:
        raise ManifestValidationError(f"execution_model.{key} must not be empty")
    return tuple(_positive_or_zero_int(item, f"execution_model.{key}") for item in raw_values)


def _parse_acceptance_gate(payload: dict[str, Any]) -> AcceptanceGate:
    allowed_fields = {
        "min_trade_count",
        "max_mdd_pct",
        "min_profit_factor",
        "oos_return_must_be_positive",
        "parameter_stability_required",
        "walk_forward_required",
        "final_holdout_required_for_promotion",
        "regime_acceptance_gate",
        "min_cagr_pct",
        "min_expectancy_per_trade_krw",
        "min_expectancy_per_trade_pct",
        "max_exposure_time_pct",
        "max_avg_holding_time_minutes",
        "max_fee_drag_ratio",
        "max_slippage_drag_ratio",
        "max_single_trade_dependency_score",
        "reject_open_position_at_end",
        "metrics_contract_required",
    }
    unknown = sorted(set(payload) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"acceptance_gate unsupported fields: {','.join(unknown)}")
    min_trade_count = _positive_int(payload.get("min_trade_count"), "acceptance_gate.min_trade_count")
    max_mdd_pct = _finite_non_negative_float(payload.get("max_mdd_pct"), "acceptance_gate.max_mdd_pct")
    min_profit_factor = _finite_non_negative_float(
        payload.get("min_profit_factor"), "acceptance_gate.min_profit_factor"
    )
    if min_profit_factor <= 0.0:
        raise ManifestValidationError("acceptance_gate.min_profit_factor must be > 0")
    return AcceptanceGate(
        min_trade_count=min_trade_count,
        max_mdd_pct=max_mdd_pct,
        min_profit_factor=min_profit_factor,
        oos_return_must_be_positive=bool(payload.get("oos_return_must_be_positive", True)),
        parameter_stability_required=bool(payload.get("parameter_stability_required", False)),
        walk_forward_required=bool(payload.get("walk_forward_required", False)),
        final_holdout_required_for_promotion=bool(payload.get("final_holdout_required_for_promotion", True)),
        min_cagr_pct=_optional_finite_float(payload.get("min_cagr_pct"), "acceptance_gate.min_cagr_pct"),
        min_expectancy_per_trade_krw=_optional_finite_float(
            payload.get("min_expectancy_per_trade_krw"),
            "acceptance_gate.min_expectancy_per_trade_krw",
        ),
        min_expectancy_per_trade_pct=_optional_finite_float(
            payload.get("min_expectancy_per_trade_pct"),
            "acceptance_gate.min_expectancy_per_trade_pct",
        ),
        max_exposure_time_pct=_optional_pct(payload.get("max_exposure_time_pct"), "acceptance_gate.max_exposure_time_pct"),
        max_avg_holding_time_minutes=_optional_finite_non_negative_float(
            payload.get("max_avg_holding_time_minutes"),
            "acceptance_gate.max_avg_holding_time_minutes",
        ),
        max_fee_drag_ratio=_optional_finite_non_negative_float(
            payload.get("max_fee_drag_ratio"),
            "acceptance_gate.max_fee_drag_ratio",
        ),
        max_slippage_drag_ratio=_optional_finite_non_negative_float(
            payload.get("max_slippage_drag_ratio"),
            "acceptance_gate.max_slippage_drag_ratio",
        ),
        max_single_trade_dependency_score=_optional_pct(
            payload.get("max_single_trade_dependency_score"),
            "acceptance_gate.max_single_trade_dependency_score",
        ),
        reject_open_position_at_end=bool(payload.get("reject_open_position_at_end", False)),
        metrics_contract_required=bool(payload.get("metrics_contract_required", False)),
        regime_acceptance_gate=_parse_regime_acceptance_gate(payload.get("regime_acceptance_gate")),
    )


def _parse_statistical_validation(
    value: Any,
    *,
    deployment_tier: str,
) -> StatisticalSelectionContract | None:
    production_bound = is_production_bound_target(deployment_tier)
    if value is None:
        if production_bound:
            raise ManifestValidationError("statistical_validation required for production-bound manifests")
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("statistical_validation must be an object")
    allowed_fields = {
        "required_for_promotion",
        "benchmark",
        "primary_metric",
        "selection_universe",
        "multiple_testing_scope",
        "bootstrap",
        "gates",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"statistical_validation unsupported fields: {','.join(unknown)}")
    required = bool(value.get("required_for_promotion", production_bound))
    if production_bound and not required:
        raise ManifestValidationError("statistical_validation.required_for_promotion must be true for production-bound manifests")
    benchmark = str(value.get("benchmark") or "").strip()
    if benchmark not in {"cash", "buy_and_hold", "configured"}:
        raise ManifestValidationError("statistical_validation.benchmark must be cash, buy_and_hold, or configured")
    primary_metric = str(value.get("primary_metric") or "").strip()
    if primary_metric not in {"net_excess_return", "return_pct", "sharpe_like"}:
        raise ManifestValidationError(
            "statistical_validation.primary_metric must be net_excess_return, return_pct, or sharpe_like"
        )
    if production_bound and primary_metric == "sharpe_like":
        raise ManifestValidationError(
            "statistical_validation.primary_metric sharpe_like is not allowed for production-bound manifests "
            "without period-return Sharpe evidence"
        )
    selection_universe = str(value.get("selection_universe") or "").strip()
    if selection_universe != "all_parameter_candidates_all_required_scenarios":
        raise ManifestValidationError(
            "statistical_validation.selection_universe must be all_parameter_candidates_all_required_scenarios"
        )
    multiple_testing_scope = str(value.get("multiple_testing_scope") or "").strip()
    if multiple_testing_scope not in {"experiment", "experiment_family"}:
        raise ManifestValidationError("statistical_validation.multiple_testing_scope must be experiment or experiment_family")
    bootstrap = _parse_statistical_bootstrap(value.get("bootstrap"))
    gates = _parse_statistical_gates(value.get("gates"))
    return StatisticalSelectionContract(
        required_for_promotion=required,
        benchmark=benchmark,
        primary_metric=primary_metric,
        selection_universe=selection_universe,
        multiple_testing_scope=multiple_testing_scope,
        bootstrap=bootstrap,
        gates=gates,
    )


def _parse_statistical_bootstrap(value: Any) -> StatisticalBootstrapConfig:
    if not isinstance(value, dict):
        raise ManifestValidationError("statistical_validation.bootstrap must be an object")
    allowed_fields = {"method", "n_bootstrap", "block_length_policy", "seed_policy"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"statistical_validation.bootstrap unsupported fields: {','.join(unknown)}")
    method = str(value.get("method") or "").strip()
    if method not in {"metric_centered_max_bootstrap", "white_reality_check_block_bootstrap"}:
        raise ManifestValidationError(
            "statistical_validation.bootstrap.method must be metric_centered_max_bootstrap or white_reality_check_block_bootstrap"
        )
    n_bootstrap = _positive_int(value.get("n_bootstrap"), "statistical_validation.bootstrap.n_bootstrap")
    block_length_policy = str(value.get("block_length_policy") or "").strip()
    if method == "metric_centered_max_bootstrap" and block_length_policy != "not_applicable_summary_metric":
        raise ManifestValidationError(
            "statistical_validation.bootstrap.block_length_policy must be not_applicable_summary_metric for metric_centered_max_bootstrap"
        )
    if method == "white_reality_check_block_bootstrap" and block_length_policy != "fixed":
        raise ManifestValidationError(
            "statistical_validation.bootstrap.block_length_policy must be fixed for white_reality_check_block_bootstrap"
        )
    seed_policy = str(value.get("seed_policy") or "").strip()
    if seed_policy != "derived_from_selection_universe_hash":
        raise ManifestValidationError(
            "statistical_validation.bootstrap.seed_policy must be derived_from_selection_universe_hash"
        )
    return StatisticalBootstrapConfig(
        method=method,
        n_bootstrap=n_bootstrap,
        block_length_policy=block_length_policy,
        seed_policy=seed_policy,
    )


def _parse_statistical_gates(value: Any) -> StatisticalValidationGates:
    if not isinstance(value, dict):
        raise ManifestValidationError("statistical_validation.gates must be an object")
    allowed_fields = {
        "max_reality_check_p_value",
        "max_spa_p_value",
        "min_deflated_sharpe_probability",
        "max_holdout_reuse_count",
        "max_attempt_index_without_new_hypothesis",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"statistical_validation.gates unsupported fields: {','.join(unknown)}")
    max_reality_check = _probability(
        value.get("max_reality_check_p_value"),
        "statistical_validation.gates.max_reality_check_p_value",
    )
    max_spa = _optional_probability(
        value.get("max_spa_p_value"),
        "statistical_validation.gates.max_spa_p_value",
    )
    min_deflated = _optional_probability(
        value.get("min_deflated_sharpe_probability"),
        "statistical_validation.gates.min_deflated_sharpe_probability",
    )
    return StatisticalValidationGates(
        max_reality_check_p_value=max_reality_check,
        max_spa_p_value=max_spa,
        min_deflated_sharpe_probability=min_deflated,
        max_holdout_reuse_count=_positive_or_zero_int(
            value.get("max_holdout_reuse_count", 0),
            "statistical_validation.gates.max_holdout_reuse_count",
        ),
        max_attempt_index_without_new_hypothesis=_positive_int(
            value.get("max_attempt_index_without_new_hypothesis", 1),
            "statistical_validation.gates.max_attempt_index_without_new_hypothesis",
        ),
    )


def _parse_stress_suite(value: Any, *, deployment_tier: str) -> StressSuiteContract | None:
    production_bound = is_production_bound_target(deployment_tier)
    if value is None:
        if production_bound:
            raise ManifestValidationError("stress_suite required for production-bound manifests")
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("stress_suite must be an object")
    allowed_fields = {
        "required_for_promotion",
        "trade_removal",
        "trade_order_monte_carlo",
        "period_ablation",
        "parameter_perturbation",
        "risk_adjusted_score",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"stress_suite unsupported fields: {','.join(unknown)}")
    required = bool(value.get("required_for_promotion", production_bound))
    if production_bound and not required:
        raise ManifestValidationError("stress_suite.required_for_promotion must be true for production-bound manifests")
    return StressSuiteContract(
        required_for_promotion=required,
        trade_removal=_parse_stress_trade_removal(value.get("trade_removal")),
        trade_order_monte_carlo=_parse_stress_trade_order_monte_carlo(value.get("trade_order_monte_carlo")),
        period_ablation=_parse_stress_period_ablation(value.get("period_ablation")),
        parameter_perturbation=_parse_stress_parameter_perturbation(value.get("parameter_perturbation")),
        risk_adjusted_score=_parse_stress_risk_adjusted_score(value.get("risk_adjusted_score")),
    )


def _parse_final_selection(value: Any, *, deployment_tier: str) -> FinalSelectionContract | None:
    production_bound = is_production_bound_target(deployment_tier)
    if value is None:
        if production_bound:
            raise ManifestValidationError("final_selection required for production-bound manifests")
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("final_selection must be an object")
    allowed_fields = {
        "schema_version",
        "required_for_promotion",
        "candidate_universe",
        "must_pass",
        "selection_exposure_policy",
        "method",
        "null_metric_policy",
        "ranking",
        "unsupported_metric_policy",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"final_selection unsupported fields: {','.join(unknown)}")
    schema_version = _positive_int(value.get("schema_version"), "final_selection.schema_version")
    if schema_version != 1:
        raise ManifestValidationError("final_selection.schema_version must be 1")
    required = bool(value.get("required_for_promotion", production_bound))
    if production_bound and not required:
        raise ManifestValidationError("final_selection.required_for_promotion must be true for production-bound manifests")
    candidate_universe = str(value.get("candidate_universe") or "").strip()
    if candidate_universe != "acceptance_gate_passed_required_scenarios":
        raise ManifestValidationError(
            "final_selection.candidate_universe must be acceptance_gate_passed_required_scenarios"
        )
    method = str(value.get("method") or "").strip()
    if method != "lexicographic":
        raise ManifestValidationError("final_selection.method must be lexicographic")
    null_metric_policy = str(value.get("null_metric_policy") or "").strip()
    if null_metric_policy != "fail_if_required_else_worst_rank":
        raise ManifestValidationError(
            "final_selection.null_metric_policy must be fail_if_required_else_worst_rank"
        )
    ranking_value = value.get("ranking")
    if not isinstance(ranking_value, list) or not ranking_value:
        raise ManifestValidationError("final_selection.ranking must be a non-empty array")
    rules = tuple(
        _parse_final_selection_metric_rule(item, index=index)
        for index, item in enumerate(ranking_value)
    )
    if rules[-1].metric != "parameter_candidate_id" or rules[-1].order != "asc":
        raise ManifestValidationError(
            "final_selection.ranking must end with parameter_candidate_id asc deterministic tie-breaker"
        )
    must_pass = _parse_final_selection_must_pass(value.get("must_pass"))
    exposure = _parse_final_selection_exposure_policy(value.get("selection_exposure_policy"), rules=rules)
    unsupported = _parse_final_selection_unsupported_metric_policy(value.get("unsupported_metric_policy"))
    return FinalSelectionContract(
        schema_version=schema_version,
        required_for_promotion=required,
        candidate_universe=candidate_universe,
        must_pass=must_pass,
        selection_exposure_policy=exposure,
        method=method,
        null_metric_policy=null_metric_policy,
        ranking=rules,
        unsupported_metric_policy=unsupported,
    )


def _parse_final_selection_must_pass(value: Any) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ManifestValidationError("final_selection.must_pass must be an object")
    allowed_fields = {
        "dataset_quality_gate_status",
        "statistical_gate_result",
        "stress_suite_gate_result",
        "production_calibration_policy_result",
        "metrics_schema_version",
        "final_holdout_present",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"final_selection.must_pass unsupported fields: {','.join(unknown)}")
    return dict(value)


def _parse_final_selection_exposure_policy(
    value: Any,
    *,
    rules: tuple[FinalSelectionMetricRule, ...],
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ManifestValidationError("final_selection.selection_exposure_policy must be an object")
    allowed_fields = {"final_holdout_usage", "counts_as_holdout_reuse"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(
            f"final_selection.selection_exposure_policy unsupported fields: {','.join(unknown)}"
        )
    final_holdout_usage = str(value.get("final_holdout_usage") or "").strip()
    if final_holdout_usage != "confirmatory_metric_in_rank":
        raise ManifestValidationError(
            "final_selection.selection_exposure_policy.final_holdout_usage must be confirmatory_metric_in_rank"
        )
    counts_as_holdout_reuse = value.get("counts_as_holdout_reuse")
    if not isinstance(counts_as_holdout_reuse, bool):
        raise ManifestValidationError(
            "final_selection.selection_exposure_policy.counts_as_holdout_reuse must be boolean"
        )
    has_final_holdout_rank_metric = any(rule.metric.startswith("final_holdout.") for rule in rules)
    if has_final_holdout_rank_metric and counts_as_holdout_reuse is not True:
        raise ManifestValidationError(
            "final_selection.selection_exposure_policy.counts_as_holdout_reuse must be true when final_holdout metrics are ranked"
        )
    if not has_final_holdout_rank_metric:
        raise ManifestValidationError(
            "final_selection.selection_exposure_policy.final_holdout_usage confirmatory_metric_in_rank requires a final_holdout ranking metric"
        )
    return {
        "final_holdout_usage": final_holdout_usage,
        "counts_as_holdout_reuse": counts_as_holdout_reuse,
    }


def _parse_final_selection_unsupported_metric_policy(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ManifestValidationError("final_selection.unsupported_metric_policy must be an object")
    allowed_fields = {"sharpe_ratio", "sortino_ratio"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(
            f"final_selection.unsupported_metric_policy unsupported fields: {','.join(unknown)}"
        )
    parsed: dict[str, str] = {}
    for key, item in value.items():
        policy = str(item or "").strip()
        if policy != "fail_if_required":
            raise ManifestValidationError(
                f"final_selection.unsupported_metric_policy.{key} must be fail_if_required"
            )
        parsed[str(key)] = policy
    return parsed


def _parse_final_selection_metric_rule(value: Any, *, index: int) -> FinalSelectionMetricRule:
    if not isinstance(value, dict):
        raise ManifestValidationError(f"final_selection.ranking[{index}] must be an object")
    allowed_fields = {"metric", "order", "required", "null_policy"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"final_selection.ranking[{index}] unsupported fields: {','.join(unknown)}")
    metric = str(value.get("metric") or "").strip()
    if not metric:
        raise ManifestValidationError(f"final_selection.ranking[{index}].metric is required")
    order = str(value.get("order") or "").strip()
    if order not in {"asc", "desc"}:
        raise ManifestValidationError(f"final_selection.ranking[{index}].order must be asc or desc")
    null_policy = str(value.get("null_policy") or "fail_if_required_else_worst_rank").strip()
    if null_policy != "fail_if_required_else_worst_rank":
        raise ManifestValidationError(
            f"final_selection.ranking[{index}].null_policy must be fail_if_required_else_worst_rank"
        )
    return FinalSelectionMetricRule(
        metric=metric,
        order=order,
        required=bool(value.get("required", True)),
        null_policy=null_policy,
    )


def _parse_stress_trade_removal(value: Any) -> StressTradeRemovalContract | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("stress_suite.trade_removal must be an object")
    allowed_fields = {"top_n_by_net_pnl", "min_return_retention_pct", "max_mdd_multiplier"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"stress_suite.trade_removal unsupported fields: {','.join(unknown)}")
    raw_top_n = value.get("top_n_by_net_pnl")
    if not isinstance(raw_top_n, list) or not raw_top_n:
        raise ManifestValidationError("stress_suite.trade_removal.top_n_by_net_pnl must be a non-empty array")
    top_n = tuple(_positive_int(item, "stress_suite.trade_removal.top_n_by_net_pnl") for item in raw_top_n)
    if len(set(top_n)) != len(top_n):
        raise ManifestValidationError("stress_suite.trade_removal.top_n_by_net_pnl must not contain duplicates")
    min_retention = _optional_pct(
        value.get("min_return_retention_pct"),
        "stress_suite.trade_removal.min_return_retention_pct",
    )
    max_mdd_multiplier = _optional_positive_float(
        value.get("max_mdd_multiplier"),
        "stress_suite.trade_removal.max_mdd_multiplier",
    )
    return StressTradeRemovalContract(
        top_n_by_net_pnl=tuple(sorted(top_n)),
        min_return_retention_pct=min_retention,
        max_mdd_multiplier=max_mdd_multiplier,
    )


def _parse_stress_trade_order_monte_carlo(value: Any) -> StressTradeOrderMonteCarloContract | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("stress_suite.trade_order_monte_carlo must be an object")
    allowed_fields = {
        "iterations",
        "seed_policy",
        "min_survival_probability",
        "ruin_max_drawdown_pct",
        "min_closed_trades",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"stress_suite.trade_order_monte_carlo unsupported fields: {','.join(unknown)}")
    seed_policy = str(value.get("seed_policy") or "").strip()
    if seed_policy != "derived_from_manifest_candidate_scenario_split_hash":
        raise ManifestValidationError(
            "stress_suite.trade_order_monte_carlo.seed_policy must be derived_from_manifest_candidate_scenario_split_hash"
        )
    return StressTradeOrderMonteCarloContract(
        iterations=_positive_int(value.get("iterations"), "stress_suite.trade_order_monte_carlo.iterations"),
        seed_policy=seed_policy,
        min_survival_probability=_probability(
            value.get("min_survival_probability"),
            "stress_suite.trade_order_monte_carlo.min_survival_probability",
        ),
        ruin_max_drawdown_pct=_finite_non_negative_float(
            value.get("ruin_max_drawdown_pct"),
            "stress_suite.trade_order_monte_carlo.ruin_max_drawdown_pct",
        ),
        min_closed_trades=_positive_int(
            value.get("min_closed_trades", 10),
            "stress_suite.trade_order_monte_carlo.min_closed_trades",
        ),
    )


def _parse_stress_risk_adjusted_score(value: Any) -> StressRiskAdjustedScoreContract | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("stress_suite.risk_adjusted_score must be an object")
    allowed_fields = {"required_metrics", "ranking"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"stress_suite.risk_adjusted_score unsupported fields: {','.join(unknown)}")
    required = _named_string_list(
        value.get("required_metrics"),
        "stress_suite.risk_adjusted_score.required_metrics",
    )
    ranking = _named_string_list(value.get("ranking"), "stress_suite.risk_adjusted_score.ranking")
    supported_metrics = {"calmar", "sharpe", "sortino"}
    unsupported = sorted(set(required) - supported_metrics)
    if unsupported:
        raise ManifestValidationError(
            f"stress_suite.risk_adjusted_score.required_metrics unsupported values: {','.join(unsupported)}"
        )
    supported_ranking = {"pass_gate", "max_calmar", "max_expectancy", "min_mdd"}
    ranking_unsupported = sorted(set(ranking) - supported_ranking)
    if ranking_unsupported:
        raise ManifestValidationError(
            f"stress_suite.risk_adjusted_score.ranking unsupported values: {','.join(ranking_unsupported)}"
        )
    return StressRiskAdjustedScoreContract(required_metrics=tuple(required), ranking=tuple(ranking))


def _parse_stress_period_ablation(value: Any) -> StressPeriodAblationContract | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("stress_suite.period_ablation must be an object")
    allowed_fields = {"calendar_years", "min_pass_ratio", "min_return_retention_pct"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"stress_suite.period_ablation unsupported fields: {','.join(unknown)}")
    years = value.get("calendar_years")
    if years != "auto" and not isinstance(years, list):
        raise ManifestValidationError("stress_suite.period_ablation.calendar_years must be auto or an array")
    parsed_years: tuple[int, ...] | str
    if years == "auto":
        parsed_years = "auto"
    else:
        if not years:
            raise ManifestValidationError("stress_suite.period_ablation.calendar_years must be auto or a non-empty array")
        parsed = tuple(_calendar_year(item, "stress_suite.period_ablation.calendar_years") for item in years)
        if len(set(parsed)) != len(parsed):
            raise ManifestValidationError("stress_suite.period_ablation.calendar_years must not contain duplicates")
        parsed_years = tuple(sorted(parsed))
    min_pass_ratio = 0.8
    if "min_pass_ratio" in value:
        min_pass_ratio = _probability(value.get("min_pass_ratio"), "stress_suite.period_ablation.min_pass_ratio")
    min_return_retention_pct = 50.0
    if "min_return_retention_pct" in value:
        parsed_retention = _optional_pct(
            value.get("min_return_retention_pct"),
            "stress_suite.period_ablation.min_return_retention_pct",
        )
        if parsed_retention is None:
            raise ManifestValidationError("stress_suite.period_ablation.min_return_retention_pct must be a number")
        min_return_retention_pct = parsed_retention
    return StressPeriodAblationContract(
        calendar_years=parsed_years,
        min_pass_ratio=min_pass_ratio,
        min_return_retention_pct=min_return_retention_pct,
    )


def _parse_stress_parameter_perturbation(value: Any) -> StressParameterPerturbationContract | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("stress_suite.parameter_perturbation must be an object")
    allowed_fields = {"relative_pct", "numeric_params_only", "min_pass_ratio"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"stress_suite.parameter_perturbation unsupported fields: {','.join(unknown)}")
    relative = value.get("relative_pct")
    if not isinstance(relative, list) or not relative:
        raise ManifestValidationError("stress_suite.parameter_perturbation.relative_pct must be a non-empty array")
    parsed_relative: list[float] = []
    for item in relative:
        parsed = _optional_finite_float(item, "stress_suite.parameter_perturbation.relative_pct")
        if parsed == 0.0:
            raise ManifestValidationError("stress_suite.parameter_perturbation.relative_pct values must be non-zero")
        parsed_relative.append(float(parsed))
    if len(set(parsed_relative)) != len(parsed_relative):
        raise ManifestValidationError("stress_suite.parameter_perturbation.relative_pct must not contain duplicates")
    if "numeric_params_only" in value and not isinstance(value.get("numeric_params_only"), bool):
        raise ManifestValidationError("stress_suite.parameter_perturbation.numeric_params_only must be boolean")
    min_pass_ratio = 0.8
    if "min_pass_ratio" in value:
        min_pass_ratio = _probability(value.get("min_pass_ratio"), "stress_suite.parameter_perturbation.min_pass_ratio")
    return StressParameterPerturbationContract(
        relative_pct=tuple(sorted(parsed_relative)),
        numeric_params_only=bool(value.get("numeric_params_only", True)),
        min_pass_ratio=min_pass_ratio,
    )


def _calendar_year(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ManifestValidationError(f"{field} values must be integer years")
    if value < 1970 or value > 9999:
        raise ManifestValidationError(f"{field} values must be valid calendar years")
    return int(value)


def _named_string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ManifestValidationError(f"{field} must be a non-empty array")
    out = [str(item).strip() for item in value]
    if any(not item for item in out):
        raise ManifestValidationError(f"{field} values must be non-empty strings")
    return out


def _parse_regime_acceptance_gate(value: Any) -> RegimeAcceptanceGate:
    if value is None:
        return RegimeAcceptanceGate(required=False)
    if not isinstance(value, dict):
        raise ManifestValidationError("acceptance_gate.regime_acceptance_gate must be an object")
    min_trade_count = int(value.get("min_trade_count_per_required_regime", 0) or 0)
    blocked_count = int(value.get("blocked_regime_max_trade_count", 0) or 0)
    if min_trade_count < 0:
        raise ManifestValidationError("acceptance_gate.regime_acceptance_gate.min_trade_count_per_required_regime must be >= 0")
    if blocked_count < 0:
        raise ManifestValidationError("acceptance_gate.regime_acceptance_gate.blocked_regime_max_trade_count must be >= 0")
    return RegimeAcceptanceGate(
        required=bool(value.get("required", False)),
        min_trade_count_per_required_regime=min_trade_count,
        required_regimes=tuple(_str_list(value.get("required_regimes"), "required_regimes")),
        blocked_regimes=tuple(_str_list(value.get("blocked_regimes"), "blocked_regimes")),
        blocked_regime_max_trade_count=blocked_count,
        blocked_regime_max_net_pnl_loss_krw=_finite_non_negative_float(
            value.get("blocked_regime_max_net_pnl_loss_krw", 0.0),
            "acceptance_gate.regime_acceptance_gate.blocked_regime_max_net_pnl_loss_krw",
        ),
        min_profit_factor_by_regime=_float_map(value.get("min_profit_factor_by_regime"), "min_profit_factor_by_regime"),
        min_expectancy_by_regime=_float_map(value.get("min_expectancy_by_regime"), "min_expectancy_by_regime"),
        max_loss_share_by_single_regime=(
            None
            if value.get("max_loss_share_by_single_regime") is None
            else _finite_non_negative_float(
                value.get("max_loss_share_by_single_regime"),
                "acceptance_gate.regime_acceptance_gate.max_loss_share_by_single_regime",
            )
        ),
        max_pnl_dependency_by_single_regime=(
            None
            if value.get("max_pnl_dependency_by_single_regime") is None
            else _finite_non_negative_float(
                value.get("max_pnl_dependency_by_single_regime"),
                "acceptance_gate.regime_acceptance_gate.max_pnl_dependency_by_single_regime",
            )
        ),
    )


def _str_list(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ManifestValidationError(f"acceptance_gate.regime_acceptance_gate.{field} must be an array")
    return [str(item).strip() for item in value if str(item).strip()]


def _float_map(value: Any, field: str) -> dict[str, float]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ManifestValidationError(f"acceptance_gate.regime_acceptance_gate.{field} must be an object")
    out: dict[str, float] = {}
    for key, raw in value.items():
        out[str(key)] = _finite_non_negative_float(raw, f"acceptance_gate.regime_acceptance_gate.{field}.{key}")
    return out


def _parse_walk_forward(value: Any) -> WalkForwardConfig | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ManifestValidationError("walk_forward must be an object")
    return WalkForwardConfig(
        train_window_days=_positive_int(value.get("train_window_days"), "walk_forward.train_window_days"),
        test_window_days=_positive_int(value.get("test_window_days"), "walk_forward.test_window_days"),
        step_days=_positive_int(value.get("step_days"), "walk_forward.step_days"),
        min_windows=_positive_int(value.get("min_windows"), "walk_forward.min_windows"),
    )


def _parse_research_run(value: Any) -> ResearchRunPolicy:
    if value is None:
        return ResearchRunPolicy()
    if not isinstance(value, dict):
        raise ManifestValidationError("research_run must be an object")
    allowed_fields = {"report_detail", "artifact_policy", "audit_trail", "resource_limits", "heartbeat"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"research_run unsupported fields: {','.join(unknown)}")
    report_detail = str(value.get("report_detail") or "summary").strip().lower()
    if report_detail not in {"summary", "standard", "full"}:
        raise ManifestValidationError("research_run.report_detail must be summary, standard, or full")
    artifact_policy = _parse_research_artifact_policy(value.get("artifact_policy"))
    audit_trail = _parse_research_audit_trail(value.get("audit_trail"))
    if artifact_policy.full_decisions_external_jsonl and value.get("audit_trail") is None:
        audit_trail = ResearchAuditTrailPolicy(
            mode="complete_external",
            decisions_required=True,
            equity_required=True,
            executions_required=True,
            hash_chain_required=True,
            required_for_promotion=True,
        )
    return ResearchRunPolicy(
        report_detail=report_detail,
        artifact_policy=artifact_policy,
        audit_trail=audit_trail,
        resource_limits=_parse_research_resource_limits(value.get("resource_limits")),
        heartbeat=_parse_research_heartbeat(value.get("heartbeat")),
    )


def _parse_research_artifact_policy(value: Any) -> ResearchArtifactPolicy:
    if value is None:
        return ResearchArtifactPolicy()
    if not isinstance(value, dict):
        raise ManifestValidationError("research_run.artifact_policy must be an object")
    allowed_fields = {"candidate_journal", "failed_candidate_evidence", "full_decisions_external_jsonl"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"research_run.artifact_policy unsupported fields: {','.join(unknown)}")
    return ResearchArtifactPolicy(
        candidate_journal=bool(value.get("candidate_journal", True)),
        failed_candidate_evidence=bool(value.get("failed_candidate_evidence", True)),
        full_decisions_external_jsonl=bool(value.get("full_decisions_external_jsonl", False)),
    )


def _parse_research_audit_trail(value: Any) -> ResearchAuditTrailPolicy:
    if value is None:
        return ResearchAuditTrailPolicy()
    if not isinstance(value, dict):
        raise ManifestValidationError("research_run.audit_trail must be an object")
    allowed_fields = {
        "mode",
        "decisions_required",
        "equity_required",
        "executions_required",
        "hash_chain_required",
        "required_for_promotion",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"research_run.audit_trail unsupported fields: {','.join(unknown)}")
    mode = str(value.get("mode") or "summary_only").strip().lower()
    if mode not in {"summary_only", "complete_external"}:
        raise ManifestValidationError("research_run.audit_trail.mode must be summary_only or complete_external")
    complete = mode == "complete_external"
    return ResearchAuditTrailPolicy(
        mode=mode,
        decisions_required=bool(value.get("decisions_required", complete)),
        equity_required=bool(value.get("equity_required", complete)),
        executions_required=bool(value.get("executions_required", complete)),
        hash_chain_required=bool(value.get("hash_chain_required", True)),
        required_for_promotion=bool(value.get("required_for_promotion", True)),
    )


def _parse_research_resource_limits(value: Any) -> ResearchResourceLimits:
    if value is None:
        return ResearchResourceLimits()
    if not isinstance(value, dict):
        raise ManifestValidationError("research_run.resource_limits must be an object")
    allowed_fields = {
        "max_runtime_s_per_candidate_split",
        "max_decisions_retained",
        "max_trades",
        "max_equity_points_retained",
        "max_rss_mb",
    }
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"research_run.resource_limits unsupported fields: {','.join(unknown)}")
    return ResearchResourceLimits(
        max_runtime_s_per_candidate_split=_optional_positive_float(
            value.get("max_runtime_s_per_candidate_split", 300.0),
            "research_run.resource_limits.max_runtime_s_per_candidate_split",
        ),
        max_decisions_retained=_optional_positive_or_zero_int(
            value.get("max_decisions_retained", 0),
            "research_run.resource_limits.max_decisions_retained",
        ),
        max_trades=_optional_positive_or_zero_int(value.get("max_trades", 5000), "research_run.resource_limits.max_trades"),
        max_equity_points_retained=_optional_positive_or_zero_int(
            value.get("max_equity_points_retained", 0),
            "research_run.resource_limits.max_equity_points_retained",
        ),
        max_rss_mb=_optional_positive_float(value.get("max_rss_mb", 1400.0), "research_run.resource_limits.max_rss_mb"),
    )


def _parse_research_heartbeat(value: Any) -> ResearchHeartbeatPolicy:
    if value is None:
        return ResearchHeartbeatPolicy()
    if not isinstance(value, dict):
        raise ManifestValidationError("research_run.heartbeat must be an object")
    allowed_fields = {"interval_s", "bar_interval"}
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ManifestValidationError(f"research_run.heartbeat unsupported fields: {','.join(unknown)}")
    return ResearchHeartbeatPolicy(
        interval_s=_optional_positive_float(value.get("interval_s", 10.0), "research_run.heartbeat.interval_s"),
        bar_interval=_optional_positive_or_zero_int(value.get("bar_interval", 10000), "research_run.heartbeat.bar_interval"),
    )


def _validate_split_order(split: DatasetSplit) -> None:
    if split.train.end_ts_ms() >= split.validation.start_ts_ms():
        raise ManifestValidationError("dataset.train must end before dataset.validation starts")
    if split.final_holdout is not None and split.validation.end_ts_ms() >= split.final_holdout.start_ts_ms():
        raise ManifestValidationError("dataset.validation must end before dataset.final_holdout starts")


def _date_start_ts_ms(value: str) -> int:
    return int(_parse_date(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _date_end_ts_ms(value: str) -> int:
    return _date_start_ts_ms(value) + 86_400_000 - 1


def _parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ManifestValidationError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


def _finite_non_negative_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be a number") from exc
    if parsed < 0.0 or parsed != parsed or parsed in {float("inf"), float("-inf")}:
        raise ManifestValidationError(f"{field} must be a finite value >= 0")
    return parsed


def _optional_finite_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be a number") from exc
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        raise ManifestValidationError(f"{field} must be finite")
    return parsed


def _optional_finite_non_negative_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    parsed = _optional_finite_float(value, field)
    assert parsed is not None
    if parsed < 0.0:
        raise ManifestValidationError(f"{field} must be >= 0")
    return parsed


def _optional_positive_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    parsed = _finite_non_negative_float(value, field)
    if parsed <= 0.0:
        return None
    return float(parsed)


def _optional_pct(value: Any, field: str) -> float | None:
    parsed = _optional_finite_non_negative_float(value, field)
    if parsed is not None and parsed > 100.0:
        raise ManifestValidationError(f"{field} must be <= 100")
    return parsed


def _probability(value: Any, field: str) -> float:
    parsed = _finite_non_negative_float(value, field)
    if parsed > 1.0:
        raise ManifestValidationError(f"{field} must be <= 1")
    return parsed


def _optional_probability(value: Any, field: str) -> float | None:
    parsed = _optional_finite_non_negative_float(value, field)
    if parsed is not None and parsed > 1.0:
        raise ManifestValidationError(f"{field} must be <= 1")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be an integer") from exc
    if parsed <= 0:
        raise ManifestValidationError(f"{field} must be > 0")
    return parsed


def _positive_or_zero_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ManifestValidationError(f"{field} must be an integer") from exc
    if parsed < 0:
        raise ManifestValidationError(f"{field} must be >= 0")
    return parsed


def _optional_positive_or_zero_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    return _positive_or_zero_int(value, field)
