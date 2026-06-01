from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic
from bithumb_bot.evidence_safety import smoke_only_evidence_rejection_reasons
from bithumb_bot.execution_reality_contract import (
    evaluate_execution_reality_policy,
    capability_contract_hash_matches,
    validate_execution_capability_contract,
)

from .hashing import content_hash_payload, report_content_hash_payload, sha256_prefixed
from .audit_trail import validate_audit_trail_binding
from .lineage import build_promotion_lineage, validate_lineage_artifact, LineageValidationError
from .experiment_registry import append_promotion_registry_event, validate_experiment_registry_binding
from .final_selection import validate_final_selection_report
from .deployment_policy import is_production_bound_target, validate_production_calibration_policy
from .strategy_spec import (
    exit_policy_from_parameters,
    exit_policy_hash,
    materialize_strategy_parameters,
    materialized_strategy_parameters_hash,
    strategy_parameter_source_map,
)
from .metrics_contract import METRICS_SCHEMA_VERSION
from .metrics_gate_policy import metrics_gate_policy_hash
from .statistical_selection import validate_statistical_evidence_for_candidate
from .stress_suite import stress_suite_required_for_candidate, validate_stress_suite_evidence_for_candidate


class PromotionGateError(ValueError):
    pass


@dataclass(frozen=True)
class PromotionResult:
    artifact: dict[str, Any]
    artifact_path: Path
    content_hash: str


@dataclass(frozen=True)
class ValidatedCandidate:
    candidate: dict[str, Any]
    profile: dict[str, Any]
    profile_hash: str
    source_report_hash: str | None = None


_CANDIDATE_BEHAVIOR_RUNTIME_KEYS = {
    "audit_trace_index",
    "artifact_namespace",
    "artifact_locator",
    "artifact_path",
    "artifact_ref",
    "attempt_id",
    "completion_order",
    "experiment_id",
    "failure_artifact_path",
    "failure_artifact_ref",
    "report_path",
    "provenance_identity",
    "run_uuid",
    "runtime_observability",
    "trace_manifest_path",
    "wall_seconds",
    "cpu_seconds",
    "candles_per_second",
    "rss_mb",
    "current_rss_mb",
    "peak_rss_mb",
    "baseline_rss_mb",
    "rss_delta_mb",
    "memory_sample_source",
    "worker_hostname",
    "worker_pid",
}
_CANDIDATE_MEMORY_OBSERVABILITY_KEYS = {
    "rss_mb",
    "current_rss_mb",
    "peak_rss_mb",
    "baseline_rss_mb",
    "rss_delta_mb",
    "memory_sample_source",
}


def build_candidate_profile(candidate: dict[str, Any]) -> dict[str, Any]:
    warning_reasons = _execution_calibration_warning_reasons(candidate)
    strategy_name = str(candidate.get("strategy_name") or "sma_with_filter")
    parameters = candidate.get("parameter_values") if isinstance(candidate.get("parameter_values"), dict) else {}
    raw_parameters = (
        candidate.get("parameter_values_raw")
        if isinstance(candidate.get("parameter_values_raw"), dict)
        else dict(parameters)
    )
    cost_model = candidate.get("cost_model") if isinstance(candidate.get("cost_model"), dict) else {}
    effective_parameters = (
        candidate.get("effective_strategy_parameters")
        if isinstance(candidate.get("effective_strategy_parameters"), dict)
        else materialize_strategy_parameters(
            strategy_name,
            raw_parameters,
            fee_rate=cost_model.get("fee_rate"),
            slippage_bps=cost_model.get("slippage_bps"),
        )
    )
    effective_parameters_hash = str(
        candidate.get("effective_strategy_parameters_hash")
        or materialized_strategy_parameters_hash(effective_parameters)
    )
    source_map = (
        candidate.get("strategy_parameter_source_map")
        if isinstance(candidate.get("strategy_parameter_source_map"), dict)
        else strategy_parameter_source_map(
            strategy_name,
            raw_parameters,
            fee_rate=cost_model.get("fee_rate"),
            slippage_bps=cost_model.get("slippage_bps"),
        )
    )
    exit_policy = candidate.get("exit_policy")
    if not isinstance(exit_policy, dict):
        exit_policy = exit_policy_from_parameters(strategy_name, effective_parameters)
    elif not _exit_policy_has_current_stop_loss_schema(exit_policy):
        exit_policy = exit_policy_from_parameters(strategy_name, effective_parameters)
    resolved_exit_policy_hash = (
        str(candidate.get("exit_policy_hash"))
        if isinstance(candidate.get("exit_policy"), dict)
        and _exit_policy_has_current_stop_loss_schema(candidate.get("exit_policy"))
        and candidate.get("exit_policy_hash")
        else exit_policy_hash(exit_policy)
    )
    profile = {
        "strategy_name": candidate.get("strategy_name"),
        "strategy_spec": candidate.get("strategy_spec"),
        "strategy_spec_hash": candidate.get("strategy_spec_hash"),
        "strategy_plugin_contract": candidate.get("strategy_plugin_contract"),
        "strategy_plugin_contract_hash": candidate.get("strategy_plugin_contract_hash"),
        "exit_policy": exit_policy,
        "exit_policy_hash": resolved_exit_policy_hash,
        "behavior_hash": candidate.get("behavior_hash"),
        "decision_behavior_hash": candidate.get("decision_behavior_hash"),
        "trade_ledger_hash": candidate.get("trade_ledger_hash"),
        "equity_curve_hash": candidate.get("equity_curve_hash"),
        "composite_behavior_hash": candidate.get("composite_behavior_hash"),
        "train_composite_behavior_hash": candidate.get("train_composite_behavior_hash"),
        "validation_composite_behavior_hash": candidate.get("validation_composite_behavior_hash"),
        "final_holdout_composite_behavior_hash": candidate.get("final_holdout_composite_behavior_hash"),
        "validation_behavior_hash": candidate.get("validation_behavior_hash"),
        "candidate_id": candidate.get("parameter_candidate_id"),
        "parameter_values": dict(effective_parameters),
        "parameter_values_raw": dict(raw_parameters),
        "effective_strategy_parameters": dict(effective_parameters),
        "effective_strategy_parameters_hash": effective_parameters_hash,
        "strategy_parameter_source_map": dict(source_map),
        "candidate_regime_policy_applied_in_research": bool(
            candidate.get("candidate_regime_policy_applied_in_research")
        ),
        "candidate_regime_policy_required_for_live": bool(
            candidate.get("candidate_regime_policy_required_for_live")
        ),
        "candidate_regime_policy_equivalence_required": bool(
            candidate.get("candidate_regime_policy_equivalence_required")
        ),
        "candidate_regime_policy_equivalence_evidence_hash": candidate.get(
            "candidate_regime_policy_equivalence_evidence_hash"
        ),
        "candidate_regime_policy_equivalence_evidence_path": candidate.get(
            "candidate_regime_policy_equivalence_evidence_path"
        ),
        "candidate_regime_policy_equivalence_evidence_status": candidate.get(
            "candidate_regime_policy_equivalence_evidence_status"
        ),
        "decision_equivalence_report_path": candidate.get("decision_equivalence_report_path"),
        "decision_equivalence_content_hash": candidate.get("decision_equivalence_content_hash"),
        "decision_equivalence_status": candidate.get("decision_equivalence_status"),
        "candidate_profile_evidence_contract_hash": candidate.get(
            "candidate_profile_evidence_contract_hash"
        ),
        "candidate_regime_policy_limitation_reasons": list(
            candidate.get("candidate_regime_policy_limitation_reasons") or []
        ),
        "cost_model": candidate.get("cost_model"),
        "base_cost_assumption": candidate.get("base_cost_assumption"),
        "cost_assumption_contract": candidate.get("cost_assumption_contract"),
        "portfolio_policy": candidate.get("portfolio_policy"),
        "portfolio_policy_hash": candidate.get("portfolio_policy_hash"),
        "simulation_policy_hash": candidate.get("simulation_policy_hash"),
        "source_experiment": candidate.get("experiment_id"),
        "manifest_hash": candidate.get("manifest_hash"),
        "experiment_family_id": candidate.get("experiment_family_id"),
        "hypothesis_id": candidate.get("hypothesis_id"),
        "hypothesis_status": candidate.get("hypothesis_status"),
        "hypothesis_identity_source": candidate.get("hypothesis_identity_source"),
        "experiment_family_identity_source": candidate.get("experiment_family_identity_source"),
        "dataset_snapshot_id": candidate.get("dataset_snapshot_id"),
        "dataset_content_hash": candidate.get("dataset_content_hash"),
        "dataset_quality_hash": candidate.get("dataset_quality_hash"),
        "dataset_quality_gate_status": candidate.get("dataset_quality_gate_status"),
        "dataset_quality_gate_reasons": candidate.get("dataset_quality_gate_reasons"),
        "dataset_quality_report_hashes": candidate.get("dataset_quality_report_hashes"),
        "top_of_book_quality_summary": candidate.get("top_of_book_quality_summary"),
        "execution_timing_policy": candidate.get("execution_timing_policy"),
        "execution_reality_contract": candidate.get("execution_reality_contract"),
        "execution_contract_hash": candidate.get("execution_contract_hash"),
        "execution_capability_contract": _candidate_capability_contract(candidate),
        "execution_capability_contract_hash": _candidate_capability_hash(candidate),
        "evidence_tier": (_candidate_capability_contract(candidate) or {}).get("evidence_tier") or candidate.get("evidence_tier"),
        "unavailable_required_capabilities": (
            (_candidate_capability_contract(candidate) or {}).get("unavailable_required_capabilities")
            or candidate.get("unavailable_required_capabilities")
        ),
        "execution_reality_summary": candidate.get("execution_reality_summary"),
        "execution_event_summary": candidate.get("execution_event_summary"),
        "train_execution_event_summary": candidate.get("train_execution_event_summary"),
        "validation_execution_event_summary": candidate.get("validation_execution_event_summary"),
        "final_holdout_execution_event_summary": candidate.get("final_holdout_execution_event_summary"),
        "strategy_diagnostics": candidate.get("strategy_diagnostics"),
        "train_strategy_diagnostics": candidate.get("train_strategy_diagnostics"),
        "validation_strategy_diagnostics": candidate.get("validation_strategy_diagnostics"),
        "final_holdout_strategy_diagnostics": candidate.get("final_holdout_strategy_diagnostics"),
        "regime_classifier_version": candidate.get("regime_classifier_version"),
        "allowed_live_regimes": candidate.get("allowed_live_regimes"),
        "blocked_live_regimes": candidate.get("blocked_live_regimes"),
        "acceptance_gate_result": candidate.get("acceptance_gate_result"),
        "scenario_policy": candidate.get("scenario_policy"),
        "deployment_tier": candidate.get("deployment_tier") or "research_only",
        "scenario_results": candidate.get("scenario_results"),
        "scenario_pass_count": candidate.get("scenario_pass_count"),
        "scenario_fail_count": candidate.get("scenario_fail_count"),
        "required_scenario_count": candidate.get("required_scenario_count"),
        "has_execution_calibration_warning": bool(warning_reasons),
        "execution_calibration_warning_reasons": warning_reasons,
        "final_holdout_present": candidate.get("final_holdout_present"),
        "final_holdout_required_for_promotion": candidate.get("final_holdout_required_for_promotion"),
        "final_holdout_metrics": candidate.get("final_holdout_metrics"),
        "validation_metrics": candidate.get("validation_metrics"),
        "metrics_schema_version": candidate.get("metrics_schema_version"),
        "metrics_gate_policy": candidate.get("metrics_gate_policy"),
        "metrics_gate_policy_hash": candidate.get("metrics_gate_policy_hash"),
        "metrics_contract_required": bool(candidate.get("metrics_contract_required")),
        "stress_suite_required": stress_suite_required_for_candidate(candidate),
        "stress_suite_contract": candidate.get("stress_suite_contract"),
        "stress_suite_contract_hash": candidate.get("stress_suite_contract_hash"),
        "validation_stress_suite": candidate.get("validation_stress_suite"),
        "final_holdout_stress_suite": candidate.get("final_holdout_stress_suite"),
        "stress_suite_gate_result": candidate.get("stress_suite_gate_result"),
        "stress_suite_fail_reasons": candidate.get("stress_suite_fail_reasons"),
        "validation_metrics_v2": candidate.get("validation_metrics_v2"),
        "final_holdout_metrics_v2": candidate.get("final_holdout_metrics_v2"),
        "walk_forward_metrics": candidate.get("walk_forward_metrics"),
        "statistical_validation_required": bool(candidate.get("statistical_validation_required")),
        "statistical_validation_contract": candidate.get("statistical_validation_contract"),
        "evidence_grade": candidate.get("evidence_grade"),
        "statistical_method": candidate.get("statistical_method"),
        "return_panel_hash": candidate.get("return_panel_hash"),
        "return_panel_path": candidate.get("return_panel_path"),
        "return_unit": candidate.get("return_unit"),
        "return_panel_observation_count": candidate.get("return_panel_observation_count"),
        "family_trial_registry_path": candidate.get("family_trial_registry_path"),
        "family_trial_registry_prior_hash": candidate.get("family_trial_registry_prior_hash"),
        "family_trial_registry_row_hash": candidate.get("family_trial_registry_row_hash"),
        "experiment_registry_path": candidate.get("experiment_registry_path"),
        "experiment_registry_prior_hash": candidate.get("experiment_registry_prior_hash"),
        "experiment_registry_row_hash": candidate.get("experiment_registry_row_hash"),
        "experiment_registry_completion_row_hash": candidate.get("experiment_registry_completion_row_hash"),
        "experiment_registry_bound_evidence_hash": candidate.get("experiment_registry_bound_evidence_hash"),
        "experiment_registry_evidence_hash_phase": candidate.get("experiment_registry_evidence_hash_phase"),
        "final_holdout_fingerprint": candidate.get("final_holdout_fingerprint"),
        "final_holdout_identity_hash": candidate.get("final_holdout_identity_hash"),
        "final_holdout_content_hash": candidate.get("final_holdout_content_hash"),
        "final_holdout_reuse_key_hash": candidate.get("final_holdout_reuse_key_hash"),
        "final_holdout_split_hash": candidate.get("final_holdout_split_hash"),
        "computed_attempt_index": candidate.get("computed_attempt_index"),
        "computed_holdout_reuse_count": candidate.get("computed_holdout_reuse_count"),
        "declared_attempt_index": candidate.get("declared_attempt_index"),
        "declared_holdout_reuse_count": candidate.get("declared_holdout_reuse_count"),
        "research_freedom_hash": candidate.get("research_freedom_hash"),
        "registry_gate_result": candidate.get("registry_gate_result"),
        "registry_gate_fail_reasons": candidate.get("registry_gate_fail_reasons"),
        "benchmark": candidate.get("benchmark"),
        "primary_metric": candidate.get("primary_metric"),
        "primary_metric_source": candidate.get("primary_metric_source"),
        "selection_universe_hash": candidate.get("selection_universe_hash"),
        "candidate_metric_values_hash": candidate.get("candidate_metric_values_hash"),
        "candidate_metric_values_summary": candidate.get("candidate_metric_values_summary"),
        "metric_value_count": candidate.get("metric_value_count"),
        "missing_metric_count": candidate.get("missing_metric_count"),
        "statistical_evidence_hash": candidate.get("statistical_evidence_hash"),
        "statistical_gate_result": candidate.get("statistical_gate_result"),
        "statistical_gate_fail_reasons": candidate.get("statistical_gate_fail_reasons"),
        "white_reality_check_p_value": candidate.get("white_reality_check_p_value"),
        "summary_metric_max_bootstrap_p_value": candidate.get("summary_metric_max_bootstrap_p_value"),
        "white_reality_check_method": candidate.get("white_reality_check_method"),
        "bootstrap_sampling_contract_hash": candidate.get("bootstrap_sampling_contract_hash"),
        "promotion_grade_limitations": candidate.get("promotion_grade_limitations"),
        "official_promotion_grade_wrc_generation_available": candidate.get(
            "official_promotion_grade_wrc_generation_available"
        ),
        "effective_trial_count": candidate.get("effective_trial_count"),
    }
    if candidate.get("execution_model") is not None:
        profile["execution_model"] = candidate.get("execution_model")
    if candidate.get("execution_calibration_required") is not None:
        profile["execution_calibration_required"] = candidate.get("execution_calibration_required")
    if candidate.get("execution_calibration_strictness") is not None:
        profile["execution_calibration_strictness"] = candidate.get("execution_calibration_strictness")
    if candidate.get("execution_calibration_gate") is not None:
        profile["execution_calibration_gate"] = candidate.get("execution_calibration_gate")
    for key in (
        "execution_calibration_artifact_hash",
        "execution_calibration_artifact_hashes",
        "execution_calibration_policy_source",
        "production_calibration_policy_result",
        "production_calibration_policy_reasons",
    ):
        if candidate.get(key) is not None:
            profile[key] = candidate.get(key)
    return _strip_candidate_memory_observability_fields(profile)


def _exit_policy_has_current_stop_loss_schema(exit_policy: object) -> bool:
    if not isinstance(exit_policy, dict):
        return False
    stop_loss = exit_policy.get("stop_loss")
    if not isinstance(stop_loss, dict):
        return False
    return (
        stop_loss.get("evaluation_price_basis") == "closed_candle_mark"
        and stop_loss.get("intrabar_stop_modeled") is False
        and isinstance(stop_loss.get("limitation_reasons"), list)
        and "intra_candle_path_unavailable" in stop_loss.get("limitation_reasons", [])
        and "candle_close_stop_may_exit_later_than_real_stop" in stop_loss.get("limitation_reasons", [])
    )


def build_candidate_behavior_profile(candidate: dict[str, Any]) -> dict[str, Any]:
    """Behavior-only candidate identity, separate from evaluation/promotion provenance."""
    profile = build_candidate_profile(candidate)
    for key in (
        "source_experiment",
        "manifest_hash",
        "experiment_family_id",
        "hypothesis_id",
        "hypothesis_status",
        "hypothesis_identity_source",
        "experiment_family_identity_source",
        "deployment_tier",
        "candidate_regime_policy_required_for_live",
        "candidate_regime_policy_equivalence_required",
        "candidate_regime_policy_equivalence_evidence_hash",
        "candidate_regime_policy_equivalence_evidence_path",
        "candidate_regime_policy_equivalence_evidence_status",
        "decision_equivalence_report_path",
        "decision_equivalence_content_hash",
        "decision_equivalence_status",
        "candidate_profile_evidence_contract_hash",
        "candidate_regime_policy_limitation_reasons",
        "acceptance_gate_result",
        "final_holdout_required_for_promotion",
        "metrics_gate_policy",
        "metrics_gate_policy_hash",
        "metrics_contract_required",
        "has_execution_calibration_warning",
        "execution_calibration_warning_reasons",
        "execution_calibration_required",
        "execution_calibration_strictness",
        "execution_calibration_gate",
        "execution_calibration_artifact_hash",
        "execution_calibration_artifact_hashes",
        "execution_calibration_policy_source",
        "production_calibration_policy_result",
        "production_calibration_policy_reasons",
        "stress_suite_required",
        "stress_suite_contract",
        "stress_suite_contract_hash",
        "validation_stress_suite",
        "final_holdout_stress_suite",
        "stress_suite_gate_result",
        "stress_suite_fail_reasons",
        "statistical_validation_required",
        "statistical_validation_contract",
        "evidence_grade",
        "statistical_method",
        "return_panel_path",
        "family_trial_registry_path",
        "family_trial_registry_prior_hash",
        "family_trial_registry_row_hash",
        "experiment_registry_path",
        "experiment_registry_prior_hash",
        "experiment_registry_row_hash",
        "experiment_registry_completion_row_hash",
        "experiment_registry_bound_evidence_hash",
        "experiment_registry_evidence_hash_phase",
        "computed_attempt_index",
        "computed_holdout_reuse_count",
        "declared_attempt_index",
        "declared_holdout_reuse_count",
        "research_freedom_hash",
        "registry_gate_result",
        "registry_gate_fail_reasons",
        "final_holdout_fingerprint",
        "final_holdout_identity_hash",
        "final_holdout_content_hash",
        "final_holdout_reuse_key_hash",
        "final_holdout_split_hash",
        "benchmark",
        "primary_metric",
        "primary_metric_source",
        "selection_universe_hash",
        "candidate_metric_values_hash",
        "candidate_metric_values_summary",
        "metric_value_count",
        "missing_metric_count",
        "statistical_evidence_hash",
        "statistical_gate_result",
        "statistical_gate_fail_reasons",
        "white_reality_check_p_value",
        "summary_metric_max_bootstrap_p_value",
        "white_reality_check_method",
        "return_panel_hash",
        "bootstrap_sampling_contract_hash",
        "promotion_grade_limitations",
        "official_promotion_grade_wrc_generation_available",
        "effective_trial_count",
        "return_unit",
        "return_panel_observation_count",
    ):
        profile.pop(key, None)
    if isinstance(profile.get("scenario_results"), list):
        profile["scenario_results"] = [
            _candidate_behavior_scenario_result(item)
            for item in profile["scenario_results"]
            if isinstance(item, dict)
        ]
    return _strip_candidate_behavior_runtime_fields(profile)


def _candidate_behavior_scenario_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _strip_candidate_behavior_runtime_fields(value)
        for key, value in result.items()
        if key
        not in {
            "scenario_acceptance_gate_result",
            "scenario_fail_reasons",
            "resource_guard",
            "failure_artifact_ref",
            "failure_artifact_path",
            "retained_detail_summary",
            "train_audit_trace_index",
            "validation_audit_trace_index",
            "final_holdout_audit_trace_index",
            "validation_equity_curve",
            "final_holdout_equity_curve",
            "execution_calibration_gate",
            "metrics_gate_policy",
            "metrics_gate_policy_hash",
            "metrics_contract_required",
            "stress_suite_contract",
            "stress_suite_contract_hash",
            "validation_stress_suite",
            "final_holdout_stress_suite",
            "stress_suite_gate_result",
            "stress_suite_fail_reasons",
        }
        | _CANDIDATE_BEHAVIOR_RUNTIME_KEYS
    }


def _strip_candidate_behavior_runtime_fields(value: Any) -> Any:
    # Compatibility boundary for behavior identity. These fields are useful in
    # reports as provenance, runtime observability, or artifact locators, but
    # must not make behavior hashes depend on run/report namespace.
    if isinstance(value, dict):
        return {
            key: _strip_candidate_behavior_runtime_fields(item)
            for key, item in value.items()
            if key not in _CANDIDATE_BEHAVIOR_RUNTIME_KEYS
        }
    if isinstance(value, list):
        return [_strip_candidate_behavior_runtime_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_candidate_behavior_runtime_fields(item) for item in value]
    return value


def _strip_candidate_memory_observability_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_candidate_memory_observability_fields(item)
            for key, item in value.items()
            if key not in _CANDIDATE_MEMORY_OBSERVABILITY_KEYS
        }
    if isinstance(value, list):
        return [_strip_candidate_memory_observability_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_candidate_memory_observability_fields(item) for item in value]
    return value


def evaluate_candidate_for_promotion(candidate: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not candidate:
        return False, ["candidate_not_found"]
    gate = candidate.get("acceptance_gate_result")
    if gate != "PASS":
        reasons.append("acceptance_gate_not_passed")
    validation_metrics = candidate.get("validation_metrics")
    if not isinstance(validation_metrics, dict):
        reasons.append("validation_oos_evidence_missing")
    elif validation_metrics.get("trade_count") is None:
        reasons.append("validation_trade_count_missing")
    if candidate.get("walk_forward_required") and candidate.get("walk_forward_gate_result") != "PASS":
        reasons.append("walk_forward_gate_not_passed")
    _extend_final_holdout_reasons(candidate, reasons)
    _extend_dataset_quality_reasons(candidate, reasons)
    _extend_scenario_policy_reasons(candidate, reasons)
    _extend_execution_reality_reasons(candidate, reasons)
    _extend_execution_event_reasons(candidate, reasons)
    _extend_execution_calibration_reasons(candidate, reasons)
    _extend_production_calibration_policy_reasons(candidate, reasons)
    _extend_metrics_contract_reasons(candidate, reasons)
    _extend_stress_suite_reasons(candidate, reasons)
    _extend_portfolio_policy_reasons(candidate, reasons)
    _extend_probe_grade_reasons(candidate, reasons)
    profile_hash = candidate.get("candidate_profile_hash")
    if not profile_hash:
        reasons.append("candidate_profile_hash_missing")
    elif sha256_prefixed(build_candidate_profile(candidate)) != profile_hash:
        reasons.append("candidate_profile_hash_mismatch")
    _extend_strategy_parameter_contract_reasons(candidate, reasons)
    _extend_candidate_regime_policy_reasons(candidate, reasons)
    _extend_execution_contract_reasons(candidate, reasons)
    _extend_promotion_decision_contract_reasons(candidate, reasons)
    _extend_production_bound_decision_evidence_reasons(candidate, reasons)
    if not _candidate_has_regime_policy(candidate):
        reasons.append("regime_policy_missing")
    return not reasons, reasons


def validate_backtest_candidate_for_promotion(candidate: dict[str, Any] | None) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not candidate:
        return False, ["backtest_candidate_not_found", "candidate_not_found"]
    smoke_reasons = smoke_only_evidence_rejection_reasons(candidate)
    if smoke_reasons:
        reasons.extend(f"backtest_{reason}" for reason in smoke_reasons)
        reasons.extend(smoke_reasons)
    if candidate.get("compatibility_fallback") is True or candidate.get(
        "research_compatibility_execution_fallback"
    ) is True:
        reasons.extend(
            [
                "backtest_compatibility_fallback_not_promotion_grade",
                "compatibility_fallback_not_promotion_grade",
            ]
        )
    gate = candidate.get("acceptance_gate_result")
    if gate != "PASS":
        reasons.extend(["backtest_acceptance_gate_not_passed", "acceptance_gate_not_passed"])
    validation_metrics = candidate.get("validation_metrics")
    if not isinstance(validation_metrics, dict):
        reasons.extend(["backtest_validation_oos_evidence_missing", "validation_oos_evidence_missing"])
    elif validation_metrics.get("trade_count") is None:
        reasons.extend(["backtest_validation_trade_count_missing", "validation_trade_count_missing"])
    profile_hash = candidate.get("candidate_profile_hash")
    if not profile_hash:
        reasons.extend(["backtest_candidate_profile_hash_missing", "candidate_profile_hash_missing"])
    elif sha256_prefixed(build_candidate_profile(candidate)) != profile_hash:
        reasons.extend(["backtest_candidate_profile_hash_mismatch", "candidate_profile_hash_mismatch"])
    _extend_strategy_parameter_contract_reasons(candidate, reasons, prefix="backtest_")
    _extend_candidate_regime_policy_reasons(candidate, reasons, prefix="backtest_")
    _extend_execution_contract_reasons(candidate, reasons, prefix="backtest_")
    _extend_promotion_decision_contract_reasons(candidate, reasons, prefix="backtest_")
    _extend_production_bound_decision_evidence_reasons(candidate, reasons, prefix="backtest_")
    if not _candidate_has_regime_policy(candidate):
        reasons.extend(["backtest_regime_policy_missing", "regime_policy_missing"])
    _extend_final_holdout_reasons(candidate, reasons, prefix="backtest_")
    _extend_dataset_quality_reasons(candidate, reasons, prefix="backtest_")
    _extend_scenario_policy_reasons(candidate, reasons, prefix="backtest_")
    _extend_execution_reality_reasons(candidate, reasons, prefix="backtest_")
    _extend_execution_event_reasons(candidate, reasons, prefix="backtest_")
    _extend_execution_calibration_reasons(candidate, reasons, prefix="backtest_")
    _extend_production_calibration_policy_reasons(candidate, reasons, prefix="backtest_")
    _extend_metrics_contract_reasons(candidate, reasons, prefix="backtest_")
    _extend_stress_suite_reasons(candidate, reasons, prefix="backtest_")
    _extend_portfolio_policy_reasons(candidate, reasons, prefix="backtest_")
    _extend_probe_grade_reasons(candidate, reasons, prefix="backtest_")
    return not reasons, reasons


def _extend_promotion_decision_contract_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    tracked_fields = (
        "policy_materialization_mode",
        "runtime_comparable",
        "policy_input_hash",
        "policy_decision_hash",
        "policy_contract_hash",
        "decision_input_bundle_hash",
        "market_feature_hash",
        "canonical_feature_projection_hash",
        "final_exit_decision_input_hash",
        "snapshot_projector_version",
        "snapshot_projector_hash",
        "strategy_evaluation_provenance",
        "replay_fingerprint_hash",
        "runtime_decision_request_hash",
        "runtime_strategy_set_manifest_hash",
        "approved_profile_hash",
    )
    if not is_production_bound_target(candidate.get("deployment_tier")) and not any(
        key in candidate for key in tracked_fields
    ):
        return

    def add(reason: str) -> None:
        if prefix:
            reasons.append(prefix + reason)
        reasons.append(reason)

    if str(candidate.get("policy_materialization_mode") or "") == "research_exploratory":
        add("policy_materialization_mode_research_exploratory_not_promotion_grade")
    if candidate.get("runtime_comparable") is False:
        add("runtime_comparable_false_not_promotion_grade")
    if candidate.get("compatibility_fallback") is True:
        add("compatibility_fallback_not_promotion_grade")
    if candidate.get("allow_execution_compatibility_fallback") is True:
        add("execution_compatibility_fallback_not_promotion_grade")
    for field_name in (
        "policy_input_hash",
        "policy_decision_hash",
        "policy_contract_hash",
        "decision_input_bundle_hash",
        "final_exit_decision_input_hash",
        "snapshot_projector_version",
        "snapshot_projector_hash",
        "replay_fingerprint_hash",
        "runtime_decision_request_hash",
        "runtime_strategy_set_manifest_hash",
        "approved_profile_hash",
    ):
        value = candidate.get(field_name)
        if field_name.endswith("_hash"):
            if not _valid_prefixed_hash(value):
                add(field_name + "_missing")
        elif not str(value or "").strip():
            add(field_name + "_missing")
    if not (
        _valid_prefixed_hash(candidate.get("market_feature_hash"))
        or _valid_prefixed_hash(candidate.get("canonical_feature_projection_hash"))
    ):
        add("market_feature_hash_missing")
    provenance = candidate.get("strategy_evaluation_provenance")
    if not isinstance(provenance, dict):
        add("strategy_evaluation_provenance_missing")
    elif provenance.get("decision_boundary") != "StrategyDecisionService.evaluate":
        add("strategy_evaluation_provenance_boundary_invalid")


def _extend_production_bound_decision_evidence_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if not is_production_bound_target(candidate.get("deployment_tier")):
        return

    def add(reason: str) -> None:
        if prefix:
            reasons.append(prefix + reason)
        reasons.append(reason)

    code_provenance = candidate.get("code_provenance")
    if not isinstance(code_provenance, dict):
        add("code_provenance_missing")
    elif str(code_provenance.get("source") or "").strip().lower() == "unavailable":
        add("code_provenance_unavailable")
    for field_name in (
        "strategy_parameters_hash",
        "candidate_profile_hash",
        "approved_profile_hash",
        "fee_authority_hash",
        "fee_model_hash",
        "order_rules_hash",
        "slippage_model_hash",
    ):
        if not _valid_prefixed_hash(candidate.get(field_name)):
            add(field_name + "_missing")
    if str(candidate.get("order_rules_hash") or "") == "sha256:" + "0" * 64:
        add("order_rules_hash_empty")
    _extend_decision_equivalence_evidence_reasons(candidate, reasons, prefix=prefix)


def _extend_decision_equivalence_evidence_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    def add(reason: str) -> None:
        if prefix:
            reasons.append(prefix + reason)
        reasons.append(reason)

    path_value = str(candidate.get("decision_equivalence_report_path") or "").strip()
    hash_value = str(candidate.get("decision_equivalence_content_hash") or "").strip()
    if not path_value:
        add("decision_equivalence_report_missing")
        return
    if not hash_value.startswith("sha256:"):
        add("decision_equivalence_content_hash_missing")
        return
    try:
        with Path(path_value).expanduser().open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except OSError:
        add("decision_equivalence_report_missing")
        return
    except json.JSONDecodeError:
        add("decision_equivalence_status_not_pass")
        return
    if not isinstance(report, dict):
        add("decision_equivalence_status_not_pass")
        return
    from bithumb_bot.decision_equivalence import validate_decision_equivalence_report

    for reason in validate_decision_equivalence_report(report, expected_hash=hash_value):
        add(reason)
    status = str(candidate.get("decision_equivalence_status") or "").strip().lower()
    if status and status not in {"pass", "passed", "verified", "ok"}:
        add("decision_equivalence_status_not_pass")


def _valid_prefixed_hash(value: object) -> bool:
    raw = str(value or "")
    return raw.startswith("sha256:") and len(raw) > len("sha256:")


def _effective_policy_requirements(
    *,
    candidate: dict[str, Any],
    report: dict[str, Any],
    validation_run_payload: dict[str, Any] | None = None,
    validation_policy_source: str | None = None,
    validation_policy_required_stage_names: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    supplied_names = [str(item) for item in validation_policy_required_stage_names or [] if str(item)]
    run_policy_names = (validation_run_payload or {}).get("validation_policy_required_stage_names")
    run_names = [str(item) for item in run_policy_names or [] if str(item)]
    required_names = set(supplied_names or run_names)
    source = (
        validation_policy_source
        or str((validation_run_payload or {}).get("validation_policy_source") or "")
        or None
    )
    production_bound = is_production_bound_target(candidate.get("deployment_tier") or report.get("deployment_tier"))
    if not required_names:
        required_names.update({"readiness", "dataset_quality", "backtest", "promotion_eligibility", "promotion", "reproduce"})
        if candidate.get("walk_forward_required") or report.get("walk_forward_required"):
            required_names.add("walk_forward")
        if candidate.get("final_holdout_required_for_promotion") is not False:
            required_names.add("final_holdout")
        if stress_suite_required_for_candidate(candidate, report):
            required_names.add("stress_suite")
        if candidate.get("statistical_validation_required") or report.get("statistical_validation_required"):
            required_names.add("statistical_validation")
        if report.get("final_selection_required"):
            required_names.add("final_selection")
        source = source or ("legacy_manifest_with_production_fallbacks" if production_bound else "manifest_acceptance_gate")
    ordered_names = [
        name
        for name in (
            "readiness",
            "dataset_quality",
            "backtest",
            "final_holdout",
            "stress_suite",
            "statistical_validation",
            "final_selection",
            "walk_forward",
            "promotion_eligibility",
            "promotion",
            "reproduce",
        )
        if name in required_names
    ]
    return {
        "validation_policy_source": source,
        "validation_policy_required_stage_names": ordered_names,
        "effective_walk_forward_required": "walk_forward" in required_names,
        "effective_final_holdout_required": "final_holdout" in required_names,
        "effective_stress_suite_required": "stress_suite" in required_names,
        "effective_statistical_validation_required": "statistical_validation" in required_names,
        "effective_final_selection_required": "final_selection" in required_names,
    }


def _manifest_policy_flags(candidate: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    return {
        "manifest_walk_forward_required": bool(candidate.get("walk_forward_required") or report.get("walk_forward_required")),
        "manifest_final_holdout_required_for_promotion": candidate.get("final_holdout_required_for_promotion") is not False,
        "manifest_stress_suite_required": stress_suite_required_for_candidate(candidate, report),
        "manifest_statistical_validation_required": bool(candidate.get("statistical_validation_required") or report.get("statistical_validation_required")),
        "manifest_final_selection_required": bool(report.get("final_selection_required")),
    }


def _with_effective_requirement_fields(candidate: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    effective = dict(candidate)
    effective["walk_forward_required"] = bool(policy.get("effective_walk_forward_required"))
    effective["final_holdout_required_for_promotion"] = bool(policy.get("effective_final_holdout_required"))
    effective["stress_suite_required"] = bool(policy.get("effective_stress_suite_required"))
    effective["statistical_validation_required"] = bool(policy.get("effective_statistical_validation_required"))
    return effective


def _validation_run_policy_field_reasons(
    *,
    validation_run_payload: dict[str, Any],
    production_bound: bool,
    allow_legacy_lineage: bool,
) -> list[str]:
    if not production_bound or allow_legacy_lineage:
        return []
    source = str(validation_run_payload.get("validation_policy_source") or "").strip()
    required_names = validation_run_payload.get("validation_policy_required_stage_names")
    if not source or not isinstance(required_names, list) or not [str(item) for item in required_names if str(item)]:
        return ["validation_run_policy_fields_missing"]
    return []


def _validation_run_walk_forward_report_hash_reasons(
    *,
    validation_run_payload: dict[str, Any],
    walk_forward: ValidatedCandidate | None,
) -> list[str]:
    recorded_hash = str(validation_run_payload.get("walk_forward_report_hash") or "").strip()
    if not recorded_hash.startswith("sha256:"):
        return ["validation_run_walk_forward_report_hash_missing"]
    canonical_hash = str((walk_forward.source_report_hash if walk_forward else "") or "").strip()
    if recorded_hash != canonical_hash:
        return ["validation_run_walk_forward_report_hash_mismatch"]
    return []


def _extend_portfolio_policy_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if not is_production_bound_target(candidate.get("deployment_tier")):
        return
    policy = candidate.get("portfolio_policy")
    policy_hash = candidate.get("portfolio_policy_hash")
    simulation_hash = candidate.get("simulation_policy_hash")
    if not isinstance(policy, dict):
        reasons.extend([f"{prefix}portfolio_policy_missing", "portfolio_policy_missing"])
    elif not isinstance(policy_hash, str) or not policy_hash.startswith("sha256:"):
        reasons.extend([f"{prefix}portfolio_policy_hash_missing", "portfolio_policy_hash_missing"])
    elif sha256_prefixed(policy) != policy_hash:
        reasons.extend([f"{prefix}portfolio_policy_hash_mismatch", "portfolio_policy_hash_mismatch"])
    if not isinstance(simulation_hash, str) or not simulation_hash.startswith("sha256:"):
            reasons.extend([f"{prefix}simulation_policy_hash_missing", "simulation_policy_hash_missing"])


def _extend_strategy_parameter_contract_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if not is_production_bound_target(candidate.get("deployment_tier")):
        return
    effective = candidate.get("effective_strategy_parameters")
    if not isinstance(effective, dict):
        reasons.extend([f"{prefix}effective_strategy_parameters_missing", "effective_strategy_parameters_missing"])
        return
    expected_hash = candidate.get("effective_strategy_parameters_hash")
    if not isinstance(expected_hash, str) or not expected_hash.startswith("sha256:"):
        reasons.extend([
            f"{prefix}effective_strategy_parameters_hash_missing",
            "effective_strategy_parameters_hash_missing",
        ])
    elif materialized_strategy_parameters_hash(effective) != expected_hash:
        reasons.extend([
            f"{prefix}effective_strategy_parameters_hash_mismatch",
            "effective_strategy_parameters_hash_mismatch",
        ])
    source_map = candidate.get("strategy_parameter_source_map")
    if not isinstance(source_map, dict):
        reasons.extend([f"{prefix}strategy_parameter_source_map_missing", "strategy_parameter_source_map_missing"])


def _extend_candidate_regime_policy_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if not is_production_bound_target(candidate.get("deployment_tier")):
        return
    if not bool(candidate.get("candidate_regime_policy_required_for_live")):
        return
    if bool(candidate.get("candidate_regime_policy_applied_in_research")):
        return
    evidence_hash = str(candidate.get("candidate_regime_policy_equivalence_evidence_hash") or "")
    if bool(candidate.get("candidate_regime_policy_equivalence_required")) and not evidence_hash.startswith("sha256:"):
        reasons.extend([
            f"{prefix}candidate_regime_policy_equivalence_evidence_missing",
            "candidate_regime_policy_equivalence_evidence_missing",
        ])
        return
    if not bool(candidate.get("candidate_regime_policy_equivalence_required")):
        return
    evidence_path = str(candidate.get("candidate_regime_policy_equivalence_evidence_path") or "").strip()
    if not evidence_path:
        reasons.extend([
            f"{prefix}candidate_regime_policy_equivalence_evidence_path_missing",
            "candidate_regime_policy_equivalence_evidence_path_missing",
        ])
        return
    try:
        from bithumb_bot.evidence_chain import (
            EvidenceValidationError,
            validate_candidate_regime_policy_equivalence_evidence,
        )

        with Path(evidence_path).expanduser().open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise EvidenceValidationError("payload_not_object")
        validate_candidate_regime_policy_equivalence_evidence(
            payload,
            candidate_or_profile=candidate,
            expected_hash=evidence_hash,
            evidence_path=evidence_path,
        )
    except (OSError, ValueError, EvidenceValidationError) as exc:
        reasons.extend([
            f"{prefix}candidate_regime_policy_equivalence_evidence_invalid",
            "candidate_regime_policy_equivalence_evidence_invalid",
            f"candidate_regime_policy_equivalence_evidence_error:{exc}",
        ])


def _policy_hash_binding_reasons(
    *,
    report: dict[str, Any],
    candidate: dict[str, Any],
    lineage: dict[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    for field in ("portfolio_policy_hash", "simulation_policy_hash"):
        candidate_value = candidate.get(field)
        report_value = report.get(field)
        lineage_value = lineage.get(field) if isinstance(lineage, dict) else None
        if not str(candidate_value or "").startswith("sha256:"):
            reasons.append(f"{field}_missing")
        if not str(report_value or "").startswith("sha256:"):
            reasons.append(f"backtest_report_{field}_missing")
        if isinstance(lineage, dict) and not str(lineage_value or "").startswith("sha256:"):
            reasons.append(f"lineage_{field}_missing")
        if candidate_value and report_value and candidate_value != report_value:
            reasons.append(f"backtest_report_{field}_mismatch")
        if candidate_value and lineage_value and candidate_value != lineage_value:
            reasons.append(f"lineage_{field}_mismatch")
        if report_value and lineage_value and report_value != lineage_value:
            reasons.append(f"{field}_mismatch")
    return reasons


def _extend_probe_grade_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    warnings = {str(item) for item in candidate.get("warnings") or []}
    if "probe_grade_gate_detected" in warnings or "probe_grade_pass_not_promotable" in warnings:
        reasons.extend([f"{prefix}probe_grade_pass_not_promotable", "probe_grade_pass_not_promotable"])


def _extend_metrics_contract_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    _extend_fallback_metrics_reasons(candidate, reasons, prefix=prefix)
    policy = candidate.get("metrics_gate_policy")
    policy_hash = candidate.get("metrics_gate_policy_hash")
    policy_required = bool(candidate.get("metrics_contract_required"))
    if isinstance(policy, dict):
        policy_required = policy_required or bool(policy.get("metrics_contract_required"))
    elif policy_required:
        reasons.extend([f"{prefix}metrics_gate_policy_missing", "metrics_gate_policy_missing"])

    if policy_required or isinstance(policy, dict) or policy_hash is not None:
        if not isinstance(policy, dict):
            reasons.extend([f"{prefix}metrics_gate_policy_missing", "metrics_gate_policy_missing"])
        elif not isinstance(policy_hash, str) or not policy_hash.startswith("sha256:"):
            reasons.extend([f"{prefix}metrics_gate_policy_hash_missing", "metrics_gate_policy_hash_missing"])
        elif metrics_gate_policy_hash(policy) != policy_hash:
            reasons.extend([f"{prefix}metrics_gate_policy_hash_mismatch", "metrics_gate_policy_hash_mismatch"])

    if not policy_required:
        return
    _extend_metrics_v2_presence_reasons(
        candidate.get("validation_metrics_v2"),
        reasons,
        missing_code="validation_metrics_v2_missing",
        prefix=prefix,
    )
    if candidate.get("final_holdout_required_for_promotion") is not False:
        _extend_metrics_v2_presence_reasons(
            candidate.get("final_holdout_metrics_v2"),
            reasons,
            missing_code="final_holdout_metrics_v2_missing",
            prefix=prefix,
        )


def _extend_stress_suite_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if not stress_suite_required_for_candidate(candidate):
        return
    stress_reasons = validate_stress_suite_evidence_for_candidate(candidate=candidate, report={})
    reasons.extend(f"{prefix}{reason}" for reason in stress_reasons)
    reasons.extend(stress_reasons)


def _extend_metrics_v2_presence_reasons(
    metrics_v2: object,
    reasons: list[str],
    *,
    missing_code: str,
    prefix: str,
) -> None:
    if not isinstance(metrics_v2, dict):
        reasons.extend([f"{prefix}{missing_code}", missing_code])
        return
    if int(metrics_v2.get("metrics_schema_version") or 0) != METRICS_SCHEMA_VERSION:
        reasons.extend([f"{prefix}metrics_contract_missing", "metrics_contract_missing"])
    if metrics_v2.get("metrics_status") == "unavailable" or metrics_v2.get("metrics_v2_source") == "failure_fallback":
        reasons.extend([f"{prefix}metrics_v2_unavailable", "metrics_v2_unavailable"])
    if bool(metrics_v2.get("candidate_failed_before_complete_metrics")):
        reasons.extend(
            [
                f"{prefix}candidate_failed_before_complete_metrics",
                "candidate_failed_before_complete_metrics",
            ]
        )


def _extend_fallback_metrics_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if bool(candidate.get("candidate_failed_before_complete_metrics")):
        reasons.extend(
            [
                f"{prefix}candidate_failed_before_complete_metrics",
                "candidate_failed_before_complete_metrics",
            ]
        )
    if candidate.get("metrics_status") == "unavailable":
        reasons.extend([f"{prefix}metrics_unavailable", "metrics_unavailable"])
    if candidate.get("metrics_v2_source") == "failure_fallback":
        reasons.extend([f"{prefix}metrics_failure_fallback", "metrics_failure_fallback"])


def _extend_production_calibration_policy_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    result = validate_production_calibration_policy(candidate)
    if result.status == "FAIL":
        reasons.extend([f"{prefix}{reason}" for reason in result.reasons])
        reasons.extend(result.reasons)


def _extend_execution_calibration_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    gate = candidate.get("execution_calibration_gate")
    if candidate.get("execution_calibration_required"):
        if not isinstance(gate, dict):
            reasons.extend([f"{prefix}execution_calibration_missing", "execution_calibration_missing"])
            return
        if gate.get("status") != "PASS":
            gate_reasons = [str(item) for item in gate.get("reasons") or ["execution_calibration_failed"]]
            reasons.extend([f"{prefix}{reason}" for reason in gate_reasons])
            reasons.extend(gate_reasons)
    elif (
        candidate.get("execution_calibration_strictness") != "warn"
        and isinstance(gate, dict)
        and gate.get("status") == "FAIL"
    ):
        gate_reasons = [str(item) for item in gate.get("reasons") or ["execution_calibration_failed"]]
        reasons.extend([f"{prefix}{reason}" for reason in gate_reasons])
        reasons.extend(gate_reasons)


def _extend_execution_reality_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    policy = candidate.get("execution_timing_policy")
    summary = candidate.get("execution_reality_summary")
    policy_is_dict = isinstance(policy, dict)
    if not policy_is_dict:
        reasons.extend([f"{prefix}execution_timing_policy_missing", "execution_timing_policy_missing"])
    if not isinstance(summary, dict):
        reasons.extend([f"{prefix}execution_reality_summary_missing", "execution_reality_summary_missing"])
    else:
        gate_reasons = [str(item) for item in summary.get("execution_reality_gate_reasons") or []]
        if summary.get("execution_reality_gate_status") == "FAIL" and gate_reasons:
            reasons.extend([f"{prefix}{reason}" for reason in gate_reasons])
            reasons.extend(gate_reasons)
        if summary.get("execution_reality_level") == "candle_close_optimistic":
            reasons.extend([
                f"{prefix}execution_reality_level_below_required",
                "execution_reality_level_below_required",
            ])

    fill_policy = str(policy.get("fill_reference_policy") or "") if policy_is_dict else ""
    if fill_policy == "candle_close_legacy":
        reasons.extend([
            f"{prefix}execution_reference_price_candle_close_not_promotable",
            "execution_reference_price_candle_close_not_promotable",
        ])

    evaluation = evaluate_execution_reality_policy(
        production_bound=is_production_bound_target(candidate.get("deployment_tier")),
        execution_timing=policy if policy_is_dict else None,
        execution_timing_declared=policy_is_dict,
        execution_timing_declared_fields=set(policy) if policy_is_dict else set(),
        dataset_top_of_book=_promotion_top_of_book_evidence(candidate),
        context="promotion",
    )
    evaluation_reasons = [str(reason) for reason in evaluation.get("reasons") or []]
    reasons.extend([f"{prefix}{reason}" for reason in evaluation_reasons])
    reasons.extend(evaluation_reasons)


def _promotion_top_of_book_evidence(candidate: dict[str, Any]) -> dict[str, Any] | None:
    summary = candidate.get("top_of_book_quality_summary")
    contract = candidate.get("execution_reality_contract")
    if not isinstance(summary, dict) or not isinstance(contract, dict):
        return None

    coverage = float(summary.get("coverage_pct") or 0.0)
    joined = int(summary.get("joined_quote_count") or 0)
    summary_safe = (
        bool(summary.get("requested"))
        and bool(summary.get("required"))
        and str(summary.get("gate_status") or "") == "PASS"
        and coverage >= 100.0
        and joined > 0
    )
    contract_safe = (
        bool(contract.get("top_of_book_required"))
        and bool(contract.get("quote_evidence_available"))
        and contract.get("top_of_book_is_full_depth") is False
    )
    production_safe = summary_safe and contract_safe
    return {
        "required": production_safe,
        "missing_policy": "fail" if production_safe else "warn",
        "min_coverage_pct": 100.0 if production_safe else coverage,
    }


def _extend_execution_contract_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    contract = candidate.get("execution_reality_contract")
    contract_hash = candidate.get("execution_contract_hash")
    if not isinstance(contract, dict):
        reasons.extend([f"{prefix}execution_reality_contract_missing", "execution_reality_contract_missing"])
        return
    from bithumb_bot.execution_reality_contract import contract_hash_matches, unsupported_capability_reasons

    if not contract_hash_matches(contract, contract_hash):
        reasons.extend([f"{prefix}execution_contract_hash_mismatch", "execution_contract_hash_mismatch"])
    capability_reasons = unsupported_capability_reasons(contract)
    reasons.extend([f"{prefix}{reason}" for reason in capability_reasons])
    reasons.extend(capability_reasons)
    capability = _candidate_capability_contract(candidate)
    capability_hash = _candidate_capability_hash(candidate)
    if not isinstance(capability, dict):
        reasons.extend([f"{prefix}execution_capability_contract_missing", "execution_capability_contract_missing"])
    elif not capability_contract_hash_matches(capability, capability_hash):
        reasons.extend([f"{prefix}execution_capability_contract_hash_mismatch", "execution_capability_contract_hash_mismatch"])
    else:
        capability_validation_reasons = validate_execution_capability_contract(capability)
        if capability_validation_reasons:
            reasons.extend([f"{prefix}{reason}" for reason in capability_validation_reasons])
            reasons.extend(capability_validation_reasons)
        unavailable = [str(item) for item in capability.get("unavailable_required_capabilities") or []]
        if unavailable:
            reasons.extend([f"{prefix}execution_capability_required_unavailable", "execution_capability_required_unavailable"])


def _candidate_capability_contract(candidate: dict[str, Any]) -> dict[str, Any] | None:
    capability = candidate.get("execution_capability_contract")
    if isinstance(capability, dict):
        return capability
    contract = candidate.get("execution_reality_contract")
    if isinstance(contract, dict) and isinstance(contract.get("execution_capability_contract"), dict):
        return dict(contract["execution_capability_contract"])
    return None


def _candidate_capability_hash(candidate: dict[str, Any]) -> str | None:
    capability = _candidate_capability_contract(candidate)
    if not isinstance(capability, dict):
        return None
    return str(candidate.get("execution_capability_contract_hash") or capability.get("execution_capability_contract_hash") or "")


def _extend_execution_event_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    summary = candidate.get("execution_event_summary")
    if not isinstance(summary, dict):
        summary = candidate.get("validation_execution_event_summary")
    if not isinstance(summary, dict):
        reasons.extend([f"{prefix}execution_event_summary_missing", "execution_event_summary_missing"])
        return
    if bool(summary.get("execution_event_timeline_incomplete")):
        reasons.extend([f"{prefix}execution_event_timeline_incomplete", "execution_event_timeline_incomplete"])
    if int(summary.get("pending_execution_after_dataset_end_count") or 0) > 0:
        reasons.extend([f"{prefix}pending_execution_after_dataset_end", "pending_execution_after_dataset_end"])
    execution_filled = int(summary.get("execution_filled_count") or summary.get("filled_execution_count") or 0)
    portfolio_applied = int(summary.get("portfolio_applied_trade_count") or 0)
    closed_trade_count = int(summary.get("closed_trade_count") or 0)
    validation_metrics = candidate.get("validation_metrics")
    validation_trade_count = (
        int(validation_metrics.get("trade_count") or 0)
        if isinstance(validation_metrics, dict)
        else None
    )
    if execution_filled > 0 and portfolio_applied <= 0:
        reasons.extend([f"{prefix}portfolio_applied_trade_count_insufficient", "portfolio_applied_trade_count_insufficient"])
    if validation_trade_count is not None and closed_trade_count != validation_trade_count:
        reasons.extend([f"{prefix}execution_event_closed_trade_count_mismatch", "execution_event_closed_trade_count_mismatch"])


def _extend_final_holdout_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if candidate.get("final_holdout_required_for_promotion") is False:
        return
    metrics = candidate.get("final_holdout_metrics")
    if candidate.get("final_holdout_present") is not True or not isinstance(metrics, dict):
        reasons.extend([f"{prefix}final_holdout_evidence_missing", "final_holdout_evidence_missing"])
    elif metrics.get("trade_count") is None:
        reasons.extend([f"{prefix}final_holdout_evidence_missing", "final_holdout_evidence_missing"])


def _extend_dataset_quality_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    status = candidate.get("dataset_quality_gate_status")
    if status is None:
        return
    if status != "PASS":
        quality_reasons = [str(item) for item in candidate.get("dataset_quality_gate_reasons") or ["dataset_quality_failed"]]
        reasons.extend([f"{prefix}{reason}" for reason in quality_reasons])
        reasons.extend(quality_reasons)


def _extend_scenario_policy_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    scenario_results = candidate.get("scenario_results")
    if not isinstance(scenario_results, list) or not scenario_results:
        reasons.extend([f"{prefix}scenario_result_missing", "scenario_result_missing"])
        return
    if candidate.get("acceptance_gate_result") != "PASS":
        for reason in candidate.get("gate_fail_reasons") or ["scenario_policy_required_scenario_failed"]:
            reason_text = str(reason)
            if reason_text.startswith("scenario_policy_") or reason_text == "scenario_result_missing":
                reasons.extend([f"{prefix}{reason_text}", reason_text])
    for result in scenario_results:
        if result.get("scenario_acceptance_gate_result") != "PASS":
            reason_text = f"scenario_policy_required_scenario_failed:{result.get('scenario_id')}"
            reasons.extend([f"{prefix}{reason_text}", reason_text])


def _candidate_has_regime_policy(candidate: dict[str, Any]) -> bool:
    return (
        isinstance(candidate.get("regime_classifier_version"), str)
        and isinstance(candidate.get("allowed_live_regimes"), list)
        and isinstance(candidate.get("blocked_live_regimes"), list)
        and isinstance(candidate.get("regime_evidence"), dict)
        and isinstance(candidate.get("regime_gate_result"), dict)
    )


def _validated_backtest_candidate(candidate: dict[str, Any] | None) -> ValidatedCandidate:
    allowed, reasons = validate_backtest_candidate_for_promotion(candidate)
    if not allowed:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    assert candidate is not None
    profile = build_candidate_profile(candidate)
    return ValidatedCandidate(candidate=candidate, profile=profile, profile_hash=sha256_prefixed(profile))


def _verify_report_content_hash(report: dict[str, Any], *, label: str) -> str:
    expected = str(report.get("content_hash") or "").strip()
    if not expected.startswith("sha256:"):
        raise PromotionGateError(f"promotion refused: {label}_content_hash_missing")
    actual = sha256_prefixed(report_content_hash_payload(report))
    if actual != expected:
        raise PromotionGateError(f"promotion refused: {label}_hash_mismatch")
    return actual


def _subprocess_candidate_isolation_reasons(report: dict[str, Any]) -> list[str]:
    observability = report.get("execution_observability")
    if not isinstance(observability, dict):
        return ["subprocess_candidate_isolation_missing"]
    work_units = observability.get("work_units")
    if not isinstance(work_units, list) or not work_units:
        return ["subprocess_candidate_isolation_missing"]
    reasons: list[str] = []
    for item in work_units:
        if not isinstance(item, dict):
            reasons.append("subprocess_candidate_isolation_missing")
            continue
        evidence = item.get("worker_process_evidence")
        if not isinstance(evidence, dict):
            reasons.append("subprocess_candidate_isolation_missing")
            continue
        for field in (
            "worker_pid",
            "command_or_callable_identity",
            "input_hash",
            "output_hash",
            "exit_status",
            "resource_status",
            "terminal_audit_trace_status",
        ):
            if evidence.get(field) in (None, ""):
                reasons.append(f"subprocess_candidate_isolation_{field}_missing")
    return sorted(set(reasons))


def _load_statistical_evidence(*, report: dict[str, Any], report_dir: Path) -> dict[str, Any] | None:
    path_value = str(report.get("statistical_evidence_path") or "").strip()
    path = Path(path_value).expanduser() if path_value else report_dir / "statistical_selection_evidence.json"
    if not path.exists():
        return None
    import json

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise PromotionGateError("promotion refused: statistical_evidence_missing")
    return payload


def promote_candidate(
    *,
    experiment_id: str,
    candidate_id: str,
    manager: PathManager,
    generated_at: str | None = None,
    allow_legacy_lineage: bool = False,
    validation_run_path: str | Path | None = None,
    validation_run_binding_hash: str | None = None,
    allow_pending_validation_run: bool = False,
    validation_policy_source: str | None = None,
    validation_policy_required_stage_names: tuple[str, ...] | list[str] | None = None,
) -> PromotionResult:
    research_report_dir = manager.data_dir() / "reports" / "research" / experiment_id
    candidate_report_path = research_report_dir / "backtest_report.json"
    promotion_artifact_path = research_report_dir / f"promotion_{candidate_id}.json"
    if not candidate_report_path.exists():
        raise PromotionGateError(f"candidate report not found: {candidate_report_path}")

    with candidate_report_path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    smoke_report_reasons = smoke_only_evidence_rejection_reasons(report)
    if smoke_report_reasons:
        raise PromotionGateError(
            "promotion refused: "
            + ",".join(smoke_report_reasons)
            + ",regenerate_via_research_validate"
        )
    backtest_report_hash = _verify_report_content_hash(report, label="backtest_report")
    if report.get("experiment_id") != experiment_id:
        raise PromotionGateError("candidate report experiment_id mismatch")
    audit_reasons = validate_audit_trail_binding(report=report, manager=manager)
    if audit_reasons:
        raise PromotionGateError(f"promotion refused: {','.join(audit_reasons)}")
    dataset_quality_legacy_bypass_used = _verify_report_dataset_quality(
        report,
        allow_legacy_lineage=allow_legacy_lineage,
    )
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise PromotionGateError("candidate report does not contain candidates")
    candidate = next(
        (item for item in candidates if item.get("parameter_candidate_id") == candidate_id),
        None,
    )
    if isinstance(candidate, dict):
        policy = _effective_policy_requirements(
            candidate=candidate,
            report=report,
            validation_policy_source=validation_policy_source,
            validation_policy_required_stage_names=validation_policy_required_stage_names,
        )
    else:
        policy = _effective_policy_requirements(
            candidate={},
            report=report,
            validation_policy_source=validation_policy_source,
            validation_policy_required_stage_names=validation_policy_required_stage_names,
        )
    backtest = _validated_backtest_candidate(candidate)
    effective_backtest_candidate = _with_effective_requirement_fields(backtest.candidate, policy)
    final_holdout_reasons: list[str] = []
    if policy["effective_final_holdout_required"]:
        _extend_final_holdout_reasons(effective_backtest_candidate, final_holdout_reasons)
    if final_holdout_reasons:
        raise PromotionGateError(f"promotion refused: {','.join(sorted(set(final_holdout_reasons)))}")
    stress_reasons = validate_stress_suite_evidence_for_candidate(candidate=effective_backtest_candidate, report=report)
    if stress_reasons:
        raise PromotionGateError(f"promotion refused: {','.join(stress_reasons)}")
    walk_forward: ValidatedCandidate | None = None
    if policy["effective_walk_forward_required"]:
        walk_forward = validate_walk_forward_candidate_for_promotion(
            report_dir=research_report_dir,
            experiment_id=experiment_id,
            candidate_id=candidate_id,
            backtest_candidate=backtest.candidate,
        )
    statistical_evidence = _load_statistical_evidence(report=report, report_dir=research_report_dir)
    statistical_reasons = validate_statistical_evidence_for_candidate(
        candidate=effective_backtest_candidate,
        report=report,
        evidence=statistical_evidence,
    )
    production_bound_candidate = is_production_bound_target(
        backtest.candidate.get("deployment_tier") or report.get("deployment_tier")
    )
    if production_bound_candidate:
        statistical_reasons.extend(
            validate_experiment_registry_binding(
                report=report,
                evidence=statistical_evidence,
                require_complete=True,
            )
        )
    if statistical_reasons:
        raise PromotionGateError(f"promotion refused: {','.join(statistical_reasons)}")
    production_bound_report = is_production_bound_target(
        backtest.candidate.get("deployment_tier") or report.get("deployment_tier")
    )
    if production_bound_report:
        isolation_reasons = _subprocess_candidate_isolation_reasons(report)
        if isolation_reasons:
            raise PromotionGateError(f"promotion refused: {','.join(isolation_reasons)}")
    final_selection_reasons = validate_final_selection_report(report)
    if production_bound_report or policy["effective_final_selection_required"]:
        if final_selection_reasons:
            raise PromotionGateError(f"promotion refused: {','.join(final_selection_reasons)}")
        if report.get("final_selection_gate_result") != "PASS":
            raise PromotionGateError("promotion refused: final_selection_gate_not_passed")
        if candidate_id != report.get("selected_candidate_id"):
            raise PromotionGateError("promotion refused: candidate_not_selected_by_final_selection_contract")
    base_lineage = report.get("lineage") if isinstance(report.get("lineage"), dict) else None
    lineage: dict[str, Any] | None = None
    if base_lineage is not None:
        try:
            validate_lineage_artifact(base_lineage)
        except LineageValidationError as exc:
            raise PromotionGateError(f"promotion refused: {exc}") from exc
    elif not allow_legacy_lineage:
        raise PromotionGateError("promotion refused: lineage_missing")

    candidate = backtest.candidate
    if production_bound_report:
        policy_binding_reasons = _policy_hash_binding_reasons(
            report=report,
            candidate=candidate,
            lineage=base_lineage,
        )
        if policy_binding_reasons:
            raise PromotionGateError(f"promotion refused: {','.join(sorted(set(policy_binding_reasons)))}")
    profile = backtest.profile
    verified_profile_hash = backtest.profile_hash
    walk_forward_required = bool(policy["effective_walk_forward_required"])
    production_calibration_policy_result = validate_production_calibration_policy(candidate)
    candidate_calibration_hash = _candidate_calibration_hash(candidate)
    candidate_calibration_hashes = _candidate_calibration_hashes(candidate)
    calibration_warning_reasons = _execution_calibration_warning_reasons(candidate)
    promotion_warnings = sorted(
        set(str(item) for item in candidate.get("promotion_warnings") or [])
        | set(calibration_warning_reasons)
    )
    if base_lineage is None:
        promotion_warnings = sorted(
            set(promotion_warnings)
            | {"legacy_lineage_compatibility_used"}
            | ({"legacy_dataset_quality_bypass_used"} if dataset_quality_legacy_bypass_used else set())
        )
    validation_run_payload: dict[str, Any] | None = None
    validation_run_hash: str | None = None
    verified_validation_run_binding_hash: str | None = None
    validation_run_resolved_path: Path | None = None
    validation_run_binding_status = "not_required"
    validation_run_promotion_artifact_hash: str | None = None
    validation_run_reasons: list[str] = []
    from .validation_pipeline import (
        default_validation_run_path,
        validate_promotion_validation_run,
        verify_validation_run_binding,
        validation_run_required_for_promotion,
    )

    validation_required = validation_run_required_for_promotion(
        deployment_tier=candidate.get("deployment_tier") or report.get("deployment_tier")
    )
    if validation_required:
        supplied_binding_hash = str(validation_run_binding_hash or "").strip()
        if allow_pending_validation_run and supplied_binding_hash.startswith("sha256:"):
            validation_run_resolved_path = (
                Path(validation_run_path).expanduser()
                if validation_run_path is not None
                else default_validation_run_path(manager=manager, experiment_id=experiment_id)
            )
            verified_validation_run_binding_hash = supplied_binding_hash
            validation_run_binding_status = "verified_pre_promotion_binding"
        elif allow_pending_validation_run:
            validation_run_binding_status = "pending_validation_pipeline"
            promotion_warnings = sorted(set(promotion_warnings) | {"validation_run_pending_pipeline_completion"})
        elif allow_legacy_lineage and validation_run_path is None:
            validation_run_binding_status = "legacy_compatibility_used"
            promotion_warnings = sorted(set(promotion_warnings) | {"legacy_validation_run_compatibility_used"})
        else:
            validation_run_resolved_path = (
                Path(validation_run_path).expanduser()
                if validation_run_path is not None
                else default_validation_run_path(manager=manager, experiment_id=experiment_id)
            )
            try:
                validation_run_payload, validation_run_reasons = validate_promotion_validation_run(
                    validation_run_path=validation_run_resolved_path,
                    experiment_id=experiment_id,
                    manifest_hash=str(candidate.get("manifest_hash") or ""),
                    candidate_id=candidate_id,
                    backtest_report_hash=backtest_report_hash,
                    walk_forward_report_hash=walk_forward.source_report_hash if walk_forward_required and walk_forward else None,
                )
            except OSError as exc:
                raise PromotionGateError(f"promotion refused: validation_run_missing:{exc}") from exc
            if validation_run_reasons:
                raise PromotionGateError(f"promotion refused: {','.join(validation_run_reasons)}")
            validation_run_policy_reasons = _validation_run_policy_field_reasons(
                validation_run_payload=validation_run_payload,
                production_bound=production_bound_candidate,
                allow_legacy_lineage=allow_legacy_lineage,
            )
            if validation_run_policy_reasons:
                raise PromotionGateError(f"promotion refused: {','.join(validation_run_policy_reasons)}")
            validation_run_hash = str(validation_run_payload.get("content_hash") or "")
            binding_reasons = verify_validation_run_binding(
                validation_run_payload,
                expected_binding_hash=validation_run_payload.get("validation_run_binding_hash"),
            )
            if binding_reasons:
                raise PromotionGateError(f"promotion refused: {','.join(binding_reasons)}")
            verified_validation_run_binding_hash = str(validation_run_payload.get("validation_run_binding_hash") or "")
            validation_run_binding_status = "verified"
            bound_promotion_hash = str(validation_run_payload.get("promotion_artifact_hash") or "").strip()
            validation_run_promotion_artifact_hash = bound_promotion_hash or None
            if bound_promotion_hash.startswith("sha256:"):
                # A final validation run is immutable custody evidence for its bound
                # promotion artifact. Operators should use that artifact or rerun the
                # full validation pipeline from a fixed manifest, not regenerate a
                # different standalone promotion against the same validation run.
                raise PromotionGateError(
                    "promotion refused: validation_run_promotion_already_bound "
                    f"existing_promotion_artifact_hash={bound_promotion_hash} "
                    f"candidate_output_path={promotion_artifact_path.resolve()} "
                    "operator_next_step="
                    "use_existing_validation_run_bound_promotion_artifact_or_rerun_research_validate_from_fixed_manifest"
                )
    if validation_run_payload is not None:
        policy = _effective_policy_requirements(
            candidate=candidate,
            report=report,
            validation_run_payload=validation_run_payload,
            validation_policy_source=validation_policy_source,
            validation_policy_required_stage_names=validation_policy_required_stage_names,
        )
        if policy["effective_walk_forward_required"] and walk_forward is None:
            walk_forward = validate_walk_forward_candidate_for_promotion(
                report_dir=research_report_dir,
                experiment_id=experiment_id,
                candidate_id=candidate_id,
                backtest_candidate=backtest.candidate,
            )
        if policy["effective_walk_forward_required"]:
            walk_forward_binding_reasons = _validation_run_walk_forward_report_hash_reasons(
                validation_run_payload=validation_run_payload,
                walk_forward=walk_forward,
            )
            if walk_forward_binding_reasons:
                raise PromotionGateError(f"promotion refused: {','.join(walk_forward_binding_reasons)}")
        walk_forward_required = bool(policy["effective_walk_forward_required"])
    artifact = {
        "promotion_schema_version": 1,
        "strategy_name": candidate["strategy_name"],
        "strategy_profile_id": f"{experiment_id}_{candidate_id}",
        "strategy_profile_source_experiment": experiment_id,
        "strategy_profile_hash": verified_profile_hash,
        "candidate_id": candidate_id,
        "manifest_hash": candidate["manifest_hash"],
        "dataset_snapshot_id": candidate["dataset_snapshot_id"],
        "dataset_content_hash": candidate["dataset_content_hash"],
        "dataset_quality_hash": candidate.get("dataset_quality_hash"),
        "dataset_adapter_provenance": report.get("dataset_adapter_provenance"),
        "dataset_adapter_provenance_hash": report.get("dataset_adapter_provenance_hash"),
        "dataset_quality_gate_status": candidate.get("dataset_quality_gate_status"),
        "dataset_quality_gate_reasons": candidate.get("dataset_quality_gate_reasons"),
        "portfolio_policy": candidate.get("portfolio_policy"),
        "portfolio_policy_hash": candidate.get("portfolio_policy_hash"),
        "simulation_policy_hash": candidate.get("simulation_policy_hash"),
        "market": report.get("market"),
        "interval": report.get("interval"),
        "repository_version": candidate.get("repository_version") or report.get("repository_version"),
        "lineage_required": base_lineage is not None,
        "legacy_compatibility_used": base_lineage is None,
        "dataset_quality_legacy_bypass_used": dataset_quality_legacy_bypass_used,
        "lineage_hash": None,
        "validation_run_required": validation_required,
        "validation_run_binding_status": validation_run_binding_status,
        "validation_run_path": str(validation_run_resolved_path.resolve()) if validation_run_resolved_path else None,
        "validation_run_hash": validation_run_hash,
        "validation_run_binding_hash": verified_validation_run_binding_hash,
        "validation_run_promotion_artifact_hash": validation_run_promotion_artifact_hash,
        "validation_run_reasons": validation_run_reasons,
        "backtest_report_path": str(candidate_report_path.resolve()),
        "backtest_report_hash": backtest_report_hash,
        "walk_forward_report_path": str((research_report_dir / "walk_forward_report.json").resolve()) if walk_forward_required else None,
        "walk_forward_report_hash": None,
        **policy,
        **_manifest_policy_flags(candidate, report),
        "deployment_tier": candidate.get("deployment_tier") or "research_only",
        "base_cost_assumption": candidate.get("base_cost_assumption"),
        "cost_assumption_contract": candidate.get("cost_assumption_contract"),
        "production_calibration_policy_result": candidate.get("production_calibration_policy_result")
        or production_calibration_policy_result.as_dict(),
        "production_calibration_policy_reasons": candidate.get("production_calibration_policy_reasons")
        or list(production_calibration_policy_result.reasons),
        "execution_calibration_policy_source": candidate.get("execution_calibration_policy_source")
        or production_calibration_policy_result.policy_source,
        "execution_calibration_required": candidate.get("execution_calibration_required"),
        "execution_calibration_strictness": candidate.get("execution_calibration_strictness"),
        "execution_calibration_gate": candidate.get("execution_calibration_gate"),
        "execution_calibration_artifact_hash": candidate_calibration_hash,
        "execution_calibration_artifact_hashes": candidate_calibration_hashes,
        "experiment_family_id": report.get("experiment_family_id"),
        "hypothesis_id": report.get("hypothesis_id"),
        "hypothesis_status": report.get("hypothesis_status"),
        "pre_registered_gate": report.get("pre_registered_gate"),
        "search_budget": report.get("search_budget"),
        "parameter_space_hash": report.get("parameter_space_hash"),
        "parameter_grid_size": report.get("parameter_grid_size"),
        "attempt_index": report.get("attempt_index"),
        "failed_candidate_count": report.get("failed_candidate_count"),
        "holdout_reuse_count": report.get("holdout_reuse_count"),
        "dataset_reuse_policy": report.get("dataset_reuse_policy"),
        "candidate_profile": profile,
        "candidate_profile_hash": verified_profile_hash,
        "verified_candidate_profile_hash": verified_profile_hash,
        "gate_result": "PASS",
        "validation_evidence_source": "backtest_report.json",
        "backtest_candidate_profile_hash": backtest.profile_hash,
        "backtest_candidate_profile_verified": True,
        "walk_forward_required": walk_forward_required,
        "walk_forward_evidence_source": "walk_forward_report.json" if walk_forward_required else None,
        "walk_forward_candidate_profile_hash": walk_forward.profile_hash if walk_forward else None,
        "walk_forward_candidate_profile_verified": bool(walk_forward),
        "decision_equivalence_report_path": candidate.get("decision_equivalence_report_path"),
        "decision_equivalence_content_hash": candidate.get("decision_equivalence_content_hash"),
        "decision_equivalence_status": candidate.get("decision_equivalence_status"),
        "final_holdout_required_for_promotion": bool(policy["effective_final_holdout_required"]),
        "final_holdout_present": candidate.get("final_holdout_present") is True,
        "final_holdout_metrics": candidate.get("final_holdout_metrics"),
        "metrics_schema_version": candidate.get("metrics_schema_version"),
        "validation_metrics_v2": candidate.get("validation_metrics_v2"),
        "final_holdout_metrics_v2": candidate.get("final_holdout_metrics_v2"),
        "metrics_gate_policy": candidate.get("metrics_gate_policy"),
        "metrics_gate_policy_hash": candidate.get("metrics_gate_policy_hash"),
        "metrics_contract_required": bool(candidate.get("metrics_contract_required")),
        "stress_suite_required": bool(policy["effective_stress_suite_required"]),
        "stress_suite_contract": candidate.get("stress_suite_contract"),
        "stress_suite_contract_hash": candidate.get("stress_suite_contract_hash"),
        "validation_stress_suite": candidate.get("validation_stress_suite"),
        "final_holdout_stress_suite": candidate.get("final_holdout_stress_suite"),
        "stress_suite_gate_result": candidate.get("stress_suite_gate_result"),
        "stress_suite_fail_reasons": candidate.get("stress_suite_fail_reasons") or [],
        "final_selection_required": bool(policy["effective_final_selection_required"]),
        "final_selection_contract": report.get("final_selection_contract"),
        "final_selection_contract_hash": report.get("final_selection_contract_hash"),
        "final_selection_gate_result": report.get("final_selection_gate_result"),
        "final_selection_fail_reasons": report.get("final_selection_fail_reasons") or [],
        "selected_candidate_id": report.get("selected_candidate_id"),
        "selected_candidate_score_hash": report.get("selected_candidate_score_hash"),
        "candidate_final_scores_hash": report.get("candidate_final_scores_hash"),
        "statistical_validation_required": bool(policy["effective_statistical_validation_required"]),
        "statistical_validation_contract": candidate.get("statistical_validation_contract"),
        "evidence_grade": candidate.get("evidence_grade"),
        "statistical_method": candidate.get("statistical_method"),
        "return_panel_hash": candidate.get("return_panel_hash") or report.get("return_panel_hash"),
        "return_panel_path": candidate.get("return_panel_path") or report.get("return_panel_path"),
        "return_unit": candidate.get("return_unit") or report.get("return_unit"),
        "return_panel_observation_count": candidate.get("return_panel_observation_count") or report.get("return_panel_observation_count"),
        "family_trial_registry_path": report.get("family_trial_registry_path"),
        "family_trial_registry_prior_hash": report.get("family_trial_registry_prior_hash"),
        "family_trial_registry_row_hash": report.get("family_trial_registry_row_hash"),
        "hypothesis_identity_source": report.get("hypothesis_identity_source"),
        "experiment_family_identity_source": report.get("experiment_family_identity_source"),
        "experiment_registry_path": report.get("experiment_registry_path"),
        "experiment_registry_prior_hash": report.get("experiment_registry_prior_hash"),
        "experiment_registry_row_hash": report.get("experiment_registry_row_hash"),
        "experiment_registry_completion_row_hash": report.get("experiment_registry_completion_row_hash"),
        "experiment_registry_bound_evidence_hash": report.get("experiment_registry_bound_evidence_hash"),
        "experiment_registry_evidence_hash_phase": report.get("experiment_registry_evidence_hash_phase"),
        "final_holdout_fingerprint": report.get("final_holdout_fingerprint"),
        "final_holdout_identity_hash": report.get("final_holdout_identity_hash"),
        "final_holdout_content_hash": report.get("final_holdout_content_hash"),
        "final_holdout_reuse_key_hash": report.get("final_holdout_reuse_key_hash"),
        "final_holdout_split_hash": report.get("final_holdout_split_hash"),
        "computed_attempt_index": report.get("computed_attempt_index"),
        "computed_holdout_reuse_count": report.get("computed_holdout_reuse_count"),
        "declared_attempt_index": report.get("declared_attempt_index"),
        "declared_holdout_reuse_count": report.get("declared_holdout_reuse_count"),
        "research_freedom_hash": report.get("research_freedom_hash"),
        "registry_gate_result": report.get("registry_gate_result"),
        "registry_gate_fail_reasons": report.get("registry_gate_fail_reasons") or [],
        "benchmark": candidate.get("benchmark"),
        "primary_metric": candidate.get("primary_metric"),
        "primary_metric_source": candidate.get("primary_metric_source"),
        "selection_universe_hash": candidate.get("selection_universe_hash"),
        "candidate_metric_values_hash": candidate.get("candidate_metric_values_hash"),
        "candidate_metric_values_summary": candidate.get("candidate_metric_values_summary"),
        "metric_value_count": candidate.get("metric_value_count"),
        "missing_metric_count": candidate.get("missing_metric_count"),
        "statistical_evidence_path": candidate.get("statistical_evidence_path") or report.get("statistical_evidence_path"),
        "statistical_evidence_hash": candidate.get("statistical_evidence_hash") or report.get("statistical_evidence_hash"),
        "statistical_gate_result": candidate.get("statistical_gate_result"),
        "statistical_gate_fail_reasons": candidate.get("statistical_gate_fail_reasons") or [],
        "white_reality_check_p_value": candidate.get("white_reality_check_p_value"),
        "summary_metric_max_bootstrap_p_value": candidate.get("summary_metric_max_bootstrap_p_value"),
        "white_reality_check_method": candidate.get("white_reality_check_method"),
        "bootstrap_sampling_contract_hash": candidate.get("bootstrap_sampling_contract_hash"),
        "promotion_grade_limitations": candidate.get("promotion_grade_limitations") or [],
        "effective_trial_count": candidate.get("effective_trial_count"),
        "metrics_v2_summary": _promotion_metrics_v2_summary(candidate),
        "candidate_regime_policy_applied_in_research": bool(
            candidate.get("candidate_regime_policy_applied_in_research")
        ),
        "candidate_regime_policy_required_for_live": bool(
            candidate.get("candidate_regime_policy_required_for_live")
        ),
        "candidate_regime_policy_equivalence_required": bool(
            candidate.get("candidate_regime_policy_equivalence_required")
        ),
        "candidate_regime_policy_equivalence_evidence_hash": candidate.get(
            "candidate_regime_policy_equivalence_evidence_hash"
        ),
        "candidate_regime_policy_equivalence_evidence_path": candidate.get(
            "candidate_regime_policy_equivalence_evidence_path"
        ),
        "candidate_regime_policy_equivalence_evidence_status": candidate.get(
            "candidate_regime_policy_equivalence_evidence_status"
        ),
        "candidate_profile_evidence_contract_hash": candidate.get(
            "candidate_profile_evidence_contract_hash"
        ),
        "candidate_regime_policy_limitation_reasons": list(
            candidate.get("candidate_regime_policy_limitation_reasons") or []
        ),
        "scenario_policy": candidate.get("scenario_policy"),
        "execution_timing_policy": candidate.get("execution_timing_policy"),
        "execution_reality_contract": candidate.get("execution_reality_contract"),
        "execution_contract_hash": candidate.get("execution_contract_hash"),
        "execution_capability_contract": _candidate_capability_contract(candidate),
        "execution_capability_contract_hash": _candidate_capability_hash(candidate),
        "evidence_tier": (_candidate_capability_contract(candidate) or {}).get("evidence_tier") or candidate.get("evidence_tier"),
        "unavailable_required_capabilities": (
            (_candidate_capability_contract(candidate) or {}).get("unavailable_required_capabilities")
            or candidate.get("unavailable_required_capabilities")
        ),
        "execution_reality_summary": candidate.get("execution_reality_summary"),
        "execution_event_summary": _candidate_execution_event_summary(candidate),
        "train_execution_event_summary": candidate.get("train_execution_event_summary"),
        "validation_execution_event_summary": candidate.get("validation_execution_event_summary"),
        "final_holdout_execution_event_summary": candidate.get("final_holdout_execution_event_summary"),
        **_candidate_execution_event_summary_counts(candidate),
        "scenario_pass_count": candidate.get("scenario_pass_count"),
        "scenario_fail_count": candidate.get("scenario_fail_count"),
        "required_scenario_count": candidate.get("required_scenario_count"),
        "has_execution_calibration_warning": bool(calibration_warning_reasons),
        "execution_calibration_warning_reasons": calibration_warning_reasons,
        "promotion_warnings": promotion_warnings,
        "regime_classifier_version": candidate["regime_classifier_version"],
        "allowed_regimes": list(candidate["allowed_live_regimes"]),
        "blocked_regimes": list(candidate["blocked_live_regimes"]),
        "regime_evidence": dict(candidate["regime_evidence"]),
        "regime_gate_result": dict(candidate["regime_gate_result"]),
        "live_regime_policy": {
            "regime_classifier_version": candidate["regime_classifier_version"],
            "allowed_regimes": list(candidate["allowed_live_regimes"]),
            "blocked_regimes": list(candidate["blocked_live_regimes"]),
            "evidence_source": "backtest_report.json",
            "missing_policy_behavior": "fail_closed",
        },
        "operator_next_step": _operator_next_step(candidate),
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
    }
    if walk_forward_required:
        artifact["walk_forward_report_hash"] = walk_forward.source_report_hash if walk_forward else None
    path = promotion_artifact_path
    if base_lineage is not None:
        try:
            lineage = build_promotion_lineage(
                base_lineage=base_lineage,
                backtest_report_path=str(candidate_report_path.resolve()),
                backtest_report_hash=backtest_report_hash,
                walk_forward_report_path=artifact["walk_forward_report_path"],
                walk_forward_report_hash=artifact["walk_forward_report_hash"],
                candidate_id=candidate_id,
                candidate_profile_hash=verified_profile_hash,
                portfolio_policy_hash=artifact.get("portfolio_policy_hash"),
                simulation_policy_hash=artifact.get("simulation_policy_hash"),
                promotion_artifact_path=str(path.resolve()),
                execution_calibration_artifact_hash=candidate_calibration_hash,
                statistical_evidence_path=artifact.get("statistical_evidence_path"),
                statistical_evidence_hash=artifact.get("statistical_evidence_hash"),
                return_panel_path=artifact.get("return_panel_path"),
                return_panel_hash=artifact.get("return_panel_hash"),
                selection_universe_hash=artifact.get("selection_universe_hash"),
                candidate_metric_values_hash=artifact.get("candidate_metric_values_hash"),
                final_selection_contract_hash=artifact.get("final_selection_contract_hash"),
                selected_candidate_id=artifact.get("selected_candidate_id"),
                selected_candidate_score_hash=artifact.get("selected_candidate_score_hash"),
                candidate_final_scores_hash=artifact.get("candidate_final_scores_hash"),
                experiment_registry_path=artifact.get("experiment_registry_path"),
                experiment_registry_prior_hash=artifact.get("experiment_registry_prior_hash"),
                experiment_registry_row_hash=artifact.get("experiment_registry_row_hash"),
                experiment_registry_completion_row_hash=artifact.get("experiment_registry_completion_row_hash"),
                final_holdout_fingerprint=artifact.get("final_holdout_fingerprint"),
                final_holdout_identity_hash=artifact.get("final_holdout_identity_hash"),
                final_holdout_content_hash=artifact.get("final_holdout_content_hash"),
                final_holdout_reuse_key_hash=artifact.get("final_holdout_reuse_key_hash"),
                final_holdout_split_hash=artifact.get("final_holdout_split_hash"),
                experiment_registry_bound_evidence_hash=artifact.get("experiment_registry_bound_evidence_hash"),
                experiment_registry_evidence_hash_phase=artifact.get("experiment_registry_evidence_hash_phase"),
                research_freedom_hash=artifact.get("research_freedom_hash"),
                hypothesis_identity_source=artifact.get("hypothesis_identity_source"),
                experiment_family_identity_source=artifact.get("experiment_family_identity_source"),
                created_at=artifact["generated_at"],
            )
        except LineageValidationError as exc:
            raise PromotionGateError(f"promotion refused: {exc}") from exc
        artifact["lineage"] = lineage
        artifact["lineage_hash"] = lineage["lineage_hash"]
    content_hash = sha256_prefixed(content_hash_payload(artifact))
    artifact["content_hash"] = content_hash
    content_hash = str(artifact["content_hash"])
    if artifact.get("experiment_registry_row_hash"):
        append_promotion_registry_event(
            manager=manager,
            reservation_row_hash=str(artifact["experiment_registry_row_hash"]),
            promotion_artifact_hash=content_hash,
            promoted_candidate_id=candidate_id,
            created_at=artifact["generated_at"],
        )
    _ensure_research_output_path_allowed(manager, path)
    write_json_atomic(path, artifact)
    return PromotionResult(artifact=artifact, artifact_path=path, content_hash=content_hash)


def _walk_forward_candidate_for_promotion(
    *,
    report_dir: Path,
    experiment_id: str,
    candidate_id: str,
    backtest_candidate: dict[str, Any],
) -> dict[str, Any]:
    return validate_walk_forward_candidate_for_promotion(
        report_dir=report_dir,
        experiment_id=experiment_id,
        candidate_id=candidate_id,
        backtest_candidate=backtest_candidate,
    ).candidate


def validate_walk_forward_candidate_for_promotion(
    *,
    report_dir: Path,
    experiment_id: str,
    candidate_id: str,
    backtest_candidate: dict[str, Any],
) -> ValidatedCandidate:
    path = report_dir / "walk_forward_report.json"
    if not path.exists():
        raise PromotionGateError("promotion refused: walk_forward_missing")
    import json

    with path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    report_hash = _verify_report_content_hash(report, label="walk_forward_report")
    if report.get("experiment_id") != experiment_id:
        raise PromotionGateError("promotion refused: walk_forward_report_experiment_id_mismatch")
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise PromotionGateError("promotion refused: walk_forward_report_candidates_missing")
    candidate = next((item for item in candidates if item.get("parameter_candidate_id") == candidate_id), None)
    if not candidate:
        raise PromotionGateError("promotion refused: walk_forward_candidate_mismatch")
    for key in (
        "experiment_id",
        "strategy_name",
        "parameter_candidate_id",
        "parameter_values",
        "cost_model",
        "base_cost_assumption",
        "cost_assumption_contract",
        "execution_model",
        "execution_calibration_required",
        "execution_calibration_strictness",
        "execution_calibration_gate",
        "execution_calibration_artifact_hash",
        "execution_calibration_artifact_hashes",
        "deployment_tier",
        "manifest_hash",
    ):
        if candidate.get(key) != backtest_candidate.get(key):
            raise PromotionGateError("promotion refused: walk_forward_candidate_mismatch")
    if candidate.get("walk_forward_gate_result") != "PASS":
        raise PromotionGateError("promotion refused: walk_forward_gate_not_passed")
    _extend_final_holdout_reasons(candidate, reasons := [], prefix="walk_forward_")
    if reasons:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    _extend_metrics_contract_reasons(candidate, reasons := [], prefix="walk_forward_")
    if reasons:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    _extend_scenario_policy_reasons(candidate, reasons := [], prefix="walk_forward_")
    if reasons:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    walk_forward_metrics = candidate.get("walk_forward_metrics")
    if not isinstance(walk_forward_metrics, dict):
        raise PromotionGateError("promotion refused: walk_forward_metrics_missing")
    profile_hash = candidate.get("candidate_profile_hash")
    if not profile_hash:
        raise PromotionGateError("promotion refused: walk_forward_candidate_profile_hash_missing")
    profile = build_candidate_profile(candidate)
    verified_profile_hash = sha256_prefixed(profile)
    if verified_profile_hash != profile_hash:
        raise PromotionGateError("promotion refused: walk_forward_candidate_profile_hash_mismatch")
    return ValidatedCandidate(
        candidate=candidate,
        profile=profile,
        profile_hash=verified_profile_hash,
        source_report_hash=report_hash,
    )


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"research output path must be outside repository: {resolved}")


def _verify_report_dataset_quality(report: dict[str, Any], *, allow_legacy_lineage: bool = False) -> bool:
    lineage_present = isinstance(report.get("lineage"), dict)
    if not lineage_present:
        status = report.get("dataset_quality_gate_status")
        if status not in {None, "PASS"}:
            reasons = [str(item) for item in report.get("dataset_quality_gate_reasons") or ["dataset_quality_failed"]]
            raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
        return bool(allow_legacy_lineage)
    if report.get("dataset_quality_gate_status") is None:
        raise PromotionGateError("promotion refused: dataset_quality_missing")
    if report.get("dataset_quality_gate_status") != "PASS":
        reasons = [str(item) for item in report.get("dataset_quality_gate_reasons") or ["dataset_quality_failed"]]
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    quality_hash = str(report.get("dataset_quality_hash") or "")
    if not quality_hash.startswith("sha256:"):
        raise PromotionGateError("promotion refused: dataset_quality_hash_missing")
    quality_reports = report.get("dataset_quality_reports")
    if not isinstance(quality_reports, dict) or not quality_reports:
        raise PromotionGateError("promotion refused: dataset_quality_report_missing")
    return False


def _execution_calibration_warning_reasons(candidate: dict[str, Any]) -> list[str]:
    if candidate.get("execution_calibration_required"):
        return []
    if candidate.get("execution_calibration_strictness") != "warn":
        return []
    gate = candidate.get("execution_calibration_gate")
    if not isinstance(gate, dict) or gate.get("status") == "PASS":
        return []
    return [str(reason) for reason in gate.get("reasons") or ["execution_calibration_failed"]]


def _candidate_calibration_hash(candidate: dict[str, Any]) -> str | None:
    value = candidate.get("execution_calibration_artifact_hash")
    if isinstance(value, str) and value.startswith("sha256:"):
        return value
    gate = candidate.get("execution_calibration_gate")
    if isinstance(gate, dict) and isinstance(gate.get("artifact_hash"), str):
        return str(gate["artifact_hash"])
    hashes = _candidate_calibration_hashes(candidate)
    if len(hashes) == 1:
        return hashes[0]
    return None


def _candidate_calibration_hashes(candidate: dict[str, Any]) -> list[str]:
    values: set[str] = set()
    raw = candidate.get("execution_calibration_artifact_hashes")
    if isinstance(raw, list):
        values.update(str(value) for value in raw if str(value).startswith("sha256:"))
    gate = candidate.get("execution_calibration_gate")
    if isinstance(gate, dict):
        if isinstance(gate.get("artifact_hash"), str) and str(gate.get("artifact_hash")).startswith("sha256:"):
            values.add(str(gate["artifact_hash"]))
        raw_gate = gate.get("artifact_hashes")
        if isinstance(raw_gate, list):
            values.update(str(value) for value in raw_gate if str(value).startswith("sha256:"))
        for scenario_gate in gate.get("scenario_gates") or ():
            if isinstance(scenario_gate, dict):
                value = scenario_gate.get("artifact_hash")
                if isinstance(value, str) and value.startswith("sha256:"):
                    values.add(value)
    return sorted(values)


def _operator_next_step(candidate: dict[str, Any]) -> str:
    policy = candidate.get("production_calibration_policy_result")
    if isinstance(policy, dict) and policy.get("status") == "FAIL":
        return str(policy.get("operator_next_step") or "regenerate_execution_quality_calibration_and_rerun_research_backtest_with_execution_calibration")
    return "Review this artifact before manual paper env/profile consideration."


def _candidate_execution_event_summary(candidate: dict[str, Any]) -> dict[str, Any] | None:
    summary = candidate.get("execution_event_summary")
    if not isinstance(summary, dict):
        summary = candidate.get("validation_execution_event_summary")
    return dict(summary) if isinstance(summary, dict) else None


def _candidate_execution_event_summary_counts(candidate: dict[str, Any]) -> dict[str, Any]:
    summary = _candidate_execution_event_summary(candidate) or {}
    return {
        "pending_execution_after_dataset_end_count": int(summary.get("pending_execution_after_dataset_end_count") or 0),
        "execution_event_timeline_incomplete": bool(summary.get("execution_event_timeline_incomplete")),
        "portfolio_applied_trade_count": int(summary.get("portfolio_applied_trade_count") or 0),
        "execution_filled_count": int(summary.get("execution_filled_count") or summary.get("filled_execution_count") or 0),
        "closed_trade_count": int(summary.get("closed_trade_count") or 0),
    }


def _promotion_metrics_v2_summary(candidate: dict[str, Any]) -> dict[str, object]:
    return {
        **_metrics_v2_compact(candidate.get("validation_metrics_v2"), prefix="validation"),
        **_metrics_v2_compact(candidate.get("final_holdout_metrics_v2"), prefix="final_holdout"),
    }


def _metrics_v2_compact(metrics: object, *, prefix: str) -> dict[str, object]:
    if not isinstance(metrics, dict):
        return {
            f"{prefix}_cagr_pct": None,
            f"{prefix}_expectancy_per_trade_krw": None,
            f"{prefix}_exposure_time_pct": None,
            f"{prefix}_avg_holding_time_ms": None,
            f"{prefix}_open_position_at_end": None,
            f"{prefix}_fee_drag_ratio": None,
            f"{prefix}_fee_drag_ratio_basis": None,
            f"{prefix}_slippage_drag_ratio": None,
            f"{prefix}_slippage_drag_ratio_basis": None,
        }
    if metrics.get("metrics_status") == "unavailable" or metrics.get("metrics_v2_source") == "failure_fallback":
        return {
            f"{prefix}_cagr_pct": None,
            f"{prefix}_expectancy_per_trade_krw": None,
            f"{prefix}_exposure_time_pct": None,
            f"{prefix}_avg_holding_time_ms": None,
            f"{prefix}_open_position_at_end": None,
            f"{prefix}_fee_drag_ratio": None,
            f"{prefix}_fee_drag_ratio_basis": None,
            f"{prefix}_slippage_drag_ratio": None,
            f"{prefix}_slippage_drag_ratio_basis": None,
            f"{prefix}_metrics_status": metrics.get("metrics_status"),
            f"{prefix}_metrics_v2_source": metrics.get("metrics_v2_source"),
        }
    return_risk = metrics.get("return_risk") if isinstance(metrics.get("return_risk"), dict) else {}
    trade_quality = metrics.get("trade_quality") if isinstance(metrics.get("trade_quality"), dict) else {}
    time_exposure = metrics.get("time_exposure") if isinstance(metrics.get("time_exposure"), dict) else {}
    cost_execution = metrics.get("cost_execution") if isinstance(metrics.get("cost_execution"), dict) else {}
    return {
        f"{prefix}_cagr_pct": return_risk.get("cagr_pct"),
        f"{prefix}_expectancy_per_trade_krw": trade_quality.get("expectancy_per_trade_krw"),
        f"{prefix}_exposure_time_pct": time_exposure.get("exposure_time_pct"),
        f"{prefix}_avg_holding_time_ms": time_exposure.get("avg_holding_time_ms"),
        f"{prefix}_open_position_at_end": return_risk.get("open_position_at_end"),
        f"{prefix}_fee_drag_ratio": cost_execution.get("fee_drag_ratio"),
        f"{prefix}_fee_drag_ratio_basis": cost_execution.get("fee_drag_ratio_basis"),
        f"{prefix}_slippage_drag_ratio": cost_execution.get("slippage_drag_ratio"),
        f"{prefix}_slippage_drag_ratio_basis": cost_execution.get("slippage_drag_ratio_basis"),
    }
