from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic as write_json_atomic_untracked
from .artifact_store import ArtifactBudget, ArtifactStore, ResearchArtifactContext
from .hashing import report_content_hash_payload, sha256_prefixed


@dataclass(frozen=True)
class ResearchReportPaths:
    derived_path: Path
    report_path: Path
    candidate_events_path: Path
    candidate_results_dir: Path
    candidate_failures_dir: Path
    trace_manifest_path: Path


@dataclass(frozen=True)
class ResearchReportWriteResult:
    paths: ResearchReportPaths
    content_hash: str
    artifact_write_summary: dict[str, Any]

    def __iter__(self):
        yield self.paths
        yield self.content_hash


def research_paths(manager: PathManager, experiment_id: str, report_name: str) -> ResearchReportPaths:
    research_derived_root = manager.data_dir() / "derived" / "research" / experiment_id
    derived_path = research_derived_root / f"{report_name}_candidates.json"
    report_path = manager.data_dir() / "reports" / "research" / experiment_id / f"{report_name}_report.json"
    candidate_events_path = research_derived_root / "candidate_events.jsonl"
    candidate_results_dir = research_derived_root / "candidate_results"
    candidate_failures_dir = research_derived_root / "candidate_failures"
    trace_manifest_path = research_derived_root / "trace_manifest.json"
    _ensure_research_output_path_allowed(manager, derived_path)
    _ensure_research_output_path_allowed(manager, report_path)
    _ensure_research_output_path_allowed(manager, candidate_events_path)
    _ensure_research_output_path_allowed(manager, candidate_results_dir)
    _ensure_research_output_path_allowed(manager, candidate_failures_dir)
    _ensure_research_output_path_allowed(manager, trace_manifest_path)
    return ResearchReportPaths(
        derived_path=derived_path,
        report_path=report_path,
        candidate_events_path=candidate_events_path,
        candidate_results_dir=candidate_results_dir,
        candidate_failures_dir=candidate_failures_dir,
        trace_manifest_path=trace_manifest_path,
    )


def research_artifact_refs(paths: ResearchReportPaths, *, manager: PathManager) -> dict[str, str]:
    data_dir = manager.data_dir().resolve()
    return {
        "derived_candidates": _relative_artifact_ref(paths.derived_path, data_dir),
        "report": _relative_artifact_ref(paths.report_path, data_dir),
        "candidate_events": _relative_artifact_ref(paths.candidate_events_path, data_dir),
        "candidate_results_dir": _relative_artifact_ref(paths.candidate_results_dir, data_dir),
        "candidate_failures_dir": _relative_artifact_ref(paths.candidate_failures_dir, data_dir),
        "audit_trace_manifest": _relative_artifact_ref(paths.trace_manifest_path, data_dir),
    }


def research_artifact_paths(paths: ResearchReportPaths) -> dict[str, str]:
    return {
        "derived_path": str(paths.derived_path.resolve()),
        "report_path": str(paths.report_path.resolve()),
        "candidate_events_path": str(paths.candidate_events_path.resolve()),
        "candidate_results_dir": str(paths.candidate_results_dir.resolve()),
        "candidate_failures_dir": str(paths.candidate_failures_dir.resolve()),
        "audit_trace_manifest_path": str(paths.trace_manifest_path.resolve()),
    }


def finalize_research_report_payload(
    *,
    manager: PathManager,
    experiment_id: str,
    report_name: str,
    payload: dict[str, Any],
) -> tuple[ResearchReportPaths, dict[str, Any], str]:
    paths = research_paths(manager, experiment_id, report_name)
    report_payload, derived_candidates_payload, derived_candidates_hash = _reference_first_report_payload(
        payload,
        paths=paths,
        manager=manager,
    )
    report_payload["artifact_refs"] = research_artifact_refs(paths, manager=manager)
    report_payload["artifact_paths"] = research_artifact_paths(paths)
    report_payload.setdefault("artifact_hashes", {})["derived_candidates"] = derived_candidates_hash
    report_payload["derived_candidates_hash"] = derived_candidates_hash
    report_payload["candidate_count"] = len(derived_candidates_payload["candidates"])
    report_payload["candidate_summary_hash"] = sha256_prefixed(
        report_content_hash_payload({"candidates": report_payload.get("candidates", [])})
    )
    content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
    report_payload["content_hash"] = content_hash
    return paths, report_payload, content_hash


def write_research_report(
    *,
    manager: PathManager,
    experiment_id: str,
    report_name: str,
    payload: dict[str, Any],
    artifact_budget: ArtifactBudget | None = None,
    artifact_context: ResearchArtifactContext | None = None,
) -> ResearchReportWriteResult:
    started = time.perf_counter()
    paths = research_paths(manager, experiment_id, report_name)
    store = artifact_context or ArtifactStore(root=manager.data_dir(), budget=artifact_budget)
    report_payload, derived_candidates_payload, derived_candidates_hash = _reference_first_report_payload(
        payload,
        paths=paths,
        manager=manager,
    )
    report_payload["artifact_refs"] = research_artifact_refs(paths, manager=manager)
    report_payload["artifact_paths"] = research_artifact_paths(paths)
    report_payload.setdefault("artifact_hashes", {})["derived_candidates"] = derived_candidates_hash
    report_payload["derived_candidates_hash"] = derived_candidates_hash
    report_payload["candidate_count"] = len(derived_candidates_payload["candidates"])
    report_payload["candidate_summary_hash"] = sha256_prefixed(
        report_content_hash_payload({"candidates": report_payload.get("candidates", [])})
    )
    artifact_write_summary = {
        "schema_version": 1,
        "derived_candidates_path": str(paths.derived_path.resolve()),
        "derived_candidates_ref": _relative_artifact_ref(paths.derived_path, manager.data_dir().resolve()),
        "derived_candidates_hash": derived_candidates_hash,
        "derived_candidates_bytes": _json_byte_count(derived_candidates_payload),
        "report_path": str(paths.report_path.resolve()),
        "report_ref": _relative_artifact_ref(paths.report_path, manager.data_dir().resolve()),
        "report_bytes": 0,
        "artifact_file_count": _predicted_file_count(store, paths.derived_path, paths.report_path),
        "artifact_total_bytes": 0,
        "write_wall_seconds": 0.0,
    }
    report_payload["artifact_write_summary"] = dict(artifact_write_summary)
    report_payload.setdefault("artifact_observability", {})["report_write"] = dict(artifact_write_summary)
    artifact_write_summary["write_wall_seconds"] = time.perf_counter() - started
    report_payload["artifact_write_summary"]["write_wall_seconds"] = artifact_write_summary["write_wall_seconds"]
    report_payload["artifact_observability"]["report_write"]["write_wall_seconds"] = artifact_write_summary["write_wall_seconds"]
    artifact_write_summary["report_bytes"] = _stable_report_byte_count(report_payload)
    artifact_write_summary["artifact_total_bytes"] = (
        _current_total_bytes(store)
        + artifact_write_summary["derived_candidates_bytes"]
        + artifact_write_summary["report_bytes"]
    )
    report_payload["artifact_write_summary"] = dict(artifact_write_summary)
    report_payload["artifact_observability"]["report_write"] = dict(artifact_write_summary)
    report_payload["content_hash"] = sha256_prefixed(report_content_hash_payload(report_payload))
    artifact_write_summary["report_bytes"] = _stable_report_byte_count(report_payload)
    artifact_write_summary["artifact_total_bytes"] = (
        _current_total_bytes(store)
        + artifact_write_summary["derived_candidates_bytes"]
        + artifact_write_summary["report_bytes"]
    )
    report_payload["artifact_write_summary"] = dict(artifact_write_summary)
    report_payload["artifact_observability"]["report_write"] = dict(artifact_write_summary)
    final_content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
    report_payload["content_hash"] = final_content_hash
    derived_event = store.write_json_atomic(paths.derived_path, derived_candidates_payload)
    final_report_event = store.write_json_atomic(paths.report_path, report_payload)
    total_before_report = store.total_bytes - final_report_event.bytes
    artifact_write_summary.update(
        {
            "derived_candidates_bytes": derived_event.bytes,
            "artifact_file_count": store.file_count,
            "write_wall_seconds": time.perf_counter() - started,
        }
    )
    final_content_hash, artifact_write_summary = persist_final_research_report_observability(
        paths=paths,
        report_payload=report_payload,
        artifact_write_summary=artifact_write_summary,
        artifact_total_bytes_base=total_before_report,
    )
    return ResearchReportWriteResult(paths=paths, content_hash=final_content_hash, artifact_write_summary=artifact_write_summary)


def persist_final_research_report_observability(
    *,
    paths: ResearchReportPaths,
    report_payload: dict[str, Any],
    artifact_write_summary: dict[str, Any],
    artifact_total_bytes_base: int | None = None,
    stage_timings: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    final_summary = dict(artifact_write_summary)
    if stage_timings is not None:
        report_payload.setdefault("execution_observability", {})["stage_timings"] = list(stage_timings)
    report_payload.setdefault("artifact_observability", {})
    if artifact_total_bytes_base is None:
        artifact_total_bytes_base = int(final_summary["artifact_total_bytes"]) - int(final_summary["report_bytes"])
    final_summary["report_bytes"] = _stable_final_report_byte_count(
        report_payload,
        final_summary,
        artifact_total_bytes_base=int(artifact_total_bytes_base),
    )
    final_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + int(final_summary["report_bytes"])
    report_payload["artifact_write_summary"] = dict(final_summary)
    report_payload["artifact_observability"]["report_write"] = dict(final_summary)
    _sync_report_write_stage(report_payload, final_summary)
    final_content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
    report_payload["content_hash"] = final_content_hash
    final_summary["report_bytes"] = _stable_final_report_byte_count(
        report_payload,
        final_summary,
        artifact_total_bytes_base=int(artifact_total_bytes_base),
    )
    final_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + int(final_summary["report_bytes"])
    report_payload["artifact_write_summary"] = dict(final_summary)
    report_payload["artifact_observability"]["report_write"] = dict(final_summary)
    final_content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
    report_payload["content_hash"] = final_content_hash
    write_json_atomic_untracked(paths.report_path, report_payload)
    actual_report_bytes = paths.report_path.stat().st_size
    if actual_report_bytes != final_summary["report_bytes"]:
        final_summary["report_bytes"] = actual_report_bytes
        final_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + actual_report_bytes
        report_payload["artifact_write_summary"] = dict(final_summary)
        report_payload["artifact_observability"]["report_write"] = dict(final_summary)
        _sync_report_write_stage(report_payload, final_summary)
        final_content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
        report_payload["content_hash"] = final_content_hash
        write_json_atomic_untracked(paths.report_path, report_payload)
    return final_content_hash, final_summary


def _reference_first_report_payload(
    payload: dict[str, Any],
    *,
    paths: ResearchReportPaths,
    manager: PathManager,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    report_payload = dict(payload)
    candidates = list(report_payload.get("candidates", []))
    report_detail = _report_detail(report_payload)
    derived_candidates_payload = {
        "detail_policy": "summary_bounded" if report_detail == "summary" else "full",
        "candidates": [summarize_derived_candidate(candidate, report_detail) for candidate in candidates],
    }
    derived_candidates_hash = sha256_prefixed(report_content_hash_payload(derived_candidates_payload))
    if report_detail == "summary":
        report_payload["candidates"] = [summarize_report_candidate(candidate) for candidate in candidates]
        report_payload["derived_candidates_ref"] = _relative_artifact_ref(paths.derived_path, manager.data_dir().resolve())
        report_payload["derived_candidates_path"] = str(paths.derived_path.resolve())
    return report_payload, derived_candidates_payload, derived_candidates_hash


def _report_detail(payload: dict[str, Any]) -> str:
    research_run = payload.get("research_run")
    if isinstance(research_run, dict):
        return str(research_run.get("report_detail") or "full")
    return "full"


def summarize_report_candidate(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        return {"candidate_repr_hash": sha256_prefixed(report_content_hash_payload(candidate))}
    summary_keys = (
        "candidate_id",
        "acceptance_gate_result",
        "acceptance_gate_status",
        "status",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "validation_metrics_v2",
        "final_holdout_metrics_v2",
        "candidate_failed_before_complete_metrics",
        "gate_fail_reasons",
        "warnings",
        "failure_artifact_path",
        "failure_artifact_ref",
        "resource_guard",
        "behavior_hash",
        "strategy_behavior_hash",
        "profile_hash",
        "candidate_profile_hash",
        "metrics_hash",
        "content_hash",
    )
    summary = {key: candidate[key] for key in summary_keys if key in candidate}
    summary["candidate_payload_hash"] = sha256_prefixed(report_content_hash_payload(candidate))
    return summary


def summarize_derived_candidate(candidate: Any, report_detail: str) -> Any:
    if report_detail != "summary":
        return candidate
    if not isinstance(candidate, dict):
        return {"candidate_repr_hash": sha256_prefixed(report_content_hash_payload(candidate))}
    summary = _derived_candidate_index_summary(candidate)
    summary["derived_detail_policy"] = "summary_bounded"
    return summary


def _derived_candidate_index_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    summary_keys = (
        "experiment_id",
        "manifest_hash",
        "dataset_snapshot_id",
        "dataset_content_hash",
        "dataset_quality_hash",
        "dataset_quality_gate_status",
        "strategy_name",
        "parameter_candidate_id",
        "candidate_id",
        "parameter_values",
        "effective_strategy_parameters_hash",
        "candidate_behavior_profile_hash",
        "candidate_profile_hash",
        "acceptance_gate_result",
        "acceptance_gate_status",
        "gate_fail_reasons",
        "warnings",
        "failure_artifact_path",
        "failure_artifact_ref",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "metrics_hash",
        "candidate_failed_before_complete_metrics",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "retained_detail_summary",
    )
    summary = {key: candidate[key] for key in summary_keys if key in candidate}
    if "resource_guard" in candidate:
        summary["resource_guard"] = _compact_resource_guard(candidate["resource_guard"])
    summary["scenario_results"] = [
        _derived_scenario_index_summary(scenario) for scenario in candidate.get("scenario_results") or []
    ]
    summary["candidate_payload_hash"] = sha256_prefixed(report_content_hash_payload(candidate))
    summary["candidate_result_detail_policy"] = "summary_bounded"
    return summary


def _derived_scenario_index_summary(scenario: Any) -> dict[str, Any]:
    if not isinstance(scenario, dict):
        return {"scenario_repr_hash": sha256_prefixed(report_content_hash_payload(scenario))}
    summary_keys = (
        "scenario_id",
        "scenario_index",
        "scenario_type",
        "scenario_role",
        "scenario_acceptance_gate_result",
        "scenario_fail_reasons",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "train_behavior_hash",
        "validation_behavior_hash",
        "final_holdout_behavior_hash",
        "candidate_failed",
        "candidate_failed_before_complete_metrics",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "failure_reason",
        "failure_artifact_ref",
        "failure_artifact_path",
        "retained_detail_summary",
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
        "train_audit_trace_index",
        "validation_audit_trace_index",
        "final_holdout_audit_trace_index",
    )
    summary = {key: scenario[key] for key in summary_keys if key in scenario}
    if "resource_guard" in scenario:
        summary["resource_guard"] = _compact_resource_guard(scenario["resource_guard"])
    summary["train_equity_curve"] = []
    summary["validation_equity_curve"] = []
    summary["final_holdout_equity_curve"] = []
    for key in (
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
    ):
        if key in summary:
            summary[key] = summarize_resource_usage_for_candidate_artifact(summary[key])
    summary["detail_artifact_ref"] = scenario.get("detail_artifact_ref")
    summary["scenario_payload_hash"] = sha256_prefixed(report_content_hash_payload(scenario))
    _ensure_scenario_retained_detail_evidence(summary)
    return summary


def summarize_candidate_result(candidate: Any, report_detail: str) -> Any:
    if report_detail != "summary":
        return candidate
    if not isinstance(candidate, dict):
        return {"candidate_repr_hash": sha256_prefixed(report_content_hash_payload(candidate))}
    summary_keys = (
        "experiment_id",
        "manifest_hash",
        "dataset_snapshot_id",
        "dataset_content_hash",
        "dataset_quality_hash",
        "dataset_quality_gate_status",
        "dataset_quality_gate_reasons",
        "dataset_quality_report_hashes",
        "top_of_book_quality_summary",
        "strategy_name",
        "parameter_candidate_id",
        "candidate_id",
        "parameter_values",
        "effective_strategy_parameters_hash",
        "candidate_behavior_profile_hash",
        "candidate_profile_hash",
        "acceptance_gate_result",
        "acceptance_gate_status",
        "gate_fail_reasons",
        "warnings",
        "failure_artifact_path",
        "failure_artifact_ref",
        "resource_guard",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "metrics_hash",
        "validation_metrics",
        "validation_metrics_v2",
        "final_holdout_metrics",
        "final_holdout_metrics_v2",
        "walk_forward_metrics",
        "production_calibration_policy_result",
        "production_calibration_policy_reasons",
        "execution_calibration_gate",
        "execution_calibration_policy_source",
        "execution_calibration_artifact_hash",
        "execution_calibration_artifact_hashes",
        "has_execution_calibration_warning",
        "execution_calibration_warning_reasons",
        "retained_detail_summary",
    )
    summary = {key: candidate[key] for key in summary_keys if key in candidate}
    _compact_candidate_artifact_summary(summary)
    summary["scenario_results"] = [
        _scenario_result_summary(scenario) for scenario in candidate.get("scenario_results") or []
    ]
    summary["candidate_payload_hash"] = sha256_prefixed(report_content_hash_payload(candidate))
    summary["candidate_result_detail_policy"] = "summary_bounded"
    return summary


def summarize_resource_usage_for_candidate_artifact(resource_usage: Any) -> Any:
    if not isinstance(resource_usage, dict):
        return resource_usage
    summary: dict[str, Any] = {}
    for key, value in resource_usage.items():
        if key in {
            "applied_resource_limits",
            "memory_sampling_policy",
            "resource_policy",
            "strategy_diagnostics",
            "strategy_specific_diagnostics",
        }:
            summary[f"{key}_hash"] = sha256_prefixed(value)
            if isinstance(value, (dict, list, tuple)):
                summary[f"{key}_count"] = len(value)
            continue
        if key == "stage_trace":
            if isinstance(value, (list, tuple)):
                summary["stage_trace_count"] = len(value)
            if "stage_trace_hash" not in resource_usage:
                summary["stage_trace_hash"] = sha256_prefixed(value)
            continue
        if isinstance(value, dict):
            summary[key] = summarize_resource_usage_for_candidate_artifact(value)
            continue
        if isinstance(value, (list, tuple)):
            summary[f"{key}_count"] = len(value)
            summary[f"{key}_hash"] = sha256_prefixed(list(value))
            continue
        summary[key] = value
    return summary


def _scenario_result_summary(scenario: Any) -> dict[str, Any]:
    if not isinstance(scenario, dict):
        return {"scenario_repr_hash": sha256_prefixed(report_content_hash_payload(scenario))}
    summary_keys = (
        "scenario_id",
        "scenario_index",
        "scenario_type",
        "scenario_role",
        "scenario_acceptance_gate_result",
        "scenario_fail_reasons",
        "validation_metrics",
        "validation_metrics_v2",
        "final_holdout_metrics",
        "final_holdout_metrics_v2",
        "train_metrics",
        "train_metrics_v2",
        "walk_forward_metrics",
        "regime_gate_result",
        "market_regime_bucket_performance",
        "market_regime_coverage",
        "execution_model_hash",
        "model_params_hash",
        "execution_contract_hash",
        "execution_capability_contract_hash",
        "execution_reality_summary",
        "train_execution_event_summary",
        "validation_execution_event_summary",
        "final_holdout_execution_event_summary",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "train_behavior_hash",
        "validation_behavior_hash",
        "final_holdout_behavior_hash",
        "candidate_failed",
        "candidate_failed_before_complete_metrics",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "failure_reason",
        "resource_guard",
        "failure_artifact_ref",
        "failure_artifact_path",
        "retained_detail_summary",
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
        "train_audit_trace_index",
        "validation_audit_trace_index",
        "final_holdout_audit_trace_index",
    )
    summary = {key: scenario[key] for key in summary_keys if key in scenario}
    _compact_candidate_artifact_summary(summary)
    for key in (
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
    ):
        if key in summary:
            summary[key] = summarize_resource_usage_for_candidate_artifact(summary[key])
    summary["train_equity_curve"] = []
    summary["validation_equity_curve"] = []
    summary["final_holdout_equity_curve"] = []
    summary["detail_artifact_ref"] = scenario.get("detail_artifact_ref")
    summary["scenario_payload_hash"] = sha256_prefixed(report_content_hash_payload(scenario))
    _ensure_scenario_retained_detail_evidence(summary)
    return summary


def _ensure_scenario_retained_detail_evidence(summary: dict[str, Any]) -> None:
    if summary.get("equity_curve_hash") or summary.get("retained_detail_summary"):
        return
    summary["retained_detail_summary"] = {
        "detail_unavailable_reason": (
            summary.get("failure_reason")
            or summary.get("evaluation_status")
            or "summary_detail_not_retained"
        ),
        "scenario_payload_hash": summary["scenario_payload_hash"],
    }


def _compact_candidate_artifact_summary(summary: dict[str, Any]) -> None:
    for key in (
        "validation_metrics",
        "validation_metrics_v2",
        "final_holdout_metrics",
        "final_holdout_metrics_v2",
        "train_metrics",
        "train_metrics_v2",
        "walk_forward_metrics",
    ):
        if key in summary:
            summary[key] = _compact_metrics_payload(summary[key])
    for key in ("market_regime_bucket_performance", "market_regime_coverage"):
        if key in summary:
            summary[key] = _hashed_collection_summary(summary[key])
    if "execution_reality_summary" in summary:
        summary["execution_reality_summary"] = _compact_execution_reality_summary(
            summary["execution_reality_summary"]
        )
    if "regime_gate_result" in summary:
        summary["regime_gate_result"] = _compact_regime_gate_result(summary["regime_gate_result"])
    if "resource_guard" in summary:
        summary["resource_guard"] = _compact_resource_guard(summary["resource_guard"])
    if "execution_calibration_gate" in summary:
        summary["execution_calibration_gate"] = _compact_status_payload(
            summary["execution_calibration_gate"],
            hash_key="execution_calibration_gate_hash",
        )
    if "production_calibration_policy_result" in summary:
        summary["production_calibration_policy_result"] = _compact_status_payload(
            summary["production_calibration_policy_result"],
            hash_key="production_calibration_policy_hash",
        )


def _compact_metrics_payload(metrics: Any) -> Any:
    if not isinstance(metrics, dict):
        return metrics
    compact: dict[str, Any] = {}
    for key in (
        "metrics_schema_version",
        "metrics_status",
        "metrics_v2_source",
        "evaluation_status",
        "candidate_failed_before_complete_metrics",
    ):
        if key in metrics:
            compact[key] = metrics[key]
    if "limitation_reasons" in metrics:
        compact["limitation_reasons"] = metrics["limitation_reasons"]
    for section, keys in {
        "return_risk": (
            "total_return_pct",
            "realized_return_pct",
            "max_drawdown_pct",
            "cagr_pct",
            "open_position_at_end",
        ),
        "trade_quality": (
            "closed_trade_count",
            "execution_count",
            "profit_factor",
            "win_rate",
            "single_trade_dependency_score",
        ),
        "time_exposure": (
            "active_bar_count",
            "exposure_time_pct",
            "period_start_ts",
            "period_end_ts",
        ),
        "cost_execution": (
            "fee_total",
            "slippage_total",
            "filled_execution_count",
            "failed_execution_count",
            "skipped_execution_count",
        ),
    }.items():
        value = metrics.get(section)
        if isinstance(value, dict):
            compact[section] = {key: value[key] for key in keys if key in value}
    for key in (
        "return_pct",
        "max_drawdown_pct",
        "profit_factor",
        "profit_factor_unbounded",
        "trade_count",
        "win_rate",
        "fee_total",
        "slippage_total",
    ):
        if key in metrics:
            compact[key] = metrics[key]
    compact["metrics_payload_hash"] = sha256_prefixed(metrics)
    return compact


def _hashed_collection_summary(value: Any) -> Any:
    if not isinstance(value, (list, tuple)):
        return value
    return {
        "item_count": len(value),
        "payload_hash": sha256_prefixed(list(value)),
    }


def _compact_execution_reality_summary(summary: Any) -> Any:
    if not isinstance(summary, dict):
        return summary
    compact = {
        key: summary[key]
        for key in (
            "execution_reality_level",
            "execution_reality_gate_status",
            "execution_reality_gate_reasons",
            "execution_reference_policy",
            "signal_event_count",
            "fillable_signal_event_count",
            "filled_execution_count",
            "failed_execution_count",
            "skipped_execution_count",
            "pending_execution_count",
            "pending_execution_at_end_count",
        )
        if key in summary
    }
    compact["execution_reality_payload_hash"] = sha256_prefixed(summary)
    return compact


def _compact_regime_gate_result(result: Any) -> Any:
    if not isinstance(result, dict):
        return result
    compact = {
        key: result[key]
        for key in (
            "result",
            "status",
            "passed",
            "reasons",
            "allowed_live_regimes",
            "blocked_live_regimes",
        )
        if key in result
    }
    compact["regime_gate_payload_hash"] = sha256_prefixed(result)
    return compact


def _compact_resource_guard(resource_guard: Any) -> Any:
    if not isinstance(resource_guard, dict):
        return resource_guard
    compact = {
        key: resource_guard[key]
        for key in (
            "status",
            "reasons",
            "stage",
            "split",
            "scenario",
            "candles_processed",
            "decision_count",
            "signal_count",
            "trade_count",
            "closed_trade_count",
            "retained_decision_count",
            "retained_equity_point_count",
            "rss_delta_mb",
        )
        if key in resource_guard
    }
    compact["resource_guard_payload_hash"] = sha256_prefixed(resource_guard)
    return compact


def _compact_status_payload(payload: Any, *, hash_key: str) -> Any:
    if not isinstance(payload, dict):
        return payload
    compact = {
        key: payload[key]
        for key in (
            "status",
            "result",
            "passed",
            "required",
            "target",
            "policy_source",
            "operator_next_step",
            "reasons",
        )
        if key in payload
    }
    for key in ("scenario_gates", "artifact_hashes"):
        value = payload.get(key)
        if isinstance(value, (list, tuple)):
            compact[f"{key}_count"] = len(value)
    compact[hash_key] = sha256_prefixed(payload)
    return compact


def _json_byte_count(payload: dict[str, Any]) -> int:
    return len(
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False).encode("utf-8")
    ) + 1


def _current_total_bytes(store: ArtifactStore | ResearchArtifactContext) -> int:
    return int(getattr(store, "total_bytes"))


def _predicted_file_count(store: ArtifactStore | ResearchArtifactContext, *paths: Path) -> int:
    known_files = _known_files(store)
    resolved_paths = {path.resolve() for path in paths}
    if known_files is None:
        return int(getattr(store, "file_count")) + len(resolved_paths)
    return len(set(known_files) | resolved_paths)


def _known_files(store: ArtifactStore | ResearchArtifactContext) -> set[Path] | None:
    inner_store = getattr(store, "store", store)
    known_files = getattr(inner_store, "_known_files", None)
    if isinstance(known_files, set):
        return {Path(path).resolve() for path in known_files}
    return None


def _stable_report_byte_count(report_payload: dict[str, Any]) -> int:
    last = -1
    current = _json_byte_count(report_payload)
    while current != last:
        last = current
        report_payload["artifact_write_summary"]["report_bytes"] = current
        report_payload["artifact_observability"]["report_write"]["report_bytes"] = current
        current = _json_byte_count(report_payload)
    return current


def _stable_final_report_byte_count(
    report_payload: dict[str, Any],
    artifact_write_summary: dict[str, Any],
    *,
    artifact_total_bytes_base: int,
) -> int:
    last = -1
    _sync_report_write_stage(report_payload, artifact_write_summary)
    current = _json_byte_count(report_payload)
    while current != last:
        last = current
        artifact_write_summary["report_bytes"] = current
        artifact_write_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + current
        report_payload["artifact_write_summary"] = dict(artifact_write_summary)
        report_payload.setdefault("artifact_observability", {})["report_write"] = dict(artifact_write_summary)
        _sync_report_write_stage(report_payload, artifact_write_summary)
        report_payload["content_hash"] = sha256_prefixed(report_content_hash_payload(report_payload))
        current = _json_byte_count(report_payload)
    return current


def _sync_report_write_stage(report_payload: dict[str, Any], artifact_write_summary: dict[str, Any]) -> None:
    execution_observability = report_payload.get("execution_observability")
    if not isinstance(execution_observability, dict):
        return
    stage_timings = execution_observability.get("stage_timings")
    if not isinstance(stage_timings, list):
        return
    for stage_timing in stage_timings:
        if isinstance(stage_timing, dict) and stage_timing.get("stage") == "report_write":
            stage_timing["artifact_total_bytes"] = artifact_write_summary["artifact_total_bytes"]
            stage_timing["artifact_file_count"] = artifact_write_summary["artifact_file_count"]
            stage_timing["derived_candidates_bytes"] = artifact_write_summary["derived_candidates_bytes"]
            stage_timing["report_bytes"] = artifact_write_summary["report_bytes"]


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"research output path must be outside repository: {resolved}")


def _relative_artifact_ref(path: Path, data_dir: Path) -> str:
    return path.resolve().relative_to(data_dir).as_posix()
