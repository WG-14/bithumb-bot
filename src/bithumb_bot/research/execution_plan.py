from __future__ import annotations

import os
import platform
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dataset_snapshot import DatasetQualityReport, DatasetSnapshot, combined_dataset_quality_hash
from .experiment_manifest import ExecutionScenario, ExperimentManifest, required_execution_scenarios
from .hashing import sha256_prefixed
from .parameter_space import candidate_id, iter_parameter_candidates
from .process_runtime import process_policy_observability
from .resource_planner import plan_research_resources
from .backtest_types import resolve_tick_observability_policy


def parallel_work_task_count(*, candidate_count: int, scenario_count: int, split_count: int, work_unit: str) -> int:
    normalized = str(work_unit or "candidate_scenario").strip().lower()
    if normalized == "candidate_scenario_split":
        return int(candidate_count) * int(scenario_count) * int(split_count)
    return int(candidate_count) * int(scenario_count)


def parallel_efficiency_payload(
    *,
    available_work_tasks: int,
    requested_max_workers: int,
    effective_max_workers: int | None = None,
    work_unit: str = "candidate_scenario",
    effective_worker_source: str = "runtime",
    observed_worker_count: int | None = None,
    worker_warning_reasons: list[str] | tuple[str, ...] | None = None,
    worker_observation_warning_reasons: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    requested = max(1, int(requested_max_workers or 1))
    effective = max(1, int(effective_max_workers if effective_max_workers is not None else requested))
    available = max(0, int(available_work_tasks))
    ratio = round(float(available) / float(effective), 6)
    expected_utilization = round(min(1.0, ratio) * 100.0, 6)
    limiting_factor = _parallelism_limiting_factor(
        available_work_tasks=available,
        effective_max_workers=effective,
        work_unit=work_unit,
    )
    warning_reasons: list[str] = []
    if available < effective:
        warning_reasons.append("available_work_tasks_below_effective_workers")
    if effective < requested:
        warning_reasons.append("effective_workers_below_requested")
    payload: dict[str, Any] = {
        "available_work_tasks": available,
        "available_parallel_work_tasks": available,
        "requested_max_workers": requested,
        "effective_max_workers": effective,
        "effective_worker_source": effective_worker_source,
        "parallel_task_to_worker_ratio": ratio,
        "expected_worker_utilization_pct": expected_utilization,
        "parallelism_limiting_factor": limiting_factor,
        "parallel_efficiency_warning_reasons": warning_reasons,
        "suggested_actions": _parallel_efficiency_suggested_actions(
            available_work_tasks=available,
            effective_max_workers=effective,
            work_unit=work_unit,
        ),
        "worker_warning_reasons": sorted(set(str(item) for item in (worker_warning_reasons or ()))),
        "worker_observation_warning_reasons": sorted(
            set(str(item) for item in (worker_observation_warning_reasons or ()))
        ),
    }
    if observed_worker_count is None:
        payload["observed_worker_count"] = None
        payload["observed_worker_utilization_pct"] = None
        payload["observed_worker_utilization_unavailable_reason"] = "worker_observation_pending"
    else:
        observed = max(0, int(observed_worker_count))
        payload["observed_worker_count"] = observed
        payload["observed_worker_utilization_pct"] = round(min(1.0, observed / float(effective)) * 100.0, 6)
    return payload


def _parallelism_limiting_factor(*, available_work_tasks: int, effective_max_workers: int, work_unit: str) -> str:
    if int(available_work_tasks) >= int(effective_max_workers):
        return "available_work_tasks_match_or_exceed_effective_workers"
    normalized = str(work_unit or "candidate_scenario").strip().lower()
    if normalized == "candidate_scenario":
        return "work_unit_granularity_candidate_scenario"
    if normalized == "candidate_scenario_split":
        return "available_split_work_tasks"
    return f"work_unit_granularity_{normalized}"


def _parallel_efficiency_suggested_actions(
    *, available_work_tasks: int, effective_max_workers: int, work_unit: str
) -> list[str]:
    if int(available_work_tasks) >= int(effective_max_workers):
        return []
    actions = ["increase_candidate_count", "increase_scenario_count", "run_research_batch", "profile_single_candidate"]
    if str(work_unit or "").strip().lower() != "candidate_scenario_split":
        actions.insert(2, "use_candidate_scenario_split")
    return actions


@dataclass(frozen=True)
class ResearchWorkUnit:
    candidate_index: int
    candidate_id: str
    scenario_index: int
    scenario_id: str
    split_name: str
    work_unit_mode: str
    parameter_values: dict[str, Any]
    dataset_content_hash: str
    portfolio_policy_hash: str
    simulation_policy_hash: str
    execution_model_hash: str
    execution_timing_hash: str
    seed_context: dict[str, Any]
    work_unit_hash: str
    work_result_input_hash: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_index": self.candidate_index,
            "candidate_id": self.candidate_id,
            "scenario_index": self.scenario_index,
            "scenario_id": self.scenario_id,
            "split_name": self.split_name,
            "work_unit_mode": self.work_unit_mode,
            "parameter_values": dict(self.parameter_values),
            "dataset_content_hash": self.dataset_content_hash,
            "portfolio_policy_hash": self.portfolio_policy_hash,
            "simulation_policy_hash": self.simulation_policy_hash,
            "execution_model_hash": self.execution_model_hash,
            "execution_timing_hash": self.execution_timing_hash,
            "seed_context": dict(self.seed_context),
            "work_unit_hash": self.work_unit_hash,
            "work_result_input_hash": self.work_result_input_hash,
        }


@dataclass(frozen=True)
class ResearchExecutionPlan:
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(self.payload)


def scenario_id(scenario: ExecutionScenario, scenario_index: int) -> str:
    return sha256_prefixed({"index": scenario_index, "scenario": scenario.as_dict()})[:24]


def build_research_execution_plan(
    *,
    manifest: ExperimentManifest,
    snapshots: dict[str, DatasetSnapshot],
    quality_reports: dict[str, DatasetQualityReport],
    db_path: str | Path,
    repository_version: str | None,
    created_at: str | None,
    include_walk_forward: bool = False,
) -> ResearchExecutionPlan:
    candidates = iter_parameter_candidates(manifest.parameter_space)
    execution_scenarios = required_execution_scenarios(manifest.execution_model.scenarios)
    split_names = _ordered_split_names(snapshots)
    dataset_hashes = precompute_dataset_hashes(snapshots)
    split_count = len(split_names)
    walk_forward_split_count = sum(1 for split_name in split_names if split_name.startswith("window_"))
    strategy_run_count = len(candidates) * len(execution_scenarios) * split_count
    if include_walk_forward:
        strategy_run_count = len(candidates) * len(execution_scenarios) * (
            max(0, split_count - walk_forward_split_count) + walk_forward_split_count
        )
    dataset_candles = sum(len(snapshot.candles) for snapshot in snapshots.values())
    plugin_complexity = _plugin_complexity_metadata(
        manifest.strategy_name,
        parameter_space=manifest.parameter_space,
        report_detail=manifest.research_run.report_detail,
        diagnostic_mode=manifest.research_run.diagnostic_mode,
        audit_trail=manifest.research_run.audit_trail,
        expected_candle_count=dataset_candles,
    )
    plugin_expected_us_per_candle = _plugin_expected_us_per_candle(plugin_complexity)
    estimated_plugin_runtime_us = (
        int(dataset_candles)
        * len(candidates)
        * len(execution_scenarios)
        * plugin_expected_us_per_candle
    )
    run_environment = build_run_environment(
        manifest=manifest,
        db_path=db_path,
        repository_version=repository_version,
    )
    effective_worker_source = "requested_pending_runtime_resolution"
    resource_plan = plan_research_resources(
        manifest=manifest,
        candidate_count=len(candidates),
        scenario_count=len(execution_scenarios),
        split_count=split_count,
    )
    resource_plan_payload = resource_plan.as_dict()
    work_unit_selection = resource_plan.work_unit_selection.as_dict()
    available_parallel_work_tasks = parallel_work_task_count(
        candidate_count=len(candidates),
        scenario_count=len(execution_scenarios),
        split_count=split_count,
        work_unit=resource_plan.work_unit_type,
    )
    parallel_capacity = parallel_efficiency_payload(
        available_work_tasks=available_parallel_work_tasks,
        requested_max_workers=resource_plan.requested_max_workers,
        effective_max_workers=resource_plan.effective_max_workers,
        work_unit=resource_plan.work_unit_type,
        effective_worker_source=effective_worker_source,
    )
    plan = {
        "schema_version": 1,
        "manifest_hash": manifest.manifest_hash(),
        "simulation_seed_scope_hash": manifest.simulation_seed_scope_hash(),
        "experiment_id": manifest.experiment_id,
        "dataset_hashes": {name: dataset_hashes[name] for name in split_names},
        "dataset_quality_hash": combined_dataset_quality_hash(tuple(quality_reports.values())),
        "candidate_count": len(candidates),
        "scenario_count": len(execution_scenarios),
        "split_count": split_count,
        "split_names": split_names,
        "estimated_strategy_runs": strategy_run_count,
        "available_parallel_work_tasks": available_parallel_work_tasks,
        "parallel_task_to_worker_ratio": parallel_capacity["parallel_task_to_worker_ratio"],
        "expected_worker_utilization_pct": parallel_capacity["expected_worker_utilization_pct"],
        "parallelism_limiting_factor": parallel_capacity["parallelism_limiting_factor"],
        "effective_worker_source": effective_worker_source,
        "dataset_candles": dataset_candles,
        "estimated_candles": dataset_candles,
        "estimated_candle_evaluations": (
            dataset_candles
            * len(candidates)
            * len(execution_scenarios)
        ),
        "plugin_complexity": plugin_complexity,
        "estimated_plugin_runtime_us": estimated_plugin_runtime_us,
        "execution_mode": resource_plan.execution_mode,
        "max_workers": resource_plan.effective_max_workers,
        "requested_max_workers": resource_plan.requested_max_workers,
        "process_start_method": manifest.research_run.execution.process_start_method,
        "work_unit_type": resource_plan.work_unit_type,
        "requested_work_unit_type": manifest.research_run.execution.work_unit,
        "resource_plan": resource_plan_payload,
        "work_unit_selection": work_unit_selection,
        "deterministic_merge_order": manifest.research_run.execution.deterministic_merge_order,
        "resume_enabled": manifest.research_run.execution.resume,
        "created_at": created_at,
        "run_environment": run_environment,
        "run_environment_hash": sha256_prefixed(run_environment),
    }
    estimated_audit_stream_rows = _estimated_audit_stream_rows(
        audit_mode=manifest.research_run.audit_trail.mode,
        dataset_candles=dataset_candles,
        candidate_count=len(candidates),
        scenario_count=len(execution_scenarios),
    )
    estimated_artifact_write_count = _estimated_artifact_write_count(
        audit_mode=manifest.research_run.audit_trail.mode,
        full_decisions_external_jsonl=manifest.research_run.artifact_policy.full_decisions_external_jsonl,
        work_unit_count=len(candidates) * len(execution_scenarios),
        split_count=split_count,
    )
    estimated_hash_payload_bytes = _estimated_hash_payload_bytes(
        dataset_candles=dataset_candles,
        candidate_count=len(candidates),
        scenario_count=len(execution_scenarios),
        split_count=split_count,
    )
    canonical_estimate = estimate_canonical_observability_cost(
        estimated_tick_events=plan["estimated_candle_evaluations"],
        report_detail=manifest.research_run.report_detail,
        diagnostic_mode=manifest.research_run.diagnostic_mode,
        audit_trail=manifest.research_run.audit_trail,
        policy_materialization_mode=(
            "research_promotion"
            if manifest.research_run.diagnostic_mode == "promotion_candidate"
            and manifest.research_run.audit_trail.complete_external
            else "research_exploratory"
        ),
    )
    pre_parallel_dataset_hash_payload_bytes = _estimated_pre_parallel_dataset_hash_payload_bytes(
        snapshots=snapshots,
        split_names=split_names,
    )
    pre_parallel_work_unit_count = len(candidates) * len(execution_scenarios)
    pre_parallel_split_hash_count = split_count
    pre_parallel_dataset_hash_call_count = pre_parallel_split_hash_count
    estimated_artifact_bytes = _estimated_artifact_bytes(
        candidate_count=len(candidates),
        scenario_count=len(execution_scenarios),
        split_count=split_count,
        audit_mode=manifest.research_run.audit_trail.mode,
        estimated_audit_stream_rows=estimated_audit_stream_rows,
        estimated_artifact_write_count=estimated_artifact_write_count,
        estimated_hash_payload_bytes=estimated_hash_payload_bytes,
        full_decisions_external_jsonl=manifest.research_run.artifact_policy.full_decisions_external_jsonl,
        report_detail=manifest.research_run.report_detail,
    )
    max_artifact_bytes = manifest.research_run.resource_limits.max_artifact_bytes
    artifact_budget_reasons: list[str] = []
    artifact_budget_status = "PASS"
    if max_artifact_bytes is not None and estimated_artifact_bytes > int(max_artifact_bytes):
        artifact_budget_status = "WARN"
        artifact_budget_reasons.append("estimated_artifact_bytes_exceed_max_artifact_bytes")
    memory_estimate = _estimated_memory_budget(
        snapshots=snapshots,
        split_names=split_names,
        candidate_count=len(candidates),
        scenario_count=len(execution_scenarios),
        max_workers=int(resource_plan.effective_max_workers),
        execution_mode=manifest.research_run.execution.mode,
        plugin_complexity=plugin_complexity,
        resource_limits=manifest.research_run.resource_limits,
    )
    from .data_plane import build_data_plane_policy

    data_plane_policy = build_data_plane_policy(
        manifest_hash=manifest.manifest_hash(),
        dataset_hashes=dataset_hashes,
        split_names=split_names,
        memory_budget_mb=resource_plan.memory_budget_mb,
        estimated_total_memory_bytes=memory_estimate.get("estimated_total_memory_bytes"),
        effective_max_workers=resource_plan.effective_max_workers,
    ).as_dict()
    plan["data_plane_policy"] = data_plane_policy
    plan["workload_estimate"] = {
        "schema_version": 1,
        "candidate_count": plan["candidate_count"],
        "scenario_count": plan["scenario_count"],
        "split_count": plan["split_count"],
        "walk_forward_window_count": walk_forward_split_count // 2,
        "estimated_strategy_runs": plan["estimated_strategy_runs"],
        "available_parallel_work_tasks": plan["available_parallel_work_tasks"],
        "parallel_task_to_worker_ratio": plan["parallel_task_to_worker_ratio"],
        "expected_worker_utilization_pct": plan["expected_worker_utilization_pct"],
        "parallelism_limiting_factor": plan["parallelism_limiting_factor"],
        "effective_worker_source": plan["effective_worker_source"],
        "resource_plan": resource_plan_payload,
        "work_unit_selection": work_unit_selection,
        "data_plane_policy": data_plane_policy,
        "estimated_tick_events": plan["estimated_candle_evaluations"],
        "plugin_complexity": plugin_complexity,
        "estimated_plugin_runtime_us": estimated_plugin_runtime_us,
        "approx_snapshot_candle_count": plan["dataset_candles"],
        "audit_mode": manifest.research_run.audit_trail.mode,
        "report_detail": manifest.research_run.report_detail,
        "full_decisions_external_jsonl": manifest.research_run.artifact_policy.full_decisions_external_jsonl,
        "estimated_audit_stream_rows": estimated_audit_stream_rows,
        "estimated_artifact_write_count": estimated_artifact_write_count,
        "estimated_hash_payload_bytes": estimated_hash_payload_bytes,
        **canonical_estimate,
        "pre_parallel_work_unit_count": pre_parallel_work_unit_count,
        "pre_parallel_split_hash_count": pre_parallel_split_hash_count,
        "pre_parallel_dataset_hash_payload_bytes": pre_parallel_dataset_hash_payload_bytes,
        "pre_parallel_dataset_hash_call_count": pre_parallel_dataset_hash_call_count,
        "pre_parallel_parent_serial_estimate_status": "precomputed_split_hashes",
        "estimated_artifact_bytes": estimated_artifact_bytes,
        "estimated_artifact_detail_policy": (
            "summary_bounded_candidate_artifacts"
            if manifest.research_run.report_detail == "summary"
            else "full_candidate_artifacts"
        ),
        "max_artifact_bytes": max_artifact_bytes,
        "artifact_budget_status": artifact_budget_status,
        "artifact_budget_reasons": artifact_budget_reasons,
        **memory_estimate,
        "estimated_snapshot_hash_count": len(snapshots),
        "uses_production_evaluator": None,
        "uses_real_parallel_executor": None,
    }
    plan["execution_plan_hash"] = sha256_prefixed(_logical_plan_payload(plan))
    plan["plan_hash"] = plan["execution_plan_hash"]
    return ResearchExecutionPlan(plan)


def build_research_work_unit(
    *,
    manifest: ExperimentManifest,
    dataset_hashes: dict[str, str] | None = None,
    snapshots: dict[str, DatasetSnapshot] | None = None,
    params: dict[str, Any],
    candidate_index: int,
    scenario: ExecutionScenario,
    scenario_index: int,
    scenario_id: str,
    manifest_hash: str,
    simulation_seed_scope_hash: str | None = None,
    split_name: str = "candidate_scenario",
) -> ResearchWorkUnit:
    candidate = candidate_id(params, candidate_index)
    if dataset_hashes is None:
        if snapshots is None:
            raise ValueError("dataset_hashes_required")
        dataset_hashes = _compat_dataset_hashes_from_snapshot_metadata(snapshots)
    ordered_dataset_hashes = {name: dataset_hashes[name] for name in sorted(dataset_hashes)}
    seed_context = {
        "simulation_seed_scope_hash": simulation_seed_scope_hash or manifest_hash,
        "scenario_id": scenario_id,
        "candidate_id": candidate,
        "split_name": split_name,
        "work_unit_mode": manifest.research_run.execution.work_unit,
    }
    payload = {
        "candidate_index": candidate_index,
        "candidate_id": candidate,
        "scenario_index": scenario_index,
        "scenario_id": scenario_id,
        "split_name": split_name,
        "work_unit_mode": manifest.research_run.execution.work_unit,
        "parameter_values": params,
        "dataset_content_hash": sha256_prefixed(ordered_dataset_hashes),
        "portfolio_policy_hash": manifest.portfolio_policy_hash(),
        "simulation_policy_hash": manifest.simulation_policy_hash(),
        "execution_model_hash": sha256_prefixed(scenario.as_dict()),
        "execution_timing_hash": sha256_prefixed(manifest.execution_timing.as_dict()),
    }
    work_unit_hash = sha256_prefixed(payload)
    result_input_payload = {
        "work_unit_hash": work_unit_hash,
        "report_detail": manifest.research_run.report_detail,
        "resource_limits": manifest.research_run.resource_limits.as_dict(),
        "audit_trail": manifest.research_run.audit_trail.as_dict(),
        "artifact_policy": manifest.research_run.artifact_policy.as_dict(),
        "heartbeat": manifest.research_run.heartbeat.as_dict(),
    }
    return ResearchWorkUnit(
        candidate_index=candidate_index,
        candidate_id=candidate,
        scenario_index=scenario_index,
        scenario_id=scenario_id,
        split_name=split_name,
        work_unit_mode=manifest.research_run.execution.work_unit,
        parameter_values=dict(params),
        dataset_content_hash=str(payload["dataset_content_hash"]),
        portfolio_policy_hash=str(payload["portfolio_policy_hash"]),
        simulation_policy_hash=str(payload["simulation_policy_hash"]),
        execution_model_hash=str(payload["execution_model_hash"]),
        execution_timing_hash=str(payload["execution_timing_hash"]),
        seed_context=seed_context,
        work_unit_hash=work_unit_hash,
        work_result_input_hash=sha256_prefixed(result_input_payload),
    )


def build_run_environment(
    *,
    manifest: ExperimentManifest,
    db_path: str | Path,
    repository_version: str | None,
) -> dict[str, Any]:
    resolved_db_path = str(Path(db_path).expanduser().resolve())
    return {
        "repository_version": repository_version or "unknown",
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "system": platform.system(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "effective_max_workers": manifest.research_run.execution.max_workers,
        "execution_mode": manifest.research_run.execution.mode,
        "multiprocessing_policy": process_policy_observability(
            requested_start_method=manifest.research_run.execution.process_start_method,
            requested_max_workers=manifest.research_run.execution.max_workers,
        ),
        "work_unit_type": manifest.research_run.execution.work_unit,
        "db_path_fingerprint": sha256_prefixed({"db_path": resolved_db_path}),
        "manifest_hash": manifest.manifest_hash(),
    }


def precompute_dataset_hashes(snapshots: dict[str, DatasetSnapshot]) -> dict[str, str]:
    return {
        split_name: snapshot.content_hash()
        for split_name, snapshot in sorted(snapshots.items())
    }


def _compat_dataset_hashes_from_snapshot_metadata(snapshots: dict[str, DatasetSnapshot]) -> dict[str, str]:
    return {
        split_name: str(
            snapshot.source_content_hash
            or sha256_prefixed(
                {
                    "snapshot_id": snapshot.snapshot_id,
                    "source": snapshot.source,
                    "market": snapshot.market,
                    "interval": snapshot.interval,
                    "split_name": snapshot.split_name,
                    "date_range": snapshot.date_range.as_dict(),
                }
            )
        )
        for split_name, snapshot in sorted(snapshots.items())
    }


def _logical_plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in plan.items()
        if key
        not in {
            "created_at",
            "run_environment",
            "run_environment_hash",
            "execution_plan_hash",
            "plan_hash",
        }
    }


def _ordered_split_names(snapshots: dict[str, DatasetSnapshot]) -> list[str]:
    preferred = [name for name in ("train", "validation", "final_holdout") if name in snapshots]
    window_names: list[str] = []
    window_ids = sorted(
        {
            name.rsplit("_", 1)[0]
            for name in snapshots
            if name.startswith("window_") and (name.endswith("_train") or name.endswith("_test"))
        }
    )
    for window_id in window_ids:
        for suffix in ("train", "test"):
            split_name = f"{window_id}_{suffix}"
            if split_name in snapshots:
                window_names.append(split_name)
    remaining = sorted(name for name in snapshots if name not in set(preferred) | set(window_names))
    return preferred + window_names + remaining


def _estimated_audit_stream_rows(
    *,
    audit_mode: str,
    dataset_candles: int,
    candidate_count: int,
    scenario_count: int,
) -> int:
    if audit_mode != "complete_external":
        return 0
    return int(dataset_candles) * int(candidate_count) * int(scenario_count) * 3


def _estimated_artifact_write_count(
    *,
    audit_mode: str,
    full_decisions_external_jsonl: bool,
    work_unit_count: int,
    split_count: int,
) -> int:
    report_and_derived = 2
    candidate_event_stream = 1
    candidate_result_files = int(work_unit_count)
    audit_files = 0
    if audit_mode == "complete_external":
        audit_files = 1 + int(work_unit_count) * int(split_count) * 3
    decision_jsonl = int(work_unit_count) * int(split_count) if full_decisions_external_jsonl else 0
    return report_and_derived + candidate_event_stream + candidate_result_files + audit_files + decision_jsonl


def _estimated_hash_payload_bytes(
    *,
    dataset_candles: int,
    candidate_count: int,
    scenario_count: int,
    split_count: int,
) -> int:
    return (
        int(dataset_candles) * 128
        + int(candidate_count) * int(scenario_count) * int(split_count) * 512
        + 4096
    )


def estimate_canonical_observability_cost(
    *,
    estimated_tick_events: int,
    report_detail: str,
    diagnostic_mode: str = "promotion_candidate",
    audit_trail: Any | None = None,
    policy_materialization_mode: str = "research_exploratory",
) -> dict[str, Any]:
    policy = resolve_tick_observability_policy(
        report_detail=report_detail,
        diagnostic_mode=diagnostic_mode,
        audit_trail=audit_trail,
        policy_materialization_mode=policy_materialization_mode,
    )
    tick_events = int(estimated_tick_events)
    if policy.full_tick_canonical_enabled:
        calls_per_tick = 2
        payload_bytes_per_tick = 8192
        decision_payload_bytes = tick_events * 8192
    elif policy.name == "diagnostic_sampled":
        sampled = min(tick_events, int(policy.diagnostic_sample_limit))
        calls_per_tick = 0
        payload_bytes_per_tick = 768
        decision_payload_bytes = tick_events * 768 + sampled * 4096
    else:
        calls_per_tick = 0
        payload_bytes_per_tick = 512
        decision_payload_bytes = tick_events * 512
    estimated_calls = (
        tick_events * calls_per_tick
        if policy.full_tick_canonical_enabled
        else max(0, min(tick_events, int(policy.diagnostic_sample_limit)) if policy.name == "diagnostic_sampled" else 0)
    )
    estimated_payload_bytes = int(estimated_calls * payload_bytes_per_tick)
    return {
        "estimated_tick_canonical_hash_call_count": int(estimated_calls),
        "estimated_tick_canonical_hash_payload_bytes": estimated_payload_bytes,
        "estimated_decision_payload_bytes": int(decision_payload_bytes),
        "estimated_observability_mode": policy.name,
        "estimated_full_tick_canonical_enabled": bool(policy.full_tick_canonical_enabled),
    }


def _estimated_pre_parallel_dataset_hash_payload_bytes(
    *,
    snapshots: dict[str, DatasetSnapshot],
    split_names: list[str],
) -> int:
    candle_payload_bytes = 0
    for split_name in split_names:
        snapshot = snapshots[split_name]
        candle_payload_bytes += len(snapshot.candles) * 128
        candle_payload_bytes += len(snapshot.top_of_book_quotes) * 96
        candle_payload_bytes += len(snapshot.top_of_book_event_quotes) * 96
        candle_payload_bytes += len(snapshot.orderbook_depth_snapshots) * 256
    return int(candle_payload_bytes + len(split_names) * 2048)


def _estimated_artifact_bytes(
    *,
    candidate_count: int,
    scenario_count: int,
    split_count: int,
    audit_mode: str,
    estimated_audit_stream_rows: int,
    estimated_artifact_write_count: int,
    estimated_hash_payload_bytes: int,
    full_decisions_external_jsonl: bool,
    report_detail: str = "full",
) -> int:
    work_unit_count = int(candidate_count) * int(scenario_count)
    report_bytes = 64 * 1024
    per_candidate_scenario_bytes = 8 * 1024 if report_detail == "summary" else 64 * 1024
    candidate_json_bytes = max(1, work_unit_count) * per_candidate_scenario_bytes
    candidate_journal_bytes = max(1, work_unit_count) * 2 * 1024
    hash_payload_bytes = int(estimated_hash_payload_bytes)
    audit_bytes = 0
    if audit_mode == "complete_external":
        audit_bytes = int(estimated_audit_stream_rows) * 512 + int(estimated_artifact_write_count) * 1024
    decision_jsonl_bytes = (
        work_unit_count * int(split_count) * 8 * 1024
        if full_decisions_external_jsonl
        else 0
    )
    return int(report_bytes + candidate_json_bytes + candidate_journal_bytes + hash_payload_bytes + audit_bytes + decision_jsonl_bytes)


def _plugin_complexity_metadata(
    strategy_name: str,
    *,
    parameter_space: dict[str, Any] | None = None,
    report_detail: str = "summary",
    diagnostic_mode: str = "exploratory",
    audit_trail: Any | None = None,
    expected_candle_count: int | None = None,
) -> dict[str, Any]:
    try:
        from .strategy_registry import resolve_research_strategy_plugin

        plugin = resolve_research_strategy_plugin(strategy_name)
    except Exception:
        return {
            "schema_version": 1,
            "strategy_name": str(strategy_name),
            "complexity_class": "unknown",
            "expected_us_per_candle": None,
            "precompute_required": None,
        }
    estimator = getattr(plugin, "estimate_complexity", None)
    raw = getattr(plugin, "complexity_metadata", None)
    if callable(estimator):
        base = dict(raw) if isinstance(raw, dict) else {}
        estimated = estimator(
            strategy_name=plugin.name,
            parameter_space=parameter_space or {},
            report_detail=report_detail,
            diagnostic_mode=diagnostic_mode,
            audit_trail=audit_trail,
            expected_candle_count=expected_candle_count,
        )
        payload = {**base, **dict(estimated)}
        payload.setdefault("schema_version", 1)
        payload["strategy_name"] = plugin.name
        payload.setdefault("complexity_class", base.get("complexity_class", "unknown"))
        payload.setdefault("precompute_required", base.get("precompute_required"))
        return payload
    if not isinstance(raw, dict):
        return {
            "schema_version": 1,
            "strategy_name": plugin.name,
            "complexity_class": "unknown",
            "expected_us_per_candle": None,
            "precompute_required": None,
        }
    payload = dict(raw)
    payload.setdefault("schema_version", 1)
    payload["strategy_name"] = plugin.name
    payload.setdefault("complexity_class", "unknown")
    payload.setdefault("expected_us_per_candle", None)
    payload.setdefault("precompute_required", None)
    return payload


def _plugin_expected_us_per_candle(plugin_complexity: dict[str, Any]) -> int:
    value = plugin_complexity.get("expected_us_per_candle")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return int(value)


def _estimated_memory_budget(
    *,
    snapshots: dict[str, DatasetSnapshot],
    split_names: list[str],
    candidate_count: int,
    scenario_count: int,
    max_workers: int,
    execution_mode: str,
    plugin_complexity: dict[str, Any],
    resource_limits: Any,
) -> dict[str, Any]:
    split_candle_counts = {name: len(snapshots[name].candles) for name in split_names}
    max_split_candles = max(split_candle_counts.values(), default=0)
    total_candles = sum(split_candle_counts.values())
    snapshot_bytes = total_candles * 160
    effective_workers = max(1, int(max_workers) if str(execution_mode) == "parallel" else 1)
    event_bytes = max_split_candles * int(plugin_complexity.get("expected_decision_payload_bytes_per_event") or 384)
    tick_bytes = 0
    stage_trace_bytes = min(max_split_candles * 6, 128) * 512
    behavior_evidence_bytes = 8 * 1024
    parent_result_bytes = int(candidate_count) * int(scenario_count) * 4096
    parallel_fanout_bytes = snapshot_bytes * effective_workers
    estimated_total = (
        parallel_fanout_bytes
        + event_bytes
        + tick_bytes
        + stage_trace_bytes
        + behavior_evidence_bytes
        + parent_result_bytes
    )
    budget_mb = getattr(resource_limits, "max_total_memory_mb", None)
    if budget_mb is None:
        budget_mb = getattr(resource_limits, "max_rss_mb", None)
    budget_bytes = int(float(budget_mb) * 1024 * 1024) if budget_mb is not None else None
    safe_workers = effective_workers
    status = "NOT_EVALUATED"
    reasons: list[str] = []
    if budget_bytes is not None:
        non_worker_bytes = estimated_total - parallel_fanout_bytes
        per_worker = max(1, snapshot_bytes)
        safe_workers = max(1, int((budget_bytes - non_worker_bytes) // per_worker)) if budget_bytes > non_worker_bytes else 1
        safe_workers = min(effective_workers, safe_workers)
        status = "PASS" if estimated_total <= budget_bytes else "WARN"
        if estimated_total > budget_bytes:
            reasons.append("estimated_parent_and_worker_bytes_exceed_memory_budget")
    return {
        "estimated_snapshot_bytes_per_worker": snapshot_bytes,
        "estimated_parallel_snapshot_fanout_bytes": parallel_fanout_bytes,
        "estimated_event_materialization_bytes_per_split": event_bytes,
        "estimated_replay_tick_materialization_bytes_per_split": tick_bytes,
        "estimated_stage_trace_bytes": stage_trace_bytes,
        "estimated_behavior_evidence_bytes": behavior_evidence_bytes,
        "estimated_parent_result_bytes": parent_result_bytes,
        "estimated_total_memory_bytes": estimated_total,
        "max_in_flight_tasks": max(1, effective_workers * 2),
        "safe_max_workers_by_memory_budget": safe_workers,
        "memory_budget_status": status,
        "memory_budget_reasons": reasons,
    }
