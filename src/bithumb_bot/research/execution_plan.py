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


@dataclass(frozen=True)
class ResearchWorkUnit:
    candidate_index: int
    candidate_id: str
    scenario_index: int
    scenario_id: str
    split_name: str
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
    split_count = len(split_names)
    walk_forward_split_count = sum(1 for split_name in split_names if split_name.startswith("window_"))
    strategy_run_count = len(candidates) * len(execution_scenarios) * split_count
    if include_walk_forward:
        strategy_run_count = len(candidates) * len(execution_scenarios) * (
            max(0, split_count - walk_forward_split_count) + walk_forward_split_count
        )
    dataset_candles = sum(len(snapshot.candles) for snapshot in snapshots.values())
    plugin_complexity = _plugin_complexity_metadata(manifest.strategy_name)
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
    plan = {
        "schema_version": 1,
        "manifest_hash": manifest.manifest_hash(),
        "simulation_seed_scope_hash": manifest.simulation_seed_scope_hash(),
        "experiment_id": manifest.experiment_id,
        "dataset_hashes": {name: snapshots[name].content_hash() for name in split_names},
        "dataset_quality_hash": combined_dataset_quality_hash(tuple(quality_reports.values())),
        "candidate_count": len(candidates),
        "scenario_count": len(execution_scenarios),
        "split_count": split_count,
        "split_names": split_names,
        "estimated_strategy_runs": strategy_run_count,
        "dataset_candles": dataset_candles,
        "estimated_candles": dataset_candles,
        "estimated_candle_evaluations": (
            dataset_candles
            * len(candidates)
            * len(execution_scenarios)
        ),
        "plugin_complexity": plugin_complexity,
        "estimated_plugin_runtime_us": estimated_plugin_runtime_us,
        "execution_mode": manifest.research_run.execution.mode,
        "max_workers": manifest.research_run.execution.max_workers,
        "process_start_method": manifest.research_run.execution.process_start_method,
        "work_unit_type": manifest.research_run.execution.work_unit,
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
    plan["workload_estimate"] = {
        "schema_version": 1,
        "candidate_count": plan["candidate_count"],
        "scenario_count": plan["scenario_count"],
        "split_count": plan["split_count"],
        "walk_forward_window_count": walk_forward_split_count // 2,
        "estimated_strategy_runs": plan["estimated_strategy_runs"],
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
        "estimated_artifact_bytes": estimated_artifact_bytes,
        "estimated_artifact_detail_policy": (
            "summary_bounded_candidate_artifacts"
            if manifest.research_run.report_detail == "summary"
            else "full_candidate_artifacts"
        ),
        "max_artifact_bytes": max_artifact_bytes,
        "artifact_budget_status": artifact_budget_status,
        "artifact_budget_reasons": artifact_budget_reasons,
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
    snapshots: dict[str, DatasetSnapshot],
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
    dataset_hashes = {name: snapshot.content_hash() for name, snapshot in sorted(snapshots.items())}
    seed_context = {
        "simulation_seed_scope_hash": simulation_seed_scope_hash or manifest_hash,
        "scenario_id": scenario_id,
        "candidate_id": candidate,
        "split_name": split_name,
    }
    payload = {
        "candidate_index": candidate_index,
        "candidate_id": candidate,
        "scenario_index": scenario_index,
        "scenario_id": scenario_id,
        "split_name": split_name,
        "parameter_values": params,
        "dataset_content_hash": sha256_prefixed(dataset_hashes),
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


def _plugin_complexity_metadata(strategy_name: str) -> dict[str, Any]:
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
    raw = getattr(plugin, "complexity_metadata", None)
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
