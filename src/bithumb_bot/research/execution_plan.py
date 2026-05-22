from __future__ import annotations

import os
import platform
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dataset_snapshot import DatasetQualityReport, DatasetSnapshot, combined_dataset_quality_hash
from .experiment_manifest import ExecutionScenario, ExperimentManifest
from .hashing import sha256_prefixed
from .parameter_space import candidate_id, iter_parameter_candidates


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
    split_names = _ordered_split_names(snapshots)
    split_count = len(split_names)
    walk_forward_split_count = sum(1 for split_name in split_names if split_name.startswith("window_"))
    strategy_run_count = len(candidates) * len(manifest.execution_model.scenarios) * split_count
    if include_walk_forward:
        strategy_run_count = len(candidates) * len(manifest.execution_model.scenarios) * (
            max(0, split_count - walk_forward_split_count) + walk_forward_split_count
        )
    dataset_candles = sum(len(snapshot.candles) for snapshot in snapshots.values())
    run_environment = build_run_environment(
        manifest=manifest,
        db_path=db_path,
        repository_version=repository_version,
    )
    plan = {
        "schema_version": 1,
        "manifest_hash": manifest.manifest_hash(),
        "experiment_id": manifest.experiment_id,
        "dataset_hashes": {name: snapshots[name].content_hash() for name in split_names},
        "dataset_quality_hash": combined_dataset_quality_hash(tuple(quality_reports.values())),
        "candidate_count": len(candidates),
        "scenario_count": len(manifest.execution_model.scenarios),
        "split_count": split_count,
        "split_names": split_names,
        "estimated_strategy_runs": strategy_run_count,
        "dataset_candles": dataset_candles,
        "estimated_candles": dataset_candles,
        "estimated_candle_evaluations": (
            dataset_candles
            * len(candidates)
            * len(manifest.execution_model.scenarios)
        ),
        "execution_mode": manifest.research_run.execution.mode,
        "max_workers": manifest.research_run.execution.max_workers,
        "work_unit_type": manifest.research_run.execution.work_unit,
        "deterministic_merge_order": manifest.research_run.execution.deterministic_merge_order,
        "resume_enabled": manifest.research_run.execution.resume,
        "created_at": created_at,
        "run_environment": run_environment,
        "run_environment_hash": sha256_prefixed(run_environment),
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
    split_name: str = "candidate_scenario",
) -> ResearchWorkUnit:
    candidate = candidate_id(params, candidate_index)
    dataset_hashes = {name: snapshot.content_hash() for name, snapshot in sorted(snapshots.items())}
    seed_context = {
        "manifest_hash": manifest_hash,
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
        "seed_context": seed_context,
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
        work_unit_hash=sha256_prefixed(payload),
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
