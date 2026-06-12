from __future__ import annotations

from typing import Any

from .data_plane import split_names
from .dataset_snapshot import _expected_bucket_count, _interval_ms, _split_range
from .execution_plan import (
    _plugin_complexity_metadata,
    _plugin_expected_us_per_candle,
    estimate_canonical_observability_cost,
)
from .experiment_manifest import ExperimentManifest, load_manifest, required_execution_scenarios
from .parameter_space import iter_parameter_candidates


def build_manifest_workload_estimate(manifest: ExperimentManifest) -> dict[str, Any]:
    candidates = iter_parameter_candidates(manifest.parameter_space)
    scenarios = required_execution_scenarios(manifest.execution_model.scenarios)
    split_ranges = _dataset_split_ranges(manifest)
    split_count = len(split_ranges)
    candidate_count = len(candidates)
    scenario_count = len(scenarios)
    work_unit_count = candidate_count * scenario_count
    expected_candles = sum(int(item["expected_candle_count"]) for item in split_ranges)
    plugin_complexity = _plugin_complexity_metadata(
        manifest.strategy_name,
        parameter_space=manifest.parameter_space,
        report_detail=manifest.research_run.report_detail,
        diagnostic_mode=manifest.research_run.diagnostic_mode,
        audit_trail=manifest.research_run.audit_trail,
        expected_candle_count=expected_candles,
    )
    estimated_plugin_runtime_us = (
        expected_candles
        * candidate_count
        * scenario_count
        * _plugin_expected_us_per_candle(plugin_complexity)
    )
    pre_parallel_dataset_hash_payload_bytes = expected_candles * 128 + split_count * 2048
    max_workers = int(manifest.research_run.execution.max_workers)
    snapshot_bytes_per_worker = expected_candles * 160
    parallel_snapshot_fanout_bytes = snapshot_bytes_per_worker * max(1, max_workers)
    event_bytes = max((int(item["expected_candle_count"]) for item in split_ranges), default=0) * int(
        plugin_complexity.get("expected_decision_payload_bytes_per_event") or 384
    )
    stage_trace_bytes = min(expected_candles * 6, 128) * 512
    parent_result_bytes = candidate_count * scenario_count * 4096
    memory_budget = manifest.research_run.resource_limits.max_total_memory_mb or manifest.research_run.resource_limits.max_rss_mb
    estimated_total_memory_bytes = (
        parallel_snapshot_fanout_bytes + event_bytes + stage_trace_bytes + 8192 + parent_result_bytes
    )
    memory_budget_bytes = int(float(memory_budget) * 1024 * 1024) if memory_budget is not None else None
    memory_budget_reasons = (
        ["estimated_parent_and_worker_bytes_exceed_memory_budget"]
        if memory_budget_bytes is not None and estimated_total_memory_bytes > memory_budget_bytes
        else []
    )
    canonical_estimate = estimate_canonical_observability_cost(
        estimated_tick_events=expected_candles * candidate_count * scenario_count,
        report_detail=manifest.research_run.report_detail,
        diagnostic_mode=manifest.research_run.diagnostic_mode,
        audit_trail=manifest.research_run.audit_trail,
    )
    return {
        "schema_version": 1,
        "manifest_hash": manifest.manifest_hash(),
        "experiment_id": manifest.experiment_id,
        "strategy_name": manifest.strategy_name,
        "candidate_count": candidate_count,
        "scenario_count": scenario_count,
        "split_count": split_count,
        "work_unit_count": work_unit_count,
        "dataset_split_ranges": split_ranges,
        "research_execution_mode": manifest.research_run.execution.mode,
        "max_workers_requested": manifest.research_run.execution.max_workers,
        "process_start_method": manifest.research_run.execution.process_start_method,
        "pre_parallel_work_unit_count": work_unit_count,
        "pre_parallel_split_hash_count": split_count,
        "pre_parallel_dataset_hash_call_count": split_count,
        "pre_parallel_dataset_hash_payload_bytes": pre_parallel_dataset_hash_payload_bytes,
        "pre_parallel_parent_serial_estimate_status": "manifest_declared_ranges_no_snapshot_load",
        "estimated_plugin_runtime_us": estimated_plugin_runtime_us,
        "plugin_complexity": plugin_complexity,
        "estimated_snapshot_bytes_per_worker": snapshot_bytes_per_worker,
        "estimated_parallel_snapshot_fanout_bytes": parallel_snapshot_fanout_bytes,
        "estimated_event_materialization_bytes_per_split": event_bytes,
        "estimated_replay_tick_materialization_bytes_per_split": 0,
        "estimated_stage_trace_bytes": stage_trace_bytes,
        "estimated_behavior_evidence_bytes": 8192,
        "estimated_parent_result_bytes": parent_result_bytes,
        "max_in_flight_tasks": max(1, max_workers * 2),
        "safe_max_workers_by_memory_budget": max(1, max_workers if not memory_budget_reasons else min(max_workers, 1)),
        "memory_budget_status": "WARN" if memory_budget_reasons else ("PASS" if memory_budget_bytes is not None else "NOT_EVALUATED"),
        "memory_budget_reasons": memory_budget_reasons,
        "memory_admission_policy": manifest.research_run.resource_limits.memory_admission_policy,
        **canonical_estimate,
        "budget_status": "NOT_EVALUATED",
        "budget_reasons": [],
    }


def build_manifest_workload_estimate_from_path(manifest_path: str) -> dict[str, Any]:
    return build_manifest_workload_estimate(load_manifest(manifest_path))


def _dataset_split_ranges(manifest: ExperimentManifest) -> list[dict[str, Any]]:
    interval_ms = _interval_ms(manifest.interval)
    ranges: list[dict[str, Any]] = []
    for split_name in split_names(manifest):
        date_range = _split_range(manifest, split_name)
        start_ts = date_range.start_ts_ms()
        end_ts = date_range.end_ts_ms()
        ranges.append(
            {
                "split_name": split_name,
                "start": date_range.start,
                "end": date_range.end,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "expected_candle_count": _expected_bucket_count(
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval_ms=interval_ms,
                ),
            }
        )
    return ranges
