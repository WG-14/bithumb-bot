from __future__ import annotations

from typing import Any

from .data_plane import split_names
from .dataset_snapshot import _expected_bucket_count, _interval_ms, _split_range
from .execution_plan import _plugin_complexity_metadata, _plugin_expected_us_per_candle
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
    plugin_complexity = _plugin_complexity_metadata(manifest.strategy_name)
    estimated_plugin_runtime_us = (
        expected_candles
        * candidate_count
        * scenario_count
        * _plugin_expected_us_per_candle(plugin_complexity)
    )
    pre_parallel_dataset_hash_payload_bytes = expected_candles * 128 + split_count * 2048
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
