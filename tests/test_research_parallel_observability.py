from __future__ import annotations

import pytest

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.validation_protocol import _execution_observability_payload
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _snapshot

pytestmark = [pytest.mark.contract, pytest.mark.resource_guard, pytest.mark.parallel_e2e]


def test_report_includes_worker_pid_set_and_observed_worker_count() -> None:
    manifest = parse_manifest(_manifest())
    snapshots = {"train": _snapshot("train")}
    payload = _execution_observability_payload(
        manifest=manifest,
        stage_timings=[{"stage": "parallel_worker_execution", "wall_seconds": 1.5}],
        work_unit_observability=[
            {"worker_process_evidence": {"worker_pid": 222}},
            {"worker_process_evidence": {"worker_pid": 111}},
            {"worker_process_evidence": {"worker_pid": 222}},
        ],
        execution_boundary={
            "actual_worker_context_mode": "process_pool",
            "actual_parallel_task_count": 3,
            "parallel_executor_used": True,
            "research_max_workers_requested": 8,
            "research_max_workers_effective": 2,
            "effective_process_start_method": "forkserver",
        },
        snapshots=snapshots,
    )

    assert payload["parallel_executor_used"] is True
    assert payload["requested_max_workers"] == 8
    assert payload["research_max_workers_requested"] == 8
    assert payload["research_max_workers_effective"] == 2
    assert payload["effective_process_start_method"] == "forkserver"
    assert payload["worker_pid_set"] == [111, 222]
    assert payload["observed_worker_count"] == 2
    assert payload["parallel_worker_execution_wall_seconds"] == 1.5


def test_serial_report_records_parallel_executor_not_used() -> None:
    manifest = parse_manifest(_manifest())
    snapshots = {"train": _snapshot("train")}
    payload = _execution_observability_payload(
        manifest=manifest,
        stage_timings=[],
        work_unit_observability=[{"worker_process_evidence": {"worker_pid": None}}],
        execution_boundary={
            "actual_worker_context_mode": "in_process_contract",
            "actual_parallel_task_count": 0,
            "parallel_executor_used": False,
            "research_max_workers_requested": 1,
            "research_max_workers_effective": 1,
            "effective_process_start_method": None,
        },
        snapshots=snapshots,
    )

    assert payload["parallel_executor_used"] is False
    assert payload["worker_pid_set"] == []
    assert payload["observed_worker_count"] == 0


def test_report_flags_effective_workers_below_requested() -> None:
    manifest = parse_manifest(_manifest())
    payload = _execution_observability_payload(
        manifest=manifest,
        stage_timings=[],
        work_unit_observability=[
            {"worker_process_evidence": {"worker_pid": 111}, "wall_seconds": 1.0},
        ],
        execution_boundary={
            "actual_worker_context_mode": "process_pool",
            "actual_parallel_task_count": 3,
            "parallel_executor_used": True,
            "research_max_workers_requested": 8,
            "research_max_workers_effective": 1,
            "effective_process_start_method": "forkserver",
        },
        snapshots={"train": _snapshot("train")},
    )

    assert "effective_workers_below_requested" in payload["worker_budget_warning_reasons"]
    assert payload["requested_max_workers"] == 8
    assert payload["research_max_workers_effective"] == 1


def test_report_flags_parent_serial_dominance() -> None:
    manifest = parse_manifest(_manifest())
    payload = _execution_observability_payload(
        manifest=manifest,
        stage_timings=[
            {"stage": "pre_parallel_hash_materialization", "wall_seconds": 5.0},
            {"stage": "parallel_worker_execution", "wall_seconds": 1.0},
        ],
        work_unit_observability=[
            {"worker_process_evidence": {"worker_pid": 111}, "wall_seconds": 1.0},
            {"worker_process_evidence": {"worker_pid": 222}, "wall_seconds": 3.0},
        ],
        execution_boundary={
            "actual_worker_context_mode": "process_pool",
            "actual_parallel_task_count": 2,
            "parallel_executor_used": True,
            "research_max_workers_requested": 2,
            "research_max_workers_effective": 2,
            "effective_process_start_method": "forkserver",
        },
        snapshots={"train": _snapshot("train")},
    )

    assert "parent_serial_stage_dominates_wall_time" in payload["worker_observation_warning_reasons"]
    assert "parallel_tail_skew_detected" in payload["worker_observation_warning_reasons"]
    assert payload["work_unit_wall_seconds_distribution"]["count"] == 2
    assert payload["tail_skew_ratio"] == 3.0


def test_report_contains_parallel_efficiency_observability() -> None:
    manifest = parse_manifest(_manifest())
    payload = _execution_observability_payload(
        manifest=manifest,
        stage_timings=[{"stage": "parallel_worker_execution", "wall_seconds": 1.0}],
        work_unit_observability=[
            {"worker_process_evidence": {"worker_pid": 111}, "wall_seconds": 1.0},
        ],
        execution_boundary={
            "actual_worker_context_mode": "process_pool",
            "actual_parallel_task_count": 1,
            "available_parallel_work_tasks": 1,
            "parallel_executor_used": True,
            "research_max_workers_requested": 8,
            "research_max_workers_effective": 8,
            "effective_process_start_method": "forkserver",
        },
        snapshots={"train": _snapshot("train")},
    )

    efficiency = payload["parallel_efficiency"]
    assert efficiency["requested_max_workers"] == 8
    assert efficiency["effective_max_workers"] == 8
    assert efficiency["available_parallel_work_tasks"] == 1
    assert efficiency["observed_worker_count"] == 1
    assert efficiency["expected_worker_utilization_pct"] == 12.5


def test_observed_workers_below_effective_is_reported() -> None:
    manifest = parse_manifest(_manifest())
    payload = _execution_observability_payload(
        manifest=manifest,
        stage_timings=[{"stage": "parallel_worker_execution", "wall_seconds": 1.0}],
        work_unit_observability=[
            {"worker_process_evidence": {"worker_pid": 111}, "wall_seconds": 1.0},
        ],
        execution_boundary={
            "actual_worker_context_mode": "process_pool",
            "actual_parallel_task_count": 1,
            "available_parallel_work_tasks": 1,
            "parallel_executor_used": True,
            "research_max_workers_requested": 8,
            "research_max_workers_effective": 8,
            "effective_process_start_method": "forkserver",
        },
        snapshots={"train": _snapshot("train")},
    )

    assert "observed_workers_below_effective" in payload["worker_observation_warning_reasons"]
    assert "observed_workers_below_effective" in payload["parallel_efficiency"]["worker_observation_warning_reasons"]
    assert payload["parallel_efficiency"]["observed_worker_count"] == 1
