from __future__ import annotations

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.validation_protocol import _execution_observability_payload
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _snapshot


def test_parallel_report_records_observed_worker_pid_set() -> None:
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
