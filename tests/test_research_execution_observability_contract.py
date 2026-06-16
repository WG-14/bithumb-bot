from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.report_writer import write_research_report
from bithumb_bot.research.validation_protocol import _execution_observability_payload
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _manager, _snapshot


REQUIRED_FIELDS = {
    "schema_version",
    "parallel_executor_used",
    "requested_max_workers",
    "research_max_workers_effective",
    "available_parallel_work_tasks",
    "observed_worker_count",
    "worker_pid_set",
    "worker_budget_warning_reasons",
    "worker_observation_warning_reasons",
    "parent_serial_stage_timings",
    "parallel_worker_execution_wall_seconds",
    "tail_skew_ratio",
    "parallel_efficiency",
    "memory_admission",
    "resource_plan",
}


def _payload(parallel: bool) -> dict[str, object]:
    payload = _execution_observability_payload(
        manifest=parse_manifest(_manifest()),
        stage_timings=[
            {"stage": "pre_parallel_hash_materialization", "wall_seconds": 0.1},
            {"stage": "parallel_worker_execution", "wall_seconds": 0.2},
        ],
        work_unit_observability=[{"worker_pid": 123, "wall_seconds": 0.2}] if parallel else [],
        execution_boundary={
            "actual_worker_context_mode": "test",
            "actual_parallel_task_count": 1 if parallel else 0,
            "available_parallel_work_tasks": 1,
            "parallel_executor_used": parallel,
            "research_max_workers_requested": 2,
            "research_max_workers_effective": 2 if parallel else 1,
            "resource_plan": {"schema_version": 1, "effective_max_workers": 2 if parallel else 1},
        },
        snapshots={"train": _snapshot("train")},
    )
    payload["memory_admission"] = {"effective_max_workers": 2 if parallel else 1}
    return payload


def test_execution_observability_contains_required_contract_fields() -> None:
    payload = _payload(True)

    assert REQUIRED_FIELDS.issubset(payload)
    assert isinstance(payload["worker_observation_warning_reasons"], list)


def test_parallel_report_records_observed_worker_count() -> None:
    payload = _payload(True)

    assert payload["parallel_executor_used"] is True
    assert payload["observed_worker_count"] == 1
    assert payload["worker_pid_set"] == [123]


def test_serial_report_records_parallel_executor_false() -> None:
    payload = _payload(False)

    assert payload["parallel_executor_used"] is False
    assert payload["observed_worker_count"] == 0


def test_report_writer_does_not_drop_execution_observability_contract(tmp_path: Path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    report = {
        "experiment_id": "observability_contract",
        "research_run": {"report_detail": "summary"},
        "candidates": [],
        "execution_observability": _payload(True),
    }
    result = write_research_report(
        manager=manager,
        experiment_id="observability_contract",
        report_name="backtest",
        payload=report,
    )
    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))

    observed = persisted["execution_observability"]
    assert REQUIRED_FIELDS.issubset(observed)
    assert observed["parallel_efficiency"]["expected_worker_utilization_pct"] == 50.0
