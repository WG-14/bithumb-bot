from __future__ import annotations

import json

from bithumb_bot.research.execution_plan import build_research_execution_plan, parallel_efficiency_payload
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.resource_planner import ResourceContract, plan_research_resources
from bithumb_bot.research.report_writer import write_research_report
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _manager, _quality_report, _snapshot


def test_parallel_efficiency_warns_when_work_tasks_below_workers(tmp_path, monkeypatch) -> None:
    payload = parallel_efficiency_payload(
        available_work_tasks=1,
        requested_max_workers=8,
        effective_max_workers=8,
        work_unit="candidate_scenario",
    )

    assert payload["expected_worker_utilization_pct"] == 12.5
    assert payload["parallel_efficiency_warning_reasons"] == ["available_work_tasks_below_effective_workers"]
    assert payload["parallelism_limiting_factor"] == "work_unit_granularity_candidate_scenario"


def test_parallel_efficiency_has_no_warning_when_tasks_match_workers() -> None:
    payload = parallel_efficiency_payload(
        available_work_tasks=8,
        requested_max_workers=8,
        effective_max_workers=8,
        work_unit="candidate_scenario",
    )

    assert payload["expected_worker_utilization_pct"] == 100.0
    assert payload["parallel_efficiency_warning_reasons"] == []


def test_parallel_efficiency_is_persisted_in_report(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    report = {
        "experiment_id": "parallel_efficiency_report",
        "research_run": {"report_detail": "summary"},
        "candidates": [],
        "execution_observability": {
            "parallel_efficiency": parallel_efficiency_payload(
                available_work_tasks=1,
                requested_max_workers=8,
                effective_max_workers=8,
                work_unit="candidate_scenario",
            ),
        },
    }

    result = write_research_report(
        manager=manager,
        experiment_id="parallel_efficiency_report",
        report_name="backtest",
        payload=report,
    )
    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))

    payload = persisted["execution_observability"]["parallel_efficiency"]
    assert payload["available_work_tasks"] == 1
    assert payload["effective_max_workers"] == 8
    assert payload["expected_worker_utilization_pct"] == 12.5
    assert payload["parallelism_limiting_factor"] == "work_unit_granularity_candidate_scenario"


def test_execution_plan_parallel_efficiency_fields_are_available() -> None:
    manifest_payload = _manifest()
    manifest_payload["research_run"] = {
        "execution": {"mode": "parallel", "max_workers": 8, "work_unit": "candidate_scenario"}
    }
    manifest = parse_manifest(manifest_payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    plan = build_research_execution_plan(
        manifest=manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        db_path="/tmp/unit.sqlite",
        repository_version="test",
        created_at="2026-06-16T00:00:00+00:00",
    ).payload

    assert plan["available_parallel_work_tasks"] == 1
    assert plan["expected_worker_utilization_pct"] == 12.5


def test_work_unit_selector_prefers_split_when_candidate_scenario_tasks_below_workers() -> None:
    manifest = parse_manifest({
        **_manifest(),
        "research_run": {"execution": {"mode": "parallel", "max_workers": 3}},
    })
    plan = plan_research_resources(
        manifest=manifest,
        candidate_count=1,
        scenario_count=1,
        split_count=3,
        resource_contract=ResourceContract(
            cpu_limit=3,
            memory_limit_mb=4096,
            swap_limit_mb=None,
            detected_source="test",
            env_worker_cap=None,
            total_process_budget=None,
        ),
    )

    assert plan.work_unit_selection.effective_work_unit_type == "candidate_scenario_split"
    assert plan.work_unit_selection.candidate_scenario_task_count == 1
    assert plan.work_unit_selection.candidate_scenario_split_task_count == 3
    assert plan.work_unit_selection.selection_reason == "split_tasks_fill_effective_workers"


def test_work_unit_selector_keeps_candidate_scenario_when_tasks_match_workers() -> None:
    manifest_payload = _manifest()
    manifest_payload["parameter_space"]["SMA_SHORT"] = [2, 3, 4]
    manifest_payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 3}}
    manifest = parse_manifest(manifest_payload)
    plan = plan_research_resources(
        manifest=manifest,
        candidate_count=3,
        scenario_count=1,
        split_count=3,
        resource_contract=ResourceContract(
            cpu_limit=3,
            memory_limit_mb=4096,
            swap_limit_mb=None,
            detected_source="test",
            env_worker_cap=None,
            total_process_budget=None,
        ),
    )

    assert plan.work_unit_selection.effective_work_unit_type == "candidate_scenario"
    assert plan.work_unit_selection.rejected_alternatives
