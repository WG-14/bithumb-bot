from __future__ import annotations

from bithumb_bot.research import validation_protocol
from bithumb_bot.research.executor import ResearchWorkResult
from bithumb_bot.research.experiment_manifest import parse_manifest
from tests.factories.research_reports import minimal_candidate_base_result
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _manager, _quality_report, _snapshot


def test_parallel_backtest_emits_parent_serial_progress_events_before_pool_start(tmp_path, monkeypatch) -> None:
    payload = _manifest()
    payload["research_run"] = {
        "execution": {
            "mode": "parallel",
            "max_workers": 2,
            "process_start_method": "auto_safe",
        }
    }
    manifest = parse_manifest(payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation", "final_holdout")}
    quality_reports = {name: _quality_report(name) for name in snapshots}
    events: list[dict[str, object]] = []

    def fake_parallel_executor(**kwargs):
        sink = kwargs.get("runtime_observability_sink")
        if isinstance(sink, list):
            sink.append(
                {
                    "research_max_workers_requested": 2,
                    "research_max_workers_effective": 2,
                    "effective_process_start_method": "forkserver",
                }
            )
        results = []
        for task in kwargs["tasks"]:
            work_unit = task["work_unit"]
            results.append(
                ResearchWorkResult(
                    work_unit=work_unit,
                    work_unit_hash=work_unit.work_unit_hash,
                    candidate_index=work_unit.candidate_index,
                    candidate_id=work_unit.candidate_id,
                    scenario_index=work_unit.scenario_index,
                    scenario_id=work_unit.scenario_id,
                    status="completed",
                    base_result=minimal_candidate_base_result(
                        index=work_unit.candidate_index,
                        candidate_id=work_unit.candidate_id,
                        parameter_values=work_unit.parameter_values,
                        include_final_holdout=True,
                    ),
                    observability={"worker_pid": 1001},
                )
            )
        return results

    monkeypatch.setattr(validation_protocol, "execute_research_work_units_parallel", fake_parallel_executor)

    result = validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        progress_callback=events.append,
    )

    stages = [str(event.get("stage")) for event in events]
    assert stages.index("workload") < stages.index("build_work_tasks_start")
    assert stages.index("build_work_tasks_start") < stages.index("build_work_tasks_complete")
    assert stages.index("candidate_start_journal_append_complete") < stages.index("parallel_worker_pool_start")
    build_complete = events[stages.index("build_work_tasks_complete")]
    assert build_complete["work_task_count"] == 1
    assert "elapsed_s" in build_complete
    assert any(item["stage"] == "build_work_tasks" for item in result.substage_timings)
