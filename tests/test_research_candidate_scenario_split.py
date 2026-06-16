from __future__ import annotations

import pytest

from bithumb_bot.research import validation_protocol
from bithumb_bot.research.executor import ResearchWorkResult
from bithumb_bot.research.experiment_manifest import parse_manifest
from tests.factories.research_reports import minimal_candidate_base_result
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _manager, _quality_report, _snapshot


class _SplitEvaluator:
    def evaluate(self, work_unit, context):
        full = minimal_candidate_base_result(
            index=work_unit.candidate_index,
            candidate_id=work_unit.candidate_id,
            parameter_values=work_unit.parameter_values,
            include_final_holdout=False,
        )
        split_name = work_unit.split_name
        if split_name == "candidate_scenario":
            return ResearchWorkResult(
                work_unit=work_unit,
                work_unit_hash=work_unit.work_unit_hash,
                candidate_index=work_unit.candidate_index,
                candidate_id=work_unit.candidate_id,
                scenario_index=work_unit.scenario_index,
                scenario_id=work_unit.scenario_id,
                status="completed",
                base_result=full,
                observability={"worker_pid": None, "wall_seconds": 0.2, "candles_processed": 4},
            )
        partial = {
            "index": full["index"],
            "candidate_id": full["candidate_id"],
            "candidate_failed": False,
            "candidate_failed_before_complete_metrics": False,
            "evaluation_status": "completed",
            "metrics_status": "partial_split",
            "metrics_v2_source": "contract_factory",
            "parameter_values": full["parameter_values"],
            "walk_forward_metrics": None,
            "warnings": [],
            "retained_detail_summary": full["retained_detail_summary"],
            "work_unit_portfolio_policy_hash": work_unit.portfolio_policy_hash,
        }
        for key, value in full.items():
            if key.startswith(f"{split_name}_"):
                partial[key] = value
        return ResearchWorkResult(
            work_unit=work_unit,
            work_unit_hash=work_unit.work_unit_hash,
            candidate_index=work_unit.candidate_index,
            candidate_id=work_unit.candidate_id,
            scenario_index=work_unit.scenario_index,
            scenario_id=work_unit.scenario_id,
            status="completed",
            base_result=partial,
            observability={"worker_pid": None, "wall_seconds": 0.1, "candles_processed": 2},
        )


def _split_manifest():
    payload = _manifest()
    payload["dataset"].pop("final_holdout", None)
    payload["research_run"] = {"execution": {"work_unit": "candidate_scenario_split"}}
    return parse_manifest(payload)


def test_candidate_scenario_split_builds_one_task_per_split(tmp_path, monkeypatch) -> None:
    manifest = _split_manifest()
    snapshots = {name: _snapshot(name) for name in ("train", "validation")}
    quality_reports = {name: _quality_report(name) for name in snapshots}
    events: list[dict[str, object]] = []

    validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        progress_callback=events.append,
        candidate_evaluator=_SplitEvaluator(),
    )

    build_complete = next(event for event in events if event.get("stage") == "build_work_tasks_complete")
    assert build_complete["work_task_count"] == 2


def test_candidate_scenario_split_merges_train_and_validation_results(tmp_path, monkeypatch) -> None:
    manifest = _split_manifest()
    snapshots = {name: _snapshot(name) for name in ("train", "validation")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    result = validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        candidate_evaluator=_SplitEvaluator(),
    )

    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate["train_metrics"]
    assert candidate["validation_metrics"]
    assert candidate["primary_validation_metrics"]["trade_count"] == 4


def test_candidate_scenario_split_selection_preserves_deterministic_merge_order(tmp_path, monkeypatch) -> None:
    payload = _manifest()
    payload["dataset"].pop("final_holdout", None)
    payload["parameter_space"]["SMA_SHORT"] = [3, 2]
    payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 2}}
    plan_manifest = parse_manifest(payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation")}
    quality_reports = {name: _quality_report(name) for name in snapshots}
    plan = validation_protocol.build_research_execution_plan(
        manifest=plan_manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        db_path="/tmp/unit.sqlite",
        repository_version="test",
        created_at="2026-06-17T00:00:00+00:00",
    )
    manifest = parse_manifest({**payload, "research_run": {"execution": {"work_unit": "candidate_scenario_split"}}})

    result = validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        candidate_evaluator=_SplitEvaluator(),
    )

    repeat = validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path / "repeat", monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        candidate_evaluator=_SplitEvaluator(),
    )
    assert [candidate["parameter_candidate_id"] for candidate in result.candidates] == [
        candidate["parameter_candidate_id"] for candidate in repeat.candidates
    ]
    assert plan.payload["work_unit_selection"]["selection_reason"]


def test_candidate_scenario_split_matches_candidate_scenario_metrics(tmp_path, monkeypatch) -> None:
    split_manifest = _split_manifest()
    base_payload = _manifest()
    base_payload["dataset"].pop("final_holdout", None)
    base_manifest = parse_manifest(base_payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    split_result = validation_protocol._evaluate_candidates(
        manifest=split_manifest,
        manager=_manager(tmp_path / "split", monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        candidate_evaluator=_SplitEvaluator(),
    ).candidates[0]
    base_result = validation_protocol._evaluate_candidates(
        manifest=base_manifest,
        manager=_manager(tmp_path / "base", monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        candidate_evaluator=_SplitEvaluator(),
    ).candidates[0]

    assert split_result["validation_metrics"]["return_pct"] == base_result["validation_metrics"]["return_pct"]
    assert split_result["validation_metrics"]["trade_count"] == base_result["validation_metrics"]["trade_count"]


def test_candidate_scenario_split_is_fail_closed_for_unsupported_walk_forward(tmp_path, monkeypatch) -> None:
    manifest = _split_manifest()
    snapshots = {
        "train": _snapshot("train"),
        "validation": _snapshot("validation"),
        "window_001_train": _snapshot("window_001_train"),
        "window_001_test": _snapshot("window_001_test"),
    }
    quality_reports = {name: _quality_report(name) for name in snapshots}

    with pytest.raises(validation_protocol.ResearchValidationError, match="walk_forward_not_supported"):
        validation_protocol._evaluate_candidates(
            manifest=manifest,
            manager=_manager(tmp_path, monkeypatch),
            snapshots=snapshots,
            quality_reports=quality_reports,
            include_walk_forward=True,
            execution_calibration=None,
            candidate_evaluator=_SplitEvaluator(),
        )
