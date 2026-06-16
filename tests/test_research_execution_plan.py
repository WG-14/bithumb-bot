from __future__ import annotations

import inspect
from pathlib import Path

from bithumb_bot.paths import PathManager
from bithumb_bot.research.dataset_snapshot import Candle, DatasetQualityReport, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange, parse_manifest
from bithumb_bot.research import execution_plan, validation_protocol
from tests.factories.research_reports import DeterministicResearchEvaluator
from tests.test_research_backtest_reproducibility import _manifest


def _snapshot(split_name: str) -> DatasetSnapshot:
    return DatasetSnapshot(
        snapshot_id=f"snapshot_{split_name}",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name=split_name,
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=(
            Candle(ts=1_700_000_000_000, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
            Candle(ts=1_700_000_060_000, open=101.0, high=102.0, low=100.0, close=101.0, volume=1.0),
        ),
    )


def _quality_report(split_name: str) -> DatasetQualityReport:
    return DatasetQualityReport(
        {
            "split_name": split_name,
            "content_hash": f"sha256:quality-{split_name}",
            "quality_gate_status": "PASS",
            "quality_gate_reasons": [],
        }
    )


def _manager(tmp_path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def test_build_research_work_units_reuse_precomputed_dataset_hashes(tmp_path, monkeypatch) -> None:
    payload = _manifest()
    payload["parameter_space"]["SMA_SHORT"] = [2, 3, 4, 5, 6]
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": [0.0, 1.0],
        "latency_ms": 0,
        "partial_fill_rate": 0.0,
        "order_failure_rate": 0.0,
        "market_order_extra_cost_bps": 0.0,
    }
    manifest = parse_manifest(payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation", "final_holdout")}
    quality_reports = {name: _quality_report(name) for name in snapshots}
    call_count = 0
    original = DatasetSnapshot.content_hash

    def counted(self):
        nonlocal call_count
        call_count += 1
        return original(self)

    monkeypatch.setattr(DatasetSnapshot, "content_hash", counted)

    result = validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        progress_callback=lambda event: None,
        candidate_evaluator=DeterministicResearchEvaluator(),
    )

    assert len(result.candidates) == 5
    assert call_count <= len(snapshots)


def test_build_research_work_unit_does_not_call_snapshot_content_hash() -> None:
    source = inspect.getsource(execution_plan.build_research_work_unit)

    assert ".content_hash(" not in source


def _plan_for_payload(payload: dict[str, object], split_names=("train", "validation")) -> dict[str, object]:
    manifest = parse_manifest(payload)
    snapshots = {name: _snapshot(name) for name in split_names}
    quality_reports = {name: _quality_report(name) for name in snapshots}
    return execution_plan.build_research_execution_plan(
        manifest=manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        db_path="/tmp/unit.sqlite",
        repository_version="test",
        created_at="2026-06-16T00:00:00+00:00",
    ).payload


def test_execution_plan_distinguishes_strategy_runs_from_parallel_work_tasks() -> None:
    payload = _manifest()
    payload.pop("research_run", None)
    payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 8}}

    plan = _plan_for_payload(payload, split_names=("train", "validation"))

    assert plan["estimated_strategy_runs"] == 2
    assert plan["available_parallel_work_tasks"] == 1


def test_execution_plan_reports_low_worker_utilization_for_single_candidate() -> None:
    payload = _manifest()
    payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 8}}

    plan = _plan_for_payload(payload, split_names=("train", "validation"))

    assert plan["expected_worker_utilization_pct"] == 12.5
    assert plan["parallelism_limiting_factor"] == "work_unit_granularity_candidate_scenario"
    assert plan["resource_plan"]["schema_version"] == 1
    assert plan["resource_plan"]["effective_max_workers"] >= 1
    assert plan["resource_plan"]["selection_reasons"]
    assert plan["work_unit_selection"]["candidate_scenario_task_count"] == 1
    assert plan["work_unit_selection"]["candidate_scenario_split_task_count"] == 2
    assert plan["work_unit_selection"]["selection_reason"]


def test_execution_plan_reports_full_worker_utilization_for_eight_candidates() -> None:
    payload = _manifest()
    payload["parameter_space"]["SMA_SHORT"] = [2, 3, 4, 5, 6, 7, 8, 9]
    payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 8}}

    plan = _plan_for_payload(payload, split_names=("train", "validation"))

    assert plan["available_parallel_work_tasks"] == 8
    assert plan["expected_worker_utilization_pct"] == 100.0


def test_split_work_unit_reports_parallel_tasks_per_split() -> None:
    payload = _manifest()
    payload["research_run"] = {
        "execution": {
            "mode": "parallel",
            "max_workers": 8,
            "work_unit": "candidate_scenario_split",
        }
    }

    plan = _plan_for_payload(payload, split_names=("train", "validation"))

    assert plan["estimated_strategy_runs"] == 2
    assert plan["available_parallel_work_tasks"] == 2


def test_execution_plan_contains_resource_and_work_unit_contracts() -> None:
    payload = _manifest()
    payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 3}}

    plan = _plan_for_payload(payload, split_names=("train", "validation", "final_holdout"))

    assert plan["resource_plan"]["schema_version"] == 1
    assert "effective_max_workers" in plan["resource_plan"]
    assert plan["resource_plan"]["selection_reasons"]
    assert plan["work_unit_selection"]["effective_work_unit_type"] in {
        "candidate_scenario",
        "candidate_scenario_split",
    }
    assert plan["data_plane_policy"]["schema_version"] == 1
    assert plan["workload_estimate"]["resource_plan"]["schema_version"] == 1
