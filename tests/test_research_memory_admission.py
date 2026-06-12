from __future__ import annotations

import pytest

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.validation_protocol import ResearchValidationError, run_research_backtest
from bithumb_bot.research.workload_estimate import build_manifest_workload_estimate
from tests.factories.research_reports import DeterministicResearchEvaluator
from tests.test_research_backtest_reproducibility import _create_db, _manifest, _research_manager


def _manifest_with_workers(
    max_workers: int,
    *,
    entry_modes: list[str] | None = None,
    max_total_memory_mb: float | None = None,
    memory_admission_policy: str = "fail_fast",
):
    payload = _manifest()
    payload["strategy_name"] = "channel_breakout_with_regime_filter"
    payload["parameter_space"] = {
        "CHANNEL_BREAKOUT_LOOKBACK": [3],
        "CHANNEL_BREAKOUT_RANGE_WINDOW": [3],
        "CHANNEL_BREAKOUT_VOLUME_WINDOW": [3],
        "ENTRY_MODE": entry_modes or ["immediate_breakout"],
    }
    payload["research_run"] = {
        "execution": {"mode": "parallel", "max_workers": max_workers},
        "resource_limits": {},
    }
    if max_total_memory_mb is not None:
        payload["research_run"]["resource_limits"]["max_total_memory_mb"] = max_total_memory_mb
    payload["research_run"]["resource_limits"]["memory_admission_policy"] = memory_admission_policy
    return parse_manifest(payload)


@pytest.mark.contract
@pytest.mark.resource_guard
def test_workload_estimate_includes_parallel_snapshot_fanout_bytes() -> None:
    one = build_manifest_workload_estimate(_manifest_with_workers(2))
    eight = build_manifest_workload_estimate(_manifest_with_workers(8))

    assert one["estimated_snapshot_bytes_per_worker"] > 0
    assert eight["estimated_parallel_snapshot_fanout_bytes"] > one[
        "estimated_parallel_snapshot_fanout_bytes"
    ]
    assert eight["max_in_flight_tasks"] == 16


@pytest.mark.contract
@pytest.mark.resource_guard
def test_memory_admission_fails_when_estimated_parent_and_worker_bytes_exceed_budget(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "research.sqlite"
    _create_db(db_path)
    manager = _research_manager(tmp_path, monkeypatch)
    manifest = _manifest_with_workers(8, max_total_memory_mb=1.0, memory_admission_policy="fail_fast")

    with pytest.raises(ResearchValidationError, match="memory_admission_budget_exceeded"):
        run_research_backtest(
            manifest=manifest,
            db_path=db_path,
            manager=manager,
            candidate_evaluator=DeterministicResearchEvaluator(),
        )


@pytest.mark.contract
@pytest.mark.resource_guard
def test_memory_admission_caps_effective_workers_when_policy_is_cap_workers(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "research.sqlite"
    _create_db(db_path)
    manager = _research_manager(tmp_path, monkeypatch)
    manifest = _manifest_with_workers(8, max_total_memory_mb=1.0, memory_admission_policy="cap_workers")

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        candidate_evaluator=DeterministicResearchEvaluator(),
    )

    memory_admission = report["execution_observability"]["memory_admission"]
    assert memory_admission["safe_max_workers_by_memory_budget"] >= 1
    assert memory_admission["safe_max_workers_by_memory_budget"] < 8
    assert report["execution_observability"]["research_max_workers_effective"] <= memory_admission[
        "safe_max_workers_by_memory_budget"
    ]
    assert memory_admission["max_in_flight_tasks"] <= memory_admission["safe_max_workers_by_memory_budget"] * 2


@pytest.mark.contract
@pytest.mark.resource_guard
def test_delayed_confirmation_parameter_space_increases_estimated_payload_bytes() -> None:
    immediate = build_manifest_workload_estimate(
        _manifest_with_workers(2, entry_modes=["immediate_breakout"])
    )
    delayed = build_manifest_workload_estimate(
        _manifest_with_workers(2, entry_modes=["immediate_breakout", "delayed_confirmation"])
    )

    assert delayed["estimated_event_materialization_bytes_per_split"] > immediate[
        "estimated_event_materialization_bytes_per_split"
    ]
