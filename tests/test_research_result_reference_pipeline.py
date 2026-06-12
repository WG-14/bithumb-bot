from __future__ import annotations

import json

import pytest

from bithumb_bot.research.execution_plan import ResearchWorkUnit
from bithumb_bot.research.executor import ResearchWorkResult
from bithumb_bot.research.report_writer import summarize_candidate_result
from bithumb_bot.research.validation_protocol import _compact_work_result_with_detail_artifact
from tests.test_research_backtest_reproducibility import _research_manager
from tests.test_research_memory_admission import _manifest_with_workers


def _work_unit() -> ResearchWorkUnit:
    return ResearchWorkUnit(
        candidate_index=0,
        candidate_id="candidate_000",
        scenario_index=0,
        scenario_id="scenario_000",
        split_name="candidate_scenario",
        parameter_values={"CHANNEL_BREAKOUT_LOOKBACK": 3},
        dataset_content_hash="sha256:dataset",
        portfolio_policy_hash="sha256:portfolio",
        simulation_policy_hash="sha256:simulation",
        execution_model_hash="sha256:execution",
        execution_timing_hash="sha256:timing",
        seed_context={"candidate_id": "candidate_000"},
        work_unit_hash="sha256:work",
        work_result_input_hash="sha256:input",
    )


def _result(base_result: dict[str, object]) -> ResearchWorkResult:
    work_unit = _work_unit()
    return ResearchWorkResult(
        work_unit=work_unit,
        work_unit_hash=work_unit.work_unit_hash,
        candidate_index=0,
        candidate_id="candidate_000",
        scenario_index=0,
        scenario_id="scenario_000",
        status="completed",
        base_result=base_result,
    )


def _base_result() -> dict[str, object]:
    return {
        "index": 0,
        "candidate_id": "candidate_000",
        "parameter_values": {"CHANNEL_BREAKOUT_LOOKBACK": 3},
        "train_metrics": {},
        "validation_metrics": {},
        "train_metrics_v2": {},
        "validation_metrics_v2": {},
        "walk_forward_metrics": None,
        "validation_closed_trades": [{"entry_ts": 1, "exit_ts": 2}],
        "train_equity_curve": [{"ts": 1, "equity": 100.0}],
        "validation_equity_curve": [{"ts": 2, "equity": 101.0}],
    }


@pytest.mark.contract
@pytest.mark.resource_guard
def test_parent_aggregation_uses_compact_work_result_summary(tmp_path, monkeypatch) -> None:
    manager = _research_manager(tmp_path, monkeypatch)
    compact = _compact_work_result_with_detail_artifact(
        manager=manager,
        manifest=_manifest_with_workers(2),
        result=_result(_base_result()),
        artifact_context=None,
    )

    assert compact.base_result is not None
    assert "validation_closed_trades" not in compact.base_result
    assert "train_equity_curve" not in compact.base_result
    assert compact.base_result["validation_closed_trade_count"] == 1
    assert "sha256:" in str(compact.base_result["detail_artifact_hash"])


@pytest.mark.contract
@pytest.mark.resource_guard
def test_candidate_detail_artifact_is_written_before_parent_profile_aggregation(tmp_path, monkeypatch) -> None:
    manager = _research_manager(tmp_path, monkeypatch)
    compact = _compact_work_result_with_detail_artifact(
        manager=manager,
        manifest=_manifest_with_workers(2),
        result=_result(_base_result()),
        artifact_context=None,
    )

    detail_path = compact.base_result["detail_artifact_path"]  # type: ignore[index]
    payload = json.loads(open(detail_path, encoding="utf-8").read())
    assert payload["artifact_type"] == "candidate_detail_result"
    assert payload["detail_artifact_hash"] == compact.base_result["detail_artifact_hash"]  # type: ignore[index]
    assert payload["base_result"]["validation_closed_trades"]


@pytest.mark.contract
@pytest.mark.resource_guard
def test_summary_report_does_not_require_full_base_result_in_memory(tmp_path, monkeypatch) -> None:
    manager = _research_manager(tmp_path, monkeypatch)
    compact = _compact_work_result_with_detail_artifact(
        manager=manager,
        manifest=_manifest_with_workers(2),
        result=_result(_base_result()),
        artifact_context=None,
    )
    candidate = {
        "parameter_candidate_id": "candidate_000",
        "scenario_results": [compact.base_result],
    }

    summary = summarize_candidate_result(candidate, "summary")
    scenario_summary = summary["scenario_results"][0]
    assert "validation_closed_trades" not in scenario_summary
    assert scenario_summary["validation_equity_curve"] == []
    assert scenario_summary["detail_artifact_ref"]
    assert scenario_summary["detail_artifact_hash"] == compact.base_result["detail_artifact_hash"]  # type: ignore[index]
