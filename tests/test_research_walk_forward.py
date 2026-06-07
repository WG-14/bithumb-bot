from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.execution_reality_contract import build_execution_reality_contract
from bithumb_bot.research.backtest_engine import BacktestRun
from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.research.hashing import content_hash_payload, report_content_hash_payload, sha256_prefixed
from bithumb_bot.research.metrics import ResearchMetrics
from bithumb_bot.research.promotion_gate import PromotionGateError, build_candidate_profile, promote_candidate
from bithumb_bot.research.validation_protocol import (
    ResearchValidationError,
    _rolling_walk_forward_windows,
    _walk_forward_metrics,
    run_research_walk_forward,
)
from bithumb_bot.research.report_writer import write_research_report
from bithumb_bot.storage_io import write_json_atomic
from tests.factories.research_reports import assert_fast_research_workload, minimal_research_report


class _SnapshotStub:
    pass


def _manifest(*, min_windows: int = 2, required: bool = True):
    return parse_manifest(
        {
            "experiment_id": "walk_unit",
            "hypothesis": "Rolling walk-forward windows should be stable.",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "unit",
                "train": {"start": "2023-01-01", "end": "2023-01-02"},
                "validation": {"start": "2023-01-03", "end": "2023-01-04"},
                "final_holdout": {"start": "2023-01-05", "end": "2023-01-06"},
            },
            "parameter_space": {"SMA_SHORT": [2], "SMA_LONG": [4]},
            "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
            "acceptance_gate": {
                "min_trade_count": 1,
                "max_mdd_pct": 20,
                "min_profit_factor": 1.0,
                "oos_return_must_be_positive": True,
                "parameter_stability_required": False,
                "walk_forward_required": required,
            },
            "walk_forward": {
                "train_window_days": 2,
                "test_window_days": 1,
                "step_days": 1,
                "min_windows": min_windows,
            },
        }
    )


def _manager(tmp_path: Path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def _run(return_pct: float) -> BacktestRun:
    return BacktestRun(
        metrics=ResearchMetrics(
            return_pct=return_pct,
            max_drawdown_pct=1.0,
            profit_factor=2.0 if return_pct > 0.0 else None,
            trade_count=2 if return_pct > 0.0 else 0,
            win_rate=1.0 if return_pct > 0.0 else 0.0,
            avg_win=1.0 if return_pct > 0.0 else None,
            avg_loss=None,
            fee_total=0.0,
            slippage_total=0.0,
            max_consecutive_losses=0,
            single_trade_dependency_score=None,
            parameter_stability_score=None,
        ),
        trades=(),
        candle_count=10,
        warnings=(),
    )


def test_invalid_walk_forward_config_is_rejected() -> None:
    payload = _manifest().raw
    payload["walk_forward"]["min_windows"] = 0

    with pytest.raises(ManifestValidationError, match="walk_forward.min_windows"):
        parse_manifest(payload)


def test_rolling_windows_are_generated_deterministically() -> None:
    windows = _rolling_walk_forward_windows(_manifest())

    assert [window["train"].as_dict() for window in windows] == [
        {"start": "2023-01-01", "end": "2023-01-02"},
        {"start": "2023-01-02", "end": "2023-01-03"},
        {"start": "2023-01-03", "end": "2023-01-04"},
        {"start": "2023-01-04", "end": "2023-01-05"},
    ]
    assert [window["test"].as_dict() for window in windows][-1] == {
        "start": "2023-01-06",
        "end": "2023-01-06",
    }


def test_walk_forward_required_refuses_missing_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = {
        "experiment_id": "walk_unit",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "unit",
        "dataset_content_hash": "sha256:dataset",
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_001",
        "parameter_values": {"SMA_SHORT": 2, "SMA_LONG": 4},
        "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
        "validation_metrics": {"return_pct": 2.0, "trade_count": 2, "max_drawdown_pct": 1.0, "profit_factor": 2.0},
        "final_holdout_metrics": {"return_pct": 2.0, "trade_count": 2, "max_drawdown_pct": 1.0, "profit_factor": 2.0},
        "final_holdout_present": True,
        "final_holdout_required_for_promotion": True,
        "acceptance_gate_result": "PASS",
        "scenario_policy": "single_scenario",
        "scenario_pass_count": 1,
        "scenario_fail_count": 0,
        "required_scenario_count": 1,
        "scenario_results": [
            {
                "scenario_id": "scenario_001_fixed_bps_unit",
                "scenario_role": "base",
                "scenario_acceptance_gate_result": "PASS",
                "scenario_fail_reasons": [],
            }
        ],
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["uptrend_normal_vol_volume_increasing"],
        "blocked_live_regimes": ["sideways_low_vol_volume_decreasing"],
        "regime_evidence": {"uptrend_normal_vol_volume_increasing": {"trade_count": 12}},
        "regime_gate_result": {"result": "PASS", "passed": True, "reasons": []},
        "execution_timing_policy": {
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "next_candle_open",
            "quote_selection": "first_after_or_equal",
            "max_quote_wait_ms": 3000,
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": False,
            "source": "test",
        },
        "execution_reality_summary": {
            "signal_event_count": 2,
            "fillable_signal_event_count": 2,
            "missing_quote_on_signal_count": 0,
            "quote_after_decision_coverage_pct": None,
            "median_quote_age_ms_on_signal": None,
            "p95_quote_age_ms_on_signal": None,
            "execution_reference_policy": "next_candle_open",
            "execution_reality_level": "candle_next_open",
            "execution_attempt_count": 4,
            "execution_filled_count": 4,
            "filled_execution_count": 4,
            "portfolio_applied_trade_count": 4,
            "pending_execution_count": 0,
            "skipped_execution_count": 0,
            "failed_execution_count": 0,
            "closed_trade_count": 2,
            "pending_execution_at_end_count": 0,
            "pending_execution_after_dataset_end_count": 0,
            "execution_event_timeline_incomplete": False,
            "execution_reality_gate_status": "PASS",
            "execution_reality_gate_reasons": [],
        },
        "execution_event_summary": {
            "execution_attempt_count": 4,
            "execution_filled_count": 4,
            "filled_execution_count": 4,
            "portfolio_applied_trade_count": 4,
            "pending_execution_count": 0,
            "skipped_execution_count": 0,
            "failed_execution_count": 0,
            "closed_trade_count": 2,
            "pending_execution_at_end_count": 0,
            "pending_execution_after_dataset_end_count": 0,
            "execution_event_timeline_incomplete": False,
        },
        "walk_forward_required": True,
    }
    execution_contract = build_execution_reality_contract(
        fill_reference_policy="next_candle_open",
        missing_quote_policy="warn",
        min_execution_reality_level_for_promotion="candle_next_open",
        allow_same_candle_close_fill=False,
        top_of_book_required=False,
        fee_source="test",
        slippage_source="test",
    )
    candidate["execution_reality_contract"] = execution_contract
    candidate["execution_contract_hash"] = execution_contract["execution_contract_hash"]
    candidate["candidate_profile_hash"] = "sha256:placeholder"
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    report = {"experiment_id": "walk_unit", "candidates": [candidate]}
    report["content_hash"] = sha256_prefixed(content_hash_payload(report))
    write_json_atomic(
        manager.data_dir() / "reports" / "research" / "walk_unit" / "backtest_report.json",
        report,
    )

    with pytest.raises(PromotionGateError, match="walk_forward_missing"):
        promote_candidate(experiment_id="walk_unit", candidate_id="candidate_001", manager=manager)


@pytest.mark.walk_forward_e2e
def test_insufficient_windows_fails_clearly(tmp_path, monkeypatch) -> None:
    with pytest.raises(ResearchValidationError, match="walk_forward_insufficient_windows"):
        run_research_walk_forward(
            manifest=_manifest(min_windows=10),
            db_path=tmp_path / "missing.sqlite",
            manager=_manager(tmp_path, monkeypatch),
        )


@pytest.mark.contract
def test_walk_forward_report_persists_artifact_discovery_metadata(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    payload = minimal_research_report(
        report_kind="walk_forward",
        experiment_id="walk_unit",
        execution_observability={
            "production_evaluator_used": False,
            "contract_evaluator_used": True,
            "parallel_executor_used": False,
        },
    )
    assert_fast_research_workload(payload)

    paths, content_hash = write_research_report(
        manager=manager,
        experiment_id="walk_unit",
        report_name="walk_forward",
        payload=payload,
    )

    persisted = json.loads(paths.report_path.read_text(encoding="utf-8"))
    assert persisted["content_hash"] == content_hash
    assert persisted["artifact_refs"]["candidate_events"] == "derived/research/walk_unit/candidate_events.jsonl"
    assert persisted["artifact_refs"]["candidate_results_dir"] == "derived/research/walk_unit/candidate_results"
    assert persisted["artifact_refs"]["candidate_failures_dir"] == "derived/research/walk_unit/candidate_failures"
    assert persisted["artifact_paths"]["report_path"] == str(paths.report_path.resolve())
    assert persisted["artifact_paths"]["derived_path"] == str(paths.derived_path.resolve())
    assert persisted["execution_observability"]["production_evaluator_used"] is False
    assert persisted["execution_observability"]["contract_evaluator_used"] is True


def test_walk_forward_summary_report_uses_candidate_artifact_ref(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = {
        "candidate_id": "candidate_001",
        "decisions": [{"ts": 1, "signal": "BUY"}],
        "equity_curve": [{"ts": 1, "equity": 1_000_000.0}],
    }
    payload = minimal_research_report(
        report_kind="walk_forward",
        experiment_id="walk_summary_ref",
        candidates=[candidate],
        execution_observability={
            "production_evaluator_used": False,
            "contract_evaluator_used": True,
            "parallel_executor_used": False,
        },
    )
    payload.setdefault("research_run", {})["report_detail"] = "summary"
    assert_fast_research_workload(payload)

    result = write_research_report(
        manager=manager,
        experiment_id="walk_summary_ref",
        report_name="walk_forward",
        payload=payload,
    )

    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))
    derived = json.loads(result.paths.derived_path.read_text(encoding="utf-8"))
    assert persisted["artifact_refs"]["derived_candidates"] == "derived/research/walk_summary_ref/walk_forward_candidates.json"
    assert "decisions" in derived["candidates"][0]
    assert "decisions" not in persisted["candidates"][0]


def test_walk_forward_persisted_report_contains_report_write_observability(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    payload = minimal_research_report(
        report_kind="walk_forward",
        experiment_id="walk_write_obs",
        execution_observability={
            "production_evaluator_used": False,
            "contract_evaluator_used": True,
            "parallel_executor_used": False,
            "stage_timings": [],
            "work_units": [],
        },
    )
    payload.setdefault("research_run", {})["report_detail"] = "summary"
    assert_fast_research_workload(payload)

    result = write_research_report(
        manager=manager,
        experiment_id="walk_write_obs",
        report_name="walk_forward",
        payload=payload,
    )

    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))
    summary = persisted["artifact_write_summary"]
    assert summary == result.artifact_write_summary
    assert persisted["artifact_observability"]["report_write"] == summary
    assert summary["report_bytes"] == result.paths.report_path.stat().st_size
    assert sha256_prefixed(report_content_hash_payload(persisted)) == persisted["content_hash"]


def test_repeated_positive_test_windows_pass_aggregate_walk_forward(monkeypatch) -> None:
    manifest = _manifest()
    windows = _rolling_walk_forward_windows(manifest)
    snapshots = {
        f"window_{index:03d}_{kind}": _SnapshotStub()
        for index in range(1, len(windows) + 1)
        for kind in ("train", "test")
    }
    for index, window in enumerate(windows, start=1):
        snapshots[f"window_{index:03d}_train"].date_range = window["train"]
        snapshots[f"window_{index:03d}_test"].date_range = window["test"]
        snapshots[f"window_{index:03d}_train"].candles = ()
        snapshots[f"window_{index:03d}_test"].candles = ()

    monkeypatch.setattr("bithumb_bot.research.validation_protocol.resolve_research_strategy", lambda _: lambda *args: _run(1.0))

    metrics = _walk_forward_metrics(
        manifest=manifest,
        snapshots=snapshots,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
        parameter_stability_score=None,
    )

    assert metrics["return_consistency_pass"] is True
    assert metrics["pass_window_count"] == metrics["window_count"]
    assert "trade_count_by_regime" in metrics["windows"][0]
    assert "candle_count_by_regime" in metrics["windows"][0]


def test_inconsistent_test_windows_fail_aggregate_walk_forward(monkeypatch) -> None:
    manifest = _manifest()
    windows = _rolling_walk_forward_windows(manifest)
    snapshots = {
        f"window_{index:03d}_{kind}": _SnapshotStub()
        for index in range(1, len(windows) + 1)
        for kind in ("train", "test")
    }
    for index, window in enumerate(windows, start=1):
        snapshots[f"window_{index:03d}_train"].date_range = window["train"]
        snapshots[f"window_{index:03d}_test"].date_range = window["test"]
        snapshots[f"window_{index:03d}_train"].candles = ()
        snapshots[f"window_{index:03d}_test"].candles = ()
    returns = iter([1.0, 1.0, 1.0, -1.0, 1.0, 1.0, 1.0, 1.0])

    monkeypatch.setattr("bithumb_bot.research.validation_protocol.resolve_research_strategy", lambda _: lambda *args: _run(next(returns)))

    metrics = _walk_forward_metrics(
        manifest=manifest,
        snapshots=snapshots,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
        parameter_stability_score=None,
    )

    assert metrics["return_consistency_pass"] is False
    assert metrics["failure_reason"] == "walk_forward_failed"
