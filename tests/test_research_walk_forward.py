from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.research.backtest_engine import BacktestRun
from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.metrics import ResearchMetrics
from bithumb_bot.research.promotion_gate import PromotionGateError, build_candidate_profile, promote_candidate
from bithumb_bot.research.validation_protocol import (
    ResearchValidationError,
    _rolling_walk_forward_windows,
    _walk_forward_metrics,
    run_research_walk_forward,
)
from bithumb_bot.storage_io import write_json_atomic


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


def test_insufficient_windows_fails_clearly(tmp_path, monkeypatch) -> None:
    with pytest.raises(ResearchValidationError, match="walk_forward_insufficient_windows"):
        run_research_walk_forward(
            manifest=_manifest(min_windows=10),
            db_path=tmp_path / "missing.sqlite",
            manager=_manager(tmp_path, monkeypatch),
        )


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
