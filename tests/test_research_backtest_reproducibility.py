from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.canonical_decision import export_research_decisions, export_runtime_replay_decisions
from bithumb_bot.decision_equivalence import compare_decision_equivalence
from bithumb_bot.research import backtest_engine
from bithumb_bot.research.backtest_engine import (
    BacktestHeartbeatPolicy,
    BacktestResourceLimitExceeded,
    BacktestResourceLimits,
    BacktestRunContext,
    run_sma_backtest,
)
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot, TopOfBookQuote
from bithumb_bot.research.execution_calibration import build_calibration_artifact
from bithumb_bot.research.execution_model import ExecutionFill, ExecutionRequest, FixedBpsExecutionModel, StressExecutionModel
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy, ManifestValidationError, parse_manifest
from bithumb_bot.research.parameter_space import candidate_id
from bithumb_bot.research.promotion_gate import PromotionGateError, _verify_report_content_hash, promote_candidate
from bithumb_bot.research.validation_protocol import run_research_backtest
from bithumb_bot.strategy.sma import create_sma_with_filter_strategy


def _ts(day: str, minute: int) -> int:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(base.timestamp() * 1000) + minute * 60_000


def _create_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE candles(
                ts INTEGER PRIMARY KEY,
                pair TEXT,
                interval TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """
        )
        pattern = [100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96]
        for day in ("2023-01-01", "2023-01-02", "2023-01-03"):
            for index in range(24 * 60):
                close = pattern[index % len(pattern)]
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                    """,
                    (_ts(day, index), close, close * 1.01, close * 0.99, close),
                )
        conn.commit()
    finally:
        conn.close()


def _manifest() -> dict[str, object]:
    return {
        "experiment_id": "deterministic_sma",
        "hypothesis": "SMA candidate remains deterministic across repeated research runs.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "unit_candles_v1",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 90,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": False,
            "parameter_stability_required": False,
        },
    }


def _stress_suite_contract(*, min_retention: float | None = None, min_survival: float = 0.0) -> dict[str, object]:
    payload = {
        "required_for_promotion": True,
        "trade_removal": {
            "top_n_by_net_pnl": [1],
        },
        "trade_order_monte_carlo": {
            "iterations": 20,
            "seed_policy": "derived_from_manifest_candidate_scenario_split_hash",
            "min_survival_probability": min_survival,
            "ruin_max_drawdown_pct": 90.0,
            "min_closed_trades": 1,
        },
    }
    if min_retention is not None:
        payload["trade_removal"]["min_return_retention_pct"] = min_retention
    return payload


class _FailSellExecutionModel:
    name = "fail_sell_test"
    version = "test_v1"

    def __init__(self) -> None:
        self._fixed = FixedBpsExecutionModel(fee_rate=0.0, slippage_bps=0.0)

    def params_payload(self) -> dict[str, object]:
        return {"type": self.name, "version": self.version}

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        fill = self._fixed.simulate(request)
        if str(request.side).upper() != "SELL":
            return fill
        return replace(
            fill,
            filled_qty=0.0,
            remaining_qty=float(request.requested_qty or 0.0),
            avg_fill_price=None,
            fee=0.0,
            fill_status="failed",
            model_name=self.name,
            model_version=self.version,
        )


class _PartialSellExecutionModel:
    name = "partial_sell_test"
    version = "test_v1"

    def __init__(self) -> None:
        self._fixed = FixedBpsExecutionModel(fee_rate=0.0, slippage_bps=0.0)

    def params_payload(self) -> dict[str, object]:
        return {"type": self.name, "version": self.version}

    def simulate(self, request: ExecutionRequest) -> ExecutionFill:
        fill = self._fixed.simulate(request)
        if str(request.side).upper() != "SELL":
            return fill
        filled_qty = float(fill.filled_qty) * 0.5
        return replace(
            fill,
            filled_qty=filled_qty,
            remaining_qty=max(0.0, float(fill.requested_qty) - filled_qty),
            fee=0.0,
            fill_status="partial",
            model_name=self.name,
            model_version=self.version,
        )


def _snapshot_from_closes(closes: list[float], *, quotes: tuple[TopOfBookQuote, ...] = ()) -> DatasetSnapshot:
    base_ts = 1_700_000_000_000
    candles = tuple(
        Candle(
            ts=base_ts + index * 60_000,
            open=float(close),
            high=max(float(close), 130.0),
            low=min(float(close), 100.0) * 0.9,
            close=float(close),
            volume=1.0,
        )
        for index, close in enumerate(closes)
    )
    manifest = parse_manifest(_manifest())
    return DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
        top_of_book_event_quotes=quotes,
    )


def test_same_manifest_and_dataset_produce_same_content_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    manifest = parse_manifest(_manifest())

    first = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert first["content_hash"] == second["content_hash"]
    assert first["candidates"][0]["candidate_profile_hash"] == second["candidates"][0]["candidate_profile_hash"]
    assert first["candidates"][0]["regime_classifier_version"] == "market_regime_v2"
    assert first["metrics_schema_version"] == 2
    assert first["candidates"][0]["validation_metrics_v2"]["metrics_schema_version"] == 2
    assert first["candidates"][0]["final_holdout_metrics_v2"]["metrics_schema_version"] == 2
    assert first["best_validation_metrics_v2"]["metrics_schema_version"] == 2
    json.dumps(first, allow_nan=False)
    json.dumps(first["candidates"][0], allow_nan=False)
    assert first["candidates"][0]["market_regime_bucket_performance"]
    assert first["candidates"][0]["market_regime_coverage"]
    assert "regime_gate_result" in first["candidates"][0]
    assert Path(first["artifact_paths"]["report_path"]).exists()
    persisted = json.loads(Path(first["artifact_paths"]["report_path"]).read_text(encoding="utf-8"))
    assert persisted["content_hash"] == first["content_hash"]
    assert persisted["artifact_refs"] == first["artifact_refs"]
    assert persisted["artifact_paths"] == first["artifact_paths"]
    assert persisted["artifact_refs"] == {
        "derived_candidates": "derived/research/deterministic_sma/backtest_candidates.json",
        "report": "reports/research/deterministic_sma/backtest_report.json",
        "candidate_events": "derived/research/deterministic_sma/candidate_events.jsonl",
        "candidate_results_dir": "derived/research/deterministic_sma/candidate_results",
        "candidate_failures_dir": "derived/research/deterministic_sma/candidate_failures",
    }
    assert _verify_report_content_hash(persisted, label="backtest_report") == persisted["content_hash"]


def test_required_stress_suite_is_attached_to_report_and_candidate(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["stress_suite"] = _stress_suite_contract()
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    candidate = report["candidates"][0]

    assert report["stress_suite_required"] is True
    assert report["stress_suite_contract_hash"].startswith("sha256:")
    assert candidate["stress_suite_gate_result"] == "PASS"
    assert candidate["validation_stress_suite"]["stress_suite_hash"].startswith("sha256:")
    assert report["best_validation_stress_suite"]["stress_suite_hash"] == candidate["validation_stress_suite"]["stress_suite_hash"]
    json.dumps(report, allow_nan=False)


def test_required_stress_suite_failure_blocks_candidate_acceptance(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["stress_suite"] = _stress_suite_contract(min_retention=100.0, min_survival=1.0)
    payload["stress_suite"]["trade_order_monte_carlo"]["ruin_max_drawdown_pct"] = 0.01
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    candidate = report["candidates"][0]

    assert candidate["acceptance_gate_result"] == "FAIL"
    assert candidate["stress_suite_gate_result"] == "FAIL"
    assert "stress_suite_gate_not_passed" in candidate["gate_fail_reasons"]


def test_report_content_hash_is_independent_of_data_root(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    manifest = parse_manifest(_manifest())

    reports = []
    for root_name in ("runtime_a", "runtime_b"):
        runtime_root = tmp_path / root_name
        for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
            monkeypatch.setenv(key, str(runtime_root / f"{key.lower()}_root"))
        monkeypatch.setenv("MODE", "paper")
        reports.append(
            run_research_backtest(
                manifest=manifest,
                db_path=db_path,
                manager=PathManager.from_env(Path.cwd()),
                generated_at="2026-05-03T00:00:00+00:00",
            )
        )

    first, second = reports
    assert first["content_hash"] == second["content_hash"]
    assert first["artifact_refs"] == second["artifact_refs"]
    assert first["artifact_paths"]["report_path"] != second["artifact_paths"]["report_path"]


def test_sma_backtest_attaches_entry_and_exit_regime_snapshots() -> None:
    candles = tuple(
        Candle(
            ts=1_700_000_000_000 + index * 60_000,
            open=float(close),
            high=float(close) * 1.02,
            low=float(close) * 0.98,
            close=float(close),
            volume=float(100 + index * 10),
        )
        for index, close in enumerate([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    )
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.0, "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    closed = [trade for trade in result.trades if trade["side"] == "SELL"]
    assert closed
    assert closed[0]["entry_regime"]
    assert closed[0]["exit_regime"]
    assert isinstance(closed[0]["entry_regime_snapshot"], dict)
    assert isinstance(closed[0]["exit_regime_snapshot"], dict)
    assert result.regime_performance
    assert result.regime_coverage
    assert result.metrics_v2 is not None
    assert result.metrics_v2.metrics_schema_version == 2
    assert result.metrics_v2.trade_quality.closed_trade_count == result.metrics.trade_count
    assert result.metrics_v2.trade_quality.execution_count == len(result.trades)
    assert result.metrics_v2.time_exposure.exposure_time_pct is not None
    assert result.decisions
    assert {"raw_signal", "final_signal", "position_state_hash"} <= set(result.decisions[0])


def test_sma_backtest_uses_bounded_regime_fast_path(monkeypatch) -> None:
    snapshot = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    calls: list[int] = []
    original = backtest_engine.classify_market_regime_from_arrays

    def counting_classifier(**kwargs):
        calls.append(int(kwargs["index"]))
        assert len(kwargs["closes"]) == len(snapshot.candles)
        return original(**kwargs)

    monkeypatch.setattr(backtest_engine, "classify_market_regime_from_arrays", counting_classifier)

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert result.decisions
    assert calls == list(range(4, len(snapshot.candles)))


def test_sma_backtest_caches_dataset_content_hash(monkeypatch) -> None:
    snapshot = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    calls = 0

    def counted_content_hash(self: DatasetSnapshot) -> str:
        nonlocal calls
        assert self is snapshot
        calls += 1
        return "sha256:cached_dataset_hash"

    monkeypatch.setattr(DatasetSnapshot, "content_hash", counted_content_hash)

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert calls == 1
    assert result.decisions
    fingerprints = [decision["replay_fingerprint_hash"] for decision in result.decisions]
    assert all(str(item).startswith("sha256:") for item in fingerprints)
    assert fingerprints == [
        decision["replay_fingerprint_hash"]
        for decision in run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
            fee_rate=0.0,
            slippage_bps=0.0,
        ).decisions
    ]


def test_tiny_three_day_sma_backtest_completes_structurally() -> None:
    base_ts = 1_700_000_000_000
    candles = tuple(
        Candle(
            ts=base_ts + index * 60_000,
            open=float(100 + (index % 17) - 8),
            high=float(101 + (index % 17) - 8),
            low=float(99 + (index % 17) - 8),
            close=float(100 + (index % 17) - 8),
            volume=1.0 + float(index % 5),
        )
        for index in range(3 * 24 * 60)
    )
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="tiny_three_day",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=manifest.dataset.split.train,
        candles=candles,
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 7, "SMA_LONG": 30},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert result.candle_count == 4320
    assert len(result.decisions) == 4320 - 30
    assert result.metrics_v2 is not None


def test_research_run_policy_participates_in_manifest_hash() -> None:
    bounded = parse_manifest(_manifest())
    full_payload = dict(_manifest())
    full_payload["research_run"] = {
        "report_detail": "full",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": None,
            "max_trades": None,
            "max_equity_points_retained": None,
            "max_rss_mb": None,
        },
    }
    full = parse_manifest(full_payload)

    assert bounded.research_run.report_detail == "summary"
    assert bounded.research_run.resource_limits.max_decisions_retained == 0
    assert bounded.manifest_hash() != full.manifest_hash()


def test_summary_mode_does_not_retain_full_per_candle_decisions_and_is_deterministic() -> None:
    snapshot = _snapshot_from_closes([100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96])
    context = BacktestRunContext(
        report_detail="summary",
        resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
    )

    first = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
        context=context,
    )
    second = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.0,
        slippage_bps=0.0,
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    assert first.decisions == ()
    assert first.equity_curve == ()
    assert first.retained_detail_summary["decision_count"] == len(snapshot.candles) - 4
    assert first.retained_detail_summary["retained_regime_snapshot_count"] == 0
    assert first.regime_coverage
    assert first.regime_performance
    assert first.retained_detail_summary["decision_hash"] == second.retained_detail_summary["decision_hash"]
    assert first.metrics.as_dict() == second.metrics.as_dict()


def test_summary_metrics_v2_match_full_when_equity_retention_is_zero() -> None:
    snapshot = _snapshot_from_closes([100, 90, 100, 80, 100, 130, 50, 40, 30, 20, 30, 45])
    kwargs = {
        "dataset": snapshot,
        "parameter_values": {"SMA_SHORT": 1, "SMA_LONG": 2},
        "fee_rate": 0.0,
        "slippage_bps": 0.0,
    }
    full = run_sma_backtest(
        **kwargs,
        context=BacktestRunContext(report_detail="full"),
    )
    summary = run_sma_backtest(
        **kwargs,
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    assert full.equity_curve
    assert summary.equity_curve == ()
    assert summary.retained_detail_summary["retained_regime_snapshot_count"] == 0
    assert full.metrics_v2 is not None
    assert summary.metrics_v2 is not None
    assert summary.metrics_v2.return_risk.cagr_pct == pytest.approx(full.metrics_v2.return_risk.cagr_pct)
    assert summary.metrics_v2.return_risk.max_drawdown_pct == pytest.approx(full.metrics_v2.return_risk.max_drawdown_pct)
    assert summary.metrics_v2.time_exposure.exposure_time_pct == pytest.approx(full.metrics_v2.time_exposure.exposure_time_pct)
    assert summary.metrics_v2.time_exposure.active_bar_count == full.metrics_v2.time_exposure.active_bar_count
    assert summary.metrics_v2.time_exposure.period_start_ts == full.metrics_v2.time_exposure.period_start_ts
    assert summary.metrics_v2.time_exposure.period_end_ts == full.metrics_v2.time_exposure.period_end_ts
    assert summary.metrics_v2.time_exposure.elapsed_ms == full.metrics_v2.time_exposure.elapsed_ms
    assert summary.metrics_v2.time_exposure.calendar_days == pytest.approx(full.metrics_v2.time_exposure.calendar_days)
    assert summary.regime_coverage == full.regime_coverage
    assert summary.regime_performance == full.regime_performance


def test_summary_and_full_metrics_v2_gates_match_for_cagr_and_exposure(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    base_payload = _manifest()
    base_payload["acceptance_gate"]["metrics_contract_required"] = True
    base_payload["acceptance_gate"]["min_cagr_pct"] = 0.0
    base_payload["acceptance_gate"]["max_exposure_time_pct"] = 100.0

    full_payload = dict(base_payload)
    full_payload["research_run"] = {
        "report_detail": "full",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": None,
            "max_trades": None,
            "max_equity_points_retained": None,
            "max_rss_mb": None,
        },
    }
    summary_payload = dict(base_payload)
    summary_payload["research_run"] = {
        "report_detail": "summary",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": None,
            "max_decisions_retained": 0,
            "max_trades": None,
            "max_equity_points_retained": 0,
            "max_rss_mb": None,
        },
    }

    full = run_research_backtest(
        manifest=parse_manifest(full_payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    summary = run_research_backtest(
        manifest=parse_manifest(summary_payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert summary["candidates"][0]["validation_metrics_v2"]["return_risk"]["cagr_pct"] == pytest.approx(
        full["candidates"][0]["validation_metrics_v2"]["return_risk"]["cagr_pct"]
    )
    assert summary["candidates"][0]["validation_metrics_v2"]["time_exposure"]["exposure_time_pct"] == pytest.approx(
        full["candidates"][0]["validation_metrics_v2"]["time_exposure"]["exposure_time_pct"]
    )
    assert summary["candidates"][0]["acceptance_gate_result"] == full["candidates"][0]["acceptance_gate_result"]
    assert summary["candidates"][0]["gate_fail_reasons"] == full["candidates"][0]["gate_fail_reasons"]


def test_heartbeat_and_max_trades_guard_trip() -> None:
    events: list[dict[str, object]] = []
    snapshot = _snapshot_from_closes(([100, 90, 110, 90, 110, 90, 110, 90] * 5))

    with pytest.raises(BacktestResourceLimitExceeded) as raised:
        run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
            fee_rate=0.0,
            slippage_bps=0.0,
            context=BacktestRunContext(
                experiment_id="guard_exp",
                candidate_id="candidate_guard",
                scenario_id="scenario_1",
                split_name="validation",
                report_detail="summary",
                resource_limits=BacktestResourceLimits(max_trades=2, max_decisions_retained=0, max_equity_points_retained=0),
                heartbeat=BacktestHeartbeatPolicy(interval_s=None, bar_interval=1),
                progress_callback=events.append,
            ),
        )

    assert any(event.get("stage") == "heartbeat" for event in events)
    assert raised.value.reason == "candidate_resource_limit_exceeded"
    assert "max_trades_exceeded" in raised.value.evidence["reasons"]
    assert raised.value.evidence["retained_decision_count"] == 0


def test_research_sweep_continues_after_guard_failure_and_writes_candidate_artifacts(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "bounded_sweep"
    payload["parameter_space"] = {
        "SMA_SHORT": [2],
        "SMA_LONG": [4],
        "SMA_FILTER_GAP_MIN_RATIO": [0.0, 1.0],
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
    }
    payload["research_run"] = {
        "report_detail": "summary",
        "resource_limits": {
            "max_runtime_s_per_candidate_split": 60,
            "max_decisions_retained": 0,
            "max_trades": 1,
            "max_equity_points_retained": 0,
            "max_rss_mb": None,
        },
        "heartbeat": {"interval_s": None, "bar_interval": 5},
    }

    report = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert len(report["candidates"]) == 2
    assert any("candidate_resource_limit_exceeded" in (candidate.get("gate_fail_reasons") or []) for candidate in report["candidates"])
    assert Path(report["artifact_paths"]["report_path"]).exists()
    assert Path(report["artifact_paths"]["derived_path"]).exists()
    assert Path(report["artifact_paths"]["candidate_events_path"]).exists()
    assert Path(report["artifact_paths"]["candidate_results_dir"]).is_dir()
    assert Path(report["artifact_paths"]["candidate_failures_dir"]).is_dir()
    persisted = json.loads(Path(report["artifact_paths"]["report_path"]).read_text(encoding="utf-8"))
    assert persisted["artifact_refs"]["candidate_events"] == "derived/research/bounded_sweep/candidate_events.jsonl"
    assert persisted["artifact_refs"]["candidate_results_dir"] == "derived/research/bounded_sweep/candidate_results"
    assert persisted["artifact_refs"]["candidate_failures_dir"] == "derived/research/bounded_sweep/candidate_failures"
    assert persisted["artifact_paths"] == report["artifact_paths"]
    root = manager.data_dir() / "derived" / "research" / "bounded_sweep"
    assert (root / "candidate_events.jsonl").exists()
    assert list((root / "candidate_results").glob("candidate_*.json"))
    failures = list((root / "candidate_failures").glob("candidate_*.json"))
    assert failures
    failed = [candidate for candidate in persisted["candidates"] if candidate.get("failure_artifact_path")]
    assert failed
    assert failed[0]["failure_artifact_ref"].startswith("derived/research/bounded_sweep/candidate_failures/")
    assert Path(failed[0]["failure_artifact_path"]).exists()
    assert failed[0]["resource_guard"]["status"] == "TRIPPED"


def test_full_decisions_external_jsonl_policy_is_rejected_clearly() -> None:
    payload = _manifest()
    payload["research_run"] = {
        "artifact_policy": {
            "full_decisions_external_jsonl": True,
        },
    }

    with pytest.raises(ManifestValidationError, match="full_decisions_external_jsonl is not implemented yet"):
        parse_manifest(payload)


def test_retention_caps_do_not_fail_candidate_but_max_trades_guard_does() -> None:
    snapshot = _snapshot_from_closes(([100, 90, 110, 90, 110, 90, 110, 90] * 2))
    capped = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    assert capped.retained_detail_summary["retained_decision_count"] == 0
    assert capped.retained_detail_summary["retained_equity_point_count"] == 0
    assert capped.retained_detail_summary["decision_count"] > 0

    with pytest.raises(BacktestResourceLimitExceeded) as raised:
        run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
            fee_rate=0.0,
            slippage_bps=0.0,
            context=BacktestRunContext(
                report_detail="summary",
                resource_limits=BacktestResourceLimits(
                    max_trades=1,
                    max_decisions_retained=0,
                    max_equity_points_retained=0,
                ),
            ),
        )
    assert raised.value.evidence["reasons"] == ["max_trades_exceeded"]


def test_failed_sell_records_failure_candle_equity_and_mdd() -> None:
    snapshot = _snapshot_from_closes([100, 90, 100, 80, 100, 130, 50, 40, 30, 20])

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=_FailSellExecutionModel(),
    )

    sell = [trade for trade in result.trades if trade["side"] == "SELL"][0]
    failure_mark = next(point for point in result.equity_curve if point.ts == sell["decision_ts"])
    assert sell["execution"]["fill_status"] == "failed"
    assert failure_mark.asset_qty > 0.0
    assert failure_mark.equity == pytest.approx(505000.0)
    assert result.metrics.max_drawdown_pct > 60.0
    assert result.metrics_v2 is not None
    assert result.metrics_v2.return_risk.max_drawdown_pct == pytest.approx(result.metrics.max_drawdown_pct)


def test_missing_quote_skipped_sell_records_failure_candle_equity_and_mdd() -> None:
    base_ts = 1_700_000_000_000
    buy_decision_ts = base_ts + 5 * 60_000
    snapshot = _snapshot_from_closes(
        [100, 90, 100, 80, 100, 130, 50, 40, 30, 20],
        quotes=(
            TopOfBookQuote(
                ts=buy_decision_ts,
                pair="KRW-BTC",
                bid_price=99.9,
                ask_price=100.1,
                spread_bps=20.0,
                source="test",
            ),
        ),
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            missing_quote_policy="warn",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    sell = [trade for trade in result.trades if trade["side"] == "SELL"][0]
    failure_mark = next(point for point in result.equity_curve if point.ts == sell["decision_ts"])
    assert sell["execution"]["fill_status"] == "skipped_with_warning"
    assert failure_mark.asset_qty > 0.0
    assert failure_mark.equity == pytest.approx(504505.49450549454)
    assert result.metrics.max_drawdown_pct > 60.0
    assert result.metrics_v2 is not None
    assert result.metrics_v2.return_risk.max_drawdown_pct == pytest.approx(result.metrics.max_drawdown_pct)


def test_partial_sell_keeps_residual_position_open_in_metrics_v2() -> None:
    snapshot = _snapshot_from_closes([100, 90, 100, 80, 100, 130, 120, 110, 90, 80])

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_model=_PartialSellExecutionModel(),
    )

    assert result.metrics_v2 is not None
    assert result.metrics.trade_count == 1
    assert result.metrics_v2.return_risk.open_position_at_end is True
    assert result.metrics_v2.return_risk.unrealized_pnl_end == pytest.approx(-99000.0)
    assert result.metrics_v2.trade_quality.closed_trade_count == 1
    assert result.closed_trades[0].net_pnl == pytest.approx(99000.0)
    assert len(result.position_intervals) == 1
    assert result.position_intervals[0].close_ts is None
    assert result.metrics_v2.time_exposure.exposure_time_pct is not None
    assert result.metrics_v2.time_exposure.exposure_time_pct > 0.0
    assert result.metrics_v2.cost_execution.filled_execution_count == 2
    assert result.metrics_v2.cost_execution.partial_fill_count == 1
    assert result.metrics_v2.cost_execution.failed_execution_count == 0
    assert result.metrics_v2.cost_execution.skipped_execution_count == 0


def test_research_runtime_decision_generation_gap_is_visible_not_silent() -> None:
    closes = [100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96]
    candles = tuple(
        Candle(
            ts=1_700_000_000_000 + index * 60_000,
            open=float(close),
            high=float(close) * 1.02,
            low=float(close) * 0.98,
            close=float(close),
            volume=1.0,
        )
        for index, close in enumerate(closes)
    )
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
    )
    research = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.0, "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0},
        fee_rate=0.0,
        slippage_bps=0.0,
    )
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE candles(ts INTEGER, pair TEXT, interval TEXT, close REAL)")
    for candle in candles:
        conn.execute(
            "INSERT INTO candles(ts, pair, interval, close) VALUES (?, ?, ?, ?)",
            (candle.ts, "KRW-BTC", "1m", candle.close),
        )
    conn.commit()
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=4,
        pair="KRW-BTC",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=1,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        exit_rule_names=["opposite_cross", "max_holding_time"],
    )
    selected_research_decision = dict(research.decisions[1])
    runtime_decisions = export_runtime_replay_decisions(
        conn=conn,
        strategy=strategy,
        through_ts_list=[selected_research_decision["candle_ts"]],
        market="KRW-BTC",
        interval="1m",
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        db_data_fingerprint="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    )
    research_decisions = export_research_decisions(
        [selected_research_decision],
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    )

    result = compare_decision_equivalence(
        research_decisions=research_decisions,
        runtime_decisions=runtime_decisions,
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert runtime_decisions
    assert result.ok is False
    assert result.report["promotion_grade_comparison"] is False or result.report["mismatch_count"] > 0


def test_fixed_bps_execution_model_preserves_legacy_backtest_metrics() -> None:
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=tuple(
            Candle(
                ts=1_700_000_000_000 + index * 60_000,
                open=float(close),
                high=float(close) * 1.01,
                low=float(close) * 0.99,
                close=float(close),
                volume=1.0,
            )
            for index, close in enumerate([100, 99, 98, 99, 101, 103, 102, 100, 98, 97, 99, 102])
        ),
    )

    legacy = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.001,
        slippage_bps=5.0,
    )
    modeled = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
        fee_rate=0.001,
        slippage_bps=5.0,
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=5.0),
    )

    assert modeled.metrics.as_dict() == legacy.metrics.as_dict()
    assert modeled.trades[0]["execution"]["model_name"] == "fixed_bps"
    assert modeled.trades[0]["execution"]["model_params_hash"].startswith("sha256:")


def test_seeded_stress_execution_model_is_deterministic_and_auditable() -> None:
    manifest = parse_manifest(_manifest())
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=tuple(
            Candle(
                ts=1_700_000_000_000 + index * 60_000,
                open=float(close),
                high=float(close) * 1.01,
                low=float(close) * 0.99,
                close=float(close),
                volume=1.0,
            )
            for index, close in enumerate([100, 99, 98, 99, 101, 103, 102, 100, 98, 97, 99, 102])
        ),
    )
    def _run():
        return run_sma_backtest(
            dataset=snapshot,
            parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
            fee_rate=0.001,
            slippage_bps=20.0,
            execution_model=StressExecutionModel(
            fee_rate=0.001,
            slippage_bps=20.0,
            latency_ms=500,
            partial_fill_rate=1.0,
            order_failure_rate=0.0,
            market_order_extra_cost_bps=5.0,
            seed=42,
            ),
        )

    first = _run()
    second = _run()

    assert first.trades == second.trades
    execution = first.trades[0]["execution"]
    assert execution["fill_status"] == "partial"
    assert execution["latency_ms"] == 500
    assert execution["slippage_bps"] == 25.0
    assert execution["fee"] >= 0.0
    assert execution["filled_qty"] > 0.0
    assert execution["remaining_qty"] > 0.0


def test_sma_signal_close_executes_next_candle_open_not_same_close() -> None:
    manifest = parse_manifest(_manifest())
    base_ts = 1_700_000_000_000
    closes = [100, 90, 100, 80, 100, 130]
    candles = tuple(
        Candle(
            ts=base_ts + index * 60_000,
            open=130.0 if index == 5 else float(close),
            high=max(float(close), 130.0 if index == 5 else float(close)) + 1.0,
            low=min(float(close), 130.0 if index == 5 else float(close)) - 1.0,
            close=float(close),
            volume=1.0,
        )
        for index, close in enumerate(closes)
    )
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=candles,
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="next_candle_open",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    assert result.trades
    execution = result.trades[0]["execution"]
    assert result.trades[0]["side"] == "BUY"
    assert result.trades[0]["price"] == 130.0
    assert execution["signal_reference_price"] == 100.0
    assert execution["fill_reference_price"] == 130.0
    assert execution["fill_reference_source"] == "next_candle_open"


def test_decision_ts_is_after_signal_candle_close() -> None:
    manifest = parse_manifest(_manifest())
    base_ts = 1_700_000_000_000
    snapshot = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=manifest.dataset.split.validation,
        candles=tuple(
            Candle(
                ts=base_ts + index * 60_000,
                open=float(close),
                high=float(close) + 1.0,
                low=float(close) - 1.0,
                close=float(close),
                volume=1.0,
            )
            for index, close in enumerate([100, 90, 100, 80, 100, 130])
        ),
    )

    result = run_sma_backtest(
        dataset=snapshot,
        parameter_values={"SMA_SHORT": 1, "SMA_LONG": 2},
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="next_candle_open",
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    execution = result.trades[0]["execution"]
    assert execution["signal_candle_start_ts"] == base_ts + 4 * 60_000
    assert execution["signal_candle_close_ts"] == base_ts + 5 * 60_000
    assert execution["decision_ts"] >= execution["signal_candle_close_ts"]
    assert execution["decision_ts"] != execution["signal_candle_start_ts"]


def test_reproducibility_hash_changes_when_execution_timing_policy_changes(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    legacy_manifest = parse_manifest(_manifest())
    next_open_payload = _manifest()
    next_open_payload["execution_timing"] = {
        "fill_reference_policy": "next_candle_open",
        "allow_same_candle_close_fill": False,
    }
    next_open_manifest = parse_manifest(next_open_payload)

    legacy = run_research_backtest(
        manifest=legacy_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    next_open = run_research_backtest(
        manifest=next_open_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert legacy["manifest_hash"] != next_open["manifest_hash"]
    assert legacy["candidates"][0]["candidate_profile_hash"] != next_open["candidates"][0]["candidate_profile_hash"]


def test_metrics_gate_threshold_change_changes_manifest_and_candidate_evidence_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    base_payload = _manifest()
    base_payload["acceptance_gate"]["metrics_contract_required"] = True
    base_payload["acceptance_gate"]["min_cagr_pct"] = 1.0
    changed_payload = _manifest()
    changed_payload["acceptance_gate"]["metrics_contract_required"] = True
    changed_payload["acceptance_gate"]["min_cagr_pct"] = 2.0
    base_manifest = parse_manifest(base_payload)
    changed_manifest = parse_manifest(changed_payload)

    base_report = run_research_backtest(
        manifest=base_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    changed_report = run_research_backtest(
        manifest=changed_manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert base_report["manifest_hash"] != changed_report["manifest_hash"]
    assert base_report["candidates"][0]["metrics_gate_policy"]["min_cagr_pct"] == 1.0
    assert changed_report["candidates"][0]["metrics_gate_policy"]["min_cagr_pct"] == 2.0
    assert base_report["candidates"][0]["metrics_gate_policy_hash"] != changed_report["candidates"][0]["metrics_gate_policy_hash"]
    assert base_report["candidates"][0]["candidate_profile_hash"] != changed_report["candidates"][0]["candidate_profile_hash"]


def test_research_backtest_fails_candidate_when_calibration_breaches_assumptions(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [5],
        "latency_ms": [100],
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)
    calibration = build_calibration_artifact(
        summary={
            "sample_count": 50,
            "median_slippage_vs_signal_bps": 8.0,
            "p90_slippage_vs_signal_bps": 12.0,
            "p95_slippage_vs_signal_bps": 20.0,
            "p95_submit_to_fill_ms": 200,
            "partial_fill_rate": 0.0,
            "unfilled_rate": 0.0,
            "model_breach_rate": 0.0,
            "quality_gate_status": "PASS",
        },
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
        execution_calibration=calibration,
    )

    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_p90_slippage_exceeds_assumption" in report["candidates"][0]["gate_fail_reasons"]


def test_research_backtest_fails_candidate_when_required_calibration_missing(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": 5,
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_missing" in report["candidates"][0]["gate_fail_reasons"]


def test_research_backtest_fails_candidate_when_calibration_market_mismatches(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": 50,
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)
    calibration = build_calibration_artifact(
        summary={
            "sample_count": 50,
            "median_slippage_vs_signal_bps": 1.0,
            "p90_slippage_vs_signal_bps": 2.0,
            "p95_slippage_vs_signal_bps": 3.0,
            "p95_submit_to_fill_ms": 0,
            "partial_fill_rate": 0.0,
            "unfilled_rate": 0.0,
            "model_breach_rate": 0.0,
            "quality_gate_status": "PASS",
        },
        market="KRW-ETH",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
        execution_calibration=calibration,
    )

    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_market_mismatch" in report["candidates"][0]["gate_fail_reasons"]


def test_research_backtest_candidate_gate_receives_execution_fill_quality_failures(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": 0.0,
        "slippage_bps": 50,
        "latency_ms": 500,
        "partial_fill_rate": 0.0,
        "order_failure_rate": 0.0,
        "calibration_required": True,
    }
    manifest = parse_manifest(payload)
    calibration = build_calibration_artifact(
        summary={
            "sample_count": 20,
            "median_slippage_vs_signal_bps": 1.0,
            "p90_slippage_vs_signal_bps": 2.0,
            "p95_slippage_vs_signal_bps": 3.0,
            "p95_submit_to_fill_ms": 100,
            "partial_fill_rate": 0.01,
            "unfilled_rate": 0.02,
            "model_breach_rate": 0.0,
            "quality_gate_status": "FAIL",
        },
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
        execution_calibration=calibration,
    )

    reasons = report["candidates"][0]["gate_fail_reasons"]
    assert report["gate_result"] == "FAIL"
    assert "execution_calibration_partial_fill_rate_exceeds_assumption" in reasons
    assert "execution_calibration_unfilled_rate_exceeds_assumption" in reasons
    assert "execution_calibration_sample_count_below_required" in reasons
    assert "execution_calibration_quality_gate_not_passed" in reasons


def test_research_backtest_aggregates_scenarios_and_promotion_refuses_failed_stress(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "scenario_aggregation_integration"
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [0.0],
        "order_failure_rate": [0.0, 1.0],
        "seed": 42,
    }
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["candidate_count"] == 1
    candidate = report["candidates"][0]
    assert candidate["scenario_policy"] == "must_pass_base_and_survive_stress"
    assert len(candidate["scenario_results"]) == 2
    assert [result["scenario_role"] for result in candidate["scenario_results"]] == ["base", "stress"]
    assert [result["scenario_role_source"] for result in candidate["scenario_results"]] == ["derived", "derived"]
    assert candidate["required_scenario_count"] == 2
    assert len(candidate["required_scenario_ids"]) == 2
    assert candidate["acceptance_gate_result"] == "FAIL"
    assert candidate["scenario_fail_count"] > 0
    assert report["gate_result"] == "FAIL"
    assert "scenario_policy_no_passing_stress_scenario" in candidate["gate_fail_reasons"]
    assert any(
        str(reason).startswith("scenario_policy_required_scenario_failed:")
        for reason in candidate["gate_fail_reasons"]
    )
    assert candidate["candidate_profile_hash"].startswith("sha256:")
    assert Path(report["artifact_paths"]["report_path"]).exists()

    with pytest.raises(PromotionGateError, match="scenario_policy"):
        promote_candidate(
            experiment_id="scenario_aggregation_integration",
            candidate_id=candidate["parameter_candidate_id"],
            manager=manager,
        )


def test_research_backtest_promotes_candidate_when_base_and_stress_pass(
    tmp_path, monkeypatch
) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["experiment_id"] = "scenario_aggregation_positive_integration"
    payload["acceptance_gate"]["max_mdd_pct"] = 99.9
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [0.0, 0.0],
        "order_failure_rate": [0.0],
        "seed": 42,
    }
    payload["execution_timing"] = {
        "fill_reference_policy": "next_candle_open",
        "allow_same_candle_close_fill": False,
    }
    manifest = parse_manifest(payload)

    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    assert report["candidate_count"] == 1
    assert report["gate_result"] == "PASS"
    candidate = report["candidates"][0]
    assert candidate["acceptance_gate_result"] == "PASS"
    assert candidate["scenario_policy"] == "must_pass_base_and_survive_stress"
    assert len(candidate["scenario_results"]) == 2
    assert candidate["scenario_pass_count"] == 2
    assert candidate["scenario_fail_count"] == 0
    assert candidate["required_scenario_count"] == 2
    assert [result["scenario_role"] for result in candidate["scenario_results"]] == ["base", "stress"]
    assert [result["scenario_role_source"] for result in candidate["scenario_results"]] == ["derived", "derived"]
    assert candidate["final_holdout_present"] is True
    assert candidate["final_holdout_metrics"]["trade_count"] is not None
    assert candidate["candidate_profile_hash"].startswith("sha256:")

    with pytest.raises(PromotionGateError, match="probe_grade_pass_not_promotable"):
        promote_candidate(
            experiment_id="scenario_aggregation_positive_integration",
            candidate_id=candidate["parameter_candidate_id"],
            manager=manager,
        )


def test_stress_report_is_candidate_order_independent(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["parameter_space"] = {
        "SMA_SHORT": [2, 3],
        "SMA_LONG": [4],
        "SMA_FILTER_GAP_MIN_RATIO": [0.0],
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
    }
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [5, 10],
        "partial_fill_rate": [0.5],
        "order_failure_rate": [0.1],
        "scenario_policy": "must_pass_base_and_survive_stress",
        "seed": 42,
    }
    reordered = dict(payload)
    reordered["parameter_space"] = {
        "SMA_SHORT": [3, 2],
        "SMA_LONG": [4],
        "SMA_FILTER_GAP_MIN_RATIO": [0.0],
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
    }
    target_params = {
        "SMA_SHORT": 2,
        "SMA_LONG": 4,
        "SMA_FILTER_GAP_MIN_RATIO": 0.0,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
    }
    target_id = candidate_id(target_params, 0)

    first = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=parse_manifest(reordered),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    first_candidate = {item["parameter_candidate_id"]: item for item in first["candidates"]}[target_id]
    second_candidate = {item["parameter_candidate_id"]: item for item in second["candidates"]}[target_id]
    for first_scenario, second_scenario in zip(
        first_candidate["scenario_results"],
        second_candidate["scenario_results"],
        strict=True,
    ):
        assert first_scenario["scenario_id"] == second_scenario["scenario_id"]
        assert first_scenario["validation_metrics"] == second_scenario["validation_metrics"]
        assert first_scenario["validation_execution_metadata"] == second_scenario["validation_execution_metadata"]
    execution = first_candidate["scenario_results"][0]["validation_execution_metadata"][0]
    assert execution["base_seed"] == 42
    assert execution["derived_seed_hash"].startswith("sha256:")
    assert execution["seed_derivation_inputs"]["parameter_candidate_id"] == target_id


def test_different_stress_seed_changes_auditable_seed_hash(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    manager = PathManager.from_env(Path.cwd())
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0],
        "slippage_bps": [5, 10],
        "partial_fill_rate": [0.5],
        "order_failure_rate": [0.1],
        "scenario_policy": "must_pass_base_and_survive_stress",
        "seed": 42,
    }
    changed_seed = dict(payload)
    changed_seed["execution_model"] = dict(payload["execution_model"])
    changed_seed["execution_model"]["seed"] = 43

    first = run_research_backtest(
        manifest=parse_manifest(payload),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    second = run_research_backtest(
        manifest=parse_manifest(changed_seed),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )

    first_execution = first["candidates"][0]["scenario_results"][0]["validation_execution_metadata"][0]
    second_execution = second["candidates"][0]["scenario_results"][0]["validation_execution_metadata"][0]
    assert first_execution["base_seed"] == 42
    assert second_execution["base_seed"] == 43
    assert first_execution["derived_seed_hash"] != second_execution["derived_seed_hash"]
