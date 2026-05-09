from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.canonical_decision import export_research_decisions, export_runtime_replay_decisions
from bithumb_bot.decision_equivalence import compare_decision_equivalence
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.execution_calibration import build_calibration_artifact
from bithumb_bot.research.execution_model import FixedBpsExecutionModel, StressExecutionModel
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy, parse_manifest
from bithumb_bot.research.parameter_space import candidate_id
from bithumb_bot.research.promotion_gate import PromotionGateError, promote_candidate
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
    assert first["candidates"][0]["market_regime_bucket_performance"]
    assert first["candidates"][0]["market_regime_coverage"]
    assert "regime_gate_result" in first["candidates"][0]
    assert Path(first["artifact_paths"]["report_path"]).exists()


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

    result = promote_candidate(
        experiment_id="scenario_aggregation_positive_integration",
        candidate_id=candidate["parameter_candidate_id"],
        manager=manager,
    )

    assert result.artifact["gate_result"] == "PASS"
    assert result.artifact["scenario_policy"] == "must_pass_base_and_survive_stress"
    assert result.artifact["scenario_pass_count"] == 2
    assert result.artifact["scenario_fail_count"] == 0
    assert result.artifact["candidate_profile_hash"] == candidate["candidate_profile_hash"]


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
