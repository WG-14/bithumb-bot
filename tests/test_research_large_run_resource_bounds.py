from __future__ import annotations

import pytest

from bithumb_bot.research.backtest_engine import BacktestResourceLimits, BacktestRunContext
from bithumb_bot.research.backtest_kernel import run_decision_event_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange, ExecutionTimingPolicy, legacy_research_portfolio_policy
from bithumb_bot.research.validation_protocol import run_research_backtest
from bithumb_bot.strategy_plugins.channel_breakout_research import (
    build_channel_breakout_research_events,
    materialize_channel_breakout_parameters,
    CHANNEL_BREAKOUT_WITH_REGIME_FILTER_PLUGIN,
)
from tests.factories.research_reports import DeterministicResearchEvaluator
from tests.test_research_backtest_reproducibility import _create_db, _research_manager
from tests.test_research_memory_admission import _manifest_with_workers


def _channel_breakout_dataset(count: int = 10_000) -> DatasetSnapshot:
    candles = []
    price = 100.0
    for index in range(count):
        if index and index % 200 == 0:
            close = price + 6.0
            high = close + 2.0
            low = price - 0.5
            volume = 250.0
        elif index and index % 200 == 1:
            close = price + 5.5
            high = close + 1.0
            low = price + 1.0
            volume = 220.0
        else:
            close = price + (index % 5) * 0.05
            high = close + 0.4
            low = close - 0.4
            volume = 100.0
        candles.append(Candle(index * 60_000, price, high, low, close, volume))
        price = close
    return DatasetSnapshot(
        snapshot_id="channel_breakout_large",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-08"),
        candles=tuple(candles),
    )


def _channel_params(**overrides: object) -> dict[str, object]:
    values = {
        "CHANNEL_BREAKOUT_LOOKBACK": 20,
        "CHANNEL_BREAKOUT_RANGE_WINDOW": 20,
        "CHANNEL_BREAKOUT_RANGE_RATIO_MIN": 0.0,
        "CHANNEL_BREAKOUT_VOLUME_WINDOW": 20,
        "CHANNEL_BREAKOUT_VOLUME_RATIO_MIN": 0.0,
        "CHANNEL_BREAKOUT_REGIME_FILTER_ENABLED": False,
    }
    values.update(overrides)
    return materialize_channel_breakout_parameters(
        plugin=CHANNEL_BREAKOUT_WITH_REGIME_FILTER_PLUGIN,
        parameter_values=values,
        fee_rate=0.001,
        slippage_bps=0.0,
    )


def _run_channel_breakout_large(*, params: dict[str, object], context: BacktestRunContext):
    dataset = _channel_breakout_dataset()
    events = build_channel_breakout_research_events(
        dataset=dataset,
        parameter_values=params,
        fee_rate=0.001,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(decision_guard_ms=0),
        portfolio_policy=legacy_research_portfolio_policy(),
    )
    return run_decision_event_backtest(
        dataset=dataset,
        strategy_name="channel_breakout_with_regime_filter",
        parameter_values=params,
        fee_rate=0.001,
        slippage_bps=0.0,
        decision_events=events,
        context=context,
    )


@pytest.mark.resource_guard
@pytest.mark.memory_sensitive
def test_channel_breakout_summary_mode_large_run_keeps_bounded_observability() -> None:
    result = _run_channel_breakout_large(
        params=_channel_params(ENTRY_MODE="immediate_breakout"),
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    usage = result.resource_usage
    assert usage["retained_decision_count"] == 0
    assert usage["stage_trace_count"] >= 10_000
    assert len(usage.get("stage_trace", ())) <= usage["stage_trace_max_retained_traces"]
    assert usage["behavior_hash_material_count"] == 10_000
    assert usage["behavior_hash"].startswith("sha256:")
    assert usage["stage_trace_hash"].startswith("sha256:")


@pytest.mark.resource_guard
@pytest.mark.memory_sensitive
def test_delayed_confirmation_large_run_does_not_retain_per_tick_feature_payloads() -> None:
    result = _run_channel_breakout_large(
        params=_channel_params(
            ENTRY_MODE="delayed_confirmation",
            CONFIRMATION_WINDOW_MIN=3,
            PULLBACK_RATIO=0.05,
        ),
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    assert result.decisions == ()
    assert result.resource_usage["behavior_hash_material_sample_count"] < 100
    assert result.resource_usage["strategy_behavior_hash"].startswith("sha256:")


@pytest.mark.resource_guard
@pytest.mark.memory_sensitive
def test_large_run_resource_usage_reports_evidence_counts_and_hashes() -> None:
    result = _run_channel_breakout_large(
        params=_channel_params(ENTRY_MODE="immediate_breakout"),
        context=BacktestRunContext(
            report_detail="summary",
            resource_limits=BacktestResourceLimits(max_decisions_retained=0, max_equity_points_retained=0),
        ),
    )

    usage = result.resource_usage
    assert usage["decision_hash_material_count"] == 10_000
    assert usage["behavior_hash_material_count"] == 10_000
    assert usage["behavior_hash"].startswith("sha256:")
    assert usage["stage_trace_hash"].startswith("sha256:")


@pytest.mark.nightly
@pytest.mark.memory_sensitive
def test_nightly_large_run_workers_8_uses_memory_admission_or_batches(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "research.sqlite"
    _create_db(db_path)
    manager = _research_manager(tmp_path, monkeypatch)
    report = run_research_backtest(
        manifest=_manifest_with_workers(
            8,
            entry_modes=["immediate_breakout", "delayed_confirmation"],
            max_total_memory_mb=1.0,
            memory_admission_policy="cap_workers",
        ),
        db_path=db_path,
        manager=manager,
        candidate_evaluator=DeterministicResearchEvaluator(),
    )

    admission = report["execution_observability"]["memory_admission"]
    assert admission["safe_max_workers_by_memory_budget"] < 8
    assert admission["status"] == "WARN"
    assert report["execution_observability"]["research_max_workers_effective"] <= admission[
        "safe_max_workers_by_memory_budget"
    ]
