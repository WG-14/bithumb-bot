from __future__ import annotations

import inspect

from bithumb_bot.runtime.cycle_pipeline import RuntimeCyclePipeline
from bithumb_bot.runtime.data_cycle_preflight import RuntimeDataCyclePreflight
from bithumb_bot.runtime.decision_coordinator import DecisionCoordinator


def test_data_cycle_preflight_reports_closed_candle_and_strategy_scope_hash() -> None:
    preflight = RuntimeDataCyclePreflight(
        status="PASS",
        reason_code=None,
        latest_candle_ts=100,
        latest_close=10.0,
        closed_candle_ts=100,
        incomplete_candle_ts=160,
        candle_age_sec=2.0,
        stale_cutoff_sec=120,
        closed_candle_allowed=True,
        runtime_data_availability_report_hash="sha256:availability",
        coverage_by_scope={"scope": {"candles": {"status": "PASS"}}},
        selected_candle_by_scope={"scope": {"selected_ts": 100}},
        freshness_by_scope={"scope": {"status": "PASS"}},
    )

    payload = preflight.as_dict()
    assert payload["closed_candle_allowed"] is True
    assert payload["runtime_data_availability_report_hash"] == "sha256:availability"
    assert payload["coverage_by_scope"] == {"scope": {"candles": {"status": "PASS"}}}
    assert payload["decision_hash"].startswith("sha256:")


def test_stale_candle_blocks_before_decision_gateway() -> None:
    source = inspect.getsource(RuntimeCyclePipeline.run_once)
    assert source.index('preflight.reason_code == "stale_candle_detected"') < source.index(".decide_cycle(")


def test_strategy_scope_preflight_failure_returns_single_reason_code() -> None:
    source = inspect.getsource(RuntimeCyclePipeline.run_once)
    assert 'preflight.reason_code == "runtime_data_preflight_failed"' in source
    assert "skip:runtime_data_preflight_failed" in source


def test_decision_bundle_references_cycle_preflight_hash() -> None:
    source = inspect.getsource(DecisionCoordinator.decide_cycle)
    assert "runtime_data_cycle_preflight_hash" in source
    assert "runtime_data_availability_report_hash" in source


def test_pipeline_shell_does_not_calculate_stale_candle_cutoff() -> None:
    source = inspect.getsource(RuntimeCyclePipeline.run_once)
    assert "candle_age_sec > stale_cutoff_sec" not in source
    assert "c.candle_reader(" not in source
    assert "c.closed_candle_selector(" not in source
    assert "runtime_state.set_last_candle_observation(" not in source
