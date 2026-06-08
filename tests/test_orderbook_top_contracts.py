from __future__ import annotations

from bithumb_bot.execution_reality_contract import build_execution_capability_contract
from bithumb_bot.research.dataset_snapshot import Candle, DatasetQualityReport, DatasetSnapshot
from bithumb_bot.research.execution_timing import execution_reality_gate
from bithumb_bot.research.experiment_manifest import DateRange, ExecutionTimingPolicy
from bithumb_bot.research.validation_protocol import _top_of_book_quality_summary
from tests.test_orderbook_top_research import (
    _execution_test_params,
    _signal_dataset,
    build_dataset_quote,
)
from bithumb_bot.research.backtest_engine import run_sma_backtest


def test_optional_top_of_book_warning_summary_without_full_backtest() -> None:
    reports = {
        "train": DatasetQualityReport(
            {
                "content_hash": "sha256:" + "0" * 64,
                "quality_gate_status": "PASS",
                "top_of_book_requested": True,
                "top_of_book_required": False,
                "top_of_book_gate_status": "WARN",
                "top_of_book_expected_signal_count": 10,
                "top_of_book_joined_count": 0,
                "top_of_book_missing_count": 10,
                "top_of_book_coverage_pct": 0.0,
                "top_of_book_join_tolerance_ms": 3000,
                "top_of_book_source": "sqlite_orderbook_top_snapshots",
                "top_of_book_gate_reasons": ["top_of_book_missing"],
            }
        ),
        "validation": DatasetQualityReport(
            {
                "content_hash": "sha256:" + "1" * 64,
                "quality_gate_status": "PASS",
                "top_of_book_requested": True,
                "top_of_book_required": False,
                "top_of_book_gate_status": "WARN",
                "top_of_book_expected_signal_count": 10,
                "top_of_book_joined_count": 0,
                "top_of_book_missing_count": 10,
                "top_of_book_coverage_pct": 0.0,
                "top_of_book_join_tolerance_ms": 3000,
                "top_of_book_source": "sqlite_orderbook_top_snapshots",
                "top_of_book_gate_reasons": ["top_of_book_missing"],
            }
        ),
    }

    summary = _top_of_book_quality_summary(reports)

    assert summary["gate_status"] == "WARN"
    assert summary["coverage_pct"] == 0.0
    assert summary["missing_quote_count"] == 20
    assert [item["split_name"] for item in summary["affected_splits"]] == ["train", "validation"]
    assert summary["warning_code"] == "top_of_book_optional_coverage_warning"


def test_joined_top_of_book_metadata_contract_from_minimal_snapshot() -> None:
    base_ts = 1_700_000_000_000
    dataset = DatasetSnapshot(
        snapshot_id="unit",
        source="sqlite_candles",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=(
            Candle(ts=base_ts, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
            Candle(ts=base_ts + 60_000, open=99.0, high=100.0, low=98.0, close=99.0, volume=1.0),
            Candle(ts=base_ts + 120_000, open=101.0, high=102.0, low=100.0, close=101.0, volume=1.0),
            Candle(ts=base_ts + 180_000, open=98.0, high=99.0, low=97.0, close=98.0, volume=1.0),
            Candle(ts=base_ts + 240_000, open=102.0, high=103.0, low=101.0, close=102.0, volume=1.0),
        ),
        top_of_book_quotes=(
            None,
            None,
            None,
            build_dataset_quote(candle_ts=base_ts + 180_000, bid=99.0, ask=101.0),
            build_dataset_quote(candle_ts=base_ts + 240_000, bid=100.0, ask=102.0),
        ),
        top_of_book_event_quotes=(
            build_dataset_quote(candle_ts=base_ts + 240_000, bid=99.0, ask=101.0),
            build_dataset_quote(candle_ts=base_ts + 300_000, bid=100.0, ask=102.0),
        ),
        top_of_book_requested=True,
    )

    result = run_sma_backtest(
        dataset=dataset,
        parameter_values=_execution_test_params(),
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(
            fill_reference_policy="first_orderbook_after_decision",
            max_quote_wait_ms=1000,
            allow_same_candle_close_fill=False,
            source="test",
        ),
    )

    metadata = result.trades[0]["execution"]
    assert metadata["best_bid"] == 100.0
    assert metadata["best_ask"] == 102.0
    assert metadata["spread_bps"] > 0
    assert metadata["reference_price"] in {97.0, 98.0, 99.0, 100.0, 101.0, 102.0}


def test_l2_depth_capability_contract_without_full_backtest() -> None:
    contract = build_execution_capability_contract(
        fill_reference_policy="next_candle_open",
        top_of_book_available=False,
        l2_depth_snapshot_available=True,
        full_orderbook_depth_available=False,
        evidence_tier="candle_close_optimistic",
    )

    capability = contract["available_capabilities"]
    assert capability["l2_depth_snapshot"] is True
    assert capability["full_orderbook_depth"] is False
    assert capability["queue_position"] is False
    assert capability["trade_ticks"] is False
    assert capability["market_impact_model"] is False
    assert capability["intra_candle_path_reconstruction"] is False


def test_missing_quote_policy_warning_reason_mapping() -> None:
    policy = ExecutionTimingPolicy(
        fill_reference_policy="first_orderbook_after_decision",
        max_quote_wait_ms=1000,
        missing_quote_policy="warn",
        allow_same_candle_close_fill=False,
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
        source="test",
    )
    result = run_sma_backtest(
        dataset=_signal_dataset(base_ts=1_700_000_000_000, quotes=()),
        parameter_values=_execution_test_params(),
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=policy,
    )

    execution = result.trades[0]["execution"]
    gate = execution_reality_gate(
        policy=policy,
        observed_levels=[execution["execution_reality_level"]],
        fill_reference_sources=[execution["fill_reference_source"]],
        quote_coverage_pct=0.0,
    )
    assert execution["fill_status"] == "skipped_with_warning"
    assert execution["execution_reference_failure_reason"] == "missing_quote_warning"
    assert gate["status"] == "FAIL"
    assert "quote_after_decision_signal_coverage_below_threshold" in gate["reasons"]
