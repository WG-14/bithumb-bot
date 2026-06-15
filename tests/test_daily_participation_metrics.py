from __future__ import annotations

from bithumb_bot.research.metrics_contract import (
    ClosedTradeRecord,
    ExecutionRecord,
    build_metrics_v2,
    build_participation_metrics,
)
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.validation_protocol import _backtest_context
from tests.test_daily_participation_sma_plugin import _manifest as _daily_manifest


def test_participation_metrics_count_kst_days() -> None:
    metrics = build_participation_metrics(
        period_start_ts=1_704_031_200_000,
        period_end_ts=1_704_117_600_000,
        execution_records=(ExecutionRecord("BUY", "filled", 1.0, 100.0, ts=1_704_031_200_000),),
    )

    assert metrics.calendar_day_count == 2
    assert metrics.days_with_filled_execution == 1


def test_zero_trade_days_detected() -> None:
    metrics = build_participation_metrics(
        period_start_ts=1_704_031_200_000,
        period_end_ts=1_704_204_000_000,
        execution_records=(ExecutionRecord("BUY", "filled", 1.0, 100.0, ts=1_704_031_200_000),),
    )

    assert metrics.zero_filled_days == 2
    assert metrics.max_consecutive_zero_filled_days == 2


def test_one_day_many_trades_does_not_satisfy_all_days() -> None:
    metrics = build_participation_metrics(
        period_start_ts=1_704_031_200_000,
        period_end_ts=1_704_204_000_000,
        execution_records=tuple(
            ExecutionRecord("BUY", "filled", 1.0, 100.0, ts=1_704_031_200_000 + index * 60_000)
            for index in range(30)
        ),
    )

    assert metrics.calendar_day_count == 3
    assert metrics.days_with_filled_execution == 1
    assert metrics.zero_filled_days == 2


def test_participation_metrics_include_count_basis_breakdown() -> None:
    metrics = build_participation_metrics(
        period_start_ts=1_704_031_200_000,
        period_end_ts=1_704_117_600_000,
        decision_records=({"decision_ts": 1_704_031_200_000, "final_signal": "BUY"},),
        execution_records=(ExecutionRecord("BUY", "filled", 1.0, 100.0, ts=1_704_031_200_000),),
        closed_trades=(ClosedTradeRecord(exit_ts=1_704_117_600_000, net_pnl=1.0),),
        count_basis="filled",
    )

    payload = metrics.as_dict()
    assert payload["days_with_intent"] == 1
    assert payload["days_with_submitted"] == 1
    assert payload["days_with_filled_execution"] == 1
    assert payload["days_with_closed_trade"] == 1


def test_fallback_filled_count_uses_entry_signal_source() -> None:
    metrics = build_participation_metrics(
        period_start_ts=1_704_031_200_000,
        period_end_ts=1_704_031_200_000,
        execution_records=(
            ExecutionRecord(
                "BUY",
                "filled",
                1.0,
                100.0,
                ts=1_704_031_200_000,
                entry_signal_source="daily_participation_fallback",
            ),
            ExecutionRecord("BUY", "filled", 1.0, 100.0, ts=1_704_031_200_000),
        ),
    )

    assert metrics.fallback_filled_count == 1


def test_manifest_participation_count_basis_is_used_in_metrics_v2() -> None:
    metrics = build_metrics_v2(
        starting_cash=1_000_000.0,
        final_cash=1_000_000.0,
        final_asset_qty=0.0,
        final_mark_price=100.0,
        equity_curve=(),
        position_intervals=(),
        closed_trades=(),
        execution_records=(),
        participation_count_basis="intent",
    )

    assert metrics.as_dict()["participation"]["count_basis"] == "intent"


def test_acceptance_gate_participation_count_basis_flows_to_backtest_context() -> None:
    payload = _daily_manifest()
    payload["acceptance_gate"]["participation_count_basis"] = "intent"  # type: ignore[index]
    payload["parameter_space"]["DAILY_PARTICIPATION_COUNT_BASIS"] = ["intent"]  # type: ignore[index]
    manifest = parse_manifest(payload)

    context = _backtest_context(
        manifest=manifest,
        manager=None,
        candidate_id="candidate",
        scenario_id="base",
        scenario_index=0,
        split_name="validation",
        dataset_content_hash="sha256:dataset",
        parameter_values={"DAILY_PARTICIPATION_COUNT_BASIS": "intent"},
        progress_callback=None,
    )

    assert context.participation_count_basis == "intent"


def test_acceptance_gate_participation_count_basis_defaults_to_filled_in_backtest_context() -> None:
    manifest = parse_manifest(_daily_manifest())

    context = _backtest_context(
        manifest=manifest,
        manager=None,
        candidate_id="candidate",
        scenario_id="base",
        scenario_index=0,
        split_name="validation",
        dataset_content_hash="sha256:dataset",
        parameter_values={},
        progress_callback=None,
    )

    assert context.participation_count_basis == "filled"
