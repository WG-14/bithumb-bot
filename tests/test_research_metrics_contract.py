from __future__ import annotations

import math

import pytest

from bithumb_bot.research.metrics_contract import (
    ClosedTradeRecord,
    EquityPoint,
    ExecutionRecord,
    PositionInterval,
    build_metrics_v2,
)


def _point(ts: int, equity: float, cash: float | None = None, qty: float = 0.0) -> EquityPoint:
    return EquityPoint(ts=ts, equity=equity, cash=equity if cash is None else cash, asset_qty=qty)


def test_same_total_return_over_different_periods_has_different_cagr() -> None:
    month = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1100.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(30 * 86_400_000, 1100.0)),
        position_intervals=(),
        closed_trades=(),
        execution_records=(),
    )
    year = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1100.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(365 * 86_400_000, 1100.0)),
        position_intervals=(),
        closed_trades=(),
        execution_records=(),
    )

    assert month.return_risk.total_return_pct == pytest.approx(10.0)
    assert year.return_risk.total_return_pct == pytest.approx(10.0)
    assert month.return_risk.cagr_pct is not None
    assert year.return_risk.cagr_pct is not None
    assert month.return_risk.cagr_pct > year.return_risk.cagr_pct


def test_same_return_with_different_exposure_reports_different_time_in_market() -> None:
    full = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1100.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(1000, 1100.0)),
        position_intervals=(PositionInterval(open_ts=0, close_ts=1000),),
        closed_trades=(),
        execution_records=(),
    )
    tenth = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1100.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(1000, 1100.0)),
        position_intervals=(PositionInterval(open_ts=0, close_ts=100),),
        closed_trades=(),
        execution_records=(),
    )

    assert full.time_exposure.exposure_time_pct == 100.0
    assert tenth.time_exposure.exposure_time_pct == 10.0


def test_open_position_at_end_separates_unrealized_and_closed_trade_stats() -> None:
    metrics = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=100.0,
        final_asset_qty=10.0,
        final_mark_price=120.0,
        final_open_cost_basis=900.0,
        equity_curve=(_point(0, 1000.0), _point(1000, 1300.0, cash=100.0, qty=10.0)),
        position_intervals=(PositionInterval(open_ts=100, close_ts=None),),
        closed_trades=(),
        execution_records=(ExecutionRecord(side="BUY", status="filled", filled_qty=10.0, price=90.0),),
    )

    assert metrics.return_risk.open_position_at_end is True
    assert metrics.return_risk.unrealized_pnl_end == 300.0
    assert metrics.trade_quality.closed_trade_count == 0
    assert metrics.time_exposure.avg_holding_time_ms is None
    assert "open_position_excluded_from_holding_time_stats" in metrics.limitation_reasons


def test_trade_quality_metrics_are_calculated_from_closed_trades() -> None:
    closed = tuple(
        [ClosedTradeRecord(exit_ts=index, net_pnl=10.0, return_pct=1.0) for index in range(10)]
        + [ClosedTradeRecord(exit_ts=11, net_pnl=-80.0, return_pct=-8.0)]
    )

    metrics = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1020.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(1000, 1020.0)),
        position_intervals=(),
        closed_trades=closed,
        execution_records=(),
    )

    assert metrics.trade_quality.profit_factor == 1.25
    assert metrics.trade_quality.expectancy_per_trade_krw == 20.0 / 11.0
    assert metrics.trade_quality.expectancy_per_trade_pct == (10.0 - 8.0) / 11.0
    assert metrics.trade_quality.payoff_ratio == 0.125
    assert metrics.trade_quality.max_consecutive_losses == 1
    assert metrics.trade_quality.single_trade_dependency_score == 80.0 / 180.0


def test_cost_drag_uses_total_traded_notional_denominator() -> None:
    zero_cost = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1000.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(1000, 1000.0)),
        position_intervals=(),
        closed_trades=(),
        execution_records=(ExecutionRecord(side="BUY", status="filled", filled_qty=1.0, price=1000.0),),
    )
    stressed = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=990.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(1000, 990.0)),
        position_intervals=(),
        closed_trades=(),
        execution_records=(
            ExecutionRecord(side="BUY", status="filled", filled_qty=1.0, price=1000.0, fee=4.0, slippage=6.0),
        ),
    )

    assert zero_cost.cost_execution.fee_drag_ratio == 0.0
    assert zero_cost.cost_execution.slippage_drag_ratio == 0.0
    assert stressed.cost_execution.fee_total == 4.0
    assert stressed.cost_execution.slippage_total == 6.0
    assert stressed.cost_execution.fee_drag_ratio == 0.004
    assert stressed.cost_execution.slippage_drag_ratio == 0.006


def test_execution_count_is_separate_from_closed_trade_count() -> None:
    metrics = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1010.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(_point(0, 1000.0), _point(1000, 1010.0)),
        position_intervals=(),
        closed_trades=(ClosedTradeRecord(exit_ts=500, net_pnl=10.0, return_pct=1.0),),
        execution_records=(
            ExecutionRecord(side="BUY", status="filled", filled_qty=1.0, price=100.0),
            ExecutionRecord(side="SELL", status="partial", filled_qty=0.5, price=110.0),
            ExecutionRecord(side="SELL", status="failed", filled_qty=0.0, price=None),
            ExecutionRecord(side="BUY", status="skipped", filled_qty=0.0, price=None),
        ),
    )

    assert metrics.trade_quality.execution_count == 4
    assert metrics.trade_quality.closed_trade_count == 1
    assert metrics.cost_execution.filled_execution_count == 2
    assert metrics.cost_execution.partial_fill_count == 1
    assert metrics.cost_execution.failed_execution_count == 1
    assert metrics.cost_execution.skipped_execution_count == 1
    assert math.isclose(metrics.cost_execution.quote_coverage_pct or 0.0, 0.0)
