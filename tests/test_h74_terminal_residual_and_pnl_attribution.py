from __future__ import annotations

import pytest

from bithumb_bot.h74_pnl_attribution import build_pnl_attribution, build_terminal_residual, pnl_attribution_passes


def test_terminal_true_dust_is_reported_with_notional_and_origin() -> None:
    residual = build_terminal_residual(residual_qty=0.00001, residual_mark_price=100_000_000, origin_cycle_id="cycle-1", allow_true_dust_next_cycle=True)

    assert residual["residual_class"] == "EXCHANGE_TRUE_DUST"
    assert residual["residual_notional_krw"] == pytest.approx(1000)
    assert residual["origin_cycle_id"] == "cycle-1"


def test_executable_residual_blocks_cycle_success_and_next_cycle() -> None:
    residual = build_terminal_residual(residual_qty=0.0001, residual_mark_price=100_000_000, origin_cycle_id="cycle-1", allow_true_dust_next_cycle=True)

    assert residual["exchange_sellable"] is True
    assert residual["next_cycle_allowed"] is False


def test_pnl_attribution_sums_to_live_minus_backtest_delta() -> None:
    attribution = build_pnl_attribution(
        backtest_expected_entry_price=100,
        live_entry_avg_price=101,
        backtest_expected_exit_price=110,
        live_exit_avg_price=111,
        qty=1,
        fee_delta_krw=1,
        slippage_delta_krw=2,
        spread_or_price_path_delta_krw=3,
        rounding_delta_krw=4,
        residual_mark_to_market_krw=5,
        live_minus_backtest_delta_krw=15,
    )

    assert attribution["unexplained_delta_krw"] == 0
    assert pnl_attribution_passes(attribution)


def test_unexplained_delta_above_tolerance_blocks_success() -> None:
    attribution = build_pnl_attribution(
        backtest_expected_entry_price=100,
        live_entry_avg_price=101,
        backtest_expected_exit_price=110,
        live_exit_avg_price=111,
        qty=1,
        live_minus_backtest_delta_krw=20,
    )

    assert pnl_attribution_passes(attribution, tolerance_krw=1.0) is False


def test_unexplained_delta_above_tolerance_blocks_cycle_success() -> None:
    attribution = build_pnl_attribution(
        backtest_expected_entry_price=100,
        live_entry_avg_price=101,
        backtest_expected_exit_price=110,
        live_exit_avg_price=111,
        qty=1,
        fee_delta_krw=0,
        slippage_delta_krw=0,
        spread_or_price_path_delta_krw=0,
        rounding_delta_krw=0,
        residual_mark_to_market_krw=0,
        live_minus_backtest_delta_krw=25,
    )
    cycle_success = pnl_attribution_passes(attribution, tolerance_krw=1.0)

    assert cycle_success is False
