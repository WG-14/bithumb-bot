from __future__ import annotations

import pytest

from bithumb_bot.research import backtest_support as support
from bithumb_bot.research.execution_model import ExecutionFill
from bithumb_bot.research.portfolio_ledger import PortfolioLedger


def _fill(
    *,
    side: str,
    signal_ts: int = 100,
    effective_ts: int = 100,
    qty: float = 1.0,
    price: float = 100.0,
    fee: float = 1.0,
    status: str = "filled",
) -> ExecutionFill:
    return ExecutionFill(
        signal_ts=signal_ts,
        decision_ts=signal_ts,
        submit_ts_assumption=signal_ts,
        side=side,
        order_type="market",
        reference_price=price,
        fill_reference_ts=effective_ts,
        requested_qty=qty,
        filled_qty=qty,
        remaining_qty=0.0,
        avg_fill_price=price if status != "failed" else None,
        fee=fee,
        fill_status=status,
        model_name="unit",
        model_version="unit",
        model_params_hash="sha256:unit",
    )


def _pending(fill: ExecutionFill, *, trade_index: int, effective_ts: int | None = None) -> support.PendingFill:
    side = str(fill.side).upper()
    cash_delta = -(float(fill.avg_fill_price or 0.0) * fill.filled_qty + fill.fee)
    if side == "SELL":
        cash_delta = float(fill.avg_fill_price or 0.0) * fill.filled_qty - fill.fee
    return support.PendingFill(
        fill=fill,
        trade_index=trade_index,
        side=side,
        effective_ts=int(effective_ts if effective_ts is not None else fill.fill_reference_ts or fill.signal_ts),
        qty=float(fill.filled_qty),
        fee=float(fill.fee),
        slippage=0.0,
        cash_delta=cash_delta,
    )


def test_portfolio_ledger_applies_buy_fill() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    fill = _fill(side="BUY", qty=2.0, price=100.0, fee=1.0)
    ledger.record_pending_fill(_pending(fill, trade_index=0), {"side": "BUY"})

    ledger.apply_pending_fills(100)

    assert ledger.cash == 799.0
    assert ledger.qty == 2.0
    assert ledger.entry_cost_basis == 201.0
    assert ledger.export_trades()[0]["is_portfolio_applied_trade"] is True


def test_portfolio_ledger_applies_sell_fill_and_closes_position() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    buy = _fill(side="BUY", qty=2.0, price=100.0, fee=0.0)
    ledger.record_pending_fill(_pending(buy, trade_index=0), {"side": "BUY"})
    ledger.apply_pending_fills(100)
    sell = _fill(side="SELL", signal_ts=200, qty=2.0, price=110.0, fee=1.0)
    ledger.record_pending_fill(_pending(sell, trade_index=1), {"side": "SELL"})

    ledger.apply_pending_fills(200)

    assert ledger.cash == 1_019.0
    assert ledger.qty == 0.0
    assert ledger.closed_pnls == [19.0]
    assert ledger.entry_ts is None


def test_portfolio_ledger_records_failed_fill_without_mutating_position() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)

    ledger.record_failed_fill(_fill(side="BUY", status="failed", qty=0.0, fee=0.0))

    assert ledger.cash == 1_000.0
    assert ledger.qty == 0.0
    assert ledger.export_trades()[0]["is_execution_filled"] is False


def test_portfolio_ledger_partial_sell_preserves_remaining_cost_basis() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    ledger.record_pending_fill(_pending(_fill(side="BUY", qty=2.0, price=100.0, fee=0.0), trade_index=0), {"side": "BUY"})
    ledger.apply_pending_fills(100)
    ledger.record_pending_fill(
        _pending(_fill(side="SELL", signal_ts=200, qty=1.0, price=120.0, fee=0.0, status="partial"), trade_index=1),
        {"side": "SELL"},
    )

    ledger.apply_pending_fills(200)

    assert ledger.qty == 1.0
    assert ledger.entry_cost_basis == 100.0
    assert ledger.closed_pnls == [20.0]


def test_portfolio_ledger_delayed_fill_waits_until_boundary() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    fill = _fill(side="BUY", qty=1.0, price=100.0, fee=0.0, effective_ts=200)
    ledger.record_pending_fill(_pending(fill, trade_index=0, effective_ts=200), {"side": "BUY"})

    ledger.apply_pending_fills(199)
    assert ledger.qty == 0.0

    ledger.apply_pending_fills(200)
    assert ledger.qty == 1.0


def test_portfolio_ledger_same_candle_close_fill_behavior() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    fill = _fill(side="BUY", qty=1.0, price=100.0, fee=0.0, effective_ts=100)
    pending = _pending(fill, trade_index=0, effective_ts=100)
    ledger.record_pending_fill(pending, {"side": "BUY"})

    ledger.apply_pending_fills(100)

    assert ledger.qty == 1.0


def test_portfolio_ledger_final_pending_fill_marking() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    fill = _fill(side="BUY", qty=1.0, price=100.0, fee=0.0, effective_ts=200)
    ledger.record_pending_fill(_pending(fill, trade_index=0, effective_ts=200), {"side": "BUY"})

    ledger.finalize(last_mark_ts=100, last_price=100.0)

    assert ledger.qty == 0.0
    assert ledger.export_trades()[0]["is_portfolio_applied_trade"] is False


def test_portfolio_ledger_high_level_tick_api_owns_mark_projection() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    fill = _fill(side="BUY", qty=1.0, price=100.0, fee=0.0, effective_ts=100)
    pending = _pending(fill, trade_index=0, effective_ts=100)
    outcome = type(
        "Outcome",
        (),
        {
            "fill": fill,
            "pending_fill": pending,
            "trade": {"side": "BUY"},
            "mark_cash_delta": pending.cash_delta,
            "mark_qty_delta": pending.qty,
        },
    )()

    tick = ledger.begin_tick(mark_boundary_ts=101, decision_boundary_ts=100, candle_ts=0, close=100.0)
    applied = ledger.apply_execution_outcome(
        outcome,
        mark_boundary_ts=101,
        mark_cash=tick.mark_cash,
        mark_qty=tick.mark_qty,
    )
    ledger.mark_tick_equity(ts=100, mark_price=100.0, tick_state=applied)

    assert applied.fill_applied_to_mark is True
    assert applied.mark_cash == 900.0
    assert applied.mark_qty == 1.0
    assert ledger.export_equity_curve()[-1].equity == 1_000.0


def test_portfolio_ledger_randomized_fill_sequence_invariants() -> None:
    ledger = PortfolioLedger.create(starting_cash=1_000.0)
    expected_cash = 1_000.0
    expected_qty = 0.0
    for index in range(20):
        if index % 3 == 0 or expected_qty <= 0.0:
            fill = _fill(side="BUY", signal_ts=index, effective_ts=index, qty=0.1, price=100.0, fee=0.01)
            expected_cash -= 10.01
            expected_qty += 0.1
        else:
            sell_qty = min(0.05, expected_qty)
            fill = _fill(side="SELL", signal_ts=index, effective_ts=index, qty=sell_qty, price=101.0, fee=0.01)
            expected_cash += sell_qty * 101.0 - 0.01
            expected_qty -= sell_qty
        ledger.record_pending_fill(_pending(fill, trade_index=index), {"side": fill.side})
        ledger.apply_pending_fills(index)

        assert ledger.cash == pytest.approx(expected_cash)
        assert ledger.qty == pytest.approx(max(0.0, expected_qty))
        assert ledger.cash >= 0.0
        assert ledger.qty >= 0.0
