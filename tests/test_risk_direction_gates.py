from __future__ import annotations

from bithumb_bot.risk_direction_gates import evaluate_risk_direction_gates


def test_fee_pending_blocks_new_buy_but_not_authorized_terminal_flat_sell() -> None:
    buy = evaluate_risk_direction_gates(
        fee_pending=True,
        side="BUY",
        broker_qty=0.0002,
        requested_qty=0.0002,
    )
    sell = evaluate_risk_direction_gates(
        fee_pending=True,
        side="SELL",
        broker_qty=0.0002,
        requested_qty=0.0002,
        terminal_flat_authority=True,
    )

    assert buy.exposure_increase_allowed is False
    assert buy.strategy_new_cycle_allowed is False
    assert sell.terminal_flat_closeout_allowed is True
    assert sell.exposure_increase_allowed is False


def test_fee_pending_blocks_unbounded_or_non_authorized_sell() -> None:
    result = evaluate_risk_direction_gates(
        fee_pending=True,
        side="SELL",
        broker_qty=0.0002,
        requested_qty=0.0002,
        terminal_flat_authority=False,
    )

    assert result.terminal_flat_closeout_allowed is False
    assert result.risk_reducing_sell_allowed is False
    assert result.reason_code == "fee_pending_blocks_unauthorized_sell"


def test_risk_reducing_sell_requires_broker_qty_and_no_open_orders() -> None:
    no_broker_qty = evaluate_risk_direction_gates(
        fee_pending=True,
        side="SELL",
        broker_qty=None,
        requested_qty=0.0002,
        terminal_flat_authority=True,
    )
    open_order = evaluate_risk_direction_gates(
        fee_pending=True,
        side="SELL",
        broker_qty=0.0002,
        requested_qty=0.0002,
        terminal_flat_authority=True,
        open_order_count=1,
    )
    too_large = evaluate_risk_direction_gates(
        fee_pending=True,
        side="SELL",
        broker_qty=0.0002,
        requested_qty=0.0003,
        terminal_flat_authority=True,
    )

    assert no_broker_qty.terminal_flat_closeout_allowed is False
    assert open_order.terminal_flat_closeout_allowed is False
    assert too_large.terminal_flat_closeout_allowed is False
