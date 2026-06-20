from __future__ import annotations

import pytest

from bithumb_bot.quantity_contracts import build_quantity_semantics


def test_quantity_semantics_distinguishes_exchange_sellable_from_strategy_dust() -> None:
    result = build_quantity_semantics(
        broker_position_qty=0.00019998,
        exchange_min_qty=0.0001,
        strategy_internal_lot_size=0.0004,
        target_delta_closeout_authorized=True,
    )

    assert result.exchange_sellable is True
    assert result.strategy_executable_lot_count == 0
    assert result.strategy_dust_qty == pytest.approx(0.00019998)
    assert result.target_delta_closeable is True


def test_quantity_semantics_marks_sub_lot_target_delta_closeable() -> None:
    result = build_quantity_semantics(
        broker_position_qty=0.00019998,
        exchange_min_qty=0.0001,
        strategy_internal_lot_size=0.0004,
        target_delta_closeout_authorized=True,
        terminal_closeout_covered_qty=0.00019998,
    )

    assert result.exchange_sellable is True
    assert result.strategy_executable_lot_count == 0
    assert result.target_delta_closeable is True


def test_quantity_boundary_matrix_min_qty_and_internal_lot_size() -> None:
    below_exchange = build_quantity_semantics(
        broker_position_qty=0.00009999,
        exchange_min_qty=0.0001,
        strategy_internal_lot_size=0.0004,
        target_delta_closeout_authorized=True,
    )
    sub_lot_sellable = build_quantity_semantics(
        broker_position_qty=0.00019998,
        exchange_min_qty=0.0001,
        strategy_internal_lot_size=0.0004,
        target_delta_closeout_authorized=True,
    )
    full_lot = build_quantity_semantics(
        broker_position_qty=0.0004,
        exchange_min_qty=0.0001,
        strategy_internal_lot_size=0.0004,
        target_delta_closeout_authorized=False,
    )

    assert below_exchange.exchange_sellable is False
    assert below_exchange.target_delta_closeable is False
    assert sub_lot_sellable.exchange_sellable is True
    assert sub_lot_sellable.strategy_executable_lot_count == 0
    assert full_lot.strategy_executable_lot_count == 1
