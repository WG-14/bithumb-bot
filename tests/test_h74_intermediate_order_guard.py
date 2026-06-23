from __future__ import annotations

from bithumb_bot.h74_cycle_classification import classify_h74_cycle
from bithumb_bot.live_trade_classification import h74_performance_samples
from tests.test_h74_cycle_validation_success import _entry, _exit


def test_target_delta_rebalance_buy_between_entry_and_exit_fails_cycle() -> None:
    result = classify_h74_cycle(entry=_entry(), exit=_exit(), terminal={"terminal_executable_qty": 0}, orders=[{"client_order_id": "buy2", "cycle_id": "cycle-1", "side": "BUY", "created_ts": 1_001_000}])

    assert result.h74_cycle_validation_success is False


def test_target_delta_rebalance_sell_between_entry_and_exit_fails_cycle() -> None:
    result = classify_h74_cycle(entry=_entry(), exit=_exit(), terminal={"terminal_executable_qty": 0}, orders=[{"client_order_id": "sell2", "cycle_id": "cycle-1", "side": "SELL", "created_ts": 1_001_000}])

    assert result.h74_cycle_validation_success is False


def test_partial_fill_same_entry_order_is_not_intermediate_order() -> None:
    result = classify_h74_cycle(entry=_entry(), exit=_exit(), terminal={"terminal_executable_qty": 0}, orders=[{"client_order_id": "entry", "cycle_id": "cycle-1", "side": "BUY", "created_ts": 1_001_000}])

    assert result.unauthorized_intermediate_order_count == 0


def test_other_strategy_order_does_not_fail_h74_cycle_when_cycle_id_differs() -> None:
    result = classify_h74_cycle(entry=_entry(), exit=_exit(), terminal={"terminal_executable_qty": 0}, orders=[{"client_order_id": "other", "cycle_id": "other-cycle", "side": "BUY", "created_ts": 1_001_000}])

    assert result.h74_cycle_validation_success is True


def test_intermediate_order_failure_is_excluded_from_performance_samples() -> None:
    failed = classify_h74_cycle(
        entry=_entry(),
        exit=_exit(),
        terminal={"terminal_executable_qty": 0},
        orders=[{"client_order_id": "buy2", "cycle_id": "cycle-1", "side": "BUY", "created_ts": 1_001_000}],
    ).as_dict()

    assert failed["h74_cycle_validation_success"] is False
    assert h74_performance_samples([failed]) == []
