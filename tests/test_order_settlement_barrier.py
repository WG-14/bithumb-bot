from __future__ import annotations

from bithumb_bot.order_settlement import OrderSettlementCoordinator, SettlementBarrierConfig


def _snapshot(*, fee_state: str, attempt: int):
    finalized = fee_state == "finalized"
    return {
        "order_state": "FILLED",
        "fill_count": 1,
        "fill_set_complete": True,
        "paid_fee_present": finalized,
        "order_level_paid_fee_present": finalized,
        "complete_fill_set_available": True,
        "fee_state": fee_state,
        "principal_applied": True,
        "accounting_finalized": finalized,
        "projection_applied": True,
        "projected_total_qty": 0.0,
        "portfolio_qty": 0.0,
        "broker_qty": 0.0,
        "broker_local_converged": True,
        "reason_code": f"attempt_{attempt}_{fee_state}",
    }


def test_barrier_waits_until_paid_fee_appears_on_later_poll() -> None:
    states = ["pending", "pending", "finalized"]
    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=5, poll_intervals_ms=(0, 0, 0), deadline_ms=100),
        sleeper=lambda _seconds: None,
    )

    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda attempt: _snapshot(fee_state=states[min(attempt, len(states) - 1)], attempt=attempt),
    )

    assert result.settled is True
    assert len(result.evidence["attempts"]) == 3
    assert result.evidence["attempts"][0]["fee_state"] == "pending"
    assert result.evidence["attempts"][2]["order_level_paid_fee_present"] is True


def test_barrier_stops_after_deadline_when_fee_never_finalizes() -> None:
    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=2, poll_intervals_ms=(0,), deadline_ms=1),
        sleeper=lambda _seconds: None,
    )

    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda attempt: _snapshot(fee_state="pending", attempt=attempt),
    )

    assert result.settled is False
    assert result.deadline_exceeded is True
    assert len(result.evidence["attempts"]) == 2


def test_barrier_records_each_attempt_evidence() -> None:
    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=3, poll_intervals_ms=(0, 0), deadline_ms=100),
        sleeper=lambda _seconds: None,
    )

    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda attempt: _snapshot(fee_state="pending", attempt=attempt),
    )

    attempts = result.evidence["attempts"]
    assert [item["attempt_index"] for item in attempts] == [0, 1, 2]
    assert {"fee_state", "fill_set_complete", "projected_total_qty", "broker_qty", "portfolio_qty"} <= set(
        attempts[0]
    )


def test_barrier_does_not_submit_cancel_or_flatten_while_polling() -> None:
    side_effects = {"submit": 1, "cancel": 0, "flatten": 0}
    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=3, poll_intervals_ms=(0, 0), deadline_ms=100),
        sleeper=lambda _seconds: None,
    )

    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda attempt: _snapshot(fee_state="pending", attempt=attempt),
    )

    assert result.settled is False
    assert side_effects == {"submit": 1, "cancel": 0, "flatten": 0}
