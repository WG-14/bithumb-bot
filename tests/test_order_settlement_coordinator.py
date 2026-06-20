from __future__ import annotations

from bithumb_bot.order_settlement import (
    OrderSettlementCoordinator,
    SettlementBarrierConfig,
    evaluate_settlement_snapshot,
)


def _complete(**overrides):
    payload = {
        "order_state": "FILLED",
        "fill_count": 1,
        "fill_set_complete": True,
        "paid_fee_present": True,
        "order_level_paid_fee_present": True,
        "complete_fill_set_available": True,
        "fee_state": "finalized",
        "principal_applied": True,
        "accounting_finalized": True,
        "projection_applied": True,
        "projected_total_qty": 0.001,
        "portfolio_qty": 0.001,
        "broker_qty": 0.001,
        "broker_local_converged": True,
        "reason_code": "evidence_complete",
    }
    payload.update(overrides)
    return payload


def test_settlement_result_requires_fee_finalized_projection_converged_and_broker_local_converged() -> None:
    pending_fee = evaluate_settlement_snapshot(
        client_order_id="c1",
        exchange_order_id="e1",
        evidence=_complete(fee_state="pending", accounting_finalized=False),
        attempts=[],
    )
    projection_mismatch = evaluate_settlement_snapshot(
        client_order_id="c1",
        exchange_order_id="e1",
        evidence=_complete(projection_applied=False, projected_total_qty=0.002),
        attempts=[],
    )
    broker_mismatch = evaluate_settlement_snapshot(
        client_order_id="c1",
        exchange_order_id="e1",
        evidence=_complete(broker_local_converged=False, broker_qty=0.002),
        attempts=[],
    )
    complete = evaluate_settlement_snapshot(
        client_order_id="c1",
        exchange_order_id="e1",
        evidence=_complete(),
        attempts=[],
    )

    assert pending_fee.settled is False
    assert projection_mismatch.settled is False
    assert broker_mismatch.settled is False
    assert complete.settled is True


def test_post_trade_reconcile_callback_success_does_not_mark_settled_without_fee_finalized() -> None:
    reconcile_calls = 0

    def _reconcile():
        nonlocal reconcile_calls
        reconcile_calls += 1

    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=1, poll_intervals_ms=(), deadline_ms=1),
        sleeper=lambda _seconds: None,
    )
    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        reconcile=_reconcile,
        observe=lambda _attempt: _complete(fee_state="pending", accounting_finalized=False),
    )

    assert reconcile_calls == 1
    assert result.fee_state == "pending"
    assert result.settled is False
    assert result.deadline_exceeded is True


def test_settlement_timeout_returns_timed_out_without_submitting_second_order() -> None:
    submit_calls = 1
    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=3, poll_intervals_ms=(0, 0, 0), deadline_ms=5),
        sleeper=lambda _seconds: None,
    )
    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda attempt: _complete(
            fee_state="pending",
            accounting_finalized=False,
            reason_code=f"fee_pending_{attempt}",
        ),
    )

    assert submit_calls == 1
    assert result.settled is False
    assert result.deadline_exceeded is True
    assert result.reason_code == "timed_out"
    assert len(result.evidence["attempts"]) == 3


def test_settlement_hard_blocked_returns_operator_action_required() -> None:
    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=5, poll_intervals_ms=(0,), deadline_ms=100),
        sleeper=lambda _seconds: None,
    )
    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda _attempt: _complete(
            fee_state="blocked",
            hard_blocked=True,
            reason_code="fee_evidence_incoherent",
        ),
    )

    assert result.settled is False
    assert result.operator_action_required is True
    assert result.reason_code == "fee_evidence_incoherent"
