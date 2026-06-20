from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.order_settlement import OrderSettlementCoordinator, SettlementBarrierConfig
from bithumb_bot.runtime.execution_coordinator import ExecutionCoordinator


class _Summary:
    submit_expected = True


def _trade() -> dict[str, object]:
    return {
        "client_order_id": "live-cid-1",
        "exchange_order_id": "live-exid-1",
        "side": "BUY",
        "filled_qty": 0.0002,
    }


def _record_reconcile_attempt(attempts: list[str]) -> None:
    attempts.append("reconciled")


def _evidence(*, fee_state: str = "finalized", projection: bool = True, broker_converged: bool = True):
    finalized = fee_state == "finalized"
    return {
        "order_state": "FILLED",
        "order_terminal": True,
        "fill_count": 1,
        "fill_set_complete": True,
        "trade_level_fee_present": finalized,
        "paid_fee_present": finalized,
        "order_level_paid_fee_present": finalized,
        "complete_fill_set_available": True,
        "single_fill_deterministic": finalized,
        "multi_fill_deterministic_allocation_available": False,
        "fee_finalized": finalized,
        "fee_pending_retryable": fee_state == "pending",
        "fee_pending_hard_blocked": fee_state == "blocked",
        "fee_state": fee_state,
        "principal_applied": True,
        "accounting_finalized": finalized,
        "projection_applied": projection,
        "projected_total_qty": 0.0002,
        "portfolio_qty": 0.0002,
        "broker_qty": 0.0002 if broker_converged else 0.0,
        "broker_local_converged": broker_converged,
        "reason_code": "settlement_evidence_complete" if finalized else "fee_pending",
    }


def test_live_runtime_checkpoint_requires_settlement_result_settled() -> None:
    coordinator = ExecutionCoordinator("lot_native")

    missing_reconcile_calls: list[str] = []
    missing = coordinator.execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=_Summary(),
        submit_invoker=_trade,
        post_trade_reconcile=lambda: _record_reconcile_attempt(missing_reconcile_calls),
        settlement_required=True,
    )
    assert missing.submitted is True
    assert missing.settlement_result is None
    assert missing.mark_processed_allowed is False
    assert missing_reconcile_calls == ["reconciled"]

    not_settled_reconcile_calls: list[str] = []
    not_settled = coordinator.execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=_Summary(),
        submit_invoker=_trade,
        post_trade_reconcile=lambda: _record_reconcile_attempt(not_settled_reconcile_calls),
        settlement_required=True,
        settlement_coordinator=lambda _trade_payload: {
            "settled": False,
            "reason_code": "fee_pending",
            "evidence": {"attempts": [_evidence(fee_state="pending")]},
        },
    )
    assert not_settled.mark_processed_allowed is False
    assert not_settled_reconcile_calls == ["reconciled"]

    settled_reconcile_calls: list[str] = []
    settled = coordinator.execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=_Summary(),
        submit_invoker=_trade,
        post_trade_reconcile=lambda: _record_reconcile_attempt(settled_reconcile_calls),
        settlement_required=True,
        settlement_coordinator=lambda _trade_payload: {
            "settled": True,
            "reason_code": "settled",
            "evidence": {"attempts": [_evidence()]},
        },
    )
    assert settled.mark_processed_allowed is True
    assert settled_reconcile_calls == ["reconciled"]


def test_live_runtime_does_not_mark_processed_when_fee_pending_after_reconcile_callback_success() -> None:
    reconcile_calls: list[str] = []

    result = ExecutionCoordinator("lot_native").execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=_Summary(),
        submit_invoker=_trade,
        post_trade_reconcile=lambda: reconcile_calls.append("reconciled"),
        settlement_required=True,
        settlement_coordinator=lambda _trade_payload: {
            "settled": False,
            "fee_state": "pending",
            "reason_code": "fee_pending",
            "evidence": {"attempts": [_evidence(fee_state="pending")]},
        },
    )

    assert reconcile_calls == ["reconciled"]
    assert result.post_trade_reconciled is True
    assert result.settlement_result is not None
    assert result.settlement_result["fee_state"] == "pending"
    assert result.mark_processed_allowed is False


def test_live_runtime_waits_for_delayed_order_level_paid_fee_before_checkpoint() -> None:
    states = ["pending", "finalized"]
    barrier = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=3, poll_intervals_ms=(0, 0), deadline_ms=100),
        sleeper=lambda _seconds: None,
    )

    def _settle(trade_payload):
        return barrier.settle(
            client_order_id=str(trade_payload["client_order_id"]),
            exchange_order_id=str(trade_payload["exchange_order_id"]),
            observe=lambda attempt: _evidence(fee_state=states[min(attempt, len(states) - 1)]),
        )

    reconcile_calls: list[str] = []
    result = ExecutionCoordinator("lot_native").execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=_Summary(),
        submit_invoker=_trade,
        post_trade_reconcile=lambda: _record_reconcile_attempt(reconcile_calls),
        settlement_required=True,
        settlement_coordinator=_settle,
    )

    assert result.mark_processed_allowed is True
    assert reconcile_calls == ["reconciled"]
    assert result.settlement_result is not None
    evidence = result.settlement_result["evidence"]
    assert len(evidence["attempts"]) == 2
    assert evidence["attempts"][0]["fee_state"] == "pending"
    assert evidence["attempts"][1]["order_level_paid_fee_present"] is True


def test_live_runtime_rejects_projection_or_broker_non_convergence_for_checkpoint() -> None:
    coordinator = ExecutionCoordinator("lot_native")
    for evidence in (
        _evidence(projection=False),
        _evidence(broker_converged=False),
    ):
        result = coordinator.execute_cycle(
            candle_ts=1,
            decision_id=1,
            execution_decision_summary=_Summary(),
            submit_invoker=_trade,
            settlement_required=True,
            settlement_coordinator=lambda _trade_payload, _evidence=evidence: {
                "settled": False,
                "reason_code": "settlement_waiting",
                "evidence": {"attempts": [_evidence]},
            },
        )
        assert result.mark_processed_allowed is False
