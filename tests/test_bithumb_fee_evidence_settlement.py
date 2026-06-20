from __future__ import annotations

import pytest

from bithumb_bot.fee_observation import (
    MultiFillTradeEvidence,
    validate_multi_fill_order_level_paid_fee_allocation,
    validate_single_fill_order_level_paid_fee,
)
from bithumb_bot.order_settlement import OrderSettlementCoordinator, SettlementBarrierConfig


def _settlement_snapshot(*, finalized: bool):
    return {
        "order_state": "FILLED",
        "fill_count": 1,
        "fill_set_complete": True,
        "trade_level_fee_present": False,
        "paid_fee_present": finalized,
        "order_level_paid_fee_present": finalized,
        "complete_fill_set_available": True,
        "single_fill_deterministic": finalized,
        "multi_fill_deterministic_allocation_available": False,
        "fee_finalized": finalized,
        "fee_pending_retryable": not finalized,
        "fee_pending_hard_blocked": False,
        "fee_state": "finalized" if finalized else "pending",
        "principal_applied": True,
        "accounting_finalized": finalized,
        "projection_applied": True,
        "projected_total_qty": 0.001,
        "portfolio_qty": 0.001,
        "broker_qty": 0.001,
        "broker_local_converged": True,
    }


def test_single_fill_order_level_paid_fee_finalizes_after_delayed_snapshot() -> None:
    evaluation = validate_single_fill_order_level_paid_fee(
        paid_fee="27.71",
        fill_qty=0.001,
        fill_price=55_420_000.0,
        fill_funds=55_420.0,
        order_executed_volume=0.001,
        order_executed_funds=55_420.0,
        single_fill_evidence=True,
        client_order_id="c1",
        exchange_order_id="e1",
        fill_id="f1",
    )
    assert evaluation.accounting_status == "accounting_complete"

    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=3, poll_intervals_ms=(0, 0), deadline_ms=100),
        sleeper=lambda _seconds: None,
    )
    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda attempt: _settlement_snapshot(finalized=attempt >= 1),
    )

    assert result.settled is True
    assert result.fee_state == "finalized"
    assert result.evidence["attempts"][0]["fee_state"] == "pending"
    assert result.evidence["attempts"][1]["order_level_paid_fee_present"] is True


def test_multi_fill_paid_fee_allocates_only_when_complete_fill_set_available() -> None:
    incomplete = validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee="30.00",
        trades=[
            MultiFillTradeEvidence(fill_id="f1", qty=0.001, price=10_000_000.0, funds=10_000.0),
        ],
        order_executed_volume=0.003,
        order_executed_funds=30_000.0,
        client_order_id="c1",
        exchange_order_id="e1",
    )
    complete = validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee="30.00",
        trades=[
            MultiFillTradeEvidence(fill_id="f1", qty=0.001, price=10_000_000.0, funds=10_000.0),
            MultiFillTradeEvidence(fill_id="f2", qty=0.002, price=10_000_000.0, funds=20_000.0),
        ],
        order_executed_volume=0.003,
        order_executed_funds=30_000.0,
        client_order_id="c1",
        exchange_order_id="e1",
    )

    assert incomplete.reason != "order_level_paid_fee_validated_allocated"
    assert incomplete.evaluations_by_fill_id == {}
    assert complete.reason.startswith("order_level_paid_fee_validated_allocated")
    assert set(complete.allocated_fees_by_fill_id) == {"f1", "f2"}


def test_missing_paid_fee_until_deadline_remains_pending_not_zero() -> None:
    evaluation = validate_single_fill_order_level_paid_fee(
        paid_fee=None,
        fill_qty=0.001,
        fill_price=55_420_000.0,
        fill_funds=55_420.0,
        order_executed_volume=0.001,
        order_executed_funds=55_420.0,
        single_fill_evidence=True,
        client_order_id="c1",
        exchange_order_id="e1",
        fill_id="f1",
    )
    assert evaluation.accounting_status == "fee_pending"
    assert evaluation.fee is None

    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=2, poll_intervals_ms=(0,), deadline_ms=1),
        sleeper=lambda _seconds: None,
    )
    result = coordinator.settle(
        client_order_id="c1",
        exchange_order_id="e1",
        observe=lambda _attempt: _settlement_snapshot(finalized=False),
    )

    assert result.settled is False
    assert result.fee_state == "pending"
    assert result.evidence["fee_state"] != "finalized"


def test_fee_rate_warning_keeps_provenance_but_finalizes_when_paid_fee_matches() -> None:
    evaluation = validate_single_fill_order_level_paid_fee(
        paid_fee="1.00",
        fill_qty=0.001,
        fill_price=55_420_000.0,
        fill_funds=55_420.0,
        order_executed_volume=0.001,
        order_executed_funds=55_420.0,
        single_fill_evidence=True,
        client_order_id="c1",
        exchange_order_id="e1",
        fill_id="f1",
        configured_fee_rate=0.0005,
    )

    assert evaluation.accounting_status == "accounting_complete"
    assert evaluation.fee == pytest.approx(1.0)
    assert evaluation.provenance == "order_level_paid_fee_validated_single_fill_fee_rate_warning"
    assert evaluation.checks["expected_fee_rate_match"] is False
