from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.fee_observation import (
    MultiFillTradeEvidence,
    validate_multi_fill_order_level_paid_fee_allocation,
    validate_single_fill_order_level_paid_fee,
)


@pytest.fixture(autouse=True)
def _restore_fee_settings():
    original_fee_rate = settings.LIVE_FEE_RATE_ESTIMATE
    original_threshold = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    try:
        yield
    finally:
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", original_fee_rate)
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_threshold)


def test_validate_single_fill_order_level_paid_fee_accepts_incident_shape() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    evaluation = validate_single_fill_order_level_paid_fee(
        paid_fee="27.71",
        fill_qty=0.00059998,
        fill_price=115_465_000.0,
        fill_funds=69_276.691,
        order_executed_volume=0.00059998,
        order_executed_funds=69_276.691,
        single_fill_evidence=True,
        client_order_id="live_1777104360000_buy_aee4c564",
        exchange_order_id="C0101000002949768709",
        fill_id="C0101000000983820316",
    )

    assert evaluation.accounting_status == "accounting_complete"
    assert evaluation.fee_status == "validated_order_level_paid_fee"
    assert evaluation.fee == pytest.approx(27.71)
    assert evaluation.reason == "order_level_paid_fee_validated_single_fill"
    assert evaluation.checks["single_fill"] is True
    assert evaluation.checks["executed_volume_match"] is True
    assert evaluation.checks["executed_funds_match"] is True
    assert evaluation.checks["expected_fee_rate_match"] is True
    assert evaluation.checks["identifiers_match"] is True


@pytest.mark.parametrize(
    ("fill_funds", "paid_fee", "fill_qty", "fill_price", "exchange_order_id", "fill_id"),
    [
        (69_276.6907, 27.71, 0.00059998, 115_465_000.0, "C0101000002949768709", "C0101000000983820316"),
        (92_580.0, 37.03, 0.0008, 115_725_000.0, "C0101000002959999001", "C0101000000983999001"),
        (46_096.0, 18.43, 0.0004, 115_240_000.0, "C0101000002959999002", "C0101000000983999002"),
    ],
)
def test_validate_single_fill_order_level_paid_fee_accepts_exchange_paid_fee_despite_estimate_mismatch(
    fill_funds: float,
    paid_fee: float,
    fill_qty: float,
    fill_price: float,
    exchange_order_id: str,
    fill_id: str,
) -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    evaluation = validate_single_fill_order_level_paid_fee(
        paid_fee=paid_fee,
        fill_qty=fill_qty,
        fill_price=fill_price,
        fill_funds=fill_funds,
        order_executed_volume=fill_qty,
        order_executed_funds=fill_funds,
        single_fill_evidence=True,
        client_order_id="incident_client_order",
        exchange_order_id=exchange_order_id,
        fill_id=fill_id,
    )

    assert evaluation.accounting_status == "accounting_complete"
    assert evaluation.fee_status == "validated_order_level_paid_fee"
    assert evaluation.fee == pytest.approx(paid_fee)
    assert evaluation.fee_authority == "exchange_order_paid_fee_single_fill"
    assert evaluation.accounting_decision == "finalize"
    assert evaluation.checks["single_fill"] is True
    assert evaluation.checks["executed_volume_match"] is True
    assert evaluation.checks["executed_funds_match"] is True
    assert evaluation.checks["expected_fee_rate_match"] is False
    assert evaluation.checks["expected_fee_rate_warning"] is True
    assert evaluation.checks["identifiers_match"] is True
    assert evaluation.reason == "order_level_paid_fee_validated_single_fill_expected_fee_rate_mismatch"
    assert evaluation.provenance == "order_level_paid_fee_validated_single_fill_fee_rate_warning"
    assert evaluation.diagnostic_flags == ("expected_fee_rate_mismatch",)


def test_validate_single_fill_order_level_paid_fee_keeps_principal_path_pending_when_funds_do_not_match() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    evaluation = validate_single_fill_order_level_paid_fee(
        paid_fee="27.71",
        fill_qty=0.00059998,
        fill_price=115_465_000.0,
        fill_funds=69_276.691,
        order_executed_volume=0.00059998,
        order_executed_funds=69_200.0,
        single_fill_evidence=True,
        client_order_id="live_1777104360000_buy_aee4c564",
        exchange_order_id="C0101000002949768709",
        fill_id="C0101000000983820316",
    )

    assert evaluation.accounting_status == "fee_pending"
    assert evaluation.accounting_eligibility == "pending"
    assert evaluation.fee_authority == "configured_estimate_or_missing_fee"
    assert evaluation.accounting_decision == "pending_fee_validation"
    assert evaluation.fee_status == "order_level_candidate"
    assert evaluation.reason == "executed_funds_mismatch"
    assert evaluation.checks["single_fill"] is True
    assert evaluation.checks["executed_volume_match"] is True
    assert evaluation.checks["executed_funds_match"] is False


@pytest.mark.parametrize(
    ("kwargs", "expected_reason"),
    [
        (
            {
                "paid_fee": None,
                "fill_qty": 0.00059998,
                "fill_price": 115_465_000.0,
                "fill_funds": 69_276.6907,
                "order_executed_volume": 0.00059998,
                "order_executed_funds": 69_276.6907,
                "single_fill_evidence": True,
                "client_order_id": "incident_client_order",
                "exchange_order_id": "C0101000002949768709",
                "fill_id": "C0101000000983820316",
            },
            "paid_fee_missing_or_unparseable",
        ),
        (
            {
                "paid_fee": -1.0,
                "fill_qty": 0.00059998,
                "fill_price": 115_465_000.0,
                "fill_funds": 69_276.6907,
                "order_executed_volume": 0.00059998,
                "order_executed_funds": 69_276.6907,
                "single_fill_evidence": True,
                "client_order_id": "incident_client_order",
                "exchange_order_id": "C0101000002949768709",
                "fill_id": "C0101000000983820316",
            },
            "negative_paid_fee",
        ),
        (
            {
                "paid_fee": 0.0,
                "fill_qty": 0.00059998,
                "fill_price": 115_465_000.0,
                "fill_funds": 69_276.6907,
                "order_executed_volume": 0.00059998,
                "order_executed_funds": 69_276.6907,
                "single_fill_evidence": True,
                "client_order_id": "incident_client_order",
                "exchange_order_id": "C0101000002949768709",
                "fill_id": "C0101000000983820316",
            },
            "zero_paid_fee_material_notional",
        ),
        (
            {
                "paid_fee": 27.71,
                "fill_qty": 0.00059998,
                "fill_price": 115_465_000.0,
                "fill_funds": 69_276.6907,
                "order_executed_volume": 0.00059998,
                "order_executed_funds": 69_276.6907,
                "single_fill_evidence": False,
                "client_order_id": "incident_client_order",
                "exchange_order_id": "C0101000002949768709",
                "fill_id": "C0101000000983820316",
            },
            "multi_fill_order_level_fee_ambiguous",
        ),
        (
            {
                "paid_fee": 27.71,
                "fill_qty": 0.00059998,
                "fill_price": 115_465_000.0,
                "fill_funds": 69_276.6907,
                "order_executed_volume": 0.0007,
                "order_executed_funds": 69_276.6907,
                "single_fill_evidence": True,
                "client_order_id": "incident_client_order",
                "exchange_order_id": "C0101000002949768709",
                "fill_id": "C0101000000983820316",
            },
            "executed_volume_mismatch",
        ),
        (
            {
                "paid_fee": 27.71,
                "fill_qty": 0.00059998,
                "fill_price": 115_465_000.0,
                "fill_funds": 69_276.6907,
                "order_executed_volume": 0.00059998,
                "order_executed_funds": 69_200.0,
                "single_fill_evidence": True,
                "client_order_id": "incident_client_order",
                "exchange_order_id": "C0101000002949768709",
                "fill_id": "C0101000000983820316",
            },
            "executed_funds_mismatch",
        ),
        (
            {
                "paid_fee": 27.71,
                "fill_qty": 0.00059998,
                "fill_price": 115_465_000.0,
                "fill_funds": 69_276.6907,
                "order_executed_volume": 0.00059998,
                "order_executed_funds": 69_276.6907,
                "single_fill_evidence": True,
                "client_order_id": "",
                "exchange_order_id": "C0101000002949768709",
                "fill_id": "C0101000000983820316",
            },
            "identifier_mismatch",
        ),
    ],
)
def test_validate_single_fill_order_level_paid_fee_keeps_unsafe_cases_pending(
    kwargs: dict[str, object],
    expected_reason: str,
) -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    evaluation = validate_single_fill_order_level_paid_fee(**kwargs)

    assert evaluation.accounting_status == "fee_pending"
    assert evaluation.fee_authority in {"configured_estimate_or_missing_fee", "invalid_or_unparseable"}
    assert evaluation.accounting_decision == "pending_fee_validation"
    assert evaluation.fee_status == "order_level_candidate"
    assert evaluation.reason == expected_reason


def test_validate_multi_fill_order_level_paid_fee_allocation_accepts_incident_shape() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    allocation = validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee="27.83",
        order_executed_volume=0.00059999,
        order_executed_funds=69_595.47103,
        client_order_id="live_1777186500000_buy_aee54bab",
        exchange_order_id="C0101000002950999701",
        trades=[
            MultiFillTradeEvidence(
                fill_id="C0101000000983890060",
                price=115_994_000.0,
                qty=0.00036902,
                funds=42_804.10588,
            ),
            MultiFillTradeEvidence(
                fill_id="C0101000000983890061",
                price=115_995_000.0,
                qty=0.00023097,
                funds=26_791.36515,
            ),
        ],
    )

    assert allocation.reason == "order_level_paid_fee_validated_allocated"
    assert allocation.allocated_fees_by_fill_id["C0101000000983890060"] == pytest.approx(17.12)
    assert allocation.allocated_fees_by_fill_id["C0101000000983890061"] == pytest.approx(10.71)
    assert sum(allocation.allocated_fees_by_fill_id.values()) == pytest.approx(27.83)
    first = allocation.evaluations_by_fill_id["C0101000000983890060"]
    second = allocation.evaluations_by_fill_id["C0101000000983890061"]
    assert first.accounting_status == "accounting_complete"
    assert first.fee_authority == "exchange_order_paid_fee_allocated"
    assert first.accounting_decision == "finalize"
    assert second.accounting_status == "accounting_complete"
    assert allocation.checks["allocated_fee_sum_match"] is True
    assert allocation.checks["executed_funds_match"] is True
    assert allocation.checks["expected_fee_rate_match"] is True


def test_order_level_paid_fee_sum_invariant_detects_fee_shortfall() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    allocation = validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee="27.43",
        order_executed_volume=0.00059997,
        order_executed_funds=68_579.88942,
        client_order_id="live_1777435320000_sell_ae60a72b",
        exchange_order_id="C0101000002956694365",
        trades=[
            MultiFillTradeEvidence(
                fill_id="C0101000000984365609",
                price=114_325_000.0,
                qty=0.00004374,
                funds=5_000.5755,
            ),
            MultiFillTradeEvidence(
                fill_id="C0101000000984365610",
                price=114_304_000.0,
                qty=0.00055623,
                funds=63_579.31392,
            ),
        ],
    )

    assert allocation.reason == "order_level_paid_fee_validated_allocated"
    assert allocation.allocated_fees_by_fill_id["C0101000000984365609"] == pytest.approx(2.00)
    assert allocation.allocated_fees_by_fill_id["C0101000000984365610"] == pytest.approx(25.43)
    assert sum(allocation.allocated_fees_by_fill_id.values()) == pytest.approx(27.43)
    assert allocation.checks["allocated_fee_sum_match"] is True
    observed_shortfall = 27.43 - 25.43
    assert observed_shortfall == pytest.approx(2.00)


def test_validate_multi_fill_order_level_paid_fee_allocation_fails_closed_on_incomplete_fill_set() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    allocation = validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee="27.83",
        order_executed_volume=0.00059999,
        order_executed_funds=69_595.47103,
        client_order_id="live_1777186500000_buy_aee54bab",
        exchange_order_id="C0101000002950999701",
        trades=[
            MultiFillTradeEvidence(
                fill_id="C0101000000983890060",
                price=115_994_000.0,
                qty=0.00036902,
                funds=42_804.10588,
            )
        ],
    )

    assert allocation.evaluations_by_fill_id == {}
    assert allocation.reason == "executed_funds_mismatch"
    assert allocation.checks["executed_funds_match"] is False


def test_validate_multi_fill_order_level_paid_fee_allocation_fails_closed_on_rounding_mismatch() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    allocation = validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee="27.831",
        order_executed_volume=0.00059999,
        order_executed_funds=69_595.47103,
        client_order_id="live_1777186500000_buy_aee54bab",
        exchange_order_id="C0101000002950999701",
        trades=[
            MultiFillTradeEvidence(
                fill_id="C0101000000983890060",
                price=115_994_000.0,
                qty=0.00036902,
                funds=42_804.10588,
            ),
            MultiFillTradeEvidence(
                fill_id="C0101000000983890061",
                price=115_995_000.0,
                qty=0.00023097,
                funds=26_791.36515,
            ),
        ],
    )

    assert allocation.evaluations_by_fill_id == {}
    assert allocation.reason == "allocated_fee_sum_mismatch"


def test_validate_multi_fill_order_level_paid_fee_allocation_blocks_wild_fee_rate() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    allocation = validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee="4000.00",
        order_executed_volume=0.00059999,
        order_executed_funds=69_595.47103,
        client_order_id="live_1777186500000_buy_aee54bab",
        exchange_order_id="C0101000002950999701",
        trades=[
            MultiFillTradeEvidence(
                fill_id="C0101000000983890060",
                price=115_994_000.0,
                qty=0.00036902,
                funds=42_804.10588,
            ),
            MultiFillTradeEvidence(
                fill_id="C0101000000983890061",
                price=115_995_000.0,
                qty=0.00023097,
                funds=26_791.36515,
            ),
        ],
    )

    assert allocation.evaluations_by_fill_id == {}
    assert allocation.reason == "suspicious_fee_rate"
