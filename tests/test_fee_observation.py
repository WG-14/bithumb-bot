from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.fee_observation import validate_single_fill_order_level_paid_fee


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
    assert evaluation.fee_status == "order_level_candidate"
    assert evaluation.reason == "executed_funds_mismatch"
    assert evaluation.checks["single_fill"] is True
    assert evaluation.checks["executed_volume_match"] is True
    assert evaluation.checks["executed_funds_match"] is False
