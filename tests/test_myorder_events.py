from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.myorder_events import normalize_myorder_event_payload
from bithumb_bot.config import settings
from bithumb_bot.fee_observation import fee_accounting_status


def _incident_myorder_payload() -> dict[str, object]:
    fixture_path = (
        Path(__file__).resolve().parent / "fixtures" / "bithumb" / "live_paid_fee_single_fill_buy_2026_04_24.json"
    )
    fixture = json.loads(fixture_path.read_text())
    trade = dict(fixture["trade"])
    return {
        "type": "myOrder",
        "ask_bid": "bid",
        "order_type": "price",
        "state": "trade",
        "uuid": "incident-order-1",
        "trade_uuid": str(trade["uuid"]),
        "client_order_id": "incident-client-1",
        "price": str(trade["price"]),
        "volume": str(trade["volume"]),
        "executed_volume": str(trade["volume"]),
        "remaining_volume": "0",
        "executed_funds": str(trade["funds"]),
        "timestamp": 1777042623000,
        "trade_timestamp": 1777042623000,
        "paid_fee": str(fixture["order_fee_fields"]["paid_fee"]),
    }


@pytest.fixture(autouse=True)
def _restore_fee_settings():
    original_rate = settings.LIVE_FEE_RATE_ESTIMATE
    original_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    try:
        yield
    finally:
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", original_rate)
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_notional)


def test_normalize_myorder_event_tolerates_unknown_fields_and_preserves_correlation_ids() -> None:
    payload = {
        "event_type": "trade",
        "uuid": "remote-123",
        "client_order_id": "cid-123",
        "coid": "cid-123",
        "state": "done",
        "side": "ask",
        "ord_type": "market",
        "executed_volume": "0.1",
        "price": "12345",
        "fee": "1.23",
        "extra_future_field": "ignored",
        "nested_future_payload": {"surprise": True},
    }

    normalized = normalize_myorder_event_payload(payload)

    assert normalized.client_order_id == "cid-123"
    assert normalized.exchange_order_id == "remote-123"
    assert normalized.status == "FILLED"
    assert normalized.event_type == "trade"
    assert normalized.qty == 0.1
    assert normalized.price == 12345.0
    assert normalized.fee == 1.23
    assert normalized.fee_status == "complete"
    assert normalized.fee_warning is None
    assert normalized.raw_payload["extra_future_field"] == "ignored"
    assert normalized.dedupe_key


def test_normalize_myorder_event_marks_missing_fee_as_uncertain() -> None:
    normalized = normalize_myorder_event_payload(
        {
            "uuid": "remote-missing-fee",
            "client_order_id": "cid-missing-fee",
            "state": "done",
            "executed_volume": "0.1",
            "price": "100000000",
        }
    )

    assert normalized.fee is None
    assert normalized.fee_status == "missing"
    assert normalized.fee_warning == "missing_fee_field"


def test_normalize_myorder_event_marks_paid_fee_as_order_level_candidate() -> None:
    normalized = normalize_myorder_event_payload(
        {
            "uuid": "remote-paid-fee",
            "client_order_id": "cid-paid-fee",
            "state": "done",
            "executed_volume": "0.1",
            "price": "100000000",
            "paid_fee": "50.0",
        }
    )

    assert normalized.fee == 50.0
    assert normalized.fee_status == "order_level_candidate"
    assert normalized.fee_warning == "order_level_fee_candidate:paid_fee"


def test_normalize_myorder_event_validates_single_fill_paid_fee_when_evidence_matches() -> None:
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0004)
    normalized = normalize_myorder_event_payload(_incident_myorder_payload())

    assert normalized.fee == 27.86
    assert normalized.fee_status == "validated_order_level_paid_fee"
    assert normalized.fee_source == "order_level_paid_fee"
    assert normalized.fee_confidence == "validated"
    assert normalized.accounting_status == "accounting_complete"
    assert normalized.fee_provenance == "order_level_paid_fee_validated_single_fill"
    assert normalized.fee_validation_reason == "order_level_paid_fee_validated_single_fill"
    assert normalized.fee_validation_checks["single_fill"] is True
    assert normalized.fee_validation_checks["executed_volume_match"] is True
    assert normalized.fee_validation_checks["executed_funds_match"] is True
    assert normalized.fee_validation_checks["expected_fee_rate_match"] is True
    assert normalized.fee_warning is None


def test_fee_accounting_status_accepts_validated_order_level_paid_fee() -> None:
    status = fee_accounting_status(
        fee=27.86,
        fee_status="validated_order_level_paid_fee",
        price=116110000.0,
        qty=0.00059999,
        material_notional_threshold=10_000.0,
        fee_source="order_level_paid_fee",
        fee_confidence="validated",
        provenance="order_level_paid_fee_validated_single_fill",
        reason="order_level_paid_fee_validated_single_fill",
        checks={
            "single_fill": True,
            "paid_fee_present": True,
            "executed_volume_match": True,
            "executed_funds_match": True,
            "expected_fee_rate_match": True,
            "identifiers_match": True,
            "material_notional_suspicious": True,
        },
    )

    assert status == "accounting_complete"


def test_bithumb_broker_exposes_myorder_normalizer() -> None:
    payload = {
        "uuid": "remote-456",
        "client_order_id": "cid-456",
        "state": "cancel",
    }

    normalized = BithumbBroker.normalize_myorder_event(payload)

    assert normalized.exchange_order_id == "remote-456"
    assert normalized.client_order_id == "cid-456"
    assert normalized.status == "CANCELED"
