from __future__ import annotations

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.myorder_events import normalize_myorder_event_payload


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
