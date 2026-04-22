from __future__ import annotations

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.oms import create_order


pytestmark = pytest.mark.fast_regression


@pytest.fixture(autouse=True)
def _restore_start_cash():
    original = settings.START_CASH_KRW
    object.__setattr__(settings, "START_CASH_KRW", 50_000_000.0)
    try:
        yield
    finally:
        object.__setattr__(settings, "START_CASH_KRW", original)


def test_myorder_runtime_ingestion_applies_fill_and_status(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_ingest.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-1",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload={
                "type": "myOrder",
                "order_id": "ex-1",
                "client_order_id": "cid-entry-1",
                "status": "partial",
                "trade_id": "fill-1",
                "price": "100000000",
                "executed_volume": "0.1",
                "fee": "50.0",
                "timestamp": 1710000000000,
            },
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT exchange_order_id, status, qty_filled FROM orders WHERE client_order_id='cid-entry-1'"
        ).fetchone()
        fill_row = conn.execute(
            "SELECT fill_id, qty, price, fee FROM fills WHERE client_order_id='cid-entry-1'"
        ).fetchone()
        stream_row = conn.execute(
            "SELECT applied, applied_status FROM private_stream_events WHERE dedupe_key=?",
            (result.dedupe_key,),
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is True
    assert order_row["exchange_order_id"] == "ex-1"
    assert order_row["status"] == "PARTIAL"
    assert order_row["qty_filled"] == pytest.approx(0.1)
    assert fill_row["fill_id"] == "fill-1"
    assert fill_row["qty"] == pytest.approx(0.1)
    assert fill_row["price"] == pytest.approx(100_000_000.0)
    assert fill_row["fee"] == pytest.approx(50.0)
    assert int(stream_row["applied"]) == 1
    assert stream_row["applied_status"] == "applied"


def test_myorder_runtime_ingestion_dedupes_repeated_event(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_duplicate.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-2",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        payload = {
            "type": "myOrder",
            "order_id": "ex-2",
            "client_order_id": "cid-entry-2",
            "status": "filled",
            "trade_id": "fill-2",
            "price": "100000000",
            "executed_volume": "0.2",
            "fee": "100.0",
            "timestamp": 1710000001000,
        }
        first = BithumbBroker.ingest_myorder_event_runtime(conn, payload=payload)
        second = BithumbBroker.ingest_myorder_event_runtime(conn, payload=payload)
        conn.commit()

        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-2'"
        ).fetchone()["cnt"]
        event_count = conn.execute("SELECT COUNT(*) AS cnt FROM private_stream_events").fetchone()["cnt"]
    finally:
        conn.close()

    assert first.accepted is True
    assert first.applied is True
    assert second.accepted is False
    assert second.action == "duplicate_event"
    assert fill_count == 1
    assert event_count == 1


def test_myorder_runtime_ingestion_records_unmatched_event_without_applying(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_unmatched.sqlite"))
    try:
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload={
                "type": "myOrder",
                "order_id": "ex-missing",
                "client_order_id": "cid-missing",
                "status": "filled",
                "trade_id": "fill-missing",
                "price": "100000000",
                "executed_volume": "0.1",
                "timestamp": 1710000002000,
            },
        )
        conn.commit()
        stream_row = conn.execute(
            "SELECT applied, applied_status FROM private_stream_events WHERE dedupe_key=?",
            (result.dedupe_key,),
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is False
    assert result.action == "no_local_order_match"
    assert int(stream_row["applied"]) == 0
    assert stream_row["applied_status"] == "no_local_order_match"


def test_myorder_runtime_ingestion_missing_fee_records_pending_observation_without_ledger_apply(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_fee_pending.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-missing-fee",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload={
                "type": "myOrder",
                "order_id": "ex-missing-fee",
                "client_order_id": "cid-entry-missing-fee",
                "status": "partial",
                "trade_id": "fill-missing-fee",
                "price": "100000000",
                "executed_volume": "0.1",
                "timestamp": 1710000003000,
            },
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-entry-missing-fee'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-missing-fee'"
        ).fetchone()["cnt"]
        trade_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM trades WHERE client_order_id='cid-entry-missing-fee'"
        ).fetchone()["cnt"]
        observation = conn.execute(
            """
            SELECT fill_id, fee, fee_status, accounting_status, source, parse_warnings
            FROM broker_fill_observations
            WHERE client_order_id='cid-entry-missing-fee'
            """
        ).fetchone()
        stream_row = conn.execute(
            "SELECT applied, applied_status FROM private_stream_events WHERE dedupe_key=?",
            (result.dedupe_key,),
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is True
    assert result.action == "recovery_required_fee_pending"
    assert result.status == "RECOVERY_REQUIRED"
    assert order_row["status"] == "RECOVERY_REQUIRED"
    assert order_row["qty_filled"] == pytest.approx(0.0)
    assert "fee-pending" in str(order_row["last_error"])
    assert fill_count == 0
    assert trade_count == 0
    assert observation is not None
    assert observation["fill_id"] == "fill-missing-fee"
    assert observation["fee"] is None
    assert observation["fee_status"] == "missing"
    assert observation["accounting_status"] == "fee_pending"
    assert observation["source"] == "myorder_private_stream_fee_pending"
    assert "missing_fee_field" in str(observation["parse_warnings"])
    assert int(stream_row["applied"]) == 1
    assert stream_row["applied_status"] == "recovery_required_fee_pending"


def test_myorder_runtime_ingestion_paid_fee_candidate_is_not_accounting_complete(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "myorder_paid_fee_candidate.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-paid-fee",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload={
                "type": "myOrder",
                "order_id": "ex-paid-fee",
                "client_order_id": "cid-entry-paid-fee",
                "status": "partial",
                "trade_id": "fill-paid-fee",
                "price": "100000000",
                "executed_volume": "0.1",
                "paid_fee": "50.0",
                "timestamp": 1710000003500,
            },
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-entry-paid-fee'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-paid-fee'"
        ).fetchone()["cnt"]
        observation = conn.execute(
            """
            SELECT fee, fee_status, accounting_status, source, parse_warnings
            FROM broker_fill_observations
            WHERE client_order_id='cid-entry-paid-fee'
            """
        ).fetchone()
    finally:
        conn.close()

    assert result.action == "recovery_required_fee_pending"
    assert order_row["status"] == "RECOVERY_REQUIRED"
    assert order_row["qty_filled"] == pytest.approx(0.0)
    assert "fee_status=order_level_candidate" in str(order_row["last_error"])
    assert fill_count == 0
    assert observation["fee"] == pytest.approx(50.0)
    assert observation["fee_status"] == "order_level_candidate"
    assert observation["accounting_status"] == "fee_pending"
    assert observation["source"] == "myorder_private_stream_fee_pending"
    assert "order_level_fee_candidate:paid_fee" in str(observation["parse_warnings"])


def test_myorder_runtime_ingestion_material_zero_fee_marks_recovery_required(tmp_path) -> None:
    object.__setattr__(settings, "MODE", "live")
    conn = ensure_db(str(tmp_path / "myorder_recovery_required.sqlite"))
    try:
        create_order(
            conn=conn,
            client_order_id="cid-entry-live-zero-fee",
            side="BUY",
            qty_req=0.2,
            price=100_000_000.0,
            status="NEW",
        )
        result = BithumbBroker.ingest_myorder_event_runtime(
            conn,
            payload={
                "type": "myOrder",
                "order_id": "ex-live-zero-fee",
                "client_order_id": "cid-entry-live-zero-fee",
                "status": "partial",
                "trade_id": "fill-live-zero-fee",
                "price": "100000000",
                "executed_volume": "0.1",
                "fee": "0",
                "timestamp": 1710000003000,
            },
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid-entry-live-zero-fee'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM fills WHERE client_order_id='cid-entry-live-zero-fee'"
        ).fetchone()["cnt"]
        observation = conn.execute(
            """
            SELECT fee, fee_status, accounting_status, parse_warnings
            FROM broker_fill_observations
            WHERE client_order_id='cid-entry-live-zero-fee'
            """
        ).fetchone()
    finally:
        conn.close()

    assert result.accepted is True
    assert result.applied is True
    assert result.action == "recovery_required_fee_pending"
    assert result.status == "RECOVERY_REQUIRED"
    assert order_row["status"] == "RECOVERY_REQUIRED"
    assert order_row["qty_filled"] == pytest.approx(0.0)
    assert "fee_status=zero_reported" in str(order_row["last_error"])
    assert fill_count == 0
    assert observation is not None
    assert observation["fee"] == pytest.approx(0.0)
    assert observation["fee_status"] == "zero_reported"
    assert observation["accounting_status"] == "fee_pending"
    assert "zero_fee_field:fee" in str(observation["parse_warnings"])
