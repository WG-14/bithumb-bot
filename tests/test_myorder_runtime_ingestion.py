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
                "timestamp": 1710000000000,
            },
        )
        conn.commit()

        order_row = conn.execute(
            "SELECT exchange_order_id, status, qty_filled FROM orders WHERE client_order_id='cid-entry-1'"
        ).fetchone()
        fill_row = conn.execute(
            "SELECT fill_id, qty, price FROM fills WHERE client_order_id='cid-entry-1'"
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
