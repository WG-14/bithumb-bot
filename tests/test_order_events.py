from __future__ import annotations

from bithumb_bot.db_core import ensure_db
from bithumb_bot.oms import add_fill, create_order, record_submit_started, set_exchange_order_id, set_status


def test_order_events_written_for_major_transitions(tmp_path):
    db_path = tmp_path / "order_events_major.sqlite"
    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o_major",
            side="BUY",
            qty_req=0.01,
            price=None,
            status="PENDING_SUBMIT",
            ts_ms=1000,
            conn=conn,
        )
        record_submit_started("o_major", conn=conn)
        set_status("o_major", "SUBMIT_UNKNOWN", last_error="submit unknown: timeout", conn=conn)
        set_exchange_order_id("o_major", "ex-major", conn=conn)
        set_status("o_major", "NEW", conn=conn)
        add_fill(
            client_order_id="o_major",
            fill_id="fill-major",
            fill_ts=1001,
            price=100000000.0,
            qty=0.01,
            fee=10.0,
            conn=conn,
        )
        conn.commit()

        rows = conn.execute(
            """
            SELECT event_type, message
            FROM order_events
            WHERE client_order_id='o_major'
            ORDER BY event_ts, id
            """
        ).fetchall()
    finally:
        conn.close()

    event_types = [r["event_type"] for r in rows]
    assert "intent_created" in event_types
    assert "submit_started" in event_types
    assert "submit_timeout" in event_types
    assert "exchange_order_id_attached" in event_types
    assert "status_changed" in event_types
    assert "fill_applied" in event_types

    timeout_row = next(r for r in rows if r["event_type"] == "submit_timeout")
    assert "submit unknown" in str(timeout_row["message"])


def test_order_lifecycle_reconstructable_in_timestamp_order(tmp_path):
    db_path = tmp_path / "order_events_timeline.sqlite"
    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o1",
            side="BUY",
            qty_req=0.01,
            price=None,
            status="PENDING_SUBMIT",
            ts_ms=1000,
            conn=conn,
        )
        record_submit_started("o1", conn=conn)
        set_exchange_order_id("o1", "ex1", conn=conn)
        set_status("o1", "NEW", conn=conn)
        add_fill(
            client_order_id="o1",
            fill_id="f1",
            fill_ts=1001,
            price=100000000.0,
            qty=0.01,
            fee=10.0,
            conn=conn,
        )
        set_status("o1", "FILLED", conn=conn)
        conn.commit()

        rows = conn.execute(
            """
            SELECT event_type, event_ts, order_status
            FROM order_events
            WHERE client_order_id='o1'
            ORDER BY event_ts, id
            """
        ).fetchall()
    finally:
        conn.close()

    assert [r["event_type"] for r in rows] == [
        "intent_created",
        "submit_started",
        "exchange_order_id_attached",
        "status_changed",
        "fill_applied",
        "status_changed",
    ]
    assert rows[0]["order_status"] == "PENDING_SUBMIT"
    assert rows[-1]["order_status"] == "FILLED"
    assert all(rows[idx]["event_ts"] <= rows[idx + 1]["event_ts"] for idx in range(len(rows) - 1))
