from __future__ import annotations

from bithumb_bot.db_core import ensure_db
from bithumb_bot.oms import add_fill, create_order, record_status_transition, record_submit_started, set_exchange_order_id, set_status, validate_status_transition
from bithumb_bot.observability import safety_event


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


def test_intent_event_persists_submit_intent_metadata(tmp_path):
    db_path = tmp_path / "order_events_intent_metadata.sqlite"
    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o_intent_meta",
            submit_attempt_id="attempt_meta",
            symbol="ETH_KRW",
            mode="live",
            side="SELL",
            qty_req=0.123,
            price=123456.0,
            status="PENDING_SUBMIT",
            ts_ms=1234567890,
            conn=conn,
        )
        conn.commit()

        row = conn.execute(
            """
            SELECT
                client_order_id,
                submit_attempt_id,
                symbol,
                side,
                qty,
                price,
                mode,
                intent_ts,
                payload_fingerprint
            FROM order_events
            WHERE client_order_id='o_intent_meta' AND event_type='intent_created'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["client_order_id"] == "o_intent_meta"
    assert row["submit_attempt_id"] == "attempt_meta"
    assert row["symbol"] == "ETH_KRW"
    assert row["side"] == "SELL"
    assert row["qty"] == 0.123
    assert row["price"] == 123456.0
    assert row["mode"] == "live"
    assert row["intent_ts"] == 1234567890
    assert row["payload_fingerprint"] is None


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


def test_status_transition_event_records_common_fields(tmp_path):
    db_path = tmp_path / "order_events_transition.sqlite"
    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o_transition",
            side="BUY",
            qty_req=0.01,
            price=None,
            status="PENDING_SUBMIT",
            ts_ms=1000,
            conn=conn,
        )
        record_status_transition(
            "o_transition",
            from_status="PENDING_SUBMIT",
            to_status="SUBMIT_UNKNOWN",
            reason="submit unknown: timeout",
            conn=conn,
        )
        conn.commit()

        row = conn.execute(
            """
            SELECT event_type, order_status, message
            FROM order_events
            WHERE client_order_id='o_transition' AND event_type='status_transition'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["event_type"] == "status_transition"
    assert row["order_status"] == "SUBMIT_UNKNOWN"
    assert "from=PENDING_SUBMIT" in str(row["message"])
    assert "to=SUBMIT_UNKNOWN" in str(row["message"])
    assert "reason=submit unknown: timeout" in str(row["message"])


def test_validate_status_transition_allows_only_whitelisted_paths():
    allowed, reason = validate_status_transition(from_status="PENDING_SUBMIT", to_status="SUBMIT_UNKNOWN")
    assert allowed is True
    assert reason is None

    allowed, reason = validate_status_transition(from_status="FILLED", to_status="NEW")
    assert allowed is False
    assert "disallowed status transition" in str(reason)


def test_disallowed_status_transition_records_block_event(tmp_path):
    db_path = tmp_path / "order_events_transition_blocked.sqlite"
    conn = ensure_db(str(db_path))
    try:
        create_order(
            client_order_id="o_blocked",
            side="BUY",
            qty_req=0.01,
            price=None,
            status="FILLED",
            ts_ms=1000,
            conn=conn,
        )

        try:
            set_status("o_blocked", "NEW", conn=conn)
            assert False, "expected ValueError"
        except ValueError as exc:
            assert "FILLED->NEW" in str(exc)

        row = conn.execute(
            """
            SELECT event_type, order_status, message
            FROM order_events
            WHERE client_order_id='o_blocked' AND event_type='status_transition_blocked'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["event_type"] == "status_transition_blocked"
    assert row["order_status"] == "FILLED"
    assert "FILLED->NEW" in str(row["message"])


def test_critical_safety_event_payloads_include_common_fields():
    submit_msg = safety_event(
        "order_submit_started",
        client_order_id="cid-1",
        submit_attempt_id="attempt-1",
        exchange_order_id="-",
        reason_code="-",
        state_to="PENDING_SUBMIT",
    )
    halt_msg = safety_event(
        "order_submit_blocked",
        client_order_id="-",
        submit_attempt_id="-",
        exchange_order_id="-",
        reason_code="RISKY_ORDER_BLOCK",
        state_to="HALTED",
    )
    recovery_msg = safety_event(
        "recovery_required_transition",
        client_order_id="cid-2",
        submit_attempt_id="-",
        exchange_order_id="-",
        reason_code="AMBIGUOUS_SUBMIT",
        state_from="SUBMIT_UNKNOWN",
        state_to="RECOVERY_REQUIRED",
    )

    for msg in (submit_msg, halt_msg, recovery_msg):
        assert "symbol=" in msg
        assert "client_order_id=" in msg
        assert "submit_attempt_id=" in msg
        assert "exchange_order_id=" in msg
        assert "reason_code=" in msg

    assert "state_to=PENDING_SUBMIT" in submit_msg
    assert "state_to=HALTED" in halt_msg
    assert "state_from=SUBMIT_UNKNOWN" in recovery_msg
    assert "state_to=RECOVERY_REQUIRED" in recovery_msg
