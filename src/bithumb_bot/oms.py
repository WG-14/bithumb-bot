from __future__ import annotations

import time
import uuid
from typing import Any
import sqlite3

from .db_core import ensure_db


OPEN_ORDER_STATUSES = ("PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN", "RECOVERY_REQUIRED")


def _record_order_event(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    event_type: str,
    event_ts: int | None = None,
    order_status: str | None = None,
    exchange_order_id: str | None = None,
    fill_id: str | None = None,
    qty: float | None = None,
    price: float | None = None,
    message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO order_events(
            client_order_id, event_type, event_ts, order_status, exchange_order_id, fill_id, qty, price, message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            client_order_id,
            event_type,
            int(event_ts if event_ts is not None else time.time() * 1000),
            order_status,
            exchange_order_id,
            fill_id,
            (float(qty) if qty is not None else None),
            (float(price) if price is not None else None),
            (message[:500] if message else None),
        ),
    )


def new_client_order_id(prefix: str = "cli") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def create_order(
    *,
    client_order_id: str,
    submit_attempt_id: str | None = None,
    side: str,
    qty_req: float,
    price: float | None,
    status: str = "NEW",
    ts_ms: int | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, submit_attempt_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            )
            VALUES (?, ?, NULL, ?, ?, ?, ?, 0, ?, ?, NULL)
            """,
            (client_order_id, submit_attempt_id, status, side, price, float(qty_req), ts, ts),
        )
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="intent_created",
            event_ts=ts,
            order_status=status,
            qty=qty_req,
            price=price,
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def record_submit_started(
    client_order_id: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="submit_started",
            event_ts=ts,
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def set_exchange_order_id(
    client_order_id: str,
    exchange_order_id: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        conn.execute(
            "UPDATE orders SET exchange_order_id=?, updated_ts=? WHERE client_order_id=?",
            (exchange_order_id, ts, client_order_id),
        )
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="exchange_order_id_attached",
            event_ts=ts,
            exchange_order_id=exchange_order_id,
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def set_status(
    client_order_id: str,
    status: str,
    last_error: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        conn.execute(
            "UPDATE orders SET status=?, updated_ts=?, last_error=? WHERE client_order_id=?",
            (status, ts, (last_error[:500] if last_error else None), client_order_id),
        )
        event_type = "status_changed"
        if status == "SUBMIT_UNKNOWN":
            event_type = "submit_timeout"
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type=event_type,
            event_ts=ts,
            order_status=status,
            message=last_error,
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def record_status_transition(
    client_order_id: str,
    *,
    from_status: str,
    to_status: str,
    reason: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Record a detailed status transition event for high-risk paths."""
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="status_transition",
            event_ts=ts,
            order_status=to_status,
            message=f"from={from_status};to={to_status};reason={reason}",
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def add_fill(
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float = 0.0,
    conn: sqlite3.Connection | None = None,
) -> None:
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        conn.execute(
            "INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee) VALUES (?, ?, ?, ?, ?, ?)",
            (client_order_id, fill_id, int(fill_ts), float(price), float(qty), float(fee)),
        )
        updated_ts = int(time.time() * 1000)
        conn.execute(
            "UPDATE orders SET qty_filled = qty_filled + ?, updated_ts=? WHERE client_order_id=?",
            (float(qty), updated_ts, client_order_id),
        )
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="fill_applied",
            event_ts=updated_ts,
            fill_id=fill_id,
            qty=qty,
            price=price,
            message=(f"fee={float(fee)}" if fee else None),
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def get_open_orders() -> list[dict[str, Any]]:
    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
        rows = conn.execute(
            f"""
            SELECT client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts
            FROM orders
            WHERE status IN ({placeholders})
            ORDER BY created_ts ASC
            """,
            OPEN_ORDER_STATUSES,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
