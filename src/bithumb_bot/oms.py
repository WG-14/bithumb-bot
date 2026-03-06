from __future__ import annotations

import time
import uuid
from typing import Any
import sqlite3

from .db_core import ensure_db


def new_client_order_id(prefix: str = "cli") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def create_order(
    *,
    client_order_id: str,
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
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            )
            VALUES (?, NULL, ?, ?, ?, ?, 0, ?, ?, NULL)
            """,
            (client_order_id, status, side, price, float(qty_req), ts, ts),
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
            "INSERT INTO fills(client_order_id, fill_ts, price, qty, fee) VALUES (?, ?, ?, ?, ?)",
            (client_order_id, int(fill_ts), float(price), float(qty), float(fee)),
        )
        conn.execute(
            "UPDATE orders SET qty_filled = qty_filled + ?, updated_ts=? WHERE client_order_id=?",
            (float(qty), int(time.time() * 1000), client_order_id),
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
        rows = conn.execute(
            """
            SELECT client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts
            FROM orders
            WHERE status IN ('NEW', 'PARTIAL')
            ORDER BY created_ts ASC
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()