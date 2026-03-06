from __future__ import annotations

import sqlite3
from typing import Any

from .config import settings
from .db_core import ensure_db, get_portfolio, init_portfolio, set_portfolio
from .oms import add_fill, create_order, set_exchange_order_id, set_status


def record_order_if_missing(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    qty_req: float,
    price: float | None,
    ts_ms: int,
) -> None:
    exists = conn.execute(
        "SELECT 1 FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if exists:
        return
    create_order(
        client_order_id=client_order_id,
        side=side,
        qty_req=qty_req,
        price=price,
        status="NEW",
        ts_ms=ts_ms,
        conn=conn,
    )


def _fill_exists(conn: sqlite3.Connection, *, client_order_id: str, fill_ts: int, price: float, qty: float) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM fills
        WHERE client_order_id=? AND fill_ts=? AND ABS(price-?) < 1e-12 AND ABS(qty-?) < 1e-12
        LIMIT 1
        """,
        (client_order_id, int(fill_ts), float(price), float(qty)),
    ).fetchone()
    return row is not None


def apply_fill_and_trade(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    note: str | None = None,
) -> dict[str, Any] | None:
    init_portfolio(conn)
    if _fill_exists(conn, client_order_id=client_order_id, fill_ts=fill_ts, price=price, qty=qty):
        return None

    cash, asset = get_portfolio(conn)
    if side == "BUY":
        cash_after = cash - (price * qty) - fee
        asset_after = asset + qty
    else:
        cash_after = cash + (price * qty) - fee
        asset_after = asset - qty

    add_fill(
        client_order_id=client_order_id,
        fill_ts=fill_ts,
        price=price,
        qty=qty,
        fee=fee,
        conn=conn,
    )

    set_portfolio(conn, cash_after, asset_after)
    conn.execute(
        """
        INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(fill_ts),
            settings.PAIR,
            settings.INTERVAL,
            side,
            float(price),
            float(qty),
            float(fee),
            float(cash_after),
            float(asset_after),
            note,
        ),
    )
    return {
        "ts": int(fill_ts),
        "side": side,
        "price": float(price),
        "qty": float(qty),
        "fee": float(fee),
        "cash": float(cash_after),
        "asset": float(asset_after),
        "client_order_id": client_order_id,
    }


def update_order_snapshot(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    exchange_order_id: str | None,
    status: str,
) -> None:
    if exchange_order_id:
        set_exchange_order_id(client_order_id, exchange_order_id, conn=conn)
    set_status(client_order_id, status, conn=conn)
