from __future__ import annotations

import sqlite3
from typing import Any

from .config import settings
from .db_core import ensure_db, get_portfolio_breakdown, init_portfolio, set_portfolio_breakdown
from .oms import add_fill, create_order, set_exchange_order_id, set_status


def record_order_if_missing(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    qty_req: float,
    submit_attempt_id: str | None = None,
    price: float | None,
    ts_ms: int,
    status: str = "NEW",
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
        submit_attempt_id=submit_attempt_id,
        price=price,
        status=status,
        ts_ms=ts_ms,
        conn=conn,
    )


def _fill_exists(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
) -> bool:
    if fill_id:
        row = conn.execute(
            """
            SELECT 1 FROM fills
            WHERE client_order_id=? AND fill_id=?
            LIMIT 1
            """,
            (client_order_id, fill_id),
        ).fetchone()
        if row is not None:
            return True

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
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    note: str | None = None,
) -> dict[str, Any] | None:
    eps = 1e-12

    if qty <= 0:
        raise RuntimeError(f"invalid fill qty for {client_order_id}: {qty}")
    if price < 0:
        raise RuntimeError(f"invalid fill price for {client_order_id}: {price}")
    if fee < 0:
        raise RuntimeError(f"invalid fill fee for {client_order_id}: {fee}")
    if side not in ("BUY", "SELL"):
        raise RuntimeError(f"invalid fill side for {client_order_id}: {side}")

    init_portfolio(conn)
    if _fill_exists(conn, client_order_id=client_order_id, fill_id=fill_id, fill_ts=fill_ts, price=price, qty=qty):
        return None

    order = conn.execute(
        "SELECT qty_req, qty_filled FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if order is not None:
        qty_req = float(order["qty_req"])
        qty_filled = float(order["qty_filled"])
        if qty_filled + qty > qty_req + eps:
            raise RuntimeError(
                f"overfill detected for {client_order_id}: existing={qty_filled}, fill={qty}, requested={qty_req}"
            )

    cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)

    def _consume_locked_then_available(locked: float, available: float, amount: float, *, field: str) -> tuple[float, float]:
        remaining = float(amount)
        locked_after = float(locked)
        available_after = float(available)

        from_locked = min(locked_after, remaining)
        locked_after -= from_locked
        remaining -= from_locked

        if remaining > eps:
            available_after -= remaining

        if locked_after < -eps or available_after < -eps:
            raise RuntimeError(
                f"negative {field} after fill for {client_order_id}: available={available_after}, locked={locked_after}, needed={amount}"
            )
        return max(locked_after, 0.0), max(available_after, 0.0)

    if side == "BUY":
        spend = (price * qty) + fee
        cash_locked_after, cash_available_after = _consume_locked_then_available(
            cash_locked,
            cash_available,
            spend,
            field="cash",
        )
        asset_available_after = asset_available + qty
        asset_locked_after = asset_locked
    else:
        cash_available_after = cash_available + (price * qty) - fee
        cash_locked_after = cash_locked
        asset_locked_after, asset_available_after = _consume_locked_then_available(
            asset_locked,
            asset_available,
            qty,
            field="asset",
        )

    cash_after = cash_available_after + cash_locked_after
    asset_after = asset_available_after + asset_locked_after

    if cash_after < -eps:
        raise RuntimeError(f"negative cash after fill for {client_order_id}: {cash_after}")
    if asset_after < -eps:
        raise RuntimeError(f"negative asset after fill for {client_order_id}: {asset_after}")

    add_fill(
        client_order_id=client_order_id,
        fill_id=fill_id,
        fill_ts=fill_ts,
        price=price,
        qty=qty,
        fee=fee,
        conn=conn,
    )

    set_portfolio_breakdown(
        conn,
        cash_available=max(cash_available_after, 0.0),
        cash_locked=max(cash_locked_after, 0.0),
        asset_available=max(asset_available_after, 0.0),
        asset_locked=max(asset_locked_after, 0.0),
    )
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
