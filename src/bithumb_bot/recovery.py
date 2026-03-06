from __future__ import annotations

from .broker.base import Broker
from .db_core import ensure_db, init_portfolio, set_portfolio
from .execution import apply_fill_and_trade
from .oms import get_open_orders, set_status


def assert_no_open_orders() -> None:
    open_orders = get_open_orders()
    if open_orders:
        raise RuntimeError(f"Open orders exist (resume required): {open_orders}")


def reconcile_with_broker(broker: Broker) -> None:
    conn = ensure_db()
    try:
        init_portfolio(conn)

        local_open = conn.execute(
            "SELECT client_order_id, exchange_order_id, side, qty_req, status FROM orders WHERE status IN ('NEW','PARTIAL')"
        ).fetchall()
        for row in local_open:
            oid = row["client_order_id"]
            remote = broker.get_order(client_order_id=oid, exchange_order_id=row["exchange_order_id"])
            set_status(oid, remote.status, conn=conn)
            fills = broker.get_fills(client_order_id=oid, exchange_order_id=remote.exchange_order_id)
            for fill in fills:
                apply_fill_and_trade(
                    conn,
                    client_order_id=oid,
                    side=row["side"],
                    fill_ts=fill.fill_ts,
                    price=fill.price,
                    qty=fill.qty,
                    fee=fill.fee,
                    note=f"reconcile exchange_order_id={remote.exchange_order_id}",
                )

        bal = broker.get_balance()
        set_portfolio(conn, bal.cash_krw, bal.asset_qty)
        conn.commit()
    finally:
        conn.close()
