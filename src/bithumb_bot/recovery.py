from __future__ import annotations

from .broker.base import Broker
from .db_core import ensure_db, init_portfolio, set_portfolio_breakdown
from .execution import apply_fill_and_trade, record_order_if_missing
from .oms import get_open_orders, set_exchange_order_id, set_status


LOCAL_RECONCILE_STATUSES = ("PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN")


def assert_no_open_orders() -> None:
    open_orders = get_open_orders()
    if open_orders:
        raise RuntimeError(f"Open orders exist (resume required): {open_orders}")


def reconcile_with_broker(broker: Broker) -> None:
    conn = ensure_db()
    try:
        init_portfolio(conn)

        placeholders = ",".join("?" for _ in LOCAL_RECONCILE_STATUSES)
        local_open = conn.execute(
            f"SELECT client_order_id, exchange_order_id, side, qty_req, status FROM orders WHERE status IN ({placeholders})",
            LOCAL_RECONCILE_STATUSES,
        ).fetchall()
        for row in local_open:
            oid = row["client_order_id"]
            if row["status"] == "SUBMIT_UNKNOWN" and not row["exchange_order_id"]:
                set_status(
                    oid,
                    "RECOVERY_REQUIRED",
                    last_error="submit_unknown without exchange_order_id; manual recovery required",
                    conn=conn,
                )
                continue

            remote = broker.get_order(client_order_id=oid, exchange_order_id=row["exchange_order_id"])
            if remote.exchange_order_id:
                set_exchange_order_id(oid, remote.exchange_order_id, conn=conn)
            set_status(oid, remote.status, conn=conn)
            fills = broker.get_fills(client_order_id=oid, exchange_order_id=remote.exchange_order_id)
            for fill in fills:
                apply_fill_and_trade(
                    conn,
                    client_order_id=oid,
                    side=row["side"],
                    fill_id=fill.fill_id,
                    fill_ts=fill.fill_ts,
                    price=fill.price,
                    qty=fill.qty,
                    fee=fill.fee,
                    note=f"reconcile exchange_order_id={remote.exchange_order_id}",
                )

        remote_open = broker.get_open_orders()
        known_exchange_ids = {
            str(r["exchange_order_id"])
            for r in conn.execute(
                "SELECT exchange_order_id FROM orders WHERE exchange_order_id IS NOT NULL"
            ).fetchall()
        }
        for remote in remote_open:
            exid = str(remote.exchange_order_id or "")
            if not exid or exid in known_exchange_ids:
                continue
            oid = f"remote_{exid}"
            record_order_if_missing(
                conn,
                client_order_id=oid,
                side=remote.side,
                qty_req=remote.qty_req,
                price=remote.price,
                ts_ms=remote.created_ts,
                status="SUBMIT_UNKNOWN",
            )
            set_exchange_order_id(oid, exid, conn=conn)
            set_status(oid, remote.status, last_error="stray remote open order detected", conn=conn)

        bal = broker.get_balance()
        set_portfolio_breakdown(
            conn,
            cash_available=bal.cash_available,
            cash_locked=bal.cash_locked,
            asset_available=bal.asset_available,
            asset_locked=bal.asset_locked,
        )
        conn.commit()
    finally:
        conn.close()
