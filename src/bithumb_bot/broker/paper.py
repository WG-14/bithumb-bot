from __future__ import annotations

from typing import Any

from ..config import settings
from ..risk import evaluate_buy_guardrails
from ..db_core import ensure_db, init_portfolio, get_portfolio
from ..oms import new_client_order_id, create_order, add_fill, set_status

POSITION_EPSILON = 1e-12


def paper_execute(signal: str, ts: int, price: float) -> dict[str, Any] | None:
    """
    paper trade -> portfolio/trades + orders/fills(OMS) 기록
    """
    conn = ensure_db()
    try:
        init_portfolio(conn)
        cash, qty = get_portfolio(conn)

        fee = 0.0
        trade_qty = 0.0
        side: str | None = None
        cash_after = cash
        qty_after = qty
        note = None
        client_order_id: str | None = None

        if signal == "BUY" and qty <= POSITION_EPSILON:
            blocked, reason = evaluate_buy_guardrails(
                conn=conn, ts_ms=int(ts), cash=cash, qty=qty, price=float(price)
            )
            if blocked:
                return None

            spend = cash * float(settings.BUY_FRACTION)
            if settings.MAX_ORDER_KRW > 0:
                spend = min(spend, float(settings.MAX_ORDER_KRW))
            if spend <= 0:
                return None

            fee = spend * float(settings.FEE_RATE)
            spend_net = max(0.0, spend - fee)
            trade_qty = spend_net / float(price)

            cash_after = cash - spend
            qty_after = qty + trade_qty
            side = "BUY"

            client_order_id = new_client_order_id("paper")
            note = f"client_order_id={client_order_id}"
            create_order(
                client_order_id=client_order_id,
                side="BUY",
                qty_req=float(trade_qty),
                price=float(price),
                status="NEW",
                ts_ms=int(ts),
                conn=conn,
            )
            add_fill(
                client_order_id=client_order_id,
                fill_ts=int(ts),
                price=float(price),
                qty=float(trade_qty),
                fee=float(fee),
                conn=conn,
            )
            set_status(client_order_id, "FILLED", conn=conn)

        elif signal == "SELL" and qty > POSITION_EPSILON:
            proceeds = qty * float(price)
            fee = proceeds * float(settings.FEE_RATE)

            cash_after = cash + (proceeds - fee)
            qty_after = 0.0
            trade_qty = qty
            side = "SELL"

            client_order_id = new_client_order_id("paper")
            note = f"client_order_id={client_order_id}"
            create_order(
                client_order_id=client_order_id,
                side="SELL",
                qty_req=float(trade_qty),
                price=float(price),
                status="NEW",
                ts_ms=int(ts),
                conn=conn,
            )
            add_fill(
                client_order_id=client_order_id,
                fill_ts=int(ts),
                price=float(price),
                qty=float(trade_qty),
                fee=float(fee),
                conn=conn,
            )
            set_status(client_order_id, "FILLED", conn=conn)

        else:
            return None

        # portfolio update
        conn.execute(
            "UPDATE portfolio SET cash_krw=?, asset_qty=? WHERE id=1",
            (float(cash_after), float(qty_after)),
        )

        # trades record (db_core schema)
        conn.execute(
            """
            INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(ts),
                settings.PAIR,
                settings.INTERVAL,
                side,
                float(price),
                float(trade_qty),
                float(fee),
                float(cash_after),
                float(qty_after),
                note,
            ),
        )
        conn.commit()

        return {
            "ts": int(ts),
            "side": side,
            "price": float(price),
            "qty": float(trade_qty),
            "fee": float(fee),
            "cash": float(cash_after),
            "asset": float(qty_after),
            "client_order_id": client_order_id,
        }

    finally:
        conn.close()
