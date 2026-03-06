from __future__ import annotations

from ..config import settings
from ..db_core import ensure_db, get_portfolio, init_portfolio
from ..execution import apply_fill_and_trade, record_order_if_missing
from ..risk import evaluate_buy_guardrails
from ..oms import set_exchange_order_id, set_status
from .base import Broker

POSITION_EPSILON = 1e-12


def _client_order_id(ts: int, side: str) -> str:
    return f"live_{ts}_{side.lower()}"


def live_execute_signal(broker: Broker, signal: str, ts: int, market_price: float) -> dict | None:
    conn = ensure_db()
    try:
        init_portfolio(conn)
        cash, qty = get_portfolio(conn)

        if signal == "BUY" and qty <= POSITION_EPSILON:
            blocked, _ = evaluate_buy_guardrails(conn=conn, ts_ms=ts, cash=cash, qty=qty, price=market_price)
            if blocked:
                return None
            spend = cash * float(settings.BUY_FRACTION)
            if settings.MAX_ORDER_KRW > 0:
                spend = min(spend, float(settings.MAX_ORDER_KRW))
            if spend <= 0:
                return None
            order_qty = max(0.0, spend / market_price)
            side = "BUY"
        elif signal == "SELL" and qty > POSITION_EPSILON:
            order_qty = qty
            side = "SELL"
        else:
            return None

        client_order_id = _client_order_id(ts, side)
        row = conn.execute(
            "SELECT status FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if row and row["status"] in ("NEW", "PARTIAL", "FILLED"):
            return None

        record_order_if_missing(
            conn,
            client_order_id=client_order_id,
            side=side,
            qty_req=order_qty,
            price=None,
            ts_ms=ts,
        )
        order = broker.place_order(client_order_id=client_order_id, side=side, qty=order_qty, price=None)
        if order.exchange_order_id:
            set_exchange_order_id(client_order_id, order.exchange_order_id, conn=conn)
        set_status(client_order_id, order.status, conn=conn)

        fills = broker.get_fills(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
        trade = None
        for fill in fills:
            trade = apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee,
                note=f"live exchange_order_id={order.exchange_order_id}",
            ) or trade

        refreshed = broker.get_order(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
        set_status(client_order_id, refreshed.status, conn=conn)
        conn.commit()
        return trade
    finally:
        conn.close()
