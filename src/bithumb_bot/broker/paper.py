from __future__ import annotations

from typing import Any

from ..config import settings
from ..risk import evaluate_buy_guardrails
from ..db_core import ensure_db, init_portfolio, get_portfolio
from ..marketdata import fetch_orderbook_top
from ..notifier import notify
from ..oms import new_client_order_id, create_order, add_fill, set_status

POSITION_EPSILON = 1e-12


def _get_fill_price(signal: str) -> float | None:
    try:
        bid, ask = fetch_orderbook_top(settings.PAIR)
    except Exception as e:
        notify(f"paper_execute blocked: orderbook fetch failed ({e})")
        return None

    if bid <= 0 or ask <= 0 or ask < bid:
        notify(f"paper_execute blocked: invalid orderbook bid={bid} ask={ask}")
        return None

    mid = (bid + ask) / 2
    spread_bps = ((ask - bid) / mid) * 10000 if mid > 0 else float("inf")
    if spread_bps > float(settings.MAX_ORDERBOOK_SPREAD_BPS):
        notify(
            f"paper_execute blocked: abnormal spread {spread_bps:.2f}bps "
            f"(limit={settings.MAX_ORDERBOOK_SPREAD_BPS}bps)"
        )
        return None

    slip = float(settings.SLIPPAGE_BPS) / 10000.0
    if signal == "BUY":
        return ask * (1 + slip)
    if signal == "SELL":
        return bid * (1 - slip)
    return None


def paper_execute(signal: str, ts: int, price: float) -> dict[str, Any] | None:
    """
    paper trade -> portfolio/trades + orders/fills(OMS) 기록
    """
    fill_price = _get_fill_price(signal)
    if fill_price is None:
        return None

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
                conn=conn, ts_ms=int(ts), cash=cash, qty=qty, price=float(fill_price)
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
            trade_qty = spend_net / float(fill_price)

            cash_after = cash - spend
            qty_after = qty + trade_qty
            side = "BUY"

            client_order_id = new_client_order_id("paper")
            note = f"client_order_id={client_order_id}; signal_price={price}"
            create_order(
                client_order_id=client_order_id,
                side="BUY",
                qty_req=float(trade_qty),
                price=float(fill_price),
                status="NEW",
                ts_ms=int(ts),
                conn=conn,
            )
            add_fill(
                client_order_id=client_order_id,
                fill_ts=int(ts),
                price=float(fill_price),
                qty=float(trade_qty),
                fee=float(fee),
                conn=conn,
            )
            set_status(client_order_id, "FILLED", conn=conn)

        elif signal == "SELL" and qty > POSITION_EPSILON:
            proceeds = qty * float(fill_price)
            fee = proceeds * float(settings.FEE_RATE)

            cash_after = cash + (proceeds - fee)
            qty_after = 0.0
            trade_qty = qty
            side = "SELL"

            client_order_id = new_client_order_id("paper")
            note = f"client_order_id={client_order_id}; signal_price={price}"
            create_order(
                client_order_id=client_order_id,
                side="SELL",
                qty_req=float(trade_qty),
                price=float(fill_price),
                status="NEW",
                ts_ms=int(ts),
                conn=conn,
            )
            add_fill(
                client_order_id=client_order_id,
                fill_ts=int(ts),
                price=float(fill_price),
                qty=float(trade_qty),
                fee=float(fee),
                conn=conn,
            )
            set_status(client_order_id, "FILLED", conn=conn)

        else:
            return None

        conn.execute(
            "UPDATE portfolio SET cash_krw=?, asset_qty=? WHERE id=1",
            (float(cash_after), float(qty_after)),
        )

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
                float(fill_price),
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
            "price": float(fill_price),
            "qty": float(trade_qty),
            "fee": float(fee),
            "cash": float(cash_after),
            "asset": float(qty_after),
            "client_order_id": client_order_id,
        }

    finally:
        conn.close()
