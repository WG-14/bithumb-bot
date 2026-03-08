from __future__ import annotations

import math
import time

from ..config import settings
from ..db_core import ensure_db, get_portfolio, init_portfolio
from ..execution import apply_fill_and_trade, record_order_if_missing
from ..marketdata import fetch_orderbook_top
from ..notifier import format_event, notify
from ..risk import evaluate_buy_guardrails, evaluate_order_submission_halt
from ..oms import new_client_order_id, record_status_transition, record_submit_started, set_exchange_order_id, set_status
from .base import Broker, BrokerSubmissionUnknownError, BrokerTemporaryError

POSITION_EPSILON = 1e-12


VALID_ORDER_SIDES = {"BUY", "SELL"}


def _submit_attempt_id() -> str:
    return new_client_order_id("attempt")


def _client_order_id(*, ts: int, side: str, submit_attempt_id: str) -> str:
    return f"live_{ts}_{side.lower()}_{submit_attempt_id}"


def _as_bps(value: float, base: float) -> float:
    if not math.isfinite(base) or base <= 0:
        return float("inf")
    return (value / base) * 10_000.0


def validate_order(*, signal: str, side: str, qty: float, market_price: float) -> None:
    if signal not in ("BUY", "SELL"):
        raise ValueError(f"unsupported signal: {signal}")
    if side not in VALID_ORDER_SIDES:
        raise ValueError(f"unsupported side: {side}")
    if not math.isfinite(float(market_price)) or float(market_price) <= 0:
        raise ValueError(f"invalid market_price: {market_price}")
    if not math.isfinite(float(qty)) or float(qty) <= 0:
        raise ValueError(f"invalid order qty: {qty}")


def normalize_order_qty(*, qty: float, market_price: float) -> float:
    normalized = float(qty)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"invalid order qty: {qty}")

    step = float(settings.LIVE_ORDER_QTY_STEP)
    if math.isfinite(step) and step > 0:
        normalized = math.floor((normalized / step) + POSITION_EPSILON) * step

    max_decimals = int(settings.LIVE_ORDER_MAX_QTY_DECIMALS)
    if max_decimals > 0:
        scale = 10 ** max_decimals
        normalized = math.floor((normalized * scale) + POSITION_EPSILON) / scale

    if normalized <= 0:
        raise ValueError(f"normalized order qty is non-positive: {normalized}")

    min_qty = float(settings.LIVE_MIN_ORDER_QTY)
    if min_qty > 0 and normalized < min_qty:
        raise ValueError(f"order qty below minimum: {normalized:.12f} < {min_qty:.12f}")

    min_notional = float(settings.MIN_ORDER_NOTIONAL_KRW)
    if min_notional > 0 and normalized * float(market_price) < min_notional:
        raise ValueError(
            f"normalized order notional below minimum: {normalized * float(market_price):.2f} < {min_notional:.2f}"
        )

    return normalized


def validate_pretrade(
    *,
    broker: Broker,
    side: str,
    qty: float,
    market_price: float,
) -> None:
    if not math.isfinite(float(qty)) or float(qty) <= 0:
        raise ValueError(f"invalid order qty: {qty}")
    if not math.isfinite(float(market_price)) or float(market_price) <= 0:
        raise ValueError(f"invalid market/reference price: {market_price}")

    notional = float(qty) * float(market_price)
    min_notional = float(settings.MIN_ORDER_NOTIONAL_KRW)
    if min_notional > 0 and notional < min_notional:
        raise ValueError(f"order notional below minimum: {notional:.2f} < {min_notional:.2f}")

    balance = broker.get_balance()
    if not math.isfinite(float(balance.cash_available)) or not math.isfinite(float(balance.asset_available)):
        raise ValueError("invalid broker balance payload")

    buffer_mult = 1.0 + max(0.0, float(settings.PRETRADE_BALANCE_BUFFER_BPS)) / 10_000.0
    if side == "BUY":
        fee_mult = 1.0 + max(0.0, float(settings.FEE_RATE))
        required_cash = notional * fee_mult * buffer_mult
        if float(balance.cash_available) + POSITION_EPSILON < required_cash:
            raise ValueError(
                f"insufficient available cash: need={required_cash:.2f} avail={float(balance.cash_available):.2f}"
            )
    elif side == "SELL":
        required_asset = float(qty) * buffer_mult
        if float(balance.asset_available) + POSITION_EPSILON < required_asset:
            raise ValueError(
                f"insufficient available asset: need={required_asset:.12f} avail={float(balance.asset_available):.12f}"
            )

    spread_limit_bps = float(settings.MAX_ORDERBOOK_SPREAD_BPS)
    slip_limit_bps = float(settings.MAX_MARKET_SLIPPAGE_BPS)
    if spread_limit_bps <= 0 and slip_limit_bps <= 0:
        return

    bid, ask = fetch_orderbook_top(settings.PAIR)
    if not math.isfinite(float(bid)) or not math.isfinite(float(ask)) or bid <= 0 or ask <= 0 or bid > ask:
        raise ValueError(f"invalid orderbook top: bid={bid} ask={ask}")

    mid = (float(bid) + float(ask)) / 2.0
    spread_bps = _as_bps(float(ask) - float(bid), mid)
    if spread_limit_bps > 0 and spread_bps > spread_limit_bps:
        raise ValueError(f"spread guard blocked: spread_bps={spread_bps:.2f} > limit={spread_limit_bps:.2f}")

    exec_price = float(ask) if side == "BUY" else float(bid)
    slippage_bps = _as_bps(abs(exec_price - float(market_price)), float(market_price))
    if slip_limit_bps > 0 and slippage_bps > slip_limit_bps:
        raise ValueError(f"slippage guard blocked: bps={slippage_bps:.2f} > limit={slip_limit_bps:.2f}")


def live_execute_signal(broker: Broker, signal: str, ts: int, market_price: float) -> dict | None:
    conn = ensure_db()
    try:
        init_portfolio(conn)
        cash, qty = get_portfolio(conn)

        if signal == "BUY" and qty <= POSITION_EPSILON:
            if not math.isfinite(float(market_price)) or float(market_price) <= 0:
                notify(f"live pretrade validation blocked (BUY): invalid market/reference price: {market_price}")
                return None

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

        try:
            normalized_qty = normalize_order_qty(qty=order_qty, market_price=market_price)
            validate_order(signal=signal, side=side, qty=normalized_qty, market_price=market_price)
            validate_pretrade(broker=broker, side=side, qty=normalized_qty, market_price=market_price)
        except ValueError as e:
            notify(f"live pretrade validation blocked ({side}): {e}")
            return None

        blocked, reason = evaluate_order_submission_halt(
            conn,
            ts_ms=int(ts),
            now_ms=int(time.time() * 1000),
            cash=float(cash),
            qty=float(qty),
            price=float(market_price),
        )
        if blocked:
            notify(f"live order placement blocked ({side}): {reason}")
            return None

        submit_attempt_id = _submit_attempt_id()
        client_order_id = _client_order_id(ts=ts, side=side, submit_attempt_id=submit_attempt_id)

        # Durable order intent before remote submit.
        record_order_if_missing(
            conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            qty_req=normalized_qty,
            price=None,
            ts_ms=ts,
            status="PENDING_SUBMIT",
        )
        record_submit_started(client_order_id, conn=conn)
        notify(format_event("order_submit_started", client_order_id=client_order_id, side=side, status="PENDING_SUBMIT"))
        conn.commit()

        try:
            order = broker.place_order(client_order_id=client_order_id, side=side, qty=normalized_qty, price=None)
        except BrokerTemporaryError as e:
            err = BrokerSubmissionUnknownError(f"submit unknown: {type(e).__name__}: {e}")
            record_status_transition(
                client_order_id,
                from_status="PENDING_SUBMIT",
                to_status="SUBMIT_UNKNOWN",
                reason=str(err),
                conn=conn,
            )
            set_status(client_order_id, "SUBMIT_UNKNOWN", last_error=str(err), conn=conn)
            notify(
                format_event(
                    "order_submit_unknown",
                    client_order_id=client_order_id,
                    side=side,
                    status="SUBMIT_UNKNOWN",
                    reason=str(err),
                )
            )
            conn.commit()
            return None

        if order.exchange_order_id:
            set_exchange_order_id(client_order_id, order.exchange_order_id, conn=conn)
            notify(
                format_event(
                    "exchange_order_id_attached",
                    client_order_id=client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    side=side,
                    status=order.status,
                )
            )
        set_status(client_order_id, order.status, conn=conn)

        if not order.exchange_order_id:
            reason = "submit acknowledged without exchange_order_id; manual recovery required"
            record_status_transition(
                client_order_id,
                from_status=order.status,
                to_status="RECOVERY_REQUIRED",
                reason=reason,
                conn=conn,
            )
            set_status(
                client_order_id,
                "RECOVERY_REQUIRED",
                last_error=reason,
                conn=conn,
            )
            notify(
                format_event(
                    "recovery_required_transition",
                    client_order_id=client_order_id,
                    side=side,
                    status="RECOVERY_REQUIRED",
                    reason=reason,
                )
            )
            conn.commit()
            return None

        fills = broker.get_fills(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
        trade = None
        for fill in fills:
            trade = apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=fill.fill_id,
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
