from __future__ import annotations

import math
import time

from ..config import settings
from ..db_core import ensure_db, get_portfolio, init_portfolio
from ..execution import apply_fill_and_trade, record_order_if_missing
from ..marketdata import fetch_orderbook_top
from ..notifier import format_event, notify
from ..observability import safety_event
from ..reason_codes import AMBIGUOUS_SUBMIT, RISKY_ORDER_BLOCK, SUBMIT_FAILED, SUBMIT_TIMEOUT
from .order_rules import get_effective_order_rules
from ..risk import evaluate_buy_guardrails, evaluate_order_submission_halt
from .. import runtime_state
from ..oms import (
    TERMINAL_ORDER_STATUSES,
    evaluate_unresolved_order_gate,
    new_client_order_id,
    payload_fingerprint,
    record_status_transition,
    record_submit_attempt,
    record_submit_blocked,
    record_submit_started,
    set_exchange_order_id,
    set_status,
)
from .base import Broker, BrokerSubmissionUnknownError, BrokerTemporaryError

POSITION_EPSILON = 1e-12
VALID_ORDER_SIDES = {"BUY", "SELL"}
UNSET_EVENT_FIELD = "-"

SUBMISSION_REASON_FAILED_BEFORE_SEND = "failed_before_send"
SUBMISSION_REASON_SENT_BUT_RESPONSE_TIMEOUT = "sent_but_response_timeout"
SUBMISSION_REASON_SENT_BUT_TRANSPORT_ERROR = "sent_but_transport_error"
SUBMISSION_REASON_AMBIGUOUS_RESPONSE = "ambiguous_response"
SUBMISSION_REASON_CONFIRMED_SUCCESS = "confirmed_success"


def _classify_temporary_submit_error(exc: Exception) -> tuple[str, bool]:
    detail = str(exc).lower()
    if "timeout" in detail or "timed out" in detail:
        return SUBMISSION_REASON_SENT_BUT_RESPONSE_TIMEOUT, True
    return SUBMISSION_REASON_SENT_BUT_TRANSPORT_ERROR, False


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

    rules = get_effective_order_rules(settings.PAIR).rules

    step = float(rules.qty_step)
    if math.isfinite(step) and step > 0:
        normalized = math.floor((normalized / step) + POSITION_EPSILON) * step

    max_decimals = int(rules.max_qty_decimals)
    if max_decimals > 0:
        scale = 10 ** max_decimals
        normalized = math.floor((normalized * scale) + POSITION_EPSILON) / scale

    if normalized <= 0:
        raise ValueError(f"normalized order qty is non-positive: {normalized}")

    min_qty = float(rules.min_qty)
    if min_qty > 0 and normalized < min_qty:
        raise ValueError(f"order qty below minimum: {normalized:.12f} < {min_qty:.12f}")

    min_notional = float(rules.min_notional_krw)
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

    rules = get_effective_order_rules(settings.PAIR).rules

    notional = float(qty) * float(market_price)
    min_notional = float(rules.min_notional_krw)
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


def _mark_submit_unknown(*, conn, client_order_id: str, submit_attempt_id: str, side: str, reason: str) -> None:
    record_status_transition(
        client_order_id,
        from_status="PENDING_SUBMIT",
        to_status="SUBMIT_UNKNOWN",
        reason=reason,
        conn=conn,
    )
    set_status(client_order_id, "SUBMIT_UNKNOWN", last_error=reason, conn=conn)
    notify(
        safety_event(
            "order_submit_unknown",
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_from="PENDING_SUBMIT",
            state_to="SUBMIT_UNKNOWN",
            reason_code=SUBMIT_TIMEOUT,
            side=side,
            status="SUBMIT_UNKNOWN",
            reason=reason,
        )
    )


def _mark_submit_failed(*, conn, client_order_id: str, submit_attempt_id: str, side: str, reason: str) -> None:
    record_status_transition(
        client_order_id,
        from_status="PENDING_SUBMIT",
        to_status="FAILED",
        reason=reason,
        conn=conn,
    )
    set_status(client_order_id, "FAILED", last_error=reason, conn=conn)
    notify(
        safety_event(
            "order_submit_failed",
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_from="PENDING_SUBMIT",
            state_to="FAILED",
            reason_code=SUBMIT_FAILED,
            side=side,
            status="FAILED",
            reason=reason,
        )
    )


def _mark_recovery_required(*, conn, client_order_id: str, side: str, from_status: str, reason: str) -> None:
    record_status_transition(
        client_order_id,
        from_status=from_status,
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
        safety_event(
            "recovery_required_transition",
            client_order_id=client_order_id,
            submit_attempt_id=UNSET_EVENT_FIELD,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_from=from_status,
            state_to="RECOVERY_REQUIRED",
            reason_code=AMBIGUOUS_SUBMIT,
            side=side,
            status="RECOVERY_REQUIRED",
            reason=reason,
        )
    )


def _block_new_submission_for_unresolved_risk(
    *,
    conn,
    client_order_id: str,
    side: str,
    qty: float,
    ts: int,
    reason_code: str,
    reason: str,
) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        submit_attempt_id=None,
        side=side,
        qty_req=qty,
        price=None,
        ts_ms=ts,
        status="FAILED",
    )
    persisted_reason = f"code={reason_code};reason={reason}"
    record_submit_blocked(client_order_id, status="FAILED", reason=persisted_reason, conn=conn)
    notify(
        safety_event(
            "order_submit_blocked",
            client_order_id=client_order_id,
            submit_attempt_id=client_order_id.split("_")[-1],
            reason_code=RISKY_ORDER_BLOCK,
            side=side,
            status="FAILED",
            reason_detail_code=reason_code,
            reason=persisted_reason,
        )
    )


def _record_submit_attempt_result(
    *,
    conn,
    client_order_id: str,
    submit_attempt_id: str,
    symbol: str,
    side: str,
    qty: float,
    ts: int,
    payload_hash: str,
    order_status: str,
    broker_response_summary: str,
    submission_reason_code: str,
    exception_class: str | None,
    timeout_flag: bool,
    exchange_order_id_obtained: bool,
) -> None:
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=None,
        submit_ts=ts,
        payload_fingerprint=payload_hash,
        broker_response_summary=broker_response_summary,
        submission_reason_code=submission_reason_code,
        exception_class=exception_class,
        timeout_flag=timeout_flag,
        exchange_order_id_obtained=exchange_order_id_obtained,
        order_status=order_status,
    )


def _record_submit_attempt_preflight(
    *,
    conn,
    client_order_id: str,
    submit_attempt_id: str,
    symbol: str,
    side: str,
    qty: float,
    ts: int,
    payload_hash: str,
) -> None:
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=None,
        submit_ts=ts,
        payload_fingerprint=payload_hash,
        broker_response_summary="submit_dispatched",
        submission_reason_code="submit_dispatched_preflight",
        exception_class=None,
        timeout_flag=False,
        exchange_order_id_obtained=False,
        order_status="PENDING_SUBMIT",
        event_type="submit_attempt_preflight",
    )


def _submit_via_standard_path(
    *,
    conn,
    broker: Broker,
    client_order_id: str,
    submit_attempt_id: str,
    side: str,
    qty: float,
    ts: int,
):
    symbol = settings.PAIR
    payload = {
        "client_order_id": client_order_id,
        "submit_attempt_id": submit_attempt_id,
        "symbol": symbol,
        "side": side,
        "qty": float(qty),
        "price": None,
        "submit_ts": int(ts),
    }
    payload_hash = payload_fingerprint(payload)

    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        side=side,
        qty_req=qty,
        price=None,
        ts_ms=ts,
        status="PENDING_SUBMIT",
    )
    record_submit_started(client_order_id, conn=conn)
    _record_submit_attempt_preflight(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        ts=ts,
        payload_hash=payload_hash,
    )
    notify(
        safety_event(
            "order_submit_started",
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_to="PENDING_SUBMIT",
            reason_code=UNSET_EVENT_FIELD,
            side=side,
            status="PENDING_SUBMIT",
        )
    )
    conn.commit()

    try:
        order = broker.place_order(client_order_id=client_order_id, side=side, qty=qty, price=None)
    except BrokerTemporaryError as e:
        err = BrokerSubmissionUnknownError(f"submit unknown: {type(e).__name__}: {e}")
        submission_reason_code, timeout_flag = _classify_temporary_submit_error(e)
        _mark_submit_unknown(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            reason=str(err),
        )
        _record_submit_attempt_result(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=symbol,
            side=side,
            qty=qty,
            ts=ts,
            payload_hash=payload_hash,
            order_status="SUBMIT_UNKNOWN",
            broker_response_summary=f"submit_exception={type(e).__name__};error={e}",
            submission_reason_code=submission_reason_code,
            exception_class=type(e).__name__,
            timeout_flag=timeout_flag,
            exchange_order_id_obtained=False,
        )
        conn.commit()
        return None
    except Exception as e:
        reason = f"submit failed: {type(e).__name__}: {e}"
        _mark_submit_failed(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            reason=reason,
        )
        _record_submit_attempt_result(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=symbol,
            side=side,
            qty=qty,
            ts=ts,
            payload_hash=payload_hash,
            order_status="FAILED",
            broker_response_summary=f"submit_exception={type(e).__name__};error={e}",
            submission_reason_code=SUBMISSION_REASON_FAILED_BEFORE_SEND,
            exception_class=type(e).__name__,
            timeout_flag=False,
            exchange_order_id_obtained=False,
        )
        conn.commit()
        return None

    if order.exchange_order_id:
        set_exchange_order_id(client_order_id, order.exchange_order_id, conn=conn)
        notify(
            safety_event(
                "exchange_order_id_attached",
                client_order_id=client_order_id,
                submit_attempt_id=submit_attempt_id,
                exchange_order_id=order.exchange_order_id,
                reason_code=UNSET_EVENT_FIELD,
                side=side,
                status=order.status,
            )
        )
    if not order.exchange_order_id:
        reason = "submit acknowledged without exchange_order_id; classification=SUBMIT_UNKNOWN"
        _mark_submit_unknown(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            reason=reason,
        )
        _record_submit_attempt_result(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=symbol,
            side=side,
            qty=qty,
            ts=ts,
            payload_hash=payload_hash,
            order_status="SUBMIT_UNKNOWN",
            broker_response_summary=f"broker_status={order.status};exchange_order_id=-",
            submission_reason_code=SUBMISSION_REASON_AMBIGUOUS_RESPONSE,
            exception_class=None,
            timeout_flag=False,
            exchange_order_id_obtained=False,
        )
        conn.commit()
        return None

    set_status(client_order_id, order.status, conn=conn)

    _record_submit_attempt_result(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        ts=ts,
        payload_hash=payload_hash,
        order_status=order.status,
        broker_response_summary=f"broker_status={order.status};exchange_order_id={order.exchange_order_id}",
        submission_reason_code=SUBMISSION_REASON_CONFIRMED_SUCCESS,
        exception_class=None,
        timeout_flag=False,
        exchange_order_id_obtained=True,
    )
    conn.commit()
    return order


def live_execute_signal(broker: Broker, signal: str, ts: int, market_price: float) -> dict | None:
    conn = ensure_db()
    try:
        init_portfolio(conn)
        state = runtime_state.snapshot()

        if state.halt_new_orders_blocked:
            halt_reason = f"runtime halted: code={state.halt_reason_code or '-'} reason={state.last_disable_reason or '-'}"
            notify(
                safety_event(  # CHANGED
                    "order_submit_blocked",
                    client_order_id=UNSET_EVENT_FIELD,
                    submit_attempt_id=UNSET_EVENT_FIELD,
                    exchange_order_id=UNSET_EVENT_FIELD,
                    status="HALTED",
                    state_to="HALTED",
                    reason_code=RISKY_ORDER_BLOCK,
                    halt_detail_code=state.halt_reason_code or "-",
                    reason=halt_reason,
                )
            )
            return None

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

        submit_attempt_id = _submit_attempt_id()
        client_order_id = _client_order_id(ts=ts, side=side, submit_attempt_id=submit_attempt_id)

        blocked, reason = evaluate_order_submission_halt(
            conn,
            ts_ms=int(ts),
            now_ms=int(time.time() * 1000),
            cash=float(cash),
            qty=float(qty),
            price=float(market_price),
        )
        if blocked:
            gate_blocked, reason_code, gate_reason = evaluate_unresolved_order_gate(
                conn,
                now_ms=int(time.time() * 1000),
                max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
            )
            if gate_blocked:
                _block_new_submission_for_unresolved_risk(
                    conn=conn,
                    client_order_id=client_order_id,
                    side=side,
                    qty=normalized_qty,
                    ts=ts,
                    reason_code=reason_code,
                    reason=gate_reason,
                )
                conn.commit()
                return None

            notify(f"live order placement blocked ({side}): {reason}")
            return None

        existing = conn.execute(
            "SELECT status FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if existing is not None:
            existing_status = str(existing["status"])
            if existing_status in TERMINAL_ORDER_STATUSES:
                reason = f"duplicate submit blocked: terminal status {existing_status}"
                record_submit_blocked(client_order_id, status=existing_status, reason=reason, conn=conn)
                notify(
                    safety_event(  # CHANGED
                        "order_submit_blocked",
                        client_order_id=client_order_id,
                        submit_attempt_id=submit_attempt_id,
                        side=side,
                        status=existing_status,
                        reason_code=RISKY_ORDER_BLOCK,
                        reason=reason,
                    )
                )
                conn.commit()
                return None

        order = _submit_via_standard_path(
            conn=conn,
            broker=broker,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            qty=normalized_qty,
            ts=ts,
        )
        if order is None:
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
