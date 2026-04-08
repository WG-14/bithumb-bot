from __future__ import annotations

import inspect
import logging
import math
from typing import Any

from ..config import settings
from ..markets import canonical_market_with_raw
from ..risk import evaluate_buy_guardrails
from ..db_core import ensure_db, get_portfolio, init_portfolio
from ..marketdata import fetch_orderbook_top, validated_best_quote_prices
from ..notifier import notify
from ..observability import format_log_kv
from ..decision_context import load_recorded_strategy_decision_context
from .. import runtime_state
from ..dust import build_normalized_exposure
from ..oms import (
    build_client_order_id,
    build_order_intent_key,
    claim_order_intent_dedup,
    new_client_order_id,
    set_status,
    update_order_intent_dedup,
)
from ..execution import apply_fill_and_trade, record_order_if_missing

POSITION_EPSILON = 1e-12
RUN_LOG = logging.getLogger("bithumb_bot.run")


def _resolve_orderbook_market() -> str:
    # Resolve the configured pair once through the shared market helper so
    # paper execution always carries the canonical exchange market code.
    market, _raw_market = canonical_market_with_raw(settings.PAIR)
    return market


def _get_fill_price(signal: str, *, market: str) -> float | None:
    try:
        quote = fetch_orderbook_top(market)
        bid, ask = validated_best_quote_prices(quote, requested_market=market)
    except Exception as e:
        notify(f"paper_execute blocked: orderbook fetch failed ({e})")
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


def paper_execute(
    signal: str,
    ts: int,
    price: float,
    *,
    strategy_name: str | None = None,
    decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
) -> dict[str, Any] | None:
    market = _resolve_orderbook_market()
    if "market" in inspect.signature(_get_fill_price).parameters:
        fill_price = _get_fill_price(signal, market=market)
    else:
        # Preserve compatibility with tests or callers that still patch the
        # older single-argument helper.
        fill_price = _get_fill_price(signal)
    if fill_price is None:
        return None
    fee_rate = max(0.0, float(settings.PAPER_FEE_RATE))

    conn = ensure_db()
    client_order_id = ""
    intent_key = ""
    try:
        init_portfolio(conn)
        cash, qty = get_portfolio(conn)
        normalized_exposure = build_normalized_exposure(
            raw_qty_open=float(qty),
            dust_context=runtime_state.snapshot().last_reconcile_metadata,
        )
        decision_context, decision_loaded = load_recorded_strategy_decision_context(
            conn,
            decision_id=decision_id,
        )
        if decision_loaded:
            effective_flat = bool(decision_context.get("effective_flat"))
            entry_allowed = bool(decision_context.get("entry_allowed"))
            normalized_exposure_active = bool(decision_context.get("normalized_exposure_active"))
            open_exposure_qty = float(decision_context.get("open_exposure_qty", qty))
        else:
            effective_flat = bool(normalized_exposure.effective_flat)
            entry_allowed = bool(normalized_exposure.entry_allowed)
            normalized_exposure_active = bool(normalized_exposure.normalized_exposure_active)
            open_exposure_qty = float(normalized_exposure.open_exposure_qty)

        fee = 0.0
        trade_qty = 0.0

        if signal == "BUY" and effective_flat:
            # Harmless dust is operationally flat for re-entry. Do not let the
            # residual dust quantity re-trigger the duplicate-entry guardrail.
            guardrail_qty = 0.0 if entry_allowed else float(
                open_exposure_qty if normalized_exposure_active else qty
            )
            blocked, _ = evaluate_buy_guardrails(
                conn=conn,
                ts_ms=int(ts),
                cash=cash,
                qty=float(guardrail_qty),
                price=float(fill_price),
            )
            if blocked:
                return None

            spend = cash * float(settings.BUY_FRACTION)
            if settings.MAX_ORDER_KRW > 0:
                spend = min(spend, float(settings.MAX_ORDER_KRW))
            if spend <= 0:
                return None

            fee = spend * fee_rate
            spend_net = max(0.0, spend - fee)
            # Order sizing owns the rounding rule: round down one
            # representable float so the simulated BUY never exceeds the cash
            # budget. Fill application must only absorb the tiny residue this
            # leaves behind; it must not perform another quantity round-down.
            trade_qty = max(0.0, math.nextafter(spend_net / float(fill_price), 0.0))
            side = "BUY"

        elif signal == "SELL" and qty > POSITION_EPSILON:
            trade_qty = qty
            fee = (qty * float(fill_price)) * fee_rate
            side = "SELL"

        else:
            return None

        client_order_id = build_client_order_id(
            mode=settings.MODE,
            side=side,
            intent_ts=int(ts),
            nonce=new_client_order_id("paper"),
        )
        intent_key = build_order_intent_key(
            symbol=market,
            side=side,
            strategy_context=f"{settings.MODE}:{settings.STRATEGY_NAME}:{settings.INTERVAL}",
            intent_ts=int(ts),
            intent_type=("market_entry" if side == "BUY" else "market_exit"),
            qty=float(trade_qty),
        )
        claimed, existing_intent = claim_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            symbol=market,
            side=side,
            strategy_context=f"{settings.MODE}:{settings.STRATEGY_NAME}:{settings.INTERVAL}",
            intent_type=("market_entry" if side == "BUY" else "market_exit"),
            intent_ts=int(ts),
            qty=float(trade_qty),
            order_status="PENDING_SUBMIT",
        )
        if not claimed:
            existing_client_order_id = (
                str(existing_intent["client_order_id"])
                if existing_intent is not None and existing_intent["client_order_id"] is not None
                else "-"
            )
            existing_status = (
                str(existing_intent["order_status"])
                if existing_intent is not None and existing_intent["order_status"] is not None
                else "UNKNOWN"
            )
            RUN_LOG.info(
                format_log_kv(
                    "[SKIP] duplicate order intent",
                    mode=settings.MODE,
                    symbol=market,
                    side=side,
                    qty=f"{float(trade_qty):.12f}",
                    intent_ts=int(ts),
                    intent_key=intent_key,
                    reason=(
                        "duplicate intent already recorded "
                        f"existing_client_order_id={existing_client_order_id} "
                        f"existing_status={existing_status}"
                    ),
                )
            )
            notify(
                f"event=order_intent_dedup_skip symbol={market} side={side} qty={float(trade_qty)} "
                f"intent_ts={int(ts)} dedup_key={intent_key} existing_client_order_id={existing_client_order_id} "
                f"existing_status={existing_status}"
            )
            conn.commit()
            return None

        RUN_LOG.info(
            format_log_kv(
                "[RUN] submit order intent",
                mode=settings.MODE,
                symbol=market,
                signal_ts=int(ts),
                candle_ts=int(ts),
                side=side,
                qty=f"{float(trade_qty):.12f}",
                submit_qty=f"{float(trade_qty):.12f}",
                intent_ts=int(ts),
                intent_key=intent_key,
                client_order_id=client_order_id,
                reason=f"client_order_id={client_order_id}",
            )
        )
        note = f"client_order_id={client_order_id}; signal_price={price}"

        record_order_if_missing(
            conn,
            client_order_id=client_order_id,
            side=side,
            qty_req=float(trade_qty),
            price=float(fill_price),
            symbol=market,
            strategy_name=(strategy_name or settings.STRATEGY_NAME),
            entry_decision_id=(decision_id if side == "BUY" else None),
            exit_decision_id=(decision_id if side == "SELL" else None),
            decision_reason=decision_reason,
            exit_rule_name=exit_rule_name,
            ts_ms=int(ts),
        )

        trade = apply_fill_and_trade(
            conn,
            client_order_id=client_order_id,
            side=side,
            fill_id=None,
            fill_ts=int(ts),
            price=float(fill_price),
            qty=float(trade_qty),
            fee=float(fee),
            strategy_name=(strategy_name or settings.STRATEGY_NAME),
            entry_decision_id=(decision_id if side == "BUY" else None),
            exit_decision_id=(decision_id if side == "SELL" else None),
            exit_reason=(decision_reason if side == "SELL" else None),
            exit_rule_name=(exit_rule_name if side == "SELL" else None),
            note=note,
            pair=market,
            signal_ts=int(ts),
        )

        set_status(client_order_id, "FILLED", conn=conn)
        update_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            order_status="FILLED",
        )
        conn.commit()
        return trade

    except Exception:
        conn.rollback()
        if intent_key:
            retry_conn = ensure_db()
            try:
                update_order_intent_dedup(
                    retry_conn,
                    intent_key=intent_key,
                    client_order_id=client_order_id,
                    order_status="FAILED",
                )
                retry_conn.commit()
            except Exception:
                retry_conn.rollback()
            finally:
                retry_conn.close()
        raise

    finally:
        conn.close()
