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
from ..lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from ..order_sizing import (
    BuyExecutionAuthority,
    SellExecutionAuthority,
    build_buy_execution_sizing,
    build_sell_execution_sizing,
)
from .order_rules import get_effective_order_rules
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


def _floor_qty_for_paper_buy(*, qty: float, qty_step: float, max_qty_decimals: int) -> float:
    normalized_qty = max(0.0, float(qty))
    step = max(0.0, float(qty_step))
    if step > 0:
        normalized_qty = math.floor((normalized_qty / step) + 1e-12) * step
    decimals = max(0, int(max_qty_decimals))
    if decimals > 0:
        scale = 10**decimals
        normalized_qty = math.floor((normalized_qty * scale) + 1e-12) / scale
    return max(0.0, normalized_qty)


def _adjust_buy_qty_for_paper_cash_safety(
    *,
    qty: float,
    market_price: float,
    cash_available: float,
    fee_rate: float,
    qty_step: float,
    max_qty_decimals: int,
) -> float:
    if not math.isfinite(float(qty)) or float(qty) <= 0:
        return 0.0
    if not math.isfinite(float(market_price)) or float(market_price) <= 0:
        return 0.0
    if not math.isfinite(float(cash_available)) or float(cash_available) <= 0:
        return 0.0

    normalized_fee_rate = max(0.0, float(fee_rate))
    safe_qty = min(float(qty), float(cash_available) / (float(market_price) * (1.0 + normalized_fee_rate)))
    safe_qty = _floor_qty_for_paper_buy(
        qty=safe_qty,
        qty_step=qty_step,
        max_qty_decimals=max_qty_decimals,
    )
    if safe_qty <= 0:
        return 0.0

    total_cost = (safe_qty * float(market_price)) * (1.0 + normalized_fee_rate)
    if total_cost <= float(cash_available) + 1e-12:
        return safe_qty

    step = max(0.0, float(qty_step))
    if step > 0:
        adjusted_qty = max(0.0, safe_qty)
        for _ in range(8):
            if adjusted_qty <= 0:
                return 0.0
            adjusted_cost = (adjusted_qty * float(market_price)) * (1.0 + normalized_fee_rate)
            if adjusted_cost <= float(cash_available) + 1e-12:
                return adjusted_qty
            adjusted_qty = _floor_qty_for_paper_buy(
                qty=max(0.0, adjusted_qty - step),
                qty_step=qty_step,
                max_qty_decimals=max_qty_decimals,
            )
        return 0.0

    adjusted_qty = math.nextafter(safe_qty, 0.0)
    if adjusted_qty > 0:
        adjusted_cost = (adjusted_qty * float(market_price)) * (1.0 + normalized_fee_rate)
        if adjusted_cost <= float(cash_available) + 1e-12:
            return adjusted_qty
    return 0.0


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
        rules = get_effective_order_rules(settings.PAIR).rules
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        raw_open_exposure_qty = float(lot_snapshot.raw_open_exposure_qty)
        raw_total_asset_qty = max(float(qty), float(lot_snapshot.raw_total_asset_qty))
        dust_tracking_qty = float(lot_snapshot.dust_tracking_qty)
        open_lot_count = int(lot_snapshot.open_lot_count)
        dust_tracking_lot_count = int(lot_snapshot.dust_tracking_lot_count)
        # Paper SELL must follow the same contract as live/operator paths:
        # raw holdings stay observational; when lot-native snapshots exist they
        # define executable vs dust semantics and aggregate qty must not
        # recreate executable exposure.
        normalized_exposure = build_normalized_exposure(
            raw_qty_open=raw_open_exposure_qty,
            dust_context=runtime_state.snapshot().last_reconcile_metadata,
            raw_total_asset_qty=raw_total_asset_qty,
            open_exposure_qty=raw_open_exposure_qty,
            dust_tracking_qty=dust_tracking_qty,
            reserved_exit_qty=summarize_reserved_exit_qty(conn, pair=settings.PAIR),
            open_lot_count=open_lot_count,
            dust_tracking_lot_count=dust_tracking_lot_count,
            market_price=float(fill_price),
            min_qty=float(rules.min_qty),
            qty_step=float(rules.qty_step),
            min_notional_krw=float(rules.min_notional_krw),
            max_qty_decimals=int(rules.max_qty_decimals),
            exit_fee_ratio=float(settings.PAPER_FEE_RATE),
            exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
            exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
        )
        decision_context, decision_loaded = load_recorded_strategy_decision_context(
            conn,
            decision_id=decision_id,
        )
        if decision_loaded:
            position_state = (
                decision_context.get("position_state")
                if isinstance(decision_context.get("position_state"), dict)
                else {}
            )
            normalized_state = (
                position_state.get("normalized_exposure")
                if isinstance(position_state.get("normalized_exposure"), dict)
                else {}
            )
            effective_flat = bool(
                normalized_state.get("effective_flat", decision_context.get("effective_flat"))
            )
            entry_allowed = bool(
                normalized_state.get("entry_allowed", decision_context.get("entry_allowed"))
            )
            normalized_exposure_active = bool(
                normalized_state.get(
                    "normalized_exposure_active",
                    decision_context.get("normalized_exposure_active"),
                )
            )
            has_executable_exposure = bool(
                normalized_state.get(
                    "has_executable_exposure",
                    decision_context.get(
                        "has_executable_exposure",
                        normalized_state.get("normalized_exposure_qty", decision_context.get("normalized_exposure_qty", 0.0)) > 1e-12,
                    ),
                )
            )
            open_exposure_qty = float(
                normalized_state.get(
                    "open_exposure_qty",
                    decision_context.get("open_exposure_qty", raw_open_exposure_qty),
                )
            )
        else:
            effective_flat = bool(normalized_exposure.effective_flat)
            entry_allowed = bool(normalized_exposure.entry_allowed)
            normalized_exposure_active = bool(normalized_exposure.normalized_exposure_active)
            has_executable_exposure = bool(normalized_exposure.has_executable_exposure)
            open_exposure_qty = float(normalized_exposure.open_exposure_qty)

        fee = 0.0
        trade_qty = 0.0

        if signal == "BUY" and effective_flat:
            # Harmless dust is operationally flat for re-entry. Do not let the
            # residual dust quantity re-trigger the duplicate-entry guardrail.
            guardrail_qty = 0.0 if entry_allowed else float(
                open_exposure_qty if has_executable_exposure else qty
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

            entry_sizing = build_buy_execution_sizing(
                pair=settings.PAIR,
                cash_krw=float(cash),
                market_price=float(fill_price),
                fee_rate=float(settings.PAPER_FEE_RATE),
                entry_intent=(
                    entry.get("intent")
                    if isinstance((entry := decision_context.get("entry")), dict)
                    else None
                ),
                authority=BuyExecutionAuthority(
                    entry_allowed=bool(entry_allowed),
                    entry_allowed_truth_source="paper.decision_context.entry_allowed",
                ),
            )
            if not entry_sizing.allowed:
                return None

            trade_qty = _adjust_buy_qty_for_paper_cash_safety(
                qty=float(entry_sizing.executable_qty),
                market_price=float(fill_price),
                cash_available=float(cash),
                fee_rate=fee_rate,
                qty_step=float(rules.qty_step),
                max_qty_decimals=int(rules.max_qty_decimals),
            )
            if trade_qty <= 0:
                return None
            fee = (trade_qty * float(fill_price)) * fee_rate
            side = "BUY"

        elif signal == "SELL":
            exit_sizing = build_sell_execution_sizing(
                pair=settings.PAIR,
                market_price=float(fill_price),
                authority=SellExecutionAuthority(
                    sellable_executable_lot_count=int(normalized_exposure.sellable_executable_lot_count),
                    exit_allowed=bool(normalized_exposure.exit_allowed),
                    exit_block_reason=str(normalized_exposure.exit_block_reason),
                ),
                lot_definition=lot_snapshot.lot_definition,
            )
            if not exit_sizing.allowed:
                return None
            trade_qty = float(exit_sizing.executable_qty)
            fee = (trade_qty * float(fill_price)) * fee_rate
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
            intended_lot_count=int(entry_sizing.intended_lot_count if side == "BUY" else exit_sizing.intended_lot_count),
            executable_lot_count=int(entry_sizing.executable_lot_count if side == "BUY" else exit_sizing.executable_lot_count),
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
            intended_lot_count=int(entry_sizing.intended_lot_count if side == "BUY" else exit_sizing.intended_lot_count),
            executable_lot_count=int(entry_sizing.executable_lot_count if side == "BUY" else exit_sizing.executable_lot_count),
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
