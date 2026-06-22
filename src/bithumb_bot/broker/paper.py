from __future__ import annotations

import inspect
import json
import logging
import math
from dataclasses import dataclass
from typing import Any

from ..config import settings
from ..execution_reality_contract import build_execution_reality_contract
from ..markets import canonical_market_with_raw
from ..risk_contract import SubmitPlan
from ..runtime_risk_engine import RuntimeRiskEngineAdapter
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
    collect_risky_order_state,
    new_client_order_id,
    payload_fingerprint,
    record_submit_attempt,
    set_status,
    update_order_intent_dedup,
)
from ..runtime_risk_engine import _classify_unresolved_state
from ..execution import apply_fill_and_trade, record_order_if_missing
from .paper_execution import (
    ImmediateTopOfBookPaperAdapter,
    PaperExecutionRequest,
    StressPaperExecutionAdapter,
)

POSITION_EPSILON = 1e-12
RUN_LOG = logging.getLogger("bithumb_bot.run")
_PAPER_EXECUTION_MODELS = {"immediate", "stress"}


@dataclass(frozen=True)
class _PaperQuoteContext:
    fill_price: float
    reference_price: float
    best_bid: float
    best_ask: float
    spread_bps: float
    quote_source: str
    quote_age_ms: int | None = None


def _resolve_orderbook_market() -> str:
    # Resolve the configured pair once through the shared market helper so
    # paper execution always carries the canonical exchange market code.
    market, _raw_market = canonical_market_with_raw(settings.PAIR)
    return market


def _get_fill_price(signal: str, *, market: str) -> float | None:
    quote_context = _get_paper_quote_context(signal, market=market)
    return quote_context.fill_price if quote_context is not None else None


def _get_paper_quote_context(signal: str, *, market: str) -> _PaperQuoteContext | None:
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
        return _PaperQuoteContext(
            fill_price=ask * (1 + slip),
            reference_price=ask,
            best_bid=bid,
            best_ask=ask,
            spread_bps=spread_bps,
            quote_source=type(quote).__name__,
        )
    if signal == "SELL":
        return _PaperQuoteContext(
            fill_price=bid * (1 - slip),
            reference_price=bid,
            best_bid=bid,
            best_ask=ask,
            spread_bps=spread_bps,
            quote_source=type(quote).__name__,
        )
    return None


def _paper_execution_model_name() -> str:
    model = str(getattr(settings, "PAPER_EXECUTION_MODEL", "immediate") or "immediate").strip().lower()
    if model not in _PAPER_EXECUTION_MODELS:
        allowed = ", ".join(sorted(_PAPER_EXECUTION_MODELS))
        raise ValueError(f"PAPER_EXECUTION_MODEL must be one of {{{allowed}}}, got {model!r}")
    return model


def _validate_unit_interval_config(name: str, value: object) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.0 or parsed > 1.0:
        raise ValueError(f"{name} must be a finite value between 0 and 1, got {parsed!r}")
    return parsed


def _validate_paper_execution_config() -> None:
    model = _paper_execution_model_name()
    latency_ms = int(getattr(settings, "PAPER_EXECUTION_LATENCY_MS", 0))
    if latency_ms < 0:
        raise ValueError(f"PAPER_EXECUTION_LATENCY_MS must be >= 0, got {latency_ms!r}")
    _validate_unit_interval_config(
        "PAPER_EXECUTION_PARTIAL_FILL_RATE",
        getattr(settings, "PAPER_EXECUTION_PARTIAL_FILL_RATE", 0.0),
    )
    partial_fraction = _validate_unit_interval_config(
        "PAPER_EXECUTION_PARTIAL_FILL_FRACTION",
        getattr(settings, "PAPER_EXECUTION_PARTIAL_FILL_FRACTION", 0.5),
    )
    if model == "stress" and (partial_fraction <= 0.0 or partial_fraction >= 1.0):
        raise ValueError(
            "PAPER_EXECUTION_PARTIAL_FILL_FRACTION must be > 0 and < 1 "
            f"for paper partial-fill semantics, got {partial_fraction!r}"
        )
    _validate_unit_interval_config(
        "PAPER_EXECUTION_ORDER_FAILURE_RATE",
        getattr(settings, "PAPER_EXECUTION_ORDER_FAILURE_RATE", 0.0),
    )


def _paper_stress_seed() -> int | None:
    raw = getattr(settings, "PAPER_EXECUTION_STRESS_SEED", None)
    if raw is None or str(raw).strip() == "":
        return None
    return int(raw)


def _build_paper_execution_request(
    *,
    side: str,
    ts: int,
    trade_qty: float,
    fill_price: float,
    quote_context: _PaperQuoteContext | None,
    intent_key: str,
    market: str,
    fee_rate: float,
) -> PaperExecutionRequest:
    seed = _paper_stress_seed()
    reference_price = (
        float(quote_context.reference_price)
        if quote_context is not None and _paper_execution_model_name() == "stress"
        else float(fill_price)
    )
    seed_inputs = {
        "intent_key": intent_key,
        "signal_ts": int(ts),
        "decision_ts": int(ts),
        "side": side,
        "symbol": market,
        "requested_qty": round(float(trade_qty), 12),
        "reference_price": round(float(reference_price), 8),
    }
    model_name = _paper_execution_model_name()
    execution_reality_level = (
        "paper_stress_top_of_book"
        if model_name == "stress"
        else "paper_immediate_top_of_book"
    )
    execution_contract = build_execution_reality_contract(
        fill_reference_policy="paper_top_of_book",
        decision_guard_ms=0,
        max_quote_wait_ms=0,
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
        allow_same_candle_close_fill=False,
        quote_source=(quote_context.quote_source if quote_context is not None else "unknown"),
        quote_age_limit_ms=(quote_context.quote_age_ms if quote_context is not None else None),
        top_of_book_required=True,
        top_of_book_is_full_depth=False,
        depth_required=False,
        trade_tick_required=False,
        queue_position_required=False,
        intra_candle_path_available=False,
        latency_model={
            "type": model_name,
            "latency_ms": int(getattr(settings, "PAPER_EXECUTION_LATENCY_MS", 0)) if model_name == "stress" else 0,
        },
        partial_fill_model={
            "type": model_name,
            "partial_fill_rate": (
                float(getattr(settings, "PAPER_EXECUTION_PARTIAL_FILL_RATE", 0.0))
                if model_name == "stress"
                else 0.0
            ),
        },
        order_failure_model={
            "type": model_name,
            "order_failure_rate": (
                float(getattr(settings, "PAPER_EXECUTION_ORDER_FAILURE_RATE", 0.0))
                if model_name == "stress"
                else 0.0
            ),
        },
        fee_source="paper_runtime_settings",
        slippage_source="paper_runtime_settings",
        calibration_required=False,
        execution_reality_level=execution_reality_level,
        extra={
            "quote_evidence_available": quote_context is not None,
            "depth_available": False,
            "trade_ticks_available": False,
            "queue_position_available": False,
            "market_impact_model_available": False,
            "intra_candle_path_required": False,
            "market": market,
        },
    )
    return PaperExecutionRequest(
        signal_ts=int(ts),
        decision_ts=int(ts),
        side=side,
        requested_qty=float(trade_qty),
        reference_price=reference_price,
        fee_rate=float(fee_rate),
        slippage_bps=float(settings.SLIPPAGE_BPS),
        best_bid=(float(quote_context.best_bid) if quote_context is not None else None),
        best_ask=(float(quote_context.best_ask) if quote_context is not None else None),
        spread_bps=(float(quote_context.spread_bps) if quote_context is not None else None),
        quote_source=(quote_context.quote_source if quote_context is not None else "unknown"),
        quote_age_ms=(quote_context.quote_age_ms if quote_context is not None else None),
        execution_reality_level=execution_reality_level,
        execution_reality_contract=execution_contract,
        base_seed=seed,
        seed_derivation_inputs=seed_inputs,
    )


def _build_paper_execution_adapter():
    if _paper_execution_model_name() == "stress":
        return StressPaperExecutionAdapter(
            fee_rate=float(settings.PAPER_FEE_RATE),
            slippage_bps=float(settings.SLIPPAGE_BPS),
            latency_ms=int(getattr(settings, "PAPER_EXECUTION_LATENCY_MS", 0)),
            partial_fill_rate=float(getattr(settings, "PAPER_EXECUTION_PARTIAL_FILL_RATE", 0.0)),
            partial_fill_fraction=float(getattr(settings, "PAPER_EXECUTION_PARTIAL_FILL_FRACTION", 0.5)),
            order_failure_rate=float(getattr(settings, "PAPER_EXECUTION_ORDER_FAILURE_RATE", 0.0)),
            seed=_paper_stress_seed(),
        )
    return ImmediateTopOfBookPaperAdapter()


def _record_paper_execution_evidence(
    conn,
    *,
    client_order_id: str,
    market: str,
    side: str,
    qty: float,
    price: float | None,
    ts: int,
    order_status: str,
    evidence: dict[str, Any],
) -> None:
    evidence_json = json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        symbol=market,
        side=side,
        qty=float(qty),
        price=price,
        submit_ts=int(ts),
        payload_fingerprint=payload_fingerprint(evidence),
        broker_response_summary=f"paper_execution fill_status={evidence.get('fill_status')}",
        submission_reason_code=f"paper_execution_{evidence.get('fill_status')}",
        exception_class=None,
        timeout_flag=False,
        submit_evidence=evidence_json,
        exchange_order_id_obtained=False,
        order_status=order_status,
        submit_attempt_id=f"{client_order_id}:paper_execution",
        submit_phase="paper_execution",
        submit_plan_id=f"{client_order_id}:paper_execution",
        message="paper execution lifecycle evidence",
        order_type="market",
    )


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
    execution_submit_plan: object | None = None,
) -> dict[str, Any] | None:
    _validate_paper_execution_config()
    market = _resolve_orderbook_market()
    fee_rate = max(0.0, float(settings.PAPER_FEE_RATE))
    plan_payload: dict[str, object] | None = None
    plan_hash: str | None = None
    plan_side = str(signal).upper()
    if execution_submit_plan is not None:
        from ..decision_equivalence import sha256_prefixed
        from ..execution_service import ExecutionSubmitPlan, validate_execution_submit_plan_payload

        if not isinstance(execution_submit_plan, ExecutionSubmitPlan):
            RUN_LOG.warning(
                format_log_kv(
                    "[ORDER_SKIP] paper submit plan rejected",
                    reason="paper_dict_only_submit_plan_not_authority",
                    signal=str(signal).upper(),
                )
            )
            return None
        plan_payload = execution_submit_plan.as_final_payload(
            extra={
                "paper_submit_plan_consumed": True,
                "paper_submit_plan_adjustment_reason": "none",
            }
        )
        validate_execution_submit_plan_payload(plan_payload, field_name="paper_submit_plan")
        plan_hash = sha256_prefixed(execution_submit_plan.as_dict())
        plan_side = str(execution_submit_plan.side or "").upper()
        if plan_side not in {"BUY", "SELL"}:
            RUN_LOG.warning(
                format_log_kv(
                    "[ORDER_SKIP] paper submit plan rejected",
                    reason="paper_submit_plan_non_submittable_side",
                    signal=str(signal).upper(),
                    submit_plan_side=plan_side,
                )
            )
            return None
        if not bool(execution_submit_plan.submit_expected) or str(execution_submit_plan.block_reason or "none") != "none":
            RUN_LOG.warning(
                format_log_kv(
                    "[ORDER_SKIP] paper submit plan blocked",
                    reason=str(execution_submit_plan.block_reason or "paper_submit_plan_submit_not_expected"),
                    signal=str(signal).upper(),
                    submit_plan_side=plan_side,
                    submit_plan_source=execution_submit_plan.source,
                    submit_plan_authority=execution_submit_plan.authority,
                )
            )
            return None

    conn = ensure_db()
    client_order_id = ""
    intent_key = ""
    try:
        init_portfolio(conn)
        cash, qty = get_portfolio(conn)
        unresolved_state = dict(
            collect_risky_order_state(
                conn,
                now_ms=int(ts),
                max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
            )
        )
        unresolved_blocked, unresolved_reason_code, unresolved_reason = _classify_unresolved_state(
            unresolved_state,
            max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
        )
        if unresolved_blocked:
            RUN_LOG.info(
                format_log_kv(
                    "[SKIP] unresolved order gate",
                    mode=settings.MODE,
                    symbol=market,
                    signal=str(signal).upper(),
                    side=str(signal).upper(),
                    signal_ts=int(ts),
                    reason_code=unresolved_reason_code,
                    reason=unresolved_reason,
                )
            )
            notify(
                f"event=paper_order_skip mode={settings.MODE} symbol={market} signal={str(signal).upper()} "
                f"reason_code={unresolved_reason_code} reason={unresolved_reason}"
            )
            conn.commit()
            return None
        quote_context: _PaperQuoteContext | None = None
        if _get_fill_price.__module__ == __name__ and _get_fill_price.__name__ == "_get_fill_price":
            quote_context = _get_paper_quote_context(plan_side, market=market)
            fill_price = quote_context.fill_price if quote_context is not None else None
        elif "market" in inspect.signature(_get_fill_price).parameters:
            fill_price = _get_fill_price(plan_side, market=market)
        else:
            # Preserve compatibility with tests or callers that still patch the
            # older single-argument helper.
            fill_price = _get_fill_price(plan_side)
        if fill_price is None:
            return None
        current_qty = float(qty)
        pre_submit_risk = RuntimeRiskEngineAdapter(conn).evaluate_pre_submit(  # broker=paper_not_applicable
            plan=SubmitPlan(side=str(signal).upper(), qty=0.0, source="paper_pre_submit"),
            ts_ms=int(ts),
            now_ms=int(ts),
            cash=float(cash),
            submit_qty=0.0,
            current_asset_qty=current_qty,
            price=float(fill_price),
            mark_price_source="paper_fill_price",
            evaluation_origin="paper_pre_submit",
        )
        if pre_submit_risk.status != "ALLOW":
            risk_fields = pre_submit_risk.identity_fields()
            RUN_LOG.info(
                format_log_kv(
                    "[SKIP] paper risk gate",
                    mode=settings.MODE,
                    symbol=market,
                    signal=str(signal).upper(),
                    side=str(signal).upper(),
                    signal_ts=int(ts),
                    reason_code=pre_submit_risk.reason_code,
                    reason=pre_submit_risk.reason,
                    **risk_fields,
                )
            )
            notify(
                f"event=paper_order_skip mode={settings.MODE} symbol={market} signal={str(signal).upper()} "
                f"reason_code={pre_submit_risk.reason_code} reason={pre_submit_risk.reason} "
                f"risk_decision_hash={pre_submit_risk.risk_decision_hash}"
            )
            conn.commit()
            return None
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
        intended_lot_count = 0
        executable_lot_count = 0

        paper_submit_plan_consumed = plan_payload is not None
        paper_submit_plan_adjustment_reason = "none"

        if plan_payload is not None:
            side = plan_side
            try:
                requested_plan_qty = float(plan_payload.get("qty") or 0.0)
            except (TypeError, ValueError):
                RUN_LOG.warning(
                    format_log_kv(
                        "[ORDER_SKIP] paper submit plan rejected",
                        reason="paper_submit_plan_invalid_qty",
                        submit_plan_source=str(plan_payload.get("source") or "-"),
                        submit_plan_authority=str(plan_payload.get("authority") or "-"),
                    )
                )
                return None
            if requested_plan_qty <= 0.0:
                return None
            if side == "BUY":
                if str(plan_payload.get("source") or "") != "target_delta" and (
                    not bool(effective_flat) or not bool(entry_allowed)
                ):
                    RUN_LOG.warning(
                        format_log_kv(
                            "[ORDER_SKIP] paper submit plan rejected",
                            reason="paper_submit_plan_entry_not_allowed",
                            effective_flat=1 if bool(effective_flat) else 0,
                            entry_allowed=1 if bool(entry_allowed) else 0,
                            submit_plan_source=str(plan_payload.get("source") or "-"),
                            submit_plan_authority=str(plan_payload.get("authority") or "-"),
                        )
                    )
                    return None
                buy_risk = RuntimeRiskEngineAdapter(conn).evaluate_buy_intent(
                    ts_ms=int(ts),
                    cash=cash,
                    qty=0.0,
                    price=float(fill_price),
                )
                if buy_risk.status != "ALLOW":
                    return None
                trade_qty = _adjust_buy_qty_for_paper_cash_safety(
                    qty=requested_plan_qty,
                    market_price=float(fill_price),
                    cash_available=float(cash),
                    fee_rate=fee_rate,
                    qty_step=float(rules.qty_step),
                    max_qty_decimals=int(rules.max_qty_decimals),
                )
                if trade_qty <= 0:
                    return None
                if (
                    trade_qty + 1e-12 < float(rules.min_qty)
                    or (trade_qty * float(fill_price)) + 1e-9 < float(rules.min_notional_krw)
                ):
                    RUN_LOG.warning(
                        format_log_kv(
                            "[ORDER_SKIP] paper submit plan rejected",
                            reason="paper_submit_plan_below_order_rules",
                            qty=f"{float(trade_qty):.12f}",
                            notional_krw=f"{float(trade_qty) * float(fill_price):.8f}",
                            min_qty=f"{float(rules.min_qty):.12f}",
                            min_notional_krw=f"{float(rules.min_notional_krw):.8f}",
                            submit_plan_source=str(plan_payload.get("source") or "-"),
                            submit_plan_authority=str(plan_payload.get("authority") or "-"),
                        )
                    )
                    return None
                if abs(float(trade_qty) - requested_plan_qty) > 1e-12:
                    paper_submit_plan_adjustment_reason = "paper_cash_safety_qty_reduced"
                fee = (trade_qty * float(fill_price)) * fee_rate
                intended_lot_count = int(plan_payload.get("intended_lot_count") or 0)
                executable_lot_count = int(plan_payload.get("executable_lot_count") or 0)
            elif side == "SELL":
                if requested_plan_qty > float(normalized_exposure.sellable_executable_qty) + 1e-12:
                    RUN_LOG.warning(
                        format_log_kv(
                            "[ORDER_SKIP] paper submit plan rejected",
                            reason="paper_submit_plan_qty_exceeds_sellable_executable_qty",
                            requested_qty=f"{requested_plan_qty:.12f}",
                            sellable_qty=f"{float(normalized_exposure.sellable_executable_qty):.12f}",
                            submit_plan_source=str(plan_payload.get("source") or "-"),
                            submit_plan_authority=str(plan_payload.get("authority") or "-"),
                        )
                    )
                    return None
                if (
                    requested_plan_qty + 1e-12 < float(rules.min_qty)
                    or (requested_plan_qty * float(fill_price)) + 1e-9 < float(rules.min_notional_krw)
                ):
                    RUN_LOG.warning(
                        format_log_kv(
                            "[ORDER_SKIP] paper submit plan rejected",
                            reason="paper_submit_plan_below_order_rules",
                            qty=f"{requested_plan_qty:.12f}",
                            notional_krw=f"{requested_plan_qty * float(fill_price):.8f}",
                            min_qty=f"{float(rules.min_qty):.12f}",
                            min_notional_krw=f"{float(rules.min_notional_krw):.8f}",
                            submit_plan_source=str(plan_payload.get("source") or "-"),
                            submit_plan_authority=str(plan_payload.get("authority") or "-"),
                        )
                    )
                    return None
                trade_qty = requested_plan_qty
                fee = (trade_qty * float(fill_price)) * fee_rate
                intended_lot_count = int(
                    plan_payload.get(
                        "target_executable_lot_count",
                        plan_payload.get("executable_lot_count", normalized_exposure.sellable_executable_lot_count),
                    )
                    or 0
                )
                executable_lot_count = intended_lot_count
            else:
                return None

        elif signal == "BUY" and effective_flat:
            # Harmless dust is operationally flat for re-entry. Do not let the
            # residual dust quantity re-trigger the duplicate-entry guardrail.
            guardrail_qty = 0.0 if entry_allowed else float(
                open_exposure_qty if has_executable_exposure else qty
            )
            buy_risk = RuntimeRiskEngineAdapter(conn).evaluate_buy_intent(
                ts_ms=int(ts),
                cash=cash,
                qty=float(guardrail_qty),
                price=float(fill_price),
            )
            if buy_risk.status != "ALLOW":
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
            intended_lot_count = int(entry_sizing.intended_lot_count)
            executable_lot_count = int(entry_sizing.executable_lot_count)

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
            intended_lot_count = int(exit_sizing.intended_lot_count)
            executable_lot_count = int(exit_sizing.executable_lot_count)

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
            intended_lot_count=int(intended_lot_count),
            executable_lot_count=int(executable_lot_count),
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
            intended_lot_count=int(intended_lot_count),
            executable_lot_count=int(executable_lot_count),
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
                execution_plan_bundle_present=1 if paper_submit_plan_consumed else 0,
                paper_submit_plan_consumed=1 if paper_submit_plan_consumed else 0,
                paper_submit_plan_hash=str(plan_hash or "-"),
                submit_plan_source=str(plan_payload.get("source") if plan_payload else "-"),
                submit_plan_authority=str(plan_payload.get("authority") if plan_payload else "-"),
                paper_submit_plan_adjustment_reason=paper_submit_plan_adjustment_reason,
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
            status="PENDING_SUBMIT",
            local_intent_state="PENDING_SUBMIT",
        )

        execution_request = _build_paper_execution_request(
            side=side,
            ts=int(ts),
            trade_qty=float(trade_qty),
            fill_price=float(fill_price),
            quote_context=quote_context,
            intent_key=intent_key,
            market=market,
            fee_rate=fee_rate,
        )
        execution_result = _build_paper_execution_adapter().execute(execution_request)
        _record_paper_execution_evidence(
            conn,
            client_order_id=client_order_id,
            market=market,
            side=side,
            qty=float(execution_result.requested_qty),
            price=(
                float(execution_result.avg_fill_price)
                if execution_result.avg_fill_price is not None
                else float(fill_price)
            ),
            ts=int(ts),
            order_status=(
                "FAILED"
                if execution_result.fill_status == "failed"
                else "PARTIAL"
                if execution_result.fill_status == "partial"
                else "FILLED"
            ),
            evidence={
                **execution_result.evidence,
                "execution_plan_bundle_present": bool(paper_submit_plan_consumed),
                "submit_plan_source": (None if plan_payload is None else plan_payload.get("source")),
                "submit_plan_authority": (None if plan_payload is None else plan_payload.get("authority")),
                "paper_submit_plan_consumed": bool(paper_submit_plan_consumed),
                "paper_submit_plan_hash": plan_hash,
                "paper_submit_plan_adjustment_reason": paper_submit_plan_adjustment_reason,
                "paper_submit_plan": plan_payload,
            },
        )

        if execution_result.fill_status == "failed" or execution_result.filled_qty <= 0.0:
            set_status(
                client_order_id,
                "FAILED",
                last_error="paper stress execution failed before fill accounting",
                conn=conn,
            )
            update_order_intent_dedup(
                conn,
                intent_key=intent_key,
                client_order_id=client_order_id,
                order_status="FAILED",
                last_error="paper stress execution failed before fill accounting",
            )
            conn.commit()
            return None

        trade = apply_fill_and_trade(
            conn,
            client_order_id=client_order_id,
            side=side,
            fill_id=None,
            fill_ts=int(ts) + int(execution_result.latency_ms),
            price=float(execution_result.avg_fill_price),
            qty=float(execution_result.filled_qty),
            fee=float(execution_result.fee),
            strategy_name=(strategy_name or settings.STRATEGY_NAME),
            entry_decision_id=(decision_id if side == "BUY" else None),
            exit_decision_id=(decision_id if side == "SELL" else None),
            exit_reason=(decision_reason if side == "SELL" else None),
            exit_rule_name=(exit_rule_name if side == "SELL" else None),
            note=note,
            pair=market,
            signal_ts=int(ts),
        )
        final_status = "PARTIAL" if execution_result.fill_status == "partial" else "FILLED"
        set_status(client_order_id, final_status, conn=conn)
        update_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            order_status=final_status,
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
