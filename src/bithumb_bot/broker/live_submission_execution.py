from __future__ import annotations

import json
import time
from dataclasses import dataclass

from ..config import settings
from ..db_core import record_broker_fill_observation
from ..execution import LiveFillFeeValidationError, apply_fill_and_trade
from ..fee_observation import fee_accounting_status
from ..fill_reading import FillReadPolicy, get_broker_fills
from ..notifier import format_event, notify
from ..observability import format_log_kv, safety_event
from ..oms import (
    TERMINAL_ORDER_STATUSES,
    build_client_order_id,
    build_order_intent_key,
    claim_order_intent_dedup,
    evaluate_unresolved_order_gate,
    payload_fingerprint,
    record_submit_attempt,
    record_submit_blocked,
    set_status,
    update_order_intent_dedup,
)
from ..reason_codes import (
    RISKY_ORDER_BLOCK,
    SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
    SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
)
from ..risk import evaluate_order_submission_halt
from .live_submit_planning import build_live_submit_plan
from .live_submit_orchestrator import (
    StandardSubmitPlanningFailureRequest,
    StandardSubmitPipelineRequest,
    _runtime_identity_fields,
    record_standard_submit_planning_failure,
    run_standard_submit_pipeline,
)


@dataclass(frozen=True)
class ConfirmedLiveSubmission:
    conn: object
    request: StandardSubmitPipelineRequest
    order: object
    client_order_id: str
    exchange_order_id: str
    side: str
    intent_key: str
    ts: int
    strategy_name: str | None
    decision_id: int | None
    decision_reason: str | None
    exit_rule_name: str | None


def _emit_notification(message: str) -> None:
    from . import live as live_module

    live_module.notify(message)


def submit_live_order_and_confirm(
    *,
    broker,
    request: StandardSubmitPipelineRequest,
    intent_key: str,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
) -> ConfirmedLiveSubmission | None:
    order = run_standard_submit_pipeline(broker=broker, request=request)
    if order is None:
        return None
    return ConfirmedLiveSubmission(
        conn=request.conn,
        request=request,
        order=order,
        client_order_id=request.client_order_id,
        exchange_order_id=str(order.exchange_order_id),
        side=request.side,
        intent_key=intent_key,
        ts=request.ts,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
    )


def _record_application_phase(
    *,
    submission: ConfirmedLiveSubmission,
    order_status: str,
    execution_state: str,
    submission_reason_code: str,
    broker_response_summary: str,
    error: Exception | None = None,
) -> None:
    request = submission.request
    client_order_id = submission.client_order_id
    submit_evidence = json.dumps(
        {
            "symbol": settings.PAIR,
            "side": request.side,
            "order_qty": request.order_qty,
            "intended_qty": request.qty,
            "normalized_qty": request.qty,
            **request.submit_observability_fields,
            **request.submit_truth_source_fields,
            "reference_price": request.reference_price,
            "top_of_book": request.top_of_book_summary,
            "request_ts": None,
            "response_ts": None,
            "submit_path": "live_standard_market",
            "contract_profile": request.contract_profile,
            "submit_phase": "application",
            "execution_state": execution_state,
            "submit_mode": settings.MODE,
            **_runtime_identity_fields(),
            "execution_trace_id": client_order_id,
            "submit_plan_id": f"{client_order_id}:plan",
            "signed_request_id": f"{client_order_id}:signed_request",
            "submission_id": f"{client_order_id}:submission",
            "confirmation_id": f"{client_order_id}:confirmation",
            "application_id": f"{client_order_id}:application",
            "exchange_order_id": submission.exchange_order_id,
            "error_class": type(error).__name__ if error is not None else None,
            "error_summary": str(error) if error is not None else None,
            "order_type": request.order_type,
            "internal_lot_size": request.internal_lot_size,
            "effective_min_trade_qty": request.effective_min_trade_qty,
            "qty_step": request.qty_step,
            "min_notional_krw": request.min_notional_krw,
            "intended_lot_count": request.intended_lot_count,
            "executable_lot_count": request.executable_lot_count,
            "final_intended_qty": request.final_intended_qty,
            "final_submitted_qty": request.final_submitted_qty,
            "decision_reason_code": request.decision_reason_code,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    record_submit_attempt(
        conn=submission.conn,
        client_order_id=client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=settings.PAIR,
        side=request.side,
        qty=request.qty,
        price=request.reference_price,
        submit_ts=request.ts,
        payload_fingerprint=request.payload_hash,
        broker_response_summary=broker_response_summary,
        submission_reason_code=submission_reason_code,
        exception_class=(type(error).__name__ if error is not None else None),
        timeout_flag=False,
        submit_phase="application",
        submit_plan_id=f"{client_order_id}:plan",
        signed_request_id=f"{client_order_id}:signed_request",
        submission_id=f"{client_order_id}:submission",
        confirmation_id=f"{client_order_id}:confirmation",
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=bool(submission.exchange_order_id),
        order_status=order_status,
        event_type="submit_attempt_application",
        order_type=request.order_type,
        internal_lot_size=request.internal_lot_size,
        effective_min_trade_qty=request.effective_min_trade_qty,
        qty_step=request.qty_step,
        min_notional_krw=request.min_notional_krw,
        intended_lot_count=request.intended_lot_count,
        executable_lot_count=request.executable_lot_count,
        final_intended_qty=request.final_intended_qty,
        final_submitted_qty=request.final_submitted_qty,
        decision_reason_code=request.decision_reason_code,
    )


def _fill_accounting_status(fill) -> str:
    return fee_accounting_status(
        fee=getattr(fill, "fee", None),
        fee_status=getattr(fill, "fee_status", "complete"),
        price=getattr(fill, "price", None),
        qty=getattr(fill, "qty", None),
        material_notional_threshold=float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW),
        fee_source=getattr(fill, "fee_source", None),
        fee_confidence=getattr(fill, "fee_confidence", None),
        provenance=getattr(fill, "fee_provenance", None),
        reason=getattr(fill, "fee_validation_reason", None),
        checks=getattr(fill, "fee_validation_checks", None),
    )


def _record_application_fill_observations(
    *,
    conn,
    client_order_id: str,
    exchange_order_id: str | None,
    side: str,
    fills: list,
    source: str,
) -> dict[str, int | str]:
    event_ts = int(time.time() * 1000)
    latest_fee_status = "none"
    fee_pending_count = 0
    observation_count = 0
    for fill in fills:
        accounting_status = _fill_accounting_status(fill)
        if accounting_status == "fee_pending":
            fee_pending_count += 1
            latest_fee_status = str(getattr(fill, "fee_status", "unknown") or "unknown")
        existing_observation = conn.execute(
            """
            SELECT id
            FROM broker_fill_observations
            WHERE client_order_id=?
              AND COALESCE(exchange_order_id, '')=COALESCE(?, '')
              AND COALESCE(fill_id, '')=COALESCE(?, '')
              AND fill_ts=?
              AND ABS(price-?) < 1e-12
              AND ABS(qty-?) < 1e-12
              AND accounting_status=?
              AND fee_status=?
              AND source=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                client_order_id,
                getattr(fill, "exchange_order_id", None) or exchange_order_id,
                getattr(fill, "fill_id", None),
                int(getattr(fill, "fill_ts", 0) or 0),
                float(getattr(fill, "price", 0.0) or 0.0),
                float(getattr(fill, "qty", 0.0) or 0.0),
                accounting_status,
                str(getattr(fill, "fee_status", "unknown") or "unknown"),
                source,
            ),
        ).fetchone()
        if existing_observation is not None:
            continue
        record_broker_fill_observation(
            conn,
            event_ts=event_ts,
            client_order_id=client_order_id,
            exchange_order_id=(getattr(fill, "exchange_order_id", None) or exchange_order_id),
            fill_id=getattr(fill, "fill_id", None),
            fill_ts=int(getattr(fill, "fill_ts", 0) or 0),
            side=side,
            price=float(getattr(fill, "price", 0.0) or 0.0),
            qty=float(getattr(fill, "qty", 0.0) or 0.0),
            fee=getattr(fill, "fee", None),
            fee_status=str(getattr(fill, "fee_status", "unknown") or "unknown"),
            accounting_status=accounting_status,
            source=source,
            fee_source=str(getattr(fill, "fee_source", "unknown") or "unknown"),
            fee_confidence=str(getattr(fill, "fee_confidence", "unknown") or "unknown"),
            fee_provenance=str(getattr(fill, "fee_provenance", "") or "") or None,
            fee_validation_reason=str(getattr(fill, "fee_validation_reason", "") or "") or None,
            fee_validation_checks=getattr(fill, "fee_validation_checks", None),
            parse_warnings=getattr(fill, "parse_warnings", ()),
            raw_payload=getattr(fill, "raw", None),
        )
        observation_count += 1
    return {
        "observation_count": observation_count,
        "fee_pending_count": fee_pending_count,
        "latest_fee_status": latest_fee_status,
    }


def reconcile_apply_fills_and_refresh(
    live_module,
    *,
    broker,
    submission: ConfirmedLiveSubmission,
):
    conn = submission.conn
    request = submission.request
    order = submission.order
    client_order_id = submission.client_order_id
    exchange_order_id = submission.exchange_order_id
    side = submission.side

    fills = get_broker_fills(
        broker,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        policy=FillReadPolicy.OBSERVATION_SALVAGE,
    )
    fee_pending_fills = [fill for fill in fills if _fill_accounting_status(fill) == "fee_pending"]
    if fee_pending_fills:
        observation_summary = _record_application_fill_observations(
            conn=conn,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            side=side,
            fills=fills,
            source="live_application_fee_pending",
        )
        from_status = str(order.status or "NEW")
        reason = (
            "live application deferred ledger apply: broker fill observed but accounting is fee-pending; "
            f"exchange_order_id={exchange_order_id or live_module.UNSET_EVENT_FIELD}; "
            f"fill_id={fee_pending_fills[0].fill_id}; "
            f"fee_status={observation_summary['latest_fee_status']}; "
            "automatic reconcile retry will continue until fee evidence is complete"
        )
        live_module._mark_accounting_pending(
            conn=conn,
            client_order_id=client_order_id,
            side=side,
            from_status=from_status,
            reason=reason,
        )
        update_order_intent_dedup(
            conn,
            intent_key=submission.intent_key,
            client_order_id=client_order_id,
            order_status="ACCOUNTING_PENDING",
        )
        exc = LiveFillFeeValidationError(reason)
        _record_application_phase(
            submission=submission,
            order_status="ACCOUNTING_PENDING",
            execution_state="application_deferred_fee_pending",
            submission_reason_code="application_deferred_fee_pending",
            broker_response_summary=(
                "application_exception=LiveFillFeeValidationError;"
                f"error={reason};observed_fill_count={observation_summary['observation_count']};"
                f"fee_pending_fill_count={observation_summary['fee_pending_count']}"
            ),
            error=exc,
        )
        conn.commit()
        live_module.RUN_LOG.error(
            format_log_kv(
                "[FILL_OBSERVATION] fee-pending broker fill observed before accounting apply",
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id or live_module.UNSET_EVENT_FIELD,
                side=side,
                observed_fill_count=observation_summary["observation_count"],
                fee_pending_fill_count=observation_summary["fee_pending_count"],
                latest_fee_status=observation_summary["latest_fee_status"],
                order_status="ACCOUNTING_PENDING",
            )
        )
        return None

    try:
        fills_to_apply = live_module._aggregate_fills_for_apply(
            fills=fills,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            side=side,
            context="_submit_via_standard_path",
        )
    except (live_module.FillFeeStrictModeError, LiveFillFeeValidationError) as exc:
        from_status = str(order.status or "NEW")
        live_module._mark_recovery_required(
            conn=conn,
            client_order_id=client_order_id,
            side=side,
            from_status=from_status,
            reason=str(exc),
        )
        update_order_intent_dedup(
            conn,
            intent_key=submission.intent_key,
            client_order_id=client_order_id,
            order_status="RECOVERY_REQUIRED",
        )
        _record_application_phase(
            submission=submission,
            order_status="RECOVERY_REQUIRED",
            execution_state="application_failed",
            submission_reason_code="application_failed",
            broker_response_summary=(
                f"application_exception={type(exc).__name__};error={exc}"
            ),
            error=exc,
        )
        conn.commit()
        live_module.RUN_LOG.error(
            format_log_kv(
                "[FILL_AGG] strict mode blocked aggregate; transitioned to recovery required",
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id or live_module.UNSET_EVENT_FIELD,
                side=side,
                from_status=from_status,
                reason=str(exc),
            )
        )
        return None

    try:
        trade = None
        for fill in fills_to_apply:
            trade = apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee,
                strategy_name=(submission.strategy_name or settings.STRATEGY_NAME),
                entry_decision_id=(submission.decision_id if side == "BUY" else None),
                exit_decision_id=(submission.decision_id if side == "SELL" else None),
                exit_reason=(submission.decision_reason if side == "SELL" else None),
                exit_rule_name=(submission.exit_rule_name if side == "SELL" else None),
                note=f"live exchange_order_id={exchange_order_id}",
                signal_ts=int(submission.ts),
            ) or trade

        refreshed = broker.get_order(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
        set_status(client_order_id, refreshed.status, conn=conn)
        update_order_intent_dedup(
            conn,
            intent_key=submission.intent_key,
            client_order_id=client_order_id,
            order_status=refreshed.status,
        )
        _record_application_phase(
            submission=submission,
            order_status=str(refreshed.status),
            execution_state="application_completed",
            submission_reason_code="application_completed",
            broker_response_summary=f"application_status={refreshed.status}",
        )
        conn.commit()
        return trade
    except Exception as exc:
        _record_application_phase(
            submission=submission,
            order_status=str(order.status or "NEW"),
            execution_state="application_failed",
            submission_reason_code="application_failed",
            broker_response_summary=f"application_exception={type(exc).__name__};error={exc}",
            error=exc,
        )
        conn.commit()
        raise


def execute_live_submission_and_application(
    live_module,
    *,
    broker,
    signal: str,
    ts: int,
    market_price: float,
    position_state,
    submission_ready,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
):
    intent = submission_ready.intent
    feasibility = submission_ready.feasibility
    conn = position_state.conn
    decision_observability = position_state.decision_observability
    submit_attempt_id = live_module._submit_attempt_id()
    client_order_id = live_module._client_order_id(ts=ts, side=feasibility.side, submit_attempt_id=submit_attempt_id)
    strategy_context = live_module._order_intent_strategy_context()
    intent_type = live_module._order_intent_type(side=feasibility.side)
    lot_sizing = feasibility.entry_sizing if feasibility.side == "BUY" else feasibility.exit_sizing
    intent_key = build_order_intent_key(
        symbol=settings.PAIR,
        side=feasibility.side,
        strategy_context=strategy_context,
        intent_ts=int(ts),
        intent_type=intent_type,
        qty=feasibility.normalized_qty,
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
    )

    reference_price: float | None = None
    top_of_book_summary: dict[str, float | str] | None = None
    if feasibility.reference_quote is not None:
        reference_price = float(feasibility.reference_quote["reference_price"])
        top_of_book_summary = {
            "bid": float(feasibility.reference_quote["bid"]),
            "ask": float(feasibility.reference_quote["ask"]),
            "spread": float(feasibility.reference_quote["ask"]) - float(feasibility.reference_quote["bid"]),
            "reference_ts": live_module._format_epoch_ts(float(feasibility.reference_quote["reference_ts_epoch_sec"])),
            "reference_source": str(feasibility.reference_quote["reference_source"]),
        }
    else:
        try:
            reference_quote = live_module._load_live_reference_quote(pair=settings.PAIR)
            reference_price = float(reference_quote["reference_price"])
            top_of_book_summary = {
                "bid": float(reference_quote["bid"]),
                "ask": float(reference_quote["ask"]),
                "spread": float(reference_quote["ask"]) - float(reference_quote["bid"]),
                "reference_ts": live_module._format_epoch_ts(float(reference_quote["reference_ts_epoch_sec"])),
                "reference_source": str(reference_quote["reference_source"]),
            }
        except ValueError as exc:
            reference_price = None
            top_of_book_summary = {"error": str(exc).removeprefix("reference price unavailable: ")}

    blocked, reason = evaluate_order_submission_halt(
        conn,
        ts_ms=int(ts),
        now_ms=int(time.time() * 1000),
        cash=float(position_state.cash),
        qty=float(position_state.portfolio_qty),
        price=float(market_price),
        broker=broker,
        mark_price_source="live_market_reference",
        evaluation_origin="submission_halt",
    )
    if blocked:
        gate_blocked, reason_code, gate_reason = evaluate_unresolved_order_gate(
            conn,
            now_ms=int(time.time() * 1000),
            max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
        )
        if gate_blocked:
            blocked_client_order_id = build_client_order_id(
                mode="live",
                side=feasibility.side,
                intent_ts=int(ts),
                submit_attempt_id=submit_attempt_id,
            )
            gate_reason = (
                f"category=unresolved_risk_gate;"
                f"reason_detail_code={reason_code};"
                f"reason={gate_reason}"
            )
            live_module.RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] unresolved risk gate",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side=feasibility.side,
                    reason=gate_reason,
                    sell_failure_category=SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
                    sell_failure_detail=SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
                    reason_detail_code=reason_code,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                    open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                    dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                    submit_qty_source=decision_observability["submit_qty_source"],
                    position_state_source=decision_observability["position_state_source"],
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            live_module._block_new_submission_for_unresolved_risk(
                conn=conn,
                client_order_id=blocked_client_order_id,
                side=feasibility.side,
                qty=feasibility.normalized_qty,
                ts=ts,
                reason_code=reason_code,
                reason=gate_reason,
            )
            conn.commit()
            return None

        live_module.RUN_LOG.info(
            format_log_kv(
                "[ORDER_SKIP] submission halt",
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                signal=signal,
                side=feasibility.side,
                reason=f"category=submission_halt;reason_detail_code=submission_halt;reason={reason}",
                sell_failure_category=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                sell_failure_detail=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                reason_detail_code="submission_halt",
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                submit_qty_source=decision_observability["submit_qty_source"],
                position_state_source=decision_observability["position_state_source"],
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            )
        )
        _emit_notification(
            f"live order placement blocked ({feasibility.side}): category=submission_halt;reason={reason}"
        )
        return None

    existing = conn.execute(
        "SELECT status FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if existing is not None:
        existing_status = str(existing["status"])
        if existing_status in TERMINAL_ORDER_STATUSES:
            reason = f"duplicate submit blocked: terminal status {existing_status}"
            live_module.RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] duplicate client order id",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side=feasibility.side,
                    reason=reason,
                    client_order_id=client_order_id,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                    open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                    dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                    submit_qty_source=decision_observability["submit_qty_source"],
                    position_state_source=decision_observability["position_state_source"],
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            record_submit_blocked(client_order_id, status=existing_status, reason=reason, conn=conn)
            _emit_notification(
                safety_event(
                    "order_submit_blocked",
                    client_order_id=client_order_id,
                    submit_attempt_id=submit_attempt_id,
                    side=feasibility.side,
                    status=existing_status,
                    reason_code=RISKY_ORDER_BLOCK,
                    signal_ts=int(ts),
                    decision_ts=int(ts),
                    decision_id=str(submit_attempt_id),
                    reason=reason,
                )
            )
            conn.commit()
            return None

    claimed, existing_intent = claim_order_intent_dedup(
        conn,
        intent_key=intent_key,
        client_order_id=client_order_id,
        symbol=settings.PAIR,
        side=feasibility.side,
        strategy_context=strategy_context,
        intent_type=intent_type,
        intent_ts=int(ts),
        qty=feasibility.normalized_qty,
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
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
        skip_reason = (
            f"duplicate intent already recorded "
            f"existing_client_order_id={existing_client_order_id} existing_status={existing_status}"
        )
        live_module.RUN_LOG.info(
            format_log_kv(
                "[ORDER_SKIP] duplicate order intent",
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                mode=settings.MODE,
                symbol=settings.PAIR,
                side=feasibility.side,
                qty=f"{float(feasibility.normalized_qty):.12f}",
                intent_ts=int(ts),
                intent_key=intent_key,
                reason=skip_reason,
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            )
        )
        _emit_notification(
            format_event(
                "order_intent_dedup_skip",
                symbol=settings.PAIR,
                side=feasibility.side,
                qty=float(feasibility.normalized_qty),
                intent_ts=int(ts),
                client_order_id=client_order_id,
                dedup_key=intent_key,
                skip_reason=skip_reason,
                existing_client_order_id=existing_client_order_id,
                existing_status=existing_status,
            )
        )
        conn.commit()
        return None

    live_module.RUN_LOG.info(
        format_log_kv(
            "[ORDER_DECISION] prepare submit planning",
            mode=settings.MODE,
            symbol=settings.PAIR,
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            signal=signal,
            side=feasibility.side,
            market_price=market_price,
            position_qty=float(intent.order_qty),
            order_qty=feasibility.order_qty,
            normalized_qty=feasibility.normalized_qty,
            submit_payload_qty_preview=float(feasibility.normalized_qty),
            reference_price=reference_price,
            client_order_id=client_order_id,
            intent_ts=int(ts),
            intent_key=intent_key,
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
            normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
            open_exposure_qty=float(decision_observability["open_exposure_qty"]),
            dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
            submit_qty_source=feasibility.submit_qty_source,
            position_state_source=str(decision_observability["position_state_source"]),
            decision_reason_code=str(lot_sizing.decision_reason_code),
            budget_krw=float(getattr(lot_sizing, "budget_krw", 0.0)),
            requested_qty=float(lot_sizing.requested_qty),
            exchange_constrained_qty=float(getattr(lot_sizing, "exchange_constrained_qty", lot_sizing.executable_qty)),
            lifecycle_executable_qty=float(getattr(lot_sizing, "lifecycle_executable_qty", lot_sizing.executable_qty)),
            rejected_qty_remainder=float(getattr(lot_sizing, "rejected_qty_remainder", 0.0)),
            unused_budget_krw=float(getattr(lot_sizing, "unused_budget_krw", 0.0)),
            internal_lot_size=float(lot_sizing.internal_lot_size),
            effective_min_trade_qty=float(lot_sizing.effective_min_trade_qty),
            min_qty=float(lot_sizing.min_qty),
            qty_step=float(lot_sizing.qty_step),
            min_notional_krw=float(lot_sizing.min_notional_krw),
            intended_lot_count=int(lot_sizing.intended_lot_count),
            executable_lot_count=int(lot_sizing.executable_lot_count),
            internal_lot_is_exchange_inflated=1 if bool(getattr(lot_sizing, "internal_lot_is_exchange_inflated", False)) else 0,
            internal_lot_would_block_buy=1 if bool(getattr(lot_sizing, "internal_lot_would_block_buy", False)) else 0,
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            top_of_book=top_of_book_summary,
        )
    )

    sell_truth_source_fields = live_module._sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=feasibility.submit_qty_source,
    )
    submit_truth_source_fields: dict[str, object] = dict(sell_truth_source_fields)
    submit_observability_fields: dict[str, object]
    sell_observability: dict[str, object]
    if feasibility.side == "SELL":
        canonical_sell_submit = live_module._CanonicalSellSubmitObservability(
            submit_qty_source=feasibility.submit_qty_source,
            submit_lot_source=str(
                decision_observability.get("sell_submit_lot_source")
                or decision_observability.get("submit_lot_source")
                or live_module._CANONICAL_SELL_SUBMIT_LOT_SOURCE
            ),
            submit_lot_count=int(decision_observability.get("sell_submit_lot_count") or 0),
            normalized_qty=feasibility.normalized_qty,
            position_state_source=str(decision_observability["position_state_source"]),
            position_state_source_truth_source=sell_truth_source_fields["position_state_source_truth_source"],
            submit_qty_source_truth_source=sell_truth_source_fields["submit_qty_source_truth_source"],
            submit_lot_source_truth_source=sell_truth_source_fields["submit_lot_source_truth_source"],
        )
        observed_sell_telemetry = live_module._ObservedSellSubmitTelemetry(
            position_qty=float(intent.order_qty),
            submit_payload_qty=feasibility.normalized_qty,
            raw_total_asset_qty=position_state.raw_total_asset_qty,
            open_exposure_qty=float(position_state.open_exposure_qty),
            dust_tracking_qty=float(position_state.dust_tracking_qty),
            sell_qty_basis_qty=float(decision_observability.get("sell_qty_basis_qty") or position_state.open_exposure_qty),
            sell_qty_basis_source=str(feasibility.submit_qty_source or decision_observability.get("sell_qty_basis_source") or "-"),
            sell_qty_basis_qty_truth_source=sell_truth_source_fields["sell_qty_basis_qty_truth_source"],
            sell_qty_basis_source_truth_source=sell_truth_source_fields["sell_qty_basis_source_truth_source"],
            sell_qty_boundary_kind=str(decision_observability.get("sell_qty_boundary_kind") or "none"),
            sell_qty_boundary_kind_truth_source=sell_truth_source_fields["sell_qty_boundary_kind_truth_source"],
            sell_normalized_exposure_qty_truth_source=sell_truth_source_fields["sell_normalized_exposure_qty_truth_source"],
            sell_open_exposure_qty_truth_source=sell_truth_source_fields["sell_open_exposure_qty_truth_source"],
            sell_dust_tracking_qty_truth_source=sell_truth_source_fields["sell_dust_tracking_qty_truth_source"],
        )
        sell_observability = live_module._sell_submit_observability_fields(
            decision_observability=decision_observability,
            canonical_submit=canonical_sell_submit,
            observed_inputs=observed_sell_telemetry,
            sell_failure_category="none",
            sell_failure_detail="none",
        )
        submit_observability_fields = dict(sell_observability)
    else:
        submit_truth_source_fields = {
            key: sell_truth_source_fields[key]
            for key in (
                "entry_allowed_truth_source",
                "effective_flat_truth_source",
                "raw_qty_open_truth_source",
                "raw_total_asset_qty_truth_source",
                "position_qty_truth_source",
                "submit_payload_qty_truth_source",
                "normalized_exposure_active_truth_source",
                "normalized_exposure_qty_truth_source",
                "open_exposure_qty_truth_source",
                "dust_tracking_qty_truth_source",
                "submit_qty_source_truth_source",
                "position_state_source_truth_source",
            )
        }
        submit_observability_fields = {
            "observed_position_qty": float(intent.order_qty),
            "requested_qty": float(getattr(lot_sizing, "requested_qty", feasibility.order_qty)),
            "exchange_constrained_qty": float(getattr(lot_sizing, "exchange_constrained_qty", feasibility.normalized_qty)),
            "lifecycle_executable_qty": float(getattr(lot_sizing, "lifecycle_executable_qty", feasibility.normalized_qty)),
            "submitted_qty": None,
            "rejected_qty_remainder": float(getattr(lot_sizing, "rejected_qty_remainder", 0.0)),
            "unused_budget_krw": float(getattr(lot_sizing, "unused_budget_krw", 0.0)),
            "submit_payload_qty": 0.0,
            "submit_qty_source": "submit_plan.pending",
            "position_state_source": str(decision_observability["position_state_source"]),
            "raw_total_asset_qty": float(position_state.raw_total_asset_qty),
            "open_exposure_qty": float(position_state.open_exposure_qty),
            "dust_tracking_qty": float(position_state.dust_tracking_qty),
        }
        sell_observability = {}

    planning_payload_hash = payload_fingerprint(
        {
            "client_order_id": client_order_id,
            "submit_attempt_id": submit_attempt_id,
            "symbol": settings.PAIR,
            "side": feasibility.side,
            "qty": float(feasibility.normalized_qty),
            "price": reference_price,
            "submit_ts": int(ts),
        }
    )
    planning_failure_fields = dict(
        conn=conn,
        signal=signal,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        side=feasibility.side,
        order_qty=float(feasibility.order_qty),
        position_qty=float(intent.order_qty),
        qty=float(feasibility.normalized_qty),
        ts=int(ts),
        intent_key=intent_key,
        market_price=float(market_price),
        raw_total_asset_qty=float(position_state.raw_total_asset_qty),
        open_exposure_qty=float(position_state.open_exposure_qty),
        dust_tracking_qty=float(position_state.dust_tracking_qty),
        submit_qty_source=feasibility.submit_qty_source,
        position_state_source=str(decision_observability["position_state_source"]),
        reference_price=reference_price,
        top_of_book_summary=top_of_book_summary,
        strategy_name=(strategy_name or settings.STRATEGY_NAME),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        contract_profile=str(settings.LIVE_SUBMIT_CONTRACT_PROFILE),
        payload_hash=planning_payload_hash,
        internal_lot_size=float(lot_sizing.internal_lot_size),
        effective_min_trade_qty=float(lot_sizing.effective_min_trade_qty),
        qty_step=float(lot_sizing.qty_step),
        min_notional_krw=float(lot_sizing.min_notional_krw),
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
        final_intended_qty=float(feasibility.order_qty),
        final_submitted_qty=0.0,
        decision_reason_code=str(lot_sizing.decision_reason_code),
        submit_truth_source_fields=submit_truth_source_fields,
        submit_observability_fields=submit_observability_fields,
        sell_observability=sell_observability,
    )
    try:
        submit_plan = build_live_submit_plan(
            broker=broker,
            client_order_id=client_order_id,
            side=feasibility.side,
            qty=float(feasibility.normalized_qty),
            ts=int(ts),
            effective_rules=position_state.effective_rules,
            reference_price=reference_price,
        )
    except Exception as error:
        record_standard_submit_planning_failure(
            request=StandardSubmitPlanningFailureRequest(
                order_type=("price" if feasibility.side == "BUY" else "market"),
                **planning_failure_fields,
            ),
            error=error,
        )
        return None
    if feasibility.side == "BUY":
        submit_truth_source_fields["submit_qty_source_truth_source"] = "submit_plan.submit_qty_authority"
        submit_observability_fields = dict(submit_observability_fields)
        submit_observability_fields.update(
            {
                "exchange_constrained_qty": float(submit_plan.exchange_constrained_qty),
                "lifecycle_executable_qty": float(submit_plan.lifecycle_executable_qty),
                "submitted_qty": float(submit_plan.submitted_qty),
                "submit_payload_qty": float(submit_plan.submitted_qty),
                "submit_qty_source": str(submit_plan.submit_qty_authority),
                "rejected_qty_remainder": float(submit_plan.rejected_qty_remainder),
                "unused_budget_krw": float(submit_plan.unused_budget_krw),
                "lifecycle_non_executable_reason": str(submit_plan.lifecycle_non_executable_reason or "none"),
            }
        )
    request_payload_hash = payload_fingerprint(
        {
            "client_order_id": client_order_id,
            "submit_attempt_id": submit_attempt_id,
            "symbol": settings.PAIR,
            "side": feasibility.side,
            "qty": float(submit_plan.submitted_qty),
            "price": reference_price,
            "submit_ts": int(ts),
        }
    )
    live_module.RUN_LOG.info(
        format_log_kv(
            "[ORDER_DECISION] submit plan ready",
            mode=settings.MODE,
            symbol=settings.PAIR,
            signal=signal,
            side=feasibility.side,
            client_order_id=client_order_id,
            requested_qty=float(submit_plan.requested_qty),
            exchange_constrained_qty=float(submit_plan.exchange_constrained_qty),
            lifecycle_executable_qty=float(submit_plan.lifecycle_executable_qty),
            submitted_qty=float(submit_plan.submitted_qty),
            submit_payload_qty=float(submit_plan.submitted_qty),
            rejected_qty_remainder=float(submit_plan.rejected_qty_remainder),
            unused_budget_krw=float(submit_plan.unused_budget_krw),
            submit_qty_authority=str(submit_plan.submit_qty_authority),
            lifecycle_non_executable_reason=str(submit_plan.lifecycle_non_executable_reason or "none"),
            exchange_order_type=str(submit_plan.exchange_order_type),
            exchange_submit_field=str(submit_plan.exchange_submit_field),
        )
    )
    submission = submit_live_order_and_confirm(
        broker=broker,
        request=StandardSubmitPipelineRequest(
            submit_plan=submit_plan,
            effective_rules=position_state.effective_rules,
            order_type=str(submit_plan.exchange_order_type),
            conn=conn,
            signal=signal,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=feasibility.side,
            order_qty=float(feasibility.order_qty),
            position_qty=float(intent.order_qty),
            qty=float(submit_plan.submitted_qty),
            ts=int(ts),
            intent_key=intent_key,
            market_price=float(market_price),
            raw_total_asset_qty=float(position_state.raw_total_asset_qty),
            open_exposure_qty=float(position_state.open_exposure_qty),
            dust_tracking_qty=float(position_state.dust_tracking_qty),
            submit_qty_source=(
                str(submit_plan.submit_qty_authority)
                if feasibility.side == "BUY"
                else feasibility.submit_qty_source
            ),
            position_state_source=str(decision_observability["position_state_source"]),
            reference_price=reference_price,
            top_of_book_summary=top_of_book_summary,
            strategy_name=(strategy_name or settings.STRATEGY_NAME),
            decision_id=decision_id,
            decision_reason=decision_reason,
            exit_rule_name=exit_rule_name,
            contract_profile=str(settings.LIVE_SUBMIT_CONTRACT_PROFILE),
            payload_hash=request_payload_hash,
            internal_lot_size=float(lot_sizing.internal_lot_size),
            effective_min_trade_qty=float(lot_sizing.effective_min_trade_qty),
            qty_step=float(lot_sizing.qty_step),
            min_notional_krw=float(lot_sizing.min_notional_krw),
            intended_lot_count=int(lot_sizing.intended_lot_count),
            executable_lot_count=int(lot_sizing.executable_lot_count),
            final_intended_qty=float(feasibility.order_qty),
            final_submitted_qty=float(submit_plan.submitted_qty),
            decision_reason_code=str(lot_sizing.decision_reason_code),
            submit_truth_source_fields=submit_truth_source_fields,
            submit_observability_fields=submit_observability_fields,
            sell_observability=sell_observability,
        ),
        intent_key=intent_key,
        strategy_name=(strategy_name or settings.STRATEGY_NAME),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
    )
    if submission is None:
        return None
    return reconcile_apply_fills_and_refresh(
        live_module,
        broker=broker,
        submission=submission,
    )
