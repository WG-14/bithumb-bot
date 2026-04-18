from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass

from ..config import settings
from ..execution import record_order_if_missing
from ..execution_models import OrderIntent, SubmitPlan
from ..notifier import notify
from ..observability import format_log_kv, safety_event
from ..oms import (
    record_status_transition,
    record_submit_attempt,
    record_submit_started,
    set_exchange_order_id,
    set_status,
    update_order_intent_dedup,
)
from ..reason_codes import (
    MANUAL_DUST_REVIEW_REQUIRED,
    SUBMIT_FAILED,
    SUBMIT_TIMEOUT,
    classify_sell_failure_category,
    sell_failure_detail_from_category,
)
from .base import Broker, BrokerOrder, BrokerRejectError, BrokerSubmissionUnknownError, BrokerTemporaryError
from .order_rules import BuyPriceNoneSubmitContract, serialize_buy_price_none_submit_contract
RUN_LOG = logging.getLogger("bithumb_bot.run")
UNSET_EVENT_FIELD = "-"
SUBMISSION_REASON_FAILED_BEFORE_SEND = "failed_before_send"
SUBMISSION_REASON_SENT_BUT_RESPONSE_TIMEOUT = "sent_but_response_timeout"
SUBMISSION_REASON_SENT_BUT_TRANSPORT_ERROR = "sent_but_transport_error"
SUBMISSION_REASON_AMBIGUOUS_RESPONSE = "ambiguous_response"
SUBMISSION_REASON_CONFIRMED_SUCCESS = "confirmed_success"
LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE = "live_explicit_submit_plan_v1"


@dataclass(frozen=True)
class StandardSubmitPipelineRequest:
    conn: object
    submit_plan: SubmitPlan | None
    signal: str
    client_order_id: str
    submit_attempt_id: str
    side: str
    order_qty: float
    position_qty: float
    qty: float
    ts: int
    intent_key: str
    market_price: float
    raw_total_asset_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    effective_rules: object
    submit_qty_source: str
    position_state_source: str
    reference_price: float | None
    top_of_book_summary: dict[str, float | str] | None
    strategy_name: str | None
    decision_id: int | None
    decision_reason: str | None
    exit_rule_name: str | None
    order_type: str
    contract_profile: str
    payload_hash: str
    internal_lot_size: float | None
    effective_min_trade_qty: float | None
    qty_step: float | None
    min_notional_krw: float | None
    intended_lot_count: int | None
    executable_lot_count: int | None
    final_intended_qty: float
    final_submitted_qty: float
    decision_reason_code: str | None
    submit_truth_source_fields: dict[str, object]
    submit_observability_fields: dict[str, object]
    sell_observability: dict[str, object]


@dataclass(frozen=True)
class _StandardSubmitAttemptContext:
    request: StandardSubmitPipelineRequest
    submit_plan: SubmitPlan
    submit_contract_context: dict[str, object] | None
    symbol: str
    submit_path: str
    phase_trace_fields: dict[str, str]
    lot_evidence_fields: dict[str, object]
    base_submit_contract_fields: dict[str, object]
    base_submit_failure_fields: dict[str, object]


def _encode_submit_evidence(*, payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _classify_temporary_submit_error(exc: Exception) -> tuple[str, bool]:
    detail = str(exc).lower()
    if "timeout" in detail or "timed out" in detail:
        return SUBMISSION_REASON_SENT_BUT_RESPONSE_TIMEOUT, True
    return SUBMISSION_REASON_SENT_BUT_TRANSPORT_ERROR, False


def _submit_phase_trace_fields(*, client_order_id: str) -> dict[str, str]:
    trace_id = client_order_id
    return {
        "execution_trace_id": trace_id,
        "submit_plan_id": f"{trace_id}:plan",
        "signed_request_id": f"{trace_id}:signed_request",
        "submission_id": f"{trace_id}:submission",
        "confirmation_id": f"{trace_id}:confirmation",
    }


def _submit_contract_fields(
    *,
    side: str,
    order_type: str | None,
    normalized_qty: float,
    contract_context: dict[str, object] | BuyPriceNoneSubmitContract | None = None,
) -> dict[str, object]:
    context = (
        serialize_buy_price_none_submit_contract(
            contract_context,
            market=settings.PAIR,
            order_side=str(side or "").strip().upper() or None,
        )
        if isinstance(contract_context, BuyPriceNoneSubmitContract)
        else (contract_context or {})
    )
    normalized_side = str(side or "").strip().upper()
    normalized_order_type = str(order_type or "").strip().lower()
    is_buy_market_notional = normalized_side == "BUY" and normalized_order_type == "price"
    exchange_submit_field = str(context.get("exchange_submit_field") or ("price" if is_buy_market_notional else "volume"))
    exchange_submit_qty = context.get("exchange_submit_qty")
    if exchange_submit_qty is None and exchange_submit_field == "volume":
        exchange_submit_qty = float(normalized_qty)
    chance_supported_order_types = context.get("chance_supported_order_types")
    if isinstance(chance_supported_order_types, (tuple, list)):
        chance_supported_order_types = [str(item) for item in chance_supported_order_types]
    elif chance_supported_order_types is not None:
        chance_supported_order_types = [str(chance_supported_order_types)]
    buy_price_none_raw_supported_types = context.get("buy_price_none_raw_supported_types")
    if isinstance(buy_price_none_raw_supported_types, (tuple, list)):
        buy_price_none_raw_supported_types = [str(item) for item in buy_price_none_raw_supported_types]
    elif buy_price_none_raw_supported_types is not None:
        buy_price_none_raw_supported_types = [str(buy_price_none_raw_supported_types)]
    return {
        "submit_contract_kind": (
            "market_buy_notional"
            if is_buy_market_notional
            else ("market_qty" if normalized_order_type == "market" else "limit_qty_price")
        ),
        "market": context.get("market"),
        "order_side": context.get("order_side"),
        "exchange_order_type": str(context.get("exchange_order_type") or normalized_order_type or "-"),
        "chance_validation_order_type": str(context.get("chance_validation_order_type") or normalized_order_type or "-"),
        "chance_supported_order_types": chance_supported_order_types,
        "exchange_submit_field": exchange_submit_field,
        "exchange_submit_qty": None if exchange_submit_qty is None else float(exchange_submit_qty),
        "exchange_submit_notional_krw": (
            None if context.get("exchange_submit_notional_krw") is None else float(context["exchange_submit_notional_krw"])
        ),
        "buy_price_none_allowed": context.get("buy_price_none_allowed"),
        "buy_price_none_decision_outcome": context.get("buy_price_none_decision_outcome"),
        "buy_price_none_decision_basis": context.get("buy_price_none_decision_basis"),
        "buy_price_none_alias_used": context.get("buy_price_none_alias_used"),
        "buy_price_none_alias_policy": context.get("buy_price_none_alias_policy"),
        "buy_price_none_block_reason": context.get("buy_price_none_block_reason"),
        "buy_price_none_support_source": context.get("buy_price_none_support_source"),
        "buy_price_none_raw_supported_types": buy_price_none_raw_supported_types,
        "buy_price_none_resolved_order_type": context.get("buy_price_none_resolved_order_type"),
        "allowed": context.get("allowed"),
        "decision_outcome": context.get("decision_outcome"),
        "decision_basis": context.get("decision_basis"),
        "alias_used": context.get("alias_used"),
        "alias_policy": context.get("alias_policy"),
        "block_reason": context.get("block_reason"),
        "support_source": context.get("support_source"),
        "raw_buy_supported_types": (
            [str(item) for item in context.get("raw_buy_supported_types", ())]
            if isinstance(context.get("raw_buy_supported_types"), (tuple, list))
            else context.get("raw_buy_supported_types")
        ),
        "resolved_order_type": context.get("resolved_order_type"),
        "resolved_contract": context.get("resolved_contract"),
        "contract_id": context.get("contract_id"),
        "submit_field": context.get("submit_field") or exchange_submit_field,
        "internal_executable_qty": float(
            context.get("internal_executable_qty")
            if context.get("internal_executable_qty") is not None
            else normalized_qty
        ),
    }


def _submit_failure_fields(
    *,
    side: str,
    order_type: str | None,
    error_class: str | None,
    error_summary: str | None,
) -> dict[str, str]:
    if not str(error_class or "").strip() and not str(error_summary or "").strip():
        return {"submit_failure_category": "none", "submit_failure_detail": "none"}
    normalized_side = str(side or "").strip().upper()
    normalized_order_type = str(order_type or "").strip().lower()
    detail = str(error_summary or "")
    detail_lower = detail.lower()
    if normalized_side == "BUY" and normalized_order_type == "price":
        if "/v1/orders/chance rejected order type" in detail_lower:
            category = "chance_order_type_mismatch"
        elif "under_min_total" in detail_lower or "order notional below side minimum" in detail_lower:
            category = "notional_rule_reject"
        else:
            category = "broker_reject"
        return {
            "submit_failure_category": category,
            "submit_failure_detail": ("buy_market_notional_contract" if category == "broker_reject" else category),
        }
    sell_failure_category = classify_sell_failure_category(error_class=error_class, error_summary=error_summary)
    return {
        "submit_failure_category": sell_failure_category,
        "submit_failure_detail": sell_failure_detail_from_category(sell_failure_category=sell_failure_category),
    }


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
    reference_price: float | None,
    order_status: str,
    broker_response_summary: str,
    submission_reason_code: str,
    exception_class: str | None,
    timeout_flag: bool,
    submit_evidence: str | None,
    exchange_order_id_obtained: bool,
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
) -> None:
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=reference_price,
        submit_ts=ts,
        payload_fingerprint=payload_hash,
        broker_response_summary=broker_response_summary,
        submission_reason_code=submission_reason_code,
        exception_class=exception_class,
        timeout_flag=timeout_flag,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=exchange_order_id_obtained,
        order_status=order_status,
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
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
    reference_price: float | None,
    submit_evidence: str | None,
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
) -> None:
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=reference_price,
        submit_ts=ts,
        payload_fingerprint=payload_hash,
        broker_response_summary="submit_dispatched",
        submission_reason_code="submit_dispatched_preflight",
        exception_class=None,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=False,
        order_status="PENDING_SUBMIT",
        event_type="submit_attempt_preflight",
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
    )


def _record_submit_attempt_signed(
    *,
    conn,
    client_order_id: str,
    submit_attempt_id: str,
    symbol: str,
    side: str,
    qty: float,
    ts: int,
    payload_hash: str,
    submit_evidence: str | None,
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
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
        broker_response_summary="signed_request_prepared",
        submission_reason_code="signed_request_prepared",
        exception_class=None,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=False,
        order_status="PENDING_SUBMIT",
        event_type="submit_attempt_signed",
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
    )


def _mark_submit_unknown(*, conn, client_order_id: str, submit_attempt_id: str, side: str, reason: str, ts: int) -> None:
    record_status_transition(client_order_id, from_status="PENDING_SUBMIT", to_status="SUBMIT_UNKNOWN", reason=reason, conn=conn)
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
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(submit_attempt_id),
            side=side,
            status="SUBMIT_UNKNOWN",
            reason=reason,
        )
    )


def _mark_submit_failed(*, conn, client_order_id: str, submit_attempt_id: str, side: str, reason: str, ts: int) -> None:
    record_status_transition(client_order_id, from_status="PENDING_SUBMIT", to_status="FAILED", reason=reason, conn=conn)
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
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(submit_attempt_id),
            side=side,
            status="FAILED",
            reason=reason,
        )
    )


def _submission_error_observability_fields(
    *,
    request: StandardSubmitPipelineRequest,
    error_class: str,
    error_summary: str,
    include_operator_actions: bool,
) -> dict[str, object]:
    fields: dict[str, object] = {
        **request.submit_observability_fields,
        **request.submit_truth_source_fields,
    }
    if request.side != "SELL":
        return fields
    if include_operator_actions:
        fields.update(
            {
                "operator_action": (
                    str(request.sell_observability.get("operator_action") or "")
                    if str(request.sell_observability.get("operator_action") or "").strip() not in {"", "-"}
                    else MANUAL_DUST_REVIEW_REQUIRED
                ),
                "dust_action": (
                    str(request.sell_observability.get("dust_action") or "")
                    if str(request.sell_observability.get("dust_action") or "").strip() not in {"", "-"}
                    else MANUAL_DUST_REVIEW_REQUIRED
                ),
            }
        )
    sell_failure_category = classify_sell_failure_category(error_class=error_class, error_summary=error_summary)
    fields.update(
        {
            "sell_failure_category": sell_failure_category,
            "sell_failure_detail": sell_failure_detail_from_category(sell_failure_category=sell_failure_category),
        }
    )
    return fields


def _planning_failure(
    *,
    request: StandardSubmitPipelineRequest,
    error: Exception,
    phase_trace_fields: dict[str, str],
) -> None:
    failure_submit_contract_fields = _submit_contract_fields(
        side=request.side,
        order_type=request.order_type,
        normalized_qty=request.qty,
        contract_context=getattr(error, "submit_contract_context", None),
    )
    failure_submit_fields = _submit_failure_fields(
        side=request.side,
        order_type=request.order_type,
        error_class=type(error).__name__,
        error_summary=str(error),
    )
    submit_evidence = _encode_submit_evidence(
        payload={
            "symbol": settings.PAIR,
            "side": request.side,
            "intended_qty": request.qty,
            "normalized_qty": request.qty,
            "reference_price": request.reference_price,
            "top_of_book": request.top_of_book_summary,
            "request_ts": None,
            "response_ts": None,
            "submit_path": "live_standard_market",
            "contract_profile": LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
            "submit_phase": "planning",
            "execution_state": "planning_failed",
            "submit_mode": settings.MODE,
            **phase_trace_fields,
            **failure_submit_contract_fields,
            **failure_submit_fields,
            "error_class": type(error).__name__,
            "error_summary": str(error),
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
        }
    )
    reason = f"submit planning failed: {type(error).__name__}: {error}"
    record_order_if_missing(
        request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        side=request.side,
        qty_req=request.qty,
        price=None,
        strategy_name=request.strategy_name,
        entry_decision_id=(request.decision_id if request.side == "BUY" else None),
        exit_decision_id=(request.decision_id if request.side == "SELL" else None),
        decision_reason=request.decision_reason,
        exit_rule_name=request.exit_rule_name,
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
        local_intent_state="PLAN_REJECTED",
        ts_ms=request.ts,
        status="FAILED",
    )
    request.conn.execute(
        "UPDATE orders SET last_error=?, updated_ts=? WHERE client_order_id=?",
        (reason[:500], int(time.time() * 1000), request.client_order_id),
    )
    _record_submit_attempt_result(
        conn=request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=settings.PAIR,
        side=request.side,
        qty=request.qty,
        ts=request.ts,
        payload_hash=request.payload_hash,
        reference_price=request.reference_price,
        order_status="FAILED",
        broker_response_summary=f"planning_exception={type(error).__name__};error={error}",
        submission_reason_code=SUBMISSION_REASON_FAILED_BEFORE_SEND,
        exception_class=type(error).__name__,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=False,
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
    update_order_intent_dedup(
        request.conn,
        intent_key=request.intent_key,
        client_order_id=request.client_order_id,
        order_status="FAILED",
        last_error=reason,
    )
    RUN_LOG.info(format_log_kv("[ORDER_SKIP] submit planning failed", signal=request.signal, side=request.side, client_order_id=request.client_order_id, reason=reason))
    request.conn.commit()


def record_standard_submit_planning_failure(
    *,
    request: StandardSubmitPipelineRequest,
    error: Exception,
) -> None:
    _planning_failure(
        request=request,
        error=error,
        phase_trace_fields=_submit_phase_trace_fields(client_order_id=request.client_order_id),
    )


def _validate_explicit_submit_plan(*, request: StandardSubmitPipelineRequest) -> SubmitPlan:
    submit_plan = request.submit_plan
    if submit_plan is None:
        raise BrokerRejectError("live submit requires explicit submit_plan before dispatch")
    if request.contract_profile != LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE:
        raise BrokerRejectError(
            "live submit contract profile invalid before dispatch: "
            f"contract_profile={request.contract_profile!r}"
        )
    if str(submit_plan.phase_identity or "") != "planning":
        raise BrokerRejectError(
            "live submit_plan phase identity invalid before dispatch: "
            f"phase_identity={submit_plan.phase_identity!r}"
        )
    if str(submit_plan.phase_result or "") != "planned":
        raise BrokerRejectError(
            "live submit_plan phase result invalid before dispatch: "
            f"phase_result={submit_plan.phase_result!r}"
        )
    if not str(submit_plan.plan_id or "").strip():
        raise BrokerRejectError("live submit_plan plan_id missing before dispatch")
    if str(submit_plan.intent.market).strip() != str(settings.PAIR).strip():
        raise BrokerRejectError(
            "live submit_plan market mismatch before dispatch: "
            f"request={settings.PAIR} planned={submit_plan.intent.market}"
        )
    if request.client_order_id != submit_plan.intent.client_order_id:
        raise BrokerRejectError(
            "live submit_plan client_order_id mismatch before dispatch: "
            f"request={request.client_order_id} planned={submit_plan.intent.client_order_id}"
        )
    if str(request.side).strip().upper() != str(submit_plan.intent.side).strip().upper():
        raise BrokerRejectError(
            "live submit_plan side mismatch before dispatch: "
            f"request={request.side} planned={submit_plan.intent.side}"
        )
    if abs(float(request.qty) - float(submit_plan.intent.qty)) > 1e-12:
        raise BrokerRejectError(
            "live submit_plan qty mismatch before dispatch: "
            f"request={float(request.qty):.12f} planned={float(submit_plan.intent.qty):.12f}"
        )
    if submit_plan.intent.price is not None:
        raise BrokerRejectError(
            "live submit_plan price mismatch before dispatch: "
            f"request=None planned={submit_plan.intent.price}"
        )
    return submit_plan


def _dispatch_kwargs_from_submit_plan(*, submit_plan: SubmitPlan) -> dict[str, object]:
    return {
        "client_order_id": submit_plan.intent.client_order_id,
        "side": submit_plan.intent.side,
        "qty": float(submit_plan.intent.qty),
        "price": submit_plan.intent.price,
        "submit_plan": submit_plan,
    }


def _build_context(*, request: StandardSubmitPipelineRequest, submit_plan: SubmitPlan) -> _StandardSubmitAttemptContext:
    phase_trace_fields = _submit_phase_trace_fields(client_order_id=request.client_order_id)
    return _StandardSubmitAttemptContext(
        request=request,
        submit_plan=submit_plan,
        submit_contract_context=submit_plan.submit_contract_context,
        symbol=settings.PAIR,
        submit_path="live_standard_market",
        phase_trace_fields=phase_trace_fields,
        lot_evidence_fields={
            "order_type": request.order_type,
            "internal_lot_size": None if request.internal_lot_size is None else float(request.internal_lot_size),
            "effective_min_trade_qty": None if request.effective_min_trade_qty is None else float(request.effective_min_trade_qty),
            "qty_step": None if request.qty_step is None else float(request.qty_step),
            "min_notional_krw": None if request.min_notional_krw is None else float(request.min_notional_krw),
            "intended_lot_count": None if request.intended_lot_count is None else int(request.intended_lot_count),
            "executable_lot_count": None if request.executable_lot_count is None else int(request.executable_lot_count),
            "final_intended_qty": float(request.final_intended_qty),
            "final_submitted_qty": float(request.final_submitted_qty),
            "decision_reason_code": request.decision_reason_code,
        },
        base_submit_contract_fields=_submit_contract_fields(
            side=request.side,
            order_type=request.order_type,
            normalized_qty=request.qty,
            contract_context=submit_plan.submit_contract_context,
        ),
        base_submit_failure_fields=_submit_failure_fields(
            side=request.side,
            order_type=request.order_type,
            error_class=None,
            error_summary=None,
        ),
    )


def _plan_submit_attempt(*, context: _StandardSubmitAttemptContext) -> None:
    request = context.request
    preflight_evidence = _encode_submit_evidence(
        payload={
            "symbol": context.symbol,
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
            "submit_path": context.submit_path,
            "contract_profile": request.contract_profile,
            "submit_phase": "planning",
            "execution_state": "validated_pre_submit",
            "submit_mode": settings.MODE,
            **context.phase_trace_fields,
            **context.base_submit_contract_fields,
            **context.base_submit_failure_fields,
            "error_class": None,
            "error_summary": None,
            **context.lot_evidence_fields,
        }
    )
    record_order_if_missing(
        request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        side=request.side,
        qty_req=request.qty,
        price=None,
        strategy_name=request.strategy_name,
        entry_decision_id=(request.decision_id if request.side == "BUY" else None),
        exit_decision_id=(request.decision_id if request.side == "SELL" else None),
        decision_reason=request.decision_reason,
        exit_rule_name=request.exit_rule_name,
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
        local_intent_state="PENDING_SUBMIT",
        ts_ms=request.ts,
        status="PENDING_SUBMIT",
    )
    record_submit_started(
        request.client_order_id,
        conn=request.conn,
        submit_attempt_id=request.submit_attempt_id,
        symbol=context.symbol,
        side=request.side,
        qty=request.qty,
        mode=settings.MODE,
    )
    _record_submit_attempt_preflight(
        conn=request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=context.symbol,
        side=request.side,
        qty=request.qty,
        ts=request.ts,
        payload_hash=request.payload_hash,
        reference_price=request.reference_price,
        submit_evidence=preflight_evidence,
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
    signed_evidence = _encode_submit_evidence(
        payload={
            "symbol": context.symbol,
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
            "submit_path": context.submit_path,
            "contract_profile": request.contract_profile,
            "submit_phase": "signed_request",
            "execution_state": "signed_request_prepared",
            "submit_mode": settings.MODE,
            "plan_phase_identity": context.submit_plan.phase_identity,
            "plan_phase_result": context.submit_plan.phase_result,
            "signed_request_phase_identity": "signed_request",
            "signed_request_phase_result": "prepared",
            **context.phase_trace_fields,
            **context.base_submit_contract_fields,
            **context.base_submit_failure_fields,
            "error_class": None,
            "error_summary": None,
            **context.lot_evidence_fields,
        }
    )
    _record_submit_attempt_signed(
        conn=request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=context.symbol,
        side=request.side,
        qty=request.qty,
        ts=request.ts,
        payload_hash=request.payload_hash,
        submit_evidence=signed_evidence,
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
    notify(
        safety_event(
            "order_submit_started",
            client_order_id=request.client_order_id,
            submit_attempt_id=request.submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_to="PENDING_SUBMIT",
            reason_code=UNSET_EVENT_FIELD,
            signal_ts=request.ts,
            decision_ts=request.ts,
            decision_id=str(request.submit_attempt_id),
            side=request.side,
            status="PENDING_SUBMIT",
        )
    )
    request.conn.commit()


def _dispatch_submit_attempt(*, context: _StandardSubmitAttemptContext, broker: Broker) -> tuple[BrokerOrder, int, int] | None:
    request = context.request
    dispatch_kwargs = _dispatch_kwargs_from_submit_plan(submit_plan=context.submit_plan)
    try:
        request_ts = int(time.time() * 1000)
        RUN_LOG.info(
            format_log_kv(
                "[ORDER_DECISION] broker.place_order dispatch",
                signal=request.signal,
                signal_ts=request.ts,
                candle_ts=request.ts,
                side=request.side,
                market_price=request.market_price,
                position_qty=request.position_qty,
                order_qty=request.order_qty,
                normalized_qty=request.qty,
                submit_payload_qty=request.qty,
                submit_qty=request.qty,
                submit_qty_source=request.submit_qty_source,
                position_state_source=request.position_state_source,
                raw_total_asset_qty=request.raw_total_asset_qty,
                open_exposure_qty=request.open_exposure_qty,
                dust_tracking_qty=request.dust_tracking_qty,
                reference_price=request.reference_price,
                client_order_id=request.client_order_id,
                **context.phase_trace_fields,
                internal_lot_size=request.internal_lot_size,
                intended_lot_count=request.intended_lot_count,
                executable_lot_count=request.executable_lot_count,
                final_intended_qty=request.final_intended_qty,
                final_submitted_qty=request.final_submitted_qty,
                decision_reason_code=request.decision_reason_code,
                exchange_order_type=context.base_submit_contract_fields["exchange_order_type"],
                exchange_submit_field=context.base_submit_contract_fields["exchange_submit_field"],
                exchange_submit_notional_krw=context.base_submit_contract_fields["exchange_submit_notional_krw"] or "",
            )
        )
        order = broker.place_order(**dispatch_kwargs)
        response_ts = int(time.time() * 1000)
        return order, request_ts, response_ts
    except BrokerTemporaryError as error:
        return _handle_temporary_submit_error(context=context, error=error, request_ts=request_ts, response_ts=int(time.time() * 1000))
    except BrokerRejectError as error:
        return _handle_reject_submit_error(context=context, error=error, request_ts=request_ts, response_ts=int(time.time() * 1000))
    except Exception as error:
        return _handle_unexpected_submit_error(context=context, error=error, request_ts=request_ts, response_ts=int(time.time() * 1000))


def _handle_temporary_submit_error(*, context: _StandardSubmitAttemptContext, error: BrokerTemporaryError, request_ts: int, response_ts: int) -> None:
    request = context.request
    err = BrokerSubmissionUnknownError(f"submit unknown: {type(error).__name__}: {error}")
    submission_reason_code, timeout_flag = _classify_temporary_submit_error(error)
    failure_submit_contract_fields = _submit_contract_fields(
        side=request.side,
        order_type=request.order_type,
        normalized_qty=request.qty,
        contract_context=(getattr(error, "submit_contract_context", None) or context.submit_contract_context),
    )
    failure_submit_fields = _submit_failure_fields(side=request.side, order_type=request.order_type, error_class=type(error).__name__, error_summary=str(error))
    submit_evidence = _encode_submit_evidence(
        payload={
            "symbol": context.symbol,
            "side": request.side,
            "order_qty": request.order_qty,
            "intended_qty": request.qty,
            "normalized_qty": request.qty,
            **_submission_error_observability_fields(request=request, error_class=type(error).__name__, error_summary=str(error), include_operator_actions=True),
            "reference_price": request.reference_price,
            "top_of_book": request.top_of_book_summary,
            "request_ts": request_ts,
            "response_ts": response_ts,
            "submit_path": context.submit_path,
            "contract_profile": request.contract_profile,
            "submit_phase": "submission",
            "execution_state": "dispatch_attempted",
            "submit_mode": settings.MODE,
            **context.phase_trace_fields,
            **failure_submit_contract_fields,
            **failure_submit_fields,
            **context.lot_evidence_fields,
            "error_class": type(error).__name__,
            "error_summary": str(error),
        }
    )
    _mark_submit_unknown(conn=request.conn, client_order_id=request.client_order_id, submit_attempt_id=request.submit_attempt_id, side=request.side, reason=str(err), ts=request.ts)
    _record_submit_attempt_result(
        conn=request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=context.symbol,
        side=request.side,
        qty=request.qty,
        ts=request.ts,
        payload_hash=request.payload_hash,
        reference_price=request.reference_price,
        order_status="SUBMIT_UNKNOWN",
        broker_response_summary=f"submit_exception={type(error).__name__};error={error}",
        submission_reason_code=submission_reason_code,
        exception_class=type(error).__name__,
        timeout_flag=timeout_flag,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=False,
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
    update_order_intent_dedup(request.conn, intent_key=request.intent_key, client_order_id=request.client_order_id, order_status="SUBMIT_UNKNOWN", last_error=str(err))
    request.conn.commit()
    return None


def _handle_reject_submit_error(*, context: _StandardSubmitAttemptContext, error: BrokerRejectError, request_ts: int, response_ts: int) -> None:
    request = context.request
    reason = f"submit rejected: {type(error).__name__}: {error}"
    is_sell_qty_step_reject = request.side == "SELL" and "qty does not match qty_step" in str(error)
    failure_submit_contract_fields = _submit_contract_fields(
        side=request.side,
        order_type=request.order_type,
        normalized_qty=request.qty,
        contract_context=(getattr(error, "submit_contract_context", None) or context.submit_contract_context),
    )
    failure_submit_fields = _submit_failure_fields(side=request.side, order_type=request.order_type, error_class=type(error).__name__, error_summary=str(error))
    submit_evidence = _encode_submit_evidence(
        payload={
            "symbol": context.symbol,
            "side": request.side,
            "order_qty": request.order_qty,
            "intended_qty": request.qty,
            "normalized_qty": request.qty,
            **_submission_error_observability_fields(request=request, error_class=type(error).__name__, error_summary=str(error), include_operator_actions=True),
            "reference_price": request.reference_price,
            "top_of_book": request.top_of_book_summary,
            "request_ts": request_ts,
            "response_ts": response_ts,
            "submit_path": context.submit_path,
            "contract_profile": request.contract_profile,
            "submit_phase": "submission",
            "execution_state": "dispatch_attempted",
            "submit_mode": settings.MODE,
            **context.phase_trace_fields,
            **failure_submit_contract_fields,
            **failure_submit_fields,
            **context.lot_evidence_fields,
            "error_class": type(error).__name__,
            "error_summary": str(error),
        }
    )
    _mark_submit_failed(conn=request.conn, client_order_id=request.client_order_id, submit_attempt_id=request.submit_attempt_id, side=request.side, reason=reason, ts=request.ts)
    if not is_sell_qty_step_reject:
        _record_submit_attempt_result(
            conn=request.conn,
            client_order_id=request.client_order_id,
            submit_attempt_id=request.submit_attempt_id,
            symbol=context.symbol,
            side=request.side,
            qty=request.qty,
            ts=request.ts,
            payload_hash=request.payload_hash,
            reference_price=request.reference_price,
            order_status="FAILED",
            broker_response_summary=f"submit_reject={type(error).__name__};error={error}",
            submission_reason_code=SUBMIT_FAILED,
            exception_class=type(error).__name__,
            timeout_flag=False,
            submit_evidence=submit_evidence,
            exchange_order_id_obtained=False,
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
    update_order_intent_dedup(request.conn, intent_key=request.intent_key, client_order_id=request.client_order_id, order_status="FAILED", last_error=reason)
    request.conn.commit()
    return None


def _handle_unexpected_submit_error(*, context: _StandardSubmitAttemptContext, error: Exception, request_ts: int, response_ts: int) -> None:
    request = context.request
    reason = f"submit failed: {type(error).__name__}: {error}"
    failure_submit_contract_fields = _submit_contract_fields(
        side=request.side,
        order_type=request.order_type,
        normalized_qty=request.qty,
        contract_context=(getattr(error, "submit_contract_context", None) or context.submit_contract_context),
    )
    failure_submit_fields = _submit_failure_fields(side=request.side, order_type=request.order_type, error_class=type(error).__name__, error_summary=str(error))
    submit_evidence = _encode_submit_evidence(
        payload={
            "symbol": context.symbol,
            "side": request.side,
            "order_qty": request.order_qty,
            "intended_qty": request.qty,
            "normalized_qty": request.qty,
            **_submission_error_observability_fields(request=request, error_class=type(error).__name__, error_summary=str(error), include_operator_actions=False),
            "reference_price": request.reference_price,
            "top_of_book": request.top_of_book_summary,
            "request_ts": request_ts,
            "response_ts": response_ts,
            "submit_path": context.submit_path,
            "contract_profile": request.contract_profile,
            "submit_phase": "submission",
            "execution_state": "dispatch_attempted",
            "submit_mode": settings.MODE,
            **context.phase_trace_fields,
            **failure_submit_contract_fields,
            **failure_submit_fields,
            **context.lot_evidence_fields,
            "error_class": type(error).__name__,
            "error_summary": str(error),
        }
    )
    _mark_submit_failed(conn=request.conn, client_order_id=request.client_order_id, submit_attempt_id=request.submit_attempt_id, side=request.side, reason=reason, ts=request.ts)
    _record_submit_attempt_result(
        conn=request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=context.symbol,
        side=request.side,
        qty=request.qty,
        ts=request.ts,
        payload_hash=request.payload_hash,
        reference_price=request.reference_price,
        order_status="FAILED",
        broker_response_summary=f"submit_exception={type(error).__name__};error={error}",
        submission_reason_code=SUBMISSION_REASON_FAILED_BEFORE_SEND,
        exception_class=type(error).__name__,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=False,
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
    update_order_intent_dedup(request.conn, intent_key=request.intent_key, client_order_id=request.client_order_id, order_status="FAILED", last_error=reason)
    request.conn.commit()
    return None


def _confirm_submit_attempt(*, context: _StandardSubmitAttemptContext, order: BrokerOrder, request_ts: int, response_ts: int) -> BrokerOrder | None:
    request = context.request
    if order.exchange_order_id:
        set_exchange_order_id(request.client_order_id, order.exchange_order_id, conn=request.conn)
        notify(
            safety_event(
                "exchange_order_id_attached",
                client_order_id=request.client_order_id,
                submit_attempt_id=request.submit_attempt_id,
                exchange_order_id=order.exchange_order_id,
                reason_code=UNSET_EVENT_FIELD,
                signal_ts=request.ts,
                decision_ts=request.ts,
                decision_id=str(request.submit_attempt_id),
                side=request.side,
                status=order.status,
            )
        )
    if not order.exchange_order_id:
        return _confirm_submit_missing_exchange_id(context=context, order=order, request_ts=request_ts, response_ts=response_ts)
    set_status(request.client_order_id, order.status, conn=request.conn)
    success_submit_contract_fields = _submit_contract_fields(
        side=request.side,
        order_type=request.order_type,
        normalized_qty=request.qty,
        contract_context=(getattr(order, "submit_contract_context", None) or context.submit_contract_context),
    )
    success_submit_failure_fields = _submit_failure_fields(side=request.side, order_type=request.order_type, error_class=None, error_summary=None)
    submit_evidence = _encode_submit_evidence(
        payload={
            "symbol": context.symbol,
            "side": request.side,
            "order_qty": request.order_qty,
            "intended_qty": request.qty,
            "normalized_qty": request.qty,
            **request.submit_observability_fields,
            **request.submit_truth_source_fields,
            "reference_price": request.reference_price,
            "top_of_book": request.top_of_book_summary,
            "request_ts": request_ts,
            "response_ts": response_ts,
            "submit_path": context.submit_path,
            "contract_profile": request.contract_profile,
            "submit_phase": "confirmation",
            "execution_state": "broker_response_received",
            "submit_mode": settings.MODE,
            **context.phase_trace_fields,
            **success_submit_contract_fields,
            **success_submit_failure_fields,
            **context.lot_evidence_fields,
            "error_class": None,
            "error_summary": None,
        }
    )
    _record_submit_attempt_result(
        conn=request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=context.symbol,
        side=request.side,
        qty=request.qty,
        ts=request.ts,
        payload_hash=request.payload_hash,
        reference_price=request.reference_price,
        order_status=order.status,
        broker_response_summary=f"broker_status={order.status};exchange_order_id={order.exchange_order_id}",
        submission_reason_code=SUBMISSION_REASON_CONFIRMED_SUCCESS,
        exception_class=None,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=True,
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
    update_order_intent_dedup(request.conn, intent_key=request.intent_key, client_order_id=request.client_order_id, order_status=order.status)
    request.conn.commit()
    return order


def _confirm_submit_missing_exchange_id(*, context: _StandardSubmitAttemptContext, order: BrokerOrder, request_ts: int, response_ts: int) -> None:
    request = context.request
    reason = "submit acknowledged without exchange_order_id; classification=SUBMIT_UNKNOWN"
    missing_id_submit_contract_fields = _submit_contract_fields(
        side=request.side,
        order_type=request.order_type,
        normalized_qty=request.qty,
        contract_context=(getattr(order, "submit_contract_context", None) or context.submit_contract_context),
    )
    missing_id_submit_failure_fields = _submit_failure_fields(side=request.side, order_type=request.order_type, error_class=None, error_summary="missing exchange_order_id")
    submit_evidence = _encode_submit_evidence(
        payload={
            "symbol": context.symbol,
            "side": request.side,
            "order_qty": request.order_qty,
            "intended_qty": request.qty,
            "normalized_qty": request.qty,
            "submit_qty_source": request.submit_qty_source,
            "position_state_source": request.position_state_source,
            **request.submit_truth_source_fields,
            "raw_total_asset_qty": request.raw_total_asset_qty,
            "open_exposure_qty": request.open_exposure_qty,
            "dust_tracking_qty": request.dust_tracking_qty,
            "reference_price": request.reference_price,
            "top_of_book": request.top_of_book_summary,
            "request_ts": request_ts,
            "response_ts": response_ts,
            "submit_path": context.submit_path,
            "contract_profile": request.contract_profile,
            "submit_phase": "confirmation",
            "execution_state": "broker_response_received",
            "submit_mode": settings.MODE,
            **context.phase_trace_fields,
            **missing_id_submit_contract_fields,
            **missing_id_submit_failure_fields,
            **context.lot_evidence_fields,
            "error_class": None,
            "error_summary": "missing exchange_order_id",
        }
    )
    _mark_submit_unknown(conn=request.conn, client_order_id=request.client_order_id, submit_attempt_id=request.submit_attempt_id, side=request.side, reason=reason, ts=request.ts)
    _record_submit_attempt_result(
        conn=request.conn,
        client_order_id=request.client_order_id,
        submit_attempt_id=request.submit_attempt_id,
        symbol=context.symbol,
        side=request.side,
        qty=request.qty,
        ts=request.ts,
        payload_hash=request.payload_hash,
        reference_price=request.reference_price,
        order_status="SUBMIT_UNKNOWN",
        broker_response_summary=f"broker_status={order.status};exchange_order_id=-",
        submission_reason_code=SUBMISSION_REASON_AMBIGUOUS_RESPONSE,
        exception_class=None,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=False,
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
    update_order_intent_dedup(request.conn, intent_key=request.intent_key, client_order_id=request.client_order_id, order_status="SUBMIT_UNKNOWN", last_error=reason)
    request.conn.commit()
    return None


def run_standard_submit_pipeline(*, broker: Broker, request: StandardSubmitPipelineRequest) -> BrokerOrder | None:
    try:
        submit_plan = _validate_explicit_submit_plan(request=request)
    except Exception as error:
        record_standard_submit_planning_failure(request=request, error=error)
        return None

    context = _build_context(request=request, submit_plan=submit_plan)
    _plan_submit_attempt(context=context)
    submission = _dispatch_submit_attempt(context=context, broker=broker)
    if submission is None:
        return None
    order, request_ts, response_ts = submission
    return _confirm_submit_attempt(context=context, order=order, request_ts=request_ts, response_ts=response_ts)
