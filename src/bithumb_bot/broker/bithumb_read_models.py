from __future__ import annotations

from ..execution_models import OrderConfirmation, SignedOrderRequest, SubmissionRecord, SubmitPlan
from .base import BrokerRejectError


def parse_order_confirmation(
    broker,
    *,
    plan: SubmitPlan,
    signed_request: SignedOrderRequest,
    submission_record: SubmissionRecord,
    response_data: dict[str, object],
    now: int,
) -> OrderConfirmation:
    response_row = response_data.get("data") if isinstance(response_data.get("data"), dict) else response_data
    resolved_client_order_id, resolved_exchange_order_id = broker._resolve_order_identifiers(
        response_row if isinstance(response_row, dict) else {},
        fallback_client_order_id=plan.intent.client_order_id,
        allow_coid_alias=True,
        context="/v2/orders submit response",
    )
    if not resolved_exchange_order_id:
        raise BrokerRejectError(f"missing order id from /v2/orders response: {response_data}")
    if resolved_client_order_id and resolved_client_order_id != plan.intent.client_order_id:
        raise BrokerRejectError(
            "order submit response client_order_id mismatch: "
            f"requested={plan.intent.client_order_id} response={resolved_client_order_id}"
        )
    raw = broker._raw_v2_order_fields(
        response_row if isinstance(response_row, dict) else {},
        fallback_client_order_id=plan.intent.client_order_id,
    )
    raw.setdefault("market", signed_request.payload.get("market"))
    raw.setdefault("order_type", signed_request.payload.get("order_type"))
    raw.setdefault("ord_type", signed_request.payload.get("order_type"))
    submission_trace_id = getattr(submission_record, "trace_id", None)
    submission_plan_id = getattr(submission_record, "plan_id", None)
    submission_request_id = getattr(submission_record, "request_id", None)
    submission_id = getattr(submission_record, "submission_id", None)
    return OrderConfirmation(
        submission=submission_record,
        client_order_id=plan.intent.client_order_id,
        exchange_order_id=resolved_exchange_order_id,
        side=plan.intent.side,
        status="NEW",
        price=plan.intent.price,
        qty=float(signed_request.internal_lot_qty),
        filled_qty=0.0,
        created_ts=now,
        updated_ts=now,
        raw=raw,
        submit_contract_context=dict(signed_request.submit_contract_context),
        trace_id=submission_trace_id or signed_request.trace_id or plan.trace_id,
        plan_id=submission_plan_id or signed_request.plan_id or plan.plan_id,
        request_id=submission_request_id or signed_request.request_id,
        submission_id=submission_id,
        confirmation_id=f"{submission_trace_id or signed_request.trace_id or plan.trace_id or plan.intent.client_order_id}:confirmation",
        phase_identity="confirmation",
        phase_result="confirmed",
    )
