from __future__ import annotations

from ..execution_models import OrderConfirmation, SignedOrderRequest, SubmissionRecord, SubmitPlan
from .base import BrokerRejectError
from .bithumb_client import submit_signed_order_request
from .bithumb_read_models import parse_order_confirmation


def execute_signed_order_request(
    broker,
    *,
    plan: SubmitPlan,
    signed_request: SignedOrderRequest,
    now: int,
    retry_safe: bool = False,
) -> OrderConfirmation:
    submission_record = SubmissionRecord(
        intent=plan.intent,
        plan=plan,
        signed_request=signed_request,
        request_ts=now,
        retry_safe=retry_safe,
        trace_id=signed_request.trace_id or plan.trace_id,
        plan_id=signed_request.plan_id or plan.plan_id,
        request_id=signed_request.request_id,
        submission_id=f"{signed_request.trace_id or plan.trace_id or plan.intent.client_order_id}:submission",
        phase_identity="submission",
        phase_result="submitted",
    )
    data = submit_signed_order_request(
        broker,
        signed_request=signed_request,
        retry_safe=retry_safe,
    )
    if not isinstance(data, dict):
        raise BrokerRejectError(f"unexpected /v2/orders payload type: {type(data).__name__}")
    return parse_order_confirmation(
        broker,
        plan=plan,
        signed_request=signed_request,
        submission_record=submission_record,
        response_data=data,
        now=now,
    )
