from __future__ import annotations

from ..execution_models import SignedOrderRequest, SubmitPlan
from .order_submit import build_place_order_payload, build_place_order_submission_flow, PlaceOrderSubmissionFlow


def build_submission_flow(
    broker,
    *,
    plan: SubmitPlan,
) -> PlaceOrderSubmissionFlow:
    return build_place_order_submission_flow(
        broker,
        plan=plan,
    )


def build_signed_order_request(
    broker,
    *,
    plan: SubmitPlan,
) -> SignedOrderRequest:
    return build_place_order_payload(broker, plan=plan)
