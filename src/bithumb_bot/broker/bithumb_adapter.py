from __future__ import annotations

from ..execution_models import SignedOrderRequest, SubmitPlan
from .order_submit import build_place_order_payload, build_place_order_submission_flow, PlaceOrderSubmissionFlow


def build_submission_flow(
    broker,
    *,
    validated_client_order_id: str,
    side: str,
    qty: float,
    price: float | None,
    buy_price_none_submit_contract,
    now: int,
) -> PlaceOrderSubmissionFlow:
    return build_place_order_submission_flow(
        broker,
        validated_client_order_id=validated_client_order_id,
        side=side,
        qty=qty,
        price=price,
        buy_price_none_submit_contract=buy_price_none_submit_contract,
        now=now,
    )


def build_signed_order_request(
    broker,
    *,
    plan: SubmitPlan,
) -> SignedOrderRequest:
    return build_place_order_payload(broker, plan=plan)
