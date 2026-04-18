from __future__ import annotations

from ..execution_models import OrderIntent
from ..execution_models import SignedOrderRequest, SubmitPlan
from .order_payloads import normalize_order_side
from .order_submit import build_place_order_payload, build_place_order_submission_flow, plan_place_order, PlaceOrderSubmissionFlow


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
    normalized_side = normalize_order_side(side)
    plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id=validated_client_order_id,
            market=broker._market(),
            side=side,
            normalized_side=normalized_side,
            qty=float(qty),
            price=price,
            created_ts=now,
            submit_contract=buy_price_none_submit_contract,
            trace_id=validated_client_order_id,
        ),
    )
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
