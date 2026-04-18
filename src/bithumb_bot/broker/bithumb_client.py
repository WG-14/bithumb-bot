from __future__ import annotations

from ..execution_models import SignedOrderRequest


def submit_signed_order_request(
    broker,
    *,
    signed_request: SignedOrderRequest,
    retry_safe: bool = False,
) -> dict | list:
    return broker._submit_validated_order_payload(
        payload_plan=signed_request,
        retry_safe=retry_safe,
    )
