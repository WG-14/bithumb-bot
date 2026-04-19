from __future__ import annotations

from ..execution_models import SignedOrderRequest
from .base import BrokerRejectError

ORDER_SUBMIT_ENDPOINT = "/v2/orders"


def submit_validated_order_payload(
    broker,
    *,
    signed_request: SignedOrderRequest,
    retry_safe: bool = False,
) -> dict | list:
    return broker._private_api.request(
        "POST",
        ORDER_SUBMIT_ENDPOINT,
        json_body=signed_request.payload,
        retry_safe=retry_safe,
        response_excerpt=broker._response_body_excerpt,
    )


def submit_signed_order_request(
    broker,
    *,
    signed_request: SignedOrderRequest,
    retry_safe: bool = False,
) -> dict | list:
    if (
        getattr(broker, "dry_run", False) is False
        and str(getattr(signed_request, "dispatch_authority", "")).strip() != "validated_place_order_flow"
    ):
        raise BrokerRejectError(
            "armed live signed-request submission requires validated place_order flow authority"
        )
    return submit_validated_order_payload(
        broker,
        signed_request=signed_request,
        retry_safe=retry_safe,
    )
