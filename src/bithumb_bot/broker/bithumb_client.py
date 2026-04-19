from __future__ import annotations

from ..execution_models import SignedOrderRequest
from .base import BrokerRejectError


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
    return broker._submit_validated_order_payload(
        payload_plan=signed_request,
        retry_safe=retry_safe,
    )
