from __future__ import annotations

from types import SimpleNamespace

from ..execution_models import SignedOrderRequest
from .live_order_contract import require_validated_order_submit_authority


def submit_validated_order_payload(
    broker,
    *,
    signed_request: SignedOrderRequest,
    retry_safe: bool = False,
) -> dict | list:
    submit_order_override = getattr(getattr(broker, "_private_api", None), "__dict__", {}).get("submit_order")
    if callable(submit_order_override):
        return broker._private_api.submit_order(
            signed_request=signed_request,
            retry_safe=retry_safe,
            response_excerpt=broker._response_body_excerpt,
        )
    payload_plan = SimpleNamespace(payload=dict(signed_request.payload))
    return broker._submit_validated_order_payload(
        payload_plan=payload_plan,
        retry_safe=retry_safe,
    )


def submit_signed_order_request(
    broker,
    *,
    signed_request: SignedOrderRequest,
    retry_safe: bool = False,
) -> dict | list:
    if getattr(broker, "dry_run", False) is False:
        require_validated_order_submit_authority(
            signed_request,
            context="armed live signed-request submission",
        )
    return submit_validated_order_payload(
        broker,
        signed_request=signed_request,
        retry_safe=retry_safe,
    )
