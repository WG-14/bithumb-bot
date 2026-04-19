from __future__ import annotations

from ..execution_models import SignedOrderRequest
from .base import BrokerRejectError

ORDER_SUBMIT_ENDPOINT = "/v2/orders"


def _has_compat_submit_override(broker) -> bool:
    compat_submit = getattr(broker, "_submit_validated_order_payload", None)
    default_submit = getattr(type(broker), "_submit_validated_order_payload", None)
    if not callable(compat_submit) or not callable(default_submit):
        return False
    compat_func = getattr(compat_submit, "__func__", compat_submit)
    default_func = getattr(default_submit, "__func__", default_submit)
    return compat_func is not default_func


def submit_validated_order_payload(
    broker,
    *,
    signed_request: SignedOrderRequest,
    retry_safe: bool = False,
) -> dict | list:
    compat_post_private = getattr(broker, "_post_private")
    default_post_private = getattr(type(broker), "_post_private")
    compat_func = getattr(compat_post_private, "__func__", compat_post_private)
    default_func = getattr(default_post_private, "__func__", default_post_private)
    if compat_func is not default_func:
        return compat_post_private(
            ORDER_SUBMIT_ENDPOINT,
            signed_request.payload,
            retry_safe=retry_safe,
        )
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
    if _has_compat_submit_override(broker):
        return broker._submit_validated_order_payload(
            payload_plan=signed_request,
            retry_safe=retry_safe,
        )
    return submit_validated_order_payload(
        broker,
        signed_request=signed_request,
        retry_safe=retry_safe,
    )
