from __future__ import annotations

from .base import BrokerRejectError

ORDER_SUBMIT_ENDPOINT = "/v2/orders"
ORDER_SUBMIT_CONTENT_TYPE = "application/json; charset=utf-8"
ORDER_SUBMIT_DISPATCH_AUTHORITY = "validated_place_order_flow"
FORBIDDEN_ALTERNATE_ORDER_SUBMIT_ENDPOINTS = frozenset({"/v1/order", "/v1/orders"})


def require_validated_order_submit_authority(signed_request: object, *, context: str) -> None:
    authority = str(getattr(signed_request, "dispatch_authority", "") or "").strip()
    if authority != ORDER_SUBMIT_DISPATCH_AUTHORITY:
        raise BrokerRejectError(
            f"{context} requires validated place_order flow authority"
        )


def reject_forbidden_order_submit_route(method: str, endpoint: str, *, context: str) -> None:
    normalized_method = str(method or "").strip().upper()
    normalized_endpoint = str(endpoint or "").strip()
    if normalized_method == "POST" and normalized_endpoint in FORBIDDEN_ALTERNATE_ORDER_SUBMIT_ENDPOINTS:
        raise BrokerRejectError(
            f"{context}: alternate order submit route is disabled; "
            f"use canonical {ORDER_SUBMIT_ENDPOINT} validated place_order flow"
        )
