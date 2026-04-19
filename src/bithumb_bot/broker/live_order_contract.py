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


def require_order_submit_transmission_contract(
    *,
    canonical_payload: str,
    query_hash_claims: dict[str, str],
    expected_query_hash_claims: dict[str, str],
    request_body_text: str,
    request_content: bytes,
    request_kwargs: dict[str, object],
    headers: dict[str, str],
    expected_content_type: str = ORDER_SUBMIT_CONTENT_TYPE,
) -> None:
    """Fail closed if the signed /v2/orders payload and transmitted bytes drift."""
    if query_hash_claims != expected_query_hash_claims:
        raise BrokerRejectError("/v2/orders query_hash claims do not match canonical payload")
    if request_content != request_body_text.encode("utf-8"):
        raise BrokerRejectError("/v2/orders transmitted content does not match JSON body text")
    if request_kwargs != {"content": request_content}:
        raise BrokerRejectError("/v2/orders must transmit exact JSON bytes via content=, not json=")
    if str(headers.get("Content-Type") or "") != expected_content_type:
        raise BrokerRejectError("/v2/orders Content-Type contract drifted")
    if not canonical_payload:
        raise BrokerRejectError("/v2/orders canonical payload must not be empty")
