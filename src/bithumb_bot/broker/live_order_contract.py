from __future__ import annotations

from .base import BrokerRejectError

ORDER_SUBMIT_ENDPOINT = "/v2/orders"
ORDER_SUBMIT_CONTENT_TYPE = "application/json; charset=utf-8"
ORDER_SUBMIT_DISPATCH_AUTHORITY = "validated_place_order_flow"


def require_validated_order_submit_authority(signed_request: object, *, context: str) -> None:
    authority = str(getattr(signed_request, "dispatch_authority", "") or "").strip()
    if authority != ORDER_SUBMIT_DISPATCH_AUTHORITY:
        raise BrokerRejectError(
            f"{context} requires validated place_order flow authority"
        )
