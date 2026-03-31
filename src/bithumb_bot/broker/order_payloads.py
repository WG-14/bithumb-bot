from __future__ import annotations

from typing import Literal

from .base import BrokerRejectError

OrderSide = Literal["bid", "ask"]
OrderKind = Literal["limit", "price", "market"]


def normalize_order_side(side: str) -> OrderSide:
    token = str(side).strip().lower()
    if token in {"buy", "bid"}:
        return "bid"
    if token in {"sell", "ask"}:
        return "ask"
    raise BrokerRejectError(f"unsupported order side: {side}")


def build_order_payload(
    *,
    market: str,
    side: str,
    ord_type: str,
    volume: str | None = None,
    price: str | None = None,
    client_order_id: str | None = None,
) -> dict[str, str]:
    normalized_side = normalize_order_side(side)
    ord_type_token = str(ord_type).strip().lower()
    if ord_type_token not in {"limit", "price", "market"}:
        raise BrokerRejectError(f"unsupported ord_type: {ord_type}")

    payload: dict[str, str] = {
        "market": str(market),
        "side": normalized_side,
        "ord_type": ord_type_token,
    }
    if client_order_id is not None:
        normalized_client_order_id = str(client_order_id).strip()
        if not normalized_client_order_id:
            raise BrokerRejectError("client_order_id must be a non-empty string")
        payload["client_order_id"] = normalized_client_order_id
    if ord_type_token == "limit":
        if not volume or not price:
            raise BrokerRejectError("limit order requires both volume and price")
        payload["volume"] = str(volume)
        payload["price"] = str(price)
        return payload

    if ord_type_token == "price":
        if normalized_side != "bid":
            raise BrokerRejectError("ord_type=price is only valid for side=bid")
        if not price:
            raise BrokerRejectError("ord_type=price requires price")
        payload["price"] = str(price)
        return payload

    if normalized_side != "ask":
        raise BrokerRejectError("ord_type=market is only valid for side=ask")
    if not volume:
        raise BrokerRejectError("ord_type=market requires volume")
    payload["volume"] = str(volume)
    return payload
