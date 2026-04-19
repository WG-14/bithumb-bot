from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal

from ..execution_models import SubmitPlan
from ..markets import ExchangeMarketCodeError, parse_documented_market_code
from .base import BrokerRejectError
from .order_serialization import format_krw_amount, format_volume

OrderSide = Literal["bid", "ask"]
OrderKind = Literal["limit", "price", "market"]
CLIENT_ORDER_ID_MAX_LENGTH = 36
CLIENT_ORDER_ID_ALLOWED_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_ORDER_SUBMIT_ALLOWED_FIELDS = frozenset(
    {
        "market",
        "side",
        "order_type",
        "price",
        "volume",
        "client_order_id",
    }
)


@dataclass(frozen=True)
class OrderPayloadFromPlan:
    payload: dict[str, str]
    submit_contract_context: dict[str, object]
    exchange_submit_field: str
    exchange_submit_notional_krw: float | None


def normalize_order_side(side: str) -> OrderSide:
    token = str(side).strip().lower()
    if token in {"buy", "bid"}:
        return "bid"
    if token in {"sell", "ask"}:
        return "ask"
    raise BrokerRejectError(f"unsupported order side: {side}")


def validate_client_order_id(client_order_id: str) -> str:
    if not isinstance(client_order_id, str):
        raise BrokerRejectError("client_order_id must be a string")
    if client_order_id == "" or client_order_id.strip() == "":
        raise BrokerRejectError("client_order_id must not be empty")
    if len(client_order_id) > CLIENT_ORDER_ID_MAX_LENGTH:
        raise BrokerRejectError(
            f"client_order_id must be at most {CLIENT_ORDER_ID_MAX_LENGTH} characters"
        )
    if not CLIENT_ORDER_ID_ALLOWED_PATTERN.fullmatch(client_order_id):
        raise BrokerRejectError(
            "client_order_id contains invalid characters; allowed: A-Z, a-z, 0-9, underscore(_), hyphen(-)"
        )
    return client_order_id


def normalize_order_type(order_type: str) -> OrderKind:
    token = str(order_type).strip().lower()
    if token in {"limit", "price", "market"}:
        return token
    raise BrokerRejectError(f"unsupported order_type: {order_type}")


def _positive_decimal(value: object, *, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise BrokerRejectError(f"{field_name} must be numeric")
    text = str(value).strip()
    if text == "":
        raise BrokerRejectError(f"{field_name} must not be empty")
    try:
        decimal_value = Decimal(text)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise BrokerRejectError(f"{field_name} must be numeric") from exc
    if not decimal_value.is_finite() or decimal_value <= 0:
        raise BrokerRejectError(f"{field_name} must be > 0")
    return decimal_value


def _decimal_to_plain_string(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def validate_order_submit_payload(payload: dict[str, object]) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise BrokerRejectError(f"/v2/orders payload must be object, got {type(payload).__name__}")
    if "ord_type" in payload:
        raise BrokerRejectError("/v2/orders payload must use documented key 'order_type'; 'ord_type' is not allowed")
    unknown_fields = sorted(str(key) for key in payload.keys() if str(key) not in _ORDER_SUBMIT_ALLOWED_FIELDS)
    if unknown_fields:
        raise BrokerRejectError(f"/v2/orders payload contains unsupported fields: {', '.join(unknown_fields)}")

    try:
        market = parse_documented_market_code(str(payload.get("market") or ""))
    except ExchangeMarketCodeError as exc:
        raise BrokerRejectError(f"/v2/orders payload market must use canonical QUOTE-BASE code: {payload.get('market')!r}") from exc
    side = normalize_order_side(str(payload.get("side") or ""))
    order_type = normalize_order_type(str(payload.get("order_type") or ""))
    client_order_id_value = payload.get("client_order_id")
    client_order_id = (
        validate_client_order_id(str(client_order_id_value))
        if client_order_id_value not in (None, "")
        else None
    )

    price_value = payload.get("price")
    volume_value = payload.get("volume")
    price_text: str | None = None
    volume_text: str | None = None
    if price_value not in (None, ""):
        price_text = _decimal_to_plain_string(_positive_decimal(price_value, field_name="price"))
    if volume_value not in (None, ""):
        volume_text = _decimal_to_plain_string(_positive_decimal(volume_value, field_name="volume"))

    if order_type == "limit":
        if price_text is None or volume_text is None:
            raise BrokerRejectError("limit order requires both price and volume")
    elif order_type == "price":
        if side != "bid":
            raise BrokerRejectError("order_type=price is only valid for side=bid")
        if price_text is None:
            raise BrokerRejectError("order_type=price requires price")
        if volume_text is not None:
            raise BrokerRejectError("order_type=price must not include volume")
    else:
        if side != "ask":
            raise BrokerRejectError("order_type=market is only valid for side=ask")
        if volume_text is None:
            raise BrokerRejectError("order_type=market requires volume")
        if price_text is not None:
            raise BrokerRejectError("order_type=market must not include price")

    normalized_payload: dict[str, str] = {
        "market": market,
        "side": side,
        "order_type": order_type,
    }
    if price_text is not None:
        normalized_payload["price"] = price_text
    if volume_text is not None:
        normalized_payload["volume"] = volume_text
    if client_order_id is not None:
        normalized_payload["client_order_id"] = client_order_id
    return normalized_payload


def build_order_payload(
    *,
    market: str,
    side: str,
    ord_type: str | None = None,
    order_type: str | None = None,
    volume: str | None = None,
    price: str | None = None,
    client_order_id: str | None = None,
) -> dict[str, str]:
    resolved_order_type = order_type if order_type is not None else ord_type
    if order_type is not None and ord_type is not None and str(order_type).strip().lower() != str(ord_type).strip().lower():
        raise BrokerRejectError(f"conflicting order_type aliases: order_type={order_type} ord_type={ord_type}")
    return validate_order_submit_payload(
        {
            "market": market,
            "side": side,
            "order_type": resolved_order_type,
            "price": price,
            "volume": volume,
            "client_order_id": client_order_id,
        }
    )


def build_order_payload_from_plan(
    *,
    plan: SubmitPlan,
) -> OrderPayloadFromPlan:
    from .order_rules import serialize_buy_price_none_submit_contract

    exchange_submit_field = str(plan.exchange_submit_field)
    exchange_submit_notional_krw = plan.exchange_submit_notional_krw
    price_text = (
        format_krw_amount(plan.exchange_submit_price)
        if plan.exchange_submit_price is not None
        else None
    )
    volume_text = (
        format_volume(plan.exchange_submit_volume)
        if plan.exchange_submit_volume is not None
        else None
    )
    payload = build_order_payload(
        market=plan.intent.market,
        side=plan.intent.normalized_side,
        ord_type=plan.exchange_order_type,
        volume=volume_text,
        price=price_text,
        client_order_id=plan.intent.client_order_id,
    )

    if plan.buy_price_none_submit_contract is not None:
        executed_submit_contract = plan.buy_price_none_submit_contract.with_execution_fields(
            exchange_submit_notional_krw=plan.exchange_submit_notional_krw,
            exchange_submit_qty=plan.exchange_submit_volume,
            internal_executable_qty=float(plan.internal_lot_qty),
        )
        submit_contract_context = serialize_buy_price_none_submit_contract(
            executed_submit_contract,
            market=plan.intent.market,
            order_side=plan.intent.order_side,
        )
        submit_contract_context["submit_contract_kind"] = "market_buy_notional"
    else:
        submit_contract_context = dict(plan.submit_contract_context)
        submit_contract_context.update(
            {
                "submit_contract_kind": str(plan.submit_contract_context.get("submit_contract_kind") or "limit_qty_price"),
                "exchange_submit_field": exchange_submit_field,
                "exchange_order_type": str(plan.exchange_order_type),
                "exchange_submit_notional_krw": plan.exchange_submit_notional_krw,
                "exchange_submit_qty": plan.exchange_submit_volume,
                "internal_executable_qty": float(plan.internal_lot_qty),
            }
        )

    return OrderPayloadFromPlan(
        payload=payload,
        submit_contract_context=submit_contract_context,
        exchange_submit_field=exchange_submit_field,
        exchange_submit_notional_krw=exchange_submit_notional_krw,
    )
