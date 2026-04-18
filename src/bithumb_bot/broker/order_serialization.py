from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN

from .base import BrokerRejectError


def decimal_from_value(value: object) -> Decimal:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise BrokerRejectError(f"invalid numeric value for order serialization: {value}") from exc
    if not decimal_value.is_finite():
        raise BrokerRejectError(f"invalid non-finite numeric value for order serialization: {value}")
    return decimal_value


def format_krw_amount(value: object) -> str:
    amount = decimal_from_value(value)
    if amount <= 0:
        return "0"
    rounded = amount.quantize(Decimal("1"), rounding=ROUND_DOWN)
    return format(rounded, "f").split(".", 1)[0]


def format_volume(qty: object, *, places: int = 8) -> str:
    quantizer = Decimal("1").scaleb(-places)
    volume = decimal_from_value(qty)
    if volume <= 0:
        return "0"
    rounded = volume.quantize(quantizer, rounding=ROUND_DOWN)
    return format(rounded, "f").rstrip("0").rstrip(".") or "0"


def truncate_volume(qty: object, *, places: int = 8) -> float:
    return float(format_volume(qty, places=places))
