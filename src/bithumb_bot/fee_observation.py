from __future__ import annotations

import math


ACCOUNTING_COMPLETE_FEE_STATUSES = frozenset({"complete", "operator_confirmed"})


def fee_accounting_status(
    *,
    fee: float | None,
    fee_status: str | None,
    price: float | None = None,
    qty: float | None = None,
    material_notional_threshold: float = 0.0,
) -> str:
    """Classify whether observed fee evidence is safe for canonical accounting."""
    if str(fee_status or "").strip() not in ACCOUNTING_COMPLETE_FEE_STATUSES:
        return "fee_pending"
    if fee is None:
        return "fee_pending"
    try:
        fee_value = float(fee)
    except (TypeError, ValueError):
        return "fee_pending"
    if not math.isfinite(fee_value) or fee_value < 0.0:
        return "fee_pending"

    notional = 0.0
    if price is not None and qty is not None:
        try:
            price_value = float(price)
            qty_value = float(qty)
        except (TypeError, ValueError):
            price_value = 0.0
            qty_value = 0.0
        if math.isfinite(price_value) and math.isfinite(qty_value):
            notional = max(0.0, price_value) * max(0.0, qty_value)

    threshold = max(0.0, float(material_notional_threshold or 0.0))
    if threshold > 0.0 and notional >= threshold and fee_value <= 1e-12:
        return "fee_pending"
    return "accounting_complete"
