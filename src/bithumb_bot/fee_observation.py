from __future__ import annotations

import math
from dataclasses import dataclass

from .config import settings
from .execution import order_fill_tolerance


ACCOUNTING_COMPLETE_FEE_STATUSES = frozenset(
    {"complete", "operator_confirmed", "validated_order_level_paid_fee"}
)
_FEE_RATE_ABS_TOLERANCE_KRW = 0.05
_FUNDS_MATCH_ABS_TOLERANCE_KRW = 0.05


@dataclass(frozen=True)
class FeeEvaluation:
    fee: float | None
    fee_status: str
    fee_source: str
    fee_confidence: str
    accounting_eligibility: str
    accounting_status: str
    provenance: str
    reason: str
    checks: dict[str, bool]


def _to_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _qty_match(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return True
    tolerance = order_fill_tolerance(max(abs(float(left)), abs(float(right))))
    return abs(float(left) - float(right)) <= max(1e-12, tolerance)


def _funds_match(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return True
    return abs(float(left) - float(right)) <= _FUNDS_MATCH_ABS_TOLERANCE_KRW


def _expected_fee_match(*, notional: float | None, fee: float | None, fee_rate: float | None) -> bool:
    if fee is None or notional is None or fee_rate is None:
        return False
    expected_fee = max(0.0, float(notional)) * max(0.0, float(fee_rate))
    tolerance = max(_FEE_RATE_ABS_TOLERANCE_KRW, expected_fee * 0.02)
    return abs(float(fee) - expected_fee) <= tolerance


def classify_fee_evaluation(
    *,
    fee: float | None,
    fee_status: str | None,
    price: float | None = None,
    qty: float | None = None,
    material_notional_threshold: float = 0.0,
    fee_source: str | None = None,
    fee_confidence: str | None = None,
    provenance: str | None = None,
    reason: str | None = None,
    checks: dict[str, bool] | None = None,
) -> FeeEvaluation:
    """Classify whether observed fee evidence is safe for canonical accounting."""
    status = str(fee_status or "").strip() or "unknown"
    source = str(fee_source or "").strip() or "unknown"
    confidence = str(fee_confidence or "").strip() or "unknown"
    provenance_text = str(provenance or "").strip() or "unknown"
    reason_text = str(reason or "").strip() or status or "unknown"
    check_map = dict(checks or {})

    fee_value = _to_float(fee)
    if fee is not None and fee_value is None:
        return FeeEvaluation(
            fee=None,
            fee_status=status,
            fee_source=source,
            fee_confidence=("invalid" if confidence == "unknown" else confidence),
            accounting_eligibility="blocked",
            accounting_status="fee_pending",
            provenance=provenance_text,
            reason=("invalid_fee_value" if reason_text == "unknown" else reason_text),
            checks=check_map,
        )
    if fee_value is not None and fee_value < 0.0:
        return FeeEvaluation(
            fee=fee_value,
            fee_status=status,
            fee_source=source,
            fee_confidence=("invalid" if confidence == "unknown" else confidence),
            accounting_eligibility="blocked",
            accounting_status="fee_pending",
            provenance=provenance_text,
            reason=("negative_fee_blocked" if reason_text == "unknown" else reason_text),
            checks=check_map,
        )

    notional = 0.0
    price_value = _to_float(price)
    qty_value = _to_float(qty)
    if price_value is not None and qty_value is not None:
        notional = max(0.0, price_value) * max(0.0, qty_value)

    threshold = max(0.0, float(material_notional_threshold or 0.0))
    suspicious_zero = threshold > 0.0 and notional >= threshold and fee_value is not None and fee_value <= 1e-12
    if suspicious_zero:
        return FeeEvaluation(
            fee=fee_value,
            fee_status=status,
            fee_source=(source if source != "unknown" else "trade_level_fee"),
            fee_confidence=("invalid" if confidence == "unknown" else confidence),
            accounting_eligibility="blocked",
            accounting_status="fee_pending",
            provenance=provenance_text,
            reason=("material_zero_fee_reported" if reason_text == "unknown" else reason_text),
            checks=check_map,
        )

    if status in ACCOUNTING_COMPLETE_FEE_STATUSES and fee_value is not None:
        resolved_source = source
        resolved_confidence = confidence
        resolved_provenance = provenance_text
        if status == "complete":
            resolved_source = resolved_source if resolved_source != "unknown" else "trade_level_fee"
            resolved_confidence = resolved_confidence if resolved_confidence != "unknown" else "authoritative"
            resolved_provenance = (
                resolved_provenance if resolved_provenance != "unknown" else "trade_level_fee_present"
            )
        elif status == "operator_confirmed":
            resolved_source = resolved_source if resolved_source != "unknown" else "operator_confirmed"
            resolved_confidence = resolved_confidence if resolved_confidence != "unknown" else "authoritative"
            resolved_provenance = (
                resolved_provenance if resolved_provenance != "unknown" else "operator_confirmed"
            )
        elif status == "validated_order_level_paid_fee":
            resolved_source = resolved_source if resolved_source != "unknown" else "order_level_paid_fee"
            resolved_confidence = resolved_confidence if resolved_confidence != "unknown" else "validated"
            resolved_provenance = (
                resolved_provenance
                if resolved_provenance != "unknown"
                else "order_level_paid_fee_validated_single_fill"
            )
        return FeeEvaluation(
            fee=fee_value,
            fee_status=status,
            fee_source=resolved_source,
            fee_confidence=resolved_confidence,
            accounting_eligibility="complete",
            accounting_status="accounting_complete",
            provenance=resolved_provenance,
            reason=("accounting_complete" if reason_text == "unknown" else reason_text),
            checks=check_map,
        )

    if status in {"invalid", "unparseable"}:
        eligibility = "blocked"
        confidence = "invalid" if confidence == "unknown" else confidence
    elif status in {"empty", "missing", "order_level_candidate", "zero_reported"}:
        eligibility = "pending"
        if confidence == "unknown":
            confidence = "ambiguous" if status == "order_level_candidate" else "invalid"
    else:
        eligibility = "pending"
    return FeeEvaluation(
        fee=fee_value,
        fee_status=status,
        fee_source=source,
        fee_confidence=confidence,
        accounting_eligibility=eligibility,
        accounting_status="fee_pending",
        provenance=provenance_text,
        reason=reason_text,
        checks=check_map,
    )


def validate_single_fill_order_level_paid_fee(
    *,
    paid_fee: object,
    fill_qty: float | None,
    fill_price: float | None,
    fill_funds: float | None = None,
    order_executed_volume: float | None = None,
    order_executed_funds: float | None = None,
    single_fill_evidence: bool,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    fill_id: str | None = None,
    configured_fee_rate: float | None = None,
    material_notional_threshold: float | None = None,
) -> FeeEvaluation:
    fee_value = _to_float(paid_fee)
    price_value = _to_float(fill_price)
    qty_value = _to_float(fill_qty)
    fill_funds_value = _to_float(fill_funds)
    if fill_funds_value is None and price_value is not None and qty_value is not None:
        fill_funds_value = price_value * qty_value
    executed_volume_value = _to_float(order_executed_volume)
    executed_funds_value = _to_float(order_executed_funds)
    threshold = max(
        0.0,
        float(
            settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
            if material_notional_threshold is None
            else material_notional_threshold
        ),
    )
    checks = {
        "single_fill": bool(single_fill_evidence),
        "paid_fee_present": fee_value is not None and fee_value >= 0.0,
        "executed_volume_match": _qty_match(qty_value, executed_volume_value),
        "executed_funds_match": _funds_match(fill_funds_value, executed_funds_value),
        "expected_fee_rate_match": _expected_fee_match(
            notional=fill_funds_value,
            fee=fee_value,
            fee_rate=_to_float(
                settings.LIVE_FEE_RATE_ESTIMATE if configured_fee_rate is None else configured_fee_rate
            ),
        ),
        "identifiers_match": bool(
            str(exchange_order_id or "").strip()
            and str(fill_id or "").strip()
            and str(client_order_id or "").strip()
        ),
        "material_notional_suspicious": bool(
            fill_funds_value is not None and fill_funds_value >= threshold
        ),
    }
    checks["expected_fee_rate_warning"] = not bool(checks["expected_fee_rate_match"])
    material_suspicious = bool(checks["material_notional_suspicious"])

    if fee_value is None:
        return classify_fee_evaluation(
            fee=None,
            fee_status="order_level_candidate",
            fee_source="order_level_paid_fee",
            fee_confidence="invalid",
            provenance="order_level_paid_fee_invalid",
            reason="paid_fee_missing_or_unparseable",
            checks=checks,
            material_notional_threshold=threshold,
            price=price_value,
            qty=qty_value,
        )
    if fee_value < 0.0:
        return classify_fee_evaluation(
            fee=fee_value,
            fee_status="order_level_candidate",
            fee_source="order_level_paid_fee",
            fee_confidence="invalid",
            provenance="order_level_paid_fee_invalid",
            reason="negative_paid_fee",
            checks=checks,
            material_notional_threshold=threshold,
            price=price_value,
            qty=qty_value,
        )
    if material_suspicious and fee_value <= 1e-12:
        return classify_fee_evaluation(
            fee=fee_value,
            fee_status="order_level_candidate",
            fee_source="order_level_paid_fee",
            fee_confidence="invalid",
            provenance="order_level_paid_fee_zero_material_notional",
            reason="zero_paid_fee_material_notional",
            checks=checks,
            material_notional_threshold=threshold,
            price=price_value,
            qty=qty_value,
        )

    if all(
        checks[key]
        for key in (
            "single_fill",
            "paid_fee_present",
            "executed_volume_match",
            "executed_funds_match",
            "identifiers_match",
        )
    ):
        validated_reason = "order_level_paid_fee_validated_single_fill"
        validated_provenance = "order_level_paid_fee_validated_single_fill"
        if not checks["expected_fee_rate_match"]:
            validated_reason = "order_level_paid_fee_validated_single_fill_expected_fee_rate_mismatch"
            validated_provenance = "order_level_paid_fee_validated_single_fill_fee_rate_warning"
        return classify_fee_evaluation(
            fee=fee_value,
            fee_status="validated_order_level_paid_fee",
            fee_source="order_level_paid_fee",
            fee_confidence="validated",
            provenance=validated_provenance,
            reason=validated_reason,
            checks=checks,
            material_notional_threshold=threshold,
            price=price_value,
            qty=qty_value,
        )

    reason = "order_level_paid_fee_validation_failed"
    confidence = "ambiguous"
    if not checks["single_fill"]:
        reason = "multi_fill_order_level_fee_ambiguous"
    elif not checks["identifiers_match"]:
        reason = "identifier_mismatch"
    elif not checks["executed_volume_match"]:
        reason = "executed_volume_mismatch"
    elif not checks["executed_funds_match"]:
        reason = "executed_funds_mismatch"
    return classify_fee_evaluation(
        fee=fee_value,
        fee_status="order_level_candidate",
        fee_source="order_level_paid_fee",
        fee_confidence=confidence,
        provenance="order_level_paid_fee_unvalidated",
        reason=reason,
        checks=checks,
        material_notional_threshold=threshold,
        price=price_value,
        qty=qty_value,
    )


def fee_accounting_status(
    *,
    fee: float | None,
    fee_status: str | None,
    price: float | None = None,
    qty: float | None = None,
    material_notional_threshold: float = 0.0,
    fee_source: str | None = None,
    fee_confidence: str | None = None,
    provenance: str | None = None,
    reason: str | None = None,
    checks: dict[str, bool] | None = None,
) -> str:
    return classify_fee_evaluation(
        fee=fee,
        fee_status=fee_status,
        price=price,
        qty=qty,
        material_notional_threshold=material_notional_threshold,
        fee_source=fee_source,
        fee_confidence=fee_confidence,
        provenance=provenance,
        reason=reason,
        checks=checks,
    ).accounting_status
