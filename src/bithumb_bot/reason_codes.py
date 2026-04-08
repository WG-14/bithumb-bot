from __future__ import annotations

import math

# Standardized observability reason codes for core safety events.
RISKY_ORDER_BLOCK = "RISKY_ORDER_BLOCK"
STARTUP_BLOCKED = "STARTUP_BLOCKED"
HALT_ENTERED = "HALT_ENTERED"
CANCEL_FAILURE = "CANCEL_FAILURE"
RECONCILE_MISMATCH = "RECONCILE_MISMATCH"
SUBMIT_TIMEOUT = "SUBMIT_TIMEOUT"
AMBIGUOUS_SUBMIT = "AMBIGUOUS_SUBMIT"
SUBMIT_FAILED = "SUBMIT_FAILED"
WEAK_ORDER_CORRELATION = "WEAK_ORDER_CORRELATION"
AMBIGUOUS_RECENT_FILL = "AMBIGUOUS_RECENT_FILL"
POSITION_LOSS_LIMIT = "POSITION_LOSS_LIMIT"
DUST_RESIDUAL_UNSELLABLE = "DUST_RESIDUAL_UNSELLABLE"
DUST_RESIDUAL_SUPPRESSED = "DUST_RESIDUAL_SUPPRESSED"
EXIT_PARTIAL_LEFT_DUST = "EXIT_PARTIAL_LEFT_DUST"
MANUAL_DUST_REVIEW_REQUIRED = "MANUAL_DUST_REVIEW_REQUIRED"

# Resume / recovery blocker taxonomy for operator-facing health and recovery reporting.
BLOCKER_TRADE_FILL_UNRESOLVED = "TRADE_FILL_UNRESOLVED"
BLOCKER_PORTFOLIO_BROKER_CASH_MISMATCH = "PORTFOLIO_BROKER_CASH_MISMATCH"
BLOCKER_EXTERNAL_CASH_ADJUSTMENT_MISSING = "EXTERNAL_CASH_ADJUSTMENT_MISSING"
BLOCKER_EXTERNAL_CASH_ADJUSTMENT_REQUIRED = "EXTERNAL_CASH_ADJUSTMENT_REQUIRED"
BLOCKER_BROKER_CASH_DELTA_UNEXPLAINED = "BROKER_CASH_DELTA_UNEXPLAINED"
BLOCKER_DUST_RESIDUAL = "DUST_RESIDUAL_BLOCK"
BLOCKER_SUBMIT_UNKNOWN_RECOVERY_REQUIRED = "SUBMIT_UNKNOWN_RECOVERY_REQUIRED"

# SELL failure categories used by operator-facing reporting and audit evidence.
SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH = "qty_step_mismatch"
SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN = "boundary_below_min"
SELL_FAILURE_CATEGORY_DUST_RESIDUAL_UNSELLABLE = "dust_residual_unsellable"
SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH = "unsafe_dust_mismatch_dust"
SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD = "remainder_dust_guard"
SELL_FAILURE_CATEGORY_DUST_SUPPRESSION = "dust_suppression"
SELL_FAILURE_CATEGORY_SUBMISSION_HALT = "submission_halt"
SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE = "unresolved_risk_gate"
SELL_FAILURE_CATEGORY_UNKNOWN = "unknown"

EMERGENCY_FLATTEN_STARTED = "EMERGENCY_FLATTEN_STARTED"
EMERGENCY_FLATTEN_SUCCEEDED = "EMERGENCY_FLATTEN_SUCCEEDED"
EMERGENCY_FLATTEN_FAILED = "EMERGENCY_FLATTEN_FAILED"


def _detail_text_from_sell_failure_inputs(
    *,
    reason_code: str | None = None,
    reason: str | None = None,
    error_class: str | None = None,
    error_summary: str | None = None,
    dust_details: dict[str, object] | None = None,
) -> str:
    dust_details = dust_details or {}
    return " ".join(
        part
        for part in (
            str(reason_code or "").strip(),
            str(reason or "").strip(),
            str(error_class or "").strip(),
            str(error_summary or "").strip(),
            str(dust_details.get("sell_failure_category") or "").strip(),
            str(dust_details.get("sell_failure_detail") or "").strip(),
            str(dust_details.get("sell_qty_boundary_kind") or "").strip(),
            str(dust_details.get("summary") or "").strip(),
        )
        if part
    ).lower()


def _float_from_dust_details(dust_details: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        raw_value = dust_details.get(key)
        if raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            return value
    return None


def _sell_qty_boundary_kind_from_dust_details(*, dust_details: dict[str, object] | None = None) -> str:
    dust_details = dust_details or {}
    boundary_kind = str(
        dust_details.get("sell_qty_boundary_kind")
        or dust_details.get("boundary_kind")
        or dust_details.get("sell_boundary_kind")
        or ""
    ).strip().lower()
    if boundary_kind in {"min_qty", "qty_step", "dust_mismatch", "remainder_after_sell"}:
        return boundary_kind

    detail_text = _detail_text_from_sell_failure_inputs(dust_details=dust_details)
    if str(dust_details.get("dust_scope") or "") == "remainder_after_sell" or "guard_action=block_sell_remainder_dust" in detail_text:
        return "remainder_after_sell"

    min_qty = _float_from_dust_details(dust_details, "min_qty")
    if min_qty is not None and min_qty > 0:
        for key in ("qty", "position_qty", "requested_qty", "normalized_qty", "sell_qty_basis_qty"):
            value = _float_from_dust_details(dust_details, key)
            if value is not None and 0 <= value < min_qty:
                return "min_qty"

    if any(
        bool(dust_details.get(key))
        for key in ("qty_below_min", "normalized_below_min", "notional_below_min", "normalized_non_positive")
    ) or any(
        token in detail_text
        for token in (
            "qty_below_min",
            "normalized_non_positive",
            "normalized_below_min",
            "notional_below_min",
            "min_qty_or_notional_boundary",
            "boundary_below_min",
        )
    ):
        return "min_qty"

    if any(
        bool(dust_details.get(key))
        for key in (
            "qty_step_mismatch",
            "qty_step",
            "max_qty_decimals",
            "broker_volume_decimals",
        )
    ) or any(
        token in detail_text
        for token in (
            "qty_step",
            "qty does not match qty_step",
            "requires explicit normalization",
            "max decimals",
        )
    ):
        return "qty_step"

    if any(
        bool(dust_details.get(key))
        for key in (
            "dust_broker_qty_is_dust",
            "dust_local_qty_is_dust",
            "dust_broker_notional_is_dust",
            "dust_local_notional_is_dust",
            "dust_qty_gap_small",
        )
    ) or any(token in detail_text for token in ("mismatch", "qty_gap_small", "dust_gap")):
        return "dust_mismatch"

    return "none"


def classify_sell_failure_category(
    *,
    reason_code: str | None = None,
    reason: str | None = None,
    error_class: str | None = None,
    error_summary: str | None = None,
    dust_details: dict[str, object] | None = None,
) -> str:
    dust_details = dust_details or {}
    detail_text = _detail_text_from_sell_failure_inputs(
        reason_code=reason_code,
        reason=reason,
        error_class=error_class,
        error_summary=error_summary,
        dust_details=dust_details,
    )
    boundary_kind = _sell_qty_boundary_kind_from_dust_details(dust_details=dust_details)
    has_boundary_detail = any(
        token in detail_text
        for token in (
            "qty_below_min",
            "normalized_non_positive",
            "normalized_below_min",
            "notional_below_min",
            "min_qty_or_notional_boundary",
            "boundary_below_min",
        )
    )
    has_qty_step_detail = any(
        token in detail_text
        for token in (
            "qty_step",
            "qty does not match qty_step",
            "requires explicit normalization",
            "max decimals",
        )
    )
    has_dust_mismatch_detail = any(
        token in detail_text
        for token in ("mismatch", "qty_gap_small", "dust_gap")
    )

    if reason_code == DUST_RESIDUAL_SUPPRESSED:
        return SELL_FAILURE_CATEGORY_DUST_SUPPRESSION
    if reason_code == DUST_RESIDUAL_UNSELLABLE:
        if boundary_kind == "remainder_after_sell":
            return SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD
        if boundary_kind == "min_qty":
            return SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN
        if boundary_kind == "qty_step":
            return SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH
        if boundary_kind == "dust_mismatch":
            return SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH
        if has_boundary_detail:
            return SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN
        if has_qty_step_detail:
            return SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH
        if has_dust_mismatch_detail:
            return SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH
        return SELL_FAILURE_CATEGORY_DUST_RESIDUAL_UNSELLABLE
    if reason_code == RISKY_ORDER_BLOCK:
        if "runtime halted" in detail_text or "halt" in detail_text:
            return SELL_FAILURE_CATEGORY_SUBMISSION_HALT
        return SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE
    if boundary_kind == "remainder_after_sell":
        return SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD
    if boundary_kind == "min_qty":
        return SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN
    if boundary_kind == "qty_step":
        return SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH
    if boundary_kind == "dust_mismatch":
        return SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH
    if has_boundary_detail:
        return SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN
    if has_qty_step_detail:
        return SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH
    if has_dust_mismatch_detail:
        return SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH
    if "runtime halted" in detail_text:
        return SELL_FAILURE_CATEGORY_SUBMISSION_HALT
    if "unresolved order gate" in detail_text or "reason_detail_code" in detail_text:
        return SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE
    return SELL_FAILURE_CATEGORY_UNKNOWN


def sell_failure_detail_from_category(
    *,
    sell_failure_category: str,
    dust_details: dict[str, object] | None = None,
) -> str:
    detail = str(sell_failure_category or "unknown").strip() or SELL_FAILURE_CATEGORY_UNKNOWN
    if detail in {
        SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
        SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD,
        SELL_FAILURE_CATEGORY_DUST_SUPPRESSION,
        SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
        SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
        SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
        SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH,
        SELL_FAILURE_CATEGORY_DUST_RESIDUAL_UNSELLABLE,
        SELL_FAILURE_CATEGORY_UNKNOWN,
    }:
        return detail

    boundary_kind = _sell_qty_boundary_kind_from_dust_details(dust_details=dust_details)
    if boundary_kind == "remainder_after_sell":
        return SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD
    if boundary_kind == "min_qty":
        return SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN
    if boundary_kind == "qty_step":
        return SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH
    if boundary_kind == "dust_mismatch":
        return SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH
    return SELL_FAILURE_CATEGORY_DUST_RESIDUAL_UNSELLABLE
