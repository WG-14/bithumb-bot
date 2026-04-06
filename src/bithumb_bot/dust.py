from __future__ import annotations

import json
import re
from dataclasses import dataclass


DUST_POSITION_EPS = 1e-12
_SUMMARY_TOKEN_RE = re.compile(r"([a-z_]+)=([^\s]+)")


@dataclass(frozen=True)
class DustClassification:
    classification: str
    present: bool
    allow_resume: bool
    effective_flat: bool
    policy_reason: str
    summary: str
    broker_qty: float
    local_qty: float
    delta_qty: float
    min_qty: float
    min_notional_krw: float
    latest_price: float | None
    broker_notional_krw: float | None
    local_notional_krw: float | None
    partial_flatten_recent: bool
    partial_flatten_reason: str
    qty_gap_tolerance: float
    qty_gap_small: bool
    broker_qty_is_dust: bool
    local_qty_is_dust: bool
    broker_notional_is_dust: bool
    local_notional_is_dust: bool

    def to_metadata(self) -> dict[str, int | float | str]:
        return {
            "dust_classification": self.classification,
            "dust_residual_present": 1 if self.present else 0,
            "dust_residual_allow_resume": 1 if self.allow_resume else 0,
            "dust_effective_flat": 1 if self.effective_flat else 0,
            "dust_policy_reason": self.policy_reason,
            "dust_partial_flatten_recent": 1 if self.partial_flatten_recent else 0,
            "dust_partial_flatten_reason": self.partial_flatten_reason,
            "dust_qty_gap_tolerance": self.qty_gap_tolerance,
            "dust_qty_gap_small": 1 if self.qty_gap_small else 0,
            "dust_broker_qty": self.broker_qty,
            "dust_local_qty": self.local_qty,
            "dust_delta_qty": self.delta_qty,
            "dust_min_qty": self.min_qty,
            "dust_min_notional_krw": self.min_notional_krw,
            "dust_latest_price": self.latest_price if self.latest_price is not None else "",
            "dust_broker_notional_krw": (
                self.broker_notional_krw if self.broker_notional_krw is not None else ""
            ),
            "dust_local_notional_krw": (
                self.local_notional_krw if self.local_notional_krw is not None else ""
            ),
            "dust_broker_qty_is_dust": 1 if self.broker_qty_is_dust else 0,
            "dust_local_qty_is_dust": 1 if self.local_qty_is_dust else 0,
            "dust_broker_notional_is_dust": 1 if self.broker_notional_is_dust else 0,
            "dust_local_notional_is_dust": 1 if self.local_notional_is_dust else 0,
            "dust_residual_summary": self.summary[:280],
        }

    @classmethod
    def from_metadata(cls, metadata_raw: str | dict[str, object] | None) -> DustClassification:
        if metadata_raw is None:
            return _metadata_fallback(policy_reason="none")
        if isinstance(metadata_raw, dict):
            metadata = metadata_raw
        else:
            try:
                metadata = json.loads(str(metadata_raw))
            except json.JSONDecodeError:
                return _metadata_fallback(policy_reason="metadata_parse_error")

        present = bool(int(metadata.get("dust_residual_present", 0) or 0) == 1)
        summary = str(metadata.get("dust_residual_summary") or "none")
        summary_values = _parse_dust_summary(summary)
        effective_flat_raw = metadata.get("dust_effective_flat")
        if effective_flat_raw is None:
            effective_flat = summary_values.get("effective_flat")
            if effective_flat is None:
                effective_flat = False
            else:
                effective_flat = bool(effective_flat)
        else:
            effective_flat = bool(int(effective_flat_raw or 0) == 1)
        broker_qty = _float_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_broker_qty",
            summary_key="broker_qty",
            default=0.0,
        )
        local_qty = _float_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_local_qty",
            summary_key="local_qty",
            default=0.0,
        )
        delta_qty = _float_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_delta_qty",
            summary_key="delta",
            default=(broker_qty - local_qty),
        )
        min_qty = _float_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_min_qty",
            summary_key="min_qty",
            default=0.0,
        )
        min_notional_krw = _float_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_min_notional_krw",
            summary_key="min_notional_krw",
            default=0.0,
        )
        latest_price = _float_or_none(metadata.get("dust_latest_price"))
        broker_notional_krw = _float_or_none(metadata.get("dust_broker_notional_krw"))
        local_notional_krw = _float_or_none(metadata.get("dust_local_notional_krw"))
        if broker_notional_krw is None:
            broker_notional_krw = _estimate_notional(broker_qty, latest_price)
        if local_notional_krw is None:
            local_notional_krw = _estimate_notional(local_qty, latest_price)
        unresolved_open_order_count = _int_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="unresolved_open_order_count",
            summary_key="unresolved_open_order_count",
            default=0,
        )
        submit_unknown_count = _int_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="submit_unknown_count",
            summary_key="submit_unknown_count",
            default=0,
        )
        recovery_required_count = _int_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="recovery_required_count",
            summary_key="recovery_required_count",
            default=0,
        )
        if (
            unresolved_open_order_count == 0
            and "unresolved_open_order_count" not in metadata
            and "unresolved_open_order_count" not in summary_values
        ):
            try:
                from . import runtime_state

                unresolved_open_order_count = max(0, int(runtime_state.snapshot().unresolved_open_order_count))
            except Exception:
                pass
        if (
            recovery_required_count == 0
            and "recovery_required_count" not in metadata
            and "recovery_required_count" not in summary_values
        ):
            try:
                from . import runtime_state

                recovery_required_count = max(0, int(runtime_state.snapshot().recovery_required_count))
            except Exception:
                pass
        qty_gap_tolerance = _float_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_qty_gap_tolerance",
            summary_key="qty_gap_tolerance",
            default=0.0,
        )
        qty_gap_small = _bool_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_qty_gap_small",
            summary_key="qty_gap_small",
            default=(abs(delta_qty) <= qty_gap_tolerance if qty_gap_tolerance > 0.0 else abs(delta_qty) <= DUST_POSITION_EPS),
        )
        partial_flatten_recent = _bool_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_partial_flatten_recent",
            summary_key="partial_flatten_recent",
            default=False,
        )
        broker_qty_is_dust = _bool_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_broker_qty_is_dust",
            summary_key="broker_qty_is_dust",
            default=bool(broker_qty > DUST_POSITION_EPS and min_qty > 0.0 and broker_qty < min_qty),
        )
        local_qty_is_dust = _bool_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_local_qty_is_dust",
            summary_key="local_qty_is_dust",
            default=bool(local_qty > DUST_POSITION_EPS and min_qty > 0.0 and local_qty < min_qty),
        )
        broker_notional_is_dust = _bool_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_broker_notional_is_dust",
            summary_key="broker_notional_is_dust",
            default=bool(
                broker_notional_krw is not None
                and min_notional_krw > 0.0
                and broker_notional_krw < min_notional_krw
            ),
        )
        local_notional_is_dust = _bool_from_metadata_or_summary(
            metadata,
            summary_values,
            metadata_key="dust_local_notional_is_dust",
            summary_key="local_notional_is_dust",
            default=bool(
                local_notional_krw is not None
                and min_notional_krw > 0.0
                and local_notional_krw < min_notional_krw
            ),
        )
        inferred_classification = _infer_dust_classification(
            present=present,
            broker_qty_is_dust=broker_qty_is_dust,
            local_qty_is_dust=local_qty_is_dust,
            broker_notional_is_dust=broker_notional_is_dust,
            local_notional_is_dust=local_notional_is_dust,
            partial_flatten_recent=partial_flatten_recent,
            qty_gap_small=qty_gap_small,
            min_notional_krw=min_notional_krw,
        )
        classification = str(
            metadata.get("dust_classification")
            or summary_values.get("classification")
            or _classification_from_policy_reason(str(metadata.get("dust_policy_reason") or ""))
            or inferred_classification
            or "none"
        )
        if effective_flat_raw is None and not bool(summary_values.get("effective_flat")):
            effective_flat = bool(classification == "matched_harmless_dust")
        allow_resume = bool(
            classification == "matched_harmless_dust"
            and effective_flat
            and unresolved_open_order_count == 0
            and submit_unknown_count == 0
            and recovery_required_count == 0
        )
        if not present:
            policy_reason = str(metadata.get("dust_policy_reason") or "none")
        elif classification == "matched_harmless_dust" and allow_resume:
            policy_reason = "matched_harmless_dust_resume_allowed"
        elif classification == "matched_harmless_dust":
            policy_reason = "matched_harmless_dust_operator_review_required"
        else:
            policy_reason = "dangerous_dust_operator_review_required"
        return cls(
            classification=classification,
            present=present,
            allow_resume=allow_resume,
            effective_flat=effective_flat,
            policy_reason=policy_reason,
            summary=summary,
            broker_qty=broker_qty,
            local_qty=local_qty,
            delta_qty=delta_qty,
            min_qty=min_qty,
            min_notional_krw=min_notional_krw,
            latest_price=latest_price,
            broker_notional_krw=broker_notional_krw,
            local_notional_krw=local_notional_krw,
            partial_flatten_recent=partial_flatten_recent,
            partial_flatten_reason=str(metadata.get("dust_partial_flatten_reason") or "none"),
            qty_gap_tolerance=qty_gap_tolerance,
            qty_gap_small=qty_gap_small,
            broker_qty_is_dust=broker_qty_is_dust,
            local_qty_is_dust=local_qty_is_dust,
            broker_notional_is_dust=broker_notional_is_dust,
            local_notional_is_dust=local_notional_is_dust,
        )


@dataclass(frozen=True)
class DustOperatorView:
    state: str
    state_label: str
    operator_action: str
    operator_message: str
    broker_local_match: bool
    new_orders_allowed: bool
    resume_allowed: bool
    treat_as_flat: bool
    broker_qty: float
    local_qty: float
    delta_qty: float
    min_qty: float
    min_notional_krw: float
    broker_qty_below_min: bool
    local_qty_below_min: bool
    broker_notional_below_min: bool
    local_notional_below_min: bool

    @property
    def qty_below_min_summary(self) -> str:
        return format_broker_local_flags(
            broker=self.broker_qty_below_min,
            local=self.local_qty_below_min,
        )

    @property
    def notional_below_min_summary(self) -> str:
        return format_broker_local_flags(
            broker=self.broker_notional_below_min,
            local=self.local_notional_below_min,
        )

    @property
    def compact_summary(self) -> str:
        return (
            f"state={self.state} "
            f"broker_qty={self.broker_qty:.8f} "
            f"local_qty={self.local_qty:.8f} "
            f"delta_qty={self.delta_qty:.8f} "
            f"min_qty={self.min_qty:.8f} "
            f"min_notional_krw={self.min_notional_krw:.1f} "
            f"qty_below_min({self.qty_below_min_summary}) "
            f"notional_below_min({self.notional_below_min_summary}) "
            f"broker_local_match={1 if self.broker_local_match else 0} "
            f"operator_action={self.operator_action} "
            f"new_orders_allowed={1 if self.new_orders_allowed else 0} "
            f"resume_allowed={1 if self.resume_allowed else 0} "
            f"treat_as_flat={1 if self.treat_as_flat else 0}"
        )


@dataclass(frozen=True)
class DustDisplayContext:
    classification: DustClassification
    operator_view: DustOperatorView
    fields: dict[str, bool | float | str]

    @property
    def qty_below_min_summary(self) -> str:
        return self.operator_view.qty_below_min_summary

    @property
    def notional_below_min_summary(self) -> str:
        return self.operator_view.notional_below_min_summary

    @property
    def compact_summary(self) -> str:
        return self.operator_view.compact_summary


def format_flat_start_reason_with_dust(
    flat_start_reason: object,
    dust_context: DustDisplayContext,
) -> str:
    raw_reason = str(flat_start_reason or "").strip()
    if not raw_reason:
        return "not_checked"

    dust = dust_context.classification
    if not dust.present:
        return raw_reason

    if dust_context.operator_view.treat_as_flat and dust_context.operator_view.resume_allowed:
        return f"flat_start_effective_flat({dust_context.compact_summary})"

    if not raw_reason.startswith("flat_start_"):
        return raw_reason

    prefix = (
        "flat_start_effective_flat"
        if dust_context.operator_view.treat_as_flat
        else "flat_start_requires_operator_review"
    )
    return f"{prefix}({dust_context.compact_summary})"


def dust_qty_gap_tolerance(*, min_qty: float, default_abs_tolerance: float) -> float:
    normalized_min_qty = max(0.0, float(min_qty))
    normalized_default = max(0.0, float(default_abs_tolerance))
    if normalized_min_qty <= 0.0:
        return normalized_default
    return max(normalized_default, normalized_min_qty * 0.5)


def classify_dust_residual(
    *,
    broker_qty: float,
    local_qty: float,
    min_qty: float,
    min_notional_krw: float,
    latest_price: float | None,
    partial_flatten_recent: bool,
    partial_flatten_reason: str,
    qty_gap_tolerance: float,
    matched_harmless_resume_allowed: bool = False,
) -> DustClassification:
    normalized_broker_qty = max(0.0, float(broker_qty))
    normalized_local_qty = max(0.0, float(local_qty))
    normalized_min_qty = max(0.0, float(min_qty))
    normalized_min_notional = max(0.0, float(min_notional_krw))
    normalized_qty_gap_tolerance = max(0.0, float(qty_gap_tolerance))
    delta_qty = normalized_broker_qty - normalized_local_qty

    broker_present = normalized_broker_qty > DUST_POSITION_EPS
    local_present = normalized_local_qty > DUST_POSITION_EPS
    broker_qty_is_dust = bool(
        broker_present and normalized_min_qty > 0.0 and normalized_broker_qty < normalized_min_qty
    )
    local_qty_is_dust = bool(
        local_present and normalized_min_qty > 0.0 and normalized_local_qty < normalized_min_qty
    )

    broker_notional = _estimate_notional(normalized_broker_qty, latest_price)
    local_notional = _estimate_notional(normalized_local_qty, latest_price)
    broker_notional_is_dust = bool(
        broker_notional is not None
        and normalized_min_notional > 0.0
        and broker_notional < normalized_min_notional
    )
    local_notional_is_dust = bool(
        local_notional is not None
        and normalized_min_notional > 0.0
        and local_notional < normalized_min_notional
    )
    qty_gap_small = abs(delta_qty) <= normalized_qty_gap_tolerance

    present = bool(broker_qty_is_dust or local_qty_is_dust)
    matched_harmless = bool(
        present
        and broker_qty_is_dust
        and local_qty_is_dust
        and qty_gap_small
    )
    classification = "none"
    if present:
        classification = "matched_harmless_dust" if matched_harmless else "dangerous_dust"

    allow_resume = bool(matched_harmless and matched_harmless_resume_allowed)
    effective_flat = bool((not broker_present and not local_present) or matched_harmless)

    if not present:
        policy_reason = "no_dust_residual"
    elif matched_harmless and allow_resume:
        policy_reason = "matched_harmless_dust_resume_allowed"
    elif matched_harmless:
        policy_reason = "matched_harmless_dust_operator_review_required"
    else:
        policy_reason = "dangerous_dust_operator_review_required"

    summary = (
        f"broker_qty={normalized_broker_qty:.8f} local_qty={normalized_local_qty:.8f} "
        f"delta={delta_qty:.8f} min_qty={normalized_min_qty:.8f} "
        f"min_notional_krw={normalized_min_notional:.1f} "
        f"classification={classification} "
        f"matched_harmless={1 if matched_harmless else 0} "
        f"broker_local_match={1 if qty_gap_small else 0} "
        f"allow_resume={1 if allow_resume else 0} "
        f"effective_flat={1 if effective_flat else 0} "
        f"qty_gap_small={1 if qty_gap_small else 0} "
        f"policy_reason={policy_reason} "
        f"partial_flatten_recent={1 if partial_flatten_recent else 0}"
    )
    return DustClassification(
        classification=classification,
        present=present,
        allow_resume=allow_resume,
        effective_flat=effective_flat,
        policy_reason=policy_reason,
        summary=summary,
        broker_qty=normalized_broker_qty,
        local_qty=normalized_local_qty,
        delta_qty=delta_qty,
        min_qty=normalized_min_qty,
        min_notional_krw=normalized_min_notional,
        latest_price=_float_or_none(latest_price),
        broker_notional_krw=broker_notional,
        local_notional_krw=local_notional,
        partial_flatten_recent=bool(partial_flatten_recent),
        partial_flatten_reason=str(partial_flatten_reason or "none"),
        qty_gap_tolerance=normalized_qty_gap_tolerance,
        qty_gap_small=qty_gap_small,
        broker_qty_is_dust=broker_qty_is_dust,
        local_qty_is_dust=local_qty_is_dust,
        broker_notional_is_dust=broker_notional_is_dust,
        local_notional_is_dust=local_notional_is_dust,
    )


def no_dust_classification(*, policy_reason: str) -> DustClassification:
    return DustClassification(
        classification="none",
        present=False,
        allow_resume=False,
        effective_flat=True,
        policy_reason=policy_reason,
        summary="none",
        broker_qty=0.0,
        local_qty=0.0,
        delta_qty=0.0,
        min_qty=0.0,
        min_notional_krw=0.0,
        latest_price=None,
        broker_notional_krw=None,
        local_notional_krw=None,
        partial_flatten_recent=False,
        partial_flatten_reason="none",
        qty_gap_tolerance=0.0,
        qty_gap_small=True,
        broker_qty_is_dust=False,
        local_qty_is_dust=False,
        broker_notional_is_dust=False,
        local_notional_is_dust=False,
    )


def build_dust_operator_view(
    metadata_raw: str | dict[str, object] | DustClassification | None,
) -> DustOperatorView:
    dust = (
        metadata_raw
        if isinstance(metadata_raw, DustClassification)
        else DustClassification.from_metadata(metadata_raw)
    )

    if dust.policy_reason == "metadata_parse_error":
        state = "unknown"
        state_label = "dust metadata unavailable"
        operator_action = "rerun_reconcile_and_review"
        operator_message = "Dust metadata could not be parsed. Re-run reconcile and review before resuming."
        new_orders_allowed = False
        resume_allowed = False
        treat_as_flat = False
    elif dust.classification == "matched_harmless_dust":
        state = "matched_harmless_dust"
        state_label = "matched harmless dust residual"
        if dust.allow_resume:
            operator_action = "matched_dust_tracked_resume_allowed"
            operator_message = (
                "Broker/local matched dust remains tracked below minimum trade size, so it is not auto-liquidated. "
                "This residual is tracked only, effective-flat gating applies, and resume/new orders are allowed."
            )
            new_orders_allowed = True
            resume_allowed = True
        else:
            operator_action = "review_matched_dust_policy"
            operator_message = (
                "Residual dust matches across broker/local state, but remains below minimum tradable quantity, so automatic resume and new orders stay blocked pending operator review."
            )
            new_orders_allowed = False
            resume_allowed = False
        treat_as_flat = True
    elif dust.present:
        state = "dangerous_dust"
        state_label = "dangerous dust residual requires manual review"
        operator_action = "manual_review_before_resume"
        operator_message = (
            "Dust residual is not harmless. Review broker/local mismatch or recovery concerns before resuming or placing new orders."
        )
        new_orders_allowed = False
        resume_allowed = False
        treat_as_flat = False
    else:
        state = "none"
        state_label = "no dust residual"
        operator_action = "none"
        operator_message = "No dust residual signal is blocking operations."
        new_orders_allowed = True
        resume_allowed = True
        treat_as_flat = bool(dust.effective_flat)

    return DustOperatorView(
        state=state,
        state_label=state_label,
        operator_action=operator_action,
        operator_message=operator_message,
        broker_local_match=bool(dust.qty_gap_small),
        new_orders_allowed=new_orders_allowed,
        resume_allowed=resume_allowed,
        treat_as_flat=treat_as_flat,
        broker_qty=dust.broker_qty,
        local_qty=dust.local_qty,
        delta_qty=dust.delta_qty,
        min_qty=dust.min_qty,
        min_notional_krw=dust.min_notional_krw,
        broker_qty_below_min=bool(dust.broker_qty_is_dust),
        local_qty_below_min=bool(dust.local_qty_is_dust),
        broker_notional_below_min=bool(dust.broker_notional_is_dust),
        local_notional_below_min=bool(dust.local_notional_is_dust),
    )


def format_broker_local_flags(*, broker: bool, local: bool) -> str:
    return f"broker={1 if broker else 0} local={1 if local else 0}"


def build_dust_display_context(
    metadata_raw: str | dict[str, object] | DustClassification | None,
) -> DustDisplayContext:
    dust = (
        metadata_raw
        if isinstance(metadata_raw, DustClassification)
        else DustClassification.from_metadata(metadata_raw)
    )
    view = build_dust_operator_view(dust)
    return DustDisplayContext(
        classification=dust,
        operator_view=view,
        fields={
            "dust_classification": dust.classification,
            "dust_residual_present": bool(dust.present),
            "dust_residual_allow_resume": bool(dust.allow_resume),
            "dust_policy_reason": dust.policy_reason,
            "dust_residual_summary": dust.summary,
            "dust_state": view.state,
            "dust_state_label": view.state_label,
            "dust_operator_action": view.operator_action,
            "dust_operator_message": view.operator_message,
            "dust_broker_local_match": bool(view.broker_local_match),
            "dust_new_orders_allowed": bool(view.new_orders_allowed),
            "dust_resume_allowed_by_policy": bool(view.resume_allowed),
            "dust_treat_as_flat": bool(view.treat_as_flat),
            "dust_broker_qty": view.broker_qty,
            "dust_local_qty": view.local_qty,
            "dust_delta_qty": view.delta_qty,
            "dust_min_qty": view.min_qty,
            "dust_min_notional_krw": view.min_notional_krw,
            "dust_broker_qty_below_min": bool(view.broker_qty_below_min),
            "dust_local_qty_below_min": bool(view.local_qty_below_min),
            "dust_broker_notional_below_min": bool(view.broker_notional_below_min),
            "dust_local_notional_below_min": bool(view.local_notional_below_min),
        },
    )


def _metadata_fallback(*, policy_reason: str) -> DustClassification:
    return DustClassification(
        classification="none",
        present=False,
        allow_resume=False,
        effective_flat=False,
        policy_reason=policy_reason,
        summary="none",
        broker_qty=0.0,
        local_qty=0.0,
        delta_qty=0.0,
        min_qty=0.0,
        min_notional_krw=0.0,
        latest_price=None,
        broker_notional_krw=None,
        local_notional_krw=None,
        partial_flatten_recent=False,
        partial_flatten_reason="none",
        qty_gap_tolerance=0.0,
        qty_gap_small=False,
        broker_qty_is_dust=False,
        local_qty_is_dust=False,
        broker_notional_is_dust=False,
        local_notional_is_dust=False,
    )


def _estimate_notional(qty: float, latest_price: float | None) -> float | None:
    normalized_price = _float_or_none(latest_price)
    if normalized_price is None:
        return None
    return qty * normalized_price


def _float_or_default(raw: object, default: float) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _float_or_none(raw: object) -> float | None:
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0.0 else None


def _parse_dust_summary(summary: str) -> dict[str, object]:
    parsed: dict[str, object] = {}
    for key, raw_value in _SUMMARY_TOKEN_RE.findall(str(summary or "")):
        if raw_value in {"0", "1"}:
            parsed[key] = raw_value == "1"
            continue
        try:
            parsed[key] = float(raw_value)
        except ValueError:
            parsed[key] = raw_value
    return parsed


def _float_from_metadata_or_summary(
    metadata: dict[str, object],
    summary_values: dict[str, object],
    *,
    metadata_key: str,
    summary_key: str,
    default: float,
) -> float:
    if metadata_key in metadata:
        return _float_or_default(metadata.get(metadata_key), default)
    summary_value = summary_values.get(summary_key)
    if summary_value is None:
        return float(default)
    return _float_or_default(summary_value, default)


def _bool_from_metadata_or_summary(
    metadata: dict[str, object],
    summary_values: dict[str, object],
    *,
    metadata_key: str,
    summary_key: str,
    default: bool,
) -> bool:
    if metadata_key in metadata:
        return bool(int(metadata.get(metadata_key, 0) or 0) == 1)
    summary_value = summary_values.get(summary_key)
    if isinstance(summary_value, bool):
        return summary_value
    if summary_value is None:
        return bool(default)
    try:
        return bool(int(summary_value))
    except (TypeError, ValueError):
        return bool(default)


def _int_from_metadata_or_summary(
    metadata: dict[str, object],
    summary_values: dict[str, object],
    *,
    metadata_key: str,
    summary_key: str,
    default: int,
) -> int:
    if metadata_key in metadata:
        try:
            return max(0, int(metadata.get(metadata_key, default) or 0))
        except (TypeError, ValueError):
            return max(0, int(default))
    summary_value = summary_values.get(summary_key)
    if summary_value is None:
        return max(0, int(default))
    try:
        return max(0, int(summary_value))
    except (TypeError, ValueError):
        return max(0, int(default))


def _classification_from_policy_reason(policy_reason: str) -> str | None:
    normalized = str(policy_reason or "").strip()
    if normalized.startswith("matched_harmless_dust_"):
        return "matched_harmless_dust"
    if normalized.startswith("dangerous_dust_"):
        return "dangerous_dust"
    if normalized == "no_dust_residual":
        return "none"
    return None


def _infer_dust_classification(
    *,
    present: bool,
    broker_qty_is_dust: bool,
    local_qty_is_dust: bool,
    broker_notional_is_dust: bool,
    local_notional_is_dust: bool,
    partial_flatten_recent: bool,
    qty_gap_small: bool,
    min_notional_krw: float,
) -> str:
    if not present:
        return "none"
    matched_harmless = bool(
        broker_qty_is_dust
        and local_qty_is_dust
        and qty_gap_small
    )
    return "matched_harmless_dust" if matched_harmless else "dangerous_dust"
