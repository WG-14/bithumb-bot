from __future__ import annotations

import json
import math
import re
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation
from dataclasses import dataclass
from enum import Enum
from typing import Final

from .lot_model import build_market_lot_rules, lot_count_to_qty, qty_to_executable_lot_count


DUST_POSITION_EPS = 1e-12
OPEN_EXPOSURE_LOT_STATE: Final = "open_exposure"
DUST_TRACKING_LOT_STATE: Final = "dust_tracking"
_SUMMARY_TOKEN_RE = re.compile(r"([a-z_]+)=([^\s]+)")
_DECIMAL_ZERO = Decimal("0")

LOT_STATE_QUANTITY_CONTRACT: Final[dict[str, dict[str, object]]] = {
    OPEN_EXPOSURE_LOT_STATE: {
        "meaning": "real strategy-visible position",
        "strategy_qty_source": "open_exposure_qty",
        "strategy_lot_source": "open_lot_count",
        "sell_submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
        "sell_submit_lot_source": "position_state.normalized_exposure.sellable_executable_lot_count",
        "sell_submission_allowed": True,
        "sell_submit_includes_dust_tracking": False,
        "qty_boundary_rule": "qty_open >= min_qty remains open_exposure; SELL sizing consumes sellable_executable_qty from normalized state",
        "operator_tracking_only": False,
    },
    DUST_TRACKING_LOT_STATE: {
        "meaning": "operator tracking residual",
        "strategy_qty_source": "dust_tracking_qty",
        "strategy_lot_source": "dust_tracking_lot_count",
        "sell_submit_qty_source": "excluded_from_sell_qty",
        "sell_submit_lot_source": "excluded_from_sell_lot_count",
        "sell_submission_allowed": False,
        "sell_submit_includes_dust_tracking": False,
        "qty_boundary_rule": "qty_open < min_qty is tracked here; SELL submission excludes dust_tracking by default",
        "operator_tracking_only": True,
    },
}


class DustState(str, Enum):
    NO_DUST = "no_dust"
    HARMLESS_DUST = "harmless_dust"
    BLOCKING_DUST = "blocking_dust"


@dataclass(frozen=True)
class ExecutableLot:
    raw_qty: float
    executable_qty: float
    dust_qty: float
    effective_min_trade_qty: float
    min_qty: float
    qty_step: float
    min_notional_krw: float
    exit_price_floor: float | None
    exit_fee_ratio: float
    exit_slippage_ratio: float
    exit_buffer_ratio: float
    exit_non_executable_reason: str

    @property
    def exit_executable(self) -> bool:
        return bool(self.executable_qty > DUST_POSITION_EPS)


def _dust_state_from_legacy_value(raw: str | DustState | None) -> str:
    normalized = str(raw or "").strip().lower()
    if normalized in {DustState.NO_DUST.value, "none", "no_dust_residual"}:
        return DustState.NO_DUST.value
    if normalized in {
        DustState.HARMLESS_DUST.value,
        "matched_harmless_dust",
        "matched_harmless_dust_resume_allowed",
        "matched_harmless_dust_operator_review_required",
    }:
        return DustState.HARMLESS_DUST.value
    if normalized in {
        DustState.BLOCKING_DUST.value,
        "dangerous_dust",
        "dangerous_dust_operator_review_required",
    }:
        return DustState.BLOCKING_DUST.value
    return normalized or DustState.NO_DUST.value


def _dust_state_label(state: str) -> str:
    if state == DustState.NO_DUST.value:
        return "no dust residual"
    if state == DustState.HARMLESS_DUST.value:
        return "harmless dust residual"
    if state == DustState.BLOCKING_DUST.value:
        return "blocking dust residual requires manual review"
    return "unknown dust residual"


def _dust_operator_action(state: str, *, allow_resume: bool) -> str:
    if state == DustState.NO_DUST.value:
        return "none"
    if state == DustState.HARMLESS_DUST.value:
        return (
            "harmless_dust_tracked_resume_allowed"
            if allow_resume
            else "harmless_dust_review_required"
        )
    return "manual_review_before_resume"


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

    @property
    def dust_state(self) -> str:
        return self.classification

    @property
    def state_label(self) -> str:
        return _dust_state_label(self.classification)

    def to_raw_holdings(self) -> "RawHoldingsSnapshot":
        return RawHoldingsSnapshot(
            classification=self.classification,
            present=self.present,
            broker_qty=self.broker_qty,
            local_qty=self.local_qty,
            delta_qty=self.delta_qty,
            min_qty=self.min_qty,
            min_notional_krw=self.min_notional_krw,
            latest_price=self.latest_price,
            broker_notional_krw=self.broker_notional_krw,
            local_notional_krw=self.local_notional_krw,
            partial_flatten_recent=self.partial_flatten_recent,
            partial_flatten_reason=self.partial_flatten_reason,
            qty_gap_tolerance=self.qty_gap_tolerance,
            qty_gap_small=self.qty_gap_small,
            broker_qty_is_dust=self.broker_qty_is_dust,
            local_qty_is_dust=self.local_qty_is_dust,
            broker_notional_is_dust=self.broker_notional_is_dust,
            local_notional_is_dust=self.local_notional_is_dust,
        )

    def to_metadata(self) -> dict[str, int | float | str]:
        return {
            "dust_state": self.classification,
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
        classification = _dust_state_from_legacy_value(
            metadata.get("dust_state")
            or metadata.get("dust_classification")
            or summary_values.get("classification")
            or _classification_from_policy_reason(str(metadata.get("dust_policy_reason") or ""))
            or inferred_classification
            or DustState.NO_DUST.value
        )
        if effective_flat_raw is None and not bool(summary_values.get("effective_flat")):
            effective_flat = bool(classification in {DustState.NO_DUST.value, DustState.HARMLESS_DUST.value})
        allow_resume = bool(
            classification == DustState.HARMLESS_DUST.value
            and effective_flat
            and unresolved_open_order_count == 0
            and submit_unknown_count == 0
            and recovery_required_count == 0
        )
        if not present:
            policy_reason = str(metadata.get("dust_policy_reason") or "none")
        elif classification == DustState.HARMLESS_DUST.value and allow_resume:
            policy_reason = "matched_harmless_dust_resume_allowed"
        elif classification == DustState.HARMLESS_DUST.value:
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
class RawHoldingsSnapshot:
    classification: str
    present: bool
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

    @property
    def state(self) -> str:
        return self.classification

    @property
    def broker_local_match(self) -> bool:
        return bool(self.qty_gap_small)

    @property
    def compact_summary(self) -> str:
        return (
            f"state={self.state} "
            f"broker_qty={self.broker_qty:.8f} "
            f"local_qty={self.local_qty:.8f} "
            f"delta_qty={self.delta_qty:.8f} "
            f"min_qty={self.min_qty:.8f} "
            f"min_notional_krw={self.min_notional_krw:.1f} "
            f"qty_below_min(broker={1 if self.broker_qty_is_dust else 0} local={1 if self.local_qty_is_dust else 0}) "
            f"notional_below_min(broker={1 if self.broker_notional_is_dust else 0} local={1 if self.local_notional_is_dust else 0}) "
            f"broker_local_match={1 if self.broker_local_match else 0}"
        )

    def as_dict(self) -> dict[str, bool | float | str]:
        return {
            "classification": self.classification,
            "present": bool(self.present),
            "broker_qty": float(self.broker_qty),
            "local_qty": float(self.local_qty),
            "delta_qty": float(self.delta_qty),
            "min_qty": float(self.min_qty),
            "min_notional_krw": float(self.min_notional_krw),
            "latest_price": self.latest_price if self.latest_price is not None else "",
            "broker_notional_krw": (
                self.broker_notional_krw if self.broker_notional_krw is not None else ""
            ),
            "local_notional_krw": self.local_notional_krw if self.local_notional_krw is not None else "",
            "partial_flatten_recent": bool(self.partial_flatten_recent),
            "partial_flatten_reason": self.partial_flatten_reason,
            "qty_gap_tolerance": float(self.qty_gap_tolerance),
            "qty_gap_small": bool(self.qty_gap_small),
            "broker_qty_is_dust": bool(self.broker_qty_is_dust),
            "local_qty_is_dust": bool(self.local_qty_is_dust),
            "broker_notional_is_dust": bool(self.broker_notional_is_dust),
            "local_notional_is_dust": bool(self.local_notional_is_dust),
            "broker_local_match": bool(self.broker_local_match),
            "compact_summary": self.compact_summary,
        }


@dataclass(frozen=True)
class DustDisplayContext:
    classification: DustClassification
    raw_holdings: RawHoldingsSnapshot
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

    @property
    def effective_flat_due_to_harmless_dust(self) -> bool:
        return bool(
            self.classification.present
            and self.classification.classification == DustState.HARMLESS_DUST.value
            and self.operator_view.resume_allowed
            and self.operator_view.treat_as_flat
        )


@dataclass(frozen=True)
class _NormalizedPositionInventory:
    raw_qty_open: float
    raw_total_asset_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    reserved_exit_qty: float


@dataclass(frozen=True)
class _ExecutableExposureDerivation:
    executable_lot: ExecutableLot
    effective_open_exposure_qty: float
    effective_reserved_exit_qty: float
    sellable_executable_qty: float
    normalized_dust_tracking_qty: float


@dataclass(frozen=True)
class PositionStateInterpretation:
    lifecycle_state: str
    lifecycle_label: str
    operator_outcome: str
    operator_message: str
    entry_status: str
    exit_status: str
    exit_submit_expected: bool

    def as_dict(self) -> dict[str, bool | str]:
        return {
            "lifecycle_state": self.lifecycle_state,
            "lifecycle_label": self.lifecycle_label,
            "operator_outcome": self.operator_outcome,
            "operator_message": self.operator_message,
            "entry_status": self.entry_status,
            "exit_status": self.exit_status,
            "exit_submit_expected": bool(self.exit_submit_expected),
        }


@dataclass(frozen=True)
class NormalizedExposure:
    raw_qty_open: float
    raw_total_asset_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    reserved_exit_qty: float
    open_lot_count: int
    dust_tracking_lot_count: int
    reserved_exit_lot_count: int
    sellable_executable_lot_count: int
    sellable_executable_qty: float
    effective_min_trade_qty: float
    exit_non_executable_reason: str
    dust_context: DustDisplayContext | DustClassification | str | dict[str, object] | None
    effective_flat: bool
    entry_allowed: bool
    entry_block_reason: str
    exit_allowed: bool
    exit_block_reason: str
    terminal_state: str
    normalized_exposure_active: bool
    has_executable_exposure: bool
    has_any_position_residue: bool
    has_non_executable_residue: bool
    has_dust_only_remainder: bool
    normalized_exposure_qty: float

    @property
    def dust_classification(self) -> str:
        if isinstance(self.dust_context, DustDisplayContext):
            return self.dust_context.classification.classification
        if isinstance(self.dust_context, DustClassification):
            return self.dust_context.classification
        return DustClassification.from_metadata(self.dust_context).classification

    @property
    def dust_state(self) -> str:
        if isinstance(self.dust_context, DustDisplayContext):
            return self.dust_context.operator_view.state
        if isinstance(self.dust_context, DustClassification):
            return self.dust_context.classification
        return DustClassification.from_metadata(self.dust_context).classification

    @property
    def dust_operator_view(self) -> DustOperatorView:
        if isinstance(self.dust_context, DustDisplayContext):
            return self.dust_context.operator_view
        if isinstance(self.dust_context, DustClassification):
            return build_dust_operator_view(self.dust_context)
        return build_dust_operator_view(self.dust_context)

    @property
    def raw_holdings(self) -> RawHoldingsSnapshot:
        if isinstance(self.dust_context, DustDisplayContext):
            return self.dust_context.raw_holdings
        if isinstance(self.dust_context, DustClassification):
            return self.dust_context.to_raw_holdings()
        return DustClassification.from_metadata(self.dust_context).to_raw_holdings()

    @property
    def harmless_dust_effective_flat(self) -> bool:
        return self.dust_context.effective_flat_due_to_harmless_dust

    @property
    def sell_submit_qty(self) -> float:
        """Return the canonical SELL submission quantity basis from shared state."""

        return float(self.sellable_executable_qty)

    @property
    def submit_lot_count(self) -> int:
        """Return the canonical lot-count basis for order submission."""

        return int(self.sellable_executable_lot_count)

    @property
    def sell_submit_qty_source(self) -> str:
        return lot_state_sell_submit_qty_source(OPEN_EXPOSURE_LOT_STATE)

    @property
    def sell_submit_lot_source(self) -> str:
        return lot_state_sell_submit_lot_source(OPEN_EXPOSURE_LOT_STATE)

    @property
    def semantic_basis(self) -> str:
        return "lot-native"

    def as_dict(self) -> dict[str, bool | float | str]:
        return {
            "semantic_basis": self.semantic_basis,
            "raw_qty_open": float(self.raw_qty_open),
            "raw_total_asset_qty": float(self.raw_total_asset_qty),
            "total_holdings_qty": float(self.raw_total_asset_qty),
            "open_exposure_qty": float(self.open_exposure_qty),
            "executable_exposure_qty": float(self.open_exposure_qty),
            "open_exposure_lot_count": int(self.open_lot_count),
            "dust_tracking_qty": float(self.dust_tracking_qty),
            "dust_remainder_lot_count": int(self.dust_tracking_lot_count),
            "reserved_exit_qty": float(self.reserved_exit_qty),
            "open_lot_count": int(self.open_lot_count),
            "dust_tracking_lot_count": int(self.dust_tracking_lot_count),
            "reserved_exit_lot_count": int(self.reserved_exit_lot_count),
            "sellable_executable_lot_count": int(self.sellable_executable_lot_count),
            "submit_lot_count": int(self.submit_lot_count),
            "position_state_lot_count": int(self.submit_lot_count),
            "sellable_executable_qty": float(self.sellable_executable_qty),
            "effective_min_trade_qty": float(self.effective_min_trade_qty),
            "exit_non_executable_reason": self.exit_non_executable_reason,
            "dust_classification": self.dust_classification,
            "dust_state": self.dust_state,
            "effective_flat": bool(self.effective_flat),
            "entry_allowed": bool(self.entry_allowed),
            "entry_block_reason": self.entry_block_reason,
            "exit_allowed": bool(self.exit_allowed),
            "exit_block_reason": self.exit_block_reason,
            "terminal_state": self.terminal_state,
            "harmless_dust_effective_flat": bool(self.harmless_dust_effective_flat),
            "effective_flat_due_to_harmless_dust": bool(self.harmless_dust_effective_flat),
            "normalized_exposure_active": bool(self.normalized_exposure_active),
            "has_executable_exposure": bool(self.has_executable_exposure),
            "has_any_position_residue": bool(self.has_any_position_residue),
            "has_non_executable_residue": bool(self.has_non_executable_residue),
            "has_dust_only_remainder": bool(self.has_dust_only_remainder),
            "normalized_exposure_qty": float(self.normalized_exposure_qty),
            "executable_exposure_qty": float(self.normalized_exposure_qty),
            "submit_lot_source": self.sell_submit_lot_source,
            "position_state_lot_source": self.sell_submit_lot_source,
            "dust_new_orders_allowed": bool(self.dust_operator_view.new_orders_allowed),
            "dust_resume_allowed": bool(self.dust_operator_view.resume_allowed),
            "dust_treat_as_flat": bool(self.dust_operator_view.treat_as_flat),
            "sell_submit_qty": float(self.sell_submit_qty),
            "sell_submit_qty_source": self.sell_submit_qty_source,
            "sell_submit_lot_source": self.sell_submit_lot_source,
        }


@dataclass(frozen=True)
class PositionStateModel:
    raw_holdings: RawHoldingsSnapshot
    normalized_exposure: NormalizedExposure
    operator_diagnostics: DustOperatorView
    state_interpretation: PositionStateInterpretation
    fields: dict[str, object]

    @property
    def raw_qty_open(self) -> float:
        return float(self.normalized_exposure.raw_qty_open)

    @property
    def effective_flat(self) -> bool:
        return bool(self.normalized_exposure.effective_flat)

    @property
    def effective_flat_due_to_harmless_dust(self) -> bool:
        return bool(self.normalized_exposure.harmless_dust_effective_flat)

    @property
    def semantic_basis(self) -> str:
        return "lot-native"

    def as_dict(self) -> dict[str, object]:
        return {
            "semantic_basis": self.semantic_basis,
            "raw_holdings": self.raw_holdings.as_dict(),
            "normalized_exposure": self.normalized_exposure.as_dict(),
            "operator_diagnostics": {
                "state": self.operator_diagnostics.state,
                "state_label": self.operator_diagnostics.state_label,
                "operator_action": self.operator_diagnostics.operator_action,
                "operator_message": self.operator_diagnostics.operator_message,
                "broker_local_match": bool(self.operator_diagnostics.broker_local_match),
                "new_orders_allowed": bool(self.operator_diagnostics.new_orders_allowed),
                "resume_allowed": bool(self.operator_diagnostics.resume_allowed),
                "treat_as_flat": bool(self.operator_diagnostics.treat_as_flat),
                "compact_summary": self.operator_diagnostics.compact_summary,
            },
            "state_interpretation": self.state_interpretation.as_dict(),
            "raw_qty_open": float(self.raw_qty_open),
            "raw_total_asset_qty": float(self.normalized_exposure.raw_total_asset_qty),
            "open_exposure_lot_count": int(self.normalized_exposure.open_lot_count),
            "dust_remainder_lot_count": int(self.normalized_exposure.dust_tracking_lot_count),
            "sellable_executable_lot_count": int(self.normalized_exposure.sellable_executable_lot_count),
            "submit_lot_count": int(self.normalized_exposure.submit_lot_count),
            "position_state_lot_count": int(self.normalized_exposure.submit_lot_count),
            "submit_lot_source": self.normalized_exposure.sell_submit_lot_source,
            "position_state_lot_source": self.normalized_exposure.sell_submit_lot_source,
            "effective_flat": bool(self.effective_flat),
            "effective_flat_due_to_harmless_dust": bool(self.effective_flat_due_to_harmless_dust),
            "fields": dict(self.fields),
        }


def lot_state_quantity_contract() -> dict[str, dict[str, object]]:
    """Return the canonical quantity routing rules for lot states.

    `open_exposure` is the real strategy-visible position and the default SELL
    submission base. `dust_tracking` is operator-only residual evidence and is
    excluded from normal SELL submission by default.
    """

    return {state: dict(contract) for state, contract in LOT_STATE_QUANTITY_CONTRACT.items()}


def lot_state_strategy_qty_source(position_state: str) -> str:
    return str(lot_state_quantity_rule(position_state)["strategy_qty_source"])


def lot_state_strategy_lot_source(position_state: str) -> str:
    return str(lot_state_quantity_rule(position_state)["strategy_lot_source"])


def lot_state_sell_submit_qty_source(position_state: str) -> str:
    return str(lot_state_quantity_rule(position_state)["sell_submit_qty_source"])


def lot_state_sell_submit_lot_source(position_state: str) -> str:
    return str(lot_state_quantity_rule(position_state)["sell_submit_lot_source"])


def lot_state_sell_submission_allowed(position_state: str) -> bool:
    return bool(lot_state_quantity_rule(position_state)["sell_submission_allowed"])


def lot_state_sell_submit_includes_dust_tracking(position_state: str) -> bool:
    return bool(lot_state_quantity_rule(position_state)["sell_submit_includes_dust_tracking"])


def lot_state_qty_boundary_rule(position_state: str) -> str:
    return str(lot_state_quantity_rule(position_state)["qty_boundary_rule"])


def lot_state_quantity_rule(position_state: str) -> dict[str, object]:
    normalized_state = str(position_state or "").strip()
    contract = LOT_STATE_QUANTITY_CONTRACT.get(normalized_state)
    if contract is None:
        return {
            "meaning": "unknown lot state",
            "strategy_qty_source": "unknown",
            "strategy_lot_source": "unknown",
            "sell_submit_qty_source": "excluded_from_sell_qty",
            "sell_submit_lot_source": "excluded_from_sell_lot_count",
            "sell_submission_allowed": False,
            "sell_submit_includes_dust_tracking": False,
            "qty_boundary_rule": "unknown lot state is not sellable",
            "operator_tracking_only": False,
        }
    return dict(contract)


def is_strictly_below_min_qty(*, qty_open: float, min_qty: float) -> bool:
    """Return True when a positive residual is strictly below the tradable minimum.

    The boundary is intentionally strict: `qty_open == min_qty` stays in
    `open_exposure`, while `qty_open < min_qty` may be reclassified as
    `dust_tracking`.
    """

    normalized_qty = max(0.0, float(qty_open))
    normalized_min_qty = max(0.0, float(min_qty))
    return bool(
        normalized_qty > DUST_POSITION_EPS
        and normalized_min_qty > 0.0
        and normalized_qty < normalized_min_qty
    )


def _effective_qty_step(*, qty_step: float, min_qty: float, max_qty_decimals: int | None) -> float:
    normalized_step = max(0.0, float(qty_step))
    if normalized_step > 0.0:
        return normalized_step
    normalized_min_qty = max(0.0, float(min_qty))
    if normalized_min_qty > 0.0:
        return normalized_min_qty
    decimals = max(0, int(max_qty_decimals or 0))
    if decimals > 0:
        return 1.0 / (10 ** decimals)
    return 0.0


def _decimal_from_number(value: float | int | str | None, *, default: Decimal = _DECIMAL_ZERO) -> Decimal:
    if value is None:
        return default
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default
    if not parsed.is_finite():
        return default
    return parsed


def _decimal_quantizer(*, max_qty_decimals: int | None) -> Decimal | None:
    decimals = max(0, int(max_qty_decimals or 0))
    if decimals <= 0:
        return None
    return Decimal("1").scaleb(-decimals)


def _effective_qty_step_decimal(*, qty_step: float, min_qty: float, max_qty_decimals: int | None) -> Decimal:
    step = _decimal_from_number(qty_step)
    if step > 0:
        return step
    minimum = _decimal_from_number(min_qty)
    if minimum > 0:
        return minimum
    quantizer = _decimal_quantizer(max_qty_decimals=max_qty_decimals)
    if quantizer is not None:
        return quantizer
    return _DECIMAL_ZERO


def _floor_qty_to_step(*, qty: float, qty_step: float, max_qty_decimals: int | None) -> float:
    normalized_qty = max(_DECIMAL_ZERO, _decimal_from_number(qty))
    step = _effective_qty_step_decimal(qty_step=qty_step, min_qty=0.0, max_qty_decimals=max_qty_decimals)
    if step > 0:
        normalized_qty = (normalized_qty / step).to_integral_value(rounding=ROUND_FLOOR) * step
    quantizer = _decimal_quantizer(max_qty_decimals=max_qty_decimals)
    if quantizer is not None:
        normalized_qty = normalized_qty.quantize(quantizer, rounding=ROUND_FLOOR)
    return max(0.0, float(normalized_qty))


def _ceil_qty_to_step(*, qty: float, qty_step: float, max_qty_decimals: int | None) -> float:
    normalized_qty = max(_DECIMAL_ZERO, _decimal_from_number(qty))
    if normalized_qty <= 0:
        return 0.0
    step = _effective_qty_step_decimal(qty_step=qty_step, min_qty=0.0, max_qty_decimals=max_qty_decimals)
    if step > 0:
        normalized_qty = (normalized_qty / step).to_integral_value(rounding=ROUND_CEILING) * step
    quantizer = _decimal_quantizer(max_qty_decimals=max_qty_decimals)
    if quantizer is not None:
        normalized_qty = normalized_qty.quantize(quantizer, rounding=ROUND_CEILING)
    return max(0.0, float(normalized_qty))


def build_executable_lot(
    *,
    qty: float,
    market_price: float | None,
    min_qty: float,
    qty_step: float,
    min_notional_krw: float,
    max_qty_decimals: int | None = None,
    exit_fee_ratio: float = 0.0,
    exit_slippage_bps: float = 0.0,
    exit_buffer_ratio: float = 0.0,
) -> ExecutableLot:
    raw_qty_decimal = max(_DECIMAL_ZERO, _decimal_from_number(qty))
    raw_qty = float(raw_qty_decimal)
    normalized_min_qty_decimal = max(_DECIMAL_ZERO, _decimal_from_number(min_qty))
    normalized_min_notional_decimal = max(_DECIMAL_ZERO, _decimal_from_number(min_notional_krw))
    normalized_exit_fee_ratio_decimal = max(_DECIMAL_ZERO, _decimal_from_number(exit_fee_ratio))
    normalized_exit_slippage_ratio_decimal = max(_DECIMAL_ZERO, _decimal_from_number(exit_slippage_bps)) / Decimal("10000")
    normalized_exit_buffer_ratio_decimal = max(_DECIMAL_ZERO, _decimal_from_number(exit_buffer_ratio))
    step_decimal = _effective_qty_step_decimal(
        qty_step=qty_step,
        min_qty=float(normalized_min_qty_decimal),
        max_qty_decimals=max_qty_decimals,
    )
    step = float(step_decimal)
    buffered_min_qty = float(normalized_min_qty_decimal)
    if buffered_min_qty > 0.0 and step > 0.0:
        buffered_min_qty = _ceil_qty_to_step(
            qty=(buffered_min_qty + step),
            qty_step=step,
            max_qty_decimals=max_qty_decimals,
        )
    exit_price_floor: float | None = None
    if market_price is not None:
        normalized_price_decimal = max(_DECIMAL_ZERO, _decimal_from_number(market_price))
        if normalized_price_decimal > 0:
            exit_price_ratio = max(
                _DECIMAL_ZERO,
                Decimal("1")
                - normalized_exit_fee_ratio_decimal
                - normalized_exit_slippage_ratio_decimal
                - normalized_exit_buffer_ratio_decimal,
            )
            exit_price_floor = float(normalized_price_decimal * exit_price_ratio)
    notional_min_qty = 0.0
    if float(normalized_min_notional_decimal) > 0.0 and exit_price_floor is not None and exit_price_floor > 0.0:
        notional_min_qty = _ceil_qty_to_step(
            qty=(float(normalized_min_notional_decimal) / exit_price_floor),
            qty_step=step,
            max_qty_decimals=max_qty_decimals,
        )
    effective_min_trade_qty = max(buffered_min_qty, notional_min_qty)
    executable_qty = _floor_qty_to_step(
        qty=raw_qty,
        qty_step=step,
        max_qty_decimals=max_qty_decimals,
    )
    dust_qty = max(0.0, raw_qty - executable_qty)
    exit_non_executable_reason = "executable"
    if executable_qty <= DUST_POSITION_EPS:
        dust_qty = raw_qty
        if raw_qty <= DUST_POSITION_EPS:
            exit_non_executable_reason = "no_position"
        elif effective_min_trade_qty > 0.0 and raw_qty < effective_min_trade_qty:
            exit_non_executable_reason = "no_executable_exit_lot"
        else:
            exit_non_executable_reason = "rounds_to_zero"
    elif effective_min_trade_qty > 0.0 and executable_qty < effective_min_trade_qty:
        executable_qty = 0.0
        dust_qty = raw_qty
        exit_non_executable_reason = "no_executable_exit_lot"
    return ExecutableLot(
        raw_qty=raw_qty,
        executable_qty=max(0.0, executable_qty),
        dust_qty=max(0.0, dust_qty),
        effective_min_trade_qty=max(0.0, effective_min_trade_qty),
        min_qty=float(normalized_min_qty_decimal),
        qty_step=max(0.0, float(step)),
        min_notional_krw=float(normalized_min_notional_decimal),
        exit_price_floor=exit_price_floor,
        exit_fee_ratio=float(normalized_exit_fee_ratio_decimal),
        exit_slippage_ratio=float(normalized_exit_slippage_ratio_decimal),
        exit_buffer_ratio=float(normalized_exit_buffer_ratio_decimal),
        exit_non_executable_reason=exit_non_executable_reason,
    )


def should_treat_as_flat_for_entry_gate(
    dust_context: DustDisplayContext | None,
) -> bool:
    """Return True when harmless dust should not block fresh BUY entries."""
    return bool(dust_context and dust_context.effective_flat_due_to_harmless_dust)


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
    broker_qty_is_dust = is_strictly_below_min_qty(
        qty_open=normalized_broker_qty,
        min_qty=normalized_min_qty,
    )
    local_qty_is_dust = is_strictly_below_min_qty(
        qty_open=normalized_local_qty,
        min_qty=normalized_min_qty,
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
        classification = (
            DustState.HARMLESS_DUST.value if matched_harmless else DustState.BLOCKING_DUST.value
        )

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
        f"harmless_dust={1 if matched_harmless else 0} "
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
        classification=DustState.NO_DUST.value,
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
    elif dust.classification == DustState.HARMLESS_DUST.value:
        state = DustState.HARMLESS_DUST.value
        state_label = _dust_state_label(state)
        if dust.allow_resume:
            operator_action = _dust_operator_action(state, allow_resume=True)
            operator_message = (
                "Broker/local matched dust remains tracked below minimum trade size, so it is not auto-liquidated. "
                "This residual is tracked only, effective-flat gating applies, and resume/new orders are allowed."
            )
            new_orders_allowed = True
            resume_allowed = True
        else:
            operator_action = _dust_operator_action(state, allow_resume=False)
            operator_message = (
                "Residual dust matches across broker/local state, but remains below minimum tradable quantity, so automatic resume and new orders stay blocked pending operator review."
            )
            new_orders_allowed = False
            resume_allowed = False
        treat_as_flat = True
    elif dust.present:
        state = DustState.BLOCKING_DUST.value
        state_label = _dust_state_label(state)
        operator_action = "manual_review_before_resume"
        operator_message = (
            "Dust residual is not harmless. Review broker/local mismatch or recovery concerns before resuming or placing new orders."
        )
        new_orders_allowed = False
        resume_allowed = False
        treat_as_flat = False
    else:
        state = DustState.NO_DUST.value
        state_label = _dust_state_label(state)
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
        raw_holdings=dust.to_raw_holdings(),
        operator_view=view,
        fields={
            "dust_classification": dust.classification,
            "dust_residual_present": bool(dust.present),
            "dust_residual_allow_resume": bool(dust.allow_resume),
            "dust_effective_flat": bool(dust.effective_flat),
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
            "effective_flat_due_to_harmless_dust": bool(
                dust.present
                and dust.classification == DustState.HARMLESS_DUST.value
                and view.resume_allowed
                and view.treat_as_flat
            ),
            "dust_broker_qty_below_min": bool(view.broker_qty_below_min),
            "dust_local_qty_below_min": bool(view.local_qty_below_min),
            "dust_broker_notional_below_min": bool(view.broker_notional_below_min),
            "dust_local_notional_below_min": bool(view.local_notional_below_min),
        },
    )


def build_position_state_model(
    *,
    raw_qty_open: float,
    metadata_raw: str | dict[str, object] | DustClassification | None,
    raw_total_asset_qty: float | None = None,
    open_exposure_qty: float | None = None,
    dust_tracking_qty: float | None = None,
    reserved_exit_qty: float | None = None,
    open_lot_count: int | None = None,
    dust_tracking_lot_count: int | None = None,
    market_price: float | None = None,
    min_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    max_qty_decimals: int | None = None,
    exit_fee_ratio: float = 0.0,
    exit_slippage_bps: float = 0.0,
    exit_buffer_ratio: float = 0.0,
) -> PositionStateModel:
    display_context = _resolve_position_display_context(metadata_raw=metadata_raw)
    normalized_exposure = _build_position_state_normalized_exposure(
        raw_qty_open=raw_qty_open,
        display_context=display_context,
        raw_total_asset_qty=raw_total_asset_qty,
        open_exposure_qty=open_exposure_qty,
        dust_tracking_qty=dust_tracking_qty,
        reserved_exit_qty=reserved_exit_qty,
        open_lot_count=open_lot_count,
        dust_tracking_lot_count=dust_tracking_lot_count,
        market_price=market_price,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        max_qty_decimals=max_qty_decimals,
        exit_fee_ratio=exit_fee_ratio,
        exit_slippage_bps=exit_slippage_bps,
        exit_buffer_ratio=exit_buffer_ratio,
    )
    state_interpretation = build_position_state_interpretation(normalized_exposure=normalized_exposure)
    return PositionStateModel(
        raw_holdings=display_context.raw_holdings,
        normalized_exposure=normalized_exposure,
        operator_diagnostics=display_context.operator_view,
        state_interpretation=state_interpretation,
        fields=_build_position_state_fields(
            display_context=display_context,
            normalized_exposure=normalized_exposure,
            state_interpretation=state_interpretation,
        ),
    )


def _resolve_position_display_context(
    *,
    metadata_raw: str | dict[str, object] | DustClassification | None,
) -> DustDisplayContext:
    dust = (
        metadata_raw
        if isinstance(metadata_raw, DustClassification)
        else DustClassification.from_metadata(metadata_raw)
    )
    return build_dust_display_context(dust)


def _build_position_state_normalized_exposure(
    *,
    raw_qty_open: float,
    display_context: DustDisplayContext,
    raw_total_asset_qty: float | None,
    open_exposure_qty: float | None,
    dust_tracking_qty: float | None,
    reserved_exit_qty: float | None,
    open_lot_count: int | None,
    dust_tracking_lot_count: int | None,
    market_price: float | None,
    min_qty: float | None,
    qty_step: float | None,
    min_notional_krw: float | None,
    max_qty_decimals: int | None,
    exit_fee_ratio: float,
    exit_slippage_bps: float,
    exit_buffer_ratio: float,
) -> NormalizedExposure:
    return build_normalized_exposure(
        raw_qty_open=raw_qty_open,
        dust_context=display_context,
        raw_total_asset_qty=raw_total_asset_qty,
        open_exposure_qty=open_exposure_qty,
        dust_tracking_qty=dust_tracking_qty,
        reserved_exit_qty=reserved_exit_qty,
        open_lot_count=open_lot_count,
        dust_tracking_lot_count=dust_tracking_lot_count,
        market_price=market_price,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        max_qty_decimals=max_qty_decimals,
        exit_fee_ratio=exit_fee_ratio,
        exit_slippage_bps=exit_slippage_bps,
        exit_buffer_ratio=exit_buffer_ratio,
    )


def _build_position_state_fields(
    *,
    display_context: DustDisplayContext,
    normalized_exposure: NormalizedExposure,
    state_interpretation: PositionStateInterpretation,
) -> dict[str, object]:
    return {
        **display_context.fields,
        **normalized_exposure.as_dict(),
        "raw_holdings": display_context.raw_holdings.as_dict(),
        "normalized_exposure": normalized_exposure.as_dict(),
        "operator_diagnostics": {
            "state": display_context.operator_view.state,
            "state_label": display_context.operator_view.state_label,
            "operator_action": display_context.operator_view.operator_action,
            "operator_message": display_context.operator_view.operator_message,
            "broker_local_match": bool(display_context.operator_view.broker_local_match),
            "new_orders_allowed": bool(display_context.operator_view.new_orders_allowed),
            "resume_allowed": bool(display_context.operator_view.resume_allowed),
            "treat_as_flat": bool(display_context.operator_view.treat_as_flat),
        },
        "state_interpretation": state_interpretation.as_dict(),
        "raw_qty_open": float(normalized_exposure.raw_qty_open),
        "raw_total_asset_qty": float(normalized_exposure.raw_total_asset_qty),
        "total_holdings_qty": float(normalized_exposure.raw_total_asset_qty),
        "open_exposure_qty": float(normalized_exposure.open_exposure_qty),
        "dust_tracking_qty": float(normalized_exposure.dust_tracking_qty),
        "reserved_exit_qty": float(normalized_exposure.reserved_exit_qty),
        "open_lot_count": int(normalized_exposure.open_lot_count),
        "dust_tracking_lot_count": int(normalized_exposure.dust_tracking_lot_count),
        "reserved_exit_lot_count": int(normalized_exposure.reserved_exit_lot_count),
        "sellable_executable_lot_count": int(normalized_exposure.sellable_executable_lot_count),
        "sellable_executable_qty": float(normalized_exposure.sellable_executable_qty),
    }


def build_normalized_exposure(
    *,
    raw_qty_open: float,
    dust_context: DustDisplayContext | DustClassification | str | dict[str, object] | None,
    raw_total_asset_qty: float | None = None,
    open_exposure_qty: float | None = None,
    dust_tracking_qty: float | None = None,
    reserved_exit_qty: float | None = None,
    open_lot_count: int | None = None,
    dust_tracking_lot_count: int | None = None,
    market_price: float | None = None,
    min_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    max_qty_decimals: int | None = None,
    exit_fee_ratio: float = 0.0,
    exit_slippage_bps: float = 0.0,
    exit_buffer_ratio: float = 0.0,
) -> NormalizedExposure:
    display_context = (
        dust_context
        if isinstance(dust_context, DustDisplayContext)
        else build_dust_display_context(dust_context)
    )
    lot_rules = build_market_lot_rules(
        market_id="unknown",
        market_price=market_price,
        rules=type(
            "_LotRules",
            (),
            {
                "market_id": "",
                "bid_min_total_krw": 0.0,
                "ask_min_total_krw": 0.0,
                "bid_price_unit": 0.0,
                "ask_price_unit": 0.0,
                "order_types": (),
                "order_sides": (),
                "bid_fee": 0.0,
                "ask_fee": 0.0,
                "maker_bid_fee": 0.0,
                "maker_ask_fee": 0.0,
                "min_qty": 0.0 if min_qty is None else float(min_qty),
                "qty_step": 0.0 if qty_step is None else float(qty_step),
                "min_notional_krw": 0.0 if min_notional_krw is None else float(min_notional_krw),
                "max_qty_decimals": 0 if max_qty_decimals is None else int(max_qty_decimals),
            },
        )(),
        exit_fee_ratio=exit_fee_ratio,
        exit_slippage_bps=exit_slippage_bps,
        exit_buffer_ratio=exit_buffer_ratio,
        source_mode="derived",
    )
    inventory = _normalize_position_inventory(
        raw_qty_open=raw_qty_open,
        raw_total_asset_qty=raw_total_asset_qty,
        open_exposure_qty=open_exposure_qty,
        dust_tracking_qty=dust_tracking_qty,
        reserved_exit_qty=reserved_exit_qty,
    )
    normalized_min_qty = 0.0 if min_qty is None else max(0.0, float(min_qty))
    executable_exposure = _derive_executable_open_exposure(
        inventory=inventory,
        market_price=market_price,
        min_qty=normalized_min_qty,
        qty_step=0.0 if qty_step is None else float(qty_step),
        min_notional_krw=0.0 if min_notional_krw is None else float(min_notional_krw),
        max_qty_decimals=max_qty_decimals,
        exit_fee_ratio=exit_fee_ratio,
        exit_slippage_bps=exit_slippage_bps,
        exit_buffer_ratio=exit_buffer_ratio,
    )
    normalized_open_lot_count = max(0, int(open_lot_count or 0))
    normalized_dust_lot_count = max(0, int(dust_tracking_lot_count or 0))
    lot_open_exposure_qty = lot_count_to_qty(
        lot_count=normalized_open_lot_count,
        lot_size=float(lot_rules.lot_size),
    )
    lot_dust_tracking_qty = lot_count_to_qty(
        lot_count=normalized_dust_lot_count,
        lot_size=float(lot_rules.lot_size),
    )
    effective_open_exposure_qty = max(0.0, float(executable_exposure.effective_open_exposure_qty))
    if effective_open_exposure_qty <= DUST_POSITION_EPS and normalized_open_lot_count > 0:
        effective_open_exposure_qty = lot_open_exposure_qty
    normalized_dust_tracking_qty = max(0.0, float(executable_exposure.normalized_dust_tracking_qty))
    if normalized_dust_tracking_qty <= DUST_POSITION_EPS and normalized_dust_lot_count > 0:
        normalized_dust_tracking_qty = lot_dust_tracking_qty
    if (
        normalized_dust_tracking_qty <= DUST_POSITION_EPS
        and normalized_open_lot_count <= 0
        and float(inventory.raw_total_asset_qty) > DUST_POSITION_EPS
    ):
        normalized_dust_tracking_qty = float(inventory.raw_total_asset_qty)
    normalized_total_asset_qty = max(
        0.0,
        float(inventory.raw_total_asset_qty)
        if float(inventory.raw_total_asset_qty) > DUST_POSITION_EPS
        else effective_open_exposure_qty + normalized_dust_tracking_qty,
    )
    effective_reserved_exit_qty = min(
        effective_open_exposure_qty,
        max(0.0, float(inventory.reserved_exit_qty)),
    )
    normalized_reserved_exit_lot_count = max(
        0,
        int(qty_to_executable_lot_count(qty=effective_reserved_exit_qty, lot_rules=lot_rules)),
    )
    normalized_sellable_lot_count = max(0, normalized_open_lot_count - normalized_reserved_exit_lot_count)
    sellable_executable_qty = max(0.0, effective_open_exposure_qty - effective_reserved_exit_qty)
    has_any_position_residue = bool(normalized_total_asset_qty > DUST_POSITION_EPS)
    entry_allowed = bool(
        normalized_total_asset_qty <= DUST_POSITION_EPS
        or should_treat_as_flat_for_entry_gate(display_context)
    )
    effective_flat = bool(normalized_total_asset_qty <= DUST_POSITION_EPS or entry_allowed)
    # `normalized_exposure_active` remains the broader lifecycle flag used by
    # restart/reconcile and operator-facing state summaries. It stays true for
    # active open lots or reserved exit inventory, while the explicit
    # executable-exposure flags below distinguish what can be traded normally.
    normalized_active = bool(normalized_open_lot_count > 0 or normalized_reserved_exit_lot_count > 0)
    has_executable_exposure = bool(normalized_sellable_lot_count > 0)
    has_non_executable_residue = bool(has_any_position_residue and not has_executable_exposure)
    has_dust_only_remainder = bool(normalized_dust_tracking_qty > DUST_POSITION_EPS and normalized_open_lot_count <= 0)
    normalized_qty = float(sellable_executable_qty if has_executable_exposure else 0.0)
    if entry_allowed:
        entry_block_reason = "none"
    elif normalized_total_asset_qty > DUST_POSITION_EPS:
        entry_block_reason = (
            "legacy_lot_metadata_missing"
            if normalized_open_lot_count <= 0 and not has_executable_exposure
            else "position_has_executable_exposure"
        )
    else:
        entry_block_reason = "none"
    if has_executable_exposure:
        exit_allowed = True
        exit_block_reason = "none"
        terminal_state = "open_exposure"
    elif normalized_open_lot_count > 0 and effective_reserved_exit_qty > DUST_POSITION_EPS:
        exit_allowed = False
        exit_block_reason = "reserved_for_open_sell_orders"
        terminal_state = "reserved_exit_pending"
    elif normalized_open_lot_count <= 0 and normalized_dust_tracking_qty > DUST_POSITION_EPS:
        exit_allowed = False
        exit_block_reason = "dust_only_remainder"
        terminal_state = "dust_only"
    elif normalized_total_asset_qty <= DUST_POSITION_EPS:
        exit_allowed = False
        exit_block_reason = "no_position"
        terminal_state = "flat"
    else:
        exit_allowed = False
        exit_block_reason = (
            "legacy_lot_metadata_missing"
            if normalized_open_lot_count <= 0
            else str(executable_exposure.executable_lot.exit_non_executable_reason or "no_executable_exit_lot")
        )
        terminal_state = "non_executable_position"
    return NormalizedExposure(
        raw_qty_open=inventory.raw_qty_open,
        raw_total_asset_qty=normalized_total_asset_qty,
        open_exposure_qty=effective_open_exposure_qty,
        dust_tracking_qty=normalized_dust_tracking_qty,
        reserved_exit_qty=effective_reserved_exit_qty,
        open_lot_count=normalized_open_lot_count,
        dust_tracking_lot_count=normalized_dust_lot_count,
        reserved_exit_lot_count=normalized_reserved_exit_lot_count,
        sellable_executable_lot_count=normalized_sellable_lot_count,
        sellable_executable_qty=sellable_executable_qty,
        effective_min_trade_qty=float(executable_exposure.executable_lot.effective_min_trade_qty),
        exit_non_executable_reason=str(executable_exposure.executable_lot.exit_non_executable_reason),
        dust_context=display_context,
        effective_flat=effective_flat,
        entry_allowed=entry_allowed,
        entry_block_reason=entry_block_reason,
        exit_allowed=exit_allowed,
        exit_block_reason=exit_block_reason,
        terminal_state=terminal_state,
        normalized_exposure_active=normalized_active,
        has_executable_exposure=has_executable_exposure,
        has_any_position_residue=has_any_position_residue,
        has_non_executable_residue=has_non_executable_residue,
        has_dust_only_remainder=has_dust_only_remainder,
        normalized_exposure_qty=normalized_qty,
    )


def build_position_state_interpretation(
    *,
    normalized_exposure: NormalizedExposure,
) -> PositionStateInterpretation:
    terminal_state = str(normalized_exposure.terminal_state or "unknown")
    lifecycle_label_map = {
        "flat": "flat position",
        "open_exposure": "open executable exposure",
        "reserved_exit_pending": "exit inventory reserved by open sell orders",
        "dust_only": "tracked unsellable residual",
        "non_executable_position": "non-executable open exposure",
    }
    operator_outcome_map = {
        "flat": "flat_no_position",
        "open_exposure": "executable_open_exposure",
        "reserved_exit_pending": "reserved_exit_pending",
        "dust_only": "tracked_unsellable_residual",
        "non_executable_position": "non_executable_open_exposure",
    }
    operator_message_map = {
        "flat": "No position remains in the shared state model.",
        "open_exposure": "Executable open exposure remains available as sellable lots for a normal SELL path.",
        "reserved_exit_pending": "Executable exposure exists, but the sellable lots are already reserved by open SELL orders.",
        "dust_only": "Residual holdings are tracked as dust lots at the state layer, so exit is a HOLD/no-submit outcome rather than a submit failure.",
        "non_executable_position": "Residual open exposure remains in state, but exchange constraints make the lots non-executable until operator review or state changes.",
    }
    entry_status = (
        "allowed"
        if normalized_exposure.entry_allowed
        else f"blocked:{normalized_exposure.entry_block_reason}"
    )
    exit_status = (
        "allowed"
        if normalized_exposure.exit_allowed
        else f"blocked:{normalized_exposure.exit_block_reason}"
    )
    return PositionStateInterpretation(
        lifecycle_state=terminal_state,
        lifecycle_label=lifecycle_label_map.get(terminal_state, terminal_state.replace("_", " ")),
        operator_outcome=operator_outcome_map.get(terminal_state, terminal_state),
        operator_message=operator_message_map.get(
            terminal_state,
            "Shared state requires operator review before execution can continue.",
        ),
        entry_status=entry_status,
        exit_status=exit_status,
        exit_submit_expected=bool(normalized_exposure.exit_allowed),
    )


def _normalize_position_inventory(
    *,
    raw_qty_open: float,
    raw_total_asset_qty: float | None,
    open_exposure_qty: float | None,
    dust_tracking_qty: float | None,
    reserved_exit_qty: float | None,
) -> _NormalizedPositionInventory:
    normalized_raw_qty = max(0.0, float(raw_qty_open))
    normalized_total_asset_qty = (
        max(0.0, float(raw_total_asset_qty))
        if raw_total_asset_qty is not None
        else normalized_raw_qty
    )
    normalized_dust_tracking_qty = (
        max(0.0, float(dust_tracking_qty))
        if dust_tracking_qty is not None
        else 0.0
    )
    return _NormalizedPositionInventory(
        raw_qty_open=normalized_raw_qty,
        raw_total_asset_qty=normalized_total_asset_qty,
        open_exposure_qty=(
            max(0.0, float(open_exposure_qty))
            if open_exposure_qty is not None
            else 0.0
        ),
        dust_tracking_qty=normalized_dust_tracking_qty,
        reserved_exit_qty=(
            max(0.0, float(reserved_exit_qty))
            if reserved_exit_qty is not None
            else 0.0
        ),
    )


def _derive_executable_open_exposure(
    *,
    inventory: _NormalizedPositionInventory,
    market_price: float | None,
    min_qty: float,
    qty_step: float,
    min_notional_krw: float,
    max_qty_decimals: int | None,
    exit_fee_ratio: float,
    exit_slippage_bps: float,
    exit_buffer_ratio: float,
) -> _ExecutableExposureDerivation:
    executable_lot = build_executable_lot(
        qty=inventory.open_exposure_qty,
        market_price=market_price,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        max_qty_decimals=max_qty_decimals,
        exit_fee_ratio=exit_fee_ratio,
        exit_slippage_bps=exit_slippage_bps,
        exit_buffer_ratio=exit_buffer_ratio,
    )
    effective_open_exposure_qty = max(0.0, executable_lot.executable_qty)
    effective_reserved_exit_qty = min(
        effective_open_exposure_qty,
        inventory.reserved_exit_qty,
    )
    sellable_executable_qty = max(
        0.0,
        effective_open_exposure_qty - effective_reserved_exit_qty,
    )
    normalized_dust_tracking_qty = max(
        0.0,
        inventory.dust_tracking_qty + executable_lot.dust_qty,
    )
    return _ExecutableExposureDerivation(
        executable_lot=executable_lot,
        effective_open_exposure_qty=effective_open_exposure_qty,
        effective_reserved_exit_qty=effective_reserved_exit_qty,
        sellable_executable_qty=sellable_executable_qty,
        normalized_dust_tracking_qty=normalized_dust_tracking_qty,
    )


def _metadata_fallback(*, policy_reason: str) -> DustClassification:
    return DustClassification(
        classification=DustState.NO_DUST.value,
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
    if normalized.startswith("matched_harmless_dust_") or normalized.startswith("harmless_dust_"):
        return DustState.HARMLESS_DUST.value
    if normalized.startswith("dangerous_dust_") or normalized.startswith("blocking_dust_"):
        return DustState.BLOCKING_DUST.value
    if normalized in {"no_dust_residual", DustState.NO_DUST.value, "none"}:
        return DustState.NO_DUST.value
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
        return DustState.NO_DUST.value
    matched_harmless = bool(
        broker_qty_is_dust
        and local_qty_is_dust
        and qty_gap_small
    )
    return DustState.HARMLESS_DUST.value if matched_harmless else DustState.BLOCKING_DUST.value
