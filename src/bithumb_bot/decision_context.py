from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .reason_codes import (
    SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
    SELL_FAILURE_CATEGORY_DUST_SUPPRESSION,
    SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
    SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD,
    SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH,
)


_CANONICAL_CONTEXT_VERSION = 12
_COMPATIBILITY_ONLY_TRUTH_SOURCES = {
    "fallback:legacy_lot_metadata_missing",
    "fallback:no_executable_open_lots",
}
_CANONICAL_COMPATIBILITY_TRUTH_SOURCES = {
    "fallback:legacy_lot_metadata_missing": "derived:lot_native_fail_closed",
    "fallback:no_executable_open_lots": "derived:no_executable_open_lots",
}
_DECLARATION_RESIDUE_SUFFIXES = ("_source", "_truth_source", "_compatibility_residue")
_DECLARATION_RESIDUE_KEYS = {"decision_compatibility_residue"}
_CANONICAL_OPEN_EXPOSURE_QTY_SOURCE = "position_state.normalized_exposure.open_exposure_qty"
_CANONICAL_SELL_LOT_AUTHORITY = "position_state.normalized_exposure.sellable_executable_lot_count"
_CANONICAL_SELL_QTY_DERIVATION = "position_state.normalized_exposure.sellable_executable_qty"


@dataclass(frozen=True)
class CanonicalPositionExposureSnapshot:
    dust_classification: str
    entry_allowed: bool
    effective_flat: bool
    entry_gate_effective_flat: bool
    holding_authority_state: str
    raw_qty_open: float
    raw_total_asset_qty: float
    position_qty: float
    submit_payload_qty: float
    normalized_exposure_active: bool
    normalized_exposure_qty: float
    has_executable_exposure: bool
    has_any_position_residue: bool
    has_non_executable_residue: bool
    has_dust_only_remainder: bool
    recovery_blocked: bool
    recovery_block_reason: str
    unresolved_order_count: int
    recovery_required_count: int
    open_exposure_qty: float
    dust_tracking_qty: float
    open_lot_count: int
    dust_tracking_lot_count: int
    reserved_exit_lot_count: int
    sellable_executable_lot_count: int
    reserved_exit_qty: float
    sellable_executable_qty: float
    sell_submit_lot_count: int
    exit_allowed: bool
    exit_block_reason: str
    sell_qty_basis_qty: float
    sell_qty_boundary_kind: str
    sell_normalized_exposure_qty: float
    sell_open_exposure_qty: float
    sell_dust_tracking_qty: float


@dataclass(frozen=True)
class _CanonicalSellAuthorityInputs:
    open_exposure_qty: float
    open_exposure_qty_truth_source: str
    open_lot_count: int
    reserved_exit_qty: float
    reserved_exit_lot_count: int
    sellable_executable_lot_count: int
    sellable_executable_qty: float
    sellable_executable_qty_truth_source: str
    semantic_basis: str


@dataclass(frozen=True)
class _SellDiagnosticObservation:
    raw_total_asset_qty: float
    open_exposure_qty: float
    reserved_exit_qty: float


@dataclass(frozen=True)
class _ResolvedSellAuthorityBoundary:
    canonical_authority: _CanonicalSellAuthorityInputs
    diagnostic_observation: _SellDiagnosticObservation


def materialize_strategy_decision_context(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: materialize_strategy_decision_context(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [materialize_strategy_decision_context(item) for item in value]
    if isinstance(value, tuple):
        return tuple(materialize_strategy_decision_context(item) for item in value)
    return value


def _strip_declaration_residue(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(key, str) and (
                key in _DECLARATION_RESIDUE_KEYS
                or key.endswith(_DECLARATION_RESIDUE_SUFFIXES)
            ):
                continue
            cleaned[key] = _strip_declaration_residue(item)
        return cleaned
    if isinstance(value, list):
        return [_strip_declaration_residue(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_declaration_residue(item) for item in value)
    return value


def load_recorded_strategy_decision_context(
    conn: sqlite3.Connection,
    *,
    decision_id: int | None,
) -> tuple[dict[str, Any], bool]:
    """Load the normalized strategy decision context stored for a prior decision.

    The stored decision context is the strategy truth source. Execution paths can
    use it to avoid re-interpreting harmless dust from a different runtime snapshot.
    """

    if decision_id is None:
        return {}, False

    try:
        row = conn.execute(
            """
            SELECT context_json
            FROM strategy_decisions
            WHERE id=?
            """,
            (int(decision_id),),
        ).fetchone()
    except sqlite3.OperationalError:
        return {}, False

    if row is None:
        return {}, False

    try:
        context = json.loads(str(row["context_json"] or "{}"))
    except json.JSONDecodeError:
        return {}, False
    if not isinstance(context, dict):
        return {}, False
    return context, True


def _as_text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _as_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off", ""}:
            return False
    if value is None:
        return False
    return bool(value)


def _as_filter_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        out: list[str] = []
        for item in raw:
            key = str(item).strip()
            if key and key not in out:
                out.append(key)
        return out
    if isinstance(raw, tuple):
        return _as_filter_list(list(raw))
    if isinstance(raw, str):
        parts = [part.strip() for part in raw.split(",")]
        return [part for part in parts if part]
    return []


def _normalized_exposure_view(position_state: dict[str, Any]) -> dict[str, Any]:
    normalized = position_state.get("normalized_exposure")
    if isinstance(normalized, dict):
        return dict(normalized)
    return {}


def _resolve_with_source(
    *candidates: tuple[str, Any],
    default_value: Any,
    default_source: str,
    value_kind: str,
) -> tuple[Any, str]:
    for source, raw in candidates:
        if raw is not None:
            if value_kind == "bool":
                return _as_bool(raw), source
            if value_kind == "float":
                resolved = _as_float_or_none(raw)
                return (0.0 if resolved is None else float(resolved)), source
            return raw, source
    if value_kind == "bool":
        return _as_bool(default_value), default_source
    if value_kind == "float":
        resolved = _as_float_or_none(default_value)
        return (0.0 if resolved is None else float(resolved)), default_source
    return default_value, default_source


def _resolve_int_with_source(
    *candidates: tuple[str, Any],
    default_value: Any,
    default_source: str,
) -> tuple[int, str]:
    resolved, source = _resolve_with_source(
        *candidates,
        default_value=default_value,
        default_source=default_source,
        value_kind="float",
    )
    try:
        return max(0, int(float(resolved))), source
    except (TypeError, ValueError):
        return 0, source


def _resolve_canonical_sell_qty_basis(
    *,
    sellable_executable_qty: float,
    sellable_executable_qty_truth_source: str,
) -> tuple[float, str, str]:
    return (
        float(sellable_executable_qty),
        _CANONICAL_SELL_LOT_AUTHORITY,
        "derived:sellable_executable_lot_count",
    )


def _derive_canonical_exit_authority(
    *,
    raw_total_asset_qty: float,
    open_lot_count: int,
    dust_tracking_qty: float,
    reserved_exit_lot_count: int,
    sellable_executable_lot_count: int,
    sellable_executable_qty: float,
) -> tuple[bool, str]:
    has_sellable_lot_authority = bool(
        int(sellable_executable_lot_count) > 0 and float(sellable_executable_qty) > 1e-12
    )
    if has_sellable_lot_authority:
        return True, "none"
    if int(open_lot_count) > 0 and int(reserved_exit_lot_count) > 0:
        return False, "reserved_for_open_sell_orders"
    if int(open_lot_count) <= 0 and float(dust_tracking_qty) > 1e-12:
        return False, "dust_only_remainder"
    if float(raw_total_asset_qty) <= 1e-12:
        return False, "no_position"
    return False, "no_executable_exit_lot"


def _extract_non_authoritative_sell_diagnostic_observation(
    *,
    payload: dict[str, Any],
    position_gate: dict[str, Any],
    position_state: dict[str, Any],
    position_normalized: dict[str, Any],
) -> _SellDiagnosticObservation:
    open_exposure_qty, open_exposure_qty_truth_source = _resolve_with_source(
        (_CANONICAL_OPEN_EXPOSURE_QTY_SOURCE, position_normalized.get("open_exposure_qty")),
        ("position_state.open_exposure_qty", position_state.get("open_exposure_qty")),
        ("context.open_exposure_qty", payload.get("open_exposure_qty")),
        ("position_gate.open_exposure_qty", position_gate.get("open_exposure_qty")),
        default_value=0.0,
        default_source="default:0.0",
        value_kind="float",
    )
    reserved_exit_qty, _ = _resolve_with_source(
        ("position_state.normalized_exposure.reserved_exit_qty", position_normalized.get("reserved_exit_qty")),
        ("position_state.reserved_exit_qty", position_state.get("reserved_exit_qty")),
        ("context.reserved_exit_qty", payload.get("reserved_exit_qty")),
        ("position_gate.reserved_exit_qty", position_gate.get("reserved_exit_qty")),
        default_value=0.0,
        default_source="default:0.0",
        value_kind="float",
    )
    return _SellDiagnosticObservation(
        raw_total_asset_qty=max(0.0, float(payload.get("raw_total_asset_qty") or 0.0)),
        open_exposure_qty=0.0 if open_exposure_qty is None else float(open_exposure_qty),
        reserved_exit_qty=0.0 if reserved_exit_qty is None else float(reserved_exit_qty),
    )


def _extract_canonical_sell_authority_inputs(
    *,
    position_state: dict[str, Any],
    position_normalized: dict[str, Any],
) -> _CanonicalSellAuthorityInputs:
    semantic_basis = _as_text(
        position_state.get("semantic_basis", position_normalized.get("semantic_basis")),
        default="",
    )
    open_lot_count, _ = _resolve_int_with_source(
        ("position_state.normalized_exposure.open_lot_count", position_normalized.get("open_lot_count")),
        default_value=0,
        default_source="default:0",
    )
    reserved_exit_lot_count, _ = _resolve_int_with_source(
        (
            "position_state.normalized_exposure.reserved_exit_lot_count",
            position_normalized.get("reserved_exit_lot_count"),
        ),
        default_value=0,
        default_source="default:0",
    )
    sellable_executable_lot_count, _ = _resolve_int_with_source(
        (_CANONICAL_SELL_LOT_AUTHORITY, position_normalized.get("sellable_executable_lot_count")),
        default_value=0,
        default_source="default:0",
    )
    open_exposure_qty, _ = _resolve_with_source(
        (_CANONICAL_OPEN_EXPOSURE_QTY_SOURCE, position_normalized.get("open_exposure_qty")),
        default_value=0.0,
        default_source="default:0.0",
        value_kind="float",
    )
    reserved_exit_qty, _ = _resolve_with_source(
        ("position_state.normalized_exposure.reserved_exit_qty", position_normalized.get("reserved_exit_qty")),
        default_value=0.0,
        default_source="default:0.0",
        value_kind="float",
    )
    sellable_executable_qty, sellable_executable_qty_truth_source = _resolve_with_source(
        (_CANONICAL_SELL_QTY_DERIVATION, position_normalized.get("sellable_executable_qty")),
        default_value=max(
            0.0,
            float(open_exposure_qty or 0.0) - float(reserved_exit_qty or 0.0),
        ),
        default_source=_CANONICAL_SELL_QTY_DERIVATION,
        value_kind="float",
    )
    return _CanonicalSellAuthorityInputs(
        open_exposure_qty=float(open_exposure_qty or 0.0),
        open_exposure_qty_truth_source=_CANONICAL_OPEN_EXPOSURE_QTY_SOURCE,
        open_lot_count=int(open_lot_count),
        reserved_exit_qty=float(reserved_exit_qty or 0.0),
        reserved_exit_lot_count=int(reserved_exit_lot_count),
        sellable_executable_lot_count=int(sellable_executable_lot_count),
        sellable_executable_qty=(
            max(0.0, float(open_exposure_qty or 0.0) - float(reserved_exit_qty or 0.0))
            if sellable_executable_qty is None
            else float(sellable_executable_qty)
        ),
        sellable_executable_qty_truth_source=sellable_executable_qty_truth_source,
        semantic_basis=semantic_basis,
    )


def _extract_fail_closed_sell_compatibility_inputs(
    *,
    position_state: dict[str, Any],
) -> _CanonicalSellAuthorityInputs:
    # Compatibility inputs are an internal fail-closed adapter only.
    # They must never be treated as peer authority with normalized_exposure.
    semantic_basis = _as_text(
        position_state.get("semantic_basis"),
        default="",
    )
    return _CanonicalSellAuthorityInputs(
        open_exposure_qty=0.0,
        open_exposure_qty_truth_source=_CANONICAL_OPEN_EXPOSURE_QTY_SOURCE,
        open_lot_count=0,
        reserved_exit_qty=0.0,
        reserved_exit_lot_count=0,
        sellable_executable_lot_count=0,
        sellable_executable_qty=0.0,
        sellable_executable_qty_truth_source=_CANONICAL_SELL_QTY_DERIVATION,
        semantic_basis=semantic_basis,
    )


def _resolve_canonical_sell_authority_inputs(
    *,
    position_state: dict[str, Any],
) -> _CanonicalSellAuthorityInputs:
    # Canonical SELL authority is read through normalized_exposure only.
    # Top-level position_state fields remain a compatibility-only boundary.
    position_normalized = _normalized_exposure_view(position_state)
    if position_normalized:
        return _extract_canonical_sell_authority_inputs(
            position_state=position_state,
            position_normalized=position_normalized,
        )
    return _extract_fail_closed_sell_compatibility_inputs(position_state=position_state)


def _apply_non_authoritative_sell_diagnostic_fallbacks(
    *,
    authority_inputs: _CanonicalSellAuthorityInputs,
    raw_total_asset_qty: float,
) -> _CanonicalSellAuthorityInputs:
    # This adapter exists only to fail closed for legacy/fallback payloads.
    # It must not be treated as the canonical SELL authority surface.
    open_exposure_qty = float(authority_inputs.open_exposure_qty)
    open_exposure_qty_truth_source = authority_inputs.open_exposure_qty_truth_source
    sellable_executable_qty = float(authority_inputs.sellable_executable_qty)
    sellable_executable_qty_truth_source = authority_inputs.sellable_executable_qty_truth_source

    legacy_lot_semantics = bool(
        authority_inputs.semantic_basis and authority_inputs.semantic_basis != "lot-native"
    )
    if legacy_lot_semantics or (
        authority_inputs.open_lot_count <= 0 and float(raw_total_asset_qty) > 1e-12
    ):
        open_exposure_qty = 0.0
        open_exposure_qty_truth_source = "fallback:legacy_lot_metadata_missing"
        sellable_executable_qty = 0.0
        sellable_executable_qty_truth_source = "fallback:legacy_lot_metadata_missing"
    if authority_inputs.open_lot_count <= 0:
        open_exposure_qty = 0.0
        if open_exposure_qty_truth_source != "fallback:legacy_lot_metadata_missing":
            open_exposure_qty_truth_source = "fallback:no_executable_open_lots"
        sellable_executable_qty = 0.0
        if sellable_executable_qty_truth_source != "fallback:legacy_lot_metadata_missing":
            sellable_executable_qty_truth_source = "fallback:no_executable_open_lots"

    return _CanonicalSellAuthorityInputs(
        open_exposure_qty=open_exposure_qty,
        open_exposure_qty_truth_source=open_exposure_qty_truth_source,
        open_lot_count=authority_inputs.open_lot_count,
        reserved_exit_qty=authority_inputs.reserved_exit_qty,
        reserved_exit_lot_count=authority_inputs.reserved_exit_lot_count,
        sellable_executable_lot_count=authority_inputs.sellable_executable_lot_count,
        sellable_executable_qty=sellable_executable_qty,
        sellable_executable_qty_truth_source=sellable_executable_qty_truth_source,
        semantic_basis=authority_inputs.semantic_basis,
    )


def _resolve_sell_authority_boundary(
    *,
    payload: dict[str, Any],
    position_gate: dict[str, Any],
    position_state: dict[str, Any],
    position_normalized: dict[str, Any],
) -> _ResolvedSellAuthorityBoundary:
    diagnostic_observation = _extract_non_authoritative_sell_diagnostic_observation(
        payload=payload,
        position_gate=position_gate,
        position_state=position_state,
        position_normalized=position_normalized,
    )
    canonical_authority = _resolve_canonical_sell_authority_inputs(position_state=position_state)
    fail_closed_canonical_authority = _apply_non_authoritative_sell_diagnostic_fallbacks(
        authority_inputs=canonical_authority,
        raw_total_asset_qty=float(diagnostic_observation.raw_total_asset_qty),
    )
    return _ResolvedSellAuthorityBoundary(
        canonical_authority=fail_closed_canonical_authority,
        diagnostic_observation=diagnostic_observation,
    )


def _as_compatibility_truth_source(source: str) -> str:
    normalized = str(source or "").strip()
    if normalized in _COMPATIBILITY_ONLY_TRUTH_SOURCES:
        return f"compatibility:{normalized}"
    return normalized


def _split_compatibility_truth_source(source: str) -> tuple[str, str | None]:
    normalized = str(source or "").strip()
    if normalized in _COMPATIBILITY_ONLY_TRUTH_SOURCES:
        return (
            _CANONICAL_COMPATIBILITY_TRUTH_SOURCES.get(normalized, normalized),
            _as_compatibility_truth_source(normalized),
        )
    return normalized, None


def _derive_sell_failure_observability(
    *,
    final_signal: str,
    sell_qty_boundary_kind: str,
    dust_classification: str,
    effective_flat: bool,
    payload: dict[str, Any],
) -> tuple[str, str, str]:
    explicit_category = _as_text(payload.get("sell_failure_category"), default="").strip()
    if explicit_category:
        explicit_detail = _as_text(payload.get("sell_failure_detail"), default=explicit_category).strip()
        if not explicit_detail:
            explicit_detail = explicit_category
        return explicit_category, explicit_detail, "context.sell_failure_category"

    final = str(final_signal or "").strip().upper()
    boundary_kind = str(sell_qty_boundary_kind or "").strip()
    dust_state = str(dust_classification or "").strip()

    if final == "SELL":
        if boundary_kind == "qty_step":
            return (
                SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
                SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
                "derived:sell_qty_boundary_kind",
            )
        if boundary_kind == "min_qty":
            return (
                SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
                SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
                "derived:sell_qty_boundary_kind",
            )
        if boundary_kind == "dust_mismatch":
            return (
                SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH,
                SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH,
                "derived:sell_qty_boundary_kind",
            )
        if boundary_kind == "remainder_after_sell":
            return (
                SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD,
                SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD,
                "derived:sell_qty_boundary_kind",
            )
        if dust_state == "harmless_dust" and bool(effective_flat):
            return (
                SELL_FAILURE_CATEGORY_DUST_SUPPRESSION,
                SELL_FAILURE_CATEGORY_DUST_SUPPRESSION,
                "derived:dust_classification",
            )

    return "none", "none", "default:none"


def _extract_market_observations(context: dict[str, Any]) -> dict[str, float | None]:
    features = context.get("features") if isinstance(context.get("features"), dict) else {}
    return {
        "gap": _as_float_or_none(context.get("gap_ratio", features.get("sma_gap_ratio"))),
        "volatility": _as_float_or_none(
            context.get("volatility_ratio", features.get("volatility_range_ratio"))
        ),
        "extension": _as_float_or_none(
            context.get("overextended_ratio", features.get("overextended_abs_return_ratio"))
        ),
    }


def resolve_canonical_position_exposure_snapshot(
    context: dict[str, Any] | None,
) -> CanonicalPositionExposureSnapshot:
    payload: dict[str, Any] = dict(context or {})
    position_gate = payload.get("position_gate") if isinstance(payload.get("position_gate"), dict) else {}
    position_state = dict(payload.get("position_state")) if isinstance(payload.get("position_state"), dict) else {}
    position_normalized = _normalized_exposure_view(position_state)

    dust_classification = _as_text(
        payload.get(
            "dust_classification",
            position_gate.get("dust_classification", position_gate.get("dust_state", "")),
        ),
        default="",
    )
    entry_allowed, _ = _resolve_with_source(
        ("position_state.normalized_exposure.entry_allowed", position_normalized.get("entry_allowed")),
        ("context.entry_allowed", payload.get("entry_allowed")),
        ("position_gate.entry_allowed", position_gate.get("entry_allowed")),
        (
            "position_gate.effective_flat_due_to_harmless_dust",
            position_gate.get("effective_flat_due_to_harmless_dust"),
        ),
        ("position_gate.dust_treat_as_flat", position_gate.get("dust_treat_as_flat")),
        default_value=False,
        default_source="default:false",
        value_kind="bool",
    )
    effective_flat, _ = _resolve_with_source(
        ("position_state.normalized_exposure.effective_flat", position_normalized.get("effective_flat")),
        ("context.effective_flat", payload.get("effective_flat")),
        (
            "position_gate.effective_flat_due_to_harmless_dust",
            position_gate.get("effective_flat_due_to_harmless_dust"),
        ),
        ("position_gate.dust_treat_as_flat", position_gate.get("dust_treat_as_flat")),
        default_value=False,
        default_source="default:false",
        value_kind="bool",
    )
    raw_qty_open, _ = _resolve_with_source(
        ("position_state.raw_qty_open", position_state.get("raw_qty_open")),
        ("position_state.normalized_exposure.raw_qty_open", position_normalized.get("raw_qty_open")),
        ("context.raw_qty_open", payload.get("raw_qty_open")),
        ("position_gate.raw_qty_open", position_gate.get("raw_qty_open")),
        default_value=0.0,
        default_source="default:0.0",
        value_kind="float",
    )
    raw_qty_open = 0.0 if raw_qty_open is None else float(raw_qty_open)
    raw_total_asset_qty, _ = _resolve_with_source(
        ("position_state.raw_total_asset_qty", position_state.get("raw_total_asset_qty")),
        (
            "position_state.normalized_exposure.raw_total_asset_qty",
            position_normalized.get("raw_total_asset_qty"),
        ),
        ("context.raw_total_asset_qty", payload.get("raw_total_asset_qty")),
        ("position_gate.raw_total_asset_qty", position_gate.get("raw_total_asset_qty")),
        ("context.raw_qty_open", payload.get("raw_qty_open")),
        (
            "position_state.normalized_exposure.raw_qty_open",
            position_normalized.get("raw_qty_open"),
        ),
        ("position_gate.raw_qty_open", position_gate.get("raw_qty_open")),
        default_value=raw_qty_open,
        default_source="fallback:raw_qty_open",
        value_kind="float",
    )
    raw_total_asset_qty = raw_qty_open if raw_total_asset_qty is None else float(raw_total_asset_qty)
    dust_tracking_qty, _ = _resolve_with_source(
        ("position_state.dust_tracking_qty", position_state.get("dust_tracking_qty")),
        ("position_state.normalized_exposure.dust_tracking_qty", position_normalized.get("dust_tracking_qty")),
        ("context.dust_tracking_qty", payload.get("dust_tracking_qty")),
        ("position_gate.dust_tracking_qty", position_gate.get("dust_tracking_qty")),
        default_value=0.0,
        default_source="default:0.0",
        value_kind="float",
    )
    dust_tracking_qty = 0.0 if dust_tracking_qty is None else float(dust_tracking_qty)
    dust_tracking_lot_count, _ = _resolve_int_with_source(
        ("position_state.dust_tracking_lot_count", position_state.get("dust_tracking_lot_count")),
        (
            "position_state.normalized_exposure.dust_tracking_lot_count",
            position_normalized.get("dust_tracking_lot_count"),
        ),
        ("context.dust_tracking_lot_count", payload.get("dust_tracking_lot_count")),
        ("position_gate.dust_tracking_lot_count", position_gate.get("dust_tracking_lot_count")),
        default_value=0,
        default_source="default:0",
    )
    sell_authority_boundary = _resolve_sell_authority_boundary(
        payload=payload,
        position_gate=position_gate,
        position_state=position_state,
        position_normalized=position_normalized,
    )
    fail_closed_sell_authority = sell_authority_boundary.canonical_authority
    open_exposure_qty = fail_closed_sell_authority.open_exposure_qty
    open_exposure_qty_truth_source = fail_closed_sell_authority.open_exposure_qty_truth_source
    open_lot_count = fail_closed_sell_authority.open_lot_count
    reserved_exit_lot_count = fail_closed_sell_authority.reserved_exit_lot_count
    sellable_executable_lot_count = fail_closed_sell_authority.sellable_executable_lot_count
    reserved_exit_qty = fail_closed_sell_authority.reserved_exit_qty
    sellable_executable_qty = fail_closed_sell_authority.sellable_executable_qty
    sellable_executable_qty_truth_source = fail_closed_sell_authority.sellable_executable_qty_truth_source
    derived_exit_allowed, derived_exit_block_reason = _derive_canonical_exit_authority(
        raw_total_asset_qty=float(raw_total_asset_qty),
        open_lot_count=int(open_lot_count),
        dust_tracking_qty=float(dust_tracking_qty),
        reserved_exit_lot_count=int(reserved_exit_lot_count),
        sellable_executable_lot_count=int(sellable_executable_lot_count),
        sellable_executable_qty=float(sellable_executable_qty),
    )
    exit_allowed, _ = _resolve_with_source(
        ("position_state.normalized_exposure.exit_allowed", position_normalized.get("exit_allowed")),
        default_value=derived_exit_allowed,
        default_source="fallback:canonical_sell_authority",
        value_kind="bool",
    )
    exit_block_reason = _as_text(
        position_normalized.get("exit_block_reason"),
        default=derived_exit_block_reason,
    )
    if effective_flat is False and float(raw_total_asset_qty) <= 1e-12:
        effective_flat = True
    if entry_allowed is False and float(raw_total_asset_qty) <= 1e-12:
        entry_allowed = True
    has_executable_exposure, _ = _resolve_with_source(
        ("position_state.has_executable_exposure", position_state.get("has_executable_exposure")),
        (
            "position_state.normalized_exposure.has_executable_exposure",
            position_normalized.get("has_executable_exposure"),
        ),
        ("context.has_executable_exposure", payload.get("has_executable_exposure")),
        ("position_gate.has_executable_exposure", position_gate.get("has_executable_exposure")),
        default_value=bool(open_lot_count > 0 and sellable_executable_qty > 1e-12),
        default_source=(
            "fallback:executable_open_lot_count"
            if open_lot_count > 0 and sellable_executable_qty > 1e-12
            else "default:false"
        ),
        value_kind="bool",
    )
    has_any_position_residue, _ = _resolve_with_source(
        ("position_state.has_any_position_residue", position_state.get("has_any_position_residue")),
        (
            "position_state.normalized_exposure.has_any_position_residue",
            position_normalized.get("has_any_position_residue"),
        ),
        ("context.has_any_position_residue", payload.get("has_any_position_residue")),
        ("position_gate.has_any_position_residue", position_gate.get("has_any_position_residue")),
        default_value=bool(raw_total_asset_qty > 1e-12),
        default_source=("fallback:raw_total_asset_qty" if raw_total_asset_qty > 1e-12 else "default:false"),
        value_kind="bool",
    )
    has_non_executable_residue, _ = _resolve_with_source(
        ("position_state.has_non_executable_residue", position_state.get("has_non_executable_residue")),
        (
            "position_state.normalized_exposure.has_non_executable_residue",
            position_normalized.get("has_non_executable_residue"),
        ),
        ("context.has_non_executable_residue", payload.get("has_non_executable_residue")),
        ("position_gate.has_non_executable_residue", position_gate.get("has_non_executable_residue")),
        default_value=bool(has_any_position_residue and not has_executable_exposure),
        default_source=(
            "fallback:non_executable_residue"
            if has_any_position_residue and not has_executable_exposure
            else "default:false"
        ),
        value_kind="bool",
    )
    has_dust_only_remainder, _ = _resolve_with_source(
        ("position_state.has_dust_only_remainder", position_state.get("has_dust_only_remainder")),
        (
            "position_state.normalized_exposure.has_dust_only_remainder",
            position_normalized.get("has_dust_only_remainder"),
        ),
        ("context.has_dust_only_remainder", payload.get("has_dust_only_remainder")),
        ("position_gate.has_dust_only_remainder", position_gate.get("has_dust_only_remainder")),
        default_value=bool(dust_tracking_qty > 1e-12 and open_lot_count <= 0),
        default_source=(
            "fallback:dust_only_remainder"
            if dust_tracking_qty > 1e-12 and open_lot_count <= 0
            else "default:false"
        ),
        value_kind="bool",
    )
    unresolved_order_count, _ = _resolve_int_with_source(
        (
            "position_state.normalized_exposure.unresolved_order_count",
            position_normalized.get("unresolved_order_count"),
        ),
        ("position_state.unresolved_order_count", position_state.get("unresolved_order_count")),
        ("context.unresolved_order_count", payload.get("unresolved_order_count")),
        (
            "context.unresolved_open_order_count",
            payload.get("unresolved_open_order_count"),
        ),
        (
            "position_gate.unresolved_order_count",
            position_gate.get("unresolved_order_count"),
        ),
        (
            "position_gate.unresolved_open_order_count",
            position_gate.get("unresolved_open_order_count"),
        ),
        default_value=0,
        default_source="default:0",
    )
    recovery_required_count, _ = _resolve_int_with_source(
        (
            "position_state.normalized_exposure.recovery_required_count",
            position_normalized.get("recovery_required_count"),
        ),
        (
            "position_state.recovery_required_count",
            position_state.get("recovery_required_count"),
        ),
        (
            "context.recovery_required_count",
            payload.get("recovery_required_count"),
        ),
        (
            "position_gate.recovery_required_count",
            position_gate.get("recovery_required_count"),
        ),
        default_value=0,
        default_source="default:0",
    )
    recovery_blocked, _ = _resolve_with_source(
        (
            "position_state.normalized_exposure.recovery_blocked",
            position_normalized.get("recovery_blocked"),
        ),
        ("position_state.recovery_blocked", position_state.get("recovery_blocked")),
        ("context.recovery_blocked", payload.get("recovery_blocked")),
        ("position_gate.recovery_blocked", position_gate.get("recovery_blocked")),
        default_value=bool(unresolved_order_count > 0 or recovery_required_count > 0),
        default_source=(
            "fallback:recovery_counts"
            if unresolved_order_count > 0 or recovery_required_count > 0
            else "default:false"
        ),
        value_kind="bool",
    )
    derived_recovery_block_reason = "none"
    if recovery_required_count > 0 and unresolved_order_count > 0:
        derived_recovery_block_reason = "recovery_required_and_unresolved_orders_present"
    elif recovery_required_count > 0:
        derived_recovery_block_reason = "recovery_required_present"
    elif unresolved_order_count > 0:
        derived_recovery_block_reason = "unresolved_orders_present"
    recovery_block_reason = _as_text(
        position_normalized.get(
            "recovery_block_reason",
            position_state.get(
                "recovery_block_reason",
                payload.get(
                    "recovery_block_reason",
                    position_gate.get("recovery_block_reason"),
                ),
            ),
        ),
        default=derived_recovery_block_reason,
    )
    normalized_exposure_qty, normalized_exposure_qty_truth_source = _resolve_with_source(
        ("position_state.normalized_exposure_qty", position_state.get("normalized_exposure_qty")),
        (
            "position_state.normalized_exposure.normalized_exposure_qty",
            position_normalized.get("normalized_exposure_qty"),
        ),
        ("context.normalized_exposure_qty", payload.get("normalized_exposure_qty")),
        ("position_gate.normalized_exposure_qty", position_gate.get("normalized_exposure_qty")),
        default_value=(open_exposure_qty if has_executable_exposure else 0.0),
        default_source=("fallback:open_exposure_qty" if has_executable_exposure else "default:0.0"),
        value_kind="float",
    )
    if normalized_exposure_qty_truth_source == "default:0.0":
        normalized_exposure_qty = float(open_exposure_qty) if has_executable_exposure else 0.0
    else:
        normalized_exposure_qty = 0.0 if normalized_exposure_qty is None else float(normalized_exposure_qty)
    if not has_executable_exposure and normalized_exposure_qty_truth_source not in {
        "position_state.normalized_exposure_qty",
        "position_state.normalized_exposure.normalized_exposure_qty",
        "context.normalized_exposure_qty",
        "position_gate.normalized_exposure_qty",
    }:
        normalized_exposure_qty = 0.0
    normalized_exposure_active, normalized_exposure_active_truth_source = _resolve_with_source(
        (
            "position_state.normalized_exposure_active",
            position_state.get("normalized_exposure_active"),
        ),
        (
            "position_state.normalized_exposure.normalized_exposure_active",
            position_normalized.get("normalized_exposure_active"),
        ),
        ("context.normalized_exposure_active", payload.get("normalized_exposure_active")),
        ("position_gate.normalized_exposure_active", position_gate.get("normalized_exposure_active")),
        default_value=bool(open_lot_count > 0 or reserved_exit_lot_count > 0),
        default_source=(
            "fallback:open_exposure_lot_count"
            if open_lot_count > 0 or reserved_exit_lot_count > 0
            else normalized_exposure_qty_truth_source
        ),
        value_kind="bool",
    )
    if normalized_exposure_active_truth_source == normalized_exposure_qty_truth_source:
        normalized_exposure_active = bool(open_lot_count > 0 or reserved_exit_lot_count > 0)
    if open_lot_count <= 0 and reserved_exit_lot_count <= 0 and normalized_exposure_active_truth_source not in {
        "position_state.normalized_exposure_active",
        "position_state.normalized_exposure.normalized_exposure_active",
        "context.normalized_exposure_active",
        "position_gate.normalized_exposure_active",
    }:
        normalized_exposure_active = False

    position_qty = float(open_exposure_qty)
    submit_payload_qty = float(normalized_exposure_qty)
    sell_submit_lot_count = int(sellable_executable_lot_count)
    sell_qty_basis_qty, _, _ = _resolve_canonical_sell_qty_basis(
        sellable_executable_qty=sellable_executable_qty,
        sellable_executable_qty_truth_source=sellable_executable_qty_truth_source,
    )
    sell_qty_boundary_kind = _as_text(payload.get("sell_qty_boundary_kind"), default="")
    if not sell_qty_boundary_kind:
        sell_qty_boundary_kind = _as_text(position_state.get("sell_qty_boundary_kind"), default="")
    if not sell_qty_boundary_kind:
        sell_qty_boundary_kind = _as_text(position_normalized.get("sell_qty_boundary_kind"), default="")
    if not sell_qty_boundary_kind:
        sell_qty_boundary_kind = "none"
    if int(open_lot_count) > 0 and int(reserved_exit_lot_count) > 0 and int(sellable_executable_lot_count) <= 0:
        holding_authority_state = "reserved_exit_pending"
    elif bool(has_executable_exposure):
        holding_authority_state = "open_exposure"
    elif bool(has_dust_only_remainder):
        holding_authority_state = "dust_only"
    elif bool(has_any_position_residue):
        holding_authority_state = "non_executable_position"
    else:
        holding_authority_state = "flat"

    return CanonicalPositionExposureSnapshot(
        dust_classification=dust_classification,
        entry_allowed=bool(entry_allowed),
        effective_flat=bool(effective_flat),
        entry_gate_effective_flat=bool(effective_flat),
        holding_authority_state=holding_authority_state,
        raw_qty_open=float(raw_qty_open),
        raw_total_asset_qty=float(raw_total_asset_qty),
        position_qty=float(position_qty),
        submit_payload_qty=float(submit_payload_qty),
        normalized_exposure_active=bool(normalized_exposure_active),
        normalized_exposure_qty=float(normalized_exposure_qty),
        has_executable_exposure=bool(has_executable_exposure),
        has_any_position_residue=bool(has_any_position_residue),
        has_non_executable_residue=bool(has_non_executable_residue),
        has_dust_only_remainder=bool(has_dust_only_remainder),
        recovery_blocked=bool(recovery_blocked),
        recovery_block_reason=recovery_block_reason,
        unresolved_order_count=int(unresolved_order_count),
        recovery_required_count=int(recovery_required_count),
        open_exposure_qty=float(open_exposure_qty),
        dust_tracking_qty=float(dust_tracking_qty),
        open_lot_count=int(open_lot_count),
        dust_tracking_lot_count=int(dust_tracking_lot_count),
        reserved_exit_lot_count=int(reserved_exit_lot_count),
        sellable_executable_lot_count=int(sellable_executable_lot_count),
        reserved_exit_qty=float(reserved_exit_qty),
        sellable_executable_qty=float(sellable_executable_qty),
        sell_submit_lot_count=int(sell_submit_lot_count),
        exit_allowed=bool(exit_allowed),
        exit_block_reason=exit_block_reason,
        sell_qty_basis_qty=float(sell_qty_basis_qty),
        sell_qty_boundary_kind=sell_qty_boundary_kind,
        sell_normalized_exposure_qty=float(normalized_exposure_qty),
        sell_open_exposure_qty=float(open_exposure_qty),
        sell_dust_tracking_qty=float(dust_tracking_qty),
    )


def normalize_strategy_decision_context(
    *,
    context: dict[str, Any] | None,
    signal: str,
    reason: str,
    strategy_name: str,
    pair: str,
    interval: str,
    decision_ts: int,
    candle_ts: int | None,
    market_price: float | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = dict(context or {})
    entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
    signal_strength = (
        payload.get("signal_strength") if isinstance(payload.get("signal_strength"), dict) else {}
    )
    position_gate = payload.get("position_gate") if isinstance(payload.get("position_gate"), dict) else {}

    base_signal = _as_text(payload.get("base_signal", entry.get("base_signal", signal)), default="HOLD")
    base_reason = _as_text(payload.get("base_reason", entry.get("base_reason", reason)), default=reason)
    entry_reason = _as_text(payload.get("entry_reason", entry.get("entry_reason", reason)), default=reason)
    raw_signal = _as_text(payload.get("raw_signal", base_signal), default=base_signal)
    final_signal = _as_text(payload.get("final_signal", payload.get("signal", signal)), default=signal)
    blocked_filters = _as_filter_list(payload.get("blocked_filters"))

    inferred_filter_blocked = bool(blocked_filters) and base_signal in {"BUY", "SELL"}
    filter_blocked = _as_bool(payload.get("filter_blocked")) or inferred_filter_blocked
    entry_blocked = _as_bool(payload.get("entry_blocked"))
    if not entry_blocked:
        entry_blocked = raw_signal in {"BUY", "SELL"} and final_signal != raw_signal

    entry_block_reason = payload.get("entry_block_reason")
    if entry_block_reason is None:
        if filter_blocked:
            entry_block_reason = payload.get("block_reason", entry_reason or reason)
        elif entry_blocked:
            entry_block_reason = payload.get("reason", reason)
    entry_block_reason_text = _as_text(entry_block_reason, default="")
    if not entry_block_reason_text:
        entry_block_reason_text = None

    signal_strength_label = _as_text(
        payload.get("signal_strength_label", signal_strength.get("label", "unknown")),
        default="unknown",
    )

    decision_type = _as_text(payload.get("decision_type"), default="")
    if not decision_type:
        if filter_blocked and base_signal in {"BUY", "SELL"}:
            decision_type = "BLOCKED_ENTRY"
        elif signal in {"BUY", "SELL", "HOLD"}:
            decision_type = signal
        else:
            decision_type = "HOLD"

    market_observations = _extract_market_observations(payload)

    block_reason_hierarchy: list[str] = []
    for item in blocked_filters:
        if item not in block_reason_hierarchy:
            block_reason_hierarchy.append(item)
    if filter_blocked and entry_reason and entry_reason not in block_reason_hierarchy:
        block_reason_hierarchy.append(entry_reason)
    if filter_blocked and base_reason and base_reason not in block_reason_hierarchy:
        block_reason_hierarchy.append(base_reason)
    if entry_blocked and not filter_blocked and entry_block_reason_text:
        block_reason_hierarchy.append(entry_block_reason_text)

    position_state = dict(payload.get("position_state")) if isinstance(payload.get("position_state"), dict) else {}
    position_normalized = _normalized_exposure_view(position_state)
    canonical_exposure = resolve_canonical_position_exposure_snapshot(payload)
    dust_classification = canonical_exposure.dust_classification
    entry_allowed = canonical_exposure.entry_allowed
    effective_flat = canonical_exposure.effective_flat
    entry_gate_effective_flat = canonical_exposure.entry_gate_effective_flat
    holding_authority_state = canonical_exposure.holding_authority_state
    raw_qty_open = canonical_exposure.raw_qty_open
    raw_total_asset_qty = canonical_exposure.raw_total_asset_qty
    position_qty = canonical_exposure.position_qty
    submit_payload_qty = canonical_exposure.submit_payload_qty
    normalized_exposure_active = canonical_exposure.normalized_exposure_active
    normalized_exposure_qty = canonical_exposure.normalized_exposure_qty
    has_executable_exposure = canonical_exposure.has_executable_exposure
    has_any_position_residue = canonical_exposure.has_any_position_residue
    has_non_executable_residue = canonical_exposure.has_non_executable_residue
    has_dust_only_remainder = canonical_exposure.has_dust_only_remainder
    recovery_blocked = canonical_exposure.recovery_blocked
    recovery_block_reason = canonical_exposure.recovery_block_reason
    unresolved_order_count = canonical_exposure.unresolved_order_count
    recovery_required_count = canonical_exposure.recovery_required_count
    open_exposure_qty = canonical_exposure.open_exposure_qty
    dust_tracking_qty = canonical_exposure.dust_tracking_qty
    open_lot_count = canonical_exposure.open_lot_count
    dust_tracking_lot_count = canonical_exposure.dust_tracking_lot_count
    reserved_exit_lot_count = canonical_exposure.reserved_exit_lot_count
    sellable_executable_lot_count = canonical_exposure.sellable_executable_lot_count
    reserved_exit_qty = canonical_exposure.reserved_exit_qty
    sellable_executable_qty = canonical_exposure.sellable_executable_qty
    sell_submit_lot_count = canonical_exposure.sell_submit_lot_count
    exit_allowed = canonical_exposure.exit_allowed
    exit_block_reason = canonical_exposure.exit_block_reason
    sell_qty_basis_qty = canonical_exposure.sell_qty_basis_qty
    sell_qty_boundary_kind = canonical_exposure.sell_qty_boundary_kind
    sell_normalized_exposure_qty = canonical_exposure.sell_normalized_exposure_qty
    sell_open_exposure_qty = canonical_exposure.sell_open_exposure_qty
    sell_dust_tracking_qty = canonical_exposure.sell_dust_tracking_qty
    entry_block_reason = _as_text(
        payload.get(
            "entry_block_reason",
            position_normalized.get("entry_block_reason", position_state.get("entry_block_reason")),
        ),
        default="",
    )
    submit_lot_source = _CANONICAL_SELL_LOT_AUTHORITY
    submit_qty_source = _CANONICAL_SELL_QTY_DERIVATION
    sell_submit_qty_source = submit_qty_source
    sell_submit_lot_source = submit_lot_source
    submit_lot_count = int(sell_submit_lot_count)
    sell_qty_basis_source = _CANONICAL_SELL_LOT_AUTHORITY
    sell_failure_category, sell_failure_detail, _ = _derive_sell_failure_observability(
        final_signal=final_signal,
        sell_qty_boundary_kind=sell_qty_boundary_kind,
        dust_classification=dust_classification,
        effective_flat=bool(effective_flat),
        payload=payload,
    )
    position_state_source = submit_lot_source

    decision_summary = {
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "entry_blocked": bool(entry_blocked),
        "entry_block_reason": entry_block_reason_text,
        "dust_classification": dust_classification,
        "entry_allowed": bool(entry_allowed),
        "entry_block_reason": entry_block_reason,
        "effective_flat": bool(effective_flat),
        "entry_gate_effective_flat": bool(entry_gate_effective_flat),
        "holding_authority_state": holding_authority_state,
        "raw_qty_open": float(raw_qty_open),
        "raw_total_asset_qty": float(raw_total_asset_qty),
        "position_qty": float(position_qty),
        "submit_payload_qty": float(submit_payload_qty),
        "submit_lot_count": int(submit_lot_count),
        "position_state_lot_count": int(submit_lot_count),
        "normalized_exposure_active": bool(normalized_exposure_active),
        "normalized_exposure_qty": float(normalized_exposure_qty),
        "has_executable_exposure": bool(has_executable_exposure),
        "has_any_position_residue": bool(has_any_position_residue),
        "has_non_executable_residue": bool(has_non_executable_residue),
        "has_dust_only_remainder": bool(has_dust_only_remainder),
        "recovery_blocked": bool(recovery_blocked),
        "recovery_block_reason": recovery_block_reason,
        "unresolved_order_count": int(unresolved_order_count),
        "recovery_required_count": int(recovery_required_count),
        "open_exposure_qty": float(open_exposure_qty),
        "dust_tracking_qty": float(dust_tracking_qty),
        "open_lot_count": int(open_lot_count),
        "dust_tracking_lot_count": int(dust_tracking_lot_count),
        "reserved_exit_lot_count": int(reserved_exit_lot_count),
        "sellable_executable_lot_count": int(sellable_executable_lot_count),
        "reserved_exit_qty": float(reserved_exit_qty),
        "sellable_executable_qty": float(sellable_executable_qty),
        "sell_submit_lot_count": int(sell_submit_lot_count),
        "exit_allowed": bool(exit_allowed),
        "exit_block_reason": exit_block_reason,
        "sell_qty_basis_qty": float(sell_qty_basis_qty),
        "sell_qty_boundary_kind": sell_qty_boundary_kind,
        "sell_normalized_exposure_qty": float(sell_normalized_exposure_qty),
        "sell_open_exposure_qty": float(sell_open_exposure_qty),
        "sell_dust_tracking_qty": float(sell_dust_tracking_qty),
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
    }

    payload["decision_context_version"] = _CANONICAL_CONTEXT_VERSION
    payload["decision_type"] = decision_type
    payload["base_reason"] = base_reason
    payload["entry_reason"] = entry_reason
    payload["raw_signal"] = raw_signal
    payload["final_signal"] = final_signal
    payload["blocked_filters"] = blocked_filters
    payload["filter_blocked"] = bool(filter_blocked)
    payload["entry_blocked"] = bool(entry_blocked)
    payload["entry_block_reason"] = entry_block_reason_text
    payload["signal_strength_label"] = signal_strength_label
    payload["market_observations"] = market_observations
    payload["dust_classification"] = dust_classification
    payload["entry_allowed"] = bool(entry_allowed)
    payload["entry_block_reason"] = entry_block_reason_text or entry_block_reason
    payload["effective_flat"] = bool(effective_flat)
    payload["entry_gate_effective_flat"] = bool(entry_gate_effective_flat)
    payload["holding_authority_state"] = holding_authority_state
    payload["raw_qty_open"] = float(raw_qty_open)
    payload["raw_total_asset_qty"] = float(raw_total_asset_qty)
    payload["position_qty"] = float(position_qty)
    payload["submit_payload_qty"] = float(submit_payload_qty)
    payload["submit_lot_count"] = int(submit_lot_count)
    payload["position_state_lot_count"] = int(submit_lot_count)
    payload["normalized_exposure_active"] = bool(normalized_exposure_active)
    payload["normalized_exposure_qty"] = float(normalized_exposure_qty)
    payload["has_executable_exposure"] = bool(has_executable_exposure)
    payload["has_any_position_residue"] = bool(has_any_position_residue)
    payload["has_non_executable_residue"] = bool(has_non_executable_residue)
    payload["has_dust_only_remainder"] = bool(has_dust_only_remainder)
    payload["recovery_blocked"] = bool(recovery_blocked)
    payload["recovery_block_reason"] = recovery_block_reason
    payload["unresolved_order_count"] = int(unresolved_order_count)
    payload["recovery_required_count"] = int(recovery_required_count)
    payload["open_exposure_qty"] = float(open_exposure_qty)
    payload["dust_tracking_qty"] = float(dust_tracking_qty)
    payload["open_lot_count"] = int(open_lot_count)
    payload["dust_tracking_lot_count"] = int(dust_tracking_lot_count)
    payload["reserved_exit_lot_count"] = int(reserved_exit_lot_count)
    payload["sellable_executable_lot_count"] = int(sellable_executable_lot_count)
    payload["reserved_exit_qty"] = float(reserved_exit_qty)
    payload["sellable_executable_qty"] = float(sellable_executable_qty)
    payload["sell_submit_lot_count"] = int(sell_submit_lot_count)
    payload["exit_allowed"] = bool(exit_allowed)
    payload["exit_block_reason"] = exit_block_reason
    payload["sell_qty_basis_qty"] = float(sell_qty_basis_qty)
    payload["sell_qty_boundary_kind"] = sell_qty_boundary_kind
    payload["sell_normalized_exposure_qty"] = float(sell_normalized_exposure_qty)
    payload["sell_open_exposure_qty"] = float(sell_open_exposure_qty)
    payload["sell_dust_tracking_qty"] = float(sell_dust_tracking_qty)
    payload["sell_failure_category"] = sell_failure_category
    payload["sell_failure_detail"] = sell_failure_detail
    payload["decision_summary"] = decision_summary

    payload["strategy_name"] = _as_text(payload.get("strategy_name", strategy_name), default=strategy_name)
    payload["pair"] = _as_text(payload.get("pair", pair), default=pair)
    payload["interval"] = _as_text(payload.get("interval", interval), default=interval)
    payload["decision_ts"] = int(decision_ts)
    payload["candle_ts"] = None if candle_ts is None else int(candle_ts)
    payload["market_price"] = None if market_price is None else float(market_price)
    payload["signal"] = _as_text(payload.get("signal", signal), default=signal)
    payload["reason"] = _as_text(payload.get("reason", reason), default=reason)
    payload["base_signal"] = base_signal
    payload["entry_signal"] = _as_text(payload.get("entry_signal", entry.get("entry_signal", signal)), default=signal)

    payload["blocked_candidate"] = bool(decision_type == "BLOCKED_ENTRY")
    if entry_block_reason_text and entry_block_reason_text not in block_reason_hierarchy:
        block_reason_hierarchy.append(entry_block_reason_text)
    payload["block_reason"] = block_reason_hierarchy[0] if block_reason_hierarchy else entry_block_reason_text
    payload["block_reason_hierarchy"] = block_reason_hierarchy
    raw_holdings = dict(position_state.get("raw_holdings")) if isinstance(position_state.get("raw_holdings"), dict) else {}
    raw_holdings.setdefault("classification", dust_classification)
    present = dust_classification not in {"", "no_dust"}
    raw_holdings.setdefault("present", present)
    raw_holdings.setdefault("broker_local_match", bool(position_gate.get("dust_broker_local_match", False)))
    raw_holdings.setdefault("compact_summary", str(position_gate.get("dust_residual_summary") or "none"))

    normalized_position_state = dict(position_state)
    normalized_position_state["raw_holdings"] = {
        **raw_holdings,
        "classification": dust_classification,
        "present": present,
        "broker_qty": float(position_gate.get("dust_broker_qty", 0.0) or 0.0),
        "local_qty": float(position_gate.get("dust_local_qty", 0.0) or 0.0),
        "delta_qty": float(position_gate.get("dust_delta_qty", 0.0) or 0.0),
        "min_qty": float(position_gate.get("dust_min_qty", 0.0) or 0.0),
        "min_notional_krw": float(position_gate.get("dust_min_notional_krw", 0.0) or 0.0),
        "broker_local_match": bool(position_gate.get("dust_broker_local_match", False)),
        "compact_summary": str(position_gate.get("dust_residual_summary") or "none"),
    }
    normalized_position_state["normalized_exposure"] = {
        **(dict(position_normalized) if isinstance(position_normalized, dict) else {}),
        "raw_qty_open": float(raw_qty_open),
        "raw_total_asset_qty": float(raw_total_asset_qty),
        "dust_classification": dust_classification,
        "dust_state": dust_classification,
        "entry_allowed": bool(entry_allowed),
        "effective_flat": bool(effective_flat),
        "entry_gate_effective_flat": bool(entry_gate_effective_flat),
        "harmless_dust_effective_flat": bool(entry_allowed and dust_classification == "harmless_dust"),
        "effective_flat_due_to_harmless_dust": bool(entry_allowed and dust_classification == "harmless_dust"),
        "holding_authority_state": holding_authority_state,
        "normalized_exposure_active": bool(normalized_exposure_active),
        "normalized_exposure_qty": float(normalized_exposure_qty),
        "has_executable_exposure": bool(has_executable_exposure),
        "has_any_position_residue": bool(has_any_position_residue),
        "has_non_executable_residue": bool(has_non_executable_residue),
        "has_dust_only_remainder": bool(has_dust_only_remainder),
        "recovery_blocked": bool(recovery_blocked),
        "recovery_block_reason": recovery_block_reason,
        "unresolved_order_count": int(unresolved_order_count),
        "recovery_required_count": int(recovery_required_count),
        "open_exposure_qty": float(open_exposure_qty),
        "dust_tracking_qty": float(dust_tracking_qty),
        "open_lot_count": int(open_lot_count),
        "dust_tracking_lot_count": int(dust_tracking_lot_count),
        "reserved_exit_lot_count": int(reserved_exit_lot_count),
        "sellable_executable_lot_count": int(sellable_executable_lot_count),
        "submit_lot_count": int(submit_lot_count),
        "position_state_lot_count": int(submit_lot_count),
        "reserved_exit_qty": float(reserved_exit_qty),
        "sellable_executable_qty": float(sellable_executable_qty),
        "exit_allowed": bool(exit_allowed),
        "exit_block_reason": exit_block_reason,
        "entry_block_reason": entry_block_reason,
    }
    normalized_position_state["operator_diagnostics"] = {
        **(
            dict(position_state.get("operator_diagnostics"))
            if isinstance(position_state.get("operator_diagnostics"), dict)
            else {}
        ),
        "state": dust_classification or "no_dust",
        "state_label": (
            "harmless dust residual"
            if dust_classification == "harmless_dust"
            else "blocking dust residual requires manual review"
            if present
            else "no dust residual"
        ),
        "operator_action": str(position_gate.get("dust_operator_action") or "-"),
        "operator_message": str(position_gate.get("dust_operator_message") or "-"),
        "broker_local_match": bool(position_gate.get("dust_broker_local_match", False)),
        "new_orders_allowed": bool(position_gate.get("dust_new_orders_allowed", False)),
        "resume_allowed": bool(position_gate.get("dust_resume_allowed_by_policy", False)),
        "treat_as_flat": bool(position_gate.get("dust_treat_as_flat", False)),
    }
    payload["position_state"] = normalized_position_state

    return _strip_declaration_residue(payload)
