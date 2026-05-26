from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

POSITIVE_EQUIVALENCE_STATE_CLASSES = frozenset(
    {
        "flat_no_dust_no_position",
        "open_exposure",
    }
)
LOT_NATIVE_RESEARCH_POSITION_MODEL = "lot_native_simulation_v1"
LEGACY_RESEARCH_POSITION_MODEL = "cash_qty_simulation_v1"
RESEARCH_LOT_NATIVE_AUTHORITY_REQUIRED_FIELDS = (
    "open_lot_count",
    "sellable_executable_lot_count",
    "reserved_exit_lot_count",
    "dust_tracking_lot_count",
    "open_exposure_qty",
    "sellable_executable_qty",
    "reserved_exit_qty",
    "dust_tracking_qty",
    "terminal_state",
    "order_rules_hash",
    "fee_authority_hash",
    "position_state_hash",
)


@dataclass(frozen=True)
class PositionAuthoritySnapshot:
    raw_total_asset_qty: float
    open_lot_count: int
    dust_tracking_lot_count: int
    reserved_exit_lot_count: int
    sellable_executable_lot_count: int
    open_exposure_qty: float
    dust_tracking_qty: float
    reserved_exit_qty: float
    sellable_executable_qty: float
    terminal_state: str
    entry_allowed: bool
    exit_allowed: bool
    recovery_blocked: bool
    recovery_block_reason: str
    order_rules_hash: str
    fee_authority_hash: str
    position_state_hash: str
    state_class: str
    unsupported_reason: str = ""
    research_position_model: str = ""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def runtime_position_authority_snapshot(
    *,
    position_gate: dict[str, Any],
    order_rules_hash: str,
    fee_authority_hash: str,
    position_state_hash: str,
) -> PositionAuthoritySnapshot:
    state_class = classify_runtime_position_state(position_gate)
    unsupported_reason = (
        ""
        if state_class in POSITIVE_EQUIVALENCE_STATE_CLASSES
        and _runtime_state_has_required_lot_native_fields(position_gate, state_class)
        else _runtime_unsupported_reason(position_gate)
    )
    return PositionAuthoritySnapshot(
        raw_total_asset_qty=_float(position_gate.get("raw_total_asset_qty")),
        open_lot_count=_int(position_gate.get("open_lot_count")),
        dust_tracking_lot_count=_int(position_gate.get("dust_tracking_lot_count")),
        reserved_exit_lot_count=_int(position_gate.get("reserved_exit_lot_count")),
        sellable_executable_lot_count=_int(position_gate.get("sellable_executable_lot_count")),
        open_exposure_qty=_float(position_gate.get("open_exposure_qty")),
        dust_tracking_qty=_float(position_gate.get("dust_tracking_qty")),
        reserved_exit_qty=_float(position_gate.get("reserved_exit_qty")),
        sellable_executable_qty=_float(position_gate.get("sellable_executable_qty")),
        terminal_state=str(
            position_gate.get("terminal_state")
            or ("flat" if state_class == "flat_no_dust_no_position" else "")
        ),
        entry_allowed=bool(position_gate.get("entry_allowed")),
        exit_allowed=bool(position_gate.get("exit_allowed")),
        recovery_blocked=bool(position_gate.get("recovery_blocked")),
        recovery_block_reason=str(position_gate.get("recovery_block_reason") or "none"),
        order_rules_hash=order_rules_hash,
        fee_authority_hash=fee_authority_hash,
        position_state_hash=position_state_hash,
        state_class=state_class,
        unsupported_reason=unsupported_reason,
        research_position_model=(
            LOT_NATIVE_RESEARCH_POSITION_MODEL
            if not unsupported_reason and state_class in POSITIVE_EQUIVALENCE_STATE_CLASSES - {"flat_no_dust_no_position"}
            else ""
        ),
    )


def research_position_authority_snapshot(
    *,
    qty: float,
    sellable_qty: float,
    order_rules_hash: str,
    fee_authority_hash: str,
    position_state_hash: str,
) -> PositionAuthoritySnapshot:
    flat_no_position = float(qty) <= 0.0 and float(sellable_qty) <= 0.0
    if flat_no_position:
        state_class = "flat_no_dust_no_position"
        unsupported_reason = ""
        terminal_state = "flat"
    else:
        state_class = "research_model_lacks_lot_native_authority"
        unsupported_reason = "research_model_lacks_lot_native_authority"
        terminal_state = "research_not_modeled"
    return PositionAuthoritySnapshot(
        raw_total_asset_qty=max(0.0, float(qty)),
        open_lot_count=0,
        dust_tracking_lot_count=0,
        reserved_exit_lot_count=0,
        sellable_executable_lot_count=0,
        open_exposure_qty=max(0.0, float(qty)),
        dust_tracking_qty=0.0,
        reserved_exit_qty=max(0.0, float(qty) - float(sellable_qty)),
        sellable_executable_qty=max(0.0, float(sellable_qty)),
        terminal_state=terminal_state,
        entry_allowed=flat_no_position,
        exit_allowed=bool(float(sellable_qty) > 0.0),
        recovery_blocked=False,
        recovery_block_reason="none",
        order_rules_hash=order_rules_hash,
        fee_authority_hash=fee_authority_hash,
        position_state_hash=position_state_hash,
        state_class=state_class,
        unsupported_reason=unsupported_reason,
        research_position_model=LEGACY_RESEARCH_POSITION_MODEL,
    )


def research_lot_native_position_authority_snapshot(
    *,
    lot_native_fields: dict[str, Any],
    order_rules_hash: str,
    fee_authority_hash: str,
    position_state_hash: str,
) -> PositionAuthoritySnapshot:
    fields = dict(lot_native_fields)
    state_class = str(fields.get("terminal_state") or fields.get("state_class") or "").strip()
    if state_class == "flat":
        state_class = "flat_no_dust_no_position"
    if bool(fields.get("recovery_blocked")):
        state_class = "recovery_blocked"
    unsupported_reason = (
        ""
        if _research_lot_native_fields_support_positive_equivalence(
            fields,
            state_class=state_class,
            order_rules_hash=order_rules_hash,
            fee_authority_hash=fee_authority_hash,
            position_state_hash=position_state_hash,
        )
        else _research_lot_native_unsupported_reason(state_class)
    )
    return PositionAuthoritySnapshot(
        raw_total_asset_qty=max(
            0.0,
            _float(fields.get("raw_total_asset_qty"))
            or _float(fields.get("open_exposure_qty")) + _float(fields.get("dust_tracking_qty")),
        ),
        open_lot_count=_int(fields.get("open_lot_count")),
        dust_tracking_lot_count=_int(fields.get("dust_tracking_lot_count")),
        reserved_exit_lot_count=_int(fields.get("reserved_exit_lot_count")),
        sellable_executable_lot_count=_int(fields.get("sellable_executable_lot_count")),
        open_exposure_qty=_float(fields.get("open_exposure_qty")),
        dust_tracking_qty=_float(fields.get("dust_tracking_qty")),
        reserved_exit_qty=_float(fields.get("reserved_exit_qty")),
        sellable_executable_qty=_float(fields.get("sellable_executable_qty")),
        terminal_state=str(fields.get("terminal_state") or state_class),
        entry_allowed=(
            bool(fields.get("entry_allowed"))
            if "entry_allowed" in fields
            else state_class == "flat_no_dust_no_position"
        ),
        exit_allowed=(
            bool(fields.get("exit_allowed"))
            if "exit_allowed" in fields
            else _int(fields.get("sellable_executable_lot_count")) > 0
        ),
        recovery_blocked=bool(fields.get("recovery_blocked")),
        recovery_block_reason=str(fields.get("recovery_block_reason") or "none"),
        order_rules_hash=order_rules_hash,
        fee_authority_hash=fee_authority_hash,
        position_state_hash=position_state_hash,
        state_class=state_class or "research_model_lacks_lot_native_authority",
        unsupported_reason=unsupported_reason,
        research_position_model=(
            LOT_NATIVE_RESEARCH_POSITION_MODEL
            if not unsupported_reason and state_class in POSITIVE_EQUIVALENCE_STATE_CLASSES
            else f"{LOT_NATIVE_RESEARCH_POSITION_MODEL}_partial"
        ),
    )


def lot_native_comparison_position_state(fields: dict[str, Any]) -> dict[str, object]:
    state_class = str(fields.get("state_class") or fields.get("terminal_state") or "").strip()
    if state_class == "flat":
        state_class = "flat_no_dust_no_position"
    if state_class == "flat_no_dust_no_position":
        return {
            "comparison_state": "flat_no_dust_no_position",
            "entry_allowed": True,
            "exit_allowed": False,
            "dust_state": "flat",
            "effective_flat": True,
            "normalized_exposure_active": False,
        }
    return {
        "comparison_state": state_class,
        "entry_allowed": bool(fields.get("entry_allowed")),
        "exit_allowed": bool(fields.get("exit_allowed")),
        "dust_state": str(fields.get("dust_state") or ""),
        "effective_flat": bool(fields.get("effective_flat")),
        "normalized_exposure_active": bool(fields.get("normalized_exposure_active")),
        "raw_total_asset_qty": _float(fields.get("raw_total_asset_qty")),
        "open_lot_count": _int(fields.get("open_lot_count")),
        "dust_tracking_lot_count": _int(fields.get("dust_tracking_lot_count")),
        "reserved_exit_lot_count": _int(fields.get("reserved_exit_lot_count")),
        "sellable_executable_lot_count": _int(fields.get("sellable_executable_lot_count")),
        "open_exposure_qty": _float(fields.get("open_exposure_qty")),
        "dust_tracking_qty": _float(fields.get("dust_tracking_qty")),
        "reserved_exit_qty": _float(fields.get("reserved_exit_qty")),
        "sellable_executable_qty": _float(fields.get("sellable_executable_qty")),
        "terminal_state": str(fields.get("terminal_state") or state_class),
        "recovery_blocked": bool(fields.get("recovery_blocked")),
        "recovery_block_reason": str(fields.get("recovery_block_reason") or "none"),
    }


def position_authority_supports_positive_equivalence(decision: dict[str, Any]) -> bool:
    authority = decision.get("position_authority")
    if not isinstance(authority, dict):
        return False
    state_class = str(authority.get("state_class") or "").strip()
    if state_class not in POSITIVE_EQUIVALENCE_STATE_CLASSES:
        return False
    if str(authority.get("unsupported_reason") or "").strip():
        return False
    if state_class == "flat_no_dust_no_position":
        return True
    model = str(authority.get("research_position_model") or "").strip()
    return model in {"", LOT_NATIVE_RESEARCH_POSITION_MODEL}


def classify_runtime_position_state(position_gate: dict[str, Any]) -> str:
    terminal = str(position_gate.get("terminal_state") or "").strip()
    if bool(position_gate.get("recovery_blocked")):
        return "recovery_blocked"
    if terminal in {"open_exposure", "reserved_exit_pending", "dust_only", "non_executable_position", "flat"}:
        return (
            "flat_no_dust_no_position"
            if terminal == "flat" and _is_runtime_flat_no_dust(position_gate)
            else terminal
        )
    if _is_runtime_flat_no_dust(position_gate):
        return "flat_no_dust_no_position"
    if _int(position_gate.get("reserved_exit_lot_count")) > 0:
        return "reserved_exit_pending"
    if _int(position_gate.get("sellable_executable_lot_count")) > 0 or bool(
        position_gate.get("normalized_exposure_active")
    ):
        return "open_exposure"
    if bool(position_gate.get("has_dust_only_remainder")) or str(
        position_gate.get("dust_state") or ""
    ) not in {"", "flat", "no_dust"}:
        return "dust_only"
    if bool(position_gate.get("has_any_position_residue")):
        return "non_executable_position"
    return "runtime_position_state_not_research_comparable"


def runtime_state_has_required_lot_native_fields(
    position_gate: dict[str, Any],
    state_class: str | None = None,
) -> bool:
    return _runtime_state_has_required_lot_native_fields(
        position_gate,
        state_class or classify_runtime_position_state(position_gate),
    )


def classify_decision_position_state(decision: dict[str, Any], *, source: str) -> tuple[str, str]:
    snapshot = decision.get("position_authority")
    if isinstance(snapshot, dict):
        state_class = str(snapshot.get("state_class") or "").strip()
        reason = str(snapshot.get("unsupported_reason") or "").strip()
        if state_class:
            return state_class, reason
    if _is_decision_flat_no_dust(decision):
        return "flat_no_dust_no_position", ""
    dust_state = str(decision.get("dust_state") or "").strip()
    if dust_state == "research_not_modeled":
        return "research_model_lacks_lot_native_authority", "research_model_lacks_lot_native_authority"
    if dust_state in {"harmless_dust", "blocking_dust", "dust_only"}:
        return "dust_only", "research_model_lacks_dust_state" if source == "research" else ""
    if bool(decision.get("normalized_exposure_active")):
        return (
            "research_model_lacks_lot_native_authority"
            if source == "research"
            else "runtime_position_state_not_research_comparable"
        ), "research_model_lacks_lot_native_authority" if source == "research" else ""
    return (
        "runtime_position_state_not_research_comparable"
        if source == "runtime"
        else "research_model_lacks_lot_native_authority",
        "",
    )


def _runtime_unsupported_reason(position_gate: dict[str, Any]) -> str:
    state_class = classify_runtime_position_state(position_gate)
    if state_class == "dust_only":
        return "research_model_lacks_dust_state"
    if state_class in {"open_exposure", "reserved_exit_pending", "non_executable_position", "recovery_blocked"}:
        return "research_model_lacks_lot_native_authority"
    return "research_runtime_state_not_comparable"


def _runtime_state_has_required_lot_native_fields(position_gate: dict[str, Any], state_class: str) -> bool:
    if state_class == "flat_no_dust_no_position":
        return _is_runtime_flat_no_dust(position_gate)
    if state_class not in POSITIVE_EQUIVALENCE_STATE_CLASSES:
        return False
    required_fields = (
        "raw_total_asset_qty",
        "open_lot_count",
        "dust_tracking_lot_count",
        "reserved_exit_lot_count",
        "sellable_executable_lot_count",
        "open_exposure_qty",
        "dust_tracking_qty",
        "reserved_exit_qty",
        "sellable_executable_qty",
        "entry_allowed",
        "exit_allowed",
        "effective_flat",
        "normalized_exposure_active",
    )
    if any(field not in position_gate for field in required_fields):
        return False
    terminal_state = str(position_gate.get("terminal_state") or state_class).strip()
    if terminal_state not in {state_class, "flat" if state_class == "flat_no_dust_no_position" else state_class}:
        return False
    open_lots = _int(position_gate.get("open_lot_count"))
    dust_lots = _int(position_gate.get("dust_tracking_lot_count"))
    reserved_lots = _int(position_gate.get("reserved_exit_lot_count"))
    sellable_lots = _int(position_gate.get("sellable_executable_lot_count"))
    open_qty = _float(position_gate.get("open_exposure_qty"))
    dust_qty = _float(position_gate.get("dust_tracking_qty"))
    reserved_qty = _float(position_gate.get("reserved_exit_qty"))
    sellable_qty = _float(position_gate.get("sellable_executable_qty"))
    raw_qty = _float(position_gate.get("raw_total_asset_qty"))
    if min(open_lots, dust_lots, reserved_lots, sellable_lots) < 0:
        return False
    if min(open_qty, dust_qty, reserved_qty, sellable_qty, raw_qty) < 0.0:
        return False
    if dust_lots != 0 or dust_qty > 1e-12:
        return False
    if bool(position_gate.get("entry_allowed")) is not False:
        return False
    if bool(position_gate.get("effective_flat")) is not False:
        return False
    if bool(position_gate.get("normalized_exposure_active")) is not True:
        return False
    if state_class == "open_exposure":
        return (
            open_lots > 0
            and reserved_lots == 0
            and sellable_lots == open_lots
            and open_qty > 1e-12
            and reserved_qty <= 1e-12
            and sellable_qty > 1e-12
            and raw_qty + 1e-12 >= open_qty
            and bool(position_gate.get("exit_allowed")) is True
        )
    if state_class == "reserved_exit_pending":
        return (
            open_lots > 0
            and reserved_lots == open_lots
            and sellable_lots == 0
            and open_qty > 1e-12
            and reserved_qty > 1e-12
            and sellable_qty <= 1e-12
            and raw_qty + 1e-12 >= open_qty
            and bool(position_gate.get("exit_allowed")) is False
        )
    return False


def _research_lot_native_fields_support_positive_equivalence(
    fields: dict[str, Any],
    *,
    state_class: str,
    order_rules_hash: str,
    fee_authority_hash: str,
    position_state_hash: str,
) -> bool:
    if state_class not in POSITIVE_EQUIVALENCE_STATE_CLASSES:
        return False
    if any(field not in fields for field in RESEARCH_LOT_NATIVE_AUTHORITY_REQUIRED_FIELDS):
        return False
    if str(fields.get("order_rules_hash") or "") != str(order_rules_hash):
        return False
    if str(fields.get("fee_authority_hash") or "") != str(fee_authority_hash):
        return False
    if str(fields.get("position_state_hash") or "") != str(position_state_hash):
        return False
    open_lots = _int(fields.get("open_lot_count"))
    sellable_lots = _int(fields.get("sellable_executable_lot_count"))
    reserved_lots = _int(fields.get("reserved_exit_lot_count"))
    dust_lots = _int(fields.get("dust_tracking_lot_count"))
    open_qty = _float(fields.get("open_exposure_qty"))
    sellable_qty = _float(fields.get("sellable_executable_qty"))
    reserved_qty = _float(fields.get("reserved_exit_qty"))
    dust_qty = _float(fields.get("dust_tracking_qty"))
    if min(open_lots, sellable_lots, reserved_lots, dust_lots) < 0:
        return False
    if min(open_qty, sellable_qty, reserved_qty, dust_qty) < 0.0:
        return False
    if bool(fields.get("recovery_blocked")):
        return False
    if state_class == "flat_no_dust_no_position":
        return (
            open_lots == 0
            and sellable_lots == 0
            and reserved_lots == 0
            and dust_lots == 0
            and open_qty <= 1e-12
            and sellable_qty <= 1e-12
            and reserved_qty <= 1e-12
            and dust_qty <= 1e-12
        )
    if state_class == "open_exposure":
        return (
            str(fields.get("terminal_state") or "") == "open_exposure"
            and open_lots > 0
            and sellable_lots == open_lots
            and reserved_lots == 0
            and dust_lots == 0
            and open_qty > 1e-12
            and sellable_qty > 1e-12
            and reserved_qty <= 1e-12
            and dust_qty <= 1e-12
        )
    return False


def _research_lot_native_unsupported_reason(state_class: str) -> str:
    if state_class == "dust_only":
        return "research_model_lacks_dust_state"
    if state_class in {
        "open_exposure",
        "reserved_exit_pending",
        "non_executable_position",
        "recovery_blocked",
        "flat_no_dust_no_position",
    }:
        return "research_model_lacks_lot_native_authority"
    return "research_runtime_state_not_comparable"


def _is_runtime_flat_no_dust(position_gate: dict[str, Any]) -> bool:
    return (
        bool(position_gate.get("entry_allowed")) is True
        and bool(position_gate.get("exit_allowed")) is False
        and str(position_gate.get("dust_state") or "") in {"flat", "no_dust"}
        and bool(position_gate.get("effective_flat")) is True
        and bool(position_gate.get("normalized_exposure_active")) is False
        and _int(position_gate.get("open_lot_count")) == 0
        and _int(position_gate.get("dust_tracking_lot_count")) == 0
        and _int(position_gate.get("sellable_executable_lot_count")) == 0
        and bool(position_gate.get("has_any_position_residue")) is False
    )


def _is_decision_flat_no_dust(decision: dict[str, Any]) -> bool:
    return (
        bool(decision.get("entry_allowed")) is True
        and bool(decision.get("exit_allowed")) is False
        and str(decision.get("dust_state") or "") in {"flat", "no_dust"}
        and bool(decision.get("effective_flat")) is True
        and bool(decision.get("normalized_exposure_active")) is False
    )


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
