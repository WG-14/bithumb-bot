from __future__ import annotations

from dataclasses import dataclass
from typing import Any


_EPS = 1e-12


@dataclass(frozen=True)
class CanonicalRecoveryState:
    canonical_state: str
    execution_flat: bool
    accounting_flat: bool
    closeout_blocking_residue: bool
    residue_kind: str
    operator_next_action: str
    policy_reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "canonical_state": self.canonical_state,
            "execution_flat": bool(self.execution_flat),
            "accounting_flat": bool(self.accounting_flat),
            "closeout_blocking_residue": bool(self.closeout_blocking_residue),
            "residue_kind": self.residue_kind,
            "operator_next_action": self.operator_next_action,
            "policy_reason": self.policy_reason,
        }


@dataclass(frozen=True)
class CanonicalTradeabilityState:
    canonical_state: str
    residual_class: str
    run_loop_allowed: bool
    new_entry_allowed: bool
    closeout_allowed: bool
    execution_flat: bool
    accounting_flat: bool
    effective_flat: bool
    operator_action_required: bool
    why_not: str
    operator_next_action: str

    def as_dict(self) -> dict[str, object]:
        return {
            "canonical_state": self.canonical_state,
            "residual_class": self.residual_class,
            "run_loop_allowed": bool(self.run_loop_allowed),
            "new_entry_allowed": bool(self.new_entry_allowed),
            "closeout_allowed": bool(self.closeout_allowed),
            "execution_flat": bool(self.execution_flat),
            "accounting_flat": bool(self.accounting_flat),
            "effective_flat": bool(self.effective_flat),
            "operator_action_required": bool(self.operator_action_required),
            "why_not": self.why_not,
            "operator_next_action": self.operator_next_action,
        }


def classify_canonical_tradeability_state(
    *,
    position_state: Any,
    recovery_state: CanonicalRecoveryState,
    run_loop_allowed: bool,
) -> CanonicalTradeabilityState:
    """Classify run-loop and trading permissions from one canonical state.

    `resume_ready` only answers whether the bot can run. This policy object
    keeps that distinct from opening a new position or submitting a closeout.
    """

    normalized = position_state.normalized_exposure
    terminal_state = str(getattr(normalized, "terminal_state", "unknown") or "unknown")
    entry_allowed = bool(getattr(normalized, "entry_allowed", False))
    exit_allowed = bool(getattr(normalized, "exit_allowed", False))
    effective_flat = bool(getattr(normalized, "effective_flat", False))
    has_dust_only = bool(getattr(normalized, "has_dust_only_remainder", False))
    has_non_executable = bool(getattr(normalized, "has_non_executable_residue", False))
    has_executable = bool(getattr(normalized, "has_executable_exposure", False))
    dust_state = str(getattr(normalized, "dust_state", "no_dust") or "no_dust")
    entry_block_reason = str(getattr(normalized, "entry_block_reason", "none") or "none")
    exit_block_reason = str(getattr(normalized, "exit_block_reason", "none") or "none")

    if has_executable or terminal_state in {"open_exposure", "reserved_exit_pending"}:
        residual_class = "EXECUTABLE_OPEN_EXPOSURE"
    elif has_dust_only and entry_allowed:
        residual_class = "HARMLESS_DUST_TREAT_AS_FLAT"
    elif has_dust_only:
        residual_class = "TRACKED_DUST_BLOCK_NEW_ENTRY"
    elif has_non_executable:
        residual_class = "NON_EXECUTABLE_RESIDUE_REQUIRES_OPERATOR_ACTION"
    elif recovery_state.accounting_flat and recovery_state.execution_flat:
        residual_class = "NONE"
    else:
        residual_class = "NON_EXECUTABLE_RESIDUE_REQUIRES_OPERATOR_ACTION"

    new_entry_allowed = bool(run_loop_allowed and entry_allowed)
    closeout_allowed = bool(run_loop_allowed and exit_allowed)
    operator_action_required = False
    operator_next_action = str(recovery_state.operator_next_action or "review_recovery_report")
    reasons: list[str] = []

    if not run_loop_allowed:
        reasons.append("run_loop_blocked")
        operator_action_required = True
    if not new_entry_allowed:
        reasons.append(f"new_entry_blocked:{entry_block_reason}")
    if not closeout_allowed and residual_class != "NONE":
        reasons.append(f"closeout_blocked:{exit_block_reason}")

    if residual_class == "TRACKED_DUST_BLOCK_NEW_ENTRY":
        operator_action_required = True
        operator_next_action = "review_tracked_dust_before_new_entry"
    elif residual_class == "NON_EXECUTABLE_RESIDUE_REQUIRES_OPERATOR_ACTION":
        operator_action_required = True
        operator_next_action = "review_non_executable_residue"
    elif residual_class == "EXECUTABLE_OPEN_EXPOSURE":
        operator_next_action = "manage_or_flatten_open_position"
    elif residual_class == "HARMLESS_DUST_TREAT_AS_FLAT":
        operator_next_action = "resume_or_continue_new_entries_allowed"
    elif residual_class == "NONE":
        operator_next_action = "resume_or_continue"

    if dust_state not in {"", "no_dust"} and residual_class not in {
        "HARMLESS_DUST_TREAT_AS_FLAT",
        "TRACKED_DUST_BLOCK_NEW_ENTRY",
    }:
        reasons.append(f"dust_state={dust_state}")

    return CanonicalTradeabilityState(
        canonical_state=str(recovery_state.canonical_state),
        residual_class=residual_class,
        run_loop_allowed=bool(run_loop_allowed),
        new_entry_allowed=new_entry_allowed,
        closeout_allowed=closeout_allowed,
        execution_flat=bool(recovery_state.execution_flat),
        accounting_flat=bool(recovery_state.accounting_flat),
        effective_flat=effective_flat,
        operator_action_required=operator_action_required,
        why_not="none" if not reasons else ";".join(reasons),
        operator_next_action=operator_next_action,
    )


def classify_canonical_recovery_state(
    *,
    position_state: Any,
    lot_snapshot: Any,
    portfolio_asset_qty: float,
    reserved_exit_qty: float,
) -> CanonicalRecoveryState:
    """Classify recovery posture from shared lot-native authority.

    Execution flatness answers whether normal SELL/flatten has executable
    inventory. Accounting flatness answers whether local accounting has no
    residual asset evidence. Dust-only tracking is therefore execution-flat but
    not accounting-flat.
    """

    normalized = position_state.normalized_exposure
    open_lot_count = int(getattr(lot_snapshot, "open_lot_count", 0) or 0)
    dust_lot_count = int(getattr(lot_snapshot, "dust_tracking_lot_count", 0) or 0)
    sellable_lot_count = int(getattr(normalized, "sellable_executable_lot_count", 0) or 0)
    has_executable = bool(getattr(normalized, "has_executable_exposure", False) or sellable_lot_count > 0)
    has_position_residue = bool(
        abs(float(portfolio_asset_qty)) > _EPS
        or abs(float(getattr(lot_snapshot, "raw_total_asset_qty", 0.0) or 0.0)) > _EPS
        or open_lot_count > 0
        or dust_lot_count > 0
        or abs(float(reserved_exit_qty)) > _EPS
    )
    accounting_flat = bool(
        abs(float(portfolio_asset_qty)) <= _EPS
        and open_lot_count <= 0
        and dust_lot_count <= 0
        and abs(float(reserved_exit_qty)) <= _EPS
    )

    authority_gap_reason = str(getattr(normalized, "authority_gap_reason", "none") or "none")
    terminal_state = str(getattr(normalized, "terminal_state", "unknown") or "unknown")
    if authority_gap_reason == "authority_missing_recovery_required":
        return CanonicalRecoveryState(
            canonical_state="AUTHORITY_MISSING",
            execution_flat=False,
            accounting_flat=accounting_flat,
            closeout_blocking_residue=True,
            residue_kind="authority_missing",
            operator_next_action="rebuild_position_authority",
            policy_reason="position residue exists without lot-native executable authority",
        )

    if has_executable or open_lot_count > 0:
        return CanonicalRecoveryState(
            canonical_state="OPEN_EXECUTABLE",
            execution_flat=False,
            accounting_flat=accounting_flat,
            closeout_blocking_residue=True,
            residue_kind="executable_exposure",
            operator_next_action="manage_or_flatten_open_position",
            policy_reason="lot-native executable exposure remains",
        )

    if dust_lot_count > 0 and open_lot_count <= 0:
        return CanonicalRecoveryState(
            canonical_state="DUST_ONLY_TRACKED",
            execution_flat=True,
            accounting_flat=False,
            closeout_blocking_residue=False,
            residue_kind="tracked_dust",
            operator_next_action="resolve_historical_accounting_debt_or_resume_if_policy_allows",
            policy_reason="tracked dust is non-executable operator evidence, not SELL authority",
        )

    if has_position_residue:
        return CanonicalRecoveryState(
            canonical_state="OPEN_NON_EXECUTABLE",
            execution_flat=True,
            accounting_flat=accounting_flat,
            closeout_blocking_residue=True,
            residue_kind=terminal_state,
            operator_next_action="review_recovery_report",
            policy_reason="non-executable residue exists outside explicit tracked-dust semantics",
        )

    return CanonicalRecoveryState(
        canonical_state="FLAT",
        execution_flat=True,
        accounting_flat=True,
        closeout_blocking_residue=False,
        residue_kind="none",
        operator_next_action="resume_now",
        policy_reason="no executable or accounting residue is present",
    )
