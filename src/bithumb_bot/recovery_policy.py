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
