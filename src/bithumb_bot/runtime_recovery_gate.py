from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .reason_codes import (
    BLOCKER_SUBMIT_UNKNOWN_RECOVERY_REQUIRED,
    BLOCKER_TRADE_FILL_UNRESOLVED,
)


@dataclass(frozen=True)
class ResumeGatePreparation:
    startup_gate_reason: str | None
    initial_reconcile_halt_cleared: bool
    live_execution_broker_halt_cleared: bool
    risk_state_mismatch_halt_cleared: bool


@dataclass(frozen=True)
class ResumeBlocker:
    code: str
    detail: str
    reason_code: str
    summary: str
    overridable: bool
    balance_delta_krw: float | None = None
    recent_external_cash_adjustment_present: bool | None = None
    recent_external_cash_adjustment_count: int | None = None


def resume_blocker(
    *,
    code: str,
    detail: str,
    overridable: bool,
    reason_code: str | None = None,
    summary: str | None = None,
    balance_delta_krw: float | None = None,
    recent_external_cash_adjustment_present: bool | None = None,
    recent_external_cash_adjustment_count: int | None = None,
) -> ResumeBlocker:
    return ResumeBlocker(
        code=code,
        detail=detail,
        reason_code=str(reason_code or code),
        summary=str(summary or detail),
        overridable=overridable,
        balance_delta_krw=balance_delta_krw,
        recent_external_cash_adjustment_present=recent_external_cash_adjustment_present,
        recent_external_cash_adjustment_count=recent_external_cash_adjustment_count,
    )


def classify_startup_gate_reason(
    startup_gate_reason: str | None,
    *,
    state,
) -> tuple[str, str]:
    reason = str(startup_gate_reason or "").strip()
    if not reason:
        return "-", "no startup gate blocker"
    if "position_authority_gap=" in reason:
        return (
            "POSITION_AUTHORITY_RECOVERY_REQUIRED",
            "lot authority is missing; manual recovery required",
        )
    if "position_authority_correction_required=" in reason:
        return (
            "POSITION_AUTHORITY_CORRECTION_REQUIRED",
            "lot authority conflicts with accounted fill evidence; authority correction required",
        )
    if "position_authority_residual_normalization_required=" in reason:
        return (
            "POSITION_AUTHORITY_RESIDUAL_NORMALIZATION_REQUIRED",
            "lot authority needs post-partial-close residual normalization",
        )
    if "position_authority_projection_repair_required=" in reason:
        return (
            "POSITION_AUTHORITY_PROJECTION_REPAIR_REQUIRED",
            "lot projection conflicts with broker/portfolio evidence; projection repair required",
        )
    if "position_authority_projection_convergence_required=" in reason:
        return (
            "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED",
            "aggregate lot projection does not converge to canonical holdings",
        )
    if "fee_gap_recovery_required=" in reason:
        return (
            "FEE_GAP_RECOVERY_REQUIRED",
            "fee-related accounting inconsistency requires manual recovery",
        )
    if "fee_pending_auto_recovering=" in reason:
        return (
            "FEE_PENDING_AUTO_RECOVERING",
            "fee-pending fill accounting is still auto-recovering",
        )
    if int(getattr(state, "recovery_required_count", 0)) > 0 or "recovery_required_orders=" in reason:
        return (
            BLOCKER_SUBMIT_UNKNOWN_RECOVERY_REQUIRED,
            "recovery-required orders remain",
        )
    if "submit_unknown_orders=" in reason:
        return (
            BLOCKER_SUBMIT_UNKNOWN_RECOVERY_REQUIRED,
            "submit unknown orders remain",
        )
    if (
        "pending_submit_orders=" in reason
        or "unresolved_open_orders=" in reason
        or "stale_new_partial_orders=" in reason
    ):
        return (
            BLOCKER_TRADE_FILL_UNRESOLVED,
            "trade/fill state remains unresolved",
        )
    return (
        BLOCKER_TRADE_FILL_UNRESOLVED,
        "startup safety gate blocked",
    )


@dataclass(frozen=True)
class RuntimeRecoveryGateService:
    """Small service boundary for recovery/readiness gate orchestration."""

    startup_gate_evaluator: Callable[[], str | None]
    stale_initial_reconcile_halt_clearer: Callable[[], bool]
    stale_live_execution_broker_halt_clearer: Callable[..., bool]
    stale_risk_state_mismatch_halt_clearer: Callable[..., bool]
    state_snapshot: Callable[[], object]

    def prepare_resume_gate(self) -> ResumeGatePreparation:
        initial_cleared = bool(self.stale_initial_reconcile_halt_clearer())
        startup_gate_reason = self.startup_gate_evaluator()
        broker_cleared = bool(
            self.stale_live_execution_broker_halt_clearer(
                startup_gate_reason=startup_gate_reason
            )
        )
        risk_cleared = bool(
            self.stale_risk_state_mismatch_halt_clearer(
                startup_gate_reason=startup_gate_reason
            )
        )
        startup_gate_reason = self.startup_gate_evaluator()
        return ResumeGatePreparation(
            startup_gate_reason=startup_gate_reason,
            initial_reconcile_halt_cleared=initial_cleared,
            live_execution_broker_halt_cleared=broker_cleared,
            risk_state_mismatch_halt_cleared=risk_cleared,
        )

    def startup_safety_resume_blockers(self, startup_gate_reason: str | None) -> list[object]:
        if not startup_gate_reason:
            return []
        state = self.state_snapshot()
        reason_code, summary = classify_startup_gate_reason(
            startup_gate_reason,
            state=state,
        )
        return [
            resume_blocker(
                code="STARTUP_SAFETY_GATE_BLOCKED",
                detail=startup_gate_reason,
                reason_code=reason_code,
                summary=summary,
                overridable=False,
            )
        ]

    def reconcile_ok_did_not_clear_blockers(
        self,
        startup_gate_reason: str | None,
    ) -> list[object]:
        if not startup_gate_reason:
            return []
        state = self.state_snapshot()
        if getattr(state, "last_reconcile_status", None) != "ok":
            return []
        reason_code, summary = classify_startup_gate_reason(
            startup_gate_reason,
            state=state,
        )
        if reason_code == "FEE_GAP_RECOVERY_REQUIRED":
            return []
        return [
            resume_blocker(
                code="LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS",
                detail="latest reconcile reported ok but startup safety gate still blocks resume",
                reason_code=reason_code,
                summary=summary,
                overridable=False,
            )
        ]
