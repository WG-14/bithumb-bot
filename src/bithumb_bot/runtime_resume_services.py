from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from typing import Callable, Sequence

from .config import settings
from .db_core import ensure_db, get_external_cash_adjustment_summary
from .dust import DustClassification, DustState, build_dust_display_context
from .operator_repair_service import OperatorRepairService
from .reason_codes import (
    BLOCKER_BROKER_CASH_DELTA_UNEXPLAINED,
    BLOCKER_DUST_RESIDUAL,
    BLOCKER_EXTERNAL_CASH_ADJUSTMENT_REQUIRED,
    BLOCKER_PORTFOLIO_BROKER_CASH_MISMATCH,
    POSITION_LOSS_LIMIT,
)
from .runtime_readiness import compute_runtime_readiness_snapshot
from .runtime_recovery_gate import ResumeBlocker, RuntimeRecoveryGateService, resume_blocker
from . import runtime_state


RISK_EXPOSURE_HALT_REASON_CODES = {
    "KILL_SWITCH",
    "DAILY_LOSS_LIMIT",
    POSITION_LOSS_LIMIT,
}


@dataclass(frozen=True)
class ResumeGuidance:
    operator_next_action: str
    recommended_command: str
    recommended_next_action: str
    resume_blocked_reason: str
    blocker_summary: str
    active_blocker_summary: str
    risk_level: str
    primary_blocker_code: str
    primary_blocker_reason_code: str
    blocker_summary_view: list[dict[str, object]]
    blockers: list[dict[str, object]]
    non_overridable_blockers: list[dict[str, object]]


def _resume_blocker(
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
    return resume_blocker(
        code=code,
        detail=detail,
        reason_code=str(reason_code or code),
        summary=str(summary or detail),
        overridable=overridable,
        balance_delta_krw=balance_delta_krw,
        recent_external_cash_adjustment_present=recent_external_cash_adjustment_present,
        recent_external_cash_adjustment_count=recent_external_cash_adjustment_count,
    )


def classify_dust_resume_blocker(dust_context: dict[str, object]) -> tuple[str, str]:
    if str(dust_context.get("classification") or "") == DustState.HARMLESS_DUST.value:
        return (
            BLOCKER_DUST_RESIDUAL,
            "harmless dust still needs policy review",
        )
    return (
        BLOCKER_DUST_RESIDUAL,
        "dust residual requires operator review",
    )


def extract_balance_split_delta_krw(summary: str) -> float | None:
    if not summary:
        return None
    match = re.search(
        r"cash_[a-z_]+\([^)]*delta=([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)\)",
        summary,
        re.IGNORECASE,
    )
    if match is None:
        return None
    try:
        value = float(match.group(1))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def reconcile_balance_split_mismatch_count(metadata_raw: str | None) -> int:
    if not metadata_raw:
        return 0
    try:
        reconcile_meta = json.loads(str(metadata_raw))
    except json.JSONDecodeError:
        return 0
    mismatch_raw = reconcile_meta.get("balance_split_mismatch_count", 0)
    try:
        return max(0, int(mismatch_raw))
    except (TypeError, ValueError):
        return 0


def reconcile_dust_context(metadata_raw: str | None) -> dict[str, object]:
    dust = DustClassification.from_metadata(metadata_raw)
    raw_holdings = dust.to_raw_holdings()
    return {
        "classification": raw_holdings.classification,
        "present": raw_holdings.present,
        "allow_resume": dust.allow_resume,
        "effective_flat": dust.effective_flat,
        "policy_reason": dust.policy_reason,
        "summary": raw_holdings.compact_summary,
    }


def dust_residual_resume_blocker(
    dust_context: dict[str, object],
) -> tuple[str, str] | None:
    if not bool(dust_context["present"]) or bool(dust_context["allow_resume"]):
        return None
    if str(dust_context.get("classification") or "") == DustState.HARMLESS_DUST.value:
        return (
            "HARMLESS_DUST_POLICY_REVIEW_REQUIRED",
            (
                "harmless dust is visible and treated as flat, but current policy still blocks resume/new orders: "
                f"policy={str(dust_context['policy_reason'])} "
                f"summary={str(dust_context['summary'])}"
            ),
        )
    return (
        "BLOCKING_DUST_REVIEW_REQUIRED",
        (
            "blocking dust residual requires operator review before resume: "
            f"policy={str(dust_context['policy_reason'])} "
            f"summary={str(dust_context['summary'])}"
        ),
    )


def classify_balance_split_blocker(metadata: dict[str, object]) -> ResumeBlocker | None:
    mismatch_count_raw = metadata.get("balance_split_mismatch_count", 0)
    try:
        mismatch_count = max(0, int(mismatch_count_raw))
    except (TypeError, ValueError):
        mismatch_count = 0
    if mismatch_count <= 0:
        return None

    summary = str(metadata.get("balance_split_mismatch_summary") or "").strip()
    external_cash_adjustment_count = 0
    try:
        external_cash_adjustment_count = max(
            0,
            int(metadata.get("external_cash_adjustment_count", 0) or 0),
        )
    except (TypeError, ValueError):
        external_cash_adjustment_count = 0
    delta_krw = extract_balance_split_delta_krw(summary)
    recent_external_cash_adjustment_present = external_cash_adjustment_count > 0

    cash_only_mismatch = (
        bool(summary)
        and ("cash_available" in summary or "cash_locked" in summary)
        and "asset_available" not in summary
        and "asset_locked" not in summary
    )
    if external_cash_adjustment_count <= 0 and cash_only_mismatch:
        return _resume_blocker(
            code=BLOCKER_EXTERNAL_CASH_ADJUSTMENT_REQUIRED,
            detail=(
                "balance split mismatch detected after reconcile: "
                f"count={mismatch_count} summary={summary or '-'} "
                f"external_cash_adjustment_present=0 delta_krw={delta_krw if delta_krw is not None else '-'}"
            ),
            reason_code=BLOCKER_BROKER_CASH_DELTA_UNEXPLAINED,
            summary="cash mismatch requires external cash adjustment evidence",
            overridable=False,
            balance_delta_krw=delta_krw,
            recent_external_cash_adjustment_present=False,
            recent_external_cash_adjustment_count=external_cash_adjustment_count,
        )

    if cash_only_mismatch and recent_external_cash_adjustment_present:
        blocker_summary = "cash split mismatch persists after external cash adjustment was recorded"
    else:
        blocker_summary = "portfolio cash split does not match broker snapshot"

    return _resume_blocker(
        code="BALANCE_SPLIT_MISMATCH",
        detail=(
            "balance split mismatch detected after reconcile: "
            f"count={mismatch_count} summary={summary or '-'} "
            f"external_cash_adjustment_present={1 if recent_external_cash_adjustment_present else 0} "
            f"delta_krw={delta_krw if delta_krw is not None else '-'}"
        ),
        reason_code=BLOCKER_PORTFOLIO_BROKER_CASH_MISMATCH,
        summary=blocker_summary,
        overridable=False,
        balance_delta_krw=delta_krw,
        recent_external_cash_adjustment_present=recent_external_cash_adjustment_present,
        recent_external_cash_adjustment_count=external_cash_adjustment_count,
    )


@dataclass(frozen=True)
class RuntimeResumeService:
    recovery_gate_factory: Callable[[], RuntimeRecoveryGateService]
    state_snapshot: Callable[[], object] = runtime_state.snapshot
    set_resume_gate: Callable[..., None] = runtime_state.set_resume_gate
    db_factory: Callable[[], object] = ensure_db
    repair_service: OperatorRepairService = OperatorRepairService()
    exposure_snapshot: Callable[[int], tuple[bool, bool]] | None = None
    ledger_external_cash_adjustment_summary: Callable[[], dict[str, object] | None] | None = None

    def evaluate_resume_eligibility(self) -> tuple[bool, list[ResumeBlocker]]:
        recovery_gate = self.recovery_gate_factory()
        resume_preparation = recovery_gate.prepare_resume_gate()
        startup_gate_reason = resume_preparation.startup_gate_reason
        state = self.state_snapshot()

        reasons: list[ResumeBlocker] = []
        if state.last_reconcile_status == "error":
            reasons.append(
                _resume_blocker(
                    code="LAST_RECONCILE_FAILED",
                    detail=(
                        "last reconcile failed: "
                        f"reason_code={state.last_reconcile_reason_code or '-'} "
                        f"error={state.last_reconcile_error or '-'}"
                    ),
                    reason_code="LAST_RECONCILE_FAILED",
                    summary="last reconcile failed",
                    overridable=False,
                )
            )

        if not startup_gate_reason and not state.halt_new_orders_blocked and not state.halt_state_unresolved:
            conn = self.db_factory()
            try:
                manual_flat_preview = self.repair_service.manual_flat_accounting_preview(conn)
            finally:
                conn.close()
            if bool(manual_flat_preview.get("safe_to_apply")):
                reasons.append(
                    _resume_blocker(
                        code="MANUAL_FLAT_ACCOUNTING_REPAIR_REQUIRED",
                        detail=(
                            f"manual-flat accounting repair pending: {manual_flat_preview.get('eligibility_reason')}; "
                            f"cash_delta={float(manual_flat_preview.get('cash_delta') or 0.0):.3f} "
                            f"asset_qty_delta={float(manual_flat_preview.get('asset_qty_delta') or 0.0):.10f}"
                        ),
                        reason_code="MANUAL_FLAT_ACCOUNTING_REPAIR_REQUIRED",
                        summary="manual-flat accounting repair required",
                        overridable=False,
                    )
                )

        reasons.extend(recovery_gate.startup_safety_resume_blockers(startup_gate_reason))
        readiness_snapshot = None
        residual_disposition = None
        try:
            conn = self.db_factory()
            try:
                readiness_snapshot = compute_runtime_readiness_snapshot(conn)
                residual_disposition = getattr(readiness_snapshot, "residual_disposition", None)
            finally:
                conn.close()
        except Exception:
            readiness_snapshot = None
            residual_disposition = None

        if state.emergency_flatten_blocked:
            reasons.append(
                _resume_blocker(
                    code="EMERGENCY_FLATTEN_UNRESOLVED",
                    detail=(
                        state.emergency_flatten_block_reason
                        or f"last_flatten_status={state.last_flatten_position_status or '-'}"
                    ),
                    reason_code="EMERGENCY_FLATTEN_UNRESOLVED",
                    summary="emergency flatten remains unresolved",
                    overridable=False,
                )
            )

        reasons.extend(recovery_gate.reconcile_ok_did_not_clear_blockers(startup_gate_reason))

        dust_context_for_halt = reconcile_dust_context(state.last_reconcile_metadata)
        residual_blocking_disposition = str(
            getattr(residual_disposition, "disposition", "") if residual_disposition is not None else ""
        )
        if (
            residual_disposition is not None
            and residual_blocking_disposition in {"BLOCKING_INCONSISTENT", "AUTHORITY_REPAIR_REQUIRED"}
            and not bool(getattr(residual_disposition, "run_allowed", False))
        ):
            reasons.append(
                _resume_blocker(
                    code=str(getattr(residual_disposition, "disposition", "BLOCKING_INCONSISTENT")),
                    detail=(
                        "residual disposition blocks resume: "
                        f"disposition={getattr(residual_disposition, 'disposition', 'unknown')} "
                        f"reason={','.join(getattr(residual_disposition, 'reason_codes', ()) or ('none',))}"
                    ),
                    reason_code=str(
                        (getattr(residual_disposition, "reason_codes", ()) or ("residual_disposition_blocked",))[0]
                    ),
                    summary="residual disposition blocks resume",
                    overridable=False,
                )
            )
        dust_resume_blocker = (
            None
            if residual_disposition is not None
            else dust_residual_resume_blocker(dust_context_for_halt)
        )
        if dust_resume_blocker is not None:
            blocker_code, blocker_detail = dust_resume_blocker
            dust_reason_code, dust_summary = classify_dust_resume_blocker(dust_context_for_halt)
            reasons.append(
                _resume_blocker(
                    code=blocker_code,
                    detail=blocker_detail,
                    reason_code=dust_reason_code,
                    summary=dust_summary,
                    overridable=False,
                )
            )

        residual_run_allowed = bool(
            residual_disposition is not None and bool(getattr(residual_disposition, "run_allowed", False))
        )
        legacy_dust_effective_flat = bool(
            residual_disposition is None and bool(dust_context_for_halt["effective_flat"])
        )
        unresolved_dust_safe = bool(
            state.halt_state_unresolved
            and (state.halt_reason_code or "") in RISK_EXPOSURE_HALT_REASON_CODES
            and int(state.unresolved_open_order_count) == 0
            and int(state.recovery_required_count) == 0
            and (residual_run_allowed or legacy_dust_effective_flat)
        )
        if state.halt_state_unresolved and not unresolved_dust_safe:
            reasons.append(
                _resume_blocker(
                    code="HALT_STATE_UNRESOLVED",
                    detail=f"halt unresolved: code={state.halt_reason_code or '-'} reason={state.last_disable_reason or '-'}",
                    reason_code="HALT_STATE_UNRESOLVED",
                    summary="halt state remains unresolved",
                    overridable=False,
                )
            )

        if state.halt_new_orders_blocked:
            if self.exposure_snapshot is None:
                open_orders_present = bool(state.halt_open_orders_present)
                position_present = bool(state.halt_position_present)
            else:
                open_orders_present, position_present = self.exposure_snapshot(int(time.time() * 1000))
            open_orders_present = bool(open_orders_present or state.halt_open_orders_present)
            position_present = bool(position_present or state.halt_position_present)
            if not position_present:
                conn = self.db_factory()
                try:
                    row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
                    position_present = bool(row is not None and abs(float(row["asset_qty"])) > 1e-12)
                except Exception:
                    position_present = True
                finally:
                    conn.close()
            dust_context = reconcile_dust_context(state.last_reconcile_metadata)
            is_risk_exposure_halt = (state.halt_reason_code or "") in RISK_EXPOSURE_HALT_REASON_CODES
            dust_exposure_only = bool(
                not open_orders_present
                and position_present
                and (
                    residual_run_allowed
                    or (
                        residual_disposition is None
                        and bool(dust_context["present"])
                        and bool(dust_context["effective_flat"])
                    )
                )
            )
            if open_orders_present or (position_present and not dust_exposure_only):
                detail = (
                    "halt blocked with open exposure: "
                    f"position_present={1 if position_present else 0} "
                    f"open_orders_present={1 if open_orders_present else 0} "
                    f"reason_code={state.halt_reason_code or '-'} "
                    f"reason={state.last_disable_reason or '-'}"
                )
                if position_present and not open_orders_present and bool(dust_context["present"]):
                    detail += (
                        f" dust_policy={str(dust_context['policy_reason'])} "
                        f"dust_summary={str(dust_context['summary'])}"
                    )
                if is_risk_exposure_halt:
                    detail = (
                        "risk halt resume rejected until exposure is flattened/resolved first; "
                        + detail
                    )
                reasons.append(
                    _resume_blocker(
                        code="HALT_RISK_OPEN_POSITION",
                        detail=detail,
                        reason_code="HALT_RISK_OPEN_POSITION",
                        summary="halt risk still has open exposure",
                        overridable=False,
                    )
                )

        ledger_adjustment_summary = (
            self.ledger_external_cash_adjustment_summary()
            if settings.MODE == "live" and self.ledger_external_cash_adjustment_summary is not None
            else None
        )
        if settings.MODE == "live" and state.last_reconcile_metadata:
            mismatch_count = reconcile_balance_split_mismatch_count(state.last_reconcile_metadata)
            try:
                reconcile_meta = json.loads(str(state.last_reconcile_metadata))
            except json.JSONDecodeError:
                reconcile_meta = {}
            ledger_adjustment_count = 0
            ledger_adjustment_total = 0.0
            if ledger_adjustment_summary is not None:
                try:
                    ledger_adjustment_count = max(
                        0,
                        int(ledger_adjustment_summary.get("adjustment_count", 0) or 0),
                    )
                except (TypeError, ValueError):
                    ledger_adjustment_count = 0
                try:
                    ledger_adjustment_total = float(
                        ledger_adjustment_summary.get("adjustment_total", 0.0) or 0.0
                    )
                except (TypeError, ValueError):
                    ledger_adjustment_total = 0.0
            if ledger_adjustment_count > 0:
                reconcile_meta = dict(reconcile_meta)
                reconcile_meta["external_cash_adjustment_count"] = max(
                    ledger_adjustment_count,
                    int(reconcile_meta.get("external_cash_adjustment_count", 0) or 0),
                )
                reconcile_meta["external_cash_adjustment_total_krw"] = (
                    float(reconcile_meta.get("external_cash_adjustment_total_krw", 0.0) or 0.0)
                    if float(reconcile_meta.get("external_cash_adjustment_total_krw", 0.0) or 0.0) != 0.0
                    else ledger_adjustment_total
                )
            if mismatch_count > 0:
                blocker_reason = classify_balance_split_blocker(reconcile_meta)
                if blocker_reason is None:
                    blocker_reason = _resume_blocker(
                        code="BALANCE_SPLIT_MISMATCH",
                        detail=(
                            "balance split mismatch detected after reconcile: "
                            f"count={mismatch_count} summary={str(reconcile_meta.get('balance_split_mismatch_summary') or '-')}"
                        ),
                        reason_code=BLOCKER_PORTFOLIO_BROKER_CASH_MISMATCH,
                        summary="portfolio cash split does not match broker snapshot",
                        overridable=False,
                    )
                reasons.append(blocker_reason)
            fee_gap_blocker = self.classify_fee_gap_recovery_blocker(reconcile_meta)
            if fee_gap_blocker is not None:
                reasons.append(fee_gap_blocker)

        gate_reason = None
        if reasons:
            gate_reason = "; ".join(f"{blocker.code}:{blocker.detail}" for blocker in reasons)
        self.set_resume_gate(blocked=bool(reasons), reason=gate_reason)
        return (len(reasons) == 0), reasons

    def classify_fee_gap_recovery_blocker(self, metadata: dict[str, object]) -> ResumeBlocker | None:
        try:
            fee_gap_recovery_required = int(metadata.get("fee_gap_recovery_required", 0) or 0)
        except (TypeError, ValueError):
            fee_gap_recovery_required = 0
        if fee_gap_recovery_required <= 0:
            return None

        try:
            zero_fee_fill_count = max(
                0,
                int(metadata.get("material_zero_fee_fill_count", 0) or 0),
            )
        except (TypeError, ValueError):
            zero_fee_fill_count = 0
        try:
            fee_gap_adjustment_count = max(
                0,
                int(metadata.get("fee_gap_adjustment_count", 0) or 0),
            )
        except (TypeError, ValueError):
            fee_gap_adjustment_count = 0

        conn = self.db_factory()
        try:
            fee_gap_preview = self.repair_service.fee_gap_accounting_preview(conn)
        finally:
            conn.close()
        if not bool(fee_gap_preview.get("needs_repair")):
            return None
        if not bool(fee_gap_preview.get("resume_blocking", True)):
            return None

        return _resume_blocker(
            code="FEE_GAP_RECOVERY_REQUIRED",
            detail=(
                "fee-related cash drift detected during reconcile: "
                f"incident_kind={fee_gap_preview.get('incident_kind') or 'unknown'} "
                f"incident_scope={fee_gap_preview.get('incident_scope') or 'unknown'} "
                f"resolution_state={fee_gap_preview.get('resolution_state') or 'unknown'} "
                f"active_issue={1 if bool(fee_gap_preview.get('active_issue')) else 0} "
                f"material_zero_fee_fill_count={zero_fee_fill_count} "
                f"fee_gap_adjustment_count={fee_gap_adjustment_count} "
                f"fee_gap_accounting_repair_count={int(fee_gap_preview.get('fee_gap_accounting_repair_count') or 0)} "
                f"resume_policy={fee_gap_preview.get('resume_policy') or 'hard_block'} "
                f"next_action={fee_gap_preview.get('next_required_action') or 'review_recovery_report'}"
            ),
            reason_code="FEE_GAP_RECOVERY_REQUIRED",
            summary="fee-related accounting inconsistency requires manual recovery",
            overridable=False,
        )


@dataclass(frozen=True)
class ResumeGuidanceService:
    last_reconcile_fee_pending_recovery_required: Callable[[], bool]

    def build_resume_guidance(
        self,
        *,
        resume_allowed: bool,
        blockers: Sequence[ResumeBlocker],
        unresolved_count: int,
        recovery_required_count: int,
        submit_unknown_count: int,
    ) -> ResumeGuidance:
        blocker_list: list[dict[str, object]] = [
            {
                "code": b.code,
                "reason_code": str(getattr(b, "reason_code", b.code)),
                "summary": str(getattr(b, "summary", b.detail)),
                "detail": b.detail,
                "overridable": bool(b.overridable),
                "balance_delta_krw": getattr(b, "balance_delta_krw", None),
                "recent_external_cash_adjustment_present": getattr(
                    b,
                    "recent_external_cash_adjustment_present",
                    None,
                ),
                "recent_external_cash_adjustment_count": getattr(
                    b,
                    "recent_external_cash_adjustment_count",
                    None,
                ),
            }
            for b in blockers
        ]
        blocker_codes = [str(b["code"]) for b in blocker_list]
        non_overridable_blockers = [b for b in blocker_list if not bool(b["overridable"])]
        primary_blocker_code = str(blocker_list[0]["code"]) if blocker_list else "-"
        primary_blocker_reason_code = str(blocker_list[0]["reason_code"]) if blocker_list else "-"
        blocker_summary = (
            f"total={len(blocker_list)} "
            f"non_overridable={len(non_overridable_blockers)} "
            f"overridable={len(blocker_list) - len(non_overridable_blockers)}"
        )

        if resume_allowed:
            operator_next_action = "resume_now"
            recommended_command = "uv run python bot.py resume"
            recommended_next_action = "No active blocker. Resume trading now."
            resume_blocked_reason = "none"
        elif blocker_list and all(bool(b["overridable"]) for b in blocker_list):
            operator_next_action = "review_and_force_resume"
            recommended_command = "uv run python bot.py resume --force"
            recommended_next_action = "Review overridable blockers and force resume only if risk is accepted."
            resume_blocked_reason = "resume blocked by overridable blockers"
        elif self.last_reconcile_fee_pending_recovery_required():
            operator_next_action = "wait_for_auto_reconcile_or_review_fee_evidence"
            recommended_command = "uv run python bot.py recovery-report"
            recommended_next_action = (
                "Wait for automatic reconcile to finalize fee-pending accounting, or inspect broker fill evidence if it does not clear."
            )
            resume_blocked_reason = "resume blocked while fee-pending accounting is auto-recovering"
        elif recovery_required_count > 0:
            operator_next_action = "manual_recovery_required"
            recommended_command = "uv run python bot.py recover-order --client-order-id <id>"
            recommended_next_action = "Recover RECOVERY_REQUIRED orders before attempting resume."
            resume_blocked_reason = "resume blocked by RECOVERY_REQUIRED orders"
        elif any(str(b["reason_code"]) == "POSITION_AUTHORITY_RECOVERY_REQUIRED" for b in blocker_list):
            operator_next_action = "manual_position_authority_recovery_required"
            recommended_command = "uv run python bot.py recovery-report --json"
            recommended_next_action = (
                "Do not resume trading. Holdings exist but canonical lot authority is missing; repair or explicitly recover the position state first."
            )
            resume_blocked_reason = "resume blocked by missing lot authority"
        elif any(str(b["reason_code"]) == "POSITION_AUTHORITY_CORRECTION_REQUIRED" for b in blocker_list):
            operator_next_action = "position_authority_correction_required"
            recommended_command = "uv run python bot.py rebuild-position-authority"
            recommended_next_action = (
                "Do not resume trading. Correct the conflicting lot authority from accounted BUY evidence, then rerun recovery-report."
            )
            resume_blocked_reason = "resume blocked by conflicting lot authority"
        elif any(str(b["reason_code"]) == "POSITION_AUTHORITY_RESIDUAL_NORMALIZATION_REQUIRED" for b in blocker_list):
            operator_next_action = "position_authority_residual_normalization_required"
            recommended_command = "uv run python bot.py rebuild-position-authority"
            recommended_next_action = (
                "Do not resume trading. Normalize the post-partial-close residual authority from accounted BUY/SELL evidence, then rerun recovery-report."
            )
            resume_blocked_reason = "resume blocked by unnormalized partial-close residual authority"
        elif any(str(b["reason_code"]) == "POSITION_AUTHORITY_PROJECTION_REPAIR_REQUIRED" for b in blocker_list):
            operator_next_action = "position_authority_projection_repair_required"
            recommended_command = "uv run python bot.py rebuild-position-authority"
            recommended_next_action = (
                "Do not resume trading. Review the broker/portfolio evidence gates and apply the projection repair only if the preview is safe."
            )
            resume_blocked_reason = "resume blocked by projection/portfolio divergence"
        elif any(str(b["reason_code"]) == "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED" for b in blocker_list):
            operator_next_action = "position_authority_projection_convergence_required"
            recommended_command = "uv run python bot.py rebuild-position-authority"
            recommended_next_action = (
                "Do not resume trading. Canonical holdings and the persisted lot projection do not converge; inspect the projection truth model before any repair."
            )
            resume_blocked_reason = "resume blocked by non-converged lot projection"
        elif "EXTERNAL_POSITION_ACCOUNTING_REPAIR_REQUIRED" in blocker_codes:
            operator_next_action = "external_position_accounting_repair_required"
            recommended_command = "uv run python bot.py external-position-accounting-repair"
            recommended_next_action = (
                "Do not resume trading. Record the replay-compatible external position adjustment before resuming."
            )
            resume_blocked_reason = "resume blocked pending replay-compatible external position repair"
        elif "ACCOUNTING_REPLAY_MISMATCH_REVIEW_REQUIRED" in blocker_codes:
            operator_next_action = "review_accounting_replay_evidence"
            recommended_command = "uv run python bot.py external-position-accounting-repair"
            recommended_next_action = (
                "Do not resume trading. Review why portfolio and accounting replay diverge, then record or correct canonical accounting evidence."
            )
            resume_blocked_reason = "resume blocked by unresolved accounting replay mismatch"
        elif "MANUAL_FLAT_ACCOUNTING_REPAIR_REQUIRED" in blocker_codes:
            operator_next_action = "manual_flat_accounting_repair_required"
            recommended_command = "uv run python bot.py manual-flat-accounting-repair"
            recommended_next_action = (
                "Do not resume trading. The runtime is flat, but accounting replay still needs an explicit manual-flat repair event."
            )
            resume_blocked_reason = "resume blocked pending manual-flat accounting repair"
        elif "HARMLESS_DUST_POLICY_REVIEW_REQUIRED" in blocker_codes:
            operator_next_action = "review_harmless_dust_policy"
            recommended_command = "uv run python bot.py recovery-report --json"
            recommended_next_action = (
                "Confirm harmless broker/local dust is truly safe, decide whether policy should keep blocking resume, and avoid forced liquidation below exchange minimums."
            )
            resume_blocked_reason = "resume blocked by harmless dust policy review"
        elif "BLOCKING_DUST_REVIEW_REQUIRED" in blocker_codes:
            operator_next_action = "manual_dust_review_required"
            recommended_command = "uv run python bot.py recovery-report --json"
            recommended_next_action = (
                "Confirm this is not a broker/local mismatch or recovery issue before resuming. Do not force extra liquidation while state is unclear."
            )
            resume_blocked_reason = "resume blocked by blocking dust manual review"
        elif "EXTERNAL_CASH_ADJUSTMENT_REQUIRED" in blocker_codes:
            operator_next_action = "record_external_cash_adjustment"
            recommended_command = "uv run python bot.py record-external-cash-adjustment --help"
            recommended_next_action = (
                "Record the missing external cash adjustment evidence, then rerun reconcile before resuming."
            )
            resume_blocked_reason = "resume blocked pending external cash adjustment evidence"
        elif "FEE_GAP_RECOVERY_REQUIRED" in blocker_codes:
            operator_next_action = "manual_fee_gap_recovery_required"
            recommended_command = "uv run python bot.py fee-gap-accounting-repair"
            recommended_next_action = (
                "Do not resume live trading. Review the fee-gap preview and record an explicit fee-gap accounting repair before resuming."
            )
            resume_blocked_reason = "resume blocked by fee-related accounting inconsistency"
        elif "BALANCE_SPLIT_MISMATCH" in blocker_codes:
            if any(bool(b.get("recent_external_cash_adjustment_present")) for b in blocker_list):
                operator_next_action = "reconcile_after_external_adjustment"
                recommended_command = "uv run python bot.py reconcile"
                recommended_next_action = (
                    "An external cash adjustment is already recorded. Verify the broker snapshot, then rerun reconcile."
                )
                resume_blocked_reason = "resume blocked by remaining balance split mismatch after external adjustment"
            else:
                operator_next_action = "investigate_blockers"
                recommended_command = "uv run python bot.py recovery-report --json"
                recommended_next_action = "Investigate non-overridable blockers and clear the root cause first."
                resume_blocked_reason = "resume blocked by non-overridable safety blockers"
        else:
            operator_next_action = "investigate_blockers"
            recommended_command = "uv run python bot.py recovery-report --json"
            recommended_next_action = "Investigate non-overridable blockers and clear the root cause first."
            resume_blocked_reason = "resume blocked by non-overridable safety blockers"

        active_blocker_summary = "none"
        if blocker_list:
            active_blocker_summary = " | ".join(
                f"{b['code']}(overridable={1 if bool(b['overridable']) else 0})"
                for b in blocker_list[:3]
            )

        risk_level = "low"
        if recovery_required_count > 0 or non_overridable_blockers:
            risk_level = "high"
        elif unresolved_count > 0 or blocker_list:
            risk_level = "medium"

        def _next_action_for_blocker(code: str) -> str:
            if code == "STARTUP_SAFETY_GATE_BLOCKED":
                if self.last_reconcile_fee_pending_recovery_required():
                    return "uv run python bot.py fee-pending-accounting-repair --help"
                if recovery_required_count > 0:
                    return "uv run python bot.py recover-order --client-order-id <id>"
                if submit_unknown_count > 0:
                    return "uv run python bot.py reconcile"
                return "uv run python bot.py recovery-report"
            if code == "LAST_RECONCILE_FAILED":
                return "uv run python bot.py reconcile"
            if code == "EXTERNAL_CASH_ADJUSTMENT_REQUIRED":
                return "uv run python bot.py record-external-cash-adjustment --help"
            if code == "MANUAL_FLAT_ACCOUNTING_REPAIR_REQUIRED":
                return "uv run python bot.py manual-flat-accounting-repair"
            if code == "FEE_GAP_RECOVERY_REQUIRED":
                return "uv run python bot.py fee-gap-accounting-repair"
            if code == "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED":
                return "uv run python bot.py rebuild-position-authority"
            if code == "BALANCE_SPLIT_MISMATCH":
                if any(bool(b.get("recent_external_cash_adjustment_present")) for b in blocker_list):
                    return "uv run python bot.py reconcile"
                return "uv run python bot.py recovery-report --json"
            if code == "HALT_RISK_OPEN_POSITION":
                return "uv run python bot.py flatten-position"
            if code in {"HARMLESS_DUST_POLICY_REVIEW_REQUIRED", "BLOCKING_DUST_REVIEW_REQUIRED"}:
                return "uv run python bot.py recovery-report --json"
            if code in {"HALT_STATE_UNRESOLVED", "EMERGENCY_FLATTEN_UNRESOLVED"}:
                return "uv run python bot.py restart-checklist"
            return recommended_command

        blocker_summary_view: list[dict[str, object]] = []
        for blocker in blocker_list[:3]:
            code = str(blocker["code"])
            reason_code = str(blocker["reason_code"])
            summary = str(blocker["summary"])
            evidence = str(blocker["detail"])
            if code == "STARTUP_SAFETY_GATE_BLOCKED":
                evidence = (
                    f"unresolved={unresolved_count} "
                    f"submit_unknown={submit_unknown_count} "
                    f"recovery_required={recovery_required_count}; "
                    f"{evidence}"
                )
            balance_delta_krw = blocker.get("balance_delta_krw")
            recent_adjustment_present = blocker.get("recent_external_cash_adjustment_present")
            recent_adjustment_count = blocker.get("recent_external_cash_adjustment_count")
            if balance_delta_krw is not None or recent_adjustment_present is not None:
                delta_text = (
                    f"{float(balance_delta_krw):.3f}"
                    if isinstance(balance_delta_krw, (int, float))
                    else str(balance_delta_krw)
                )
                evidence = (
                    f"{evidence} "
                    f"delta_krw={delta_text} "
                    f"recent_external_cash_adjustment_present={1 if bool(recent_adjustment_present) else 0} "
                    f"recent_external_cash_adjustment_count={int(recent_adjustment_count or 0)}"
                )
            blocker_summary_view.append(
                {
                    "blocker": code,
                    "reason_code": reason_code,
                    "summary": summary,
                    "evidence": evidence,
                    "recommended_next_action": _next_action_for_blocker(code),
                    "delta_krw": balance_delta_krw,
                    "recent_external_cash_adjustment_present": recent_adjustment_present,
                    "recent_external_cash_adjustment_count": recent_adjustment_count,
                }
            )

        if not blocker_summary_view:
            blocker_summary_view.append(
                {
                    "blocker": "none",
                    "evidence": "resume gates clear",
                    "recommended_next_action": "uv run python bot.py resume",
                }
            )

        return ResumeGuidance(
            operator_next_action=operator_next_action,
            recommended_command=recommended_command,
            recommended_next_action=recommended_next_action,
            resume_blocked_reason=resume_blocked_reason,
            blocker_summary=blocker_summary,
            active_blocker_summary=active_blocker_summary,
            risk_level=risk_level,
            primary_blocker_code=primary_blocker_code,
            primary_blocker_reason_code=primary_blocker_reason_code,
            blocker_summary_view=blocker_summary_view,
            blockers=blocker_list,
            non_overridable_blockers=non_overridable_blockers,
        )


@dataclass(frozen=True)
class RestartReadinessService:
    resume_evaluator: Callable[[], tuple[bool, list[ResumeBlocker]]]
    state_snapshot: Callable[[], object] = runtime_state.snapshot
    db_factory: Callable[[], object] = ensure_db
    readiness_snapshot_builder: Callable[[object], object] = compute_runtime_readiness_snapshot
    repair_service: OperatorRepairService = OperatorRepairService()

    def evaluate_restart_readiness(self) -> list[tuple[str, bool, str]]:
        resume_allowed, blockers = self.resume_evaluator()
        state = self.state_snapshot()

        conn = self.db_factory()
        try:
            open_row = conn.execute(
                """
                SELECT COUNT(*) AS open_count
                FROM orders
                WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING')
                """
            ).fetchone()
            recovery_row = conn.execute(
                "SELECT COUNT(*) AS recovery_required_count FROM orders WHERE status='RECOVERY_REQUIRED'"
            ).fetchone()
            readiness_snapshot = self.readiness_snapshot_builder(conn)
        finally:
            conn.close()

        open_order_count = int(open_row["open_count"] if open_row else 0)
        recovery_required_count = int(recovery_row["recovery_required_count"] if recovery_row else 0)
        unresolved_count = max(0, open_order_count + recovery_required_count)
        position_state = readiness_snapshot.position_state
        normalized_exposure = position_state.normalized_exposure
        dust_context = build_dust_display_context(readiness_snapshot.reconcile_metadata)
        dust_present = bool(dust_context.classification.present)
        dust_resume_safe = bool(dust_present and dust_context.operator_view.resume_allowed)
        persisted_dust_resume_safe = bool(
            readiness_snapshot.resume_ready
            and str(normalized_exposure.terminal_state) == "dust_only"
            and normalized_exposure.has_dust_only_remainder
            and str(normalized_exposure.authority_gap_reason or "none") == "none"
        )
        asset_qty = float(normalized_exposure.raw_qty_open)
        raw_qty_without_dust_evidence = bool(
            asset_qty > 1e-12
            and not dust_present
            and not normalized_exposure.has_dust_only_remainder
        )
        raw_qty_residue_without_resume_safe_dust = bool(
            raw_qty_without_dust_evidence
            or (
                asset_qty > 1e-12
                and not normalized_exposure.has_any_position_residue
                and not dust_resume_safe
                and not persisted_dust_resume_safe
            )
        )
        authority_gap_reason = str(normalized_exposure.authority_gap_reason or "")
        executable_open_position_manageable = bool(
            normalized_exposure.has_executable_exposure
            and str(normalized_exposure.terminal_state) == "open_exposure"
            and not authority_gap_reason
        )
        readiness_deferred_open_position = bool(
            readiness_snapshot.resume_ready
            and readiness_snapshot.recovery_stage == "RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT"
        )
        position_state_clear = bool(
            readiness_deferred_open_position
            or (
                not raw_qty_residue_without_resume_safe_dust
                and (
                    executable_open_position_manageable
                    or str(normalized_exposure.terminal_state) == "flat"
                    or (
                        str(normalized_exposure.terminal_state) == "dust_only"
                        and (dust_resume_safe or persisted_dust_resume_safe)
                    )
                )
            )
        )
        if str(readiness_snapshot.recovery_stage).startswith("AUTHORITY_") and not readiness_deferred_open_position:
            position_state_clear = False
        display_terminal_state = (
            "open_exposure"
            if raw_qty_residue_without_resume_safe_dust
            else str(normalized_exposure.terminal_state)
        )
        display_has_executable_exposure = bool(
            raw_qty_residue_without_resume_safe_dust or normalized_exposure.has_executable_exposure
        )
        display_has_dust_only_remainder = bool(
            (not raw_qty_residue_without_resume_safe_dust) and normalized_exposure.has_dust_only_remainder
        )

        last_reconcile_summary = "none"
        if state.last_reconcile_status:
            last_reconcile_summary = (
                f"status={state.last_reconcile_status} "
                f"reason_code={state.last_reconcile_reason_code or '-'}"
            )
            if state.last_reconcile_error:
                last_reconcile_summary += f" error={state.last_reconcile_error}"
        last_reconcile_ok = bool(str(state.last_reconcile_status or "").lower() in {"", "ok"})

        blocker_codes = {blocker.code for blocker in blockers}
        halt_clear = bool(
            state.halt_new_orders_blocked is False
            and state.halt_state_unresolved is False
            and "HALT_STATE_UNRESOLVED" not in blocker_codes
            and "HALT_RISK_OPEN_POSITION" not in blocker_codes
        )

        conn = self.db_factory()
        try:
            manual_flat_preview = self.repair_service.manual_flat_accounting_preview(conn)
            fee_gap_preview = self.repair_service.fee_gap_accounting_preview(conn)
        finally:
            conn.close()
        manual_flat_repair_needed = bool(manual_flat_preview.get("safe_to_apply"))
        manual_flat_repair_detail = (
            f"needed={1 if manual_flat_repair_needed else 0} "
            f"safe_to_apply={1 if bool(manual_flat_preview.get('safe_to_apply')) else 0} "
            f"reason={manual_flat_preview.get('eligibility_reason') or 'none'}"
        )
        fee_gap_repair_needed = bool(
            fee_gap_preview.get("needs_repair") and fee_gap_preview.get("resume_blocking", True)
        )
        fee_gap_repair_detail = (
            f"incident_kind={fee_gap_preview.get('incident_kind') or 'unknown'} "
            f"incident_scope={fee_gap_preview.get('incident_scope') or 'unknown'} "
            f"resolution_state={fee_gap_preview.get('resolution_state') or 'unknown'} "
            f"active_issue={1 if bool(fee_gap_preview.get('active_issue')) else 0} "
            f"needed={1 if bool(fee_gap_preview.get('needs_repair')) else 0} "
            f"resume_blocking={1 if bool(fee_gap_preview.get('resume_blocking')) else 0} "
            f"closeout_blocking={1 if bool(fee_gap_preview.get('closeout_blocking')) else 0} "
            f"resume_policy={fee_gap_preview.get('resume_policy') or 'none'} "
            f"safe_to_apply={1 if bool(fee_gap_preview.get('safe_to_apply')) else 0} "
            f"repair_count={int(fee_gap_preview.get('fee_gap_accounting_repair_count') or 0)} "
            f"reason={fee_gap_preview.get('eligibility_reason') or 'none'}"
        )

        return [
            (
                "unresolved/recovery-required orders",
                unresolved_count == 0,
                (
                    f"unresolved={unresolved_count} "
                    f"recovery_required={recovery_required_count}"
                ),
            ),
            (
                "open orders",
                open_order_count == 0,
                f"open_orders={open_order_count}",
            ),
            (
                "normalized position state",
                position_state_clear,
                (
                    f"terminal_state={display_terminal_state} "
                    f"has_executable_exposure={1 if display_has_executable_exposure else 0} "
                    f"has_dust_only_remainder={1 if display_has_dust_only_remainder else 0} "
                    f"dust_resume_allowed={1 if dust_resume_safe else 0} "
                    f"recovery_stage={readiness_snapshot.recovery_stage}"
                ),
            ),
            (
                "halt state",
                halt_clear and resume_allowed,
                (
                    f"halt_blocked={1 if state.halt_new_orders_blocked else 0} "
                    f"halt_unresolved={1 if state.halt_state_unresolved else 0} "
                    f"detail={state.last_disable_reason or 'none'}"
                ),
            ),
            (
                "last reconcile",
                last_reconcile_ok,
                last_reconcile_summary,
            ),
            (
                "manual-flat accounting repair",
                not manual_flat_repair_needed,
                manual_flat_repair_detail,
            ),
            (
                "fee-gap accounting repair",
                not fee_gap_repair_needed,
                fee_gap_repair_detail,
            ),
        ]


def default_ledger_external_cash_adjustment_summary() -> dict[str, object] | None:
    try:
        conn = ensure_db()
    except Exception:
        return None
    try:
        return get_external_cash_adjustment_summary(conn)
    except Exception:
        return None
    finally:
        conn.close()


def last_reconcile_fee_pending_recovery_required() -> bool:
    state = runtime_state.snapshot()
    try:
        metadata = json.loads(str(state.last_reconcile_metadata or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    try:
        return (
            int(metadata.get("fee_pending_auto_recovering", 0) or 0) > 0
            or int(metadata.get("unaccounted_fee_pending_observation_count", 0) or 0) > 0
        )
    except (TypeError, ValueError):
        return False
