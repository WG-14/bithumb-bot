from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .db_core import normalize_cash_amount


_POSITION_RESIDUE_REPAIR_BLOCKERS = {
    "portfolio_not_flat",
    "lot_residue_present",
    "reserved_exit_qty",
}


@dataclass(frozen=True)
class FeeGapDebtPolicy:
    repair_eligibility_state: str
    repair_blocker_reasons: tuple[str, ...]
    resume_policy: str
    resume_blocking: bool
    closeout_blocking: bool
    readiness_stage: str
    blocker_category: str
    operator_next_action: str
    recommended_command: str
    next_required_action: str
    policy_reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "repair_eligibility_state": self.repair_eligibility_state,
            "repair_blocker_reasons": list(self.repair_blocker_reasons),
            "resume_policy": self.resume_policy,
            "resume_blocking": bool(self.resume_blocking),
            "closeout_blocking": bool(self.closeout_blocking),
            "readiness_stage": self.readiness_stage,
            "blocker_category": self.blocker_category,
            "operator_next_action": self.operator_next_action,
            "recommended_command": self.recommended_command,
            "next_required_action": self.next_required_action,
            "policy_reason": self.policy_reason,
        }


def matching_fee_gap_repair_present(
    *,
    repair_summary: dict[str, Any],
    fee_gap_adjustment_count: int,
    fee_gap_adjustment_total_krw: float,
    fee_gap_adjustment_latest_event_ts: int,
    material_zero_fee_fill_count: int,
    material_zero_fee_fill_latest_ts: int,
) -> bool:
    basis_raw = repair_summary.get("last_repair_basis")
    if not basis_raw:
        return False
    try:
        basis = json.loads(str(basis_raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return bool(
        int(basis.get("fee_gap_adjustment_count", 0) or 0) == fee_gap_adjustment_count
        and normalize_cash_amount(basis.get("fee_gap_adjustment_total_krw", 0.0) or 0.0)
        == normalize_cash_amount(fee_gap_adjustment_total_krw)
        and int(basis.get("fee_gap_adjustment_latest_event_ts", 0) or 0) == fee_gap_adjustment_latest_event_ts
        and int(basis.get("material_zero_fee_fill_count", 0) or 0) == material_zero_fee_fill_count
        and int(basis.get("material_zero_fee_fill_latest_ts", 0) or 0) == material_zero_fee_fill_latest_ts
    )


def _reason_prefix(reason: str) -> str:
    return str(reason).split("=", 1)[0]


def classify_fee_gap_debt_policy(
    *,
    needs_repair: bool,
    already_repaired: bool,
    repair_blocker_reasons: list[str],
    blocked_by_authority_rebuild: bool,
    blocked_by_open_exposure: bool,
    blocked_by_dust_residue: bool,
    has_executable_open_exposure: bool,
    canonical_state: str = "UNKNOWN",
    execution_flat: bool = False,
    accounting_flat: bool = False,
) -> FeeGapDebtPolicy:
    unique_reasons = tuple(dict.fromkeys(str(reason) for reason in repair_blocker_reasons if str(reason)))
    reason_prefixes = {_reason_prefix(reason) for reason in unique_reasons}

    if already_repaired and not needs_repair:
        return FeeGapDebtPolicy(
            repair_eligibility_state="already_repaired",
            repair_blocker_reasons=unique_reasons,
            resume_policy="not_applicable",
            resume_blocking=False,
            closeout_blocking=False,
            readiness_stage="RESUME_READY",
            blocker_category="none",
            operator_next_action="resume_now",
            recommended_command="uv run python bot.py resume",
            next_required_action="none",
            policy_reason="matching fee-gap accounting repair already recorded",
        )
    if not needs_repair:
        return FeeGapDebtPolicy(
            repair_eligibility_state="not_needed",
            repair_blocker_reasons=unique_reasons,
            resume_policy="not_applicable",
            resume_blocking=False,
            closeout_blocking=False,
            readiness_stage="RESUME_READY",
            blocker_category="none",
            operator_next_action="resume_now",
            recommended_command="uv run python bot.py resume",
            next_required_action="none",
            policy_reason="no fee-gap accounting repair needed",
        )
    if blocked_by_authority_rebuild:
        return FeeGapDebtPolicy(
            repair_eligibility_state="blocked_by_authority",
            repair_blocker_reasons=unique_reasons,
            resume_policy="hard_block",
            resume_blocking=True,
            closeout_blocking=True,
            readiness_stage="HISTORICAL_FEE_GAP_PENDING",
            blocker_category="historical_accounting_debt",
            operator_next_action="rebuild_position_authority",
            recommended_command="uv run python bot.py rebuild-position-authority",
            next_required_action="rebuild_position_authority",
            policy_reason="fee-gap repair is waiting on executable authority recovery",
        )

    if not unique_reasons:
        return FeeGapDebtPolicy(
            repair_eligibility_state="safe_to_apply_now",
            repair_blocker_reasons=unique_reasons,
            resume_policy="hard_block_until_applied",
            resume_blocking=True,
            closeout_blocking=True,
            readiness_stage="HISTORICAL_FEE_GAP_PENDING",
            blocker_category="historical_accounting_debt",
            operator_next_action="apply_fee_gap_accounting_repair",
            recommended_command="uv run python bot.py fee-gap-accounting-repair --apply --yes",
            next_required_action="apply_fee_gap_accounting_repair",
            policy_reason="historical fee-gap repair is applicable now and must be recorded before resume",
        )

    only_position_residue_blocks = bool(reason_prefixes) and reason_prefixes <= _POSITION_RESIDUE_REPAIR_BLOCKERS
    if (
        only_position_residue_blocks
        and str(canonical_state) == "DUST_ONLY_TRACKED"
        and bool(execution_flat)
        and not bool(accounting_flat)
        and blocked_by_dust_residue
        and not has_executable_open_exposure
    ):
        return FeeGapDebtPolicy(
            repair_eligibility_state="safe_to_apply_with_tracked_dust",
            repair_blocker_reasons=unique_reasons,
            resume_policy="hard_block_until_applied",
            resume_blocking=True,
            closeout_blocking=True,
            readiness_stage="HISTORICAL_FEE_GAP_PENDING",
            blocker_category="historical_accounting_debt",
            operator_next_action="apply_fee_gap_accounting_repair",
            recommended_command="uv run python bot.py fee-gap-accounting-repair --apply --yes",
            next_required_action="apply_fee_gap_accounting_repair",
            policy_reason=(
                "fee-gap debt is historical and current residue is tracked dust only; "
                "execution is flat, so the accounting repair may be recorded without a SELL"
            ),
        )

    if (
        only_position_residue_blocks
        and has_executable_open_exposure
        and (blocked_by_open_exposure or blocked_by_dust_residue)
    ):
        return FeeGapDebtPolicy(
            repair_eligibility_state="blocked_until_flattened",
            repair_blocker_reasons=unique_reasons,
            resume_policy="defer_for_open_position_management",
            resume_blocking=False,
            closeout_blocking=True,
            readiness_stage="RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT",
            blocker_category="advisory_historical_debt",
            operator_next_action="resume_manage_open_position_then_repair_fee_gap_after_flatten",
            recommended_command="uv run python bot.py resume",
            next_required_action="manage_open_position_until_flat_then_apply_fee_gap_repair",
            policy_reason=(
                "fee-gap debt is historical and repair is flat-only; executable authority is present, "
                "so resume may continue managing the open position while debt remains closeout-blocking"
            ),
        )

    if blocked_by_open_exposure or blocked_by_dust_residue:
        next_action = "resolve_open_exposure_before_fee_gap_repair"
        policy_reason = "fee-gap repair is flat-only and current residue is not an explicitly deferable executable position"
    else:
        next_action = "review_recovery_report"
        policy_reason = "fee-gap repair has non-position safety blockers"
    return FeeGapDebtPolicy(
        repair_eligibility_state="blocked",
        repair_blocker_reasons=unique_reasons,
        resume_policy="hard_block",
        resume_blocking=True,
        closeout_blocking=True,
        readiness_stage="HISTORICAL_FEE_GAP_PENDING",
        blocker_category="historical_accounting_debt",
        operator_next_action="review_fee_gap_accounting_repair",
        recommended_command="uv run python bot.py fee-gap-accounting-repair",
        next_required_action=next_action,
        policy_reason=policy_reason,
    )
