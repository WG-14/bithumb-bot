from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from . import runtime_state
from .config import settings
from .db_core import ensure_db, get_fee_gap_accounting_repair_summary, portfolio_asset_total
from .dust import build_dust_display_context, build_position_state_model
from .external_position_repair import build_external_position_accounting_repair_preview
from .fee_gap_policy import classify_fee_gap_debt_policy, matching_fee_gap_repair_present
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from .position_authority_state import build_position_authority_assessment
from .recovery_policy import (
    build_tradeability_operator_fields,
    classify_canonical_recovery_state,
    classify_canonical_tradeability_state,
)


@dataclass(frozen=True)
class RuntimeReadinessSnapshot:
    recovery_stage: str
    resume_ready: bool
    resume_blockers: tuple[str, ...]
    blocker_categories: tuple[str, ...]
    operator_next_action: str
    recommended_command: str
    position_state: Any
    lot_snapshot: Any
    reconcile_metadata: dict[str, object]
    fee_pending_count: int
    fee_gap_recovery_required: bool
    fee_gap_resume_blocking: bool
    fee_gap_resume_policy: str
    fee_gap_closeout_blocking: bool
    fee_gap_adjustment_count: int
    material_zero_fee_fill_count: int
    open_order_count: int
    recovery_required_count: int
    position_authority_assessment: dict[str, object]
    canonical_state: str
    residual_class: str
    run_loop_allowed: bool
    new_entry_allowed: bool
    closeout_allowed: bool
    effective_flat: bool
    operator_action_required: bool
    why_not: str
    execution_flat: bool
    accounting_flat: bool
    tradeability: Any
    tradeability_operator_fields: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "recovery_stage": self.recovery_stage,
            "resume_ready": bool(self.resume_ready),
            "resume_blockers": list(self.resume_blockers),
            "blocker_categories": list(self.blocker_categories),
            "operator_next_action": self.operator_next_action,
            "recommended_command": self.recommended_command,
            "position_authority_summary": self.position_state.normalized_exposure.position_authority_summary,
            "normalized_exposure": self.position_state.normalized_exposure.as_dict(),
            "lot_snapshot": self.lot_snapshot.as_dict(),
            "fee_pending_count": int(self.fee_pending_count),
            "fee_gap_recovery_required": bool(self.fee_gap_recovery_required),
            "fee_gap_resume_blocking": bool(self.fee_gap_resume_blocking),
            "fee_gap_resume_policy": self.fee_gap_resume_policy,
            "fee_gap_closeout_blocking": bool(self.fee_gap_closeout_blocking),
            "fee_gap_adjustment_count": int(self.fee_gap_adjustment_count),
            "material_zero_fee_fill_count": int(self.material_zero_fee_fill_count),
            "open_order_count": int(self.open_order_count),
            "recovery_required_count": int(self.recovery_required_count),
            "position_authority_assessment": dict(self.position_authority_assessment),
            "canonical_state": self.canonical_state,
            "residual_class": self.residual_class,
            "run_loop_allowed": bool(self.run_loop_allowed),
            "new_entry_allowed": bool(self.new_entry_allowed),
            "closeout_allowed": bool(self.closeout_allowed),
            "effective_flat": bool(self.effective_flat),
            "operator_action_required": bool(self.operator_action_required),
            "why_not": self.why_not,
            "execution_flat": bool(self.execution_flat),
            "accounting_flat": bool(self.accounting_flat),
            "tradeability": self.tradeability.as_dict(),
            **self.tradeability_operator_fields,
        }


def _metadata_dict(raw: object | None) -> dict[str, object]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _metadata_int(metadata: dict[str, object], key: str) -> int:
    try:
        return max(0, int(metadata.get(key, 0) or 0))
    except (TypeError, ValueError):
        return 0


def _row_int(row: Any, key: str, default: int = 0) -> int:
    if row is None:
        return default
    try:
        keys = row.keys()
    except AttributeError:
        keys = ()
    if key not in keys:
        return default
    try:
        return int(row[key] or 0)
    except (TypeError, ValueError, KeyError):
        return default


def _unaccounted_fee_pending_observation_count(conn: Any) -> int:
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM broker_fill_observations b
            WHERE b.accounting_status='fee_pending'
              AND NOT EXISTS (
                  SELECT 1
                  FROM fills f
                  WHERE f.client_order_id=b.client_order_id
                    AND f.fee IS NOT NULL
                    AND f.fee > 1e-12
                    AND (
                         (b.fill_id IS NOT NULL AND f.fill_id=b.fill_id)
                         OR (
                              f.fill_ts=b.fill_ts
                          AND ABS(f.price-b.price) < 1e-12
                          AND ABS(f.qty-b.qty) < 1e-12
                         )
                    )
              )
            """
        ).fetchone()
    except (AssertionError, sqlite3.OperationalError):
        return 0
    return _row_int(row, "cnt")


def compute_runtime_readiness_snapshot(conn=None) -> RuntimeReadinessSnapshot:
    """Build the canonical recovery/readiness interpretation for one DB snapshot.

    This is intentionally read-only. Mutation-specific previews can depend on
    this snapshot for stage and ordering, but they still own their individual
    safety checks.
    """

    close_conn = False
    if conn is None:
        conn = ensure_db()
        close_conn = True
    try:
        state = runtime_state.snapshot()
        metadata = _metadata_dict(state.last_reconcile_metadata)
        metadata.setdefault("unresolved_open_order_count", int(state.unresolved_open_order_count or 0))
        metadata.setdefault("recovery_required_count", int(state.recovery_required_count or 0))

        open_row = conn.execute(
            """
            SELECT
                COUNT(*) AS open_order_count,
                COALESCE(SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END), 0)
                    AS recovery_required_count
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN',
                             'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
            """
        ).fetchone()
        open_order_count = _row_int(open_row, "open_order_count")
        recovery_required_count = _row_int(open_row, "recovery_required_count")

        portfolio_row = conn.execute(
            "SELECT asset_qty, asset_available, asset_locked FROM portfolio WHERE id=1"
        ).fetchone()
        if portfolio_row is None:
            portfolio_asset_qty = 0.0
        elif "asset_available" in portfolio_row.keys():
            portfolio_asset_qty = portfolio_asset_total(
                asset_available=float(portfolio_row["asset_available"] or 0.0),
                asset_locked=float(portfolio_row["asset_locked"] or 0.0),
            )
        else:
            portfolio_asset_qty = float(portfolio_row["asset_qty"] or 0.0)

        dust_context = build_dust_display_context(metadata)
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        lot_definition = getattr(lot_snapshot, "lot_definition", None)
        reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
        position_state = build_position_state_model(
            raw_qty_open=portfolio_asset_qty,
            metadata_raw=metadata,
            raw_total_asset_qty=max(
                portfolio_asset_qty,
                float(lot_snapshot.raw_total_asset_qty),
                float(dust_context.raw_holdings.broker_qty),
            ),
            open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
            dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
            open_lot_count=int(lot_snapshot.open_lot_count),
            dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
            reserved_exit_qty=reserved_exit_qty,
            internal_lot_size=(None if lot_definition is None else lot_definition.internal_lot_size),
            min_qty=(None if lot_definition is None else lot_definition.min_qty),
            qty_step=(None if lot_definition is None else lot_definition.qty_step),
            min_notional_krw=(None if lot_definition is None else lot_definition.min_notional_krw),
            max_qty_decimals=(None if lot_definition is None else lot_definition.max_qty_decimals),
        )
        fee_pending_count = _unaccounted_fee_pending_observation_count(conn)
        fee_gap_required = _metadata_int(metadata, "fee_gap_recovery_required") > 0
        fee_gap_adjustment_count = _metadata_int(metadata, "fee_gap_adjustment_count")
        fee_gap_adjustment_latest_event_ts = _metadata_int(metadata, "fee_gap_adjustment_latest_event_ts")
        fee_gap_adjustment_total_krw = 0.0
        try:
            fee_gap_adjustment_total_krw = float(metadata.get("fee_gap_adjustment_total_krw", 0.0) or 0.0)
        except (TypeError, ValueError):
            fee_gap_adjustment_total_krw = 0.0
        material_zero_fee_fill_count = _metadata_int(metadata, "material_zero_fee_fill_count")
        material_zero_fee_fill_latest_ts = _metadata_int(metadata, "material_zero_fee_fill_latest_ts")
        authority_assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
        canonical_recovery = classify_canonical_recovery_state(
            position_state=position_state,
            lot_snapshot=lot_snapshot,
            portfolio_asset_qty=portfolio_asset_qty,
            reserved_exit_qty=reserved_exit_qty,
        )
        repair_summary = get_fee_gap_accounting_repair_summary(conn)
        already_repaired_fee_gap = matching_fee_gap_repair_present(
            repair_summary=repair_summary,
            fee_gap_adjustment_count=fee_gap_adjustment_count,
            fee_gap_adjustment_total_krw=fee_gap_adjustment_total_krw,
            fee_gap_adjustment_latest_event_ts=fee_gap_adjustment_latest_event_ts,
            material_zero_fee_fill_count=material_zero_fee_fill_count,
            material_zero_fee_fill_latest_ts=material_zero_fee_fill_latest_ts,
        )
        fee_gap_needs_repair = bool(
            fee_gap_required
            and material_zero_fee_fill_count > 0
            and fee_gap_adjustment_count > 0
            and not already_repaired_fee_gap
        )
        fee_gap_reasons: list[str] = []
        external_cash_adjustment_reason = str(metadata.get("external_cash_adjustment_reason") or "none")
        if fee_gap_required and material_zero_fee_fill_count <= 0:
            fee_gap_reasons.append("material_zero_fee_fill_count=0")
        if fee_gap_required and fee_gap_adjustment_count <= 0:
            fee_gap_reasons.append("fee_gap_adjustment_count=0")
        if external_cash_adjustment_reason not in {"reconcile_fee_gap_cash_drift", "none"}:
            fee_gap_reasons.append(f"external_cash_adjustment_reason={external_cash_adjustment_reason}")
        if open_order_count > 0:
            fee_gap_reasons.append(f"open_or_unresolved_orders={open_order_count}")
        if recovery_required_count > 0:
            fee_gap_reasons.append(f"recovery_required_orders={recovery_required_count}")
        if str(state.last_reconcile_status or "").lower() != "ok":
            fee_gap_reasons.append(f"last_reconcile_status={state.last_reconcile_status or 'none'}")
        if abs(float(portfolio_asset_qty)) > 1e-12:
            fee_gap_reasons.append(f"portfolio_not_flat=asset_qty={float(portfolio_asset_qty):.12f}")
        if int(lot_snapshot.open_lot_count) > 0 or int(lot_snapshot.dust_tracking_lot_count) > 0:
            fee_gap_reasons.append(
                "lot_residue_present="
                f"open_lot_count={int(lot_snapshot.open_lot_count)},dust_tracking_lot_count={int(lot_snapshot.dust_tracking_lot_count)}"
            )
        if abs(float(reserved_exit_qty)) > 1e-12:
            fee_gap_reasons.append(f"reserved_exit_qty={float(reserved_exit_qty):.12f}")
        blocked_by_authority_rebuild = bool(
            bool(authority_assessment.get("needs_correction"))
            or bool(authority_assessment.get("needs_residual_normalization"))
            or str(position_state.normalized_exposure.authority_gap_reason or "")
            == "authority_missing_recovery_required"
        )
        fee_gap_policy = classify_fee_gap_debt_policy(
            needs_repair=fee_gap_needs_repair,
            already_repaired=already_repaired_fee_gap,
            repair_blocker_reasons=fee_gap_reasons,
            blocked_by_authority_rebuild=blocked_by_authority_rebuild,
            blocked_by_open_exposure=bool(
                int(lot_snapshot.open_lot_count) > 0 or abs(float(portfolio_asset_qty)) > 1e-12
            ),
            blocked_by_dust_residue=bool(int(lot_snapshot.dust_tracking_lot_count) > 0),
            has_executable_open_exposure=bool(
                int(lot_snapshot.open_lot_count) > 0
                and position_state.normalized_exposure.has_executable_exposure
            ),
            canonical_state=canonical_recovery.canonical_state,
            execution_flat=canonical_recovery.execution_flat,
            accounting_flat=canonical_recovery.accounting_flat,
        )
        replay_mismatch_preview = build_external_position_accounting_repair_preview(conn)

        blockers: list[str] = []
        categories: list[str] = []
        stage = "RESUME_READY"
        operator_next_action = "resume_now"
        recommended_command = "uv run python bot.py resume"

        if fee_pending_count > 0 or _metadata_int(metadata, "fee_pending_recovery_required") > 0:
            stage = "ACCOUNTING_PENDING_FEE"
            blockers.append("FEE_PENDING_ACCOUNTING_REQUIRED")
            categories.append("incident_local")
            operator_next_action = "apply_fee_pending_accounting_repair"
            recommended_command = (
                "uv run python bot.py fee-pending-accounting-repair "
                "--client-order-id <id> --fill-id <fill_id> --fee <fee> "
                "--fee-provenance <source> --apply --yes"
            )
        elif bool(authority_assessment.get("needs_residual_normalization")):
            stage = "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING"
            blockers.append("POSITION_AUTHORITY_RESIDUAL_NORMALIZATION_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = (
                "apply_rebuild_position_authority"
                if bool(authority_assessment.get("safe_to_normalize_residual"))
                else "review_position_authority_evidence"
            )
            recommended_command = (
                "uv run python bot.py rebuild-position-authority --apply --yes"
                if bool(authority_assessment.get("safe_to_normalize_residual"))
                else "uv run python bot.py rebuild-position-authority"
            )
        elif bool(authority_assessment.get("needs_portfolio_projection_repair")):
            stage = "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING"
            blockers.append("POSITION_AUTHORITY_PROJECTION_REPAIR_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = "review_position_authority_evidence"
            recommended_command = "uv run python bot.py rebuild-position-authority"
        elif bool(authority_assessment.get("needs_correction")):
            stage = "AUTHORITY_CORRECTION_PENDING"
            blockers.append("POSITION_AUTHORITY_CORRECTION_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = (
                "apply_rebuild_position_authority"
                if bool(authority_assessment.get("safe_to_correct"))
                else "review_position_authority_evidence"
            )
            recommended_command = (
                "uv run python bot.py rebuild-position-authority --apply --yes"
                if bool(authority_assessment.get("safe_to_correct"))
                else "uv run python bot.py rebuild-position-authority"
            )
        elif str(position_state.normalized_exposure.authority_gap_reason or "") == "authority_missing_recovery_required":
            stage = "AUTHORITY_REBUILD_PENDING"
            blockers.append("POSITION_AUTHORITY_RECOVERY_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = "rebuild_position_authority"
            recommended_command = "uv run python bot.py rebuild-position-authority --apply --yes"
        elif fee_gap_policy.closeout_blocking and not fee_gap_policy.resume_blocking:
            stage = fee_gap_policy.readiness_stage
            categories.append(fee_gap_policy.blocker_category)
            operator_next_action = fee_gap_policy.operator_next_action
            recommended_command = fee_gap_policy.recommended_command
        elif fee_gap_policy.resume_blocking:
            stage = fee_gap_policy.readiness_stage
            blockers.append("FEE_GAP_RECOVERY_REQUIRED")
            categories.append(fee_gap_policy.blocker_category)
            operator_next_action = fee_gap_policy.operator_next_action
            recommended_command = fee_gap_policy.recommended_command
        elif bool(replay_mismatch_preview.get("needs_repair")) and bool(replay_mismatch_preview.get("safe_to_apply")):
            stage = "ACCOUNTING_EXTERNAL_POSITION_REPAIR_PENDING"
            blockers.append("EXTERNAL_POSITION_ACCOUNTING_REPAIR_REQUIRED")
            categories.append("accounting_truth")
            operator_next_action = "apply_external_position_accounting_repair"
            recommended_command = "uv run python bot.py external-position-accounting-repair --apply --yes"
        elif bool(replay_mismatch_preview.get("needs_repair")):
            stage = "ACCOUNTING_REPLAY_MISMATCH_PENDING"
            blockers.append("ACCOUNTING_REPLAY_MISMATCH_REVIEW_REQUIRED")
            categories.append("accounting_truth")
            operator_next_action = "review_accounting_replay_evidence"
            recommended_command = "uv run python bot.py external-position-accounting-repair"
        elif open_order_count > 0 or recovery_required_count > 0:
            stage = "RESUME_BLOCKED_BY_POLICY"
            blockers.append("ORDER_RECOVERY_REQUIRED")
            categories.append("runtime_resume_gate")
            operator_next_action = "recover_or_reconcile_orders"
            recommended_command = "uv run python bot.py recovery-report"
        elif bool(state.halt_new_orders_blocked or state.halt_state_unresolved):
            stage = "RESUME_BLOCKED_BY_POLICY"
            blockers.append("HALT_STATE_UNRESOLVED")
            categories.append("runtime_resume_gate")
            operator_next_action = "review_halt_state"
            recommended_command = "uv run python bot.py recovery-report"

        resume_ready = not blockers
        tradeability = classify_canonical_tradeability_state(
            position_state=position_state,
            recovery_state=canonical_recovery,
            run_loop_allowed=resume_ready,
        )
        tradeability_operator_fields = build_tradeability_operator_fields(
            tradeability=tradeability,
            dust_fields=dust_context.fields,
        )

        return RuntimeReadinessSnapshot(
            recovery_stage=stage,
            resume_ready=resume_ready,
            resume_blockers=tuple(blockers),
            blocker_categories=tuple(dict.fromkeys(categories)),
            operator_next_action=(
                operator_next_action
                if not tradeability.operator_action_required or not resume_ready
                else tradeability.operator_next_action
            ),
            recommended_command=recommended_command,
            position_state=position_state,
            lot_snapshot=lot_snapshot,
            reconcile_metadata=metadata,
            fee_pending_count=fee_pending_count,
            fee_gap_recovery_required=fee_gap_required,
            fee_gap_resume_blocking=fee_gap_policy.resume_blocking,
            fee_gap_resume_policy=fee_gap_policy.resume_policy,
            fee_gap_closeout_blocking=fee_gap_policy.closeout_blocking,
            fee_gap_adjustment_count=fee_gap_adjustment_count,
            material_zero_fee_fill_count=material_zero_fee_fill_count,
            open_order_count=open_order_count,
            recovery_required_count=recovery_required_count,
            position_authority_assessment=authority_assessment,
            canonical_state=canonical_recovery.canonical_state,
            residual_class=tradeability.residual_class,
            run_loop_allowed=tradeability.run_loop_allowed,
            new_entry_allowed=tradeability.new_entry_allowed,
            closeout_allowed=tradeability.closeout_allowed,
            effective_flat=tradeability.effective_flat,
            operator_action_required=tradeability.operator_action_required,
            why_not=tradeability.why_not,
            execution_flat=canonical_recovery.execution_flat,
            accounting_flat=canonical_recovery.accounting_flat,
            tradeability=tradeability,
            tradeability_operator_fields=tradeability_operator_fields,
        )
    finally:
        if close_conn:
            conn.close()
