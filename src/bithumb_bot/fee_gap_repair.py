from __future__ import annotations

import json
import time
from typing import Any

from . import runtime_state
from .config import settings
from .db_core import (
    get_fee_gap_accounting_repair_summary,
    normalize_asset_qty,
    normalize_cash_amount,
    record_fee_gap_accounting_repair,
)
from .fee_authority import resolve_fee_authority_snapshot
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from .manual_flat_repair import build_manual_flat_accounting_repair_preview
from .recovery_policy import classify_canonical_recovery_state
from .runtime_readiness import compute_runtime_readiness_snapshot

_POSITION_RESIDUE_REPAIR_BLOCKERS = {
    "portfolio_not_flat",
    "lot_residue_present",
    "reserved_exit_qty",
}


def _metadata_int(metadata: dict[str, object], key: str) -> int:
    try:
        return int(metadata.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0


def _metadata_float(metadata: dict[str, object], key: str) -> float:
    try:
        return float(metadata.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _reason_prefix(reason: str) -> str:
    return str(reason).split("=", 1)[0]


def build_fee_gap_accounting_repair_preview(conn) -> dict[str, Any]:
    state = runtime_state.snapshot()
    try:
        fee_authority_evidence = resolve_fee_authority_snapshot(settings.PAIR).as_dict()
    except Exception as exc:
        fee_authority_evidence = {
            "unavailable": True,
            "error": f"{type(exc).__name__}: {exc}",
        }
    try:
        metadata = json.loads(str(state.last_reconcile_metadata or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        metadata = {}

    repair_summary = get_fee_gap_accounting_repair_summary(conn)
    readiness = compute_runtime_readiness_snapshot(conn)
    manual_flat_preview = build_manual_flat_accounting_repair_preview(conn)
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    unresolved_row = conn.execute(
        """
        SELECT
            COUNT(*) AS open_order_count,
            COALESCE(SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END), 0) AS recovery_required_count
        FROM orders
        WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
        """
    ).fetchone()
    portfolio_row = conn.execute(
        """
        SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
        FROM portfolio
        WHERE id=1
        """
    ).fetchone()

    fee_gap_recovery_required = _metadata_int(metadata, "fee_gap_recovery_required")
    material_zero_fee_fill_count = _metadata_int(metadata, "material_zero_fee_fill_count")
    material_zero_fee_fill_latest_ts = _metadata_int(metadata, "material_zero_fee_fill_latest_ts")
    fee_gap_adjustment_count = _metadata_int(metadata, "fee_gap_adjustment_count")
    fee_gap_adjustment_latest_event_ts = _metadata_int(metadata, "fee_gap_adjustment_latest_event_ts")
    fee_gap_adjustment_total_krw = normalize_cash_amount(_metadata_float(metadata, "fee_gap_adjustment_total_krw"))
    external_cash_adjustment_reason = str(metadata.get("external_cash_adjustment_reason") or "none")

    open_order_count = int(unresolved_row["open_order_count"] if unresolved_row is not None else 0)
    recovery_required_count = int(unresolved_row["recovery_required_count"] if unresolved_row is not None else 0)
    portfolio_asset_qty = normalize_asset_qty(portfolio_row["asset_qty"] if portfolio_row is not None else 0.0)
    asset_available = normalize_asset_qty(portfolio_row["asset_available"] if portfolio_row is not None else portfolio_asset_qty)
    asset_locked = normalize_asset_qty(portfolio_row["asset_locked"] if portfolio_row is not None else 0.0)
    cash_krw = normalize_cash_amount(portfolio_row["cash_krw"] if portfolio_row is not None else settings.START_CASH_KRW)
    cash_available = normalize_cash_amount(portfolio_row["cash_available"] if portfolio_row is not None else cash_krw)
    cash_locked = normalize_cash_amount(portfolio_row["cash_locked"] if portfolio_row is not None else 0.0)

    # Runtime readiness owns the current operator verdict. This preview may add
    # mutation-specific safety checks, but it must not reinterpret the incident.
    incident = readiness.fee_gap_incident
    policy = incident.policy
    already_repaired = bool(incident.already_repaired)
    needs_repair = bool(incident.needs_repair)

    reasons: list[str] = []
    if fee_gap_recovery_required <= 0:
        reasons.append("fee_gap_recovery_not_required")
    if material_zero_fee_fill_count <= 0:
        reasons.append("material_zero_fee_fill_count=0")
    if fee_gap_adjustment_count <= 0:
        reasons.append("fee_gap_adjustment_count=0")
    if external_cash_adjustment_reason not in {"reconcile_fee_gap_cash_drift", "none"}:
        reasons.append(f"external_cash_adjustment_reason={external_cash_adjustment_reason}")
    if open_order_count > 0:
        reasons.append(f"open_or_unresolved_orders={open_order_count}")
    if recovery_required_count > 0:
        reasons.append(f"recovery_required_orders={recovery_required_count}")
    if str(state.last_reconcile_status or "").lower() != "ok":
        reasons.append(f"last_reconcile_status={state.last_reconcile_status or 'none'}")
    if bool(manual_flat_preview.get("needs_repair")):
        reasons.append(
            "manual_flat_accounting_repair_pending="
            f"{manual_flat_preview.get('eligibility_reason') or 'manual_flat_accounting_repair_required'}"
        )
    if abs(float(asset_available)) > 1e-12 or abs(float(asset_locked)) > 1e-12:
        reasons.append(
            "portfolio_not_flat="
            f"asset_available={float(asset_available):.12f},asset_locked={float(asset_locked):.12f}"
        )
    if int(lot_snapshot.open_lot_count) > 0 or int(lot_snapshot.dust_tracking_lot_count) > 0:
        reasons.append(
            "lot_residue_present="
            f"open_lot_count={int(lot_snapshot.open_lot_count)},dust_tracking_lot_count={int(lot_snapshot.dust_tracking_lot_count)}"
        )
    if abs(float(reserved_exit_qty)) > 1e-12:
        reasons.append(f"reserved_exit_qty={float(reserved_exit_qty):.12f}")

    blocked_by_authority_rebuild = bool(
        readiness.recovery_stage in {
            "AUTHORITY_REBUILD_PENDING",
            "AUTHORITY_CORRECTION_PENDING",
            "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING",
        }
        or str(readiness.position_state.normalized_exposure.authority_gap_reason or "")
        == "authority_missing_recovery_required"
    )
    blocked_by_open_exposure = bool(
        int(lot_snapshot.open_lot_count) > 0
        or abs(float(asset_available)) > 1e-12
        or abs(float(asset_locked)) > 1e-12
    )
    blocked_by_dust_residue = bool(int(lot_snapshot.dust_tracking_lot_count) > 0)
    has_executable_open_exposure = bool(
        int(lot_snapshot.open_lot_count) > 0
        and bool(readiness.position_state.normalized_exposure.has_executable_exposure)
    )
    canonical_recovery = classify_canonical_recovery_state(
        position_state=readiness.position_state,
        lot_snapshot=lot_snapshot,
        portfolio_asset_qty=portfolio_asset_qty,
        reserved_exit_qty=reserved_exit_qty,
    )
    if already_repaired and not needs_repair:
        eligibility_reason = "matching fee-gap accounting repair already recorded"
    elif not needs_repair and not reasons:
        eligibility_reason = "no fee-gap accounting repair needed"
    else:
        eligibility_reason = ", ".join(reasons or ["fee-gap accounting repair not applicable"])

    reason_prefixes = {_reason_prefix(reason) for reason in reasons if str(reason)}
    mutation_reasons_allow_tracked_dust = bool(reason_prefixes) and reason_prefixes <= _POSITION_RESIDUE_REPAIR_BLOCKERS
    safe_to_apply = bool(
        needs_repair
        and (
            not reasons
            or (
                str(policy.repair_eligibility_state) == "safe_to_apply_with_tracked_dust"
                and mutation_reasons_allow_tracked_dust
            )
        )
    )
    if safe_to_apply:
        eligibility_reason = (
            "fee-gap accounting repair applicable with tracked dust-only residue"
            if str(policy.repair_eligibility_state) == "safe_to_apply_with_tracked_dust"
            else "fee-gap accounting repair applicable"
        )
    elif needs_repair and str(policy.repair_eligibility_state) == "blocked_until_flattened":
        eligibility_reason = (
            "fee-gap accounting repair deferred until open position is flat: "
            + ", ".join(policy.repair_blocker_reasons)
        )

    preview = {
        "needs_repair": needs_repair,
        "safe_to_apply": safe_to_apply,
        "eligibility_reason": eligibility_reason,
        "repair_eligibility_state": policy.repair_eligibility_state,
        "repair_blocker_reasons": list(policy.repair_blocker_reasons),
        "resume_policy": policy.resume_policy,
        "resume_blocking": bool(policy.resume_blocking),
        "closeout_blocking": bool(policy.closeout_blocking),
        "fee_gap_policy_reason": policy.policy_reason,
        "fee_authority": fee_authority_evidence,
        "fee_gap_incident": incident.as_dict(),
        "incident_kind": incident.incident_kind,
        "incident_scope": incident.incident_scope,
        "resolution_state": incident.resolution_state,
        "active_issue": bool(incident.active_issue),
        "historical_context": bool(incident.historical_context),
        "already_repaired": already_repaired,
        "open_order_count": open_order_count,
        "recovery_required_count": recovery_required_count,
        "last_reconcile_status": state.last_reconcile_status or "none",
        "last_reconcile_reason_code": state.last_reconcile_reason_code or "none",
        "fee_gap_recovery_required": fee_gap_recovery_required,
        "material_zero_fee_fill_count": material_zero_fee_fill_count,
        "material_zero_fee_fill_latest_ts": material_zero_fee_fill_latest_ts,
        "fee_gap_adjustment_count": fee_gap_adjustment_count,
        "fee_gap_adjustment_total_krw": fee_gap_adjustment_total_krw,
        "fee_gap_adjustment_latest_event_ts": fee_gap_adjustment_latest_event_ts,
        "external_cash_adjustment_reason": external_cash_adjustment_reason,
        "portfolio_cash": float(cash_krw),
        "cash_available": float(cash_available),
        "cash_locked": float(cash_locked),
        "portfolio_qty": float(portfolio_asset_qty),
        "asset_available": float(asset_available),
        "asset_locked": float(asset_locked),
        "open_lot_count": int(lot_snapshot.open_lot_count),
        "dust_tracking_lot_count": int(lot_snapshot.dust_tracking_lot_count),
        "reserved_exit_qty": float(reserved_exit_qty),
        "canonical_state": canonical_recovery.canonical_state,
        "execution_flat": bool(canonical_recovery.execution_flat),
        "accounting_flat": bool(canonical_recovery.accounting_flat),
        "closeout_blocking_residue": bool(canonical_recovery.closeout_blocking_residue),
        "residue_kind": canonical_recovery.residue_kind,
        "fee_gap_accounting_repair_count": int(repair_summary.get("repair_count") or 0),
        "fee_gap_accounting_repair_last_event_ts": repair_summary.get("last_event_ts"),
        "recovery_stage": readiness.recovery_stage,
        "blocker_categories": list(readiness.blocker_categories),
        "blocked_by_authority_rebuild": blocked_by_authority_rebuild,
        "blocked_by_authority_correction": readiness.recovery_stage == "AUTHORITY_CORRECTION_PENDING",
        "blocked_by_authority_residual_normalization": (
            readiness.recovery_stage == "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING"
        ),
        "blocked_by_open_exposure": blocked_by_open_exposure,
        "blocked_by_dust_residue": blocked_by_dust_residue,
        "repair_safety_reasons": list(dict.fromkeys(reasons)),
        "next_required_action": policy.next_required_action,
        "recommended_command": policy.recommended_command,
    }
    preview.update(policy.as_dict())
    return preview


def apply_fee_gap_accounting_repair(conn, *, note: str | None = None) -> dict[str, Any]:
    preview = build_fee_gap_accounting_repair_preview(conn)
    if not bool(preview["safe_to_apply"]):
        raise RuntimeError(f"fee-gap accounting repair is not safe to apply: {preview['eligibility_reason']}")

    event_ts = int(time.time() * 1000)
    repair_basis = {
        "event_type": "fee_gap_accounting_repair",
        "last_reconcile_status": preview["last_reconcile_status"],
        "last_reconcile_reason_code": preview["last_reconcile_reason_code"],
        "open_order_count": preview["open_order_count"],
        "recovery_required_count": preview["recovery_required_count"],
        "material_zero_fee_fill_count": preview["material_zero_fee_fill_count"],
        "material_zero_fee_fill_latest_ts": preview["material_zero_fee_fill_latest_ts"],
        "fee_gap_adjustment_count": preview["fee_gap_adjustment_count"],
        "fee_gap_adjustment_total_krw": preview["fee_gap_adjustment_total_krw"],
        "fee_gap_adjustment_latest_event_ts": preview["fee_gap_adjustment_latest_event_ts"],
        "external_cash_adjustment_reason": preview["external_cash_adjustment_reason"],
        "open_lot_count": preview["open_lot_count"],
        "dust_tracking_lot_count": preview["dust_tracking_lot_count"],
        "reserved_exit_qty": preview["reserved_exit_qty"],
        "portfolio_cash_basis": preview["portfolio_cash"],
        "portfolio_qty_basis": preview["portfolio_qty"],
        "fee_authority": preview["fee_authority"],
    }
    repair = record_fee_gap_accounting_repair(
        conn,
        event_ts=event_ts,
        source="manual_fee_gap_recovery",
        reason="fee_gap_accounting_repair",
        repair_basis=repair_basis,
        note=note,
    )
    return {"preview": preview, "repair": repair}
