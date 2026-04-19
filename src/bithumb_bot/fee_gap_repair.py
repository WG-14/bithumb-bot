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
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from .manual_flat_repair import build_manual_flat_accounting_repair_preview
from .runtime_readiness import compute_runtime_readiness_snapshot


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


def _matching_fee_gap_repair_present(
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


def build_fee_gap_accounting_repair_preview(conn) -> dict[str, Any]:
    state = runtime_state.snapshot()
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

    already_repaired = _matching_fee_gap_repair_present(
        repair_summary=repair_summary,
        fee_gap_adjustment_count=fee_gap_adjustment_count,
        fee_gap_adjustment_total_krw=fee_gap_adjustment_total_krw,
        fee_gap_adjustment_latest_event_ts=fee_gap_adjustment_latest_event_ts,
        material_zero_fee_fill_count=material_zero_fee_fill_count,
        material_zero_fee_fill_latest_ts=material_zero_fee_fill_latest_ts,
    )
    needs_repair = bool(
        fee_gap_recovery_required > 0
        and material_zero_fee_fill_count > 0
        and fee_gap_adjustment_count > 0
        and not already_repaired
    )

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
        readiness.recovery_stage == "AUTHORITY_REBUILD_PENDING"
        or str(readiness.position_state.normalized_exposure.authority_gap_reason or "")
        == "authority_missing_recovery_required"
    )
    blocked_by_open_exposure = bool(
        int(lot_snapshot.open_lot_count) > 0
        or abs(float(asset_available)) > 1e-12
        or abs(float(asset_locked)) > 1e-12
    )
    blocked_by_dust_residue = bool(int(lot_snapshot.dust_tracking_lot_count) > 0)

    if already_repaired and not needs_repair:
        eligibility_reason = "matching fee-gap accounting repair already recorded"
    elif not needs_repair and not reasons:
        eligibility_reason = "no fee-gap accounting repair needed"
    else:
        eligibility_reason = ", ".join(reasons or ["fee-gap accounting repair not applicable"])

    safe_to_apply = bool(needs_repair and not reasons)
    if safe_to_apply:
        eligibility_reason = "fee-gap accounting repair applicable"

    return {
        "needs_repair": needs_repair,
        "safe_to_apply": safe_to_apply,
        "eligibility_reason": eligibility_reason,
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
        "fee_gap_accounting_repair_count": int(repair_summary.get("repair_count") or 0),
        "fee_gap_accounting_repair_last_event_ts": repair_summary.get("last_event_ts"),
        "recovery_stage": readiness.recovery_stage,
        "blocker_categories": list(readiness.blocker_categories),
        "blocked_by_authority_rebuild": blocked_by_authority_rebuild,
        "blocked_by_open_exposure": blocked_by_open_exposure,
        "blocked_by_dust_residue": blocked_by_dust_residue,
        "next_required_action": (
            "rebuild_position_authority"
            if blocked_by_authority_rebuild
            else (
                "resolve_open_exposure_before_fee_gap_repair"
                if blocked_by_open_exposure or blocked_by_dust_residue
                else ("apply_fee_gap_accounting_repair" if safe_to_apply else "review_recovery_report")
            )
        ),
        "recommended_command": (
            "uv run python bot.py fee-gap-accounting-repair --apply --yes"
            if safe_to_apply
            else (
                "uv run python bot.py rebuild-position-authority"
                if blocked_by_authority_rebuild
                else ("uv run python bot.py recovery-report" if needs_repair or reasons else "none")
            )
        ),
    }


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
