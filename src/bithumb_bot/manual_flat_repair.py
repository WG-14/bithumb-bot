from __future__ import annotations

import time
from typing import Any

from . import runtime_state
from .config import settings
from .db_core import (
    compute_accounting_replay,
    get_manual_flat_accounting_repair_summary,
    normalize_asset_qty,
    normalize_cash_amount,
    record_manual_flat_accounting_repair,
)
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty


def _read_portfolio_snapshot_for_preview(conn) -> tuple[float, float, float, float, float, float]:
    row = conn.execute(
        """
        SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
        FROM portfolio
        WHERE id=1
        """
    ).fetchone()

    if row is None:
        portfolio_cash = normalize_cash_amount(settings.START_CASH_KRW)
        return (
            portfolio_cash,
            0.0,
            portfolio_cash,
            0.0,
            0.0,
            0.0,
        )

    payload = dict(row) if not isinstance(row, dict) else row
    portfolio_cash = normalize_cash_amount(payload.get("cash_krw", settings.START_CASH_KRW))
    portfolio_qty = normalize_asset_qty(payload.get("asset_qty", 0.0))
    cash_available = normalize_cash_amount(payload.get("cash_available", portfolio_cash))
    cash_locked = normalize_cash_amount(payload.get("cash_locked", 0.0))
    asset_available = normalize_asset_qty(payload.get("asset_available", portfolio_qty))
    asset_locked = normalize_asset_qty(payload.get("asset_locked", 0.0))
    return (
        portfolio_cash,
        portfolio_qty,
        cash_available,
        cash_locked,
        asset_available,
        asset_locked,
    )


def build_manual_flat_accounting_repair_preview(conn) -> dict[str, Any]:
    (
        portfolio_cash,
        portfolio_qty,
        _cash_available,
        _cash_locked,
        asset_available,
        asset_locked,
    ) = _read_portfolio_snapshot_for_preview(conn)
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    repair_summary = get_manual_flat_accounting_repair_summary(conn)
    state = runtime_state.snapshot()

    unresolved_row = conn.execute(
        """
        SELECT
            COUNT(*) AS open_order_count,
            COALESCE(SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END), 0) AS recovery_required_count
        FROM orders
        WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
        """
    ).fetchone()
    open_order_count = int(unresolved_row["open_order_count"] if unresolved_row is not None else 0)
    recovery_required_count = int(unresolved_row["recovery_required_count"] if unresolved_row is not None else 0)

    replay_error = None
    try:
        replay = compute_accounting_replay(conn)
    except Exception as exc:
        replay = {
            "replay_cash": portfolio_cash,
            "replay_qty": portfolio_qty,
        }
        replay_error = f"accounting_replay_unavailable={type(exc).__name__}: {exc}"

    replay_cash = float(replay["replay_cash"])
    replay_qty = float(replay["replay_qty"])
    cash_delta = portfolio_cash - replay_cash
    asset_qty_delta = portfolio_qty - replay_qty

    reasons: list[str] = []
    if open_order_count > 0:
        reasons.append(f"open_or_unresolved_orders={open_order_count}")
    if recovery_required_count > 0:
        reasons.append(f"recovery_required_orders={recovery_required_count}")
    if str(state.last_reconcile_status or "").lower() != "ok":
        reasons.append(f"last_reconcile_status={state.last_reconcile_status or 'none'}")
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
    if replay_error is not None:
        reasons.append(replay_error)

    needs_repair = bool(abs(asset_qty_delta) > 1e-12 or abs(cash_delta) > 1e-8)
    safe_to_apply = bool(needs_repair and not reasons)
    eligibility_reason = "manual-flat accounting repair applicable" if safe_to_apply else (
        "no repair needed" if not needs_repair and not reasons else ", ".join(reasons or ["manual-flat repair not applicable"])
    )

    return {
        "needs_repair": needs_repair,
        "safe_to_apply": safe_to_apply,
        "eligibility_reason": eligibility_reason,
        "open_order_count": open_order_count,
        "recovery_required_count": recovery_required_count,
        "last_reconcile_status": state.last_reconcile_status or "none",
        "last_reconcile_reason_code": state.last_reconcile_reason_code or "none",
        "replay_cash": replay_cash,
        "replay_qty": replay_qty,
        "portfolio_cash": float(portfolio_cash),
        "portfolio_qty": float(portfolio_qty),
        "cash_delta": float(cash_delta),
        "asset_qty_delta": float(asset_qty_delta),
        "open_lot_count": int(lot_snapshot.open_lot_count),
        "dust_tracking_lot_count": int(lot_snapshot.dust_tracking_lot_count),
        "reserved_exit_qty": float(reserved_exit_qty),
        "manual_flat_accounting_repair_count": int(repair_summary["repair_count"] or 0),
        "manual_flat_accounting_repair_cash_total": float(repair_summary["cash_total"] or 0.0),
        "manual_flat_accounting_repair_asset_total": float(repair_summary["asset_qty_total"] or 0.0),
        "recommended_command": "uv run python bot.py manual-flat-accounting-repair --apply --yes" if safe_to_apply else (
            "uv run python bot.py recovery-report" if reasons else "none"
        ),
    }


def apply_manual_flat_accounting_repair(conn, *, note: str | None = None) -> dict[str, Any]:
    preview = build_manual_flat_accounting_repair_preview(conn)
    if not bool(preview["safe_to_apply"]):
        raise RuntimeError(f"manual-flat accounting repair is not safe to apply: {preview['eligibility_reason']}")

    event_ts = int(time.time() * 1000)
    repair_basis = {
        "event_type": "manual_flat_accounting_repair",
        "last_reconcile_status": preview["last_reconcile_status"],
        "last_reconcile_reason_code": preview["last_reconcile_reason_code"],
        "open_order_count": preview["open_order_count"],
        "recovery_required_count": preview["recovery_required_count"],
        "open_lot_count": preview["open_lot_count"],
        "dust_tracking_lot_count": preview["dust_tracking_lot_count"],
        "reserved_exit_qty": preview["reserved_exit_qty"],
        "replay_cash_before": preview["replay_cash"],
        "replay_qty_before": preview["replay_qty"],
        "portfolio_cash_basis": preview["portfolio_cash"],
        "portfolio_qty_basis": preview["portfolio_qty"],
    }
    repair = record_manual_flat_accounting_repair(
        conn,
        event_ts=event_ts,
        asset_qty_delta=float(preview["asset_qty_delta"]),
        cash_delta=float(preview["cash_delta"]),
        source="manual_flat_recovery",
        reason="manual_flat_accounting_repair",
        repair_basis=repair_basis,
        note=note,
    )
    return {"preview": preview, "repair": repair}
