from __future__ import annotations

import json
import time
from typing import Any

from . import runtime_state
from .config import settings
from .db_core import (
    compute_accounting_replay,
    get_external_position_adjustment_summary,
    normalize_asset_qty,
    normalize_cash_amount,
    record_external_position_adjustment,
)
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from .position_authority_incidents import PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def _read_portfolio_snapshot(conn) -> tuple[float, float, float, float]:
    row = conn.execute(
        """
        SELECT cash_available, cash_locked, asset_available, asset_locked
        FROM portfolio
        WHERE id=1
        """
    ).fetchone()
    if row is None:
        return (0.0, 0.0, 0.0, 0.0)
    return (
        normalize_cash_amount(_row_value(row, "cash_available", _row_value(row, "cash_krw", 0.0))),
        normalize_cash_amount(_row_value(row, "cash_locked", 0.0)),
        normalize_asset_qty(_row_value(row, "asset_available", _row_value(row, "asset_qty", 0.0))),
        normalize_asset_qty(_row_value(row, "asset_locked", 0.0)),
    )


def build_external_position_accounting_repair_preview(conn) -> dict[str, Any]:
    cash_available, cash_locked, asset_available, asset_locked = _read_portfolio_snapshot(conn)
    portfolio_cash = normalize_cash_amount(cash_available + cash_locked)
    portfolio_qty = normalize_asset_qty(asset_available + asset_locked)
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    adjustment_summary = get_external_position_adjustment_summary(conn)
    projection_repair_row = conn.execute(
        """
        SELECT COUNT(*) AS repair_count
        FROM position_authority_repairs
        WHERE reason=?
        """,
        (PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON,),
    ).fetchone()
    projection_repair_count = int(projection_repair_row["repair_count"] if projection_repair_row is not None else 0)
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
        replay = {"replay_cash": portfolio_cash, "replay_qty": portfolio_qty}
        replay_error = f"accounting_replay_unavailable={type(exc).__name__}: {exc}"

    replay_cash = float(replay["replay_cash"])
    replay_qty = float(replay["replay_qty"])
    cash_delta = normalize_cash_amount(portfolio_cash - replay_cash)
    asset_qty_delta = normalize_asset_qty(portfolio_qty - replay_qty)

    try:
        raw_metadata = state.last_reconcile_metadata
    except AttributeError:
        raw_metadata = None
    if isinstance(raw_metadata, dict):
        metadata = dict(raw_metadata)
    elif raw_metadata:
        try:
            parsed = json.loads(str(raw_metadata))
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = {}
        metadata = parsed if isinstance(parsed, dict) else {}
    else:
        metadata = {}
    last_reconcile_status = getattr(state, "last_reconcile_status", None)
    last_reconcile_reason_code = getattr(state, "last_reconcile_reason_code", None)
    balance_observed_ts_ms = int(metadata.get("balance_observed_ts_ms", 0) or 0)

    reasons: list[str] = []
    if open_order_count > 0:
        reasons.append(f"open_or_unresolved_orders={open_order_count}")
    if recovery_required_count > 0:
        reasons.append(f"recovery_required_orders={recovery_required_count}")
    if str(last_reconcile_status or "").lower() != "ok":
        reasons.append(f"last_reconcile_status={last_reconcile_status or 'none'}")
    if balance_observed_ts_ms <= 0:
        reasons.append("balance_snapshot_evidence_missing")
    if projection_repair_count <= 0:
        reasons.append("projection_repair_evidence_missing")
    if int(lot_snapshot.open_lot_count) > 0:
        reasons.append(f"open_exposure_lots_present={int(lot_snapshot.open_lot_count)}")
    if abs(float(reserved_exit_qty)) > 1e-12:
        reasons.append(f"reserved_exit_qty={float(reserved_exit_qty):.12f}")
    if asset_qty_delta > 1e-12:
        reasons.append(f"asset_delta_increases_position={asset_qty_delta:.12f}")
    if replay_error is not None:
        reasons.append(replay_error)

    needs_repair = bool(
        projection_repair_count > 0 and (abs(asset_qty_delta) > 1e-12 or abs(cash_delta) > 1e-8)
    )
    safe_to_apply = bool(needs_repair and not reasons)
    eligibility_reason = (
        "external-position accounting repair applicable"
        if safe_to_apply
        else ("no repair needed" if not needs_repair and not reasons else ", ".join(reasons))
    )

    return {
        "needs_repair": needs_repair,
        "safe_to_apply": safe_to_apply,
        "eligibility_reason": eligibility_reason,
        "replay_cash": replay_cash,
        "replay_qty": replay_qty,
        "portfolio_cash": float(portfolio_cash),
        "portfolio_qty": float(portfolio_qty),
        "cash_delta": float(cash_delta),
        "asset_qty_delta": float(asset_qty_delta),
        "open_order_count": open_order_count,
        "recovery_required_count": recovery_required_count,
        "open_lot_count": int(lot_snapshot.open_lot_count),
        "dust_tracking_lot_count": int(lot_snapshot.dust_tracking_lot_count),
        "reserved_exit_qty": float(reserved_exit_qty),
        "last_reconcile_status": last_reconcile_status or "none",
        "last_reconcile_reason_code": last_reconcile_reason_code or "none",
        "balance_observed_ts_ms": balance_observed_ts_ms,
        "projection_repair_count": projection_repair_count,
        "external_position_adjustment_count": int(adjustment_summary["adjustment_count"] or 0),
        "external_position_adjustment_cash_total": float(adjustment_summary["cash_total"] or 0.0),
        "external_position_adjustment_asset_total": float(adjustment_summary["asset_qty_total"] or 0.0),
        "recommended_command": (
            "uv run python bot.py external-position-accounting-repair --apply --yes"
            if safe_to_apply
            else ("uv run python bot.py recovery-report" if reasons else "none")
        ),
    }


def apply_external_position_accounting_repair(conn, *, note: str | None = None) -> dict[str, Any]:
    preview = build_external_position_accounting_repair_preview(conn)
    if not bool(preview["safe_to_apply"]):
        raise RuntimeError(
            "external-position accounting repair is not safe to apply: "
            f"{preview['eligibility_reason']}"
        )

    adjustment_basis = {
        "event_type": "external_position_adjustment",
        "last_reconcile_status": preview["last_reconcile_status"],
        "last_reconcile_reason_code": preview["last_reconcile_reason_code"],
        "balance_observed_ts_ms": preview["balance_observed_ts_ms"],
        "replay_cash_before": preview["replay_cash"],
        "replay_qty_before": preview["replay_qty"],
        "portfolio_cash_basis": preview["portfolio_cash"],
        "portfolio_qty_basis": preview["portfolio_qty"],
        "open_lot_count": preview["open_lot_count"],
        "dust_tracking_lot_count": preview["dust_tracking_lot_count"],
        "reserved_exit_qty": preview["reserved_exit_qty"],
    }
    adjustment = record_external_position_adjustment(
        conn,
        event_ts=int(time.time() * 1000),
        asset_qty_delta=float(preview["asset_qty_delta"]),
        cash_delta=float(preview["cash_delta"]),
        source="manual_external_position_recovery",
        reason="external_position_accounting_repair",
        adjustment_basis=adjustment_basis,
        note=note,
    )
    return {"preview": preview, "adjustment": adjustment}
