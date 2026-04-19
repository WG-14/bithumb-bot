from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from . import runtime_state
from .config import settings
from .db_core import ensure_db, get_broker_fill_observation_summary, portfolio_asset_total
from .dust import build_dust_display_context, build_position_state_model
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty


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
    fee_gap_adjustment_count: int
    material_zero_fee_fill_count: int
    open_order_count: int
    recovery_required_count: int

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
            "fee_gap_adjustment_count": int(self.fee_gap_adjustment_count),
            "material_zero_fee_fill_count": int(self.material_zero_fee_fill_count),
            "open_order_count": int(self.open_order_count),
            "recovery_required_count": int(self.recovery_required_count),
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


def _safe_broker_fill_observation_summary(conn: Any) -> dict[str, object]:
    try:
        return get_broker_fill_observation_summary(conn)
    except (AssertionError, sqlite3.OperationalError):
        return {
            "observation_count": 0,
            "fee_pending_count": 0,
            "accounting_complete_count": 0,
            "last_event_ts": None,
        }


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
        )
        observation_summary = _safe_broker_fill_observation_summary(conn)
        fee_pending_count = int(observation_summary.get("fee_pending_count") or 0)
        fee_gap_required = _metadata_int(metadata, "fee_gap_recovery_required") > 0
        fee_gap_adjustment_count = _metadata_int(metadata, "fee_gap_adjustment_count")
        material_zero_fee_fill_count = _metadata_int(metadata, "material_zero_fee_fill_count")

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
        elif str(position_state.normalized_exposure.authority_gap_reason or "") == "authority_missing_recovery_required":
            stage = "AUTHORITY_REBUILD_PENDING"
            blockers.append("POSITION_AUTHORITY_RECOVERY_REQUIRED")
            categories.append("executable_authority")
            operator_next_action = "rebuild_position_authority"
            recommended_command = "uv run python bot.py rebuild-position-authority --apply --yes"
        elif fee_gap_required:
            stage = "HISTORICAL_FEE_GAP_PENDING"
            blockers.append("FEE_GAP_RECOVERY_REQUIRED")
            categories.append("historical_accounting_debt")
            operator_next_action = "review_fee_gap_accounting_repair"
            recommended_command = "uv run python bot.py fee-gap-accounting-repair"
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

        return RuntimeReadinessSnapshot(
            recovery_stage=stage,
            resume_ready=not blockers,
            resume_blockers=tuple(blockers),
            blocker_categories=tuple(dict.fromkeys(categories)),
            operator_next_action=operator_next_action,
            recommended_command=recommended_command,
            position_state=position_state,
            lot_snapshot=lot_snapshot,
            reconcile_metadata=metadata,
            fee_pending_count=fee_pending_count,
            fee_gap_recovery_required=fee_gap_required,
            fee_gap_adjustment_count=fee_gap_adjustment_count,
            material_zero_fee_fill_count=material_zero_fee_fill_count,
            open_order_count=open_order_count,
            recovery_required_count=recovery_required_count,
        )
    finally:
        if close_conn:
            conn.close()
