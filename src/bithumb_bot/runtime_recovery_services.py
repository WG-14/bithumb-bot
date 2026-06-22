from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Callable

from .config import settings
from .db_core import (
    compute_accounting_replay,
    ensure_db,
    get_portfolio_breakdown,
    portfolio_asset_total,
    portfolio_cash_total,
)
from .external_position_repair import build_external_position_accounting_repair_preview
from .oms import collect_risky_order_state
from .observability import format_log_kv
from .risk import RISK_STATE_MISMATCH
from .runtime_readiness import compute_runtime_readiness_snapshot, evaluate_clean_account_gate
from .strategy_performance import evaluate_strategy_performance_gate


STARTUP_RECOVERY_GATE_PREFIX = "startup safety gate"
STALE_RISK_STATE_MISMATCH_CLEAR_RECONCILE_REASON_CODES = {
    "FEE_PENDING_ACCOUNTING_REPAIR_COMPLETED",
    "MANUAL_RECOVERY_COMPLETED",
    "POSITION_AUTHORITY_REBUILD_COMPLETED",
    "RECENT_FILL_APPLIED",
    "RECONCILE_OK",
}


@dataclass(frozen=True)
class StartupSafetyGateService:
    state_snapshot: Callable[[], object]
    refresh_open_order_health: Callable[[], None]
    emergency_flatten_blocker: Callable[[], str | None]
    set_startup_gate_reason: Callable[[str | None], None]
    balance_split_mismatch_counter: Callable[[object], int]
    logger: logging.Logger = logging.getLogger("bithumb_bot.run")

    def evaluate(self) -> str | None:
        self.refresh_open_order_health()
        state = self.state_snapshot()
        now_ms = int(time.time() * 1000)
        reconcile_metadata: dict[str, object] = {}
        if state.last_reconcile_metadata:
            try:
                reconcile_metadata = json.loads(str(state.last_reconcile_metadata))
            except (TypeError, ValueError, json.JSONDecodeError):
                reconcile_metadata = {}

        conn = ensure_db()
        try:
            risky_state = collect_risky_order_state(
                conn,
                now_ms=now_ms,
                max_open_order_age_sec=max(1, int(settings.MAX_OPEN_ORDER_AGE_SEC)),
            )
        finally:
            conn.close()

        status_counts = {
            "pending_submit": 0,
            "accounting_pending": 0,
            "submit_unknown": 0,
            "recovery_required": 0,
            "stale_new_partial": 0,
        }
        conn = ensure_db()
        try:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status='PENDING_SUBMIT' THEN 1 ELSE 0 END) AS pending_submit_count,
                    SUM(CASE WHEN status='ACCOUNTING_PENDING' THEN 1 ELSE 0 END) AS accounting_pending_count,
                    SUM(CASE WHEN status='SUBMIT_UNKNOWN' THEN 1 ELSE 0 END) AS submit_unknown_count,
                    SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END) AS recovery_required_count,
                    SUM(
                        CASE
                            WHEN status IN ('NEW', 'PARTIAL')
                             AND (? - created_ts) > (? * 1000)
                            THEN 1
                            ELSE 0
                        END
                    ) AS stale_new_partial_count
                FROM orders
                """,
                (
                    now_ms,
                    max(1, int(settings.MAX_OPEN_ORDER_AGE_SEC)),
                ),
            ).fetchone()
        finally:
            conn.close()

        if row is not None:
            status_counts = {
                "pending_submit": int(row["pending_submit_count"] or 0),
                "accounting_pending": int(row["accounting_pending_count"] or 0),
                "submit_unknown": int(row["submit_unknown_count"] or 0),
                "recovery_required": int(row["recovery_required_count"] or 0),
                "stale_new_partial": int(row["stale_new_partial_count"] or 0),
            }

        reasons: list[str] = []
        flatten_block_reason = self.emergency_flatten_blocker()
        if flatten_block_reason:
            reasons.append(f"emergency_flatten_unresolved={flatten_block_reason}")

        if status_counts["pending_submit"] > 0:
            reasons.append(f"pending_submit_orders={status_counts['pending_submit']}")
        if status_counts.get("accounting_pending", 0) > 0:
            reasons.append(f"accounting_pending_orders={status_counts['accounting_pending']}")
        if status_counts["submit_unknown"] > 0:
            reasons.append(f"submit_unknown_orders={status_counts['submit_unknown']}")
        if status_counts["recovery_required"] > 0:
            reasons.append(f"recovery_required_orders={status_counts['recovery_required']}")
        if status_counts["stale_new_partial"] > 0:
            reasons.append(f"stale_new_partial_orders={status_counts['stale_new_partial']}")

        if state.unresolved_open_order_count > 0:
            reasons.append(f"unresolved_open_orders={state.unresolved_open_order_count}")
        if state.recovery_required_count > status_counts["recovery_required"]:
            reasons.append(f"recovery_required_orders={state.recovery_required_count}")

        portfolio_conn = ensure_db()
        try:
            readiness_snapshot = compute_runtime_readiness_snapshot(portfolio_conn)
        finally:
            portfolio_conn.close()

        normalized_position = readiness_snapshot.position_state.normalized_exposure
        residual_disposition = getattr(readiness_snapshot, "residual_disposition", None)
        residual_disposition_name = str(
            getattr(residual_disposition, "disposition", "") if residual_disposition is not None else ""
        )
        residual_run_allowed = bool(
            residual_disposition is not None and bool(getattr(residual_disposition, "run_allowed", False))
        )
        clean_account_gate = evaluate_clean_account_gate(readiness_snapshot)
        if (
            residual_disposition is not None
            and residual_disposition_name in {"BLOCKING_INCONSISTENT", "AUTHORITY_REPAIR_REQUIRED"}
            and not residual_run_allowed
        ):
            reasons.append(
                "residual_disposition="
                f"{residual_disposition_name or 'unknown'}"
                f"(run_allowed=0,"
                f"reason_code={(getattr(residual_disposition, 'reason_codes', ()) or ('residual_disposition_blocked',))[0]},"
                f"recommended_action={getattr(residual_disposition, 'recommended_action', 'review_recovery_report')})"
            )
        if not clean_account_gate.allowed and not (
            residual_disposition_name == "TRACKED_NON_EXECUTABLE" and residual_run_allowed
        ):
            reasons.append(
                "clean_account_gate="
                f"{clean_account_gate.reason_code}"
                f"(sellable_residual_qty={clean_account_gate.sellable_residual_qty:.12f},"
                f"sellable_residual_notional_krw={clean_account_gate.sellable_residual_notional_krw:.2f},"
                f"recommended_command={clean_account_gate.recommended_command})"
            )
        if readiness_snapshot.recovery_stage == "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING":
            assessment = readiness_snapshot.position_authority_assessment
            reasons.append(
                "position_authority_residual_normalization_required="
                f"{assessment.get('reason') or 'partial-close residual authority normalization required'}"
            )
        if readiness_snapshot.recovery_stage == "AUTHORITY_CORRECTION_PENDING":
            assessment = readiness_snapshot.position_authority_assessment
            reasons.append(
                "position_authority_correction_required="
                f"{assessment.get('reason') or 'authority conflict'}"
            )
        if readiness_snapshot.recovery_stage == "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING":
            assessment = readiness_snapshot.position_authority_assessment
            reasons.append(
                "position_authority_projection_repair_required="
                f"{assessment.get('reason') or 'projection/portfolio divergence'}"
            )
        if readiness_snapshot.recovery_stage == "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING":
            projection = readiness_snapshot.projection_convergence
            reasons.append(
                "position_authority_projection_convergence_required="
                f"projected_total_qty={float(projection.get('projected_total_qty') or 0.0):.12f},"
                f"portfolio_qty={float(projection.get('portfolio_qty') or 0.0):.12f},"
                f"reason={projection.get('reason') or 'none'}"
            )
        if readiness_snapshot.recovery_stage == "ACCOUNTING_EXTERNAL_POSITION_REPAIR_PENDING":
            reasons.append("external_position_accounting_repair_required=portfolio/replay mismatch after external position change")
        if readiness_snapshot.recovery_stage == "ACCOUNTING_REPLAY_MISMATCH_PENDING":
            reasons.append("accounting_replay_mismatch_review_required=portfolio and replay remain split")
        if readiness_snapshot.recovery_stage == "FEE_VALIDATION_BLOCKED":
            reasons.append("fee_validation_blocked=principal_applied_operator_review_required")
        if str(normalized_position.authority_gap_reason or "") == "authority_missing_recovery_required":
            reasons.append(
                "position_authority_gap="
                f"{normalized_position.authority_gap_reason}"
                f"(terminal_state={normalized_position.terminal_state})"
            )
        if int(readiness_snapshot.fee_pending_count or 0) > 0:
            reasons.append(f"fee_pending_auto_recovering={int(readiness_snapshot.fee_pending_count)}")

        if (
            str(settings.MODE).strip().lower() == "live"
            and bool(settings.LIVE_REAL_ORDER_ARMED)
            and not bool(settings.LIVE_DRY_RUN)
        ):
            perf_conn = ensure_db()
            try:
                performance_gate = evaluate_strategy_performance_gate(
                    perf_conn,
                    strategy_name=str(settings.STRATEGY_NAME),
                    pair=str(settings.PAIR),
                )
            finally:
                perf_conn.close()
            if not performance_gate.allowed:
                payload = performance_gate.as_dict()
                summary = dict(payload.get("summary") or {})
                reasons.append(
                    "strategy_performance_gate="
                    f"{payload.get('reason_code')}"
                    f"(sample_count={summary.get('sample_count')},"
                    f"expectancy_per_trade={summary.get('expectancy_per_trade')},"
                    f"net_pnl={summary.get('net_pnl')},"
                    f"fee_total={summary.get('fee_total')},"
                    f"profit_factor={summary.get('profit_factor')})"
                )

        fee_gap_incident = readiness_snapshot.fee_gap_incident
        try:
            fee_gap_recovery_required = int(reconcile_metadata.get("fee_gap_recovery_required", 0) or 0)
        except (TypeError, ValueError):
            fee_gap_recovery_required = 0
        try:
            fee_gap_metadata_fill_count = int(reconcile_metadata.get("material_zero_fee_fill_count", 0) or 0)
        except (TypeError, ValueError):
            fee_gap_metadata_fill_count = 0
        try:
            fee_gap_metadata_adjustment_count = int(reconcile_metadata.get("fee_gap_adjustment_count", 0) or 0)
        except (TypeError, ValueError):
            fee_gap_metadata_adjustment_count = 0
        if bool(fee_gap_incident.active_issue) and bool(fee_gap_incident.policy.resume_blocking):
            reasons.append(
                "fee_gap_recovery_required="
                f"{fee_gap_recovery_required}"
                f"(incident_kind={fee_gap_incident.incident_kind},"
                f"resolution_state={fee_gap_incident.resolution_state})"
            )
        elif (
            fee_gap_recovery_required > 0
            and fee_gap_metadata_fill_count <= 0
            and fee_gap_metadata_adjustment_count <= 0
        ):
            reasons.append(
                "fee_gap_recovery_required="
                f"{fee_gap_recovery_required}"
                "(incident_kind=reconcile_metadata,"
                "resolution_state=manual_review_required)"
            )

        submit_unknown_without_exchange_count = int(risky_state["submit_unknown_without_exchange_id_count"])
        if submit_unknown_without_exchange_count > 0:
            reasons.append(
                "submit_unknown_without_exchange_id="
                f"{submit_unknown_without_exchange_count}"
            )

        stray_remote_open_count = int(risky_state["stray_remote_open_order_count"])
        if stray_remote_open_count > 0:
            reasons.append(f"stray_remote_open_orders={stray_remote_open_count}")

        self.logger.info(
            format_log_kv(
                "[STARTUP_GATE] evaluated",
                pending_submit=status_counts["pending_submit"],
                submit_unknown=status_counts["submit_unknown"],
                recovery_required=status_counts["recovery_required"],
                stale_new_partial=status_counts["stale_new_partial"],
                unresolved_open_orders=state.unresolved_open_order_count,
                runtime_recovery_required=state.recovery_required_count,
                fee_gap_recovery_required=fee_gap_recovery_required,
                position_authority_gap_reason=normalized_position.authority_gap_reason or "-",
                submit_unknown_without_exchange_id=submit_unknown_without_exchange_count,
                stray_remote_open_orders=stray_remote_open_count,
                gate_blocked=bool(reasons),
            )
        )

        if not reasons:
            self.set_startup_gate_reason(None)
            return None

        reason = f"{STARTUP_RECOVERY_GATE_PREFIX}: " + ", ".join(reasons)
        self.logger.warning(format_log_kv("[STARTUP_GATE] blocked", reason=reason))
        self.set_startup_gate_reason(reason)
        return reason


def _risk_state_mismatch_recent_context(state) -> bool:
    return bool(
        state.halt_reason_code == RISK_STATE_MISMATCH
        or RISK_STATE_MISMATCH in str(state.last_disable_reason or "")
    )


def _stale_risk_state_mismatch_reason_code_allowed(
    *,
    reconcile_reason_code: str,
    readiness_snapshot,
) -> bool:
    if reconcile_reason_code in STALE_RISK_STATE_MISMATCH_CLEAR_RECONCILE_REASON_CODES:
        return True
    if reconcile_reason_code != "FEE_GAP_RECOVERY_REQUIRED":
        return False
    fee_gap_incident = readiness_snapshot.fee_gap_incident
    return bool(
        fee_gap_incident.incident_kind == "historical_fee_gap_repaired"
        and not fee_gap_incident.active_issue
        and not fee_gap_incident.policy.resume_blocking
    )


@dataclass(frozen=True)
class StaleRiskStateMismatchHaltService:
    balance_split_mismatch_counter: Callable[[object], int]

    def evaluate(self, *, state, startup_gate_reason: str | None) -> dict[str, object]:
        candidate = bool(_risk_state_mismatch_recent_context(state))
        reconcile_reason_code = str(state.last_reconcile_reason_code or "").strip()
        blockers: list[str] = []
        fee_gap_active_issue = False
        fee_gap_resume_blocking = False
        reason_code_allowed = False
        accounting_projection_ok = False
        broker_portfolio_converged = False
        broker_qty_known = False
        balance_source_stale = False
        if state.last_reconcile_status != "ok":
            blockers.append(f"last_reconcile_status={state.last_reconcile_status or 'none'}")
        if startup_gate_reason:
            blockers.append(f"startup_gate_blocked:{startup_gate_reason}")
        if state.emergency_flatten_blocked:
            blockers.append("emergency_flatten_unresolved")
        if self.balance_split_mismatch_counter(state.last_reconcile_metadata) > 0:
            blockers.append("balance_split_mismatch")

        conn = ensure_db()
        try:
            readiness_snapshot = compute_runtime_readiness_snapshot(conn)
            cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
            external_position_preview = build_external_position_accounting_repair_preview(conn)
            risky_state = collect_risky_order_state(
                conn,
                now_ms=int(time.time() * 1000),
                max_open_order_age_sec=max(1, int(settings.MAX_OPEN_ORDER_AGE_SEC)),
            )
            try:
                accounting_projection = compute_accounting_replay(conn)
            except RuntimeError:
                accounting_projection = None
        finally:
            conn.close()

        fee_gap_incident = readiness_snapshot.fee_gap_incident
        fee_gap_active_issue = bool(fee_gap_incident.active_issue)
        fee_gap_resume_blocking = bool(fee_gap_incident.policy.resume_blocking)
        reason_code_allowed = _stale_risk_state_mismatch_reason_code_allowed(
            reconcile_reason_code=reconcile_reason_code,
            readiness_snapshot=readiness_snapshot,
        )

        broker_evidence = readiness_snapshot.broker_position_evidence
        projection_convergence = readiness_snapshot.projection_convergence
        authority_assessment = readiness_snapshot.position_authority_assessment
        broker_qty_known = bool(broker_evidence.get("broker_qty_known"))
        balance_source_stale = bool(broker_evidence.get("balance_source_stale"))
        broker_qty = float(broker_evidence.get("broker_qty") or 0.0)
        portfolio_qty = float(projection_convergence.get("portfolio_qty") or 0.0)
        portfolio_cash = portfolio_cash_total(
            cash_available=float(cash_available),
            cash_locked=float(cash_locked),
        )
        accounting_portfolio_qty = portfolio_asset_total(
            asset_available=float(asset_available),
            asset_locked=float(asset_locked),
        )
        accounting_projection_ok = bool(
            accounting_projection is not None
            and abs(float(accounting_projection.get("replay_cash") or 0.0) - portfolio_cash) <= 1e-8
            and abs(float(accounting_projection.get("replay_qty") or 0.0) - accounting_portfolio_qty) <= 1e-12
        )
        broker_portfolio_converged = bool(
            broker_qty_known and abs(broker_qty - portfolio_qty) <= 1e-12
        )
        authority_gap_reason = str(readiness_snapshot.position_state.normalized_exposure.authority_gap_reason or "")
        metadata = readiness_snapshot.reconcile_metadata
        submit_unknown_unresolved = int(metadata.get("submit_unknown_unresolved", 0) or 0)
        remote_open_order_found = int(metadata.get("remote_open_order_found", 0) or 0)
        source_conflict_halt = int(metadata.get("source_conflict_halt", 0) or 0)
        submit_unknown_without_exchange_count = int(
            risky_state.get("submit_unknown_without_exchange_id_count", 0) or 0
        )
        stray_remote_open_count = int(risky_state.get("stray_remote_open_order_count", 0) or 0)

        if not accounting_projection_ok:
            blockers.append("accounting_projection_not_converged")
        if int(readiness_snapshot.fee_pending_count or 0) > 0:
            blockers.append(f"fee_pending_count={int(readiness_snapshot.fee_pending_count or 0)}")
        if int(readiness_snapshot.open_order_count or 0) > 0:
            blockers.append(f"open_order_count={int(readiness_snapshot.open_order_count or 0)}")
        if int(readiness_snapshot.recovery_required_count or 0) > 0:
            blockers.append(f"recovery_required_count={int(readiness_snapshot.recovery_required_count or 0)}")
        if fee_gap_active_issue:
            blockers.append("fee_gap_active_issue")
        if fee_gap_resume_blocking:
            blockers.append("fee_gap_resume_blocking")
        if not bool(projection_convergence.get("converged")):
            blockers.append(f"lot_projection_not_converged:{projection_convergence.get('reason') or 'none'}")
        if bool(authority_assessment.get("needs_correction")):
            blockers.append("position_authority_correction_required")
        if bool(authority_assessment.get("needs_residual_normalization")):
            blockers.append("position_authority_residual_normalization_required")
        if bool(authority_assessment.get("needs_full_projection_rebuild")):
            blockers.append("position_authority_full_projection_rebuild_required")
        if bool(authority_assessment.get("needs_portfolio_projection_repair")):
            blockers.append("position_authority_projection_repair_required")
        if authority_gap_reason == "authority_missing_recovery_required":
            blockers.append("position_authority_gap_missing")
        if bool(external_position_preview.get("needs_repair")):
            blockers.append("external_position_accounting_repair_required")
        if not bool(broker_evidence.get("balance_snapshot_available_for_health")):
            blockers.append("balance_snapshot_missing_for_health")
        if balance_source_stale:
            blockers.append("balance_snapshot_stale")
        if not broker_qty_known:
            blockers.append("broker_qty_unknown")
        if not broker_portfolio_converged:
            blockers.append("broker_portfolio_not_converged")
        if submit_unknown_unresolved > 0:
            blockers.append(f"submit_unknown_unresolved={submit_unknown_unresolved}")
        if remote_open_order_found > 0:
            blockers.append(f"remote_open_order_found={remote_open_order_found}")
        if source_conflict_halt > 0:
            blockers.append(f"source_conflict_halt={source_conflict_halt}")
        if submit_unknown_without_exchange_count > 0:
            blockers.append(
                f"submit_unknown_without_exchange_id={submit_unknown_without_exchange_count}"
            )
        if stray_remote_open_count > 0:
            blockers.append(f"stray_remote_open_orders={stray_remote_open_count}")

        current_evidence_converged = bool(candidate and not blockers)
        halt_reason_current_evidence = (
            "stale"
            if candidate and current_evidence_converged
            else ("current" if candidate else "not_applicable")
        )
        return {
            "stale_halt_clear_candidate": candidate,
            "stale_halt_clear_allowed": bool(candidate and current_evidence_converged),
            "stale_halt_clear_blockers": blockers,
            "stale_halt_clear_reason_code_allowed": bool(reason_code_allowed),
            "stale_halt_clear_current_evidence_converged": current_evidence_converged,
            "halt_reason_current_evidence": halt_reason_current_evidence,
            "fee_gap_resume_blocking": fee_gap_resume_blocking,
            "fee_gap_active_issue": fee_gap_active_issue,
            "stale_halt_clear_reconcile_reason_code": reconcile_reason_code or "none",
            "stale_halt_clear_last_reconcile_status": str(state.last_reconcile_status or "none"),
            "stale_halt_clear_accounting_projection_ok": accounting_projection_ok,
            "stale_halt_clear_broker_portfolio_converged": broker_portfolio_converged,
            "stale_halt_clear_lot_projection_converged": bool(projection_convergence.get("converged")),
            "stale_halt_clear_broker_qty_known": broker_qty_known,
            "stale_halt_clear_balance_source_stale": balance_source_stale,
        }
