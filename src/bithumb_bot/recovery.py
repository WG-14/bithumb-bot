from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from enum import Enum

from .broker.base import (
    Broker,
    BrokerFill,
    BrokerIdentifierMismatchError,
    BrokerOrder,
    BrokerRejectError,
    BrokerSchemaError,
    BrokerTemporaryError,
)
from .broker.balance_source import fetch_balance_snapshot
from .broker.order_rules import get_effective_order_rules
from .config import settings
from .db_core import (
    ensure_db,
    record_broker_fill_observation,
    get_portfolio_breakdown,
    init_portfolio,
    portfolio_cash_total,
    normalize_cash_amount,
    record_external_cash_adjustment,
    set_portfolio_breakdown,
)
from .dust import build_dust_display_context, build_position_state_model, classify_dust_residual, dust_qty_gap_tolerance
from .execution import (
    LiveFillFeeValidationError,
    apply_fill_and_trade,
    apply_fill_principal_with_pending_fee,
    order_fill_tolerance,
    record_order_if_missing,
)
from .fee_observation import fee_accounting_status
from .fill_reading import FillReadPolicy, get_broker_fills
from .lifecycle import mark_harmless_dust_positions, summarize_position_lots
from .oms import (
    get_open_orders,
    record_status_transition,
    set_exchange_order_id,
    set_status,
    synchronize_order_state_invariants,
    validate_status_transition,
)
from . import runtime_state
from .notifier import format_event, notify
from .observability import safety_event
from .reason_codes import AMBIGUOUS_RECENT_FILL, AMBIGUOUS_SUBMIT, RECONCILE_MISMATCH, WEAK_ORDER_CORRELATION


LOCAL_RECONCILE_STATUSES = ("PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN", "ACCOUNTING_PENDING", "CANCEL_REQUESTED")

REASON_REMOTE_OPEN_ORDER_FOUND = "REMOTE_OPEN_ORDER_FOUND"
REASON_RECENT_FILL_APPLIED = "RECENT_FILL_APPLIED"
REASON_SUBMIT_UNKNOWN_UNRESOLVED = "SUBMIT_UNKNOWN_UNRESOLVED"
REASON_STARTUP_GATE_BLOCKED = "STARTUP_GATE_BLOCKED"
REASON_SOURCE_CONFLICT_HALT = "SOURCE_CONFLICT_HALT"
REASON_RECONCILE_OK = "RECONCILE_OK"
REASON_RECONCILE_FAILED = "RECONCILE_FAILED"
REASON_RECENT_FILL_INVALID_PRICE = "RECENT_FILL_INVALID_PRICE"
REASON_IDENTIFIER_LOOKUP_REQUIRES_RECOVERY = "IDENTIFIER_LOOKUP_REQUIRES_RECOVERY"
REASON_FEE_GAP_RECOVERY_REQUIRED = "FEE_GAP_RECOVERY_REQUIRED"
REASON_FILL_FEE_PENDING_RECOVERY_REQUIRED = "FILL_FEE_PENDING_RECOVERY_REQUIRED"

OPEN_ORDER_TRUSTED_STATUSES = {"PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN", "ACCOUNTING_PENDING", "CANCEL_REQUESTED"}
UNRESOLVED_ORDER_STATUSES = {"PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN", "ACCOUNTING_PENDING", "RECOVERY_REQUIRED", "CANCEL_REQUESTED"}
NON_CLEARING_RECONCILE_REASON_CODES = {
    REASON_RECONCILE_FAILED,
    REASON_SOURCE_CONFLICT_HALT,
    REASON_STARTUP_GATE_BLOCKED,
    REASON_SUBMIT_UNKNOWN_UNRESOLVED,
    REASON_RECENT_FILL_INVALID_PRICE,
    REASON_IDENTIFIER_LOOKUP_REQUIRES_RECOVERY,
    REASON_FILL_FEE_PENDING_RECOVERY_REQUIRED,
}
CANCEL_REQUESTED_STATUS = "CANCEL_REQUESTED"
TERMINAL_TRUTH_STATUSES = {"FILLED", "CANCELED", "FAILED"}


class RecoveryDisposition(str, Enum):
    AUTO_RECOVERABLE_CANDIDATE = "auto_recoverable_candidate"
    MANUAL_RECOVERY_REQUIRED = "manual_recovery_required"
    HARD_STOP = "hard_stop"


class RecoveryProgressState(str, Enum):
    EVALUATING = "evaluating"
    CANDIDATE_IDENTIFIED = "candidate_identified"
    MANUAL_INTERVENTION_REQUIRED = "manual_intervention_required"
    HALTED = "halted"


@dataclass(frozen=True)
class RecoveryClassification:
    disposition: RecoveryDisposition
    progress_state: RecoveryProgressState
    reason: str


@dataclass(frozen=True)
class SubmitUnknownResolution:
    recovered: bool
    applied_fill: bool
    resolved_client_order_id: str | None
    resolved_exchange_order_id: str | None
    reason_code: str | None = None
    metadata_updates: dict[str, int | str] | None = None


def classify_recovery_outcome(
    *,
    reason_code: str,
    metadata: dict[str, int | str],
    source_conflicts: list[str],
) -> RecoveryClassification:
    """Classifies reconcile outcomes into a conservative recovery state-machine skeleton."""
    if source_conflicts or reason_code == REASON_SOURCE_CONFLICT_HALT:
        return RecoveryClassification(
            disposition=RecoveryDisposition.HARD_STOP,
            progress_state=RecoveryProgressState.HALTED,
            reason="reconcile source conflict requires hard stop",
        )

    if int(metadata.get("submit_unknown_unresolved", 0)) > 0:
        return RecoveryClassification(
            disposition=RecoveryDisposition.MANUAL_RECOVERY_REQUIRED,
            progress_state=RecoveryProgressState.MANUAL_INTERVENTION_REQUIRED,
            reason="submit_unknown unresolved with insufficient evidence",
        )

    if int(metadata.get("startup_gate_blocked", 0)) > 0:
        return RecoveryClassification(
            disposition=RecoveryDisposition.MANUAL_RECOVERY_REQUIRED,
            progress_state=RecoveryProgressState.MANUAL_INTERVENTION_REQUIRED,
            reason="startup gate remains blocked",
        )

    if int(metadata.get("fee_pending_auto_recovering", 0)) > 0:
        return RecoveryClassification(
            disposition=RecoveryDisposition.AUTO_RECOVERABLE_CANDIDATE,
            progress_state=RecoveryProgressState.CANDIDATE_IDENTIFIED,
            reason="broker fill observed but fee is pending; automatic accounting reconcile remains in progress",
        )

    if int(metadata.get("fee_gap_recovery_required", 0)) > 0:
        return RecoveryClassification(
            disposition=RecoveryDisposition.MANUAL_RECOVERY_REQUIRED,
            progress_state=RecoveryProgressState.MANUAL_INTERVENTION_REQUIRED,
            reason="fee-related cash drift requires manual accounting recovery",
        )

    if int(metadata.get("recent_fill_applied", 0)) > 0:
        return RecoveryClassification(
            disposition=RecoveryDisposition.AUTO_RECOVERABLE_CANDIDATE,
            progress_state=RecoveryProgressState.CANDIDATE_IDENTIFIED,
            reason="recent fill evidence reconciled consistently",
        )

    return RecoveryClassification(
        disposition=RecoveryDisposition.MANUAL_RECOVERY_REQUIRED,
        progress_state=RecoveryProgressState.EVALUATING,
        reason="no deterministic auto-recovery evidence",
    )


def load_recent_order_lifecycle(conn, *, limit: int = 5) -> list[dict[str, str | int | float]]:
    rows = conn.execute(
        """
        SELECT
            client_order_id,
            submit_attempt_id,
            exchange_order_id,
            status,
            side,
            qty_req,
            intended_lot_count,
            executable_lot_count,
            final_intended_qty,
            final_submitted_qty,
            created_ts
        FROM orders
        ORDER BY created_ts DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()

    lifecycle: list[dict[str, str | int | float]] = []
    for row in rows:
        context = _load_submit_attempt_context(conn, row=row)
        submit_attempt_id = str(context.get("submit_attempt_id") or "")
        lot_basis_qty, lot_basis_source = _order_lot_basis_qty(row=row)
        submit_event = None
        intent_event = conn.execute(
            """
            SELECT intent_ts, event_ts
            FROM order_events
            WHERE client_order_id=? AND event_type='intent_created'
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(row["client_order_id"]),),
        ).fetchone()
        if submit_attempt_id:
            submit_event = conn.execute(
                """
                SELECT submit_ts, event_ts, timeout_flag, exchange_order_id_obtained
                FROM order_events
                WHERE client_order_id=? AND submit_attempt_id=?
                    AND event_type='submit_attempt_recorded'
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(row["client_order_id"]), submit_attempt_id),
            ).fetchone()

        exchange_order_id = str(row["exchange_order_id"] or "").strip()
        if exchange_order_id:
            mapping_status = "mapped"
        elif submit_event and submit_event["exchange_order_id_obtained"] is not None:
            mapping_status = (
                "submit_no_mapping"
                if not bool(submit_event["exchange_order_id_obtained"])
                else "submit_mapped"
            )
        elif submit_attempt_id:
            mapping_status = "submit_attempt_only"
        else:
            mapping_status = "unknown"

        correlation = "none"
        if submit_attempt_id:
            correlation = (
                f"attempt={submit_attempt_id} "
                f"meta={1 if bool(context.get('metadata_present')) else 0} "
                f"timeout={1 if bool(context.get('timeout_submit_unknown')) else 0}"
            )

        intent_ts = int(row["created_ts"])
        if intent_event and intent_event["intent_ts"] is not None:
            intent_ts = int(intent_event["intent_ts"])
        elif intent_event and intent_event["event_ts"] is not None:
            intent_ts = int(intent_event["event_ts"])

        submit_ts = None
        if submit_event and submit_event["submit_ts"] is not None:
            submit_ts = int(submit_event["submit_ts"])
        elif submit_event and submit_event["event_ts"] is not None:
            submit_ts = int(submit_event["event_ts"])

        status = str(row["status"])
        lifecycle.append(
            {
                "client_order_id": str(row["client_order_id"]),
                "intent_ts": intent_ts,
                "submit_ts": submit_ts if submit_ts is not None else "-",
                "correlation": correlation,
                "mapping_status": mapping_status,
                "state": status,
                "unresolved": 1 if status in UNRESOLVED_ORDER_STATUSES else 0,
                "requested_qty": float(row["qty_req"] or 0.0),
                "requested_lot_count": int(row["intended_lot_count"] or 0),
                "executable_lot_count": int(row["executable_lot_count"] or 0),
                "final_intended_qty": float(row["final_intended_qty"] or 0.0) if row["final_intended_qty"] is not None else 0.0,
                "final_submitted_qty": float(row["final_submitted_qty"] or 0.0) if row["final_submitted_qty"] is not None else 0.0,
                "lot_basis_qty": lot_basis_qty,
                "lot_basis_source": lot_basis_source,
            }
        )

    return lifecycle


def _extract_submit_attempt_id(*, client_order_id: str, submit_attempt_id: str | None) -> str:
    if submit_attempt_id:
        return str(submit_attempt_id)
    parts = str(client_order_id).split("_")
    if not parts:
        return ""
    tail = parts[-1]
    if tail.startswith("attempt"):
        return tail
    return ""


def _load_submit_attempt_context(conn, *, row) -> dict[str, str | float | bool]:
    client_order_id = str(row["client_order_id"])
    submit_attempt_id = _extract_submit_attempt_id(
        client_order_id=client_order_id,
        submit_attempt_id=(str(row["submit_attempt_id"]) if "submit_attempt_id" in row.keys() and row["submit_attempt_id"] else None),
    )
    context: dict[str, str | float | bool] = {
        "submit_attempt_id": submit_attempt_id,
        "timeout_submit_unknown": False,
        "metadata_present": False,
        "preflight_side": str(row["side"]),
        "preflight_qty": float(row["qty_req"]),
    }
    if not submit_attempt_id:
        return context

    event_rows = conn.execute(
        """
        SELECT event_type, side, qty, order_status, timeout_flag
        FROM order_events
        WHERE client_order_id=? AND submit_attempt_id=?
        ORDER BY id DESC
        """,
        (client_order_id, submit_attempt_id),
    ).fetchall()

    context["metadata_present"] = len(event_rows) > 0
    for ev in event_rows:
        ev_type = str(ev["event_type"] or "")
        ev_side = str(ev["side"] or "")
        ev_qty = ev["qty"]
        if ev_type == "submit_attempt_preflight":
            if ev_side:
                context["preflight_side"] = ev_side
            if ev_qty is not None:
                context["preflight_qty"] = float(ev_qty)
        if ev_type == "submit_attempt_recorded" and str(ev["order_status"] or "") == "SUBMIT_UNKNOWN":
            context["timeout_submit_unknown"] = bool(ev["timeout_flag"])
            break

    return context


def _strong_submit_unknown_correlation(
    *,
    local_row,
    submit_attempt_context: dict[str, str | float | bool],
    remote_client_order_id: str | None,
    remote_exchange_order_id: str | None,
    remote_side: str | None,
    remote_qty: float | None,
) -> bool:
    local_client_order_id = str(local_row["client_order_id"])
    local_exchange_order_id = str(local_row["exchange_order_id"] or "")
    if _strong_order_correlation(
        local_client_order_id=local_client_order_id,
        local_exchange_order_id=(local_exchange_order_id or None),
        remote_client_order_id=remote_client_order_id,
        remote_exchange_order_id=remote_exchange_order_id,
    ):
        return True

    if not bool(submit_attempt_context.get("timeout_submit_unknown")):
        return False

    if str(remote_client_order_id or "") != local_client_order_id:
        return False

    local_side = str(submit_attempt_context.get("preflight_side") or local_row["side"])
    if remote_side and remote_side != local_side:
        return False

    local_qty = float(submit_attempt_context.get("preflight_qty") or local_row["qty_req"])
    if remote_qty is not None and abs(float(remote_qty) - local_qty) > 1e-12:
        return False

    return True


def _strong_submit_unknown_fill_correlation(
    *,
    local_row,
    submit_attempt_context: dict[str, str | float | bool],
    remote_client_order_id: str | None,
    remote_exchange_order_id: str | None,
) -> bool:
    local_client_order_id = str(local_row["client_order_id"])
    local_exchange_order_id = str(local_row["exchange_order_id"] or "")
    if _strong_order_correlation(
        local_client_order_id=local_client_order_id,
        local_exchange_order_id=(local_exchange_order_id or None),
        remote_client_order_id=remote_client_order_id,
        remote_exchange_order_id=remote_exchange_order_id,
    ):
        return True

    if not bool(submit_attempt_context.get("timeout_submit_unknown")):
        return False

    if str(remote_client_order_id or "") != local_client_order_id:
        return False

    if local_exchange_order_id and remote_exchange_order_id and remote_exchange_order_id != local_exchange_order_id:
        return False

    return True


def _strong_order_correlation(*, local_client_order_id: str, local_exchange_order_id: str | None, remote_client_order_id: str | None, remote_exchange_order_id: str | None) -> bool:
    local_client = str(local_client_order_id or "")
    local_exchange = str(local_exchange_order_id or "")
    remote_client = str(remote_client_order_id or "")
    remote_exchange = str(remote_exchange_order_id or "")

    if local_exchange and remote_exchange and local_exchange == remote_exchange:
        return True
    return False


def _mark_recovery_required_with_reason(
    conn,
    *,
    client_order_id: str,
    side: str,
    from_status: str,
    reason_code: str,
    reason: str,
) -> None:
    current = conn.execute(
        "SELECT status FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    current_status = str(current["status"]) if current is not None else str(from_status)
    incident_status = str(from_status or current_status)
    if incident_status in TERMINAL_TRUTH_STATUSES:
        if current is not None and current_status != incident_status:
            allowed, blocked_reason = validate_status_transition(
                from_status=current_status,
                to_status=incident_status,
            )
            if allowed:
                set_status(
                    client_order_id,
                    incident_status,
                    last_error=reason,
                    conn=conn,
                )
                current_status = incident_status
            else:
                record_status_transition(
                    client_order_id,
                    from_status=current_status,
                    to_status=current_status,
                    reason=(
                        "recovery incident preserved current status after terminal truth update "
                        f"was blocked: attempted={incident_status}; blocked_reason={blocked_reason}; "
                        f"incident_reason={reason}"
                    ),
                    conn=conn,
                )
                conn.execute(
                    "UPDATE orders SET last_error=? WHERE client_order_id=?",
                    (reason[:500], client_order_id),
                )
        elif current is not None:
            conn.execute(
                "UPDATE orders SET last_error=? WHERE client_order_id=?",
                (reason[:500], client_order_id),
            )
        record_status_transition(
            client_order_id,
            from_status=current_status,
            to_status=current_status,
            reason=(
                "recovery incident recorded without terminal status downgrade; "
                f"incident_status={incident_status}; reason={reason}"
            ),
            conn=conn,
        )
        notify(
            safety_event(
                "recovery_required_incident",
                client_order_id=client_order_id,
                exchange_order_id="-",
                side=side,
                status=current_status,
                state_from=current_status,
                state_to=current_status,
                reason_code=reason_code,
                reason=reason,
                operator_next_action="review broker fill observations and resolve accounting incident before resume",
                operator_hint_command="uv run python bot.py recovery-report --json",
            )
        )
        return
    record_status_transition(
        client_order_id,
        from_status=from_status,
        to_status="RECOVERY_REQUIRED",
        reason=reason,
        conn=conn,
    )
    set_status(
        client_order_id,
        "RECOVERY_REQUIRED",
        last_error=reason,
        conn=conn,
    )
    notify(
        safety_event(
            "recovery_required_transition",
            client_order_id=client_order_id,
            exchange_order_id="-",
            side=side,
            status="RECOVERY_REQUIRED",
            state_from=from_status,
            state_to="RECOVERY_REQUIRED",
            reason_code=reason_code,
            reason=reason,
            operator_next_action="review submit ambiguity and recover order with exchange_order_id",
            operator_hint_command=(
                f"uv run python bot.py recover-order --client-order-id {client_order_id} --exchange-order-id <exchange_order_id>"
            ),
        )
    )


def _mark_accounting_pending_with_reason(
    conn,
    *,
    client_order_id: str,
    side: str,
    from_status: str,
    reason: str,
) -> None:
    current = conn.execute(
        "SELECT status FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    current_status = str(current["status"]) if current is not None else str(from_status)
    base_from_status = current_status or str(from_status)
    target_status = "ACCOUNTING_PENDING"
    allowed, blocked_reason = validate_status_transition(
        from_status=base_from_status,
        to_status=target_status,
    )
    if not allowed:
        conn.execute(
            "UPDATE orders SET last_error=? WHERE client_order_id=?",
            (reason[:500], client_order_id),
        )
        record_status_transition(
            client_order_id,
            from_status=base_from_status,
            to_status=base_from_status,
            reason=(
                "accounting-pending incident preserved current status after blocked transition; "
                f"blocked_reason={blocked_reason}; incident_reason={reason}"
            ),
            conn=conn,
        )
        return

    record_status_transition(
        client_order_id,
        from_status=base_from_status,
        to_status=target_status,
        reason=reason,
        conn=conn,
    )
    set_status(
        client_order_id,
        target_status,
        last_error=reason,
        conn=conn,
    )
    notify(
        safety_event(
            "accounting_pending_transition",
            client_order_id=client_order_id,
            exchange_order_id="-",
            side=side,
            status=target_status,
            state_from=base_from_status,
            state_to=target_status,
            reason_code=REASON_FILL_FEE_PENDING_RECOVERY_REQUIRED,
            reason=reason,
            operator_next_action="allow automatic reconcile retry or inspect broker fill evidence if it does not clear",
            operator_hint_command="uv run python bot.py recovery-report --json",
        )
    )


def _classify_lookup_error(exc: Exception) -> str:
    if isinstance(exc, BrokerIdentifierMismatchError):
        return "identifier_mismatch"
    if isinstance(exc, BrokerSchemaError):
        return "schema_mismatch"
    if isinstance(exc, BrokerTemporaryError):
        return "temporary_broker_error"
    if isinstance(exc, BrokerRejectError):
        detail = str(exc).lower()
        if "not found" in detail:
            return "lookup_not_found"
        return "broker_reject"
    return "unexpected_error"


def _order_lot_basis_qty(*, row) -> tuple[float, str]:
    def _read_value(key: str, index: int) -> object | None:
        if row is None:
            return None
        if hasattr(row, "keys"):
            try:
                return row[key]  # type: ignore[index]
            except (KeyError, IndexError, TypeError):
                pass
        try:
            return row[index]  # type: ignore[index]
        except (IndexError, KeyError, TypeError):
            return None

    for key, source in (
        ("final_submitted_qty", "final_submitted_qty"),
        ("final_intended_qty", "final_intended_qty"),
        ("qty_req", "qty_req"),
    ):
        value = _read_value(key, 0)
        if value is None:
            continue
        try:
            qty = max(0.0, float(value))
        except (TypeError, ValueError):
            continue
        return qty, source
    return 0.0, "qty_req"


CASH_SPLIT_ABS_TOL = 1e-6
ASSET_SPLIT_ABS_TOL = 1e-10
_LOG = logging.getLogger(__name__)
RECENT_FLATTEN_WINDOW_SEC = 600.0
CASH_ADJUSTMENT_KEY_VERSION = "reconcile_cash_drift_v2"


def _balance_split_mismatch_summary(
    *,
    broker_cash_available: float,
    broker_cash_locked: float,
    broker_asset_available: float,
    broker_asset_locked: float,
    local_cash_available: float,
    local_cash_locked: float,
    local_asset_available: float,
    local_asset_locked: float,
) -> tuple[int, str]:
    mismatches: list[str] = []

    def _append_if_mismatch(*, field: str, local: float, broker: float, tol: float) -> None:
        delta = float(broker) - float(local)
        if abs(delta) <= tol:
            return
        mismatches.append(f"{field}(local={local:.12g},broker={broker:.12g},delta={delta:.12g})")

    _append_if_mismatch(
        field="cash_available",
        local=local_cash_available,
        broker=broker_cash_available,
        tol=CASH_SPLIT_ABS_TOL,
    )
    _append_if_mismatch(
        field="cash_locked",
        local=local_cash_locked,
        broker=broker_cash_locked,
        tol=CASH_SPLIT_ABS_TOL,
    )
    _append_if_mismatch(
        field="asset_available",
        local=local_asset_available,
        broker=broker_asset_available,
        tol=ASSET_SPLIT_ABS_TOL,
    )
    _append_if_mismatch(
        field="asset_locked",
        local=local_asset_locked,
        broker=broker_asset_locked,
        tol=ASSET_SPLIT_ABS_TOL,
    )

    return len(mismatches), "; ".join(mismatches)


def _external_cash_adjustment_key(
    *,
    balance_source: str,
    broker_cash_available: float,
    broker_cash_locked: float,
    broker_cash_total: float,
    local_cash_available: float,
    local_cash_locked: float,
    local_cash_total: float,
    cash_delta: float,
    recent_fill_applied: int,
    remote_open_order_found: int,
    invalid_fill_price_blocked: int,
    unresolved_open_order_count: int,
    submit_unknown_count: int,
    recovery_required_count: int,
) -> str:
    payload = {
        "event_type": "external_cash_adjustment",
        "key_version": CASH_ADJUSTMENT_KEY_VERSION,
        "currency": "KRW",
        "balance_source": str(balance_source or "-"),
        "broker_cash_available": f"{normalize_cash_amount(broker_cash_available):.8f}",
        "broker_cash_locked": f"{normalize_cash_amount(broker_cash_locked):.8f}",
        "broker_cash_total": f"{normalize_cash_amount(broker_cash_total):.8f}",
        "local_cash_available": f"{normalize_cash_amount(local_cash_available):.8f}",
        "local_cash_locked": f"{normalize_cash_amount(local_cash_locked):.8f}",
        "local_cash_total": f"{normalize_cash_amount(local_cash_total):.8f}",
        "cash_delta": f"{normalize_cash_amount(cash_delta):.8f}",
        "recent_fill_applied": int(recent_fill_applied),
        "remote_open_order_found": int(remote_open_order_found),
        "invalid_fill_price_blocked": int(invalid_fill_price_blocked),
        "unresolved_open_order_count": int(unresolved_open_order_count),
        "submit_unknown_count": int(submit_unknown_count),
        "recovery_required_count": int(recovery_required_count),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _latest_price_for_notional_estimate(conn) -> float | None:
    row = conn.execute(
        """
        SELECT close
        FROM candles
        WHERE close IS NOT NULL
        ORDER BY ts DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    try:
        price = float(row["close"])
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return price


def _is_partial_flatten_recent(*, now_sec: float) -> tuple[bool, str]:
    try:
        state = runtime_state.snapshot()
    except Exception as exc:
        return False, f"flatten_state_unavailable({type(exc).__name__})"
    status = str(state.last_flatten_position_status or "").strip()
    flatten_ts = state.last_flatten_position_epoch_sec
    if status != "submitted" or flatten_ts is None:
        return False, "flatten_not_recent"
    age_sec = max(0.0, now_sec - float(flatten_ts))
    if age_sec > RECENT_FLATTEN_WINDOW_SEC:
        return False, f"flatten_too_old(age_sec={age_sec:.1f})"
    summary_raw = str(state.last_flatten_position_summary or "").strip()
    trigger = "-"
    if summary_raw:
        try:
            summary = json.loads(summary_raw)
            trigger = str(summary.get("trigger") or "-")
        except json.JSONDecodeError:
            trigger = "-"
    return True, f"flatten_recent(age_sec={age_sec:.1f},trigger={trigger})"


def _evaluate_dust_residual_policy(
    *,
    conn,
    broker_asset_available: float,
    broker_asset_locked: float,
    local_asset_available: float,
    local_asset_locked: float,
) -> dict[str, int | float | str]:
    broker_qty = max(0.0, float(broker_asset_available) + float(broker_asset_locked))
    local_qty = max(0.0, float(local_asset_available) + float(local_asset_locked))
    min_qty = 0.0
    min_notional = 0.0
    try:
        rules = get_effective_order_rules(settings.PAIR).rules
        min_qty = max(0.0, float(rules.min_qty))
        min_notional = max(0.0, float(rules.min_notional_krw))
    except Exception:
        min_qty = 0.0
        min_notional = 0.0

    status_counts = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'ACCOUNTING_PENDING', 'CANCEL_REQUESTED') THEN 1 ELSE 0 END) AS unresolved_open_order_count,
            SUM(CASE WHEN status='SUBMIT_UNKNOWN' THEN 1 ELSE 0 END) AS submit_unknown_count,
            SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END) AS recovery_required_count
        FROM orders
        """
    ).fetchone()
    unresolved_open_order_count = int(status_counts["unresolved_open_order_count"] or 0) if status_counts else 0
    submit_unknown_count = int(status_counts["submit_unknown_count"] or 0) if status_counts else 0
    recovery_required_count = int(status_counts["recovery_required_count"] or 0) if status_counts else 0

    recent_flatten, recent_flatten_reason = _is_partial_flatten_recent(now_sec=time.time())
    est_price = _latest_price_for_notional_estimate(conn)
    dust_eval = classify_dust_residual(
        broker_qty=broker_qty,
        local_qty=local_qty,
        min_qty=min_qty,
        min_notional_krw=min_notional,
        latest_price=est_price,
        partial_flatten_recent=recent_flatten,
        partial_flatten_reason=recent_flatten_reason,
        qty_gap_tolerance=dust_qty_gap_tolerance(
            min_qty=min_qty,
            default_abs_tolerance=ASSET_SPLIT_ABS_TOL * 10.0,
        ),
        matched_harmless_resume_allowed=(
            unresolved_open_order_count == 0
            and submit_unknown_count == 0
            and recovery_required_count == 0
        ),
    )
    metadata = dust_eval.to_metadata()
    metadata.update(
        {
            "unresolved_open_order_count": unresolved_open_order_count,
            "submit_unknown_count": submit_unknown_count,
            "recovery_required_count": recovery_required_count,
        }
    )
    return metadata


def assert_no_open_orders() -> None:
    open_orders = get_open_orders()
    if open_orders:
        raise RuntimeError(f"Open orders exist (resume required): {open_orders}")


def _safe_recovery_client_order_id(*, tag: str, exchange_order_id: str | None, ts: int) -> str:
    base = exchange_order_id or f"{tag}_{ts}"
    clean = re.sub(r"[^a-zA-Z0-9_-]", "_", str(base))[:64]
    return f"recovery_{clean}"


def _record_unmatched_recent_activity(
    conn,
    *,
    exchange_order_id: str | None,
    side: str,
    qty_req: float,
    ts_ms: int,
    status: str,
    message: str,
) -> None:
    oid = _safe_recovery_client_order_id(tag="recent", exchange_order_id=exchange_order_id, ts=ts_ms)
    record_order_if_missing(
        conn,
        client_order_id=oid,
        side=(side if side in ("BUY", "SELL") else "BUY"),
        qty_req=max(0.0, float(qty_req)),
        price=None,
        ts_ms=int(ts_ms),
        status="SUBMIT_UNKNOWN",
    )
    if exchange_order_id:
        set_exchange_order_id(oid, str(exchange_order_id), conn=conn)
    record_status_transition(
        oid,
        from_status="SUBMIT_UNKNOWN",
        to_status=status,
        reason=message,
        conn=conn,
    )
    set_status(oid, status, last_error=message, conn=conn)


def _sync_recent_order_activity(
    conn,
    recent_orders: list[BrokerOrder],
    *,
    trusted_open_exchange_ids: set[str],
) -> list[str]:
    local_rows = conn.execute(
        "SELECT client_order_id, exchange_order_id, side, status FROM orders"
    ).fetchall()
    by_exchange_id = {str(r["exchange_order_id"]): r for r in local_rows if r["exchange_order_id"]}
    by_client_order_id = {str(r["client_order_id"]): r for r in local_rows}

    conflicts: list[str] = []

    for remote in recent_orders:
        remote_exchange_id = str(remote.exchange_order_id or "")
        remote_client_order_id = str(remote.client_order_id or "")

        if (
            remote_exchange_id
            and remote_exchange_id in trusted_open_exchange_ids
            and remote.status not in OPEN_ORDER_TRUSTED_STATUSES
        ):
            conflicts.append(
                f"exchange_order_id={remote_exchange_id} open_orders=OPEN recent_orders={remote.status}"
            )
            continue

        local = by_exchange_id.get(remote_exchange_id)
        weak_match_local = None
        if local is None and remote_client_order_id:
            candidate = by_client_order_id.get(remote_client_order_id)
            if candidate is not None:
                candidate_exchange = str(candidate["exchange_order_id"] or "")
                if _strong_order_correlation(
                    local_client_order_id=str(candidate["client_order_id"]),
                    local_exchange_order_id=(candidate_exchange or None),
                    remote_client_order_id=remote_client_order_id,
                    remote_exchange_order_id=(remote_exchange_id or None),
                ):
                    local = candidate
                else:
                    weak_match_local = candidate

        if local is not None:
            local_id = str(local["client_order_id"])
            if remote_exchange_id:
                set_exchange_order_id(local_id, remote_exchange_id, conn=conn)
                refreshed = conn.execute(
                    "SELECT client_order_id, exchange_order_id, side, status FROM orders WHERE client_order_id=?",
                    (local_id,),
                ).fetchone()
                if refreshed is not None and refreshed["exchange_order_id"]:
                    by_exchange_id[str(refreshed["exchange_order_id"])] = refreshed
            set_status(local_id, remote.status, conn=conn)
            continue

        if weak_match_local is not None:
            _mark_recovery_required_with_reason(
                conn,
                client_order_id=str(weak_match_local["client_order_id"]),
                side=str(weak_match_local["side"]),
                from_status=str(weak_match_local["status"]),
                reason_code=WEAK_ORDER_CORRELATION,
                reason="recent order matched only weakly by client_order_id; manual recovery required",
            )
            continue

        _record_unmatched_recent_activity(
            conn,
            exchange_order_id=(remote_exchange_id or None),
            side=remote.side,
            qty_req=remote.qty_req,
            ts_ms=remote.updated_ts,
            status="RECOVERY_REQUIRED",
            message="unmatched recent remote order detected; manual recovery required",
        )

    return conflicts


def _status_after_recent_fill_replay(*, current_status: str, qty_req: float, qty_filled: float) -> str | None:
    fill_tol = order_fill_tolerance(qty_req)
    if qty_req > 0 and qty_filled >= qty_req - fill_tol:
        return "FILLED"
    if qty_filled <= 1e-12:
        return None
    if current_status in {"FILLED", "CANCELED"}:
        _LOG.info(
            "recent_fill_terminal_state_preserved current_status=%s qty_req=%.12g qty_filled=%.12g",
            current_status,
            qty_req,
            qty_filled,
        )
        return current_status
    return "PARTIAL"


def _apply_recent_fills(
    conn,
    recent_fills: list[BrokerFill],
    *,
    trusted_open_exchange_ids: set[str],
) -> tuple[bool, list[str], int, dict[str, int | str] | None]:
    local_rows = conn.execute(
        "SELECT client_order_id, exchange_order_id, side, status, qty_req, qty_filled FROM orders"
    ).fetchall()
    by_exchange_id = {str(r["exchange_order_id"]): r for r in local_rows if r["exchange_order_id"]}
    by_client_order_id = {str(r["client_order_id"]): r for r in local_rows}

    applied = False
    conflicts: list[str] = []
    blocked_invalid_price = 0
    fee_pending_updates: dict[str, int | str] | None = None

    for fill in recent_fills:
        remote_exchange_id = str(fill.exchange_order_id or "")
        remote_client_order_id = str(fill.client_order_id or "")

        if remote_exchange_id and remote_exchange_id in trusted_open_exchange_ids:
            conflicts.append(f"exchange_order_id={remote_exchange_id} open_orders=OPEN recent_fills=HAS_FILL")
            continue

        local = by_exchange_id.get(remote_exchange_id)
        weak_match_client_order_id: str | None = None
        if local is None and remote_client_order_id:
            local_by_client = by_client_order_id.get(remote_client_order_id)
            if local_by_client is not None:
                local_exchange = str(local_by_client["exchange_order_id"] or "")
                if _strong_order_correlation(
                    local_client_order_id=str(local_by_client["client_order_id"]),
                    local_exchange_order_id=(local_exchange or None),
                    remote_client_order_id=remote_client_order_id,
                    remote_exchange_order_id=(remote_exchange_id or None),
                ):
                    local = local_by_client
                elif (
                    not remote_exchange_id
                    and local_exchange
                    and str(local_by_client["status"]) not in {"SUBMIT_UNKNOWN", "RECOVERY_REQUIRED"}
                ):
                    local = local_by_client
                else:
                    weak_match_client_order_id = str(local_by_client["client_order_id"])

        if local is None:
            if weak_match_client_order_id:
                _mark_recovery_required_with_reason(
                    conn,
                    client_order_id=weak_match_client_order_id,
                    side=str(local_by_client["side"]),
                    from_status=str(local_by_client["status"]),
                    reason_code=AMBIGUOUS_RECENT_FILL,
                    reason="recent fill matched only weakly by client_order_id; manual recovery required",
                )
                applied = False
                continue
            _record_unmatched_recent_activity(
                conn,
                exchange_order_id=(remote_exchange_id or None),
                side="BUY",
                qty_req=fill.qty,
                ts_ms=fill.fill_ts,
                status="RECOVERY_REQUIRED",
                message="unmatched recent remote fill detected; manual recovery required",
            )
            continue

        local_id = str(local["client_order_id"])
        if remote_exchange_id:
            set_exchange_order_id(local_id, remote_exchange_id, conn=conn)

        if float(fill.price) <= 0:
            _mark_recovery_required_with_reason(
                conn,
                client_order_id=local_id,
                side=str(local["side"]),
                from_status=str(local["status"]),
                reason_code=REASON_RECENT_FILL_INVALID_PRICE,
                reason=(
                    "recent fill has missing/invalid execution price; "
                    f"exchange_order_id={remote_exchange_id or '<none>'}; manual recovery required"
                ),
            )
            blocked_invalid_price += 1
            continue

        if not _fill_fee_is_accounting_complete(fill):
            observation_summary = _record_fee_pending_observations(
                conn,
                client_order_id=local_id,
                side=str(local["side"]),
                exchange_order_id=(remote_exchange_id or None),
                fills=[fill],
                source="reconcile_recent_activity_fee_pending",
            )
            apply_fill_principal_with_pending_fee(
                conn,
                client_order_id=local_id,
                side=str(local["side"]),
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=getattr(fill, "fee", None),
                fee_status=getattr(fill, "fee_status", "unknown"),
                fee_source=getattr(fill, "fee_source", None),
                fee_confidence=getattr(fill, "fee_confidence", None),
                fee_provenance=getattr(fill, "fee_provenance", None),
                fee_validation_reason=getattr(fill, "fee_validation_reason", None),
                fee_validation_checks=getattr(fill, "fee_validation_checks", None),
                note=f"reconcile recent exchange_order_id={remote_exchange_id or '<none>'}",
                allow_entry_decision_fallback=False,
            )
            updates = _fee_pending_metadata_updates(observation_summary)
            if fee_pending_updates is None:
                fee_pending_updates = updates
            else:
                _merge_fee_pending_metadata(fee_pending_updates, updates)
            applied = True
            order_row = conn.execute(
                "SELECT status, qty_req, qty_filled FROM orders WHERE client_order_id=?",
                (local_id,),
            ).fetchone()
            if order_row is not None:
                next_status = _status_after_recent_fill_replay(
                    current_status=str(order_row["status"]),
                    qty_req=float(order_row["qty_req"]),
                    qty_filled=float(order_row["qty_filled"]),
                )
                if next_status is not None:
                    set_status(local_id, next_status, conn=conn)
            continue

        try:
            apply_fill_and_trade(
                conn,
                client_order_id=local_id,
                side=str(local["side"]),
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee,
                note=f"reconcile recent exchange_order_id={remote_exchange_id or '<none>'}",
                allow_entry_decision_fallback=False,
            )
        except LiveFillFeeValidationError as exc:
            _mark_recovery_required_with_reason(
                conn,
                client_order_id=local_id,
                side=str(local["side"]),
                from_status=str(local["status"]),
                reason_code=REASON_FEE_GAP_RECOVERY_REQUIRED,
                reason=f"recent fill fee validation blocked ledger apply; manual recovery required ({exc})",
            )
            continue
        applied = True

        order_row = conn.execute(
            "SELECT status, qty_req, qty_filled FROM orders WHERE client_order_id=?",
            (local_id,),
        ).fetchone()
        if order_row is None:
            continue
        next_status = _status_after_recent_fill_replay(
            current_status=str(order_row["status"]),
            qty_req=float(order_row["qty_req"]),
            qty_filled=float(order_row["qty_filled"]),
        )
        if next_status is not None:
            set_status(local_id, next_status, conn=conn)

    return applied, conflicts, blocked_invalid_price, fee_pending_updates


def _halt_on_source_conflict(conflicts: list[str]) -> None:
    detail = "; ".join(conflicts[:3])
    if len(conflicts) > 3:
        detail = f"{detail}; +{len(conflicts) - 3} more"
    reason = f"recovery source conflict detected; manual review required ({detail})"
    runtime_state.enter_halt(
        reason_code="RECOVERY_SOURCE_CONFLICT",
        reason=reason,
        unresolved=True,
    )
    notify(
        safety_event(
            "reconcile_source_conflict",
            alert_kind="halt",
            reason_code=RECONCILE_MISMATCH,
            reason=reason,
            primary_blocker_code="RECOVERY_SOURCE_CONFLICT",
            operator_next_action="review conflicted orders and recover each unresolved order",
            force_resume_allowed=0,
            operator_hint_command="uv run python bot.py recovery-report --json",
        )
    )


@dataclass(frozen=True)
class _SubmitUnknownRecentActivityInterpretation:
    outcome: str
    candidate_count: int
    matched_exchange_order_id: str | None
    matched_order: BrokerOrder | None
    matched_orders: tuple[BrokerOrder, ...]
    matched_fills: tuple[BrokerFill, ...]
    has_partial_fill_evidence: bool


def _interpret_submit_unknown_recent_activity(
    *,
    local_row,
    submit_attempt_context: dict[str, str | float | bool],
    recent_orders: list[BrokerOrder],
    recent_fills: list[BrokerFill],
) -> _SubmitUnknownRecentActivityInterpretation:
    candidate_orders: list[BrokerOrder] = []
    candidate_fills: list[BrokerFill] = []
    candidate_exchange_ids: set[str] = set()

    for remote in recent_orders:
        remote_client_order_id = str(remote.client_order_id or "")
        remote_exchange_order_id = str(remote.exchange_order_id or "")
        if not _strong_submit_unknown_correlation(
            local_row=local_row,
            submit_attempt_context=submit_attempt_context,
            remote_client_order_id=remote_client_order_id or None,
            remote_exchange_order_id=remote_exchange_order_id or None,
            remote_side=remote.side,
            remote_qty=remote.qty_req,
        ):
            continue

        status_allowed, _ = validate_status_transition(from_status="SUBMIT_UNKNOWN", to_status=remote.status)
        if not status_allowed:
            continue
        candidate_orders.append(remote)
        if remote_exchange_order_id:
            candidate_exchange_ids.add(remote_exchange_order_id)

    for fill in recent_fills:
        remote_client_order_id = str(fill.client_order_id or "")
        remote_exchange_order_id = str(fill.exchange_order_id or "")
        if not _strong_submit_unknown_fill_correlation(
            local_row=local_row,
            submit_attempt_context=submit_attempt_context,
            remote_client_order_id=remote_client_order_id or None,
            remote_exchange_order_id=remote_exchange_order_id or None,
        ):
            continue
        candidate_fills.append(fill)
        if remote_exchange_order_id:
            candidate_exchange_ids.add(remote_exchange_order_id)

    if not candidate_orders and not candidate_fills:
        return _SubmitUnknownRecentActivityInterpretation(
            outcome="insufficient_evidence",
            candidate_count=0,
            matched_exchange_order_id=None,
            matched_order=None,
            matched_orders=(),
            matched_fills=(),
            has_partial_fill_evidence=False,
        )

    if len(candidate_exchange_ids) > 1:
        return _SubmitUnknownRecentActivityInterpretation(
            outcome="ambiguous",
            candidate_count=len(candidate_exchange_ids),
            matched_exchange_order_id=None,
            matched_order=None,
            matched_orders=tuple(candidate_orders),
            matched_fills=tuple(candidate_fills),
            has_partial_fill_evidence=False,
        )

    matched_exchange_order_id = next(iter(candidate_exchange_ids)) if candidate_exchange_ids else None
    matched_order = None
    if candidate_orders:
        matched_order = max(
            candidate_orders,
            key=lambda order: (int(order.updated_ts), int(order.created_ts)),
        )

    total_fill_qty = sum(max(0.0, float(fill.qty)) for fill in candidate_fills)
    local_qty_basis, _ = _order_lot_basis_qty(row=local_row)
    has_partial_fill_evidence = bool(total_fill_qty > 1e-12 and local_qty_basis > total_fill_qty + 1e-12)

    candidate_count = 1
    if candidate_exchange_ids:
        candidate_count = len(candidate_exchange_ids)

    return _SubmitUnknownRecentActivityInterpretation(
        outcome="success",
        candidate_count=candidate_count,
        matched_exchange_order_id=matched_exchange_order_id,
        matched_order=matched_order,
        matched_orders=tuple(candidate_orders),
        matched_fills=tuple(candidate_fills),
        has_partial_fill_evidence=has_partial_fill_evidence,
    )


def _try_resolve_submit_unknown_from_recent_activity(
    conn,
    *,
    row,
    recent_orders: list[BrokerOrder],
    recent_fills: list[BrokerFill],
) -> SubmitUnknownResolution:
    client_order_id = str(row["client_order_id"])
    side = str(row["side"])
    submit_attempt_context = _load_submit_attempt_context(conn, row=row)

    interpretation = _interpret_submit_unknown_recent_activity(
        local_row=row,
        submit_attempt_context=submit_attempt_context,
        recent_orders=recent_orders,
        recent_fills=recent_fills,
    )

    if interpretation.outcome != "success":
        _record_submit_unknown_autolink_event(
            conn,
            client_order_id=client_order_id,
            side=side,
            submit_attempt_context=submit_attempt_context,
            outcome=interpretation.outcome,
            candidate_count=interpretation.candidate_count,
            exchange_order_id=None,
            matched_order_count=len(interpretation.matched_orders),
            matched_fill_count=len(interpretation.matched_fills),
        )
        return SubmitUnknownResolution(False, False, None, None)

    matched_exchange_order_id = interpretation.matched_exchange_order_id
    matched_order = interpretation.matched_order
    evidence_mode = (
        "order_and_fill"
        if interpretation.matched_orders and interpretation.matched_fills
        else ("order_only" if interpretation.matched_orders else "fill_only")
    )
    _record_submit_unknown_autolink_event(
        conn,
        client_order_id=client_order_id,
        side=side,
        submit_attempt_context=submit_attempt_context,
        outcome=evidence_mode,
        candidate_count=interpretation.candidate_count,
        exchange_order_id=matched_exchange_order_id,
        matched_order_count=len(interpretation.matched_orders),
        matched_fill_count=len(interpretation.matched_fills),
    )

    if matched_exchange_order_id:
        set_exchange_order_id(client_order_id, matched_exchange_order_id, conn=conn)
        notify(
            format_event(
                "exchange_order_id_attached",
                client_order_id=client_order_id,
                exchange_order_id=matched_exchange_order_id,
                side=side,
                status=(matched_order.status if matched_order else "SUBMIT_UNKNOWN"),
                reason="reconcile_recent_activity",
            )
        )

    applied_fill = False
    for fill in interpretation.matched_fills:
        if float(fill.price) <= 0:
            reason = (
                "submit_unknown recent fill has missing/invalid execution price; "
                f"exchange_order_id={matched_exchange_order_id or '<none>'}; "
                f"fill_id={fill.fill_id}"
            )
            _mark_recovery_required_with_reason(
                conn,
                client_order_id=client_order_id,
                side=side,
                from_status="SUBMIT_UNKNOWN",
                reason_code=REASON_RECENT_FILL_INVALID_PRICE,
                reason=reason,
            )
            return SubmitUnknownResolution(
                True,
                False,
                client_order_id,
                matched_exchange_order_id,
                reason_code=REASON_RECENT_FILL_INVALID_PRICE,
                metadata_updates={"invalid_fill_price_blocked": 1},
            )
        if not _fill_fee_is_accounting_complete(fill):
            observation_summary = _record_fee_pending_observations(
                conn,
                client_order_id=client_order_id,
                side=side,
                exchange_order_id=matched_exchange_order_id,
                fills=list(interpretation.matched_fills),
                source="reconcile_recent_activity_fee_pending",
            )
            apply_fill_principal_with_pending_fee(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=getattr(fill, "fee", None),
                fee_status=getattr(fill, "fee_status", "unknown"),
                fee_source=getattr(fill, "fee_source", None),
                fee_confidence=getattr(fill, "fee_confidence", None),
                fee_provenance=getattr(fill, "fee_provenance", None),
                fee_validation_reason=getattr(fill, "fee_validation_reason", None),
                fee_validation_checks=getattr(fill, "fee_validation_checks", None),
                note=f"reconcile submit_unknown recent exchange_order_id={matched_exchange_order_id or '<none>'}",
                allow_entry_decision_fallback=False,
            )
            order_row = conn.execute(
                "SELECT status, qty_req, qty_filled FROM orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            if order_row is not None:
                next_status = _status_after_recent_fill_replay(
                    current_status=str(order_row["status"]),
                    qty_req=float(order_row["qty_req"]),
                    qty_filled=float(order_row["qty_filled"]),
                )
                if next_status is not None:
                    set_status(client_order_id, next_status, conn=conn)
            return SubmitUnknownResolution(
                True,
                True,
                client_order_id,
                matched_exchange_order_id,
                reason_code=REASON_FILL_FEE_PENDING_RECOVERY_REQUIRED,
                metadata_updates=_fee_pending_metadata_updates(observation_summary),
            )
        apply_fill_and_trade(
            conn,
            client_order_id=client_order_id,
            side=side,
            fill_id=fill.fill_id,
            fill_ts=fill.fill_ts,
            price=fill.price,
            qty=fill.qty,
            fee=fill.fee,
            note=f"reconcile submit_unknown recent exchange_order_id={matched_exchange_order_id or '<none>'}",
            allow_entry_decision_fallback=False,
        )
        applied_fill = True

    prev_status = str(row["status"])
    next_status = prev_status
    order_row = conn.execute(
        "SELECT status, qty_req, qty_filled FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if order_row is not None:
        if applied_fill:
            reconciled_status = _status_after_recent_fill_replay(
                current_status=str(order_row["status"]),
                qty_req=float(order_row["qty_req"]),
                qty_filled=float(order_row["qty_filled"]),
            )
            if reconciled_status is not None:
                next_status = reconciled_status
        elif matched_order is not None:
            next_status = matched_order.status

    set_status(client_order_id, next_status, conn=conn)
    if prev_status != next_status:
        notify(
            format_event(
                "reconcile_status_change",
                client_order_id=client_order_id,
                exchange_order_id=matched_exchange_order_id,
                side=side,
                status=next_status,
                reason=f"from={prev_status}",
            )
        )

    return SubmitUnknownResolution(True, applied_fill, client_order_id, matched_exchange_order_id)


def _record_submit_unknown_autolink_event(
    conn,
    *,
    client_order_id: str,
    side: str,
    submit_attempt_context: dict[str, str | float | bool],
    outcome: str,
    candidate_count: int,
    exchange_order_id: str | None,
    matched_order_count: int = 0,
    matched_fill_count: int = 0,
) -> None:
    submit_attempt_id = str(submit_attempt_context.get("submit_attempt_id") or "")
    event_message = format_event(
        "reconcile_submit_unknown_autolink",
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        side=side,
        outcome=outcome,
        candidate_count=max(0, int(candidate_count)),
        matched_order_count=max(0, int(matched_order_count)),
        matched_fill_count=max(0, int(matched_fill_count)),
        submit_attempt_id=submit_attempt_id or None,
        timeout_submit_unknown=1 if bool(submit_attempt_context.get("timeout_submit_unknown")) else 0,
    )
    notify(event_message)
    conn.execute(
        """
        INSERT INTO order_events(
            client_order_id,
            event_type,
            event_ts,
            order_status,
            exchange_order_id,
            message,
            side,
            submit_attempt_id
        ) VALUES (?, 'reconcile_submit_unknown_autolink', ?, 'SUBMIT_UNKNOWN', ?, ?, ?, ?)
        """,
        (
            client_order_id,
            int(time.time() * 1000),
            exchange_order_id,
            event_message,
            side,
            submit_attempt_id or None,
        ),
    )




def _capture_broker_read_journal(metadata: dict[str, int | str], broker: Broker) -> None:
    getter = getattr(broker, "get_read_journal_summary", None)
    if not callable(getter):
        return
    try:
        journal = getter()
    except Exception:
        return
    if not isinstance(journal, dict):
        return
    compact = {str(k): str(v)[:200] for k, v in list(journal.items())[:8]}
    if compact:
        metadata["broker_read_journal"] = str(compact)[:500]


def _known_identifier_sets(local_rows: list[object]) -> tuple[list[str], list[str]]:
    exchange_ids: set[str] = set()
    client_ids: set[str] = set()
    for row in local_rows:
        exchange_id = str(row["exchange_order_id"] or "").strip()
        client_id = str(row["client_order_id"] or "").strip()
        if exchange_id:
            exchange_ids.add(exchange_id)
        if client_id:
            client_ids.add(client_id)
    return sorted(exchange_ids), sorted(client_ids)


def _get_recent_orders_for_known_ids(
    broker: Broker,
    *,
    limit: int,
    exchange_order_ids: list[str],
    client_order_ids: list[str],
) -> list[BrokerOrder]:
    if not exchange_order_ids and not client_order_ids:
        return []
    try:
        return broker.get_recent_orders(
            limit=limit,
            exchange_order_ids=exchange_order_ids,
            client_order_ids=client_order_ids,
        )
    except TypeError:
        raise BrokerRejectError(
            "broker.get_recent_orders must support identifier-scoped lookups; broad scans are unsupported"
        )


def _get_open_orders_for_known_ids(
    broker: Broker,
    *,
    exchange_order_ids: list[str],
    client_order_ids: list[str],
) -> list[BrokerOrder]:
    if not exchange_order_ids and not client_order_ids:
        return []
    try:
        return broker.get_open_orders(
            exchange_order_ids=exchange_order_ids,
            client_order_ids=client_order_ids,
        )
    except TypeError:
        raise BrokerRejectError(
            "broker.get_open_orders must support identifier-scoped lookups; broad scans are unsupported"
        )


def _get_recent_fills_for_known_orders(
    broker: Broker,
    *,
    exchange_order_ids: list[str],
    client_order_ids_without_exchange_id: list[str],
) -> list[BrokerFill]:
    fills_by_id: dict[str, BrokerFill] = {}
    for exchange_order_id in exchange_order_ids:
        for fill in get_broker_fills(
            broker,
            exchange_order_id=exchange_order_id,
            policy=FillReadPolicy.OBSERVATION_SALVAGE,
        ):
            fills_by_id[str(fill.fill_id)] = fill
    for client_order_id in client_order_ids_without_exchange_id:
        for fill in get_broker_fills(
            broker,
            client_order_id=client_order_id,
            policy=FillReadPolicy.OBSERVATION_SALVAGE,
        ):
            fills_by_id[str(fill.fill_id)] = fill
    return list(fills_by_id.values())


def _clear_reconcile_halt_if_safe(
    *,
    conn,
    reason_code: str,
    metadata: dict[str, int | str],
    broker_open_order_count: int,
) -> None:
    state = runtime_state.snapshot()
    if not state.halt_new_orders_blocked:
        return
    if (state.halt_reason_code or "") == "MANUAL_PAUSE":
        return
    if reason_code in NON_CLEARING_RECONCILE_REASON_CODES:
        return
    if int(metadata.get("balance_split_mismatch_count", 0) or 0) != 0:
        _LOG.info(
            "reconcile_halt_retained reason=balance_split_mismatch halt_reason_code=%s mismatch_count=%s",
            state.halt_reason_code or "-",
            int(metadata.get("balance_split_mismatch_count", 0) or 0),
        )
        return
    unresolved_row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING') THEN 1 ELSE 0 END) AS unresolved_count,
            SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END) AS recovery_required_count
        FROM orders
        """
    ).fetchone()
    portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
    unresolved_count = int(unresolved_row["unresolved_count"] or 0) if unresolved_row else 0
    recovery_required_count = int(unresolved_row["recovery_required_count"] or 0) if unresolved_row else 0
    position_qty = float(portfolio_row["asset_qty"] or 0.0) if portfolio_row else 0.0
    dust_resume_allowed = bool(int(metadata.get("dust_residual_allow_resume", 0) or 0) == 1)
    dust_context = build_dust_display_context(metadata)
    lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    lot_definition = getattr(lot_snapshot, "lot_definition", None)
    position_state = build_position_state_model(
        raw_qty_open=position_qty,
        metadata_raw=metadata,
        raw_total_asset_qty=max(
            position_qty,
            float(lot_snapshot.raw_total_asset_qty),
            float(dust_context.raw_holdings.broker_qty),
        ),
        open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        open_lot_count=int(lot_snapshot.open_lot_count),
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
        internal_lot_size=(None if lot_definition is None else lot_definition.internal_lot_size),
        min_qty=(None if lot_definition is None else lot_definition.min_qty),
        qty_step=(None if lot_definition is None else lot_definition.qty_step),
        min_notional_krw=(None if lot_definition is None else lot_definition.min_notional_krw),
        max_qty_decimals=(None if lot_definition is None else lot_definition.max_qty_decimals),
    )
    normalized_exposure = position_state.normalized_exposure
    position_flat = str(normalized_exposure.terminal_state) == "flat"
    position_dust_only = bool(normalized_exposure.has_dust_only_remainder)
    position_has_executable_exposure = bool(normalized_exposure.has_executable_exposure)
    _LOG.info(
        "reconcile_exposure_decision unresolved_count=%s recovery_required_count=%s broker_open_order_count=%s position_terminal_state=%s raw_total_asset_qty=%.8f executable_exposure_qty=%.8f dust_tracking_qty=%.8f open_lot_count=%s dust_tracking_lot_count=%s sellable_executable_lot_count=%s sellable_executable_qty=%.8f exit_block_reason=%s position_has_executable_exposure=%s position_dust_only=%s dust_resume_allowed=%s halt_reason_code=%s",
        unresolved_count,
        recovery_required_count,
        broker_open_order_count,
        normalized_exposure.terminal_state,
        float(normalized_exposure.raw_total_asset_qty),
        float(normalized_exposure.open_exposure_qty),
        float(normalized_exposure.dust_tracking_qty),
        int(normalized_exposure.open_lot_count),
        int(normalized_exposure.dust_tracking_lot_count),
        int(normalized_exposure.sellable_executable_lot_count),
        float(normalized_exposure.sellable_executable_qty),
        normalized_exposure.exit_block_reason,
        int(position_has_executable_exposure),
        int(position_dust_only),
        int(dust_resume_allowed),
        state.halt_reason_code or "-",
    )
    if not (
        unresolved_count == 0
        and recovery_required_count == 0
        and broker_open_order_count == 0
        and not position_has_executable_exposure
        and (position_flat or (position_dust_only and dust_resume_allowed))
    ):
        _LOG.info(
            "reconcile_halt_retained reason=safety_blockers_remaining unresolved_count=%s recovery_required_count=%s broker_open_order_count=%s raw_total_asset_qty=%.8f executable_exposure_qty=%.8f dust_tracking_qty=%.8f open_lot_count=%s dust_tracking_lot_count=%s sellable_executable_lot_count=%s sellable_executable_qty=%.8f exit_block_reason=%s position_terminal_state=%s position_has_executable_exposure=%s position_dust_only=%s dust_resume_allowed=%s",
            unresolved_count,
            recovery_required_count,
            broker_open_order_count,
            float(normalized_exposure.raw_total_asset_qty),
            float(normalized_exposure.open_exposure_qty),
            float(normalized_exposure.dust_tracking_qty),
            int(normalized_exposure.open_lot_count),
            int(normalized_exposure.dust_tracking_lot_count),
            int(normalized_exposure.sellable_executable_lot_count),
            float(normalized_exposure.sellable_executable_qty),
            normalized_exposure.exit_block_reason,
            normalized_exposure.terminal_state,
            int(position_has_executable_exposure),
            int(position_dust_only),
            int(dust_resume_allowed),
        )
        return
    _LOG.info(
        "reconcile_halt_cleared previous_reason_code=%s previous_unresolved=%s reconcile_reason_code=%s",
        state.halt_reason_code or "-",
        int(state.halt_state_unresolved),
        reason_code,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason=None,
        halt_new_orders_blocked=False,
        unresolved=False,
    )
    runtime_state.set_resume_gate(blocked=False, reason=None)


def _material_zero_fee_fill_summary(
    conn,
    *,
    observed_ts_ms: int | None,
) -> dict[str, int | float]:
    min_notional = max(0.0, float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW))
    if min_notional <= 0:
        return {
            "material_zero_fee_fill_count": 0,
            "material_zero_fee_fill_notional_krw": 0.0,
            "material_zero_fee_fill_latest_ts": 0,
        }

    params: list[object] = [min_notional]
    observed_filter = ""
    if observed_ts_ms is not None and int(observed_ts_ms) > 0:
        observed_filter = "AND fill_ts <= ?"
        params.append(int(observed_ts_ms))
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS fill_count,
            COALESCE(SUM(price * qty), 0.0) AS total_notional,
            COALESCE(MAX(fill_ts), 0) AS latest_fill_ts
        FROM fills
        WHERE price > 0
          AND qty > 0
          AND ABS(COALESCE(fee, 0.0)) <= 1e-12
          AND (price * qty) >= ?
          {observed_filter}
        """,
        tuple(params),
    ).fetchone()
    return {
        "material_zero_fee_fill_count": int(row["fill_count"] or 0),
        "material_zero_fee_fill_notional_krw": float(row["total_notional"] or 0.0),
        "material_zero_fee_fill_latest_ts": int(row["latest_fill_ts"] or 0),
    }


def _fee_gap_adjustment_history_summary(conn) -> dict[str, int | float]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS adjustment_count,
            COALESCE(SUM(delta_amount), 0.0) AS total_delta,
            COALESCE(MAX(event_ts), 0) AS latest_event_ts
        FROM external_cash_adjustments
        WHERE reason='reconcile_fee_gap_cash_drift'
        """
    ).fetchone()
    return {
        "fee_gap_adjustment_count": int(row["adjustment_count"] or 0),
        "fee_gap_adjustment_total_krw": float(row["total_delta"] or 0.0),
        "fee_gap_adjustment_latest_event_ts": int(row["latest_event_ts"] or 0),
    }


def _unaccounted_fee_pending_observation_summary(conn) -> dict[str, int | str]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS pending_count,
            COALESCE(MAX(b.event_ts), 0) AS latest_event_ts,
            COALESCE(MAX(b.fill_ts), 0) AS latest_fill_ts
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
    latest = conn.execute(
        """
        SELECT b.fill_id, b.fee_status
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
        ORDER BY b.event_ts DESC, b.id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "unaccounted_fee_pending_observation_count": int(row["pending_count"] or 0),
        "unaccounted_fee_pending_latest_event_ts": int(row["latest_event_ts"] or 0),
        "unaccounted_fee_pending_latest_fill_ts": int(row["latest_fill_ts"] or 0),
        "unaccounted_fee_pending_latest_fill_id": (
            str(latest["fill_id"]) if latest is not None and latest["fill_id"] is not None else "none"
        ),
        "unaccounted_fee_pending_latest_fee_status": (
            str(latest["fee_status"]) if latest is not None and latest["fee_status"] is not None else "none"
        ),
    }


def _fill_fee_accounting_status(fill: BrokerFill) -> str:
    """Classify whether an observed broker fill is safe for canonical accounting."""
    return fee_accounting_status(
        fee=fill.fee,
        fee_status=getattr(fill, "fee_status", "complete"),
        price=fill.price,
        qty=fill.qty,
        material_notional_threshold=float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW),
        fee_source=getattr(fill, "fee_source", None),
        fee_confidence=getattr(fill, "fee_confidence", None),
        provenance=getattr(fill, "fee_provenance", None),
        reason=getattr(fill, "fee_validation_reason", None),
        checks=getattr(fill, "fee_validation_checks", None),
    )


def _fill_fee_is_accounting_complete(fill: BrokerFill) -> bool:
    return _fill_fee_accounting_status(fill) == "accounting_complete"


def _get_salvage_fills(
    broker: Broker,
    *,
    client_order_id: str,
    exchange_order_id: str | None,
) -> list[BrokerFill]:
    return get_broker_fills(
        broker,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        policy=FillReadPolicy.OBSERVATION_SALVAGE,
    )


def _record_fee_pending_observations(
    conn,
    *,
    client_order_id: str,
    side: str,
    exchange_order_id: str | None,
    fills: list[BrokerFill],
    source: str,
    strict_error: Exception | None = None,
) -> dict[str, int | str]:
    observed_count = 0
    fee_pending_count = 0
    latest_fill_ts = 0
    latest_fee_status = "none"
    latest_fill_id = "none"
    event_ts = int(time.time() * 1000)
    for fill in fills:
        observed_count += 1
        latest_fill_ts = max(latest_fill_ts, int(fill.fill_ts))
        latest_fee_status = str(getattr(fill, "fee_status", "unknown") or "unknown")
        latest_fill_id = str(fill.fill_id or "none")
        accounting_status = _fill_fee_accounting_status(fill)
        if accounting_status != "accounting_complete":
            fee_pending_count += 1
        existing_observation = conn.execute(
            """
            SELECT id
            FROM broker_fill_observations
            WHERE client_order_id=?
              AND COALESCE(exchange_order_id, '')=COALESCE(?, '')
              AND COALESCE(fill_id, '')=COALESCE(?, '')
              AND fill_ts=?
              AND ABS(price-?) < 1e-12
              AND ABS(qty-?) < 1e-12
              AND accounting_status=?
              AND fee_status=?
              AND source=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                client_order_id,
                fill.exchange_order_id or exchange_order_id,
                fill.fill_id,
                int(fill.fill_ts),
                float(fill.price),
                float(fill.qty),
                accounting_status,
                str(getattr(fill, "fee_status", "unknown") or "unknown"),
                source,
            ),
        ).fetchone()
        if existing_observation is not None:
            continue
        record_broker_fill_observation(
            conn,
            event_ts=event_ts,
            client_order_id=client_order_id,
            exchange_order_id=fill.exchange_order_id or exchange_order_id,
            fill_id=fill.fill_id,
            fill_ts=fill.fill_ts,
            side=side,
            price=fill.price,
            qty=fill.qty,
            fee=fill.fee,
            fee_status=str(getattr(fill, "fee_status", "unknown") or "unknown"),
            accounting_status=accounting_status,
            source=source,
            fee_source=str(getattr(fill, "fee_source", "unknown") or "unknown"),
            fee_confidence=str(getattr(fill, "fee_confidence", "unknown") or "unknown"),
            fee_provenance=str(getattr(fill, "fee_provenance", "") or "") or None,
            fee_validation_reason=str(getattr(fill, "fee_validation_reason", "") or "") or None,
            fee_validation_checks=getattr(fill, "fee_validation_checks", None),
            parse_warnings=getattr(fill, "parse_warnings", ()) or (),
            raw_payload=getattr(fill, "raw", None),
        )
    return {
        "observed_fill_count": observed_count,
        "fee_pending_fill_count": fee_pending_count,
        "fee_pending_latest_fill_ts": latest_fill_ts,
        "fee_pending_latest_fee_status": latest_fee_status,
        "fee_pending_latest_fill_id": latest_fill_id,
        "fee_pending_strict_error": f"{type(strict_error).__name__}: {strict_error}" if strict_error is not None else "none",
    }


def _fee_pending_metadata_updates(observation_summary: dict[str, int | str]) -> dict[str, int | str]:
    return {
        "observed_fill_count": int(observation_summary["observed_fill_count"]),
        "fee_pending_fill_count": int(observation_summary["fee_pending_fill_count"]),
        "fee_pending_auto_recovering": 1,
        "fee_pending_latest_fill_ts": int(observation_summary["fee_pending_latest_fill_ts"]),
        "fee_pending_latest_fee_status": str(observation_summary["fee_pending_latest_fee_status"]),
        "fee_pending_latest_fill_id": str(observation_summary["fee_pending_latest_fill_id"]),
        "fee_pending_operator_next_action": (
            "await automatic reconcile retry or inspect broker_fill_observations before manual accounting repair"
        ),
    }


def _merge_fee_pending_metadata(
    metadata: dict[str, int | str],
    updates: dict[str, int | str],
) -> None:
    metadata["observed_fill_count"] = int(metadata["observed_fill_count"]) + int(updates["observed_fill_count"])
    metadata["fee_pending_fill_count"] = int(metadata["fee_pending_fill_count"]) + int(updates["fee_pending_fill_count"])
    metadata["fee_pending_auto_recovering"] = max(
        int(metadata.get("fee_pending_auto_recovering", 0) or 0),
        int(updates.get("fee_pending_auto_recovering", 0) or 0),
    )
    metadata["fee_pending_latest_fill_ts"] = int(updates["fee_pending_latest_fill_ts"])
    metadata["fee_pending_latest_fee_status"] = str(updates["fee_pending_latest_fee_status"])
    metadata["fee_pending_latest_fill_id"] = str(updates["fee_pending_latest_fill_id"])
    metadata["fee_pending_operator_next_action"] = str(updates["fee_pending_operator_next_action"])


def reconcile_with_broker(broker: Broker) -> None:
    conn = ensure_db()
    reason_code = REASON_RECONCILE_OK
    source_conflicts: list[str] = []
    metadata: dict[str, int | str] = {
        "remote_open_order_found": 0,
        "recent_fill_applied": 0,
        "submit_unknown_unresolved": 0,
        "startup_gate_blocked": 0,
        "source_conflict_halt": 0,
        "invalid_fill_price_blocked": 0,
        "balance_split_mismatch_count": 0,
        "lookup_known_exchange_id": 0,
        "lookup_known_client_order_id": 0,
        "lookup_identifier_missing": 0,
        "lookup_not_found": 0,
        "lookup_identifier_mismatch": 0,
        "lookup_temporary_broker_error": 0,
        "lookup_schema_mismatch": 0,
        "balance_source": "-",
        "balance_observed_ts_ms": 0,
        "dust_residual_present": 0,
        "dust_residual_allow_resume": 0,
        "dust_policy_reason": "no_dust_residual",
        "material_zero_fee_fill_count": 0,
        "material_zero_fee_fill_latest_ts": 0,
        "fee_gap_recovery_required": 0,
        "observed_fill_count": 0,
        "fee_pending_fill_count": 0,
        "fee_pending_auto_recovering": 0,
        "fee_pending_latest_fill_ts": 0,
        "fee_pending_latest_fee_status": "none",
        "fee_pending_latest_fill_id": "none",
        "fee_pending_operator_next_action": "none",
        "fee_gap_adjustment_count": 0,
        "fee_gap_adjustment_latest_event_ts": 0,
    }
    try:
        synchronize_order_state_invariants(conn)
        init_portfolio(conn)

        placeholders = ",".join("?" for _ in LOCAL_RECONCILE_STATUSES)
        local_open = conn.execute(
            f"SELECT client_order_id, submit_attempt_id, exchange_order_id, side, qty_req, status FROM orders WHERE status IN ({placeholders})",
            LOCAL_RECONCILE_STATUSES,
        ).fetchall()
        known_exchange_order_ids, known_client_order_ids = _known_identifier_sets(local_open)
        client_order_ids_without_exchange_id = sorted(
            {
                str(row["client_order_id"])
                for row in local_open
                if not str(row["exchange_order_id"] or "").strip()
            }
        )
        recent_orders = _get_recent_orders_for_known_ids(
            broker,
            limit=100,
            exchange_order_ids=known_exchange_order_ids,
            client_order_ids=known_client_order_ids,
        )
        recent_fills = _get_recent_fills_for_known_orders(
            broker,
            exchange_order_ids=known_exchange_order_ids,
            client_order_ids_without_exchange_id=client_order_ids_without_exchange_id,
        )
        resolved_submit_unknown_client_ids: set[str] = set()
        resolved_submit_unknown_exchange_ids: set[str] = set()
        for row in local_open:
            oid = row["client_order_id"]
            if row["status"] == "SUBMIT_UNKNOWN" and not row["exchange_order_id"]:
                resolution = _try_resolve_submit_unknown_from_recent_activity(
                    conn,
                    row=row,
                    recent_orders=recent_orders,
                    recent_fills=recent_fills,
                )
                if resolution.recovered:
                    if resolution.resolved_client_order_id:
                        resolved_submit_unknown_client_ids.add(str(resolution.resolved_client_order_id))
                    if resolution.resolved_exchange_order_id:
                        resolved_submit_unknown_exchange_ids.add(str(resolution.resolved_exchange_order_id))
                    if resolution.metadata_updates:
                        if "invalid_fill_price_blocked" in resolution.metadata_updates:
                            metadata["invalid_fill_price_blocked"] = (
                                int(metadata["invalid_fill_price_blocked"])
                                + int(resolution.metadata_updates["invalid_fill_price_blocked"])
                            )
                        if "fee_pending_auto_recovering" in resolution.metadata_updates:
                            _merge_fee_pending_metadata(metadata, resolution.metadata_updates)
                    if resolution.reason_code and reason_code == REASON_RECONCILE_OK:
                        reason_code = resolution.reason_code
                    if resolution.applied_fill:
                        metadata["recent_fill_applied"] += 1
                        if reason_code == REASON_RECONCILE_OK:
                            reason_code = REASON_RECENT_FILL_APPLIED
                    continue
                submit_attempt_context = _load_submit_attempt_context(conn, row=row)
                reason_detail = "no matching recent order/fill"
                if not bool(submit_attempt_context.get("submit_attempt_id")):
                    reason_detail = "missing submit_attempt_id metadata"
                elif not bool(submit_attempt_context.get("metadata_present")):
                    reason_detail = "no submit-attempt events persisted"
                elif not bool(submit_attempt_context.get("timeout_submit_unknown")):
                    reason_detail = "submit-attempt metadata does not confirm timeout ambiguity"
                reason = (
                    "submit_unknown unresolved; "
                    f"{reason_detail}; manual recovery required"
                )
                _mark_recovery_required_with_reason(
                    conn,
                    client_order_id=oid,
                    side=str(row["side"]),
                    from_status="SUBMIT_UNKNOWN",
                    reason_code=WEAK_ORDER_CORRELATION,
                    reason=reason,
                )
                metadata["submit_unknown_unresolved"] += 1
                reason_code = REASON_SUBMIT_UNKNOWN_UNRESOLVED
                continue

            exchange_order_id = str(row["exchange_order_id"] or "").strip()
            client_order_id = str(oid or "").strip()
            lookup_mode = "missing_identifier"
            if exchange_order_id:
                lookup_mode = "known_exchange_order_id"
                metadata["lookup_known_exchange_id"] += 1
            elif client_order_id:
                lookup_mode = "known_client_order_id"
                metadata["lookup_known_client_order_id"] += 1
            else:
                metadata["lookup_identifier_missing"] += 1
                _mark_recovery_required_with_reason(
                    conn,
                    client_order_id=oid,
                    side=str(row["side"]),
                    from_status=str(row["status"]),
                    reason_code=REASON_IDENTIFIER_LOOKUP_REQUIRES_RECOVERY,
                    reason="reconcile identifier lookup failed: identifier missing; manual recovery required",
                )
                if reason_code == REASON_RECONCILE_OK:
                    reason_code = REASON_IDENTIFIER_LOOKUP_REQUIRES_RECOVERY
                continue
            try:
                remote = broker.get_order(
                    client_order_id=(client_order_id if client_order_id else None),
                    exchange_order_id=(exchange_order_id if exchange_order_id else None),
                )
            except Exception as exc:
                failure_kind = _classify_lookup_error(exc)
                if failure_kind == "lookup_not_found":
                    metadata["lookup_not_found"] += 1
                elif failure_kind == "identifier_mismatch":
                    metadata["lookup_identifier_mismatch"] += 1
                elif failure_kind == "temporary_broker_error":
                    metadata["lookup_temporary_broker_error"] += 1
                elif failure_kind == "schema_mismatch":
                    metadata["lookup_schema_mismatch"] += 1

                if failure_kind in {"lookup_not_found", "identifier_mismatch", "schema_mismatch"}:
                    _mark_recovery_required_with_reason(
                        conn,
                        client_order_id=oid,
                        side=str(row["side"]),
                        from_status=str(row["status"]),
                        reason_code=REASON_IDENTIFIER_LOOKUP_REQUIRES_RECOVERY,
                        reason=(
                            f"reconcile identifier lookup failed: {failure_kind}; "
                            f"lookup_mode={lookup_mode}; manual recovery required ({type(exc).__name__}: {exc})"
                        ),
                    )
                    if reason_code == REASON_RECONCILE_OK:
                        reason_code = REASON_IDENTIFIER_LOOKUP_REQUIRES_RECOVERY
                    continue
                raise
            if remote.exchange_order_id:
                set_exchange_order_id(oid, remote.exchange_order_id, conn=conn)
                notify(
                    format_event(
                        "exchange_order_id_attached",
                        client_order_id=oid,
                        exchange_order_id=remote.exchange_order_id,
                        side=row["side"],
                        status=remote.status,
                        reason="reconcile",
                    )
                )
            prev_status = row["status"]
            defer_terminal_fill_status = str(remote.status) == "FILLED"
            if not defer_terminal_fill_status:
                set_status(oid, remote.status, conn=conn)
                if prev_status != remote.status:
                    notify(
                        format_event(
                            "reconcile_status_change",
                            client_order_id=oid,
                            exchange_order_id=remote.exchange_order_id,
                            side=row["side"],
                            status=remote.status,
                            reason=f"from={prev_status}",
                        )
                    )
            fills = _get_salvage_fills(
                broker,
                client_order_id=str(oid),
                exchange_order_id=remote.exchange_order_id,
            )
            invalid_fill = next((fill for fill in fills if float(fill.price) <= 0), None)
            if invalid_fill is not None:
                reason = (
                    "reconcile blocked: fill has missing/invalid execution price; "
                    f"exchange_order_id={remote.exchange_order_id or '<none>'}; "
                    f"fill_id={invalid_fill.fill_id}"
                )
                _mark_recovery_required_with_reason(
                    conn,
                    client_order_id=oid,
                    side=str(row["side"]),
                    from_status=remote.status,
                    reason_code=REASON_RECENT_FILL_INVALID_PRICE,
                    reason=reason,
                )
                metadata["invalid_fill_price_blocked"] += 1
                if reason_code == REASON_RECONCILE_OK:
                    reason_code = REASON_RECENT_FILL_INVALID_PRICE
                continue
            fee_pending_fill = next((fill for fill in fills if not _fill_fee_is_accounting_complete(fill)), None)
            if fee_pending_fill is not None:
                observation_summary = _record_fee_pending_observations(
                    conn,
                    client_order_id=str(oid),
                    side=str(row["side"]),
                    exchange_order_id=remote.exchange_order_id,
                    fills=fills,
                    source="reconcile_fee_pending",
                )
                for fill in fills:
                    if _fill_fee_is_accounting_complete(fill):
                        apply_fill_and_trade(
                            conn,
                            client_order_id=oid,
                            side=row["side"],
                            fill_id=fill.fill_id,
                            fill_ts=fill.fill_ts,
                            price=fill.price,
                            qty=fill.qty,
                            fee=fill.fee,
                            note=f"reconcile exchange_order_id={remote.exchange_order_id}",
                            allow_entry_decision_fallback=False,
                        )
                    else:
                        apply_fill_principal_with_pending_fee(
                            conn,
                            client_order_id=oid,
                            side=row["side"],
                            fill_id=fill.fill_id,
                            fill_ts=fill.fill_ts,
                            price=fill.price,
                            qty=fill.qty,
                            fee=getattr(fill, "fee", None),
                            fee_status=getattr(fill, "fee_status", "unknown"),
                            fee_source=getattr(fill, "fee_source", None),
                            fee_confidence=getattr(fill, "fee_confidence", None),
                            fee_provenance=getattr(fill, "fee_provenance", None),
                            fee_validation_reason=getattr(fill, "fee_validation_reason", None),
                            fee_validation_checks=getattr(fill, "fee_validation_checks", None),
                            note=f"reconcile exchange_order_id={remote.exchange_order_id}",
                            allow_entry_decision_fallback=False,
                        )
                _merge_fee_pending_metadata(metadata, _fee_pending_metadata_updates(observation_summary))
                if reason_code == REASON_RECONCILE_OK:
                    reason_code = REASON_FILL_FEE_PENDING_RECOVERY_REQUIRED
                if defer_terminal_fill_status:
                    set_status(oid, remote.status, conn=conn)
                continue
            fill_apply_blocked = False
            for fill in fills:
                try:
                    apply_fill_and_trade(
                        conn,
                        client_order_id=oid,
                        side=row["side"],
                        fill_id=fill.fill_id,
                        fill_ts=fill.fill_ts,
                        price=fill.price,
                        qty=fill.qty,
                        fee=fill.fee,
                        note=f"reconcile exchange_order_id={remote.exchange_order_id}",
                        allow_entry_decision_fallback=False,
                    )
                except LiveFillFeeValidationError as exc:
                    _mark_recovery_required_with_reason(
                        conn,
                        client_order_id=oid,
                        side=str(row["side"]),
                        from_status=remote.status,
                        reason_code=REASON_FEE_GAP_RECOVERY_REQUIRED,
                        reason=(
                            "reconcile blocked: fill fee validation failed during ledger apply; "
                            f"exchange_order_id={remote.exchange_order_id or '<none>'}; "
                            f"fill_id={fill.fill_id}; manual recovery required ({exc})"
                        ),
                    )
                    metadata["fee_gap_recovery_required"] = 1
                    if reason_code == REASON_RECONCILE_OK:
                        reason_code = REASON_FEE_GAP_RECOVERY_REQUIRED
                    fill_apply_blocked = True
                    break
            if defer_terminal_fill_status and not fill_apply_blocked:
                set_status(oid, remote.status, conn=conn)
                if prev_status != remote.status:
                    notify(
                        format_event(
                            "reconcile_status_change",
                            client_order_id=oid,
                            exchange_order_id=remote.exchange_order_id,
                            side=row["side"],
                            status=remote.status,
                            reason=f"from={prev_status}",
                        )
                    )

        remote_open = _get_open_orders_for_known_ids(
            broker,
            exchange_order_ids=known_exchange_order_ids,
            client_order_ids=known_client_order_ids,
        )
        trusted_open_exchange_ids = {
            str(order.exchange_order_id)
            for order in remote_open
            if order.exchange_order_id
        }
        known_exchange_ids = {
            str(r["exchange_order_id"])
            for r in conn.execute(
                "SELECT exchange_order_id FROM orders WHERE exchange_order_id IS NOT NULL"
            ).fetchall()
        }
        for remote in remote_open:
            exid = str(remote.exchange_order_id or "")
            if not exid or exid in known_exchange_ids:
                continue
            oid = f"remote_{exid}"
            record_order_if_missing(
                conn,
                client_order_id=oid,
                side=remote.side,
                qty_req=remote.qty_req,
                price=remote.price,
                ts_ms=remote.created_ts,
                status="SUBMIT_UNKNOWN",
            )
            set_exchange_order_id(oid, exid, conn=conn)
            set_status(oid, remote.status, last_error="stray remote open order detected", conn=conn)
            metadata["remote_open_order_found"] += 1
            if reason_code == REASON_RECONCILE_OK:
                reason_code = REASON_REMOTE_OPEN_ORDER_FOUND
            notify(
                format_event(
                    "reconcile_status_change",
                    client_order_id=oid,
                    exchange_order_id=exid,
                    side=remote.side,
                    status=remote.status,
                    reason="stray remote open order detected",
                )
            )

        filtered_recent_orders = [
            remote
            for remote in recent_orders
            if str(remote.client_order_id or "") not in resolved_submit_unknown_client_ids
            and str(remote.exchange_order_id or "") not in resolved_submit_unknown_exchange_ids
        ]
        filtered_recent_fills = [
            fill
            for fill in recent_fills
            if str(fill.client_order_id or "") not in resolved_submit_unknown_client_ids
            and str(fill.exchange_order_id or "") not in resolved_submit_unknown_exchange_ids
        ]
        conflicts = _sync_recent_order_activity(
            conn,
            filtered_recent_orders,
            trusted_open_exchange_ids=trusted_open_exchange_ids,
        )
        (
            applied_recent_fill,
            fill_conflicts,
            blocked_recent_fill_price,
            recent_fill_fee_pending_updates,
        ) = _apply_recent_fills(
            conn,
            filtered_recent_fills,
            trusted_open_exchange_ids=trusted_open_exchange_ids,
        )
        conflicts.extend(fill_conflicts)
        metadata["invalid_fill_price_blocked"] += int(blocked_recent_fill_price)
        if recent_fill_fee_pending_updates is not None:
            _merge_fee_pending_metadata(metadata, recent_fill_fee_pending_updates)
            if reason_code == REASON_RECONCILE_OK:
                reason_code = REASON_FILL_FEE_PENDING_RECOVERY_REQUIRED

        if applied_recent_fill:
            metadata["recent_fill_applied"] += 1
            if reason_code == REASON_RECONCILE_OK:
                reason_code = REASON_RECENT_FILL_APPLIED

        if int(metadata["invalid_fill_price_blocked"]) > 0 and reason_code == REASON_RECONCILE_OK:
            reason_code = REASON_RECENT_FILL_INVALID_PRICE

        if conflicts:
            source_conflicts = conflicts
            metadata["source_conflict_halt"] = len(conflicts)
            reason_code = REASON_SOURCE_CONFLICT_HALT

        balance_snapshot = fetch_balance_snapshot(broker)
        bal = balance_snapshot.balance
        metadata["balance_source"] = str(balance_snapshot.source_id or "-")
        metadata["balance_observed_ts_ms"] = int(balance_snapshot.observed_ts_ms)
        local_cash_available, local_cash_locked, local_asset_available, local_asset_locked = get_portfolio_breakdown(conn)
        has_open_orders = bool(local_open) or bool(remote_open)

        broker_cash_available = float(bal.cash_available)
        broker_cash_locked = float(bal.cash_locked)
        asset_locked = float(bal.asset_locked)
        local_cash_total = portfolio_cash_total(
            cash_available=local_cash_available,
            cash_locked=local_cash_locked,
        )
        broker_cash_total = portfolio_cash_total(
            cash_available=broker_cash_available,
            cash_locked=broker_cash_locked,
        )
        portfolio_cash_available = broker_cash_available
        portfolio_asset_available = float(bal.asset_available)
        portfolio_asset_locked = asset_locked
        cash_delta = broker_cash_total - local_cash_total
        cash_locked = broker_cash_locked
        # Accounts snapshot source can briefly report locked=0 around in-flight open orders.
        # Keep local locked split as conservative floor when remote split is zero during open-order windows.
        if has_open_orders and cash_locked <= 1e-12 and local_cash_locked > 1e-12:
            cash_locked = local_cash_locked

        pre_adjustment_mismatch_count, pre_adjustment_mismatch_summary = _balance_split_mismatch_summary(
            broker_cash_available=broker_cash_available,
            broker_cash_locked=broker_cash_locked,
            broker_asset_available=float(bal.asset_available),
            broker_asset_locked=asset_locked,
            local_cash_available=local_cash_available,
            local_cash_locked=local_cash_locked,
            local_asset_available=local_asset_available,
            local_asset_locked=local_asset_locked,
        )
        zero_fee_fill_summary = _material_zero_fee_fill_summary(
            conn,
            observed_ts_ms=int(balance_snapshot.observed_ts_ms or 0),
        )
        metadata["material_zero_fee_fill_count"] = int(zero_fee_fill_summary["material_zero_fee_fill_count"])
        metadata["material_zero_fee_fill_notional_krw"] = float(zero_fee_fill_summary["material_zero_fee_fill_notional_krw"])
        metadata["material_zero_fee_fill_latest_ts"] = int(zero_fee_fill_summary["material_zero_fee_fill_latest_ts"])
        pending_observation_summary = _unaccounted_fee_pending_observation_summary(conn)
        metadata.update(pending_observation_summary)
        if int(pending_observation_summary["unaccounted_fee_pending_observation_count"]) > 0:
            metadata["fee_pending_auto_recovering"] = 1
            metadata["fee_pending_fill_count"] = max(
                int(metadata.get("fee_pending_fill_count", 0) or 0),
                int(pending_observation_summary["unaccounted_fee_pending_observation_count"]),
            )
            metadata["fee_pending_latest_fill_ts"] = int(
                pending_observation_summary["unaccounted_fee_pending_latest_fill_ts"]
            )
            metadata["fee_pending_latest_fee_status"] = str(
                pending_observation_summary["unaccounted_fee_pending_latest_fee_status"]
            )
            metadata["fee_pending_latest_fill_id"] = str(
                pending_observation_summary["unaccounted_fee_pending_latest_fill_id"]
            )
            if str(metadata.get("fee_pending_operator_next_action") or "none") == "none":
                metadata["fee_pending_operator_next_action"] = (
                    "resolve unaccounted broker_fill_observations before cash-drift adjustment or resume"
                )
            if reason_code == REASON_RECONCILE_OK:
                reason_code = REASON_FILL_FEE_PENDING_RECOVERY_REQUIRED
        external_cash_adjustment = None
        should_record_external_cash_adjustment = (
            abs(cash_delta) > CASH_SPLIT_ABS_TOL
            and reason_code not in NON_CLEARING_RECONCILE_REASON_CODES
            and not conflicts
            and pre_adjustment_mismatch_count == 1
            and "cash_available" in pre_adjustment_mismatch_summary
            and "cash_locked" not in pre_adjustment_mismatch_summary
            and "asset_available" not in pre_adjustment_mismatch_summary
            and "asset_locked" not in pre_adjustment_mismatch_summary
        )
        if should_record_external_cash_adjustment:
            fee_gap_drift_detected = (
                int(zero_fee_fill_summary["material_zero_fee_fill_count"]) > 0
                and abs(cash_delta) > CASH_SPLIT_ABS_TOL
            )
            adjustment_key = _external_cash_adjustment_key(
                balance_source=str(balance_snapshot.source_id or "-"),
                broker_cash_available=broker_cash_available,
                broker_cash_locked=broker_cash_locked,
                broker_cash_total=broker_cash_total,
                local_cash_available=float(local_cash_available),
                local_cash_locked=float(local_cash_locked),
                local_cash_total=local_cash_total,
                cash_delta=cash_delta,
                recent_fill_applied=int(metadata["recent_fill_applied"]),
                remote_open_order_found=int(metadata["remote_open_order_found"]),
                invalid_fill_price_blocked=int(metadata["invalid_fill_price_blocked"]),
                unresolved_open_order_count=int(metadata.get("unresolved_open_order_count", 0) or 0),
                submit_unknown_count=int(metadata.get("submit_unknown_count", 0) or 0),
                recovery_required_count=int(metadata.get("recovery_required_count", 0) or 0),
            )
            external_cash_adjustment = record_external_cash_adjustment(
                conn,
                event_ts=int(balance_snapshot.observed_ts_ms or (time.time() * 1000)),
                currency="KRW",
                delta_amount=cash_delta,
                source=str(balance_snapshot.source_id or "reconcile"),
                reason=("reconcile_fee_gap_cash_drift" if fee_gap_drift_detected else "reconcile_cash_drift"),
                broker_snapshot_basis={
                    "key_version": CASH_ADJUSTMENT_KEY_VERSION,
                    "balance_source": str(balance_snapshot.source_id or "-"),
                    "observed_ts_ms": int(balance_snapshot.observed_ts_ms),
                    "asset_ts_ms": int(balance_snapshot.asset_ts_ms),
                    "broker_cash_available": broker_cash_available,
                    "broker_cash_locked": broker_cash_locked,
                    "broker_cash_total": broker_cash_total,
                    "local_cash_available": float(local_cash_available),
                    "local_cash_locked": float(local_cash_locked),
                    "local_cash_total": local_cash_total,
                    "cash_delta": cash_delta,
                    "reconcile_reason_code": reason_code,
                    "material_zero_fee_fill_count": int(zero_fee_fill_summary["material_zero_fee_fill_count"]),
                    "material_zero_fee_fill_notional_krw": float(zero_fee_fill_summary["material_zero_fee_fill_notional_krw"]),
                },
                correlation_metadata={
                    "remote_open_order_found": int(metadata["remote_open_order_found"]),
                    "recent_fill_applied": int(metadata["recent_fill_applied"]),
                    "invalid_fill_price_blocked": int(metadata["invalid_fill_price_blocked"]),
                    "fee_gap_recovery_required": 1 if fee_gap_drift_detected else 0,
                },
                note=(
                    "cash drift inferred from reconcile balance snapshot; material zero-fee fill history present"
                    if fee_gap_drift_detected
                    else "cash drift inferred from reconcile balance snapshot"
                ),
                adjustment_key=adjustment_key,
            )
            metadata["external_cash_adjustment_count"] = 1
            metadata["external_cash_adjustment_delta_krw"] = cash_delta
            metadata["external_cash_adjustment_total_krw"] = cash_delta
            metadata["external_cash_adjustment_event_ts"] = int(balance_snapshot.observed_ts_ms or 0)
            metadata["external_cash_adjustment_created"] = 1 if external_cash_adjustment and external_cash_adjustment.get("created") else 0
            metadata["external_cash_adjustment_key"] = str(external_cash_adjustment["adjustment_key"]) if external_cash_adjustment else adjustment_key
            metadata["external_cash_adjustment_reason"] = str(external_cash_adjustment["reason"]) if external_cash_adjustment else "reconcile_cash_drift"

            adjusted_cash_available, adjusted_cash_locked, adjusted_asset_available, adjusted_asset_locked = get_portfolio_breakdown(conn)
            adjusted_cash_total = portfolio_cash_total(
                cash_available=adjusted_cash_available,
                cash_locked=adjusted_cash_locked,
            )
            residual_after_adjustment = broker_cash_total - adjusted_cash_total
            if abs(residual_after_adjustment) > CASH_SPLIT_ABS_TOL:
                raise RuntimeError(
                    "external cash adjustment failed to absorb broker/local cash delta: "
                    f"broker_cash_total={broker_cash_total:.12g} "
                    f"adjusted_cash_total={adjusted_cash_total:.12g} "
                    f"residual={residual_after_adjustment:.12g}"
                )
            metadata["external_cash_adjustment_residual_krw"] = residual_after_adjustment
            local_cash_available = float(adjusted_cash_available)
            local_cash_locked = float(adjusted_cash_locked)
            local_asset_available = float(adjusted_asset_available)
            local_asset_locked = float(adjusted_asset_locked)
            local_cash_total = adjusted_cash_total
            portfolio_cash_available = local_cash_available
        fee_gap_adjustment_history = _fee_gap_adjustment_history_summary(conn)
        metadata["fee_gap_adjustment_count"] = int(fee_gap_adjustment_history["fee_gap_adjustment_count"])
        metadata["fee_gap_adjustment_total_krw"] = float(fee_gap_adjustment_history["fee_gap_adjustment_total_krw"])
        metadata["fee_gap_adjustment_latest_event_ts"] = int(fee_gap_adjustment_history["fee_gap_adjustment_latest_event_ts"])
        if int(metadata["fee_gap_adjustment_count"]) > 0:
            metadata["fee_gap_recovery_required"] = 1
            if reason_code == REASON_RECONCILE_OK:
                reason_code = REASON_FEE_GAP_RECOVERY_REQUIRED
        if has_open_orders and asset_locked <= 1e-12 and local_asset_locked > 1e-12:
            asset_locked = local_asset_locked
            portfolio_asset_locked = asset_locked
        if int(metadata.get("unaccounted_fee_pending_observation_count", 0) or 0) > 0:
            portfolio_cash_available = local_cash_available
            cash_locked = local_cash_locked
            portfolio_asset_available = local_asset_available
            portfolio_asset_locked = local_asset_locked
            metadata["portfolio_projection_update_deferred_reason"] = "unaccounted_fee_pending_observation"

        mismatch_count, mismatch_summary = _balance_split_mismatch_summary(
            broker_cash_available=broker_cash_available,
            broker_cash_locked=cash_locked,
            broker_asset_available=float(bal.asset_available),
            broker_asset_locked=asset_locked,
            local_cash_available=local_cash_available,
            local_cash_locked=local_cash_locked,
            local_asset_available=local_asset_available,
            local_asset_locked=local_asset_locked,
        )
        metadata["balance_split_mismatch_count"] = mismatch_count
        if mismatch_summary:
            metadata["balance_split_mismatch_summary"] = mismatch_summary[:500]
        dust_eval = _evaluate_dust_residual_policy(
            conn=conn,
            broker_asset_available=float(bal.asset_available),
            broker_asset_locked=asset_locked,
            local_asset_available=local_asset_available,
            local_asset_locked=local_asset_locked,
        )
        for key, value in dust_eval.items():
            metadata[key] = value
        dust_tracking_count = mark_harmless_dust_positions(
            conn,
            pair=settings.PAIR,
            dust_metadata=dust_eval,
        )
        metadata["dust_tracking_position_lot_count"] = dust_tracking_count

        set_portfolio_breakdown(
            conn,
            cash_available=portfolio_cash_available,
            cash_locked=cash_locked,
            asset_available=portfolio_asset_available,
            asset_locked=portfolio_asset_locked,
        )
        conn.commit()
    except Exception as e:
        _capture_broker_read_journal(metadata, broker)
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        conn = None
        runtime_state.record_reconcile_result(
            success=False,
            error=f"{type(e).__name__}: {e}",
            reason_code=REASON_RECONCILE_FAILED,
            metadata=metadata,
        )
        runtime_state.refresh_open_order_health()
        raise
    else:
        if source_conflicts:
            _halt_on_source_conflict(source_conflicts)
        if metadata["startup_gate_blocked"] > 0:
            reason_code = REASON_STARTUP_GATE_BLOCKED

        classification = classify_recovery_outcome(
            reason_code=reason_code,
            metadata=metadata,
            source_conflicts=source_conflicts,
        )
        metadata["recovery_disposition"] = classification.disposition.value
        metadata["recovery_progress_state"] = classification.progress_state.value
        metadata["recovery_classification_reason"] = classification.reason

        _capture_broker_read_journal(metadata, broker)
        runtime_state.record_reconcile_result(success=True, reason_code=reason_code, metadata=metadata)
        runtime_state.refresh_open_order_health()
        _clear_reconcile_halt_if_safe(
            conn=conn,
            reason_code=reason_code,
            metadata=metadata,
            broker_open_order_count=len(remote_open),
        )
    finally:
        if conn is not None:
            conn.close()


def recover_order_with_exchange_id(
    broker: Broker,
    *,
    client_order_id: str,
    exchange_order_id: str,
) -> None:
    conn = ensure_db()
    order_found = False
    try:
        order = conn.execute(
            "SELECT client_order_id, side FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if order is None:
            raise RuntimeError(f"unknown client_order_id: {client_order_id}")

        order_found = True
        side = str(order["side"])
        set_exchange_order_id(client_order_id, exchange_order_id, conn=conn)

        remote = broker.get_order(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
        resolved_exchange_order_id = str(remote.exchange_order_id or exchange_order_id)
        if resolved_exchange_order_id:
            set_exchange_order_id(client_order_id, resolved_exchange_order_id, conn=conn)

        fills = _get_salvage_fills(
            broker,
            client_order_id=client_order_id,
            exchange_order_id=resolved_exchange_order_id,
        )
        invalid_fill = next((fill for fill in fills if float(fill.price) <= 0), None)
        if invalid_fill is not None:
            raise RuntimeError(
                "manual recovery blocked: fill has missing/invalid execution price; "
                f"exchange_order_id={resolved_exchange_order_id}; fill_id={invalid_fill.fill_id}"
            )
        fee_pending_fill = next((fill for fill in fills if not _fill_fee_is_accounting_complete(fill)), None)
        if fee_pending_fill is not None:
            _record_fee_pending_observations(
                conn,
                client_order_id=client_order_id,
                side=side,
                exchange_order_id=resolved_exchange_order_id,
                fills=fills,
                source="manual_recover_order_fee_pending",
            )
            for fill in fills:
                if _fill_fee_is_accounting_complete(fill):
                    apply_fill_and_trade(
                        conn,
                        client_order_id=client_order_id,
                        side=side,
                        fill_id=fill.fill_id,
                        fill_ts=fill.fill_ts,
                        price=fill.price,
                        qty=fill.qty,
                        fee=fill.fee,
                        note=f"manual recovery exchange_order_id={resolved_exchange_order_id}",
                        allow_entry_decision_fallback=False,
                    )
                else:
                    apply_fill_principal_with_pending_fee(
                        conn,
                        client_order_id=client_order_id,
                        side=side,
                        fill_id=fill.fill_id,
                        fill_ts=fill.fill_ts,
                        price=fill.price,
                        qty=fill.qty,
                        fee=getattr(fill, "fee", None),
                        fee_status=getattr(fill, "fee_status", "unknown"),
                        fee_source=getattr(fill, "fee_source", None),
                        fee_confidence=getattr(fill, "fee_confidence", None),
                        fee_provenance=getattr(fill, "fee_provenance", None),
                        fee_validation_reason=getattr(fill, "fee_validation_reason", None),
                        fee_validation_checks=getattr(fill, "fee_validation_checks", None),
                        note=f"manual recovery exchange_order_id={resolved_exchange_order_id}",
                        allow_entry_decision_fallback=False,
                    )
            set_status(client_order_id, remote.status, conn=conn)
            conn.commit()
            return
        for fill in fills:
            apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee,
                note=f"manual recovery exchange_order_id={resolved_exchange_order_id}",
                allow_entry_decision_fallback=False,
            )

        if remote.status in LOCAL_RECONCILE_STATUSES:
            raise RuntimeError(f"order still unresolved after recovery: status={remote.status}")

        set_status(client_order_id, remote.status, conn=conn)
        conn.commit()
    except Exception as e:
        if order_found:
            reason = f"manual recovery failed: {type(e).__name__}: {e}"
            current = conn.execute(
                "SELECT status FROM orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            record_status_transition(
                client_order_id,
                from_status=(str(current["status"]) if current and current["status"] else "UNKNOWN"),
                to_status="RECOVERY_REQUIRED",
                reason=reason,
                conn=conn,
            )
            set_status(
                client_order_id,
                "RECOVERY_REQUIRED",
                last_error=reason,
                conn=conn,
            )
            conn.commit()
        raise
    finally:
        conn.close()
        runtime_state.refresh_open_order_health()


def backfill_broker_order_with_exchange_id(
    broker: Broker,
    *,
    exchange_order_id: str,
) -> dict[str, str | int | float]:
    exchange_order_id = str(exchange_order_id or "").strip()
    if not exchange_order_id:
        raise RuntimeError("exchange_order_id is required for broker-known backfill")

    conn = ensure_db()
    client_order_id = ""
    try:
        existing = conn.execute(
            """
            SELECT client_order_id, status
            FROM orders
            WHERE exchange_order_id=?
            """,
            (exchange_order_id,),
        ).fetchone()
        if existing is not None:
            raise RuntimeError(
                "broker order already has local lineage: "
                f"client_order_id={existing['client_order_id']} status={existing['status']}"
            )

        remote = broker.get_order(client_order_id=None, exchange_order_id=exchange_order_id)
        resolved_exchange_order_id = str(remote.exchange_order_id or exchange_order_id).strip()
        remote_client_order_id = str(remote.client_order_id or "").strip()
        if remote_client_order_id:
            client_order_id = remote_client_order_id
            existing_client = conn.execute(
                "SELECT status FROM orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            if existing_client is not None:
                raise RuntimeError(
                    "broker order client_order_id already exists locally with different/missing exchange linkage: "
                    f"client_order_id={client_order_id} status={existing_client['status']}"
                )
        else:
            client_order_id = _safe_recovery_client_order_id(
                tag="broker_backfill",
                exchange_order_id=resolved_exchange_order_id,
                ts=int(remote.updated_ts or time.time() * 1000),
            )

        side = str(remote.side or "").upper()
        if side not in {"BUY", "SELL"}:
            raise RuntimeError(f"broker order side is invalid for backfill: {remote.side}")

        record_order_if_missing(
            conn,
            client_order_id=client_order_id,
            submit_attempt_id=f"{client_order_id}:broker_backfill",
            symbol=settings.PAIR,
            side=side,
            qty_req=float(remote.qty_req or 0.0),
            price=remote.price,
            strategy_name="broker_backfill",
            decision_reason="broker-known local-missing recovery backfill",
            order_type=None,
            local_intent_state="BACKFILLED_BROKER_OBSERVED",
            ts_ms=int(remote.created_ts or remote.updated_ts or time.time() * 1000),
            status="RECOVERY_REQUIRED",
        )
        set_exchange_order_id(client_order_id, resolved_exchange_order_id, conn=conn)

        fills = _get_salvage_fills(
            broker,
            client_order_id=client_order_id,
            exchange_order_id=resolved_exchange_order_id,
        )
        invalid_fill = next((fill for fill in fills if float(fill.price) <= 0), None)
        if invalid_fill is not None:
            reason = (
                "broker-known backfill blocked: fill has missing/invalid execution price; "
                f"exchange_order_id={resolved_exchange_order_id}; fill_id={invalid_fill.fill_id}"
            )
            record_status_transition(
                client_order_id,
                from_status="RECOVERY_REQUIRED",
                to_status="RECOVERY_REQUIRED",
                reason=reason,
                conn=conn,
            )
            set_status(client_order_id, "RECOVERY_REQUIRED", last_error=reason, conn=conn)
            conn.commit()
            return {
                "client_order_id": client_order_id,
                "exchange_order_id": resolved_exchange_order_id,
                "status": "RECOVERY_REQUIRED",
                "fill_count": len(fills),
                "applied_fill_count": 0,
                "blocked_reason": "invalid_fill_price",
            }

        fee_pending_fill = next((fill for fill in fills if not _fill_fee_is_accounting_complete(fill)), None)
        if fee_pending_fill is not None:
            _record_fee_pending_observations(
                conn,
                client_order_id=client_order_id,
                side=side,
                exchange_order_id=resolved_exchange_order_id,
                fills=fills,
                source="broker_known_backfill_fee_pending",
            )
            applied_fill_count = 0
            for fill in fills:
                if _fill_fee_is_accounting_complete(fill):
                    apply_fill_and_trade(
                        conn,
                        client_order_id=client_order_id,
                        side=side,
                        fill_id=fill.fill_id,
                        fill_ts=fill.fill_ts,
                        price=fill.price,
                        qty=fill.qty,
                        fee=fill.fee,
                        note=f"broker-known backfill exchange_order_id={resolved_exchange_order_id}",
                        allow_entry_decision_fallback=False,
                    )
                else:
                    apply_fill_principal_with_pending_fee(
                        conn,
                        client_order_id=client_order_id,
                        side=side,
                        fill_id=fill.fill_id,
                        fill_ts=fill.fill_ts,
                        price=fill.price,
                        qty=fill.qty,
                        fee=getattr(fill, "fee", None),
                        fee_status=getattr(fill, "fee_status", "unknown"),
                        fee_source=getattr(fill, "fee_source", None),
                        fee_confidence=getattr(fill, "fee_confidence", None),
                        fee_provenance=getattr(fill, "fee_provenance", None),
                        fee_validation_reason=getattr(fill, "fee_validation_reason", None),
                        fee_validation_checks=getattr(fill, "fee_validation_checks", None),
                        note=f"broker-known backfill exchange_order_id={resolved_exchange_order_id}",
                        allow_entry_decision_fallback=False,
                    )
                applied_fill_count += 1
            remote_status = str(remote.status or "").upper()
            if remote_status == "CANCELLED":
                remote_status = "CANCELED"
            if remote_status == "REJECTED":
                remote_status = "FAILED"
            set_status(client_order_id, remote_status, conn=conn)
            conn.commit()
            return {
                "client_order_id": client_order_id,
                "exchange_order_id": resolved_exchange_order_id,
                "status": remote_status,
                "fill_count": len(fills),
                "applied_fill_count": applied_fill_count,
                "blocked_reason": "principal_applied_fee_pending",
            }

        applied_fill_count = 0
        for fill in fills:
            apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee,
                note=f"broker-known backfill exchange_order_id={resolved_exchange_order_id}",
                allow_entry_decision_fallback=False,
            )
            applied_fill_count += 1

        remote_status = str(remote.status or "").upper()
        if remote_status == "CANCELLED":
            remote_status = "CANCELED"
        if remote_status == "REJECTED":
            remote_status = "FAILED"
        if remote_status == "FILLED" and applied_fill_count <= 0:
            reason = (
                "broker-known backfill blocked: terminal FILLED broker order has no recoverable fills; "
                f"exchange_order_id={resolved_exchange_order_id}"
            )
            record_status_transition(
                client_order_id,
                from_status="RECOVERY_REQUIRED",
                to_status="RECOVERY_REQUIRED",
                reason=reason,
                conn=conn,
            )
            set_status(client_order_id, "RECOVERY_REQUIRED", last_error=reason, conn=conn)
            conn.commit()
            return {
                "client_order_id": client_order_id,
                "exchange_order_id": resolved_exchange_order_id,
                "status": "RECOVERY_REQUIRED",
                "fill_count": len(fills),
                "applied_fill_count": applied_fill_count,
                "blocked_reason": "filled_without_fills",
            }

        if remote_status in LOCAL_RECONCILE_STATUSES:
            reason = (
                "broker-known backfill observed unresolved broker order; "
                f"exchange_order_id={resolved_exchange_order_id}; status={remote_status}; "
                "cancel/reconcile required before resume"
            )
            record_status_transition(
                client_order_id,
                from_status="RECOVERY_REQUIRED",
                to_status="RECOVERY_REQUIRED",
                reason=reason,
                conn=conn,
            )
            set_status(client_order_id, "RECOVERY_REQUIRED", last_error=reason, conn=conn)
            conn.commit()
            return {
                "client_order_id": client_order_id,
                "exchange_order_id": resolved_exchange_order_id,
                "status": "RECOVERY_REQUIRED",
                "fill_count": len(fills),
                "applied_fill_count": applied_fill_count,
                "blocked_reason": "remote_unresolved",
            }

        if remote_status not in {"FILLED", "CANCELED", "FAILED"}:
            reason = (
                "broker-known backfill blocked: broker order terminal status is unsupported; "
                f"exchange_order_id={resolved_exchange_order_id}; status={remote_status or '<missing>'}"
            )
            record_status_transition(
                client_order_id,
                from_status="RECOVERY_REQUIRED",
                to_status="RECOVERY_REQUIRED",
                reason=reason,
                conn=conn,
            )
            set_status(client_order_id, "RECOVERY_REQUIRED", last_error=reason, conn=conn)
            conn.commit()
            return {
                "client_order_id": client_order_id,
                "exchange_order_id": resolved_exchange_order_id,
                "status": "RECOVERY_REQUIRED",
                "fill_count": len(fills),
                "applied_fill_count": applied_fill_count,
                "blocked_reason": "unsupported_remote_status",
            }

        set_status(client_order_id, remote_status, conn=conn)
        conn.commit()
        return {
            "client_order_id": client_order_id,
            "exchange_order_id": resolved_exchange_order_id,
            "status": remote_status,
            "fill_count": len(fills),
            "applied_fill_count": applied_fill_count,
            "blocked_reason": "none",
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        runtime_state.refresh_open_order_health()


def cancel_open_orders_with_broker(broker: Broker) -> dict[str, int | list[str]]:
    conn = ensure_db()
    try:
        local_by_exchange_id: dict[str, str] = {}
        local_rows_by_client_order_id: dict[str, object] = {}
        placeholders = ",".join("?" for _ in sorted(UNRESOLVED_ORDER_STATUSES))
        rows = conn.execute(
            f"SELECT client_order_id, exchange_order_id, side, status FROM orders WHERE status IN ({placeholders})",
            tuple(sorted(UNRESOLVED_ORDER_STATUSES)),
        ).fetchall()
        for row in rows:
            local_id = str(row["client_order_id"])
            local_rows_by_client_order_id[local_id] = row
            if row["exchange_order_id"]:
                local_by_exchange_id[str(row["exchange_order_id"])] = local_id

        known_exchange_order_ids, known_client_order_ids = _known_identifier_sets(rows)
        remote_open = _get_open_orders_for_known_ids(
            broker,
            exchange_order_ids=known_exchange_order_ids,
            client_order_ids=known_client_order_ids,
        )
        if not remote_open:
            return {
                "remote_open_count": 0,
                "canceled_count": 0,
                "cancel_accepted_count": 0,
                "cancel_confirm_pending_count": 0,
                "matched_local_count": 0,
                "stray_canceled_count": 0,
                "failed_count": 0,
                "stray_messages": [],
                "error_messages": [],
            }

        canceled_count = 0
        cancel_accepted_count = 0
        cancel_confirm_pending_count = 0
        matched_local_count = 0
        stray_canceled_count = 0
        failed_count = 0
        stray_messages: list[str] = []
        error_messages: list[str] = []

        for remote in remote_open:
            remote_exchange_id = str(remote.exchange_order_id or "")
            remote_client_order_id = str(remote.client_order_id or "")
            local_id = local_by_exchange_id.get(remote_exchange_id)
            local_weak_mismatch_id: str | None = None
            if local_id is None and remote_client_order_id:
                candidate = local_rows_by_client_order_id.get(remote_client_order_id)
                if candidate is not None:
                    candidate_exchange = str(candidate["exchange_order_id"] or "")
                    if _strong_order_correlation(
                        local_client_order_id=str(candidate["client_order_id"]),
                        local_exchange_order_id=(candidate_exchange or None),
                        remote_client_order_id=remote_client_order_id,
                        remote_exchange_order_id=(remote_exchange_id or None),
                    ):
                        local_id = str(candidate["client_order_id"])
                    else:
                        local_weak_mismatch_id = str(candidate["client_order_id"])

            if local_weak_mismatch_id is not None:
                failed_count += 1
                error_messages.append(
                    "cancel skipped due to identifier mismatch: "
                    f"remote_exchange_order_id={remote_exchange_id or '<none>'} "
                    f"remote_client_order_id={remote_client_order_id or '<none>'} "
                    f"local_client_order_id={local_weak_mismatch_id}"
                )
                continue

            if local_id is None:
                stray_messages.append(
                    "cancel skipped for remote open order without local unresolved mapping: "
                    f"exchange_order_id={remote_exchange_id or '<none>'} "
                    f"client_order_id={remote_client_order_id or '<none>'}"
                )
                continue

            cancel_client_order_id = local_id or remote_client_order_id or f"remote_{remote_exchange_id or 'unknown'}"

            try:
                request_cancel = getattr(broker, "request_cancel_order", broker.cancel_order)
                cancel_result = request_cancel(
                    client_order_id=cancel_client_order_id,
                    exchange_order_id=remote.exchange_order_id,
                )
                cancel_accepted_count += 1
            except Exception as e:
                error_text = f"{type(e).__name__}: {e}"
                if "NOT_FOUND_NEEDS_RECONCILE" in error_text or "order not found" in error_text.lower():
                    try:
                        cancel_result = broker.get_order(
                            client_order_id=cancel_client_order_id,
                            exchange_order_id=(remote.exchange_order_id or None),
                        )
                        cancel_accepted_count += 1
                    except Exception as lookup_exc:
                        failed_count += 1
                        target = remote_exchange_id or cancel_client_order_id
                        error_messages.append(
                            f"failed to cancel {target}: {error_text}; lookup={type(lookup_exc).__name__}: {lookup_exc}"
                        )
                        current = conn.execute(
                            "SELECT status, side FROM orders WHERE client_order_id=?",
                            (local_id,),
                        ).fetchone()
                        from_status = str(current["status"]) if current and current["status"] else "UNKNOWN"
                        local_side = str(current["side"]) if current and current["side"] else str(remote.side)
                        _mark_recovery_required_with_reason(
                            conn,
                            client_order_id=local_id,
                            side=local_side,
                            from_status=from_status,
                            reason_code=RECONCILE_MISMATCH,
                            reason=(
                                "cancel not found needs interpretation and lookup failed; "
                                f"exchange_order_id={remote_exchange_id or '<none>'}; "
                                f"client_order_id={cancel_client_order_id}"
                            ),
                        )
                        continue
                failed_count += 1
                target = remote_exchange_id or cancel_client_order_id
                error_messages.append(f"failed to cancel {target}: {error_text}")
                continue

            final_status = str(cancel_result.status or "").strip()
            if final_status == CANCEL_REQUESTED_STATUS:
                try:
                    post_cancel = broker.get_order(
                        client_order_id=cancel_client_order_id,
                        exchange_order_id=(remote.exchange_order_id or None),
                    )
                    final_status = str(post_cancel.status or "").strip() or final_status
                except Exception as e:
                    final_status = CANCEL_REQUESTED_STATUS
                    error_messages.append(
                        "post-cancel confirmation lookup failed: "
                        f"exchange_order_id={remote_exchange_id or '<none>'} "
                        f"client_order_id={cancel_client_order_id} "
                        f"error={type(e).__name__}: {e}"
                    )
                    _LOG.warning(
                        "post-cancel confirmation lookup failed exchange_order_id=%s client_order_id=%s err=%s: %s",
                        remote_exchange_id or "-",
                        cancel_client_order_id,
                        type(e).__name__,
                        e,
                    )

            is_final_canceled = final_status == "CANCELED"
            is_final_filled = final_status == "FILLED"
            if is_final_canceled:
                canceled_count += 1
            elif final_status == CANCEL_REQUESTED_STATUS:
                cancel_confirm_pending_count += 1

            if local_id:
                if remote_exchange_id:
                    set_exchange_order_id(local_id, remote_exchange_id, conn=conn)
                if is_final_canceled:
                    set_status(local_id, "CANCELED", conn=conn)
                elif is_final_filled:
                    set_status(local_id, "FILLED", conn=conn)
                elif final_status == CANCEL_REQUESTED_STATUS:
                    set_status(local_id, CANCEL_REQUESTED_STATUS, conn=conn)
                else:
                    reason = (
                        "cancel accepted but final status unresolved; "
                        f"exchange_order_id={remote_exchange_id or '<none>'}; "
                        f"status={final_status or CANCEL_REQUESTED_STATUS}; manual recovery required"
                    )
                    current = conn.execute(
                        "SELECT status, side FROM orders WHERE client_order_id=?",
                        (local_id,),
                    ).fetchone()
                    from_status = str(current["status"]) if current and current["status"] else "UNKNOWN"
                    local_side = str(current["side"]) if current and current["side"] else str(remote.side)
                    _mark_recovery_required_with_reason(
                        conn,
                        client_order_id=local_id,
                        side=local_side,
                        from_status=from_status,
                        reason_code=RECONCILE_MISMATCH,
                        reason=reason,
                    )
                    failed_count += 1
                    error_messages.append(
                        "cancel final status unresolved for local order: "
                        f"client_order_id={local_id} exchange_order_id={remote_exchange_id or '<none>'} "
                        f"status={final_status or CANCEL_REQUESTED_STATUS}"
                    )
                matched_local_count += 1
            else:
                stray_messages.append(
                    "cancel skipped for remote open order without local unresolved mapping: "
                    f"exchange_order_id={remote_exchange_id or '<none>'} "
                    f"client_order_id={remote_client_order_id or '<none>'} "
                    f"status={final_status or CANCEL_REQUESTED_STATUS}"
                )

        conn.commit()
        return {
            "remote_open_count": len(remote_open),
            "canceled_count": canceled_count,
            "cancel_accepted_count": cancel_accepted_count,
            "cancel_confirm_pending_count": cancel_confirm_pending_count,
            "matched_local_count": matched_local_count,
            "stray_canceled_count": stray_canceled_count,
            "failed_count": failed_count,
            "stray_messages": stray_messages,
            "error_messages": error_messages,
        }
    finally:
        conn.close()
