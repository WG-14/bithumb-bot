from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from enum import Enum

from .broker.base import Broker, BrokerFill, BrokerOrder
from .db_core import ensure_db, get_portfolio_breakdown, init_portfolio, set_portfolio_breakdown
from .execution import apply_fill_and_trade, order_fill_tolerance, record_order_if_missing
from .oms import get_open_orders, record_status_transition, set_exchange_order_id, set_status, validate_status_transition
from . import runtime_state
from .notifier import format_event, notify
from .observability import safety_event
from .reason_codes import AMBIGUOUS_RECENT_FILL, AMBIGUOUS_SUBMIT, RECONCILE_MISMATCH, WEAK_ORDER_CORRELATION


LOCAL_RECONCILE_STATUSES = ("PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN")

REASON_REMOTE_OPEN_ORDER_FOUND = "REMOTE_OPEN_ORDER_FOUND"
REASON_RECENT_FILL_APPLIED = "RECENT_FILL_APPLIED"
REASON_SUBMIT_UNKNOWN_UNRESOLVED = "SUBMIT_UNKNOWN_UNRESOLVED"
REASON_STARTUP_GATE_BLOCKED = "STARTUP_GATE_BLOCKED"
REASON_SOURCE_CONFLICT_HALT = "SOURCE_CONFLICT_HALT"
REASON_RECONCILE_OK = "RECONCILE_OK"
REASON_RECONCILE_FAILED = "RECONCILE_FAILED"
REASON_RECENT_FILL_INVALID_PRICE = "RECENT_FILL_INVALID_PRICE"

OPEN_ORDER_TRUSTED_STATUSES = {"PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN"}
UNRESOLVED_ORDER_STATUSES = {"PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN", "RECOVERY_REQUIRED"}
NON_CLEARING_RECONCILE_REASON_CODES = {
    REASON_RECONCILE_FAILED,
    REASON_SOURCE_CONFLICT_HALT,
    REASON_STARTUP_GATE_BLOCKED,
    REASON_SUBMIT_UNKNOWN_UNRESOLVED,
}
CANCEL_REQUESTED_STATUS = "CANCEL_REQUESTED"


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


def load_recent_order_lifecycle(conn, *, limit: int = 5) -> list[dict[str, str | int]]:
    rows = conn.execute(
        """
        SELECT client_order_id, submit_attempt_id, exchange_order_id, status, side, qty_req, created_ts
        FROM orders
        ORDER BY created_ts DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()

    lifecycle: list[dict[str, str | int]] = []
    for row in rows:
        context = _load_submit_attempt_context(conn, row=row)
        submit_attempt_id = str(context.get("submit_attempt_id") or "")
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

    if local_exchange_order_id:
        return False

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

CASH_SPLIT_ABS_TOL = 1e-6
ASSET_SPLIT_ABS_TOL = 1e-10
_LOG = logging.getLogger(__name__)


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
) -> tuple[bool, list[str], int]:
    local_rows = conn.execute(
        "SELECT client_order_id, exchange_order_id, side, status, qty_req, qty_filled FROM orders"
    ).fetchall()
    by_exchange_id = {str(r["exchange_order_id"]): r for r in local_rows if r["exchange_order_id"]}
    by_client_order_id = {str(r["client_order_id"]): r for r in local_rows}

    applied = False
    conflicts: list[str] = []
    blocked_invalid_price = 0

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
        )
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

    return applied, conflicts, blocked_invalid_price


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
    matched_fills: tuple[BrokerFill, ...]
    has_partial_fill_evidence: bool


def _interpret_submit_unknown_recent_activity(
    *,
    local_row,
    submit_attempt_context: dict[str, str | float | bool],
    recent_orders: list[BrokerOrder],
    recent_fills: list[BrokerFill],
) -> _SubmitUnknownRecentActivityInterpretation:
    candidate_orders: dict[str, BrokerOrder] = {}
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
        if not remote_exchange_order_id:
            continue
        candidate_orders[remote_exchange_order_id] = remote

    candidate_count = len(candidate_orders)
    if candidate_count != 1:
        return _SubmitUnknownRecentActivityInterpretation(
            outcome=("ambiguous" if candidate_count > 1 else "insufficient_evidence"),
            candidate_count=candidate_count,
            matched_exchange_order_id=None,
            matched_order=None,
            matched_fills=(),
            has_partial_fill_evidence=False,
        )

    matched_exchange_order_id = next(iter(candidate_orders.keys()))
    matched_order = candidate_orders[matched_exchange_order_id]
    matched_fills: list[BrokerFill] = []
    for fill in recent_fills:
        remote_client_order_id = str(fill.client_order_id or "")
        remote_exchange_order_id = str(fill.exchange_order_id or "")
        if remote_exchange_order_id and remote_exchange_order_id != matched_exchange_order_id:
            continue
        if not _strong_submit_unknown_correlation(
            local_row=local_row,
            submit_attempt_context=submit_attempt_context,
            remote_client_order_id=remote_client_order_id or None,
            remote_exchange_order_id=remote_exchange_order_id or None,
            remote_side=str(local_row["side"]),
            remote_qty=None,
        ):
            continue
        matched_fills.append(fill)

    total_fill_qty = sum(max(0.0, float(fill.qty)) for fill in matched_fills)
    local_qty_req = max(0.0, float(local_row["qty_req"]))
    has_partial_fill_evidence = bool(total_fill_qty > 1e-12 and local_qty_req > total_fill_qty + 1e-12)

    return _SubmitUnknownRecentActivityInterpretation(
        outcome="success",
        candidate_count=1,
        matched_exchange_order_id=matched_exchange_order_id,
        matched_order=matched_order,
        matched_fills=tuple(matched_fills),
        has_partial_fill_evidence=has_partial_fill_evidence,
    )


def _try_resolve_submit_unknown_from_recent_activity(
    conn,
    *,
    row,
    recent_orders: list[BrokerOrder],
    recent_fills: list[BrokerFill],
) -> tuple[bool, bool]:
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
        )
        return False, False

    matched_exchange_order_id = interpretation.matched_exchange_order_id
    matched_order = interpretation.matched_order
    _record_submit_unknown_autolink_event(
        conn,
        client_order_id=client_order_id,
        side=side,
        submit_attempt_context=submit_attempt_context,
        outcome="success",
        candidate_count=1,
        exchange_order_id=matched_exchange_order_id,
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
        )
        applied_fill = True

    prev_status = str(row["status"])
    next_status = prev_status
    if matched_order is not None:
        next_status = matched_order.status
    elif applied_fill:
        order_row = conn.execute(
            "SELECT status, qty_req, qty_filled FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if order_row is not None:
            reconciled_status = _status_after_recent_fill_replay(
                current_status=str(order_row["status"]),
                qty_req=float(order_row["qty_req"]),
                qty_filled=float(order_row["qty_filled"]),
            )
            if reconciled_status is not None:
                next_status = reconciled_status

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

    return True, applied_fill

def _record_submit_unknown_autolink_event(
    conn,
    *,
    client_order_id: str,
    side: str,
    submit_attempt_context: dict[str, str | float | bool],
    outcome: str,
    candidate_count: int,
    exchange_order_id: str | None,
) -> None:
    submit_attempt_id = str(submit_attempt_context.get("submit_attempt_id") or "")
    event_message = format_event(
        "reconcile_submit_unknown_autolink",
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        side=side,
        outcome=outcome,
        candidate_count=max(0, int(candidate_count)),
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
        try:
            return broker.get_recent_orders(limit=limit)
        except TypeError:
            return broker.get_recent_orders()


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
        return broker.get_open_orders()


def _get_recent_fills_for_known_orders(
    broker: Broker,
    *,
    exchange_order_ids: list[str],
    client_order_ids_without_exchange_id: list[str],
) -> list[BrokerFill]:
    fills_by_id: dict[str, BrokerFill] = {}
    for exchange_order_id in exchange_order_ids:
        for fill in broker.get_fills(exchange_order_id=exchange_order_id):
            fills_by_id[str(fill.fill_id)] = fill
    for client_order_id in client_order_ids_without_exchange_id:
        for fill in broker.get_fills(client_order_id=client_order_id):
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
            SUM(CASE WHEN status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN') THEN 1 ELSE 0 END) AS unresolved_count,
            SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END) AS recovery_required_count
        FROM orders
        """
    ).fetchone()
    portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
    unresolved_count = int(unresolved_row["unresolved_count"] or 0) if unresolved_row else 0
    recovery_required_count = int(unresolved_row["recovery_required_count"] or 0) if unresolved_row else 0
    position_flat = abs(float(portfolio_row["asset_qty"] or 0.0)) <= 1e-12 if portfolio_row else True
    _LOG.info(
        "reconcile_exposure_decision unresolved_count=%s recovery_required_count=%s broker_open_order_count=%s position_flat=%s halt_reason_code=%s",
        unresolved_count,
        recovery_required_count,
        broker_open_order_count,
        int(position_flat),
        state.halt_reason_code or "-",
    )
    if not (
        unresolved_count == 0
        and recovery_required_count == 0
        and broker_open_order_count == 0
        and position_flat
    ):
        _LOG.info(
            "reconcile_halt_retained reason=safety_blockers_remaining unresolved_count=%s recovery_required_count=%s broker_open_order_count=%s position_flat=%s",
            unresolved_count,
            recovery_required_count,
            broker_open_order_count,
            int(position_flat),
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
    }
    try:
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
        for row in local_open:
            oid = row["client_order_id"]
            if row["status"] == "SUBMIT_UNKNOWN" and not row["exchange_order_id"]:
                recovered, applied_fill = _try_resolve_submit_unknown_from_recent_activity(
                    conn,
                    row=row,
                    recent_orders=recent_orders,
                    recent_fills=recent_fills,
                )
                if recovered:
                    if applied_fill:
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

            remote = broker.get_order(client_order_id=oid, exchange_order_id=row["exchange_order_id"])
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
            fills = broker.get_fills(client_order_id=oid, exchange_order_id=remote.exchange_order_id)
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
            for fill in fills:
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

        conflicts = _sync_recent_order_activity(
            conn,
            recent_orders,
            trusted_open_exchange_ids=trusted_open_exchange_ids,
        )
        applied_recent_fill, fill_conflicts, blocked_recent_fill_price = _apply_recent_fills(
            conn,
            recent_fills,
            trusted_open_exchange_ids=trusted_open_exchange_ids,
        )
        conflicts.extend(fill_conflicts)
        metadata["invalid_fill_price_blocked"] += int(blocked_recent_fill_price)

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

        bal = broker.get_balance()
        local_cash_available, local_cash_locked, local_asset_available, local_asset_locked = get_portfolio_breakdown(conn)
        has_open_orders = bool(local_open) or bool(remote_open)

        cash_locked = float(bal.cash_locked)
        asset_locked = float(bal.asset_locked)
        if has_open_orders and cash_locked <= 1e-12 and local_cash_locked > 1e-12:
            cash_locked = local_cash_locked
        if has_open_orders and asset_locked <= 1e-12 and local_asset_locked > 1e-12:
            asset_locked = local_asset_locked

        mismatch_count, mismatch_summary = _balance_split_mismatch_summary(
            broker_cash_available=float(bal.cash_available),
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

        set_portfolio_breakdown(
            conn,
            cash_available=bal.cash_available,
            cash_locked=cash_locked,
            asset_available=bal.asset_available,
            asset_locked=asset_locked,
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

        fills = broker.get_fills(
            client_order_id=client_order_id,
            exchange_order_id=resolved_exchange_order_id,
        )
        invalid_fill = next((fill for fill in fills if float(fill.price) <= 0), None)
        if invalid_fill is not None:
            raise RuntimeError(
                "manual recovery blocked: fill has missing/invalid execution price; "
                f"exchange_order_id={resolved_exchange_order_id}; fill_id={invalid_fill.fill_id}"
            )
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


def cancel_open_orders_with_broker(broker: Broker) -> dict[str, int | list[str]]:
    conn = ensure_db()
    try:
        remote_open = broker.get_open_orders()
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

        local_by_exchange_id: dict[str, str] = {}
        local_rows_by_client_order_id: dict[str, object] = {}
        rows = conn.execute(
            "SELECT client_order_id, exchange_order_id, side, status FROM orders"
        ).fetchall()
        for row in rows:
            local_id = str(row["client_order_id"])
            local_rows_by_client_order_id[local_id] = row
            if row["exchange_order_id"]:
                local_by_exchange_id[str(row["exchange_order_id"])] = local_id

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

            cancel_client_order_id = local_id or remote_client_order_id or f"remote_{remote_exchange_id or 'unknown'}"

            try:
                cancel_result = broker.cancel_order(
                    client_order_id=cancel_client_order_id,
                    exchange_order_id=remote.exchange_order_id,
                )
                cancel_accepted_count += 1
            except Exception as e:
                failed_count += 1
                target = remote_exchange_id or cancel_client_order_id
                error_messages.append(f"failed to cancel {target}: {type(e).__name__}: {e}")
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
                if is_final_canceled:
                    stray_canceled_count += 1
                    stray_messages.append(
                        f"stray remote order canceled exchange_order_id={remote_exchange_id or '<none>'} side={remote.side} qty={remote.qty_req}"
                    )
                else:
                    stray_messages.append(
                        f"stray remote cancel accepted but unconfirmed exchange_order_id={remote_exchange_id or '<none>'} status={final_status or CANCEL_REQUESTED_STATUS}"
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
