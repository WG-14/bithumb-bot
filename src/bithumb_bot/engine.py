from __future__ import annotations

import math
import time
import json
from dataclasses import dataclass

from .config import settings, validate_live_mode_preflight
from .marketdata import cmd_sync
from .strategy.sma import compute_signal
from .broker.paper import paper_execute
from .broker.live import live_execute_signal
from .broker.bithumb import BithumbBroker
from .broker.base import BrokerError
from .db_core import ensure_db
from .utils_time import kst_str, parse_interval_sec
from .notifier import format_event, notify
from .observability import safety_event
from .reason_codes import CANCEL_FAILURE, POSITION_LOSS_LIMIT, RISKY_ORDER_BLOCK, STARTUP_BLOCKED
from . import runtime_state
from .risk import evaluate_daily_loss_breach, evaluate_position_loss_breach
from .oms import collect_risky_order_state
from .flatten import flatten_btc_position


FAILSAFE_RETRY_DELAY_SEC = 180
STARTUP_RECOVERY_GATE_PREFIX = "startup safety gate"


@dataclass(frozen=True)
class HaltReason:
    code: str
    detail: str


@dataclass(frozen=True)
class ResumeBlocker:
    code: str
    detail: str
    overridable: bool


def _halt_reason(code: str, detail: str) -> HaltReason:
    return HaltReason(code=code, detail=detail)


def _resume_blocker(*, code: str, detail: str, overridable: bool) -> ResumeBlocker:
    return ResumeBlocker(code=code, detail=detail, overridable=overridable)

LIVE_UNRESOLVED_ORDER_STATUSES = (
    "PENDING_SUBMIT",
    "NEW",
    "PARTIAL",
    "SUBMIT_UNKNOWN",
    "RECOVERY_REQUIRED",
)

RISK_EXPOSURE_HALT_REASON_CODES = {
    "KILL_SWITCH",
    "DAILY_LOSS_LIMIT",
    POSITION_LOSS_LIMIT,
}


def _get_open_order_snapshot(now_ms: int) -> tuple[int, float | None]:
    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in LIVE_UNRESOLVED_ORDER_STATUSES)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS open_count, MIN(created_ts) AS oldest_created_ts
            FROM orders
            WHERE status IN ({placeholders})
            """,
            LIVE_UNRESOLVED_ORDER_STATUSES,
        ).fetchone()
        open_count = int(row["open_count"])
        oldest_created_ts = (
            int(row["oldest_created_ts"])
            if row["oldest_created_ts"] is not None
            else None
        )
        if open_count <= 0 or oldest_created_ts is None:
            return 0, None
        age_sec = max(0.0, (now_ms - oldest_created_ts) / 1000)
        return open_count, age_sec
    finally:
        conn.close()


def _get_exposure_snapshot(now_ms: int) -> tuple[bool, bool]:
    open_count, _ = _get_open_order_snapshot(now_ms)
    conn = ensure_db()
    try:
        portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
    finally:
        conn.close()

    asset_qty = float(portfolio_row["asset_qty"] if portfolio_row is not None else 0.0)
    return open_count > 0, asset_qty > 1e-12


def _mark_open_orders_recovery_required(reason: str, now_ms: int) -> int:
    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in LIVE_UNRESOLVED_ORDER_STATUSES)
        res = conn.execute(
            f"""
            UPDATE orders
            SET status='RECOVERY_REQUIRED', updated_ts=?, last_error=?
            WHERE status IN ({placeholders})
            """,
            (now_ms, reason, *LIVE_UNRESOLVED_ORDER_STATUSES),
        )
        conn.commit()
        return int(res.rowcount or 0)
    finally:
        conn.close()


def get_health_status() -> dict[str, float | int | bool | str | None]:
    state = runtime_state.snapshot()
    return {
        "last_candle_age_sec": state.last_candle_age_sec,
        "error_count": state.error_count,
        "trading_enabled": state.trading_enabled,
        "retry_at_epoch_sec": state.retry_at_epoch_sec,
        "last_disable_reason": state.last_disable_reason,
        "halt_new_orders_blocked": state.halt_new_orders_blocked,
        "halt_reason_code": state.halt_reason_code,
        "halt_state_unresolved": state.halt_state_unresolved,
        "halt_policy_stage": state.halt_policy_stage,
        "halt_policy_block_new_orders": state.halt_policy_block_new_orders,
        "halt_policy_attempt_cancel_open_orders": state.halt_policy_attempt_cancel_open_orders,
        "halt_policy_auto_liquidate_positions": state.halt_policy_auto_liquidate_positions,
        "halt_position_present": state.halt_position_present,
        "halt_open_orders_present": state.halt_open_orders_present,
        "halt_operator_action_required": state.halt_operator_action_required,
        "unresolved_open_order_count": state.unresolved_open_order_count,
        "oldest_unresolved_order_age_sec": state.oldest_unresolved_order_age_sec,
        "recovery_required_count": state.recovery_required_count,
        "last_reconcile_epoch_sec": state.last_reconcile_epoch_sec,
        "last_reconcile_status": state.last_reconcile_status,
        "last_reconcile_error": state.last_reconcile_error,
        "last_reconcile_reason_code": state.last_reconcile_reason_code,
        "last_reconcile_metadata": state.last_reconcile_metadata,
        "last_cancel_open_orders_epoch_sec": state.last_cancel_open_orders_epoch_sec,
        "last_cancel_open_orders_trigger": state.last_cancel_open_orders_trigger,
        "last_cancel_open_orders_status": state.last_cancel_open_orders_status,
        "last_cancel_open_orders_summary": state.last_cancel_open_orders_summary,
        "startup_gate_reason": state.startup_gate_reason,
        "resume_gate_blocked": state.resume_gate_blocked,
        "resume_gate_reason": state.resume_gate_reason,
    }



def evaluate_startup_safety_gate() -> str | None:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()
    now_ms = int(time.time() * 1000)

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
            "submit_unknown": int(row["submit_unknown_count"] or 0),
            "recovery_required": int(row["recovery_required_count"] or 0),
            "stale_new_partial": int(row["stale_new_partial_count"] or 0),
        }

    reasons: list[str] = []
    if status_counts["pending_submit"] > 0:
        reasons.append(f"pending_submit_orders={status_counts['pending_submit']}")
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

    submit_unknown_without_exchange_count = int(risky_state["submit_unknown_without_exchange_id_count"])
    if submit_unknown_without_exchange_count > 0:
        reasons.append(
            "submit_unknown_without_exchange_id="
            f"{submit_unknown_without_exchange_count}"
        )

    stray_remote_open_count = int(risky_state["stray_remote_open_order_count"])
    if stray_remote_open_count > 0:
        reasons.append(f"stray_remote_open_orders={stray_remote_open_count}")

    if not reasons:
        runtime_state.set_startup_gate_reason(None)
        return None

    reason = f"{STARTUP_RECOVERY_GATE_PREFIX}: " + ", ".join(reasons)
    runtime_state.set_startup_gate_reason(reason)
    return reason


def evaluate_resume_eligibility() -> tuple[bool, list[ResumeBlocker]]:
    """Returns whether operator resume may proceed and structured blockers."""
    startup_gate_reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()

    reasons: list[ResumeBlocker] = []
    if state.last_reconcile_status == "error":
        reasons.append(
            _resume_blocker(
                code="LAST_RECONCILE_FAILED",
                detail=(
                    "last reconcile failed: "
                    f"reason_code={state.last_reconcile_reason_code or '-'} "
                    f"error={state.last_reconcile_error or '-'}"
                ),
                overridable=False,
            )
        )

    if startup_gate_reason:
        reasons.append(
            _resume_blocker(
                code="STARTUP_SAFETY_GATE_BLOCKED",
                detail=startup_gate_reason,
                overridable=False,
            )
        )
        if state.last_reconcile_status == "ok":
            reasons.append(
                _resume_blocker(
                    code="LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS",
                    detail="latest reconcile reported ok but startup safety gate still blocks resume",
                    overridable=False,
                )
            )

    if state.halt_state_unresolved:
        reasons.append(
            _resume_blocker(
                code="HALT_STATE_UNRESOLVED",
                detail=f"halt unresolved: code={state.halt_reason_code or '-'} reason={state.last_disable_reason or '-'}",
                overridable=False,
            )
        )

    if state.halt_new_orders_blocked:
        open_orders_present, position_present = _get_exposure_snapshot(int(time.time() * 1000))
        is_risk_exposure_halt = (state.halt_reason_code or "") in RISK_EXPOSURE_HALT_REASON_CODES
        if open_orders_present or position_present:
            detail = (
                "halt blocked with open exposure: "
                f"position_present={1 if position_present else 0} "
                f"open_orders_present={1 if open_orders_present else 0} "
                f"reason_code={state.halt_reason_code or '-'} "
                f"reason={state.last_disable_reason or '-'}"
            )
            if is_risk_exposure_halt:
                detail = (
                    "risk halt resume rejected until exposure is flattened/resolved first; "
                    + detail
                )
            reasons.append(
                _resume_blocker(
                    code="HALT_RISK_OPEN_POSITION",
                    detail=detail,
                    overridable=False,
                )
            )

    if settings.MODE == "live" and state.last_reconcile_metadata:
        try:
            reconcile_meta = json.loads(str(state.last_reconcile_metadata))
        except json.JSONDecodeError:
            reconcile_meta = {}
        mismatch_raw = reconcile_meta.get("balance_split_mismatch_count", 0)
        try:
            mismatch_count = max(0, int(mismatch_raw))
        except (TypeError, ValueError):
            mismatch_count = 0
        if mismatch_count > 0:
            mismatch_summary = str(reconcile_meta.get("balance_split_mismatch_summary") or "-")
            reasons.append(
                _resume_blocker(
                    code="BALANCE_SPLIT_MISMATCH",
                    detail=f"balance split mismatch detected after reconcile: count={mismatch_count} summary={mismatch_summary}",
                    overridable=False,
                )
            )

    gate_reason = None
    if reasons:
        gate_reason = "; ".join(f"{blocker.code}:{blocker.detail}" for blocker in reasons)
    runtime_state.set_resume_gate(blocked=bool(reasons), reason=gate_reason)
    return (len(reasons) == 0), reasons

def _halt_trading(reason: HaltReason, *, unresolved: bool = False) -> None:
    runtime_state.enter_halt(
        reason_code=reason.code,
        reason=reason.detail,
        unresolved=unresolved,
    )
    halt_state = runtime_state.snapshot()
    _, resume_blockers = evaluate_resume_eligibility()
    force_resume_allowed = bool(resume_blockers) and all(bool(b.overridable) for b in resume_blockers)
    primary_blocker_code = resume_blockers[0].code if resume_blockers else "-"
    blocker_summary = (
        f"total={len(resume_blockers)} "
        f"non_overridable={sum(1 for b in resume_blockers if not bool(b.overridable))} "
        f"overridable={sum(1 for b in resume_blockers if bool(b.overridable))}"
    )
    latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
    operator_action_required = bool(halt_state.halt_operator_action_required)
    open_order_count = _count_open_orders()
    position_summary = _position_summary()
    recommended_commands = _recommended_operator_commands(
        reason_code=reason.code,
        startup_gate=False,
        recovery_required=False,
        unresolved_count=int(halt_state.unresolved_open_order_count),
    )
    operator_next_action = _format_operator_next_action(
        reason_code=reason.code,
        unresolved=unresolved,
        operator_action_required=operator_action_required,
        open_orders_present=bool(halt_state.halt_open_orders_present),
        position_present=bool(halt_state.halt_position_present),
    )
    notify(
        format_event(
            "trading_halted",
            status="HALTED",
            severity="CRITICAL",
            alert_kind="halt",
            symbol=settings.PAIR,
            reason=reason.detail,
            reason_code=reason.code,
            unresolved=int(unresolved),
            unresolved_order_count=halt_state.unresolved_open_order_count,
            position_may_remain=int(halt_state.halt_position_present),
            latest_client_order_id=latest_client_order_id,
            latest_exchange_order_id=latest_exchange_order_id,
            operator_action_required=int(operator_action_required),
            operator_next_action=operator_next_action,
            operator_hint_command=_operator_hint_command(reason.code),
            primary_blocker_code=primary_blocker_code,
            blocker_summary=blocker_summary,
            force_resume_allowed=int(force_resume_allowed),
            halt_policy_stage=halt_state.halt_policy_stage,
            block_new_orders=int(halt_state.halt_policy_block_new_orders),
            attempt_cancel_open_orders=int(halt_state.halt_policy_attempt_cancel_open_orders),
            auto_liquidate_positions=int(halt_state.halt_policy_auto_liquidate_positions),
            halt_position_present=int(halt_state.halt_position_present),
            halt_open_orders_present=int(halt_state.halt_open_orders_present),
            open_order_count=open_order_count,
            position_summary=position_summary,
            operator_recommended_commands=" | ".join(recommended_commands),
            operator_compact_summary=_operator_compact_summary(
                halt_reason=reason.code,
                unresolved_order_count=int(halt_state.unresolved_open_order_count),
                open_order_count=open_order_count,
                position_summary=position_summary,
                recommended_commands=recommended_commands,
            ),
        )
    )




def _format_operator_next_action(*, reason_code: str, unresolved: bool, operator_action_required: bool, open_orders_present: bool, position_present: bool) -> str:
    if reason_code in {"DAILY_LOSS_LIMIT", POSITION_LOSS_LIMIT}:
        return "review risk breach details, verify exposure, then run recovery-report"
    if "RECONCILE" in reason_code:
        return "run reconcile, validate order state, then run recovery-report before resume"
    if operator_action_required or unresolved:
        if open_orders_present or position_present:
            return "operator must review open exposure and reconcile before resume"
        return "operator must review halt reason and run safe resume checks"
    return "no immediate operator action required"


def _operator_hint_command(reason_code: str) -> str:
    if "RECONCILE" in reason_code:
        return "uv run python bot.py reconcile && uv run python bot.py recovery-report"
    return "uv run python bot.py recovery-report"


def _latest_order_identifiers() -> tuple[str | None, str | None]:
    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT client_order_id, exchange_order_id
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'RECOVERY_REQUIRED')
            ORDER BY updated_ts DESC, created_ts DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None, None
        return row['client_order_id'], row['exchange_order_id']
    finally:
        conn.close()


def _count_open_orders() -> int:
    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in LIVE_UNRESOLVED_ORDER_STATUSES)
        row = conn.execute(
            f"SELECT COUNT(*) AS open_order_count FROM orders WHERE status IN ({placeholders})",
            LIVE_UNRESOLVED_ORDER_STATUSES,
        ).fetchone()
        return int(row["open_order_count"] or 0) if row is not None else 0
    finally:
        conn.close()


def _position_summary() -> str:
    conn = ensure_db()
    try:
        row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
    finally:
        conn.close()

    qty = float(row["asset_qty"] or 0.0) if row is not None else 0.0
    if abs(qty) <= 1e-12:
        return "flat"
    return f"long_qty={qty:.8f}"


def _recommended_operator_commands(
    *,
    reason_code: str,
    startup_gate: bool,
    recovery_required: bool,
    unresolved_count: int,
) -> list[str]:
    if startup_gate:
        return [
            "uv run python bot.py reconcile",
            "uv run python bot.py recovery-report",
        ]
    if recovery_required:
        return [
            "uv run python bot.py recover-order --client-order-id <id>",
            "uv run python bot.py recovery-report",
        ]
    if reason_code == "KILL_SWITCH":
        return [
            "uv run python bot.py recovery-report",
            "uv run python bot.py resume",
        ]
    if unresolved_count > 0:
        return ["uv run python bot.py recovery-report"]
    return ["uv run python bot.py resume"]


def _operator_compact_summary(
    *,
    halt_reason: str,
    unresolved_order_count: int,
    open_order_count: int,
    position_summary: str,
    recommended_commands: list[str],
) -> str:
    return (
        f"halt_reason={halt_reason} "
        f"unresolved_order_count={unresolved_order_count} "
        f"open_order_count={open_order_count} "
        f"position={position_summary} "
        f"next={' | '.join(recommended_commands)}"
    )


def _attempt_open_order_cancellation(broker: BithumbBroker, trigger: str) -> bool:
    from .recovery import cancel_open_orders_with_broker

    try:
        summary = cancel_open_orders_with_broker(broker)
    except Exception as e:
        reason_code = "CANCEL_OPEN_ORDERS_ERROR"
        runtime_state.record_cancel_open_orders_result(
            trigger=trigger,
            status="error",
            summary={"error": f"{type(e).__name__}: {e}"},
        )
        notify(
            safety_event(
                "cancel_open_orders_failed",
                alert_kind="cancel_failure",
                trigger=trigger,
                reason_code=CANCEL_FAILURE,
                cancel_detail_code=reason_code,
                error_type=type(e).__name__,
                reason=str(e),
            )
        )
        return False

    remote_open_count = int(summary["remote_open_count"])
    canceled_count = int(summary["canceled_count"])
    failed_count = int(summary["failed_count"])
    notify(
        format_event(
            "cancel_open_orders_result",
            trigger=trigger,
            remote_open_count=remote_open_count,
            canceled_count=canceled_count,
            failed_count=failed_count,
            status="partial" if failed_count > 0 else "ok",
        )
    )

    for message in summary["stray_messages"]:
        notify(message)
    for message in summary["error_messages"]:
        notify(message)

    status = "partial" if failed_count > 0 else "ok"
    runtime_state.record_cancel_open_orders_result(trigger=trigger, status=status, summary=summary)

    if failed_count > 0:
        notify(
            safety_event(
                "cancel_open_orders_failed",
                alert_kind="cancel_failure",
                trigger=trigger,
                reason_code=CANCEL_FAILURE,
                cancel_detail_code="CANCEL_OPEN_ORDERS_INCOMPLETE",
                failed_count=failed_count,
            )
        )
        return False
    return True


def _attempt_risk_breach_flatten(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
) -> tuple[HaltReason, bool, bool]:
    canceled_ok = _attempt_open_order_cancellation(broker, trigger=cancel_trigger)
    flatten_outcome = flatten_btc_position(
        broker=broker,
        dry_run=bool(settings.LIVE_DRY_RUN),
        trigger=flatten_trigger,
    )
    flatten_status = str(flatten_outcome.get("status") or "-")
    detail_parts = [
        reason_detail,
        (
            "emergency cancellation attempted"
            if canceled_ok
            else "emergency cancellation failed"
        ),
        f"flatten_status={flatten_status}",
    ]
    flatten_failed = flatten_status == "failed"
    if flatten_failed:
        detail_parts.append(f"flatten_error={str(flatten_outcome.get('error') or '-')}")
    return _halt_reason(reason_code, "; ".join(detail_parts)), canceled_ok, flatten_failed


def run_loop(short_n: int, long_n: int) -> None:
    from .recovery import reconcile_with_broker

    validate_live_mode_preflight(settings)

    state = runtime_state.snapshot()
    if state.halt_new_orders_blocked:
        reason = state.last_disable_reason or "persisted halt state requires explicit operator resume"
        reason_code = state.halt_reason_code or "PERSISTED_HALT_STATE"
        latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
        _, resume_blockers = evaluate_resume_eligibility()
        force_resume_allowed = bool(resume_blockers) and all(bool(b.overridable) for b in resume_blockers)
        primary_blocker_code = resume_blockers[0].code if resume_blockers else "-"
        notify(
            format_event(
                "startup_halt_state_blocked",
                alert_kind="startup_gate",
                symbol=settings.PAIR,
                reason_code=reason_code,
                reason=reason,
                unresolved_order_count=state.unresolved_open_order_count,
                position_may_remain=int(state.halt_position_present),
                latest_client_order_id=latest_client_order_id,
                latest_exchange_order_id=latest_exchange_order_id,
                operator_action_required=int(state.halt_operator_action_required),
                operator_next_action=_format_operator_next_action(
                    reason_code=reason_code,
                    unresolved=bool(state.halt_state_unresolved),
                    operator_action_required=bool(state.halt_operator_action_required),
                    open_orders_present=bool(state.halt_open_orders_present),
                    position_present=bool(state.halt_position_present),
                ),
                primary_blocker_code=primary_blocker_code,
                force_resume_allowed=int(force_resume_allowed),
                operator_hint_command=(
                    "uv run python bot.py resume --force"
                    if force_resume_allowed
                    else "uv run python bot.py recovery-report"
                ),
            )
        )
        print("[RUN] persisted runtime halt is active. refusing to enter trading loop.")
        return

    broker = None
    if settings.MODE == "live":
        broker = BithumbBroker()
        try:
            reconcile_with_broker(broker)
        except Exception as e:
            _halt_trading(_halt_reason("INITIAL_RECONCILE_FAILED", f"initial reconcile failed ({type(e).__name__}): {e}"), unresolved=True)
            return

        startup_gate_reason = evaluate_startup_safety_gate()
        if startup_gate_reason is not None:
            latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
            startup_open_order_count = _count_open_orders()
            startup_position_summary = _position_summary()
            startup_commands = _recommended_operator_commands(
                reason_code="STARTUP_SAFETY_GATE",
                startup_gate=True,
                recovery_required=(state.recovery_required_count > 0),
                unresolved_count=int(state.unresolved_open_order_count),
            )
            notify(
                safety_event(
                    "startup_gate_blocked",
                    alert_kind="startup_gate",
                    reason_code=STARTUP_BLOCKED,
                    reason=startup_gate_reason,
                    unresolved_order_count=state.unresolved_open_order_count,
                    position_may_remain=int(state.halt_position_present),
                    latest_client_order_id=latest_client_order_id,
                    latest_exchange_order_id=latest_exchange_order_id,
                    operator_action_required=1,
                    operator_next_action="operator must reconcile unresolved orders before startup",
                    open_order_count=startup_open_order_count,
                    position_summary=startup_position_summary,
                    operator_recommended_commands=" | ".join(startup_commands),
                    operator_compact_summary=_operator_compact_summary(
                        halt_reason="STARTUP_SAFETY_GATE",
                        unresolved_order_count=int(state.unresolved_open_order_count),
                        open_order_count=startup_open_order_count,
                        position_summary=startup_position_summary,
                        recommended_commands=startup_commands,
                    ),
                    state_to="HALTED",
                )
            )
            _halt_trading(_halt_reason("STARTUP_SAFETY_GATE", startup_gate_reason), unresolved=True)
            return

    sec = parse_interval_sec(settings.INTERVAL)
    print(
        f"[RUN] MODE={settings.MODE} PAIR={settings.PAIR} "
        f"INTERVAL={settings.INTERVAL} (every {sec}s) short={short_n} long={long_n}"
    )
    print("중지: Ctrl+C")
    fail_count = 0
    MAX_FAILS = 5
    last_open_order_reconcile_at: float | None = None

    try:
        while True:
            tick_now = time.time()
            sleep_s = sec - (tick_now % sec) + 2
            time.sleep(sleep_s)
            now = time.time()

            state = runtime_state.snapshot()
            if (not state.trading_enabled) and state.retry_at_epoch_sec:
                if math.isinf(state.retry_at_epoch_sec):
                    print("[RUN] trading halted indefinitely. exiting run loop.")
                    return
                if now < state.retry_at_epoch_sec:
                    wait_sec = max(0, int(state.retry_at_epoch_sec - now))
                    print(f"[RUN] failsafe active. trading paused for {wait_sec}s")
                    continue
                runtime_state.enable_trading()
                notify("failsafe retry window reached, attempting auto-resume")

            try:
                cmd_sync(quiet=True)
                conn = ensure_db()
                try:
                    row = conn.execute(
                        "SELECT ts, close FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
                        (settings.PAIR, settings.INTERVAL),
                    ).fetchone()
                finally:
                    conn.close()

                if row is None:
                    notify("no candles after sync")
                    continue

                last_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
                last_close = float(row["close"] if hasattr(row, "keys") else row[1])
                candle_age_sec = max(0.0, (time.time() * 1000 - last_ts) / 1000)
                runtime_state.set_last_candle_age_sec(candle_age_sec)

                fail_count = 0
                runtime_state.set_error_count(fail_count)
            except Exception as e:
                fail_count += 1
                runtime_state.set_error_count(fail_count)
                notify(f"sync failed ({fail_count}/{MAX_FAILS}): {e}")
                if fail_count >= MAX_FAILS:
                    retry_at = time.time() + FAILSAFE_RETRY_DELAY_SEC
                    runtime_state.disable_trading_until(retry_at)
                    notify(
                        "failsafe enabled after consecutive sync failures. "
                        f"trading paused until epoch={int(retry_at)}"
                    )
                continue

            stale_cutoff_sec = sec * 2
            if candle_age_sec > stale_cutoff_sec:
                notify(
                    f"stale candle detected: age={candle_age_sec:.1f}s > "
                    f"{stale_cutoff_sec}s; order blocked"
                )
                continue

            if settings.MODE == "live" and broker is not None:
                if settings.KILL_SWITCH:
                    canceled_ok = _attempt_open_order_cancellation(
                        broker, trigger="kill-switch"
                    )
                    flatten_outcome: dict[str, object] | None = None
                    flatten_failed = False
                    if settings.KILL_SWITCH_LIQUIDATE:
                        flatten_outcome = flatten_btc_position(
                            broker=broker,
                            dry_run=bool(settings.LIVE_DRY_RUN),
                            trigger="kill-switch",
                        )
                        flatten_status = str(flatten_outcome.get("status") or "")
                        flatten_failed = flatten_status == "failed"

                    open_orders_present, position_present = _get_exposure_snapshot(int(now * 1000))
                    risk_open = open_orders_present or position_present
                    cancel_status = (
                        "emergency cancellation attempted"
                        if canceled_ok
                        else "emergency cancellation failed"
                    )
                    reason_detail = f"KILL_SWITCH=ON; {cancel_status}"
                    if flatten_outcome is not None:
                        reason_detail += f"; flatten_status={str(flatten_outcome.get('status') or '-') }"
                        if flatten_failed:
                            reason_detail += f"; flatten_error={str(flatten_outcome.get('error') or '-') }"
                    if risk_open:
                        reason_detail += (
                            "; risk_open_exposure_remains"
                            f"(open_orders={1 if open_orders_present else 0},"
                            f"position={1 if position_present else 0})"
                        )
                    if (not canceled_ok) or flatten_failed:
                        _halt_trading(
                            _halt_reason("KILL_SWITCH", reason_detail),
                            unresolved=True,
                        )
                    else:
                        _halt_trading(
                            _halt_reason("KILL_SWITCH", reason_detail),
                            unresolved=risk_open,
                        )
                    continue

                conn = ensure_db()
                try:
                    portfolio = conn.execute(
                        "SELECT cash_krw, asset_qty FROM portfolio WHERE id=1"
                    ).fetchone()
                    if portfolio is not None:
                        # Use latest candle close as the mark price for daily-loss evaluation.
                        blocked, reason = evaluate_daily_loss_breach(
                            conn,
                            ts_ms=int(now * 1000),
                            cash=float(portfolio["cash_krw"]),
                            qty=float(portfolio["asset_qty"]),
                            price=float(last_close),
                        )
                        if blocked:
                            halt_reason, canceled_ok, flatten_failed = _attempt_risk_breach_flatten(
                                broker,
                                reason_code="DAILY_LOSS_LIMIT",
                                reason_detail=reason,
                                cancel_trigger="daily-loss-halt",
                                flatten_trigger="daily-loss-halt",
                            )
                            _halt_trading(
                                halt_reason,
                                unresolved=(not canceled_ok) or flatten_failed,
                            )
                            continue

                        blocked, reason = evaluate_position_loss_breach(
                            conn,
                            qty=float(portfolio["asset_qty"]),
                            price=float(last_close),
                        )
                        if blocked:
                            halt_reason, canceled_ok, flatten_failed = _attempt_risk_breach_flatten(
                                broker,
                                reason_code=POSITION_LOSS_LIMIT,
                                reason_detail=reason,
                                cancel_trigger="position-loss-halt",
                                flatten_trigger="position-loss-halt",
                            )
                            _halt_trading(
                                halt_reason,
                                unresolved=(not canceled_ok) or flatten_failed,
                            )
                            continue
                finally:
                    conn.close()

                open_count, oldest_open_age_sec = _get_open_order_snapshot(int(now * 1000))
                if open_count > 0:
                    min_reconcile_sec = max(
                        1, int(settings.OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC)
                    )
                    if (
                        last_open_order_reconcile_at is None
                        or (now - last_open_order_reconcile_at) >= min_reconcile_sec
                    ):
                        try:
                            reconcile_with_broker(broker)
                            last_open_order_reconcile_at = now
                        except Exception as e:
                            _halt_trading(
                                _halt_reason(
                                    "PERIODIC_RECONCILE_FAILED",
                                    f"periodic reconcile failed ({type(e).__name__}): {e}",
                                ),
                                unresolved=True,
                            )
                            continue

                    open_count, oldest_open_age_sec = _get_open_order_snapshot(
                        int(now * 1000)
                    )
                    if open_count > 0 and oldest_open_age_sec is not None:
                        max_age_sec = max(1, int(settings.MAX_OPEN_ORDER_AGE_SEC))
                        if oldest_open_age_sec > max_age_sec:
                            reason = (
                                "stale unresolved open order detected: "
                                f"age={oldest_open_age_sec:.1f}s > {max_age_sec}s"
                            )
                            marked = _mark_open_orders_recovery_required(
                                reason, int(now * 1000)
                            )
                            latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
                            notify(
                                format_event(
                                    "recovery_required_marked",
                                    alert_kind="recovery_required",
                                    symbol=settings.PAIR,
                                    reason_code="STALE_OPEN_ORDER",
                                    marked_count=marked,
                                    latest_client_order_id=latest_client_order_id,
                                    latest_exchange_order_id=latest_exchange_order_id,
                                    reason=reason,
                                    operator_next_action="inspect stale order(s), run reconcile, then recovery-report",
                                    operator_hint_command="uv run python bot.py reconcile && uv run python bot.py recovery-report",
                                    open_order_count=_count_open_orders(),
                                    position_summary=_position_summary(),
                                    operator_recommended_commands=(
                                        "uv run python bot.py reconcile"
                                        " | uv run python bot.py recover-order --client-order-id <id>"
                                    ),
                                    operator_compact_summary=_operator_compact_summary(
                                        halt_reason="STALE_OPEN_ORDER",
                                        unresolved_order_count=runtime_state.snapshot().unresolved_open_order_count,
                                        open_order_count=_count_open_orders(),
                                        position_summary=_position_summary(),
                                        recommended_commands=[
                                            "uv run python bot.py reconcile",
                                            "uv run python bot.py recover-order --client-order-id <id>",
                                        ],
                                    ),
                                )
                            )
                            canceled_ok = _attempt_open_order_cancellation(
                                broker, trigger="stale-open-order-halt"
                            )
                            if not canceled_ok:
                                _halt_trading(
                                    _halt_reason(
                                        "STALE_OPEN_ORDER",
                                        f"{reason}; marked={marked} recovery_required; emergency cancellation failed",
                                    ),
                                    unresolved=True,
                                )
                            else:
                                _halt_trading(
                                    _halt_reason(
                                        "STALE_OPEN_ORDER",
                                        f"{reason}; marked={marked} recovery_required; emergency cancellation attempted",
                                    ),
                                    unresolved=True,
                                )
                            continue

                    if open_count > 0:
                        notify(safety_event("order_submit_blocked", reason_code=RISKY_ORDER_BLOCK, reason="unresolved open order exists; skip new order placement"))
                        continue

            conn = ensure_db()
            r = compute_signal(conn, short_n, long_n)
            conn.close()

            if r is None:
                print("[RUN] 데이터 부족. sync가 쌓이면 다시 계산됨.")
                continue

            print(
                f"[RUN] {kst_str(r['ts'])} close={r['last_close']:,.0f}  "
                f"SMA{short_n}={r['curr_s']:.2f}  "
                f"SMA{long_n}={r['curr_l']:.2f}  => {r['signal']}"
            )

            if r["signal"] not in ("BUY", "SELL"):
                continue

            trade = None
            if settings.MODE == "paper":
                trade = paper_execute(r["signal"], r["ts"], r["last_close"])
            elif settings.MODE == "live" and broker is not None:
                try:
                    trade = live_execute_signal(
                        broker, r["signal"], r["ts"], r["last_close"]
                    )
                except BrokerError as e:
                    _halt_trading(
                        _halt_reason(
                            "LIVE_EXECUTION_BROKER_ERROR",
                            f"live execution broker error ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue
                except Exception as e:
                    _halt_trading(
                        _halt_reason(
                            "LIVE_EXECUTION_FAILED",
                            f"live execution failed ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue
                try:
                    reconcile_with_broker(broker)
                except Exception as e:
                    _halt_trading(
                        _halt_reason(
                            "POST_TRADE_RECONCILE_FAILED",
                            f"reconcile failed ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue

            if trade:
                print(
                    f"  [{settings.MODE.upper()}] {trade['side']} "
                    f"qty={trade['qty']:.8f} price={trade['price']:,.0f} "
                    f"fee={trade['fee']:,.0f} cash={trade['cash']:,.0f} "
                    f"asset={trade['asset']:.8f}"
                )

    except KeyboardInterrupt:
        print("\n[RUN] stopped by user (Ctrl+C)")
