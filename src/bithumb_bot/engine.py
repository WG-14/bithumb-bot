from __future__ import annotations

import logging
import math
import re
import time
import json
import os
from dataclasses import dataclass

from .config import (
    DEFAULT_RUNTIME_STRATEGY,
    MarketPreflightValidationError,
    settings,
    validate_live_mode_preflight,
    validate_market_preflight,
    validate_market_runtime,
)
from .marketdata import cmd_sync
from .strategy import create_strategy
from .broker.paper import paper_execute
from .broker.live import live_execute_signal, record_harmless_dust_exit_suppression
from .broker.bithumb import BithumbBroker, build_broker_with_auth_diagnostics
from .broker.base import BrokerError
from .db_core import ensure_db, get_external_cash_adjustment_summary
from .db_core import record_strategy_decision
from .dust import (
    DustClassification,
    DustState,
    build_position_state_model,
)
from .utils_time import kst_str, parse_interval_sec
from .notifier import format_event, notify
from .observability import configure_runtime_logging, format_log_kv, safety_event
from .bootstrap import get_last_explicit_env_load_summary
from .reason_codes import (
    BLOCKER_DUST_RESIDUAL,
    BLOCKER_BROKER_CASH_DELTA_UNEXPLAINED,
    BLOCKER_EXTERNAL_CASH_ADJUSTMENT_REQUIRED,
    BLOCKER_PORTFOLIO_BROKER_CASH_MISMATCH,
    BLOCKER_SUBMIT_UNKNOWN_RECOVERY_REQUIRED,
    BLOCKER_TRADE_FILL_UNRESOLVED,
    CANCEL_FAILURE,
    POSITION_LOSS_LIMIT,
    RISKY_ORDER_BLOCK,
    STARTUP_BLOCKED,
)
from . import runtime_state
from .risk import evaluate_daily_loss_breach, evaluate_position_loss_breach
from .oms import collect_risky_order_state
from .flatten import flatten_btc_position


FAILSAFE_RETRY_DELAY_SEC = 180
STARTUP_RECOVERY_GATE_PREFIX = "startup safety gate"
CLEANUP_REVALIDATION_MAX_ATTEMPTS = 2
CLEANUP_REVALIDATION_POSITION_EPS = 1e-12
RUN_LOG = logging.getLogger("bithumb_bot.run")


def compute_signal(
    conn,
    short_n: int,
    long_n: int,
    *,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
):
    selected_strategy_name = str(strategy_name or settings.STRATEGY_NAME).strip().lower()
    strategy = create_strategy(
        selected_strategy_name,
        short_n=short_n,
        long_n=long_n,
        pair=settings.PAIR,
        interval=settings.INTERVAL,
    )
    decision = strategy.decide(conn, through_ts_ms=through_ts_ms)
    if decision is None:
        return None
    payload = decision.as_dict()
    payload.setdefault("strategy", strategy.name)
    return payload


@dataclass(frozen=True)
class HaltReason:
    code: str
    detail: str


@dataclass(frozen=True)
class ResumeBlocker:
    code: str
    detail: str
    reason_code: str
    summary: str
    overridable: bool
    balance_delta_krw: float | None = None
    recent_external_cash_adjustment_present: bool | None = None
    recent_external_cash_adjustment_count: int | None = None


def _halt_reason(code: str, detail: str) -> HaltReason:
    return HaltReason(code=code, detail=detail)


def _resume_blocker(
    *,
    code: str,
    detail: str,
    overridable: bool,
    reason_code: str | None = None,
    summary: str | None = None,
    balance_delta_krw: float | None = None,
    recent_external_cash_adjustment_present: bool | None = None,
    recent_external_cash_adjustment_count: int | None = None,
) -> ResumeBlocker:
    return ResumeBlocker(
        code=code,
        detail=detail,
        reason_code=str(reason_code or code),
        summary=str(summary or detail),
        overridable=overridable,
        balance_delta_krw=balance_delta_krw,
        recent_external_cash_adjustment_present=recent_external_cash_adjustment_present,
        recent_external_cash_adjustment_count=recent_external_cash_adjustment_count,
    )


def _classify_startup_gate_reason(startup_gate_reason: str | None, *, state) -> tuple[str, str]:
    reason = str(startup_gate_reason or "").strip()
    if not reason:
        return "-", "no startup gate blocker"
    if int(state.recovery_required_count) > 0 or "recovery_required_orders=" in reason:
        return (
            BLOCKER_SUBMIT_UNKNOWN_RECOVERY_REQUIRED,
            "recovery-required orders remain",
        )
    if "submit_unknown_orders=" in reason:
        return (
            BLOCKER_SUBMIT_UNKNOWN_RECOVERY_REQUIRED,
            "submit unknown orders remain",
        )
    if (
        "pending_submit_orders=" in reason
        or "unresolved_open_orders=" in reason
        or "stale_new_partial_orders=" in reason
    ):
        return (
            BLOCKER_TRADE_FILL_UNRESOLVED,
            "trade/fill state remains unresolved",
        )
    return (
        BLOCKER_TRADE_FILL_UNRESOLVED,
        "startup safety gate blocked",
    )


def _classify_dust_resume_blocker(dust_context: dict[str, object]) -> tuple[str, str]:
    if str(dust_context.get("classification") or "") == DustState.HARMLESS_DUST.value:
        return (
            BLOCKER_DUST_RESIDUAL,
            "harmless dust still needs policy review",
        )
    return (
        BLOCKER_DUST_RESIDUAL,
        "dust residual requires operator review",
    )


def _extract_balance_split_delta_krw(summary: str) -> float | None:
    if not summary:
        return None
    match = re.search(
        r"cash_[a-z_]+\([^)]*delta=([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)\)",
        summary,
        re.IGNORECASE,
    )
    if match is None:
        return None
    try:
        value = float(match.group(1))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _ledger_external_cash_adjustment_summary() -> dict[str, object] | None:
    try:
        conn = ensure_db()
    except Exception:
        return None
    try:
        return get_external_cash_adjustment_summary(conn)
    except Exception:
        return None
    finally:
        conn.close()


def _classify_balance_split_blocker(metadata: dict[str, object]) -> ResumeBlocker | None:
    mismatch_count_raw = metadata.get("balance_split_mismatch_count", 0)
    try:
        mismatch_count = max(0, int(mismatch_count_raw))
    except (TypeError, ValueError):
        mismatch_count = 0
    if mismatch_count <= 0:
        return None

    summary = str(metadata.get("balance_split_mismatch_summary") or "").strip()
    external_cash_adjustment_count = 0
    try:
        external_cash_adjustment_count = max(0, int(metadata.get("external_cash_adjustment_count", 0) or 0))
    except (TypeError, ValueError):
        external_cash_adjustment_count = 0
    delta_krw = _extract_balance_split_delta_krw(summary)
    recent_external_cash_adjustment_present = external_cash_adjustment_count > 0

    cash_only_mismatch = (
        bool(summary)
        and ("cash_available" in summary or "cash_locked" in summary)
        and "asset_available" not in summary
        and "asset_locked" not in summary
    )
    if external_cash_adjustment_count <= 0 and cash_only_mismatch:
        return _resume_blocker(
            code=BLOCKER_EXTERNAL_CASH_ADJUSTMENT_REQUIRED,
            detail=(
                "balance split mismatch detected after reconcile: "
                f"count={mismatch_count} summary={summary or '-'} "
                f"external_cash_adjustment_present=0 delta_krw={delta_krw if delta_krw is not None else '-'}"
            ),
            reason_code=BLOCKER_BROKER_CASH_DELTA_UNEXPLAINED,
            summary="cash mismatch requires external cash adjustment evidence",
            overridable=False,
            balance_delta_krw=delta_krw,
            recent_external_cash_adjustment_present=False,
            recent_external_cash_adjustment_count=external_cash_adjustment_count,
        )

    if cash_only_mismatch and recent_external_cash_adjustment_present:
        blocker_summary = "cash split mismatch persists after external cash adjustment was recorded"
    else:
        blocker_summary = "portfolio cash split does not match broker snapshot"

    return _resume_blocker(
        code="BALANCE_SPLIT_MISMATCH",
        detail=(
            "balance split mismatch detected after reconcile: "
            f"count={mismatch_count} summary={summary or '-'} "
            f"external_cash_adjustment_present={1 if recent_external_cash_adjustment_present else 0} "
            f"delta_krw={delta_krw if delta_krw is not None else '-'}"
        ),
        reason_code=BLOCKER_PORTFOLIO_BROKER_CASH_MISMATCH,
        summary=blocker_summary,
        overridable=False,
        balance_delta_krw=delta_krw,
        recent_external_cash_adjustment_present=recent_external_cash_adjustment_present,
        recent_external_cash_adjustment_count=external_cash_adjustment_count,
    )


def _reconcile_balance_split_mismatch_count(metadata_raw: str | None) -> int:
    if not metadata_raw:
        return 0
    try:
        reconcile_meta = json.loads(str(metadata_raw))
    except json.JSONDecodeError:
        return 0
    mismatch_raw = reconcile_meta.get("balance_split_mismatch_count", 0)
    try:
        return max(0, int(mismatch_raw))
    except (TypeError, ValueError):
        return 0


def _reconcile_dust_context(metadata_raw: str | None) -> dict[str, object]:
    dust = DustClassification.from_metadata(metadata_raw)
    raw_holdings = dust.to_raw_holdings()
    return {
        "classification": raw_holdings.classification,
        "present": raw_holdings.present,
        "allow_resume": dust.allow_resume,
        "effective_flat": dust.effective_flat,
        "policy_reason": dust.policy_reason,
        "summary": raw_holdings.compact_summary,
    }


def _dust_residual_resume_blocker(dust_context: dict[str, object]) -> tuple[str, str] | None:
    if not bool(dust_context["present"]) or bool(dust_context["allow_resume"]):
        return None
    if str(dust_context.get("classification") or "") == DustState.HARMLESS_DUST.value:
        return (
            "HARMLESS_DUST_POLICY_REVIEW_REQUIRED",
            (
                "harmless dust is visible and treated as flat, but current policy still blocks resume/new orders: "
                f"policy={str(dust_context['policy_reason'])} "
                f"summary={str(dust_context['summary'])}"
            ),
        )
    return (
        "BLOCKING_DUST_REVIEW_REQUIRED",
        (
            "blocking dust residual requires operator review before resume: "
            f"policy={str(dust_context['policy_reason'])} "
            f"summary={str(dust_context['summary'])}"
        ),
    )


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

SAFE_CLEARABLE_RECONCILE_HALT_REASON_CODES = {
    "INITIAL_RECONCILE_FAILED",
    "PERIODIC_RECONCILE_FAILED",
    "POST_TRADE_RECONCILE_FAILED",
}

NON_CLEARING_RECONCILE_REASON_CODES = {
    "RECONCILE_FAILED",
    "SOURCE_CONFLICT_HALT",
    "STARTUP_GATE_BLOCKED",
    "SUBMIT_UNKNOWN_UNRESOLVED",
}

def _log_loop_event(level: int, prefix: str, /, **fields: object) -> None:
    RUN_LOG.log(level, format_log_kv(prefix, mode=settings.MODE, **fields))


def _close_guard_ms(interval_sec: int) -> int:
    interval_ms = max(1, int(interval_sec)) * 1000
    return max(2_000, min(30_000, interval_ms // 20))


def _candle_close_ts_ms(*, candle_start_ts_ms: int, interval_sec: int) -> int:
    interval_ms = max(1, int(interval_sec)) * 1000
    return int(candle_start_ts_ms) + interval_ms


def _is_closed_candle(*, candle_ts_ms: int, now_ms: int, interval_sec: int) -> bool:
    """
    Return True when candle identified by DB key `candles.ts` is safely closed.

    `candles.ts` is the candle bucket start timestamp (UTC epoch ms), not the
    exchange payload's per-trade snapshot timestamp. Closedness is therefore
    judged from candle-start + interval (+ guard), not from raw payload
    `timestamp`.
    """
    interval_ms = max(1, int(interval_sec)) * 1000
    close_ready_ts_ms = _candle_close_ts_ms(
        candle_start_ts_ms=candle_ts_ms,
        interval_sec=interval_sec,
    ) + _close_guard_ms(interval_sec)
    return now_ms >= close_ready_ts_ms


def _select_latest_closed_candle(conn, *, pair: str, interval: str, interval_sec: int, now_ms: int):
    cursor = conn.execute(
        """
        SELECT ts, close
        FROM candles
        WHERE pair=? AND interval=?
        ORDER BY ts DESC
        LIMIT 5
        """,
        (pair, interval),
    )
    if hasattr(cursor, "fetchall"):
        rows = cursor.fetchall()
    else:
        row = cursor.fetchone()
        if row is None:
            return None, None
        # Compatibility path for lightweight test/mocked cursor objects that only
        # implement fetchone(); preserve historical single-row behavior without
        # altering the sqlite cursor contract used in production.
        return row, None
    if not rows:
        return None, None

    latest_row = rows[0]
    latest_ts = int(latest_row["ts"]) if hasattr(latest_row, "keys") else int(latest_row[0])
    incomplete_ts = None
    if not _is_closed_candle(candle_ts_ms=latest_ts, now_ms=now_ms, interval_sec=interval_sec):
        incomplete_ts = latest_ts

    for row in rows:
        candle_ts_ms = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
        if _is_closed_candle(candle_ts_ms=candle_ts_ms, now_ms=now_ms, interval_sec=interval_sec):
            return row, incomplete_ts

    return None, incomplete_ts


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
        state = runtime_state.snapshot()
    finally:
        conn.close()

    asset_qty = float(portfolio_row["asset_qty"] if portfolio_row is not None else 0.0)
    position_state = build_position_state_model(
        raw_qty_open=asset_qty,
        metadata_raw=state.last_reconcile_metadata,
    )
    return open_count > 0, position_state.normalized_exposure.normalized_exposure_active


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


def _can_clear_reconcile_failure_halt(*, state, startup_gate_reason: str | None) -> bool:
    open_orders_present, position_present = _get_exposure_snapshot(int(time.time() * 1000))
    mismatch_count = _reconcile_balance_split_mismatch_count(state.last_reconcile_metadata)
    dust_context = _reconcile_dust_context(state.last_reconcile_metadata)
    reconcile_reason_code = str(state.last_reconcile_reason_code or "").strip()
    return bool(
        state.last_reconcile_status == "ok"
        and reconcile_reason_code
        and reconcile_reason_code not in NON_CLEARING_RECONCILE_REASON_CODES
        and mismatch_count == 0
        and not startup_gate_reason
        and state.unresolved_open_order_count == 0
        and state.recovery_required_count == 0
        and not open_orders_present
        and (not position_present or bool(dust_context["effective_flat"]))
    )


def maybe_clear_stale_initial_reconcile_halt() -> bool:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()

    if not (
        state.halt_new_orders_blocked
        and state.halt_state_unresolved
        and state.halt_reason_code in SAFE_CLEARABLE_RECONCILE_HALT_REASON_CODES
    ):
        return False

    startup_gate_reason = evaluate_startup_safety_gate()
    if not _can_clear_reconcile_failure_halt(
        state=runtime_state.snapshot(),
        startup_gate_reason=startup_gate_reason,
    ):
        return False

    runtime_state.disable_trading_until(
        float("inf"),
        reason=None,
        halt_new_orders_blocked=False,
        unresolved=False,
    )
    runtime_state.set_resume_gate(blocked=False, reason=None)
    _log_loop_event(
        logging.INFO,
        "[RUN] stale_reconcile_failure_halt_cleared",
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
    )
    return True


def maybe_clear_stale_live_execution_broker_halt(*, startup_gate_reason: str | None = None) -> bool:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()

    if not (
        state.halt_new_orders_blocked
        and state.halt_state_unresolved
        and state.halt_reason_code == "LIVE_EXECUTION_BROKER_ERROR"
    ):
        return False

    gate_reason = startup_gate_reason if startup_gate_reason is not None else evaluate_startup_safety_gate()
    if not _can_clear_reconcile_failure_halt(
        state=runtime_state.snapshot(),
        startup_gate_reason=gate_reason,
    ):
        return False

    runtime_state.disable_trading_until(
        float("inf"),
        reason=state.last_disable_reason,
        halt_new_orders_blocked=False,
        unresolved=False,
    )
    runtime_state.set_resume_gate(blocked=False, reason=None)
    _log_loop_event(
        logging.INFO,
        "[RUN] stale_live_execution_broker_halt_cleared",
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
    )
    return True

def get_health_status() -> dict[str, float | int | bool | str | None]:
    maybe_clear_stale_initial_reconcile_halt()
    maybe_clear_stale_live_execution_broker_halt()
    state = runtime_state.snapshot()
    return {
        "last_candle_age_sec": state.last_candle_age_sec,
        "last_candle_status": state.last_candle_status,
        "last_candle_sync_epoch_sec": state.last_candle_sync_epoch_sec,
        "last_candle_ts_ms": state.last_candle_ts_ms,
        "last_processed_candle_ts_ms": state.last_processed_candle_ts_ms,
        "last_candle_status_detail": state.last_candle_status_detail,
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
        "emergency_flatten_blocked": state.emergency_flatten_blocked,
        "emergency_flatten_block_reason": state.emergency_flatten_block_reason,
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
    flatten_block_reason = runtime_state.get_emergency_flatten_blocker()
    if flatten_block_reason:
        reasons.append(f"emergency_flatten_unresolved={flatten_block_reason}")

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

    RUN_LOG.info(
        format_log_kv(
            "[STARTUP_GATE] evaluated",
            pending_submit=status_counts["pending_submit"],
            submit_unknown=status_counts["submit_unknown"],
            recovery_required=status_counts["recovery_required"],
            stale_new_partial=status_counts["stale_new_partial"],
            unresolved_open_orders=state.unresolved_open_order_count,
            runtime_recovery_required=state.recovery_required_count,
            submit_unknown_without_exchange_id=submit_unknown_without_exchange_count,
            stray_remote_open_orders=stray_remote_open_count,
            gate_blocked=bool(reasons),
        )
    )

    if not reasons:
        runtime_state.set_startup_gate_reason(None)
        return None

    reason = f"{STARTUP_RECOVERY_GATE_PREFIX}: " + ", ".join(reasons)
    RUN_LOG.warning(format_log_kv("[STARTUP_GATE] blocked", reason=reason))
    runtime_state.set_startup_gate_reason(reason)
    return reason


def evaluate_resume_eligibility() -> tuple[bool, list[ResumeBlocker]]:
    """Returns whether operator resume may proceed and structured blockers."""
    maybe_clear_stale_initial_reconcile_halt()
    startup_gate_reason = evaluate_startup_safety_gate()
    maybe_clear_stale_live_execution_broker_halt(startup_gate_reason=startup_gate_reason)
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
                reason_code="LAST_RECONCILE_FAILED",
                summary="last reconcile failed",
                overridable=False,
            )
        )

    if startup_gate_reason:
        startup_blocker_reason_code, startup_blocker_summary = _classify_startup_gate_reason(
            startup_gate_reason,
            state=state,
        )
        reasons.append(
            _resume_blocker(
                code="STARTUP_SAFETY_GATE_BLOCKED",
                detail=startup_gate_reason,
                reason_code=startup_blocker_reason_code,
                summary=startup_blocker_summary,
                overridable=False,
            )
        )

    if state.emergency_flatten_blocked:
        reasons.append(
            _resume_blocker(
                code="EMERGENCY_FLATTEN_UNRESOLVED",
                detail=(
                    state.emergency_flatten_block_reason
                    or f"last_flatten_status={state.last_flatten_position_status or '-'}"
                ),
                reason_code="EMERGENCY_FLATTEN_UNRESOLVED",
                summary="emergency flatten remains unresolved",
                overridable=False,
            )
        )

    if startup_gate_reason and state.last_reconcile_status == "ok":
        startup_blocker_reason_code, startup_blocker_summary = _classify_startup_gate_reason(
            startup_gate_reason,
            state=state,
        )
        reasons.append(
            _resume_blocker(
                code="LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS",
                detail="latest reconcile reported ok but startup safety gate still blocks resume",
                reason_code=startup_blocker_reason_code,
                summary=startup_blocker_summary,
                overridable=False,
            )
        )

    dust_context_for_halt = _reconcile_dust_context(state.last_reconcile_metadata)
    dust_resume_blocker = _dust_residual_resume_blocker(dust_context_for_halt)
    if dust_resume_blocker is not None:
        blocker_code, blocker_detail = dust_resume_blocker
        dust_reason_code, dust_summary = _classify_dust_resume_blocker(dust_context_for_halt)
        reasons.append(
            _resume_blocker(
                code=blocker_code,
                detail=blocker_detail,
                reason_code=dust_reason_code,
                summary=dust_summary,
                overridable=False,
            )
        )

    unresolved_dust_safe = bool(
        state.halt_state_unresolved
        and (state.halt_reason_code or "") in RISK_EXPOSURE_HALT_REASON_CODES
        and int(state.unresolved_open_order_count) == 0
        and int(state.recovery_required_count) == 0
        and bool(dust_context_for_halt["effective_flat"])
    )
    if state.halt_state_unresolved and not unresolved_dust_safe:
            reasons.append(
                _resume_blocker(
                    code="HALT_STATE_UNRESOLVED",
                    detail=f"halt unresolved: code={state.halt_reason_code or '-'} reason={state.last_disable_reason or '-'}",
                    reason_code="HALT_STATE_UNRESOLVED",
                    summary="halt state remains unresolved",
                    overridable=False,
                )
            )

    if state.halt_new_orders_blocked:
        open_orders_present, position_present = _get_exposure_snapshot(int(time.time() * 1000))
        dust_context = _reconcile_dust_context(state.last_reconcile_metadata)
        is_risk_exposure_halt = (state.halt_reason_code or "") in RISK_EXPOSURE_HALT_REASON_CODES
        dust_exposure_only = bool(
            not open_orders_present
            and position_present
            and bool(dust_context["effective_flat"])
        )
        if open_orders_present or (position_present and not dust_exposure_only):
            detail = (
                "halt blocked with open exposure: "
                f"position_present={1 if position_present else 0} "
                f"open_orders_present={1 if open_orders_present else 0} "
                f"reason_code={state.halt_reason_code or '-'} "
                f"reason={state.last_disable_reason or '-'}"
            )
            if position_present and not open_orders_present and bool(dust_context["present"]):
                detail += (
                    f" dust_policy={str(dust_context['policy_reason'])} "
                    f"dust_summary={str(dust_context['summary'])}"
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
                    reason_code="HALT_RISK_OPEN_POSITION",
                    summary="halt risk still has open exposure",
                    overridable=False,
                )
            )

    ledger_adjustment_summary = _ledger_external_cash_adjustment_summary() if settings.MODE == "live" else None
    if settings.MODE == "live" and state.last_reconcile_metadata:
        mismatch_count = _reconcile_balance_split_mismatch_count(state.last_reconcile_metadata)
        try:
            reconcile_meta = json.loads(str(state.last_reconcile_metadata))
        except json.JSONDecodeError:
            reconcile_meta = {}
        ledger_adjustment_count = 0
        ledger_adjustment_total = 0.0
        if ledger_adjustment_summary is not None:
            try:
                ledger_adjustment_count = max(0, int(ledger_adjustment_summary.get("adjustment_count", 0) or 0))
            except (TypeError, ValueError):
                ledger_adjustment_count = 0
            try:
                ledger_adjustment_total = float(ledger_adjustment_summary.get("adjustment_total", 0.0) or 0.0)
            except (TypeError, ValueError):
                ledger_adjustment_total = 0.0
        if ledger_adjustment_count > 0:
            reconcile_meta = dict(reconcile_meta)
            reconcile_meta["external_cash_adjustment_count"] = max(
                ledger_adjustment_count,
                int(reconcile_meta.get("external_cash_adjustment_count", 0) or 0),
            )
            reconcile_meta["external_cash_adjustment_total_krw"] = (
                float(reconcile_meta.get("external_cash_adjustment_total_krw", 0.0) or 0.0)
                if float(reconcile_meta.get("external_cash_adjustment_total_krw", 0.0) or 0.0) != 0.0
                else ledger_adjustment_total
            )
        if mismatch_count > 0:
            blocker_reason = _classify_balance_split_blocker(reconcile_meta)
            if blocker_reason is None:
                blocker_reason = _resume_blocker(
                    code="BALANCE_SPLIT_MISMATCH",
                    detail=(
                        "balance split mismatch detected after reconcile: "
                        f"count={mismatch_count} summary={str(reconcile_meta.get('balance_split_mismatch_summary') or '-')}"
                    ),
                    reason_code=BLOCKER_PORTFOLIO_BROKER_CASH_MISMATCH,
                    summary="portfolio cash split does not match broker snapshot",
                    overridable=False,
                )
            reasons.append(blocker_reason)

    gate_reason = None
    if reasons:
        gate_reason = "; ".join(f"{blocker.code}:{blocker.detail}" for blocker in reasons)
    runtime_state.set_resume_gate(blocked=bool(reasons), reason=gate_reason)
    return (len(reasons) == 0), reasons

def _halt_trading(reason: HaltReason, *, unresolved: bool = False, attempt_flatten: bool = False) -> None:
    runtime_state.enter_halt(
        reason_code=reason.code,
        reason=reason.detail,
        unresolved=unresolved,
        attempt_flatten=attempt_flatten,
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


def _revalidate_cleanup_state_after_failure(
    broker: BithumbBroker,
    *,
    trigger: str,
    max_attempts: int = CLEANUP_REVALIDATION_MAX_ATTEMPTS,
) -> tuple[bool, str]:
    """Performs bounded broker-side revalidation after uncertain cleanup results."""
    from .recovery import reconcile_with_broker

    attempts = max(1, int(max_attempts))
    last_open_orders_present: bool | None = None
    last_position_present: bool | None = None
    last_errors: list[str] = []

    for attempt in range(1, attempts + 1):
        try:
            reconcile_with_broker(broker)
        except Exception as exc:
            last_errors.append(f"attempt={attempt} reconcile={type(exc).__name__}: {exc}")

        open_orders_present: bool | None = None
        position_present: bool | None = None

        try:
            conn = ensure_db()
            try:
                placeholders = ",".join("?" for _ in LIVE_UNRESOLVED_ORDER_STATUSES)
                rows = conn.execute(
                    f"""
                    SELECT client_order_id, exchange_order_id
                    FROM orders
                    WHERE status IN ({placeholders})
                    """,
                    LIVE_UNRESOLVED_ORDER_STATUSES,
                ).fetchall()
            finally:
                conn.close()
            exchange_order_ids = sorted(
                {
                    str(row["exchange_order_id"]).strip()
                    for row in rows
                    if str(row["exchange_order_id"] or "").strip()
                }
            )
            client_order_ids = sorted(
                {
                    str(row["client_order_id"]).strip()
                    for row in rows
                    if str(row["client_order_id"] or "").strip()
                }
            )
            if exchange_order_ids or client_order_ids:
                open_orders_present = (
                    len(
                        broker.get_open_orders(
                            exchange_order_ids=exchange_order_ids,
                            client_order_ids=client_order_ids,
                        )
                    )
                    > 0
                )
            else:
                open_orders_present = False
        except Exception as exc:
            last_errors.append(f"attempt={attempt} open_orders={type(exc).__name__}: {exc}")

        try:
            balance = broker.get_balance()
            position_present = (
                float(balance.asset_available) + float(balance.asset_locked)
            ) > CLEANUP_REVALIDATION_POSITION_EPS
        except Exception as exc:
            last_errors.append(f"attempt={attempt} balance={type(exc).__name__}: {exc}")

        if open_orders_present is not None:
            last_open_orders_present = open_orders_present
        if position_present is not None:
            last_position_present = position_present

        if open_orders_present is False and position_present is False:
            return True, (
                f"cleanup_revalidation(trigger={trigger}) attempts={attempt}/{attempts} "
                "broker_confirms_no_open_orders_and_no_position"
            )

    status_parts = [
        f"cleanup_revalidation(trigger={trigger}) attempts={attempts}/{attempts}",
        (
            f"open_orders_present={1 if last_open_orders_present else 0}"
            if last_open_orders_present is not None
            else "open_orders_present=unknown"
        ),
        (
            f"position_present={1 if last_position_present else 0}"
            if last_position_present is not None
            else "position_present=unknown"
        ),
    ]
    if last_errors:
        status_parts.append("errors=" + " | ".join(last_errors))
    return False, "; ".join(status_parts)


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


def _attempt_cleanup_with_optional_flatten(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
    attempt_flatten: bool,
) -> tuple[HaltReason, bool, bool]:
    initial_open_orders_present, initial_position_present = _get_exposure_snapshot(int(time.time() * 1000))
    canceled_ok = _attempt_open_order_cancellation(broker, trigger=cancel_trigger)
    flatten_outcome: dict[str, object] | None = None
    if attempt_flatten and canceled_ok:
        flatten_outcome = flatten_btc_position(
            broker=broker,
            dry_run=bool(settings.LIVE_DRY_RUN),
            trigger=flatten_trigger,
        )
        flatten_status = str(flatten_outcome.get("status") or "-")
    elif attempt_flatten:
        flatten_status = "skipped_cancel_failed"
    else:
        flatten_status = "skipped"

    if flatten_status in {"skipped", "skipped_cancel_failed"}:
        runtime_state.record_flatten_position_result(
            status=flatten_status,
            summary={
                "status": flatten_status,
                "attempted": int(bool(attempt_flatten)),
                "cancel_ok": int(bool(canceled_ok)),
                "reason_code": reason_code,
                "reason_detail": reason_detail,
                "trigger": flatten_trigger,
            },
        )

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
    if flatten_failed and flatten_outcome is not None:
        detail_parts.append(f"flatten_error={str(flatten_outcome.get('error') or '-')}")

    cleanup_uncertain = (not canceled_ok) or flatten_failed
    unresolved = True
    if cleanup_uncertain:
        revalidated_safe, revalidation_detail = _revalidate_cleanup_state_after_failure(
            broker,
            trigger=flatten_trigger,
        )
        detail_parts.append(revalidation_detail)
        unresolved = not revalidated_safe
    else:
        post_open_orders_present, post_position_present = _get_exposure_snapshot(int(time.time() * 1000))
        if post_open_orders_present or post_position_present:
            detail_parts.append(
                "risk_open_exposure_remains("
                f"open_orders={1 if post_open_orders_present else 0},"
                f"position={1 if post_position_present else 0})"
            )
        unresolved = post_open_orders_present or post_position_present

    if initial_open_orders_present or initial_position_present:
        detail_parts.append(
            "cleanup_started_with_exposure("
            f"open_orders={1 if initial_open_orders_present else 0},"
            f"position={1 if initial_position_present else 0})"
        )

    return _halt_reason(reason_code, "; ".join(detail_parts)), canceled_ok, unresolved


def _attempt_risk_breach_flatten(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
) -> tuple[HaltReason, bool, bool]:
    return _attempt_cleanup_with_optional_flatten(
        broker,
        reason_code=reason_code,
        reason_detail=reason_detail,
        cancel_trigger=cancel_trigger,
        flatten_trigger=flatten_trigger,
        attempt_flatten=True,
    )


def perform_panic_stop_cleanup(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
    attempt_flatten: bool,
) -> tuple[HaltReason, bool, bool]:
    return _attempt_cleanup_with_optional_flatten(
        broker,
        reason_code=reason_code,
        reason_detail=reason_detail,
        cancel_trigger=cancel_trigger,
        flatten_trigger=flatten_trigger,
        attempt_flatten=attempt_flatten,
    )


def run_loop(short_n: int, long_n: int) -> None:
    from .recovery import reconcile_with_broker

    configure_runtime_logging()
    if settings.MODE != "live":
        try:
            validate_market_preflight(settings)
        except MarketPreflightValidationError as exc:
            _log_loop_event(
                logging.ERROR,
                "[RUN] startup_blocked",
                symbol=settings.PAIR,
                interval=settings.INTERVAL,
                reason=f"market preflight failed: {exc}",
            )
            raise
    validate_live_mode_preflight(settings)

    maybe_clear_stale_initial_reconcile_halt()
    maybe_clear_stale_live_execution_broker_halt()
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
        _log_loop_event(
            logging.WARNING,
            "[RUN] startup_blocked",
            symbol=settings.PAIR,
            interval=settings.INTERVAL,
            reason="persisted runtime halt is active; refusing to enter trading loop",
        )
        return

    broker = None
    if settings.MODE == "live":
        broker, _auth_diag = build_broker_with_auth_diagnostics(
            caller="run_loop",
            env_summary=get_last_explicit_env_load_summary().as_dict(),
            broker_factory=BithumbBroker,
        )
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
    _log_loop_event(
        logging.INFO,
        "[RUN] loop_start",
        symbol=settings.PAIR,
        interval=settings.INTERVAL,
        every_sec=sec,
        strategy=settings.STRATEGY_NAME,
        strategy_source=(
            "env:STRATEGY_NAME"
            if os.getenv("STRATEGY_NAME") not in (None, "")
            else f"default:{DEFAULT_RUNTIME_STRATEGY}"
        ),
        sma_short=short_n,
        sma_long=long_n,
    )
    _log_loop_event(logging.INFO, "[RUN] operator_hint", action="Ctrl+C to stop")
    fail_count = 0
    MAX_FAILS = 5
    last_open_order_reconcile_at: float | None = None
    last_market_runtime_check_at: float | None = None

    try:
        while True:
            tick_now = time.time()
            sleep_s = sec - (tick_now % sec) + 2
            time.sleep(sleep_s)
            now = time.time()

            state = runtime_state.snapshot()
            if (not state.trading_enabled) and state.retry_at_epoch_sec:
                if math.isinf(state.retry_at_epoch_sec):
                    _log_loop_event(
                        logging.WARNING,
                        "[RUN] halted_exit",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        reason="trading halted indefinitely",
                    )
                    return
                if now < state.retry_at_epoch_sec:
                    wait_sec = max(0, int(state.retry_at_epoch_sec - now))
                    _log_loop_event(
                        logging.WARNING,
                        "[RUN] failsafe_pause",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        wait_sec=wait_sec,
                        reason="retry window not reached",
                    )
                    continue
                runtime_state.enable_trading()
                notify("failsafe retry window reached, attempting auto-resume")

            try:
                cmd_sync(quiet=True)
                sync_observed_epoch_sec = time.time()
                conn = ensure_db()
                try:
                    row = conn.execute(
                        "SELECT ts, close FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
                        (settings.PAIR, settings.INTERVAL),
                    ).fetchone()
                    closed_row, incomplete_ts = _select_latest_closed_candle(
                        conn,
                        pair=settings.PAIR,
                        interval=settings.INTERVAL,
                        interval_sec=sec,
                        now_ms=int(sync_observed_epoch_sec * 1000),
                    )
                finally:
                    conn.close()

                if row is None:
                    runtime_state.set_last_candle_observation(
                        status="missing_after_sync",
                        age_sec=None,
                        sync_epoch_sec=sync_observed_epoch_sec,
                        candle_ts_ms=None,
                        detail="sync completed but latest candle row was not found",
                    )
                    notify("no candles after sync")
                    continue

                last_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
                last_close = float(row["close"] if hasattr(row, "keys") else row[1])
                candle_age_sec = max(0.0, (time.time() * 1000 - last_ts) / 1000)
                runtime_state.set_last_candle_observation(
                    status="ok",
                    age_sec=candle_age_sec,
                    sync_epoch_sec=sync_observed_epoch_sec,
                    candle_ts_ms=last_ts,
                    detail=(
                        None
                        if incomplete_ts is None
                        else f"latest candle ts={incomplete_ts} still open; using latest fully closed candle"
                    ),
                )

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

            runtime_market_check_interval = float(settings.MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC)
            if runtime_market_check_interval < 0:
                _halt_trading(
                    _halt_reason(
                        "MARKET_RUNTIME_POLICY_INVALID",
                        "MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC must be >= 0",
                    ),
                    unresolved=False,
                )
                continue
            should_check_runtime_market = settings.MODE == "live" and runtime_market_check_interval > 0 and (
                last_market_runtime_check_at is None
                or (now - last_market_runtime_check_at) >= runtime_market_check_interval
            )
            if should_check_runtime_market:
                try:
                    validate_market_runtime(settings)
                    last_market_runtime_check_at = now
                except MarketPreflightValidationError as exc:
                    _halt_trading(
                        _halt_reason(
                            "MARKET_RUNTIME_CONTRACT_FAILED",
                            f"market runtime contract failed: {exc}",
                        ),
                        unresolved=False,
                    )
                    continue

            if settings.MODE == "live" and broker is not None:
                if settings.KILL_SWITCH:
                    halt_reason, _canceled_ok, unresolved = perform_panic_stop_cleanup(
                        broker,
                        reason_code="KILL_SWITCH",
                        reason_detail="KILL_SWITCH=ON",
                        cancel_trigger="kill-switch",
                        flatten_trigger="kill-switch",
                        attempt_flatten=bool(settings.KILL_SWITCH_LIQUIDATE),
                    )
                    _halt_trading(
                        halt_reason,
                        unresolved=unresolved,
                        attempt_flatten=bool(settings.KILL_SWITCH_LIQUIDATE),
                    )
                    continue

                conn = ensure_db()
                portfolio_cash = 0.0
                portfolio_qty = 0.0
                try:
                    portfolio = conn.execute(
                        "SELECT cash_krw, asset_qty FROM portfolio WHERE id=1"
                    ).fetchone()
                    if portfolio is not None:
                        portfolio_cash = float(portfolio["cash_krw"])
                        portfolio_qty = float(portfolio["asset_qty"])
                        # Use latest candle close as the mark price for daily-loss evaluation.
                        blocked, reason = evaluate_daily_loss_breach(
                            conn,
                            ts_ms=int(now * 1000),
                            cash=portfolio_cash,
                            qty=portfolio_qty,
                            price=float(last_close),
                        )
                        if blocked:
                            halt_reason, canceled_ok, cleanup_unresolved = _attempt_risk_breach_flatten(
                                broker,
                                reason_code="DAILY_LOSS_LIMIT",
                                reason_detail=reason,
                                cancel_trigger="daily-loss-halt",
                                flatten_trigger="daily-loss-halt",
                            )
                            _halt_trading(
                                halt_reason,
                                unresolved=cleanup_unresolved,
                            )
                            continue

                        blocked, reason = evaluate_position_loss_breach(
                            conn,
                            qty=portfolio_qty,
                            price=float(last_close),
                        )
                        if blocked:
                            halt_reason, canceled_ok, cleanup_unresolved = _attempt_risk_breach_flatten(
                                broker,
                                reason_code=POSITION_LOSS_LIMIT,
                                reason_detail=reason,
                                cancel_trigger="position-loss-halt",
                                flatten_trigger="position-loss-halt",
                            )
                            _halt_trading(
                                halt_reason,
                                unresolved=cleanup_unresolved,
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
                            halt_detail = (
                                f"{reason}; marked={marked} recovery_required; "
                                + (
                                    "emergency cancellation attempted"
                                    if canceled_ok
                                    else "emergency cancellation failed"
                                )
                            )
                            if not canceled_ok:
                                revalidated_safe, revalidation_detail = _revalidate_cleanup_state_after_failure(
                                    broker,
                                    trigger="stale-open-order-halt",
                                )
                                halt_detail += f"; {revalidation_detail}"
                                _halt_trading(
                                    _halt_reason("STALE_OPEN_ORDER", halt_detail),
                                    unresolved=not revalidated_safe,
                                )
                            else:
                                _halt_trading(
                                    _halt_reason("STALE_OPEN_ORDER", halt_detail),
                                    unresolved=True,
                                )
                            continue

                    if open_count > 0:
                        notify(safety_event("order_submit_blocked", reason_code=RISKY_ORDER_BLOCK, reason="unresolved open order exists; skip new order placement"))
                        continue

            state = runtime_state.snapshot()
            last_processed_candle_ts_ms = state.last_processed_candle_ts_ms

            if incomplete_ts is not None:
                _log_loop_event(
                    logging.INFO,
                    "[SKIP] incomplete/open candle",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=incomplete_ts,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason=f"latest candle has not cleared close guard ({_close_guard_ms(sec)}ms)",
                )

            if closed_row is None:
                _log_loop_event(
                    logging.INFO,
                    "[SKIP] incomplete/open candle",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=incomplete_ts,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason="no fully closed candle available yet",
                )
                continue

            closed_candle_ts_ms = int(closed_row["ts"]) if hasattr(closed_row, "keys") else int(closed_row[0])
            if last_processed_candle_ts_ms is not None:
                if closed_candle_ts_ms == last_processed_candle_ts_ms:
                    _log_loop_event(
                        logging.INFO,
                        "[SKIP] duplicate candle",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        candle_ts=closed_candle_ts_ms,
                        last_processed_candle_ts=last_processed_candle_ts_ms,
                        reason="closed candle already processed before restart/previous tick",
                    )
                    continue
                if closed_candle_ts_ms < last_processed_candle_ts_ms:
                    _log_loop_event(
                        logging.INFO,
                        "[SKIP] stale candle",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        candle_ts=closed_candle_ts_ms,
                        last_processed_candle_ts=last_processed_candle_ts_ms,
                        reason="closed candle is older than persisted last processed candle",
                    )
                    continue

            conn = ensure_db()
            try:
                try:
                    r = compute_signal(
                        conn,
                        short_n,
                        long_n,
                        through_ts_ms=closed_candle_ts_ms,
                        strategy_name=settings.STRATEGY_NAME,
                    )
                except TypeError as exc:
                    err = str(exc)
                    if ("through_ts_ms" not in err) and ("strategy_name" not in err):
                        raise
                    try:
                        r = compute_signal(
                            conn,
                            short_n,
                            long_n,
                            through_ts_ms=closed_candle_ts_ms,
                        )
                    except TypeError as compat_exc:
                        compat_err = str(compat_exc)
                        if "through_ts_ms" not in compat_err:
                            raise
                        # Compatibility path for tests/mocks still patching the older
                        # compute_signal(conn, short_n, long_n) signature.
                        r = compute_signal(conn, short_n, long_n)
            finally:
                conn.close()

            if r is None:
                _log_loop_event(
                    logging.INFO,
                    "[RUN] signal_skipped",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=closed_candle_ts_ms,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason="insufficient candle history; signal will be recalculated after more syncs",
                )
                continue

            _log_loop_event(
                logging.INFO,
                "[RUN] processed closed candle",
                symbol=settings.PAIR,
                interval=settings.INTERVAL,
                candle_ts=r["ts"],
                last_processed_candle_ts=last_processed_candle_ts_ms,
                close=f"{r['last_close']:,.0f}",
                signal=r["signal"],
                sma_short=f"SMA{short_n}={r['curr_s']:.2f}",
                sma_long=f"SMA{long_n}={r['curr_l']:.2f}",
            )
            runtime_state.mark_processed_candle(candle_ts_ms=int(r["ts"]), now_epoch_sec=now)

            conn = ensure_db()
            decision_id: int | None = None
            decision_reason_for_trade: str | None = None
            decision_exit_rule_name: str | None = None
            decision_strategy_name_for_trade: str | None = None
            try:
                context = dict(r)
                strategy_name = str(context.pop("strategy", settings.STRATEGY_NAME))
                signal = str(context.pop("signal", "HOLD"))
                reason = str(context.pop("reason", ""))
                exit_ctx = context.get("exit")
                if isinstance(exit_ctx, dict):
                    raw_rule = exit_ctx.get("rule")
                    if raw_rule is not None:
                        decision_exit_rule_name = str(raw_rule)
                decision_reason_for_trade = reason
                decision_strategy_name_for_trade = strategy_name
                candle_ts_raw = context.get("ts")
                market_price_raw = context.get("last_close")
                confidence_raw = context.get("confidence")
                _log_loop_event(
                    logging.INFO,
                    "[RUN] strategy decision",
                    strategy=strategy_name,
                    decision_type=str(context.get("decision_type") or "-"),
                    raw_signal=str(context.get("raw_signal") or context.get("base_signal") or signal),
                    final_signal=signal,
                    entry_blocked=1 if bool(context.get("entry_blocked")) else 0,
                    entry_block_reason=str(context.get("entry_block_reason") or "-"),
                    dust_classification=str(context.get("dust_classification") or context.get("position_gate", {}).get("dust_state") or "-"),
                    effective_flat=1 if bool(context.get("effective_flat")) else 0,
                    raw_qty_open=f"{float(context.get('raw_qty_open', 0.0) or 0.0):.8f}",
                    normalized_exposure_active=1 if bool(context.get("normalized_exposure_active")) else 0,
                    normalized_exposure_qty=f"{float(context.get('normalized_exposure_qty', 0.0) or 0.0):.8f}",
                    reason=reason,
                )
                try:
                    decision_id = record_strategy_decision(
                        conn,
                        decision_ts=int(now * 1000),
                        strategy_name=strategy_name,
                        signal=signal,
                        reason=reason,
                        candle_ts=int(candle_ts_raw) if candle_ts_raw is not None else None,
                        market_price=float(market_price_raw) if market_price_raw is not None else None,
                        confidence=float(confidence_raw) if confidence_raw is not None else None,
                        context=context,
                    )
                    conn.commit()
                except Exception as exc:
                    _log_loop_event(
                        logging.WARNING,
                        "[WARN] strategy decision persistence failed",
                        error=f"{type(exc).__name__}: {exc}",
                        strategy=strategy_name,
                        signal=signal,
                    )
            finally:
                conn.close()

            if r["signal"] not in ("BUY", "SELL"):
                continue

            trade = None
            if settings.MODE == "paper":
                try:
                    trade = paper_execute(
                        r["signal"],
                        r["ts"],
                        r["last_close"],
                        strategy_name=decision_strategy_name_for_trade,
                        decision_id=decision_id,
                        decision_reason=decision_reason_for_trade,
                        exit_rule_name=decision_exit_rule_name,
                    )
                except TypeError as exc:
                    compat_err = str(exc)
                    if "unexpected keyword argument" not in compat_err:
                        raise
                    trade = paper_execute(r["signal"], r["ts"], r["last_close"])
            elif settings.MODE == "live" and broker is not None:
                if r["signal"] == "SELL" and portfolio_qty > 0:
                    suppression_conn = ensure_db()
                    try:
                        suppression_preview = {
                            "normalized_qty": portfolio_qty,
                        }
                        if record_harmless_dust_exit_suppression(
                            conn=suppression_conn,
                            state=runtime_state.snapshot(),
                            signal=r["signal"],
                            side="SELL",
                            requested_qty=portfolio_qty,
                            market_price=float(r["last_close"]),
                            normalized_qty=float(suppression_preview["normalized_qty"]),
                            strategy_name=decision_strategy_name_for_trade,
                            decision_id=decision_id,
                            decision_reason=decision_reason_for_trade,
                            exit_rule_name=decision_exit_rule_name,
                        ):
                            suppression_conn.commit()
                            continue
                    finally:
                        suppression_conn.close()
                try:
                    try:
                        trade = live_execute_signal(
                            broker,
                            r["signal"],
                            r["ts"],
                            r["last_close"],
                            strategy_name=decision_strategy_name_for_trade,
                            decision_id=decision_id,
                            decision_reason=decision_reason_for_trade,
                            exit_rule_name=decision_exit_rule_name,
                        )
                    except TypeError as exc:
                        compat_err = str(exc)
                        if "unexpected keyword argument" not in compat_err:
                            raise
                        trade = live_execute_signal(broker, r["signal"], r["ts"], r["last_close"])
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
                _log_loop_event(
                    logging.INFO,
                    "[RUN] trade_applied",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=trade.get("candle_ts", r["ts"]),
                    signal_ts=trade.get("signal_ts", r["ts"]),
                    client_order_id=trade.get("client_order_id", "-"),
                    exchange_order_id=trade.get("exchange_order_id", "-"),
                    side=trade["side"],
                    qty=f"{trade['qty']:.8f}",
                    submit_qty=f"{float(trade.get('submit_qty', trade['qty'])):.8f}",
                    filled_qty=f"{float(trade.get('filled_qty', trade['qty'])):.8f}",
                    price=f"{trade['price']:,.0f}",
                    fee=f"{trade['fee']:,.0f}",
                    cash=f"{trade['cash']:,.0f}",
                    asset=f"{trade['asset']:.8f}",
                    post_trade_cash=f"{float(trade.get('post_trade_cash', trade['cash'])):,.0f}",
                    post_trade_asset=f"{float(trade.get('post_trade_asset', trade['asset'])):.8f}",
                )

    except KeyboardInterrupt:
        _log_loop_event(
            logging.INFO,
            "[RUN] stopped",
            symbol=settings.PAIR,
            interval=settings.INTERVAL,
            reason="stopped by user (Ctrl+C)",
        )
