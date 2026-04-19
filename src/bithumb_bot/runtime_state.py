from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from threading import Lock

from .config import settings
from .db_core import ensure_db
from .oms import OPEN_ORDER_STATUSES
from .reason_codes import HALT_ENTERED, STARTUP_BLOCKED
from .observability import safety_event
from .sqlite_resilience import run_with_locked_db_retry
from .dust import build_dust_display_context, build_position_state_model
from .lifecycle import summarize_position_lots

HALT_POLICY_STAGE = "SAFE_HALT_REVIEW_ONLY"
_HEALTH_SUMMARY_MAX_LEN = 1400


def _clip(v: str | None, max_len: int = 500) -> str | None:
    if v is None:
        return None
    return str(v)[:max_len]


def _json_with_size_limit(
    payload: dict[str, object] | None,
    *,
    max_len: int = _HEALTH_SUMMARY_MAX_LEN,
    preserve_keys: tuple[str, ...] = (),
) -> str | None:
    if payload is None:
        return None
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if len(encoded) <= max_len:
        return encoded

    compact: dict[str, object] = {}
    for key, value in payload.items():
        if isinstance(value, str):
            compact[key] = value[:160]
        else:
            compact[key] = value
    encoded = json.dumps(compact, ensure_ascii=False, sort_keys=True)
    if len(encoded) <= max_len:
        return encoded

    protected_keys = [str(k) for k in preserve_keys]
    protected = set(protected_keys)
    removable_keys = [k for k in sorted(compact.keys()) if k not in protected]
    for key in removable_keys:
        compact.pop(key, None)
        encoded = json.dumps(compact, ensure_ascii=False, sort_keys=True)
        if len(encoded) <= max_len:
            return encoded

    for key in reversed(protected_keys):
        if key not in compact:
            continue
        compact.pop(key, None)
        encoded = json.dumps(compact, ensure_ascii=False, sort_keys=True)
        if len(encoded) <= max_len:
            return encoded

    return "{}"


@dataclass
class RuntimeState:
    trading_enabled: bool = True
    halt_new_orders_blocked: bool = False
    halt_reason_code: str | None = None
    halt_state_unresolved: bool = False
    halt_policy_stage: str = HALT_POLICY_STAGE
    halt_policy_block_new_orders: bool = True
    halt_policy_attempt_cancel_open_orders: bool = True
    halt_policy_auto_liquidate_positions: bool = False
    halt_position_present: bool = False
    halt_open_orders_present: bool = False
    halt_operator_action_required: bool = False
    error_count: int = 0
    last_candle_age_sec: float | None = None
    last_candle_status: str = "waiting_first_sync"
    last_candle_sync_epoch_sec: float | None = None
    last_candle_ts_ms: int | None = None
    last_processed_candle_ts_ms: int | None = None
    last_candle_status_detail: str | None = None
    retry_at_epoch_sec: float | None = None
    last_disable_reason: str | None = None
    unresolved_open_order_count: int = 0
    oldest_unresolved_order_age_sec: float | None = None
    recovery_required_count: int = 0
    last_reconcile_epoch_sec: float | None = None
    last_reconcile_status: str | None = None
    last_reconcile_error: str | None = None
    last_reconcile_reason_code: str | None = None
    last_reconcile_metadata: str | None = None
    last_cancel_open_orders_epoch_sec: float | None = None
    last_cancel_open_orders_trigger: str | None = None
    last_cancel_open_orders_status: str | None = None
    last_cancel_open_orders_summary: str | None = None
    last_flatten_position_epoch_sec: float | None = None
    last_flatten_position_status: str | None = None
    last_flatten_position_summary: str | None = None
    emergency_flatten_blocked: bool = False
    emergency_flatten_block_reason: str | None = None
    startup_gate_reason: str | None = None
    resume_gate_blocked: bool = False
    resume_gate_reason: str | None = None


_STATE = RuntimeState()
_LOCK = Lock()
_LOG = logging.getLogger(__name__)


def _sync_state_from_persisted_locked() -> None:
    persisted = _read_persisted_state()
    if persisted is None:
        return
    _STATE.trading_enabled = persisted.trading_enabled
    _STATE.halt_new_orders_blocked = persisted.halt_new_orders_blocked
    _STATE.halt_reason_code = persisted.halt_reason_code
    _STATE.halt_state_unresolved = persisted.halt_state_unresolved
    _STATE.halt_policy_stage = persisted.halt_policy_stage
    _STATE.halt_policy_block_new_orders = persisted.halt_policy_block_new_orders
    _STATE.halt_policy_attempt_cancel_open_orders = persisted.halt_policy_attempt_cancel_open_orders
    _STATE.halt_policy_auto_liquidate_positions = persisted.halt_policy_auto_liquidate_positions
    _STATE.halt_position_present = persisted.halt_position_present
    _STATE.halt_open_orders_present = persisted.halt_open_orders_present
    _STATE.halt_operator_action_required = persisted.halt_operator_action_required
    _STATE.error_count = persisted.error_count
    _STATE.last_candle_age_sec = persisted.last_candle_age_sec
    _STATE.last_candle_status = persisted.last_candle_status
    _STATE.last_candle_sync_epoch_sec = persisted.last_candle_sync_epoch_sec
    _STATE.last_candle_ts_ms = persisted.last_candle_ts_ms
    _STATE.last_processed_candle_ts_ms = persisted.last_processed_candle_ts_ms
    _STATE.last_candle_status_detail = persisted.last_candle_status_detail
    _STATE.retry_at_epoch_sec = persisted.retry_at_epoch_sec
    _STATE.last_disable_reason = persisted.last_disable_reason
    _STATE.unresolved_open_order_count = persisted.unresolved_open_order_count
    _STATE.oldest_unresolved_order_age_sec = persisted.oldest_unresolved_order_age_sec
    _STATE.recovery_required_count = persisted.recovery_required_count
    _STATE.last_reconcile_epoch_sec = persisted.last_reconcile_epoch_sec
    _STATE.last_reconcile_status = persisted.last_reconcile_status
    _STATE.last_reconcile_error = persisted.last_reconcile_error
    _STATE.last_reconcile_reason_code = persisted.last_reconcile_reason_code
    _STATE.last_reconcile_metadata = persisted.last_reconcile_metadata
    _STATE.last_cancel_open_orders_epoch_sec = persisted.last_cancel_open_orders_epoch_sec
    _STATE.last_cancel_open_orders_trigger = persisted.last_cancel_open_orders_trigger
    _STATE.last_cancel_open_orders_status = persisted.last_cancel_open_orders_status
    _STATE.last_cancel_open_orders_summary = persisted.last_cancel_open_orders_summary
    _STATE.last_flatten_position_epoch_sec = persisted.last_flatten_position_epoch_sec
    _STATE.last_flatten_position_status = persisted.last_flatten_position_status
    _STATE.last_flatten_position_summary = persisted.last_flatten_position_summary
    _STATE.emergency_flatten_blocked = persisted.emergency_flatten_blocked
    _STATE.emergency_flatten_block_reason = persisted.emergency_flatten_block_reason
    _STATE.startup_gate_reason = persisted.startup_gate_reason
    _STATE.resume_gate_blocked = persisted.resume_gate_blocked
    _STATE.resume_gate_reason = persisted.resume_gate_reason


def _persist_state(state: RuntimeState) -> None:
    """Persist runtime health with retry for transient SQLite lock contention.

    This path is shared by runtime loop, healthcheck, and systemd backup windows.
    """
    conn = ensure_db()
    try:
        def _write() -> None:
            conn.execute(
                """
                INSERT INTO bot_health (
                    id,
                    trading_enabled,
                    halt_new_orders_blocked,
                    halt_reason_code,
                    halt_state_unresolved,
                    halt_policy_stage,
                    halt_policy_block_new_orders,
                    halt_policy_attempt_cancel_open_orders,
                    halt_policy_auto_liquidate_positions,
                    halt_position_present,
                    halt_open_orders_present,
                    halt_operator_action_required,
                    error_count,
                    last_candle_age_sec,
                    last_candle_status,
                    last_candle_sync_epoch_sec,
                    last_candle_ts_ms,
                    last_processed_candle_ts_ms,
                    last_candle_status_detail,
                    retry_at_epoch_sec,
                    last_disable_reason,
                    unresolved_open_order_count,
                    oldest_unresolved_order_age_sec,
                    recovery_required_count,
                    last_reconcile_epoch_sec,
                    last_reconcile_status,
                    last_reconcile_error,
                    last_reconcile_reason_code,
                    last_reconcile_metadata,
                    last_cancel_open_orders_epoch_sec,
                    last_cancel_open_orders_trigger,
                    last_cancel_open_orders_status,
                    last_cancel_open_orders_summary,
                    last_flatten_position_epoch_sec,
                    last_flatten_position_status,
                    last_flatten_position_summary,
                    emergency_flatten_blocked,
                    emergency_flatten_block_reason,
                    startup_gate_reason,
                    resume_gate_blocked,
                    resume_gate_reason,
                    updated_ts
                )
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
                ON CONFLICT(id) DO UPDATE SET
                    trading_enabled=excluded.trading_enabled,
                    halt_new_orders_blocked=excluded.halt_new_orders_blocked,
                    halt_reason_code=excluded.halt_reason_code,
                    halt_state_unresolved=excluded.halt_state_unresolved,
                    halt_policy_stage=excluded.halt_policy_stage,
                    halt_policy_block_new_orders=excluded.halt_policy_block_new_orders,
                    halt_policy_attempt_cancel_open_orders=excluded.halt_policy_attempt_cancel_open_orders,
                    halt_policy_auto_liquidate_positions=excluded.halt_policy_auto_liquidate_positions,
                    halt_position_present=excluded.halt_position_present,
                    halt_open_orders_present=excluded.halt_open_orders_present,
                    halt_operator_action_required=excluded.halt_operator_action_required,
                    error_count=excluded.error_count,
                    last_candle_age_sec=excluded.last_candle_age_sec,
                    last_candle_status=excluded.last_candle_status,
                    last_candle_sync_epoch_sec=excluded.last_candle_sync_epoch_sec,
                    last_candle_ts_ms=excluded.last_candle_ts_ms,
                    last_processed_candle_ts_ms=excluded.last_processed_candle_ts_ms,
                    last_candle_status_detail=excluded.last_candle_status_detail,
                    retry_at_epoch_sec=excluded.retry_at_epoch_sec,
                    last_disable_reason=excluded.last_disable_reason,
                    unresolved_open_order_count=excluded.unresolved_open_order_count,
                    oldest_unresolved_order_age_sec=excluded.oldest_unresolved_order_age_sec,
                    recovery_required_count=excluded.recovery_required_count,
                    last_reconcile_epoch_sec=excluded.last_reconcile_epoch_sec,
                    last_reconcile_status=excluded.last_reconcile_status,
                    last_reconcile_error=excluded.last_reconcile_error,
                    last_reconcile_reason_code=excluded.last_reconcile_reason_code,
                    last_reconcile_metadata=excluded.last_reconcile_metadata,
                    last_cancel_open_orders_epoch_sec=excluded.last_cancel_open_orders_epoch_sec,
                    last_cancel_open_orders_trigger=excluded.last_cancel_open_orders_trigger,
                    last_cancel_open_orders_status=excluded.last_cancel_open_orders_status,
                    last_cancel_open_orders_summary=excluded.last_cancel_open_orders_summary,
                    last_flatten_position_epoch_sec=excluded.last_flatten_position_epoch_sec,
                    last_flatten_position_status=excluded.last_flatten_position_status,
                    last_flatten_position_summary=excluded.last_flatten_position_summary,
                    emergency_flatten_blocked=excluded.emergency_flatten_blocked,
                    emergency_flatten_block_reason=excluded.emergency_flatten_block_reason,
                    startup_gate_reason=excluded.startup_gate_reason,
                    resume_gate_blocked=excluded.resume_gate_blocked,
                    resume_gate_reason=excluded.resume_gate_reason,
                    updated_ts=excluded.updated_ts
                """,
                (
                    1 if state.trading_enabled else 0,
                    1 if state.halt_new_orders_blocked else 0,
                    _clip(state.halt_reason_code),
                    1 if state.halt_state_unresolved else 0,
                    _clip(state.halt_policy_stage),
                    1 if state.halt_policy_block_new_orders else 0,
                    1 if state.halt_policy_attempt_cancel_open_orders else 0,
                    1 if state.halt_policy_auto_liquidate_positions else 0,
                    1 if state.halt_position_present else 0,
                    1 if state.halt_open_orders_present else 0,
                    1 if state.halt_operator_action_required else 0,
                    int(state.error_count),
                    state.last_candle_age_sec,
                    _clip(state.last_candle_status),
                    state.last_candle_sync_epoch_sec,
                    state.last_candle_ts_ms,
                    state.last_processed_candle_ts_ms,
                    _clip(state.last_candle_status_detail),
                    state.retry_at_epoch_sec,
                    _clip(state.last_disable_reason),
                    int(state.unresolved_open_order_count),
                    state.oldest_unresolved_order_age_sec,
                    int(state.recovery_required_count),
                    state.last_reconcile_epoch_sec,
                    _clip(state.last_reconcile_status),
                    _clip(state.last_reconcile_error),
                    _clip(state.last_reconcile_reason_code),
                    _clip(state.last_reconcile_metadata, max_len=_HEALTH_SUMMARY_MAX_LEN),
                    state.last_cancel_open_orders_epoch_sec,
                    _clip(state.last_cancel_open_orders_trigger),
                    _clip(state.last_cancel_open_orders_status),
                    _clip(state.last_cancel_open_orders_summary, max_len=_HEALTH_SUMMARY_MAX_LEN),
                    state.last_flatten_position_epoch_sec,
                    _clip(state.last_flatten_position_status),
                    _clip(state.last_flatten_position_summary, max_len=_HEALTH_SUMMARY_MAX_LEN),
                    1 if state.emergency_flatten_blocked else 0,
                    _clip(state.emergency_flatten_block_reason),
                    _clip(state.startup_gate_reason),
                    1 if state.resume_gate_blocked else 0,
                    _clip(state.resume_gate_reason),
                ),
            )
            conn.commit()

        run_with_locked_db_retry(_write, context="runtime_state.persist")
    finally:
        conn.close()


def _read_persisted_state() -> RuntimeState | None:
    conn = ensure_db()
    try:
        def _read():
            return conn.execute(
                """
                SELECT
                    trading_enabled,
                    halt_new_orders_blocked,
                    halt_reason_code,
                    halt_state_unresolved,
                    halt_policy_stage,
                    halt_policy_block_new_orders,
                    halt_policy_attempt_cancel_open_orders,
                    halt_policy_auto_liquidate_positions,
                    halt_position_present,
                    halt_open_orders_present,
                    halt_operator_action_required,
                    error_count,
                    last_candle_age_sec,
                    last_candle_status,
                    last_candle_sync_epoch_sec,
                    last_candle_ts_ms,
                    last_processed_candle_ts_ms,
                    last_candle_status_detail,
                    retry_at_epoch_sec,
                    last_disable_reason,
                    unresolved_open_order_count,
                    oldest_unresolved_order_age_sec,
                    recovery_required_count,
                    last_reconcile_epoch_sec,
                    last_reconcile_status,
                    last_reconcile_error,
                    last_reconcile_reason_code,
                    last_reconcile_metadata,
                    last_cancel_open_orders_epoch_sec,
                    last_cancel_open_orders_trigger,
                    last_cancel_open_orders_status,
                    last_cancel_open_orders_summary,
                    last_flatten_position_epoch_sec,
                    last_flatten_position_status,
                    last_flatten_position_summary,
                    emergency_flatten_blocked,
                    emergency_flatten_block_reason,
                    startup_gate_reason,
                    resume_gate_blocked,
                    resume_gate_reason
                FROM bot_health
                WHERE id = 1
                """
            ).fetchone()

        row = run_with_locked_db_retry(_read, context="runtime_state.read")
    finally:
        conn.close()

    if row is None:
        return None

    return RuntimeState(
        trading_enabled=bool(int(row["trading_enabled"])),
        halt_new_orders_blocked=bool(int(row["halt_new_orders_blocked"])),
        halt_reason_code=(str(row["halt_reason_code"]) if row["halt_reason_code"] is not None else None),
        halt_state_unresolved=bool(int(row["halt_state_unresolved"])),
        halt_policy_stage=(str(row["halt_policy_stage"]) if row["halt_policy_stage"] is not None else HALT_POLICY_STAGE),
        halt_policy_block_new_orders=bool(int(row["halt_policy_block_new_orders"])),
        halt_policy_attempt_cancel_open_orders=bool(int(row["halt_policy_attempt_cancel_open_orders"])),
        halt_policy_auto_liquidate_positions=bool(int(row["halt_policy_auto_liquidate_positions"])),
        halt_position_present=bool(int(row["halt_position_present"])),
        halt_open_orders_present=bool(int(row["halt_open_orders_present"])),
        halt_operator_action_required=bool(int(row["halt_operator_action_required"])),
        error_count=max(0, int(row["error_count"])),
        last_candle_age_sec=(
            float(row["last_candle_age_sec"]) if row["last_candle_age_sec"] is not None else None
        ),
        last_candle_status=(
            str(row["last_candle_status"])
            if row["last_candle_status"] is not None
            else "waiting_first_sync"
        ),
        last_candle_sync_epoch_sec=(
            float(row["last_candle_sync_epoch_sec"])
            if row["last_candle_sync_epoch_sec"] is not None
            else None
        ),
        last_candle_ts_ms=(
            int(row["last_candle_ts_ms"]) if row["last_candle_ts_ms"] is not None else None
        ),
        last_processed_candle_ts_ms=(
            int(row["last_processed_candle_ts_ms"])
            if row["last_processed_candle_ts_ms"] is not None
            else None
        ),
        last_candle_status_detail=(
            str(row["last_candle_status_detail"])
            if row["last_candle_status_detail"] is not None
            else None
        ),
        retry_at_epoch_sec=(float(row["retry_at_epoch_sec"]) if row["retry_at_epoch_sec"] is not None else None),
        last_disable_reason=(
            str(row["last_disable_reason"]) if row["last_disable_reason"] is not None else None
        ),
        unresolved_open_order_count=max(0, int(row["unresolved_open_order_count"])),
        oldest_unresolved_order_age_sec=(
            float(row["oldest_unresolved_order_age_sec"])
            if row["oldest_unresolved_order_age_sec"] is not None
            else None
        ),
        recovery_required_count=max(0, int(row["recovery_required_count"])),
        last_reconcile_epoch_sec=(
            float(row["last_reconcile_epoch_sec"]) if row["last_reconcile_epoch_sec"] is not None else None
        ),
        last_reconcile_status=(
            str(row["last_reconcile_status"])
            if row["last_reconcile_status"] is not None
            else None
        ),
        last_reconcile_error=(
            str(row["last_reconcile_error"])
            if row["last_reconcile_error"] is not None
            else None
        ),
        last_reconcile_reason_code=(
            str(row["last_reconcile_reason_code"])
            if row["last_reconcile_reason_code"] is not None
            else None
        ),
        last_reconcile_metadata=(
            str(row["last_reconcile_metadata"])
            if row["last_reconcile_metadata"] is not None
            else None
        ),
        last_cancel_open_orders_epoch_sec=(
            float(row["last_cancel_open_orders_epoch_sec"])
            if row["last_cancel_open_orders_epoch_sec"] is not None
            else None
        ),
        last_cancel_open_orders_trigger=(
            str(row["last_cancel_open_orders_trigger"])
            if row["last_cancel_open_orders_trigger"] is not None
            else None
        ),
        last_cancel_open_orders_status=(
            str(row["last_cancel_open_orders_status"])
            if row["last_cancel_open_orders_status"] is not None
            else None
        ),
        last_cancel_open_orders_summary=(
            str(row["last_cancel_open_orders_summary"])
            if row["last_cancel_open_orders_summary"] is not None
            else None
        ),
        last_flatten_position_epoch_sec=(
            float(row["last_flatten_position_epoch_sec"])
            if row["last_flatten_position_epoch_sec"] is not None
            else None
        ),
        last_flatten_position_status=(
            str(row["last_flatten_position_status"])
            if row["last_flatten_position_status"] is not None
            else None
        ),
        last_flatten_position_summary=(
            str(row["last_flatten_position_summary"])
            if row["last_flatten_position_summary"] is not None
            else None
        ),
        emergency_flatten_blocked=bool(int(row["emergency_flatten_blocked"])),
        emergency_flatten_block_reason=(
            str(row["emergency_flatten_block_reason"])
            if row["emergency_flatten_block_reason"] is not None
            else None
        ),
        startup_gate_reason=(
            str(row["startup_gate_reason"]) if row["startup_gate_reason"] is not None else None
        ),
        resume_gate_blocked=bool(int(row["resume_gate_blocked"])),
        resume_gate_reason=(
            str(row["resume_gate_reason"]) if row["resume_gate_reason"] is not None else None
        ),
    )


def snapshot() -> RuntimeState:
    with _LOCK:
        _sync_state_from_persisted_locked()
        return RuntimeState(**_STATE.__dict__)


def refresh_open_order_health(now_epoch_sec: float | None = None) -> None:
    now_sec = now_epoch_sec
    if now_sec is None:
        import time

        now_sec = time.time()

    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
        unresolved_row = conn.execute(
            f"""
            SELECT COUNT(*) AS unresolved_count, MIN(created_ts) AS oldest_created_ts
            FROM orders
            WHERE status IN ({placeholders})
            """,
            OPEN_ORDER_STATUSES,
        ).fetchone()
        recovery_row = conn.execute(
            "SELECT COUNT(*) AS recovery_required_count FROM orders WHERE status='RECOVERY_REQUIRED'"
        ).fetchone()
    finally:
        conn.close()

    unresolved_count = int(unresolved_row["unresolved_count"] if unresolved_row else 0)
    recovery_required_count = int(recovery_row["recovery_required_count"] if recovery_row else 0)
    oldest_created_ts = unresolved_row["oldest_created_ts"] if unresolved_row else None
    oldest_age_sec = None
    if unresolved_count > 0 and oldest_created_ts is not None:
        oldest_age_sec = max(0.0, float(now_sec) - (float(oldest_created_ts) / 1000.0))

    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.unresolved_open_order_count = max(0, unresolved_count)
        _STATE.oldest_unresolved_order_age_sec = oldest_age_sec
        _STATE.recovery_required_count = max(0, recovery_required_count)
        _persist_state(_STATE)


def record_reconcile_result(
    *,
    success: bool,
    error: str | None = None,
    reason_code: str | None = None,
    metadata: dict[str, int | float | str | bool | None] | None = None,
    now_epoch_sec: float | None = None,
) -> None:
    ts = now_epoch_sec
    if ts is None:
        import time

        ts = time.time()

    payload = _json_with_size_limit(
        metadata,
        max_len=_HEALTH_SUMMARY_MAX_LEN,
        preserve_keys=(
            "balance_source",
            "balance_observed_ts_ms",
            "broker_read_journal",
            "balance_split_mismatch_count",
            "balance_split_mismatch_summary",
            "external_cash_adjustment_count",
            "external_cash_adjustment_delta_krw",
            "external_cash_adjustment_total_krw",
            "external_cash_adjustment_event_ts",
            "external_cash_adjustment_created",
            "external_cash_adjustment_key",
            "external_cash_adjustment_reason",
            "external_cash_adjustment_residual_krw",
            "material_zero_fee_fill_count",
            "material_zero_fee_fill_notional_krw",
            "material_zero_fee_fill_latest_ts",
            "fee_gap_recovery_required",
            "observed_fill_count",
            "fee_pending_fill_count",
            "fee_pending_recovery_required",
            "fee_pending_latest_fill_ts",
            "fee_pending_latest_fee_status",
            "fee_pending_latest_fill_id",
            "fee_pending_operator_next_action",
            "fee_gap_adjustment_count",
            "fee_gap_adjustment_total_krw",
            "fee_gap_adjustment_latest_event_ts",
            "dust_state",
            "dust_classification",
            "dust_residual_present",
            "dust_residual_allow_resume",
            "dust_policy_reason",
            "dust_residual_summary",
            "dust_state_label",
            "dust_effective_flat",
            "dust_partial_flatten_recent",
            "dust_partial_flatten_reason",
            "unresolved_open_order_count",
            "submit_unknown_count",
            "recovery_required_count",
            "dust_qty_gap_tolerance",
            "dust_qty_gap_small",
            "dust_broker_qty",
            "dust_local_qty",
            "dust_delta_qty",
            "dust_min_qty",
            "dust_min_notional_krw",
            "dust_latest_price",
            "dust_broker_notional_krw",
            "dust_local_notional_krw",
            "dust_broker_qty_is_dust",
            "dust_local_qty_is_dust",
            "dust_broker_notional_is_dust",
            "dust_local_notional_is_dust",
            "recovery_disposition",
            "recovery_progress_state",
            "recovery_classification_reason",
        ),
    )

    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.last_reconcile_epoch_sec = float(ts)
        _STATE.last_reconcile_status = "ok" if success else "error"
        _STATE.last_reconcile_error = None if success else (error[:500] if error else "unknown")
        _STATE.last_reconcile_reason_code = _clip(reason_code)
        _STATE.last_reconcile_metadata = payload
        _persist_state(_STATE)


def record_cancel_open_orders_result(
    *,
    trigger: str,
    status: str,
    summary: dict[str, int | list[str]] | None = None,
    now_epoch_sec: float | None = None,
) -> None:
    ts = now_epoch_sec
    if ts is None:
        import time

        ts = time.time()

    payload = None
    if summary is not None:
        payload = json.dumps(summary, ensure_ascii=False, sort_keys=True)

    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.last_cancel_open_orders_epoch_sec = float(ts)
        _STATE.last_cancel_open_orders_trigger = _clip(trigger)
        _STATE.last_cancel_open_orders_status = _clip(status)
        _STATE.last_cancel_open_orders_summary = _clip(payload, max_len=_HEALTH_SUMMARY_MAX_LEN)
        _persist_state(_STATE)


def record_flatten_position_result(
    *,
    status: str,
    summary: dict[str, int | float | str | bool | None] | None = None,
    now_epoch_sec: float | None = None,
) -> None:
    ts = now_epoch_sec
    if ts is None:
        import time

        ts = time.time()

    payload = None
    if summary is not None:
        payload = json.dumps(summary, ensure_ascii=False, sort_keys=True)

    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.last_flatten_position_epoch_sec = float(ts)
        _STATE.last_flatten_position_status = _clip(status)
        _STATE.last_flatten_position_summary = _clip(payload, max_len=_HEALTH_SUMMARY_MAX_LEN)
        if str(status) in {"failed", "started"}:
            _STATE.emergency_flatten_blocked = True
            _STATE.emergency_flatten_block_reason = _clip(
                "emergency flatten unresolved: "
                f"status={status} summary={payload or '-'}"
            )
        elif str(status) in {"submitted", "no_position"}:
            _STATE.emergency_flatten_blocked = False
            _STATE.emergency_flatten_block_reason = None
        _persist_state(_STATE)


def get_emergency_flatten_blocker() -> str | None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        if not _STATE.emergency_flatten_blocked:
            return None
        return _STATE.emergency_flatten_block_reason or "emergency flatten unresolved"


def set_startup_gate_reason(reason: str | None) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.startup_gate_reason = _clip(reason)
        existing_metadata: dict[str, object] = {}
        if _STATE.last_reconcile_metadata:
            try:
                decoded = json.loads(_STATE.last_reconcile_metadata)
            except (TypeError, ValueError, json.JSONDecodeError):
                decoded = None
            if isinstance(decoded, dict):
                existing_metadata = dict(decoded)
        if reason:
            _STATE.last_reconcile_reason_code = "STARTUP_GATE_BLOCKED"
            existing_metadata["startup_gate_reason"] = _STATE.startup_gate_reason
            existing_metadata["startup_gate_blocked"] = True
            _STATE.last_reconcile_metadata = _json_with_size_limit(
                existing_metadata,
                max_len=_HEALTH_SUMMARY_MAX_LEN,
                preserve_keys=(
                    "startup_gate_reason",
                    "startup_gate_blocked",
                    "balance_source",
                    "balance_observed_ts_ms",
                    "broker_read_journal",
                    "balance_split_mismatch_count",
                    "balance_split_mismatch_summary",
                    "external_cash_adjustment_count",
                    "external_cash_adjustment_delta_krw",
                    "external_cash_adjustment_total_krw",
                    "external_cash_adjustment_event_ts",
                    "external_cash_adjustment_created",
                    "external_cash_adjustment_key",
                    "external_cash_adjustment_reason",
                    "external_cash_adjustment_residual_krw",
                    "material_zero_fee_fill_count",
                    "material_zero_fee_fill_notional_krw",
                    "material_zero_fee_fill_latest_ts",
                    "fee_gap_recovery_required",
                    "fee_gap_adjustment_count",
                    "fee_gap_adjustment_total_krw",
                    "fee_gap_adjustment_latest_event_ts",
                    "dust_state",
                    "dust_classification",
                    "dust_residual_present",
                    "dust_residual_allow_resume",
                    "dust_policy_reason",
                    "dust_residual_summary",
                    "dust_state_label",
                    "dust_effective_flat",
                    "dust_partial_flatten_recent",
                    "dust_partial_flatten_reason",
                    "dust_qty_gap_tolerance",
                    "dust_qty_gap_small",
                    "dust_broker_qty",
                    "dust_local_qty",
                    "dust_delta_qty",
                    "dust_min_qty",
                    "dust_min_notional_krw",
                    "dust_latest_price",
                    "dust_broker_notional_krw",
                    "dust_local_notional_krw",
                    "dust_broker_qty_is_dust",
                    "dust_local_qty_is_dust",
                    "dust_broker_notional_is_dust",
                    "dust_local_notional_is_dust",
                    "recovery_disposition",
                    "recovery_progress_state",
                    "recovery_classification_reason",
                ),
            )
        elif _STATE.last_reconcile_reason_code == "STARTUP_GATE_BLOCKED":
            _STATE.last_reconcile_reason_code = None
            existing_metadata.pop("startup_gate_reason", None)
            existing_metadata.pop("startup_gate_blocked", None)
            _STATE.last_reconcile_metadata = (
                _json_with_size_limit(existing_metadata, max_len=_HEALTH_SUMMARY_MAX_LEN)
                if existing_metadata
                else None
            )
        _persist_state(_STATE)


def set_resume_gate(*, blocked: bool, reason: str | None) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.resume_gate_blocked = bool(blocked)
        _STATE.resume_gate_reason = _clip(reason) if blocked else None
        _persist_state(_STATE)


def set_error_count(n: int) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.error_count = max(0, n)
        _persist_state(_STATE)


def set_last_candle_age_sec(age_sec: float | None) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.last_candle_age_sec = age_sec
        if age_sec is None:
            _STATE.last_candle_status = "age_not_available"
            _STATE.last_candle_status_detail = "candle age value was cleared"
        else:
            _STATE.last_candle_status = "ok"
            _STATE.last_candle_status_detail = None
        _persist_state(_STATE)


def set_last_candle_observation(
    *,
    status: str,
    age_sec: float | None,
    sync_epoch_sec: float | None,
    candle_ts_ms: int | None,
    detail: str | None = None,
) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.last_candle_status = _clip(status) or "unknown"
        _STATE.last_candle_age_sec = age_sec
        _STATE.last_candle_sync_epoch_sec = sync_epoch_sec
        _STATE.last_candle_ts_ms = candle_ts_ms
        _STATE.last_candle_status_detail = _clip(detail)
        _persist_state(_STATE)




def mark_processed_candle(*, candle_ts_ms: int, now_epoch_sec: float | None = None) -> None:
    ts = now_epoch_sec
    if ts is None:
        import time

        ts = time.time()

    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.last_processed_candle_ts_ms = int(candle_ts_ms)
        _STATE.last_candle_status = "processed_closed"
        _STATE.last_candle_status_detail = _clip(
            f"processed closed candle ts={int(candle_ts_ms)} at epoch={float(ts):.3f}"
        )
        _persist_state(_STATE)


def disable_trading_until(
    epoch_sec: float,
    reason: str | None = None,
    *,
    reason_code: str | None = None,
    halt_new_orders_blocked: bool = False,
    unresolved: bool = False,
    attempt_flatten: bool = False,
) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.trading_enabled = False
        _STATE.retry_at_epoch_sec = epoch_sec
        _STATE.last_disable_reason = _clip(reason)
        _STATE.halt_reason_code = _clip(reason_code)
        _STATE.halt_new_orders_blocked = bool(halt_new_orders_blocked)
        _STATE.halt_state_unresolved = bool(unresolved)
        _STATE.halt_policy_stage = HALT_POLICY_STAGE
        _STATE.halt_policy_block_new_orders = True
        _STATE.halt_policy_attempt_cancel_open_orders = True
        _STATE.halt_policy_auto_liquidate_positions = bool(attempt_flatten)
        _STATE.halt_position_present = False
        _STATE.halt_open_orders_present = False
        _STATE.halt_operator_action_required = bool(unresolved)
        if halt_new_orders_blocked:
            conn = ensure_db()
            try:
                open_row = conn.execute(
                    "SELECT COUNT(*) AS open_count FROM orders WHERE status IN ({})".format(",".join("?" for _ in OPEN_ORDER_STATUSES)),
                    OPEN_ORDER_STATUSES,
                ).fetchone()
                portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
                lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
            finally:
                conn.close()
            open_count = int(open_row["open_count"] if open_row else 0)
            asset_qty = float(portfolio_row["asset_qty"] if portfolio_row is not None else 0.0)
            dust_context = build_dust_display_context(_STATE.last_reconcile_metadata)
            position_state = build_position_state_model(
                raw_qty_open=asset_qty,
                metadata_raw=_STATE.last_reconcile_metadata,
                raw_total_asset_qty=max(
                    asset_qty,
                    float(lot_snapshot.raw_total_asset_qty),
                    float(dust_context.raw_holdings.broker_qty),
                ),
                open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
                dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
                open_lot_count=int(lot_snapshot.open_lot_count),
                dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
            )
            _STATE.halt_open_orders_present = open_count > 0
            _STATE.halt_position_present = bool(position_state.normalized_exposure.has_any_position_residue)
            _STATE.halt_operator_action_required = bool(
                unresolved
                or _STATE.halt_open_orders_present
                or _STATE.halt_position_present
                or bool(position_state.normalized_exposure.has_dust_only_remainder)
            )
        _persist_state(_STATE)


def enter_halt(
    *,
    reason_code: str,
    reason: str,
    unresolved: bool,
    attempt_flatten: bool = False,
) -> None:
    disable_trading_until(
        float("inf"),
        reason=reason,
        reason_code=reason_code,
        halt_new_orders_blocked=True,
        unresolved=unresolved,
        attempt_flatten=attempt_flatten,
    )
    _LOG.error(
        safety_event(
            "trading_halted",
            reason_code=HALT_ENTERED,
            state_to="HALTED",
            unresolved=int(unresolved),
            halt_detail_code=reason_code,
            reason=reason,
        )
    )


def enable_trading() -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.trading_enabled = True
        _STATE.retry_at_epoch_sec = None
        _STATE.last_disable_reason = None
        _STATE.halt_new_orders_blocked = False
        _STATE.halt_reason_code = None
        _STATE.halt_state_unresolved = False
        _STATE.halt_position_present = False
        _STATE.halt_open_orders_present = False
        _STATE.halt_operator_action_required = False
        _STATE.resume_gate_blocked = False
        _STATE.resume_gate_reason = None
        _persist_state(_STATE)
