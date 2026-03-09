from __future__ import annotations

import json
from dataclasses import dataclass
from threading import Lock

from .db_core import ensure_db
from .oms import OPEN_ORDER_STATUSES


def _clip(v: str | None, max_len: int = 500) -> str | None:
    if v is None:
        return None
    return str(v)[:max_len]


@dataclass
class RuntimeState:
    trading_enabled: bool = True
    halt_new_orders_blocked: bool = False
    halt_reason_code: str | None = None
    halt_state_unresolved: bool = False
    error_count: int = 0
    last_candle_age_sec: float | None = None
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
    startup_gate_reason: str | None = None


_STATE = RuntimeState()
_LOCK = Lock()


def _sync_state_from_persisted_locked() -> None:
    persisted = _read_persisted_state()
    if persisted is None:
        return
    _STATE.trading_enabled = persisted.trading_enabled
    _STATE.halt_new_orders_blocked = persisted.halt_new_orders_blocked
    _STATE.halt_reason_code = persisted.halt_reason_code
    _STATE.halt_state_unresolved = persisted.halt_state_unresolved
    _STATE.error_count = persisted.error_count
    _STATE.last_candle_age_sec = persisted.last_candle_age_sec
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
    _STATE.startup_gate_reason = persisted.startup_gate_reason


def _persist_state(state: RuntimeState) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO bot_health (
                id,
                trading_enabled,
                halt_new_orders_blocked,
                halt_reason_code,
                halt_state_unresolved,
                error_count,
                last_candle_age_sec,
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
                startup_gate_reason,
                updated_ts
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            ON CONFLICT(id) DO UPDATE SET
                trading_enabled=excluded.trading_enabled,
                halt_new_orders_blocked=excluded.halt_new_orders_blocked,
                halt_reason_code=excluded.halt_reason_code,
                halt_state_unresolved=excluded.halt_state_unresolved,
                error_count=excluded.error_count,
                last_candle_age_sec=excluded.last_candle_age_sec,
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
                startup_gate_reason=excluded.startup_gate_reason,
                updated_ts=excluded.updated_ts
            """,
            (
                1 if state.trading_enabled else 0,
                1 if state.halt_new_orders_blocked else 0,
                _clip(state.halt_reason_code),
                1 if state.halt_state_unresolved else 0,
                int(state.error_count),
                state.last_candle_age_sec,
                state.retry_at_epoch_sec,
                _clip(state.last_disable_reason),
                int(state.unresolved_open_order_count),
                state.oldest_unresolved_order_age_sec,
                int(state.recovery_required_count),
                state.last_reconcile_epoch_sec,
                _clip(state.last_reconcile_status),
                _clip(state.last_reconcile_error),
                _clip(state.last_reconcile_reason_code),
                _clip(state.last_reconcile_metadata, max_len=1000),
                state.last_cancel_open_orders_epoch_sec,
                _clip(state.last_cancel_open_orders_trigger),
                _clip(state.last_cancel_open_orders_status),
                _clip(state.last_cancel_open_orders_summary, max_len=1000),
                _clip(state.startup_gate_reason),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _read_persisted_state() -> RuntimeState | None:
    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT
                trading_enabled,
                halt_new_orders_blocked,
                halt_reason_code,
                halt_state_unresolved,
                error_count,
                last_candle_age_sec,
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
                startup_gate_reason
            FROM bot_health
            WHERE id = 1
            """
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return RuntimeState(
        trading_enabled=bool(int(row["trading_enabled"])),
        halt_new_orders_blocked=bool(int(row["halt_new_orders_blocked"])),
        halt_reason_code=(str(row["halt_reason_code"]) if row["halt_reason_code"] is not None else None),
        halt_state_unresolved=bool(int(row["halt_state_unresolved"])),
        error_count=max(0, int(row["error_count"])),
        last_candle_age_sec=(
            float(row["last_candle_age_sec"]) if row["last_candle_age_sec"] is not None else None
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
        startup_gate_reason=(
            str(row["startup_gate_reason"]) if row["startup_gate_reason"] is not None else None
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

    payload = None
    if metadata is not None:
        payload = json.dumps(metadata, ensure_ascii=False, sort_keys=True)

    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.last_reconcile_epoch_sec = float(ts)
        _STATE.last_reconcile_status = "ok" if success else "error"
        _STATE.last_reconcile_error = None if success else (error[:500] if error else "unknown")
        _STATE.last_reconcile_reason_code = _clip(reason_code)
        _STATE.last_reconcile_metadata = _clip(payload, max_len=1000)
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
        _STATE.last_cancel_open_orders_summary = _clip(payload, max_len=1000)
        _persist_state(_STATE)


def set_startup_gate_reason(reason: str | None) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.startup_gate_reason = _clip(reason)
        if reason:
            _STATE.last_reconcile_reason_code = "STARTUP_GATE_BLOCKED"
            _STATE.last_reconcile_metadata = _clip(
                json.dumps({"startup_gate_reason": reason}, ensure_ascii=False, sort_keys=True),
                max_len=1000,
            )
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
        _persist_state(_STATE)


def disable_trading_until(
    epoch_sec: float,
    reason: str | None = None,
    *,
    reason_code: str | None = None,
    halt_new_orders_blocked: bool = False,
    unresolved: bool = False,
) -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.trading_enabled = False
        _STATE.retry_at_epoch_sec = epoch_sec
        _STATE.last_disable_reason = _clip(reason)
        _STATE.halt_reason_code = _clip(reason_code)
        _STATE.halt_new_orders_blocked = bool(halt_new_orders_blocked)
        _STATE.halt_state_unresolved = bool(unresolved)
        _persist_state(_STATE)


def enable_trading() -> None:
    with _LOCK:
        _sync_state_from_persisted_locked()
        _STATE.trading_enabled = True
        _STATE.retry_at_epoch_sec = None
        _STATE.last_disable_reason = None
        _STATE.halt_new_orders_blocked = False
        _STATE.halt_reason_code = None
        _STATE.halt_state_unresolved = False
        _persist_state(_STATE)
