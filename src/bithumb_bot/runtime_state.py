from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from .db_core import ensure_db


@dataclass
class RuntimeState:
    trading_enabled: bool = True
    error_count: int = 0
    last_candle_age_sec: float | None = None
    retry_at_epoch_sec: float | None = None
    last_disable_reason: str | None = None


_STATE = RuntimeState()
_LOCK = Lock()


def _persist_state(state: RuntimeState) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO bot_health (
                id,
                trading_enabled,
                error_count,
                last_candle_age_sec,
                retry_at_epoch_sec,
                last_disable_reason,
                updated_ts
            )
            VALUES (1, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            ON CONFLICT(id) DO UPDATE SET
                trading_enabled=excluded.trading_enabled,
                error_count=excluded.error_count,
                last_candle_age_sec=excluded.last_candle_age_sec,
                retry_at_epoch_sec=excluded.retry_at_epoch_sec,
                last_disable_reason=excluded.last_disable_reason,
                updated_ts=excluded.updated_ts
            """,
            (
                1 if state.trading_enabled else 0,
                int(state.error_count),
                state.last_candle_age_sec,
                state.retry_at_epoch_sec,
                state.last_disable_reason,
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
                error_count,
                last_candle_age_sec,
                retry_at_epoch_sec,
                last_disable_reason
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
        error_count=max(0, int(row["error_count"])),
        last_candle_age_sec=(
            float(row["last_candle_age_sec"]) if row["last_candle_age_sec"] is not None else None
        ),
        retry_at_epoch_sec=(float(row["retry_at_epoch_sec"]) if row["retry_at_epoch_sec"] is not None else None),
        last_disable_reason=(
            str(row["last_disable_reason"]) if row["last_disable_reason"] is not None else None
        ),
    )


def snapshot() -> RuntimeState:
    with _LOCK:
        persisted = _read_persisted_state()
        if persisted is not None:
            _STATE.trading_enabled = persisted.trading_enabled
            _STATE.error_count = persisted.error_count
            _STATE.last_candle_age_sec = persisted.last_candle_age_sec
            _STATE.retry_at_epoch_sec = persisted.retry_at_epoch_sec
            _STATE.last_disable_reason = persisted.last_disable_reason
        return RuntimeState(
            trading_enabled=_STATE.trading_enabled,
            error_count=_STATE.error_count,
            last_candle_age_sec=_STATE.last_candle_age_sec,
            retry_at_epoch_sec=_STATE.retry_at_epoch_sec,
            last_disable_reason=_STATE.last_disable_reason,
        )


def set_error_count(n: int) -> None:
    with _LOCK:
        _STATE.error_count = max(0, n)
        _persist_state(_STATE)


def set_last_candle_age_sec(age_sec: float | None) -> None:
    with _LOCK:
        _STATE.last_candle_age_sec = age_sec
        _persist_state(_STATE)


def disable_trading_until(epoch_sec: float, reason: str | None = None) -> None:
    with _LOCK:
        _STATE.trading_enabled = False
        _STATE.retry_at_epoch_sec = epoch_sec
        _STATE.last_disable_reason = reason
        _persist_state(_STATE)


def enable_trading() -> None:
    with _LOCK:
        _STATE.trading_enabled = True
        _STATE.retry_at_epoch_sec = None
        _STATE.last_disable_reason = None
        _persist_state(_STATE)
