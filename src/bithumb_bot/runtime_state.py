from __future__ import annotations

from dataclasses import dataclass
from threading import Lock


@dataclass
class RuntimeState:
    trading_enabled: bool = True
    error_count: int = 0
    last_candle_age_sec: float | None = None
    retry_at_epoch_sec: float | None = None
    last_disable_reason: str | None = None


_STATE = RuntimeState()
_LOCK = Lock()


def snapshot() -> RuntimeState:
    with _LOCK:
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


def set_last_candle_age_sec(age_sec: float | None) -> None:
    with _LOCK:
        _STATE.last_candle_age_sec = age_sec


def disable_trading_until(epoch_sec: float, reason: str | None = None) -> None:
    with _LOCK:
        _STATE.trading_enabled = False
        _STATE.retry_at_epoch_sec = epoch_sec
        _STATE.last_disable_reason = reason


def enable_trading() -> None:
    with _LOCK:
        _STATE.trading_enabled = True
        _STATE.retry_at_epoch_sec = None
        _STATE.last_disable_reason = None
