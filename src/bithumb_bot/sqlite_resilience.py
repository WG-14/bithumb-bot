from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from typing import TypeVar

from .config import settings

T = TypeVar("T")
_LOG = logging.getLogger(__name__)


def configure_connection(conn: sqlite3.Connection) -> None:
    """Apply common SQLite durability/locking settings."""
    conn.execute(f"PRAGMA busy_timeout={max(0, int(settings.DB_BUSY_TIMEOUT_MS))};")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")


def is_lock_error(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "database is locked" in str(exc).lower()


def run_with_locked_db_retry(
    fn: Callable[[], T],
    *,
    context: str = "sqlite_operation",
    retries: int | None = None,
    backoff_ms: int | None = None,
) -> T:
    attempts = max(0, int(settings.DB_LOCK_RETRY_COUNT if retries is None else retries))
    sleep_ms = max(0, int(settings.DB_LOCK_RETRY_BACKOFF_MS if backoff_ms is None else backoff_ms))

    for attempt in range(attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if not is_lock_error(exc) or attempt >= attempts:
                raise
            _LOG.warning(
                "sqlite_lock_retry context=%s attempt=%s/%s error=%s",
                context,
                attempt + 1,
                attempts + 1,
                exc,
            )
            if sleep_ms > 0:
                time.sleep((sleep_ms / 1000.0) * (attempt + 1))

    raise RuntimeError("unreachable")
