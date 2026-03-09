from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.sqlite_resilience import is_lock_error, run_with_locked_db_retry


def test_is_lock_error_matches_sqlite_locked_message() -> None:
    assert is_lock_error(sqlite3.OperationalError("database is locked")) is True
    assert is_lock_error(sqlite3.OperationalError("DATABASE IS LOCKED")) is True
    assert is_lock_error(RuntimeError("database is locked")) is False


def test_run_with_locked_db_retry_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _fn() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    monkeypatch.setattr("bithumb_bot.sqlite_resilience.time.sleep", lambda _: None)
    result = run_with_locked_db_retry(_fn, retries=2, backoff_ms=1)

    assert result == "ok"
    assert calls["count"] == 3


def test_run_with_locked_db_retry_raises_after_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _fn() -> None:
        calls["count"] += 1
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr("bithumb_bot.sqlite_resilience.time.sleep", lambda _: None)

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        run_with_locked_db_retry(_fn, retries=1, backoff_ms=1)

    assert calls["count"] == 2


def test_run_with_locked_db_retry_does_not_retry_non_lock_error() -> None:
    calls = {"count": 0}

    def _fn() -> None:
        calls["count"] += 1
        raise sqlite3.OperationalError("syntax error")

    with pytest.raises(sqlite3.OperationalError, match="syntax error"):
        run_with_locked_db_retry(_fn, retries=5, backoff_ms=1)

    assert calls["count"] == 1


def test_ensure_db_applies_busy_timeout(tmp_path) -> None:
    db_path = tmp_path / "busy_timeout.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "DB_BUSY_TIMEOUT_MS", 4321)

    conn = ensure_db()
    try:
        row = conn.execute("PRAGMA busy_timeout;").fetchone()
    finally:
        conn.close()

    assert int(row[0]) == 4321
