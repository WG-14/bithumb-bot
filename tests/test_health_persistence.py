from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db


def _set_tmp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "health.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    return db_path


def test_health_state_written_by_one_component_read_by_another(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.set_error_count(4)
    runtime_state.set_last_candle_age_sec(8.5)
    runtime_state.disable_trading_until(456.0, reason="manual stop")

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT trading_enabled, error_count, last_candle_age_sec, retry_at_epoch_sec, last_disable_reason
            FROM bot_health
            WHERE id = 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert int(row["trading_enabled"]) == 0
    assert int(row["error_count"]) == 4
    assert float(row["last_candle_age_sec"]) == 8.5
    assert float(row["retry_at_epoch_sec"]) == 456.0
    assert str(row["last_disable_reason"]) == "manual stop"


def test_healthcheck_reports_disabled_state_from_persistent_store(tmp_path):
    db_path = _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(1.0)
    runtime_state.disable_trading_until(999.0, reason="recovery required")

    env = dict(os.environ)
    env["DB_PATH"] = str(db_path)
    env["PYTHONPATH"] = str(Path.cwd() / "src")
    env["NOTIFIER_ENABLED"] = "false"

    proc = subprocess.run(
        [sys.executable, "scripts/healthcheck.py"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 1
    assert "[HEALTHCHECK] FAIL" in proc.stdout
    assert "trading disabled" in proc.stdout


def test_healthcheck_healthy_default_path(tmp_path):
    db_path = _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)

    env = dict(os.environ)
    env["DB_PATH"] = str(db_path)
    env["PYTHONPATH"] = str(Path.cwd() / "src")
    env["NOTIFIER_ENABLED"] = "false"

    proc = subprocess.run(
        [sys.executable, "scripts/healthcheck.py"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert "[HEALTHCHECK] OK" in proc.stdout
