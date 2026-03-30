from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _base_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    env["ENV_ROOT"] = "/var/lib/bithumb-bot/env"
    env["RUN_ROOT"] = "/var/lib/bithumb-bot/run"
    env["DATA_ROOT"] = "/var/lib/bithumb-bot/data"
    env["LOG_ROOT"] = "/var/lib/bithumb-bot/logs"
    env["BACKUP_ROOT"] = "/var/lib/bithumb-bot/backup"
    return env


def test_db_path_uses_path_manager_when_unset() -> None:
    env = _base_env()
    env["MODE"] = "paper"
    env.pop("DB_PATH", None)
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.DB_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0
    assert out.stdout.strip() == "/var/lib/bithumb-bot/data/paper/trades/paper.sqlite"


def test_db_path_keeps_explicit_override() -> None:
    env = _base_env()
    env["MODE"] = "paper"
    env["DB_PATH"] = "/tmp/custom.sqlite"
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.DB_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0
    assert out.stdout.strip() == "/tmp/custom.sqlite"


def test_run_lock_uses_path_manager_when_unset() -> None:
    env = _base_env()
    env["MODE"] = "live"
    env["DB_PATH"] = "/var/lib/bithumb-bot/data/live/trades/live.sqlite"
    env.pop("RUN_LOCK_PATH", None)
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.RUN_LOCK_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0
    assert out.stdout.strip() == "/var/lib/bithumb-bot/run/live/bithumb-bot.lock"


def test_run_lock_keeps_explicit_override() -> None:
    env = _base_env()
    env["MODE"] = "live"
    env["DB_PATH"] = "/var/lib/bithumb-bot/data/live/trades/live.sqlite"
    env["RUN_LOCK_PATH"] = "/tmp/live.lock"
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.RUN_LOCK_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0
    assert out.stdout.strip() == "/tmp/live.lock"


def test_live_blocks_repo_relative_db_path() -> None:
    env = _base_env()
    env["MODE"] = "live"
    env["DB_PATH"] = "data/live.sqlite"
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; c.validate_live_mode_preflight(c.settings)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode != 0
    assert "DB_PATH must be outside repository when MODE=live" in (out.stdout + out.stderr)


def test_live_blocks_paper_lock_path() -> None:
    env = _base_env()
    env["MODE"] = "live"
    env["DB_PATH"] = "/var/lib/bithumb-bot/data/live/trades/live.sqlite"
    env["RUN_LOCK_PATH"] = "/var/lib/bithumb-bot/run/paper/bithumb-bot.lock"
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "import bithumb_bot.config as c; c.validate_live_mode_preflight(c.settings)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode != 0
    assert "RUN_LOCK_PATH must not point to a paper-scoped path" in (out.stdout + out.stderr)
