from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from bithumb_bot import config
from bithumb_bot.config import settings


@pytest.fixture(autouse=True)
def _restore_settings() -> None:
    old_mode = settings.MODE
    old_db_path = settings.DB_PATH
    yield
    object.__setattr__(settings, "MODE", old_mode)
    object.__setattr__(settings, "DB_PATH", old_db_path)


def _base_env(tmp_path: Path) -> dict[str, str]:
    roots = {
        "ENV_ROOT": tmp_path / "env",
        "RUN_ROOT": tmp_path / "run",
        "DATA_ROOT": tmp_path / "data",
        "LOG_ROOT": tmp_path / "logs",
        "BACKUP_ROOT": tmp_path / "backup",
    }
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    for key, path in roots.items():
        env[key] = str(path.resolve())
    return env


def test_db_path_uses_path_manager_when_unset(tmp_path: Path) -> None:
    env = _base_env(tmp_path)
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
    assert Path(out.stdout.strip()) == Path(env["DATA_ROOT"]) / "paper" / "trades" / "paper.sqlite"


def test_db_path_keeps_explicit_override(tmp_path: Path) -> None:
    env = _base_env(tmp_path)
    env["MODE"] = "paper"
    env["DB_PATH"] = str((tmp_path / "custom.sqlite").resolve())
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.DB_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0
    assert Path(out.stdout.strip()) == Path(env["DB_PATH"])


def test_db_path_rejects_relative_override(tmp_path: Path) -> None:
    env = _base_env(tmp_path)
    env["MODE"] = "paper"
    env["DB_PATH"] = "data/paper.sqlite"
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.DB_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode != 0
    assert "DB_PATH must be an absolute path" in (out.stderr + out.stdout)


def test_run_lock_uses_path_manager_when_unset(tmp_path: Path) -> None:
    env = _base_env(tmp_path)
    env["MODE"] = "live"
    env["DB_PATH"] = str((Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve())
    env.pop("RUN_LOCK_PATH", None)
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.RUN_LOCK_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0
    assert Path(out.stdout.strip()) == Path(env["RUN_ROOT"]) / "live" / "bithumb-bot.lock"


def test_run_lock_keeps_explicit_override(tmp_path: Path) -> None:
    env = _base_env(tmp_path)
    env["MODE"] = "live"
    env["DB_PATH"] = str((Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve())
    env["RUN_LOCK_PATH"] = str((tmp_path / "live.lock").resolve())
    out = subprocess.run(
        [sys.executable, "-c", "import bithumb_bot.config as c; print(c.settings.RUN_LOCK_PATH)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0
    assert Path(out.stdout.strip()) == Path(env["RUN_LOCK_PATH"])


def test_live_blocks_repo_relative_db_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _base_env(tmp_path)
    for key, value in env.items():
        if key in {"ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT"}:
            monkeypatch.setenv(key, value)
    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", str((Path.cwd() / "data/live.sqlite").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DB_PATH must be outside repository when MODE=live" in str(exc.value)


def test_live_blocks_paper_lock_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _base_env(tmp_path)
    for key, value in env.items():
        if key in {"ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT"}:
            monkeypatch.setenv(key, value)
    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("DB_PATH", str((Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve()))
    monkeypatch.setenv("RUN_LOCK_PATH", str((Path(env["RUN_ROOT"]) / "paper" / "bithumb-bot.lock").resolve()))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", str((Path(env["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "RUN_LOCK_PATH must not point to a paper-scoped path" in str(exc.value)
