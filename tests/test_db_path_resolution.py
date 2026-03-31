from __future__ import annotations

from pathlib import Path

from bithumb_bot import config
import pytest

from bithumb_bot.db import connect
from bithumb_bot.db_core import ensure_db


def test_relative_db_path_is_rejected() -> None:
    with pytest.raises(ValueError) as exc:
        config.resolve_db_path("data/test.sqlite")
    assert "DB_PATH must be an absolute path" in str(exc.value)


def test_absolute_db_path_is_preserved(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(config, "PROJECT_ROOT", project_root)

    absolute_db_path = tmp_path / "abs" / "absolute.sqlite"
    conn = ensure_db(str(absolute_db_path))
    conn.close()

    assert absolute_db_path.exists()


def test_connect_rejects_relative_paths() -> None:
    with pytest.raises(ValueError):
        connect("data/test.sqlite")


def test_settings_db_path_accepts_absolute_path(tmp_path):
    old_db_path = config.settings.DB_PATH
    db_path = (tmp_path / "managed" / "test.sqlite").resolve()
    object.__setattr__(config.settings, "DB_PATH", str(db_path))

    try:
        conn = ensure_db()
        conn.close()
    finally:
        object.__setattr__(config.settings, "DB_PATH", old_db_path)

    assert db_path.exists()


def test_relative_run_lock_path_is_project_root_based(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(config, "PROJECT_ROOT", project_root)

    resolved = config.resolve_run_lock_path("run/paper/instance-a.lock")

    assert resolved == str((project_root / "run" / "paper" / "instance-a.lock").resolve())


def test_default_run_lock_path_is_mode_scoped():
    paper_lock = config.default_run_lock_path("paper")
    live_lock = config.default_run_lock_path("live")
    assert "/paper/" in paper_lock.replace("\\", "/")
    assert "/live/" in live_lock.replace("\\", "/")


def test_live_run_lock_path_rejects_relative_override() -> None:
    with pytest.raises(ValueError) as exc:
        config.resolve_run_lock_path("run/live/instance-a.lock", mode="live")
    assert "RUN_LOCK_PATH must be an absolute path when MODE=live" in str(exc.value)
