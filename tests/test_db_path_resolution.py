from __future__ import annotations

from pathlib import Path

from bithumb_bot import config
from bithumb_bot.db import connect
from bithumb_bot.db_core import ensure_db


def test_relative_db_path_is_project_root_based_across_cwd(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(config, "PROJECT_ROOT", project_root)

    first_cwd = tmp_path / "cwd-a"
    second_cwd = tmp_path / "cwd-b"
    first_cwd.mkdir()
    second_cwd.mkdir()

    monkeypatch.chdir(first_cwd)
    conn = ensure_db("data/test.sqlite")
    conn.close()

    monkeypatch.chdir(second_cwd)
    conn = ensure_db("data/test.sqlite")
    conn.close()

    assert (project_root / "data" / "test.sqlite").exists()
    assert not (first_cwd / "data" / "test.sqlite").exists()
    assert not (second_cwd / "data" / "test.sqlite").exists()


def test_absolute_db_path_is_preserved(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(config, "PROJECT_ROOT", project_root)

    absolute_db_path = tmp_path / "abs" / "absolute.sqlite"
    conn = ensure_db(str(absolute_db_path))
    conn.close()

    assert absolute_db_path.exists()


def test_connect_uses_same_resolution_rule_for_relative_paths(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(config, "PROJECT_ROOT", project_root)

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)

    conn = connect("data/test.sqlite")
    conn.close()

    assert (project_root / "data" / "test.sqlite").exists()
    assert not (work_dir / "data" / "test.sqlite").exists()


def test_settings_db_path_is_resolved_from_project_root(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(config, "PROJECT_ROOT", project_root)
    old_db_path = config.settings.DB_PATH
    object.__setattr__(config.settings, "DB_PATH", config.resolve_db_path("data/test.sqlite"))

    try:
        conn = ensure_db()
        conn.close()
    finally:
        object.__setattr__(config.settings, "DB_PATH", old_db_path)

    assert (project_root / "data" / "test.sqlite").exists()


def test_relative_run_lock_path_is_project_root_based(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(config, "PROJECT_ROOT", project_root)

    resolved = config.resolve_run_lock_path("data/locks/instance-a.lock")

    assert resolved == str((project_root / "data" / "locks" / "instance-a.lock").resolve())


def test_default_run_lock_path_is_mode_scoped():
    assert config.default_run_lock_path("paper") == "data/locks/bithumb-bot-run-paper.lock"
    assert config.default_run_lock_path("live") == "data/locks/bithumb-bot-run-live.lock"
