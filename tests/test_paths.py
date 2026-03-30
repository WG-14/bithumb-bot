from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager, PathPolicyError


def _set_roots(monkeypatch: pytest.MonkeyPatch, root: Path, mode: str) -> None:
    monkeypatch.setenv("MODE", mode)
    monkeypatch.setenv("ENV_ROOT", str(root / "env"))
    monkeypatch.setenv("RUN_ROOT", str(root / "run"))
    monkeypatch.setenv("DATA_ROOT", str(root / "data"))
    monkeypatch.setenv("LOG_ROOT", str(root / "logs"))
    monkeypatch.setenv("BACKUP_ROOT", str(root / "backup"))


def test_path_manager_builds_mode_scoped_layout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_roots(monkeypatch, tmp_path, "paper")
    pm = PathManager.from_env(project_root=tmp_path / "repo")

    assert pm.run_dir() == tmp_path / "run" / "paper"
    assert pm.data_dir() == tmp_path / "data" / "paper"
    assert pm.log_dir() == tmp_path / "logs" / "paper"
    assert pm.primary_db_path() == tmp_path / "data" / "paper" / "trades" / "paper.sqlite"


def test_path_manager_separates_paper_and_live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_roots(monkeypatch, tmp_path, "paper")
    paper = PathManager.from_env(project_root=tmp_path / "repo")

    _set_roots(monkeypatch, tmp_path, "live")
    live = PathManager.from_env(project_root=tmp_path / "repo")

    assert paper.run_lock_path() != live.run_lock_path()
    assert "/paper/" in str(paper.run_lock_path()).replace("\\", "/")
    assert "/live/" in str(live.run_lock_path()).replace("\\", "/")


def test_path_manager_uses_topic_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_roots(monkeypatch, tmp_path, "live")
    pm = PathManager.from_env(project_root=tmp_path / "repo")

    assert pm.raw_path("market", day="2026-03-30") == tmp_path / "data" / "live" / "raw" / "market" / "market_2026-03-30.jsonl"
    assert pm.derived_path("validation", day="2026-03-30", ext="parquet") == tmp_path / "data" / "live" / "derived" / "validation" / "validation_2026-03-30.parquet"
    assert pm.trade_data_path("fills", day="2026-03-30") == tmp_path / "data" / "live" / "trades" / "fills" / "fills_2026-03-30.jsonl"
    assert pm.report_path("ops", day="2026-03-30") == tmp_path / "data" / "live" / "reports" / "ops" / "ops_2026-03-30.json"
    assert pm.log_path("errors", day="2026-03-30") == tmp_path / "logs" / "live" / "errors" / "errors_2026-03-30.log"


def test_live_blocks_repo_relative_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("ENV_ROOT", "env")
    monkeypatch.setenv("RUN_ROOT", "run")
    monkeypatch.setenv("DATA_ROOT", "data")
    monkeypatch.setenv("LOG_ROOT", "logs")
    monkeypatch.setenv("BACKUP_ROOT", "backup")

    with pytest.raises(PathPolicyError):
        PathManager.from_env(project_root=repo_root)
