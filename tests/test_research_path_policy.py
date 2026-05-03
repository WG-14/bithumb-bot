from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.research.report_writer import write_research_report


def test_research_entrypoints_do_not_implicitly_load_repo_dotenv() -> None:
    assert "load_dotenv" not in Path("backtest2.py").read_text(encoding="utf-8")
    research_cli = Path("src/bithumb_bot/research/cli.py").read_text(encoding="utf-8")
    assert ".env" not in research_cli


def test_research_outputs_reject_repo_internal_data_root(monkeypatch) -> None:
    monkeypatch.setenv("MODE", "paper")
    monkeypatch.setenv("DATA_ROOT", "research-output")
    for key in ("ENV_ROOT", "RUN_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.delenv(key, raising=False)
    manager = PathManager.from_env(Path.cwd())

    with pytest.raises(PathPolicyError, match="outside repository"):
        write_research_report(
            manager=manager,
            experiment_id="repo_internal",
            report_name="backtest",
            payload={
                "experiment_id": "repo_internal",
                "candidates": [],
                "generated_at": "2026-05-03T00:00:00+00:00",
            },
        )
