from __future__ import annotations

from pathlib import Path


def _doc() -> str:
    return Path("docs/runbooks/wsl-research-backtest.md").read_text(encoding="utf-8")


def test_wsl_runbook_does_not_claim_max_workers_guarantees_cpu_saturation() -> None:
    doc = _doc().lower()

    forbidden = (
        "max_workers=8 always uses 8 cores",
        "max_workers=8 guarantees",
        "always use 8 cores",
        "always saturates",
    )
    for phrase in forbidden:
        assert phrase not in doc
    assert "`pytest_xdist_workers` must not be used as a substitute for research cli workers" in doc


def test_wsl_runbook_mentions_available_parallel_work_tasks() -> None:
    doc = _doc()

    assert "available_parallel_work_tasks" in doc
    assert "work_task_count < max_workers" in doc
    assert "candidate_count=1" in doc
    assert "12.5%" in doc


def test_wsl_runbook_does_not_recommend_python_backtest_as_official_path() -> None:
    doc = _doc()

    assert "Do not recommend `python backtest.py` as the official backtest path." in doc
    assert "Use `uv run bithumb-bot ...` as the canonical CLI form." in doc
