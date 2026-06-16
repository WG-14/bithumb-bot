from __future__ import annotations

import subprocess
from pathlib import Path

from bithumb_bot.research import batch_runner
from bithumb_bot.research.batch_runner import allocate_batch_child_process_budget
from bithumb_bot.research.resource_planner import ResourceContract


def test_batch_runner_divides_total_budget_by_concurrency(monkeypatch) -> None:
    monkeypatch.setenv("BITHUMB_TOTAL_PROCESS_BUDGET", "8")

    budget = allocate_batch_child_process_budget(max_concurrent_manifests=2)

    assert budget["total_process_budget"] == 8
    assert budget["child_process_budget"] <= 4


def test_batch_runner_warns_when_budget_unknown_and_concurrency_gt_one(monkeypatch) -> None:
    monkeypatch.delenv("BITHUMB_TOTAL_PROCESS_BUDGET", raising=False)
    monkeypatch.setattr(
        batch_runner,
        "detect_resource_contract",
        lambda: ResourceContract(
            cpu_limit=None,
            memory_limit_mb=None,
            swap_limit_mb=None,
            detected_source="test",
            env_worker_cap=None,
            total_process_budget=None,
            fallback_reasons=(),
        ),
    )

    budget = allocate_batch_child_process_budget(max_concurrent_manifests=2)

    assert "total_process_budget_unknown_for_concurrent_batch" in budget["fallback_reasons"]


def test_batch_runner_passes_child_worker_budget_env(tmp_path: Path, monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        seen["env"] = kwargs["env"]
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(batch_runner.subprocess, "run", fake_run)
    manifest = type("Manifest", (), {"experiment_id": "batch_env"})()
    manager = type(
        "Manager",
        (),
        {
            "project_root": tmp_path / "repo",
            "data_dir": lambda self: tmp_path / "data",
        },
    )()
    (tmp_path / "repo").mkdir()
    (tmp_path / "data").mkdir()

    batch_runner._run_one_manifest(
        path=tmp_path / "manifest.json",
        manifest=manifest,
        command="research-backtest",
        manager=manager,
        project_root=tmp_path / "repo",
        log_dir=tmp_path / "data" / "logs",
        child_env={
            "BITHUMB_TOTAL_PROCESS_BUDGET": "8",
            "BITHUMB_RESEARCH_MAX_WORKERS": "4",
            "BITHUMB_BATCH_CHILD_WORKER_BUDGET": "4",
        },
    )

    env = seen["env"]
    assert env["BITHUMB_TOTAL_PROCESS_BUDGET"] == "8"
    assert env["BITHUMB_RESEARCH_MAX_WORKERS"] == "4"
    assert env["BITHUMB_BATCH_CHILD_WORKER_BUDGET"] == "4"


def test_batch_summary_records_child_process_budget(monkeypatch) -> None:
    monkeypatch.setenv("BITHUMB_TOTAL_PROCESS_BUDGET", "8")

    budget = allocate_batch_child_process_budget(max_concurrent_manifests=2)

    assert budget["schema_version"] == 1
    assert budget["total_process_budget"] == 8
    assert budget["max_concurrent_manifests"] == 2
    assert budget["child_process_budget"] == 4
