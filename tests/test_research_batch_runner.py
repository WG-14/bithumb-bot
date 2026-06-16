from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from bithumb_bot.research import batch_runner
from bithumb_bot.research.batch_runner import run_research_batch
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _manager


def _write_manifest(path: Path, experiment_id: str) -> None:
    payload = _manifest()
    payload["experiment_id"] = experiment_id
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_research_batch_discovers_manifest_glob(tmp_path, monkeypatch) -> None:
    manifest_dir = tmp_path / "manifests"
    manifest_dir.mkdir()
    for index in range(5):
        _write_manifest(manifest_dir / f"m{index}.json", f"batch_{index}")
    manager = _manager(tmp_path / "runtime", monkeypatch)

    monkeypatch.setattr(
        batch_runner,
        "_run_one_manifest",
        lambda *, path, manifest, command, manager, project_root, log_dir: {
            "manifest_path": str(path),
            "experiment_id": manifest.experiment_id,
            "status": "succeeded",
            "report_path": str(tmp_path / f"{manifest.experiment_id}.json"),
            "log_path": str(tmp_path / f"{manifest.experiment_id}.log"),
            "elapsed_s": 0.0,
            "failure_reason": None,
        },
    )

    result = run_research_batch(
        manifest_glob=str(manifest_dir / "*.json"),
        max_concurrent_manifests=2,
        command="research-backtest",
        fail_fast=False,
        out_path="summary.json",
        manager=manager,
        project_root=Path.cwd(),
    )

    assert result.payload["manifest_count"] == 5
    assert len(result.payload["manifests"]) == 5


def test_research_batch_limits_concurrent_manifests(tmp_path, monkeypatch) -> None:
    manifest_dir = tmp_path / "manifests"
    manifest_dir.mkdir()
    for index in range(5):
        _write_manifest(manifest_dir / f"m{index}.json", f"limited_{index}")
    manager = _manager(tmp_path / "runtime", monkeypatch)
    lock = threading.Lock()
    active = 0
    observed_max = 0

    def fake_run(*, path, manifest, command, manager, project_root, log_dir):
        nonlocal active, observed_max
        with lock:
            active += 1
            observed_max = max(observed_max, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return {
            "manifest_path": str(path),
            "experiment_id": manifest.experiment_id,
            "status": "succeeded",
            "report_path": str(tmp_path / f"{manifest.experiment_id}.json"),
            "log_path": str(tmp_path / f"{manifest.experiment_id}.log"),
            "elapsed_s": 0.0,
            "failure_reason": None,
        }

    monkeypatch.setattr(batch_runner, "_run_one_manifest", fake_run)

    run_research_batch(
        manifest_glob=str(manifest_dir / "*.json"),
        max_concurrent_manifests=2,
        command="research-backtest",
        fail_fast=False,
        out_path="summary.json",
        manager=manager,
        project_root=Path.cwd(),
    )

    assert observed_max <= 2


def test_research_batch_writes_per_manifest_status(tmp_path, monkeypatch) -> None:
    manifest_dir = tmp_path / "manifests"
    manifest_dir.mkdir()
    _write_manifest(manifest_dir / "ok.json", "batch_ok")
    _write_manifest(manifest_dir / "fail.json", "batch_fail")
    manager = _manager(tmp_path / "runtime", monkeypatch)

    def fake_run(*, path, manifest, command, manager, project_root, log_dir):
        failed = manifest.experiment_id.endswith("fail")
        return {
            "manifest_path": str(path),
            "experiment_id": manifest.experiment_id,
            "status": "failed" if failed else "succeeded",
            "report_path": str(tmp_path / f"{manifest.experiment_id}.json"),
            "log_path": str(tmp_path / f"{manifest.experiment_id}.log"),
            "elapsed_s": 0.01,
            "failure_reason": "unit_failure" if failed else None,
        }

    monkeypatch.setattr(batch_runner, "_run_one_manifest", fake_run)
    result = run_research_batch(
        manifest_glob=str(manifest_dir / "*.json"),
        max_concurrent_manifests=2,
        command="research-backtest",
        fail_fast=False,
        out_path="summary.json",
        manager=manager,
        project_root=Path.cwd(),
    )
    persisted = json.loads(result.summary_path.read_text(encoding="utf-8"))

    assert {item["status"] for item in persisted["manifests"]} == {"succeeded", "failed"}
    for item in persisted["manifests"]:
        for key in {"manifest_path", "experiment_id", "status", "report_path", "log_path", "elapsed_s"}:
            assert key in item


def test_research_batch_rejects_duplicate_experiment_ids(tmp_path, monkeypatch) -> None:
    manifest_dir = tmp_path / "manifests"
    manifest_dir.mkdir()
    _write_manifest(manifest_dir / "a.json", "dupe")
    _write_manifest(manifest_dir / "b.json", "dupe")

    with pytest.raises(ValueError, match="duplicate_experiment_ids"):
        run_research_batch(
            manifest_glob=str(manifest_dir / "*.json"),
            max_concurrent_manifests=2,
            command="research-backtest",
            fail_fast=False,
            out_path="summary.json",
            manager=_manager(tmp_path / "runtime", monkeypatch),
            project_root=Path.cwd(),
        )
