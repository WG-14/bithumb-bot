from __future__ import annotations

from pathlib import Path

from bithumb_bot.research.data_plane import build_data_plane_policy
from bithumb_bot.research import validation_protocol
from bithumb_bot.research.validation_protocol import _evaluate_candidates
from tests.factories.research_reports import DeterministicResearchEvaluator
from tests.test_research_backtest_reproducibility import _manifest
from tests.test_research_execution_plan import _manager, _quality_report, _snapshot
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.execution_plan import build_research_execution_plan


def test_data_plane_policy_chooses_cache_when_memory_headroom_exists() -> None:
    policy = build_data_plane_policy(
        manifest_hash="sha256:manifest",
        dataset_hashes={"train": "sha256:dataset"},
        split_names=["train"],
        memory_budget_mb=12 * 1024,
        estimated_total_memory_bytes=1024 * 1024 * 1024,
        effective_max_workers=4,
    ).as_dict()

    assert policy["dataset_cache_budget_mb"] > 0
    assert policy["worker_snapshot_load_policy"] == "worker_local_lazy_cache"
    assert policy["applied_snapshot_load_policy"] == "worker_local_lazy_cache"
    assert "worker_local_lazy_cache_not_implemented" not in policy["disabled_reasons"]


def test_data_plane_policy_records_disabled_reason_when_budget_unknown() -> None:
    policy = build_data_plane_policy(
        manifest_hash="sha256:manifest",
        dataset_hashes={"train": "sha256:dataset"},
        split_names=["train"],
        memory_budget_mb=None,
        estimated_total_memory_bytes=1024,
        effective_max_workers=1,
    ).as_dict()

    assert policy["dataset_cache_budget_mb"] == 0
    assert policy["disabled_reasons"]


def test_worker_context_includes_data_plane_policy(tmp_path: Path, monkeypatch) -> None:
    payload = _manifest()
    payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 2, "work_unit": "candidate_scenario_split"}}
    manifest = parse_manifest(payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    result = _evaluate_candidates(
        manifest=manifest,
        manager=_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        execution_plan=build_research_execution_plan(
            manifest=manifest,
            snapshots=snapshots,
            quality_reports=quality_reports,
            db_path="/tmp/unit.sqlite",
            repository_version="test",
            created_at="2026-06-17T00:00:00+00:00",
        ),
        candidate_evaluator=DeterministicResearchEvaluator(),
    )

    assert result.execution_boundary["data_plane_policy"]["schema_version"] == 1


def test_worker_snapshot_loader_uses_declared_data_plane_policy(monkeypatch) -> None:
    manifest = parse_manifest(_manifest())
    calls: list[tuple[str, str]] = []
    validation_protocol._WORKER_LOCAL_SNAPSHOT_CACHE.clear()

    def fake_load_dataset_split(*, db_path, manifest, split_name):
        calls.append((str(db_path), str(split_name)))
        return _snapshot(split_name)

    monkeypatch.setattr(validation_protocol, "load_dataset_split", fake_load_dataset_split)
    task = {
        "db_path": "/tmp/unit.sqlite",
        "split_names": ("train", "validation"),
        "data_plane_policy": {
            "worker_snapshot_load_policy": "worker_local_lazy_cache",
            "cache_key_material": {
                "manifest_hash": "sha256:manifest",
                "dataset_hashes": {
                    "train": "sha256:train",
                    "validation": "sha256:validation",
                },
                "split_names": ["train", "validation"],
            },
            "disabled_reasons": [],
        },
    }

    snapshots = validation_protocol._load_worker_task_snapshots(task=task, manifest=manifest)
    second_task = {
        "db_path": "/tmp/unit.sqlite",
        "split_names": ("train", "validation"),
        "data_plane_policy": dict(task["data_plane_policy"]),
    }
    second_snapshots = validation_protocol._load_worker_task_snapshots(task=second_task, manifest=manifest)

    assert sorted(snapshots) == ["train", "validation"]
    assert sorted(second_snapshots) == ["train", "validation"]
    assert calls == [("/tmp/unit.sqlite", "train"), ("/tmp/unit.sqlite", "validation")]
    assert task["data_plane_policy"]["applied_snapshot_load_policy"] == "worker_local_lazy_cache"
    assert task["data_plane_policy"]["worker_local_lazy_cache_status"] == "miss_stored"
    assert second_task["data_plane_policy"]["applied_snapshot_load_policy"] == "worker_local_lazy_cache"
    assert second_task["data_plane_policy"]["worker_local_lazy_cache_status"] == "hit"


def test_cache_key_includes_dataset_hash_and_split_name() -> None:
    policy = build_data_plane_policy(
        manifest_hash="sha256:manifest",
        dataset_hashes={"train": "sha256:dataset"},
        split_names=["train"],
        memory_budget_mb=2048,
        estimated_total_memory_bytes=1024,
        effective_max_workers=1,
    ).as_dict()

    key = policy["cache_key_material"]
    assert key["manifest_hash"] == "sha256:manifest"
    assert key["dataset_hashes"]["train"] == "sha256:dataset"
    assert key["split_names"] == ["train"]
