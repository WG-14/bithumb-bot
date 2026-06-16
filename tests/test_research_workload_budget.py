from __future__ import annotations

import json
import subprocess
import sys
import ast
from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.research.artifact_store import (
    ArtifactBudget,
    ArtifactBudgetExceeded,
    ArtifactStore,
    ResearchArtifactContext,
)
from bithumb_bot.research.audit_trail import AuditTraceScope, AuditTrailPolicy, write_trace_manifest
from bithumb_bot.research.experiment_manifest import ResearchResourceLimits, parse_manifest
from bithumb_bot.research.execution_plan import _estimated_artifact_bytes, build_research_execution_plan
from bithumb_bot.research.experiment_registry import (
    EXPERIMENT_REGISTRY_BUDGET_POLICY,
    reserve_research_attempt,
)
from bithumb_bot.research.family_registry import (
    FAMILY_TRIAL_REGISTRY_BUDGET_POLICY,
    append_family_trial_registry_row,
)
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.report_writer import write_research_report
from bithumb_bot.research.return_panel import write_candidate_return_panel
from bithumb_bot.research.statistical_selection import write_statistical_selection_evidence
from bithumb_bot.research.validation_protocol import _append_candidate_event
from bithumb_bot.research import validation_protocol
from tests.factories.research_reports import DeterministicResearchEvaluator
from tests.test_research_execution_plan import _quality_report, _snapshot
from tests.policy.research_runner_policy import load_inventory, research_workload_summary
from tests.test_research_backtest_reproducibility import _manifest


def _paper_manager(tmp_path: Path, monkeypatch) -> PathManager:
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    monkeypatch.setenv("MODE", "paper")
    return PathManager.from_env(Path.cwd())


def test_research_resource_artifact_limits_are_hash_material() -> None:
    base = ResearchResourceLimits(max_artifact_bytes=1024)
    changed = ResearchResourceLimits(max_artifact_bytes=2048)

    assert base.as_dict()["max_artifact_bytes"] == 1024
    assert base.as_dict()["max_audit_stream_rows"] is not None
    assert base.as_dict()["max_audit_stream_bytes"] is not None
    assert base.as_dict()["max_artifact_file_count"] is not None
    assert sha256_prefixed(base.as_dict()) != sha256_prefixed(changed.as_dict())


def test_artifact_store_counts_and_rejects_budget_excess(tmp_path: Path) -> None:
    store = ArtifactStore(
        root=tmp_path,
        budget=ArtifactBudget(
            max_artifact_bytes=80,
            max_audit_stream_rows=1,
            max_audit_stream_bytes=80,
            max_artifact_file_count=1,
        ),
    )
    store.append_jsonl(tmp_path / "decisions.jsonl", {"x": 1}, audit_stream=True)

    assert store.file_count == 1
    assert store.audit_stream_rows == 1
    assert store.total_bytes > 0
    with pytest.raises(ArtifactBudgetExceeded) as excinfo:
        store.append_jsonl(tmp_path / "decisions.jsonl", {"x": 2}, audit_stream=True)
    assert excinfo.value.reason == "artifact_budget_max_audit_stream_rows_exceeded"


def test_artifact_store_reports_overwrite_existing_path_for_same_json_path(tmp_path: Path) -> None:
    first_payload = {"x": "a"}
    second_payload = {"x": "b" * 64}
    store = ArtifactStore(
        root=tmp_path,
        budget=ArtifactBudget(max_artifact_bytes=40),
    )
    path = tmp_path / "candidate_results" / "candidate_001.json"

    store.write_json_atomic(path, first_payload)
    with pytest.raises(ArtifactBudgetExceeded) as excinfo:
        store.write_json_atomic(path, second_payload)

    payload = excinfo.value.as_dict()
    assert payload["overwrite_existing_path"] is True
    assert payload["attempted_write_bytes"] > 0
    assert payload["prior_total_bytes"] > 0
    assert payload["next_total_bytes"] > payload["prior_total_bytes"]


def test_summary_artifact_estimate_uses_bounded_candidate_size() -> None:
    common = {
        "candidate_count": 8,
        "scenario_count": 2,
        "split_count": 3,
        "audit_mode": "summary_only",
        "estimated_audit_stream_rows": 0,
        "estimated_artifact_write_count": 19,
        "estimated_hash_payload_bytes": 8192,
        "full_decisions_external_jsonl": False,
    }

    summary = _estimated_artifact_bytes(**common, report_detail="summary")
    full = _estimated_artifact_bytes(**common, report_detail="full")

    assert summary < full
    assert summary > common["estimated_hash_payload_bytes"]


def test_execution_plan_reports_pre_parallel_workload_fields() -> None:
    manifest = parse_manifest(_manifest())
    snapshots = {name: _snapshot(name) for name in ("train", "validation", "final_holdout")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    plan = build_research_execution_plan(
        manifest=manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        db_path="/tmp/unit.sqlite",
        repository_version="test",
        created_at="2026-06-11T00:00:00+00:00",
    )
    estimate = plan.payload["workload_estimate"]

    assert estimate["pre_parallel_work_unit_count"] == 1
    assert estimate["pre_parallel_split_hash_count"] == 3
    assert estimate["pre_parallel_dataset_hash_call_count"] == 3
    assert "pre_parallel_dataset_hash_payload_bytes" in estimate
    assert estimate["pre_parallel_parent_serial_estimate_status"] == "precomputed_split_hashes"
    assert estimate["resource_plan"]["schema_version"] == 1
    assert estimate["data_plane_policy"]["schema_version"] == 1


def test_workload_estimate_includes_canonical_observability_fields() -> None:
    manifest = parse_manifest(_manifest())
    snapshots = {name: _snapshot(name) for name in ("train", "validation", "final_holdout")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    plan = build_research_execution_plan(
        manifest=manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        db_path="/tmp/unit.sqlite",
        repository_version="test",
        created_at="2026-06-11T00:00:00+00:00",
    )
    estimate = plan.payload["workload_estimate"]

    assert "estimated_tick_canonical_hash_call_count" in estimate
    assert "estimated_tick_canonical_hash_payload_bytes" in estimate
    assert "estimated_decision_payload_bytes" in estimate
    assert estimate["estimated_observability_mode"] in {
        "summary_aggregate",
        "diagnostic_sampled",
        "full_tick_canonical",
        "promotion_evidence",
    }
    assert "estimated_full_tick_canonical_enabled" in estimate


def test_workload_estimate_includes_parallel_task_capacity() -> None:
    manifest_payload = _manifest()
    manifest_payload["research_run"] = {"execution": {"mode": "parallel", "max_workers": 8}}
    manifest = parse_manifest(manifest_payload)
    snapshots = {name: _snapshot(name) for name in ("train", "validation")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    plan = build_research_execution_plan(
        manifest=manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        db_path="/tmp/unit.sqlite",
        repository_version="test",
        created_at="2026-06-16T00:00:00+00:00",
    )
    estimate = plan.payload["workload_estimate"]

    assert estimate["available_parallel_work_tasks"] == 1
    assert estimate["expected_worker_utilization_pct"] == 12.5
    assert estimate["parallel_task_to_worker_ratio"] == 0.125
    assert estimate["parallelism_limiting_factor"] == "work_unit_granularity_candidate_scenario"
    assert estimate["resource_plan"]["effective_max_workers"] >= 1
    assert estimate["resource_plan"]["selection_reasons"]


def test_workload_budget_fails_canonical_hash_call_excess(tmp_path: Path) -> None:
    estimate_path = tmp_path / "estimate.json"
    estimate = {
        "estimated_tick_events": 0,
        "estimated_audit_stream_rows": 0,
        "estimated_artifact_write_count": 0,
        "estimated_hash_payload_bytes": 0,
        "estimated_artifact_bytes": 0,
        "estimated_artifact_file_count": 0,
        "estimated_plugin_runtime_us": 0,
        "pre_parallel_work_unit_count": 0,
        "pre_parallel_dataset_hash_payload_bytes": 0,
        "pre_parallel_dataset_hash_call_count": 0,
        "estimated_tick_canonical_hash_call_count": 999,
        "estimated_tick_canonical_hash_payload_bytes": 0,
        "estimated_decision_payload_bytes": 0,
    }
    estimate_path.write_text(json.dumps(estimate), encoding="utf-8")
    policy_path = tmp_path / "policy.json"
    policy = {
        "schema_version": 1,
        "suites": {
            suite: {
                "max_estimated_tick_events": 10**12,
                "max_estimated_audit_stream_rows": 10**12,
                "max_estimated_artifact_write_count": 10**12,
                "max_estimated_hash_payload_bytes": 10**12,
                "max_estimated_artifact_bytes": 10**12,
                "max_estimated_artifact_file_count": 10**12,
                "max_estimated_plugin_runtime_us": 10**12,
                "max_pre_parallel_work_unit_count": 10**12,
                "max_pre_parallel_dataset_hash_payload_bytes": 10**12,
                "max_pre_parallel_dataset_hash_call_count": 10**12,
                "max_estimated_tick_canonical_hash_call_count": 0,
                "max_estimated_tick_canonical_hash_payload_bytes": 10**12,
                "max_estimated_decision_payload_bytes": 10**12,
            }
            for suite in ("fast", "research-nightly", "full")
        },
    }
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/check_research_workload_budget.py",
            "--suite",
            "fast",
            "--policy-json",
            str(policy_path),
            "--estimate-json",
            str(estimate_path),
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "estimated_tick_canonical_hash_call_count" in result.stderr


def test_research_workload_budget_script_fails_pre_parallel_hash_call_excess(tmp_path: Path) -> None:
    estimate_path = tmp_path / "estimate.json"
    estimate = {
        "estimated_tick_events": 0,
        "estimated_audit_stream_rows": 0,
        "estimated_artifact_write_count": 0,
        "estimated_hash_payload_bytes": 0,
        "estimated_artifact_bytes": 0,
        "estimated_artifact_file_count": 0,
        "estimated_plugin_runtime_us": 0,
        "pre_parallel_work_unit_count": 0,
        "pre_parallel_dataset_hash_payload_bytes": 0,
        "pre_parallel_dataset_hash_call_count": 129,
        "estimated_tick_canonical_hash_call_count": 0,
        "estimated_tick_canonical_hash_payload_bytes": 0,
        "estimated_decision_payload_bytes": 0,
    }
    estimate_path.write_text(json.dumps(estimate), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/check_research_workload_budget.py",
            "--suite",
            "fast",
            "--estimate-json",
            str(estimate_path),
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "pre_parallel_dataset_hash_call_count" in result.stderr


def test_research_workload_summary_includes_pre_parallel_totals() -> None:
    summary = research_workload_summary()

    assert "total_pre_parallel_work_unit_count" in summary
    assert "total_pre_parallel_dataset_hash_payload_bytes" in summary
    assert "total_pre_parallel_dataset_hash_call_count" in summary
    assert int(summary["total_pre_parallel_work_unit_count"]) >= 0
    assert int(summary["total_pre_parallel_dataset_hash_payload_bytes"]) >= 0
    assert int(summary["total_pre_parallel_dataset_hash_call_count"]) >= 0


def test_research_workload_budget_script_default_summary_fails_pre_parallel_hash_call_excess(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "policy.json"
    policy = {
        "schema_version": 1,
        "suites": {
            suite: {
                "max_estimated_tick_events": 10**12,
                "max_estimated_audit_stream_rows": 10**12,
                "max_estimated_artifact_write_count": 10**12,
                "max_estimated_hash_payload_bytes": 10**12,
                "max_estimated_artifact_bytes": 10**12,
                "max_estimated_artifact_file_count": 10**12,
                "max_estimated_plugin_runtime_us": 10**12,
                "max_pre_parallel_work_unit_count": 10**12,
                "max_pre_parallel_dataset_hash_payload_bytes": 10**12,
                "max_pre_parallel_dataset_hash_call_count": 0 if suite == "fast" else 10**12,
                "max_estimated_tick_canonical_hash_call_count": 10**12,
                "max_estimated_tick_canonical_hash_payload_bytes": 10**12,
                "max_estimated_decision_payload_bytes": 10**12,
            }
            for suite in ("fast", "research-nightly", "full")
        },
    }
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/check_research_workload_budget.py",
            "--suite",
            "fast",
            "--policy-json",
            str(policy_path),
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "pre_parallel_dataset_hash_call_count" in result.stderr


def test_research_workload_budget_script_requires_pre_parallel_fields_in_estimate_json(tmp_path: Path) -> None:
    estimate_path = tmp_path / "estimate.json"
    estimate = {
        "estimated_tick_events": 0,
        "estimated_audit_stream_rows": 0,
        "estimated_artifact_write_count": 0,
        "estimated_hash_payload_bytes": 0,
        "estimated_artifact_bytes": 0,
        "estimated_artifact_file_count": 0,
        "estimated_plugin_runtime_us": 0,
    }
    estimate_path.write_text(json.dumps(estimate), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/check_research_workload_budget.py",
            "--suite",
            "fast",
            "--estimate-json",
            str(estimate_path),
        ],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "pre_parallel_work_unit_count" in result.stderr


def test_research_workload_inventory_requires_pre_parallel_fields(tmp_path: Path) -> None:
    inventory = json.loads(Path("tests/policy/research_e2e_inventory.json").read_text(encoding="utf-8"))
    del inventory["tests"][0]["expected_workload"]["pre_parallel_dataset_hash_call_count"]
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")

    with pytest.raises(AssertionError, match="pre_parallel_dataset_hash_call_count"):
        load_inventory(inventory_path)


def test_candidate_start_append_is_timed_and_counted(tmp_path: Path, monkeypatch) -> None:
    manifest = parse_manifest(_manifest())
    snapshots = {name: _snapshot(name) for name in ("train", "validation", "final_holdout")}
    quality_reports = {name: _quality_report(name) for name in snapshots}

    result = validation_protocol._evaluate_candidates(
        manifest=manifest,
        manager=_paper_manager(tmp_path, monkeypatch),
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=None,
        candidate_evaluator=DeterministicResearchEvaluator(),
    )

    timing = next(item for item in result.substage_timings if item["stage"] == "append_candidate_start_events")
    assert timing["event_count"] == 1
    assert timing["work_task_count"] == 1
    assert "bytes_written" in timing


def test_run_wide_artifact_context_accumulates_trace_scopes_and_reports(tmp_path: Path, monkeypatch) -> None:
    manager = _paper_manager(tmp_path, monkeypatch)
    context = ResearchArtifactContext(
        manager=manager,
        experiment_id="run_wide_budget",
        budget=ArtifactBudget(max_artifact_bytes=1_000_000, max_artifact_file_count=12),
    )
    indexes = []
    for split in ("train", "validation"):
        scope = AuditTraceScope(
            manager=manager,
            experiment_id="run_wide_budget",
            manifest_hash="sha256:manifest",
            dataset_content_hash=f"sha256:{split}",
            candidate_id="candidate_001",
            scenario_id="scenario_001",
            scenario_index=0,
            split=split,
            artifact_context=context,
        )
        scope.write_decision({"decision_ts": 1, "raw_signal": "HOLD", "split": split})
        indexes.append(scope.complete())

    write_research_report(
        manager=manager,
        experiment_id="run_wide_budget",
        report_name="backtest",
        payload={"candidates": [{"parameter_candidate_id": "candidate_001"}]},
        artifact_context=context,
    )

    assert context.audit_stream_rows == 2
    assert context.file_count == 6
    assert context.total_bytes > 0


def test_report_records_observed_hash_payload_bytes(tmp_path: Path, monkeypatch) -> None:
    manager = _paper_manager(tmp_path, monkeypatch)

    result = write_research_report(
        manager=manager,
        experiment_id="observed_hash_budget",
        report_name="backtest",
        payload={
            "experiment_id": "observed_hash_budget",
            "research_run": {"report_detail": "summary"},
            "workload_estimate": {
                "estimated_hash_payload_bytes": 1_000_000,
                "estimated_artifact_bytes": 1_000_000,
            },
            "candidates": [{"candidate_id": "candidate_001", "behavior_hash": "sha256:behavior"}],
        },
    )

    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))
    assert persisted["artifact_observability"]["report_write"]["observed_hash_payload_bytes"] > 0
    assert persisted["workload_estimate_comparison"]["observed_hash_payload_bytes"] > 0


def test_observed_hash_payload_bytes_compared_to_estimate(tmp_path: Path, monkeypatch) -> None:
    manager = _paper_manager(tmp_path, monkeypatch)

    result = write_research_report(
        manager=manager,
        experiment_id="observed_hash_ratio",
        report_name="backtest",
        payload={
            "experiment_id": "observed_hash_ratio",
            "research_run": {"report_detail": "summary"},
            "workload_estimate": {
                "estimated_hash_payload_bytes": 1_000_000,
                "estimated_artifact_bytes": 1_000_000,
            },
            "candidates": [{"candidate_id": "candidate_001", "behavior_hash": "sha256:behavior"}],
        },
    )

    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))
    comparison = persisted["workload_estimate_comparison"]
    assert comparison["hash_payload_ratio"] is not None
    assert comparison["status"] == "PASS"


def test_hash_payload_budget_warning_when_observed_exceeds_threshold(tmp_path: Path, monkeypatch) -> None:
    manager = _paper_manager(tmp_path, monkeypatch)

    result = write_research_report(
        manager=manager,
        experiment_id="observed_hash_warn",
        report_name="backtest",
        payload={
            "experiment_id": "observed_hash_warn",
            "research_run": {"report_detail": "summary"},
            "workload_estimate": {
                "estimated_hash_payload_bytes": 1,
                "estimated_artifact_bytes": 1,
            },
            "candidates": [{"candidate_id": "candidate_001", "behavior_hash": "sha256:behavior"}],
        },
    )

    persisted = json.loads(result.paths.report_path.read_text(encoding="utf-8"))
    comparison = persisted["workload_estimate_comparison"]
    assert comparison["status"] in {"WARN", "FAIL"}
    assert comparison["reasons"]


def test_candidate_journal_and_trace_manifest_are_run_wide_accounted(tmp_path: Path, monkeypatch) -> None:
    manager = _paper_manager(tmp_path, monkeypatch)
    payload = _manifest()
    payload["experiment_id"] = "journal_manifest_accounting"
    manifest = parse_manifest(payload)
    context = ResearchArtifactContext(
        manager=manager,
        experiment_id=manifest.experiment_id,
        budget=ArtifactBudget(max_artifact_bytes=1_000_000, max_artifact_file_count=4),
    )

    _append_candidate_event(
        manager=manager,
        manifest=manifest,
        event={"stage": "candidate_start", "candidate_id": "candidate_001"},
        artifact_context=context,
    )
    write_trace_manifest(
        manager=manager,
        experiment_id=manifest.experiment_id,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        trace_indexes=[],
        policy=AuditTrailPolicy(mode="complete_external"),
        artifact_context=context,
    )

    assert context.file_count == 2
    assert context.total_bytes > 0


def test_return_panel_and_statistical_evidence_are_run_wide_accounted(tmp_path: Path, monkeypatch) -> None:
    manager = _paper_manager(tmp_path, monkeypatch)
    context = ResearchArtifactContext(
        manager=manager,
        experiment_id="statistical_artifact_accounting",
        budget=ArtifactBudget(max_artifact_bytes=1_000_000, max_artifact_file_count=2),
    )

    panel_path = write_candidate_return_panel(
        manager=manager,
        experiment_id="statistical_artifact_accounting",
        panel={"artifact_type": "candidate_return_panel", "content_hash": "sha256:panel"},
        artifact_context=context,
    )
    evidence_path = write_statistical_selection_evidence(
        manager=manager,
        experiment_id="statistical_artifact_accounting",
        evidence={"artifact_type": "statistical_selection_evidence", "content_hash": "sha256:evidence"},
        artifact_context=context,
    )

    assert panel_path.exists()
    assert evidence_path.exists()
    assert context.file_count == 2
    assert context.total_bytes > 0


def test_family_and_experiment_registries_are_explicit_append_only_budget_exemptions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manager = _paper_manager(tmp_path, monkeypatch)
    family_result = append_family_trial_registry_row(
        manager=manager,
        experiment_family_id="family-001",
        experiment_id="experiment-001",
        manifest_hash="sha256:manifest",
        hypothesis_id="hypothesis-001",
        hypothesis_status="active",
        attempt_index=0,
        holdout_reuse_count=0,
        dataset_content_hash="sha256:dataset",
        parameter_space_hash="sha256:parameters",
        candidate_count=1,
        return_panel_hash="sha256:panel",
        statistical_evidence_hash="sha256:evidence",
        result_status="PASS",
        created_at="2026-05-03T00:00:00+00:00",
    )
    family_row = json.loads(Path(family_result["path"]).read_text(encoding="utf-8").splitlines()[-1])

    experiment_result = reserve_research_attempt(
        manager=manager,
        base_payload={
            "experiment_id": "experiment-001",
            "experiment_family_id": "family-001",
            "hypothesis_id": "hypothesis-001",
            "hypothesis_status": "active",
            "dataset_snapshot_id": "snapshot-001",
            "train_split_hash": "sha256:train",
            "validation_split_hash": "sha256:validation",
            "final_holdout_identity_hash": "sha256:holdout",
            "final_holdout_reuse_key_hash": "sha256:holdout",
            "parameter_space_hash": "sha256:parameters",
        },
        created_at="2026-05-03T00:00:00+00:00",
    )
    experiment_row = experiment_result["row"]

    assert family_row["budget_policy"] == FAMILY_TRIAL_REGISTRY_BUDGET_POLICY
    assert Path(family_result["path"]).relative_to(manager.data_dir()).as_posix() == (
        "reports/research/families/family-001/trial_registry.jsonl"
    )
    assert experiment_row["budget_policy"] == EXPERIMENT_REGISTRY_BUDGET_POLICY
    assert Path(experiment_result["path"]).relative_to(manager.data_dir()).as_posix() == (
        "reports/research/_registry/experiment_registry.jsonl"
    )


def test_research_raw_writer_policy_classifies_remaining_direct_storage_calls() -> None:
    allowed_by_module: dict[str, set[str]] = {
        "artifact_store.py": {
            "append_jsonl",
            "write_json_atomic",
        },
        "audit_trail.py": {
            "append_jsonl",
            "write_json_atomic",
        },
        "batch_runner.py": {
            "write_json_atomic",
        },
        "cli.py": {
            "write_json_atomic",
        },
        "data_plane.py": {
            "write_json_atomic",
        },
        "execution_calibration.py": {
            "write_json_atomic",
        },
        "experiment_registry.py": {
            "append_jsonl",
        },
        "family_registry.py": {
            "append_jsonl",
        },
        "forward_diagnostics_cli.py": {
            "write_json_atomic",
        },
        "forward_diagnostics_failure_report.py": {
            "write_json_atomic",
        },
        "forward_diagnostics_policy_denial.py": {
            "write_json_atomic",
        },
        "forward_diagnostics_report.py": {
            "write_json_atomic",
        },
        "promotion_gate.py": {
            "write_json_atomic",
        },
        "profiling.py": {
            "write_json_atomic",
        },
        "report_writer.py": {
            "write_json_atomic",
        },
        "return_panel.py": {
            "write_json_atomic",
        },
        "statistical_selection.py": {
            "write_json_atomic",
        },
        "validation_pipeline.py": {
            "write_json_atomic",
        },
        "validation_protocol.py": {
            "append_jsonl",
            "write_json_atomic",
        },
    }
    classifications = {
        "artifact_store.py": "accounted research artifact adapter to storage_io",
        "audit_trail.py": "accounted audit trace writes through ArtifactStore or ResearchArtifactContext",
        "batch_runner.py": "diagnostic research batch summary through PathManager-managed research reports root",
        "cli.py": "untracked minimal artifact-budget failure report through managed research reports root",
        "data_plane.py": "operator-specified diagnostic report outputs validated outside repository",
        "execution_calibration.py": "accounted non-research execution-quality report artifact",
        "experiment_registry.py": "explicit append-only registry artifact budget exemption",
        "family_registry.py": "explicit append-only registry artifact budget exemption",
        "forward_diagnostics_cli.py": "operator-specified diagnostic report export validated outside repository",
        "forward_diagnostics_failure_report.py": "diagnostic-only unavailable-status report artifact through PathManager data roots",
        "forward_diagnostics_policy_denial.py": "diagnostic-only policy-denial report artifact through PathManager data roots",
        "forward_diagnostics_report.py": "diagnostic-only report and derived warning artifacts through PathManager data roots",
        "promotion_gate.py": "operator promotion report artifact with existing path policy",
        "profiling.py": "diagnostic-only research profile artifact through PathManager-managed derived research root",
        "report_writer.py": "accounted research report writes through ResearchArtifactContext",
        "return_panel.py": "accounted research return panel through ResearchArtifactContext",
        "statistical_selection.py": "accounted statistical evidence through ResearchArtifactContext",
        "validation_pipeline.py": "validation-run report artifact outside experiment-run accounting",
        "validation_protocol.py": "accounted candidate journal/result/failure writes through ResearchArtifactContext",
    }
    assert set(allowed_by_module) == set(classifications)
    observed: dict[str, set[str]] = {}
    for path in Path("src/bithumb_bot/research").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = ""
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr
            if name in {"append_jsonl", "write_json_atomic"}:
                observed.setdefault(path.name, set()).add(name)

    assert observed == allowed_by_module


def test_research_workload_budget_script_passes_bounded_synthetic_estimate(tmp_path: Path) -> None:
    estimate = tmp_path / "estimate.json"
    estimate.write_text(
        json.dumps(
            {
                "estimated_tick_events": 10,
                "estimated_audit_stream_rows": 0,
                "estimated_artifact_write_count": 2,
                "estimated_hash_payload_bytes": 1024,
                "estimated_artifact_bytes": 1024,
                "estimated_artifact_file_count": 2,
                "estimated_plugin_runtime_us": 500,
                "pre_parallel_work_unit_count": 1,
                "pre_parallel_dataset_hash_payload_bytes": 1024,
                "pre_parallel_dataset_hash_call_count": 1,
                "estimated_tick_canonical_hash_call_count": 0,
                "estimated_tick_canonical_hash_payload_bytes": 0,
                "estimated_decision_payload_bytes": 0,
            }
        ),
        encoding="utf-8",
    )

    proc = subprocess.run(
        [sys.executable, "scripts/check_research_workload_budget.py", "--suite", "fast", "--estimate-json", str(estimate)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "research workload budget: ok" in proc.stdout


def test_research_workload_budget_script_fails_oversized_synthetic_estimate(tmp_path: Path) -> None:
    estimate = tmp_path / "estimate.json"
    estimate.write_text(
        json.dumps(
            {
                "estimated_tick_events": 250_001,
                "estimated_audit_stream_rows": 125_001,
                "estimated_artifact_write_count": 251,
                "estimated_hash_payload_bytes": 33_554_433,
                "estimated_artifact_bytes": 100_663_297,
                "estimated_artifact_file_count": 501,
                "estimated_plugin_runtime_us": 5_000_001,
                "pre_parallel_work_unit_count": 251,
                "pre_parallel_dataset_hash_payload_bytes": 33_554_433,
                "pre_parallel_dataset_hash_call_count": 129,
                "estimated_tick_canonical_hash_call_count": 500_001,
                "estimated_tick_canonical_hash_payload_bytes": 67_108_865,
                "estimated_decision_payload_bytes": 134_217_729,
            }
        ),
        encoding="utf-8",
    )

    proc = subprocess.run(
        [sys.executable, "scripts/check_research_workload_budget.py", "--suite", "fast", "--estimate-json", str(estimate)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    output = proc.stdout + proc.stderr
    assert "suite=fast field=estimated_tick_events observed=250001 limit=250000" in output
    assert "suite=fast field=estimated_audit_stream_rows observed=125001 limit=125000" in output
    assert "suite=fast field=estimated_artifact_bytes observed=100663297 limit=100663296" in output
    assert "suite=fast field=estimated_plugin_runtime_us observed=5000001 limit=5000000" in output


def test_research_workload_budget_policy_requires_suite_fields(tmp_path: Path) -> None:
    policy = tmp_path / "policy.json"
    policy.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "suites": {
                    "fast": {},
                    "research-nightly": {},
                    "full": {},
                },
            }
        ),
        encoding="utf-8",
    )
    estimate = tmp_path / "estimate.json"
    estimate.write_text(
        json.dumps(
            {
                "estimated_tick_events": 1,
                "estimated_audit_stream_rows": 0,
                "estimated_artifact_write_count": 1,
                "estimated_hash_payload_bytes": 1,
                "estimated_artifact_bytes": 1,
                "estimated_artifact_file_count": 1,
                "estimated_plugin_runtime_us": 1,
                "pre_parallel_work_unit_count": 1,
                "pre_parallel_dataset_hash_payload_bytes": 1,
                "pre_parallel_dataset_hash_call_count": 1,
                "estimated_tick_canonical_hash_call_count": 1,
                "estimated_tick_canonical_hash_payload_bytes": 1,
                "estimated_decision_payload_bytes": 1,
            }
        ),
        encoding="utf-8",
    )

    proc = subprocess.run(
        [
            sys.executable,
            "scripts/check_research_workload_budget.py",
            "--suite",
            "fast",
            "--policy-json",
            str(policy),
            "--estimate-json",
            str(estimate),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "policy suite=fast field max_estimated_tick_events" in (proc.stdout + proc.stderr)


def test_research_workload_budget_script_requires_plugin_runtime_in_estimate_json(tmp_path: Path) -> None:
    estimate = tmp_path / "estimate.json"
    estimate.write_text(
        json.dumps(
            {
                "estimated_tick_events": 10,
                "estimated_audit_stream_rows": 0,
                "estimated_artifact_write_count": 2,
                "estimated_hash_payload_bytes": 1024,
                "estimated_artifact_bytes": 1024,
                "estimated_artifact_file_count": 2,
            }
        ),
        encoding="utf-8",
    )

    proc = subprocess.run(
        [sys.executable, "scripts/check_research_workload_budget.py", "--suite", "fast", "--estimate-json", str(estimate)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "estimated_plugin_runtime_us" in (proc.stdout + proc.stderr)


def test_research_workload_budget_default_inventory_path_includes_artifact_bytes() -> None:
    summary = research_workload_summary()
    assert summary["total_estimated_artifact_bytes"] > 0
    assert summary["total_estimated_artifact_file_count"] > 0

    proc = subprocess.run(
        [sys.executable, "scripts/check_research_workload_budget.py", "--suite", "research-nightly"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "research workload budget: ok suite=research-nightly" in proc.stdout


def test_research_workload_budget_default_inventory_fails_artifact_byte_limit(tmp_path: Path) -> None:
    policy_payload = json.loads(Path("tests/policy/research_workload_budget_policy.json").read_text(encoding="utf-8"))
    policy_payload["suites"]["research-nightly"]["max_estimated_artifact_bytes"] = 1
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps(policy_payload), encoding="utf-8")

    proc = subprocess.run(
        [
            sys.executable,
            "scripts/check_research_workload_budget.py",
            "--suite",
            "research-nightly",
            "--policy-json",
            str(policy),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "suite=research-nightly field=estimated_artifact_bytes" in (proc.stdout + proc.stderr)
