from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from bithumb_bot.cli.main import main as cli_main
from bithumb_bot.paths import PathManager
from bithumb_bot.research import cli as research_cli
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research import validation_pipeline as pipeline
from bithumb_bot.storage_io import write_json_atomic


def test_validation_pipeline_rejects_forward_diagnostics_report_as_candidate_report() -> None:
    reasons = pipeline._report_evidence_rejection_reasons(
        {
            "artifact_type": "forward_return_diagnostic_report",
            "diagnostic_only": True,
            "promotion_evidence": False,
            "approved_profile_evidence": False,
            "live_readiness_evidence": False,
            "capital_allocation_evidence": False,
            "evidence_scope": "diagnostic_feature_mining",
            "promotion_eligible": False,
            "promotion_grade": False,
            "non_promotable": True,
            "forbidden_uses": [
                "strategy_promotion",
                "approved_profile",
                "live_readiness",
                "capital_allocation",
            ],
            "operator_next_action": "run_research_validate_from_fixed_manifest",
        },
        label="backtest",
    )

    assert "backtest_diagnostic_feature_mining_not_promotable" in reasons
    assert "backtest_forbidden_use:strategy_promotion" in reasons


def _manager(tmp_path: Path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def _manifest(*, walk_forward_required: bool = True):
    return SimpleNamespace(
        experiment_id="validation_exp",
        deployment_tier="paper_candidate",
        acceptance_gate=SimpleNamespace(walk_forward_required=walk_forward_required),
        manifest_hash=lambda: "sha256:manifest",
    )


def _report(
    manager: PathManager,
    *,
    kind: str,
    candidate_id: str = "candidate_001",
    hash_suffix: str = "",
    standalone_backtest_marker: bool = False,
):
    path = manager.data_dir() / "reports" / "research" / "validation_exp" / f"{kind}_report.json"
    payload = {
        "experiment_id": "validation_exp",
        "manifest_hash": "sha256:manifest",
        "strategy_name": "sma_with_filter",
        "deployment_tier": "paper_candidate",
        "execution_model": {"source": "test"},
        "execution_calibration_required": False,
        "execution_calibration_artifact_hash": None,
        "selected_candidate_id": candidate_id,
        "best_candidate_id": candidate_id,
        "promotion_eligibility_gate_result": "PASS",
        "promotion_blocking_reasons": [],
        "dataset_quality_hash": "sha256:dataset-quality",
        "dataset_quality_gate_status": "PASS",
        "dataset_quality_gate_reasons": [],
        "dataset_splits": {
            "final_holdout": {
                "content_hash": "sha256:final-holdout-split",
                "quality_hash": "sha256:final-holdout-quality",
            }
        },
        "final_holdout_content_hash": "sha256:final-holdout-content",
        "stress_suite_required": True,
        "stress_suite_contract_hash": "sha256:stress-contract",
        "stress_suite_gate_result": "PASS",
        "stress_suite_fail_reasons": [],
        "statistical_validation_required": True,
        "statistical_evidence_hash": "sha256:statistical-evidence",
        "return_panel_hash": "sha256:return-panel",
        "candidate_metric_values_hash": "sha256:candidate-metrics",
        "selection_universe_hash": "sha256:selection-universe",
        "bootstrap_sampling_contract_hash": "sha256:bootstrap",
        "statistical_gate_result": "PASS",
        "statistical_gate_fail_reasons": [],
        "evidence_grade": "PROMOTION_GRADE_WRC",
        "official_promotion_grade_wrc_generation_available": True,
        "final_selection_required": True,
        "final_selection_gate_result": "PASS",
        "final_selection_fail_reasons": [],
        "final_selection_contract_hash": "sha256:final-selection-contract",
        "selected_candidate_score_hash": "sha256:selected-score",
        "candidate_final_scores_hash": "sha256:final-scores",
        "artifact_paths": {"report_path": str(path.resolve())},
        "candidates": [
            {
                "parameter_candidate_id": candidate_id,
                "parameter_values": {"SMA_SHORT": 2},
                "cost_model": {"fee_rate": 0.0},
                "base_cost_assumption": {"label": "base"},
                "cost_assumption_contract": {"source": "test"},
                "execution_model": {"source": "test"},
                "execution_calibration_gate": None,
                "execution_calibration_artifact_hash": None,
                "execution_calibration_artifact_hashes": [],
                "manifest_hash": "sha256:manifest",
                "final_holdout_present": True,
                "final_holdout_required_for_promotion": True,
                "final_holdout_metrics": {"return_pct": 1.0},
                "statistical_validation_required": True,
                "statistical_evidence_hash": "sha256:statistical-evidence",
                "return_panel_hash": "sha256:return-panel",
                "candidate_metric_values_hash": "sha256:candidate-metrics",
                "selection_universe_hash": "sha256:selection-universe",
                "statistical_gate_result": "PASS",
                "statistical_gate_fail_reasons": [],
                "evidence_grade": "PROMOTION_GRADE_WRC",
            }
        ],
    }
    if standalone_backtest_marker:
        payload.update(
            {
                "promotion_eligibility_gate_result": "FAIL",
                "promotion_blocking_reasons": ["walk_forward_required_but_not_executed_in_this_run"],
                "validation_run_complete": False,
                "diagnostic_only": True,
                "standalone_backtest_not_full_validation": True,
            }
        )
    payload["content_hash"] = sha256_prefixed({"kind": kind, "candidate_id": candidate_id, "hash_suffix": hash_suffix})
    return payload


def test_research_validate_cli_dispatches(monkeypatch):
    captured = {}

    def fake_cmd(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(research_cli, "cmd_research_validate", fake_cmd)

    status = cli_main(
        [
            "research-validate",
            "--manifest",
            "manifest.json",
            "--execution-calibration",
            "calibration.json",
            "--candidate-id",
            "candidate_001",
            "--out",
            "/tmp/validation_run.json",
            "--mode",
            "strict",
        ]
    )

    assert status == 0
    assert captured == {
        "manifest_path": "manifest.json",
        "execution_calibration_path": "calibration.json",
        "candidate_id": "candidate_001",
        "out_path": "/tmp/validation_run.json",
        "mode": "strict",
        "notification_policy": None,
    }


def test_validation_run_requires_walk_forward_stage_and_writes_failure_artifact(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest(walk_forward_required=True)

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_backtest",
        lambda **kwargs: _report(manager, kind="backtest"),
    )

    def fail_walk_forward(**kwargs):
        raise pipeline.ValidationRunError("walk_forward_failed")

    monkeypatch.setattr(pipeline, "run_research_walk_forward", fail_walk_forward)

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
    )

    assert payload["end_to_end_validation_result"] == "FAIL_CLOSED"
    assert "walk_forward" in payload["required_stage_names"]
    assert any(stage["name"] == "walk_forward" and stage["status"] == "ERROR" for stage in payload["stages"])
    written = Path(payload["validation_run_path"])
    assert written.exists()


def test_validation_run_content_hash_recomputes_deterministically(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    payload = {
        "validation_run_schema_version": 1,
        "validation_run_id": "sha256:test",
        "experiment_id": "validation_exp",
        "manifest_path": "/tmp/manifest.json",
        "manifest_hash": "sha256:manifest",
        "repository_version": "test",
        "deployment_tier": "paper_candidate",
        "mode": "strict",
        "command_args_hash": "sha256:args",
        "required_stage_names": ["readiness"],
        "stages": [
            {"name": "readiness", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {}, "artifact_hashes": {}, "reasons": []}
        ],
        "selected_candidate_id": "candidate_001",
        "backtest_report_hash": "sha256:backtest",
        "walk_forward_report_hash": None,
        "promotion_artifact_hash": "sha256:promotion",
        "reproduce_ok": True,
        "promotion_allowed": True,
        "end_to_end_validation_result": "PASS",
        "fail_closed_reasons": [],
        "validation_run_path": str((manager.data_dir() / "reports" / "research" / "validation_exp" / "validation_run.json").resolve()),
        "generated_at": None,
    }
    payload["validation_run_binding_hash"] = pipeline.validation_run_binding_hash(payload)
    payload["content_hash"] = pipeline.validation_run_content_hash(payload)

    assert pipeline.verify_validation_run_payload(
        payload,
        experiment_id="validation_exp",
        selected_candidate_id="candidate_001",
        backtest_report_hash="sha256:backtest",
    ) == []

    tampered = dict(payload)
    tampered["selected_candidate_id"] = "candidate_002"
    reasons = pipeline.verify_validation_run_payload(tampered, selected_candidate_id="candidate_001")
    assert "validation_run_content_hash_mismatch" in reasons
    assert "validation_run_selected_candidate_mismatch" in reasons


def test_validation_run_verification_rejects_smoke_only_artifact_markers(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    payload = {
        "validation_run_schema_version": 1,
        "validation_run_id": "sha256:test",
        "experiment_id": "validation_exp",
        "manifest_path": "/tmp/manifest.json",
        "manifest_hash": "sha256:manifest",
        "repository_version": "test",
        "deployment_tier": "paper_candidate",
        "mode": "strict",
        "command_args_hash": "sha256:args",
        "required_stage_names": ["readiness"],
        "stages": [
            {"name": "readiness", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {}, "artifact_hashes": {}, "reasons": []}
        ],
        "selected_candidate_id": "candidate_001",
        "backtest_report_hash": "sha256:backtest",
        "walk_forward_report_hash": None,
        "promotion_artifact_hash": "sha256:promotion",
        "reproduce_ok": True,
        "promotion_allowed": True,
        "end_to_end_validation_result": "PASS",
        "fail_closed_reasons": [],
        "validation_run_path": str((manager.data_dir() / "reports" / "research" / "validation_exp" / "validation_run.json").resolve()),
        "diagnostic_only": True,
        "non_promotable": True,
        "promotion_grade": False,
        "evidence_scope": "smoke_only_not_manifest_backed",
        "standalone_backtest_not_full_validation": True,
        "generated_at": None,
    }
    payload["validation_run_binding_hash"] = pipeline.validation_run_binding_hash(payload)
    payload["content_hash"] = pipeline.validation_run_content_hash(payload)

    reasons = pipeline.verify_validation_run_payload(payload)

    assert "validation_run_smoke_backtest_artifact_not_promotable" in reasons
    assert "validation_run_promotion_grade_validation_required" in reasons
    assert "regenerate_via_research_validate" in reasons


def test_validation_run_verification_rejects_standalone_backtest_marker_alone(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    payload = {
        "validation_run_schema_version": 1,
        "validation_run_id": "sha256:test",
        "experiment_id": "validation_exp",
        "manifest_path": "/tmp/manifest.json",
        "manifest_hash": "sha256:manifest",
        "repository_version": "test",
        "deployment_tier": "paper_candidate",
        "mode": "strict",
        "command_args_hash": "sha256:args",
        "required_stage_names": ["readiness"],
        "stages": [
            {"name": "readiness", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {}, "artifact_hashes": {}, "reasons": []}
        ],
        "selected_candidate_id": "candidate_001",
        "backtest_report_hash": "sha256:backtest",
        "walk_forward_report_hash": None,
        "promotion_artifact_hash": "sha256:promotion",
        "reproduce_ok": True,
        "promotion_allowed": True,
        "end_to_end_validation_result": "PASS",
        "fail_closed_reasons": [],
        "validation_run_path": str((manager.data_dir() / "reports" / "research" / "validation_exp" / "validation_run.json").resolve()),
        "standalone_backtest_not_full_validation": True,
        "generated_at": None,
    }
    payload["validation_run_binding_hash"] = pipeline.validation_run_binding_hash(payload)
    payload["content_hash"] = pipeline.validation_run_content_hash(payload)

    reasons = pipeline.verify_validation_run_payload(payload)

    assert "validation_run_standalone_backtest_not_full_validation" in reasons
    assert "regenerate_via_research_validate" in reasons


def test_validation_run_verification_rejects_compatibility_fallback_markers(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    payload = {
        "validation_run_schema_version": 1,
        "validation_run_id": "sha256:test",
        "experiment_id": "validation_exp",
        "manifest_path": "/tmp/manifest.json",
        "manifest_hash": "sha256:manifest",
        "repository_version": "test",
        "deployment_tier": "paper_candidate",
        "mode": "strict",
        "command_args_hash": "sha256:args",
        "required_stage_names": ["readiness"],
        "stages": [
            {"name": "readiness", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {}, "artifact_hashes": {}, "reasons": []}
        ],
        "selected_candidate_id": "candidate_001",
        "backtest_report_hash": "sha256:backtest",
        "walk_forward_report_hash": None,
        "promotion_artifact_hash": "sha256:promotion",
        "reproduce_ok": True,
        "promotion_allowed": True,
        "end_to_end_validation_result": "PASS",
        "fail_closed_reasons": [],
        "validation_run_path": str((manager.data_dir() / "reports" / "research" / "validation_exp" / "validation_run.json").resolve()),
        "compatibility_fallback": True,
        "research_compatibility_execution_fallback": True,
        "generated_at": None,
    }
    payload["validation_run_binding_hash"] = pipeline.validation_run_binding_hash(payload)
    payload["content_hash"] = pipeline.validation_run_content_hash(payload)

    reasons = pipeline.verify_validation_run_payload(payload)

    assert "validation_run_compatibility_fallback_not_promotion_grade" in reasons
    assert "regenerate_via_research_validate" in reasons


def test_research_validate_success_binds_promotion_to_validation_run(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest(walk_forward_required=True)

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_backtest",
        lambda **kwargs: _report(manager, kind="backtest"),
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_walk_forward",
        lambda **kwargs: _report(manager, kind="walk_forward"),
    )

    def fake_promotion(
        *,
        stage,
        experiment_id,
        candidate_id,
        manager,
        validation_run_path,
        validation_run_binding_hash,
        validation_policy_source,
        validation_policy_required_stage_names,
    ):
        path = manager.data_dir() / "reports" / "research" / experiment_id / f"promotion_{candidate_id}.json"
        artifact = {
            "validation_run_required": True,
            "validation_run_binding_status": "verified_pre_promotion_binding",
            "validation_run_path": validation_run_path,
            "validation_run_hash": None,
            "validation_run_binding_hash": validation_run_binding_hash,
            "validation_policy_source": validation_policy_source,
            "validation_policy_required_stage_names": list(validation_policy_required_stage_names),
            "gate_result": "PASS",
        }
        artifact["content_hash"] = sha256_prefixed(artifact)
        write_json_atomic(path, artifact)
        stage.artifact_paths["promotion_artifact_path"] = str(path.resolve())
        stage.artifact_hashes["promotion_artifact_hash"] = artifact["content_hash"]
        return SimpleNamespace(artifact=artifact, artifact_path=path, content_hash=artifact["content_hash"])

    monkeypatch.setattr(pipeline, "_stage_promotion", fake_promotion)
    monkeypatch.setattr(pipeline, "_stage_reproduce", lambda *, stage, promotion_path: {"ok": True})

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
    )

    assert payload["end_to_end_validation_result"] == "PASS"
    assert str(payload["validation_run_binding_hash"]).startswith("sha256:")
    assert payload["promotion_artifact_hash"].startswith("sha256:")
    assert payload["reproduce_ok"] is True
    promotion = pipeline.json.loads(Path(payload["promotion_artifact_path"]).read_text(encoding="utf-8"))
    assert promotion["validation_run_required"] is True
    assert promotion["validation_run_binding_status"] == "verified_pre_promotion_binding"
    assert promotion["validation_run_binding_status"] != "pending_validation_pipeline"
    assert promotion["validation_run_binding_hash"] == payload["validation_run_binding_hash"]


def test_validation_run_emits_expanded_policy_stage_records(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest(walk_forward_required=True)

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_backtest",
        lambda **kwargs: _report(manager, kind="backtest"),
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_walk_forward",
        lambda **kwargs: _report(manager, kind="walk_forward"),
    )
    monkeypatch.setattr(
        pipeline,
        "_stage_promotion",
        lambda **kwargs: SimpleNamespace(
            artifact={"gate_result": "PASS"},
            artifact_path=manager.data_dir()
            / "reports"
            / "research"
            / kwargs["experiment_id"]
            / f"promotion_{kwargs['candidate_id']}.json",
            content_hash="sha256:promotion",
        ),
    )
    monkeypatch.setattr(pipeline, "_stage_reproduce", lambda *, stage, promotion_path: {"ok": True})

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
    )

    stages = {stage["name"]: stage for stage in payload["stages"]}
    for name in (
        "readiness",
        "dataset_quality",
        "backtest",
        "final_holdout",
        "stress_suite",
        "statistical_validation",
        "final_selection",
        "walk_forward",
        "promotion_eligibility",
        "promotion",
        "reproduce",
    ):
        assert name in stages
        assert name in payload["required_stage_names"]
        assert stages[name]["required"] is True
        assert stages[name]["status"] == "PASS"
        assert set(stages[name]) >= {
            "name",
            "required",
            "status",
            "input_hashes",
            "output_hashes",
            "artifact_paths",
            "artifact_hashes",
            "reasons",
        }
    assert stages["statistical_validation"]["artifact_hashes"]["statistical_evidence_hash"] == "sha256:statistical-evidence"
    assert stages["final_selection"]["output_hashes"]["candidate_final_scores_hash"] == "sha256:final-scores"
    assert payload["validation_policy_source"] == "repo_research_validation_policy_v1"
    assert payload["effective_walk_forward_required"] is True


def test_production_policy_requires_walk_forward_even_if_manifest_flag_is_weaker(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest(walk_forward_required=False)

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_backtest",
        lambda **kwargs: _report(manager, kind="backtest"),
    )

    def fail_walk_forward(**kwargs):
        raise pipeline.ValidationRunError("policy_required_walk_forward")

    monkeypatch.setattr(pipeline, "run_research_walk_forward", fail_walk_forward)

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
    )

    assert "walk_forward" in payload["required_stage_names"]
    stages = {stage["name"]: stage for stage in payload["stages"]}
    assert stages["walk_forward"]["status"] == "ERROR"
    assert payload["end_to_end_validation_result"] == "FAIL_CLOSED"


def test_research_validate_fails_closed_for_standalone_backtest_marker(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest(walk_forward_required=True)
    calls = {"walk_forward": 0}

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_backtest",
        lambda **kwargs: _report(manager, kind="backtest", standalone_backtest_marker=True),
    )

    def fake_walk_forward(**kwargs):
        calls["walk_forward"] += 1
        return _report(manager, kind="walk_forward")

    monkeypatch.setattr(pipeline, "run_research_walk_forward", fake_walk_forward)
    monkeypatch.setattr(
        pipeline,
        "_stage_promotion",
        lambda **kwargs: SimpleNamespace(
            artifact={"gate_result": "PASS"},
            artifact_path=manager.data_dir()
            / "reports"
            / "research"
            / kwargs["experiment_id"]
            / f"promotion_{kwargs['candidate_id']}.json",
            content_hash="sha256:promotion",
        ),
    )
    monkeypatch.setattr(pipeline, "_stage_reproduce", lambda *, stage, promotion_path: {"ok": True})

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
    )

    stages = {stage["name"]: stage for stage in payload["stages"]}
    assert calls["walk_forward"] == 0
    assert stages["backtest"]["status"] == "FAIL_CLOSED"
    assert "backtest_standalone_backtest_not_full_validation" in stages["backtest"]["reasons"]
    assert "regenerate_via_research_validate" in stages["backtest"]["reasons"]
    assert payload["end_to_end_validation_result"] == "FAIL_CLOSED"
    assert "backtest_standalone_backtest_not_full_validation" in payload["fail_closed_reasons"]
    assert "regenerate_via_research_validate" in payload["fail_closed_reasons"]


def test_research_validate_fails_closed_for_standalone_walk_forward_marker(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest(walk_forward_required=True)

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_backtest",
        lambda **kwargs: _report(manager, kind="backtest"),
    )
    monkeypatch.setattr(
        pipeline,
        "run_research_walk_forward",
        lambda **kwargs: _report(manager, kind="walk_forward", standalone_backtest_marker=True),
    )

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
    )

    stages = {stage["name"]: stage for stage in payload["stages"]}
    assert stages["backtest"]["status"] == "PASS"
    assert stages["walk_forward"]["status"] == "FAIL_CLOSED"
    assert "walk_forward_standalone_backtest_not_full_validation" in stages["walk_forward"]["reasons"]
    assert "regenerate_via_research_validate" in stages["walk_forward"]["reasons"]
    assert payload["end_to_end_validation_result"] == "FAIL_CLOSED"
    assert "walk_forward_standalone_backtest_not_full_validation" in payload["fail_closed_reasons"]


def test_promotion_eligibility_rechecks_backtest_and_walk_forward_report_markers(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    run = pipeline.ValidationRun(
        validation_run_id="sha256:validation",
        experiment_id="validation_exp",
        manifest_path=str(tmp_path / "manifest.json"),
        manifest_hash="sha256:manifest",
        repository_version=None,
        deployment_tier="paper_candidate",
        mode="strict",
        command_args_hash="sha256:args",
        validation_policy_source="test_policy",
        validation_policy_required_stage_names=["promotion_eligibility"],
        effective_walk_forward_required=True,
        effective_final_holdout_required=False,
        effective_stress_suite_required=False,
        effective_statistical_validation_required=False,
        effective_final_selection_required=False,
        stages=[
            pipeline.ValidationStage("backtest", True, status="PASS"),
            pipeline.ValidationStage("walk_forward", True, status="PASS"),
            pipeline.ValidationStage("promotion_eligibility", True),
        ],
        required_stage_names=["promotion_eligibility"],
    )
    backtest_report = _report(manager, kind="backtest", standalone_backtest_marker=True)
    walk_forward_report = _report(manager, kind="walk_forward")
    walk_forward_report.update(
        {
            "promotion_eligibility_gate_result": "PASS",
            "promotion_blocking_reasons": [],
            "evidence_scope": "smoke_only_not_manifest_backed",
            "non_promotable": True,
            "promotion_grade": False,
            "research_compatibility_execution_fallback": True,
        }
    )

    pipeline._project_promotion_eligibility_stage(
        run=run,
        backtest_report=backtest_report,
        walk_forward_report=walk_forward_report,
    )

    stage = {stage.name: stage for stage in run.stages}["promotion_eligibility"]
    assert stage.status == "FAIL_CLOSED"
    assert "backtest_standalone_backtest_not_full_validation" in stage.reasons
    assert "walk_forward_smoke_backtest_artifact_not_promotable" in stage.reasons
    assert "walk_forward_non_promotable_evidence_artifact" in stage.reasons
    assert "walk_forward_compatibility_fallback_not_promotion_grade" in stage.reasons
    assert "regenerate_via_research_validate" in stage.reasons
    assert "walk_forward_required_but_not_executed_in_this_run" not in stage.reasons


def test_statistical_screening_only_fails_production_validation_stage(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    manifest = _manifest(walk_forward_required=True)
    report = _report(manager, kind="backtest")
    report["evidence_grade"] = "SCREENING_SUMMARY_BOOTSTRAP"
    report["official_promotion_grade_wrc_generation_available"] = False
    report["statistical_gate_result"] = "PASS"
    report["promotion_eligibility_gate_result"] = "PASS"
    report["promotion_blocking_reasons"] = []
    report["candidates"][0]["evidence_grade"] = "SCREENING_SUMMARY_BOOTSTRAP"
    report["candidates"][0]["statistical_gate_result"] = "PASS"

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(pipeline, "run_research_backtest", lambda **kwargs: report)

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
    )

    stages = {stage["name"]: stage for stage in payload["stages"]}
    assert stages["statistical_validation"]["status"] == "FAIL_CLOSED"
    assert "SCREENING_ONLY_NOT_PROMOTABLE" in stages["statistical_validation"]["reasons"]
    assert "UNAVAILABLE_CAPABILITY" in stages["statistical_validation"]["reasons"]
    assert payload["end_to_end_validation_result"] == "FAIL_CLOSED"


def test_standalone_production_backtest_returns_nonzero_for_diagnostic_report(monkeypatch):
    monkeypatch.setattr(research_cli, "load_manifest", lambda path: SimpleNamespace(deployment_tier="paper_candidate"))
    monkeypatch.setattr(research_cli, "load_calibration_artifact", lambda path: None)
    monkeypatch.setattr(
        research_cli,
        "run_research_backtest",
        lambda **kwargs: {
            "experiment_id": "validation_exp",
            "deployment_tier": "paper_candidate",
            "promotion_eligibility_gate_result": "FAIL",
            "promotion_blocking_reasons": ["walk_forward_required_but_not_executed_in_this_run"],
            "validation_run_complete": False,
            "diagnostic_only": True,
            "standalone_backtest_not_full_validation": True,
            "candidates": [],
        },
    )

    assert research_cli.cmd_research_backtest(manifest_path="manifest.json") == 1


def test_standalone_research_only_backtest_keeps_diagnostic_zero_exit(monkeypatch):
    monkeypatch.setattr(research_cli, "load_manifest", lambda path: SimpleNamespace(deployment_tier="research_only"))
    monkeypatch.setattr(
        research_cli,
        "run_research_backtest",
        lambda **kwargs: {
            "experiment_id": "validation_exp",
            "deployment_tier": "research_only",
            "promotion_eligibility_gate_result": "FAIL",
            "promotion_blocking_reasons": ["diagnostic_failure"],
            "validation_run_complete": False,
            "diagnostic_only": True,
            "standalone_backtest_not_full_validation": True,
            "candidates": [],
        },
    )

    assert research_cli.cmd_research_backtest(manifest_path="manifest.json") == 0
