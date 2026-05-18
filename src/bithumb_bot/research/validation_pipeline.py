from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic

from .deployment_policy import is_production_bound_target
from .experiment_manifest import ExperimentManifest
from .hashing import content_hash_payload, report_content_hash_payload, sha256_prefixed
from .lineage import reproduce_promotion
from .validation_protocol import run_research_backtest, run_research_walk_forward


VALIDATION_RUN_SCHEMA_VERSION = 1
VALIDATION_RUN_HASH_FIELD = "content_hash"
VALIDATION_RUN_BINDING_HASH_FIELD = "validation_run_binding_hash"
PASS = "PASS"
FAIL_CLOSED = "FAIL_CLOSED"
SKIPPED_NOT_REQUIRED = "SKIPPED_NOT_REQUIRED"
NOT_RUN = "NOT_RUN"
ERROR = "ERROR"
TERMINAL_BAD_STATUSES = {FAIL_CLOSED, NOT_RUN, ERROR}
VALIDATION_STAGE_ORDER = (
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
)


class ValidationRunError(ValueError):
    pass


@dataclass(frozen=True)
class ResearchValidationPolicy:
    deployment_tier: str
    production_bound: bool
    required_stage_names: tuple[str, ...]
    policy_source: str = "repo_research_validation_policy_v1"

    def stage_required(self, name: str) -> bool:
        return name in set(self.required_stage_names)


def research_validation_policy(manifest: ExperimentManifest) -> ResearchValidationPolicy:
    deployment_tier = str(getattr(manifest, "deployment_tier", None) or "research_only")
    production_bound = is_production_bound_target(deployment_tier)
    required: set[str] = {"readiness", "dataset_quality", "backtest", "promotion_eligibility", "promotion", "reproduce"}
    gate = getattr(manifest, "acceptance_gate", None)
    if production_bound or bool(getattr(gate, "walk_forward_required", False)):
        required.add("walk_forward")
    if production_bound or bool(getattr(gate, "final_holdout_required_for_promotion", False)):
        required.add("final_holdout")
    stress_suite = getattr(manifest, "stress_suite", None)
    if production_bound or bool(getattr(stress_suite, "required_for_promotion", False)):
        required.add("stress_suite")
    statistical_validation = getattr(manifest, "statistical_validation", None)
    if production_bound or bool(getattr(statistical_validation, "required_for_promotion", False)):
        required.add("statistical_validation")
    final_selection = getattr(manifest, "final_selection", None)
    if production_bound or bool(getattr(final_selection, "required_for_promotion", False)):
        required.add("final_selection")
    return ResearchValidationPolicy(
        deployment_tier=deployment_tier,
        production_bound=production_bound,
        required_stage_names=tuple(name for name in VALIDATION_STAGE_ORDER if name in required),
    )


def build_research_readiness_report(**kwargs: Any) -> dict[str, Any]:
    from .readiness import build_research_readiness_report as _build

    return _build(**kwargs)


@dataclass
class ValidationStage:
    name: str
    required: bool
    status: str = NOT_RUN
    started_at: str | None = None
    completed_at: str | None = None
    input_hashes: dict[str, Any] = field(default_factory=dict)
    output_hashes: dict[str, Any] = field(default_factory=dict)
    artifact_paths: dict[str, Any] = field(default_factory=dict)
    artifact_hashes: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "required": self.required,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "input_hashes": dict(self.input_hashes),
            "output_hashes": dict(self.output_hashes),
            "artifact_paths": dict(self.artifact_paths),
            "artifact_hashes": dict(self.artifact_hashes),
            "reasons": list(self.reasons),
        }


@dataclass
class ValidationRun:
    validation_run_id: str
    experiment_id: str
    manifest_path: str
    manifest_hash: str
    repository_version: str | None
    deployment_tier: str
    mode: str
    command_args_hash: str
    validation_policy_source: str
    validation_policy_required_stage_names: list[str]
    effective_walk_forward_required: bool
    effective_final_holdout_required: bool
    effective_stress_suite_required: bool
    effective_statistical_validation_required: bool
    effective_final_selection_required: bool
    stages: list[ValidationStage]
    required_stage_names: list[str]
    selected_candidate_id: str | None = None
    backtest_report_path: str | None = None
    backtest_report_hash: str | None = None
    walk_forward_report_path: str | None = None
    walk_forward_report_hash: str | None = None
    promotion_artifact_path: str | None = None
    promotion_artifact_hash: str | None = None
    validation_run_binding_hash: str | None = None
    reproduce_ok: bool | None = None
    promotion_allowed: bool = False
    end_to_end_validation_result: str = NOT_RUN
    fail_closed_reasons: list[str] = field(default_factory=list)
    validation_run_path: str | None = None
    generated_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "validation_run_schema_version": VALIDATION_RUN_SCHEMA_VERSION,
            "validation_run_id": self.validation_run_id,
            "experiment_id": self.experiment_id,
            "manifest_path": self.manifest_path,
            "manifest_hash": self.manifest_hash,
            "repository_version": self.repository_version,
            "deployment_tier": self.deployment_tier,
            "mode": self.mode,
            "command_args_hash": self.command_args_hash,
            "validation_policy_source": self.validation_policy_source,
            "validation_policy_required_stage_names": list(self.validation_policy_required_stage_names),
            "effective_walk_forward_required": self.effective_walk_forward_required,
            "effective_final_holdout_required": self.effective_final_holdout_required,
            "effective_stress_suite_required": self.effective_stress_suite_required,
            "effective_statistical_validation_required": self.effective_statistical_validation_required,
            "effective_final_selection_required": self.effective_final_selection_required,
            "required_stage_names": list(self.required_stage_names),
            "stages": [stage.as_dict() for stage in self.stages],
            "selected_candidate_id": self.selected_candidate_id,
            "backtest_report_path": self.backtest_report_path,
            "backtest_report_hash": self.backtest_report_hash,
            "walk_forward_report_path": self.walk_forward_report_path,
            "walk_forward_report_hash": self.walk_forward_report_hash,
            "promotion_artifact_path": self.promotion_artifact_path,
            "promotion_artifact_hash": self.promotion_artifact_hash,
            VALIDATION_RUN_BINDING_HASH_FIELD: self.validation_run_binding_hash,
            "reproduce_ok": self.reproduce_ok,
            "promotion_allowed": self.promotion_allowed,
            "end_to_end_validation_result": self.end_to_end_validation_result,
            "fail_closed_reasons": sorted(set(self.fail_closed_reasons)),
            "validation_run_path": self.validation_run_path,
            "generated_at": self.generated_at,
        }
        if not payload[VALIDATION_RUN_BINDING_HASH_FIELD]:
            payload[VALIDATION_RUN_BINDING_HASH_FIELD] = validation_run_binding_hash(payload)
            self.validation_run_binding_hash = str(payload[VALIDATION_RUN_BINDING_HASH_FIELD])
        payload[VALIDATION_RUN_HASH_FIELD] = validation_run_content_hash(payload)
        return payload


def validation_run_hash_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in content_hash_payload(payload).items() if key != VALIDATION_RUN_HASH_FIELD}


def validation_run_content_hash(payload: dict[str, Any]) -> str:
    return sha256_prefixed(validation_run_hash_payload(payload))


def validation_run_binding_hash_payload(payload: dict[str, Any]) -> dict[str, Any]:
    stage_rows = payload.get("stages") if isinstance(payload.get("stages"), list) else []
    pre_promotion_stages: list[dict[str, Any]] = []
    for row in stage_rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "")
        if name in {"promotion", "reproduce"}:
            continue
        pre_promotion_stages.append(
            {
                "name": name,
                "required": bool(row.get("required")),
                "status": row.get("status"),
                "input_hashes": row.get("input_hashes") or {},
                "output_hashes": row.get("output_hashes") or {},
                "artifact_hashes": row.get("artifact_hashes") or {},
                "reasons": sorted(str(item) for item in row.get("reasons") or []),
            }
        )
    return {
        "validation_run_schema_version": payload.get("validation_run_schema_version"),
        "validation_run_id": payload.get("validation_run_id"),
        "experiment_id": payload.get("experiment_id"),
        "manifest_hash": payload.get("manifest_hash"),
        "repository_version": payload.get("repository_version"),
        "deployment_tier": payload.get("deployment_tier"),
        "mode": payload.get("mode"),
        "command_args_hash": payload.get("command_args_hash"),
        "required_stage_names": [
            str(item)
            for item in payload.get("required_stage_names") or []
            if str(item) not in {"promotion", "reproduce"}
        ],
        "validation_policy_source": payload.get("validation_policy_source"),
        "validation_policy_required_stage_names": [
            str(item)
            for item in payload.get("validation_policy_required_stage_names") or []
            if str(item) not in {"promotion", "reproduce"}
        ],
        "effective_walk_forward_required": bool(payload.get("effective_walk_forward_required")),
        "effective_final_holdout_required": bool(payload.get("effective_final_holdout_required")),
        "effective_stress_suite_required": bool(payload.get("effective_stress_suite_required")),
        "effective_statistical_validation_required": bool(payload.get("effective_statistical_validation_required")),
        "effective_final_selection_required": bool(payload.get("effective_final_selection_required")),
        "pre_promotion_stages": pre_promotion_stages,
        "selected_candidate_id": payload.get("selected_candidate_id"),
        "backtest_report_hash": payload.get("backtest_report_hash"),
        "walk_forward_report_hash": payload.get("walk_forward_report_hash"),
    }


def validation_run_binding_hash(payload: dict[str, Any]) -> str:
    return sha256_prefixed(validation_run_binding_hash_payload(payload))


def verify_validation_run_binding(
    payload: dict[str, Any],
    *,
    expected_binding_hash: str | None,
) -> list[str]:
    reasons: list[str] = []
    expected = str(expected_binding_hash or payload.get(VALIDATION_RUN_BINDING_HASH_FIELD) or "")
    if not expected.startswith("sha256:"):
        reasons.append("validation_run_binding_hash_missing")
        return reasons
    actual = validation_run_binding_hash(payload)
    if actual != expected:
        reasons.append("validation_run_binding_hash_mismatch")
    embedded = str(payload.get(VALIDATION_RUN_BINDING_HASH_FIELD) or "")
    if embedded and embedded != expected:
        reasons.append("validation_run_embedded_binding_hash_mismatch")
    return reasons


def default_validation_run_path(*, manager: PathManager, experiment_id: str) -> Path:
    path = manager.data_dir() / "reports" / "research" / experiment_id / "validation_run.json"
    _ensure_research_output_path_allowed(manager, path)
    return path


def write_validation_run(*, manager: PathManager, validation_run: ValidationRun, out_path: str | Path | None = None) -> tuple[Path, str]:
    path = Path(out_path).expanduser() if out_path else default_validation_run_path(
        manager=manager,
        experiment_id=validation_run.experiment_id,
    )
    _ensure_research_output_path_allowed(manager, path)
    validation_run.validation_run_path = str(path.resolve())
    payload = validation_run.as_dict()
    write_json_atomic(path, payload)
    return path, str(payload[VALIDATION_RUN_HASH_FIELD])


def verify_validation_run_payload(
    payload: dict[str, Any],
    *,
    experiment_id: str | None = None,
    manifest_hash: str | None = None,
    selected_candidate_id: str | None = None,
    backtest_report_hash: str | None = None,
    walk_forward_report_hash: str | None = None,
    require_pass: bool = True,
) -> list[str]:
    reasons: list[str] = []
    if int(payload.get("validation_run_schema_version") or 0) != VALIDATION_RUN_SCHEMA_VERSION:
        reasons.append("validation_run_schema_version_mismatch")
    reasons.extend(verify_validation_run_binding(payload, expected_binding_hash=None))
    expected = str(payload.get(VALIDATION_RUN_HASH_FIELD) or "")
    if not expected.startswith("sha256:"):
        reasons.append("validation_run_content_hash_missing")
    elif validation_run_content_hash(payload) != expected:
        reasons.append("validation_run_content_hash_mismatch")
    if experiment_id is not None and payload.get("experiment_id") != experiment_id:
        reasons.append("validation_run_experiment_id_mismatch")
    if manifest_hash is not None and payload.get("manifest_hash") != manifest_hash:
        reasons.append("validation_run_manifest_hash_mismatch")
    if selected_candidate_id is not None and payload.get("selected_candidate_id") != selected_candidate_id:
        reasons.append("validation_run_selected_candidate_mismatch")
    if backtest_report_hash is not None and payload.get("backtest_report_hash") != backtest_report_hash:
        reasons.append("validation_run_backtest_report_hash_mismatch")
    if walk_forward_report_hash is not None and payload.get("walk_forward_report_hash") != walk_forward_report_hash:
        reasons.append("validation_run_walk_forward_report_hash_mismatch")
    stage_rows = payload.get("stages")
    if not isinstance(stage_rows, list):
        reasons.append("validation_run_stages_missing")
        stage_rows = []
    required_names = {str(item) for item in payload.get("required_stage_names") or []}
    seen_names: set[str] = set()
    for row in stage_rows:
        if not isinstance(row, dict):
            reasons.append("validation_run_stage_invalid")
            continue
        name = str(row.get("name") or "")
        if name:
            seen_names.add(name)
        required = bool(row.get("required")) or name in required_names
        status = str(row.get("status") or NOT_RUN)
        if required and status in TERMINAL_BAD_STATUSES:
            reasons.append(f"validation_run_required_stage_{name or 'unknown'}_{status.lower()}")
        if required and status == SKIPPED_NOT_REQUIRED:
            reasons.append(f"validation_run_required_stage_{name or 'unknown'}_invalid_skip")
    for required_name in sorted(required_names - seen_names):
        reasons.append(f"validation_run_required_stage_{required_name}_missing")
    if require_pass:
        if payload.get("end_to_end_validation_result") != PASS:
            reasons.append("validation_run_not_passed")
        if payload.get("promotion_allowed") is not True:
            reasons.append("validation_run_promotion_not_allowed")
        if payload.get("reproduce_ok") is not True:
            reasons.append("validation_run_reproduce_not_ok")
    return sorted(set(reasons))


def load_and_verify_validation_run(
    path: str | Path,
    *,
    experiment_id: str | None = None,
    manifest_hash: str | None = None,
    selected_candidate_id: str | None = None,
    backtest_report_hash: str | None = None,
    walk_forward_report_hash: str | None = None,
    require_pass: bool = True,
) -> tuple[dict[str, Any], list[str]]:
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}, ["validation_run_payload_not_object"]
    return payload, verify_validation_run_payload(
        payload,
        experiment_id=experiment_id,
        manifest_hash=manifest_hash,
        selected_candidate_id=selected_candidate_id,
        backtest_report_hash=backtest_report_hash,
        walk_forward_report_hash=walk_forward_report_hash,
        require_pass=require_pass,
    )


def run_research_validation(
    *,
    manifest: ExperimentManifest,
    db_path: str | Path,
    manager: PathManager,
    manifest_path: str,
    mode: str = "strict",
    execution_calibration: dict[str, Any] | None = None,
    execution_calibration_path: str | None = None,
    candidate_id: str | None = None,
    out_path: str | Path | None = None,
    generated_at: str | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    if mode != "strict":
        raise ValidationRunError("validation_run_mode_unsupported")
    now = generated_at or datetime.now(timezone.utc).isoformat()
    command_args = {
        "manifest": manifest_path,
        "execution_calibration": execution_calibration_path,
        "candidate_id": candidate_id,
        "mode": mode,
    }
    policy = research_validation_policy(manifest)
    required_stage_names = list(policy.required_stage_names)
    walk_forward_required = policy.stage_required("walk_forward")
    run = ValidationRun(
        validation_run_id=sha256_prefixed({"experiment_id": manifest.experiment_id, "manifest_hash": manifest.manifest_hash(), "generated_at": now}),
        experiment_id=manifest.experiment_id,
        manifest_path=str(Path(manifest_path).expanduser().resolve()),
        manifest_hash=manifest.manifest_hash(),
        repository_version=None,
        deployment_tier=manifest.deployment_tier,
        mode=mode,
        command_args_hash=sha256_prefixed(command_args),
        validation_policy_source=policy.policy_source,
        validation_policy_required_stage_names=list(policy.required_stage_names),
        effective_walk_forward_required=policy.stage_required("walk_forward"),
        effective_final_holdout_required=policy.stage_required("final_holdout"),
        effective_stress_suite_required=policy.stage_required("stress_suite"),
        effective_statistical_validation_required=policy.stage_required("statistical_validation"),
        effective_final_selection_required=policy.stage_required("final_selection"),
        stages=[
            ValidationStage(
                name,
                policy.stage_required(name),
                status=NOT_RUN if policy.stage_required(name) else SKIPPED_NOT_REQUIRED,
                input_hashes={"policy_source": policy.policy_source} if name in policy.required_stage_names else {},
            )
            for name in VALIDATION_STAGE_ORDER
        ],
        required_stage_names=required_stage_names,
        generated_at=now,
    )

    try:
        _run_stage(run, "readiness", lambda stage: _stage_readiness(
            stage=stage,
            manifest=manifest,
            manifest_path=manifest_path,
            db_path=db_path,
            execution_calibration_path=execution_calibration_path,
        ))
        if _has_failures(run):
            return _finalize_validation_run(run, manager=manager, out_path=out_path)

        backtest_report = _run_stage(run, "backtest", lambda stage: _stage_backtest(
            stage=stage,
            manifest=manifest,
            db_path=db_path,
            manager=manager,
            generated_at=generated_at,
            execution_calibration=execution_calibration,
            manifest_path=manifest_path,
            command_args=command_args,
            progress_callback=progress_callback,
        ))
        run.repository_version = str(backtest_report.get("repository_version") or "") or None
        run.backtest_report_hash = str(backtest_report.get("content_hash") or "") or None
        run.backtest_report_path = _report_path(backtest_report)
        run.selected_candidate_id = _select_candidate_id(backtest_report, candidate_id)
        if run.selected_candidate_id is None:
            _stage(run, "backtest").status = FAIL_CLOSED
            _stage(run, "backtest").reasons.append("selected_candidate_missing")
            return _finalize_validation_run(run, manager=manager, out_path=out_path)
        _project_backtest_report_stages(
            run=run,
            report=backtest_report,
            selected_candidate_id=run.selected_candidate_id,
            policy=policy,
        )
        if _has_failures(run):
            return _finalize_validation_run(run, manager=manager, out_path=out_path)

        walk_report: dict[str, Any] | None = None
        if walk_forward_required:
            walk_report = _run_stage(run, "walk_forward", lambda stage: _stage_walk_forward(
                stage=stage,
                manifest=manifest,
                db_path=db_path,
                manager=manager,
                generated_at=generated_at,
                execution_calibration=execution_calibration,
                manifest_path=manifest_path,
                command_args=command_args,
                progress_callback=progress_callback,
            ))
            run.walk_forward_report_hash = str(walk_report.get("content_hash") or "") or None
            run.walk_forward_report_path = _report_path(walk_report)
            mismatch_reasons = _evidence_mismatch_reasons(
                backtest_report=backtest_report,
                walk_forward_report=walk_report,
                candidate_id=run.selected_candidate_id,
            )
            if mismatch_reasons:
                stage = _stage(run, "walk_forward")
                stage.status = FAIL_CLOSED
                stage.reasons.extend(mismatch_reasons)
                return _finalize_validation_run(run, manager=manager, out_path=out_path)

        _project_promotion_eligibility_stage(
            run=run,
            backtest_report=backtest_report,
            walk_forward_report=walk_report,
        )
        if _has_failures(run):
            return _finalize_validation_run(run, manager=manager, out_path=out_path)

        validation_run_path = _resolved_validation_run_path(
            manager=manager,
            experiment_id=manifest.experiment_id,
            out_path=out_path,
        )
        run.validation_run_path = str(validation_run_path.resolve())
        run.validation_run_binding_hash = validation_run_binding_hash(run.as_dict())

        promotion = _run_stage(run, "promotion", lambda stage: _stage_promotion(
            stage=stage,
            experiment_id=manifest.experiment_id,
            candidate_id=str(run.selected_candidate_id),
            manager=manager,
            validation_run_path=str(validation_run_path.resolve()),
            validation_run_binding_hash=run.validation_run_binding_hash,
            validation_policy_source=policy.policy_source,
            validation_policy_required_stage_names=policy.required_stage_names,
        ))
        run.promotion_artifact_path = str(promotion.artifact_path.resolve())
        run.promotion_artifact_hash = promotion.content_hash
        run.promotion_allowed = promotion.artifact.get("gate_result") == PASS

        reproduce = _run_stage(run, "reproduce", lambda stage: _stage_reproduce(
            stage=stage,
            promotion_path=str(promotion.artifact_path.resolve()),
        ))
        run.reproduce_ok = bool(reproduce.get("ok"))
        if not run.reproduce_ok:
            _stage(run, "reproduce").status = FAIL_CLOSED
            _stage(run, "reproduce").reasons.append(str(reproduce.get("reason") or "reproduce_failed"))
    except Exception as exc:
        active = _first_active_or_not_run_required_stage(run)
        active.status = ERROR
        active.reasons.append(f"{type(exc).__name__}:{exc}")
    return _finalize_validation_run(run, manager=manager, out_path=out_path)


def _run_stage(run: ValidationRun, name: str, func: Callable[[ValidationStage], Any]) -> Any:
    stage = _stage(run, name)
    if not stage.required:
        stage.status = SKIPPED_NOT_REQUIRED
        return None
    stage.started_at = datetime.now(timezone.utc).isoformat()
    try:
        result = func(stage)
    except Exception as exc:
        stage.status = ERROR
        stage.reasons.append(f"{type(exc).__name__}:{exc}")
        raise
    finally:
        stage.completed_at = datetime.now(timezone.utc).isoformat()
    if stage.status == NOT_RUN:
        stage.status = PASS
    return result


def _stage_readiness(
    *,
    stage: ValidationStage,
    manifest: ExperimentManifest,
    manifest_path: str,
    db_path: str | Path,
    execution_calibration_path: str | None,
) -> dict[str, Any]:
    report = build_research_readiness_report(
        manifest_path=manifest_path,
        db_path=db_path,
        execution_calibration_path=execution_calibration_path,
    )
    stage.input_hashes["manifest_hash"] = manifest.manifest_hash()
    stage.output_hashes["readiness_report_hash"] = sha256_prefixed(report)
    stage.reasons.extend(str(item) for item in report.get("next_actions") or [] if str(item) != "none")
    if report.get("status") != PASS:
        stage.status = FAIL_CLOSED
        stage.reasons.append("readiness_failed")
    return report


def _stage_backtest(
    *,
    stage: ValidationStage,
    manifest: ExperimentManifest,
    db_path: str | Path,
    manager: PathManager,
    generated_at: str | None,
    execution_calibration: dict[str, Any] | None,
    manifest_path: str,
    command_args: dict[str, Any],
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> dict[str, Any]:
    report = run_research_backtest(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at=generated_at,
        execution_calibration=execution_calibration,
        manifest_path=manifest_path,
        command_args=command_args,
        progress_callback=progress_callback,
    )
    _record_report_stage(stage, report, "backtest_report")
    if report.get("promotion_eligibility_gate_result") != PASS:
        reasons = [str(item) for item in report.get("promotion_blocking_reasons") or ["backtest_promotion_gate_failed"]]
        if reasons != ["walk_forward_required_but_not_executed_in_this_run"]:
            stage.status = FAIL_CLOSED
            stage.reasons.extend(reasons)
    return report


def _project_backtest_report_stages(
    *,
    run: ValidationRun,
    report: dict[str, Any],
    selected_candidate_id: str,
    policy: ResearchValidationPolicy,
) -> None:
    candidate = _candidate(report, selected_candidate_id) or {}
    report_path = _report_path(report)
    report_hash = str(report.get("content_hash") or "")
    _project_dataset_quality_stage(run, report=report, report_path=report_path, report_hash=report_hash)
    _project_final_holdout_stage(run, report=report, candidate=candidate, report_path=report_path, report_hash=report_hash)
    _project_stress_suite_stage(run, report=report, candidate=candidate, report_path=report_path, report_hash=report_hash)
    _project_statistical_validation_stage(
        run,
        report=report,
        candidate=candidate,
        report_path=report_path,
        report_hash=report_hash,
        policy=policy,
    )
    _project_final_selection_stage(run, report=report, report_path=report_path, report_hash=report_hash)


def _project_dataset_quality_stage(
    run: ValidationRun,
    *,
    report: dict[str, Any],
    report_path: str | None,
    report_hash: str,
) -> None:
    status = report.get("dataset_quality_gate_status")
    reasons = [str(item) for item in report.get("dataset_quality_gate_reasons") or []]
    stage = _stage(run, "dataset_quality")
    _bind_report_evidence(stage, report_path=report_path, report_hash=report_hash)
    _record_hash_if_present(stage.output_hashes, "dataset_quality_hash", report.get("dataset_quality_hash"))
    if status == PASS:
        stage.status = PASS
    elif stage.required:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["dataset_quality_evidence_missing_or_failed"])
    elif status is not None:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["dataset_quality_failed"])


def _project_final_holdout_stage(
    run: ValidationRun,
    *,
    report: dict[str, Any],
    candidate: dict[str, Any],
    report_path: str | None,
    report_hash: str,
) -> None:
    stage = _stage(run, "final_holdout")
    _bind_report_evidence(stage, report_path=report_path, report_hash=report_hash)
    split = _dataset_split(report, "final_holdout")
    _record_hash_if_present(stage.input_hashes, "final_holdout_split_hash", split.get("content_hash") if split else None)
    for key in ("final_holdout_identity_hash", "final_holdout_content_hash", "final_holdout_reuse_key_hash"):
        _record_hash_if_present(stage.output_hashes, key, report.get(key) or candidate.get(key))
    present = candidate.get("final_holdout_present") is True or isinstance(split, dict)
    has_metrics = isinstance(candidate.get("final_holdout_metrics"), dict) or isinstance(
        candidate.get("final_holdout_metrics_v2"), dict
    )
    has_hash = _is_hash((split or {}).get("content_hash")) or _is_hash(report.get("final_holdout_content_hash"))
    if present and (has_metrics or has_hash):
        stage.status = PASS
    elif stage.required:
        stage.status = FAIL_CLOSED
        if candidate.get("final_holdout_required_for_promotion") is False:
            stage.reasons.append("policy_requires_final_holdout_despite_manifest_candidate_flag")
        stage.reasons.append("final_holdout_evidence_missing")


def _project_stress_suite_stage(
    run: ValidationRun,
    *,
    report: dict[str, Any],
    candidate: dict[str, Any],
    report_path: str | None,
    report_hash: str,
) -> None:
    stage = _stage(run, "stress_suite")
    _bind_report_evidence(stage, report_path=report_path, report_hash=report_hash)
    _record_hash_if_present(
        stage.input_hashes,
        "stress_suite_contract_hash",
        report.get("stress_suite_contract_hash") or candidate.get("stress_suite_contract_hash"),
    )
    gate = report.get("stress_suite_gate_result") or candidate.get("stress_suite_gate_result")
    reasons = [str(item) for item in report.get("stress_suite_fail_reasons") or candidate.get("stress_suite_fail_reasons") or []]
    if gate == PASS:
        stage.status = PASS
    elif stage.required:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["stress_suite_evidence_missing_or_failed"])
    elif gate is not None:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["stress_suite_failed"])


def _project_statistical_validation_stage(
    run: ValidationRun,
    *,
    report: dict[str, Any],
    candidate: dict[str, Any],
    report_path: str | None,
    report_hash: str,
    policy: ResearchValidationPolicy,
) -> None:
    stage = _stage(run, "statistical_validation")
    _bind_report_evidence(stage, report_path=report_path, report_hash=report_hash)
    for path_key in ("statistical_evidence_path", "return_panel_path", "family_trial_registry_path"):
        if report.get(path_key):
            stage.artifact_paths[path_key] = str(report[path_key])
    for hash_key in (
        "statistical_evidence_hash",
        "return_panel_hash",
        "candidate_metric_values_hash",
        "selection_universe_hash",
        "bootstrap_sampling_contract_hash",
        "family_trial_registry_prior_hash",
        "family_trial_registry_row_hash",
    ):
        _record_hash_if_present(stage.artifact_hashes, hash_key, report.get(hash_key) or candidate.get(hash_key))
    gate = report.get("statistical_gate_result") or candidate.get("statistical_gate_result")
    reasons = [str(item) for item in report.get("statistical_gate_fail_reasons") or candidate.get("statistical_gate_fail_reasons") or []]
    evidence_grade = str(report.get("evidence_grade") or candidate.get("evidence_grade") or "")
    if stage.required and not _is_hash(report.get("statistical_evidence_hash") or candidate.get("statistical_evidence_hash")):
        reasons.append("statistical_evidence_missing")
    if stage.required and not _is_hash(report.get("return_panel_hash") or candidate.get("return_panel_hash")):
        reasons.append("MISSING_RETURN_PANEL")
    if policy.production_bound and evidence_grade == "SCREENING_SUMMARY_BOOTSTRAP":
        reasons.append("SCREENING_ONLY_NOT_PROMOTABLE")
    if policy.production_bound and report.get("official_promotion_grade_wrc_generation_available") is False:
        reasons.append("UNAVAILABLE_CAPABILITY")
    if gate == PASS and not reasons:
        stage.status = PASS
    elif stage.required:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["statistical_validation_evidence_missing_or_failed"])
    elif gate is not None:
        stage.status = FAIL_CLOSED if gate != PASS else PASS
        if gate != PASS:
            stage.reasons.extend(reasons or ["statistical_validation_failed"])


def _project_final_selection_stage(
    run: ValidationRun,
    *,
    report: dict[str, Any],
    report_path: str | None,
    report_hash: str,
) -> None:
    stage = _stage(run, "final_selection")
    _bind_report_evidence(stage, report_path=report_path, report_hash=report_hash)
    for hash_key in ("final_selection_contract_hash", "selected_candidate_score_hash", "candidate_final_scores_hash"):
        _record_hash_if_present(stage.output_hashes, hash_key, report.get(hash_key))
    gate = report.get("final_selection_gate_result")
    reasons = [str(item) for item in report.get("final_selection_fail_reasons") or []]
    if gate == PASS:
        stage.status = PASS
    elif stage.required:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["final_selection_evidence_missing_or_failed"])
    elif gate is not None:
        stage.status = FAIL_CLOSED if gate != PASS else PASS
        if gate != PASS:
            stage.reasons.extend(reasons or ["final_selection_failed"])


def _project_promotion_eligibility_stage(
    *,
    run: ValidationRun,
    backtest_report: dict[str, Any],
    walk_forward_report: dict[str, Any] | None,
) -> None:
    stage = _stage(run, "promotion_eligibility")
    backtest_path = _report_path(backtest_report)
    backtest_hash = str(backtest_report.get("content_hash") or "")
    _bind_report_evidence(stage, report_path=backtest_path, report_hash=backtest_hash)
    final_report = walk_forward_report if walk_forward_report is not None else backtest_report
    final_path = _report_path(final_report)
    final_hash = str(final_report.get("content_hash") or "")
    if walk_forward_report is not None:
        if final_path:
            stage.artifact_paths["walk_forward_report_path"] = final_path
        if final_hash:
            stage.artifact_hashes["walk_forward_report_hash"] = final_hash
    gate = final_report.get("promotion_eligibility_gate_result")
    reasons = [str(item) for item in final_report.get("promotion_blocking_reasons") or []]
    backtest_reasons = [str(item) for item in backtest_report.get("promotion_blocking_reasons") or []]
    for reason in backtest_reasons:
        if reason == "walk_forward_required_but_not_executed_in_this_run" and walk_forward_report is not None:
            continue
        if reason not in reasons:
            reasons.append(reason)
    for prior_stage in run.stages:
        if prior_stage.name == "promotion_eligibility":
            break
        if prior_stage.required and prior_stage.status != PASS:
            reasons.append(f"{prior_stage.name}_stage_not_passed")
    if gate == PASS and not reasons:
        stage.status = PASS
    elif stage.required:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["promotion_eligibility_gate_failed"])
    elif gate is not None:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(reasons or ["promotion_eligibility_gate_failed"])


def _stage_walk_forward(
    *,
    stage: ValidationStage,
    manifest: ExperimentManifest,
    db_path: str | Path,
    manager: PathManager,
    generated_at: str | None,
    execution_calibration: dict[str, Any] | None,
    manifest_path: str,
    command_args: dict[str, Any],
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> dict[str, Any]:
    report = run_research_walk_forward(
        manifest=manifest,
        db_path=db_path,
        manager=manager,
        generated_at=generated_at,
        execution_calibration=execution_calibration,
        manifest_path=manifest_path,
        command_args=command_args,
        progress_callback=progress_callback,
    )
    _record_report_stage(stage, report, "walk_forward_report")
    if report.get("promotion_eligibility_gate_result") != PASS:
        stage.status = FAIL_CLOSED
        stage.reasons.extend(str(item) for item in report.get("promotion_blocking_reasons") or ["walk_forward_promotion_gate_failed"])
    return report


def _stage_promotion(
    *,
    stage: ValidationStage,
    experiment_id: str,
    candidate_id: str,
    manager: PathManager,
    validation_run_path: str | None,
    validation_run_binding_hash: str | None,
    validation_policy_source: str | None,
    validation_policy_required_stage_names: tuple[str, ...] | list[str],
) -> Any:
    from .promotion_gate import PromotionGateError, promote_candidate

    try:
        result = promote_candidate(
            experiment_id=experiment_id,
            candidate_id=candidate_id,
            manager=manager,
            validation_run_path=validation_run_path,
            validation_run_binding_hash=validation_run_binding_hash,
            allow_pending_validation_run=True,
            validation_policy_source=validation_policy_source,
            validation_policy_required_stage_names=validation_policy_required_stage_names,
        )
    except PromotionGateError as exc:
        stage.status = FAIL_CLOSED
        stage.reasons.append(str(exc))
        raise
    stage.artifact_paths["promotion_artifact_path"] = str(result.artifact_path.resolve())
    stage.artifact_hashes["promotion_artifact_hash"] = result.content_hash
    return result


def _stage_reproduce(*, stage: ValidationStage, promotion_path: str) -> dict[str, Any]:
    result = reproduce_promotion(promotion_path)
    stage.artifact_paths["promotion_artifact_path"] = promotion_path
    stage.output_hashes["reproduce_summary_hash"] = sha256_prefixed(result.summary)
    if not result.ok:
        stage.status = FAIL_CLOSED
        stage.reasons.append(str(result.summary.get("reason") or "reproduce_failed"))
    return result.summary


def _record_report_stage(stage: ValidationStage, report: dict[str, Any], label: str) -> None:
    report_hash = str(report.get("content_hash") or "")
    if report_hash:
        stage.artifact_hashes[f"{label}_hash"] = report_hash
    path = _report_path(report)
    if path:
        stage.artifact_paths[f"{label}_path"] = path


def _bind_report_evidence(stage: ValidationStage, *, report_path: str | None, report_hash: str) -> None:
    if report_path:
        stage.artifact_paths.setdefault("backtest_report_path", report_path)
    if report_hash:
        stage.artifact_hashes.setdefault("backtest_report_hash", report_hash)


def _record_hash_if_present(target: dict[str, Any], key: str, value: Any) -> None:
    if _is_hash(value):
        target[key] = str(value)


def _is_hash(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("sha256:")


def _dataset_split(report: dict[str, Any], split_name: str) -> dict[str, Any] | None:
    splits = report.get("dataset_splits")
    if not isinstance(splits, dict):
        return None
    split = splits.get(split_name)
    return split if isinstance(split, dict) else None


def _report_path(report: dict[str, Any]) -> str | None:
    paths = report.get("artifact_paths")
    if isinstance(paths, dict) and paths.get("report_path"):
        return str(paths["report_path"])
    return None


def _select_candidate_id(report: dict[str, Any], requested: str | None) -> str | None:
    selected = requested or report.get("selected_candidate_id") or report.get("best_candidate_id")
    if not selected:
        return None
    candidates = report.get("candidates")
    if isinstance(candidates, list) and any(
        isinstance(candidate, dict) and candidate.get("parameter_candidate_id") == selected for candidate in candidates
    ):
        return str(selected)
    return None


def _evidence_mismatch_reasons(
    *,
    backtest_report: dict[str, Any],
    walk_forward_report: dict[str, Any],
    candidate_id: str,
) -> list[str]:
    reasons: list[str] = []
    for key in (
        "experiment_id",
        "manifest_hash",
        "strategy_name",
        "deployment_tier",
        "execution_model",
        "execution_calibration_required",
        "execution_calibration_artifact_hash",
    ):
        if backtest_report.get(key) != walk_forward_report.get(key):
            reasons.append(f"{key}_mismatch")
    backtest_candidate = _candidate(backtest_report, candidate_id)
    walk_candidate = _candidate(walk_forward_report, candidate_id)
    if not isinstance(backtest_candidate, dict) or not isinstance(walk_candidate, dict):
        reasons.append("candidate_missing")
        return reasons
    for key in (
        "parameter_values",
        "cost_model",
        "base_cost_assumption",
        "cost_assumption_contract",
        "execution_model",
        "execution_calibration_gate",
        "execution_calibration_artifact_hash",
        "execution_calibration_artifact_hashes",
        "manifest_hash",
    ):
        if backtest_candidate.get(key) != walk_candidate.get(key):
            reasons.append(f"candidate_{key}_mismatch")
    return sorted(set(reasons))


def _candidate(report: dict[str, Any], candidate_id: str) -> dict[str, Any] | None:
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        return None
    return next(
        (
            candidate for candidate in candidates
            if isinstance(candidate, dict) and candidate.get("parameter_candidate_id") == candidate_id
        ),
        None,
    )


def _finalize_validation_run(run: ValidationRun, *, manager: PathManager, out_path: str | Path | None) -> dict[str, Any]:
    fail_reasons: list[str] = []
    for stage in run.stages:
        if stage.required and stage.status in TERMINAL_BAD_STATUSES:
            fail_reasons.append(f"{stage.name}:{stage.status}")
            fail_reasons.extend(stage.reasons)
    if run.promotion_allowed is not True:
        fail_reasons.append("promotion_not_allowed")
    if run.reproduce_ok is not True:
        fail_reasons.append("reproduce_not_ok")
    run.fail_closed_reasons = sorted(set(str(item) for item in fail_reasons if str(item)))
    run.end_to_end_validation_result = FAIL_CLOSED if run.fail_closed_reasons else PASS
    path, content_hash = write_validation_run(manager=manager, validation_run=run, out_path=out_path)
    payload = run.as_dict()
    payload["validation_run_path"] = str(path.resolve())
    payload[VALIDATION_RUN_HASH_FIELD] = content_hash
    return payload


def _resolved_validation_run_path(*, manager: PathManager, experiment_id: str, out_path: str | Path | None) -> Path:
    path = Path(out_path).expanduser() if out_path else default_validation_run_path(
        manager=manager,
        experiment_id=experiment_id,
    )
    _ensure_research_output_path_allowed(manager, path)
    return path


def _has_failures(run: ValidationRun) -> bool:
    return any(stage.required and stage.status in {FAIL_CLOSED, ERROR} for stage in run.stages)


def _first_active_or_not_run_required_stage(run: ValidationRun) -> ValidationStage:
    for stage in run.stages:
        if stage.required and stage.status == NOT_RUN:
            return stage
    return next(stage for stage in reversed(run.stages) if stage.required)


def _stage(run: ValidationRun, name: str) -> ValidationStage:
    for stage in run.stages:
        if stage.name == name:
            return stage
    raise ValidationRunError(f"validation_stage_missing:{name}")


def validate_promotion_validation_run(
    *,
    validation_run_path: str | Path,
    experiment_id: str,
    manifest_hash: str,
    candidate_id: str,
    backtest_report_hash: str,
    walk_forward_report_hash: str | None,
) -> tuple[dict[str, Any], list[str]]:
    return load_and_verify_validation_run(
        validation_run_path,
        experiment_id=experiment_id,
        manifest_hash=manifest_hash,
        selected_candidate_id=candidate_id,
        backtest_report_hash=backtest_report_hash,
        walk_forward_report_hash=walk_forward_report_hash,
        require_pass=True,
    )


def validation_run_required_for_promotion(*, deployment_tier: object) -> bool:
    return is_production_bound_target(deployment_tier)


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"validation run output path must be outside repository: {resolved}")
