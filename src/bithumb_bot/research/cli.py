from __future__ import annotations

import json
from pathlib import Path
from time import monotonic

from bithumb_bot.config import PATH_MANAGER, settings
from bithumb_bot.notifier import AlertSeverity, format_event, notify

from .experiment_manifest import ManifestValidationError, load_manifest
from .experiment_registry import (
    PROMOTION_PERMITTED_STATUSES,
    append_attempt_aborted,
    compute_row_hash,
    experiment_registry_path,
    load_experiment_registry_rows,
    validate_experiment_registry_binding,
)
from .hashing import content_hash_payload, report_content_hash_payload, sha256_prefixed
from .audit_trail import validate_audit_trail_binding, verify_audit_trail
from .return_panel import validate_return_panel_binding
from .execution_calibration import ExecutionCalibrationError, load_calibration_artifact
from .deployment_policy import is_production_bound_target
from .promotion_gate import PromotionGateError, promote_candidate
from .lineage import reproduce_promotion
from .run_summary import ResearchRunSummary, build_research_run_summary
from .validation_pipeline import ValidationRunError, run_research_validation
from .validation_protocol import ResearchValidationError, run_research_backtest, run_research_walk_forward
from .forward_diagnostics_cli import cmd_research_forward_diagnostics


def _notify_research_command_finished(command: str, started_at: float, rc: int, **fields: object) -> None:
    status = "success" if rc == 0 else "failure"
    notify(
        format_event(
            "research_command_finished",
            command=command,
            status=status,
            exit_code=rc,
            elapsed_sec=f"{monotonic() - started_at:.1f}",
            **fields,
        ),
        severity=AlertSeverity.INFO if rc == 0 else AlertSeverity.WARN,
    )


def cmd_research_backtest(*, manifest_path: str, execution_calibration_path: str | None = None) -> int:
    started_at = monotonic()
    rc = 1
    try:
        try:
            manifest = load_manifest(manifest_path)
            calibration = load_calibration_artifact(execution_calibration_path) if execution_calibration_path else None
            report = run_research_backtest(
                manifest=manifest,
                db_path=settings.DB_PATH,
                manager=PATH_MANAGER,
                execution_calibration=calibration,
                manifest_path=manifest_path,
                command_args={
                    "manifest": manifest_path,
                    "execution_calibration": execution_calibration_path,
                },
                progress_callback=_print_research_backtest_progress,
            )
        except (ManifestValidationError, ExecutionCalibrationError, ResearchValidationError, OSError, ValueError) as exc:
            print(f"[RESEARCH-BACKTEST] error={exc}")
            rc = 1
            return rc
        _print_report_summary("RESEARCH-BACKTEST", report)
        if _standalone_report_is_non_promotable_production_diagnostic(report):
            rc = 1
            return rc
        rc = 0
        return rc
    finally:
        _notify_research_command_finished(
            "research-backtest",
            started_at,
            rc,
            manifest=manifest_path,
            execution_calibration=execution_calibration_path,
        )


def cmd_research_walk_forward(*, manifest_path: str, execution_calibration_path: str | None = None) -> int:
    started_at = monotonic()
    rc = 1
    try:
        try:
            manifest = load_manifest(manifest_path)
            calibration = load_calibration_artifact(execution_calibration_path) if execution_calibration_path else None
            report = run_research_walk_forward(
                manifest=manifest,
                db_path=settings.DB_PATH,
                manager=PATH_MANAGER,
                execution_calibration=calibration,
                manifest_path=manifest_path,
                command_args={
                    "manifest": manifest_path,
                    "execution_calibration": execution_calibration_path,
                },
                progress_callback=_print_research_walk_forward_progress,
            )
        except (ManifestValidationError, ExecutionCalibrationError, ResearchValidationError, OSError, ValueError) as exc:
            print(f"[RESEARCH-WALK-FORWARD] error={exc}")
            rc = 1
            return rc
        _print_report_summary("RESEARCH-WALK-FORWARD", report)
        if _standalone_report_is_non_promotable_production_diagnostic(report):
            rc = 1
            return rc
        rc = 0
        return rc
    finally:
        _notify_research_command_finished(
            "research-walk-forward",
            started_at,
            rc,
            manifest=manifest_path,
            execution_calibration=execution_calibration_path,
        )


def cmd_research_validate(
    *,
    manifest_path: str,
    execution_calibration_path: str | None = None,
    candidate_id: str | None = None,
    out_path: str | None = None,
    mode: str = "strict",
) -> int:
    started_at = monotonic()
    rc = 1
    try:
        try:
            manifest = load_manifest(manifest_path)
            calibration = load_calibration_artifact(execution_calibration_path) if execution_calibration_path else None
            validation_run = run_research_validation(
                manifest=manifest,
                db_path=settings.DB_PATH,
                manager=PATH_MANAGER,
                manifest_path=manifest_path,
                mode=mode,
                execution_calibration=calibration,
                execution_calibration_path=execution_calibration_path,
                candidate_id=candidate_id,
                out_path=out_path,
                progress_callback=_print_research_backtest_progress,
            )
        except (
            ManifestValidationError,
            ExecutionCalibrationError,
            ResearchValidationError,
            ValidationRunError,
            OSError,
            ValueError,
        ) as exc:
            print(f"[RESEARCH-VALIDATE] error={exc}")
            rc = 1
            return rc
        _print_validation_run_summary(validation_run)
        rc = 0 if validation_run.get("end_to_end_validation_result") == "PASS" else 1
        return rc
    finally:
        _notify_research_command_finished(
            "research-validate",
            started_at,
            rc,
            manifest=manifest_path,
            execution_calibration=execution_calibration_path,
            candidate_id=candidate_id,
            out=out_path,
            mode=mode,
        )


def cmd_research_reproduce(*, promotion_path: str) -> int:
    result = reproduce_promotion(promotion_path, manager=PATH_MANAGER)
    print(json.dumps(result.summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result.ok else 1


def cmd_research_registry_inspect(*, row_hash: str) -> int:
    path = experiment_registry_path(manager=PATH_MANAGER)
    rows = load_experiment_registry_rows(path)
    row = next((item for item in rows if item.get("row_hash") == row_hash), None)
    if not isinstance(row, dict):
        print(json.dumps({"ok": False, "reason": "experiment_registry_row_hash_mismatch", "row_hash": row_hash}, sort_keys=True, indent=2))
        return 1
    completion = next(
        (
            item
            for item in reversed(rows)
            if item.get("event_type") in {"research_attempt_completed", "research_attempt_aborted"}
            and item.get("reservation_row_hash") == row_hash
        ),
        None,
    )
    summary = {
        "ok": True,
        "registry_path": str(path.resolve()),
        "row": row,
        "completion_or_abort": completion,
        "attempt_status": completion.get("result_status") if isinstance(completion, dict) else row.get("result_status"),
        "incomplete": completion is None and row.get("event_type") == "research_attempt_reserved",
    }
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


def cmd_research_registry_validate(*, experiment_id: str) -> int:
    path = experiment_registry_path(manager=PATH_MANAGER)
    rows = load_experiment_registry_rows(path)
    reservations = [
        item
        for item in rows
        if item.get("event_type") == "research_attempt_reserved" and item.get("experiment_id") == experiment_id
    ]
    if not reservations:
        print(json.dumps({
            "ok": False,
            "validation_scope": "registry_only",
            "reason": "experiment_registry_row_hash_mismatch",
            "experiment_id": experiment_id,
            "artifact_binding_valid": "unknown",
            "report_loaded": False,
            "evidence_loaded": False,
            "return_panel_loaded": False,
            "warning": "artifact_binding_not_checked",
        }, sort_keys=True, indent=2))
        return 1
    ok = True
    report_path = PATH_MANAGER.data_dir() / "reports" / "research" / experiment_id / "backtest_report.json"
    evidence_path = PATH_MANAGER.data_dir() / "reports" / "research" / experiment_id / "statistical_selection_evidence.json"
    panel_path = PATH_MANAGER.data_dir() / "reports" / "research" / experiment_id / "candidate_return_panel.json"
    report = _load_json_if_exists(report_path)
    evidence = _load_json_if_exists(evidence_path)
    panel = _load_json_if_exists(panel_path)
    artifact_reasons: list[str] = []
    validation_scope = "registry_and_artifacts" if isinstance(report, dict) else "registry_only"
    report_loaded = isinstance(report, dict)
    evidence_loaded = isinstance(evidence, dict)
    return_panel_loaded = isinstance(panel, dict)
    artifact_bound_row_hash: str | None = None
    artifact_binding_valid: bool | str = "unknown"
    if report_loaded:
        evidence_row_hash = str(evidence.get("experiment_registry_row_hash") or "").strip() if isinstance(evidence, dict) else ""
        report_row_hash = str(report.get("experiment_registry_row_hash") or "").strip()
        if evidence_row_hash and report_row_hash and evidence_row_hash != report_row_hash:
            artifact_reasons.append("experiment_registry_report_evidence_row_hash_mismatch")
            artifact_reasons.append("experiment_registry_artifact_bound_row_hash_mismatch")
        artifact_bound_row_hash = evidence_row_hash or report_row_hash or None
        if not artifact_bound_row_hash:
            artifact_reasons.append("experiment_registry_row_hash_missing")
        elif not any(row.get("row_hash") == artifact_bound_row_hash for row in reservations):
            artifact_reasons.append("experiment_registry_artifact_bound_row_missing")
        artifact_reasons.extend(_content_hash_reasons(report, report_hash=True, label="backtest_report"))
        evidence_required = bool(report.get("statistical_validation_required")) or bool(report.get("statistical_evidence_hash"))
        if evidence_required and not evidence_loaded:
            artifact_reasons.append("statistical_evidence_missing")
        if evidence_loaded:
            artifact_reasons.extend(_content_hash_reasons(evidence, report_hash=False, label="statistical_evidence"))
            artifact_reasons.extend(validate_return_panel_binding(report=report, evidence=evidence, panel=panel))
        artifact_reasons.extend(validate_audit_trail_binding(report=report, manager=PATH_MANAGER))
    else:
        artifact_reasons.append("artifact_binding_not_checked")
    if validation_scope == "registry_and_artifacts" and artifact_bound_row_hash and "experiment_registry_artifact_bound_row_missing" not in artifact_reasons:
        bound_report = dict(report) if isinstance(report, dict) else {}
        bound_completion = _completion_for_row(rows, artifact_bound_row_hash)
        if isinstance(bound_completion, dict) and bound_report.get("experiment_registry_completion_row_hash") is None:
            bound_report["experiment_registry_completion_row_hash"] = bound_completion.get("row_hash")
        binding_reasons = validate_experiment_registry_binding(
            report=bound_report,
            evidence=evidence if isinstance(evidence, dict) else None,
            require_complete=True,
        )
        artifact_reasons.extend(binding_reasons)
        artifact_binding_valid = not artifact_reasons
    elif validation_scope == "registry_and_artifacts":
        artifact_binding_valid = False
    lifecycle_summary = []
    for row in reservations:
        completion = _completion_for_row(rows, str(row.get("row_hash") or ""))
        lifecycle = _registry_lifecycle_row(row=row, completion=completion, artifact_bound=row.get("row_hash") == artifact_bound_row_hash)
        lifecycle["report_loaded"] = report_loaded
        lifecycle["evidence_loaded"] = evidence_loaded
        lifecycle["return_panel_loaded"] = return_panel_loaded
        if lifecycle["artifact_bound"]:
            lifecycle["artifact_binding_valid"] = artifact_binding_valid
            lifecycle["reasons"] = sorted(set([str(item) for item in lifecycle["reasons"]] + artifact_reasons))
        lifecycle_summary.append(lifecycle)
        ok = ok and lifecycle["registry_row_valid"]
    if validation_scope == "registry_and_artifacts":
        ok = ok and artifact_binding_valid is True
    payload = {
        "ok": ok,
        "validation_scope": validation_scope,
        "experiment_id": experiment_id,
        "registry_path": str(path.resolve()),
        "artifact_bound_row_hash": artifact_bound_row_hash,
        "artifact_reasons": sorted(set(artifact_reasons)),
        "report_path": str(report_path.resolve()),
        "evidence_path": str(evidence_path.resolve()),
        "return_panel_path": str(panel_path.resolve()),
        "report_loaded": report_loaded,
        "evidence_loaded": evidence_loaded,
        "return_panel_loaded": return_panel_loaded,
        "artifact_binding_valid": artifact_binding_valid,
        "warning": "artifact_binding_not_checked" if validation_scope == "registry_only" else None,
        "registry_lifecycle_summary": lifecycle_summary,
        "results": lifecycle_summary,
    }
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0 if ok else 1


def cmd_research_verify_audit(*, experiment_id: str) -> int:
    result = verify_audit_trail(manager=PATH_MANAGER, experiment_id=experiment_id)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result.get("ok") is True else 1


def _completion_for_row(rows: list[dict[str, object]], row_hash: str) -> dict[str, object] | None:
    return next(
        (
            item
            for item in reversed(rows)
            if item.get("event_type") in {"research_attempt_completed", "research_attempt_aborted"}
            and item.get("reservation_row_hash") == row_hash
        ),
        None,
    )


def _registry_lifecycle_row(
    *,
    row: dict[str, object],
    completion: dict[str, object] | None,
    artifact_bound: bool,
) -> dict[str, object]:
    reasons: list[str] = []
    registry_row_valid = compute_row_hash(row) == row.get("row_hash")
    if not registry_row_valid:
        reasons.append("experiment_registry_row_hash_mismatch")
    completion_status = str(completion.get("result_status") or "") if isinstance(completion, dict) else str(row.get("result_status") or "")
    lifecycle_complete = completion_status in PROMOTION_PERMITTED_STATUSES
    incomplete = not lifecycle_complete
    if not lifecycle_complete:
        reasons.append("experiment_registry_incomplete_attempt")
    completion_row_valid = True
    if isinstance(completion, dict):
        completion_row_valid = compute_row_hash(completion) == completion.get("row_hash")
        if not completion_row_valid:
            reasons.append("experiment_registry_row_hash_mismatch")
    row_valid_only = registry_row_valid and not lifecycle_complete
    return {
        "row_hash": row.get("row_hash"),
        "artifact_bound": artifact_bound,
        "event_type": row.get("event_type"),
        "result_status": row.get("result_status"),
        "registry_row_valid": registry_row_valid,
        "completion_row_valid": completion_row_valid,
        "completion_row_hash": completion.get("row_hash") if isinstance(completion, dict) else None,
        "completion_status": completion_status,
        "incomplete": incomplete,
        "lifecycle_complete": lifecycle_complete,
        "promotion_permitted": lifecycle_complete,
        "row_valid_only": row_valid_only,
        "artifact_binding_valid": "unknown",
        "report_loaded": False,
        "evidence_loaded": False,
        "return_panel_loaded": False,
        "ok": registry_row_valid and completion_row_valid and lifecycle_complete,
        "reasons": sorted(set(reasons)),
    }


def _load_json_if_exists(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else None


def _content_hash_reasons(payload: dict[str, object], *, report_hash: bool, label: str) -> list[str]:
    expected = str(payload.get("content_hash") or "").strip()
    if not expected.startswith("sha256:"):
        return [f"{label}_content_hash_missing"]
    actual = sha256_prefixed(
        report_content_hash_payload(payload) if report_hash else content_hash_payload({k: v for k, v in payload.items() if k != "content_hash"})
    )
    return [] if actual == expected else [f"{label}_content_hash_mismatch"]


def cmd_research_mark_attempt_aborted(*, row_hash: str, reason: str) -> int:
    result = append_attempt_aborted(manager=PATH_MANAGER, reservation_row_hash=row_hash, reason=reason)
    if result is None:
        print(json.dumps({"ok": False, "reason": "experiment_registry_row_hash_mismatch", "row_hash": row_hash}, sort_keys=True, indent=2))
        return 1
    print(json.dumps({"ok": True, **result}, sort_keys=True, indent=2))
    return 0


def cmd_research_promote_candidate(
    *,
    experiment_id: str,
    candidate_id: str,
    allow_legacy_lineage: bool = False,
    validation_run_path: str | None = None,
) -> int:
    try:
        result = promote_candidate(
            experiment_id=experiment_id,
            candidate_id=candidate_id,
            manager=PATH_MANAGER,
            allow_legacy_lineage=allow_legacy_lineage,
            validation_run_path=validation_run_path,
        )
    except PromotionGateError as exc:
        print(f"[RESEARCH-PROMOTE-CANDIDATE] error={exc}")
        return 1
    print("[RESEARCH-PROMOTE-CANDIDATE]")
    print(f"  experiment_id={experiment_id}")
    print(f"  candidate_id={candidate_id}")
    print(f"  gate_result={result.artifact['gate_result']}")
    print(f"  artifact_path={result.artifact_path}")
    print(f"  content_hash={result.content_hash}")
    print(f"  statistical_validation_required={1 if result.artifact.get('statistical_validation_required') else 0}")
    print(f"  validation_policy_source={result.artifact.get('validation_policy_source') or 'none'}")
    print(
        "  validation_policy_required_stage_names="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('validation_policy_required_stage_names') or []))}"
    )
    print(f"  effective_walk_forward_required={1 if result.artifact.get('effective_walk_forward_required') else 0}")
    print(f"  effective_final_holdout_required={1 if result.artifact.get('effective_final_holdout_required') else 0}")
    print(f"  effective_stress_suite_required={1 if result.artifact.get('effective_stress_suite_required') else 0}")
    print(
        "  effective_statistical_validation_required="
        f"{1 if result.artifact.get('effective_statistical_validation_required') else 0}"
    )
    print(f"  effective_final_selection_required={1 if result.artifact.get('effective_final_selection_required') else 0}")
    print(f"  selection_universe_hash={result.artifact.get('selection_universe_hash') or 'none'}")
    print(f"  statistical_evidence_hash={result.artifact.get('statistical_evidence_hash') or 'none'}")
    print(f"  evidence_grade={result.artifact.get('evidence_grade') or 'none'}")
    print(f"  statistical_method={result.artifact.get('statistical_method') or 'none'}")
    print(
        "  official_promotion_grade_wrc_generation_available="
        f"{1 if result.artifact.get('official_promotion_grade_wrc_generation_available') else 0}"
    )
    print(
        "  promotion_grade_limitations="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('promotion_grade_limitations') or []))}"
    )
    print(
        "  promotion_blocking_reasons="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('promotion_blocking_reasons') or []))}"
    )
    print(f"  return_panel_hash={result.artifact.get('return_panel_hash') or 'none'}")
    print(f"  return_unit={result.artifact.get('return_unit') or 'none'}")
    print(f"  return_panel_observation_count={result.artifact.get('return_panel_observation_count')}")
    print(f"  family_trial_registry_path={result.artifact.get('family_trial_registry_path') or 'none'}")
    print(f"  family_trial_registry_prior_hash={result.artifact.get('family_trial_registry_prior_hash') or 'none'}")
    print(f"  family_trial_registry_row_hash={result.artifact.get('family_trial_registry_row_hash') or 'none'}")
    _print_experiment_registry_summary(result.artifact)
    print(f"  white_reality_check_p_value={result.artifact.get('white_reality_check_p_value')}")
    print(f"  white_reality_check_method={result.artifact.get('white_reality_check_method') or 'none'}")
    print(f"  summary_metric_max_bootstrap_p_value={result.artifact.get('summary_metric_max_bootstrap_p_value')}")
    print(f"  bootstrap_sampling_contract_hash={result.artifact.get('bootstrap_sampling_contract_hash') or 'none'}")
    print(f"  statistical_gate_result={result.artifact.get('statistical_gate_result') or 'none'}")
    print(
        "  statistical_gate_fail_reasons="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('statistical_gate_fail_reasons') or []))}"
    )
    _print_execution_capability_summary(result.artifact)
    _print_stress_suite_summary(result.artifact)
    _print_execution_event_summary(result.artifact.get("execution_event_summary"))
    print(
        "  has_execution_calibration_warning="
        f"{1 if result.artifact.get('has_execution_calibration_warning') else 0}"
    )
    print(
        "  execution_calibration_warning_reasons="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('execution_calibration_warning_reasons') or []))}"
    )
    print(
        "  promotion_warnings="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('promotion_warnings') or []))}"
    )
    print(f"  legacy_compatibility_used={1 if result.artifact.get('legacy_compatibility_used') else 0}")
    print(f"  validation_run_required={1 if result.artifact.get('validation_run_required') else 0}")
    print(f"  validation_run_binding_status={result.artifact.get('validation_run_binding_status') or 'none'}")
    print(f"  validation_run_hash={result.artifact.get('validation_run_hash') or 'none'}")
    print(f"  validation_run_binding_hash={result.artifact.get('validation_run_binding_hash') or 'none'}")
    print(
        "  validation_run_promotion_artifact_hash="
        f"{result.artifact.get('validation_run_promotion_artifact_hash') or 'none'}"
    )
    print(
        "  dataset_quality_legacy_bypass_used="
        f"{1 if result.artifact.get('dataset_quality_legacy_bypass_used') else 0}"
    )
    print(f"  operator_next_step={result.artifact['operator_next_step']}")
    return 0


def _print_validation_run_summary(payload: dict[str, object]) -> None:
    print("[RESEARCH-VALIDATE]")
    print(f"  validation_run_path={payload.get('validation_run_path')}")
    print(f"  validation_run_hash={payload.get('content_hash')}")
    print(f"  validation_run_binding_hash={payload.get('validation_run_binding_hash')}")
    print(f"  validation_policy_source={payload.get('validation_policy_source') or 'none'}")
    print(
        "  validation_policy_required_stage_names="
        f"{_format_items(tuple(str(item) for item in payload.get('validation_policy_required_stage_names') or []))}"
    )
    print(f"  end_to_end_validation_result={payload.get('end_to_end_validation_result')}")
    print(f"  selected_candidate_id={payload.get('selected_candidate_id') or 'none'}")
    print(f"  backtest_report_hash={payload.get('backtest_report_hash') or 'none'}")
    print(f"  walk_forward_report_hash={payload.get('walk_forward_report_hash') or 'none'}")
    print(f"  promotion_artifact_hash={payload.get('promotion_artifact_hash') or 'none'}")
    print(f"  reproduce_ok={1 if payload.get('reproduce_ok') else 0}")
    reasons = payload.get("fail_closed_reasons") or []
    print(f"  fail_closed_reasons={_format_items(tuple(str(item) for item in reasons))}")
    print(f"  next_required_action={_validation_next_action(payload)}")


def _standalone_report_is_non_promotable_production_diagnostic(report: dict[str, object]) -> bool:
    if not is_production_bound_target(report.get("deployment_tier")):
        return False
    if report.get("validation_run_complete") is True and report.get("promotion_eligibility_gate_result") == "PASS":
        return False
    if report.get("diagnostic_only") is True or report.get("standalone_backtest_not_full_validation") is True:
        return True
    return report.get("promotion_eligibility_gate_result") != "PASS"


def _validation_next_action(payload: dict[str, object]) -> str:
    if payload.get("end_to_end_validation_result") == "PASS":
        return "review_validation_run_and_promotion_artifact"
    reasons = [str(item) for item in payload.get("fail_closed_reasons") or []]
    if any("readiness" in reason for reason in reasons):
        return "fix_data_readiness_then_rerun_research-validate"
    if any("walk_forward" in reason for reason in reasons):
        return "fix_walk_forward_evidence_then_rerun_research-validate"
    if any("reproduce" in reason for reason in reasons):
        return "inspect_promotion_reproducibility_then_rerun_research-validate"
    return "inspect_validation_run_failure_reasons"


def _print_report_summary(label: str, report: dict[str, object]) -> None:
    artifact_paths = report.get("artifact_paths") if isinstance(report.get("artifact_paths"), dict) else {}
    summary = build_research_run_summary(report)
    print(f"[{label}]")
    print(f"  experiment_id={report.get('experiment_id')}")
    print(f"  manifest_hash={report.get('manifest_hash')}")
    print(f"  dataset_snapshot_id={report.get('dataset_snapshot_id')}")
    print(f"  dataset_content_hash={report.get('dataset_content_hash')}")
    print(f"  candidates_evaluated={report.get('candidate_count')}")
    print(f"  best_candidate_id={report.get('best_candidate_id') or 'none'}")
    print(f"  final_selection_required={1 if report.get('final_selection_required') else 0}")
    print(f"  final_selection_gate_result={report.get('final_selection_gate_result') or 'none'}")
    print(
        "  final_selection_fail_reasons="
        f"{_format_items(tuple(str(item) for item in report.get('final_selection_fail_reasons') or []))}"
    )
    print(f"  selected_candidate_id={report.get('selected_candidate_id') or 'none'}")
    print(f"  selected_candidate_score_hash={report.get('selected_candidate_score_hash') or 'none'}")
    print(f"  final_selection_contract_hash={report.get('final_selection_contract_hash') or 'none'}")
    print(f"  candidate_final_scores_hash={report.get('candidate_final_scores_hash') or 'none'}")
    _print_final_selection_components(report)
    print(f"  gate_result={report.get('gate_result')}")
    print(f"  promotion_eligibility_gate_result={report.get('promotion_eligibility_gate_result') or report.get('gate_result')}")
    print(
        "  promotion_blocking_reasons="
        f"{_format_items(tuple(str(item) for item in report.get('promotion_blocking_reasons') or []))}"
    )
    print(f"  candidate_gate_counts={_format_counts(summary.candidate_gate_counts)}")
    print(f"  top_fail_reasons={_format_counts(summary.top_fail_reasons)}")
    print(f"  strategy_diagnostics_summary={_format_strategy_diagnostics_summary(summary)}")
    print(f"  top_exit_reasons={_format_counts(summary.top_exit_reasons)}")
    print(
        "  validation_raw_sell_filter_blocked_while_in_position_count="
        f"{_format_optional(summary.validation_raw_sell_filter_blocked_while_in_position_count)}"
    )
    print(
        "  final_holdout_raw_sell_filter_blocked_while_in_position_count="
        f"{_format_optional(summary.final_holdout_raw_sell_filter_blocked_while_in_position_count)}"
    )
    print(f"  validation_p95_mae_pct={_format_optional(summary.validation_p95_mae_pct)}")
    print(f"  final_holdout_p95_mae_pct={_format_optional(summary.final_holdout_p95_mae_pct)}")
    print(f"  validation_worst_trade_mae_pct={_format_optional(summary.validation_worst_trade_mae_pct)}")
    print(f"  final_holdout_worst_trade_mae_pct={_format_optional(summary.final_holdout_worst_trade_mae_pct)}")
    print(f"  promotion_allowed={1 if summary.promotion_allowed else 0}")
    print(f"  validation_run_complete={1 if report.get('validation_run_complete') else 0}")
    print(f"  diagnostic_only={1 if report.get('diagnostic_only') else 0}")
    print(f"  next_required_stage={report.get('next_required_stage') or 'none'}")
    if report.get("standalone_backtest_not_full_validation"):
        print("  reason=standalone_backtest_not_full_validation")
    print(f"  statistical_validation_required={1 if report.get('statistical_validation_required') else 0}")
    print(f"  statistical_candidate_count={report.get('candidate_count')}")
    print(f"  statistical_parameter_grid_size={report.get('parameter_grid_size')}")
    print(f"  statistical_search_budget={report.get('search_budget')}")
    print(f"  statistical_attempt_index={report.get('attempt_index')}")
    print(f"  statistical_holdout_reuse_count={report.get('holdout_reuse_count')}")
    print(f"  selection_universe_hash={report.get('selection_universe_hash') or 'none'}")
    print(f"  candidate_metric_values_hash={report.get('candidate_metric_values_hash') or 'none'}")
    print(f"  statistical_metric_value_count={report.get('metric_value_count')}")
    print(f"  statistical_missing_metric_count={report.get('missing_metric_count')}")
    print(f"  statistical_evidence_hash={report.get('statistical_evidence_hash') or 'none'}")
    print(f"  evidence_grade={report.get('evidence_grade') or 'none'}")
    print(f"  statistical_method={report.get('statistical_method') or 'none'}")
    print(
        "  official_promotion_grade_wrc_generation_available="
        f"{1 if report.get('official_promotion_grade_wrc_generation_available') else 0}"
    )
    print(
        "  promotion_grade_limitations="
        f"{_format_items(tuple(str(item) for item in report.get('promotion_grade_limitations') or []))}"
    )
    print(f"  return_panel_hash={report.get('return_panel_hash') or 'none'}")
    print(f"  return_unit={report.get('return_unit') or 'none'}")
    print(f"  return_panel_observation_count={report.get('return_panel_observation_count')}")
    print(f"  audit_mode={_nested(report, 'audit_trail_policy', 'mode') or 'none'}")
    print(f"  audit_status={report.get('audit_trail_status') or 'none'}")
    print(f"  audit_trace_manifest_ref={report.get('audit_trail_trace_manifest_ref') or 'none'}")
    print(f"  audit_trace_manifest_hash={report.get('audit_trail_trace_manifest_hash') or 'none'}")
    print(
        "  audit_fail_reasons="
        f"{_format_items(tuple(str(item) for item in report.get('audit_trail_fail_reasons') or []))}"
    )
    print(f"  family_trial_registry_path={report.get('family_trial_registry_path') or 'none'}")
    print(f"  family_trial_registry_prior_hash={report.get('family_trial_registry_prior_hash') or 'none'}")
    print(f"  family_trial_registry_row_hash={report.get('family_trial_registry_row_hash') or 'none'}")
    _print_experiment_registry_summary(report)
    print(f"  summary_metric_max_bootstrap_p_value={report.get('summary_metric_max_bootstrap_p_value')}")
    print(f"  white_reality_check_p_value={report.get('white_reality_check_p_value')}")
    print(f"  white_reality_check_method={report.get('white_reality_check_method') or 'none'}")
    print(f"  bootstrap_sampling_contract_hash={report.get('bootstrap_sampling_contract_hash') or 'none'}")
    print(f"  statistical_gate_result={report.get('statistical_gate_result') or 'none'}")
    print(
        "  statistical_gate_fail_reasons="
        f"{_format_items(tuple(str(item) for item in report.get('statistical_gate_fail_reasons') or []))}"
    )
    _print_stress_suite_summary(report)
    print(f"  nearest_failed_candidate_id={summary.nearest_failed_candidate_id or 'none'}")
    print(
        "  nearest_failed_candidate_fail_reasons="
        f"{_format_items(summary.nearest_failed_candidate_fail_reasons)}"
    )
    print(f"  walk_forward_window_summary={_format_walk_forward_window_summary(summary)}")
    print(f"  top_window_fail_reasons={_format_counts(summary.top_window_fail_reasons)}")
    print(f"  execution_reference_policy={_nested(report, 'execution_timing_policy', 'fill_reference_policy') or 'unknown'}")
    print(f"  execution_reality_level={report.get('execution_reality_level') or 'unknown'}")
    _print_execution_capability_summary(report)
    print(f"  execution_reality_gate_status={report.get('execution_reality_gate_status') or 'unknown'}")
    print(
        "  execution_reality_gate_reasons="
        f"{_format_items(tuple(str(item) for item in report.get('execution_reality_gate_reasons') or []))}"
    )
    signal_coverage = report.get("signal_quote_coverage_summary")
    if isinstance(signal_coverage, dict):
        print(
            "  signal_quote_coverage="
            f"signal_event_count={signal_coverage.get('signal_event_count')} "
            f"fillable_signal_event_count={signal_coverage.get('fillable_signal_event_count')} "
            f"missing_quote_on_signal_count={signal_coverage.get('missing_quote_on_signal_count')} "
            f"skipped_execution_signal_count={signal_coverage.get('skipped_execution_signal_count')} "
            f"missing_quote_warning_count={signal_coverage.get('missing_quote_warning_count')} "
            f"quote_after_decision_coverage_pct={signal_coverage.get('quote_after_decision_coverage_pct')} "
            f"median_quote_age_ms={signal_coverage.get('median_quote_age_ms_on_signal')} "
            f"p95_quote_age_ms={signal_coverage.get('p95_quote_age_ms_on_signal')} "
            f"latency_submit_count={signal_coverage.get('latency_applied_to_submit_ts_count')} "
            f"latency_reference_count={signal_coverage.get('latency_applied_to_fill_reference_count')} "
            f"execution_attempt_count={signal_coverage.get('execution_attempt_count')} "
            f"execution_filled_count={signal_coverage.get('execution_filled_count')} "
            f"filled_execution_count={signal_coverage.get('filled_execution_count')} "
            f"portfolio_applied_trade_count={signal_coverage.get('portfolio_applied_trade_count')} "
            f"pending_execution_count={signal_coverage.get('pending_execution_count')} "
            f"skipped_execution_count={signal_coverage.get('skipped_execution_count')} "
            f"failed_execution_count={signal_coverage.get('failed_execution_count')} "
            f"closed_trade_count={signal_coverage.get('closed_trade_count')} "
            f"execution_event_timeline_incomplete={signal_coverage.get('execution_event_timeline_incomplete')}"
        )
    execution_events = report.get("execution_event_summary")
    if isinstance(execution_events, dict):
        print(
            "  execution_event_summary="
            f"execution_attempt_count={execution_events.get('execution_attempt_count')} "
            f"execution_filled_count={execution_events.get('execution_filled_count')} "
            f"portfolio_applied_trade_count={execution_events.get('portfolio_applied_trade_count')} "
            f"pending_execution_count={execution_events.get('pending_execution_count')} "
            f"skipped_execution_count={execution_events.get('skipped_execution_count')} "
            f"failed_execution_count={execution_events.get('failed_execution_count')} "
            f"closed_trade_count={execution_events.get('closed_trade_count')} "
            f"execution_event_timeline_incomplete={execution_events.get('execution_event_timeline_incomplete')}"
        )
    _print_metrics_v2_summary(report)
    print(f"  next_action={summary.next_action}")
    print(f"  report_path={artifact_paths.get('report_path')}")
    print(f"  derived_path={artifact_paths.get('derived_path')}")
    print(f"  content_hash={report.get('content_hash')}")
    warnings = report.get("warnings") or []
    print(f"  warnings={','.join(str(item) for item in warnings) if warnings else 'none'}")
    _print_top_of_book_summary(report)


def _print_research_backtest_progress(event: dict[str, object]) -> None:
    _print_progress_event("RESEARCH-BACKTEST", event)


def _print_research_walk_forward_progress(event: dict[str, object]) -> None:
    _print_progress_event("RESEARCH-WALK-FORWARD", event)


def _print_progress_event(label: str, event: dict[str, object]) -> None:
    parts = [f"stage={event.get('stage', 'unknown')}"]
    for key in sorted(key for key in event if key != "stage"):
        value = event[key]
        if isinstance(value, bool):
            rendered = "1" if value else "0"
        else:
            rendered = str(value)
        parts.append(f"{key}={rendered}")
    print(f"[{label}] " + " ".join(parts), flush=True)


def _format_optional(value: object) -> str:
    return "None" if value is None else str(value)


def _format_strategy_diagnostics_summary(summary: ResearchRunSummary) -> str:
    return (
        "top_exit_reasons="
        f"{_format_counts(summary.top_exit_reasons)} "
        "validation_raw_sell_filter_blocked_while_in_position_count="
        f"{_format_optional(summary.validation_raw_sell_filter_blocked_while_in_position_count)} "
        "final_holdout_raw_sell_filter_blocked_while_in_position_count="
        f"{_format_optional(summary.final_holdout_raw_sell_filter_blocked_while_in_position_count)} "
        f"validation_p95_mae_pct={_format_optional(summary.validation_p95_mae_pct)} "
        f"final_holdout_p95_mae_pct={_format_optional(summary.final_holdout_p95_mae_pct)} "
        f"validation_worst_trade_mae_pct={_format_optional(summary.validation_worst_trade_mae_pct)} "
        f"final_holdout_worst_trade_mae_pct={_format_optional(summary.final_holdout_worst_trade_mae_pct)}"
    )


def _print_metrics_v2_summary(report: dict[str, object]) -> None:
    metrics = report.get("best_validation_metrics_v2")
    if isinstance(metrics, dict) and (
        metrics.get("metrics_status") == "unavailable" or metrics.get("metrics_v2_source") == "failure_fallback"
    ):
        print(
            "  metrics_v2_summary="
            f"status={metrics.get('metrics_status')} source={metrics.get('metrics_v2_source')}"
        )
        metrics = None
    if not isinstance(metrics, dict):
        candidates = report.get("candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                if isinstance(candidate, dict) and candidate.get("acceptance_gate_result") == "PASS":
                    metrics = candidate.get("validation_metrics_v2")
                    if isinstance(metrics, dict) and (
                        metrics.get("metrics_status") == "unavailable"
                        or metrics.get("metrics_v2_source") == "failure_fallback"
                    ):
                        print(
                            "  metrics_v2_summary="
                            f"status={metrics.get('metrics_status')} source={metrics.get('metrics_v2_source')}"
                        )
                        metrics = None
                        continue
                    break
    if not isinstance(metrics, dict):
        return
    return_risk = metrics.get("return_risk") if isinstance(metrics.get("return_risk"), dict) else {}
    trade_quality = metrics.get("trade_quality") if isinstance(metrics.get("trade_quality"), dict) else {}
    time_exposure = metrics.get("time_exposure") if isinstance(metrics.get("time_exposure"), dict) else {}
    cost_execution = metrics.get("cost_execution") if isinstance(metrics.get("cost_execution"), dict) else {}
    print(
        "  metrics_v2_summary="
        f"schema={metrics.get('metrics_schema_version')} "
        f"cagr_pct={return_risk.get('cagr_pct')} "
        f"expectancy_per_trade_krw={trade_quality.get('expectancy_per_trade_krw')} "
        f"exposure_time_pct={time_exposure.get('exposure_time_pct')} "
        f"avg_holding_time_ms={time_exposure.get('avg_holding_time_ms')} "
        f"open_position_at_end={return_risk.get('open_position_at_end')} "
        f"fee_drag_ratio={cost_execution.get('fee_drag_ratio')} "
        f"fee_drag_ratio_basis={cost_execution.get('fee_drag_ratio_basis')} "
        f"slippage_drag_ratio={cost_execution.get('slippage_drag_ratio')} "
        f"slippage_drag_ratio_basis={cost_execution.get('slippage_drag_ratio_basis')}"
    )


def _print_final_selection_components(report: dict[str, object]) -> None:
    selected = str(report.get("selected_candidate_id") or "")
    scores = report.get("candidate_final_scores")
    if not selected or not isinstance(scores, list):
        return
    row = next(
        (
            item for item in scores
            if isinstance(item, dict) and str(item.get("candidate_id") or "") == selected
        ),
        None,
    )
    if not isinstance(row, dict):
        return
    components = row.get("rank_components")
    if not isinstance(components, list):
        return
    for index, component in enumerate(components[:2], start=1):
        if not isinstance(component, dict):
            continue
        print(
            f"  top_final_selection_component_{index}="
            f"{component.get('metric')}:{component.get('value')}:{component.get('order')}"
        )


def _print_execution_event_summary(summary: object) -> None:
    if not isinstance(summary, dict):
        return
    print(
        "  execution_event_summary="
        f"execution_attempt_count={summary.get('execution_attempt_count')} "
        f"execution_filled_count={summary.get('execution_filled_count')} "
        f"portfolio_applied_trade_count={summary.get('portfolio_applied_trade_count')} "
        f"pending_execution_count={summary.get('pending_execution_count')} "
        f"pending_execution_after_dataset_end_count={summary.get('pending_execution_after_dataset_end_count')} "
        f"skipped_execution_count={summary.get('skipped_execution_count')} "
        f"failed_execution_count={summary.get('failed_execution_count')} "
        f"closed_trade_count={summary.get('closed_trade_count')} "
        f"execution_event_timeline_incomplete={summary.get('execution_event_timeline_incomplete')}"
    )


def _print_stress_suite_summary(payload: dict[str, object]) -> None:
    required = bool(payload.get("stress_suite_required"))
    evidence = payload.get("validation_stress_suite")
    if not isinstance(evidence, dict):
        evidence = payload.get("best_validation_stress_suite")
    trade_removal = evidence.get("trade_removal") if isinstance(evidence, dict) and isinstance(evidence.get("trade_removal"), dict) else {}
    monte_carlo = (
        evidence.get("trade_order_monte_carlo")
        if isinstance(evidence, dict) and isinstance(evidence.get("trade_order_monte_carlo"), dict)
        else {}
    )
    period_ablation = (
        evidence.get("period_ablation")
        if isinstance(evidence, dict) and isinstance(evidence.get("period_ablation"), dict)
        else {}
    )
    parameter_perturbation = (
        evidence.get("parameter_perturbation")
        if isinstance(evidence, dict) and isinstance(evidence.get("parameter_perturbation"), dict)
        else {}
    )
    print(f"  stress_suite_required={1 if required else 0}")
    print(f"  stress_suite_gate_result={payload.get('stress_suite_gate_result') or 'none'}")
    print(
        "  stress_suite_fail_reasons="
        f"{_format_items(tuple(str(item) for item in payload.get('stress_suite_fail_reasons') or []))}"
    )
    print(f"  stress_trade_removal_status={trade_removal.get('status') or 'none'}")
    print(f"  stress_period_ablation_status={period_ablation.get('status') or 'none'}")
    print(f"  stress_period_ablation_pass_ratio={period_ablation.get('pass_ratio')}")
    print(f"  stress_parameter_perturbation_status={parameter_perturbation.get('status') or 'none'}")
    print(f"  stress_parameter_perturbation_pass_ratio={parameter_perturbation.get('pass_ratio')}")
    print(f"  stress_monte_carlo_survival_probability={monte_carlo.get('survival_probability')}")
    print(f"  stress_monte_carlo_max_drawdown_pct_p95={monte_carlo.get('max_drawdown_pct_p95')}")


def _print_experiment_registry_summary(payload: dict[str, object]) -> None:
    print(f"  experiment_registry_path={payload.get('experiment_registry_path') or 'none'}")
    print(f"  experiment_registry_prior_hash={payload.get('experiment_registry_prior_hash') or 'none'}")
    print(f"  experiment_registry_row_hash={payload.get('experiment_registry_row_hash') or 'none'}")
    print(f"  experiment_registry_completion_row_hash={payload.get('experiment_registry_completion_row_hash') or 'none'}")
    print(f"  experiment_registry_bound_evidence_hash={payload.get('experiment_registry_bound_evidence_hash') or 'none'}")
    print(f"  experiment_registry_evidence_hash_phase={payload.get('experiment_registry_evidence_hash_phase') or 'none'}")
    print(f"  final_holdout_fingerprint={payload.get('final_holdout_fingerprint') or 'none'}")
    print(f"  final_holdout_identity_hash={payload.get('final_holdout_identity_hash') or 'none'}")
    print(f"  final_holdout_content_hash={payload.get('final_holdout_content_hash') or 'none'}")
    print(f"  final_holdout_reuse_key_hash={payload.get('final_holdout_reuse_key_hash') or 'none'}")
    print(f"  final_holdout_split_hash={payload.get('final_holdout_split_hash') or 'none'}")
    print(f"  computed_attempt_index={payload.get('computed_attempt_index')}")
    print(f"  computed_holdout_reuse_count={payload.get('computed_holdout_reuse_count')}")
    print(f"  declared_attempt_index={payload.get('declared_attempt_index')}")
    print(f"  declared_holdout_reuse_count={payload.get('declared_holdout_reuse_count')}")
    print(f"  registry_gate_result={payload.get('registry_gate_result') or 'none'}")
    print(
        "  registry_gate_fail_reasons="
        f"{_format_items(tuple(str(item) for item in payload.get('registry_gate_fail_reasons') or []))}"
    )
    print(f"  research_freedom_hash={payload.get('research_freedom_hash') or 'none'}")


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ",".join(f"{key}:{value}" for key, value in counts.items())


def _format_items(items: tuple[str, ...]) -> str:
    if not items:
        return "none"
    return ",".join(items)


def _format_walk_forward_window_summary(summary: ResearchRunSummary) -> str:
    if summary.walk_forward_window_count is None:
        return "none"
    return (
        f"window_count:{summary.walk_forward_window_count},"
        f"pass:{summary.walk_forward_pass_window_count if summary.walk_forward_pass_window_count is not None else 'unknown'},"
        f"fail:{summary.walk_forward_fail_window_count if summary.walk_forward_fail_window_count is not None else 'unknown'}"
    )


def _print_top_of_book_summary(report: dict[str, object]) -> None:
    summary = report.get("top_of_book_quality_summary")
    if not isinstance(summary, dict) or not bool(summary.get("requested")):
        return
    affected = summary.get("affected_splits")
    affected_names = []
    if isinstance(affected, list):
        affected_names = [
            str(item.get("split_name"))
            for item in affected
            if isinstance(item, dict) and item.get("split_name")
        ]
    print(
        "  top_of_book_quote_coverage="
        f"requested=1 required={1 if summary.get('required') else 0} "
        f"gate_status={summary.get('gate_status')} "
        f"coverage_pct={summary.get('coverage_pct')} "
        f"joined_count={summary.get('joined_quote_count')} "
        f"missing_count={summary.get('missing_quote_count')} "
        f"join_tolerance_ms={summary.get('join_tolerance_ms')} "
        f"affected_splits={','.join(affected_names) if affected_names else 'none'}"
    )
    print(
        "  top_of_book_limitations="
        "best_bid_ask_only_not_full_depth,intra_candle_path_unavailable"
    )
    if summary.get("next_action"):
        print(f"  top_of_book_next_action={summary.get('next_action')}")


def _print_execution_capability_summary(report: dict[str, object]) -> None:
    capability = report.get("execution_capability_contract")
    unavailable: object = report.get("unavailable_required_capabilities")
    market_impact_available: object = report.get("market_impact_model_available")
    top_of_book_is_full_depth: object = report.get("top_of_book_is_full_depth")
    if isinstance(capability, dict):
        available = capability.get("available_capabilities") if isinstance(capability.get("available_capabilities"), dict) else {}
        unavailable = capability.get("unavailable_required_capabilities", unavailable)
        market_impact_available = available.get("market_impact_model", market_impact_available)
        top_of_book_is_full_depth = available.get("top_of_book_is_full_depth", top_of_book_is_full_depth)
    unavailable_items = tuple(str(item) for item in (unavailable or [])) if isinstance(unavailable, list) else ()
    print(f"  execution_capability_contract_hash={report.get('execution_capability_contract_hash') or 'none'}")
    print(f"  evidence_tier={report.get('evidence_tier') or (capability.get('evidence_tier') if isinstance(capability, dict) else 'unknown')}")
    print(f"  unavailable_required_capabilities={_format_items(unavailable_items)}")
    print(f"  market_impact_required={report.get('market_impact_required')}")
    print(f"  market_impact_model_available={market_impact_available}")
    print(f"  top_of_book_is_full_depth={top_of_book_is_full_depth}")
    if unavailable_items:
        print("  execution_capability_next_action=remove unsupported requirements or add implemented evidence/model support")


def _nested(payload: dict[str, object], *keys: str) -> object | None:
    current: object = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
