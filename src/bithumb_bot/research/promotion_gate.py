from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic

from .hashing import content_hash_payload, sha256_prefixed
from .lineage import build_promotion_lineage, validate_lineage_artifact, LineageValidationError


class PromotionGateError(ValueError):
    pass


@dataclass(frozen=True)
class PromotionResult:
    artifact: dict[str, Any]
    artifact_path: Path
    content_hash: str


@dataclass(frozen=True)
class ValidatedCandidate:
    candidate: dict[str, Any]
    profile: dict[str, Any]
    profile_hash: str
    source_report_hash: str | None = None


def build_candidate_profile(candidate: dict[str, Any]) -> dict[str, Any]:
    warning_reasons = _execution_calibration_warning_reasons(candidate)
    profile = {
        "strategy_name": candidate.get("strategy_name"),
        "candidate_id": candidate.get("parameter_candidate_id"),
        "parameter_values": candidate.get("parameter_values"),
        "cost_model": candidate.get("cost_model"),
        "source_experiment": candidate.get("experiment_id"),
        "manifest_hash": candidate.get("manifest_hash"),
        "dataset_snapshot_id": candidate.get("dataset_snapshot_id"),
        "dataset_content_hash": candidate.get("dataset_content_hash"),
        "dataset_quality_hash": candidate.get("dataset_quality_hash"),
        "dataset_quality_gate_status": candidate.get("dataset_quality_gate_status"),
        "dataset_quality_gate_reasons": candidate.get("dataset_quality_gate_reasons"),
        "dataset_quality_report_hashes": candidate.get("dataset_quality_report_hashes"),
        "top_of_book_quality_summary": candidate.get("top_of_book_quality_summary"),
        "execution_timing_policy": candidate.get("execution_timing_policy"),
        "execution_reality_summary": candidate.get("execution_reality_summary"),
        "execution_event_summary": candidate.get("execution_event_summary"),
        "train_execution_event_summary": candidate.get("train_execution_event_summary"),
        "validation_execution_event_summary": candidate.get("validation_execution_event_summary"),
        "final_holdout_execution_event_summary": candidate.get("final_holdout_execution_event_summary"),
        "regime_classifier_version": candidate.get("regime_classifier_version"),
        "allowed_live_regimes": candidate.get("allowed_live_regimes"),
        "blocked_live_regimes": candidate.get("blocked_live_regimes"),
        "acceptance_gate_result": candidate.get("acceptance_gate_result"),
        "scenario_policy": candidate.get("scenario_policy"),
        "scenario_results": candidate.get("scenario_results"),
        "scenario_pass_count": candidate.get("scenario_pass_count"),
        "scenario_fail_count": candidate.get("scenario_fail_count"),
        "required_scenario_count": candidate.get("required_scenario_count"),
        "has_execution_calibration_warning": bool(warning_reasons),
        "execution_calibration_warning_reasons": warning_reasons,
        "final_holdout_present": candidate.get("final_holdout_present"),
        "final_holdout_required_for_promotion": candidate.get("final_holdout_required_for_promotion"),
        "final_holdout_metrics": candidate.get("final_holdout_metrics"),
        "validation_metrics": candidate.get("validation_metrics"),
        "walk_forward_metrics": candidate.get("walk_forward_metrics"),
    }
    if candidate.get("execution_model") is not None:
        profile["execution_model"] = candidate.get("execution_model")
    if candidate.get("execution_calibration_required") is not None:
        profile["execution_calibration_required"] = candidate.get("execution_calibration_required")
    if candidate.get("execution_calibration_strictness") is not None:
        profile["execution_calibration_strictness"] = candidate.get("execution_calibration_strictness")
    if candidate.get("execution_calibration_gate") is not None:
        profile["execution_calibration_gate"] = candidate.get("execution_calibration_gate")
    return profile


def evaluate_candidate_for_promotion(candidate: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not candidate:
        return False, ["candidate_not_found"]
    gate = candidate.get("acceptance_gate_result")
    if gate != "PASS":
        reasons.append("acceptance_gate_not_passed")
    validation_metrics = candidate.get("validation_metrics")
    if not isinstance(validation_metrics, dict):
        reasons.append("validation_oos_evidence_missing")
    elif validation_metrics.get("trade_count") is None:
        reasons.append("validation_trade_count_missing")
    if candidate.get("walk_forward_required") and candidate.get("walk_forward_gate_result") != "PASS":
        reasons.append("walk_forward_gate_not_passed")
    _extend_final_holdout_reasons(candidate, reasons)
    _extend_dataset_quality_reasons(candidate, reasons)
    _extend_scenario_policy_reasons(candidate, reasons)
    _extend_execution_reality_reasons(candidate, reasons)
    _extend_execution_event_reasons(candidate, reasons)
    _extend_execution_calibration_reasons(candidate, reasons)
    profile_hash = candidate.get("candidate_profile_hash")
    if not profile_hash:
        reasons.append("candidate_profile_hash_missing")
    elif sha256_prefixed(build_candidate_profile(candidate)) != profile_hash:
        reasons.append("candidate_profile_hash_mismatch")
    if not _candidate_has_regime_policy(candidate):
        reasons.append("regime_policy_missing")
    return not reasons, reasons


def validate_backtest_candidate_for_promotion(candidate: dict[str, Any] | None) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not candidate:
        return False, ["backtest_candidate_not_found", "candidate_not_found"]
    gate = candidate.get("acceptance_gate_result")
    if gate != "PASS":
        reasons.extend(["backtest_acceptance_gate_not_passed", "acceptance_gate_not_passed"])
    validation_metrics = candidate.get("validation_metrics")
    if not isinstance(validation_metrics, dict):
        reasons.extend(["backtest_validation_oos_evidence_missing", "validation_oos_evidence_missing"])
    elif validation_metrics.get("trade_count") is None:
        reasons.extend(["backtest_validation_trade_count_missing", "validation_trade_count_missing"])
    profile_hash = candidate.get("candidate_profile_hash")
    if not profile_hash:
        reasons.extend(["backtest_candidate_profile_hash_missing", "candidate_profile_hash_missing"])
    elif sha256_prefixed(build_candidate_profile(candidate)) != profile_hash:
        reasons.extend(["backtest_candidate_profile_hash_mismatch", "candidate_profile_hash_mismatch"])
    if not _candidate_has_regime_policy(candidate):
        reasons.extend(["backtest_regime_policy_missing", "regime_policy_missing"])
    _extend_final_holdout_reasons(candidate, reasons, prefix="backtest_")
    _extend_dataset_quality_reasons(candidate, reasons, prefix="backtest_")
    _extend_scenario_policy_reasons(candidate, reasons, prefix="backtest_")
    _extend_execution_reality_reasons(candidate, reasons, prefix="backtest_")
    _extend_execution_event_reasons(candidate, reasons, prefix="backtest_")
    _extend_execution_calibration_reasons(candidate, reasons, prefix="backtest_")
    return not reasons, reasons


def _extend_execution_calibration_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    gate = candidate.get("execution_calibration_gate")
    if candidate.get("execution_calibration_required"):
        if not isinstance(gate, dict):
            reasons.extend([f"{prefix}execution_calibration_missing", "execution_calibration_missing"])
            return
        if gate.get("status") != "PASS":
            gate_reasons = [str(item) for item in gate.get("reasons") or ["execution_calibration_failed"]]
            reasons.extend([f"{prefix}{reason}" for reason in gate_reasons])
            reasons.extend(gate_reasons)
    elif (
        candidate.get("execution_calibration_strictness") != "warn"
        and isinstance(gate, dict)
        and gate.get("status") == "FAIL"
    ):
        gate_reasons = [str(item) for item in gate.get("reasons") or ["execution_calibration_failed"]]
        reasons.extend([f"{prefix}{reason}" for reason in gate_reasons])
        reasons.extend(gate_reasons)


def _extend_execution_reality_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    policy = candidate.get("execution_timing_policy")
    summary = candidate.get("execution_reality_summary")
    if not isinstance(policy, dict):
        reasons.extend([f"{prefix}execution_timing_policy_missing", "execution_timing_policy_missing"])
        return
    if not isinstance(summary, dict):
        reasons.extend([f"{prefix}execution_reality_summary_missing", "execution_reality_summary_missing"])
        return
    gate_reasons = [str(item) for item in summary.get("execution_reality_gate_reasons") or []]
    if summary.get("execution_reality_gate_status") == "FAIL" and gate_reasons:
        reasons.extend([f"{prefix}{reason}" for reason in gate_reasons])
        reasons.extend(gate_reasons)
    fill_policy = str(policy.get("fill_reference_policy") or "")
    if fill_policy == "candle_close_legacy":
        reasons.extend([
            f"{prefix}execution_reference_price_candle_close_not_promotable",
            "execution_reference_price_candle_close_not_promotable",
        ])
    if summary.get("execution_reality_level") == "candle_close_optimistic":
        reasons.extend([
            f"{prefix}execution_reality_level_below_required",
            "execution_reality_level_below_required",
        ])


def _extend_execution_event_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    summary = candidate.get("execution_event_summary")
    if not isinstance(summary, dict):
        summary = candidate.get("validation_execution_event_summary")
    if not isinstance(summary, dict):
        reasons.extend([f"{prefix}execution_event_summary_missing", "execution_event_summary_missing"])
        return
    if bool(summary.get("execution_event_timeline_incomplete")):
        reasons.extend([f"{prefix}execution_event_timeline_incomplete", "execution_event_timeline_incomplete"])
    if int(summary.get("pending_execution_after_dataset_end_count") or 0) > 0:
        reasons.extend([f"{prefix}pending_execution_after_dataset_end", "pending_execution_after_dataset_end"])
    execution_filled = int(summary.get("execution_filled_count") or summary.get("filled_execution_count") or 0)
    portfolio_applied = int(summary.get("portfolio_applied_trade_count") or 0)
    closed_trade_count = int(summary.get("closed_trade_count") or 0)
    validation_metrics = candidate.get("validation_metrics")
    validation_trade_count = (
        int(validation_metrics.get("trade_count") or 0)
        if isinstance(validation_metrics, dict)
        else None
    )
    if execution_filled > 0 and portfolio_applied <= 0:
        reasons.extend([f"{prefix}portfolio_applied_trade_count_insufficient", "portfolio_applied_trade_count_insufficient"])
    if validation_trade_count is not None and closed_trade_count != validation_trade_count:
        reasons.extend([f"{prefix}execution_event_closed_trade_count_mismatch", "execution_event_closed_trade_count_mismatch"])


def _extend_final_holdout_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    if candidate.get("final_holdout_required_for_promotion") is False:
        return
    metrics = candidate.get("final_holdout_metrics")
    if candidate.get("final_holdout_present") is not True or not isinstance(metrics, dict):
        reasons.extend([f"{prefix}final_holdout_evidence_missing", "final_holdout_evidence_missing"])
    elif metrics.get("trade_count") is None:
        reasons.extend([f"{prefix}final_holdout_evidence_missing", "final_holdout_evidence_missing"])


def _extend_dataset_quality_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    status = candidate.get("dataset_quality_gate_status")
    if status is None:
        return
    if status != "PASS":
        quality_reasons = [str(item) for item in candidate.get("dataset_quality_gate_reasons") or ["dataset_quality_failed"]]
        reasons.extend([f"{prefix}{reason}" for reason in quality_reasons])
        reasons.extend(quality_reasons)


def _extend_scenario_policy_reasons(
    candidate: dict[str, Any],
    reasons: list[str],
    *,
    prefix: str = "",
) -> None:
    scenario_results = candidate.get("scenario_results")
    if not isinstance(scenario_results, list) or not scenario_results:
        reasons.extend([f"{prefix}scenario_result_missing", "scenario_result_missing"])
        return
    if candidate.get("acceptance_gate_result") != "PASS":
        for reason in candidate.get("gate_fail_reasons") or ["scenario_policy_required_scenario_failed"]:
            reason_text = str(reason)
            if reason_text.startswith("scenario_policy_") or reason_text == "scenario_result_missing":
                reasons.extend([f"{prefix}{reason_text}", reason_text])
    for result in scenario_results:
        if result.get("scenario_acceptance_gate_result") != "PASS":
            reason_text = f"scenario_policy_required_scenario_failed:{result.get('scenario_id')}"
            reasons.extend([f"{prefix}{reason_text}", reason_text])


def _candidate_has_regime_policy(candidate: dict[str, Any]) -> bool:
    return (
        isinstance(candidate.get("regime_classifier_version"), str)
        and isinstance(candidate.get("allowed_live_regimes"), list)
        and isinstance(candidate.get("blocked_live_regimes"), list)
        and isinstance(candidate.get("regime_evidence"), dict)
        and isinstance(candidate.get("regime_gate_result"), dict)
    )


def _validated_backtest_candidate(candidate: dict[str, Any] | None) -> ValidatedCandidate:
    allowed, reasons = validate_backtest_candidate_for_promotion(candidate)
    if not allowed:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    assert candidate is not None
    profile = build_candidate_profile(candidate)
    return ValidatedCandidate(candidate=candidate, profile=profile, profile_hash=sha256_prefixed(profile))


def _verify_report_content_hash(report: dict[str, Any], *, label: str) -> str:
    expected = str(report.get("content_hash") or "").strip()
    if not expected.startswith("sha256:"):
        raise PromotionGateError(f"promotion refused: {label}_content_hash_missing")
    actual = sha256_prefixed(
        content_hash_payload({key: value for key, value in report.items() if key != "content_hash"})
    )
    if actual != expected:
        raise PromotionGateError(f"promotion refused: {label}_hash_mismatch")
    return actual


def promote_candidate(
    *,
    experiment_id: str,
    candidate_id: str,
    manager: PathManager,
    generated_at: str | None = None,
    allow_legacy_lineage: bool = False,
) -> PromotionResult:
    research_report_dir = manager.data_dir() / "reports" / "research" / experiment_id
    candidate_report_path = research_report_dir / "backtest_report.json"
    if not candidate_report_path.exists():
        raise PromotionGateError(f"candidate report not found: {candidate_report_path}")
    import json

    with candidate_report_path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    backtest_report_hash = _verify_report_content_hash(report, label="backtest_report")
    if report.get("experiment_id") != experiment_id:
        raise PromotionGateError("candidate report experiment_id mismatch")
    dataset_quality_legacy_bypass_used = _verify_report_dataset_quality(
        report,
        allow_legacy_lineage=allow_legacy_lineage,
    )
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise PromotionGateError("candidate report does not contain candidates")
    candidate = next(
        (item for item in candidates if item.get("parameter_candidate_id") == candidate_id),
        None,
    )
    backtest = _validated_backtest_candidate(candidate)
    walk_forward: ValidatedCandidate | None = None
    if backtest.candidate.get("walk_forward_required"):
        walk_forward = validate_walk_forward_candidate_for_promotion(
            report_dir=research_report_dir,
            experiment_id=experiment_id,
            candidate_id=candidate_id,
            backtest_candidate=backtest.candidate,
        )
    base_lineage = report.get("lineage") if isinstance(report.get("lineage"), dict) else None
    lineage: dict[str, Any] | None = None
    if base_lineage is not None:
        try:
            validate_lineage_artifact(base_lineage)
        except LineageValidationError as exc:
            raise PromotionGateError(f"promotion refused: {exc}") from exc
    elif not allow_legacy_lineage:
        raise PromotionGateError("promotion refused: lineage_missing")

    candidate = backtest.candidate
    profile = backtest.profile
    verified_profile_hash = backtest.profile_hash
    walk_forward_required = bool(candidate.get("walk_forward_required"))
    calibration_warning_reasons = _execution_calibration_warning_reasons(candidate)
    promotion_warnings = sorted(
        set(str(item) for item in candidate.get("promotion_warnings") or [])
        | set(calibration_warning_reasons)
    )
    if base_lineage is None:
        promotion_warnings = sorted(
            set(promotion_warnings)
            | {"legacy_lineage_compatibility_used"}
            | ({"legacy_dataset_quality_bypass_used"} if dataset_quality_legacy_bypass_used else set())
        )
    artifact = {
        "promotion_schema_version": 1,
        "strategy_name": candidate["strategy_name"],
        "strategy_profile_id": f"{experiment_id}_{candidate_id}",
        "strategy_profile_source_experiment": experiment_id,
        "strategy_profile_hash": verified_profile_hash,
        "candidate_id": candidate_id,
        "manifest_hash": candidate["manifest_hash"],
        "dataset_snapshot_id": candidate["dataset_snapshot_id"],
        "dataset_content_hash": candidate["dataset_content_hash"],
        "dataset_quality_hash": candidate.get("dataset_quality_hash"),
        "dataset_quality_gate_status": candidate.get("dataset_quality_gate_status"),
        "dataset_quality_gate_reasons": candidate.get("dataset_quality_gate_reasons"),
        "market": report.get("market"),
        "interval": report.get("interval"),
        "repository_version": candidate.get("repository_version") or report.get("repository_version"),
        "lineage_required": base_lineage is not None,
        "legacy_compatibility_used": base_lineage is None,
        "dataset_quality_legacy_bypass_used": dataset_quality_legacy_bypass_used,
        "lineage_hash": None,
        "backtest_report_path": str(candidate_report_path.resolve()),
        "backtest_report_hash": backtest_report_hash,
        "walk_forward_report_path": str((research_report_dir / "walk_forward_report.json").resolve()) if walk_forward_required else None,
        "walk_forward_report_hash": None,
        "execution_calibration_artifact_hash": _candidate_calibration_hash(candidate),
        "experiment_family_id": report.get("experiment_family_id"),
        "hypothesis_id": report.get("hypothesis_id"),
        "hypothesis_status": report.get("hypothesis_status"),
        "pre_registered_gate": report.get("pre_registered_gate"),
        "search_budget": report.get("search_budget"),
        "parameter_space_hash": report.get("parameter_space_hash"),
        "parameter_grid_size": report.get("parameter_grid_size"),
        "attempt_index": report.get("attempt_index"),
        "failed_candidate_count": report.get("failed_candidate_count"),
        "holdout_reuse_count": report.get("holdout_reuse_count"),
        "dataset_reuse_policy": report.get("dataset_reuse_policy"),
        "candidate_profile": profile,
        "candidate_profile_hash": verified_profile_hash,
        "verified_candidate_profile_hash": verified_profile_hash,
        "gate_result": "PASS",
        "validation_evidence_source": "backtest_report.json",
        "backtest_candidate_profile_hash": backtest.profile_hash,
        "backtest_candidate_profile_verified": True,
        "walk_forward_required": walk_forward_required,
        "walk_forward_evidence_source": "walk_forward_report.json" if walk_forward_required else None,
        "walk_forward_candidate_profile_hash": walk_forward.profile_hash if walk_forward else None,
        "walk_forward_candidate_profile_verified": bool(walk_forward),
        "final_holdout_required_for_promotion": candidate.get("final_holdout_required_for_promotion") is not False,
        "final_holdout_present": candidate.get("final_holdout_present") is True,
        "final_holdout_metrics": candidate.get("final_holdout_metrics"),
        "scenario_policy": candidate.get("scenario_policy"),
        "execution_timing_policy": candidate.get("execution_timing_policy"),
        "execution_reality_summary": candidate.get("execution_reality_summary"),
        "scenario_pass_count": candidate.get("scenario_pass_count"),
        "scenario_fail_count": candidate.get("scenario_fail_count"),
        "required_scenario_count": candidate.get("required_scenario_count"),
        "has_execution_calibration_warning": bool(calibration_warning_reasons),
        "execution_calibration_warning_reasons": calibration_warning_reasons,
        "promotion_warnings": promotion_warnings,
        "regime_classifier_version": candidate["regime_classifier_version"],
        "allowed_regimes": list(candidate["allowed_live_regimes"]),
        "blocked_regimes": list(candidate["blocked_live_regimes"]),
        "regime_evidence": dict(candidate["regime_evidence"]),
        "regime_gate_result": dict(candidate["regime_gate_result"]),
        "live_regime_policy": {
            "regime_classifier_version": candidate["regime_classifier_version"],
            "allowed_regimes": list(candidate["allowed_live_regimes"]),
            "blocked_regimes": list(candidate["blocked_live_regimes"]),
            "evidence_source": "backtest_report.json",
            "missing_policy_behavior": "fail_closed",
        },
        "operator_next_step": "Review this artifact before manual paper env/profile consideration.",
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
    }
    if walk_forward_required:
        artifact["walk_forward_report_hash"] = walk_forward.source_report_hash if walk_forward else None
    path = manager.data_dir() / "reports" / "research" / experiment_id / f"promotion_{candidate_id}.json"
    if base_lineage is not None:
        lineage = build_promotion_lineage(
            base_lineage=base_lineage,
            backtest_report_path=str(candidate_report_path.resolve()),
            backtest_report_hash=backtest_report_hash,
            walk_forward_report_path=artifact["walk_forward_report_path"],
            walk_forward_report_hash=artifact["walk_forward_report_hash"],
            candidate_id=candidate_id,
            candidate_profile_hash=verified_profile_hash,
            promotion_artifact_path=str(path.resolve()),
            created_at=artifact["generated_at"],
        )
        artifact["lineage"] = lineage
        artifact["lineage_hash"] = lineage["lineage_hash"]
    content_hash = sha256_prefixed(content_hash_payload(artifact))
    artifact["content_hash"] = content_hash
    content_hash = str(artifact["content_hash"])
    _ensure_research_output_path_allowed(manager, path)
    write_json_atomic(path, artifact)
    return PromotionResult(artifact=artifact, artifact_path=path, content_hash=content_hash)


def _walk_forward_candidate_for_promotion(
    *,
    report_dir: Path,
    experiment_id: str,
    candidate_id: str,
    backtest_candidate: dict[str, Any],
) -> dict[str, Any]:
    return validate_walk_forward_candidate_for_promotion(
        report_dir=report_dir,
        experiment_id=experiment_id,
        candidate_id=candidate_id,
        backtest_candidate=backtest_candidate,
    ).candidate


def validate_walk_forward_candidate_for_promotion(
    *,
    report_dir: Path,
    experiment_id: str,
    candidate_id: str,
    backtest_candidate: dict[str, Any],
) -> ValidatedCandidate:
    path = report_dir / "walk_forward_report.json"
    if not path.exists():
        raise PromotionGateError("promotion refused: walk_forward_missing")
    import json

    with path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    report_hash = _verify_report_content_hash(report, label="walk_forward_report")
    if report.get("experiment_id") != experiment_id:
        raise PromotionGateError("promotion refused: walk_forward_report_experiment_id_mismatch")
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise PromotionGateError("promotion refused: walk_forward_report_candidates_missing")
    candidate = next((item for item in candidates if item.get("parameter_candidate_id") == candidate_id), None)
    if not candidate:
        raise PromotionGateError("promotion refused: walk_forward_candidate_mismatch")
    for key in (
        "experiment_id",
        "strategy_name",
        "parameter_candidate_id",
        "parameter_values",
        "cost_model",
        "execution_model",
        "execution_calibration_required",
        "execution_calibration_gate",
        "manifest_hash",
    ):
        if candidate.get(key) != backtest_candidate.get(key):
            raise PromotionGateError("promotion refused: walk_forward_candidate_mismatch")
    if candidate.get("walk_forward_gate_result") != "PASS":
        raise PromotionGateError("promotion refused: walk_forward_gate_not_passed")
    _extend_final_holdout_reasons(candidate, reasons := [], prefix="walk_forward_")
    if reasons:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    _extend_scenario_policy_reasons(candidate, reasons := [], prefix="walk_forward_")
    if reasons:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    walk_forward_metrics = candidate.get("walk_forward_metrics")
    if not isinstance(walk_forward_metrics, dict):
        raise PromotionGateError("promotion refused: walk_forward_metrics_missing")
    profile_hash = candidate.get("candidate_profile_hash")
    if not profile_hash:
        raise PromotionGateError("promotion refused: walk_forward_candidate_profile_hash_missing")
    profile = build_candidate_profile(candidate)
    verified_profile_hash = sha256_prefixed(profile)
    if verified_profile_hash != profile_hash:
        raise PromotionGateError("promotion refused: walk_forward_candidate_profile_hash_mismatch")
    return ValidatedCandidate(
        candidate=candidate,
        profile=profile,
        profile_hash=verified_profile_hash,
        source_report_hash=report_hash,
    )


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"research output path must be outside repository: {resolved}")


def _verify_report_dataset_quality(report: dict[str, Any], *, allow_legacy_lineage: bool = False) -> bool:
    lineage_present = isinstance(report.get("lineage"), dict)
    if not lineage_present:
        status = report.get("dataset_quality_gate_status")
        if status not in {None, "PASS"}:
            reasons = [str(item) for item in report.get("dataset_quality_gate_reasons") or ["dataset_quality_failed"]]
            raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
        return bool(allow_legacy_lineage)
    if report.get("dataset_quality_gate_status") is None:
        raise PromotionGateError("promotion refused: dataset_quality_missing")
    if report.get("dataset_quality_gate_status") != "PASS":
        reasons = [str(item) for item in report.get("dataset_quality_gate_reasons") or ["dataset_quality_failed"]]
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    quality_hash = str(report.get("dataset_quality_hash") or "")
    if not quality_hash.startswith("sha256:"):
        raise PromotionGateError("promotion refused: dataset_quality_hash_missing")
    quality_reports = report.get("dataset_quality_reports")
    if not isinstance(quality_reports, dict) or not quality_reports:
        raise PromotionGateError("promotion refused: dataset_quality_report_missing")
    return False


def _execution_calibration_warning_reasons(candidate: dict[str, Any]) -> list[str]:
    if candidate.get("execution_calibration_required"):
        return []
    if candidate.get("execution_calibration_strictness") != "warn":
        return []
    gate = candidate.get("execution_calibration_gate")
    if not isinstance(gate, dict) or gate.get("status") == "PASS":
        return []
    return [str(reason) for reason in gate.get("reasons") or ["execution_calibration_failed"]]


def _candidate_calibration_hash(candidate: dict[str, Any]) -> str | None:
    gate = candidate.get("execution_calibration_gate")
    if isinstance(gate, dict) and isinstance(gate.get("artifact_hash"), str):
        return str(gate["artifact_hash"])
    return None
