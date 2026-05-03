from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic

from .hashing import content_hash_payload, sha256_prefixed


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


def build_candidate_profile(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_name": candidate.get("strategy_name"),
        "candidate_id": candidate.get("parameter_candidate_id"),
        "parameter_values": candidate.get("parameter_values"),
        "cost_model": candidate.get("cost_model"),
        "source_experiment": candidate.get("experiment_id"),
        "manifest_hash": candidate.get("manifest_hash"),
        "dataset_snapshot_id": candidate.get("dataset_snapshot_id"),
        "dataset_content_hash": candidate.get("dataset_content_hash"),
    }


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
    profile_hash = candidate.get("candidate_profile_hash")
    if not profile_hash:
        reasons.append("candidate_profile_hash_missing")
    elif sha256_prefixed(build_candidate_profile(candidate)) != profile_hash:
        reasons.append("candidate_profile_hash_mismatch")
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
    return not reasons, reasons


def _validated_backtest_candidate(candidate: dict[str, Any] | None) -> ValidatedCandidate:
    allowed, reasons = validate_backtest_candidate_for_promotion(candidate)
    if not allowed:
        raise PromotionGateError(f"promotion refused: {','.join(reasons)}")
    assert candidate is not None
    profile = build_candidate_profile(candidate)
    return ValidatedCandidate(candidate=candidate, profile=profile, profile_hash=sha256_prefixed(profile))


def promote_candidate(
    *,
    experiment_id: str,
    candidate_id: str,
    manager: PathManager,
    generated_at: str | None = None,
) -> PromotionResult:
    research_report_dir = manager.data_dir() / "reports" / "research" / experiment_id
    candidate_report_path = research_report_dir / "backtest_report.json"
    if not candidate_report_path.exists():
        raise PromotionGateError(f"candidate report not found: {candidate_report_path}")
    import json

    with candidate_report_path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    if report.get("experiment_id") != experiment_id:
        raise PromotionGateError("candidate report experiment_id mismatch")
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

    candidate = backtest.candidate
    profile = backtest.profile
    verified_profile_hash = backtest.profile_hash
    walk_forward_required = bool(candidate.get("walk_forward_required"))
    artifact = {
        "strategy_name": candidate["strategy_name"],
        "strategy_profile_id": f"{experiment_id}_{candidate_id}",
        "strategy_profile_source_experiment": experiment_id,
        "strategy_profile_hash": verified_profile_hash,
        "candidate_id": candidate_id,
        "manifest_hash": candidate["manifest_hash"],
        "dataset_snapshot_id": candidate["dataset_snapshot_id"],
        "dataset_content_hash": candidate["dataset_content_hash"],
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
        "operator_next_step": "Review this artifact before manual paper env/profile consideration.",
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
    }
    content_hash = sha256_prefixed(content_hash_payload(artifact))
    artifact["content_hash"] = content_hash
    path = manager.data_dir() / "reports" / "research" / experiment_id / f"promotion_{candidate_id}.json"
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
        "manifest_hash",
    ):
        if candidate.get(key) != backtest_candidate.get(key):
            raise PromotionGateError("promotion refused: walk_forward_candidate_mismatch")
    if candidate.get("walk_forward_gate_result") != "PASS":
        raise PromotionGateError("promotion refused: walk_forward_gate_not_passed")
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
    return ValidatedCandidate(candidate=candidate, profile=profile, profile_hash=verified_profile_hash)


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"research output path must be outside repository: {resolved}")
