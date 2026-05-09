from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .hashing import content_hash_payload, sha256_prefixed


LINEAGE_SCHEMA_VERSION = 1
LINEAGE_HASH_FIELD = "lineage_hash"
LINEAGE_HASH_EXCLUDED_FIELDS = frozenset({LINEAGE_HASH_FIELD, "created_at"})
SECRET_KEY_FRAGMENTS = ("secret", "api_key", "apikey", "token", "password", "webhook")


class LineageValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ReproducibilityResult:
    summary: dict[str, Any]

    @property
    def ok(self) -> bool:
        return bool(self.summary.get("ok"))


def lineage_hash_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in LINEAGE_HASH_EXCLUDED_FIELDS}


def compute_lineage_hash(payload: dict[str, Any]) -> str:
    return sha256_prefixed(content_hash_payload(lineage_hash_payload(payload)))


def validate_lineage_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise LineageValidationError("lineage_payload_not_object")
    if int(payload.get("lineage_schema_version") or 0) != LINEAGE_SCHEMA_VERSION:
        raise LineageValidationError("lineage_schema_version_mismatch")
    expected = payload.get(LINEAGE_HASH_FIELD)
    if not isinstance(expected, str) or not expected.startswith("sha256:"):
        raise LineageValidationError("lineage_hash_missing")
    actual = compute_lineage_hash(payload)
    if actual != expected:
        raise LineageValidationError("lineage_hash_mismatch")
    return dict(payload)


def normalized_command_args_hash(args: dict[str, Any] | None) -> str | None:
    if args is None:
        return None
    return sha256_prefixed(_redacted_mapping(args))


def safe_environment_fingerprint(values: dict[str, Any] | None) -> str | None:
    if values is None:
        return None
    return sha256_prefixed(_redacted_mapping(values))


def build_research_lineage(
    *,
    experiment_id: str,
    manifest_hash: str,
    manifest_canonical_hash: str | None = None,
    manifest_path: str | None = None,
    dataset_snapshot_id: str | None = None,
    dataset_content_hash: str | None = None,
    dataset_quality_hash: str | None = None,
    dataset_split_hash: str | None = None,
    data_source_fingerprint: str | None = None,
    repository_version: str | None = None,
    command_name: str | None = None,
    command_args: dict[str, Any] | None = None,
    environment: dict[str, Any] | None = None,
    cost_execution_model_hash: str | None = None,
    execution_calibration_artifact_hash: str | None = None,
    search_budget: int | None = None,
    parameter_grid_size: int | None = None,
    attempt_index: int | None = None,
    failed_candidate_count: int | None = None,
    holdout_reuse_count: int | None = None,
    dataset_reuse_policy: str | None = None,
    hypothesis_id: str | None = None,
    hypothesis_status: str | None = None,
    experiment_family_id: str | None = None,
    pre_registered_at: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "lineage_schema_version": LINEAGE_SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "experiment_family_id": experiment_family_id,
        "hypothesis_id": hypothesis_id,
        "hypothesis_status": hypothesis_status,
        "pre_registered_at": pre_registered_at,
        "manifest_path": manifest_path,
        "manifest_hash": manifest_hash,
        "manifest_canonical_hash": manifest_canonical_hash or manifest_hash,
        "dataset_snapshot_id": dataset_snapshot_id,
        "dataset_content_hash": dataset_content_hash,
        "dataset_quality_hash": dataset_quality_hash,
        "dataset_split_hash": dataset_split_hash or dataset_content_hash,
        "data_source_fingerprint": data_source_fingerprint,
        "repository_version": repository_version,
        "command_name": command_name,
        "normalized_command_args": _redacted_mapping(command_args or {}),
        "command_args_hash": normalized_command_args_hash(command_args or {}),
        "environment_config_fingerprint": safe_environment_fingerprint(environment or {}),
        "cost_execution_model_hash": cost_execution_model_hash,
        "execution_calibration_artifact_hash": execution_calibration_artifact_hash,
        "search_budget": search_budget,
        "parameter_grid_size": parameter_grid_size,
        "attempt_index": attempt_index,
        "failed_candidate_count": failed_candidate_count,
        "holdout_reuse_count": holdout_reuse_count,
        "dataset_reuse_policy": dataset_reuse_policy,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
    }
    payload[LINEAGE_HASH_FIELD] = compute_lineage_hash(payload)
    return payload


def build_promotion_lineage(
    *,
    base_lineage: dict[str, Any],
    backtest_report_path: str,
    backtest_report_hash: str,
    walk_forward_report_path: str | None,
    walk_forward_report_hash: str | None,
    candidate_id: str,
    candidate_profile_hash: str,
    promotion_artifact_path: str | None = None,
    promotion_artifact_hash: str | None = None,
    approved_profile_path: str | None = None,
    approved_profile_hash: str | None = None,
    paper_validation_evidence_path: str | None = None,
    paper_validation_evidence_hash: str | None = None,
    live_readiness_evidence_path: str | None = None,
    live_readiness_evidence_hash: str | None = None,
    decision_equivalence_report_path: str | None = None,
    decision_equivalence_report_hash: str | None = None,
    execution_calibration_artifact_hash: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    lineage = validate_lineage_artifact(base_lineage)
    base_calibration_hash = _normalized_sha256(lineage.get("execution_calibration_artifact_hash"))
    candidate_calibration_hash = _normalized_sha256(execution_calibration_artifact_hash)
    if (
        base_calibration_hash is not None
        and candidate_calibration_hash is not None
        and base_calibration_hash != candidate_calibration_hash
    ):
        raise LineageValidationError("lineage_execution_calibration_artifact_hash_mismatch")
    lineage.update(
        {
            "backtest_report_path": backtest_report_path,
            "backtest_report_hash": backtest_report_hash,
            "walk_forward_report_path": walk_forward_report_path,
            "walk_forward_report_hash": walk_forward_report_hash,
            "candidate_id": candidate_id,
            "candidate_profile_hash": candidate_profile_hash,
            "promotion_artifact_path": promotion_artifact_path,
            "promotion_artifact_hash": promotion_artifact_hash,
            "approved_profile_path": approved_profile_path,
            "approved_profile_hash": approved_profile_hash,
            "paper_validation_evidence_path": paper_validation_evidence_path,
            "paper_validation_evidence_hash": paper_validation_evidence_hash,
            "live_readiness_evidence_path": live_readiness_evidence_path,
            "live_readiness_evidence_hash": live_readiness_evidence_hash,
            "decision_equivalence_report_path": decision_equivalence_report_path,
            "decision_equivalence_report_hash": decision_equivalence_report_hash,
            "execution_calibration_artifact_hash": candidate_calibration_hash or base_calibration_hash,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        }
    )
    lineage.pop(LINEAGE_HASH_FIELD, None)
    lineage[LINEAGE_HASH_FIELD] = compute_lineage_hash(lineage)
    return lineage


def _normalized_sha256(value: object) -> str | None:
    text = str(value or "").strip()
    if text.startswith("sha256:"):
        return text
    return None


def reproduce_promotion(promotion_path: str | Path) -> ReproducibilityResult:
    path = Path(promotion_path).expanduser()
    summary: dict[str, Any] = {
        "ok": False,
        "reason": "unknown",
        "promotion_path": str(path),
        "promotion_content_hash": None,
        "lineage_hash": None,
        "manifest_hash": None,
        "dataset_content_hash": None,
        "dataset_quality_hash": None,
        "backtest_report_hash": None,
        "walk_forward_report_hash": None,
        "candidate_profile_hash": None,
        "execution_calibration_artifact_hash": None,
        "mismatches": [],
        "missing_artifacts": [],
        "legacy_compatibility_used": False,
    }
    if not path.exists():
        summary["reason"] = "promotion_path_missing"
        summary["missing_artifacts"].append({"field": "promotion_path", "path": str(path)})
        return ReproducibilityResult(summary)
    try:
        promotion = _load_object(path)
    except ValueError as exc:
        summary["reason"] = str(exc)
        return ReproducibilityResult(summary)

    expected_promotion_hash = str(promotion.get("content_hash") or "")
    actual_promotion_hash = sha256_prefixed(content_hash_payload({k: v for k, v in promotion.items() if k != "content_hash"}))
    summary["promotion_content_hash"] = expected_promotion_hash or None
    if actual_promotion_hash != expected_promotion_hash:
        summary["reason"] = "promotion_hash_mismatch"
        summary["mismatches"].append(_mismatch("promotion_content_hash", expected_promotion_hash, actual_promotion_hash))
        return ReproducibilityResult(summary)

    lineage = promotion.get("lineage")
    if not isinstance(lineage, dict):
        summary["reason"] = "lineage_missing"
        summary["legacy_compatibility_used"] = bool(promotion.get("legacy_compatibility_used"))
        return ReproducibilityResult(summary)
    try:
        lineage = validate_lineage_artifact(lineage)
    except LineageValidationError as exc:
        summary["reason"] = str(exc)
        return ReproducibilityResult(summary)

    summary["lineage_hash"] = lineage.get("lineage_hash")
    summary["manifest_hash"] = lineage.get("manifest_hash")
    summary["dataset_content_hash"] = lineage.get("dataset_content_hash")
    summary["dataset_quality_hash"] = lineage.get("dataset_quality_hash")
    summary["backtest_report_hash"] = lineage.get("backtest_report_hash")
    summary["walk_forward_report_hash"] = lineage.get("walk_forward_report_hash")
    summary["candidate_profile_hash"] = lineage.get("candidate_profile_hash")
    summary["execution_calibration_artifact_hash"] = lineage.get("execution_calibration_artifact_hash")

    _compare(summary, "manifest_hash", promotion.get("manifest_hash"), lineage.get("manifest_hash"), "manifest_hash_mismatch")
    _compare(
        summary,
        "dataset_content_hash",
        promotion.get("dataset_content_hash"),
        lineage.get("dataset_content_hash"),
        "dataset_content_hash_mismatch",
    )
    if promotion.get("dataset_quality_hash") or lineage.get("dataset_quality_hash"):
        _compare(
            summary,
            "dataset_quality_hash",
            promotion.get("dataset_quality_hash"),
            lineage.get("dataset_quality_hash"),
            "dataset_quality_hash_mismatch",
        )
    _compare(
        summary,
        "candidate_profile_hash",
        promotion.get("candidate_profile_hash"),
        lineage.get("candidate_profile_hash"),
        "candidate_hash_mismatch",
    )

    _verify_artifact_hash(summary, lineage, "backtest_report", required=True)
    walk_required = bool(promotion.get("walk_forward_required"))
    _verify_artifact_hash(
        summary,
        lineage,
        "walk_forward_report",
        required=walk_required,
        missing_reason="walk_forward_required_but_missing",
    )
    calibration_required = bool(promotion.get("execution_calibration_required"))
    promotion_calibration_hash = str(promotion.get("execution_calibration_artifact_hash") or "").strip()
    lineage_calibration_hash = str(lineage.get("execution_calibration_artifact_hash") or "").strip()
    if calibration_required and not promotion_calibration_hash:
        summary["mismatches"].append(
            _mismatch(
                "execution_calibration_artifact_hash",
                "sha256:<required>",
                promotion_calibration_hash or None,
                "calibration_hash_missing",
            )
        )
    if calibration_required and not lineage_calibration_hash:
        summary["mismatches"].append(
            _mismatch(
                "lineage.execution_calibration_artifact_hash",
                promotion_calibration_hash or "sha256:<required>",
                lineage_calibration_hash or None,
                "calibration_hash_missing",
            )
        )
    if promotion_calibration_hash or lineage_calibration_hash:
        _compare(
            summary,
            "execution_calibration_artifact_hash",
            promotion_calibration_hash,
            lineage_calibration_hash,
            "calibration_hash_mismatch",
        )
    if promotion.get("command_args_hash_expected") and promotion.get("command_args_hash_expected") != lineage.get("command_args_hash"):
        _compare(
            summary,
            "command_args_hash",
            promotion.get("command_args_hash_expected"),
            lineage.get("command_args_hash"),
            "command_args_hash_mismatch",
        )

    if summary["mismatches"]:
        summary["reason"] = str(summary["mismatches"][0]["reason"])
    elif summary["missing_artifacts"]:
        summary["reason"] = str(summary["missing_artifacts"][0]["reason"])
    else:
        summary["ok"] = True
        summary["reason"] = "ok"
    return ReproducibilityResult(summary)


def _verify_artifact_hash(
    summary: dict[str, Any],
    lineage: dict[str, Any],
    stem: str,
    *,
    required: bool,
    missing_reason: str | None = None,
) -> None:
    path_value = str(lineage.get(f"{stem}_path") or "").strip()
    expected = str(lineage.get(f"{stem}_hash") or "").strip()
    if not path_value or not expected:
        if required:
            summary["missing_artifacts"].append(
                {"field": stem, "path": path_value or None, "reason": missing_reason or f"{stem}_missing"}
            )
        return
    path = Path(path_value).expanduser()
    if not path.exists():
        summary["missing_artifacts"].append({"field": stem, "path": str(path), "reason": missing_reason or f"{stem}_missing"})
        return
    try:
        payload = _load_object(path)
    except ValueError as exc:
        summary["mismatches"].append({"field": stem, "reason": str(exc), "path": str(path)})
        return
    actual = sha256_prefixed(content_hash_payload({k: v for k, v in payload.items() if k != "content_hash"}))
    embedded = str(payload.get("content_hash") or "").strip()
    if actual != expected:
        reason = f"{stem}_hash_mismatch"
        summary["mismatches"].append(_mismatch(f"{stem}_hash", expected, actual, reason))
    elif embedded != actual:
        summary["mismatches"].append(
            _mismatch(
                f"{stem}_embedded_content_hash",
                actual,
                embedded or None,
                f"{stem}_embedded_content_hash_mismatch",
            )
        )


def _compare(summary: dict[str, Any], field: str, expected: object, actual: object, reason: str) -> None:
    if str(expected or "").strip() != str(actual or "").strip():
        summary["mismatches"].append(_mismatch(field, expected, actual, reason))


def _mismatch(field: str, expected: object, actual: object, reason: str) -> dict[str, object]:
    return {"field": field, "expected": expected, "actual": actual, "reason": reason}


def _load_object(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid_json: {exc}") from exc
    except OSError as exc:
        raise ValueError(f"unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("payload_not_object")
    return payload


def _redacted_mapping(values: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in sorted(values.items()):
        lowered = str(key).lower()
        if any(fragment in lowered for fragment in SECRET_KEY_FRAGMENTS):
            out[str(key)] = "<redacted-present>" if str(value or "") else "<redacted-empty>"
        else:
            out[str(key)] = value
    return out
