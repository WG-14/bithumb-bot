from __future__ import annotations

import json
import os
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import PathManager, PathPolicyError
from .evidence_chain import validate_profile_transition_evidence
from .decision_equivalence import compute_decision_equivalence_hash
from .research.hashing import content_hash_payload, sha256_prefixed
from .research.lineage import validate_lineage_artifact, LineageValidationError
from .research.promotion_gate import build_candidate_profile
from .storage_io import write_json_atomic


APPROVED_PROFILE_SCHEMA_VERSION = 1
APPROVED_PROFILE_MODES = {"paper", "live_dry_run", "small_live"}
LIVE_COMPATIBLE_PROFILE_MODES = {"live_dry_run", "small_live"}
PROFILE_HASH_FIELD = "profile_content_hash"
PROFILE_HASH_EXCLUDED_FIELDS = frozenset({PROFILE_HASH_FIELD, "generated_at"})
LEGACY_PROFILE_SELECTOR_ENV = "STRATEGY_CANDIDATE_PROFILE_PATH"
APPROVED_PROFILE_SELECTOR_ENV = "APPROVED_STRATEGY_PROFILE_PATH"

STRATEGY_PARAMETER_ENV_KEYS = (
    "SMA_SHORT",
    "SMA_LONG",
    "SMA_FILTER_GAP_MIN_RATIO",
    "SMA_FILTER_VOL_WINDOW",
    "SMA_FILTER_VOL_MIN_RANGE_RATIO",
    "SMA_FILTER_OVEREXT_LOOKBACK",
    "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
    "SMA_COST_EDGE_ENABLED",
    "SMA_COST_EDGE_MIN_RATIO",
    "ENTRY_EDGE_BUFFER_RATIO",
    "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
    "STRATEGY_EXIT_RULES",
    "STRATEGY_EXIT_MAX_HOLDING_MIN",
    "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
    "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
)
COST_MODEL_ENV_KEYS = (
    "LIVE_FEE_RATE_ESTIMATE",
    "PAPER_FEE_RATE",
    "PAPER_FEE_RATE_ESTIMATE",
    "FEE_RATE",
    "STRATEGY_ENTRY_SLIPPAGE_BPS",
    "MAX_MARKET_SLIPPAGE_BPS",
    "SLIPPAGE_BPS",
)


class ApprovedProfileError(ValueError):
    pass


@dataclass(frozen=True)
class ProfileVerificationResult:
    ok: bool
    reason: str
    profile_path: str | None
    profile_hash: str | None
    promotion_hash: str | None
    lineage_hash: str | None
    candidate_profile_hash: str | None
    manifest_hash: str | None
    dataset_content_hash: str | None
    mode: str | None
    expected_runtime_mode: str | None
    mismatches: tuple[dict[str, object], ...]
    profile: dict[str, Any] | None = None
    profile_loaded: bool = False
    profile_schema_hash_valid: bool = False
    source_verified: bool = False
    evidence_verified: bool = False
    runtime_verified: bool = False
    contract_scope: str = "full_approved_profile"

    def audit_fields(self) -> dict[str, object]:
        profile = self.profile if isinstance(self.profile, dict) else {}
        return {
            "approved_profile_path": self.profile_path,
            "approved_profile_hash": self.profile_hash,
            "approved_profile_mode": self.mode,
            "approved_profile_verification_ok": self.ok,
            "approved_profile_block_reason": self.reason,
            "approved_profile_loaded": self.profile_loaded,
            "approved_profile_schema_hash_valid": self.profile_schema_hash_valid,
            "approved_profile_source_verified": self.source_verified,
            "approved_profile_evidence_verified": self.evidence_verified,
            "approved_profile_runtime_verified": self.runtime_verified,
            "approved_profile_contract_scope": self.contract_scope,
            "legacy_candidate_profile_path_used": False,
            "source_promotion_artifact_path": profile.get("source_promotion_artifact_path"),
            "promotion_content_hash": self.promotion_hash,
            "lineage_hash": self.lineage_hash,
            "legacy_compatibility_used": bool(profile.get("legacy_compatibility_used")),
            "candidate_profile_hash": self.candidate_profile_hash,
            "manifest_hash": self.manifest_hash,
            "dataset_content_hash": self.dataset_content_hash,
            "paper_validation_evidence_path": profile.get("paper_validation_evidence_path"),
            "paper_validation_evidence_content_hash": profile.get("paper_validation_evidence_content_hash"),
            "decision_equivalence_report_path": profile.get("decision_equivalence_report_path"),
            "decision_equivalence_content_hash": profile.get("decision_equivalence_content_hash"),
            "live_readiness_evidence_path": profile.get("live_readiness_evidence_path"),
            "live_readiness_evidence_content_hash": profile.get("live_readiness_evidence_content_hash"),
            "approved_profile_mismatch_count": len(self.mismatches),
            "approved_profile_mismatches": [dict(item) for item in self.mismatches],
        }


def approved_profile_path_from_env() -> str:
    return (
        os.getenv(APPROVED_PROFILE_SELECTOR_ENV, "").strip()
        or os.getenv("STRATEGY_APPROVED_PROFILE_PATH", "").strip()
    )


def _load_json(path: str | Path) -> dict[str, Any]:
    try:
        with Path(path).expanduser().open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ApprovedProfileError(f"invalid_json: {exc}") from exc
    except OSError as exc:
        raise ApprovedProfileError(f"unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise ApprovedProfileError("payload_not_object")
    return payload


def _verify_payload_hash(payload: dict[str, Any], *, field: str, label: str) -> str:
    expected = payload.get(field)
    if not isinstance(expected, str) or not expected.startswith("sha256:"):
        raise ApprovedProfileError(f"{label}_hash_missing")
    actual = sha256_prefixed(content_hash_payload({k: v for k, v in payload.items() if k != field}))
    if actual != expected:
        raise ApprovedProfileError(f"{label}_hash_mismatch")
    return actual


def approved_profile_hash_payload(profile: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in profile.items() if k not in PROFILE_HASH_EXCLUDED_FIELDS}


def compute_approved_profile_hash(profile: dict[str, Any]) -> str:
    return sha256_prefixed(content_hash_payload(approved_profile_hash_payload(profile)))


def compute_file_content_hash(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).expanduser().open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def resolve_runtime_artifact_path(
    path: str | Path,
    *,
    manager: PathManager | None = None,
    label: str,
    must_exist: bool = True,
) -> Path:
    raw = str(path or "").strip()
    if not raw:
        raise ApprovedProfileError(f"{label}_path_missing")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        if manager is None:
            raise ApprovedProfileError(f"{label}_path_must_be_absolute")
        candidate = manager.project_root / candidate
    resolved = candidate.resolve()
    project_root = (manager.project_root if manager is not None else Path.cwd()).resolve()
    if PathManager._is_within(resolved, project_root):
        raise ApprovedProfileError(f"{label}_path_repo_local_not_allowed")
    if must_exist and not resolved.exists():
        raise ApprovedProfileError(f"{label}_path_not_found")
    if must_exist and not resolved.is_file():
        raise ApprovedProfileError(f"{label}_path_not_file")
    return resolved


def _resolved_profile_selector_path(raw_path: object) -> str:
    raw = str(raw_path or "").strip()
    if not raw:
        return ""
    return str(Path(raw).expanduser().resolve())


def _verify_selector_matches_profile(
    mismatches: list[dict[str, object]],
    *,
    profile_path: str | Path | None,
    runtime: dict[str, Any],
) -> None:
    if profile_path is None:
        return
    expected = _resolved_profile_selector_path(profile_path)
    actual = _resolved_profile_selector_path(runtime.get("profile_selector"))
    if actual != expected:
        mismatches.append(
            {
                "field": "approved_profile_selector",
                "expected": expected,
                "actual": actual or None,
            }
        )


def expected_profile_modes_for_runtime(runtime: dict[str, Any]) -> tuple[set[str] | None, str | None]:
    mode = str(runtime.get("mode") or "").strip().lower()
    if mode == "paper":
        return {"paper"}, None
    if mode != "live":
        return None, None
    live_dry_run = _bool_value(runtime.get("live_dry_run"))
    live_real_order_armed = _bool_value(runtime.get("live_real_order_armed"))
    if live_dry_run and live_real_order_armed:
        return set(), "live_mode_arming_flags_ambiguous"
    if not live_dry_run and not live_real_order_armed:
        return set(), "live_mode_not_dry_run_or_armed"
    if live_dry_run:
        return {"live_dry_run"}, None
    return {"small_live"}, None


def verify_promotion_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    _verify_payload_hash(payload, field="content_hash", label="promotion_content")
    profile = payload.get("candidate_profile")
    if not isinstance(profile, dict):
        raise ApprovedProfileError("promotion_candidate_profile_missing")
    expected_profile_hash = payload.get("candidate_profile_hash") or payload.get("verified_candidate_profile_hash")
    if sha256_prefixed(profile) != expected_profile_hash:
        raise ApprovedProfileError("promotion_candidate_profile_hash_mismatch")
    if sha256_prefixed(build_candidate_profile(_candidate_like_from_promotion(payload))) != expected_profile_hash:
        raise ApprovedProfileError("promotion_candidate_profile_rebuild_mismatch")
    if payload.get("lineage_required"):
        lineage = payload.get("lineage")
        if not isinstance(lineage, dict):
            raise ApprovedProfileError("lineage_missing")
        try:
            validated_lineage = validate_lineage_artifact(lineage)
        except LineageValidationError as exc:
            raise ApprovedProfileError(str(exc)) from exc
        if validated_lineage.get("lineage_hash") != payload.get("lineage_hash"):
            raise ApprovedProfileError("lineage_hash_mismatch")
        for key in ("manifest_hash", "dataset_content_hash", "candidate_profile_hash"):
            if not _values_equal(payload.get(key), validated_lineage.get(key)):
                raise ApprovedProfileError(f"lineage_{key}_mismatch")
    live_regime_policy = payload.get("live_regime_policy")
    if not isinstance(live_regime_policy, dict):
        raise ApprovedProfileError("promotion_regime_policy_missing")
    return payload


def _candidate_like_from_promotion(payload: dict[str, Any]) -> dict[str, Any]:
    profile = payload.get("candidate_profile") if isinstance(payload.get("candidate_profile"), dict) else {}
    live_policy = payload.get("live_regime_policy") if isinstance(payload.get("live_regime_policy"), dict) else {}
    return {
        "strategy_name": payload.get("strategy_name") or profile.get("strategy_name"),
        "parameter_candidate_id": payload.get("candidate_id") or profile.get("candidate_id"),
        "parameter_values": profile.get("parameter_values"),
        "cost_model": profile.get("cost_model"),
        "execution_model": profile.get("execution_model"),
        "execution_calibration_required": profile.get("execution_calibration_required"),
        "execution_calibration_gate": profile.get("execution_calibration_gate"),
        "experiment_id": payload.get("strategy_profile_source_experiment") or profile.get("source_experiment"),
        "manifest_hash": payload.get("manifest_hash") or profile.get("manifest_hash"),
        "dataset_snapshot_id": payload.get("dataset_snapshot_id") or profile.get("dataset_snapshot_id"),
        "dataset_content_hash": payload.get("dataset_content_hash") or profile.get("dataset_content_hash"),
        "regime_classifier_version": payload.get("regime_classifier_version") or live_policy.get("regime_classifier_version"),
        "allowed_live_regimes": payload.get("allowed_regimes") or live_policy.get("allowed_regimes"),
        "blocked_live_regimes": payload.get("blocked_regimes") or live_policy.get("blocked_regimes"),
    }


def _strategy_parameters_from_promotion(payload: dict[str, Any]) -> dict[str, object]:
    profile = payload.get("candidate_profile") if isinstance(payload.get("candidate_profile"), dict) else {}
    parameters = profile.get("parameter_values")
    if not isinstance(parameters, dict):
        raise ApprovedProfileError("promotion_parameter_values_missing")
    return dict(parameters)


def _cost_model_from_promotion(payload: dict[str, Any]) -> dict[str, object]:
    profile = payload.get("candidate_profile") if isinstance(payload.get("candidate_profile"), dict) else {}
    cost_model = profile.get("cost_model")
    if not isinstance(cost_model, dict):
        raise ApprovedProfileError("promotion_cost_model_missing")
    return dict(cost_model)


def build_approved_profile(
    *,
    promotion: dict[str, Any],
    mode: str,
    source_promotion_path: str,
    market: str,
    interval: str,
    generated_at: str | None = None,
    parent_profile: dict[str, Any] | None = None,
    paper_validation_evidence: str | None = None,
    live_readiness_evidence: str | None = None,
    repository_version: str | None = None,
    manager: PathManager | None = None,
) -> dict[str, Any]:
    verified_promotion = verify_promotion_artifact(dict(promotion))
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in APPROVED_PROFILE_MODES:
        raise ApprovedProfileError(f"invalid_profile_mode: {normalized_mode}")
    source_hash = str(verified_promotion["content_hash"])
    parent_hash = None
    if parent_profile is not None:
        validate_approved_profile(parent_profile)
        parent_hash = str(parent_profile[PROFILE_HASH_FIELD])
    live_policy = verified_promotion.get("live_regime_policy")
    if not isinstance(live_policy, dict):
        raise ApprovedProfileError("regime_policy_missing")
    resolved_source_path = resolve_runtime_artifact_path(
        source_promotion_path,
        manager=manager,
        label="source_promotion_artifact",
    )
    source_promotion = verify_promotion_artifact(_load_json(resolved_source_path))
    if str(source_promotion.get("content_hash") or "") != source_hash:
        raise ApprovedProfileError("source_promotion_content_hash_mismatch")
    payload: dict[str, Any] = {
        "profile_schema_version": APPROVED_PROFILE_SCHEMA_VERSION,
        "profile_mode": normalized_mode,
        "source_promotion_artifact_path": str(resolved_source_path),
        "source_promotion_content_hash": source_hash,
        "lineage_hash": verified_promotion.get("lineage_hash"),
        "legacy_compatibility_used": bool(verified_promotion.get("legacy_compatibility_used")),
        "dataset_quality_legacy_bypass_used": bool(verified_promotion.get("dataset_quality_legacy_bypass_used")),
        "candidate_profile_hash": verified_promotion.get("candidate_profile_hash"),
        "manifest_hash": verified_promotion.get("manifest_hash"),
        "dataset_content_hash": verified_promotion.get("dataset_content_hash"),
        "experiment_family_id": verified_promotion.get("experiment_family_id"),
        "hypothesis_id": verified_promotion.get("hypothesis_id"),
        "hypothesis_status": verified_promotion.get("hypothesis_status"),
        "search_budget": verified_promotion.get("search_budget"),
        "parameter_space_hash": verified_promotion.get("parameter_space_hash"),
        "parameter_grid_size": verified_promotion.get("parameter_grid_size"),
        "attempt_index": verified_promotion.get("attempt_index"),
        "failed_candidate_count": verified_promotion.get("failed_candidate_count"),
        "holdout_reuse_count": verified_promotion.get("holdout_reuse_count"),
        "dataset_reuse_policy": verified_promotion.get("dataset_reuse_policy"),
        "backtest_report_hash": verified_promotion.get("backtest_report_hash"),
        "walk_forward_report_hash": verified_promotion.get("walk_forward_report_hash"),
        "repository_version": repository_version or verified_promotion.get("repository_version") or "unknown",
        "strategy_name": verified_promotion.get("strategy_name"),
        "market": str(market),
        "interval": str(interval),
        "strategy_parameters": _strategy_parameters_from_promotion(verified_promotion),
        "cost_model": _cost_model_from_promotion(verified_promotion),
        "regime_policy": dict(live_policy),
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "parent_profile_hash": parent_hash,
    }
    if paper_validation_evidence:
        path, content_hash = verified_evidence_artifact(
            paper_validation_evidence,
            manager=manager,
            label="paper_validation_evidence",
        )
        payload["paper_validation_evidence_path"] = str(path)
        payload["paper_validation_evidence_content_hash"] = content_hash
    if live_readiness_evidence:
        path, content_hash = verified_evidence_artifact(
            live_readiness_evidence,
            manager=manager,
            label="live_readiness_evidence",
        )
        payload["live_readiness_evidence_path"] = str(path)
        payload["live_readiness_evidence_content_hash"] = content_hash
    payload[PROFILE_HASH_FIELD] = compute_approved_profile_hash(payload)
    return payload


def verified_evidence_artifact(
    path: str | Path,
    *,
    manager: PathManager | None = None,
    label: str,
) -> tuple[Path, str]:
    resolved = resolve_runtime_artifact_path(path, manager=manager, label=label)
    return resolved, compute_file_content_hash(resolved)


def validate_approved_profile(profile: dict[str, Any]) -> dict[str, Any]:
    if int(profile.get("profile_schema_version") or 0) != APPROVED_PROFILE_SCHEMA_VERSION:
        raise ApprovedProfileError("profile_schema_mismatch")
    mode = str(profile.get("profile_mode") or "").strip().lower()
    if mode not in APPROVED_PROFILE_MODES:
        raise ApprovedProfileError("profile_mode_invalid")
    for key in (
        "source_promotion_content_hash",
        "candidate_profile_hash",
        "manifest_hash",
        "dataset_content_hash",
        "strategy_name",
        "market",
        "interval",
    ):
        if not str(profile.get(key) or "").strip():
            raise ApprovedProfileError(f"{key}_missing")
    if not isinstance(profile.get("strategy_parameters"), dict):
        raise ApprovedProfileError("strategy_parameters_missing")
    if not isinstance(profile.get("cost_model"), dict):
        raise ApprovedProfileError("cost_model_missing")
    regime_policy = profile.get("regime_policy")
    if not isinstance(regime_policy, dict):
        raise ApprovedProfileError("regime_policy_missing")
    if not str(regime_policy.get("regime_classifier_version") or "").strip():
        raise ApprovedProfileError("regime_policy_missing_classifier_version")
    if not isinstance(regime_policy.get("allowed_regimes"), list):
        raise ApprovedProfileError("regime_policy_missing_allowed_regimes")
    if not isinstance(regime_policy.get("blocked_regimes"), list):
        raise ApprovedProfileError("regime_policy_missing_blocked_regimes")
    expected = profile.get(PROFILE_HASH_FIELD)
    if not isinstance(expected, str) or not expected.startswith("sha256:"):
        raise ApprovedProfileError("profile_content_hash_missing")
    actual = compute_approved_profile_hash(profile)
    if actual != expected:
        raise ApprovedProfileError("profile_content_hash_mismatch")
    return profile


def load_approved_profile(path: str | Path) -> dict[str, Any]:
    return validate_approved_profile(_load_json(path))


def verify_profile_source_promotion(profile: dict[str, Any]) -> dict[str, Any]:
    promotion = verify_profile_source_artifact(profile)
    verify_profile_evidence_artifacts(profile)
    return promotion


def verify_profile_source_artifact(profile: dict[str, Any]) -> dict[str, Any]:
    validated = validate_approved_profile(profile)
    source_path = str(validated.get("source_promotion_artifact_path") or "").strip()
    resolved_source_path = resolve_runtime_artifact_path(
        source_path,
        label="source_promotion_artifact",
    )
    if str(resolved_source_path) != source_path:
        raise ApprovedProfileError("source_promotion_artifact_path_policy_mismatch")
    promotion = verify_promotion_artifact(_load_json(resolved_source_path))
    expected_hash = str(validated.get("source_promotion_content_hash") or "")
    actual_hash = str(promotion.get("content_hash") or "")
    if actual_hash != expected_hash:
        raise ApprovedProfileError("source_promotion_content_hash_mismatch")
    for key in ("candidate_profile_hash", "manifest_hash", "dataset_content_hash", "strategy_name"):
        if not _values_equal(validated.get(key), promotion.get(key)):
            raise ApprovedProfileError(f"source_promotion_{key}_mismatch")
    if promotion.get("lineage_required"):
        if not str(validated.get("lineage_hash") or "").strip():
            raise ApprovedProfileError("lineage_hash_missing")
        if validated.get("lineage_hash") != promotion.get("lineage_hash"):
            raise ApprovedProfileError("source_promotion_lineage_hash_mismatch")
    return promotion


def verify_profile_evidence_artifacts(profile: dict[str, Any]) -> None:
    for label, expected_type, expected_mode in (
        ("paper_validation_evidence", "paper_validation", "paper"),
        ("live_readiness_evidence", "live_readiness", "live"),
    ):
        path_key = f"{label}_path"
        hash_key = f"{label}_content_hash"
        path = str(profile.get(path_key) or "").strip()
        expected_hash = str(profile.get(hash_key) or "").strip()
        if not path and not expected_hash:
            continue
        if not path:
            raise ApprovedProfileError(f"{path_key}_missing")
        if not expected_hash.startswith("sha256:"):
            raise ApprovedProfileError(f"{hash_key}_missing")
        resolved = resolve_runtime_artifact_path(path, label=label)
        if str(resolved) != path:
            raise ApprovedProfileError(f"{label}_path_policy_mismatch")
        evidence_parent = dict(profile)
        anchor_key = (
            "paper_validation_approved_profile_hash"
            if label == "paper_validation_evidence"
            else "live_readiness_approved_profile_hash"
        )
        anchor_hash = str(profile.get(anchor_key) or "").strip()
        if anchor_hash:
            evidence_parent[PROFILE_HASH_FIELD] = anchor_hash
        try:
            payload = _load_json(resolved)
            recorded_hash = str(payload.get("content_hash") or "").strip()
            if recorded_hash != expected_hash:
                raise ApprovedProfileError(f"{hash_key}_mismatch")
            semantic_hash = validate_profile_transition_evidence(
                payload,
                label=label,
                expected_type=expected_type,
                expected_mode=expected_mode,
                parent_profile=evidence_parent,
                evidence_path=resolved,
            )
            _validate_decision_equivalence_evidence(
                payload,
                label=label,
                parent_profile=evidence_parent,
            )
        except ApprovedProfileError:
            raise
        except ValueError as exc:
            raise ApprovedProfileError(str(exc)) from exc
        if semantic_hash != expected_hash:
            raise ApprovedProfileError(f"{hash_key}_mismatch")


def load_profile_or_promotion_regime_policy(
    path: str | Path | None,
    *,
    verify_source: bool = False,
    approved_profile_contract_scope: str = "full_approved_profile",
) -> dict[str, object] | None:
    raw = str(path or "").strip()
    if not raw:
        return None
    try:
        payload = _load_json(raw)
    except ApprovedProfileError as exc:
        return {
            "_policy_load_error": str(exc),
            "_policy_source": raw,
        }
    if "profile_schema_version" in payload:
        try:
            profile = validate_approved_profile(payload)
        except ApprovedProfileError as exc:
            return {
                "_policy_load_error": str(exc),
                "_policy_source": raw,
                "approved_profile_verification_ok": False,
                "approved_profile_block_reason": str(exc),
                "approved_profile_loaded": False,
                "approved_profile_schema_hash_valid": False,
                "approved_profile_source_verified": False,
                "approved_profile_evidence_verified": False,
                "approved_profile_runtime_verified": False,
                "approved_profile_contract_scope": approved_profile_contract_scope,
            }
        if verify_source:
            try:
                verify_profile_source_artifact(profile)
            except ApprovedProfileError as exc:
                return {
                    "_policy_load_error": str(exc),
                    "_policy_source": raw,
                    "approved_profile_verification_ok": False,
                    "approved_profile_block_reason": str(exc),
                    "approved_profile_loaded": True,
                    "approved_profile_schema_hash_valid": True,
                    "approved_profile_source_verified": False,
                    "approved_profile_evidence_verified": False,
                    "approved_profile_runtime_verified": False,
                    "approved_profile_contract_scope": approved_profile_contract_scope,
                    "approved_profile_mode": profile.get("profile_mode"),
                    "approved_profile_path": str(Path(raw).expanduser().resolve()),
                    "approved_profile_hash": profile.get(PROFILE_HASH_FIELD),
                    "source_promotion_content_hash": profile.get("source_promotion_content_hash"),
                    "promotion_content_hash": profile.get("source_promotion_content_hash"),
                    "lineage_hash": profile.get("lineage_hash"),
                    "legacy_compatibility_used": bool(profile.get("legacy_compatibility_used")),
                    "source_promotion_artifact_path": profile.get("source_promotion_artifact_path"),
                    "candidate_profile_hash": profile.get("candidate_profile_hash"),
                    "manifest_hash": profile.get("manifest_hash"),
                    "dataset_content_hash": profile.get("dataset_content_hash"),
                    "paper_validation_evidence_path": profile.get("paper_validation_evidence_path"),
                    "paper_validation_evidence_content_hash": profile.get(
                        "paper_validation_evidence_content_hash"
                    ),
                    "live_readiness_evidence_path": profile.get("live_readiness_evidence_path"),
                    "live_readiness_evidence_content_hash": profile.get(
                        "live_readiness_evidence_content_hash"
                    ),
                }
            try:
                verify_profile_evidence_artifacts(profile)
            except ApprovedProfileError as exc:
                return {
                    "_policy_load_error": str(exc),
                    "_policy_source": raw,
                    "approved_profile_verification_ok": False,
                    "approved_profile_block_reason": str(exc),
                    "approved_profile_loaded": True,
                    "approved_profile_schema_hash_valid": True,
                    "approved_profile_source_verified": True,
                    "approved_profile_evidence_verified": False,
                    "approved_profile_runtime_verified": False,
                    "approved_profile_contract_scope": approved_profile_contract_scope,
                    "approved_profile_mode": profile.get("profile_mode"),
                    "approved_profile_path": str(Path(raw).expanduser().resolve()),
                    "approved_profile_hash": profile.get(PROFILE_HASH_FIELD),
                    "source_promotion_content_hash": profile.get("source_promotion_content_hash"),
                    "promotion_content_hash": profile.get("source_promotion_content_hash"),
                    "lineage_hash": profile.get("lineage_hash"),
                    "legacy_compatibility_used": bool(profile.get("legacy_compatibility_used")),
                    "source_promotion_artifact_path": profile.get("source_promotion_artifact_path"),
                    "candidate_profile_hash": profile.get("candidate_profile_hash"),
                    "manifest_hash": profile.get("manifest_hash"),
                    "dataset_content_hash": profile.get("dataset_content_hash"),
                    "paper_validation_evidence_path": profile.get("paper_validation_evidence_path"),
                    "paper_validation_evidence_content_hash": profile.get(
                        "paper_validation_evidence_content_hash"
                    ),
                    "live_readiness_evidence_path": profile.get("live_readiness_evidence_path"),
                    "live_readiness_evidence_content_hash": profile.get(
                        "live_readiness_evidence_content_hash"
                    ),
                }
        policy = profile.get("regime_policy")
        if isinstance(policy, dict):
            source_verified = bool(verify_source)
            block_reason = "ok" if source_verified else "legacy_regime_policy_only_source_not_verified"
            return {
                "live_regime_policy": dict(policy),
                "strategy_profile_id": profile.get("profile_id") or profile.get(PROFILE_HASH_FIELD),
                "strategy_profile_hash": profile.get(PROFILE_HASH_FIELD),
                "content_hash": profile.get(PROFILE_HASH_FIELD),
                "approved_profile_mode": profile.get("profile_mode"),
                "approved_profile_path": str(Path(raw).expanduser().resolve()),
                "approved_profile_hash": profile.get(PROFILE_HASH_FIELD),
                "approved_profile_verification_ok": source_verified,
                "approved_profile_block_reason": block_reason,
                "approved_profile_loaded": True,
                "approved_profile_schema_hash_valid": True,
                "approved_profile_source_verified": source_verified,
                "approved_profile_evidence_verified": source_verified,
                "approved_profile_runtime_verified": source_verified,
                "approved_profile_contract_scope": approved_profile_contract_scope,
                "source_promotion_content_hash": profile.get("source_promotion_content_hash"),
                "promotion_content_hash": profile.get("source_promotion_content_hash"),
                "lineage_hash": profile.get("lineage_hash"),
                "legacy_compatibility_used": bool(profile.get("legacy_compatibility_used")),
                "source_promotion_artifact_path": profile.get("source_promotion_artifact_path"),
                "candidate_profile_hash": profile.get("candidate_profile_hash"),
                "manifest_hash": profile.get("manifest_hash"),
                "dataset_content_hash": profile.get("dataset_content_hash"),
                "paper_validation_evidence_path": profile.get("paper_validation_evidence_path"),
                "paper_validation_evidence_content_hash": profile.get("paper_validation_evidence_content_hash"),
                "live_readiness_evidence_path": profile.get("live_readiness_evidence_path"),
                "live_readiness_evidence_content_hash": profile.get("live_readiness_evidence_content_hash"),
            }
    return payload


def write_approved_profile_atomic(path: str | Path, profile: dict[str, Any], *, manager: PathManager) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (manager.project_root / resolved).resolve()
    else:
        resolved = resolved.resolve()
    _ensure_profile_output_path_allowed(manager, resolved)
    write_json_atomic(resolved, validate_approved_profile(dict(profile)))
    return resolved


def _ensure_profile_output_path_allowed(manager: PathManager, path: Path) -> None:
    if PathManager._is_within(path.resolve(), manager.project_root.resolve()):
        raise PathPolicyError(f"profile output path must be outside repository: {path.resolve()}")


def default_profile_output_path(*, manager: PathManager, profile: dict[str, Any]) -> Path:
    profile_hash = str(profile.get(PROFILE_HASH_FIELD) or "sha256:unknown").split(":", 1)[-1][:16]
    return manager.data_dir() / "reports" / "profiles" / f"{profile.get('profile_mode')}_{profile_hash}.json"


def parse_env_file(path: str | Path) -> dict[str, str]:
    values: dict[str, str] = {}
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or key.startswith("export "):
                key = key.removeprefix("export ").strip()
            values[key] = value.strip().strip('"').strip("'")
    return values


def runtime_contract_from_env_values(env: dict[str, str]) -> dict[str, Any]:
    def _value(*keys: str, default: str = "") -> str:
        for key in keys:
            if env.get(key, "").strip() != "":
                return env[key]
        return default

    strategy_parameters = {
        "SMA_SHORT": _value("SMA_SHORT", default="7"),
        "SMA_LONG": _value("SMA_LONG", default="30"),
        "SMA_FILTER_GAP_MIN_RATIO": _value("SMA_FILTER_GAP_MIN_RATIO", default="0.0012"),
        "SMA_FILTER_VOL_WINDOW": _value("SMA_FILTER_VOL_WINDOW", default="10"),
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": _value("SMA_FILTER_VOL_MIN_RANGE_RATIO", default="0.003"),
        "SMA_FILTER_OVEREXT_LOOKBACK": _value("SMA_FILTER_OVEREXT_LOOKBACK", default="3"),
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": _value("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", default="0.02"),
        "SMA_COST_EDGE_ENABLED": _value("SMA_COST_EDGE_ENABLED", default="true"),
        "SMA_COST_EDGE_MIN_RATIO": _value(
            "SMA_COST_EDGE_MIN_RATIO",
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
            default="0",
        ),
        "ENTRY_EDGE_BUFFER_RATIO": _value("ENTRY_EDGE_BUFFER_RATIO", default="0.0005"),
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": _value("STRATEGY_MIN_EXPECTED_EDGE_RATIO", default="0"),
        "STRATEGY_EXIT_RULES": _value("STRATEGY_EXIT_RULES", default="opposite_cross,max_holding_time"),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": _value("STRATEGY_EXIT_MAX_HOLDING_MIN", default="0"),
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": _value("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", default="0"),
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": _value(
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
            default="0",
        ),
    }
    return {
        "mode": _value("MODE", default="paper"),
        "live_dry_run": _value("LIVE_DRY_RUN", default="true"),
        "live_real_order_armed": _value("LIVE_REAL_ORDER_ARMED", default="false"),
        "profile_selector": _value(APPROVED_PROFILE_SELECTOR_ENV, "STRATEGY_APPROVED_PROFILE_PATH"),
        "strategy_name": _value("STRATEGY_NAME", default="sma_with_filter"),
        "market": _value("MARKET", "PAIR", default="KRW-BTC"),
        "interval": _value("INTERVAL", default="1m"),
        "strategy_parameters": strategy_parameters,
        "cost_model": {
            "fee_rate": _value(
                "LIVE_FEE_RATE_ESTIMATE",
                "PAPER_FEE_RATE",
                "PAPER_FEE_RATE_ESTIMATE",
                "FEE_RATE",
                default="0.0025",
            ),
            "slippage_bps": _value(
                "STRATEGY_ENTRY_SLIPPAGE_BPS",
                "MAX_MARKET_SLIPPAGE_BPS",
                "SLIPPAGE_BPS",
                default="0",
            ),
        },
    }


def runtime_contract_from_settings(cfg: object) -> dict[str, Any]:
    profile_selector = (
        str(getattr(cfg, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
        or str(getattr(cfg, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
    )
    return {
        "mode": str(getattr(cfg, "MODE", "")),
        "live_dry_run": bool(getattr(cfg, "LIVE_DRY_RUN", True)),
        "live_real_order_armed": bool(getattr(cfg, "LIVE_REAL_ORDER_ARMED", False)),
        "profile_selector": profile_selector,
        "strategy_name": str(getattr(cfg, "STRATEGY_NAME", "")),
        "market": str(getattr(cfg, "PAIR", "")),
        "interval": str(getattr(cfg, "INTERVAL", "")),
        "strategy_parameters": {
            "SMA_SHORT": int(getattr(cfg, "SMA_SHORT")),
            "SMA_LONG": int(getattr(cfg, "SMA_LONG")),
            "SMA_FILTER_GAP_MIN_RATIO": float(getattr(cfg, "SMA_FILTER_GAP_MIN_RATIO")),
            "SMA_FILTER_VOL_WINDOW": int(getattr(cfg, "SMA_FILTER_VOL_WINDOW")),
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": float(getattr(cfg, "SMA_FILTER_VOL_MIN_RANGE_RATIO")),
            "SMA_FILTER_OVEREXT_LOOKBACK": int(getattr(cfg, "SMA_FILTER_OVEREXT_LOOKBACK")),
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": float(getattr(cfg, "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO")),
            "SMA_COST_EDGE_ENABLED": bool(getattr(cfg, "SMA_COST_EDGE_ENABLED")),
            "SMA_COST_EDGE_MIN_RATIO": float(getattr(cfg, "SMA_COST_EDGE_MIN_RATIO")),
            "ENTRY_EDGE_BUFFER_RATIO": float(getattr(cfg, "ENTRY_EDGE_BUFFER_RATIO")),
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO": float(getattr(cfg, "STRATEGY_MIN_EXPECTED_EDGE_RATIO")),
            "STRATEGY_EXIT_RULES": str(getattr(cfg, "STRATEGY_EXIT_RULES")),
            "STRATEGY_EXIT_MAX_HOLDING_MIN": int(getattr(cfg, "STRATEGY_EXIT_MAX_HOLDING_MIN")),
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": float(getattr(cfg, "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO")),
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": float(
                getattr(cfg, "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO")
            ),
        },
        "cost_model": {
            "fee_rate": float(getattr(cfg, "LIVE_FEE_RATE_ESTIMATE")),
            "slippage_bps": float(getattr(cfg, "STRATEGY_ENTRY_SLIPPAGE_BPS")),
        },
    }


def diff_profile_to_runtime(
    profile: dict[str, Any],
    runtime: dict[str, Any],
    *,
    profile_path: str | Path | None = None,
) -> tuple[dict[str, object], ...]:
    validate_approved_profile(profile)
    mismatches: list[dict[str, object]] = []
    _verify_selector_matches_profile(mismatches, profile_path=profile_path, runtime=runtime)
    _compare_profile_mode(mismatches, profile, runtime)
    _compare_scalar(mismatches, "strategy_name", profile.get("strategy_name"), runtime.get("strategy_name"))
    _compare_scalar(mismatches, "market", profile.get("market"), runtime.get("market"))
    _compare_scalar(mismatches, "interval", profile.get("interval"), runtime.get("interval"))
    profile_params = profile.get("strategy_parameters") if isinstance(profile.get("strategy_parameters"), dict) else {}
    runtime_params = runtime.get("strategy_parameters") if isinstance(runtime.get("strategy_parameters"), dict) else {}
    for key, expected in profile_params.items():
        if key not in runtime_params:
            mismatches.append({"field": f"strategy_parameters.{key}", "expected": expected, "actual": None})
            continue
        if not _values_equal(expected, runtime_params[key]):
            mismatches.append(
                {"field": f"strategy_parameters.{key}", "expected": expected, "actual": runtime_params[key]}
            )
    profile_cost = profile.get("cost_model") if isinstance(profile.get("cost_model"), dict) else {}
    runtime_cost = runtime.get("cost_model") if isinstance(runtime.get("cost_model"), dict) else {}
    for key, expected in profile_cost.items():
        if key not in runtime_cost:
            mismatches.append({"field": f"cost_model.{key}", "expected": expected, "actual": None})
            continue
        if not _values_equal(expected, runtime_cost[key]):
            mismatches.append({"field": f"cost_model.{key}", "expected": expected, "actual": runtime_cost[key]})
    return tuple(mismatches)


def _compare_profile_mode(mismatches: list[dict[str, object]], profile: dict[str, Any], runtime: dict[str, Any]) -> None:
    profile_mode = str(profile.get("profile_mode") or "").strip().lower()
    runtime_mode = str(runtime.get("mode") or "").strip().lower()
    if profile_mode == "paper":
        ok = runtime_mode == "paper"
        expected = "MODE=paper"
    elif profile_mode in LIVE_COMPATIBLE_PROFILE_MODES:
        ok = runtime_mode == "live"
        expected = "MODE=live"
    else:
        ok = False
        expected = "valid approved profile mode"
    if not ok:
        mismatches.append(
            {
                "field": "profile_mode_compatibility",
                "expected": expected,
                "actual": f"MODE={runtime_mode or '-'}",
            }
        )


def _compare_scalar(mismatches: list[dict[str, object]], field: str, expected: object, actual: object) -> None:
    if not _values_equal(expected, actual):
        mismatches.append({"field": field, "expected": expected, "actual": actual})


def _values_equal(left: object, right: object) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return _bool_value(left) == _bool_value(right)
    try:
        return abs(float(left) - float(right)) <= 1e-12
    except (TypeError, ValueError):
        return str(left).strip() == str(right).strip()


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def verify_profile_against_runtime(
    *,
    profile_path: str | Path | None,
    runtime: dict[str, Any],
    require_profile: bool,
    expected_profile_modes: set[str] | None = None,
    expected_profile_mode_reason: str | None = None,
    verify_source_promotion: bool = False,
) -> ProfileVerificationResult:
    raw_path = str(profile_path or "").strip()
    if not raw_path:
        reason = "approved_profile_missing" if require_profile else "approved_profile_not_configured"
        return _verification_result(False, reason, None, None, tuple(), None)
    try:
        if expected_profile_modes is not None and len(expected_profile_modes) == 0:
            reason = expected_profile_mode_reason
            if reason is None:
                _, reason = expected_profile_modes_for_runtime(runtime)
            return _verification_result(
                False,
                reason or "profile_expected_mode_unavailable",
                raw_path,
                None,
                tuple(),
                runtime,
            )
        profile = load_approved_profile(raw_path)
        if verify_source_promotion:
            try:
                verify_profile_source_artifact(profile)
            except ApprovedProfileError as exc:
                return _verification_result(
                    False,
                    str(exc),
                    raw_path,
                    profile,
                    tuple(),
                    runtime,
                    profile_loaded=True,
                    profile_schema_hash_valid=True,
                    source_verified=False,
                    evidence_verified=False,
                    runtime_verified=False,
                )
            try:
                verify_profile_evidence_artifacts(profile)
            except ApprovedProfileError as exc:
                return _verification_result(
                    False,
                    str(exc),
                    raw_path,
                    profile,
                    tuple(),
                    runtime,
                    profile_loaded=True,
                    profile_schema_hash_valid=True,
                    source_verified=True,
                    evidence_verified=False,
                    runtime_verified=False,
                )
        mode = str(profile.get("profile_mode"))
        if expected_profile_modes is not None and mode not in expected_profile_modes:
            return _verification_result(
                False,
                f"profile_mode_mismatch: expected={sorted(expected_profile_modes)} actual={mode}",
                raw_path,
                profile,
                tuple(),
                runtime,
                profile_loaded=True,
                profile_schema_hash_valid=True,
                source_verified=bool(verify_source_promotion),
                evidence_verified=bool(verify_source_promotion),
                runtime_verified=False,
            )
        mismatches = diff_profile_to_runtime(profile, runtime, profile_path=raw_path)
        if mismatches:
            return _verification_result(
                False,
                "approved_profile_runtime_mismatch",
                raw_path,
                profile,
                mismatches,
                runtime,
                profile_loaded=True,
                profile_schema_hash_valid=True,
                source_verified=bool(verify_source_promotion),
                evidence_verified=bool(verify_source_promotion),
                runtime_verified=False,
            )
        return _verification_result(
            True,
            "ok",
            raw_path,
            profile,
            tuple(),
            runtime,
            profile_loaded=True,
            profile_schema_hash_valid=True,
            source_verified=bool(verify_source_promotion),
            evidence_verified=bool(verify_source_promotion),
            runtime_verified=True,
        )
    except ApprovedProfileError as exc:
        return _verification_result(False, str(exc), raw_path, None, tuple(), runtime)


def _verification_result(
    ok: bool,
    reason: str,
    path: str | None,
    profile: dict[str, Any] | None,
    mismatches: tuple[dict[str, object], ...],
    runtime: dict[str, Any] | None,
    profile_loaded: bool = False,
    profile_schema_hash_valid: bool = False,
    source_verified: bool = False,
    evidence_verified: bool = False,
    runtime_verified: bool = False,
    contract_scope: str = "full_approved_profile",
) -> ProfileVerificationResult:
    return ProfileVerificationResult(
        ok=bool(ok),
        reason=str(reason),
        profile_path=None if path is None else str(Path(path).expanduser().resolve()),
        profile_hash=None if profile is None else str(profile.get(PROFILE_HASH_FIELD) or ""),
        promotion_hash=None if profile is None else str(profile.get("source_promotion_content_hash") or ""),
        lineage_hash=None if profile is None else str(profile.get("lineage_hash") or ""),
        candidate_profile_hash=None if profile is None else str(profile.get("candidate_profile_hash") or ""),
        manifest_hash=None if profile is None else str(profile.get("manifest_hash") or ""),
        dataset_content_hash=None if profile is None else str(profile.get("dataset_content_hash") or ""),
        mode=None if profile is None else str(profile.get("profile_mode") or ""),
        expected_runtime_mode=None if runtime is None else str(runtime.get("mode") or ""),
        mismatches=mismatches,
        profile=profile,
        profile_loaded=profile_loaded,
        profile_schema_hash_valid=profile_schema_hash_valid,
        source_verified=source_verified,
        evidence_verified=evidence_verified,
        runtime_verified=runtime_verified,
        contract_scope=contract_scope,
    )


def promote_profile_mode(
    *,
    parent_profile: dict[str, Any],
    target_mode: str,
    paper_validation_evidence: str | None = None,
    live_readiness_evidence: str | None = None,
    generated_at: str | None = None,
    manager: PathManager | None = None,
) -> dict[str, Any]:
    parent = validate_approved_profile(dict(parent_profile))
    verify_profile_source_promotion(parent)
    if bool(parent.get("dataset_quality_legacy_bypass_used")):
        raise ApprovedProfileError("legacy_dataset_quality_bypass_not_live_ready")
    parent_mode = str(parent["profile_mode"])
    target = str(target_mode or "").strip().lower()
    if target == "live_dry_run":
        if parent_mode != "paper":
            raise ApprovedProfileError("profile_transition_requires_paper_parent")
        if not str(paper_validation_evidence or "").strip():
            raise ApprovedProfileError("paper_validation_evidence_required")
    elif target == "small_live":
        if parent_mode != "live_dry_run":
            raise ApprovedProfileError("profile_transition_requires_live_dry_run_parent")
        if not str(live_readiness_evidence or "").strip():
            raise ApprovedProfileError("live_readiness_evidence_required")
    else:
        raise ApprovedProfileError(f"profile_transition_target_invalid: {target}")
    child = dict(parent)
    child["profile_mode"] = target
    child["parent_profile_hash"] = parent[PROFILE_HASH_FIELD]
    child.pop("paper_validation_evidence", None)
    child.pop("live_readiness_evidence", None)
    if target == "live_dry_run":
        evidence_path, evidence_hash = verified_evidence_artifact(
            paper_validation_evidence or "",
            manager=manager,
            label="paper_validation_evidence",
        )
        _validate_transition_evidence_file(
            evidence_path,
            label="paper_validation_evidence",
            expected_type="paper_validation",
            expected_mode="paper",
            parent_profile=parent,
            expected_hash=None,
        )
        evidence_hash = validate_profile_transition_evidence(
            _load_json(evidence_path),
            label="paper_validation_evidence",
            expected_type="paper_validation",
            expected_mode="paper",
            parent_profile=parent,
            evidence_path=evidence_path,
        )
        _validate_decision_equivalence_evidence(
            _load_json(evidence_path),
            label="paper_validation_evidence",
            parent_profile=parent,
        )
        child["paper_validation_evidence_path"] = str(evidence_path)
        child["paper_validation_evidence_content_hash"] = evidence_hash
        child["paper_validation_approved_profile_hash"] = parent[PROFILE_HASH_FIELD]
        _copy_decision_equivalence_fields(
            child,
            _load_json(evidence_path),
        )
        child.pop("live_readiness_evidence_path", None)
        child.pop("live_readiness_evidence_content_hash", None)
        child.pop("live_readiness_approved_profile_hash", None)
    else:
        if not str(parent.get("paper_validation_evidence_path") or "").strip():
            raise ApprovedProfileError("paper_validation_evidence_path_missing")
        verify_profile_evidence_artifacts(parent)
        evidence_path, evidence_hash = verified_evidence_artifact(
            live_readiness_evidence or "",
            manager=manager,
            label="live_readiness_evidence",
        )
        _validate_transition_evidence_file(
            evidence_path,
            label="live_readiness_evidence",
            expected_type="live_readiness",
            expected_mode="live",
            parent_profile=parent,
            expected_hash=None,
        )
        evidence_hash = validate_profile_transition_evidence(
            _load_json(evidence_path),
            label="live_readiness_evidence",
            expected_type="live_readiness",
            expected_mode="live",
            parent_profile=parent,
            evidence_path=evidence_path,
        )
        _validate_decision_equivalence_evidence(
            _load_json(evidence_path),
            label="live_readiness_evidence",
            parent_profile=parent,
        )
        child["live_readiness_evidence_path"] = str(evidence_path)
        child["live_readiness_evidence_content_hash"] = evidence_hash
        child["live_readiness_approved_profile_hash"] = parent[PROFILE_HASH_FIELD]
        _copy_decision_equivalence_fields(
            child,
            _load_json(evidence_path),
        )
    child["generated_at"] = generated_at or datetime.now(timezone.utc).isoformat()
    child.pop(PROFILE_HASH_FIELD, None)
    child[PROFILE_HASH_FIELD] = compute_approved_profile_hash(child)
    return validate_approved_profile(child)


def _validate_transition_evidence_file(
    path: Path,
    *,
    label: str,
    expected_type: str,
    expected_mode: str,
    parent_profile: dict[str, Any],
    expected_hash: str | None,
) -> None:
    try:
        semantic_hash = validate_profile_transition_evidence(
            _load_json(path),
            label=label,
            expected_type=expected_type,
            expected_mode=expected_mode,
            parent_profile=parent_profile,
            evidence_path=path,
        )
    except ApprovedProfileError:
        raise
    except ValueError as exc:
        raise ApprovedProfileError(str(exc)) from exc
    if expected_hash is not None and semantic_hash != expected_hash:
        raise ApprovedProfileError(f"{label}_content_hash_mismatch")


def _copy_decision_equivalence_fields(child: dict[str, Any], evidence: dict[str, Any]) -> None:
    child["decision_equivalence_report_path"] = evidence.get("decision_equivalence_report_path")
    child["decision_equivalence_content_hash"] = evidence.get("decision_equivalence_content_hash")
    child["decision_equivalence_matched_decision_count"] = evidence.get("matched_decision_count")
    child["decision_equivalence_mismatch_count"] = evidence.get("mismatch_count")


def _validate_decision_equivalence_evidence(
    payload: dict[str, Any],
    *,
    label: str,
    parent_profile: dict[str, Any],
) -> None:
    path_value = str(payload.get("decision_equivalence_report_path") or "").strip()
    hash_value = str(payload.get("decision_equivalence_content_hash") or "").strip()
    if not path_value or not hash_value:
        raise ApprovedProfileError(f"{label}_decision_equivalence_missing")
    resolved = resolve_runtime_artifact_path(path_value, label=f"{label}_decision_equivalence")
    report = _load_json(resolved)
    recorded_hash = str(report.get("content_hash") or "").strip()
    if recorded_hash != hash_value:
        raise ApprovedProfileError(f"{label}_decision_equivalence_hash_mismatch")
    if compute_decision_equivalence_hash(report) != hash_value:
        raise ApprovedProfileError(f"{label}_decision_equivalence_hash_mismatch")
    expected_profile_hash = str(parent_profile.get(PROFILE_HASH_FIELD) or "").strip()
    actual_profile_hash = str(
        report.get("approved_profile_hash")
        or report.get("profile_content_hash")
        or payload.get("decision_equivalence_approved_profile_hash")
        or ""
    ).strip()
    if actual_profile_hash != expected_profile_hash:
        raise ApprovedProfileError(f"{label}_decision_equivalence_profile_hash_mismatch")
    if str(report.get("market") or "").strip() != str(parent_profile.get("market") or "").strip():
        raise ApprovedProfileError(f"{label}_decision_equivalence_market_mismatch")
    if str(report.get("interval") or "").strip() != str(parent_profile.get("interval") or "").strip():
        raise ApprovedProfileError(f"{label}_decision_equivalence_interval_mismatch")
    dataset_hash = str(parent_profile.get("dataset_content_hash") or "").strip()
    actual_dataset_hash = str(
        report.get("dataset_content_hash")
        or report.get("data_fingerprint")
        or payload.get("decision_equivalence_dataset_content_hash")
        or ""
    ).strip()
    if dataset_hash and actual_dataset_hash and actual_dataset_hash != dataset_hash:
        raise ApprovedProfileError(f"{label}_decision_equivalence_dataset_hash_mismatch")
    evidence_db_fingerprint = str(payload.get("db_data_fingerprint") or "").strip()
    report_data_fingerprint = str(report.get("db_data_fingerprint") or "").strip()
    if evidence_db_fingerprint and report_data_fingerprint and evidence_db_fingerprint != report_data_fingerprint:
        raise ApprovedProfileError(f"{label}_decision_equivalence_data_fingerprint_mismatch")
    mismatch_count = int(report.get("mismatch_count") or report.get("mismatched_decision_count") or 0)
    if mismatch_count > 0:
        raise ApprovedProfileError(f"{label}_decision_equivalence_mismatch_count_nonzero")
    if int(report.get("canonical_incomplete_decision_count") or 0) > 0:
        raise ApprovedProfileError(f"{label}_decision_equivalence_incomplete_canonical")
    if report.get("missing_research_decisions"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_missing_research_decisions")
    if report.get("missing_runtime_decisions"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_missing_runtime_decisions")
    if bool(report.get("blocked_decision_equivalence")):
        raise ApprovedProfileError(f"{label}_decision_equivalence_blocked")
    if report.get("comparison_contract_version") != "canonical_decision_v1" or report.get("canonical_schema") is not True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_legacy_schema")
    if report.get("legacy_schema") is True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_legacy_schema")
    if report.get("promotion_grade_comparison") is not True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_not_promotion_grade")
    if report.get("ok") is not True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_not_ok")
