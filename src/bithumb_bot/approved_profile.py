from __future__ import annotations

import json
import os
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import PathManager, PathPolicyError
from .execution_reality_contract import (
    build_execution_reality_contract,
    capability_contract_hash_matches,
    contract_hash_matches,
    execution_capability_contract_mismatch_reasons,
    execution_contract_mismatch_reasons,
    unsupported_capability_reasons,
    validate_execution_capability_contract,
)
from .evidence_chain import (
    EvidenceValidationError,
    validate_candidate_regime_policy_equivalence_evidence,
    validate_profile_transition_evidence,
)
from .decision_equivalence import compute_decision_equivalence_hash
from .research.hashing import content_hash_payload, sha256_prefixed
from .research.lineage import validate_lineage_artifact, LineageValidationError
from .research.promotion_gate import build_candidate_profile
from .research.deployment_policy import deployment_tier_for_profile_mode, validate_production_calibration_policy
from .research.strategy_spec import (
    exit_policy_from_parameters,
    materialized_strategy_parameters_hash,
    strategy_parameter_source_map,
    strategy_spec_for_name,
)
from .research.strategy_registry import (
    ResearchStrategyRegistryError,
    resolve_research_strategy_plugin,
    runtime_strategy_parameter_env_keys,
    runtime_strategy_parameters_from_env,
    runtime_strategy_parameters_from_settings,
)
from .storage_io import write_json_atomic


APPROVED_PROFILE_SCHEMA_VERSION = 1
APPROVED_PROFILE_MODES = {"paper", "live_dry_run", "small_live"}
LIVE_COMPATIBLE_PROFILE_MODES = {"live_dry_run", "small_live"}
PROFILE_HASH_FIELD = "profile_content_hash"
PROFILE_HASH_EXCLUDED_FIELDS = frozenset({PROFILE_HASH_FIELD, "generated_at"})
LEGACY_PROFILE_SELECTOR_ENV = "STRATEGY_CANDIDATE_PROFILE_PATH"
APPROVED_PROFILE_SELECTOR_ENV = "APPROVED_STRATEGY_PROFILE_PATH"
SUPPORTED_DECISION_EQUIVALENCE_CONTRACTS = frozenset(
    {"canonical_decision_v1", "canonical_decision_v2"}
)

STRATEGY_PARAMETER_ENV_KEYS = runtime_strategy_parameter_env_keys("sma_with_filter")
COST_MODEL_ENV_KEYS = (
    "LIVE_FEE_RATE_ESTIMATE",
    "PAPER_FEE_RATE",
    "PAPER_FEE_RATE_ESTIMATE",
    "FEE_RATE",
    "STRATEGY_ENTRY_SLIPPAGE_BPS",
    "MAX_MARKET_SLIPPAGE_BPS",
    "SLIPPAGE_BPS",
)
EXECUTION_CONTRACT_ENV_KEYS = (
    "EXECUTION_FILL_REFERENCE_POLICY",
    "EXECUTION_DECISION_GUARD_MS",
    "EXECUTION_MAX_QUOTE_WAIT_MS",
    "EXECUTION_MISSING_QUOTE_POLICY",
    "EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION",
    "EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL",
    "EXECUTION_QUOTE_SOURCE",
    "EXECUTION_QUOTE_AGE_LIMIT_MS",
    "EXECUTION_TOP_OF_BOOK_REQUIRED",
    "EXECUTION_TOP_OF_BOOK_IS_FULL_DEPTH",
    "EXECUTION_DEPTH_REQUIRED",
    "EXECUTION_TRADE_TICK_REQUIRED",
    "EXECUTION_QUEUE_POSITION_REQUIRED",
    "EXECUTION_MARKET_IMPACT_REQUIRED",
    "EXECUTION_INTRA_CANDLE_PATH_AVAILABLE",
    "EXECUTION_REALITY_LEVEL",
    "EXECUTION_LATENCY_MODEL_TYPE",
    "EXECUTION_LATENCY_MS",
    "EXECUTION_PARTIAL_FILL_MODEL_TYPE",
    "EXECUTION_PARTIAL_FILL_RATE",
    "EXECUTION_ORDER_FAILURE_MODEL_TYPE",
    "EXECUTION_ORDER_FAILURE_RATE",
    "EXECUTION_FEE_SOURCE",
    "EXECUTION_SLIPPAGE_SOURCE",
    "EXECUTION_CALIBRATION_REQUIRED",
    "EXECUTION_CALIBRATION_ARTIFACT_HASH",
)
PROFILE_RUNTIME_COST_MISMATCH_ACTION = (
    "Profile/runtime cost mismatch. Regenerate or select an approved profile whose base cost assumption "
    "matches the current runtime fee/slippage contract, or adjust the runtime env and rerun "
    "config/runtime-contract dump."
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
            "candidate_regime_policy_applied_in_research": profile.get(
                "candidate_regime_policy_applied_in_research"
            ),
            "candidate_regime_policy_required_for_live": profile.get(
                "candidate_regime_policy_required_for_live"
            ),
            "candidate_regime_policy_equivalence_required": profile.get(
                "candidate_regime_policy_equivalence_required"
            ),
            "candidate_regime_policy_equivalence_evidence_hash": profile.get(
                "candidate_regime_policy_equivalence_evidence_hash"
            ),
            "candidate_regime_policy_equivalence_evidence_path": profile.get(
                "candidate_regime_policy_equivalence_evidence_path"
            ),
            "candidate_regime_policy_equivalence_evidence_status": profile.get(
                "candidate_regime_policy_equivalence_evidence_status"
            ),
            "candidate_regime_policy_limitation_reasons": list(
                profile.get("candidate_regime_policy_limitation_reasons") or []
            ),
            "candidate_regime_policy_next_action": _candidate_regime_policy_next_action(profile),
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
        promotion_calibration_hash = str(payload.get("execution_calibration_artifact_hash") or "").strip()
        lineage_calibration_hash = str(validated_lineage.get("execution_calibration_artifact_hash") or "").strip()
        if promotion_calibration_hash and promotion_calibration_hash != lineage_calibration_hash:
            raise ApprovedProfileError("lineage_execution_calibration_artifact_hash_mismatch")
    live_regime_policy = payload.get("live_regime_policy")
    if not isinstance(live_regime_policy, dict):
        raise ApprovedProfileError("promotion_regime_policy_missing")
    _verify_validation_run_binding_for_promotion(payload)
    return payload


def _verify_validation_run_binding_for_promotion(payload: dict[str, Any]) -> None:
    if not payload.get("validation_run_required"):
        return
    status = str(payload.get("validation_run_binding_status") or "").strip()
    if status not in {"verified", "verified_pre_promotion_binding"}:
        raise ApprovedProfileError("validation_run_not_verified")
    binding_hash = str(payload.get("validation_run_binding_hash") or "").strip()
    validation_hash = str(payload.get("validation_run_hash") or "").strip()
    if not binding_hash.startswith("sha256:") and not validation_hash.startswith("sha256:"):
        raise ApprovedProfileError("validation_run_hash_missing")
    path = str(payload.get("validation_run_path") or "").strip()
    if not path:
        raise ApprovedProfileError("validation_run_path_missing")
    validation_path = Path(path).expanduser()
    if not validation_path.is_absolute():
        raise ApprovedProfileError("validation_run_path_must_be_absolute")
    validation_run = _load_json(validation_path)
    from .research.validation_pipeline import (
        validate_promotion_validation_run,
        validation_run_content_hash,
        verify_validation_run_binding,
    )

    expected_content_hash = str(validation_run.get("content_hash") or "")
    if not expected_content_hash.startswith("sha256:"):
        raise ApprovedProfileError("validation_run_content_hash_missing")
    if validation_run_content_hash(validation_run) != expected_content_hash:
        raise ApprovedProfileError("validation_run_content_hash_mismatch")
    if validation_hash.startswith("sha256:") and validation_hash != expected_content_hash:
        raise ApprovedProfileError("validation_run_hash_mismatch")
    binding_reasons = verify_validation_run_binding(
        validation_run,
        expected_binding_hash=binding_hash or validation_run.get("validation_run_binding_hash"),
    )
    if binding_reasons:
        raise ApprovedProfileError(",".join(binding_reasons))
    _, reasons = validate_promotion_validation_run(
        validation_run_path=validation_path,
        experiment_id=str(payload.get("strategy_profile_source_experiment") or ""),
        manifest_hash=str(payload.get("manifest_hash") or ""),
        candidate_id=str(payload.get("candidate_id") or ""),
        backtest_report_hash=str(payload.get("backtest_report_hash") or ""),
        walk_forward_report_hash=(
            str(payload.get("walk_forward_report_hash") or "")
            if payload.get("walk_forward_required")
            else None
        ),
    )
    if reasons:
        raise ApprovedProfileError(",".join(reasons))
    promotion_hash = str(payload.get("content_hash") or "")
    validation_promotion_hash = str(validation_run.get("promotion_artifact_hash") or "")
    if (
        validation_promotion_hash.startswith("sha256:")
        and promotion_hash.startswith("sha256:")
        and validation_promotion_hash != promotion_hash
    ):
        raise ApprovedProfileError("validation_run_promotion_artifact_hash_mismatch")


def _candidate_like_from_promotion(payload: dict[str, Any]) -> dict[str, Any]:
    profile = payload.get("candidate_profile") if isinstance(payload.get("candidate_profile"), dict) else {}
    live_policy = payload.get("live_regime_policy") if isinstance(payload.get("live_regime_policy"), dict) else {}
    candidate_like = dict(profile)
    candidate_like.update(
        {
            "strategy_name": payload.get("strategy_name") or profile.get("strategy_name"),
            "parameter_candidate_id": payload.get("candidate_id") or profile.get("candidate_id"),
            "experiment_id": payload.get("strategy_profile_source_experiment") or profile.get("source_experiment"),
            "deployment_tier": profile.get("deployment_tier") or payload.get("deployment_tier"),
            "manifest_hash": payload.get("manifest_hash") or profile.get("manifest_hash"),
            "dataset_snapshot_id": payload.get("dataset_snapshot_id") or profile.get("dataset_snapshot_id"),
            "dataset_content_hash": payload.get("dataset_content_hash") or profile.get("dataset_content_hash"),
            "regime_classifier_version": payload.get("regime_classifier_version")
            or profile.get("regime_classifier_version")
            or live_policy.get("regime_classifier_version"),
            "allowed_live_regimes": payload.get("allowed_regimes")
            or profile.get("allowed_live_regimes")
            or live_policy.get("allowed_regimes"),
            "blocked_live_regimes": payload.get("blocked_regimes")
            or profile.get("blocked_live_regimes")
            or live_policy.get("blocked_regimes"),
        }
    )
    return candidate_like


def _strategy_parameters_from_promotion(payload: dict[str, Any]) -> dict[str, object]:
    profile = payload.get("candidate_profile") if isinstance(payload.get("candidate_profile"), dict) else {}
    parameters = profile.get("effective_strategy_parameters")
    if parameters is None:
        parameters = profile.get("parameter_values")
    if not isinstance(parameters, dict):
        raise ApprovedProfileError("promotion_parameter_values_missing")
    strategy_name = str(payload.get("strategy_name") or profile.get("strategy_name") or "sma_with_filter")
    return _runtime_bound_strategy_parameters(strategy_name, dict(parameters))


def _runtime_bound_strategy_parameters(strategy_name: str, parameters: dict[str, Any]) -> dict[str, Any]:
    spec = strategy_spec_for_name(strategy_name)
    research_only = set(spec.research_only_parameter_names)
    return {key: value for key, value in parameters.items() if key not in research_only}


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
    promotion_profile = (
        verified_promotion.get("candidate_profile")
        if isinstance(verified_promotion.get("candidate_profile"), dict)
        else {}
    )
    promotion_source = {**promotion_profile, **verified_promotion}
    promotion_execution_contract = promotion_source.get("execution_reality_contract")
    promotion_capability_contract = promotion_source.get("execution_capability_contract")
    if not isinstance(promotion_capability_contract, dict) and isinstance(promotion_execution_contract, dict):
        promotion_capability_contract = promotion_execution_contract.get("execution_capability_contract")
    production_policy = validate_production_calibration_policy(
        promotion_source,
        target=deployment_tier_for_profile_mode(normalized_mode),
    )
    if production_policy.status != "PASS":
        raise ApprovedProfileError(
            "production_calibration_policy_failed:" + ",".join(production_policy.reasons)
        )
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
        "deployment_tier": production_policy.target,
        "production_calibration_policy_result": production_policy.as_dict(),
        "production_calibration_policy_reasons": list(production_policy.reasons),
        "execution_calibration_policy_source": production_policy.policy_source,
        "execution_model": promotion_source.get("execution_model"),
        "execution_model_source": promotion_source.get("execution_model_source"),
        "execution_reality_contract": promotion_execution_contract,
        "execution_contract_hash": promotion_source.get("execution_contract_hash"),
        "execution_capability_contract": promotion_capability_contract,
        "execution_capability_contract_hash": (
            promotion_source.get("execution_capability_contract_hash")
            or (
                promotion_capability_contract.get("execution_capability_contract_hash")
                if isinstance(promotion_capability_contract, dict)
                else None
            )
        ),
        "execution_calibration_required": promotion_source.get("execution_calibration_required"),
        "execution_calibration_strictness": promotion_source.get("execution_calibration_strictness"),
        "execution_calibration_gate": promotion_source.get("execution_calibration_gate"),
        "execution_calibration_artifact_hash": production_policy.artifact_hash,
        "execution_calibration_artifact_hashes": list(production_policy.artifact_hashes),
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
        "hypothesis_identity_source": verified_promotion.get("hypothesis_identity_source"),
        "experiment_family_identity_source": verified_promotion.get("experiment_family_identity_source"),
        "search_budget": verified_promotion.get("search_budget"),
        "parameter_space_hash": verified_promotion.get("parameter_space_hash"),
        "parameter_grid_size": verified_promotion.get("parameter_grid_size"),
        "attempt_index": verified_promotion.get("attempt_index"),
        "failed_candidate_count": verified_promotion.get("failed_candidate_count"),
        "holdout_reuse_count": verified_promotion.get("holdout_reuse_count"),
        "experiment_registry_path": verified_promotion.get("experiment_registry_path"),
        "experiment_registry_prior_hash": verified_promotion.get("experiment_registry_prior_hash"),
        "experiment_registry_row_hash": verified_promotion.get("experiment_registry_row_hash"),
        "experiment_registry_completion_row_hash": verified_promotion.get("experiment_registry_completion_row_hash"),
        "experiment_registry_bound_evidence_hash": verified_promotion.get("experiment_registry_bound_evidence_hash"),
        "experiment_registry_evidence_hash_phase": verified_promotion.get("experiment_registry_evidence_hash_phase"),
        "final_holdout_fingerprint": verified_promotion.get("final_holdout_fingerprint"),
        "final_holdout_identity_hash": verified_promotion.get("final_holdout_identity_hash"),
        "final_holdout_content_hash": verified_promotion.get("final_holdout_content_hash"),
        "final_holdout_reuse_key_hash": verified_promotion.get("final_holdout_reuse_key_hash"),
        "final_holdout_split_hash": verified_promotion.get("final_holdout_split_hash"),
        "computed_attempt_index": verified_promotion.get("computed_attempt_index"),
        "computed_holdout_reuse_count": verified_promotion.get("computed_holdout_reuse_count"),
        "declared_attempt_index": verified_promotion.get("declared_attempt_index"),
        "declared_holdout_reuse_count": verified_promotion.get("declared_holdout_reuse_count"),
        "research_freedom_hash": verified_promotion.get("research_freedom_hash"),
        "dataset_reuse_policy": verified_promotion.get("dataset_reuse_policy"),
        "backtest_report_hash": verified_promotion.get("backtest_report_hash"),
        "walk_forward_report_hash": verified_promotion.get("walk_forward_report_hash"),
        "validation_run_required": bool(verified_promotion.get("validation_run_required")),
        "validation_run_binding_status": verified_promotion.get("validation_run_binding_status"),
        "validation_run_path": verified_promotion.get("validation_run_path"),
        "validation_run_hash": verified_promotion.get("validation_run_hash"),
        "validation_run_binding_hash": verified_promotion.get("validation_run_binding_hash"),
        "repository_version": repository_version or verified_promotion.get("repository_version") or "unknown",
        "strategy_name": verified_promotion.get("strategy_name"),
        "strategy_spec": promotion_source.get("strategy_spec"),
        "strategy_spec_hash": promotion_source.get("strategy_spec_hash"),
        "strategy_plugin_contract": promotion_source.get("strategy_plugin_contract"),
        "strategy_plugin_contract_hash": promotion_source.get("strategy_plugin_contract_hash"),
        "exit_policy": promotion_source.get("exit_policy"),
        "exit_policy_hash": promotion_source.get("exit_policy_hash"),
        "behavior_hash": promotion_source.get("behavior_hash"),
        "validation_behavior_hash": promotion_source.get("validation_behavior_hash"),
        "market": str(market),
        "interval": str(interval),
        "strategy_parameters": _strategy_parameters_from_promotion(verified_promotion),
        "parameter_values_raw": (
            dict(promotion_source.get("parameter_values_raw"))
            if isinstance(promotion_source.get("parameter_values_raw"), dict)
            else dict(promotion_source.get("parameter_values") or {})
        ),
        "effective_strategy_parameters": _strategy_parameters_from_promotion(verified_promotion),
        "effective_strategy_parameters_hash": materialized_strategy_parameters_hash(
            _strategy_parameters_from_promotion(verified_promotion)
        ),
        "strategy_parameter_source_map": _runtime_bound_strategy_parameters(
            str(verified_promotion.get("strategy_name") or "sma_with_filter"),
            (
                dict(promotion_source.get("strategy_parameter_source_map"))
                if isinstance(promotion_source.get("strategy_parameter_source_map"), dict)
                else strategy_parameter_source_map(
                    str(verified_promotion.get("strategy_name") or "sma_with_filter"),
                    dict(
                        promotion_source.get("parameter_values_raw")
                        or promotion_source.get("parameter_values")
                        or {}
                    ),
                    fee_rate=(_cost_model_from_promotion(verified_promotion)).get("fee_rate"),
                    slippage_bps=(_cost_model_from_promotion(verified_promotion)).get("slippage_bps"),
                )
            ),
        ),
        "candidate_regime_policy_applied_in_research": bool(
            promotion_source.get("candidate_regime_policy_applied_in_research")
        ),
        "candidate_regime_policy_required_for_live": bool(
            promotion_source.get("candidate_regime_policy_required_for_live")
        ),
        "candidate_regime_policy_equivalence_required": bool(
            promotion_source.get("candidate_regime_policy_equivalence_required")
        ),
        "candidate_regime_policy_equivalence_evidence_hash": promotion_source.get(
            "candidate_regime_policy_equivalence_evidence_hash"
        ),
        "candidate_regime_policy_equivalence_evidence_path": promotion_source.get(
            "candidate_regime_policy_equivalence_evidence_path"
        ),
        "candidate_regime_policy_equivalence_evidence_status": promotion_source.get(
            "candidate_regime_policy_equivalence_evidence_status"
        ),
        "candidate_profile_evidence_contract_hash": promotion_source.get(
            "candidate_profile_evidence_contract_hash"
        ),
        "candidate_regime_policy_limitation_reasons": list(
            promotion_source.get("candidate_regime_policy_limitation_reasons") or []
        ),
        "cost_model": _cost_model_from_promotion(verified_promotion),
        "base_cost_assumption": promotion_source.get("base_cost_assumption"),
        "cost_assumption_contract": promotion_source.get("cost_assumption_contract"),
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
    if not isinstance(profile.get("effective_strategy_parameters"), dict):
        raise ApprovedProfileError("effective_strategy_parameters_missing")
    expected = profile.get(PROFILE_HASH_FIELD)
    if (
        isinstance(expected, str)
        and expected.startswith("sha256:")
        and profile.get("strategy_parameters") != profile.get("effective_strategy_parameters")
        and compute_approved_profile_hash(profile) != expected
    ):
        raise ApprovedProfileError("profile_content_hash_mismatch")
    if profile.get("strategy_parameters") != profile.get("effective_strategy_parameters"):
        raise ApprovedProfileError("strategy_parameters_not_effective")
    if not str(profile.get("effective_strategy_parameters_hash") or "").startswith("sha256:"):
        raise ApprovedProfileError("effective_strategy_parameters_hash_missing")
    if materialized_strategy_parameters_hash(profile["effective_strategy_parameters"]) != profile.get(
        "effective_strategy_parameters_hash"
    ):
        raise ApprovedProfileError("effective_strategy_parameters_hash_mismatch")
    if not isinstance(profile.get("strategy_parameter_source_map"), dict):
        raise ApprovedProfileError("strategy_parameter_source_map_missing")
    _validate_strategy_plugin_contract(profile)
    if not isinstance(profile.get("exit_policy"), dict):
        raise ApprovedProfileError("exit_policy_missing")
    if not str(profile.get("exit_policy_hash") or "").startswith("sha256:"):
        raise ApprovedProfileError("exit_policy_hash_missing")
    if sha256_prefixed(profile.get("exit_policy")) != profile.get("exit_policy_hash"):
        raise ApprovedProfileError("exit_policy_hash_mismatch")
    if not isinstance(profile.get("cost_model"), dict):
        raise ApprovedProfileError("cost_model_missing")
    if profile.get("base_cost_assumption") is not None and not isinstance(profile.get("base_cost_assumption"), dict):
        raise ApprovedProfileError("base_cost_assumption_invalid")
    if mode in LIVE_COMPATIBLE_PROFILE_MODES or mode == "paper":
        policy = validate_production_calibration_policy(
            profile,
            target=profile.get("deployment_tier") or deployment_tier_for_profile_mode(mode),
        )
        if policy.status != "PASS":
            raise ApprovedProfileError(
                "production_calibration_policy_failed:" + ",".join(policy.reasons)
            )
    if profile.get("validation_run_required"):
        status = str(profile.get("validation_run_binding_status") or "").strip()
        if status not in {"verified", "verified_pre_promotion_binding"}:
            raise ApprovedProfileError("validation_run_not_verified")
        if not any(
            str(profile.get(key) or "").startswith("sha256:")
            for key in ("validation_run_hash", "validation_run_binding_hash")
        ):
            raise ApprovedProfileError("validation_run_hash_missing")
    contract = profile.get("execution_reality_contract")
    if not isinstance(contract, dict):
        raise ApprovedProfileError("execution_reality_contract_missing")
    if not contract_hash_matches(contract, profile.get("execution_contract_hash")):
        raise ApprovedProfileError("execution_contract_hash_mismatch")
    unsupported = unsupported_capability_reasons(contract)
    if unsupported:
        raise ApprovedProfileError("execution_contract_unsupported_capability:" + ",".join(unsupported))
    capability = profile.get("execution_capability_contract") or contract.get("execution_capability_contract")
    if not isinstance(capability, dict):
        raise ApprovedProfileError("execution_capability_contract_missing")
    capability_hash = profile.get("execution_capability_contract_hash") or capability.get("execution_capability_contract_hash")
    if not capability_contract_hash_matches(capability, capability_hash):
        raise ApprovedProfileError("execution_capability_contract_hash_mismatch")
    capability_reasons = validate_execution_capability_contract(capability)
    if capability_reasons:
        raise ApprovedProfileError("execution_capability_contract_unsupported:" + ",".join(capability_reasons))
    if capability.get("unavailable_required_capabilities"):
        raise ApprovedProfileError("execution_capability_required_unavailable")
    regime_policy = profile.get("regime_policy")
    if not isinstance(regime_policy, dict):
        raise ApprovedProfileError("regime_policy_missing")
    if not str(regime_policy.get("regime_classifier_version") or "").strip():
        raise ApprovedProfileError("regime_policy_missing_classifier_version")
    if not isinstance(regime_policy.get("allowed_regimes"), list):
        raise ApprovedProfileError("regime_policy_missing_allowed_regimes")
    if not isinstance(regime_policy.get("blocked_regimes"), list):
        raise ApprovedProfileError("regime_policy_missing_blocked_regimes")
    if mode in LIVE_COMPATIBLE_PROFILE_MODES and bool(profile.get("candidate_regime_policy_required_for_live")):
        if not bool(profile.get("candidate_regime_policy_applied_in_research")):
            _validate_profile_candidate_regime_policy_evidence(profile)
    expected = profile.get(PROFILE_HASH_FIELD)
    if not isinstance(expected, str) or not expected.startswith("sha256:"):
        raise ApprovedProfileError("profile_content_hash_missing")
    actual = compute_approved_profile_hash(profile)
    if actual != expected:
        raise ApprovedProfileError("profile_content_hash_mismatch")
    return profile


def _candidate_regime_policy_next_action(profile: dict[str, Any]) -> str:
    if not bool(profile.get("candidate_regime_policy_required_for_live")):
        return "none"
    if bool(profile.get("candidate_regime_policy_applied_in_research")):
        return "none"
    if str(profile.get("candidate_regime_policy_equivalence_evidence_hash") or "").startswith("sha256:"):
        return "verify_candidate_regime_policy_equivalence_evidence"
    return "generate_and_bind_candidate_regime_policy_equivalence_evidence"


def _validate_strategy_plugin_contract(profile: dict[str, Any]) -> None:
    mode = str(profile.get("profile_mode") or "").strip().lower()
    contract = profile.get("strategy_plugin_contract")
    contract_hash = str(profile.get("strategy_plugin_contract_hash") or "").strip()
    legacy_allowed = mode == "paper" and bool(profile.get("legacy_compatibility_used"))
    if contract is None and not contract_hash and legacy_allowed:
        return
    if not isinstance(contract, dict):
        raise ApprovedProfileError("strategy_plugin_contract_missing")
    if not contract_hash.startswith("sha256:"):
        raise ApprovedProfileError("strategy_plugin_contract_hash_missing")
    if sha256_prefixed(contract) != contract_hash:
        raise ApprovedProfileError("strategy_plugin_contract_hash_mismatch")
    strategy_name = str(profile.get("strategy_name") or "").strip()
    if not strategy_name:
        raise ApprovedProfileError("strategy_name_missing")
    try:
        plugin = resolve_research_strategy_plugin(strategy_name)
    except ResearchStrategyRegistryError as exc:
        raise ApprovedProfileError(f"strategy_plugin_unsupported:{strategy_name}") from exc
    if plugin.contract_hash() != contract_hash:
        raise ApprovedProfileError("strategy_plugin_contract_hash_registry_mismatch")


def _validate_profile_candidate_regime_policy_evidence(profile: dict[str, Any]) -> None:
    expected_hash = str(profile.get("candidate_regime_policy_equivalence_evidence_hash") or "").strip()
    if not expected_hash.startswith("sha256:"):
        raise ApprovedProfileError("candidate_regime_policy_equivalence_evidence_missing")
    path = str(profile.get("candidate_regime_policy_equivalence_evidence_path") or "").strip()
    if not path:
        raise ApprovedProfileError("candidate_regime_policy_equivalence_evidence_path_missing")
    try:
        resolved = resolve_runtime_artifact_path(path, label="candidate_regime_policy_equivalence_evidence")
        payload = _load_json(resolved)
        validate_candidate_regime_policy_equivalence_evidence(
            payload,
            candidate_or_profile=profile,
            expected_hash=expected_hash,
            evidence_path=resolved,
        )
    except ApprovedProfileError:
        raise
    except (OSError, ValueError, EvidenceValidationError) as exc:
        raise ApprovedProfileError(f"candidate_regime_policy_equivalence_evidence_invalid:{exc}") from exc


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
    promotion_profile = promotion.get("candidate_profile") if isinstance(promotion.get("candidate_profile"), dict) else {}
    for key in ("candidate_profile_hash", "manifest_hash", "dataset_content_hash", "strategy_name"):
        if not _values_equal(validated.get(key), promotion.get(key)):
            raise ApprovedProfileError(f"source_promotion_{key}_mismatch")
    profile_plugin_hash = str(validated.get("strategy_plugin_contract_hash") or "").strip()
    promotion_plugin_hash = str(promotion.get("strategy_plugin_contract_hash") or "").strip()
    if not promotion_plugin_hash and isinstance(promotion_profile, dict):
        promotion_plugin_hash = str(promotion_profile.get("strategy_plugin_contract_hash") or "").strip()
    if not promotion_plugin_hash:
        raise ApprovedProfileError("source_promotion_strategy_plugin_contract_hash_missing")
    if profile_plugin_hash != promotion_plugin_hash:
        raise ApprovedProfileError("source_promotion_strategy_plugin_contract_hash_mismatch")
    profile_contract = validated.get("execution_reality_contract")
    promotion_contract = promotion.get("execution_reality_contract") or promotion_profile.get("execution_reality_contract")
    contract_mismatches = execution_contract_mismatch_reasons(
        expected=promotion_contract if isinstance(promotion_contract, dict) else None,
        observed=profile_contract if isinstance(profile_contract, dict) else None,
    )
    if contract_mismatches:
        raise ApprovedProfileError("source_promotion_execution_contract_mismatch")
    profile_capability = validated.get("execution_capability_contract")
    if not isinstance(profile_capability, dict) and isinstance(profile_contract, dict):
        profile_capability = profile_contract.get("execution_capability_contract")
    promotion_capability = promotion.get("execution_capability_contract") or promotion_profile.get("execution_capability_contract")
    if not isinstance(promotion_capability, dict) and isinstance(promotion_contract, dict):
        promotion_capability = promotion_contract.get("execution_capability_contract")
    capability_mismatches = execution_capability_contract_mismatch_reasons(
        expected=promotion_capability if isinstance(promotion_capability, dict) else None,
        observed=profile_capability if isinstance(profile_capability, dict) else None,
    )
    if capability_mismatches:
        raise ApprovedProfileError("source_promotion_execution_capability_contract_mismatch")
    if promotion.get("lineage_required"):
        if not str(validated.get("lineage_hash") or "").strip():
            raise ApprovedProfileError("lineage_hash_missing")
        if validated.get("lineage_hash") != promotion.get("lineage_hash"):
            raise ApprovedProfileError("source_promotion_lineage_hash_mismatch")
        profile_calibration_hash = str(validated.get("execution_calibration_artifact_hash") or "").strip()
        promotion_calibration_hash = str(promotion.get("execution_calibration_artifact_hash") or "").strip()
        if profile_calibration_hash and profile_calibration_hash != promotion_calibration_hash:
            raise ApprovedProfileError("source_promotion_execution_calibration_artifact_hash_mismatch")
    if promotion.get("validation_run_required"):
        for key in ("validation_run_binding_status", "validation_run_path", "validation_run_hash", "validation_run_binding_hash"):
            if not _values_equal(validated.get(key), promotion.get(key)):
                raise ApprovedProfileError(f"source_promotion_{key}_mismatch")
    for key in (
        "candidate_regime_policy_equivalence_evidence_hash",
        "candidate_regime_policy_equivalence_evidence_path",
        "candidate_profile_evidence_contract_hash",
    ):
        promotion_value = promotion.get(key)
        if promotion_value is None and isinstance(promotion_profile, dict):
            promotion_value = promotion_profile.get(key)
        if promotion_value is not None and not _values_equal(validated.get(key), promotion_value):
            raise ApprovedProfileError(f"source_promotion_{key}_mismatch")
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
                "candidate_regime_policy_applied_in_research": profile.get(
                    "candidate_regime_policy_applied_in_research"
                ),
                "candidate_regime_policy_required_for_live": profile.get(
                    "candidate_regime_policy_required_for_live"
                ),
                "candidate_regime_policy_equivalence_required": profile.get(
                    "candidate_regime_policy_equivalence_required"
                ),
                "candidate_regime_policy_equivalence_evidence_hash": profile.get(
                    "candidate_regime_policy_equivalence_evidence_hash"
                ),
                "candidate_regime_policy_equivalence_evidence_path": profile.get(
                    "candidate_regime_policy_equivalence_evidence_path"
                ),
                "candidate_regime_policy_equivalence_evidence_status": profile.get(
                    "candidate_regime_policy_equivalence_evidence_status"
                ),
                "candidate_regime_policy_limitation_reasons": list(
                    profile.get("candidate_regime_policy_limitation_reasons") or []
                ),
                "candidate_regime_policy_next_action": _candidate_regime_policy_next_action(profile),
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

    raw_strategy_name = _value("STRATEGY_NAME")
    mode = _value("MODE", default="paper")
    live_dry_run = _value("LIVE_DRY_RUN", default="true")
    live_real_order_armed = _value("LIVE_REAL_ORDER_ARMED", default="false")
    if not raw_strategy_name and _live_like_runtime_requires_explicit_strategy(
        mode=mode,
        live_dry_run=live_dry_run,
        live_real_order_armed=live_real_order_armed,
    ):
        raise ApprovedProfileError("runtime_strategy_name_required_for_live_like_mode")
    # Backward-compatibility default for existing paper/runtime env files.
    # Unsupported or non-runtime-capable explicit strategy names still fail closed below.
    strategy_name = raw_strategy_name or "sma_with_filter"
    strategy_name_default_source = (
        "explicit_env" if raw_strategy_name else "backward_compatibility_sma_default"
    )
    _require_runtime_replay_supported_strategy(strategy_name)
    strategy_parameters = runtime_strategy_parameters_from_env(strategy_name, env)
    runtime = {
        "mode": mode,
        "live_dry_run": live_dry_run,
        "live_real_order_armed": live_real_order_armed,
        "profile_selector": _value(APPROVED_PROFILE_SELECTOR_ENV, "STRATEGY_APPROVED_PROFILE_PATH"),
        "strategy_name": strategy_name,
        "strategy_name_default_source": strategy_name_default_source,
        "market": _value("MARKET", "PAIR", default="KRW-BTC"),
        "interval": _value("INTERVAL", default="1m"),
        "strategy_parameters": strategy_parameters,
        "cost_model": {
            "fee_rate": _value(
                "LIVE_FEE_RATE_ESTIMATE",
                "PAPER_FEE_RATE",
                "PAPER_FEE_RATE_ESTIMATE",
                "FEE_RATE",
                default="0.0004",
            ),
            "slippage_bps": _value(
                "STRATEGY_ENTRY_SLIPPAGE_BPS",
                "MAX_MARKET_SLIPPAGE_BPS",
                "SLIPPAGE_BPS",
                default="0",
            ),
        },
    }
    runtime["exit_policy"] = exit_policy_from_parameters(strategy_name, strategy_parameters)
    runtime["exit_policy_hash"] = sha256_prefixed(runtime["exit_policy"])
    execution_contract = _execution_contract_from_env_values(env)
    if execution_contract is not None:
        runtime["execution_reality_contract"] = execution_contract
        runtime["execution_contract_hash"] = execution_contract["execution_contract_hash"]
        runtime["execution_capability_contract"] = execution_contract.get("execution_capability_contract")
        runtime["execution_capability_contract_hash"] = execution_contract.get("execution_capability_contract_hash")
    return runtime


def runtime_contract_from_settings(cfg: object) -> dict[str, Any]:
    profile_selector = (
        str(getattr(cfg, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
        or str(getattr(cfg, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
    )
    mode = str(getattr(cfg, "MODE", ""))
    live_dry_run = bool(getattr(cfg, "LIVE_DRY_RUN", True))
    live_real_order_armed = bool(getattr(cfg, "LIVE_REAL_ORDER_ARMED", False))
    raw_strategy_name = str(getattr(cfg, "STRATEGY_NAME", "") or "").strip()
    if not raw_strategy_name and _live_like_runtime_requires_explicit_strategy(
        mode=mode,
        live_dry_run=live_dry_run,
        live_real_order_armed=live_real_order_armed,
    ):
        raise ApprovedProfileError("runtime_strategy_name_required_for_live_like_mode")
    # Backward-compatibility default for existing paper/runtime settings objects.
    # Unsupported or non-runtime-capable explicit strategy names still fail closed below.
    strategy_name = raw_strategy_name or "sma_with_filter"
    strategy_name_default_source = (
        "explicit_settings" if raw_strategy_name else "backward_compatibility_sma_default"
    )
    _require_runtime_replay_supported_strategy(strategy_name)
    strategy_parameters = runtime_strategy_parameters_from_settings(strategy_name, cfg)
    runtime = {
        "mode": mode,
        "live_dry_run": live_dry_run,
        "live_real_order_armed": live_real_order_armed,
        "profile_selector": profile_selector,
        "strategy_name": strategy_name,
        "strategy_name_default_source": strategy_name_default_source,
        "market": str(getattr(cfg, "PAIR", "")),
        "interval": str(getattr(cfg, "INTERVAL", "")),
        "strategy_parameters": strategy_parameters,
        "cost_model": {
            "fee_rate": float(getattr(cfg, "LIVE_FEE_RATE_ESTIMATE")),
            "slippage_bps": float(getattr(cfg, "STRATEGY_ENTRY_SLIPPAGE_BPS")),
        },
    }
    runtime["exit_policy"] = exit_policy_from_parameters(strategy_name, runtime["strategy_parameters"])
    runtime["exit_policy_hash"] = sha256_prefixed(runtime["exit_policy"])
    execution_contract = _execution_contract_from_settings(cfg)
    if execution_contract is not None:
        runtime["execution_reality_contract"] = execution_contract
        runtime["execution_contract_hash"] = execution_contract["execution_contract_hash"]
        runtime["execution_capability_contract"] = execution_contract.get("execution_capability_contract")
        runtime["execution_capability_contract_hash"] = execution_contract.get("execution_capability_contract_hash")
    return runtime


def _require_runtime_replay_supported_strategy(strategy_name: str) -> None:
    try:
        plugin = resolve_research_strategy_plugin(str(strategy_name or ""))
    except ResearchStrategyRegistryError as exc:
        raise ApprovedProfileError(f"runtime_strategy_unsupported:{strategy_name}") from exc
    if plugin.runtime_replay_builder is None:
        raise ApprovedProfileError(f"runtime_replay_unsupported_for_strategy:{plugin.name}")


def _live_like_runtime_requires_explicit_strategy(
    *,
    mode: object,
    live_dry_run: object,
    live_real_order_armed: object,
) -> bool:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode != "live":
        return False
    return _bool_value(live_dry_run) or _bool_value(live_real_order_armed)


def _execution_contract_from_env_values(env: dict[str, str]) -> dict[str, Any] | None:
    if not any(str(env.get(key, "")).strip() for key in EXECUTION_CONTRACT_ENV_KEYS):
        return None

    def _value(key: str, default: str | None = None) -> str | None:
        raw = str(env.get(key, "")).strip()
        return raw if raw else default

    fill_reference_policy = _value("EXECUTION_FILL_REFERENCE_POLICY")
    if not fill_reference_policy:
        return None
    top_of_book_required = _bool_value(_value("EXECUTION_TOP_OF_BOOK_REQUIRED", "false")) or fill_reference_policy in {
        "first_orderbook_after_decision",
        "latency_adjusted_orderbook",
    }
    return build_execution_reality_contract(
        fill_reference_policy=fill_reference_policy,
        decision_guard_ms=_int_env(_value("EXECUTION_DECISION_GUARD_MS", "0")),
        max_quote_wait_ms=_int_env(_value("EXECUTION_MAX_QUOTE_WAIT_MS", "0")),
        missing_quote_policy=_value("EXECUTION_MISSING_QUOTE_POLICY", "warn") or "warn",
        min_execution_reality_level_for_promotion=_value("EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION"),
        allow_same_candle_close_fill=_bool_value(_value("EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL", "false")),
        quote_source=_value("EXECUTION_QUOTE_SOURCE"),
        quote_age_limit_ms=_optional_int_env(_value("EXECUTION_QUOTE_AGE_LIMIT_MS")),
        top_of_book_required=top_of_book_required,
        top_of_book_available=top_of_book_required,
        top_of_book_is_full_depth=_bool_value(_value("EXECUTION_TOP_OF_BOOK_IS_FULL_DEPTH", "false")),
        depth_required=_bool_value(_value("EXECUTION_DEPTH_REQUIRED", "false")),
        trade_tick_required=_bool_value(_value("EXECUTION_TRADE_TICK_REQUIRED", "false")),
        queue_position_required=_bool_value(_value("EXECUTION_QUEUE_POSITION_REQUIRED", "false")),
        market_impact_required=_bool_value(_value("EXECUTION_MARKET_IMPACT_REQUIRED", "false")),
        intra_candle_path_available=_bool_value(_value("EXECUTION_INTRA_CANDLE_PATH_AVAILABLE", "false")),
        latency_model={
            "type": _value("EXECUTION_LATENCY_MODEL_TYPE", "fixed_bps"),
            "latency_ms": _int_env(_value("EXECUTION_LATENCY_MS", "0")),
        },
        partial_fill_model={
            "type": _value("EXECUTION_PARTIAL_FILL_MODEL_TYPE", _value("EXECUTION_LATENCY_MODEL_TYPE", "fixed_bps")),
            "partial_fill_rate": _float_env(_value("EXECUTION_PARTIAL_FILL_RATE", "0")),
        },
        order_failure_model={
            "type": _value("EXECUTION_ORDER_FAILURE_MODEL_TYPE", _value("EXECUTION_LATENCY_MODEL_TYPE", "fixed_bps")),
            "order_failure_rate": _float_env(_value("EXECUTION_ORDER_FAILURE_RATE", "0")),
        },
        fee_source=_value("EXECUTION_FEE_SOURCE"),
        slippage_source=_value("EXECUTION_SLIPPAGE_SOURCE"),
        calibration_required=_bool_value(_value("EXECUTION_CALIBRATION_REQUIRED", "false")),
        calibration_artifact_hash=_value("EXECUTION_CALIBRATION_ARTIFACT_HASH"),
        execution_reality_level=_value("EXECUTION_REALITY_LEVEL"),
    )


def _execution_contract_from_settings(cfg: object) -> dict[str, Any] | None:
    fill_reference_policy = str(getattr(cfg, "EXECUTION_FILL_REFERENCE_POLICY", "") or "").strip()
    if not fill_reference_policy:
        return None
    top_of_book_required = bool(getattr(cfg, "EXECUTION_TOP_OF_BOOK_REQUIRED", False)) or fill_reference_policy in {
        "first_orderbook_after_decision",
        "latency_adjusted_orderbook",
    }
    return build_execution_reality_contract(
        fill_reference_policy=fill_reference_policy,
        decision_guard_ms=int(getattr(cfg, "EXECUTION_DECISION_GUARD_MS", 0)),
        max_quote_wait_ms=int(getattr(cfg, "EXECUTION_MAX_QUOTE_WAIT_MS", 0)),
        missing_quote_policy=str(getattr(cfg, "EXECUTION_MISSING_QUOTE_POLICY", "warn")),
        min_execution_reality_level_for_promotion=_optional_settings_str(
            cfg, "EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION"
        ),
        allow_same_candle_close_fill=bool(getattr(cfg, "EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL", False)),
        quote_source=_optional_settings_str(cfg, "EXECUTION_QUOTE_SOURCE"),
        quote_age_limit_ms=getattr(cfg, "EXECUTION_QUOTE_AGE_LIMIT_MS", None),
        top_of_book_required=top_of_book_required,
        top_of_book_available=top_of_book_required,
        top_of_book_is_full_depth=bool(getattr(cfg, "EXECUTION_TOP_OF_BOOK_IS_FULL_DEPTH", False)),
        depth_required=bool(getattr(cfg, "EXECUTION_DEPTH_REQUIRED", False)),
        trade_tick_required=bool(getattr(cfg, "EXECUTION_TRADE_TICK_REQUIRED", False)),
        queue_position_required=bool(getattr(cfg, "EXECUTION_QUEUE_POSITION_REQUIRED", False)),
        market_impact_required=bool(getattr(cfg, "EXECUTION_MARKET_IMPACT_REQUIRED", False)),
        intra_candle_path_available=bool(getattr(cfg, "EXECUTION_INTRA_CANDLE_PATH_AVAILABLE", False)),
        latency_model={
            "type": str(getattr(cfg, "EXECUTION_LATENCY_MODEL_TYPE", "fixed_bps")),
            "latency_ms": int(getattr(cfg, "EXECUTION_LATENCY_MS", 0)),
        },
        partial_fill_model={
            "type": str(getattr(cfg, "EXECUTION_PARTIAL_FILL_MODEL_TYPE", getattr(cfg, "EXECUTION_LATENCY_MODEL_TYPE", "fixed_bps"))),
            "partial_fill_rate": float(getattr(cfg, "EXECUTION_PARTIAL_FILL_RATE", 0.0)),
        },
        order_failure_model={
            "type": str(getattr(cfg, "EXECUTION_ORDER_FAILURE_MODEL_TYPE", getattr(cfg, "EXECUTION_LATENCY_MODEL_TYPE", "fixed_bps"))),
            "order_failure_rate": float(getattr(cfg, "EXECUTION_ORDER_FAILURE_RATE", 0.0)),
        },
        fee_source=_optional_settings_str(cfg, "EXECUTION_FEE_SOURCE"),
        slippage_source=_optional_settings_str(cfg, "EXECUTION_SLIPPAGE_SOURCE"),
        calibration_required=bool(getattr(cfg, "EXECUTION_CALIBRATION_REQUIRED", False)),
        calibration_artifact_hash=_optional_settings_str(cfg, "EXECUTION_CALIBRATION_ARTIFACT_HASH"),
        execution_reality_level=_optional_settings_str(cfg, "EXECUTION_REALITY_LEVEL"),
    )


def _optional_settings_str(cfg: object, name: str) -> str | None:
    value = str(getattr(cfg, name, "") or "").strip()
    return value or None


def _int_env(value: object) -> int:
    return int(str(value or "0").strip())


def _optional_int_env(value: object) -> int | None:
    raw = str(value or "").strip()
    return int(raw) if raw else None


def _float_env(value: object) -> float:
    return float(str(value or "0").strip())


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
    _compare_behavior_parameter_coverage(mismatches, profile, runtime, profile_params, runtime_params)
    for key, expected in profile_params.items():
        if key not in runtime_params:
            mismatches.append({"field": f"strategy_parameters.{key}", "expected": expected, "actual": None})
            continue
        if not _values_equal(expected, runtime_params[key]):
            mismatches.append(
                {"field": f"strategy_parameters.{key}", "expected": expected, "actual": runtime_params[key]}
            )
    runtime_exit_policy = runtime.get("exit_policy")
    runtime_exit_policy_hash = runtime.get("exit_policy_hash")
    if isinstance(runtime_exit_policy, dict):
        runtime_exit_policy_hash = sha256_prefixed(runtime_exit_policy)
    if profile.get("exit_policy_hash") != runtime_exit_policy_hash:
        mismatches.append(
            {
                "field": "exit_policy_hash",
                "expected": profile.get("exit_policy_hash"),
                "actual": runtime_exit_policy_hash,
                "reason": "runtime_exit_policy_mismatch",
            }
        )
    profile_cost = profile.get("cost_model") if isinstance(profile.get("cost_model"), dict) else {}
    runtime_cost = runtime.get("cost_model") if isinstance(runtime.get("cost_model"), dict) else {}
    for key, expected in profile_cost.items():
        if key not in runtime_cost:
            mismatches.append({"field": f"cost_model.{key}", "expected": expected, "actual": None})
            continue
        if not _values_equal(expected, runtime_cost[key]):
            mismatches.append({"field": f"cost_model.{key}", "expected": expected, "actual": runtime_cost[key]})
    cost_status = profile_runtime_cost_match_status(profile, runtime)
    if cost_status["status"] == "FAIL":
        mismatches.append(
            {
                "field": "runtime_profile_cost_mismatch",
                "expected": cost_status.get("expected"),
                "actual": cost_status.get("actual"),
                "reason": cost_status.get("reason"),
                "operator_next_step": PROFILE_RUNTIME_COST_MISMATCH_ACTION,
            }
        )
    runtime_contract = runtime.get("execution_reality_contract")
    if isinstance(runtime_contract, dict):
        for mismatch in execution_contract_mismatch_reasons(
            expected=profile.get("execution_reality_contract"),
            observed=runtime_contract,
        ):
            mismatches.append(mismatch)
    elif str(profile.get("profile_mode") or "").strip().lower() in APPROVED_PROFILE_MODES:
        mismatches.append(
            {
                "field": "execution_reality_contract",
                "expected": profile.get("execution_contract_hash"),
                "actual": None,
                "reason": "runtime_execution_contract_missing",
            }
        )
    runtime_capability = runtime.get("execution_capability_contract")
    if not isinstance(runtime_capability, dict) and isinstance(runtime_contract, dict):
        runtime_capability = runtime_contract.get("execution_capability_contract")
    expected_capability = profile.get("execution_capability_contract")
    profile_contract = profile.get("execution_reality_contract")
    if not isinstance(expected_capability, dict) and isinstance(profile_contract, dict):
        expected_capability = profile_contract.get("execution_capability_contract")
    if isinstance(runtime_capability, dict):
        for mismatch in execution_capability_contract_mismatch_reasons(
            expected=expected_capability if isinstance(expected_capability, dict) else None,
            observed=runtime_capability,
        ):
            mismatches.append(mismatch)
    elif str(profile.get("profile_mode") or "").strip().lower() in APPROVED_PROFILE_MODES:
        mismatches.append(
            {
                "field": "execution_capability_contract",
                "expected": profile.get("execution_capability_contract_hash"),
                "actual": None,
                "reason": "runtime_execution_capability_contract_missing",
            }
        )
    return tuple(_dedupe_mismatches(mismatches))


def _compare_behavior_parameter_coverage(
    mismatches: list[dict[str, object]],
    profile: dict[str, Any],
    runtime: dict[str, Any],
    profile_params: dict[str, Any],
    runtime_params: dict[str, Any],
) -> None:
    strategy_name = str(profile.get("strategy_name") or runtime.get("strategy_name") or "sma_with_filter")
    spec = strategy_spec_for_name(strategy_name)
    required = sorted(set(spec.behavior_affecting_parameter_names) - set(spec.research_only_parameter_names))
    for key in required:
        if key not in profile_params:
            mismatches.append(
                {
                    "field": f"strategy_parameters.{key}",
                    "expected": "profile_behavior_parameter_present",
                    "actual": None,
                    "reason": "profile_behavior_parameter_missing",
                }
            )
        if key not in runtime_params:
            mismatches.append(
                {
                    "field": f"strategy_parameters.{key}",
                    "expected": "runtime_behavior_parameter_present",
                    "actual": None,
                    "reason": "runtime_behavior_parameter_missing",
                }
            )
    unbound_runtime = sorted(key for key in runtime_params if key in set(required) and key not in profile_params)
    for key in unbound_runtime:
        mismatches.append(
            {
                "field": f"strategy_parameters.{key}",
                "expected": None,
                "actual": runtime_params.get(key),
                "reason": "runtime_behavior_parameter_unbound_by_profile",
            }
        )


def _dedupe_mismatches(mismatches: list[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[str] = set()
    result: list[dict[str, object]] = []
    for item in mismatches:
        key = json.dumps(item, sort_keys=True, separators=(",", ":"), default=str)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def profile_runtime_cost_match_status(profile: dict[str, Any] | None, runtime: dict[str, Any]) -> dict[str, object]:
    if not isinstance(profile, dict):
        return {"status": "WARN", "reason": "approved_profile_not_loaded"}
    assumption = profile.get("base_cost_assumption")
    if not isinstance(assumption, dict):
        return {"status": "WARN", "reason": "approved_profile_base_cost_assumption_missing"}
    if str(assumption.get("role") or "").strip() != "base" or assumption.get("promotable_as_base") is not True:
        return {
            "status": "FAIL",
            "reason": "stress_cost_is_not_runtime_base_cost",
            "expected": assumption,
            "actual": runtime.get("cost_model"),
            "operator_next_step": PROFILE_RUNTIME_COST_MISMATCH_ACTION,
        }
    runtime_cost = runtime.get("cost_model") if isinstance(runtime.get("cost_model"), dict) else {}
    expected = {
        "fee_rate": assumption.get("fee_rate"),
        "slippage_bps": assumption.get("slippage_bps"),
    }
    actual = {
        "fee_rate": runtime_cost.get("fee_rate"),
        "slippage_bps": runtime_cost.get("slippage_bps"),
    }
    if not _values_equal(expected["fee_rate"], actual["fee_rate"]) or not _values_equal(
        expected["slippage_bps"],
        actual["slippage_bps"],
    ):
        return {
            "status": "FAIL",
            "reason": "runtime_profile_cost_mismatch",
            "expected": expected,
            "actual": actual,
            "operator_next_step": PROFILE_RUNTIME_COST_MISMATCH_ACTION,
        }
    if runtime_cost.get("fee_authority_degraded") is True:
        return {
            "status": "WARN",
            "reason": "runtime_fee_authority_degraded",
            "expected": expected,
            "actual": actual,
            "operator_next_step": PROFILE_RUNTIME_COST_MISMATCH_ACTION,
        }
    return {"status": "PASS", "reason": "runtime_profile_cost_match", "expected": expected, "actual": actual}


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
    transition_policy = validate_production_calibration_policy(
        parent,
        target=deployment_tier_for_profile_mode(target),
    )
    if transition_policy.status != "PASS":
        raise ApprovedProfileError(
            "production_calibration_policy_failed:" + ",".join(transition_policy.reasons)
        )
    child = dict(parent)
    child["profile_mode"] = target
    child["deployment_tier"] = transition_policy.target
    child["production_calibration_policy_result"] = transition_policy.as_dict()
    child["production_calibration_policy_reasons"] = list(transition_policy.reasons)
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
    report_path = str(evidence.get("decision_equivalence_report_path") or "").strip()
    if not report_path:
        return
    try:
        report = _load_json(report_path)
    except ApprovedProfileError:
        return
    claims_scope = report.get("claims_scope") if isinstance(report.get("claims_scope"), dict) else {}
    child["decision_equivalence_outcome"] = report.get("outcome")
    child["decision_equivalence_claims_scope"] = dict(claims_scope)
    child["decision_equivalence_positive_state_classes"] = list(
        claims_scope.get("positive_equivalence_state_classes") or []
    )
    child["decision_equivalence_unsupported_state_classes"] = list(
        claims_scope.get("unsupported_state_classes") or []
    )
    child["decision_equivalence_full_lifecycle_supported"] = bool(
        claims_scope.get("full_lifecycle_equivalence_supported")
    )
    child["decision_equivalence_signal_equivalence_supported"] = bool(
        claims_scope.get("signal_equivalence_supported")
    )
    child["decision_equivalence_position_lifecycle_supported"] = bool(
        claims_scope.get("position_lifecycle_equivalence_supported")
    )
    child["decision_equivalence_fail_closed_unmodeled_state_count"] = int(
        claims_scope.get("fail_closed_unmodeled_state_count") or 0
    )


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
    if int(report.get("mismatched_decision_count") or 0) > 0:
        raise ApprovedProfileError(f"{label}_decision_equivalence_mismatch_count_nonzero")
    if int(report.get("canonical_incomplete_decision_count") or 0) > 0:
        raise ApprovedProfileError(f"{label}_decision_equivalence_incomplete_canonical")
    if int(report.get("canonical_missing_field_count") or 0) > 0:
        raise ApprovedProfileError(f"{label}_decision_equivalence_incomplete_canonical")
    if report.get("missing_research_decisions"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_missing_research_decisions")
    if report.get("missing_runtime_decisions"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_missing_runtime_decisions")
    if report.get("binding_validation"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_binding_validation_nonempty")
    if report.get("artifact_binding_validation"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_artifact_binding_validation_nonempty")
    expected_plugin_hash = str(parent_profile.get("strategy_plugin_contract_hash") or "").strip()
    research_plugin_hash = str(report.get("research_strategy_plugin_contract_hash") or "").strip()
    runtime_plugin_hash = str(report.get("runtime_strategy_plugin_contract_hash") or "").strip()
    if not research_plugin_hash.startswith("sha256:"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_research_strategy_plugin_contract_hash_missing")
    if not runtime_plugin_hash.startswith("sha256:"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_runtime_strategy_plugin_contract_hash_missing")
    if research_plugin_hash != expected_plugin_hash:
        raise ApprovedProfileError(f"{label}_decision_equivalence_research_strategy_plugin_contract_hash_mismatch")
    if runtime_plugin_hash != expected_plugin_hash:
        raise ApprovedProfileError(f"{label}_decision_equivalence_runtime_strategy_plugin_contract_hash_mismatch")
    expected_decision_contract_version = ""
    strategy_plugin_contract = parent_profile.get("strategy_plugin_contract")
    if isinstance(strategy_plugin_contract, dict):
        expected_decision_contract_version = str(
            strategy_plugin_contract.get("decision_contract_version") or ""
        ).strip()
    report_decision_contract_version = str(report.get("strategy_decision_contract_version") or "").strip()
    if not report_decision_contract_version:
        raise ApprovedProfileError(f"{label}_decision_equivalence_strategy_decision_contract_version_missing")
    if expected_decision_contract_version and report_decision_contract_version != expected_decision_contract_version:
        raise ApprovedProfileError(f"{label}_decision_equivalence_strategy_decision_contract_version_mismatch")
    if bool(report.get("blocked_decision_equivalence")):
        raise ApprovedProfileError(f"{label}_decision_equivalence_blocked")
    if (
        report.get("comparison_contract_version") not in SUPPORTED_DECISION_EQUIVALENCE_CONTRACTS
        or report.get("canonical_schema") is not True
    ):
        raise ApprovedProfileError(f"{label}_decision_equivalence_legacy_schema")
    if report.get("legacy_schema") is True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_legacy_schema")
    if "outcome" not in report:
        raise ApprovedProfileError(f"{label}_decision_equivalence_outcome_missing")
    if report.get("outcome") != "PASS_POSITIVE_EQUIVALENCE":
        raise ApprovedProfileError(f"{label}_decision_equivalence_outcome_not_positive")
    claims_scope = report.get("claims_scope")
    if not isinstance(claims_scope, dict):
        raise ApprovedProfileError(f"{label}_decision_equivalence_claims_scope_missing")
    state_coverage_matrix = report.get("state_coverage_matrix")
    if not isinstance(state_coverage_matrix, dict):
        raise ApprovedProfileError(f"{label}_decision_equivalence_state_coverage_matrix_missing")
    positive_classes = claims_scope.get("positive_equivalence_state_classes")
    if not isinstance(positive_classes, list) or not positive_classes:
        raise ApprovedProfileError(f"{label}_decision_equivalence_positive_state_classes_missing")
    unsupported_classes = claims_scope.get("unsupported_state_classes")
    if not isinstance(unsupported_classes, list):
        raise ApprovedProfileError(f"{label}_decision_equivalence_unsupported_state_classes_invalid")
    if unsupported_classes:
        raise ApprovedProfileError(f"{label}_decision_equivalence_unsupported_state_present")
    if int(claims_scope.get("fail_closed_unmodeled_state_count") or 0) > 0:
        raise ApprovedProfileError(f"{label}_decision_equivalence_unmodeled_state_present")
    for state_class in list(positive_classes) + list(unsupported_classes):
        entry = state_coverage_matrix.get(str(state_class))
        if not isinstance(entry, dict):
            raise ApprovedProfileError(f"{label}_decision_equivalence_state_coverage_matrix_incomplete")
    if claims_scope.get("full_lifecycle_equivalence_supported") is not True:
        if claims_scope.get("promotion_claim") != (
            "positive_decision_equivalence_for_explicitly_modeled_state_classes_only"
        ):
            raise ApprovedProfileError(f"{label}_decision_equivalence_scope_claim_missing")
    if claims_scope.get("signal_equivalence_supported") is not True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_signal_scope_not_supported")
    if claims_scope.get("position_lifecycle_equivalence_supported") is True and (
        claims_scope.get("full_lifecycle_equivalence_supported") is not True
    ):
        raise ApprovedProfileError(f"{label}_decision_equivalence_scope_contradiction")
    if report.get("promotion_grade_comparison") is not True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_not_promotion_grade")
    if report.get("legacy_or_unverified_export") is True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_unverified_export")
    if report.get("repo_owned_export_artifacts") is not True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_unverified_export")
    if not str(report.get("research_export_content_hash") or "").startswith("sha256:"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_research_export_hash_missing")
    if not str(report.get("runtime_export_content_hash") or "").startswith("sha256:"):
        raise ApprovedProfileError(f"{label}_decision_equivalence_runtime_export_hash_missing")
    if report.get("ok") is not True:
        raise ApprovedProfileError(f"{label}_decision_equivalence_not_ok")
