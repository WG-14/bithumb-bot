from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import PathManager, PathPolicyError
from .research.hashing import content_hash_payload, sha256_prefixed
from .research.promotion_gate import build_candidate_profile
from .storage_io import write_json_atomic


APPROVED_PROFILE_SCHEMA_VERSION = 1
APPROVED_PROFILE_MODES = {"paper", "live_dry_run", "small_live"}
LIVE_COMPATIBLE_PROFILE_MODES = {"live_dry_run", "small_live"}
PROFILE_HASH_FIELD = "profile_content_hash"
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
    candidate_profile_hash: str | None
    manifest_hash: str | None
    dataset_content_hash: str | None
    mode: str | None
    expected_runtime_mode: str | None
    mismatches: tuple[dict[str, object], ...]
    profile: dict[str, Any] | None = None

    def audit_fields(self) -> dict[str, object]:
        return {
            "approved_profile_path": self.profile_path,
            "approved_profile_hash": self.profile_hash,
            "approved_profile_mode": self.mode,
            "approved_profile_verification_ok": self.ok,
            "approved_profile_block_reason": self.reason,
            "promotion_content_hash": self.promotion_hash,
            "candidate_profile_hash": self.candidate_profile_hash,
            "manifest_hash": self.manifest_hash,
            "dataset_content_hash": self.dataset_content_hash,
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
    payload: dict[str, Any] = {
        "profile_schema_version": APPROVED_PROFILE_SCHEMA_VERSION,
        "profile_mode": normalized_mode,
        "source_promotion_artifact_path": str(source_promotion_path),
        "source_promotion_content_hash": source_hash,
        "candidate_profile_hash": verified_promotion.get("candidate_profile_hash"),
        "manifest_hash": verified_promotion.get("manifest_hash"),
        "dataset_content_hash": verified_promotion.get("dataset_content_hash"),
        "repository_version": repository_version or verified_promotion.get("repository_version") or "unknown",
        "strategy_name": verified_promotion.get("strategy_name"),
        "market": str(market),
        "interval": str(interval),
        "strategy_parameters": _strategy_parameters_from_promotion(verified_promotion),
        "cost_model": _cost_model_from_promotion(verified_promotion),
        "regime_policy": dict(live_policy),
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "parent_profile_hash": parent_hash,
        "paper_validation_evidence": paper_validation_evidence,
        "live_readiness_evidence": live_readiness_evidence,
    }
    payload[PROFILE_HASH_FIELD] = sha256_prefixed(content_hash_payload(payload))
    return payload


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
    _verify_payload_hash(profile, field=PROFILE_HASH_FIELD, label="profile_content")
    return profile


def load_approved_profile(path: str | Path) -> dict[str, Any]:
    return validate_approved_profile(_load_json(path))


def load_profile_or_promotion_regime_policy(path: str | Path | None) -> dict[str, object] | None:
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
            }
        policy = profile.get("regime_policy")
        if isinstance(policy, dict):
            return {
                "live_regime_policy": dict(policy),
                "strategy_profile_id": profile.get("profile_id") or profile.get(PROFILE_HASH_FIELD),
                "strategy_profile_hash": profile.get(PROFILE_HASH_FIELD),
                "content_hash": profile.get(PROFILE_HASH_FIELD),
                "source_promotion_content_hash": profile.get("source_promotion_content_hash"),
                "candidate_profile_hash": profile.get("candidate_profile_hash"),
                "manifest_hash": profile.get("manifest_hash"),
                "dataset_content_hash": profile.get("dataset_content_hash"),
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
    return {
        "mode": str(getattr(cfg, "MODE", "")),
        "profile_selector": str(getattr(cfg, "APPROVED_STRATEGY_PROFILE_PATH", "")),
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


def diff_profile_to_runtime(profile: dict[str, Any], runtime: dict[str, Any]) -> tuple[dict[str, object], ...]:
    validate_approved_profile(profile)
    mismatches: list[dict[str, object]] = []
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
) -> ProfileVerificationResult:
    raw_path = str(profile_path or "").strip()
    if not raw_path:
        reason = "approved_profile_missing" if require_profile else "approved_profile_not_configured"
        return _verification_result(False, reason, None, None, tuple(), None)
    try:
        profile = load_approved_profile(raw_path)
        mode = str(profile.get("profile_mode"))
        if expected_profile_modes and mode not in expected_profile_modes:
            raise ApprovedProfileError(f"profile_mode_mismatch: expected={sorted(expected_profile_modes)} actual={mode}")
        mismatches = diff_profile_to_runtime(profile, runtime)
        if mismatches:
            return _verification_result(False, "approved_profile_runtime_mismatch", raw_path, profile, mismatches, runtime)
        return _verification_result(True, "ok", raw_path, profile, tuple(), runtime)
    except ApprovedProfileError as exc:
        return _verification_result(False, str(exc), raw_path, None, tuple(), runtime)


def _verification_result(
    ok: bool,
    reason: str,
    path: str | None,
    profile: dict[str, Any] | None,
    mismatches: tuple[dict[str, object], ...],
    runtime: dict[str, Any] | None,
) -> ProfileVerificationResult:
    return ProfileVerificationResult(
        ok=bool(ok),
        reason=str(reason),
        profile_path=path,
        profile_hash=None if profile is None else str(profile.get(PROFILE_HASH_FIELD) or ""),
        promotion_hash=None if profile is None else str(profile.get("source_promotion_content_hash") or ""),
        candidate_profile_hash=None if profile is None else str(profile.get("candidate_profile_hash") or ""),
        manifest_hash=None if profile is None else str(profile.get("manifest_hash") or ""),
        dataset_content_hash=None if profile is None else str(profile.get("dataset_content_hash") or ""),
        mode=None if profile is None else str(profile.get("profile_mode") or ""),
        expected_runtime_mode=None if runtime is None else str(runtime.get("mode") or ""),
        mismatches=mismatches,
        profile=profile,
    )


def promote_profile_mode(
    *,
    parent_profile: dict[str, Any],
    target_mode: str,
    paper_validation_evidence: str | None = None,
    live_readiness_evidence: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    parent = validate_approved_profile(dict(parent_profile))
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
    child["paper_validation_evidence"] = paper_validation_evidence or parent.get("paper_validation_evidence")
    child["live_readiness_evidence"] = live_readiness_evidence
    child["generated_at"] = generated_at or datetime.now(timezone.utc).isoformat()
    child.pop(PROFILE_HASH_FIELD, None)
    child[PROFILE_HASH_FIELD] = sha256_prefixed(content_hash_payload(child))
    return validate_approved_profile(child)
