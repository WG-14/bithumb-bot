from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .execution_reality_contract import contract_hash_matches
from .research.hashing import content_hash_payload, sha256_prefixed


EVIDENCE_SCHEMA_VERSION = 1
EVIDENCE_HASH_FIELD = "content_hash"
EVIDENCE_HASH_EXCLUDED_FIELDS = frozenset({EVIDENCE_HASH_FIELD, "generated_at"})


class EvidenceValidationError(ValueError):
    pass


@dataclass(frozen=True)
class EvidenceValidationPolicy:
    min_observation_seconds: float
    min_decision_count: int
    min_closed_lifecycle_count: int
    max_blocked_decision_ratio: float
    max_execution_quality_breach_count: int
    require_execution_quality_applicable: bool = True
    require_db_data_fingerprint: bool = True


PAPER_VALIDATION_EVIDENCE_POLICY = EvidenceValidationPolicy(
    min_observation_seconds=86400.0,
    min_decision_count=10,
    min_closed_lifecycle_count=3,
    max_blocked_decision_ratio=0.5,
    max_execution_quality_breach_count=0,
    require_execution_quality_applicable=False,
    require_db_data_fingerprint=True,
)
LIVE_READINESS_EVIDENCE_POLICY = EvidenceValidationPolicy(
    min_observation_seconds=86400.0,
    min_decision_count=10,
    min_closed_lifecycle_count=3,
    max_blocked_decision_ratio=0.5,
    max_execution_quality_breach_count=0,
    require_execution_quality_applicable=True,
    require_db_data_fingerprint=True,
)


def evidence_validation_policy_for(*, expected_type: str, expected_mode: str) -> EvidenceValidationPolicy:
    evidence_type = str(expected_type or "").strip().lower()
    mode = str(expected_mode or "").strip().lower()
    if evidence_type == "paper_validation" and mode == "paper":
        return PAPER_VALIDATION_EVIDENCE_POLICY
    if evidence_type == "live_readiness" and mode == "live":
        return LIVE_READINESS_EVIDENCE_POLICY
    return EvidenceValidationPolicy(
        min_observation_seconds=86400.0,
        min_decision_count=10,
        min_closed_lifecycle_count=3,
        max_blocked_decision_ratio=0.5,
        max_execution_quality_breach_count=0,
        require_execution_quality_applicable=True,
        require_db_data_fingerprint=True,
    )


def evidence_hash_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in EVIDENCE_HASH_EXCLUDED_FIELDS}


def compute_evidence_content_hash(payload: dict[str, Any]) -> str:
    return sha256_prefixed(content_hash_payload(evidence_hash_payload(payload)))


def validate_evidence_content_hash(payload: dict[str, Any], *, label: str) -> str:
    expected = payload.get(EVIDENCE_HASH_FIELD)
    if not isinstance(expected, str) or not expected.startswith("sha256:"):
        raise EvidenceValidationError(f"{label}_schema_invalid:content_hash_missing")
    try:
        actual = compute_evidence_content_hash(payload)
    except ValueError as exc:
        raise EvidenceValidationError(f"{label}_schema_invalid:non_finite_json") from exc
    if actual != expected:
        raise EvidenceValidationError(f"{label}_content_hash_mismatch")
    return actual


def validate_profile_transition_evidence(
    payload: dict[str, Any],
    *,
    label: str,
    expected_type: str,
    expected_mode: str,
    parent_profile: dict[str, Any],
    evidence_path: str | Path | None = None,
    policy: EvidenceValidationPolicy | None = None,
) -> str:
    prefix = str(label)
    validation_policy = policy or evidence_validation_policy_for(
        expected_type=expected_type,
        expected_mode=expected_mode,
    )
    _require_object(payload, prefix)
    if int(payload.get("evidence_schema_version") or 0) != EVIDENCE_SCHEMA_VERSION:
        raise EvidenceValidationError(f"{prefix}_schema_invalid:schema_version")
    _require_equal(payload.get("evidence_type"), expected_type, f"{prefix}_schema_invalid:evidence_type")
    _require_equal(payload.get("mode"), expected_mode, f"{prefix}_schema_invalid:mode")
    for key in ("market", "interval", "strategy_name"):
        _require_equal(payload.get(key), parent_profile.get(key), f"{prefix}_{key}_mismatch")
    _require_equal(
        payload.get("approved_profile_content_hash"),
        parent_profile.get("profile_content_hash"),
        f"{prefix}_profile_hash_mismatch",
    )
    source_hash = str(parent_profile.get("source_promotion_content_hash") or "")
    if source_hash:
        _require_equal(
            payload.get("source_promotion_content_hash"),
            source_hash,
            f"{prefix}_source_promotion_hash_mismatch",
        )
    parent_contract_hash = str(parent_profile.get("execution_contract_hash") or "").strip()
    evidence_contract_hash = str(payload.get("execution_contract_hash") or "").strip()
    if not parent_contract_hash:
        raise EvidenceValidationError(f"{prefix}_parent_execution_contract_hash_missing")
    if not evidence_contract_hash:
        raise EvidenceValidationError(f"{prefix}_execution_contract_hash_missing")
    if evidence_contract_hash != parent_contract_hash:
        raise EvidenceValidationError(f"{prefix}_execution_contract_hash_mismatch")
    evidence_contract = payload.get("execution_reality_contract")
    if evidence_contract is not None:
        if not isinstance(evidence_contract, dict):
            raise EvidenceValidationError(f"{prefix}_schema_invalid:execution_reality_contract")
        if not contract_hash_matches(evidence_contract, evidence_contract_hash):
            raise EvidenceValidationError(f"{prefix}_execution_contract_hash_mismatch")
    if evidence_path is not None:
        recorded_path = str(payload.get("evidence_path") or "").strip()
        if recorded_path and str(Path(evidence_path).expanduser().resolve()) != recorded_path:
            raise EvidenceValidationError(f"{prefix}_schema_invalid:evidence_path_mismatch")

    validate_evidence_content_hash(payload, label=prefix)
    start = _parse_timestamp(payload.get("observation_start"), f"{prefix}_schema_invalid:observation_start")
    end = _parse_timestamp(payload.get("observation_end"), f"{prefix}_schema_invalid:observation_end")
    if end <= start:
        raise EvidenceValidationError(f"{prefix}_schema_invalid:observation_window_order")
    duration_seconds = _number(payload.get("observation_duration_seconds"), f"{prefix}_schema_invalid:observation_duration_seconds")
    actual_duration = (end - start).total_seconds()
    if duration_seconds <= 0 or abs(duration_seconds - actual_duration) > 1.0:
        raise EvidenceValidationError(f"{prefix}_schema_invalid:observation_duration_mismatch")

    thresholds = payload.get("thresholds")
    if not isinstance(thresholds, dict):
        raise EvidenceValidationError(f"{prefix}_schema_invalid:thresholds_missing")
    artifact_min_duration = _number(
        thresholds.get("min_observation_seconds"),
        f"{prefix}_schema_invalid:min_observation_seconds",
    )
    artifact_min_decisions = _integer(
        thresholds.get("min_decision_count"),
        f"{prefix}_schema_invalid:min_decision_count",
    )
    artifact_min_closed = _integer(
        thresholds.get("min_closed_lifecycle_count"),
        f"{prefix}_schema_invalid:min_closed_lifecycle_count",
    )
    artifact_max_blocked_ratio = _number(
        thresholds.get("max_blocked_decision_ratio"),
        f"{prefix}_schema_invalid:max_blocked_decision_ratio",
    )
    artifact_max_eq_breaches = _integer(
        thresholds.get("max_execution_quality_breach_count"),
        f"{prefix}_schema_invalid:max_execution_quality_breach_count",
    )
    _reject_weaker_threshold(
        artifact_min_duration,
        validation_policy.min_observation_seconds,
        prefix,
        "min_observation_seconds",
        minimum=True,
    )
    _reject_weaker_threshold(
        artifact_min_decisions,
        validation_policy.min_decision_count,
        prefix,
        "min_decision_count",
        minimum=True,
    )
    _reject_weaker_threshold(
        artifact_min_closed,
        validation_policy.min_closed_lifecycle_count,
        prefix,
        "min_closed_lifecycle_count",
        minimum=True,
    )
    _reject_weaker_threshold(
        artifact_max_blocked_ratio,
        validation_policy.max_blocked_decision_ratio,
        prefix,
        "max_blocked_decision_ratio",
        minimum=False,
    )
    _reject_weaker_threshold(
        artifact_max_eq_breaches,
        validation_policy.max_execution_quality_breach_count,
        prefix,
        "max_execution_quality_breach_count",
        minimum=False,
    )
    min_duration = max(artifact_min_duration, validation_policy.min_observation_seconds)
    min_decisions = max(artifact_min_decisions, validation_policy.min_decision_count)
    min_closed = max(artifact_min_closed, validation_policy.min_closed_lifecycle_count)
    max_blocked_ratio = min(artifact_max_blocked_ratio, validation_policy.max_blocked_decision_ratio)
    max_eq_breaches = min(artifact_max_eq_breaches, validation_policy.max_execution_quality_breach_count)

    decision_count = _integer(payload.get("decision_count"), f"{prefix}_schema_invalid:decision_count")
    blocked_count = _integer(payload.get("blocked_decision_count"), f"{prefix}_schema_invalid:blocked_decision_count")
    closed_count = _integer(payload.get("closed_lifecycle_count"), f"{prefix}_schema_invalid:closed_lifecycle_count")
    if blocked_count > decision_count:
        raise EvidenceValidationError(f"{prefix}_schema_invalid:blocked_decision_count")
    if duration_seconds < min_duration:
        raise EvidenceValidationError(f"{prefix}_observation_window_insufficient")
    if decision_count < min_decisions:
        raise EvidenceValidationError(f"{prefix}_decision_count_insufficient")
    if closed_count < min_closed:
        raise EvidenceValidationError(f"{prefix}_closed_lifecycle_count_insufficient")
    if decision_count > 0 and (blocked_count / decision_count) > max_blocked_ratio:
        raise EvidenceValidationError(f"{prefix}_blocked_decision_ratio_excessive")

    for key in ("gross_pnl", "fee_total", "net_pnl"):
        _number(payload.get(key), f"{prefix}_schema_invalid:{key}")
    for key in ("expectancy_per_trade", "profit_factor", "fee_drag_ratio"):
        if payload.get(key) is not None:
            _number(payload.get(key), f"{prefix}_schema_invalid:{key}")
    if payload.get("fee_drag_ratio") is not None:
        basis = str(payload.get("fee_drag_ratio_basis") or "").strip()
        if basis not in {"traded_notional", "gross_pnl_abs"}:
            raise EvidenceValidationError(f"{prefix}_schema_invalid:fee_drag_ratio_basis")

    execution_quality_status = str(payload.get("execution_quality_status") or "").strip().lower()
    if execution_quality_status not in {"pass", "ok", "not_applicable"}:
        raise EvidenceValidationError(f"{prefix}_execution_quality_breached")
    if execution_quality_status == "not_applicable" and validation_policy.require_execution_quality_applicable:
        raise EvidenceValidationError(f"{prefix}_execution_quality_not_applicable")
    breach_count = _integer(
        payload.get("execution_quality_breach_count"),
        f"{prefix}_schema_invalid:execution_quality_breach_count",
    )
    if breach_count > max_eq_breaches:
        raise EvidenceValidationError(f"{prefix}_execution_quality_breached")
    if _integer(payload.get("unresolved_open_orders_count"), f"{prefix}_schema_invalid:unresolved_open_orders_count") > 0:
        raise EvidenceValidationError(f"{prefix}_unresolved_orders_present")
    if _integer(payload.get("recovery_blocker_count"), f"{prefix}_schema_invalid:recovery_blocker_count") > 0:
        raise EvidenceValidationError(f"{prefix}_recovery_blocker_present")
    drift_status = str(payload.get("runtime_profile_drift_status") or "").strip().lower()
    if drift_status not in {"none", "pass", "ok"}:
        raise EvidenceValidationError(f"{prefix}_runtime_profile_drift_present")
    if validation_policy.require_db_data_fingerprint:
        _require_sha256_fingerprint(
            payload.get("db_data_fingerprint"),
            f"{prefix}_schema_invalid:db_data_fingerprint",
        )

    return str(payload[EVIDENCE_HASH_FIELD])


def evidence_report_failure_payload(*, command: str, error: str, artifact_path: str | None = None) -> dict[str, object]:
    return {
        "ok": False,
        "command": command,
        "error": error,
        "artifact_path": artifact_path,
        "recommended_next_action": _recommended_next_action(error),
    }


def _recommended_next_action(error: str) -> str:
    if "policy_threshold_too_weak" in error:
        return "regenerate_typed_evidence_with_repo_trusted_thresholds_or_update_policy"
    if "db_data_fingerprint" in error:
        return "regenerate_typed_evidence_with_db_fingerprint"
    if "execution_quality_not_applicable" in error:
        return "generate_or_attach_execution_quality_evidence_before_promotion"
    if "execution_contract_hash" in error:
        return "regenerate_typed_evidence_with_matching_execution_contract"
    if "profile_hash_mismatch" in error or "source_promotion_hash_mismatch" in error:
        return "fix_profile_selection_or_rerun_evidence_for_selected_profile"
    if "observation_window_insufficient" in error or "decision_count_insufficient" in error:
        return "continue_paper_or_live_dry_run_observation_before_promotion"
    if "execution_quality_breached" in error:
        return "inspect_execution_quality_report_before_promotion"
    if "unresolved_orders_present" in error or "recovery_blocker_present" in error:
        return "stop_promotion_and_resolve_recovery_or_order_state"
    if "schema_invalid" in error:
        return "regenerate_typed_evidence_artifact"
    return "operator_review_required"


def _require_object(payload: object, prefix: str) -> None:
    if not isinstance(payload, dict):
        raise EvidenceValidationError(f"{prefix}_schema_invalid:payload_not_object")


def _require_equal(actual: object, expected: object, reason: str) -> None:
    if str(actual or "").strip() != str(expected or "").strip():
        raise EvidenceValidationError(reason)


def _reject_weaker_threshold(
    artifact_value: float | int,
    policy_value: float | int,
    prefix: str,
    field: str,
    *,
    minimum: bool,
) -> None:
    weaker = artifact_value < policy_value if minimum else artifact_value > policy_value
    if weaker:
        raise EvidenceValidationError(f"{prefix}_policy_threshold_too_weak:{field}")


def _require_sha256_fingerprint(value: object, reason: str) -> str:
    if not isinstance(value, str):
        raise EvidenceValidationError(reason)
    raw = value.strip()
    if not raw.startswith("sha256:") or len(raw) <= len("sha256:"):
        raise EvidenceValidationError(reason)
    return raw


def _parse_timestamp(value: object, reason: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise EvidenceValidationError(reason)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceValidationError(reason) from exc


def _integer(value: object, reason: str) -> int:
    if isinstance(value, bool):
        raise EvidenceValidationError(reason)
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceValidationError(reason) from exc
    if parsed < 0:
        raise EvidenceValidationError(reason)
    return parsed


def _number(value: object, reason: str) -> float:
    if isinstance(value, bool):
        raise EvidenceValidationError(reason)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceValidationError(reason) from exc
    if not math.isfinite(parsed):
        raise EvidenceValidationError(reason)
    return parsed
