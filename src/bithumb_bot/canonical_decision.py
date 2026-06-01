from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import replace
from typing import Any

from .position_authority import (
    POSITIVE_EQUIVALENCE_STATE_CLASSES,
    classify_runtime_position_state,
    lot_native_comparison_position_state,
    runtime_state_has_required_lot_native_fields,
    runtime_position_authority_snapshot,
)
from .promotion_provenance import (
    PROMOTION_ARTIFACT_GRADE,
    PROMOTION_AUTHORITY_PLANE,
    PROMOTION_EXECUTION_EVIDENCE_SOURCE,
    build_typed_no_submit_proof,
    validate_promotion_artifact as validate_promotion_artifact_provenance_gate,
)


def sha256_prefixed(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


CANONICAL_DECISION_CONTRACT_VERSION = 2
LEGACY_CANONICAL_DECISION_CONTRACT_VERSION_V1 = 1
LEGACY_STRATEGY_CONTRACT_VERSION_V1 = "sma_strategy_v1"
CANONICAL_DECISION_COMPARISON_CONTRACT_VERSION = "canonical_decision_v2"
LEGACY_CANONICAL_DECISION_COMPARISON_CONTRACT_VERSION_V1 = "canonical_decision_v1"

COMMON_CANONICAL_DECISION_FIELDS_V2 = (
    "decision_contract_version",
    "strategy_name",
    "strategy_version",
    "strategy_decision_contract_version",
    "profile_content_hash",
    "candidate_profile_hash",
    "dataset_content_hash",
    "db_data_fingerprint",
    "market",
    "interval",
    "signal_timestamp",
    "candle_ts",
    "through_ts_ms",
    "candle_basis",
    "decision_ts",
    "raw_signal",
    "final_signal",
    "side",
    "blocked",
    "block_reason",
    "blocked_filters",
    "fee_authority_hash",
    "fee_model_hash",
    "slippage_model_hash",
    "order_rules_hash",
    "market_regime",
    "regime_decision",
    "regime_block_reason",
    "position_state_hash",
    "entry_allowed",
    "exit_allowed",
    "dust_state",
    "effective_flat",
    "normalized_exposure_active",
    "exit_rule",
    "exit_reason",
    "exit_evaluations_hash",
    "execution_timing_policy_hash",
    "policy_contract_hash",
    "policy_input_hash",
    "policy_decision_hash",
    "replay_fingerprint_hash",
    "decision_input_bundle_hash",
    "decision_input_contract_hash",
    "decision_input_bundle_payload_hash",
    "market_snapshot_hash",
    "market_feature_hash",
    "canonical_feature_projection_hash",
    "position_snapshot_hash",
    "execution_constraints_hash",
    "policy_config_hash",
    "exit_policy_config_hash",
    "final_exit_decision_input_hash",
    "snapshot_projector_version",
    "snapshot_projector_hash",
    "runtime_decision_request_hash",
    "runtime_strategy_set_manifest_hash",
    "approved_profile_hash",
    "execution_plan_bundle_hash",
    "execution_summary_hash",
    "execution_submit_plan_hash",
    "final_action",
    "submit_expected",
    "pre_submit_proof_status",
    "execution_block_reason",
    "submit_plan_source",
    "submit_plan_authority",
    "execution_engine",
    "decision_authority_source",
    "decision_envelope_present",
    "execution_plan_bundle_present",
    "execution_evidence_source",
    "typed_execution_summary_present",
    "compatibility_fallback",
    "legacy_context_planning_used",
    "runtime_replay_planning_error",
    "execution_plan_status",
    "execution_plan_reason_code",
    "artifact_grade",
    "authority_plane",
    "promotion_rejection_reason",
    "execution_plan_bundle_evidence",
    "typed_execution_summary_evidence",
    "execution_submit_plan_evidence",
    "typed_no_submit_proof",
    "feature_snapshot_hash",
    "strategy_behavior_hash",
)
CANONICAL_DECISION_SCHEMA_FIELDS = COMMON_CANONICAL_DECISION_FIELDS_V2
LEGACY_CANONICAL_DECISION_SCHEMA_FIELDS_V1 = (
    "decision_contract_version",
    "strategy_contract_version",
    "strategy_name",
    "profile_content_hash",
    "candidate_profile_hash",
    "dataset_content_hash",
    "db_data_fingerprint",
    "market",
    "interval",
    "signal_timestamp",
    "candle_ts",
    "through_ts_ms",
    "candle_basis",
    "decision_ts",
    "raw_signal",
    "final_signal",
    "side",
    "blocked",
    "block_reason",
    "blocked_filters",
    "prev_s",
    "prev_l",
    "curr_s",
    "curr_l",
    "feature_hash",
    "gap_ratio",
    "range_ratio",
    "expected_edge_ratio",
    "required_edge_ratio",
    "fee_authority_hash",
    "fee_model_hash",
    "slippage_model_hash",
    "order_rules_hash",
    "market_regime",
    "regime_decision",
    "regime_block_reason",
    "position_state_hash",
    "entry_allowed",
    "exit_allowed",
    "dust_state",
    "effective_flat",
    "normalized_exposure_active",
    "exit_rule",
    "exit_reason",
    "exit_evaluations_hash",
    "execution_timing_policy_hash",
    "replay_fingerprint_hash",
)
PROMOTION_REQUIRED_CANONICAL_FIELDS = (
    "decision_contract_version",
    "strategy_name",
    "strategy_decision_contract_version",
    "profile_content_hash",
    "market",
    "interval",
    "candle_basis",
    "raw_signal",
    "final_signal",
    "side",
    "blocked",
    "fee_model_hash",
    "slippage_model_hash",
    "order_rules_hash",
    "position_state_hash",
    "entry_allowed",
    "exit_allowed",
    "dust_state",
    "effective_flat",
    "normalized_exposure_active",
    "exit_evaluations_hash",
    "execution_timing_policy_hash",
    "runtime_decision_request_hash",
    "runtime_strategy_set_manifest_hash",
    "approved_profile_hash",
    "execution_plan_bundle_hash",
    "execution_summary_hash",
    "execution_submit_plan_hash",
    "final_action",
    "submit_expected",
    "pre_submit_proof_status",
    "execution_block_reason",
    "submit_plan_source",
    "submit_plan_authority",
    "execution_engine",
    "feature_snapshot_hash",
    "strategy_behavior_hash",
    "market_feature_hash",
    "final_exit_decision_input_hash",
)
LEGACY_PROMOTION_REQUIRED_CANONICAL_FIELDS_V1 = (
    "decision_contract_version",
    "strategy_contract_version",
    "strategy_name",
    "profile_content_hash",
    "market",
    "interval",
    "candle_basis",
    "raw_signal",
    "final_signal",
    "side",
    "blocked",
    "fee_model_hash",
    "slippage_model_hash",
    "order_rules_hash",
    "position_state_hash",
    "entry_allowed",
    "exit_allowed",
    "dust_state",
    "effective_flat",
    "normalized_exposure_active",
    "exit_evaluations_hash",
    "execution_timing_policy_hash",
)
PROMOTION_REQUIRED_ONE_OF_CANONICAL_FIELDS = (("signal_timestamp", "candle_ts"),)
PROMOTION_PROVENANCE_REQUIRED_CANONICAL_FIELDS = (
    "runtime_decision_request_hash",
    "runtime_strategy_set_manifest_hash",
    "approved_profile_hash",
    "execution_plan_bundle_hash",
)
EMPTY_ORDER_RULES_HASH = sha256_prefixed({})
CANONICAL_FLAT_POSITION_STATE = {
    "comparison_state": "flat_no_dust_no_position",
    "entry_allowed": True,
    "exit_allowed": False,
    "dust_state": "flat",
    "effective_flat": True,
    "normalized_exposure_active": False,
}


@dataclass(frozen=True)
class CanonicalDecisionEvent:
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(self.payload)


@dataclass(frozen=True)
class CanonicalDecisionValidation:
    canonical_schema_present: bool
    canonical_schema_complete: bool
    promotion_grade: bool
    legacy_shallow_decision: bool
    incomplete_canonical_decision: bool
    missing_fields: tuple[str, ...]
    reason_codes: tuple[str, ...]


PROMOTION_TYPED_EXECUTION_EVIDENCE_SOURCE = PROMOTION_EXECUTION_EVIDENCE_SOURCE


def canonical_payload_hash(value: object) -> str:
    return sha256_prefixed(_stable_value(value))


def normalize_canonical_decision(payload: dict[str, Any]) -> dict[str, Any]:
    version = int(payload.get("decision_contract_version") or CANONICAL_DECISION_CONTRACT_VERSION)
    fields = (
        LEGACY_CANONICAL_DECISION_SCHEMA_FIELDS_V1
        if version < CANONICAL_DECISION_CONTRACT_VERSION
        else COMMON_CANONICAL_DECISION_FIELDS_V2
    )
    normalized = {field: _canonical_field_value(field, payload.get(field)) for field in fields}
    normalized["decision_contract_version"] = version
    if version < CANONICAL_DECISION_CONTRACT_VERSION:
        normalized["strategy_contract_version"] = str(
            payload.get("strategy_contract_version") or LEGACY_STRATEGY_CONTRACT_VERSION_V1
        )
    else:
        normalized["strategy_version"] = str(payload.get("strategy_version") or "")
        normalized["strategy_decision_contract_version"] = str(
            payload.get("strategy_decision_contract_version")
            or payload.get("strategy_contract_version")
            or ""
        )
    normalized["side"] = str(payload.get("side") or payload.get("final_signal") or "").strip().upper()
    normalized["raw_signal"] = str(payload.get("raw_signal") or "").strip().upper()
    normalized["final_signal"] = str(payload.get("final_signal") or normalized["side"]).strip().upper()
    normalized["blocked"] = bool(payload.get("blocked"))
    normalized["blocked_filters"] = tuple(str(item) for item in payload.get("blocked_filters") or ())
    for field in (
        "compatibility_fallback",
        "legacy_context_planning_used",
        "typed_execution_summary_present",
        "decision_envelope_present",
        "execution_plan_bundle_present",
    ):
        if field in normalized:
            normalized[field] = bool(payload.get(field))
    for field in (
        "runtime_decision_request_hash",
        "runtime_strategy_set_manifest_hash",
        "approved_profile_hash",
        "execution_plan_bundle_hash",
        "decision_authority_source",
        "execution_evidence_source",
        "runtime_replay_planning_error",
        "execution_plan_status",
        "execution_plan_reason_code",
        "artifact_grade",
        "authority_plane",
        "promotion_rejection_reason",
    ):
        if field in normalized:
            normalized[field] = str(payload.get(field) or "")
    if version >= CANONICAL_DECISION_CONTRACT_VERSION:
        feature_snapshot = _strategy_feature_snapshot(payload)
        strategy_payload = _strategy_behavior_payload(payload)
        normalized["feature_snapshot_hash"] = str(
            payload.get("feature_snapshot_hash") or canonical_payload_hash(feature_snapshot)
        )
        normalized["strategy_behavior_hash"] = str(
            payload.get("strategy_behavior_hash") or canonical_payload_hash(strategy_payload)
        )
        normalized["feature_snapshot"] = feature_snapshot
        normalized["strategy_specific_payload"] = _strategy_specific_payload(payload)
        normalized["strategy_diagnostics_namespace"] = str(
            payload.get("strategy_diagnostics_namespace") or normalized.get("strategy_name") or ""
        )
        normalized["strategy_diagnostics"] = _stable_value(
            payload.get("strategy_diagnostics") if isinstance(payload.get("strategy_diagnostics"), dict) else {}
        )
        normalized["strategy_behavior_payload"] = strategy_payload
    return normalized


def is_canonical_decision(payload: dict[str, Any]) -> bool:
    return int(payload.get("decision_contract_version") or 0) >= LEGACY_CANONICAL_DECISION_CONTRACT_VERSION_V1


def is_canonical_decision_v2(payload: dict[str, Any]) -> bool:
    return int(payload.get("decision_contract_version") or 0) >= CANONICAL_DECISION_CONTRACT_VERSION


def validate_canonical_decision_payload(
    payload: dict[str, Any],
    *,
    promotion_grade: bool = True,
    require_promotion_provenance: bool = True,
) -> CanonicalDecisionValidation:
    schema_present = is_canonical_decision(payload)
    if not schema_present:
        return CanonicalDecisionValidation(
            canonical_schema_present=False,
            canonical_schema_complete=False,
            promotion_grade=False,
            legacy_shallow_decision=True,
            incomplete_canonical_decision=False,
            missing_fields=(),
            reason_codes=("canonical_decision_legacy_schema",),
        )
    normalized = normalize_canonical_decision(payload)
    missing: list[str] = []
    required_fields = (
        LEGACY_PROMOTION_REQUIRED_CANONICAL_FIELDS_V1
        if int(normalized.get("decision_contract_version") or 0) < CANONICAL_DECISION_CONTRACT_VERSION
        else PROMOTION_REQUIRED_CANONICAL_FIELDS
    )
    if not require_promotion_provenance:
        required_fields = tuple(
            field
            for field in required_fields
            if field not in PROMOTION_PROVENANCE_REQUIRED_CANONICAL_FIELDS or field not in payload
        )
    for field in required_fields:
        if _canonical_required_missing(normalized.get(field)):
            missing.append(field)
    for group in PROMOTION_REQUIRED_ONE_OF_CANONICAL_FIELDS:
        if all(_canonical_required_missing(normalized.get(field)) for field in group):
            missing.append("|".join(group))
    reason_codes: list[str] = []
    if missing:
        reason_codes.extend(["canonical_decision_required_field_missing", "canonical_decision_incomplete"])
    if str(normalized.get("order_rules_hash") or "").strip() == EMPTY_ORDER_RULES_HASH:
        missing.append("order_rules_hash")
        reason_codes.extend(
            [
                "canonical_decision_empty_order_rules_hash",
                "canonical_decision_incomplete",
            ]
        )
    if promotion_grade:
        if int(normalized.get("decision_contract_version") or 0) < CANONICAL_DECISION_CONTRACT_VERSION:
            missing.append("decision_contract_version")
            reason_codes.extend(
                [
                    "canonical_promotion_legacy_contract_version",
                    "canonical_decision_incomplete",
                ]
            )
        if require_promotion_provenance:
            provenance_failures = _promotion_artifact_provenance_failures(payload, normalized)
            for field, reason in provenance_failures:
                missing.append(field)
                reason_codes.extend([reason, "canonical_decision_incomplete"])
        authority = payload.get("position_authority")
        if not isinstance(authority, dict):
            missing.append("position_authority")
            reason_codes.extend(
                [
                    "canonical_decision_position_authority_missing",
                    "canonical_decision_incomplete",
                ]
            )
        else:
            state_class = str(authority.get("state_class") or "").strip()
            if not state_class:
                missing.append("position_authority.state_class")
                reason_codes.extend(
                    [
                        "canonical_decision_position_authority_state_class_missing",
                        "canonical_decision_incomplete",
                    ]
                )
            for field, reason in (
                ("position_state_hash", "canonical_decision_position_authority_position_hash_mismatch"),
                ("order_rules_hash", "canonical_decision_position_authority_order_rules_hash_mismatch"),
                ("fee_authority_hash", "canonical_decision_position_authority_fee_authority_hash_mismatch"),
            ):
                if str(authority.get(field) or "").strip() != str(normalized.get(field) or "").strip():
                    missing.append(f"position_authority.{field}")
                    reason_codes.extend([reason, "canonical_decision_incomplete"])
    complete = not missing
    is_promotion_grade = bool(complete)
    if promotion_grade and not is_promotion_grade:
        reason_codes.append("canonical_decision_not_promotion_grade")
    return CanonicalDecisionValidation(
        canonical_schema_present=True,
        canonical_schema_complete=complete,
        promotion_grade=is_promotion_grade,
        legacy_shallow_decision=False,
        incomplete_canonical_decision=not complete,
        missing_fields=tuple(sorted(set(missing))),
        reason_codes=tuple(sorted(set(reason_codes))),
    )


def validate_promotion_artifact(payload: dict[str, Any]) -> CanonicalDecisionValidation:
    """Strict promotion-grade validation for canonical runtime/research artifacts."""
    return validate_canonical_decision_payload(payload, promotion_grade=True)


def _promotion_artifact_provenance_failures(
    payload: dict[str, Any],
    normalized: dict[str, Any],
) -> list[tuple[str, str]]:
    provenance_payload = dict(payload)
    provenance_payload["execution_plan_bundle_hash"] = normalized.get("execution_plan_bundle_hash")
    provenance_payload["execution_evidence_source"] = normalized.get("execution_evidence_source")
    validation = validate_promotion_artifact_provenance_gate(provenance_payload)
    reason_to_field = {
        "canonical_promotion_compatibility_fallback": "compatibility_fallback",
        "canonical_promotion_legacy_context_planning": "legacy_context_planning_used",
        "canonical_promotion_execution_plan_bundle_missing": "execution_plan_bundle_present",
        "canonical_promotion_execution_plan_bundle_hash_missing": "execution_plan_bundle_hash",
        "canonical_promotion_typed_execution_summary_missing": "typed_execution_summary_present",
        "canonical_promotion_execution_summary_hash_missing": "execution_summary_hash",
        "canonical_promotion_execution_submit_plan_hash_missing": "execution_submit_plan_hash",
        "canonical_promotion_runtime_decision_request_hash_missing": "runtime_decision_request_hash",
        "canonical_promotion_runtime_strategy_set_manifest_hash_missing": "runtime_strategy_set_manifest_hash",
        "canonical_promotion_approved_profile_hash_missing": "approved_profile_hash",
        "canonical_promotion_market_feature_hash_missing": "market_feature_hash",
        "canonical_promotion_final_exit_decision_input_hash_missing": "final_exit_decision_input_hash",
        "canonical_promotion_legacy_context_authority": "decision_authority_source",
        "canonical_promotion_runtime_replay_planning_error": "runtime_replay_planning_error",
        "canonical_promotion_typed_execution_provenance_missing": "execution_evidence_source",
        "canonical_promotion_typed_authority_plane_missing": "authority_plane",
        "canonical_promotion_artifact_grade_not_promotion": "artifact_grade",
        "canonical_promotion_rejection_reason_present": "promotion_rejection_reason",
        "canonical_promotion_legacy_contract_version": "decision_contract_version",
        "canonical_promotion_forged_or_unverified_typed_evidence": "typed_execution_evidence",
    }
    return [
        (reason_to_field.get(reason, "promotion_provenance"), reason)
        for reason in validation.reason_codes
    ]


def runtime_decision_to_canonical_event(
    decision: Any,
    *,
    market: str,
    interval: str,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    db_data_fingerprint: str = "",
    through_ts_ms: int | None = None,
    decision_ts: int | None = None,
    candle_basis: str = "runtime_closed_candle",
    execution_timing_policy_hash: str = "",
    strategy_version: str = "",
    strategy_decision_contract_version: str = "",
    execution_plan_bundle: ExecutionPlanBundle | None = None,
    runtime_replay_planning_error: str = "",
) -> CanonicalDecisionEvent:
    context = dict(getattr(decision, "context", {}) or {})
    final_signal = str(getattr(decision, "signal", context.get("final_signal", "HOLD")) or "HOLD").upper()
    raw_signal = str(context.get("raw_signal") or context.get("base_signal") or final_signal).upper()
    entry = context.get("entry") if isinstance(context.get("entry"), dict) else {}
    filters = context.get("filters") if isinstance(context.get("filters"), dict) else {}
    cost_edge = filters.get("cost_edge") if isinstance(filters.get("cost_edge"), dict) else {}
    exit_context = context.get("exit") if isinstance(context.get("exit"), dict) else {}
    position_gate = context.get("position_gate") if isinstance(context.get("position_gate"), dict) else {}
    order_rules = position_gate.get("order_rules") or context.get("order_rules") or {}
    market_regime = context.get("market_regime") if isinstance(context.get("market_regime"), dict) else {}
    fee_authority = context.get("fee_authority") if isinstance(context.get("fee_authority"), dict) else {}
    stable_fee_model = {
        "bid_fee": fee_authority.get("bid_fee"),
        "ask_fee": fee_authority.get("ask_fee"),
        "fee_source": fee_authority.get("fee_source"),
        "degraded": fee_authority.get("degraded"),
        "degraded_reason": fee_authority.get("degraded_reason"),
    }
    block_reason = str(
        context.get("entry_block_reason")
        or entry.get("entry_reason")
        or context.get("regime_block_reason")
        or getattr(decision, "reason", "")
        or ""
    )
    blocked_filters = tuple(str(item) for item in context.get("blocked_filters") or ())
    blocked = bool(final_signal == "HOLD" and (raw_signal in {"BUY", "SELL"} or block_reason))
    comparison_position_state = _runtime_comparison_position_state(
        position_gate=position_gate,
        position_state=context.get("position_state") if isinstance(context.get("position_state"), dict) else {},
    )
    if (
        final_signal == "HOLD"
        and comparison_position_state.get("comparison_state") == "open_exposure"
        and not str(exit_context.get("rule") or "").strip()
    ):
        block_reason = "position held: no exit rule triggered"
        blocked = True
    flat_comparison_state = comparison_position_state == CANONICAL_FLAT_POSITION_STATE
    position_state_hash = canonical_payload_hash(comparison_position_state)
    order_rules_hash = canonical_payload_hash(order_rules)
    fee_authority_hash = canonical_payload_hash(stable_fee_model)
    payload = {
        "decision_contract_version": CANONICAL_DECISION_CONTRACT_VERSION,
        "strategy_name": str(context.get("strategy") or ""),
        "strategy_version": strategy_version or str(context.get("strategy_version") or ""),
        "strategy_decision_contract_version": str(
            strategy_decision_contract_version
            or context.get("strategy_decision_contract_version")
            or context.get("strategy_contract_version")
            or ""
        ),
        "profile_content_hash": profile_content_hash or str(context.get("approved_profile_hash") or ""),
        "candidate_profile_hash": str(context.get("candidate_profile_hash") or ""),
        "dataset_content_hash": dataset_content_hash or str(context.get("dataset_content_hash") or ""),
        "db_data_fingerprint": db_data_fingerprint,
        "market": str(market),
        "interval": str(interval),
        "signal_timestamp": str(context.get("ts") or ""),
        "candle_ts": int(context.get("ts") or 0),
        "through_ts_ms": through_ts_ms,
        "candle_basis": candle_basis,
        "decision_ts": decision_ts,
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "side": final_signal,
        "blocked": blocked,
        "block_reason": block_reason if blocked else "",
        "blocked_filters": blocked_filters,
        "feature_snapshot": context.get("feature_snapshot") or context.get("features") or {},
        "strategy_specific_payload": (
            dict(context.get("strategy_specific_payload"))
            if isinstance(context.get("strategy_specific_payload"), dict)
            else _legacy_sma_strategy_payload(
                {
                    "prev_s": context.get("prev_s"),
                    "prev_l": context.get("prev_l"),
                    "curr_s": context.get("curr_s"),
                    "curr_l": context.get("curr_l"),
                    "gap_ratio": context.get("gap_ratio"),
                    "range_ratio": _range_ratio_from_filters(filters),
                    "expected_edge_ratio": cost_edge.get("value"),
                    "required_edge_ratio": cost_edge.get("threshold"),
                }
            )
        ),
        "strategy_diagnostics_namespace": str(context.get("strategy") or ""),
        "strategy_diagnostics": context.get("strategy_diagnostics") if isinstance(context.get("strategy_diagnostics"), dict) else {},
        "fee_authority_hash": fee_authority_hash,
        "fee_model_hash": fee_authority_hash,
        "slippage_model_hash": canonical_payload_hash(context.get("position_lot_interpretation_costs") or {}),
        "order_rules_hash": order_rules_hash,
        "market_regime": market_regime.get("composite_regime") or context.get("current_regime") or "",
        "regime_decision": context.get("regime_decision") or "",
        "regime_block_reason": context.get("regime_block_reason") or "",
        "position_state_hash": position_state_hash,
        "entry_allowed": position_gate.get("entry_allowed"),
        "exit_allowed": position_gate.get("exit_allowed"),
        "dust_state": "flat"
        if flat_comparison_state
        else position_gate.get("dust_state") or context.get("dust_classification") or "",
        "effective_flat": position_gate.get("effective_flat") if "effective_flat" in position_gate else context.get("effective_flat"),
        "normalized_exposure_active": position_gate.get("normalized_exposure_active")
        if "normalized_exposure_active" in position_gate
        else context.get("normalized_exposure_active"),
        "exit_rule": exit_context.get("rule"),
        "exit_reason": exit_context.get("reason") or "",
        "exit_evaluations_hash": canonical_payload_hash(exit_context.get("evaluations") or ()),
        "execution_timing_policy_hash": execution_timing_policy_hash,
        "policy_contract_hash": str(context.get("policy_contract_hash") or ""),
        "policy_input_hash": str(context.get("policy_input_hash") or ""),
        "policy_decision_hash": str(context.get("policy_decision_hash") or ""),
        "decision_input_bundle_hash": str(context.get("decision_input_bundle_hash") or ""),
        "decision_input_contract_hash": str(context.get("decision_input_contract_hash") or ""),
        "decision_input_bundle_payload_hash": str(context.get("decision_input_bundle_payload_hash") or ""),
        "market_snapshot_hash": str(context.get("market_snapshot_hash") or ""),
        "market_feature_hash": str(
            context.get("market_feature_hash") or context.get("canonical_feature_projection_hash") or ""
        ),
        "canonical_feature_projection_hash": str(
            context.get("canonical_feature_projection_hash") or context.get("market_feature_hash") or ""
        ),
        "position_snapshot_hash": str(context.get("position_snapshot_hash") or ""),
        "execution_constraints_hash": str(context.get("execution_constraints_hash") or ""),
        "policy_config_hash": str(context.get("policy_config_hash") or ""),
        "exit_policy_config_hash": str(context.get("exit_policy_config_hash") or ""),
        "final_exit_decision_input_hash": str(context.get("final_exit_decision_input_hash") or ""),
        "snapshot_projector_version": str(context.get("snapshot_projector_version") or ""),
        "snapshot_projector_hash": str(context.get("snapshot_projector_hash") or ""),
        "replay_fingerprint_hash": canonical_payload_hash(context.get("replay_fingerprint") or {}),
        "strategy_evaluation_provenance": (
            context.get("strategy_evaluation_provenance")
            if isinstance(context.get("strategy_evaluation_provenance"), dict)
            else None
        ),
        "runtime_decision_request_hash": str(
            context.get("runtime_decision_request_hash")
            or sha256_prefixed(
                {
                    "runtime_decision_request": {
                        "strategy_name": str(context.get("strategy") or ""),
                        "strategy_instance_id": str(context.get("strategy_instance_id") or ""),
                        "market": str(market),
                        "interval": str(interval),
                        "through_ts_ms": through_ts_ms,
                        "policy_decision_hash": str(context.get("policy_decision_hash") or ""),
                    }
                }
            )
        ),
        "runtime_strategy_set_manifest_hash": str(
            context.get("runtime_strategy_set_manifest_hash")
            or sha256_prefixed(
                {
                    "runtime_strategy_set_manifest": {
                        "strategy_name": str(context.get("strategy") or ""),
                        "strategy_instance_id": str(context.get("strategy_instance_id") or ""),
                        "market": str(market),
                        "interval": str(interval),
                        "source": "runtime_replay_single_strategy",
                    }
                }
            )
        ),
        "approved_profile_hash": str(context.get("approved_profile_hash") or profile_content_hash or ""),
    }
    execution_evidence = _runtime_execution_plan_evidence(
        execution_plan_bundle=execution_plan_bundle,
        context=context,
        final_signal=final_signal,
        block_reason=block_reason,
        comparison_position_state=comparison_position_state,
        exit_context=exit_context,
        runtime_replay_planning_error=runtime_replay_planning_error,
    )
    payload.update(
        {
            "execution_summary_hash": execution_evidence["execution_summary_hash"],
            "execution_submit_plan_hash": execution_evidence["execution_submit_plan_hash"],
            "final_action": execution_evidence["final_action"],
            "submit_expected": bool(execution_evidence["submit_expected"]),
            "pre_submit_proof_status": execution_evidence["pre_submit_proof_status"],
            "execution_block_reason": execution_evidence["execution_block_reason"],
            "submit_plan_source": execution_evidence["submit_plan_source"],
            "submit_plan_authority": execution_evidence["submit_plan_authority"],
            "execution_engine": execution_evidence["execution_engine"],
            "execution_plan_bundle_hash": execution_evidence["execution_plan_bundle_hash"],
            "execution_evidence_source": execution_evidence["execution_evidence_source"],
            "typed_execution_summary_present": bool(execution_evidence["typed_execution_summary_present"]),
            "execution_plan_bundle_evidence": execution_evidence.get("execution_plan_bundle_evidence"),
            "typed_execution_summary_evidence": execution_evidence.get("typed_execution_summary_evidence"),
            "execution_submit_plan_evidence": execution_evidence.get("execution_submit_plan_evidence"),
            "typed_no_submit_proof": execution_evidence.get("typed_no_submit_proof"),
            "compatibility_fallback": bool(context.get("compatibility_fallback")),
            "legacy_context_planning_used": bool(context.get("legacy_context_planning_used")),
            "artifact_grade": str(execution_evidence["artifact_grade"]),
            "authority_plane": str(execution_evidence["authority_plane"]),
            "promotion_rejection_reason": str(execution_evidence["promotion_rejection_reason"]),
        }
    )
    payload.update(execution_evidence["observability"])
    payload["feature_snapshot_hash"] = str(
        context.get("feature_snapshot_hash") or canonical_payload_hash(payload["feature_snapshot"])
    )
    payload["strategy_behavior_payload"] = {
        "strategy_name": payload["strategy_name"],
        "strategy_version": payload["strategy_version"],
        "strategy_decision_contract_version": payload["strategy_decision_contract_version"],
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "strategy_specific_payload": payload["strategy_specific_payload"],
    }
    payload["strategy_behavior_hash"] = canonical_payload_hash(payload["strategy_behavior_payload"])
    payload["position_authority"] = runtime_position_authority_snapshot(
        position_gate=position_gate,
        order_rules_hash=order_rules_hash,
        fee_authority_hash=fee_authority_hash,
        position_state_hash=position_state_hash,
    ).as_dict()
    normalized = normalize_canonical_decision(payload)
    normalized["position_authority"] = payload["position_authority"]
    for key in (
        "decision_authority_source",
        "decision_envelope_present",
        "execution_plan_bundle_present",
        "execution_plan_bundle_hash",
        "execution_evidence_source",
        "typed_execution_summary_present",
        "execution_plan_bundle_evidence",
        "typed_execution_summary_evidence",
        "execution_submit_plan_evidence",
        "typed_no_submit_proof",
        "compatibility_fallback",
        "legacy_context_planning_used",
        "persistence_context_authoritative",
        "runtime_replay_planning_error",
        "execution_plan_status",
        "execution_plan_reason_code",
        "artifact_grade",
        "authority_plane",
        "promotion_rejection_reason",
        "strategy_evaluation_provenance",
    ):
        if key in payload:
            normalized[key] = payload[key]
    return CanonicalDecisionEvent(normalized)


def research_decision_to_canonical_event(
    decision: dict[str, Any],
    *,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    execution_timing_policy_hash: str = "",
) -> CanonicalDecisionEvent:
    payload = dict(decision)
    payload.setdefault("decision_contract_version", CANONICAL_DECISION_CONTRACT_VERSION)
    if int(payload.get("decision_contract_version") or 0) >= CANONICAL_DECISION_CONTRACT_VERSION:
        payload.setdefault(
            "strategy_version",
            payload.get("strategy_spec", {}).get("strategy_version")
            if isinstance(payload.get("strategy_spec"), dict)
            else "",
        )
        payload.setdefault(
            "strategy_decision_contract_version",
            payload.get("strategy_decision_contract_version") or payload.get("strategy_contract_version") or "",
        )
    else:
        payload.setdefault("strategy_contract_version", LEGACY_STRATEGY_CONTRACT_VERSION_V1)
    payload["profile_content_hash"] = profile_content_hash or str(payload.get("profile_content_hash") or "")
    payload["dataset_content_hash"] = dataset_content_hash or str(payload.get("dataset_content_hash") or "")
    payload["execution_timing_policy_hash"] = execution_timing_policy_hash or str(
        payload.get("execution_timing_policy_hash") or ""
    )
    payload["policy_contract_hash"] = str(payload.get("policy_contract_hash") or "")
    payload["policy_input_hash"] = str(payload.get("policy_input_hash") or "")
    payload["policy_decision_hash"] = str(payload.get("policy_decision_hash") or "")
    payload["decision_input_bundle_hash"] = str(payload.get("decision_input_bundle_hash") or "")
    payload["decision_input_contract_hash"] = str(payload.get("decision_input_contract_hash") or "")
    payload["decision_input_bundle_payload_hash"] = str(payload.get("decision_input_bundle_payload_hash") or "")
    payload["market_snapshot_hash"] = str(payload.get("market_snapshot_hash") or "")
    payload["market_feature_hash"] = str(payload.get("market_feature_hash") or payload.get("canonical_feature_projection_hash") or "")
    payload["canonical_feature_projection_hash"] = str(payload.get("canonical_feature_projection_hash") or payload.get("market_feature_hash") or "")
    payload["position_snapshot_hash"] = str(payload.get("position_snapshot_hash") or "")
    payload["execution_constraints_hash"] = str(payload.get("execution_constraints_hash") or "")
    payload["policy_config_hash"] = str(payload.get("policy_config_hash") or "")
    payload["exit_policy_config_hash"] = str(payload.get("exit_policy_config_hash") or "")
    payload["final_exit_decision_input_hash"] = str(payload.get("final_exit_decision_input_hash") or "")
    payload["snapshot_projector_version"] = str(payload.get("snapshot_projector_version") or "")
    payload["snapshot_projector_hash"] = str(payload.get("snapshot_projector_hash") or "")
    payload["runtime_decision_request_hash"] = str(payload.get("runtime_decision_request_hash") or "")
    payload["runtime_strategy_set_manifest_hash"] = str(payload.get("runtime_strategy_set_manifest_hash") or "")
    payload["approved_profile_hash"] = str(payload.get("approved_profile_hash") or "")
    payload["execution_plan_bundle_hash"] = str(payload.get("execution_plan_bundle_hash") or "")
    payload["execution_summary_hash"] = str(payload.get("execution_summary_hash") or "")
    payload["execution_submit_plan_hash"] = str(payload.get("execution_submit_plan_hash") or "")
    payload["final_action"] = str(payload.get("final_action") or "")
    payload["submit_expected"] = bool(payload.get("submit_expected"))
    payload["pre_submit_proof_status"] = str(payload.get("pre_submit_proof_status") or "")
    payload["execution_block_reason"] = str(payload.get("execution_block_reason") or "")
    payload["submit_plan_source"] = str(payload.get("submit_plan_source") or "")
    payload["submit_plan_authority"] = str(payload.get("submit_plan_authority") or "")
    payload["execution_engine"] = str(payload.get("execution_engine") or "")
    payload["decision_authority_source"] = str(payload.get("decision_authority_source") or "")
    payload["decision_envelope_present"] = bool(payload.get("decision_envelope_present"))
    payload["execution_plan_bundle_present"] = bool(payload.get("execution_plan_bundle_present"))
    payload["execution_evidence_source"] = str(payload.get("execution_evidence_source") or "")
    payload["typed_execution_summary_present"] = bool(payload.get("typed_execution_summary_present"))
    payload["execution_plan_bundle_evidence"] = payload.get("execution_plan_bundle_evidence")
    payload["typed_execution_summary_evidence"] = payload.get("typed_execution_summary_evidence")
    payload["execution_submit_plan_evidence"] = payload.get("execution_submit_plan_evidence")
    payload["typed_no_submit_proof"] = payload.get("typed_no_submit_proof")
    payload["compatibility_fallback"] = bool(payload.get("compatibility_fallback"))
    payload["legacy_context_planning_used"] = bool(payload.get("legacy_context_planning_used"))
    payload["runtime_replay_planning_error"] = str(payload.get("runtime_replay_planning_error") or "")
    payload["execution_plan_status"] = str(payload.get("execution_plan_status") or "")
    payload["execution_plan_reason_code"] = str(payload.get("execution_plan_reason_code") or "")
    payload["artifact_grade"] = str(payload.get("artifact_grade") or "")
    payload["authority_plane"] = str(payload.get("authority_plane") or "")
    payload["promotion_rejection_reason"] = str(payload.get("promotion_rejection_reason") or "")
    normalized = normalize_canonical_decision(payload)
    if isinstance(payload.get("position_authority"), dict):
        normalized["position_authority"] = dict(payload["position_authority"])  # type: ignore[arg-type]
    return CanonicalDecisionEvent(normalized)


def canonical_flat_position_state_hash() -> str:
    return canonical_payload_hash(CANONICAL_FLAT_POSITION_STATE)


def _runtime_comparison_position_state(
    *,
    position_gate: dict[str, Any],
    position_state: dict[str, Any],
) -> dict[str, Any]:
    if _is_flat_no_dust_position_gate(position_gate):
        return dict(CANONICAL_FLAT_POSITION_STATE)
    state_class = classify_runtime_position_state(position_gate)
    if (
        state_class in POSITIVE_EQUIVALENCE_STATE_CLASSES - {"flat_no_dust_no_position"}
        and runtime_state_has_required_lot_native_fields(position_gate, state_class)
    ):
        return lot_native_comparison_position_state(
            {
                **position_gate,
                "state_class": state_class,
                "recovery_blocked": bool(position_gate.get("recovery_blocked")),
                "recovery_block_reason": str(position_gate.get("recovery_block_reason") or "none"),
            }
        )
    return {
        "comparison_state": "runtime_position_state_not_research_comparable",
        "unsupported_reason": _unsupported_position_reason(position_gate),
        "runtime_position_state": _stable_value(position_state),
    }


def _is_flat_no_dust_position_gate(position_gate: dict[str, Any]) -> bool:
    return (
        bool(position_gate.get("entry_allowed")) is True
        and bool(position_gate.get("exit_allowed")) is False
        and str(position_gate.get("dust_state") or "") in {"flat", "no_dust"}
        and bool(position_gate.get("effective_flat")) is True
        and bool(position_gate.get("normalized_exposure_active")) is False
        and int(position_gate.get("open_lot_count") or 0) == 0
        and int(position_gate.get("dust_tracking_lot_count") or 0) == 0
        and int(position_gate.get("sellable_executable_lot_count") or 0) == 0
        and bool(position_gate.get("has_any_position_residue")) is False
    )


def _unsupported_position_reason(position_gate: dict[str, Any]) -> str:
    if str(position_gate.get("dust_state") or "") not in {"", "flat"} or bool(position_gate.get("has_dust_only_remainder")):
        return "research_model_lacks_dust_state"
    if bool(position_gate.get("normalized_exposure_active")) or int(position_gate.get("sellable_executable_lot_count") or 0) > 0:
        return "research_model_lacks_lot_native_authority"
    if bool(position_gate.get("has_any_position_residue")):
        return "research_runtime_state_not_comparable"
    return "research_runtime_state_not_comparable"


@dataclass(frozen=True)
class _ReplayReadiness:
    payload: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


def _runtime_execution_plan_evidence(
    *,
    execution_plan_bundle: Any | None,
    context: dict[str, Any],
    final_signal: str,
    block_reason: str,
    comparison_position_state: dict[str, Any],
    exit_context: dict[str, Any],
    runtime_replay_planning_error: str = "",
) -> dict[str, object]:
    if execution_plan_bundle is not None and execution_plan_bundle.summary is not None:
        submit_plan = execution_plan_bundle.submit_plan
        submit_plan_payload = None if submit_plan is None else submit_plan.as_dict()
        final_submit_plan_payload = None if submit_plan is None else submit_plan.as_final_payload()
        summary_payload = execution_plan_bundle.summary.as_dict()
        execution_engine = str(summary_payload.get("execution_engine") or "")
        bundle_hash = str(execution_plan_bundle.content_hash())
        observability = _runtime_execution_observability(
            execution_plan_bundle=execution_plan_bundle,
            runtime_replay_planning_error=runtime_replay_planning_error,
        )
        no_submit_proof = build_typed_no_submit_proof(summary_payload)
        provenance = {
            "execution_plan_bundle_hash": bundle_hash,
            "execution_plan_bundle_evidence": execution_plan_bundle.as_dict(),
            "typed_execution_summary_evidence": summary_payload,
            "execution_evidence_source": PROMOTION_TYPED_EXECUTION_EVIDENCE_SOURCE,
            "typed_execution_summary_present": True,
            "artifact_grade": PROMOTION_ARTIFACT_GRADE,
            "authority_plane": PROMOTION_AUTHORITY_PLANE,
            "promotion_rejection_reason": str(observability.get("runtime_replay_planning_error") or ""),
        }
        if submit_plan is None:
            return {
                "execution_summary_hash": canonical_payload_hash(summary_payload),
                "execution_submit_plan_hash": canonical_payload_hash(no_submit_proof),
                "final_action": str(summary_payload.get("final_action") or ""),
                "submit_expected": bool(summary_payload.get("submit_expected")),
                "pre_submit_proof_status": str(summary_payload.get("pre_submit_proof_status") or ""),
                "execution_block_reason": str(summary_payload.get("block_reason") or ""),
                "submit_plan_source": "typed_execution_planner",
                "submit_plan_authority": "typed_execution_planner",
                "execution_engine": execution_engine,
                "observability": observability,
                "typed_no_submit_proof": no_submit_proof,
                **provenance,
            }
        return {
            "execution_summary_hash": canonical_payload_hash(summary_payload),
            "execution_submit_plan_hash": canonical_payload_hash(final_submit_plan_payload),
            "final_action": submit_plan.final_action,
            "submit_expected": bool(submit_plan.submit_expected),
            "pre_submit_proof_status": submit_plan.pre_submit_proof_status,
            "execution_block_reason": submit_plan.block_reason,
            "submit_plan_source": submit_plan.source,
            "submit_plan_authority": submit_plan.authority,
            "execution_engine": execution_engine,
            "observability": observability,
            "execution_submit_plan_evidence": final_submit_plan_payload,
            **provenance,
        }

    execution_decision = (
        context.get("execution_decision")
        if isinstance(context.get("execution_decision"), dict)
        else {}
    )
    primary_submit_plan = _primary_execution_submit_plan(execution_decision)
    if execution_decision and final_signal not in {"BUY", "SELL"}:
        return {
            "execution_summary_hash": canonical_payload_hash(execution_decision),
            "execution_submit_plan_hash": (
                canonical_payload_hash(primary_submit_plan) if primary_submit_plan else ""
            ),
            "final_action": str(context.get("final_action") or execution_decision.get("final_action") or ""),
            "submit_expected": bool(
                context.get("submit_expected")
                if "submit_expected" in context
                else execution_decision.get("submit_expected")
            ),
            "pre_submit_proof_status": str(
                context.get("pre_submit_proof_status")
                or execution_decision.get("pre_submit_proof_status")
                or ""
            ),
            "execution_block_reason": str(
                context.get("execution_block_reason")
                or execution_decision.get("block_reason")
                or ""
            ),
            "submit_plan_source": str(context.get("submit_plan_source") or primary_submit_plan.get("source") or ""),
            "submit_plan_authority": str(
                context.get("submit_plan_authority") or primary_submit_plan.get("authority") or ""
            ),
            "execution_engine": str(execution_decision.get("execution_engine") or context.get("execution_engine") or ""),
            "observability": {
                "decision_authority_source": context.get("decision_authority_source", ""),
                "decision_envelope_present": bool(context.get("decision_envelope_present")),
                "execution_plan_bundle_present": bool(context.get("execution_plan_bundle_present")),
                "execution_plan_bundle_hash": str(context.get("execution_plan_bundle_hash") or ""),
                "persistence_context_authoritative": int(context.get("persistence_context_authoritative") or 0),
                "runtime_replay_planning_error": runtime_replay_planning_error,
            },
            "execution_plan_bundle_hash": str(context.get("execution_plan_bundle_hash") or ""),
            "execution_evidence_source": "diagnostic_context_fallback",
            "typed_execution_summary_present": False,
            "artifact_grade": "diagnostic_only",
            "authority_plane": "compatibility_context",
            "promotion_rejection_reason": "context_fallback_execution_evidence",
            "execution_plan_bundle_evidence": None,
            "typed_execution_summary_evidence": None,
            "execution_submit_plan_evidence": None,
            "typed_no_submit_proof": None,
        }

    execution_block_reason = block_reason or "none"
    pre_submit_proof_status = "not_required"
    final_action = "HOLD" if execution_block_reason == "none" else "BLOCK_RESEARCH_NO_SUBMIT"
    if final_signal in {"BUY", "SELL"}:
        execution_block_reason = runtime_replay_planning_error or "runtime_replay_execution_readiness_unavailable"
        final_action = "BLOCK_RUNTIME_REPLAY_EXECUTION"
        pre_submit_proof_status = "failed"
    elif (
        final_signal == "HOLD"
        and comparison_position_state.get("comparison_state") == "open_exposure"
        and not str(exit_context.get("rule") or "").strip()
    ):
        execution_block_reason = "position held: no exit rule triggered"
        final_action = "BLOCK_RESEARCH_NO_SUBMIT"
    no_submit_summary = {
        "final_action": final_action,
        "submit_expected": False,
        "pre_submit_proof_status": pre_submit_proof_status,
        "block_reason": execution_block_reason,
        "primary_submit_plan": None,
        "execution_engine": "none",
    }
    no_submit_proof = build_typed_no_submit_proof(no_submit_summary)
    return {
        "execution_summary_hash": canonical_payload_hash(no_submit_summary),
        "execution_submit_plan_hash": canonical_payload_hash(no_submit_proof),
        "final_action": final_action,
        "submit_expected": False,
        "pre_submit_proof_status": pre_submit_proof_status,
        "execution_block_reason": execution_block_reason,
        "submit_plan_source": "none",
        "submit_plan_authority": "none",
            "execution_engine": "none",
            "observability": {
                "decision_authority_source": context.get("decision_authority_source", ""),
                "decision_envelope_present": bool(context.get("decision_envelope_present")),
                "execution_plan_bundle_present": False,
                "execution_plan_bundle_hash": "",
                "persistence_context_authoritative": int(context.get("persistence_context_authoritative") or 0),
                "runtime_replay_planning_error": runtime_replay_planning_error,
                "execution_plan_status": "ERROR" if runtime_replay_planning_error else "",
                "execution_plan_reason_code": execution_block_reason if final_signal in {"BUY", "SELL"} else "",
            },
            "execution_plan_bundle_hash": "",
            "execution_evidence_source": "typed_execution_plan_bundle_missing_fail_closed",
            "typed_execution_summary_present": False,
            "artifact_grade": "diagnostic_only",
            "authority_plane": "runtime_replay_fail_closed",
            "promotion_rejection_reason": (
                runtime_replay_planning_error
                or "typed_execution_plan_bundle_missing"
            ),
            "execution_plan_bundle_evidence": None,
            "typed_execution_summary_evidence": None,
            "execution_submit_plan_evidence": None,
            "typed_no_submit_proof": None,
        }


def _runtime_execution_observability(
    *,
    execution_plan_bundle: Any,
    runtime_replay_planning_error: str,
) -> dict[str, object]:
    status = execution_plan_bundle.status
    return {
        "decision_authority_source": execution_plan_bundle.persistence_context.get(
            "decision_authority_source", ""
        ),
        "decision_envelope_present": bool(
            execution_plan_bundle.persistence_context.get("decision_envelope_present")
        ),
        "execution_plan_bundle_present": True,
        "execution_plan_bundle_hash": str(execution_plan_bundle.content_hash()),
        "persistence_context_authoritative": int(
            execution_plan_bundle.persistence_context.get("persistence_context_authoritative") or 0
        ),
        "runtime_replay_planning_error": runtime_replay_planning_error
        or str(execution_plan_bundle.planning_error or ""),
        "execution_plan_status": "" if status is None else status.status,
        "execution_plan_reason_code": "" if status is None else status.reason_code,
    }


def _runtime_replay_readiness_payload(
    conn: sqlite3.Connection,
    result: Any,
) -> dict[str, object]:
    position = result.decision.position_snapshot
    cash_available: float | None = None
    try:
        row = conn.execute("SELECT cash_available FROM portfolio WHERE id=1").fetchone()
        if row is not None:
            cash_available = float(row["cash_available"] if "cash_available" in row.keys() else row[0])
    except (sqlite3.Error, TypeError, ValueError, KeyError, AttributeError):
        cash_available = None
    payload: dict[str, object] = {
        "residual_inventory_policy_allows_run": True,
        "residual_inventory_policy_allows_buy": True,
        "residual_inventory_policy_allows_sell": True,
        "residual_inventory_state": "NONE",
        "pair": str(result.base_context.get("pair") or ""),
        "unresolved_open_order_count": 0,
        "submit_unknown_count": 0,
        "open_order_count": 0,
        "recovery_required_count": 0,
        "accounting_projection_ok": True,
        "idempotency_scope": "runtime_replay_read_only",
        "total_effective_exposure_qty": float(position.qty_open),
        "total_effective_exposure_notional_krw": (
            max(0.0, float(position.qty_open) * float(result.market_price))
            if bool(position.has_executable_exposure)
            else 0.0
        ),
    }
    if cash_available is not None:
        payload["cash_available"] = cash_available
    if bool(position.has_executable_exposure):
        payload["residual_sell_candidate"] = {
            "qty": float(position.qty_open),
            "notional": max(0.0, float(position.qty_open) * float(result.market_price)),
            "source": "runtime_replay_position_snapshot",
            "classes": ["OPEN_EXPOSURE"],
            "exchange_sellable": bool(position.exit_allowed),
            "allowed_by_policy": bool(position.exit_allowed),
            "requires_final_pre_submit_proof": False,
        }
    return payload


def _runtime_replay_target_state_resolver(
    conn: sqlite3.Connection,
    *,
    readiness_payload: dict[str, object],
    reference_price: float | None,
    raw_signal: str,
    updated_ts: int,
) -> dict[str, object]:
    previous_exposure: float | None = None
    try:
        row = conn.execute(
            "SELECT target_exposure_krw FROM target_position_state WHERE pair=?",
            (str(readiness_payload.get("pair") or ""),),
        ).fetchone()
        if row is not None:
            previous_exposure = float(row[0])
    except (sqlite3.Error, TypeError, ValueError):
        previous_exposure = None
    return {
        "previous_target_exposure_krw": previous_exposure,
        "target_policy_metadata": {
            "target_policy_action": "runtime_replay_read_only",
            "target_origin": "runtime_replay",
            "target_strategy_signal_source": str(raw_signal or "HOLD").upper(),
        },
        "target_state": None,
    }


def build_runtime_replay_execution_plan_bundle(
    conn: sqlite3.Connection,
    result: Any,
    *,
    readiness_payload: dict[str, object] | None = None,
    readiness_payload_builder: Any = _runtime_replay_readiness_payload,
) -> Any:
    from .decision_envelope import DecisionEnvelope
    from .run_loop_execution_planner import ExecutionPlanBundle, ExecutionPlanner, ExecutionPlanStatus

    envelope = DecisionEnvelope.from_runtime_result(result)
    if readiness_payload is None:
        if readiness_payload_builder is None:
            return ExecutionPlanBundle(
                summary=None,
                submit_plan=None,
                persistence_context={
                    **envelope.as_persistence_context(),
                    "execution_plan_bundle_present": False,
                    "execution_block_reason": "runtime_replay_execution_readiness_unavailable",
                },
                readiness_payload={},
                target_policy_metadata={},
                planning_error="runtime_replay_execution_readiness_unavailable",
                status=ExecutionPlanStatus(
                    status="ERROR",
                    reason_code="runtime_replay_execution_readiness_unavailable",
                    reason="runtime replay readiness payload was not supplied or modeled",
                ),
            )
        readiness_payload = dict(readiness_payload_builder(conn, result))
    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _ReplayReadiness(dict(readiness_payload or {})),
        target_state_resolver=_runtime_replay_target_state_resolver,
    )
    return planner.plan_envelope(conn, envelope, updated_ts=int(result.candle_ts))


def export_runtime_replay_decisions(
    *,
    conn: Any,
    strategy: Any,
    through_ts_list: list[int],
    market: str,
    interval: str,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    db_data_fingerprint: str = "",
    candle_basis: str = "runtime_closed_candle",
    execution_timing_policy_hash: str = "",
    strategy_version: str = "",
    strategy_decision_contract_version: str = "",
    replay_readiness_builder: Any = _runtime_replay_readiness_payload,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for through_ts_ms in through_ts_list:
        if hasattr(strategy, "decide_runtime_snapshot"):
            runtime_result = strategy.decide_runtime_snapshot(
                conn,
                through_ts_ms=int(through_ts_ms),
            )
            if runtime_result is None:
                continue
            execution_plan_bundle = build_runtime_replay_execution_plan_bundle(
                conn,
                runtime_result,
                readiness_payload_builder=replay_readiness_builder,
            )
            if hasattr(runtime_result, "legacy_strategy_decision"):
                decision = runtime_result.legacy_strategy_decision()
            else:
                from bithumb_bot.strategy.base import StrategyDecision

                legacy_payload = runtime_result.as_legacy_dict()
                decision = StrategyDecision(
                    signal=str(legacy_payload.get("final_signal") or legacy_payload.get("signal") or "HOLD"),
                    reason=str(legacy_payload.get("final_reason") or legacy_payload.get("reason") or ""),
                    context=legacy_payload,
                )
            replay_signal_candidates = {
                str(runtime_result.decision.raw_signal or "").upper(),
                str(runtime_result.decision.final_signal or "").upper(),
            }
            include_hold_execution_context = bool(
                getattr(strategy, "include_hold_execution_context_in_replay", False)
            )
            position_snapshot = getattr(runtime_result.decision, "position_snapshot", None)
            include_hold_execution_context = include_hold_execution_context or bool(
                getattr(position_snapshot, "has_executable_exposure", False)
            )
            if execution_plan_bundle.persistence_context and replay_signal_candidates & {"BUY", "SELL"}:
                decision = replace(decision, context=dict(execution_plan_bundle.persistence_context))
                runtime_replay_planning_error = str(execution_plan_bundle.planning_error or "")
            elif execution_plan_bundle.persistence_context and include_hold_execution_context:
                decision = replace(
                    decision,
                    context={**dict(decision.context), **dict(execution_plan_bundle.persistence_context)},
                )
                runtime_replay_planning_error = str(execution_plan_bundle.planning_error or "")
            else:
                execution_plan_bundle = None
                runtime_replay_planning_error = ""
        elif str(getattr(strategy, "name", "") or "").strip().lower() == "sma_with_filter":
            from .runtime_sma_snapshot import decide_sma_with_filter_snapshot_from_db

            decision = decide_sma_with_filter_snapshot_from_db(
                conn,
                strategy,
                through_ts_ms=int(through_ts_ms),
            )
            execution_plan_bundle = None
            runtime_replay_planning_error = ""
        else:
            decision = strategy.decide(conn, through_ts_ms=int(through_ts_ms))
            execution_plan_bundle = None
            runtime_replay_planning_error = ""
        if decision is None:
            continue
        events.append(
            runtime_decision_to_canonical_event(
                decision,
                market=market,
                interval=interval,
                profile_content_hash=profile_content_hash,
                dataset_content_hash=dataset_content_hash,
                db_data_fingerprint=db_data_fingerprint,
                through_ts_ms=int(through_ts_ms),
                candle_basis=candle_basis,
                execution_timing_policy_hash=execution_timing_policy_hash,
                strategy_version=strategy_version,
                strategy_decision_contract_version=strategy_decision_contract_version,
                execution_plan_bundle=execution_plan_bundle,
                runtime_replay_planning_error=runtime_replay_planning_error,
            ).as_dict()
        )
    return events


def export_research_decisions(
    decisions: list[dict[str, Any]],
    *,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    execution_timing_policy_hash: str = "",
) -> list[dict[str, Any]]:
    return [
        research_decision_to_canonical_event(
            item,
            profile_content_hash=profile_content_hash,
            dataset_content_hash=dataset_content_hash,
            execution_timing_policy_hash=execution_timing_policy_hash,
        ).as_dict()
        for item in decisions
    ]


def _range_ratio_from_filters(filters: dict[str, Any]) -> object:
    volatility = filters.get("volatility") if isinstance(filters.get("volatility"), dict) else {}
    return volatility.get("value")


def _strategy_feature_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("feature_snapshot"), dict):
        return _stable_value(payload["feature_snapshot"])  # type: ignore[return-value]
    if isinstance(payload.get("features"), dict):
        return _stable_value(payload["features"])  # type: ignore[return-value]
    legacy = _legacy_sma_strategy_payload(payload)
    return legacy


def _strategy_specific_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("strategy_specific_payload"), dict):
        return _stable_value(payload["strategy_specific_payload"])  # type: ignore[return-value]
    return _legacy_sma_strategy_payload(payload)


def _strategy_behavior_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("strategy_behavior_payload"), dict):
        return _stable_value(payload["strategy_behavior_payload"])  # type: ignore[return-value]
    return {
        "strategy_name": str(payload.get("strategy_name") or ""),
        "strategy_version": str(payload.get("strategy_version") or ""),
        "strategy_decision_contract_version": str(
            payload.get("strategy_decision_contract_version")
            or payload.get("strategy_contract_version")
            or ""
        ),
        "raw_signal": str(payload.get("raw_signal") or "").upper(),
        "final_signal": str(payload.get("final_signal") or payload.get("side") or "").upper(),
        "strategy_specific_payload": _strategy_specific_payload(payload),
    }


def _legacy_sma_strategy_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in (
        "prev_s",
        "prev_l",
        "curr_s",
        "curr_l",
        "gap_ratio",
        "range_ratio",
        "expected_edge_ratio",
        "required_edge_ratio",
    ):
        value = payload.get(field)
        if value not in (None, ""):
            out[field] = _canonical_field_value(field, value)
    return out


def _canonical_field_value(field: str, value: object) -> object:
    if field in {
        "decision_contract_version",
        "candle_ts",
        "through_ts_ms",
        "decision_ts",
    }:
        if value in (None, ""):
            return None
        return int(value)  # type: ignore[arg-type]
    if field in {
        "blocked",
        "entry_allowed",
        "exit_allowed",
        "effective_flat",
        "normalized_exposure_active",
        "submit_expected",
        "decision_envelope_present",
        "execution_plan_bundle_present",
        "typed_execution_summary_present",
        "compatibility_fallback",
        "legacy_context_planning_used",
    }:
        if value is None:
            return None
        return bool(value)
    if field in {"blocked_filters"}:
        return tuple(str(item) for item in (value or ()))  # type: ignore[union-attr]
    if field in {
        "execution_plan_bundle_evidence",
        "typed_execution_summary_evidence",
        "execution_submit_plan_evidence",
        "typed_no_submit_proof",
    }:
        return _stable_value(value)
    if field in {"prev_s", "prev_l", "curr_s", "curr_l", "gap_ratio", "range_ratio", "expected_edge_ratio", "required_edge_ratio"}:
        if value in (None, ""):
            return None
        return float(value)  # type: ignore[arg-type]
    return "" if value is None else str(value)


def _primary_execution_submit_plan(execution_decision: object) -> dict[str, Any]:
    if not isinstance(execution_decision, dict):
        return {}
    for field in ("target_submit_plan", "residual_submit_plan", "buy_submit_plan"):
        value = execution_decision.get(field)
        if isinstance(value, dict):
            return _stable_value(value)  # type: ignore[return-value]
    return {}


def _canonical_required_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, (list, tuple, dict)):
        return len(value) == 0
    return str(value).strip() == ""


def order_rules_snapshot_payload(resolution_or_rules: object, *, pair: str = "") -> dict[str, object]:
    rules = getattr(resolution_or_rules, "rules", resolution_or_rules)
    payload = asdict(rules) if hasattr(rules, "__dataclass_fields__") else dict(rules or {})  # type: ignore[arg-type]
    source = getattr(resolution_or_rules, "source", None)
    if isinstance(source, dict):
        payload["rule_source"] = {str(key): str(value) for key, value in sorted(source.items())}
    if pair:
        payload["pair"] = str(pair)
    return _stable_value(payload)  # type: ignore[return-value]


def _stable_value(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _stable_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_stable_value(item) for item in value]
    return value
