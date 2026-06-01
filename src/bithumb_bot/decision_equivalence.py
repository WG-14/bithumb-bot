from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical_decision import (
    CANONICAL_DECISION_SCHEMA_FIELDS,
    CANONICAL_DECISION_COMPARISON_CONTRACT_VERSION,
    LEGACY_CANONICAL_DECISION_COMPARISON_CONTRACT_VERSION_V1,
    LEGACY_CANONICAL_DECISION_SCHEMA_FIELDS_V1,
    is_canonical_decision,
    is_canonical_decision_v2,
    normalize_canonical_decision,
    validate_canonical_decision_payload,
)
from .position_authority import classify_decision_position_state, position_authority_supports_positive_equivalence
from .evidence_safety import smoke_only_evidence_rejection_reasons
from .lifecycle_evidence import (
    CanonicalLifecycleEvidenceBundle,
    validate_lifecycle_evidence_scope,
)
from .research.hashing import content_hash_payload, sha256_prefixed


DECISION_EQUIVALENCE_SCHEMA_VERSION = 2
CANONICAL_COMPARISON_CONTRACT_VERSION = CANONICAL_DECISION_COMPARISON_CONTRACT_VERSION
LEGACY_CANONICAL_COMPARISON_CONTRACT_VERSION = LEGACY_CANONICAL_DECISION_COMPARISON_CONTRACT_VERSION_V1
LEGACY_COMPARISON_CONTRACT_VERSION = "legacy_shallow_v1"
DECISION_EQUIVALENCE_HASH_FIELD = "content_hash"
DECISION_EQUIVALENCE_HASH_EXCLUDED_FIELDS = frozenset({DECISION_EQUIVALENCE_HASH_FIELD, "generated_at"})
DECISION_EXPORT_HASH_FIELD = "content_hash"
DECISION_EXPORT_HASH_EXCLUDED_FIELDS = frozenset({DECISION_EXPORT_HASH_FIELD, "generated_at"})
LEGACY_DECISION_FIELDS = (
    "signal_timestamp",
    "candle_basis",
    "side",
    "strategy_name",
    "profile_content_hash",
    "market",
    "interval",
    "fee_model_hash",
    "slippage_model_hash",
    "blocked",
    "block_reason",
)
CANONICAL_EQUIVALENCE_FIELDS_V2 = tuple(
    field
    for field in CANONICAL_DECISION_SCHEMA_FIELDS
    if field
    not in {
        # These are artifact/provenance or source-timing diagnostics. The
        # semantic fields they derive from remain compared directly. For v2,
        # strategy-owned behavior drift is represented by strategy_behavior_hash;
        # feature_snapshot_hash is diagnostic provenance and is intentionally not
        # promotion-equivalence authority.
        "decision_ts",
        "approved_profile_hash",
        "db_data_fingerprint",
        "replay_fingerprint_hash",
        "runtime_decision_request_hash",
        "runtime_strategy_set_manifest_hash",
        "execution_plan_bundle_hash",
        "feature_snapshot_hash",
        "decision_envelope_present",
        "execution_plan_bundle_present",
        "execution_evidence_source",
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
        "market_regime",
        "regime_decision",
        "regime_block_reason",
        "decision_input_bundle_payload_hash",
    }
)
CANONICAL_EQUIVALENCE_FIELDS = CANONICAL_EQUIVALENCE_FIELDS_V2
LEGACY_CANONICAL_EQUIVALENCE_FIELDS_V1 = tuple(
    field
    for field in LEGACY_CANONICAL_DECISION_SCHEMA_FIELDS_V1
    if field
    not in {
        "decision_ts",
        "db_data_fingerprint",
        "feature_hash",
        "replay_fingerprint_hash",
    }
)
DIAGNOSTIC_DRIFT_FIELDS = (
    "market",
    "interval",
    "side",
    "strategy_name",
    "profile_content_hash",
    "fee_model_hash",
    "slippage_model_hash",
    "blocked",
)
DECISION_EQUIVALENCE_OUTCOMES = (
    "PASS_POSITIVE_EQUIVALENCE",
    "FAIL_CLOSED_UNMODELED_STATE",
    "FAIL_ACTUAL_DRIFT",
    "FAIL_INCOMPLETE_CANONICAL_PAYLOAD",
    "FAIL_EXPORT_BINDING",
)
STATE_COVERAGE_CLASSES = (
    "flat_no_dust_no_position",
    "open_exposure",
    "reserved_exit_pending",
    "dust_only",
    "non_executable_position",
    "recovery_blocked",
    "runtime_position_state_not_research_comparable",
    "research_model_lacks_lot_native_authority",
)


@dataclass(frozen=True)
class DecisionEquivalenceResult:
    report: dict[str, Any]

    @property
    def ok(self) -> bool:
        return bool(self.report.get("ok"))


@dataclass(frozen=True)
class DecisionExportArtifact:
    payload: dict[str, Any]
    decisions: list[dict[str, Any]]
    source: str
    content_hash: str
    profile_content_hash: str
    market: str
    interval: str
    data_fingerprint: str
    dataset_content_hash: str
    db_data_fingerprint: str
    strategy_plugin_contract: dict[str, Any]
    strategy_plugin_contract_hash: str
    strategy_decision_contract_version: str
    path: str = ""


DECISION_EQUIVALENCE_POLICY_INPUT_COVERAGE_FIELDS = (
    "candle_ts",
    "through_ts_ms",
    "decision_input_bundle_hash",
    "policy_input_hash",
    "policy_decision_hash",
    "policy_contract_hash",
    "final_exit_decision_input_hash",
    "final_signal",
    "exit_rule",
    "exit_evaluations_hash",
)
DECISION_EQUIVALENCE_FEATURE_COVERAGE_ONE_OF = (
    "market_feature_hash",
    "canonical_feature_projection_hash",
)
DECISION_EQUIVALENCE_EXECUTION_PLAN_COVERAGE_FIELDS = (
    "execution_submit_plan_hash",
    "final_action",
    "submit_expected",
    "submit_plan_source",
    "submit_plan_authority",
    "execution_engine",
)


def compare_decision_equivalence(
    *,
    research_decisions: list[dict[str, Any]],
    runtime_decisions: list[dict[str, Any]],
    profile_hash: str,
    market: str,
    interval: str,
    data_fingerprint: str,
    lifecycle_evidence: CanonicalLifecycleEvidenceBundle | dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> DecisionEquivalenceResult:
    canonical_comparison = all(is_canonical_decision(item) for item in research_decisions + runtime_decisions)
    canonical_v2_comparison = canonical_comparison and all(
        is_canonical_decision_v2(item) for item in research_decisions + runtime_decisions
    )
    canonical_v1_comparison = canonical_comparison and not canonical_v2_comparison and all(
        int(item.get("decision_contract_version") or 0) == 1 for item in research_decisions + runtime_decisions
    )
    mixed_canonical_contracts = canonical_comparison and not (canonical_v2_comparison or canonical_v1_comparison)
    if canonical_v2_comparison:
        comparison_fields = CANONICAL_EQUIVALENCE_FIELDS_V2
        comparison_contract_version = CANONICAL_COMPARISON_CONTRACT_VERSION
    elif canonical_v1_comparison:
        comparison_fields = LEGACY_CANONICAL_EQUIVALENCE_FIELDS_V1
        comparison_contract_version = LEGACY_CANONICAL_COMPARISON_CONTRACT_VERSION
    else:
        comparison_fields = LEGACY_DECISION_FIELDS
        comparison_contract_version = LEGACY_COMPARISON_CONTRACT_VERSION
    normalized_research = [_normalize_for_comparison(item, canonical=canonical_comparison) for item in research_decisions]
    normalized_runtime = [_normalize_for_comparison(item, canonical=canonical_comparison) for item in runtime_decisions]
    canonical_validation_items = _canonical_validation_items(
        research_decisions=research_decisions,
        runtime_decisions=runtime_decisions,
        canonical=canonical_comparison,
    )
    binding_items = _binding_validation_items(
        research_decisions=normalized_research,
        runtime_decisions=normalized_runtime,
        canonical=canonical_comparison,
        profile_hash=profile_hash,
        market=market,
        interval=interval,
        data_fingerprint=data_fingerprint,
    )
    research_by_key = {_decision_key(item): item for item in normalized_research}
    runtime_by_key = {_decision_key(item): item for item in normalized_runtime}
    mismatch_items: list[dict[str, object]] = []
    missing_research = sorted(set(runtime_by_key) - set(research_by_key))
    missing_runtime = sorted(set(research_by_key) - set(runtime_by_key))
    for key in sorted(set(research_by_key) & set(runtime_by_key)):
        left = research_by_key[key]
        right = runtime_by_key[key]
        field_mismatches = []
        for field in comparison_fields:
            if _normalized(left.get(field)) != _normalized(right.get(field)):
                field_mismatches.append(
                    {
                        "field": field,
                        "reason_code": _reason_for_field(field),
                        "research": left.get(field),
                        "runtime": right.get(field),
                    }
                )
        if (
            canonical_comparison
            and position_authority_supports_positive_equivalence(left)
            and position_authority_supports_positive_equivalence(right)
            and (
                classify_decision_position_state(left, source="research")[0] != "flat_no_dust_no_position"
                or classify_decision_position_state(right, source="runtime")[0] != "flat_no_dust_no_position"
            )
        ):
            if _stable_json(left.get("position_authority")) != _stable_json(right.get("position_authority")):
                field_mismatches.append(
                    {
                        "field": "position_authority",
                        "reason_code": "decision_position_authority_mismatch",
                        "research": left.get("position_authority"),
                        "runtime": right.get("position_authority"),
                    }
                )
        if field_mismatches:
            state_classes = sorted(
                set(
                    filter(
                        None,
                        (
                            classify_decision_position_state(left, source="research")[0],
                            classify_decision_position_state(right, source="runtime")[0],
                        ),
                    )
                )
            )
            mismatch_items.append(
                {
                    "decision_key": key,
                    "reason_code": "decision_field_mismatch",
                    "fields": field_mismatches,
                    "state_classes": state_classes,
                    "drift_diagnostics": _drift_diagnostics(left, right),
                }
            )
    mismatch_items.extend(
        _timestamp_only_diagnostics(
            research_decisions=normalized_research,
            runtime_decisions=normalized_runtime,
            missing_runtime_keys=set(missing_runtime),
            missing_research_keys=set(missing_research),
        )
    )
    reason_codes = []
    if mixed_canonical_contracts:
        reason_codes.append("canonical_decision_contract_version_mismatch")
    if missing_research:
        reason_codes.append("missing_research_decision")
    if missing_runtime:
        reason_codes.append("missing_runtime_decision")
    for item in mismatch_items:
        reason_codes.extend(_field_reasons(item))
    for item in canonical_validation_items + binding_items:
        reason_codes.extend(str(code) for code in item.get("reason_codes") or [item.get("reason_code")])
    exact_mismatch_count = sum(1 for item in mismatch_items if not item.get("diagnostic_only"))
    canonical_missing_fields_by_decision = {
        str(item["decision_key"]): list(item.get("missing_fields") or ())
        for item in canonical_validation_items
        if item.get("missing_fields")
    }
    canonical_incomplete_decision_count = len(
        [item for item in canonical_validation_items if item.get("incomplete_canonical_decision")]
    )
    canonical_complete_and_bound = bool(
        canonical_comparison
        and canonical_incomplete_decision_count == 0
        and not binding_items
    )
    reason_code_set = sorted(set(reason_codes))
    state_coverage_matrix = _state_coverage_matrix(
        research_decisions=research_decisions,
        runtime_decisions=runtime_decisions,
        mismatch_items=mismatch_items,
        missing_research=missing_research,
        missing_runtime=missing_runtime,
        reason_codes=reason_code_set,
    )
    drift_counts = _drift_counts(
        mismatch_items=mismatch_items,
        missing_research=missing_research,
        missing_runtime=missing_runtime,
    )
    execution_equivalence = _execution_equivalence_report(
        normalized_research=normalized_research,
        normalized_runtime=normalized_runtime,
        mismatch_items=mismatch_items,
        missing_research=missing_research,
        missing_runtime=missing_runtime,
        lifecycle_evidence=lifecycle_evidence,
    )
    outcome = _equivalence_outcome(
        reason_codes=reason_code_set,
        canonical_incomplete_decision_count=canonical_incomplete_decision_count,
        binding_items=binding_items,
        exact_mismatch_count=exact_mismatch_count,
        missing_research=missing_research,
        missing_runtime=missing_runtime,
        actual_semantic_drift_count=drift_counts["actual_semantic_drift_count"],
        state_coverage_matrix=state_coverage_matrix,
    )
    report: dict[str, Any] = {
        "schema_version": DECISION_EQUIVALENCE_SCHEMA_VERSION,
        "comparison_contract_version": comparison_contract_version,
        "canonical_schema": canonical_comparison,
        "canonical_v2_schema": canonical_v2_comparison,
        "legacy_schema": not canonical_comparison,
        "compatibility_reason_codes": (
            ["legacy_sma_canonical_v1_payload"] if canonical_v1_comparison else []
        ),
        "promotion_grade_comparison": (
            canonical_complete_and_bound and outcome == "PASS_POSITIVE_EQUIVALENCE"
        ),
        "ok": outcome == "PASS_POSITIVE_EQUIVALENCE" and not reason_code_set,
        "outcome": outcome,
        "reason_codes": reason_code_set,
        "profile_content_hash": profile_hash,
        "market": market,
        "interval": interval,
        "data_fingerprint": data_fingerprint,
        "dataset_content_hash": data_fingerprint,
        "research_decision_count": len(research_decisions),
        "runtime_decision_count": len(runtime_decisions),
        "matched_decision_count": len(set(research_by_key) & set(runtime_by_key)) - exact_mismatch_count,
        "mismatched_decision_count": len(mismatch_items),
        "mismatch_count": exact_mismatch_count,
        "missing_research_decisions": missing_research,
        "missing_runtime_decisions": missing_runtime,
        "mismatches": mismatch_items,
        "comparison_fields": list(comparison_fields),
        "required_policy_input_coverage_fields": list(DECISION_EQUIVALENCE_POLICY_INPUT_COVERAGE_FIELDS),
        "required_feature_coverage_one_of": list(DECISION_EQUIVALENCE_FEATURE_COVERAGE_ONE_OF),
        "required_execution_plan_coverage_fields": list(DECISION_EQUIVALENCE_EXECUTION_PLAN_COVERAGE_FIELDS),
        "policy_input_hash_coverage": _field_coverage(
            normalized_research + normalized_runtime,
            fields=DECISION_EQUIVALENCE_POLICY_INPUT_COVERAGE_FIELDS,
            one_of_groups=(DECISION_EQUIVALENCE_FEATURE_COVERAGE_ONE_OF,),
        ),
        "execution_plan_coverage": _field_coverage(
            normalized_research + normalized_runtime,
            fields=DECISION_EQUIVALENCE_EXECUTION_PLAN_COVERAGE_FIELDS,
            one_of_groups=(),
        ),
        "canonical_missing_field_count": sum(len(fields) for fields in canonical_missing_fields_by_decision.values()),
        "canonical_missing_fields_by_decision": canonical_missing_fields_by_decision,
        "canonical_incomplete_decision_count": canonical_incomplete_decision_count,
        "canonical_validation": canonical_validation_items,
        "binding_validation": binding_items,
        "actual_semantic_drift_count": drift_counts["actual_semantic_drift_count"],
        "lifecycle_unmodeled_mismatch_count": drift_counts["lifecycle_unmodeled_mismatch_count"],
        "claim_scope": execution_equivalence["claim_scope"],
        "scope_badge": execution_equivalence["scope_badge"],
        "submit_plan_equivalence_supported": execution_equivalence["submit_plan_equivalence_supported"],
        "full_lifecycle_equivalence_supported": execution_equivalence["full_lifecycle_equivalence_supported"],
        "simulated_fill_equivalence_supported": execution_equivalence["simulated_fill_equivalence_supported"],
        "paper_submit_fill_equivalence_supported": execution_equivalence["paper_submit_fill_equivalence_supported"],
        "live_submit_equivalence_supported": execution_equivalence["live_submit_equivalence_supported"],
        "accounting_replay_equivalence_supported": execution_equivalence["accounting_replay_equivalence_supported"],
        "unsupported_lifecycle_reasons": execution_equivalence["unsupported_lifecycle_reasons"],
        "claims_scope": _claims_scope(
            state_coverage_matrix=state_coverage_matrix,
            lifecycle_evidence=lifecycle_evidence,
        ),
        "execution_equivalence": execution_equivalence,
        "state_coverage_matrix": state_coverage_matrix,
        "recommended_next_action": _recommended_next_action(
            outcome=outcome,
            reason_codes=reason_code_set,
            canonical_comparison=canonical_comparison,
            state_coverage_matrix=state_coverage_matrix,
        ),
        "generated_at": generated_at,
    }
    report[DECISION_EQUIVALENCE_HASH_FIELD] = compute_decision_equivalence_hash(report)
    return DecisionEquivalenceResult(report=report)


def compare_decision_export_artifacts(
    *,
    research_artifact: DecisionExportArtifact,
    runtime_artifact: DecisionExportArtifact,
    profile_hash: str,
    market: str,
    interval: str,
    data_fingerprint: str,
    lifecycle_evidence: CanonicalLifecycleEvidenceBundle | dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> DecisionEquivalenceResult:
    result = compare_decision_equivalence(
        research_decisions=research_artifact.decisions,
        runtime_decisions=runtime_artifact.decisions,
        profile_hash=profile_hash,
        market=market,
        interval=interval,
        data_fingerprint=data_fingerprint,
        lifecycle_evidence=lifecycle_evidence,
        generated_at=generated_at,
    )
    report = dict(result.report)
    artifact_binding = _artifact_binding_validation_items(
        research_artifact=research_artifact,
        runtime_artifact=runtime_artifact,
        profile_hash=profile_hash,
        market=market,
        interval=interval,
        data_fingerprint=data_fingerprint,
    )
    reason_codes = sorted(set(list(report.get("reason_codes") or ()) + [
        str(code)
        for item in artifact_binding
        for code in (item.get("reason_codes") or [item.get("reason_code")])
        if code
    ]))
    report.update(
        {
            "research_export_content_hash": research_artifact.content_hash,
            "runtime_export_content_hash": runtime_artifact.content_hash,
            "research_export_source": research_artifact.source,
            "runtime_export_source": runtime_artifact.source,
            "research_export_path": research_artifact.path,
            "runtime_export_path": runtime_artifact.path,
            "research_strategy_plugin_contract_hash": research_artifact.strategy_plugin_contract_hash,
            "runtime_strategy_plugin_contract_hash": runtime_artifact.strategy_plugin_contract_hash,
            "strategy_decision_contract_version": research_artifact.strategy_decision_contract_version,
            "repo_owned_export_artifacts": True,
            "legacy_or_unverified_export": False,
            "artifact_binding_validation": artifact_binding,
            "reason_codes": reason_codes,
        }
    )
    if artifact_binding:
        report["promotion_grade_comparison"] = False
        report["ok"] = False
        report["outcome"] = "FAIL_EXPORT_BINDING"
        report["recommended_next_action"] = _recommended_next_action(
            outcome=report["outcome"],
            reason_codes=reason_codes,
            canonical_comparison=bool(report.get("canonical_schema")),
            state_coverage_matrix=report.get("state_coverage_matrix") if isinstance(report.get("state_coverage_matrix"), dict) else None,
        )
    else:
        matrix = report.get("state_coverage_matrix") if isinstance(report.get("state_coverage_matrix"), dict) else {}
        report["outcome"] = _equivalence_outcome(
            reason_codes=reason_codes,
            canonical_incomplete_decision_count=int(report.get("canonical_incomplete_decision_count") or 0),
            binding_items=list(report.get("binding_validation") or ()),
            exact_mismatch_count=int(report.get("mismatch_count") or 0),
            missing_research=list(report.get("missing_research_decisions") or ()),
            missing_runtime=list(report.get("missing_runtime_decisions") or ()),
            actual_semantic_drift_count=int(report.get("actual_semantic_drift_count") or 0),
            state_coverage_matrix=matrix,
        )
        report["ok"] = report["outcome"] == "PASS_POSITIVE_EQUIVALENCE" and not reason_codes
        report["recommended_next_action"] = _recommended_next_action(
            outcome=report["outcome"],
            reason_codes=reason_codes,
            canonical_comparison=bool(report.get("canonical_schema")),
            state_coverage_matrix=matrix,
        )
    report[DECISION_EQUIVALENCE_HASH_FIELD] = compute_decision_equivalence_hash(report)
    return DecisionEquivalenceResult(report=report)


def compute_decision_equivalence_hash(report: dict[str, Any]) -> str:
    payload = {
        key: value
        for key, value in report.items()
        if key not in DECISION_EQUIVALENCE_HASH_EXCLUDED_FIELDS
    }
    return sha256_prefixed(content_hash_payload(payload))


PROMOTION_GRADE_DECISION_EQUIVALENCE_REQUIRED_TRUE_FIELDS = (
    "ok",
    "promotion_grade_comparison",
    "canonical_schema",
    "canonical_v2_schema",
    "repo_owned_export_artifacts",
)


def promotion_grade_decision_equivalence_fail_reasons(report: dict[str, Any]) -> tuple[str, ...]:
    """Return fail-closed reasons when a report is not promotion-grade v2 evidence."""
    reasons: list[str] = []
    reasons.extend(f"decision_equivalence_{reason}" for reason in smoke_only_evidence_rejection_reasons(report))
    for field in PROMOTION_GRADE_DECISION_EQUIVALENCE_REQUIRED_TRUE_FIELDS:
        if report.get(field) is not True:
            reasons.append(f"decision_equivalence_{field}_not_true")
    if report.get("outcome") != "PASS_POSITIVE_EQUIVALENCE":
        reasons.append("decision_equivalence_outcome_not_positive")
    if report.get("legacy_schema") is True:
        reasons.append("decision_equivalence_legacy_schema")
    if report.get("legacy_or_unverified_export") is True:
        reasons.append("decision_equivalence_unverified_export")
    for field in (
        "execution_submit_plan_hash",
        "submit_plan_source",
        "submit_plan_authority",
        "execution_engine",
    ):
        if any(field in missing for missing in _missing_canonical_fields(report)):
            reasons.append(f"decision_equivalence_missing_{field}")
    if int(report.get("canonical_incomplete_decision_count") or 0) > 0:
        reasons.append("decision_equivalence_incomplete_canonical")
    if int(report.get("canonical_missing_field_count") or 0) > 0:
        reasons.append("decision_equivalence_missing_canonical_fields")
    if report.get("binding_validation"):
        reasons.append("decision_equivalence_binding_validation_nonempty")
    if report.get("artifact_binding_validation"):
        reasons.append("decision_equivalence_artifact_binding_validation_nonempty")
    if report.get("reason_codes"):
        reasons.append("decision_equivalence_reason_codes_nonempty")
    reasons.extend(decision_equivalence_report_semantic_fail_reasons(report))
    claims_scope = report.get("claims_scope")
    if not isinstance(claims_scope, dict):
        reasons.append("decision_equivalence_claims_scope_missing")
    else:
        if claims_scope.get("signal_equivalence_supported") is not True:
            reasons.append("decision_equivalence_signal_scope_not_supported")
        if claims_scope.get("full_lifecycle_equivalence_supported") is True:
            execution_equivalence = report.get("execution_equivalence")
            execution_scope = execution_equivalence if isinstance(execution_equivalence, dict) else {}
            lifecycle_evidence_ok = bool(
                execution_scope.get("simulated_fill_equivalence_supported")
                and execution_scope.get("paper_submit_fill_equivalence_supported")
                and execution_scope.get("live_submit_equivalence_supported")
                and execution_scope.get("accounting_replay_equivalence_supported")
                and execution_scope.get("position_lifecycle_equivalence_supported")
            )
            if not lifecycle_evidence_ok:
                reasons.append("decision_equivalence_full_lifecycle_equivalence_evidence_missing")
        if claims_scope.get("unsupported_state_classes"):
            reasons.append("decision_equivalence_unsupported_state_present")
        if int(claims_scope.get("fail_closed_unmodeled_state_count") or 0) > 0:
            reasons.append("decision_equivalence_unmodeled_state_present")
    return tuple(sorted(set(reasons)))


def decision_equivalence_report_semantic_fail_reasons(report: dict[str, Any]) -> tuple[str, ...]:
    """Return fail-closed reasons for source promotion/profile evidence use."""
    reasons: list[str] = []
    if report.get("ok") is not True or report.get("outcome") != "PASS_POSITIVE_EQUIVALENCE":
        reasons.append("decision_equivalence_status_not_pass")
    if report.get("promotion_grade_comparison") is not True:
        reasons.append("decision_equivalence_status_not_pass")
    if report.get("repo_owned_export_artifacts") is not True or report.get("legacy_or_unverified_export") is True:
        reasons.append("decision_equivalence_not_repo_owned_actual_exports")
    if str(report.get("research_export_source") or "") != "research":
        reasons.append("decision_equivalence_not_repo_owned_actual_exports")
    if str(report.get("runtime_export_source") or "") != "runtime_replay":
        reasons.append("decision_equivalence_not_repo_owned_actual_exports")
    if not str(report.get("research_export_content_hash") or "").startswith("sha256:"):
        reasons.append("decision_equivalence_not_repo_owned_actual_exports")
    if not str(report.get("runtime_export_content_hash") or "").startswith("sha256:"):
        reasons.append("decision_equivalence_not_repo_owned_actual_exports")
    if not _coverage_ok(report.get("policy_input_hash_coverage")):
        reasons.append("decision_equivalence_missing_policy_hash_coverage")
    if not _coverage_ok(report.get("execution_plan_coverage")):
        reasons.append("decision_equivalence_missing_execution_plan_coverage")
    claims_scope = report.get("claims_scope")
    if not isinstance(claims_scope, dict) or claims_scope.get("signal_equivalence_supported") is not True:
        reasons.append("decision_equivalence_missing_policy_hash_coverage")
    execution_equivalence = report.get("execution_equivalence")
    if (
        not isinstance(execution_equivalence, dict)
        or execution_equivalence.get("submit_plan_equivalence_supported") is not True
        or execution_equivalence.get("submit_plan_equivalence_ok") is not True
    ):
        reasons.append("decision_equivalence_missing_execution_plan_coverage")
    return tuple(sorted(set(reasons)))


def validate_decision_equivalence_report(
    report: dict[str, Any],
    *,
    expected_hash: str | None = None,
) -> tuple[str, ...]:
    reasons: list[str] = []
    recorded_hash = str(report.get(DECISION_EQUIVALENCE_HASH_FIELD) or "").strip()
    actual_hash = compute_decision_equivalence_hash(report)
    if not recorded_hash.startswith("sha256:"):
        reasons.append("decision_equivalence_content_hash_missing")
    elif recorded_hash != actual_hash:
        reasons.append("decision_equivalence_report_hash_mismatch")
    if expected_hash is not None:
        expected = str(expected_hash or "").strip()
        if not expected.startswith("sha256:"):
            reasons.append("decision_equivalence_content_hash_missing")
        elif recorded_hash != expected or actual_hash != expected:
            reasons.append("decision_equivalence_report_hash_mismatch")
    reasons.extend(promotion_grade_decision_equivalence_fail_reasons(report))
    return tuple(sorted(set(reasons)))


def require_promotion_grade_decision_equivalence(report: dict[str, Any]) -> None:
    reasons = promotion_grade_decision_equivalence_fail_reasons(report)
    if reasons:
        raise ValueError("decision_equivalence_not_promotion_grade:" + ",".join(reasons))


def _coverage_ok(value: object) -> bool:
    return isinstance(value, dict) and value.get("ok") is True


def _field_coverage(
    decisions: list[dict[str, Any]],
    *,
    fields: tuple[str, ...],
    one_of_groups: tuple[tuple[str, ...], ...],
) -> dict[str, Any]:
    missing: dict[str, list[str]] = {}
    for decision in decisions:
        key = _decision_key(decision)
        missing_fields = [
            field
            for field in fields
            if (decision.get(field) is None if field == "exit_rule" else _execution_required_missing(decision.get(field)))
        ]
        for group in one_of_groups:
            if not any(not _execution_required_missing(decision.get(field)) for field in group):
                missing_fields.append("|".join(group))
        if missing_fields:
            missing[key] = missing_fields
    return {
        "ok": not missing,
        "checked_decision_count": len(decisions),
        "required_fields": list(fields),
        "required_one_of_groups": [list(group) for group in one_of_groups],
        "missing_by_decision": missing,
    }


def _missing_canonical_fields(report: dict[str, Any]) -> tuple[str, ...]:
    missing_by_decision = report.get("canonical_missing_fields_by_decision")
    if not isinstance(missing_by_decision, dict):
        return ()
    missing: list[str] = []
    for fields in missing_by_decision.values():
        if isinstance(fields, list | tuple):
            missing.extend(str(field) for field in fields)
    return tuple(missing)


def load_decision_list(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict) and isinstance(payload.get("decisions"), list):
        payload = payload["decisions"]
    if not isinstance(payload, list):
        raise ValueError("decision_payload_not_list")
    decisions: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("decision_item_not_object")
        decisions.append(dict(item))
    return decisions


def load_decision_export_artifact(
    path: str | Path,
    *,
    expected_source: str | None = None,
) -> DecisionExportArtifact:
    with Path(path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("decision_export_payload_not_object")
    source = str(payload.get("source") or "").strip()
    if expected_source is not None and source != expected_source:
        raise ValueError(f"decision_export_source_mismatch:{source or 'missing'}")
    if not source:
        raise ValueError("decision_export_source_missing")
    if int(payload.get("schema_version") or 0) <= 0:
        raise ValueError("decision_export_schema_version_missing")
    if int(payload.get("decision_contract_version") or 0) <= 0:
        raise ValueError("decision_export_contract_version_missing")
    if payload.get("promotion_grade_export") is not True:
        raise ValueError("decision_export_not_promotion_grade")
    plugin_contract = payload.get("strategy_plugin_contract")
    if not isinstance(plugin_contract, dict):
        raise ValueError("decision_export_strategy_plugin_contract_missing")
    plugin_contract_hash = str(payload.get("strategy_plugin_contract_hash") or "").strip()
    if not plugin_contract_hash.startswith("sha256:"):
        raise ValueError("decision_export_strategy_plugin_contract_hash_missing")
    if sha256_prefixed(plugin_contract) != plugin_contract_hash:
        raise ValueError("decision_export_strategy_plugin_contract_hash_mismatch")
    strategy_decision_contract_version = str(payload.get("strategy_decision_contract_version") or "").strip()
    if not strategy_decision_contract_version:
        raise ValueError("decision_export_strategy_decision_contract_version_missing")
    recorded_hash = str(payload.get(DECISION_EXPORT_HASH_FIELD) or "").strip()
    if not recorded_hash.startswith("sha256:"):
        raise ValueError("decision_export_content_hash_missing")
    actual_hash = compute_decision_export_hash(payload)
    if actual_hash != recorded_hash:
        raise ValueError("decision_export_content_hash_mismatch")
    decisions_raw = payload.get("decisions")
    if not isinstance(decisions_raw, list):
        raise ValueError("decision_export_decisions_not_list")
    if int(payload.get("decision_count") or -1) != len(decisions_raw):
        raise ValueError("decision_export_decision_count_mismatch")
    decisions: list[dict[str, Any]] = []
    for item in decisions_raw:
        if not isinstance(item, dict):
            raise ValueError("decision_export_decision_item_not_object")
        decisions.append(dict(item))
    profile_hash = _required_export_text(payload, "profile_content_hash")
    market = _required_export_text(payload, "market")
    interval = _required_export_text(payload, "interval")
    dataset_hash = str(payload.get("dataset_content_hash") or "").strip()
    db_fingerprint = str(payload.get("db_data_fingerprint") or "").strip()
    if not dataset_hash and not db_fingerprint:
        raise ValueError("decision_export_data_fingerprint_missing")
    data_fingerprint = dataset_hash or db_fingerprint
    for decision in decisions:
        _validate_decision_bound_to_export(
            decision,
            source=source,
            profile_hash=profile_hash,
            market=market,
            interval=interval,
            dataset_hash=dataset_hash,
            db_fingerprint=db_fingerprint,
        )
    return DecisionExportArtifact(
        payload=dict(payload),
        decisions=decisions,
        source=source,
        content_hash=recorded_hash,
        profile_content_hash=profile_hash,
        market=market,
        interval=interval,
        data_fingerprint=data_fingerprint,
        dataset_content_hash=dataset_hash,
        db_data_fingerprint=db_fingerprint,
        strategy_plugin_contract=dict(plugin_contract),
        strategy_plugin_contract_hash=plugin_contract_hash,
        strategy_decision_contract_version=strategy_decision_contract_version,
        path=str(Path(path).expanduser().resolve()),
    )


def compute_decision_export_hash(payload: dict[str, Any]) -> str:
    return sha256_prefixed(
        content_hash_payload(
            {
                key: value
                for key, value in payload.items()
                if key not in DECISION_EXPORT_HASH_EXCLUDED_FIELDS
            }
        )
    )


def _decision_key(item: dict[str, Any]) -> str:
    return "|".join(
        (
            str(item.get("signal_timestamp") or ""),
            str(item.get("candle_ts") or ""),
            str(item.get("market") or ""),
            str(item.get("interval") or ""),
        )
    )


def _normalize_for_comparison(item: dict[str, Any], *, canonical: bool) -> dict[str, Any]:
    if canonical:
        normalized = normalize_canonical_decision(item)
        if isinstance(item.get("position_authority"), dict):
            normalized["position_authority"] = dict(item["position_authority"])
        return normalized
    return dict(item)


def _canonical_validation_items(
    *,
    research_decisions: list[dict[str, Any]],
    runtime_decisions: list[dict[str, Any]],
    canonical: bool,
) -> list[dict[str, object]]:
    if not canonical:
        return [
            {
                "decision_key": _decision_key(dict(item)),
                "source": source,
                "legacy_shallow_decision": True,
                "incomplete_canonical_decision": False,
                "missing_fields": [],
                "reason_codes": ["canonical_decision_legacy_schema"],
            }
            for source, decisions in (("research", research_decisions), ("runtime", runtime_decisions))
            for item in decisions
        ]
    out: list[dict[str, object]] = []
    for source, decisions in (("research", research_decisions), ("runtime", runtime_decisions)):
        for item in decisions:
            authority = item.get("position_authority") if isinstance(item.get("position_authority"), dict) else {}
            state_class = str(authority.get("state_class") or "").strip()
            result = validate_canonical_decision_payload(
                item,
                promotion_grade=is_canonical_decision_v2(item),
                require_promotion_provenance=state_class != "flat_no_dust_no_position",
            )
            if result.reason_codes:
                out.append(
                    {
                        "decision_key": _decision_key(normalize_canonical_decision(item)),
                        "source": source,
                        "canonical_schema_present": result.canonical_schema_present,
                        "canonical_schema_complete": result.canonical_schema_complete,
                        "promotion_grade": result.promotion_grade,
                        "legacy_shallow_decision": result.legacy_shallow_decision,
                        "incomplete_canonical_decision": result.incomplete_canonical_decision,
                        "missing_fields": list(result.missing_fields),
                        "reason_codes": list(result.reason_codes),
                    }
                )
    return out


def _binding_validation_items(
    *,
    research_decisions: list[dict[str, Any]],
    runtime_decisions: list[dict[str, Any]],
    canonical: bool,
    profile_hash: str,
    market: str,
    interval: str,
    data_fingerprint: str,
) -> list[dict[str, object]]:
    if not canonical:
        return []
    out: list[dict[str, object]] = []
    expected_profile = str(profile_hash or "").strip()
    expected_market = str(market or "").strip()
    expected_interval = str(interval or "").strip()
    expected_data = str(data_fingerprint or "").strip()
    for source, decisions in (("research", research_decisions), ("runtime", runtime_decisions)):
        for item in decisions:
            reasons: list[str] = []
            if str(item.get("profile_content_hash") or "").strip() != expected_profile:
                reasons.append("decision_profile_hash_not_bound_to_report")
            if str(item.get("market") or "").strip() != expected_market:
                reasons.append("decision_market_not_bound_to_report")
            if str(item.get("interval") or "").strip() != expected_interval:
                reasons.append("decision_interval_not_bound_to_report")
            dataset_hash = str(item.get("dataset_content_hash") or "").strip()
            db_fingerprint = str(item.get("db_data_fingerprint") or "").strip()
            if expected_data and expected_data not in {dataset_hash, db_fingerprint}:
                reasons.append("decision_data_fingerprint_not_bound_to_report")
            if reasons:
                out.append(
                    {
                        "decision_key": _decision_key(item),
                        "source": source,
                        "reason_codes": reasons,
                        "profile_content_hash": item.get("profile_content_hash"),
                        "market": item.get("market"),
                        "interval": item.get("interval"),
                        "dataset_content_hash": item.get("dataset_content_hash"),
                        "db_data_fingerprint": item.get("db_data_fingerprint"),
                    }
                )
    return out


def _artifact_binding_validation_items(
    *,
    research_artifact: DecisionExportArtifact,
    runtime_artifact: DecisionExportArtifact,
    profile_hash: str,
    market: str,
    interval: str,
    data_fingerprint: str,
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for label, artifact, expected_source in (
        ("research", research_artifact, "research"),
        ("runtime", runtime_artifact, "runtime_replay"),
    ):
        reasons: list[str] = []
        if artifact.source != expected_source:
            reasons.append(f"{label}_export_source_mismatch")
        if artifact.profile_content_hash != str(profile_hash or "").strip():
            reasons.append(f"{label}_export_profile_hash_mismatch")
        if artifact.market != str(market or "").strip():
            reasons.append(f"{label}_export_market_mismatch")
        if artifact.interval != str(interval or "").strip():
            reasons.append(f"{label}_export_interval_mismatch")
        if not artifact.strategy_plugin_contract_hash:
            reasons.append(f"{label}_export_strategy_plugin_contract_hash_missing")
        expected_data = str(data_fingerprint or "").strip()
        if expected_data and expected_data not in {artifact.dataset_content_hash, artifact.db_data_fingerprint}:
            reasons.append(f"{label}_export_data_fingerprint_mismatch")
        if reasons:
            out.append(
                {
                    "source": label,
                    "reason_codes": reasons,
                    "export_content_hash": artifact.content_hash,
                    "profile_content_hash": artifact.profile_content_hash,
                    "market": artifact.market,
                    "interval": artifact.interval,
                    "dataset_content_hash": artifact.dataset_content_hash,
                    "db_data_fingerprint": artifact.db_data_fingerprint,
                    "strategy_plugin_contract_hash": artifact.strategy_plugin_contract_hash,
                }
            )
    if research_artifact.profile_content_hash != runtime_artifact.profile_content_hash:
        out.append(
            {
                "source": "artifact_pair",
                "reason_codes": ["export_profile_hash_pair_mismatch"],
                "research_profile_content_hash": research_artifact.profile_content_hash,
                "runtime_profile_content_hash": runtime_artifact.profile_content_hash,
            }
        )
    if (
        research_artifact.strategy_plugin_contract_hash
        and runtime_artifact.strategy_plugin_contract_hash
        and research_artifact.strategy_plugin_contract_hash != runtime_artifact.strategy_plugin_contract_hash
    ):
        out.append(
            {
                "source": "artifact_pair",
                "reason_codes": ["export_strategy_plugin_contract_hash_pair_mismatch"],
                "research_strategy_plugin_contract_hash": research_artifact.strategy_plugin_contract_hash,
                "runtime_strategy_plugin_contract_hash": runtime_artifact.strategy_plugin_contract_hash,
            }
        )
    if (
        research_artifact.strategy_decision_contract_version
        and runtime_artifact.strategy_decision_contract_version
        and research_artifact.strategy_decision_contract_version != runtime_artifact.strategy_decision_contract_version
    ):
        out.append(
            {
                "source": "artifact_pair",
                "reason_codes": ["export_strategy_decision_contract_version_pair_mismatch"],
                "research_strategy_decision_contract_version": research_artifact.strategy_decision_contract_version,
                "runtime_strategy_decision_contract_version": runtime_artifact.strategy_decision_contract_version,
            }
        )
    return out


def _required_export_text(payload: dict[str, Any], field: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise ValueError(f"decision_export_{field}_missing")
    return value


def _validate_decision_bound_to_export(
    decision: dict[str, Any],
    *,
    source: str,
    profile_hash: str,
    market: str,
    interval: str,
    dataset_hash: str,
    db_fingerprint: str,
) -> None:
    if str(decision.get("profile_content_hash") or "").strip() != profile_hash:
        raise ValueError(f"decision_export_{source}_decision_profile_hash_mismatch")
    if str(decision.get("market") or "").strip() != market:
        raise ValueError(f"decision_export_{source}_decision_market_mismatch")
    if str(decision.get("interval") or "").strip() != interval:
        raise ValueError(f"decision_export_{source}_decision_interval_mismatch")
    decision_dataset = str(decision.get("dataset_content_hash") or "").strip()
    decision_db = str(decision.get("db_data_fingerprint") or "").strip()
    if dataset_hash and decision_dataset != dataset_hash:
        raise ValueError(f"decision_export_{source}_decision_dataset_hash_mismatch")
    if db_fingerprint and decision_db != db_fingerprint:
        raise ValueError(f"decision_export_{source}_decision_db_fingerprint_mismatch")


def _timestamp_only_diagnostics(
    *,
    research_decisions: list[dict[str, Any]],
    runtime_decisions: list[dict[str, Any]],
    missing_runtime_keys: set[str],
    missing_research_keys: set[str],
) -> list[dict[str, object]]:
    diagnostics: list[dict[str, object]] = []
    runtime_by_timestamp = _decisions_by_timestamp(runtime_decisions)
    for research in research_decisions:
        research_key = _decision_key(research)
        if research_key not in missing_runtime_keys:
            continue
        candidates = [
            item
            for item in runtime_by_timestamp.get(str(research.get("signal_timestamp") or ""), [])
            if _decision_key(item) in missing_research_keys
        ]
        runtime = _best_timestamp_candidate(research, candidates)
        if runtime is None:
            continue
        fields = [
            {"field": field, "research": research.get(field), "runtime": runtime.get(field)}
            for field in DIAGNOSTIC_DRIFT_FIELDS
            if _normalized(research.get(field)) != _normalized(runtime.get(field))
        ]
        if fields:
            diagnostics.append(
                {
                    "decision_key": research_key,
                    "runtime_decision_key": _decision_key(runtime),
                    "reason_code": "decision_timestamp_candidate_field_mismatch",
                    "diagnostic_only": True,
                    "fields": fields,
                }
            )
    return diagnostics


def _drift_diagnostics(research: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    def pick(payload: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in payload:
                return payload.get(key)
        strategy_payload = payload.get("strategy_specific_payload")
        if isinstance(strategy_payload, dict):
            for key in keys:
                if key in strategy_payload:
                    return strategy_payload.get(key)
        feature_snapshot = payload.get("feature_snapshot")
        if isinstance(feature_snapshot, dict):
            for key in keys:
                if key in feature_snapshot:
                    return feature_snapshot.get(key)
        behavior = payload.get("strategy_behavior_payload")
        if isinstance(behavior, dict):
            nested = behavior.get("strategy_specific_payload")
            if isinstance(nested, dict):
                for key in keys:
                    if key in nested:
                        return nested.get(key)
        return None

    def side(payload: dict[str, Any]) -> dict[str, Any]:
        position_authority = payload.get("position_authority") if isinstance(payload.get("position_authority"), dict) else {}
        submit_evidence = (
            payload.get("execution_submit_plan_evidence")
            if isinstance(payload.get("execution_submit_plan_evidence"), dict)
            else {}
        )
        return {
            "previous_cross_state": pick(payload, "previous_cross_state"),
            "allow_initial_cross": pick(payload, "allow_initial_cross"),
            "gap_ratio": pick(payload, "gap_ratio"),
            "volatility_ratio": pick(payload, "volatility_ratio", "range_ratio"),
            "overextended_ratio": pick(payload, "overextended_ratio", "overextended_abs_return_ratio"),
            "market_regime_snapshot": pick(payload, "market_regime_snapshot", "current_market_regime_snapshot"),
            "position_terminal_state": position_authority.get("terminal_state") or payload.get("terminal_state"),
            "position_effective_flat": payload.get("effective_flat"),
            "position_dust_state": payload.get("dust_state"),
            "fee_authority_hash": payload.get("fee_authority_hash"),
            "fee_authority": payload.get("fee_authority"),
            "order_rules_hash": payload.get("order_rules_hash"),
            "order_rules": payload.get("order_rules"),
            "execution_intent": payload.get("execution_intent")
            or payload.get("execution_intent_v2")
            or submit_evidence,
            "final_signal": payload.get("final_signal"),
            "policy_input_hash": payload.get("policy_input_hash"),
            "policy_decision_hash": payload.get("policy_decision_hash"),
            "decision_input_bundle_hash": payload.get("decision_input_bundle_hash"),
            "execution_submit_plan_hash": payload.get("execution_submit_plan_hash"),
        }

    return {"research": side(research), "runtime": side(runtime)}


def _decisions_by_timestamp(decisions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_timestamp: dict[str, list[dict[str, Any]]] = {}
    for item in decisions:
        by_timestamp.setdefault(str(item.get("signal_timestamp") or ""), []).append(item)
    return by_timestamp


def _best_timestamp_candidate(
    research: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: sum(
            1
            for field in DIAGNOSTIC_DRIFT_FIELDS
            if _normalized(research.get(field)) != _normalized(item.get(field))
        ),
    )


def _field_reasons(item: dict[str, object]) -> list[str]:
    fields = item.get("fields")
    if not isinstance(fields, list) or not fields:
        return ["decision_field_mismatch"]
    reasons: list[str] = []
    for field_item in fields:
        if not isinstance(field_item, dict):
            continue
        field = str(field_item.get("field") or "field")
        reasons.append(str(field_item.get("reason_code") or _reason_for_field(field)))
    return reasons or ["decision_field_mismatch"]


def _normalized(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return str(value or "").strip()


def _stable_json(value: object) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _reason_for_field(field: str) -> str:
    if field == "position_authority":
        return "decision_position_authority_mismatch"
    if field in {"signal_timestamp", "candle_ts", "through_ts_ms", "candle_basis", "decision_ts"}:
        return "decision_timestamp_candle_basis_mismatch"
    if field == "raw_signal":
        return "decision_raw_signal_mismatch"
    if field in {"final_signal", "side"}:
        return "decision_final_signal_mismatch"
    if field in {"blocked", "block_reason", "blocked_filters"}:
        return "decision_filter_block_reason_mismatch"
    if field in {"fee_authority_hash", "fee_model_hash"}:
        return "decision_fee_authority_mismatch"
    if field == "slippage_model_hash":
        return "decision_slippage_model_mismatch"
    if field == "order_rules_hash":
        return "decision_order_rules_mismatch"
    if field in {"market_regime", "regime_decision", "regime_block_reason"}:
        return "decision_regime_mismatch"
    if field in {
        "position_state_hash",
        "entry_allowed",
        "exit_allowed",
        "dust_state",
        "effective_flat",
        "normalized_exposure_active",
    }:
        return "decision_position_dust_mismatch"
    if field in {"exit_rule", "exit_reason", "exit_evaluations_hash"}:
        return "decision_exit_rule_mismatch"
    if field == "execution_timing_policy_hash":
        return "decision_execution_timing_policy_mismatch"
    if field == "execution_summary_hash":
        return "execution_summary_hash_mismatch"
    if field == "execution_submit_plan_hash":
        return "execution_submit_plan_hash_mismatch"
    if field == "final_action":
        return "execution_final_action_mismatch"
    if field == "submit_expected":
        return "execution_submit_expected_mismatch"
    if field == "pre_submit_proof_status":
        return "execution_pre_submit_proof_status_mismatch"
    if field == "execution_block_reason":
        return "execution_block_reason_mismatch"
    if field == "submit_plan_source":
        return "execution_submit_plan_source_mismatch"
    if field == "submit_plan_authority":
        return "execution_submit_plan_authority_mismatch"
    if field == "execution_engine":
        return "execution_engine_mismatch"
    if field == "policy_contract_hash":
        return "policy_contract_hash_mismatch"
    if field == "policy_input_hash":
        return "policy_input_hash_mismatch"
    if field == "policy_decision_hash":
        return "policy_decision_hash_mismatch"
    if field in {"profile_content_hash", "candidate_profile_hash"}:
        return "decision_profile_hash_mismatch"
    if field in {"dataset_content_hash", "db_data_fingerprint"}:
        return "decision_data_fingerprint_mismatch"
    if field == "strategy_behavior_hash":
        return "decision_strategy_behavior_hash_mismatch"
    if field in {
        "feature_snapshot_hash",
        "feature_hash",
        "market_feature_hash",
        "canonical_feature_projection_hash",
        "prev_s",
        "prev_l",
        "curr_s",
        "curr_l",
        "gap_ratio",
        "range_ratio",
        "expected_edge_ratio",
        "required_edge_ratio",
    }:
        return "decision_feature_mismatch"
    if field == "final_exit_decision_input_hash":
        return "decision_final_exit_input_mismatch"
    return f"decision_{field}_mismatch"


def _state_coverage_matrix(
    *,
    research_decisions: list[dict[str, Any]],
    runtime_decisions: list[dict[str, Any]],
    mismatch_items: list[dict[str, object]],
    missing_research: list[str],
    missing_runtime: list[str],
    reason_codes: list[str],
) -> dict[str, dict[str, object]]:
    matrix: dict[str, dict[str, object]] = {
        state: {
            "research_decision_count": 0,
            "runtime_decision_count": 0,
            "positive_equivalence_supported": state == "flat_no_dust_no_position",
            "fail_closed_expected": state != "flat_no_dust_no_position",
            "supported_decision_count": 0,
            "unsupported_decision_count": 0,
            "mismatch_count": 0,
            "representative_reason_codes": [],
        }
        for state in STATE_COVERAGE_CLASSES
    }
    key_classes: dict[str, set[str]] = {}
    for source, decisions in (("research", research_decisions), ("runtime", runtime_decisions)):
        for decision in decisions:
            state_class, unsupported_reason = classify_decision_position_state(decision, source=source)
            state_class = state_class if state_class in matrix else (
                "runtime_position_state_not_research_comparable"
                if source == "runtime"
                else "research_model_lacks_lot_native_authority"
            )
            entry = matrix[state_class]
            entry[f"{source}_decision_count"] = int(entry[f"{source}_decision_count"]) + 1
            key = _decision_key(
                _normalize_for_comparison(
                    decision,
                    canonical=is_canonical_decision(decision),
                )
            )
            key_classes.setdefault(key, set()).add(state_class)
            if unsupported_reason:
                entry["unsupported_decision_count"] = int(entry["unsupported_decision_count"]) + 1
                reasons = list(entry["representative_reason_codes"])
                if unsupported_reason not in reasons:
                    reasons.append(unsupported_reason)
                entry["representative_reason_codes"] = sorted(reasons)
            elif position_authority_supports_positive_equivalence(decision):
                entry["supported_decision_count"] = int(entry["supported_decision_count"]) + 1
            dust_detail = _dust_detail_class(decision)
            if dust_detail and state_class == "dust_only":
                dust_details = list(entry.setdefault("dust_detail_classes", []))
                if dust_detail not in dust_details:
                    dust_details.append(dust_detail)
                entry["dust_detail_classes"] = sorted(dust_details)
                operability_states = list(entry.setdefault("dust_operability_states", []))
                operability = _dust_operability_state(dust_detail)
                if operability not in operability_states:
                    operability_states.append(operability)
                entry["dust_operability_states"] = sorted(operability_states)
    for item in mismatch_items:
        key = str(item.get("decision_key") or "")
        for state_class in key_classes.get(key, set()):
            matrix[state_class]["mismatch_count"] = int(matrix[state_class]["mismatch_count"]) + 1
            for reason in _field_reasons(item):
                reasons = list(matrix[state_class]["representative_reason_codes"])
                if reason not in reasons:
                    reasons.append(reason)
                matrix[state_class]["representative_reason_codes"] = sorted(reasons)
    missing_research_set = set(missing_research)
    missing_runtime_set = set(missing_runtime)
    for key in missing_research_set | missing_runtime_set:
        for state_class in key_classes.get(key, set()):
            matrix[state_class]["mismatch_count"] = int(matrix[state_class]["mismatch_count"]) + 1
            reason = "missing_research_decision" if key in missing_research_set else "missing_runtime_decision"
            reasons = list(matrix[state_class]["representative_reason_codes"])
            if reason not in reasons:
                reasons.append(reason)
            matrix[state_class]["representative_reason_codes"] = sorted(reasons)
    for state_class, entry in matrix.items():
        has_decisions = int(entry["research_decision_count"]) > 0 or int(entry["runtime_decision_count"]) > 0
        if int(entry["supported_decision_count"]) > 0 and int(entry["unsupported_decision_count"]) == 0:
            entry["positive_equivalence_supported"] = True
            entry["fail_closed_expected"] = False
        if state_class != "flat_no_dust_no_position" and has_decisions and bool(entry["fail_closed_expected"]):
            reasons = list(entry["representative_reason_codes"])
            if "fail_closed_unmodeled_state" not in reasons:
                reasons.append("fail_closed_unmodeled_state")
            entry["representative_reason_codes"] = sorted(reasons)
    if reason_codes:
        for entry in matrix.values():
            if int(entry["research_decision_count"]) > 0 or int(entry["runtime_decision_count"]) > 0:
                entry["representative_reason_codes"] = sorted(
                    set(list(entry["representative_reason_codes"]) + list(reason_codes[:5]))
                )
    return matrix


def _dust_detail_class(decision: dict[str, Any]) -> str:
    dust_state = str(decision.get("dust_state") or "").strip()
    if dust_state == "harmless_dust":
        return "harmless_dust_effective_flat"
    if dust_state == "blocking_dust":
        return "blocking_dust"
    if dust_state == "dust_only":
        return "dust_only"
    return ""


def _dust_operability_state(dust_detail: str) -> str:
    if dust_detail == "harmless_dust_effective_flat":
        return "entry_gate_effective_flat_but_not_lifecycle_equivalent"
    if dust_detail == "blocking_dust":
        return "entry_blocking_dust"
    return "dust_only_unmodeled"


def _claims_scope(
    *,
    state_coverage_matrix: dict[str, dict[str, object]],
    lifecycle_evidence: CanonicalLifecycleEvidenceBundle | dict[str, Any] | None,
) -> dict[str, object]:
    positive_classes = [
        state
        for state, entry in state_coverage_matrix.items()
        if bool(entry.get("positive_equivalence_supported"))
        and (int(entry.get("research_decision_count") or 0) > 0 or int(entry.get("runtime_decision_count") or 0) > 0)
    ]
    unsupported_classes = [
        state
        for state, entry in state_coverage_matrix.items()
        if bool(entry.get("fail_closed_expected"))
        and (int(entry.get("research_decision_count") or 0) > 0 or int(entry.get("runtime_decision_count") or 0) > 0)
    ]
    fail_closed_count = sum(
        int(entry.get("research_decision_count") or 0) + int(entry.get("runtime_decision_count") or 0)
        for state, entry in state_coverage_matrix.items()
        if bool(entry.get("fail_closed_expected"))
    )
    lifecycle_validation = validate_lifecycle_evidence_scope(lifecycle_evidence)
    lifecycle_scope = lifecycle_validation.as_dict()
    claim_scope = (
        "full_lifecycle_equivalence"
        if lifecycle_validation.full_lifecycle_equivalence_supported
        else "submit_plan_equivalence_only"
    )
    scope_badge = (
        "FULL_LIFECYCLE_EQUIVALENCE"
        if lifecycle_validation.full_lifecycle_equivalence_supported
        else "SUBMIT_PLAN_EQUIVALENCE_ONLY"
    )
    return {
        "claim_scope": claim_scope,
        "scope_badge": scope_badge,
        "positive_equivalence_state_classes": positive_classes,
        "unsupported_state_classes": unsupported_classes,
        "promotion_claim": "positive_decision_equivalence_for_explicitly_modeled_state_classes_only",
        "full_lifecycle_equivalence_supported": lifecycle_validation.full_lifecycle_equivalence_supported,
        "submit_plan_equivalence_supported": True,
        "simulated_fill_equivalence_supported": lifecycle_validation.simulated_fill_equivalence_supported,
        "paper_submit_fill_equivalence_supported": lifecycle_validation.paper_submit_fill_equivalence_supported,
        "live_submit_equivalence_supported": lifecycle_validation.live_submit_equivalence_supported,
        "accounting_replay_equivalence_supported": lifecycle_validation.accounting_replay_equivalence_supported,
        "signal_equivalence_supported": bool(positive_classes),
        "execution_plan_equivalence_supported": True,
        "position_lifecycle_equivalence_supported": lifecycle_validation.position_lifecycle_equivalence_supported,
        "fail_closed_unmodeled_state_count": fail_closed_count,
        "unsupported_lifecycle_reasons": list(lifecycle_validation.reason_codes),
        "lifecycle_evidence_validation": lifecycle_scope,
        "limitations": [
            "research_position_model_cash_qty_simulation_v1_is_not_lot_native_authority",
            "non_flat_dust_reserved_exit_residue_and_recovery_states_fail_closed_until_explicitly_modeled",
            "fail_closed_unmodeled_state_is_not_full_lifecycle_equivalence_evidence",
        ],
    }


def _execution_equivalence_report(
    *,
    normalized_research: list[dict[str, Any]],
    normalized_runtime: list[dict[str, Any]],
    mismatch_items: list[dict[str, object]],
    missing_research: list[str],
    missing_runtime: list[str],
    lifecycle_evidence: CanonicalLifecycleEvidenceBundle | dict[str, Any] | None,
) -> dict[str, object]:
    field_mismatches: dict[str, list[dict[str, object]]] = {}
    for item in mismatch_items:
        if item.get("diagnostic_only"):
            continue
        for field in item.get("fields") or ():
            if isinstance(field, dict):
                field_mismatches.setdefault(str(field.get("field") or ""), []).append(field)
    submit_plan_missing = [
        _decision_key(item)
        for item in normalized_research + normalized_runtime
        if _execution_required_missing(item.get("execution_submit_plan_hash"))
    ]
    summary_missing = [
        _decision_key(item)
        for item in normalized_research + normalized_runtime
        if _execution_required_missing(item.get("execution_summary_hash"))
    ]
    final_action_mismatch = field_mismatches.get("final_action", [])
    submit_plan_mismatch = field_mismatches.get("execution_submit_plan_hash", [])
    signal_mismatch = field_mismatches.get("final_signal", []) + field_mismatches.get("side", [])
    fail_reasons: list[str] = []
    if missing_research or missing_runtime:
        fail_reasons.append("execution_decision_pair_missing")
    if signal_mismatch:
        fail_reasons.append("signal_equivalence_mismatch")
    if final_action_mismatch:
        fail_reasons.append("final_action_equivalence_mismatch")
    if summary_missing:
        fail_reasons.append("execution_summary_evidence_missing")
    if submit_plan_missing:
        fail_reasons.append("execution_submit_plan_evidence_missing")
    if submit_plan_mismatch:
        fail_reasons.append("execution_submit_plan_hash_mismatch")
    supported_scope_ok = not fail_reasons
    lifecycle_validation = validate_lifecycle_evidence_scope(lifecycle_evidence)
    claim_scope = (
        "full_lifecycle_equivalence"
        if lifecycle_validation.full_lifecycle_equivalence_supported
        else "submit_plan_equivalence_only"
    )
    scope_badge = (
        "FULL_LIFECYCLE_EQUIVALENCE"
        if lifecycle_validation.full_lifecycle_equivalence_supported
        else "SUBMIT_PLAN_EQUIVALENCE_ONLY"
    )
    return {
        "schema_version": 1,
        "claim_scope": claim_scope,
        "scope_badge": scope_badge,
        "ok": supported_scope_ok,
        "signal_equivalence_supported": True,
        "signal_equivalence_ok": not signal_mismatch and not missing_research and not missing_runtime,
        "final_action_equivalence_supported": True,
        "final_action_equivalence_ok": not final_action_mismatch and not missing_research and not missing_runtime,
        "submit_plan_equivalence_supported": True,
        "submit_plan_equivalence_ok": not submit_plan_mismatch and not submit_plan_missing,
        "simulated_fill_equivalence_supported": lifecycle_validation.simulated_fill_equivalence_supported,
        "paper_submit_fill_equivalence_supported": lifecycle_validation.paper_submit_fill_equivalence_supported,
        "live_submit_equivalence_supported": lifecycle_validation.live_submit_equivalence_supported,
        "accounting_replay_equivalence_supported": lifecycle_validation.accounting_replay_equivalence_supported,
        "position_lifecycle_equivalence_supported": lifecycle_validation.position_lifecycle_equivalence_supported,
        "full_lifecycle_equivalence_supported": lifecycle_validation.full_lifecycle_equivalence_supported,
        "full_lifecycle_scope_badge": (
            "FULL_LIFECYCLE_EQUIVALENCE_SUPPORTED"
            if lifecycle_validation.full_lifecycle_equivalence_supported
            else "FULL_LIFECYCLE_EQUIVALENCE_UNSUPPORTED"
        ),
        "fail_reasons": sorted(set(fail_reasons)),
        "unsupported_lifecycle_reasons": list(lifecycle_validation.reason_codes),
        "lifecycle_evidence_validation": lifecycle_validation.as_dict(),
        "missing_execution_submit_plan_evidence": submit_plan_missing,
        "missing_execution_summary_evidence": summary_missing,
        "submit_plan_mismatches": submit_plan_mismatch,
        "final_action_mismatches": final_action_mismatch,
    }


def _execution_required_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _drift_counts(
    *,
    mismatch_items: list[dict[str, object]],
    missing_research: list[str],
    missing_runtime: list[str],
) -> dict[str, int]:
    actual = len(missing_research) + len(missing_runtime)
    lifecycle = 0
    for item in mismatch_items:
        if item.get("diagnostic_only"):
            continue
        reasons = set(_field_reasons(item))
        state_classes = set(str(value) for value in item.get("state_classes") or ())
        if reasons == {"decision_position_dust_mismatch"} and any(
            state != "flat_no_dust_no_position" for state in state_classes
        ):
            lifecycle += 1
        else:
            actual += 1
    return {
        "actual_semantic_drift_count": actual,
        "lifecycle_unmodeled_mismatch_count": lifecycle,
    }


def _equivalence_outcome(
    *,
    reason_codes: list[str],
    canonical_incomplete_decision_count: int,
    binding_items: list[dict[str, object]],
    exact_mismatch_count: int,
    missing_research: list[str],
    missing_runtime: list[str],
    actual_semantic_drift_count: int,
    state_coverage_matrix: dict[str, dict[str, object]],
) -> str:
    if binding_items or any(
        "export_" in code or code.endswith("_not_bound_to_report")
        for code in reason_codes
    ):
        return "FAIL_EXPORT_BINDING"
    if canonical_incomplete_decision_count > 0 or any(code.startswith("canonical_decision_") for code in reason_codes):
        return "FAIL_INCOMPLETE_CANONICAL_PAYLOAD"
    if actual_semantic_drift_count > 0:
        return "FAIL_ACTUAL_DRIFT"
    unsupported_count = sum(
        int(entry.get("research_decision_count") or 0) + int(entry.get("runtime_decision_count") or 0)
        for state, entry in state_coverage_matrix.items()
        if bool(entry.get("fail_closed_expected"))
    )
    if unsupported_count > 0:
        return "FAIL_CLOSED_UNMODELED_STATE"
    if exact_mismatch_count > 0 or missing_research or missing_runtime or reason_codes:
        return "FAIL_ACTUAL_DRIFT"
    return "PASS_POSITIVE_EQUIVALENCE"


def _recommended_next_action(
    *,
    outcome: str | None = None,
    reason_codes: list[str],
    canonical_comparison: bool,
    state_coverage_matrix: dict[str, dict[str, object]] | None = None,
) -> str:
    if outcome == "FAIL_CLOSED_UNMODELED_STATE":
        return "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
    if not canonical_comparison:
        return "regenerate_decisions_with_repo_owned_export_commands"
    if "decision_export_artifact_unverified" in reason_codes:
        return "regenerate_decisions_with_repo_owned_export_commands"
    if any(code.startswith("canonical_decision_") for code in reason_codes):
        return "regenerate_decisions_with_repo_owned_export_commands"
    if any(code.endswith("_not_bound_to_report") for code in reason_codes):
        return "bind_decisions_to_requested_profile_market_interval_data_fingerprint"
    if "decision_order_rules_mismatch" in reason_codes:
        return "populate_runtime_order_rules_hash_before_replay"
    if not reason_codes:
        return "none"
    if "decision_timestamp_candle_basis_mismatch" in reason_codes:
        return "align_candle_cutoff_through_ts_and_execution_timing_policy_then_replay"
    if "decision_exit_rule_mismatch" in reason_codes:
        return "inspect_strategy_exit_rule_profile_and_runtime_configuration"
    if "decision_position_dust_mismatch" in reason_codes:
        return "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
    if "runtime_position_state_not_research_comparable" in reason_codes:
        return "scope_runtime_only_state_as_fail_closed_or_extend_research_model"
    if "decision_fee_authority_mismatch" in reason_codes:
        return "inspect_fee_authority_order_rules_and_cost_model_inputs"
    if "decision_regime_mismatch" in reason_codes:
        return "replay_runtime_with_approved_regime_policy"
    if state_coverage_matrix and any(
        state != "flat_no_dust_no_position"
        and int(entry.get("research_decision_count") or 0) + int(entry.get("runtime_decision_count") or 0) > 0
        for state, entry in state_coverage_matrix.items()
    ):
        return "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
    return "inspect_research_runtime_decision_drift_before_promotion"
