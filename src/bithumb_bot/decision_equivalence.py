from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical_decision import (
    CANONICAL_DECISION_SCHEMA_FIELDS,
    is_canonical_decision,
    normalize_canonical_decision,
    validate_canonical_decision_payload,
)
from .research.hashing import content_hash_payload, sha256_prefixed


DECISION_EQUIVALENCE_SCHEMA_VERSION = 2
CANONICAL_COMPARISON_CONTRACT_VERSION = "canonical_decision_v1"
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
CANONICAL_EQUIVALENCE_FIELDS = CANONICAL_DECISION_SCHEMA_FIELDS
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


def compare_decision_equivalence(
    *,
    research_decisions: list[dict[str, Any]],
    runtime_decisions: list[dict[str, Any]],
    profile_hash: str,
    market: str,
    interval: str,
    data_fingerprint: str,
    generated_at: str | None = None,
) -> DecisionEquivalenceResult:
    canonical_comparison = all(is_canonical_decision(item) for item in research_decisions + runtime_decisions)
    comparison_fields = CANONICAL_EQUIVALENCE_FIELDS if canonical_comparison else LEGACY_DECISION_FIELDS
    comparison_contract_version = (
        CANONICAL_COMPARISON_CONTRACT_VERSION if canonical_comparison else LEGACY_COMPARISON_CONTRACT_VERSION
    )
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
        if field_mismatches:
            mismatch_items.append(
                {
                    "decision_key": key,
                    "reason_code": "decision_field_mismatch",
                    "fields": field_mismatches,
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
    promotion_grade_comparison = bool(canonical_comparison and canonical_incomplete_decision_count == 0 and not binding_items)
    report: dict[str, Any] = {
        "schema_version": DECISION_EQUIVALENCE_SCHEMA_VERSION,
        "comparison_contract_version": comparison_contract_version,
        "canonical_schema": canonical_comparison,
        "legacy_schema": not canonical_comparison,
        "promotion_grade_comparison": promotion_grade_comparison,
        "ok": not reason_codes,
        "reason_codes": sorted(set(reason_codes)),
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
        "canonical_missing_field_count": sum(len(fields) for fields in canonical_missing_fields_by_decision.values()),
        "canonical_missing_fields_by_decision": canonical_missing_fields_by_decision,
        "canonical_incomplete_decision_count": canonical_incomplete_decision_count,
        "canonical_validation": canonical_validation_items,
        "binding_validation": binding_items,
        "recommended_next_action": _recommended_next_action(
            reason_codes=sorted(set(reason_codes)),
            canonical_comparison=canonical_comparison,
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
    generated_at: str | None = None,
) -> DecisionEquivalenceResult:
    result = compare_decision_equivalence(
        research_decisions=research_artifact.decisions,
        runtime_decisions=runtime_artifact.decisions,
        profile_hash=profile_hash,
        market=market,
        interval=interval,
        data_fingerprint=data_fingerprint,
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
            "repo_owned_export_artifacts": True,
            "legacy_or_unverified_export": False,
            "artifact_binding_validation": artifact_binding,
            "reason_codes": reason_codes,
        }
    )
    if artifact_binding:
        report["promotion_grade_comparison"] = False
        report["ok"] = False
        report["recommended_next_action"] = _recommended_next_action(
            reason_codes=reason_codes,
            canonical_comparison=bool(report.get("canonical_schema")),
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
        return normalize_canonical_decision(item)
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
            result = validate_canonical_decision_payload(item, promotion_grade=True)
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


def _reason_for_field(field: str) -> str:
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
    if field in {"profile_content_hash", "candidate_profile_hash"}:
        return "decision_profile_hash_mismatch"
    if field in {"dataset_content_hash", "db_data_fingerprint"}:
        return "decision_data_fingerprint_mismatch"
    if field in {"feature_hash", "prev_s", "prev_l", "curr_s", "curr_l", "gap_ratio", "range_ratio", "expected_edge_ratio", "required_edge_ratio"}:
        return "decision_feature_mismatch"
    return f"decision_{field}_mismatch"


def _recommended_next_action(*, reason_codes: list[str], canonical_comparison: bool) -> str:
    if not canonical_comparison:
        return "regenerate_decisions_with_canonical_schema_before_promotion"
    if any(code.startswith("canonical_decision_") for code in reason_codes):
        return "regenerate_decisions_with_canonical_schema_before_promotion"
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
        return "inspect_runtime_position_snapshot_dust_state_and_lot_authority"
    if "decision_fee_authority_mismatch" in reason_codes:
        return "inspect_fee_authority_order_rules_and_cost_model_inputs"
    if "decision_regime_mismatch" in reason_codes:
        return "inspect_candidate_regime_policy_and_market_regime_snapshot"
    return "inspect_research_runtime_decision_drift_before_promotion"
