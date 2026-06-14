from __future__ import annotations

import math
from typing import Any

from .experiment_manifest import FinalSelectionContract
from .hashing import sha256_prefixed


FINAL_SELECTION_SCHEMA_VERSION = 1
LEGACY_IMPLICIT_FINAL_RANK_WARNING = "legacy_implicit_final_rank_policy_v1"


def is_computed_candidate(candidate: dict[str, Any]) -> bool:
    return (
        candidate.get("metrics_v2_source") == "computed"
        and candidate.get("candidate_failed_before_complete_metrics") is False
        and candidate.get("evaluation_status") == "completed"
        and candidate.get("metrics_status") == "complete"
    )


def apply_final_selection_contract(
    *,
    contract: FinalSelectionContract | dict[str, Any] | None,
    candidates: list[dict[str, Any]],
    report_context: dict[str, Any],
    production_bound: bool,
) -> dict[str, Any]:
    contract_payload = _contract_payload(contract)
    if contract_payload is None:
        reasons = ["final_selection_contract_missing"] if production_bound else [LEGACY_IMPLICIT_FINAL_RANK_WARNING]
        return {
            "final_selection_schema_version": FINAL_SELECTION_SCHEMA_VERSION,
            "final_selection_contract": None,
            "final_selection_contract_hash": None,
            "candidate_universe": None,
            "selected_candidate_id": None,
            "selected_candidate_score_hash": None,
            "candidate_final_scores_hash": None,
            "candidate_final_scores": [],
            "gate_result": "FAIL" if production_bound else "WARN",
            "fail_reasons": reasons,
        }

    contract_hash = sha256_prefixed(contract_payload)
    candidate_ids = [str(candidate.get("parameter_candidate_id") or "") for candidate in candidates]
    duplicate_ids = sorted({candidate_id for candidate_id in candidate_ids if candidate_ids.count(candidate_id) > 1})
    if duplicate_ids:
        return {
            "final_selection_schema_version": FINAL_SELECTION_SCHEMA_VERSION,
            "final_selection_contract": contract_payload,
            "final_selection_contract_hash": contract_hash,
            "candidate_universe": contract_payload.get("candidate_universe"),
            "selected_candidate_id": None,
            "selected_candidate_score_hash": None,
            "candidate_final_scores_hash": None,
            "candidate_final_scores": [],
            "gate_result": "FAIL",
            "fail_reasons": ["final_selection_duplicate_candidate_id"],
        }
    ranking = list(contract_payload.get("ranking") or [])
    scored = [
        _score_candidate(
            contract=contract_payload,
            ranking=ranking,
            candidate=candidate,
            report_context=report_context,
        )
        for candidate in candidates
    ]
    eligible = [item for item in scored if item["eligible"]]
    selected: dict[str, Any] | None = None
    if eligible:
        selected = min(eligible, key=lambda item: tuple(item["_sort_key"]))
    # The score list is evidence, not presentation. Canonicalize it by the
    # final-selection rank tuple and candidate id so its hash cannot inherit
    # input order or the legacy _candidate_rank_key ordering used elsewhere.
    scored = sorted(
        scored,
        key=lambda item: (
            0 if item["eligible"] else 1,
            tuple(item["_sort_key"]),
            str(item["candidate_id"]),
        ),
    )
    public_scores = [{key: value for key, value in item.items() if key != "_sort_key"} for item in scored]
    for item in public_scores:
        item["score_hash"] = sha256_prefixed(
            {key: value for key, value in item.items() if key != "score_hash"}
        )
    selected_public = None
    if selected is not None:
        selected_public = next(
            item for item in public_scores if item["candidate_id"] == selected["candidate_id"]
        )
    fail_reasons = sorted(
        {
            str(reason)
            for item in public_scores
            if not item["eligible"]
            for reason in item.get("eligibility_reasons") or []
        }
    )
    if not eligible:
        fail_reasons = sorted(set(fail_reasons) | {"final_selection_no_eligible_candidates"})
    scores_hash = sha256_prefixed(public_scores) if public_scores else None
    return {
        "final_selection_schema_version": FINAL_SELECTION_SCHEMA_VERSION,
        "final_selection_contract": contract_payload,
        "final_selection_contract_hash": contract_hash,
        "candidate_universe": contract_payload.get("candidate_universe"),
        "selected_candidate_id": selected_public.get("candidate_id") if selected_public else None,
        "selected_candidate_score_hash": selected_public.get("score_hash") if selected_public else None,
        "candidate_final_scores_hash": scores_hash,
        "candidate_final_scores": public_scores,
        "gate_result": "PASS" if selected_public is not None else "FAIL",
        "fail_reasons": [] if selected_public is not None else fail_reasons,
    }


def validate_final_selection_report(report: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if report.get("final_selection_required") and not isinstance(report.get("final_selection_contract"), dict):
        return ["final_selection_contract_missing"]
    if not report.get("final_selection_required") and report.get("final_selection_gate_result") == "WARN":
        return []
    contract = report.get("final_selection_contract")
    if not isinstance(contract, dict):
        return ["final_selection_contract_missing"]
    if not str(report.get("final_selection_contract_hash") or "").startswith("sha256:"):
        reasons.append("final_selection_contract_hash_missing")
    elif sha256_prefixed(contract) != report.get("final_selection_contract_hash"):
        reasons.append("final_selection_contract_hash_mismatch")
    candidates = report.get("candidates")
    if not isinstance(candidates, list) or not all(isinstance(item, dict) for item in candidates):
        return sorted(set(reasons) | {"final_selection_score_hash_mismatch"})
    recomputed = apply_final_selection_contract(
        contract=contract,
        candidates=list(candidates),
        report_context=report,
        production_bound=bool(report.get("final_selection_required")),
    )
    for field, missing_reason, mismatch_reason in (
        ("candidate_final_scores_hash", "final_selection_score_hash_missing", "final_selection_score_hash_mismatch"),
        ("selected_candidate_score_hash", "final_selection_score_hash_missing", "final_selection_score_hash_mismatch"),
    ):
        expected = report.get(field)
        if not str(expected or "").startswith("sha256:"):
            reasons.append(missing_reason)
        elif expected != recomputed.get(field):
            reasons.append(mismatch_reason)
    if report.get("final_selection_contract_hash") != recomputed.get("final_selection_contract_hash"):
        reasons.append("final_selection_contract_hash_mismatch")
    if report.get("selected_candidate_id") != recomputed.get("selected_candidate_id"):
        reasons.append("final_selection_selected_candidate_mismatch")
    if report.get("best_candidate_id") != recomputed.get("selected_candidate_id"):
        reasons.append("final_selection_selected_candidate_mismatch")
    if report.get("final_selection_gate_result") != "PASS" or recomputed.get("gate_result") != "PASS":
        reasons.append("final_selection_gate_not_passed")
    return sorted(set(reasons))


def _contract_payload(contract: FinalSelectionContract | dict[str, Any] | None) -> dict[str, Any] | None:
    if contract is None:
        return None
    if isinstance(contract, FinalSelectionContract):
        return contract.as_dict()
    if isinstance(contract, dict):
        return dict(contract)
    return None


def _score_candidate(
    *,
    contract: dict[str, Any],
    ranking: list[Any],
    candidate: dict[str, Any],
    report_context: dict[str, Any],
) -> dict[str, Any]:
    reasons = _candidate_universe_reasons(contract=contract, candidate=candidate)
    metric_source_reasons = _metric_source_semantics_reasons(candidate)
    reasons.extend(metric_source_reasons)
    reasons.extend(_fallback_metrics_reasons(candidate))
    reasons.extend(_must_pass_reasons(contract=contract, candidate=candidate, report_context=report_context))
    components: list[dict[str, Any]] = []
    sort_key: list[Any] = []
    rank_tuple: list[Any] = []
    for rule in ranking:
        if not isinstance(rule, dict):
            reasons.append("final_selection_ranking_rule_malformed")
            continue
        metric = str(rule.get("metric") or "")
        order = str(rule.get("order") or "asc")
        required = bool(rule.get("required", True))
        null_policy = str(rule.get("null_policy") or contract.get("null_metric_policy") or "")
        unsupported_reason = _unsupported_metric_reason(metric)
        value, source = _metric_value(candidate=candidate, metric=metric)
        if unsupported_reason is not None and required:
            reasons.append(unsupported_reason)
        elif value is None and required:
            reasons.append(f"final_selection_required_metric_missing:{metric}")
        component = {
            "metric": metric,
            "value": _json_scalar(value),
            "order": order,
            "required": required,
            "null_policy": null_policy,
            "source": source,
            "primary_metric_source_semantics": candidate.get("primary_metric_source_semantics"),
            "primary_metric_scenario_role": candidate.get("primary_metric_scenario_role"),
            "primary_metric_scenario_id": candidate.get("primary_metric_scenario_id"),
            "aggregate_gate_source": candidate.get("aggregate_gate_source"),
        }
        components.append(component)
        sort_value = _sort_value(value=value, order=order, required=required)
        sort_key.append(sort_value)
        rank_tuple.append(_json_scalar(sort_value))
    return {
        "candidate_id": str(candidate.get("parameter_candidate_id") or ""),
        "eligible": not reasons,
        "eligibility_reasons": sorted(set(reasons)),
        "rank_tuple": rank_tuple,
        "rank_components": components,
        "selection_metric_policy": {
            "primary_metric_source": candidate.get("primary_metric_source"),
            "primary_metric_source_semantics": candidate.get("primary_metric_source_semantics"),
            "primary_metric_scenario_role": candidate.get("primary_metric_scenario_role"),
            "primary_metric_scenario_id": candidate.get("primary_metric_scenario_id"),
            "aggregate_gate_source": candidate.get("aggregate_gate_source"),
            "candidate_eligibility_gate": "aggregate_acceptance_gate_result",
        },
        "_sort_key": sort_key,
    }


def _candidate_universe_reasons(*, contract: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    universe = contract.get("candidate_universe")
    if universe != "acceptance_gate_passed_required_scenarios":
        return ["final_selection_candidate_universe_unsupported"]
    aggregate_gate = candidate.get("aggregate_acceptance_gate_result", candidate.get("acceptance_gate_result"))
    if aggregate_gate != "PASS":
        return ["final_selection_acceptance_gate_not_passed"]
    return []


def _metric_source_semantics_reasons(candidate: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if candidate.get("primary_metric_source_semantics") != "primary_base_scenario_alias":
        reasons.append("final_selection_primary_metric_source_semantics_missing")
    if candidate.get("primary_metric_scenario_role") != "base":
        reasons.append("final_selection_primary_metric_scenario_role_missing")
    if candidate.get("aggregate_gate_source") != "required_scenario_policy":
        reasons.append("final_selection_aggregate_gate_source_missing")
    return reasons


def _fallback_metrics_reasons(candidate: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not is_computed_candidate(candidate):
        reasons.append("final_selection_candidate_not_computed_complete")
    if bool(candidate.get("candidate_failed_before_complete_metrics")):
        reasons.append("final_selection_candidate_failed_before_complete_metrics")
    if candidate.get("metrics_status") == "unavailable":
        reasons.append("final_selection_metrics_unavailable")
    if candidate.get("metrics_v2_source") == "failure_fallback":
        reasons.append("final_selection_metrics_failure_fallback")
    if candidate.get("evaluation_status") != "completed":
        reasons.append("final_selection_evaluation_not_completed")
    if candidate.get("metrics_status") != "complete":
        reasons.append("final_selection_metrics_not_complete")
    if candidate.get("metrics_v2_source") != "computed":
        reasons.append("final_selection_metrics_not_computed")
    for split_key in ("train_metrics_v2", "validation_metrics_v2", "final_holdout_metrics_v2"):
        metrics = candidate.get(split_key)
        if not isinstance(metrics, dict):
            continue
        if metrics.get("metrics_status") == "unavailable":
            reasons.append(f"final_selection_{split_key}_unavailable")
        if metrics.get("metrics_v2_source") == "failure_fallback":
            reasons.append(f"final_selection_{split_key}_failure_fallback")
        if bool(metrics.get("candidate_failed_before_complete_metrics")):
            reasons.append(f"final_selection_{split_key}_candidate_failed_before_complete_metrics")
    return sorted(set(reasons))


def _must_pass_reasons(
    *,
    contract: dict[str, Any],
    candidate: dict[str, Any],
    report_context: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    must_pass = contract.get("must_pass") if isinstance(contract.get("must_pass"), dict) else {}
    for field, expected in must_pass.items():
        actual = _must_pass_value(field=str(field), candidate=candidate, report_context=report_context)
        if actual != expected:
            reasons.append(f"final_selection_must_pass_failed:{field}")
    return reasons


def _must_pass_value(*, field: str, candidate: dict[str, Any], report_context: dict[str, Any]) -> Any:
    if field == "dataset_quality_gate_status":
        return report_context.get("dataset_quality_gate_status")
    if field == "statistical_gate_result":
        return candidate.get("statistical_gate_result") or report_context.get("statistical_gate_result")
    if field == "stress_suite_gate_result":
        return candidate.get("stress_suite_gate_result") or report_context.get("stress_suite_gate_result")
    if field == "production_calibration_policy_result":
        value = candidate.get("production_calibration_policy_result")
        return value.get("status") if isinstance(value, dict) else value
    if field == "metrics_schema_version":
        return candidate.get("metrics_schema_version")
    if field == "final_holdout_present":
        return candidate.get("final_holdout_present")
    return candidate.get(field, report_context.get(field))


def _metric_value(*, candidate: dict[str, Any], metric: str) -> tuple[Any, str]:
    if metric == "parameter_candidate_id":
        return str(candidate.get("parameter_candidate_id") or ""), "candidate.parameter_candidate_id"
    prefixes = {
        "validation.metrics_v2.": "validation_metrics_v2",
        "final_holdout.metrics_v2.": "final_holdout_metrics_v2",
        "validation.stress.": "validation_stress_suite",
        "final_holdout.stress.": "final_holdout_stress_suite",
        "validation.benchmark.": "benchmark_metrics.validation",
        "final_holdout.benchmark.": "benchmark_metrics.final_holdout",
    }
    for prefix, source_key in prefixes.items():
        if metric.startswith(prefix):
            source = _source_payload(candidate, source_key)
            if (
                isinstance(source, dict)
                and source_key.endswith("metrics_v2")
                and (source.get("metrics_status") == "unavailable" or source.get("metrics_v2_source") == "failure_fallback")
            ):
                return None, source_key
            value = _nested_value(source, metric[len(prefix):])
            return value, source_key
    return _nested_value(candidate, metric), "candidate"


def _source_payload(candidate: dict[str, Any], source_key: str) -> Any:
    if source_key.startswith("benchmark_metrics."):
        metrics = candidate.get("benchmark_metrics")
        if not isinstance(metrics, dict):
            return None
        split = source_key.split(".", 1)[1]
        return metrics.get(split)
    return candidate.get(source_key)


def _nested_value(payload: Any, dotted: str) -> Any:
    current = payload
    for part in dotted.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    if isinstance(current, (int, float)) and not isinstance(current, bool):
        value = float(current)
        return value if math.isfinite(value) else None
    if isinstance(current, str):
        return current
    if isinstance(current, bool):
        return current
    return None


def _unsupported_metric_reason(metric: str) -> str | None:
    if metric.endswith("sharpe_ratio") or ".sharpe_ratio" in metric or metric == "sharpe_ratio":
        return "final_selection_sharpe_unavailable_without_period_return_series"
    if metric.endswith("sortino_ratio") or ".sortino_ratio" in metric or metric == "sortino_ratio":
        return "final_selection_sortino_unavailable_without_period_return_series"
    return None


def _sort_value(*, value: Any, order: str, required: bool) -> Any:
    if value is None:
        return math.inf
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        value = 1.0 if value else 0.0
    numeric = float(value)
    if not math.isfinite(numeric):
        return math.inf
    return numeric if order == "asc" else -numeric


def _json_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    return value
