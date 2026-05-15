from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic

from .deployment_policy import is_production_bound_target
from .experiment_manifest import ExperimentManifest, StatisticalSelectionContract
from .family_registry import validate_family_registry_binding
from .hashing import content_hash_payload, sha256_prefixed
from .return_panel import validate_return_panel_binding


STATISTICAL_SELECTION_EVIDENCE_SCHEMA_VERSION = 1
PRIMARY_METRIC_SOURCE = "validation_metrics"
SCREENING_SUMMARY_BOOTSTRAP = "SCREENING_SUMMARY_BOOTSTRAP"
PROMOTION_GRADE_WRC = "PROMOTION_GRADE_WRC"
PROMOTION_GRADE_WRC_SPA_DSR = "PROMOTION_GRADE_WRC_SPA_DSR"
SCREENING_METHOD = "metric_centered_max_bootstrap"
SCREENING_METHOD_DETAIL = "summary_metric_centered_max_bootstrap"
PROMOTION_GRADE_EVIDENCE_GRADES = {PROMOTION_GRADE_WRC, PROMOTION_GRADE_WRC_SPA_DSR}
PROMOTION_GRADE_METHODS = {"white_reality_check_block_bootstrap", "white_reality_check_stationary_bootstrap"}


def statistical_validation_required(manifest_or_payload: ExperimentManifest | dict[str, Any]) -> bool:
    if isinstance(manifest_or_payload, ExperimentManifest):
        if manifest_or_payload.statistical_validation is not None:
            return bool(manifest_or_payload.statistical_validation.required_for_promotion)
        return is_production_bound_target(manifest_or_payload.deployment_tier)
    contract = manifest_or_payload.get("statistical_validation_contract")
    if isinstance(contract, dict) and contract.get("required_for_promotion") is not None:
        return bool(contract.get("required_for_promotion"))
    return is_production_bound_target(manifest_or_payload.get("deployment_tier"))


def selection_universe_hash(
    *,
    manifest_hash: str,
    dataset_content_hash: str,
    dataset_quality_hash: str | None,
    experiment_family_id: str | None,
    hypothesis_id: str | None,
    hypothesis_status: str | None,
    candidates: list[dict[str, Any]],
    required_scenario_ids: list[str],
    primary_metric_source: str,
    benchmark: str,
    statistical_validation_contract: dict[str, Any],
) -> str:
    return sha256_prefixed(
        {
            "manifest_hash": manifest_hash,
            "dataset_content_hash": dataset_content_hash,
            "dataset_quality_hash": dataset_quality_hash,
            "experiment_family_id": experiment_family_id,
            "hypothesis_id": hypothesis_id,
            "hypothesis_status": hypothesis_status,
            "candidates": [
                {
                    "candidate_id": str(candidate.get("parameter_candidate_id") or ""),
                    "parameter_values": candidate.get("parameter_values") or {},
                }
                for candidate in sorted(candidates, key=lambda item: str(item.get("parameter_candidate_id") or ""))
            ],
            "required_scenario_ids": sorted(str(item) for item in required_scenario_ids),
            "primary_metric_source": primary_metric_source,
            "benchmark": benchmark,
            "statistical_validation_contract": statistical_validation_contract,
        }
    )


def candidate_metric_universe_payload(
    *,
    candidates: list[dict[str, Any]],
    required_scenario_ids: list[str],
    primary_metric: str,
    primary_metric_source: str,
    benchmark: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for candidate in sorted(candidates, key=lambda item: str(item.get("parameter_candidate_id") or "")):
        metrics = candidate.get(primary_metric_source)
        value = _metric_value(metrics, primary_metric, benchmark) if isinstance(metrics, dict) else None
        candidate_required_scenarios = candidate.get("required_scenario_ids")
        if not isinstance(candidate_required_scenarios, list):
            candidate_required_scenarios = required_scenario_ids
        rows.append(
            {
                "candidate_id": str(candidate.get("parameter_candidate_id") or ""),
                "parameter_values": candidate.get("parameter_values") or {},
                "scenario_policy": candidate.get("scenario_policy"),
                "required_scenario_ids": sorted(str(item) for item in candidate_required_scenarios),
                "primary_metric": primary_metric,
                "primary_metric_source": primary_metric_source,
                "validation_metric_value": value,
                "validation_metric_missing": value is None,
                "acceptance_gate_result": candidate.get("acceptance_gate_result"),
            }
        )
    return {
        "primary_metric": primary_metric,
        "primary_metric_source": primary_metric_source,
        "benchmark": benchmark,
        "required_scenario_ids": sorted(str(item) for item in required_scenario_ids),
        "candidates": rows,
    }


def candidate_metric_values_hash(
    *,
    candidates: list[dict[str, Any]],
    required_scenario_ids: list[str],
    primary_metric: str,
    primary_metric_source: str,
    benchmark: str,
) -> str:
    return sha256_prefixed(
        candidate_metric_universe_payload(
            candidates=candidates,
            required_scenario_ids=required_scenario_ids,
            primary_metric=primary_metric,
            primary_metric_source=primary_metric_source,
            benchmark=benchmark,
        )
    )


def recompute_candidate_metric_values_hash_from_report(
    *,
    report: dict[str, Any],
    evidence: dict[str, Any],
) -> str | None:
    candidates = report.get("candidates")
    required_scenario_ids = evidence.get("required_scenario_ids")
    primary_metric = evidence.get("primary_metric")
    primary_metric_source = evidence.get("primary_metric_source")
    benchmark = evidence.get("benchmark")
    if not isinstance(candidates, list) or not all(isinstance(item, dict) for item in candidates):
        return None
    if not isinstance(required_scenario_ids, list):
        return None
    if not isinstance(primary_metric, str) or not primary_metric:
        return None
    if not isinstance(primary_metric_source, str) or not primary_metric_source:
        return None
    if not isinstance(benchmark, str) or not benchmark:
        return None
    return candidate_metric_values_hash(
        candidates=candidates,
        required_scenario_ids=[str(item) for item in required_scenario_ids],
        primary_metric=primary_metric,
        primary_metric_source=primary_metric_source,
        benchmark=benchmark,
    )


def build_statistical_selection_evidence(
    *,
    manifest: ExperimentManifest,
    candidates: list[dict[str, Any]],
    manifest_hash: str,
    dataset_content_hash: str,
    dataset_quality_hash: str | None,
    experiment_family_id: str | None,
    hypothesis_id: str | None,
    hypothesis_status: str | None,
    selection_hash: str,
    required_scenario_ids: list[str] | None = None,
    search_budget: int,
    parameter_grid_size: int,
    attempt_index: int,
    holdout_reuse_count: int,
    dataset_reuse_policy: str,
    return_panel: dict[str, Any] | None = None,
    return_panel_path: Path | None = None,
    family_trial_registry_prior_hash: str | None = None,
    family_trial_registry_path: Path | None = None,
    family_trial_registry_row_hash: str | None = None,
) -> dict[str, Any] | None:
    contract = manifest.statistical_validation
    if contract is None:
        return None
    contract_payload = contract.as_dict()
    primary_metric_source = PRIMARY_METRIC_SOURCE
    required_scenario_ids = list(required_scenario_ids or [])
    metric_payload = candidate_metric_universe_payload(
        candidates=candidates,
        required_scenario_ids=required_scenario_ids,
        primary_metric=contract.primary_metric,
        primary_metric_source=primary_metric_source,
        benchmark=contract.benchmark,
    )
    metric_values = _candidate_metric_values_from_payload(metric_payload)
    metric_value_count = len(metric_values)
    missing_metric_count = len(candidates) - metric_value_count
    effective_trial_count = _effective_trial_count(
        candidate_count=len(candidates),
        metric_value_count=metric_value_count,
        search_budget=search_budget,
        parameter_grid_size=parameter_grid_size,
        attempt_index=attempt_index,
        holdout_reuse_count=holdout_reuse_count,
    )
    p_value, seed = _metric_centered_max_bootstrap_p_value(
        metric_values=metric_values,
        n_bootstrap=contract.bootstrap.n_bootstrap,
        selection_hash=selection_hash,
    )
    gate_reasons = _statistical_gate_fail_reasons(
        contract=contract,
        p_value=p_value,
        attempt_index=attempt_index,
        holdout_reuse_count=holdout_reuse_count,
        metric_values=metric_values,
        candidate_count=len(candidates),
    )
    sampling_contract = _bootstrap_sampling_contract(
        contract=contract,
        selection_hash=selection_hash,
        observation_count=int(return_panel.get("observation_count") or 0) if isinstance(return_panel, dict) else 0,
        return_unit=str(return_panel.get("return_unit") or "unavailable") if isinstance(return_panel, dict) else "unavailable",
        benchmark=contract.benchmark,
    )
    payload: dict[str, Any] = {
        "artifact_type": "statistical_selection_evidence",
        "schema_version": STATISTICAL_SELECTION_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": manifest.experiment_id,
        "experiment_family_id": experiment_family_id,
        "hypothesis_id": hypothesis_id,
        "manifest_hash": manifest_hash,
        "dataset_content_hash": dataset_content_hash,
        "dataset_quality_hash": dataset_quality_hash,
        "selection_universe_hash": selection_hash,
        "candidate_metric_values_hash": sha256_prefixed(metric_payload),
        "required_scenario_ids": sorted(str(item) for item in required_scenario_ids),
        "candidate_metric_values_summary": {
            "candidate_count": len(candidates),
            "metric_value_count": metric_value_count,
            "missing_metric_count": missing_metric_count,
            "primary_metric": contract.primary_metric,
            "primary_metric_source": primary_metric_source,
            "benchmark": contract.benchmark,
        },
        "candidate_count": len(candidates),
        "metric_value_count": metric_value_count,
        "missing_metric_count": missing_metric_count,
        "search_budget": search_budget,
        "parameter_grid_size": parameter_grid_size,
        "attempt_index": attempt_index,
        "holdout_reuse_count": holdout_reuse_count,
        "dataset_reuse_policy": dataset_reuse_policy,
        "benchmark": contract.benchmark,
        "primary_metric": contract.primary_metric,
        "primary_metric_source": primary_metric_source,
        "bootstrap_method": contract.bootstrap.method,
        "statistical_method": SCREENING_METHOD_DETAIL,
        "evidence_grade": SCREENING_SUMMARY_BOOTSTRAP,
        "minimum_promotion_evidence_grade": PROMOTION_GRADE_WRC,
        "promotion_grade_available": False,
        "bootstrap_sampling_contract": sampling_contract,
        "bootstrap_sampling_contract_hash": sampling_contract["content_hash"],
        "return_panel_path": str(return_panel_path.resolve()) if return_panel_path else None,
        "return_panel_hash": return_panel.get("content_hash") if isinstance(return_panel, dict) else None,
        "return_panel_artifact_type": return_panel.get("artifact_type") if isinstance(return_panel, dict) else None,
        "return_panel_split": return_panel.get("split") if isinstance(return_panel, dict) else None,
        "return_unit": return_panel.get("return_unit") if isinstance(return_panel, dict) else "unavailable",
        "return_panel_observation_count": return_panel.get("observation_count") if isinstance(return_panel, dict) else 0,
        "family_trial_registry_path": str(family_trial_registry_path.resolve()) if family_trial_registry_path else None,
        "family_trial_registry_prior_hash": family_trial_registry_prior_hash,
        "family_trial_registry_row_hash": family_trial_registry_row_hash,
        "n_bootstrap": contract.bootstrap.n_bootstrap,
        "block_length": None,
        "block_length_policy": contract.bootstrap.block_length_policy,
        "seed": seed,
        "effective_trial_count": effective_trial_count,
        "summary_metric_max_bootstrap_p_value": p_value,
        "selection_adjusted_summary_p_value": p_value,
        "white_reality_check_p_value": None,
        "white_reality_check_method": None,
        "white_reality_check_available": False,
        "statistical_gate_result": "FAIL" if gate_reasons else "PASS",
        "gate_fail_reasons": gate_reasons,
        "limitations": _limitations(contract),
        "promotion_grade_limitations": _promotion_grade_limitations(contract),
        "statistical_validation_contract": contract_payload,
    }
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    return payload


def write_statistical_selection_evidence(
    *,
    manager: PathManager,
    experiment_id: str,
    evidence: dict[str, Any],
) -> Path:
    path = manager.data_dir() / "reports" / "research" / experiment_id / "statistical_selection_evidence.json"
    _ensure_research_output_path_allowed(manager, path)
    write_json_atomic(path, evidence)
    return path


def validate_statistical_evidence_for_candidate(
    *,
    candidate: dict[str, Any],
    report: dict[str, Any],
    evidence: dict[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    required = bool(candidate.get("statistical_validation_required")) or is_production_bound_target(
        candidate.get("deployment_tier") or report.get("deployment_tier")
    )
    contract = candidate.get("statistical_validation_contract") or report.get("statistical_validation_contract")
    if required and not isinstance(contract, dict):
        reasons.append("statistical_contract_missing")
    if not required:
        return reasons
    if not isinstance(evidence, dict):
        reasons.append("statistical_evidence_missing")
        return reasons
    expected_hash = str(candidate.get("statistical_evidence_hash") or report.get("statistical_evidence_hash") or "")
    if not expected_hash.startswith("sha256:"):
        reasons.append("statistical_evidence_hash_missing")
    actual_hash = sha256_prefixed(content_hash_payload({k: v for k, v in evidence.items() if k != "content_hash"}))
    embedded_hash = str(evidence.get("content_hash") or "")
    if expected_hash.startswith("sha256:") and actual_hash != expected_hash:
        reasons.append("statistical_evidence_hash_mismatch")
    if embedded_hash != actual_hash:
        reasons.append("statistical_evidence_hash_mismatch")

    expected_metric_hash = str(
        candidate.get("candidate_metric_values_hash") or report.get("candidate_metric_values_hash") or ""
    )
    actual_metric_hash = str(evidence.get("candidate_metric_values_hash") or "")
    if not expected_metric_hash.startswith("sha256:"):
        reasons.append("candidate_metric_values_hash_missing")
    elif actual_metric_hash != expected_metric_hash:
        reasons.append("candidate_metric_values_hash_mismatch")
    _extend_candidate_metric_recompute_reasons(
        candidate=candidate,
        report=report,
        evidence=evidence,
        reasons=reasons,
    )

    expected_universe = str(candidate.get("selection_universe_hash") or report.get("selection_universe_hash") or "")
    if not expected_universe.startswith("sha256:"):
        reasons.append("selection_universe_hash_missing")
    elif str(evidence.get("selection_universe_hash") or "") != expected_universe:
        reasons.append("selection_universe_hash_mismatch")
    for field in ("manifest_hash", "dataset_content_hash", "dataset_quality_hash"):
        expected = candidate.get(field) or report.get(field)
        actual = evidence.get(field)
        if expected or actual:
            if str(expected or "") != str(actual or ""):
                reasons.append("selection_universe_hash_mismatch")
                break
    _extend_statistical_metadata_reasons(candidate=candidate, report=report, evidence=evidence, reasons=reasons)
    evidence_grade = str(evidence.get("evidence_grade") or "").strip()
    production_bound = is_production_bound_target(candidate.get("deployment_tier") or report.get("deployment_tier"))
    if production_bound and evidence_grade not in PROMOTION_GRADE_EVIDENCE_GRADES:
        reasons.append("statistical_evidence_grade_insufficient")
    if not evidence_grade:
        reasons.append("statistical_evidence_grade_missing")
    method = evidence.get("statistical_method") or evidence.get("white_reality_check_method")
    if evidence_grade == SCREENING_SUMMARY_BOOTSTRAP:
        p_value = evidence.get("summary_metric_max_bootstrap_p_value")
        if p_value is None or _as_float(p_value) is None:
            reasons.append("summary_metric_max_bootstrap_p_value_missing")
        if evidence.get("white_reality_check_p_value") is not None:
            reasons.append("white_reality_check_field_overloaded")
        if evidence.get("white_reality_check_method") is not None:
            reasons.append("white_reality_check_field_overloaded")
    else:
        p_value = evidence.get("white_reality_check_p_value")
        if p_value is None or _as_float(p_value) is None:
            reasons.append("reality_check_p_value_missing")
        if method not in PROMOTION_GRADE_METHODS and evidence_grade in PROMOTION_GRADE_EVIDENCE_GRADES:
            reasons.append("statistical_method_unavailable")
    summary_p_value = evidence.get("summary_metric_max_bootstrap_p_value")
    if evidence_grade == SCREENING_SUMMARY_BOOTSTRAP and summary_p_value is None:
        reasons.append("summary_metric_max_bootstrap_p_value_missing")
    if production_bound:
        reasons.extend(validate_return_panel_binding(report=report, evidence=evidence, panel=_load_return_panel(evidence, report)))
        reasons.extend(validate_family_registry_binding(report=report, evidence=evidence))
    if evidence.get("effective_trial_count") is None:
        reasons.append("effective_trial_count_missing")
    elif _as_int(evidence.get("effective_trial_count")) is not None:
        expected_effective = _expected_effective_trial_count(candidate=candidate, report=report, evidence=evidence)
        if expected_effective is not None and int(evidence["effective_trial_count"]) < expected_effective:
            reasons.append("statistical_effective_trial_count_underreported")
    if _as_int(evidence.get("metric_value_count")) != _as_int(candidate.get("metric_value_count") or report.get("metric_value_count")):
        reasons.append("statistical_metric_value_count_mismatch")
    metric_value_count = _as_int(evidence.get("metric_value_count"))
    candidate_count = _as_int(evidence.get("candidate_count"))
    missing_metric_count = _as_int(evidence.get("missing_metric_count"))
    if (
        metric_value_count is None
        or candidate_count is None
        or missing_metric_count is None
        or metric_value_count != candidate_count
        or missing_metric_count != 0
    ):
        reasons.append("statistical_metric_values_missing")
    if evidence.get("statistical_gate_result") != "PASS":
        gate_reasons = [str(item) for item in evidence.get("gate_fail_reasons") or []]
        reasons.extend(gate_reasons or ["reality_check_p_value_failed"])
    if isinstance(contract, dict):
        gates = contract.get("gates")
        if isinstance(gates, dict):
            if gates.get("max_spa_p_value") is not None and evidence.get("spa_p_value") is None:
                reasons.append("spa_method_unavailable")
            if gates.get("min_deflated_sharpe_probability") is not None and evidence.get("deflated_sharpe_probability") is None:
                reasons.append("deflated_sharpe_missing")
    return sorted(set(reasons))


def _extend_candidate_metric_recompute_reasons(
    *,
    candidate: dict[str, Any],
    report: dict[str, Any],
    evidence: dict[str, Any],
    reasons: list[str],
) -> None:
    candidates = report.get("candidates")
    if not isinstance(candidates, list) or not all(isinstance(item, dict) for item in candidates):
        reasons.append("candidate_metric_values_hash_missing")
        reasons.append("statistical_metadata_mismatch")
        return
    actual_candidate_count = len(candidates)
    for field_value in (
        report.get("candidate_count") if "candidate_count" in report else actual_candidate_count,
        evidence.get("candidate_count"),
    ):
        if _as_int(field_value) != actual_candidate_count:
            reasons.append("statistical_candidate_count_mismatch")
            reasons.append("statistical_metadata_mismatch")
    summary = evidence.get("candidate_metric_values_summary")
    if not isinstance(summary, dict):
        reasons.append("statistical_metadata_mismatch")
        return
    if _as_int(summary.get("candidate_count")) != actual_candidate_count:
        reasons.append("statistical_candidate_count_mismatch")
        reasons.append("statistical_metadata_mismatch")
    if _as_int(summary.get("metric_value_count")) != _as_int(evidence.get("metric_value_count")):
        reasons.append("statistical_metric_value_count_mismatch")
        reasons.append("statistical_metadata_mismatch")
    if _as_int(summary.get("missing_metric_count")) != _as_int(evidence.get("missing_metric_count")):
        reasons.append("statistical_metric_value_count_mismatch")
        reasons.append("statistical_metadata_mismatch")

    recomputed = recompute_candidate_metric_values_hash_from_report(report=report, evidence=evidence)
    if recomputed is None:
        reasons.append("candidate_metric_values_hash_missing")
        reasons.append("statistical_metadata_mismatch")
        return
    observed_hashes = {
        "evidence": evidence.get("candidate_metric_values_hash"),
        "report": report.get("candidate_metric_values_hash"),
        "candidate": candidate.get("candidate_metric_values_hash"),
    }
    for value in observed_hashes.values():
        if not isinstance(value, str) or not value.startswith("sha256:"):
            reasons.append("candidate_metric_values_hash_missing")
            return
    if len({str(value) for value in observed_hashes.values()}) > 1:
        reasons.append("candidate_metric_values_hash_mismatch")
    if any(str(value) != recomputed for value in observed_hashes.values()):
        reasons.append("candidate_metric_values_hash_recompute_mismatch")


def _extend_statistical_metadata_reasons(
    *,
    candidate: dict[str, Any],
    report: dict[str, Any],
    evidence: dict[str, Any],
    reasons: list[str],
) -> None:
    code_by_field = {
        "candidate_count": "statistical_candidate_count_mismatch",
        "search_budget": "statistical_search_budget_mismatch",
        "parameter_grid_size": "statistical_parameter_grid_size_mismatch",
        "attempt_index": "statistical_attempt_index_mismatch",
        "holdout_reuse_count": "statistical_holdout_reuse_count_mismatch",
        "dataset_reuse_policy": "statistical_dataset_reuse_policy_mismatch",
        "benchmark": "statistical_benchmark_mismatch",
        "primary_metric": "statistical_primary_metric_mismatch",
        "primary_metric_source": "statistical_primary_metric_mismatch",
    }
    for field, code in code_by_field.items():
        expected = candidate.get(field)
        if expected is None:
            expected = report.get(field)
        actual = evidence.get(field)
        if expected is None and actual is None:
            reasons.append(code)
            reasons.append("statistical_metadata_mismatch")
        elif str(expected or "") != str(actual or ""):
            reasons.append(code)
            reasons.append("statistical_metadata_mismatch")
    expected_contract = candidate.get("statistical_validation_contract") or report.get("statistical_validation_contract")
    if expected_contract != evidence.get("statistical_validation_contract"):
        reasons.append("statistical_contract_mismatch")
        reasons.append("statistical_metadata_mismatch")


def _candidate_metric_values(
    candidates: list[dict[str, Any]],
    contract: StatisticalSelectionContract,
) -> list[float]:
    values: list[float] = []
    for candidate in candidates:
        metrics = candidate.get("validation_metrics")
        if not isinstance(metrics, dict):
            continue
        value = _metric_value(metrics, contract.primary_metric, contract.benchmark)
        if value is not None:
            values.append(value)
    return values


def _candidate_metric_values_from_payload(payload: dict[str, Any]) -> list[float]:
    values: list[float] = []
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        return values
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        value = _as_float(candidate.get("validation_metric_value"))
        if value is not None:
            values.append(value)
    return values


def _metric_value(metrics: dict[str, Any], primary_metric: str, benchmark: str) -> float | None:
    if primary_metric in {"net_excess_return", "return_pct"}:
        raw = _as_float(metrics.get("return_pct"))
    elif primary_metric == "sharpe_like":
        raw = _as_float(metrics.get("sharpe_like"))
        if raw is None:
            raw = _as_float(metrics.get("return_pct"))
    else:
        raw = None
    if raw is None:
        return None
    benchmark_value = 0.0
    if benchmark in {"cash", "buy_and_hold", "configured"}:
        return raw - benchmark_value
    return raw


def _metric_centered_max_bootstrap_p_value(
    *,
    metric_values: list[float],
    n_bootstrap: int,
    selection_hash: str,
) -> tuple[float | None, int | None]:
    if not metric_values:
        return None, None
    observed = max(metric_values)
    if observed <= 0.0:
        return 1.0, _seed_from_hash(selection_hash)
    mean_value = sum(metric_values) / len(metric_values)
    centered = [value - mean_value for value in metric_values]
    seed = _seed_from_hash(selection_hash)
    rng = random.Random(seed)
    exceed_count = 0
    sample_size = len(centered)
    for _ in range(n_bootstrap):
        sample_max = max(centered[rng.randrange(sample_size)] for _ in range(sample_size))
        if sample_max >= observed:
            exceed_count += 1
    return round((exceed_count + 1) / (n_bootstrap + 1), 12), seed


def _statistical_gate_fail_reasons(
    *,
    contract: StatisticalSelectionContract,
    p_value: float | None,
    attempt_index: int,
    holdout_reuse_count: int,
    metric_values: list[float],
    candidate_count: int,
) -> list[str]:
    reasons: list[str] = []
    if not metric_values:
        reasons.append("effective_trial_count_missing")
    if len(metric_values) != candidate_count:
        reasons.append("statistical_metric_values_missing")
    if p_value is None:
        reasons.append("reality_check_p_value_missing")
    elif p_value > contract.gates.max_reality_check_p_value:
        reasons.append("reality_check_p_value_failed")
    if holdout_reuse_count > contract.gates.max_holdout_reuse_count:
        reasons.append("holdout_reuse_budget_exceeded")
    if attempt_index > contract.gates.max_attempt_index_without_new_hypothesis:
        reasons.append("attempt_budget_exceeded")
    if contract.gates.max_spa_p_value is not None:
        reasons.append("spa_p_value_missing")
    if contract.gates.min_deflated_sharpe_probability is not None:
        reasons.append("deflated_sharpe_missing")
    return sorted(set(reasons))


def _limitations(contract: StatisticalSelectionContract) -> list[str]:
    limitations = [
        "metric_summary_bootstrap_not_trade_or_bar_return_bootstrap",
        "summary_metric_centered_max_bootstrap_screening_only",
        "not_white_reality_check",
    ]
    if contract.gates.max_spa_p_value is None:
        limitations.append("spa_not_implemented")
    if contract.gates.min_deflated_sharpe_probability is None:
        limitations.append("deflated_sharpe_not_implemented")
    return limitations


def _promotion_grade_limitations(contract: StatisticalSelectionContract) -> list[str]:
    limitations = [
        "not_full_white_reality_check",
        "not_bar_return_bootstrap",
        "not_trade_return_bootstrap",
    ]
    if contract.gates.max_spa_p_value is None:
        limitations.append("spa_not_implemented")
    if contract.gates.min_deflated_sharpe_probability is None:
        limitations.append("deflated_sharpe_not_implemented")
    return limitations


def _bootstrap_sampling_contract(
    *,
    contract: StatisticalSelectionContract,
    selection_hash: str,
    observation_count: int,
    return_unit: str,
    benchmark: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "method_name": contract.bootstrap.method,
        "n_bootstrap": contract.bootstrap.n_bootstrap,
        "seed_policy": contract.bootstrap.seed_policy,
        "derived_seed": _seed_from_hash(selection_hash),
        "block_length": None,
        "block_length_policy": contract.bootstrap.block_length_policy,
        "stationary_bootstrap_probability": None,
        "observation_count": int(observation_count),
        "return_unit": return_unit,
        "benchmark": benchmark,
        "missing_observation_policy": "skip_missing_candidate_trade_returns",
    }
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    return payload


def _load_return_panel(evidence: dict[str, Any], report: dict[str, Any]) -> dict[str, Any] | None:
    path_value = str(evidence.get("return_panel_path") or report.get("return_panel_path") or "").strip()
    if not path_value:
        return None
    try:
        import json

        with Path(path_value).expanduser().open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _effective_trial_count(
    *,
    candidate_count: int,
    metric_value_count: int,
    search_budget: int,
    parameter_grid_size: int,
    attempt_index: int,
    holdout_reuse_count: int,
) -> int:
    base = max(
        int(candidate_count),
        int(metric_value_count),
        int(search_budget),
        int(parameter_grid_size),
    )
    return base * max(1, int(attempt_index)) * max(1, int(holdout_reuse_count) + 1)


def _expected_effective_trial_count(
    *,
    candidate: dict[str, Any],
    report: dict[str, Any],
    evidence: dict[str, Any],
) -> int | None:
    values = {
        "candidate_count": _as_int(candidate.get("candidate_count") or report.get("candidate_count") or evidence.get("candidate_count")),
        "metric_value_count": _as_int(evidence.get("metric_value_count")),
        "search_budget": _as_int(candidate.get("search_budget") or report.get("search_budget") or evidence.get("search_budget")),
        "parameter_grid_size": _as_int(
            candidate.get("parameter_grid_size") or report.get("parameter_grid_size") or evidence.get("parameter_grid_size")
        ),
        "attempt_index": _as_int(candidate.get("attempt_index") or report.get("attempt_index") or evidence.get("attempt_index")),
        "holdout_reuse_count": _as_int(
            candidate.get("holdout_reuse_count") or report.get("holdout_reuse_count") or evidence.get("holdout_reuse_count")
        ),
    }
    if any(value is None for value in values.values()):
        return None
    return _effective_trial_count(**{key: int(value) for key, value in values.items() if value is not None})


def _seed_from_hash(value: str) -> int:
    text = value.split("sha256:", 1)[-1]
    return int(text[:16], 16)


def _as_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return None
    return parsed


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"research output path must be outside repository: {resolved}")
