from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .deployment_policy import is_production_bound_target
from .experiment_manifest import StressSuiteContract
from .hashing import content_hash_payload, sha256_prefixed
from .metrics_contract import ClosedTradeRecord


STRESS_SUITE_SCHEMA_VERSION = 1
MONTE_CARLO_LIMITATIONS = (
    "monte_carlo_does_not_reconstruct_intratrade_equity_path",
    "monte_carlo_uses_closed_trade_pnl_not_bar_return_series",
)
PERIOD_ABLATION_LIMITATIONS = (
    "period_ablation_uses_closed_trade_exit_year_not_full_signal_rerun",
)
PARAMETER_PERTURBATION_LIMITATIONS = (
    "parameter_perturbation_uses_existing_grid_candidates_not_synthetic_reruns",
)


@dataclass(frozen=True)
class StressSuiteContext:
    manifest_hash: str
    experiment_id: str
    candidate_id: str
    scenario_id: str
    split_name: str
    parameter_values: dict[str, Any]
    portfolio_policy_hash: str | None = None
    simulation_policy_hash: str | None = None


def stress_suite_required(manifest_or_payload: Any) -> bool:
    if hasattr(manifest_or_payload, "stress_suite"):
        contract = getattr(manifest_or_payload, "stress_suite")
        return bool(contract.required_for_promotion) if contract is not None else False
    contract = manifest_or_payload.get("stress_suite_contract") if isinstance(manifest_or_payload, dict) else None
    return bool(contract.get("required_for_promotion")) if isinstance(contract, dict) else False


def stress_suite_required_for_candidate(
    candidate: dict[str, Any],
    report: dict[str, Any] | None = None,
) -> bool:
    report_payload = report or {}
    return (
        bool(candidate.get("stress_suite_required"))
        or bool(report_payload.get("stress_suite_required"))
        or is_production_bound_target(candidate.get("deployment_tier") or report_payload.get("deployment_tier"))
    )


def analyze_stress_suite(
    *,
    contract: StressSuiteContract,
    context: StressSuiteContext,
    original_metrics: dict[str, Any],
    metrics_v2: dict[str, Any] | None,
    closed_trades: tuple[ClosedTradeRecord, ...],
    starting_cash: float,
    parameter_perturbation_candidates: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    contract_payload = contract.as_dict()
    contract_hash = sha256_prefixed(contract_payload)
    seed_material = {
        "manifest_hash": context.manifest_hash,
        "experiment_id": context.experiment_id,
        "candidate_id": context.candidate_id,
        "scenario_id": context.scenario_id,
        "split_name": context.split_name,
        "parameter_values": context.parameter_values,
        "contract_hash": contract_hash,
        "portfolio_policy_hash": context.portfolio_policy_hash,
        "simulation_policy_hash": context.simulation_policy_hash,
        "starting_cash": float(starting_cash),
    }
    fail_reasons: list[str] = []
    limitations: list[str] = []
    payload: dict[str, Any] = {
        "stress_suite_schema_version": STRESS_SUITE_SCHEMA_VERSION,
        "contract_hash": contract_hash,
        "seed_material_hash": sha256_prefixed(seed_material),
        "context": {
            "experiment_id": context.experiment_id,
            "candidate_id": context.candidate_id,
            "scenario_id": context.scenario_id,
            "split_name": context.split_name,
            "portfolio_policy_hash": context.portfolio_policy_hash,
            "simulation_policy_hash": context.simulation_policy_hash,
        },
        "starting_cash": float(starting_cash),
    }
    if contract.period_ablation is not None:
        section = analyze_period_ablation(
            contract=contract.period_ablation.as_dict(),
            closed_trades=closed_trades,
            starting_cash=starting_cash,
        )
        payload["period_ablation"] = section
        fail_reasons.extend(str(reason) for reason in section.get("fail_reasons") or [])
        limitations.extend(str(item) for item in section.get("limitations") or [])
    if contract.parameter_perturbation is not None:
        section = analyze_parameter_perturbation(
            contract=contract.parameter_perturbation.as_dict(),
            base_parameter_values=context.parameter_values,
            candidates=parameter_perturbation_candidates,
        )
        payload["parameter_perturbation"] = section
        fail_reasons.extend(str(reason) for reason in section.get("fail_reasons") or [])
        limitations.extend(str(item) for item in section.get("limitations") or [])
    if contract.trade_removal is not None:
        section = analyze_trade_removal(
            contract=contract.trade_removal.as_dict(),
            original_metrics=original_metrics,
            metrics_v2=metrics_v2,
            closed_trades=closed_trades,
            starting_cash=starting_cash,
        )
        payload["trade_removal"] = section
        fail_reasons.extend(str(reason) for reason in section.get("fail_reasons") or [])
    if contract.trade_order_monte_carlo is not None:
        section = analyze_trade_order_monte_carlo(
            contract=contract.trade_order_monte_carlo.as_dict(),
            seed_material=seed_material,
            closed_trades=closed_trades,
            starting_cash=starting_cash,
        )
        payload["trade_order_monte_carlo"] = section
        fail_reasons.extend(str(reason) for reason in section.get("fail_reasons") or [])
        limitations.extend(str(item) for item in section.get("limitations") or [])
    if contract.risk_adjusted_score is not None:
        section = analyze_risk_adjusted_score(
            contract=contract.risk_adjusted_score.as_dict(),
            metrics_v2=metrics_v2,
        )
        payload["risk_adjusted_score"] = section
        fail_reasons.extend(str(reason) for reason in section.get("fail_reasons") or [])
        limitations.extend(str(item) for item in section.get("limitations") or [])
    payload["limitations"] = sorted(set(limitations))
    payload["fail_reasons"] = sorted(set(fail_reasons))
    payload["gate_result"] = "PASS" if not payload["fail_reasons"] else "FAIL"
    payload["stress_suite_hash"] = sha256_prefixed(content_hash_payload(payload))
    return _json_safe(payload)


def analyze_trade_removal(
    *,
    contract: dict[str, Any],
    original_metrics: dict[str, Any],
    metrics_v2: dict[str, Any] | None,
    closed_trades: tuple[ClosedTradeRecord, ...],
    starting_cash: float,
) -> dict[str, Any]:
    fail_reasons: list[str] = []
    cases: list[dict[str, Any]] = []
    if not closed_trades:
        return {
            "status": "FAIL",
            "cases": [],
            "fail_reasons": ["stress_trade_removal_no_closed_trades"],
        }
    top_values = [int(item) for item in contract.get("top_n_by_net_pnl") or []]
    original = _trade_summary(closed_trades, starting_cash=starting_cash)
    original_mdd = _metrics_v2_max_drawdown(metrics_v2)
    for top_n in top_values:
        winners = sorted(
            [trade for trade in closed_trades if float(trade.net_pnl) > 0.0],
            key=lambda trade: (float(trade.net_pnl), int(trade.exit_ts)),
            reverse=True,
        )
        removed = winners[:top_n]
        removed_ids = {id(trade) for trade in removed}
        kept = tuple(trade for trade in closed_trades if id(trade) not in removed_ids)
        stressed = _trade_summary(kept, starting_cash=starting_cash)
        retention = (
            (stressed["realized_return_pct"] / original["realized_return_pct"] * 100.0)
            if original["realized_return_pct"] > 0.0
            else None
        )
        case_reasons: list[str] = []
        min_retention = contract.get("min_return_retention_pct")
        if min_retention is not None:
            if retention is None or float(retention) < float(min_retention):
                case_reasons.append("stress_trade_removal_return_retention_failed")
        if contract.get("max_mdd_multiplier") is not None:
            case_reasons.append("stress_trade_removal_mdd_replay_unavailable")
        case = {
            "top_n": top_n,
            "removed_trade_count": len(removed),
            "original_realized_return_pct": original["realized_return_pct"],
            "stressed_realized_return_pct": stressed["realized_return_pct"],
            "return_retention_pct": retention,
            "original_profit_factor": original["profit_factor"],
            "stressed_profit_factor": stressed["profit_factor"],
            "original_expectancy_per_trade_krw": original["expectancy_per_trade_krw"],
            "stressed_expectancy_per_trade_krw": stressed["expectancy_per_trade_krw"],
            "original_win_rate": original["win_rate"],
            "stressed_win_rate": stressed["win_rate"],
            "trade_count_after_removal": stressed["trade_count"],
            "original_max_drawdown_pct": original_mdd,
            "stressed_max_drawdown_pct": None,
            "limitations": ["trade_removal_mdd_replay_unavailable"],
            "gate_result": "PASS" if not case_reasons else "FAIL",
            "fail_reasons": sorted(set(case_reasons)),
        }
        cases.append(case)
        fail_reasons.extend(case_reasons)
    return _json_safe(
        {
            "status": "PASS" if not fail_reasons else "FAIL",
            "cases": cases,
            "fail_reasons": sorted(set(fail_reasons)),
        }
    )


def analyze_trade_order_monte_carlo(
    *,
    contract: dict[str, Any],
    seed_material: dict[str, Any],
    closed_trades: tuple[ClosedTradeRecord, ...],
    starting_cash: float,
) -> dict[str, Any]:
    fail_reasons: list[str] = []
    if not closed_trades:
        return {
            "status": "FAIL",
            "iterations": int(contract.get("iterations") or 0),
            "fail_reasons": ["stress_monte_carlo_no_closed_trades"],
            "limitations": list(MONTE_CARLO_LIMITATIONS),
        }
    min_closed_trades = int(contract.get("min_closed_trades") or 10)
    if len(closed_trades) < min_closed_trades:
        fail_reasons.append("stress_monte_carlo_insufficient_trades")
    seed_hash = sha256_prefixed(seed_material)
    seed = int(seed_hash.split(":", 1)[1][:16], 16)
    rng = random.Random(seed)
    iterations = int(contract.get("iterations") or 0)
    ruin_mdd = float(contract.get("ruin_max_drawdown_pct"))
    pnls = [float(trade.net_pnl) for trade in closed_trades]
    terminal_equities: list[float] = []
    max_drawdowns: list[float] = []
    losing_streaks: list[int] = []
    survival_count = 0
    for _ in range(iterations):
        ordered = list(pnls)
        rng.shuffle(ordered)
        terminal, mdd, streak = _pnl_path_stats(ordered, starting_cash=starting_cash)
        terminal_equities.append(terminal)
        max_drawdowns.append(mdd)
        losing_streaks.append(streak)
        if mdd <= ruin_mdd:
            survival_count += 1
    survival_probability = survival_count / iterations if iterations > 0 else 0.0
    if survival_probability < float(contract.get("min_survival_probability")):
        fail_reasons.append("stress_monte_carlo_survival_probability_failed")
    return _json_safe(
        {
            "status": "PASS" if not fail_reasons else "FAIL",
            "iterations": iterations,
            "seed": seed,
            "seed_material_hash": seed_hash,
            "terminal_equity_p05": _percentile(terminal_equities, 5.0),
            "terminal_equity_median": _percentile(terminal_equities, 50.0),
            "terminal_equity_p95": _percentile(terminal_equities, 95.0),
            "max_drawdown_pct_p50": _percentile(max_drawdowns, 50.0),
            "max_drawdown_pct_p95": _percentile(max_drawdowns, 95.0),
            "longest_losing_streak_p50": _percentile(losing_streaks, 50.0),
            "longest_losing_streak_p95": _percentile(losing_streaks, 95.0),
            "survival_probability": survival_probability,
            "ruin_max_drawdown_pct": ruin_mdd,
            "fail_reasons": sorted(set(fail_reasons)),
            "limitations": list(MONTE_CARLO_LIMITATIONS),
        }
    )


def analyze_period_ablation(
    *,
    contract: dict[str, Any],
    closed_trades: tuple[ClosedTradeRecord, ...],
    starting_cash: float,
) -> dict[str, Any]:
    min_pass_ratio = _period_ablation_min_pass_ratio(contract)
    min_return_retention_pct = _period_ablation_min_return_retention_pct(contract)
    if not closed_trades:
        return {
            "method": "leave_one_calendar_year_out_closed_trade_exit_year",
            "status": "FAIL",
            "calendar_years": [],
            "min_pass_ratio": min_pass_ratio,
            "min_return_retention_pct": min_return_retention_pct,
            "pass_ratio": 0.0,
            "cases": [],
            "limitations": list(PERIOD_ABLATION_LIMITATIONS),
            "fail_reasons": ["stress_period_ablation_no_closed_trades"],
        }
    trade_years: list[int] = []
    for trade in closed_trades:
        year = _trade_exit_year(trade)
        if year is None:
            return {
                "method": "leave_one_calendar_year_out_closed_trade_exit_year",
                "status": "FAIL",
                "calendar_years": [],
                "min_pass_ratio": min_pass_ratio,
                "min_return_retention_pct": min_return_retention_pct,
                "pass_ratio": 0.0,
                "cases": [],
                "limitations": list(PERIOD_ABLATION_LIMITATIONS),
                "fail_reasons": ["stress_period_ablation_exit_timestamp_missing"],
            }
        trade_years.append(year)
    requested = contract.get("calendar_years")
    calendar_years = sorted(set(trade_years)) if requested == "auto" else [int(item) for item in requested or []]
    matching_years = [year for year in calendar_years if year in set(trade_years)]
    if not matching_years:
        return {
            "method": "leave_one_calendar_year_out_closed_trade_exit_year",
            "status": "FAIL",
            "calendar_years": calendar_years,
            "min_pass_ratio": min_pass_ratio,
            "min_return_retention_pct": min_return_retention_pct,
            "pass_ratio": 0.0,
            "cases": [],
            "limitations": list(PERIOD_ABLATION_LIMITATIONS),
            "fail_reasons": ["stress_period_ablation_no_matching_years"],
        }
    original = _trade_summary(closed_trades, starting_cash=starting_cash)
    cases: list[dict[str, Any]] = []
    passing = 0
    required_data_missing = False
    case_fail_reasons: list[str] = []
    for year in matching_years:
        kept = tuple(trade for trade in closed_trades if _trade_exit_year(trade) != year)
        removed_count = len(closed_trades) - len(kept)
        stressed = _trade_summary(kept, starting_cash=starting_cash)
        retention = (
            (stressed["realized_return_pct"] / original["realized_return_pct"] * 100.0)
            if original["realized_return_pct"] > 0.0
            else None
        )
        case_reasons: list[str] = []
        if not kept:
            case_reasons.append("stress_period_ablation_required_data_missing")
            required_data_missing = True
        elif retention is None:
            case_reasons.append("stress_period_ablation_required_data_missing")
            required_data_missing = True
        elif retention < min_return_retention_pct:
            case_reasons.append("stress_period_ablation_return_retention_failed")
        if not case_reasons:
            passing += 1
        case_fail_reasons.extend(case_reasons)
        cases.append(
            {
                "year": year,
                "removed_trade_count": removed_count,
                "remaining_trade_count": len(kept),
                "original_realized_return_pct": original["realized_return_pct"],
                "stressed_realized_return_pct": stressed["realized_return_pct"],
                "return_retention_pct": retention,
                "stressed_profit_factor": stressed["profit_factor"],
                "stressed_expectancy_per_trade_krw": stressed["expectancy_per_trade_krw"],
                "stressed_win_rate": stressed["win_rate"],
                "gate_result": "PASS" if not case_reasons else "FAIL",
                "fail_reasons": sorted(set(case_reasons)),
            }
        )
    pass_ratio = passing / len(cases) if cases else 0.0
    fail_reasons: list[str] = []
    if pass_ratio < min_pass_ratio:
        fail_reasons.append("stress_period_ablation_pass_ratio_failed")
        fail_reasons.extend(
            reason for reason in case_fail_reasons if reason != "stress_period_ablation_required_data_missing"
        )
    if required_data_missing:
        fail_reasons.append("stress_period_ablation_required_data_missing")
    return _json_safe(
        {
            "method": "leave_one_calendar_year_out_closed_trade_exit_year",
            "status": "PASS" if not fail_reasons else "FAIL",
            "calendar_years": calendar_years,
            "min_pass_ratio": min_pass_ratio,
            "min_return_retention_pct": min_return_retention_pct,
            "pass_ratio": pass_ratio,
            "cases": cases,
            "limitations": list(PERIOD_ABLATION_LIMITATIONS),
            "fail_reasons": sorted(set(fail_reasons)),
        }
    )


def _period_ablation_min_pass_ratio(contract: dict[str, Any]) -> float:
    value = contract.get("min_pass_ratio")
    return 0.8 if value is None else float(value)


def _period_ablation_min_return_retention_pct(contract: dict[str, Any]) -> float:
    value = contract.get("min_return_retention_pct")
    return 50.0 if value is None else float(value)


def analyze_parameter_perturbation(
    *,
    contract: dict[str, Any],
    base_parameter_values: dict[str, Any],
    candidates: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    relative_values = [float(item) for item in contract.get("relative_pct") or []]
    min_pass_ratio = float(contract.get("min_pass_ratio") or 0.8)
    numeric_items = [(key, value) for key, value in sorted(base_parameter_values.items()) if _is_numeric_param_value(value)]
    if not numeric_items:
        return {
            "method": "existing_grid_relative_parameter_perturbation",
            "status": "FAIL",
            "relative_pct": relative_values,
            "min_pass_ratio": min_pass_ratio,
            "pass_ratio": 0.0,
            "cases": [],
            "limitations": list(PARAMETER_PERTURBATION_LIMITATIONS),
            "fail_reasons": ["stress_parameter_perturbation_no_numeric_parameters"],
        }
    if not candidates:
        return {
            "method": "existing_grid_relative_parameter_perturbation",
            "status": "FAIL",
            "relative_pct": relative_values,
            "min_pass_ratio": min_pass_ratio,
            "pass_ratio": 0.0,
            "cases": [],
            "limitations": list(PARAMETER_PERTURBATION_LIMITATIONS),
            "fail_reasons": ["stress_parameter_perturbation_required_data_missing"],
        }
    candidate_by_params = {
        _parameter_signature(dict(item.get("parameter_values") or {})): item
        for item in candidates
        if isinstance(item.get("parameter_values"), dict)
    }
    cases: list[dict[str, Any]] = []
    passing = 0
    for parameter, base_value in numeric_items:
        for relative_pct in relative_values:
            target_value = _perturbed_value(base_value, relative_pct)
            target_params = dict(base_parameter_values)
            target_params[parameter] = target_value
            matched = candidate_by_params.get(_parameter_signature(target_params))
            case_reasons: list[str] = []
            if matched is None:
                case_reasons.append("stress_parameter_perturbation_candidate_missing")
                matched_fail_reasons: list[str] = []
                matched_candidate_id = None
                validation_return = None
                validation_mdd = None
                final_holdout_return = None
                matched_gate = None
            else:
                matched_candidate_id = matched.get("candidate_id")
                validation_metrics = matched.get("validation_metrics") if isinstance(matched.get("validation_metrics"), dict) else {}
                final_holdout_metrics = (
                    matched.get("final_holdout_metrics") if isinstance(matched.get("final_holdout_metrics"), dict) else None
                )
                validation_return = _finite_or_none(validation_metrics.get("return_pct"))
                validation_mdd = _finite_or_none(validation_metrics.get("max_drawdown_pct"))
                final_holdout_return = (
                    _finite_or_none(final_holdout_metrics.get("return_pct")) if isinstance(final_holdout_metrics, dict) else None
                )
                matched_gate = str(matched.get("scenario_acceptance_gate_result") or "")
                matched_fail_reasons = [str(reason) for reason in matched.get("scenario_fail_reasons") or []]
                if matched_gate != "PASS":
                    case_reasons.append("stress_parameter_perturbation_constraint_invalid")
            if not case_reasons:
                passing += 1
            cases.append(
                {
                    "parameter": parameter,
                    "base_value": base_value,
                    "relative_pct": relative_pct,
                    "target_value": target_value,
                    "matched_candidate_id": matched_candidate_id,
                    "validation_return_pct": validation_return,
                    "validation_max_drawdown_pct": validation_mdd,
                    "final_holdout_return_pct": final_holdout_return,
                    "matched_candidate_gate_result": matched_gate,
                    "matched_candidate_fail_reasons": matched_fail_reasons,
                    "gate_result": "PASS" if not case_reasons else "FAIL",
                    "fail_reasons": sorted(set(case_reasons)),
                }
            )
    pass_ratio = passing / len(cases) if cases else 0.0
    fail_reasons = sorted({reason for case in cases for reason in case.get("fail_reasons") or []})
    if pass_ratio < min_pass_ratio:
        fail_reasons.append("stress_parameter_perturbation_pass_ratio_failed")
    return _json_safe(
        {
            "method": "existing_grid_relative_parameter_perturbation",
            "status": "PASS" if not fail_reasons else "FAIL",
            "relative_pct": relative_values,
            "min_pass_ratio": min_pass_ratio,
            "pass_ratio": pass_ratio,
            "cases": cases,
            "limitations": list(PARAMETER_PERTURBATION_LIMITATIONS),
            "fail_reasons": sorted(set(fail_reasons)),
        }
    )


def analyze_risk_adjusted_score(*, contract: dict[str, Any], metrics_v2: dict[str, Any] | None) -> dict[str, Any]:
    return_risk = metrics_v2.get("return_risk") if isinstance(metrics_v2, dict) and isinstance(metrics_v2.get("return_risk"), dict) else {}
    cagr = _finite_or_none(return_risk.get("cagr_pct"))
    mdd = _finite_or_none(return_risk.get("max_drawdown_pct"))
    calmar = (cagr / mdd) if cagr is not None and mdd is not None and mdd > 0.0 else None
    sharpe = _finite_or_none(return_risk.get("sharpe_ratio"))
    sortino = _finite_or_none(return_risk.get("sortino_ratio"))
    limitations = [str(item) for item in (metrics_v2 or {}).get("limitation_reasons", [])] if isinstance(metrics_v2, dict) else []
    limitations = [
        item
        for item in limitations
        if item in {"sharpe_unavailable_without_period_return_series", "sortino_unavailable_without_period_return_series"}
    ]
    if sharpe is None:
        limitations.append("sharpe_unavailable_without_period_return_series")
    if sortino is None:
        limitations.append("sortino_unavailable_without_period_return_series")
    fail_reasons: list[str] = []
    required = {str(item) for item in contract.get("required_metrics") or []}
    if "calmar" in required and calmar is None:
        fail_reasons.append("stress_risk_adjusted_calmar_missing")
    if "sharpe" in required and sharpe is None:
        fail_reasons.append("stress_risk_adjusted_sharpe_missing")
    if "sortino" in required and sortino is None:
        fail_reasons.append("stress_risk_adjusted_sortino_missing")
    return _json_safe(
        {
            "calmar_ratio": calmar,
            "sortino_ratio": sortino,
            "sharpe_ratio": sharpe,
            "ranking": list(contract.get("ranking") or []),
            "limitations": limitations,
            "fail_reasons": fail_reasons,
            "status": "PASS" if not fail_reasons else "FAIL",
        }
    )


def validate_stress_suite_evidence_for_candidate(candidate: dict[str, Any], report: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    required = stress_suite_required_for_candidate(candidate, report)
    if not required:
        return reasons
    contract = candidate.get("stress_suite_contract")
    if not isinstance(contract, dict):
        reasons.append("stress_suite_contract_mismatch")
    if candidate.get("stress_suite_gate_result") != "PASS":
        reasons.append("stress_suite_gate_not_passed")
    expected_contract_hash = str(candidate.get("stress_suite_contract_hash") or "")
    actual_contract_hash = sha256_prefixed(contract) if isinstance(contract, dict) else ""
    if not expected_contract_hash.startswith("sha256:"):
        reasons.append("stress_suite_hash_missing")
    elif actual_contract_hash != expected_contract_hash:
        reasons.append("stress_suite_hash_mismatch")
    report_contract = report.get("stress_suite_contract")
    report_contract_hash = str(report.get("stress_suite_contract_hash") or "")
    if isinstance(report_contract, dict) and isinstance(contract, dict) and report_contract != contract:
        reasons.append("stress_suite_contract_mismatch")
    if report_contract_hash.startswith("sha256:") and report_contract_hash != expected_contract_hash:
        reasons.append("stress_suite_contract_mismatch")
    _validate_stress_evidence(
        candidate.get("validation_stress_suite"),
        reasons,
        expected_contract_hash=expected_contract_hash,
        missing_code="stress_suite_required_but_missing",
        hash_missing_code="stress_suite_hash_missing",
        hash_mismatch_code="stress_suite_hash_mismatch",
        gate_failed_code="stress_suite_gate_not_passed",
    )
    if _final_holdout_stress_required(candidate):
        _validate_stress_evidence(
            candidate.get("final_holdout_stress_suite"),
            reasons,
            expected_contract_hash=expected_contract_hash,
            missing_code="final_holdout_stress_suite_required_but_missing",
            hash_missing_code="final_holdout_stress_suite_hash_missing",
            hash_mismatch_code="final_holdout_stress_suite_hash_mismatch",
            gate_failed_code="final_holdout_stress_suite_gate_not_passed",
        )
    return sorted(set(reasons))


def _final_holdout_stress_required(candidate: dict[str, Any]) -> bool:
    return candidate.get("final_holdout_present") is True or candidate.get("final_holdout_required_for_promotion") is True


def _validate_stress_evidence(
    evidence: Any,
    reasons: list[str],
    *,
    expected_contract_hash: str,
    missing_code: str,
    hash_missing_code: str,
    hash_mismatch_code: str,
    gate_failed_code: str,
) -> None:
    if not isinstance(evidence, dict):
        reasons.append(missing_code)
        return
    embedded_hash = str(evidence.get("stress_suite_hash") or "")
    if not embedded_hash.startswith("sha256:"):
        reasons.append(hash_missing_code)
    else:
        actual_hash = sha256_prefixed(content_hash_payload({k: v for k, v in evidence.items() if k != "stress_suite_hash"}))
        if actual_hash != embedded_hash:
            reasons.append(hash_mismatch_code)
    if evidence.get("contract_hash") != expected_contract_hash:
        reasons.append("stress_suite_contract_mismatch")
    if evidence.get("gate_result") != "PASS":
        reasons.append(gate_failed_code)


def _trade_exit_year(trade: ClosedTradeRecord) -> int | None:
    try:
        exit_ts = int(trade.exit_ts)
    except (TypeError, ValueError):
        return None
    if exit_ts <= 0:
        return None
    try:
        return datetime.fromtimestamp(exit_ts / 1000.0, tz=timezone.utc).year
    except (OSError, OverflowError, ValueError):
        return None


def _is_numeric_param_value(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _perturbed_value(base_value: Any, relative_pct: float) -> Any:
    target = float(base_value) * (1.0 + float(relative_pct))
    if isinstance(base_value, int) and not isinstance(base_value, bool):
        return int(round(target))
    return round(target, 12)


def _parameter_signature(parameter_values: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple((str(key), repr(value)) for key, value in sorted(parameter_values.items()))


def _trade_summary(trades: tuple[ClosedTradeRecord, ...], *, starting_cash: float) -> dict[str, Any]:
    values = [float(trade.net_pnl) for trade in trades]
    wins = [value for value in values if value > 0.0]
    losses = [value for value in values if value < 0.0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "trade_count": len(values),
        "realized_return_pct": (sum(values) / starting_cash * 100.0) if starting_cash > 0.0 else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0.0 else None,
        "expectancy_per_trade_krw": (sum(values) / len(values)) if values else None,
        "win_rate": (len(wins) / len(values)) if values else 0.0,
    }


def _pnl_path_stats(values: list[float], *, starting_cash: float) -> tuple[float, float, int]:
    equity = float(starting_cash)
    peak = max(equity, 1e-12)
    max_drawdown = 0.0
    longest_streak = 0
    streak = 0
    for value in values:
        equity += float(value)
        peak = max(peak, equity)
        drawdown = ((peak - equity) / peak * 100.0) if peak > 0.0 else 0.0
        max_drawdown = max(max_drawdown, drawdown)
        if value < 0.0:
            streak += 1
            longest_streak = max(longest_streak, streak)
        else:
            streak = 0
    return equity, max_drawdown, longest_streak


def _percentile(values: list[float] | list[int], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (float(pct) / 100.0) * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _metrics_v2_max_drawdown(metrics_v2: dict[str, Any] | None) -> float | None:
    if not isinstance(metrics_v2, dict) or not isinstance(metrics_v2.get("return_risk"), dict):
        return None
    return _finite_or_none(metrics_v2["return_risk"].get("max_drawdown_pct"))


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value
