from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot import app as app_module
from bithumb_bot.execution_reality_contract import build_execution_reality_contract
from bithumb_bot.execution_reality_contract import execution_capability_contract_hash, execution_contract_hash
from bithumb_bot.research import cli as research_cli
from bithumb_bot.research.hashing import content_hash_payload, report_content_hash_payload, sha256_prefixed
from bithumb_bot.research.final_selection import apply_final_selection_contract
from bithumb_bot.research.lineage import build_research_lineage, compute_lineage_hash, reproduce_promotion
from bithumb_bot.research.experiment_registry import (
    EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
    append_attempt_completion,
    reserve_research_attempt,
)
from bithumb_bot.research.statistical_selection import (
    candidate_metric_values_hash,
    recompute_white_reality_check_block_bootstrap,
)
from bithumb_bot.research.metrics_gate_policy import metrics_gate_policy_hash
from bithumb_bot.research.promotion_gate import (
    PromotionGateError,
    build_candidate_profile,
    promote_candidate,
    validate_backtest_candidate_for_promotion,
)
from bithumb_bot.research import validation_pipeline as pipeline
from bithumb_bot.research.validation_pipeline import validation_run_binding_hash, validation_run_content_hash
from bithumb_bot.approved_profile import build_approved_profile, verify_promotion_artifact
from bithumb_bot.storage_io import write_json_atomic


def _manager(tmp_path: Path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def _production_safe_execution_timing(**overrides):
    payload = {
        "signal_basis": "closed_candle",
        "decision_time": "candle_close",
        "decision_guard_ms": 0,
        "fill_reference_policy": "next_candle_open",
        "quote_selection": "first_after_or_equal",
        "max_quote_wait_ms": 3000,
        "missing_quote_policy": "warn",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "candle_next_open",
        "source": "manifest",
    }
    payload.update(overrides)
    return payload


def _candidate(**overrides):
    execution_contract = build_execution_reality_contract(
        fill_reference_policy="next_candle_open",
        missing_quote_policy="warn",
        min_execution_reality_level_for_promotion="candle_next_open",
        allow_same_candle_close_fill=False,
        top_of_book_required=False,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test",
        slippage_source="test",
        calibration_required=False,
        calibration_artifact_hash=None,
    )
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "dataset_quality_gate_status": "PASS",
        "dataset_quality_gate_reasons": [],
        "dataset_quality_report_hashes": {"train": "sha256:quality-train", "validation": "sha256:quality-validation"},
        "execution_timing_policy": {
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "next_candle_open",
            "quote_selection": "first_after_or_equal",
            "max_quote_wait_ms": 3000,
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": False,
            "source": "test",
        },
        "execution_reality_contract": execution_contract,
        "execution_contract_hash": execution_contract["execution_contract_hash"],
        "execution_reality_summary": {
            "signal_event_count": 4,
            "fillable_signal_event_count": 4,
            "missing_quote_on_signal_count": 0,
            "quote_after_decision_coverage_pct": None,
            "median_quote_age_ms_on_signal": None,
            "p95_quote_age_ms_on_signal": None,
            "execution_reference_policy": "next_candle_open",
            "execution_reality_level": "candle_next_open",
            "execution_attempt_count": 8,
            "execution_filled_count": 8,
            "filled_execution_count": 8,
            "portfolio_applied_trade_count": 8,
            "pending_execution_count": 0,
            "skipped_execution_count": 0,
            "failed_execution_count": 0,
            "closed_trade_count": 4,
            "pending_execution_at_end_count": 0,
            "pending_execution_after_dataset_end_count": 0,
            "execution_event_timeline_incomplete": False,
            "execution_reality_gate_status": "PASS",
            "execution_reality_gate_reasons": [],
        },
        "execution_event_summary": {
            "execution_attempt_count": 8,
            "execution_filled_count": 8,
            "filled_execution_count": 8,
            "portfolio_applied_trade_count": 8,
            "pending_execution_count": 0,
            "skipped_execution_count": 0,
            "failed_execution_count": 0,
            "closed_trade_count": 4,
            "pending_execution_at_end_count": 0,
            "pending_execution_after_dataset_end_count": 0,
            "execution_event_timeline_incomplete": False,
        },
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_001",
        "parameter_values": {"SMA_SHORT": 2, "SMA_LONG": 4},
        "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
        "validation_metrics": {
            "trade_count": 4,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        "final_holdout_metrics": {
            "trade_count": 4,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        "final_holdout_present": True,
        "final_holdout_required_for_promotion": True,
        "acceptance_gate_result": "PASS",
        "scenario_policy": "single_scenario",
        "scenario_pass_count": 1,
        "scenario_fail_count": 0,
        "required_scenario_count": 1,
        "scenario_results": [
            {
                "scenario_id": "scenario_001_fixed_bps_unit",
                "scenario_index": 0,
                "scenario_type": "fixed_bps",
                "scenario_role": "base",
                "scenario_acceptance_gate_result": "PASS",
                "scenario_fail_reasons": [],
                "execution_model_hash": "sha256:model",
                "execution_model": {"type": "fixed_bps", "model_params_hash": "sha256:model"},
                "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
                "validation_metrics": {
                    "trade_count": 4,
                    "max_drawdown_pct": 1.0,
                    "profit_factor": 2.0,
                    "return_pct": 1.0,
                },
                "final_holdout_metrics": {
                    "trade_count": 4,
                    "max_drawdown_pct": 1.0,
                    "profit_factor": 2.0,
                    "return_pct": 1.0,
                },
            }
        ],
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["uptrend_normal_vol_volume_increasing"],
        "blocked_live_regimes": ["sideways_low_vol_volume_decreasing"],
        "regime_evidence": {
            "uptrend_normal_vol_volume_increasing": {
                "trade_count": 12,
                "profit_factor": 1.4,
                "expectancy": 100.0,
            }
        },
        "regime_gate_result": {
            "result": "PASS",
            "passed": True,
            "reasons": [],
        },
        "walk_forward_required": False,
    }
    payload.update(overrides)
    explicit_hash = overrides.get("candidate_profile_hash")
    payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _candidate_with_evidence_tier(tier: str) -> dict[str, object]:
    candidate = _candidate()
    contract = dict(candidate["execution_reality_contract"])
    capability = dict(contract["execution_capability_contract"])
    capability["evidence_tier"] = tier
    capability["execution_capability_contract_hash"] = execution_capability_contract_hash(capability)
    contract["execution_reality_level"] = tier
    contract["execution_capability_contract"] = capability
    contract["execution_capability_contract_hash"] = capability["execution_capability_contract_hash"]
    contract["execution_contract_hash"] = execution_contract_hash(contract)
    candidate["execution_reality_contract"] = contract
    candidate["execution_contract_hash"] = contract["execution_contract_hash"]
    candidate["execution_capability_contract"] = capability
    candidate["execution_capability_contract_hash"] = capability["execution_capability_contract_hash"]
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    return candidate


def _metrics_v2_payload(*, schema_version: int = 2) -> dict[str, object]:
    return {
        "metrics_schema_version": schema_version,
        "return_risk": {
            "total_return_pct": 1.0,
            "cagr_pct": 12.0,
            "max_drawdown_pct": 1.0,
            "realized_return_pct": 1.0,
            "unrealized_pnl_end": 0.0,
            "open_position_at_end": False,
        },
        "trade_quality": {
            "closed_trade_count": 4,
            "execution_count": 8,
            "win_rate": 0.75,
            "avg_win": 100.0,
            "avg_loss": -50.0,
            "payoff_ratio": 2.0,
            "profit_factor": 2.0,
            "profit_factor_unbounded": False,
            "expectancy_per_trade_krw": 50.0,
            "expectancy_per_trade_pct": 0.5,
            "max_consecutive_losses": 1,
            "single_trade_dependency_score": 0.25,
        },
        "time_exposure": {
            "period_start_ts": 1,
            "period_end_ts": 2,
            "elapsed_ms": 1,
            "calendar_days": 0.1,
            "active_bar_count": 2,
            "exposure_time_pct": 25.0,
            "avg_holding_time_ms": 600000.0,
            "median_holding_time_ms": 600000.0,
            "max_holding_time_ms": 600000,
        },
        "cost_execution": {
            "fee_total": 1.0,
            "slippage_total": 1.0,
            "fee_drag_ratio": 0.001,
            "fee_drag_ratio_basis": "traded_notional",
            "slippage_drag_ratio": 0.001,
            "slippage_drag_ratio_basis": "traded_notional",
            "filled_execution_count": 8,
            "partial_fill_count": 0,
            "failed_execution_count": 0,
            "skipped_execution_count": 0,
            "quote_coverage_pct": 100.0,
            "median_quote_age_ms": 1.0,
            "p95_quote_age_ms": 2.0,
        },
        "limitation_reasons": [],
    }


def _metrics_gate_policy(**overrides) -> dict[str, object]:
    policy = {
        "metrics_schema_version": 2,
        "min_cagr_pct": 1.0,
        "min_expectancy_per_trade_krw": 1.0,
        "min_expectancy_per_trade_pct": 0.1,
        "max_exposure_time_pct": 80.0,
        "max_avg_holding_time_minutes": 60.0,
        "max_fee_drag_ratio": 0.01,
        "max_slippage_drag_ratio": 0.01,
        "reject_open_position_at_end": True,
        "metrics_contract_required": True,
    }
    policy.update(overrides)
    return policy


def _candidate_with_required_metrics_contract(**overrides) -> dict[str, object]:
    policy = _metrics_gate_policy()
    payload = _candidate(
        metrics_schema_version=2,
        validation_metrics_v2=_metrics_v2_payload(),
        final_holdout_metrics_v2=_metrics_v2_payload(),
        metrics_gate_policy=policy,
        metrics_gate_policy_hash=metrics_gate_policy_hash(policy),
        metrics_contract_required=True,
    )
    payload.update(overrides)
    payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(payload))
    return payload


def _production_candidate(*, attach_stress_suite: bool = True, **overrides):
    base_cost_assumption = {
        "label": "test_realistic_fee_0004_slippage_5bps",
        "role": "base",
        "fee_rate": 0.0004,
        "fee_source": "operator_declared_bithumb_app_fee",
        "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
        "slippage_bps": 5.0,
        "slippage_source": "test_execution_calibration",
        "promotable_as_base": True,
        "source": "execution_model",
    }
    execution_model = {
        "source": "execution_model",
        "scenario_policy": "single_scenario",
        "calibration_required": True,
        "calibration_strictness": "fail",
        "scenarios": [
            {
                "type": "fixed_bps",
                "fee_rate": 0.0004,
                "slippage_bps": 5.0,
                "latency_ms": 0,
                "partial_fill_rate": 0.0,
                "order_failure_rate": 0.0,
                "market_order_extra_cost_bps": 0.0,
                "seed": None,
                "source": "execution_model",
                "scenario_policy": "single_scenario",
                "scenario_role": "base",
                "scenario_role_source": "manifest",
                "cost_assumption": base_cost_assumption,
                "model_params_hash": "sha256:model",
            }
        ],
        "model_params_hash": "sha256:model",
    }
    payload = _candidate(
        deployment_tier="paper_candidate",
        execution_timing_policy=_production_safe_execution_timing(),
        cost_model={"fee_rate": 0.0004, "slippage_bps": 5.0},
        base_cost_assumption=base_cost_assumption,
        cost_assumption_contract=execution_model,
        execution_model_source="execution_model",
        execution_model=execution_model,
        execution_calibration_required=True,
        execution_calibration_strictness="fail",
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
            "artifact_hashes": ["sha256:calibration"],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "reasons": [],
                    "artifact_hash": "sha256:calibration",
                    "content_hash_present": True,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "expected_fill_reference_policy": "next_candle_open",
                    "artifact_fill_reference_policy": "next_candle_open",
                    "artifact_execution_reality_level": "candle_next_open",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "PASS",
                }
            ],
        },
        execution_calibration_artifact_hash="sha256:calibration",
        execution_calibration_artifact_hashes=["sha256:calibration"],
        execution_calibration_policy_source="repo_production_calibration_policy_v1",
        production_calibration_policy_result={
            "target": "paper_candidate",
            "production_bound": True,
            "required": True,
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
            "artifact_hashes": ["sha256:calibration"],
            "policy_source": "repo_production_calibration_policy_v1",
            "operator_next_step": "none",
        },
        production_calibration_policy_reasons=[],
    )
    if "execution_reality_contract" not in overrides and "execution_contract_hash" not in overrides:
        execution_contract = build_execution_reality_contract(
            fill_reference_policy="next_candle_open",
            missing_quote_policy="warn",
            min_execution_reality_level_for_promotion="candle_next_open",
            allow_same_candle_close_fill=False,
            top_of_book_required=False,
            latency_model={"type": "fixed_bps", "latency_ms": 0},
            partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
            order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
            fee_source="operator_declared_bithumb_app_fee",
            slippage_source="test_execution_calibration",
            calibration_required=True,
            calibration_artifact_hash="sha256:calibration",
        )
        payload["execution_reality_contract"] = execution_contract
        payload["execution_contract_hash"] = execution_contract["execution_contract_hash"]
    payload.update(overrides)
    if attach_stress_suite:
        _attach_stress_suite(payload)
    explicit_hash = overrides.get("candidate_profile_hash")
    payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _lineage(*, execution_calibration_artifact_hash: str | None = None) -> dict[str, object]:
    return build_research_lineage(
        experiment_id="promo_exp",
        experiment_family_id="family_001",
        hypothesis_id="hypothesis_001",
        hypothesis_status="pre_registered",
        manifest_hash="sha256:manifest",
        dataset_snapshot_id="snap",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        repository_version="test",
        command_name="research-backtest",
        command_args={"manifest": "/external/manifest.json"},
        execution_calibration_artifact_hash=execution_calibration_artifact_hash,
        search_budget=4,
        parameter_grid_size=4,
        attempt_index=2,
        failed_candidate_count=1,
        holdout_reuse_count=3,
        dataset_reuse_policy="visible_reuse_not_hard_blocked",
        created_at="2026-05-04T00:00:00+00:00",
    )


def _write_report(manager: PathManager, candidate: dict[str, object]) -> None:
    _write_report_with_lineage(manager, candidate)


def _write_report_without_lineage(manager: PathManager, candidate: dict[str, object]) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "dataset_quality_gate_status": "PASS",
        "dataset_quality_gate_reasons": [],
        "dataset_quality_reports": {
            "train": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-train",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
            "validation": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-validation",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
        },
        "repository_version": "test",
        "candidates": [candidate],
    }
    if str(candidate.get("deployment_tier") or "") in {
        "paper_candidate",
        "live_dry_run_candidate",
        "small_live_candidate",
    }:
        payload["statistical_gate_result"] = candidate.get("statistical_gate_result") or "PASS"
        _attach_final_selection(payload, candidate)
    payload["content_hash"] = sha256_prefixed(report_content_hash_payload(payload))
    write_json_atomic(path, payload)


def _write_report_with_lineage(
    manager: PathManager,
    candidate: dict[str, object],
    *,
    lineage_calibration_hash: str | None | object = ...,
    report_overrides: dict[str, object] | None = None,
    include_statistical_evidence: bool = True,
    statistical_evidence_overrides: dict[str, object] | None = None,
) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    if lineage_calibration_hash is ...:
        lineage_calibration_hash = (
            str(candidate.get("execution_calibration_artifact_hash"))
            if str(candidate.get("execution_calibration_artifact_hash") or "").startswith("sha256:")
            else None
        )
    lineage = _lineage(
        execution_calibration_artifact_hash=(
            str(lineage_calibration_hash)
            if str(lineage_calibration_hash or "").startswith("sha256:")
            else None
        )
    )
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "dataset_quality_gate_status": "PASS",
        "dataset_quality_gate_reasons": [],
        "dataset_quality_reports": {
            "train": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-train",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
            "validation": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-validation",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
        },
        "repository_version": "test",
        "experiment_family_id": "family_001",
        "hypothesis_id": "hypothesis_001",
        "hypothesis_status": "pre_registered",
        "search_budget": 4,
        "parameter_space_hash": "sha256:parameter-space",
        "parameter_grid_size": 4,
        "attempt_index": 2,
        "failed_candidate_count": 1,
        "holdout_reuse_count": 3,
        "dataset_reuse_policy": "visible_reuse_not_hard_blocked",
        "candidate_count": 1,
        "lineage": lineage,
        "lineage_hash": lineage["lineage_hash"],
        "candidates": [candidate],
    }
    if report_overrides:
        payload.update(report_overrides)
    if include_statistical_evidence and str(candidate.get("deployment_tier") or "") in {
        "paper_candidate",
        "live_dry_run_candidate",
        "small_live_candidate",
    }:
        _attach_statistical_evidence(manager, payload, candidate, statistical_evidence_overrides or {})
        if not report_overrides or "final_selection_required" not in report_overrides:
            _attach_final_selection(payload, candidate)
    payload["content_hash"] = sha256_prefixed(report_content_hash_payload(payload))
    write_json_atomic(path, payload)
    _write_validation_run_if_ready(manager, candidate)


def _final_selection_contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "required_for_promotion": True,
        "candidate_universe": "acceptance_gate_passed_required_scenarios",
        "must_pass": {
            "dataset_quality_gate_status": "PASS",
            "statistical_gate_result": "PASS",
            "production_calibration_policy_result": "PASS",
            "final_holdout_present": True,
        },
        "selection_exposure_policy": {
            "final_holdout_usage": "confirmatory_metric_in_rank",
            "counts_as_holdout_reuse": True,
        },
        "method": "lexicographic",
        "null_metric_policy": "fail_if_required_else_worst_rank",
        "ranking": [{"metric": "parameter_candidate_id", "order": "asc", "required": True}],
        "unsupported_metric_policy": {
            "sharpe_ratio": "fail_if_required",
            "sortino_ratio": "fail_if_required",
        },
    }


def _attach_final_selection(report: dict[str, object], candidate: dict[str, object]) -> None:
    selection = apply_final_selection_contract(
        contract=_final_selection_contract(),
        candidates=[candidate],
        report_context={
            "dataset_quality_gate_status": report.get("dataset_quality_gate_status"),
            "statistical_gate_result": report.get("statistical_gate_result"),
        },
        production_bound=True,
    )
    report.update(
        {
            "final_selection_required": True,
            "final_selection_contract": selection["final_selection_contract"],
            "final_selection_contract_hash": selection["final_selection_contract_hash"],
            "final_selection_gate_result": selection["gate_result"],
            "final_selection_fail_reasons": selection["fail_reasons"],
            "selected_candidate_id": selection["selected_candidate_id"],
            "selected_candidate_score_hash": selection["selected_candidate_score_hash"],
            "candidate_final_scores_hash": selection["candidate_final_scores_hash"],
            "candidate_final_scores": selection["candidate_final_scores"],
            "best_candidate_id": selection["selected_candidate_id"],
        }
    )


def _statistical_contract(**gate_overrides: object) -> dict[str, object]:
    gates = {
        "max_reality_check_p_value": 0.05,
        "max_spa_p_value": None,
        "min_deflated_sharpe_probability": None,
        "max_holdout_reuse_count": 10,
        "max_attempt_index_without_new_hypothesis": 20,
    }
    gates.update(gate_overrides)
    return {
        "required_for_promotion": True,
        "benchmark": "cash",
        "primary_metric": "net_excess_return",
        "selection_universe": "all_parameter_candidates_all_required_scenarios",
        "multiple_testing_scope": "experiment",
        "bootstrap": {
            "method": "white_reality_check_block_bootstrap",
            "n_bootstrap": 100,
            "block_length_policy": "fixed",
            "seed_policy": "derived_from_selection_universe_hash",
        },
        "gates": gates,
    }


def _attach_statistical_evidence(
    manager: PathManager,
    report: dict[str, object],
    candidate: dict[str, object],
    overrides: dict[str, object],
) -> None:
    contract = overrides.pop("statistical_validation_contract", None) or _statistical_contract()
    selection_hash = str(overrides.pop("selection_universe_hash", None) or "sha256:selection")
    required_scenario_ids = list(overrides.pop("required_scenario_ids", None) or ["scenario_001_fixed_bps_unit"])
    metric_hash = str(
        overrides.pop("candidate_metric_values_hash", None)
        or candidate_metric_values_hash(
            candidates=report.get("candidates") or [candidate],
            required_scenario_ids=[str(item) for item in required_scenario_ids],
            primary_metric="net_excess_return",
            primary_metric_source="validation_metrics",
            benchmark="cash",
        )
    )
    candidate_count = int(report.get("candidate_count") or 1)
    search_budget = int(report.get("search_budget") or candidate_count)
    parameter_grid_size = int(report.get("parameter_grid_size") or candidate_count)
    attempt_index = int(report.get("attempt_index") or 1)
    holdout_reuse_count = int(report.get("holdout_reuse_count") or 0)
    evidence = {
        "artifact_type": "statistical_selection_evidence",
        "schema_version": 1,
        "experiment_id": report["experiment_id"],
        "experiment_family_id": report.get("experiment_family_id"),
        "hypothesis_id": report.get("hypothesis_id"),
        "manifest_hash": report["manifest_hash"],
        "dataset_content_hash": report["dataset_content_hash"],
        "dataset_quality_hash": report.get("dataset_quality_hash"),
        "selection_universe_hash": selection_hash,
        "candidate_metric_values_hash": metric_hash,
        "required_scenario_ids": required_scenario_ids,
        "candidate_metric_values_summary": {
            "candidate_count": candidate_count,
            "metric_value_count": candidate_count,
            "missing_metric_count": 0,
            "primary_metric": "net_excess_return",
            "primary_metric_source": "validation_metrics",
            "benchmark": "cash",
        },
        "candidate_count": candidate_count,
        "metric_value_count": candidate_count,
        "missing_metric_count": 0,
        "search_budget": report.get("search_budget"),
        "parameter_grid_size": report.get("parameter_grid_size"),
        "attempt_index": report.get("attempt_index"),
        "holdout_reuse_count": report.get("holdout_reuse_count"),
        "dataset_reuse_policy": report.get("dataset_reuse_policy"),
        "benchmark": "cash",
        "primary_metric": "net_excess_return",
        "primary_metric_source": "validation_metrics",
        "bootstrap_method": "white_reality_check_block_bootstrap",
        "statistical_method": "white_reality_check_block_bootstrap",
        "evidence_grade": "PROMOTION_GRADE_WRC",
        "minimum_promotion_evidence_grade": "PROMOTION_GRADE_WRC",
        "promotion_grade_available": True,
        "bootstrap_sampling_contract": {
            "method": "white_reality_check_block_bootstrap",
            "n_bootstrap": 100,
            "seed_policy": "derived_from_selection_universe_hash",
            "derived_seed": 1,
            "block_length": 2,
            "block_length_policy": "fixed",
            "stationary_bootstrap_probability": None,
            "observation_count": 2,
            "return_unit": "bar_excess_return",
            "benchmark": "cash",
            "missing_observation_policy": "skip_missing_candidate_trade_returns",
        },
        "n_bootstrap": 100,
        "block_length": 2,
        "block_length_policy": "fixed",
        "seed": 1,
        "effective_trial_count": max(candidate_count, search_budget, parameter_grid_size)
        * max(1, attempt_index)
        * max(1, holdout_reuse_count + 1),
        "summary_metric_max_bootstrap_p_value": 0.01,
        "white_reality_check_p_value": 0.01,
        "white_reality_check_method": "white_reality_check_block_bootstrap",
        "white_reality_check_available": True,
        "statistical_gate_result": "PASS",
        "gate_fail_reasons": [],
        "limitations": [
            "metric_summary_bootstrap_not_trade_or_bar_return_bootstrap",
            "spa_not_implemented",
            "deflated_sharpe_not_implemented",
        ],
        "promotion_grade_limitations": [
            "not_full_white_reality_check",
            "not_bar_return_bootstrap",
            "not_trade_return_bootstrap",
            "spa_not_implemented",
            "deflated_sharpe_not_implemented",
        ],
        "statistical_validation_contract": contract,
        "method_provenance": {
            "implementation": "bithumb_bot.research.statistical_selection.recompute_white_reality_check_block_bootstrap",
            "version": 1,
        },
    }
    evidence["bootstrap_sampling_contract"]["content_hash"] = sha256_prefixed(
        content_hash_payload(evidence["bootstrap_sampling_contract"])
    )
    evidence["bootstrap_sampling_contract_hash"] = evidence["bootstrap_sampling_contract"]["content_hash"]
    panel = {
        "artifact_type": "candidate_return_panel",
        "schema_version": 1,
        "experiment_id": report["experiment_id"],
        "manifest_hash": report["manifest_hash"],
        "dataset_content_hash": report["dataset_content_hash"],
        "dataset_quality_hash": report.get("dataset_quality_hash"),
        "split": "validation",
        "return_unit": "bar_excess_return",
        "benchmark": "cash",
        "ordered_time_index": [1, 2],
        "ordered_time_index_hash": sha256_prefixed([1, 2]),
        "candidate_count": candidate_count,
        "candidate_ids": [str(item.get("parameter_candidate_id") or "") for item in report.get("candidates") or [candidate]],
        "candidate_return_series": [
            {
                "candidate_id": str(candidate.get("parameter_candidate_id") or ""),
                "parameter_values": candidate.get("parameter_values") or {},
                "scenario_ids": ["scenario_001_fixed_bps_unit"],
                "return_unit": "bar_excess_return",
                "benchmark": "cash",
                "observation_count": 2,
                "time_index": [1, 2],
                "time_index_hash": sha256_prefixed([1, 2]),
                "candidate_return_series_values": [
                    {"ts": 1, "sequence": 0, "return_pct": 0.5},
                    {"ts": 2, "sequence": 1, "return_pct": 0.5},
                ],
                "candidate_return_series_hash": sha256_prefixed(
                    [
                        {"ts": 1, "sequence": 0, "return_pct": 0.5},
                        {"ts": 2, "sequence": 1, "return_pct": 0.5},
                    ]
                ),
                "benchmark_return_series_values": [
                    {"ts": 1, "sequence": 0, "return_pct": 0.0},
                    {"ts": 2, "sequence": 1, "return_pct": 0.0},
                ],
                "benchmark_series_hash": sha256_prefixed(
                    [
                        {"ts": 1, "sequence": 0, "return_pct": 0.0},
                        {"ts": 2, "sequence": 1, "return_pct": 0.0},
                    ]
                ),
                "excess_return_series_values": [
                    {"ts": 1, "sequence": 0, "excess_return_pct": 0.5},
                    {"ts": 2, "sequence": 1, "excess_return_pct": 0.5},
                ],
                "benchmark_excess_return_series_hash": sha256_prefixed(
                    [
                        {"ts": 1, "sequence": 0, "excess_return_pct": 0.5},
                        {"ts": 2, "sequence": 1, "excess_return_pct": 0.5},
                    ]
                ),
                "missing_observation_policy": "skip_missing_candidate_trade_returns",
                "return_series_available": True,
            }
        ],
        "observation_count": 2,
        "missing_observation_policy": "skip_missing_candidate_trade_returns",
        "limitations": ["test_fixture_panel"],
    }
    panel["panel_content_hash"] = sha256_prefixed(content_hash_payload(panel))
    panel["content_hash"] = sha256_prefixed(content_hash_payload(panel))
    panel_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "candidate_return_panel.json"
    write_json_atomic(panel_path, panel)
    evidence["white_reality_check_p_value"] = recompute_white_reality_check_block_bootstrap(
        panel=panel,
        sampling_contract=evidence["bootstrap_sampling_contract"],
    )
    registry_path = manager.data_dir() / "reports" / "research" / "families" / "family_001" / "trial_registry.jsonl"
    registry_row = {
        "schema_version": 1,
        "experiment_family_id": "family_001",
        "experiment_id": report["experiment_id"],
        "manifest_hash": report["manifest_hash"],
        "hypothesis_id": report.get("hypothesis_id"),
        "hypothesis_status": report.get("hypothesis_status"),
        "attempt_index": attempt_index,
        "holdout_reuse_count": holdout_reuse_count,
        "dataset_content_hash": report["dataset_content_hash"],
        "parameter_space_hash": report.get("parameter_space_hash") or "sha256:parameter-space",
        "candidate_count": candidate_count,
        "return_panel_hash": panel["content_hash"],
        "statistical_evidence_hash": None,
        "statistical_evidence_hash_phase": "pre_registry_evidence_hash",
        "result_status": "PASS",
        "prior_registry_hash": "sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e191b6417b1db8ce3cdd82e",
        "created_at": "2026-05-04T00:00:00+00:00",
    }
    registry_row["row_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in registry_row.items() if k != "row_hash"}))
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps(registry_row, sort_keys=True) + "\n", encoding="utf-8")
    evidence["return_panel_path"] = str(panel_path)
    evidence["return_panel_hash"] = panel["content_hash"]
    evidence["return_panel_artifact_type"] = "candidate_return_panel"
    evidence["return_panel_split"] = "validation"
    evidence["return_unit"] = "bar_excess_return"
    evidence["return_panel_observation_count"] = 2
    evidence["family_trial_registry_path"] = str(registry_path)
    evidence["family_trial_registry_prior_hash"] = registry_row["prior_registry_hash"]
    evidence["family_trial_registry_bound_evidence_hash"] = None
    evidence["content_hash"] = sha256_prefixed(content_hash_payload(evidence))
    evidence["family_trial_registry_bound_evidence_hash"] = evidence["content_hash"]
    registry_row["statistical_evidence_hash"] = evidence["family_trial_registry_bound_evidence_hash"]
    registry_row["row_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in registry_row.items() if k != "row_hash"}))
    registry_path.write_text(json.dumps(registry_row, sort_keys=True) + "\n", encoding="utf-8")
    evidence["family_trial_registry_row_hash"] = registry_row["row_hash"]
    registry_base = {
        "run_id": report["experiment_id"],
        "experiment_family_id": report.get("experiment_family_id"),
        "hypothesis_id": report.get("hypothesis_id"),
        "hypothesis_status": report.get("hypothesis_status"),
        "experiment_id": report["experiment_id"],
        "manifest_hash": report["manifest_hash"],
        "manifest_metadata_hash": "sha256:metadata",
        "dataset_snapshot_id": report.get("dataset_snapshot_id"),
        "dataset_content_hash": report["dataset_content_hash"],
        "dataset_quality_hash": report.get("dataset_quality_hash"),
        "train_split_hash": "sha256:train",
        "validation_split_hash": "sha256:validation",
        "final_holdout_split_hash": "sha256:final-holdout",
        "final_holdout_fingerprint": "sha256:final-holdout-identity",
        "final_holdout_identity_hash": "sha256:final-holdout-identity",
        "final_holdout_content_hash": "sha256:final-holdout-content",
        "final_holdout_reuse_key_hash": "sha256:final-holdout-identity",
        "parameter_space_hash": report.get("parameter_space_hash") or "sha256:parameter-space",
        "parameter_grid_size": parameter_grid_size,
        "candidate_count": None,
        "declared_attempt_index": attempt_index,
        "declared_holdout_reuse_count": holdout_reuse_count,
        "statistical_evidence_hash": None,
        "return_panel_hash": None,
        "promotion_artifact_hash": None,
        "promoted_candidate_id": None,
        "repository_version": "test",
        "command_args_hash": "sha256:args",
    }
    reserve_research_attempt(
        manager=manager,
        base_payload={**registry_base, "run_id": "prior_family", "experiment_id": "prior_family"},
    )
    for index in range(2):
        reserve_research_attempt(
            manager=manager,
            base_payload={
                **registry_base,
                "run_id": f"prior_reuse_{index}",
                "experiment_id": f"prior_reuse_{index}",
                "experiment_family_id": f"other_family_{index}",
                "hypothesis_id": f"other_hypothesis_{index}",
            },
        )
    reservation = reserve_research_attempt(manager=manager, base_payload=registry_base)
    pre_completion_evidence_hash = str(evidence.get("content_hash") or "")
    completion = append_attempt_completion(
        manager=manager,
        reservation=reservation,
        updates={
            "candidate_count": candidate_count,
            "return_panel_hash": panel["content_hash"],
            "statistical_evidence_hash": pre_completion_evidence_hash,
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
            "statistical_gate_result": "PASS",
        },
        result_status="COMPLETED",
        created_at="2026-05-04T00:00:00+00:00",
    )
    registry_fields = {
        "experiment_registry_path": reservation["path"],
        "experiment_registry_prior_hash": reservation["prior_hash"],
        "experiment_registry_row_hash": reservation["row_hash"],
        "experiment_registry_completion_row_hash": completion["row_hash"],
        "final_holdout_fingerprint": registry_base["final_holdout_fingerprint"],
        "final_holdout_identity_hash": registry_base["final_holdout_identity_hash"],
        "final_holdout_content_hash": registry_base["final_holdout_content_hash"],
        "final_holdout_reuse_key_hash": registry_base["final_holdout_reuse_key_hash"],
        "final_holdout_split_hash": registry_base["final_holdout_split_hash"],
        "experiment_registry_bound_evidence_hash": pre_completion_evidence_hash,
        "experiment_registry_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        "computed_attempt_index": reservation["computed_attempt_index"],
        "computed_holdout_reuse_count": reservation["computed_holdout_reuse_count"],
        "declared_attempt_index": attempt_index,
        "declared_holdout_reuse_count": holdout_reuse_count,
        "registry_gate_result": "PASS",
        "registry_gate_fail_reasons": [],
    }
    registry_fields["research_freedom_hash"] = sha256_prefixed(
        {
            "experiment_family_id": report.get("experiment_family_id"),
            "hypothesis_id": report.get("hypothesis_id"),
            "hypothesis_status": report.get("hypothesis_status"),
            "dataset_snapshot_id": report.get("dataset_snapshot_id"),
            "train_split_hash": registry_base["train_split_hash"],
            "validation_split_hash": registry_base["validation_split_hash"],
            "final_holdout_split_hash": registry_base["final_holdout_split_hash"],
            "final_holdout_fingerprint": registry_base["final_holdout_fingerprint"],
            "final_holdout_identity_hash": registry_base["final_holdout_identity_hash"],
            "final_holdout_content_hash": registry_base["final_holdout_content_hash"],
            "final_holdout_reuse_key_hash": registry_base["final_holdout_reuse_key_hash"],
            "parameter_space_hash": registry_base["parameter_space_hash"],
            "computed_attempt_index": reservation["computed_attempt_index"],
            "computed_holdout_reuse_count": reservation["computed_holdout_reuse_count"],
            "experiment_registry_path": reservation["path"],
            "experiment_registry_prior_hash": reservation["prior_hash"],
            "experiment_registry_row_hash": reservation["row_hash"],
        }
    )
    evidence.update(registry_fields)
    evidence.update(overrides)
    evidence["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in evidence.items() if k != "content_hash"}))
    completion_row = completion["row"]
    assert isinstance(completion_row, dict)
    completion_row["statistical_evidence_hash"] = pre_completion_evidence_hash
    completion_row["statistical_evidence_hash_phase"] = EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE
    completion_row["row_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in completion_row.items() if k != "row_hash"}))
    rows = []
    exp_registry_path = Path(str(reservation["path"]))
    for line in exp_registry_path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row.get("event_type") == "research_attempt_completed" and row.get("reservation_row_hash") == reservation["row_hash"]:
            row = completion_row
        rows.append(row)
    exp_registry_path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
    registry_fields["experiment_registry_completion_row_hash"] = completion_row["row_hash"]
    evidence["experiment_registry_completion_row_hash"] = completion_row["row_hash"]
    evidence["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in evidence.items() if k != "content_hash"}))
    evidence_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "statistical_selection_evidence.json"
    write_json_atomic(evidence_path, evidence)
    report["statistical_validation_required"] = True
    report["statistical_validation_contract"] = contract
    report["benchmark"] = evidence["benchmark"]
    report["primary_metric"] = evidence["primary_metric"]
    report["primary_metric_source"] = evidence["primary_metric_source"]
    report["selection_universe_hash"] = selection_hash
    report["candidate_metric_values_hash"] = metric_hash
    report["candidate_metric_values_summary"] = evidence["candidate_metric_values_summary"]
    report["metric_value_count"] = evidence["metric_value_count"]
    report["missing_metric_count"] = evidence["missing_metric_count"]
    report["statistical_evidence_hash"] = evidence["content_hash"]
    report["statistical_evidence_path"] = str(evidence_path)
    report["evidence_grade"] = evidence.get("evidence_grade")
    report["statistical_method"] = evidence.get("statistical_method")
    report["return_panel_hash"] = evidence.get("return_panel_hash")
    report["return_panel_path"] = evidence.get("return_panel_path")
    report["return_unit"] = evidence.get("return_unit")
    report["return_panel_observation_count"] = evidence.get("return_panel_observation_count")
    report["family_trial_registry_path"] = evidence.get("family_trial_registry_path")
    report["family_trial_registry_prior_hash"] = evidence.get("family_trial_registry_prior_hash")
    report["family_trial_registry_row_hash"] = evidence.get("family_trial_registry_row_hash")
    report.update(registry_fields)
    if isinstance(report.get("lineage"), dict):
        report["lineage"].update(registry_fields)
        report["lineage"]["lineage_hash"] = compute_lineage_hash(report["lineage"])
        report["lineage_hash"] = report["lineage"]["lineage_hash"]
    report["statistical_gate_result"] = evidence.get("statistical_gate_result")
    report["statistical_gate_fail_reasons"] = evidence.get("gate_fail_reasons")
    report["white_reality_check_p_value"] = evidence.get("white_reality_check_p_value")
    report["summary_metric_max_bootstrap_p_value"] = evidence.get("summary_metric_max_bootstrap_p_value")
    report["white_reality_check_method"] = evidence.get("white_reality_check_method")
    report["bootstrap_sampling_contract_hash"] = evidence.get("bootstrap_sampling_contract_hash")
    report["promotion_grade_limitations"] = evidence.get("promotion_grade_limitations")
    report["effective_trial_count"] = evidence.get("effective_trial_count")
    candidate["statistical_validation_required"] = True
    candidate["statistical_validation_contract"] = contract
    candidate["benchmark"] = evidence["benchmark"]
    candidate["primary_metric"] = evidence["primary_metric"]
    candidate["primary_metric_source"] = evidence["primary_metric_source"]
    candidate["selection_universe_hash"] = selection_hash
    candidate["candidate_metric_values_hash"] = metric_hash
    candidate["candidate_metric_values_summary"] = evidence["candidate_metric_values_summary"]
    candidate["candidate_count"] = evidence["candidate_count"]
    candidate["metric_value_count"] = evidence["metric_value_count"]
    candidate["missing_metric_count"] = evidence["missing_metric_count"]
    candidate["statistical_evidence_hash"] = evidence["content_hash"]
    candidate["statistical_evidence_path"] = str(evidence_path)
    candidate["evidence_grade"] = evidence.get("evidence_grade")
    candidate["statistical_method"] = evidence.get("statistical_method")
    candidate["return_panel_hash"] = evidence.get("return_panel_hash")
    candidate["return_panel_path"] = evidence.get("return_panel_path")
    candidate["return_unit"] = evidence.get("return_unit")
    candidate["return_panel_observation_count"] = evidence.get("return_panel_observation_count")
    candidate["family_trial_registry_path"] = evidence.get("family_trial_registry_path")
    candidate["family_trial_registry_prior_hash"] = evidence.get("family_trial_registry_prior_hash")
    candidate["family_trial_registry_row_hash"] = evidence.get("family_trial_registry_row_hash")
    candidate.update(registry_fields)
    candidate["statistical_gate_result"] = evidence.get("statistical_gate_result")
    candidate["statistical_gate_fail_reasons"] = evidence.get("gate_fail_reasons")
    candidate["white_reality_check_p_value"] = evidence.get("white_reality_check_p_value")
    candidate["summary_metric_max_bootstrap_p_value"] = evidence.get("summary_metric_max_bootstrap_p_value")
    candidate["white_reality_check_method"] = evidence.get("white_reality_check_method")
    candidate["bootstrap_sampling_contract_hash"] = evidence.get("bootstrap_sampling_contract_hash")
    candidate["promotion_grade_limitations"] = evidence.get("promotion_grade_limitations")
    candidate["effective_trial_count"] = evidence.get("effective_trial_count")
    candidate.pop("candidate_profile_hash", None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))


def _attach_stress_suite(candidate: dict[str, object], *, tamper_hash: bool = False) -> None:
    contract = {
        "required_for_promotion": True,
        "trade_removal": {"top_n_by_net_pnl": [1], "min_return_retention_pct": 50.0},
        "trade_order_monte_carlo": {
            "iterations": 100,
            "seed_policy": "derived_from_manifest_candidate_scenario_split_hash",
            "min_survival_probability": 0.95,
            "ruin_max_drawdown_pct": 35.0,
            "min_closed_trades": 4,
        },
    }
    contract_hash = sha256_prefixed(contract)
    evidence = {
        "stress_suite_schema_version": 1,
        "contract_hash": contract_hash,
        "seed_material_hash": "sha256:seed",
        "context": {
            "experiment_id": "promo_exp",
            "candidate_id": "candidate_001",
            "scenario_id": "scenario_001_fixed_bps_unit",
            "split_name": "validation",
        },
        "gate_result": "PASS",
        "fail_reasons": [],
        "trade_removal": {"status": "PASS", "cases": [], "fail_reasons": []},
        "trade_order_monte_carlo": {
            "status": "PASS",
            "iterations": 100,
            "seed": 123,
            "terminal_equity_p05": 990000.0,
            "terminal_equity_median": 1010000.0,
            "terminal_equity_p95": 1020000.0,
            "max_drawdown_pct_p50": 5.0,
            "max_drawdown_pct_p95": 10.0,
            "longest_losing_streak_p50": 1.0,
            "longest_losing_streak_p95": 2.0,
            "survival_probability": 1.0,
            "ruin_max_drawdown_pct": 35.0,
            "fail_reasons": [],
        },
        "limitations": [],
    }
    evidence["stress_suite_hash"] = sha256_prefixed(content_hash_payload(evidence))
    final_evidence = dict(evidence, context={**evidence["context"], "split_name": "final_holdout"})
    final_evidence.pop("stress_suite_hash", None)
    final_evidence["stress_suite_hash"] = sha256_prefixed(content_hash_payload(final_evidence))
    if tamper_hash:
        evidence["stress_suite_hash"] = "sha256:tampered"
    candidate["stress_suite_required"] = True
    candidate["stress_suite_contract"] = contract
    candidate["stress_suite_contract_hash"] = contract_hash
    candidate["validation_stress_suite"] = evidence
    candidate["final_holdout_stress_suite"] = final_evidence
    candidate["stress_suite_gate_result"] = "PASS"
    candidate["stress_suite_fail_reasons"] = []
    candidate.pop("candidate_profile_hash", None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))


def _walk_forward_candidate(backtest_candidate: dict[str, object], **overrides) -> dict[str, object]:
    payload = dict(backtest_candidate)
    payload.update(
        {
            "walk_forward_metrics": {
                "window_count": 3,
                "pass_window_count": 3,
                "fail_window_count": 0,
                "mean_test_return_pct": 1.0,
                "median_test_return_pct": 1.0,
                "worst_test_return_pct": 0.5,
                "return_consistency_pass": True,
            },
            "walk_forward_gate_result": "PASS",
        }
    )
    payload.pop("candidate_profile_hash", None)
    payload.update(overrides)
    explicit_hash = payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _write_walk_forward_report(manager: PathManager, candidate: dict[str, object]) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "candidates": [candidate],
    }
    payload["content_hash"] = sha256_prefixed(report_content_hash_payload(payload))
    write_json_atomic(path, payload)
    _write_validation_run_if_ready(manager, candidate)


def _write_validation_run_if_ready(manager: PathManager, candidate: dict[str, object]) -> None:
    if str(candidate.get("deployment_tier") or "") not in {
        "paper_candidate",
        "live_dry_run_candidate",
        "small_live_candidate",
    }:
        return
    report_dir = manager.data_dir() / "reports" / "research" / "promo_exp"
    backtest_path = report_dir / "backtest_report.json"
    if not backtest_path.exists():
        return
    backtest = json.loads(backtest_path.read_text(encoding="utf-8"))
    if not isinstance(backtest, dict) or not str(backtest.get("content_hash") or "").startswith("sha256:"):
        return
    walk_required = bool(candidate.get("walk_forward_required"))
    walk_path = report_dir / "walk_forward_report.json"
    walk_hash = None
    if walk_required:
        if not walk_path.exists():
            return
        walk = json.loads(walk_path.read_text(encoding="utf-8"))
        walk_hash = str(walk.get("content_hash") or "")
        if not walk_hash.startswith("sha256:"):
            return
    stages = [
        {"name": "readiness", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {}, "artifact_hashes": {}, "reasons": []},
        {"name": "backtest", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {"backtest_report_path": str(backtest_path.resolve())}, "artifact_hashes": {"backtest_report_hash": backtest["content_hash"]}, "reasons": []},
        {"name": "walk_forward", "required": walk_required, "status": "PASS" if walk_required else "SKIPPED_NOT_REQUIRED", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {"walk_forward_report_path": str(walk_path.resolve())} if walk_required else {}, "artifact_hashes": {"walk_forward_report_hash": walk_hash} if walk_required else {}, "reasons": []},
        {"name": "promotion", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {}, "artifact_hashes": {}, "reasons": []},
        {"name": "reproduce", "required": True, "status": "PASS", "started_at": None, "completed_at": None, "input_hashes": {}, "output_hashes": {}, "artifact_paths": {}, "artifact_hashes": {}, "reasons": []},
    ]
    payload = {
        "validation_run_schema_version": 1,
        "validation_run_id": "sha256:test-validation-run",
        "experiment_id": "promo_exp",
        "manifest_path": "/tmp/manifest.json",
        "manifest_hash": candidate.get("manifest_hash"),
        "repository_version": "test",
        "deployment_tier": candidate.get("deployment_tier"),
        "mode": "strict",
        "command_args_hash": "sha256:args",
        "required_stage_names": ["readiness", "backtest", *(["walk_forward"] if walk_required else []), "promotion", "reproduce"],
        "stages": stages,
        "selected_candidate_id": candidate.get("parameter_candidate_id"),
        "backtest_report_path": str(backtest_path.resolve()),
        "backtest_report_hash": backtest["content_hash"],
        "walk_forward_report_path": str(walk_path.resolve()) if walk_required else None,
        "walk_forward_report_hash": walk_hash,
        "promotion_artifact_path": None,
        "promotion_artifact_hash": None,
        "reproduce_ok": True,
        "promotion_allowed": True,
        "end_to_end_validation_result": "PASS",
        "fail_closed_reasons": [],
        "validation_run_path": str((report_dir / "validation_run.json").resolve()),
        "generated_at": None,
    }
    payload["validation_run_binding_hash"] = validation_run_binding_hash(payload)
    payload["content_hash"] = validation_run_content_hash(payload)
    write_json_atomic(report_dir / "validation_run.json", payload)


def _canonical_report_hash(payload: dict[str, object]) -> str:
    return sha256_prefixed(report_content_hash_payload(payload))


def _rewrite_report(path: Path, payload: dict[str, object]) -> None:
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(report_content_hash_payload(payload))
    write_json_atomic(path, payload)


def _refresh_candidate_profile_hash(candidate: dict[str, object]) -> None:
    candidate.pop("candidate_profile_hash", None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))


def _rewrite_promotion_artifact(path: Path, payload: dict[str, object]) -> None:
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    write_json_atomic(path, payload)


def _bind_validation_run_to_promotion(validation_run_path: Path, promotion: dict[str, object], promotion_path: Path) -> None:
    payload = json.loads(validation_run_path.read_text(encoding="utf-8"))
    payload["promotion_artifact_path"] = str(promotion_path.resolve())
    payload["promotion_artifact_hash"] = promotion["content_hash"]
    for stage in payload.get("stages") or []:
        if isinstance(stage, dict) and stage.get("name") == "promotion":
            stage["artifact_paths"] = {"promotion_artifact_path": str(promotion_path.resolve())}
            stage["artifact_hashes"] = {"promotion_artifact_hash": promotion["content_hash"]}
    payload.pop("content_hash", None)
    payload["content_hash"] = validation_run_content_hash(payload)
    write_json_atomic(validation_run_path, payload)


def _statistical_metric_hash_for_report(report: dict[str, object]) -> str:
    return candidate_metric_values_hash(
        candidates=report["candidates"],
        required_scenario_ids=["scenario_001_fixed_bps_unit"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="cash",
    )


def _rewrite_promotion_with_backtest_hash(promotion_path: Path, backtest_hash: str) -> None:
    promotion = json.loads(promotion_path.read_text(encoding="utf-8"))
    promotion["backtest_report_hash"] = backtest_hash
    promotion["lineage"]["backtest_report_hash"] = backtest_hash
    promotion["lineage"].pop("lineage_hash", None)
    promotion["lineage"]["lineage_hash"] = compute_lineage_hash(promotion["lineage"])
    promotion["lineage_hash"] = promotion["lineage"]["lineage_hash"]
    promotion.pop("content_hash", None)
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    write_json_atomic(promotion_path, promotion)


def test_promotion_refuses_candidate_without_validation_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(validation_metrics=None)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="validation_oos_evidence_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


@pytest.mark.parametrize(
    "candidate",
    [
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["min_trade_count_failed"]),
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["max_drawdown_failed"]),
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["profit_factor_failed"]),
    ],
)
def test_promotion_refuses_failed_gate_candidates(tmp_path, monkeypatch, candidate) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="acceptance_gate_not_passed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_does_not_mutate_env_file(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    env_file = tmp_path / "live.env"
    env_file.write_text("SMA_SHORT=99\n", encoding="utf-8")
    before = env_file.read_text(encoding="utf-8")
    _write_report(manager, _candidate())

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact_path.exists()
    assert env_file.read_text(encoding="utf-8") == before
    assert result.artifact["operator_next_step"].startswith("Review this artifact")


def test_promotion_refuses_candidate_profile_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(candidate_profile_hash="sha256:tampered")
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not (manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json").exists()


def test_reserved_future_evidence_tier_fails_promotion(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate_with_evidence_tier("impact_model_calibrated")
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_evidence_tier_reserved_not_implemented"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_unknown_evidence_tier_fails_promotion(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate_with_evidence_tier("unknown_future_scalar_proxy")
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_evidence_tier_unsupported"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_backtest_candidate_hash_mismatch_even_when_walk_forward_exists(
    tmp_path, monkeypatch
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    backtest_candidate["candidate_profile_hash"] = "sha256:tampered"
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    with pytest.raises(PromotionGateError, match="backtest_candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not (manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json").exists()


def test_promotion_refuses_backtest_gate_failure_even_when_walk_forward_passes(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(
        acceptance_gate_result="FAIL",
        gate_fail_reasons=["min_trade_count_failed"],
        walk_forward_required=True,
    )
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))

    with pytest.raises(PromotionGateError, match="backtest_acceptance_gate_not_passed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_candidate_profile_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(
        manager,
        _walk_forward_candidate(backtest_candidate, candidate_profile_hash="sha256:tampered"),
    )

    with pytest.raises(PromotionGateError, match="walk_forward_candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_cost_contract_drift(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _production_candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    drifted_base = dict(walk_forward_candidate["base_cost_assumption"])
    drifted_base["fee_rate"] = 0.0005
    walk_forward_candidate["base_cost_assumption"] = drifted_base
    walk_forward_candidate.pop("candidate_profile_hash", None)
    walk_forward_candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(walk_forward_candidate))
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    with pytest.raises(PromotionGateError, match="walk_forward_candidate_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_uses_verified_candidate_profile_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    expected_hash = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["candidate_profile_hash"] == expected_hash
    assert result.artifact["verified_candidate_profile_hash"] == expected_hash
    assert result.artifact["strategy_profile_hash"] == expected_hash


def test_promotion_artifact_exposes_execution_event_summary_top_level(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["execution_event_summary"] == candidate["execution_event_summary"]
    assert result.artifact["train_execution_event_summary"] == candidate.get("train_execution_event_summary")
    assert result.artifact["validation_execution_event_summary"] == candidate.get("validation_execution_event_summary")
    assert result.artifact["final_holdout_execution_event_summary"] == candidate.get("final_holdout_execution_event_summary")
    assert result.artifact["pending_execution_after_dataset_end_count"] == 0
    assert result.artifact["execution_event_timeline_incomplete"] is False
    assert result.artifact["portfolio_applied_trade_count"] == 8
    assert result.artifact["execution_filled_count"] == 8
    assert result.artifact["closed_trade_count"] == 4


def test_candidate_profile_hash_binds_metrics_gate_policy() -> None:
    candidate = _candidate_with_required_metrics_contract()
    changed_policy = dict(candidate["metrics_gate_policy"])
    changed_policy["min_cagr_pct"] = 2.0
    changed = dict(candidate)
    changed["metrics_gate_policy"] = changed_policy
    changed["metrics_gate_policy_hash"] = metrics_gate_policy_hash(changed_policy)
    changed.pop("candidate_profile_hash", None)

    assert metrics_gate_policy_hash(candidate["metrics_gate_policy"]) == candidate["metrics_gate_policy_hash"]
    assert sha256_prefixed(build_candidate_profile(candidate)) != sha256_prefixed(build_candidate_profile(changed))


def test_candidate_profile_hash_binds_official_promotion_grade_wrc_availability() -> None:
    candidate = _candidate()
    candidate["official_promotion_grade_wrc_generation_available"] = False
    changed = dict(candidate)
    changed["official_promotion_grade_wrc_generation_available"] = True
    changed.pop("candidate_profile_hash", None)

    profile = build_candidate_profile(candidate)

    assert profile["official_promotion_grade_wrc_generation_available"] is False
    assert sha256_prefixed(profile) != sha256_prefixed(build_candidate_profile(changed))


def test_promotion_refuses_required_metrics_contract_when_validation_v2_removed(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate_with_required_metrics_contract(validation_metrics_v2=None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="validation_metrics_v2_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_required_metrics_contract_when_schema_mismatches(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate_with_required_metrics_contract(validation_metrics_v2=_metrics_v2_payload(schema_version=1))
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="metrics_contract_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_metrics_gate_policy_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate_with_required_metrics_contract(metrics_gate_policy_hash="sha256:tampered")
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="metrics_gate_policy_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_exposes_metrics_contract_evidence_top_level_and_strict_json(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate_with_required_metrics_contract()
    json.dumps(build_candidate_profile(candidate), allow_nan=False)
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["metrics_schema_version"] == 2
    assert result.artifact["validation_metrics_v2"] == candidate["validation_metrics_v2"]
    assert result.artifact["final_holdout_metrics_v2"] == candidate["final_holdout_metrics_v2"]
    assert result.artifact["metrics_gate_policy"] == candidate["metrics_gate_policy"]
    assert result.artifact["metrics_gate_policy_hash"] == candidate["metrics_gate_policy_hash"]
    assert result.artifact["metrics_contract_required"] is True
    assert result.artifact["metrics_v2_summary"]["validation_cagr_pct"] == 12.0
    assert result.artifact["metrics_v2_summary"]["validation_open_position_at_end"] is False
    assert result.artifact["metrics_v2_summary"]["validation_fee_drag_ratio_basis"] == "traded_notional"
    assert result.artifact["metrics_v2_summary"]["final_holdout_slippage_drag_ratio_basis"] == "traded_notional"
    json.dumps(result.artifact, allow_nan=False)


def test_promotion_artifact_exposes_required_stress_suite_and_reproduces(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    summary = reproduce_promotion(result.artifact_path).summary

    assert result.artifact["stress_suite_required"] is True
    assert result.artifact["stress_suite_contract_hash"].startswith("sha256:")
    assert result.artifact["validation_stress_suite"]["stress_suite_hash"].startswith("sha256:")
    assert result.artifact["stress_suite_gate_result"] == "PASS"
    assert summary["ok"] is True


@pytest.mark.parametrize("field_action", ["missing", "false"])
def test_production_bound_promotion_refuses_missing_stress_suite_when_flag_not_true(
    tmp_path,
    monkeypatch,
    field_action,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(attach_stress_suite=False)
    if field_action == "missing":
        candidate.pop("stress_suite_required", None)
    else:
        candidate["stress_suite_required"] = False
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="stress_suite_required_but_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_research_only_promotion_can_omit_stress_suite(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    candidate.pop("stress_suite_required", None)
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["deployment_tier"] == "research_only"
    assert result.artifact["stress_suite_required"] is False


def test_promotion_refuses_unavailable_execution_capability_requirement() -> None:
    contract = build_execution_reality_contract(
        fill_reference_policy="next_candle_open",
        missing_quote_policy="warn",
        min_execution_reality_level_for_promotion="candle_next_open",
        allow_same_candle_close_fill=False,
        top_of_book_required=False,
        market_impact_required=True,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test",
        slippage_source="test",
    )
    candidate = _candidate(
        execution_reality_contract=contract,
        execution_contract_hash=contract["execution_contract_hash"],
        execution_capability_contract=contract["execution_capability_contract"],
        execution_capability_contract_hash=contract["execution_capability_contract_hash"],
    )

    allowed, reasons = validate_backtest_candidate_for_promotion(candidate)

    assert allowed is False
    assert "execution_capability_required_unavailable" in reasons
    assert "execution_market_impact_required_but_unavailable" in reasons


def test_promotion_refuses_top_level_reserved_evidence_tier_even_if_reality_contract_is_clean() -> None:
    candidate = _candidate()
    clean_reality_contract = candidate["execution_reality_contract"]
    capability = dict(clean_reality_contract["execution_capability_contract"])
    capability["evidence_tier"] = "impact_model_calibrated"
    capability["execution_capability_contract_hash"] = execution_capability_contract_hash(capability)
    candidate["execution_reality_contract"] = clean_reality_contract
    candidate["execution_contract_hash"] = clean_reality_contract["execution_contract_hash"]
    candidate["execution_capability_contract"] = capability
    candidate["execution_capability_contract_hash"] = capability["execution_capability_contract_hash"]
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))

    allowed, reasons = validate_backtest_candidate_for_promotion(candidate)

    assert allowed is False
    assert "execution_evidence_tier_reserved_not_implemented" in reasons


def test_promotion_refuses_top_level_unknown_evidence_tier_even_if_reality_contract_is_clean() -> None:
    candidate = _candidate()
    clean_reality_contract = candidate["execution_reality_contract"]
    capability = dict(clean_reality_contract["execution_capability_contract"])
    capability["evidence_tier"] = "scalar_stress_not_real_impact"
    capability["execution_capability_contract_hash"] = execution_capability_contract_hash(capability)
    candidate["execution_reality_contract"] = clean_reality_contract
    candidate["execution_contract_hash"] = clean_reality_contract["execution_contract_hash"]
    candidate["execution_capability_contract"] = capability
    candidate["execution_capability_contract_hash"] = capability["execution_capability_contract_hash"]
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))

    allowed, reasons = validate_backtest_candidate_for_promotion(candidate)

    assert allowed is False
    assert "execution_evidence_tier_unsupported" in reasons


def test_promotion_refuses_top_level_capability_unavailable_required_mismatch() -> None:
    candidate = _candidate()
    clean_reality_contract = candidate["execution_reality_contract"]
    capability = dict(clean_reality_contract["execution_capability_contract"])
    required = dict(capability["strategy_required_capabilities"])
    required["market_impact_model"] = True
    capability["strategy_required_capabilities"] = required
    capability["unavailable_required_capabilities"] = []
    capability["execution_capability_contract_hash"] = execution_capability_contract_hash(capability)
    candidate["execution_reality_contract"] = clean_reality_contract
    candidate["execution_contract_hash"] = clean_reality_contract["execution_contract_hash"]
    candidate["execution_capability_contract"] = capability
    candidate["execution_capability_contract_hash"] = capability["execution_capability_contract_hash"]
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))

    allowed, reasons = validate_backtest_candidate_for_promotion(candidate)

    assert allowed is False
    assert "execution_capability_unavailable_required_capabilities_mismatch" in reasons


def test_promotion_refuses_missing_final_holdout_stress_when_final_holdout_present(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    candidate["final_holdout_stress_suite"] = None
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="final_holdout_stress_suite_required_but_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_missing_final_holdout_stress_when_required_for_promotion(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(final_holdout_present=False, final_holdout_metrics=None)
    _attach_stress_suite(candidate)
    candidate["final_holdout_stress_suite"] = None
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="final_holdout_stress_suite_required_but_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_allows_missing_final_holdout_stress_when_no_holdout_not_required(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(final_holdout_present=False, final_holdout_required_for_promotion=False, final_holdout_metrics=None)
    _attach_stress_suite(candidate)
    candidate["final_holdout_stress_suite"] = None
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["final_holdout_stress_suite"] is None


def test_promotion_refuses_required_stress_suite_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate, tamper_hash=True)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="stress_suite_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_required_final_holdout_stress_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    candidate["final_holdout_stress_suite"]["stress_suite_hash"] = "sha256:tampered"
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="final_holdout_stress_suite_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_missing_candidate_stress_contract_with_report_fallback(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    report_contract = candidate["stress_suite_contract"]
    report_contract_hash = candidate["stress_suite_contract_hash"]
    candidate["stress_suite_contract"] = None
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(
        manager,
        candidate,
        report_overrides={
            "stress_suite_required": True,
            "stress_suite_contract": report_contract,
            "stress_suite_contract_hash": report_contract_hash,
        },
    )

    with pytest.raises(PromotionGateError, match="stress_suite_contract_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_candidate_stress_contract_hash_without_body(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    candidate["stress_suite_contract"] = None
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="stress_suite_contract_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_report_candidate_stress_contract_drift(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    report_contract = {
        **candidate["stress_suite_contract"],
        "trade_removal": {"top_n_by_net_pnl": [2], "min_return_retention_pct": 50.0},
    }
    _write_report_with_lineage(
        manager,
        candidate,
        report_overrides={
            "stress_suite_required": True,
            "stress_suite_contract": report_contract,
            "stress_suite_contract_hash": sha256_prefixed(report_contract),
        },
    )

    with pytest.raises(PromotionGateError, match="stress_suite_contract_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_validation_stress_contract_hash_drift(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    candidate["validation_stress_suite"]["contract_hash"] = "sha256:other-contract"
    candidate["validation_stress_suite"].pop("stress_suite_hash", None)
    candidate["validation_stress_suite"]["stress_suite_hash"] = sha256_prefixed(
        content_hash_payload(candidate["validation_stress_suite"])
    )
    _refresh_candidate_profile_hash(candidate)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="stress_suite_contract_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_execution_event_summary_matches_candidate_profile(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    profile = result.artifact["candidate_profile"]
    assert result.artifact["execution_event_summary"] == profile["execution_event_summary"]
    assert result.artifact["train_execution_event_summary"] == profile["train_execution_event_summary"]
    assert result.artifact["validation_execution_event_summary"] == profile["validation_execution_event_summary"]
    assert (
        result.artifact["final_holdout_execution_event_summary"]
        == profile["final_holdout_execution_event_summary"]
    )


def test_promotion_artifact_hash_changes_when_execution_event_summary_changes(tmp_path, monkeypatch) -> None:
    clean_manager = _manager(tmp_path / "clean", monkeypatch)
    clean_candidate = _candidate()
    _write_report(clean_manager, clean_candidate)
    clean = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=clean_manager,
        generated_at="2026-05-07T00:00:00+00:00",
    )

    changed_manager = _manager(tmp_path / "changed", monkeypatch)
    changed_summary = dict(clean_candidate["execution_event_summary"])
    changed_summary["execution_attempt_count"] = 9
    changed_summary["skipped_execution_count"] = 1
    changed_candidate = _candidate(execution_event_summary=changed_summary)
    _write_report(changed_manager, changed_candidate)
    changed = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=changed_manager,
        generated_at="2026-05-07T00:00:00+00:00",
    )

    assert clean.content_hash != changed.content_hash
    assert clean.artifact["execution_event_summary"] != changed.artifact["execution_event_summary"]


def test_promotion_artifact_records_backtest_and_walk_forward_evidence_hashes(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["validation_evidence_source"] == "backtest_report.json"
    assert result.artifact["backtest_candidate_profile_hash"] == backtest_candidate["candidate_profile_hash"]
    assert result.artifact["backtest_candidate_profile_verified"] is True
    assert result.artifact["walk_forward_required"] is True
    assert result.artifact["walk_forward_evidence_source"] == "walk_forward_report.json"
    assert result.artifact["walk_forward_candidate_profile_hash"] == walk_forward_candidate["candidate_profile_hash"]
    assert result.artifact["walk_forward_candidate_profile_verified"] is True


def test_promotion_artifact_uses_verified_report_hashes_in_artifact_and_lineage(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    backtest_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    expected_backtest_hash = _canonical_report_hash(json.loads(backtest_path.read_text(encoding="utf-8")))
    expected_walk_hash = _canonical_report_hash(json.loads(walk_path.read_text(encoding="utf-8")))

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["backtest_report_hash"] == expected_backtest_hash
    assert result.artifact["walk_forward_report_hash"] == expected_walk_hash
    assert result.artifact["lineage"]["backtest_report_hash"] == expected_backtest_hash
    assert result.artifact["lineage"]["walk_forward_report_hash"] == expected_walk_hash


def test_production_promotion_refuses_missing_validation_run_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report(manager, candidate)
    (manager.data_dir() / "reports" / "research" / "promo_exp" / "validation_run.json").unlink()

    with pytest.raises(PromotionGateError, match="validation_run_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_validation_run_candidate_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report(manager, candidate)
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "validation_run.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["selected_candidate_id"] = "candidate_002"
    payload["content_hash"] = validation_run_content_hash(payload)
    write_json_atomic(path, payload)

    with pytest.raises(PromotionGateError, match="validation_run_selected_candidate_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_validation_run_walk_forward_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _production_candidate(walk_forward_required=True, walk_forward_gate_result="PASS")
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "validation_run.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["walk_forward_report_hash"] = "sha256:mismatch"
    payload["content_hash"] = validation_run_content_hash(payload)
    write_json_atomic(path, payload)

    with pytest.raises(PromotionGateError, match="validation_run_walk_forward_report_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_backtest_body_tamper_with_stale_embedded_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report(manager, _candidate())
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["dataset_content_hash"] = "sha256:tampered_dataset"
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="backtest_report_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_backtest_missing_content_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report(manager, _candidate())
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload.pop("content_hash", None)
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="backtest_report_content_hash_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_body_tamper_with_stale_embedded_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = json.loads(walk_path.read_text(encoding="utf-8"))
    payload["manifest_hash"] = "sha256:tampered_manifest"
    write_json_atomic(walk_path, payload)

    with pytest.raises(PromotionGateError, match="walk_forward_report_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_missing_content_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = json.loads(walk_path.read_text(encoding="utf-8"))
    payload.pop("content_hash", None)
    write_json_atomic(walk_path, payload)

    with pytest.raises(PromotionGateError, match="walk_forward_report_content_hash_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_records_no_walk_forward_evidence_when_not_required(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=False)
    _write_report(manager, backtest_candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["validation_evidence_source"] == "backtest_report.json"
    assert result.artifact["backtest_candidate_profile_hash"] == backtest_candidate["candidate_profile_hash"]
    assert result.artifact["backtest_candidate_profile_verified"] is True
    assert result.artifact["walk_forward_required"] is False
    assert result.artifact["walk_forward_evidence_source"] is None
    assert result.artifact["walk_forward_candidate_profile_hash"] is None
    assert result.artifact["walk_forward_candidate_profile_verified"] is False
    assert result.artifact["regime_classifier_version"] == "market_regime_v2"
    assert result.artifact["allowed_regimes"] == ["uptrend_normal_vol_volume_increasing"]
    assert result.artifact["blocked_regimes"] == ["sideways_low_vol_volume_decreasing"]
    assert result.artifact["live_regime_policy"]["missing_policy_behavior"] == "fail_closed"


def test_lineage_backed_promotion_records_reproducibility_fields(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    summary = reproduce_promotion(result.artifact_path).summary

    assert result.artifact["lineage_required"] is True
    assert result.artifact["legacy_compatibility_used"] is False
    assert result.artifact["lineage_hash"].startswith("sha256:")
    assert result.artifact["experiment_family_id"] == "family_001"
    assert result.artifact["hypothesis_id"] == "hypothesis_001"
    assert result.artifact["search_budget"] == 4
    assert result.artifact["parameter_grid_size"] == 4
    assert result.artifact["attempt_index"] == 2
    assert result.artifact["failed_candidate_count"] == 1
    assert result.artifact["holdout_reuse_count"] == 3
    assert result.artifact["dataset_reuse_policy"] == "visible_reuse_not_hard_blocked"
    assert summary["ok"] is True
    assert summary["lineage_hash"] == result.artifact["lineage_hash"]
    assert summary["candidate_profile_hash"] == result.artifact["candidate_profile_hash"]


def test_production_promotion_refuses_deterministic_pass_without_statistical_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        parameter_candidate_id="candidate_lucky_winner",
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4, "SMA_FILTER_GAP_MIN_RATIO": 0.01},
    )
    candidate.pop("candidate_profile_hash", None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    _write_report_with_lineage(
        manager,
        candidate,
        report_overrides={
            "search_budget": 5000,
            "parameter_grid_size": 5000,
            "attempt_index": 17,
            "holdout_reuse_count": 9,
        },
        include_statistical_evidence=False,
    )

    with pytest.raises(PromotionGateError, match="statistical_contract_missing|statistical_evidence_missing"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_lucky_winner",
            manager=manager,
        )

def test_production_promotion_refuses_excessive_holdout_reuse_and_attempt_budget(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        report_overrides={
            "attempt_index": 17,
            "holdout_reuse_count": 9,
        },
        statistical_evidence_overrides={
            "attempt_index": 17,
            "holdout_reuse_count": 9,
            "statistical_gate_result": "FAIL",
            "gate_fail_reasons": ["attempt_budget_exceeded", "holdout_reuse_budget_exceeded"],
        },
    )

    with pytest.raises(PromotionGateError, match="attempt_budget_exceeded"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_missing_experiment_registry_row(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    Path(str(report["experiment_registry_path"])).unlink()

    with pytest.raises(PromotionGateError, match="experiment_registry_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_statistical_evidence_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["statistical_evidence_hash"] = "sha256:tampered"
    report["candidates"][0]["statistical_evidence_hash"] = "sha256:tampered"
    report["candidates"][0].pop("candidate_profile_hash", None)
    report["candidates"][0]["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(report["candidates"][0]))
    report.pop("content_hash", None)
    report["content_hash"] = sha256_prefixed(report_content_hash_payload(report))
    write_json_atomic(report_path, report)

    with pytest.raises(PromotionGateError, match="statistical_evidence_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_selection_universe_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        statistical_evidence_overrides={"selection_universe_hash": "sha256:evidence-selection"},
    )
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["selection_universe_hash"] = "sha256:report-selection"
    report["candidates"][0]["selection_universe_hash"] = "sha256:report-selection"
    report["candidates"][0].pop("candidate_profile_hash", None)
    report["candidates"][0]["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(report["candidates"][0]))
    report.pop("content_hash", None)
    report["content_hash"] = sha256_prefixed(report_content_hash_payload(report))
    write_json_atomic(report_path, report)

    with pytest.raises(PromotionGateError, match="selection_universe_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_stale_statistical_holdout_reuse_count(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        statistical_evidence_overrides={"holdout_reuse_count": 2},
    )

    with pytest.raises(PromotionGateError, match="statistical_holdout_reuse_count_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_stale_statistical_attempt_index(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        statistical_evidence_overrides={"attempt_index": 1},
    )

    with pytest.raises(PromotionGateError, match="statistical_attempt_index_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("search_budget", "statistical_search_budget_mismatch"),
        ("parameter_grid_size", "statistical_parameter_grid_size_mismatch"),
    ],
)
def test_production_promotion_refuses_stale_statistical_search_universe(
    tmp_path,
    monkeypatch,
    field,
    reason,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        statistical_evidence_overrides={field: 1},
    )

    with pytest.raises(PromotionGateError, match=reason):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_stale_candidate_metric_values_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        statistical_evidence_overrides={"candidate_metric_values_hash": "sha256:evidence-metric-values"},
    )
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["candidate_metric_values_hash"] = "sha256:report-metric-values"
    report["candidates"][0]["candidate_metric_values_hash"] = "sha256:report-metric-values"
    report["candidates"][0].pop("candidate_profile_hash", None)
    report["candidates"][0]["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(report["candidates"][0]))
    report.pop("content_hash", None)
    report["content_hash"] = sha256_prefixed(report_content_hash_payload(report))
    write_json_atomic(report_path, report)

    with pytest.raises(PromotionGateError, match="candidate_metric_values_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_stale_candidate_metric_values_hash_after_report_metric_change(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["candidates"][0]["validation_metrics"]["return_pct"] = 2.0
    _refresh_candidate_profile_hash(report["candidates"][0])
    _rewrite_report(report_path, report)

    with pytest.raises(PromotionGateError, match="candidate_metric_values_hash_recompute_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_candidate_count_field_that_disagrees_with_report_candidates_length(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    second = _production_candidate(
        parameter_candidate_id="candidate_002",
        parameter_values={"SMA_SHORT": 3, "SMA_LONG": 4},
    )
    report["candidates"].append(second)
    report["candidate_count"] = 1
    _rewrite_report(report_path, report)

    with pytest.raises(PromotionGateError, match="statistical_candidate_count_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_evidence_candidate_count_that_disagrees_with_report_candidates_length(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    second = _production_candidate(
        parameter_candidate_id="candidate_002",
        parameter_values={"SMA_SHORT": 3, "SMA_LONG": 4},
        validation_metrics={
            "trade_count": 4,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 0.5,
        },
    )
    report["candidates"].append(second)
    report["candidate_count"] = 2
    metric_hash = _statistical_metric_hash_for_report(report)
    report["candidate_metric_values_hash"] = metric_hash
    report["candidate_metric_values_summary"] = {
        "candidate_count": 2,
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "primary_metric": "net_excess_return",
        "primary_metric_source": "validation_metrics",
        "benchmark": "cash",
    }
    report["metric_value_count"] = 2
    report["missing_metric_count"] = 0
    report["candidates"][0]["candidate_metric_values_hash"] = metric_hash
    report["candidates"][0]["candidate_metric_values_summary"] = report["candidate_metric_values_summary"]
    report["candidates"][0]["candidate_count"] = 2
    report["candidates"][0]["metric_value_count"] = 2
    report["candidates"][0]["missing_metric_count"] = 0
    _refresh_candidate_profile_hash(report["candidates"][0])
    _rewrite_report(report_path, report)

    with pytest.raises(PromotionGateError, match="statistical_candidate_count_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_incomplete_metric_universe_even_when_p_value_passes(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        report_overrides={
            "candidate_count": 5000,
            "search_budget": 5000,
            "parameter_grid_size": 5000,
        },
        statistical_evidence_overrides={
            "candidate_count": 5000,
            "metric_value_count": 1,
            "missing_metric_count": 4999,
            "candidate_metric_values_summary": {
                "candidate_count": 5000,
                "metric_value_count": 1,
                "missing_metric_count": 4999,
                "primary_metric": "net_excess_return",
                "primary_metric_source": "validation_metrics",
                "benchmark": "cash",
            },
            "effective_trial_count": 40000,
            "statistical_gate_result": "PASS",
            "gate_fail_reasons": [],
        },
    )

    with pytest.raises(PromotionGateError, match="statistical_metric_values_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_research_only_promotion_keeps_explicit_statistical_compatibility(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["gate_result"] == "PASS"
    assert result.artifact["statistical_validation_required"] is False


def test_production_promotion_accepts_complete_next_candle_open_timing_policy(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["gate_result"] == "PASS"
    assert (
        result.artifact["execution_timing_policy"]["min_execution_reality_level_for_promotion"]
        == "candle_next_open"
    )


def test_production_promotion_refuses_missing_execution_timing_min_level(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    timing = _production_safe_execution_timing()
    timing.pop("min_execution_reality_level_for_promotion")
    candidate = _production_candidate(execution_timing_policy=timing)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_min_execution_reality_level_required"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_legacy_default_execution_timing_source(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(execution_timing_policy=_production_safe_execution_timing(source="legacy_default"))
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_legacy_execution_timing_not_promotable"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_same_candle_close_fill(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_timing_policy=_production_safe_execution_timing(allow_same_candle_close_fill=True)
    )
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_same_candle_close_fill_not_allowed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_candle_close_legacy_policy(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_timing_policy=_production_safe_execution_timing(
            fill_reference_policy="candle_close_legacy",
            allow_same_candle_close_fill=True,
            min_execution_reality_level_for_promotion="candle_close_optimistic",
        )
    )
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_execution_reference_price_candle_close_not_promotable"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def _production_orderbook_candidate(**overrides):
    timing = _production_safe_execution_timing(
        fill_reference_policy="first_orderbook_after_decision",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
    )
    execution_contract = build_execution_reality_contract(
        fill_reference_policy="first_orderbook_after_decision",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
        allow_same_candle_close_fill=False,
        top_of_book_required=True,
        top_of_book_is_full_depth=False,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="operator_declared_bithumb_app_fee",
        slippage_source="test_execution_calibration",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
        extra={"quote_evidence_available": True},
    )
    top_summary = {
        "requested": True,
        "required": True,
        "fail_closed": False,
        "gate_status": "PASS",
        "joined_quote_count": 8,
        "missing_quote_count": 0,
        "expected_signal_count": 8,
        "coverage_pct": 100.0,
    }
    payload = _production_candidate(
        execution_timing_policy=timing,
        execution_reality_contract=execution_contract,
        execution_contract_hash=execution_contract["execution_contract_hash"],
        top_of_book_quality_summary=top_summary,
    )
    payload.update(overrides)
    payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(payload))
    return payload


def test_production_promotion_refuses_orderbook_policy_with_min_level_below_policy_reference(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_orderbook_candidate(
        execution_timing_policy=_production_safe_execution_timing(
            fill_reference_policy="first_orderbook_after_decision",
            missing_quote_policy="fail",
            min_execution_reality_level_for_promotion="candle_next_open",
        )
    )
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_execution_reality_level_below_policy_reference"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_latency_orderbook_policy_with_min_level_below_policy_reference(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    execution_contract = build_execution_reality_contract(
        fill_reference_policy="latency_adjusted_orderbook",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
        allow_same_candle_close_fill=False,
        top_of_book_required=True,
        top_of_book_is_full_depth=False,
        latency_model={"type": "fixed_bps", "latency_ms": 100},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="operator_declared_bithumb_app_fee",
        slippage_source="test_execution_calibration",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
        extra={"quote_evidence_available": True},
    )
    candidate = _production_orderbook_candidate(
        execution_timing_policy=_production_safe_execution_timing(
            fill_reference_policy="latency_adjusted_orderbook",
            missing_quote_policy="fail",
            min_execution_reality_level_for_promotion="top_of_book_after_decision",
        ),
        execution_reality_contract=execution_contract,
        execution_contract_hash=execution_contract["execution_contract_hash"],
    )
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_execution_reality_level_below_policy_reference"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_promotion_refuses_orderbook_policy_without_top_of_book_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_orderbook_candidate(top_of_book_quality_summary=None)
    _write_report_with_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_top_of_book_required"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_research_promote_refuses_regenerating_different_artifact_for_bound_validation_run(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_with_lineage(manager, _production_candidate())
    validation_run_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "validation_run.json"
    initial = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        validation_run_path=validation_run_path,
    )
    _bind_validation_run_to_promotion(validation_run_path, initial.artifact, initial.artifact_path)
    initial.artifact_path.unlink()

    with pytest.raises(PromotionGateError) as excinfo:
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            validation_run_path=validation_run_path,
        )

    message = str(excinfo.value)
    assert "validation_run_promotion_already_bound" in message
    assert f"existing_promotion_artifact_hash={initial.content_hash}" in message
    assert f"candidate_output_path={initial.artifact_path.resolve()}" in message
    assert (
        "operator_next_step="
        "use_existing_validation_run_bound_promotion_artifact_or_rerun_research_validate_from_fixed_manifest"
    ) in message
    assert not initial.artifact_path.exists()


def test_research_promote_refuses_existing_validation_run_bound_promotion_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_with_lineage(manager, _production_candidate())
    validation_run_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "validation_run.json"
    initial = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        validation_run_path=validation_run_path,
    )
    before = initial.artifact_path.read_text(encoding="utf-8")
    _bind_validation_run_to_promotion(validation_run_path, initial.artifact, initial.artifact_path)

    with pytest.raises(PromotionGateError, match="validation_run_promotion_already_bound"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            validation_run_path=validation_run_path,
        )

    assert initial.artifact_path.read_text(encoding="utf-8") == before


def test_research_validate_success_real_promotion_artifact_passes_profile_source_verification(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(
        manager,
        candidate,
        report_overrides={
            "promotion_eligibility_gate_result": "PASS",
            "promotion_blocking_reasons": [],
        },
    )
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    manifest = SimpleNamespace(
        experiment_id="promo_exp",
        deployment_tier="paper_candidate",
        acceptance_gate=SimpleNamespace(walk_forward_required=False),
        manifest_hash=lambda: "sha256:manifest",
    )

    monkeypatch.setattr(
        pipeline,
        "build_research_readiness_report",
        lambda **kwargs: {"status": "PASS", "next_actions": []},
    )
    monkeypatch.setattr(pipeline, "run_research_backtest", lambda **kwargs: report)

    payload = pipeline.run_research_validation(
        manifest=manifest,
        db_path=tmp_path / "paper.sqlite",
        manager=manager,
        manifest_path=str(tmp_path / "manifest.json"),
        generated_at="2026-05-04T00:00:00+00:00",
    )

    promotion_path = Path(str(payload["promotion_artifact_path"]))
    promotion = json.loads(promotion_path.read_text(encoding="utf-8"))
    verified = verify_promotion_artifact(promotion)
    profile = build_approved_profile(
        promotion=promotion,
        mode="paper",
        source_promotion_path=str(promotion_path.resolve()),
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-04T00:00:00+00:00",
        manager=manager,
    )

    assert payload["end_to_end_validation_result"] == "PASS"
    assert str(payload["validation_run_binding_hash"]).startswith("sha256:")
    assert payload["promotion_artifact_hash"] == promotion["content_hash"]
    assert promotion["validation_run_binding_status"] == "verified_pre_promotion_binding"
    assert promotion["validation_run_binding_hash"] == payload["validation_run_binding_hash"]
    assert verified["content_hash"] == promotion["content_hash"]
    assert profile["source_promotion_content_hash"] == promotion["content_hash"]


def test_promotion_refuses_missing_lineage_by_default(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())

    with pytest.raises(PromotionGateError, match="promotion refused: lineage_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_invalid_lineage_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    _write_report_with_lineage(manager, candidate)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["lineage"]["lineage_hash"] = "sha256:tampered"
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(report_content_hash_payload(payload))
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="promotion refused: lineage_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_allows_missing_lineage_only_with_explicit_compatibility(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())

    result = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        allow_legacy_lineage=True,
    )

    assert result.artifact["lineage_required"] is False
    assert result.artifact["legacy_compatibility_used"] is True
    assert result.artifact["dataset_quality_legacy_bypass_used"] is True
    assert result.artifact["lineage_hash"] is None
    assert "legacy_lineage_compatibility_used" in result.artifact["promotion_warnings"]
    assert "legacy_dataset_quality_bypass_used" in result.artifact["promotion_warnings"]


def test_legacy_lineage_promotion_records_dataset_quality_bypass_when_quality_evidence_missing(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    for key in ("dataset_quality_hash", "dataset_quality_gate_status", "dataset_quality_gate_reasons", "dataset_quality_reports"):
        payload.pop(key, None)
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(report_content_hash_payload(payload))
    write_json_atomic(report_path, payload)

    result = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        allow_legacy_lineage=True,
    )

    assert result.artifact["dataset_quality_legacy_bypass_used"] is True
    assert "legacy_dataset_quality_bypass_used" in result.artifact["promotion_warnings"]
    assert result.artifact["dataset_quality_hash"] == "sha256:quality"


def test_legacy_lineage_promotion_does_not_bypass_failed_dataset_quality(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(
        manager,
        _candidate(dataset_quality_gate_status="FAIL", dataset_quality_gate_reasons=["missing_candles"]),
    )
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["dataset_quality_gate_status"] = "FAIL"
    payload["dataset_quality_gate_reasons"] = ["dataset_quality_train_missing_candles"]
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(report_content_hash_payload(payload))
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="dataset_quality_train_missing_candles"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            allow_legacy_lineage=True,
        )


def test_reproduce_fails_closed_when_lineage_missing_in_legacy_artifact(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())
    result = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        allow_legacy_lineage=True,
    )

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "lineage_missing"
    assert summary["legacy_compatibility_used"] is True


def test_reproduce_reports_backtest_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = {"experiment_id": "promo_exp", "content_hash": "sha256:drifted", "candidates": []}
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "backtest_report_hash_mismatch"
    assert summary["mismatches"][0]["field"] == "backtest_report_hash"


def test_reproduce_fails_when_statistical_evidence_missing(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    Path(result.artifact["statistical_evidence_path"]).unlink()

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "statistical_evidence_missing"


def test_reproduce_fails_when_statistical_evidence_hash_mismatches(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    evidence_path = Path(result.artifact["statistical_evidence_path"])
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    evidence["white_reality_check_p_value"] = 0.99
    evidence["content_hash"] = sha256_prefixed(
        content_hash_payload({key: value for key, value in evidence.items() if key != "content_hash"})
    )
    write_json_atomic(evidence_path, evidence)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "statistical_evidence_hash_mismatch"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("missing", "experiment_family_universe_missing"),
        ("row_hash_mismatch", "experiment_family_registry_row_hash_mismatch"),
        ("statistical_evidence_hash_mismatch", "experiment_family_registry_statistical_evidence_hash_mismatch"),
        ("prior_hash_mismatch", "experiment_family_registry_prior_hash_mismatch"),
        ("return_panel_hash_mismatch", "experiment_family_registry_return_panel_hash_mismatch"),
        ("stale_experiment_id", "experiment_family_registry_stale"),
        ("stale_manifest_hash", "experiment_family_registry_stale"),
        ("stale_dataset_hash", "experiment_family_registry_stale"),
        ("stale_attempt_index", "experiment_family_registry_stale"),
        ("stale_holdout_reuse_count", "experiment_family_registry_stale"),
    ],
)
def test_reproduce_validates_family_registry_binding(tmp_path, monkeypatch, mutation, reason) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    registry_path = Path(result.artifact["family_trial_registry_path"])
    if mutation == "missing":
        registry_path.unlink()
    else:
        row = json.loads(registry_path.read_text(encoding="utf-8").strip())
        if mutation == "row_hash_mismatch":
            row["result_status"] = "TAMPERED"
        elif mutation == "statistical_evidence_hash_mismatch":
            row["statistical_evidence_hash"] = "sha256:wrong"
        elif mutation == "prior_hash_mismatch":
            row["prior_registry_hash"] = "sha256:wrong"
        elif mutation == "return_panel_hash_mismatch":
            row["return_panel_hash"] = "sha256:wrong"
        elif mutation == "stale_experiment_id":
            row["experiment_id"] = "old_exp"
        elif mutation == "stale_manifest_hash":
            row["manifest_hash"] = "sha256:old-manifest"
        elif mutation == "stale_dataset_hash":
            row["dataset_content_hash"] = "sha256:old-dataset"
        elif mutation == "stale_attempt_index":
            row["attempt_index"] = 999
        elif mutation == "stale_holdout_reuse_count":
            row["holdout_reuse_count"] = 999
        registry_path.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert any(item["reason"] == reason for item in summary["mismatches"]) or summary["reason"] == reason


def test_reproduce_fails_when_required_final_holdout_stress_removed(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    promotion = json.loads(result.artifact_path.read_text(encoding="utf-8"))
    promotion["final_holdout_stress_suite"] = None
    _rewrite_promotion_artifact(result.artifact_path, promotion)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "final_holdout_stress_suite_required_but_missing"


def test_reproduce_fails_when_required_final_holdout_stress_hash_mismatches(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    promotion = json.loads(result.artifact_path.read_text(encoding="utf-8"))
    promotion["final_holdout_stress_suite"]["stress_suite_hash"] = "sha256:tampered"
    _rewrite_promotion_artifact(result.artifact_path, promotion)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "final_holdout_stress_suite_hash_mismatch"


@pytest.mark.parametrize("field_action", ["missing", "false"])
def test_reproduce_requires_stress_suite_for_production_bound_artifact_even_without_flag(
    tmp_path,
    monkeypatch,
    field_action,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    promotion = json.loads(result.artifact_path.read_text(encoding="utf-8"))
    if field_action == "missing":
        promotion.pop("stress_suite_required", None)
    else:
        promotion["stress_suite_required"] = False
    promotion["validation_stress_suite"] = None
    promotion["final_holdout_stress_suite"] = None
    _rewrite_promotion_artifact(result.artifact_path, promotion)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "stress_suite_required_but_missing"


@pytest.mark.parametrize("field_action", ["missing", "false"])
def test_reproduce_accepts_complete_production_bound_stress_suite_even_without_flag(
    tmp_path,
    monkeypatch,
    field_action,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    promotion = json.loads(result.artifact_path.read_text(encoding="utf-8"))
    if field_action == "missing":
        promotion.pop("stress_suite_required", None)
    else:
        promotion["stress_suite_required"] = False
    _rewrite_promotion_artifact(result.artifact_path, promotion)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is True


def test_reproduce_fails_when_promotion_report_stress_contract_drifts(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _attach_stress_suite(candidate)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["candidates"][0]["stress_suite_contract_hash"] = "sha256:drifted"
    _refresh_candidate_profile_hash(report["candidates"][0])
    _rewrite_report(report_path, report)
    promotion = json.loads(result.artifact_path.read_text(encoding="utf-8"))
    promotion["backtest_report_hash"] = _canonical_report_hash(report)
    promotion["lineage"]["backtest_report_hash"] = _canonical_report_hash(report)
    promotion["lineage"].pop("lineage_hash", None)
    promotion["lineage"]["lineage_hash"] = compute_lineage_hash(promotion["lineage"])
    promotion["lineage_hash"] = promotion["lineage"]["lineage_hash"]
    _rewrite_promotion_artifact(result.artifact_path, promotion)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "stress_suite_contract_mismatch"


@pytest.mark.parametrize("field_action", ["false", "missing"])
def test_reproduce_requires_statistical_evidence_for_production_bound_artifact_even_without_flag(
    tmp_path,
    monkeypatch,
    field_action,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    Path(result.artifact["statistical_evidence_path"]).unlink()
    promotion = json.loads(Path(result.artifact_path).read_text(encoding="utf-8"))
    if field_action == "false":
        promotion["statistical_validation_required"] = False
    else:
        promotion.pop("statistical_validation_required", None)
    promotion.pop("content_hash", None)
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    write_json_atomic(result.artifact_path, promotion)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "statistical_evidence_missing"


def test_reproduce_fails_when_candidate_metric_values_hash_mismatches(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    evidence_path = Path(result.artifact["statistical_evidence_path"])
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    evidence["candidate_metric_values_hash"] = "sha256:tampered-metric-values"
    evidence["content_hash"] = sha256_prefixed(
        content_hash_payload({key: value for key, value in evidence.items() if key != "content_hash"})
    )
    write_json_atomic(evidence_path, evidence)

    promotion = json.loads(Path(result.artifact_path).read_text(encoding="utf-8"))
    promotion["statistical_evidence_hash"] = evidence["content_hash"]
    promotion["lineage"]["statistical_evidence_hash"] = evidence["content_hash"]
    promotion["lineage"].pop("lineage_hash", None)
    promotion["lineage"]["lineage_hash"] = compute_lineage_hash(promotion["lineage"])
    promotion["lineage_hash"] = promotion["lineage"]["lineage_hash"]
    promotion.pop("content_hash", None)
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    write_json_atomic(result.artifact_path, promotion)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "candidate_metric_values_hash_mismatch"


def test_reproduce_refuses_stale_candidate_metric_values_hash_after_backtest_report_metric_change(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["candidates"][0]["validation_metrics"]["return_pct"] = 2.0
    _refresh_candidate_profile_hash(report["candidates"][0])
    _rewrite_report(report_path, report)
    _rewrite_promotion_with_backtest_hash(result.artifact_path, report["content_hash"])

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "candidate_metric_values_hash_recompute_mismatch"


def test_reproduce_refuses_backtest_report_candidate_count_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    second = _production_candidate(
        parameter_candidate_id="candidate_002",
        parameter_values={"SMA_SHORT": 3, "SMA_LONG": 4},
    )
    report["candidates"].append(second)
    report["candidate_count"] = 1
    _rewrite_report(report_path, report)
    _rewrite_promotion_with_backtest_hash(result.artifact_path, report["content_hash"])

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "statistical_candidate_count_mismatch"


def test_reproduce_recomputes_backtest_hash_when_body_tampered_but_embedded_hash_unchanged(
    tmp_path, monkeypatch
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["dataset_content_hash"] = "sha256:tampered_dataset"
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "backtest_report_hash_mismatch"
    assert summary["mismatches"][0]["expected"] == result.artifact["backtest_report_hash"]
    assert summary["mismatches"][0]["actual"] != payload["content_hash"]


def test_reproduce_fails_when_backtest_embedded_content_hash_tampered_only(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["content_hash"] = "sha256:tampered_embedded_hash"
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "backtest_report_embedded_content_hash_mismatch"


def test_reproduce_recomputes_walk_forward_hash_when_body_tampered_but_embedded_hash_unchanged(
    tmp_path, monkeypatch
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=True)
    _write_report_with_lineage(manager, candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(candidate))
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["manifest_hash"] = "sha256:tampered_manifest"
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "walk_forward_report_hash_mismatch"


def test_reproduce_reports_lineage_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    artifact = dict(result.artifact)
    artifact["lineage"] = dict(artifact["lineage"])
    artifact["lineage"]["holdout_reuse_count"] = 99
    artifact.pop("content_hash")
    artifact["content_hash"] = sha256_prefixed(content_hash_payload(artifact))
    write_json_atomic(result.artifact_path, artifact)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "lineage_hash_mismatch"


def test_reproduce_reports_manifest_and_dataset_mismatches(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    artifact = dict(result.artifact)
    artifact["manifest_hash"] = "sha256:other_manifest"
    artifact["dataset_content_hash"] = "sha256:other_dataset"
    artifact.pop("content_hash")
    artifact["content_hash"] = sha256_prefixed(content_hash_payload(artifact))
    write_json_atomic(result.artifact_path, artifact)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert {item["reason"] for item in summary["mismatches"]} >= {
        "manifest_hash_mismatch",
        "dataset_content_hash_mismatch",
    }


def test_reproduce_reports_command_args_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    artifact = dict(result.artifact)
    artifact["command_args_hash_expected"] = "sha256:other_args"
    artifact.pop("content_hash")
    artifact["content_hash"] = sha256_prefixed(content_hash_payload(artifact))
    write_json_atomic(result.artifact_path, artifact)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "command_args_hash_mismatch"


def test_reproduce_reports_walk_forward_required_but_missing(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=True)
    _write_report_with_lineage(manager, candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(candidate))
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    walk_path.unlink()

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "walk_forward_required_but_missing"


def test_promotion_refuses_old_candidate_without_regime_policy(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    for key in (
        "regime_classifier_version",
        "allowed_live_regimes",
        "blocked_live_regimes",
        "regime_evidence",
        "regime_gate_result",
    ):
        candidate.pop(key, None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="regime_policy_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_execution_calibration_breach(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_model={
            "type": "stress",
            "fee_rate": 0.0,
            "slippage_bps": 5.0,
            "latency_ms": 100,
            "model_params_hash": "sha256:model",
        },
        execution_calibration_required=True,
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_slippage_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_calibration_p95_slippage_exceeds_assumption"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_candle_close_execution_for_live_ready_candidate(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_timing_policy={
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "candle_close_legacy",
            "quote_selection": "first_after_or_equal",
            "max_quote_wait_ms": 3000,
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": True,
            "source": "legacy_default",
        },
        execution_reality_summary={
            "signal_event_count": 4,
            "fillable_signal_event_count": 4,
            "missing_quote_on_signal_count": 0,
            "quote_after_decision_coverage_pct": None,
            "median_quote_age_ms_on_signal": None,
            "p95_quote_age_ms_on_signal": None,
            "execution_reference_policy": "candle_close_legacy",
            "execution_reality_level": "candle_close_optimistic",
            "execution_reality_gate_status": "PASS",
            "execution_reality_gate_reasons": [],
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_reference_price_candle_close_not_promotable"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_allows_optional_warn_calibration_breach(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_content_hash_missing"],
            "artifact_hash": None,
        },
    )
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["gate_result"] == "PASS"
    assert result.artifact["has_execution_calibration_warning"] is True
    assert result.artifact["execution_calibration_warning_reasons"] == ["execution_calibration_content_hash_missing"]
    assert "execution_calibration_content_hash_missing" in result.artifact["promotion_warnings"]


def test_promotion_artifact_records_empty_calibration_warning_fields_when_no_breach(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["gate_result"] == "PASS"
    assert result.artifact["has_execution_calibration_warning"] is False
    assert result.artifact["execution_calibration_warning_reasons"] == []
    assert result.artifact["promotion_warnings"] == []


def test_promotion_cli_prints_optional_warn_calibration_breach(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_slippage_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 0
    assert "  has_execution_calibration_warning=1" in output
    assert (
        "  execution_calibration_warning_reasons="
        "execution_calibration_p95_slippage_exceeds_assumption"
    ) in output
    assert "  promotion_warnings=execution_calibration_p95_slippage_exceeds_assumption" in output


def test_promotion_cli_prints_empty_warning_fields_when_no_breach(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 0
    assert "  has_execution_calibration_warning=0" in output
    assert "  execution_calibration_warning_reasons=none" in output
    assert "  promotion_warnings=none" in output


def test_promotion_cli_prints_execution_event_summary(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    _write_report(manager, _candidate())

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 0
    assert (
        "  execution_event_summary="
        "execution_attempt_count=8 "
        "execution_filled_count=8 "
        "portfolio_applied_trade_count=8 "
        "pending_execution_count=0 "
        "pending_execution_after_dataset_end_count=0 "
        "skipped_execution_count=0 "
        "failed_execution_count=0 "
        "closed_trade_count=4 "
        "execution_event_timeline_incomplete=False"
    ) in output


def test_promotion_cli_prints_stress_suite_summary(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    candidate = _candidate()
    _attach_stress_suite(candidate)
    _write_report(manager, candidate)

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 0
    assert "  stress_suite_required=1" in output
    assert "  stress_suite_gate_result=PASS" in output
    assert "  stress_suite_fail_reasons=none" in output
    assert "  stress_trade_removal_status=PASS" in output
    assert "  stress_monte_carlo_survival_probability=1.0" in output
    assert "  stress_monte_carlo_max_drawdown_pct_p95=10.0" in output


def test_promotion_cli_refuses_required_calibration_failure_without_success_block(
    tmp_path, monkeypatch, capsys
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    candidate = _candidate(
        execution_calibration_required=True,
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_slippage_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 1
    assert "[RESEARCH-PROMOTE-CANDIDATE] error=promotion refused:" in output
    assert "execution_calibration_p95_slippage_exceeds_assumption" in output
    assert "  gate_result=PASS" not in output
    assert "  artifact_path=" not in output
    assert "  has_execution_calibration_warning=" not in output


def test_promotion_cli_refuses_missing_lineage_without_flag(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    _write_report_without_lineage(manager, _candidate())

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 1
    assert "promotion refused: lineage_missing" in output


def test_hypothesis_identity_source_is_in_lineage_promotion_profile_and_candidate_profile() -> None:
    lineage = build_research_lineage(
        experiment_id="promo_exp",
        manifest_hash="sha256:manifest",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        hypothesis_identity_source="manifest.hypothesis",
        experiment_family_identity_source="experiment_id",
    )
    candidate = {
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_001",
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "experiment_family_id": "family",
        "hypothesis_id": "hypothesis",
        "hypothesis_status": "pre_registered",
        "hypothesis_identity_source": "manifest.hypothesis",
        "experiment_family_identity_source": "experiment_id",
    }

    profile = build_candidate_profile(candidate)

    assert lineage["hypothesis_identity_source"] == "manifest.hypothesis"
    assert profile["hypothesis_identity_source"] == "manifest.hypothesis"
    assert profile["experiment_family_identity_source"] == "experiment_id"


def test_promotion_cli_allows_legacy_lineage_with_explicit_flag(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    _write_report_without_lineage(manager, _candidate())

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        allow_legacy_lineage=True,
    )

    output = capsys.readouterr().out
    assert status == 0
    assert "legacy_lineage_compatibility_used" in output
    assert "  legacy_compatibility_used=1" in output
    assert "  dataset_quality_legacy_bypass_used=1" in output


def test_allow_legacy_lineage_does_not_bypass_execution_reality_gate(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_timing_policy={
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "candle_close_legacy",
            "quote_selection": "first_after_or_equal",
            "max_quote_wait_ms": 3000,
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": True,
            "source": "legacy_default",
        },
        execution_reality_summary={
            "signal_event_count": 4,
            "fillable_signal_event_count": 4,
            "missing_quote_on_signal_count": 0,
            "quote_after_decision_coverage_pct": None,
            "median_quote_age_ms_on_signal": None,
            "p95_quote_age_ms_on_signal": None,
            "execution_reference_policy": "candle_close_legacy",
            "execution_reality_level": "candle_close_optimistic",
            "execution_reality_gate_status": "FAIL",
            "execution_reality_gate_reasons": ["execution_reference_price_candle_close_not_promotable"],
        },
    )
    _write_report_without_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_reference_price_candle_close_not_promotable"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            allow_legacy_lineage=True,
        )


def test_allow_legacy_lineage_does_not_bypass_pending_execution_refusal(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    summary = {
        "execution_attempt_count": 1,
        "execution_filled_count": 1,
        "filled_execution_count": 1,
        "portfolio_applied_trade_count": 0,
        "pending_execution_count": 1,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 0,
        "pending_execution_at_end_count": 1,
        "pending_execution_after_dataset_end_count": 1,
        "execution_event_timeline_incomplete": True,
    }
    candidate = _candidate(
        acceptance_gate_result="PASS",
        validation_metrics={
            "trade_count": 0,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        execution_event_summary=summary,
    )
    _write_report_without_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="pending_execution_after_dataset_end"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            allow_legacy_lineage=True,
        )


def test_promotion_uses_portfolio_applied_trade_count_not_execution_filled_count(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    summary = {
        "execution_attempt_count": 2,
        "execution_filled_count": 2,
        "filled_execution_count": 2,
        "portfolio_applied_trade_count": 0,
        "pending_execution_count": 2,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 0,
        "pending_execution_at_end_count": 0,
        "pending_execution_after_dataset_end_count": 0,
        "execution_event_timeline_incomplete": False,
    }
    candidate = _candidate(
        validation_metrics={
            "trade_count": 0,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        execution_event_summary=summary,
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="portfolio_applied_trade_count_insufficient"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_execution_event_timeline_incomplete(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    summary = {
        "execution_attempt_count": 1,
        "execution_filled_count": 1,
        "filled_execution_count": 1,
        "portfolio_applied_trade_count": 0,
        "pending_execution_count": 1,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 0,
        "pending_execution_at_end_count": 1,
        "pending_execution_after_dataset_end_count": 1,
        "execution_event_timeline_incomplete": True,
    }
    candidate = _candidate(
        validation_metrics={
            "trade_count": 0,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        execution_event_summary=summary,
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_event_timeline_incomplete"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_cli_argument_wires_allow_legacy_lineage(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_promote(**kwargs) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(app_module, "cmd_research_promote_candidate", fake_promote)

    status = app_module.main(
        [
            "research-promote-candidate",
            "--experiment-id",
            "promo_exp",
            "--candidate-id",
            "candidate_001",
            "--allow-legacy-lineage",
        ]
    )

    assert status == 0
    assert captured["allow_legacy_lineage"] is True


def test_candidate_profile_hash_changes_when_calibration_warning_evidence_changes() -> None:
    clean = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={"status": "PASS", "reasons": [], "artifact_hash": "sha256:calibration"},
    )
    warned = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_latency_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )

    assert clean["candidate_profile_hash"] != warned["candidate_profile_hash"]


def test_production_bound_candidate_refuses_warn_mode_calibration(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_quality_gate_not_passed"],
            "artifact_hash": "sha256:calibration",
            "scenario_gates": [
                {
                    "status": "FAIL",
                    "reasons": ["execution_calibration_quality_gate_not_passed"],
                    "artifact_hash": "sha256:calibration",
                    "content_hash_present": True,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "FAIL",
                }
            ],
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_execution_calibration_required"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_bound_candidate_refuses_hashless_calibration(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_calibration_artifact_hash=None,
        execution_calibration_artifact_hashes=[],
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "reasons": [],
                    "content_hash_present": False,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "PASS",
                }
            ],
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_execution_calibration_hash_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_bound_promotion_binds_calibration_hash_into_lineage_and_reproduces(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    summary = reproduce_promotion(result.artifact_path).summary

    assert result.artifact["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["lineage"]["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["production_calibration_policy_result"]["status"] == "PASS"
    assert summary["ok"] is True
    assert summary["execution_calibration_artifact_hash"] == "sha256:calibration"


def test_reproduce_rejects_final_selection_lineage_hash_drift(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    path = result.artifact_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["lineage"]["candidate_final_scores_hash"] == payload["candidate_final_scores_hash"]

    payload["lineage"]["candidate_final_scores_hash"] = "sha256:" + "0" * 64
    payload["lineage"].pop("lineage_hash", None)
    payload["lineage"]["lineage_hash"] = compute_lineage_hash(payload["lineage"])
    payload["lineage_hash"] = payload["lineage"]["lineage_hash"]
    payload["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in payload.items() if k != "content_hash"}))
    write_json_atomic(path, payload)

    summary = reproduce_promotion(path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "final_selection_score_hash_mismatch"


def test_production_bound_promotion_binds_calibration_hash_when_base_lineage_lacks_it(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate, lineage_calibration_hash=None)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    summary = reproduce_promotion(result.artifact_path).summary

    assert result.artifact["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["lineage"]["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["lineage_hash"].startswith("sha256:")
    assert summary["ok"] is True


def test_production_bound_promotion_refuses_stale_base_lineage_calibration_hash_before_write(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_calibration_artifact_hash="sha256:calibration-correct",
        execution_calibration_artifact_hashes=["sha256:calibration-correct"],
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration-correct",
            "artifact_hashes": ["sha256:calibration-correct"],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "reasons": [],
                    "artifact_hash": "sha256:calibration-correct",
                    "content_hash_present": True,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "expected_fill_reference_policy": "next_candle_open",
                    "artifact_fill_reference_policy": "next_candle_open",
                    "artifact_execution_reality_level": "candle_next_open",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "PASS",
                }
            ],
        },
        production_calibration_policy_result={
            "target": "paper_candidate",
            "production_bound": True,
            "required": True,
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration-correct",
            "artifact_hashes": ["sha256:calibration-correct"],
            "policy_source": "repo_production_calibration_policy_v1",
            "operator_next_step": "none",
        },
        candidate_profile_hash=None,
    )
    _write_report_with_lineage(
        manager,
        candidate,
        lineage_calibration_hash="sha256:calibration-stale",
    )
    artifact_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json"

    with pytest.raises(PromotionGateError, match="lineage_execution_calibration_artifact_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not artifact_path.exists()


def test_reproduce_fails_when_required_calibration_hash_drifts(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    path = result.artifact_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["execution_calibration_artifact_hash"] = "sha256:tampered"
    payload["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in payload.items() if k != "content_hash"}))
    write_json_atomic(path, payload)

    summary = reproduce_promotion(path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "calibration_hash_mismatch"


def test_reproduce_fails_when_required_lineage_calibration_hash_missing(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    path = result.artifact_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["lineage"]["execution_calibration_artifact_hash"] = None
    payload["lineage"].pop("lineage_hash", None)
    payload["lineage"]["lineage_hash"] = compute_lineage_hash(payload["lineage"])
    payload["lineage_hash"] = payload["lineage"]["lineage_hash"]
    payload["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in payload.items() if k != "content_hash"}))
    write_json_atomic(path, payload)

    summary = reproduce_promotion(path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "calibration_hash_missing"


def test_candidate_profile_hash_changes_when_pending_execution_summary_changes() -> None:
    complete = _candidate()
    pending_summary = {
        "execution_attempt_count": 8,
        "execution_filled_count": 8,
        "filled_execution_count": 8,
        "portfolio_applied_trade_count": 7,
        "pending_execution_count": 1,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 4,
        "pending_execution_at_end_count": 1,
        "pending_execution_after_dataset_end_count": 1,
        "execution_event_timeline_incomplete": True,
    }
    pending = _candidate(execution_event_summary=pending_summary)

    assert complete["candidate_profile_hash"] != pending["candidate_profile_hash"]


def test_promotion_refuses_missing_final_holdout_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(final_holdout_present=False, final_holdout_metrics=None)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="final_holdout_evidence_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_failed_required_scenario_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    scenario_results = [
        {
            "scenario_id": "scenario_001_fixed_bps_base",
            "scenario_role": "base",
            "scenario_acceptance_gate_result": "PASS",
            "scenario_fail_reasons": [],
        },
        {
            "scenario_id": "scenario_002_fixed_bps_stress",
            "scenario_role": "stress",
            "scenario_acceptance_gate_result": "FAIL",
            "scenario_fail_reasons": ["profit_factor_failed"],
        },
    ]
    candidate = _candidate(
        acceptance_gate_result="FAIL",
        gate_fail_reasons=["scenario_policy_required_scenario_failed:scenario_002_fixed_bps_stress:profit_factor_failed"],
        scenario_policy="must_pass_base_and_survive_stress",
        scenario_pass_count=1,
        scenario_fail_count=1,
        required_scenario_count=2,
        scenario_results=scenario_results,
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="scenario_policy_required_scenario_failed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_candidate_profile_hash_changes_when_final_holdout_evidence_changes() -> None:
    first = _candidate(final_holdout_metrics={"trade_count": 4, "return_pct": 1.0})
    second = _candidate(final_holdout_metrics={"trade_count": 4, "return_pct": 2.0})

    assert first["candidate_profile_hash"] != second["candidate_profile_hash"]
