from __future__ import annotations

import copy
import json

import pytest

from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.research.experiment_registry import (
    EMPTY_EXPERIMENT_REGISTRY_HASH,
    FINAL_HOLDOUT_REUSE_KEY_SCHEMA_VERSION,
    compute_row_hash,
    validate_experiment_registry_binding,
)
from bithumb_bot.research.final_selection import apply_final_selection_contract, validate_final_selection_report
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.run_summary import build_research_run_summary
from bithumb_bot.research.statistical_selection import candidate_metric_universe_payload


def _final_selection(ranking: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "schema_version": 1,
        "required_for_promotion": True,
        "candidate_universe": "acceptance_gate_passed_required_scenarios",
        "must_pass": {
            "dataset_quality_gate_status": "PASS",
            "statistical_gate_result": "PASS",
            "stress_suite_gate_result": "PASS",
            "production_calibration_policy_result": "PASS",
            "metrics_schema_version": 2,
            "final_holdout_present": True,
        },
        "selection_exposure_policy": {
            "final_holdout_usage": "confirmatory_metric_in_rank",
            "counts_as_holdout_reuse": True,
        },
        "method": "lexicographic",
        "null_metric_policy": "fail_if_required_else_worst_rank",
        "ranking": ranking
        or [
            {
                "metric": "final_holdout.metrics_v2.trade_quality.expectancy_per_trade_krw",
                "order": "desc",
                "required": True,
            },
            {
                "metric": "final_holdout.metrics_v2.return_risk.max_drawdown_pct",
                "order": "asc",
                "required": True,
            },
            {
                "metric": "validation.stress.risk_adjusted_score.calmar_ratio",
                "order": "desc",
                "required": True,
            },
            {
                "metric": "final_holdout.benchmark.excess_return_vs_buy_and_hold_pct",
                "order": "desc",
                "required": True,
            },
            {"metric": "parameter_candidate_id", "order": "asc", "required": True},
        ],
        "unsupported_metric_policy": {
            "sharpe_ratio": "fail_if_required",
            "sortino_ratio": "fail_if_required",
        },
    }


def _candidate(
    candidate_id: str,
    *,
    validation_expectancy: float = 100.0,
    final_expectancy: float = 100.0,
    final_mdd: float = 5.0,
    calmar: float = 1.0,
    buy_hold_excess: float = 0.0,
) -> dict[str, object]:
    return {
        "parameter_candidate_id": candidate_id,
        "acceptance_gate_result": "PASS",
        "aggregate_acceptance_gate_result": "PASS",
        "primary_metric_source": "primary_base_scenario_alias",
        "primary_metric_source_semantics": "primary_base_scenario_alias",
        "primary_metric_scenario_role": "base",
        "primary_metric_scenario_id": "scenario_base",
        "aggregate_gate_source": "required_scenario_policy",
        "metrics_schema_version": 2,
        "metrics_v2_source": "computed",
        "candidate_failed_before_complete_metrics": False,
        "evaluation_status": "completed",
        "metrics_status": "complete",
        "final_holdout_present": True,
        "statistical_gate_result": "PASS",
        "stress_suite_gate_result": "PASS",
        "production_calibration_policy_result": {"status": "PASS"},
        "validation_metrics": {
            "return_pct": validation_expectancy,
            "benchmark_buy_and_hold_return_pct": 0.0,
        },
        "validation_metrics_v2": {
            "trade_quality": {
                "expectancy_per_trade_krw": validation_expectancy,
                "single_trade_dependency_score": 0.1,
            },
            "return_risk": {"max_drawdown_pct": 10.0, "cagr_pct": validation_expectancy},
            "cost_execution": {"fee_drag_ratio": 0.1, "slippage_drag_ratio": 0.1},
        },
        "final_holdout_metrics_v2": {
            "trade_quality": {"expectancy_per_trade_krw": final_expectancy},
            "return_risk": {"max_drawdown_pct": final_mdd},
        },
        "validation_stress_suite": {
            "risk_adjusted_score": {"calmar_ratio": calmar},
            "trade_order_monte_carlo": {"survival_probability": 1.0},
        },
        "benchmark_metrics": {
            "final_holdout": {"excess_return_vs_buy_and_hold_pct": buy_hold_excess},
            "validation": {"excess_return_vs_buy_and_hold_pct": validation_expectancy},
        },
    }


def _context() -> dict[str, object]:
    return {"dataset_quality_gate_status": "PASS", "statistical_gate_result": "PASS"}


def test_final_selection_rejects_fallback_metrics_even_when_gate_is_pass() -> None:
    candidate = _candidate("candidate_001")
    candidate["candidate_failed_before_complete_metrics"] = True
    candidate["metrics_status"] = "unavailable"
    candidate["metrics_v2_source"] = "failure_fallback"
    candidate["validation_metrics_v2"] = {
        **candidate["validation_metrics_v2"],
        "metrics_status": "unavailable",
        "metrics_v2_source": "failure_fallback",
        "candidate_failed_before_complete_metrics": True,
    }
    candidate["final_holdout_metrics_v2"] = {
        **candidate["final_holdout_metrics_v2"],
        "metrics_status": "unavailable",
        "metrics_v2_source": "failure_fallback",
        "candidate_failed_before_complete_metrics": True,
    }

    result = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[candidate],
        report_context=_context(),
        production_bound=True,
    )

    assert result["gate_result"] == "FAIL"
    assert result["selected_candidate_id"] is None
    reasons = result["candidate_final_scores"][0]["eligibility_reasons"]
    assert "final_selection_candidate_failed_before_complete_metrics" in reasons
    assert "final_selection_metrics_unavailable" in reasons
    assert "final_selection_metrics_failure_fallback" in reasons


def test_final_selection_computed_only_excludes_failure_fallback_placeholder_metrics_by_default() -> None:
    fallback = _candidate("candidate_fallback", final_expectancy=0.0)
    fallback["candidate_failed_before_complete_metrics"] = True
    fallback["evaluation_status"] = "resource_limited"
    fallback["metrics_status"] = "unavailable"
    fallback["metrics_v2_source"] = "failure_fallback"
    fallback["final_holdout_metrics_v2"] = {
        "metrics_status": "unavailable",
        "metrics_v2_source": "failure_fallback",
        "candidate_failed_before_complete_metrics": True,
        "trade_quality": {"expectancy_per_trade_krw": 0.0},
        "return_risk": {"max_drawdown_pct": 0.0},
    }
    computed = _candidate("candidate_computed", final_expectancy=-1.5, final_mdd=25.0)

    result = apply_final_selection_contract(
        contract=_final_selection(
            ranking=[
                {
                    "metric": "final_holdout.metrics_v2.trade_quality.expectancy_per_trade_krw",
                    "order": "desc",
                    "required": True,
                },
                {"metric": "parameter_candidate_id", "order": "asc", "required": True},
            ]
        ),
        candidates=[fallback, computed],
        report_context=_context(),
        production_bound=True,
    )

    assert result["gate_result"] == "PASS"
    assert result["selected_candidate_id"] == "candidate_computed"
    fallback_score = next(
        item for item in result["candidate_final_scores"] if item["candidate_id"] == "candidate_fallback"
    )
    assert fallback_score["eligible"] is False
    assert "final_selection_candidate_not_computed_complete" in fallback_score["eligibility_reasons"]


def test_final_selection_rejects_candidate_without_metric_source_semantics() -> None:
    candidate = _candidate("candidate_001")
    for key in (
        "primary_metric_source_semantics",
        "primary_metric_scenario_role",
        "aggregate_gate_source",
    ):
        candidate.pop(key)

    result = apply_final_selection_contract(
        contract=_final_selection(
            ranking=[
                {
                    "metric": "validation.metrics_v2.trade_quality.expectancy_per_trade_krw",
                    "order": "desc",
                    "required": True,
                }
            ]
        ),
        candidates=[candidate],
        report_context=_context(),
        production_bound=True,
    )

    assert result["gate_result"] == "FAIL"
    reasons = result["candidate_final_scores"][0]["eligibility_reasons"]
    assert "final_selection_primary_metric_source_semantics_missing" in reasons
    assert "final_selection_primary_metric_scenario_role_missing" in reasons


def _risk_policy() -> dict[str, object]:
    return {
        "schema_version": 1,
        "max_daily_loss_krw": 50_000.0,
        "max_daily_order_count": 20,
        "max_position_loss_pct": 5.0,
        "kill_switch": False,
        "source": "manifest",
    }


def _manifest_payload() -> dict[str, object]:
    return {
        "experiment_id": "selection_contract_v1",
        "hypothesis": "selection contract is explicit",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "deployment_tier": "paper_candidate",
        "risk_policy": _risk_policy(),
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "candles_v1",
            "top_of_book": {
                "source": "sqlite_orderbook_top_snapshots",
                "required": False,
                "join_tolerance_ms": 3000,
                "missing_policy": "warn",
            },
            "train": {"start": "2024-01-01", "end": "2024-01-02"},
            "validation": {"start": "2024-01-03", "end": "2024-01-04"},
            "final_holdout": {"start": "2024-01-05", "end": "2024-01-06"},
        },
        "parameter_space": {
            "SMA_SHORT": [2],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
            "SMA_FILTER_VOL_WINDOW": [10],
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
            "SMA_FILTER_OVEREXT_LOOKBACK": [3],
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": [0.02],
            "SMA_MARKET_REGIME_ENABLED": [True],
            "SMA_COST_EDGE_ENABLED": [True],
            "SMA_COST_EDGE_MIN_RATIO": [0.0],
            "ENTRY_EDGE_BUFFER_RATIO": [0.0005],
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO": [0.0],
            "STRATEGY_ENTRY_SLIPPAGE_BPS": [0.0],
            "LIVE_FEE_RATE_ESTIMATE": [0.001],
            "STRATEGY_EXIT_RULES": ["opposite_cross,max_holding_time"],
            "STRATEGY_EXIT_STOP_LOSS_RATIO": [0.0],
            "STRATEGY_EXIT_MAX_HOLDING_MIN": [0],
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": [0.0],
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
        "portfolio_policy": {
            "schema_version": 1,
            "starting_cash_krw": 1_000_000.0,
            "quote_currency": "KRW",
            "initial_position_qty": 0.0,
            "cash_interest_policy": "zero",
            "position_sizing": {
                "type": "fractional_cash",
                "buy_fraction": 0.99,
                "sell_policy": "sell_all_available_position",
                "cash_buffer_policy": "retain_1_percent_before_fees",
                "min_order_krw": None,
                "max_order_krw": None,
                "rounding_policy": "engine_float_no_exchange_lot_rounding",
            },
            "source": "manifest",
        },
        "execution_model": {
            "scenario_policy": "single_scenario",
            "scenarios": [
                {
                    "scenario_role": "base",
                    "label": "base",
                    "fee_rate": 0.001,
                    "fee_source": "operator_declared_bithumb_app_fee",
                    "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
                    "slippage_bps": 1,
                    "slippage_source": "execution_calibration",
                    "promotable_as_base": True,
                    "type": "fixed_bps",
                }
            ],
            "calibration_required": True,
            "calibration_strictness": "fail",
        },
        "execution_timing": {
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "fill_reference_policy": "next_candle_open",
            "quote_selection": "first_after_or_equal",
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "candle_next_open",
        },
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 50,
            "min_profit_factor": 1.0,
            "oos_return_must_be_positive": True,
            "parameter_stability_required": False,
            "metrics_contract_required": True,
        },
        "statistical_validation": {
            "required_for_promotion": True,
            "benchmark": "cash",
            "primary_metric": "net_excess_return",
            "selection_universe": "all_parameter_candidates_all_required_scenarios",
            "multiple_testing_scope": "experiment",
            "bootstrap": {
                "method": "metric_centered_max_bootstrap",
                "n_bootstrap": 100,
                "block_length_policy": "not_applicable_summary_metric",
                "seed_policy": "derived_from_selection_universe_hash",
            },
            "gates": {
                "max_reality_check_p_value": 0.05,
                "max_spa_p_value": None,
                "min_deflated_sharpe_probability": None,
                "max_holdout_reuse_count": 0,
                "max_attempt_index_without_new_hypothesis": 1,
            },
        },
        "stress_suite": {
            "required_for_promotion": True,
            "trade_order_monte_carlo": {
                "iterations": 100,
                "seed_policy": "derived_from_manifest_candidate_scenario_split_hash",
                "min_survival_probability": 0.95,
                "ruin_max_drawdown_pct": 35,
                "min_closed_trades": 1,
            },
            "risk_adjusted_score": {
                "required_metrics": ["calmar"],
                "ranking": ["max_calmar"],
            },
        },
        "final_selection": _final_selection(),
    }


def test_final_selection_contract_parse_strict_unknown_fields() -> None:
    payload = _manifest_payload()
    payload["final_selection"] = {**_final_selection(), "extra": True}
    with pytest.raises(ManifestValidationError, match="final_selection unsupported fields"):
        parse_manifest(payload)


def test_final_selection_must_pass_rejects_unknown_key() -> None:
    payload = _manifest_payload()
    final_selection = copy.deepcopy(_final_selection())
    final_selection["must_pass"]["dataset_quality_gate_statsu"] = "PASS"
    payload["final_selection"] = final_selection
    with pytest.raises(ManifestValidationError, match="final_selection.must_pass unsupported fields"):
        parse_manifest(payload)


def test_final_selection_exposure_policy_rejects_unknown_key() -> None:
    payload = _manifest_payload()
    final_selection = copy.deepcopy(_final_selection())
    final_selection["selection_exposure_policy"]["count_as_holdout_reuse"] = True
    payload["final_selection"] = final_selection
    with pytest.raises(ManifestValidationError, match="final_selection.selection_exposure_policy unsupported fields"):
        parse_manifest(payload)


def test_final_selection_exposure_policy_requires_holdout_reuse_when_holdout_ranked() -> None:
    payload = _manifest_payload()
    final_selection = copy.deepcopy(_final_selection())
    final_selection["selection_exposure_policy"]["counts_as_holdout_reuse"] = False
    payload["final_selection"] = final_selection
    with pytest.raises(ManifestValidationError, match="counts_as_holdout_reuse must be true"):
        parse_manifest(payload)


def test_final_holdout_reuse_blocks_promotion_grade_selection(tmp_path) -> None:
    registry_path = tmp_path / "experiment_registry.jsonl"
    row = {
        "event_type": "research_attempt_reserved",
        "deployment_tier": "paper_candidate",
        "experiment_id": "exp_001",
        "experiment_family_id": "family_001",
        "hypothesis_id": "hypothesis_001",
        "final_holdout_reuse_key_hash_v1": "sha256:holdout-v1",
        "final_holdout_reuse_key_hash": "sha256:holdout-v2",
        "final_holdout_reuse_key_schema_version": FINAL_HOLDOUT_REUSE_KEY_SCHEMA_VERSION,
        "final_holdout_reuse_key_hash_v2": "sha256:holdout-v2",
        "objective_metric": "net_excess_return",
        "computed_attempt_index": 1,
        "computed_holdout_reuse_count": 2,
        "prior_registry_hash": EMPTY_EXPERIMENT_REGISTRY_HASH,
    }
    row["row_hash"] = compute_row_hash(row)
    registry_path.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")
    report = {
        "deployment_tier": "paper_candidate",
        "experiment_id": "exp_001",
        "experiment_family_id": "family_001",
        "hypothesis_id": "hypothesis_001",
        "experiment_registry_path": str(registry_path),
        "experiment_registry_row_hash": row["row_hash"],
        "experiment_registry_prior_hash": EMPTY_EXPERIMENT_REGISTRY_HASH,
        "computed_holdout_reuse_count": 2,
        "computed_attempt_index": 1,
        "final_holdout_reuse_key_hash_v1": "sha256:holdout-v1",
        "final_holdout_reuse_key_hash": "sha256:holdout-v2",
        "final_holdout_reuse_key_schema_version": FINAL_HOLDOUT_REUSE_KEY_SCHEMA_VERSION,
        "final_holdout_reuse_key_hash_v2": "sha256:holdout-v2",
        "objective_metric": "net_excess_return",
        "statistical_validation_contract": {
            "gates": {
                "max_holdout_reuse_count": 0,
                "max_attempt_index_without_new_hypothesis": 1,
            }
        },
    }

    reasons = validate_experiment_registry_binding(report=report, require_complete=False)

    assert "holdout_reuse_budget_exceeded" in reasons
    assert "experiment_registry_budget_exceeded" in reasons

    summary = build_research_run_summary(
        {
            **report,
            "registry_gate_result": "FAIL",
            "registry_gate_fail_reasons": reasons,
            "best_candidate_id": "candidate_001",
            "promotion_eligibility_gate_result": "PASS",
        }
    )
    assert summary.promotion_allowed is False


def test_final_holdout_reuse_missing_v2_identity_fails_production_binding(tmp_path) -> None:
    registry_path = tmp_path / "experiment_registry.jsonl"
    row = {
        "event_type": "research_attempt_reserved",
        "deployment_tier": "paper_candidate",
        "experiment_id": "exp_001",
        "experiment_family_id": "family_001",
        "hypothesis_id": "hypothesis_001",
        "final_holdout_reuse_key_hash_v1": "sha256:holdout-v1",
        "final_holdout_reuse_key_hash": "sha256:holdout-v2",
        "computed_attempt_index": 1,
        "computed_holdout_reuse_count": 0,
        "prior_registry_hash": EMPTY_EXPERIMENT_REGISTRY_HASH,
    }
    row["row_hash"] = compute_row_hash(row)
    registry_path.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")
    report = {
        **row,
        "deployment_tier": "paper_candidate",
        "experiment_registry_path": str(registry_path),
        "experiment_registry_row_hash": row["row_hash"],
        "experiment_registry_prior_hash": EMPTY_EXPERIMENT_REGISTRY_HASH,
    }

    reasons = validate_experiment_registry_binding(report=report, require_complete=False)

    assert "final_holdout_reuse_key_schema_version_missing" in reasons
    assert "objective_metric_missing" in reasons

    missing_key_report = {
        **report,
        "final_holdout_reuse_key_schema_version": FINAL_HOLDOUT_REUSE_KEY_SCHEMA_VERSION,
        "final_holdout_reuse_key_hash": None,
        "objective_metric": "net_excess_return",
    }
    reasons = validate_experiment_registry_binding(report=missing_key_report, require_complete=False)
    assert "final_holdout_reuse_key_hash_v2_missing" in reasons


def test_missing_registry_evidence_blocks_production_promotion_summary() -> None:
    summary = build_research_run_summary(
        {
            "deployment_tier": "paper_candidate",
            "best_candidate_id": "candidate_001",
            "promotion_eligibility_gate_result": "PASS",
            "registry_gate_result": "WARN",
            "registry_gate_fail_reasons": ["experiment_registry_missing"],
        }
    )

    assert summary.promotion_allowed is False
    assert summary.next_action == "do_not_promote_review_experiment_registry"


def test_selected_candidate_requires_validation_run_complete() -> None:
    report = {
        "validation_run_complete": False,
        "diagnostic_only": True,
        "standalone_backtest_not_full_validation": True,
        "next_required_stage": "research-validate",
        "selected_candidate_id": "candidate_001",
        "best_candidate_id": "candidate_001",
        "promotion_eligibility_gate_result": "PASS",
    }
    summary = build_research_run_summary(report)

    assert summary.promotion_allowed is False
    assert report["selected_candidate_id"] == "candidate_001"
    assert report["next_required_stage"] == "research-validate"


def test_final_selection_unsupported_metric_policy_rejects_unknown_value() -> None:
    payload = _manifest_payload()
    final_selection = copy.deepcopy(_final_selection())
    final_selection["unsupported_metric_policy"]["sharpe_ratio"] = "ignore"
    payload["final_selection"] = final_selection
    with pytest.raises(ManifestValidationError, match="unsupported_metric_policy.sharpe_ratio must be fail_if_required"):
        parse_manifest(payload)


def test_final_selection_contract_in_manifest_hash() -> None:
    payload = _manifest_payload()
    first = parse_manifest(payload).manifest_hash()
    changed = copy.deepcopy(payload)
    changed["final_selection"] = _final_selection(
        [
            {"metric": "final_holdout.metrics_v2.return_risk.max_drawdown_pct", "order": "asc", "required": True},
            {"metric": "parameter_candidate_id", "order": "asc", "required": True},
        ]
    )
    assert parse_manifest(changed).manifest_hash() != first


def test_final_selection_required_for_production_bound_manifest() -> None:
    payload = _manifest_payload()
    payload.pop("final_selection")
    with pytest.raises(ManifestValidationError, match="final_selection required"):
        parse_manifest(payload)


def test_research_only_missing_final_selection_gets_legacy_warning() -> None:
    result = apply_final_selection_contract(
        contract=None,
        candidates=[_candidate("candidate_001")],
        report_context=_context(),
        production_bound=False,
    )
    assert result["gate_result"] == "WARN"
    assert result["fail_reasons"] == ["legacy_implicit_final_rank_policy_v1"]


def test_final_selection_lexicographic_rank_deterministic() -> None:
    result = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_001", final_expectancy=10), _candidate("candidate_002", final_expectancy=20)],
        report_context=_context(),
        production_bound=True,
    )
    assert result["selected_candidate_id"] == "candidate_002"
    assert result["gate_result"] == "PASS"


def test_final_selection_tie_breaks_by_parameter_candidate_id() -> None:
    result = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_002"), _candidate("candidate_001")],
        report_context=_context(),
        production_bound=True,
    )
    assert result["selected_candidate_id"] == "candidate_001"


def test_final_selection_scores_hash_independent_of_input_candidate_order() -> None:
    first = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_001", final_expectancy=10), _candidate("candidate_002", final_expectancy=20)],
        report_context=_context(),
        production_bound=True,
    )
    second = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_002", final_expectancy=20), _candidate("candidate_001", final_expectancy=10)],
        report_context=_context(),
        production_bound=True,
    )

    assert first["selected_candidate_id"] == second["selected_candidate_id"] == "candidate_002"
    assert first["selected_candidate_score_hash"] == second["selected_candidate_score_hash"]
    assert first["candidate_final_scores_hash"] == second["candidate_final_scores_hash"]
    assert first["candidate_final_scores"] == second["candidate_final_scores"]


def test_final_selection_hash_changes_when_ranking_changes() -> None:
    candidates = [_candidate("candidate_001"), _candidate("candidate_002", final_expectancy=200)]
    first = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=candidates,
        report_context=_context(),
        production_bound=True,
    )
    second = apply_final_selection_contract(
        contract=_final_selection(
            [
                {"metric": "validation.stress.risk_adjusted_score.calmar_ratio", "order": "desc", "required": True},
                {"metric": "parameter_candidate_id", "order": "asc", "required": True},
            ]
        ),
        candidates=candidates,
        report_context=_context(),
        production_bound=True,
    )
    assert first["final_selection_contract_hash"] != second["final_selection_contract_hash"]
    assert first["candidate_final_scores_hash"] != second["candidate_final_scores_hash"]


def test_final_selection_rejects_required_metric_missing() -> None:
    candidate = _candidate("candidate_001")
    candidate["final_holdout_metrics_v2"] = {}
    result = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[candidate],
        report_context=_context(),
        production_bound=True,
    )
    assert result["gate_result"] == "FAIL"
    assert "final_selection_no_eligible_candidates" in result["fail_reasons"]


def test_final_selection_sharpe_sortino_fail_without_period_return_panel() -> None:
    contract = _final_selection(
        [
            {"metric": "validation.stress.risk_adjusted_score.sharpe_ratio", "order": "desc", "required": True},
            {"metric": "validation.stress.risk_adjusted_score.sortino_ratio", "order": "desc", "required": True},
            {"metric": "parameter_candidate_id", "order": "asc", "required": True},
        ]
    )
    result = apply_final_selection_contract(
        contract=contract,
        candidates=[_candidate("candidate_001")],
        report_context=_context(),
        production_bound=True,
    )
    reasons = result["candidate_final_scores"][0]["eligibility_reasons"]
    assert "final_selection_sharpe_unavailable_without_period_return_series" in reasons
    assert "final_selection_sortino_unavailable_without_period_return_series" in reasons


def test_final_selection_buy_and_hold_excess_differs_from_cash() -> None:
    candidates = [
        {
            "parameter_candidate_id": "candidate_001",
            "parameter_values": {},
            "scenario_policy": "single_scenario",
            "required_scenario_ids": ["base"],
            "validation_metrics": {
                "return_pct": 12.0,
                "benchmark_buy_and_hold_return_pct": 5.0,
            },
            "acceptance_gate_result": "PASS",
        }
    ]
    cash = candidate_metric_universe_payload(
        candidates=candidates,
        required_scenario_ids=["base"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="cash",
    )
    buy_hold = candidate_metric_universe_payload(
        candidates=candidates,
        required_scenario_ids=["base"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="buy_and_hold",
    )
    assert cash["candidates"][0]["validation_metric_value"] == 12.0
    assert buy_hold["candidates"][0]["validation_metric_value"] == 7.0


def test_final_selection_final_holdout_can_drive_rank_when_contract_says_so() -> None:
    result = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[
            _candidate("candidate_validation_winner", validation_expectancy=500, final_expectancy=10),
            _candidate("candidate_holdout_winner", validation_expectancy=100, final_expectancy=50),
        ],
        report_context=_context(),
        production_bound=True,
    )
    assert result["selected_candidate_id"] == "candidate_holdout_winner"


def test_final_selection_report_best_candidate_matches_selected_candidate() -> None:
    selection = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_001")],
        report_context=_context(),
        production_bound=True,
    )
    report = {
        "final_selection_required": True,
        "final_selection_contract": selection["final_selection_contract"],
        "final_selection_contract_hash": selection["final_selection_contract_hash"],
        "final_selection_gate_result": "PASS",
        "selected_candidate_id": selection["selected_candidate_id"],
        "selected_candidate_score_hash": selection["selected_candidate_score_hash"],
        "candidate_final_scores_hash": selection["candidate_final_scores_hash"],
        "best_candidate_id": selection["selected_candidate_id"],
        "candidates": [_candidate("candidate_001")],
        **_context(),
    }
    assert validate_final_selection_report(report) == []


def test_promotion_rejects_candidate_not_selected_by_final_selection() -> None:
    selection = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_001"), _candidate("candidate_002", final_expectancy=200)],
        report_context=_context(),
        production_bound=True,
    )
    report = {
        "final_selection_required": True,
        "final_selection_contract": selection["final_selection_contract"],
        "final_selection_contract_hash": selection["final_selection_contract_hash"],
        "final_selection_gate_result": "PASS",
        "selected_candidate_id": "candidate_001",
        "selected_candidate_score_hash": selection["selected_candidate_score_hash"],
        "candidate_final_scores_hash": selection["candidate_final_scores_hash"],
        "best_candidate_id": "candidate_001",
        "candidates": [_candidate("candidate_001"), _candidate("candidate_002", final_expectancy=200)],
        **_context(),
    }
    assert "final_selection_selected_candidate_mismatch" in validate_final_selection_report(report)


def test_reproduce_rejects_final_selection_score_drift() -> None:
    selection = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_001")],
        report_context=_context(),
        production_bound=True,
    )
    report = {
        "final_selection_required": True,
        "final_selection_contract": selection["final_selection_contract"],
        "final_selection_contract_hash": selection["final_selection_contract_hash"],
        "final_selection_gate_result": "PASS",
        "selected_candidate_id": selection["selected_candidate_id"],
        "selected_candidate_score_hash": "sha256:" + "0" * 64,
        "candidate_final_scores_hash": selection["candidate_final_scores_hash"],
        "best_candidate_id": selection["selected_candidate_id"],
        "candidates": [_candidate("candidate_001")],
        **_context(),
    }
    assert "final_selection_score_hash_mismatch" in validate_final_selection_report(report)


def test_reproduce_rejects_final_selection_contract_drift() -> None:
    selection = apply_final_selection_contract(
        contract=_final_selection(),
        candidates=[_candidate("candidate_001")],
        report_context=_context(),
        production_bound=True,
    )
    contract = copy.deepcopy(selection["final_selection_contract"])
    contract["selection_exposure_policy"] = {"final_holdout_usage": "changed"}
    report = {
        "final_selection_required": True,
        "final_selection_contract": contract,
        "final_selection_contract_hash": selection["final_selection_contract_hash"],
        "final_selection_gate_result": "PASS",
        "selected_candidate_id": selection["selected_candidate_id"],
        "selected_candidate_score_hash": selection["selected_candidate_score_hash"],
        "candidate_final_scores_hash": selection["candidate_final_scores_hash"],
        "best_candidate_id": selection["selected_candidate_id"],
        "candidates": [_candidate("candidate_001")],
        **_context(),
    }
    assert "final_selection_contract_hash_mismatch" in validate_final_selection_report(report)


def test_legacy_candidate_rank_key_only_research_only_warning() -> None:
    result = apply_final_selection_contract(
        contract=None,
        candidates=[_candidate("candidate_001")],
        report_context=_context(),
        production_bound=False,
    )
    assert result["gate_result"] == "WARN"
    assert result["final_selection_contract_hash"] is None
