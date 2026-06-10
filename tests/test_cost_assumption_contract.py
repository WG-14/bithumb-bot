from __future__ import annotations

import pytest

from bithumb_bot.research.deployment_policy import validate_production_calibration_policy
from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest


def _runtime_bound_parameter_space() -> dict[str, list[object]]:
    return {
        "SMA_SHORT": [7],
        "SMA_LONG": [30],
        "SMA_FILTER_GAP_MIN_RATIO": [0.0012],
        "SMA_FILTER_VOL_WINDOW": [10],
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.003],
        "SMA_FILTER_OVEREXT_LOOKBACK": [3],
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": [0.02],
        "SMA_MARKET_REGIME_ENABLED": [True],
        "SMA_COST_EDGE_ENABLED": [True],
        "SMA_COST_EDGE_MIN_RATIO": [0.0],
        "ENTRY_EDGE_BUFFER_RATIO": [0.0005],
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": [0.0],
        "STRATEGY_ENTRY_SLIPPAGE_BPS": [10],
        "LIVE_FEE_RATE_ESTIMATE": [0.0004],
        "STRATEGY_EXIT_RULES": ["opposite_cross,max_holding_time"],
        "STRATEGY_EXIT_STOP_LOSS_RATIO": [0.0],
        "STRATEGY_EXIT_MAX_HOLDING_MIN": [0],
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": [0.0],
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": [0.0],
    }


def _portfolio_policy() -> dict[str, object]:
    return {
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
    }


def _risk_policy() -> dict[str, object]:
    return {
        "schema_version": 1,
        "max_daily_loss_krw": 30_000,
        "max_position_loss_pct": 10.0,
        "max_daily_order_count": 20,
        "kill_switch": False,
        "max_open_positions": 1,
        "unresolved_order_policy": "block",
        "missing_policy": "fail_closed_for_promotion",
    }


def _manifest(*, deployment_tier: str = "paper_candidate") -> dict[str, object]:
    payload: dict[str, object] = {
        "experiment_id": "cost_contract_test",
        "hypothesis": "Cost assumptions are explicit.",
        "strategy_name": "sma_with_filter",
        "deployment_tier": deployment_tier,
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "candles_v1",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": _runtime_bound_parameter_space(),
        "cost_model": {"fee_rate": 0.0004, "slippage_bps": [10]},
        "acceptance_gate": {
            "min_trade_count": 30,
            "max_mdd_pct": 15,
            "min_profit_factor": 1.2,
            "oos_return_must_be_positive": True,
            "parameter_stability_required": True,
            "walk_forward_required": True,
            "final_holdout_required_for_promotion": True,
            "metrics_contract_required": True,
            "min_cagr_pct": 0,
            "min_expectancy_per_trade_krw": 0,
            "reject_open_position_at_end": True,
        },
        "walk_forward": {
            "train_window_days": 2,
            "test_window_days": 1,
            "step_days": 1,
            "min_windows": 1,
        },
        "statistical_validation": {
            "required_for_promotion": True,
            "benchmark": "cash",
            "primary_metric": "net_excess_return",
            "selection_universe": "all_parameter_candidates_all_required_scenarios",
            "multiple_testing_scope": "experiment_family",
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
    }
    if deployment_tier != "research_only":
        payload["portfolio_policy"] = _portfolio_policy()
        payload["risk_policy"] = _risk_policy()
        payload["execution_timing"] = {
            "fill_reference_policy": "next_candle_open",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "candle_next_open",
        }
        payload["stress_suite"] = _stress_suite()
        payload["final_selection"] = _final_selection()
    return payload


def _final_selection() -> dict[str, object]:
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
        "ranking": [
            {
                "metric": "final_holdout.metrics_v2.trade_quality.expectancy_per_trade_krw",
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


def _stress_suite() -> dict[str, object]:
    return {
        "required_for_promotion": True,
        "trade_removal": {
            "top_n_by_net_pnl": [1],
            "min_return_retention_pct": 50.0,
        },
        "trade_order_monte_carlo": {
            "iterations": 100,
            "seed_policy": "derived_from_manifest_candidate_scenario_split_hash",
            "min_survival_probability": 0.95,
            "ruin_max_drawdown_pct": 35.0,
            "min_closed_trades": 3,
        },
    }


def _base_scenario() -> dict[str, object]:
    return {
        "scenario_role": "base",
        "label": "realistic_bithumb_app_fee_0004",
        "fee_rate": 0.0004,
        "fee_source": "operator_declared_bithumb_app_fee",
        "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
        "slippage_bps": 10,
        "slippage_source": "execution_calibration",
        "promotable_as_base": True,
    }


def _stress_scenario() -> dict[str, object]:
    return {
        "scenario_role": "stress",
        "label": "stress_fee_0025_slippage_20bps",
        "fee_rate": 0.0025,
        "fee_source": "stress_assumption",
        "fee_authority_policy": "not_promotable_as_runtime_base",
        "slippage_bps": 20,
        "slippage_source": "stress_assumption",
        "promotable_as_base": False,
    }


def test_production_bound_manifest_without_base_cost_assumption_fails() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "scenario_policy": "must_pass_base_and_survive_stress",
        "scenarios": [_stress_scenario()],
        "calibration_required": True,
    }

    with pytest.raises(ManifestValidationError, match="production_base_cost_assumption_required"):
        parse_manifest(payload)


def test_production_bound_legacy_cost_model_fails() -> None:
    with pytest.raises(ManifestValidationError, match="production_legacy_cost_model_not_promotable"):
        parse_manifest(_manifest())


def test_production_bound_unlabeled_base_cost_assumption_fails() -> None:
    payload = _manifest()
    base = _base_scenario()
    base["label"] = ""
    payload["execution_model"] = {
        "scenario_policy": "must_pass_base_and_survive_stress",
        "scenarios": [base, _stress_scenario()],
        "calibration_required": True,
    }

    with pytest.raises(ManifestValidationError, match="production_cost_assumption_label_required"):
        parse_manifest(payload)


def test_explicit_base_and_stress_cost_assumptions_pass_policy() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "scenario_policy": "must_pass_base_and_survive_stress",
        "scenarios": [_base_scenario(), _stress_scenario()],
        "calibration_required": True,
    }

    manifest = parse_manifest(payload)

    base = next(
        scenario for scenario in manifest.execution_model.scenarios if scenario.scenario_role == "base"
    )
    stress = next(
        scenario for scenario in manifest.execution_model.scenarios if scenario.scenario_role == "stress"
    )

    assert base.cost_assumption is not None
    assert base.cost_assumption.promotable_as_base is True
    assert stress.scenario_role == "stress"


def test_legacy_cost_model_research_only_is_marked_legacy_non_promotable() -> None:
    manifest = parse_manifest(_manifest(deployment_tier="research_only"))

    scenario = manifest.execution_model.scenarios[0]
    assert manifest.execution_model.source == "legacy_cost_model"
    assert scenario.cost_assumption is not None
    assert scenario.cost_assumption.fee_source == "legacy_cost_model"
    assert scenario.cost_assumption.promotable_as_base is False


def _production_calibration_gate() -> dict[str, object]:
    return {
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
                "sample_count": 30,
                "min_sample_count": 30,
                "quality_gate_status": "PASS",
            }
        ],
    }


def _generated_candidate_shape(*, include_contract: bool = True) -> dict[str, object]:
    candidate: dict[str, object] = {
        "deployment_tier": "paper_candidate",
        "execution_model_source": "execution_model",
        "execution_model": {
            "type": "stress",
            "fee_rate": 0.0004,
            "slippage_bps": 10,
            "model_params_hash": "sha256:model",
        },
        "execution_calibration_required": True,
        "execution_calibration_strictness": "fail",
        "execution_calibration_gate": _production_calibration_gate(),
    }
    if include_contract:
        candidate["cost_assumption_contract"] = {
            "source": "execution_model",
            "scenario_policy": "must_pass_base_and_survive_stress",
            "calibration_required": True,
            "calibration_strictness": "fail",
            "scenarios": [
                {
                    "scenario_role": "base",
                    "cost_assumption": {
                        "label": "realistic_bithumb_app_fee_0004",
                        "role": "base",
                        "fee_rate": 0.0004,
                        "fee_source": "operator_declared_bithumb_app_fee",
                        "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
                        "slippage_bps": 10,
                        "slippage_source": "execution_calibration",
                        "promotable_as_base": True,
                    },
                }
            ],
        }
    return candidate


def test_production_policy_accepts_generated_candidate_cost_contract_shape() -> None:
    result = validate_production_calibration_policy(_generated_candidate_shape())

    assert "production_base_cost_assumption_required" not in result.reasons
    assert "production_stress_only_cost_model_not_promotable" not in result.reasons
    assert result.status == "PASS"


def test_production_policy_rejects_primary_scenario_without_full_cost_contract() -> None:
    result = validate_production_calibration_policy(_generated_candidate_shape(include_contract=False))

    assert result.status == "FAIL"
    assert "production_base_cost_assumption_required" in result.reasons
