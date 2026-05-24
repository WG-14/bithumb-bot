from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.research.strategy_spec import strategy_spec_for_name


def _manifest() -> dict[str, object]:
    return {
        "experiment_id": "sma_filter_v1_2026_05",
        "hypothesis": "SMA filter has positive expectancy after costs.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "candles_v1",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2, 3],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 50,
            "min_profit_factor": 1.0,
            "oos_return_must_be_positive": True,
            "parameter_stability_required": False,
        },
    }


def _all_runtime_behavior_parameter_space() -> dict[str, list[object]]:
    spec = strategy_spec_for_name("sma_with_filter")
    values = {
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
        "STRATEGY_EXIT_RULES": ["stop_loss,opposite_cross,max_holding_time"],
        "STRATEGY_EXIT_STOP_LOSS_RATIO": [0.0],
        "STRATEGY_EXIT_MAX_HOLDING_MIN": [0],
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": [0.0],
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": [0.0],
    }
    required = set(spec.behavior_affecting_parameter_names) - set(spec.research_only_parameter_names)
    return {key: values[key] for key in sorted(required)}


def test_research_rejects_unknown_strategy_params() -> None:
    payload = _manifest()
    payload["parameter_space"] = {
        **payload["parameter_space"],  # type: ignore[arg-type]
        "UNUSED_NOOP_PARAM": [1],
    }

    with pytest.raises(ManifestValidationError, match="unknown strategy parameter"):
        parse_manifest(payload)


def test_noop_baseline_manifest_uses_its_own_parameter_contract() -> None:
    payload = _manifest()
    payload["strategy_name"] = "noop_baseline"
    payload["parameter_space"] = {"NOOP_DECISION_START_INDEX": [0, 2]}

    manifest = parse_manifest(payload)

    assert manifest.strategy_name == "noop_baseline"
    assert manifest.parameter_space == {"NOOP_DECISION_START_INDEX": (0, 2)}


def test_noop_baseline_manifest_rejects_sma_parameters() -> None:
    payload = _manifest()
    payload["strategy_name"] = "noop_baseline"
    payload["parameter_space"] = {"SMA_SHORT": [2], "SMA_LONG": [4]}

    with pytest.raises(ManifestValidationError, match="unknown strategy parameter"):
        parse_manifest(payload)


def test_buy_and_hold_baseline_manifest_uses_its_own_parameter_contract() -> None:
    payload = _manifest()
    payload["strategy_name"] = "buy_and_hold_baseline"
    payload["parameter_space"] = {
        "BUY_HOLD_BUY_INDEX": [1],
        "BUY_HOLD_DECISION_REASON": ["architecture_canary_buy"],
    }

    manifest = parse_manifest(payload)

    assert manifest.strategy_name == "buy_and_hold_baseline"
    assert manifest.parameter_space == {
        "BUY_HOLD_BUY_INDEX": (1,),
        "BUY_HOLD_DECISION_REASON": ("architecture_canary_buy",),
    }


def test_buy_and_hold_baseline_manifest_rejects_sma_parameters() -> None:
    payload = _manifest()
    payload["strategy_name"] = "buy_and_hold_baseline"
    payload["parameter_space"] = {"SMA_SHORT": [2], "SMA_LONG": [4]}

    with pytest.raises(ManifestValidationError, match="unknown strategy parameter"):
        parse_manifest(payload)


def test_production_bound_buy_and_hold_manifest_requires_behavior_parameters() -> None:
    payload = _production_manifest()
    payload["strategy_name"] = "buy_and_hold_baseline"
    payload["parameter_space"] = {"BUY_HOLD_BUY_INDEX": [1]}

    with pytest.raises(ManifestValidationError, match="BUY_HOLD_DECISION_REASON"):
        parse_manifest(payload)


def test_production_bound_noop_manifest_requires_noop_behavior_parameters() -> None:
    payload = _production_manifest()
    payload["strategy_name"] = "noop_baseline"
    payload["parameter_space"] = {"NOOP_DECISION_REASON": ["hold_for_architecture_canary"]}

    with pytest.raises(ManifestValidationError, match="NOOP_DECISION_START_INDEX"):
        parse_manifest(payload)


def test_research_rejects_unused_behavior_params_for_production_bound() -> None:
    payload = _manifest()
    payload["deployment_tier"] = "paper_candidate"
    payload["portfolio_policy"] = _portfolio_policy()
    payload["execution_model"] = {
        "source": "manifest",
        "scenario_policy": "single_base",
        "calibration_required": False,
        "calibration_strictness": "warn",
        "scenarios": [
            {
                "type": "fixed_bps",
                "fee_rate": 0.001,
                "slippage_bps": 0.0,
                "scenario_role": "base",
                "promotable_as_base": True,
                "fee_source": "manifest",
                "slippage_source": "manifest",
                "fee_authority_policy": "runtime_fee_authority_or_config_fallback",
            }
        ],
    }
    payload["statistical_validation"] = _statistical_validation()
    payload["stress_suite"] = _stress_suite()
    payload["final_selection"] = _final_selection()
    payload["parameter_space"] = {**_all_runtime_behavior_parameter_space(), "UNUSED_NOOP_PARAM": [1]}

    with pytest.raises(ManifestValidationError, match="unknown strategy parameter"):
        parse_manifest(payload)


def test_production_bound_manifest_requires_all_behavior_affecting_strategy_parameters() -> None:
    payload = _production_manifest()
    payload["parameter_space"] = {
        "SMA_SHORT": [2],
        "SMA_LONG": [4],
    }

    with pytest.raises(ManifestValidationError, match="behavior-affecting strategy parameter"):
        parse_manifest(payload)


def test_missing_behavior_parameter_fails_closed_for_production_bound() -> None:
    payload = _production_manifest()
    payload["parameter_space"] = _all_runtime_behavior_parameter_space()
    payload["parameter_space"].pop("SMA_MARKET_REGIME_ENABLED")

    with pytest.raises(ManifestValidationError, match="SMA_MARKET_REGIME_ENABLED"):
        parse_manifest(payload)


def test_missing_stop_loss_parameter_fails_closed_for_production_bound() -> None:
    payload = _production_manifest()
    payload["parameter_space"] = _all_runtime_behavior_parameter_space()
    payload["parameter_space"].pop("STRATEGY_EXIT_STOP_LOSS_RATIO")

    with pytest.raises(ManifestValidationError, match="STRATEGY_EXIT_STOP_LOSS_RATIO"):
        parse_manifest(payload)


def test_negative_stop_loss_parameter_is_rejected() -> None:
    payload = _manifest()
    payload["parameter_space"] = {
        **payload["parameter_space"],  # type: ignore[arg-type]
        "STRATEGY_EXIT_STOP_LOSS_RATIO": [-0.01],
    }

    with pytest.raises(ManifestValidationError, match="STRATEGY_EXIT_STOP_LOSS_RATIO"):
        parse_manifest(payload)


def test_positive_stop_loss_requires_stop_loss_rule() -> None:
    payload = _manifest()
    payload["parameter_space"] = {
        **payload["parameter_space"],  # type: ignore[arg-type]
        "STRATEGY_EXIT_RULES": ["opposite_cross,max_holding_time"],
        "STRATEGY_EXIT_STOP_LOSS_RATIO": [0.05],
    }

    with pytest.raises(ManifestValidationError, match="does not include stop_loss"):
        parse_manifest(payload)


def _portfolio_policy(*, starting_cash: float = 1_000_000.0, buy_fraction: float = 0.99) -> dict[str, object]:
    cash_buffer_policy = (
        "retain_1_percent_before_fees"
        if buy_fraction == 0.99
        else "derived_from_buy_fraction_before_fees"
    )
    return {
        "schema_version": 1,
        "starting_cash_krw": starting_cash,
        "quote_currency": "KRW",
        "initial_position_qty": 0.0,
        "cash_interest_policy": "zero",
        "position_sizing": {
            "type": "fractional_cash",
            "buy_fraction": buy_fraction,
            "sell_policy": "sell_all_available_position",
            "cash_buffer_policy": cash_buffer_policy,
            "min_order_krw": None,
            "max_order_krw": None,
            "rounding_policy": "engine_float_no_exchange_lot_rounding",
        },
        "source": "manifest",
    }


def _statistical_validation() -> dict[str, object]:
    return {
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
    }


def _stress_suite() -> dict[str, object]:
    return {
        "required_for_promotion": True,
        "trade_removal": {
            "top_n_by_net_pnl": [1, 3],
            "min_return_retention_pct": 50.0,
        },
        "trade_order_monte_carlo": {
            "iterations": 100,
            "seed_policy": "derived_from_manifest_candidate_scenario_split_hash",
            "min_survival_probability": 0.95,
            "ruin_max_drawdown_pct": 35.0,
            "min_closed_trades": 3,
        },
        "risk_adjusted_score": {
            "required_metrics": ["calmar"],
            "ranking": ["pass_gate", "max_calmar", "max_expectancy", "min_mdd"],
        },
    }


def _final_selection() -> dict[str, object]:
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
        "ranking": [
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
            {"metric": "parameter_candidate_id", "order": "asc", "required": True},
        ],
        "unsupported_metric_policy": {
            "sharpe_ratio": "fail_if_required",
            "sortino_ratio": "fail_if_required",
        },
    }


def _production_manifest() -> dict[str, object]:
    payload = _manifest()
    payload["deployment_tier"] = "paper_candidate"
    payload["parameter_space"] = _all_runtime_behavior_parameter_space()
    payload["portfolio_policy"] = _portfolio_policy()
    payload["execution_model"] = {
        "scenario_policy": "single_scenario",
        "scenarios": [
            {
                "scenario_role": "base",
                "label": "realistic_bithumb_app_fee_0004",
                "fee_rate": 0.0004,
                "fee_source": "operator_declared_bithumb_app_fee",
                "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
                "slippage_bps": 10,
                "slippage_source": "execution_calibration",
                "promotable_as_base": True,
                "type": "fixed_bps",
            }
        ],
        "calibration_required": True,
        "calibration_strictness": "fail",
    }
    payload["statistical_validation"] = _statistical_validation()
    payload["stress_suite"] = _stress_suite()
    payload["final_selection"] = _final_selection()
    return payload


def test_manifest_parses_required_contract() -> None:
    manifest = parse_manifest(_manifest())

    assert manifest.experiment_id == "sma_filter_v1_2026_05"
    assert manifest.hypothesis
    assert manifest.manifest_hash().startswith("sha256:")
    assert manifest.execution_model.source == "legacy_cost_model"
    assert manifest.execution_model.scenarios[0].type == "fixed_bps"
    assert manifest.execution_model.scenarios[0].slippage_bps == 0.0
    assert manifest.portfolio_policy.source == "legacy_research_default"
    assert "legacy_portfolio_policy_default_used" in manifest.portfolio_policy.warning_codes()


def test_production_example_manifest_declares_default_stop_loss_policy_family() -> None:
    path = Path("examples/research/sma_filter_manifest.production.example.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    parameter_space = payload["parameter_space"]
    assert parameter_space["STRATEGY_EXIT_RULES"] == ["stop_loss,opposite_cross,max_holding_time"]
    assert parameter_space["STRATEGY_EXIT_STOP_LOSS_RATIO"] == [0.0]
    parse_manifest(payload)


def test_portfolio_policy_binds_manifest_hash() -> None:
    payload = _manifest()
    payload["portfolio_policy"] = _portfolio_policy()
    baseline = parse_manifest(payload)

    changed_cash = _manifest()
    changed_cash["portfolio_policy"] = _portfolio_policy(starting_cash=2_000_000.0)
    changed_fraction = _manifest()
    changed_fraction["portfolio_policy"] = _portfolio_policy(buy_fraction=0.5)

    assert baseline.canonical_payload()["portfolio_policy"]["source"] == "manifest"
    assert parse_manifest(changed_cash).manifest_hash() != baseline.manifest_hash()
    assert parse_manifest(changed_fraction).manifest_hash() != baseline.manifest_hash()


def test_production_bound_manifest_requires_portfolio_policy() -> None:
    payload = _production_manifest()
    payload.pop("portfolio_policy")

    with pytest.raises(ManifestValidationError, match="portfolio_policy is required"):
        parse_manifest(payload)


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda policy: policy.update({"starting_cash_krw": 0}), "starting_cash_krw"),
        (
            lambda policy: policy["position_sizing"].update({"buy_fraction": 0}),
            "buy_fraction",
        ),
        (
            lambda policy: policy["position_sizing"].update({"buy_fraction": 1.1}),
            "buy_fraction",
        ),
        (
            lambda policy: policy["position_sizing"].update({"type": "fixed_notional"}),
            "type must be fractional_cash",
        ),
        (
            lambda policy: policy["position_sizing"].update({"sell_policy": "sell_half"}),
            "sell_policy must be sell_all_available_position",
        ),
        (
            lambda policy: policy["position_sizing"].update({"cash_buffer_policy": "none"}),
            "cash_buffer_policy must be derived_from_buy_fraction_before_fees",
        ),
        (
            lambda policy: policy.update({"initial_position_qty": 0.1}),
            "portfolio_policy.initial_position_qty non-zero is not supported yet",
        ),
        (
            lambda policy: policy["position_sizing"].update({"min_order_krw": 5000.0}),
            "portfolio_policy.position_sizing.min_order_krw is not supported yet",
        ),
        (
            lambda policy: policy["position_sizing"].update({"max_order_krw": 50000.0}),
            "portfolio_policy.position_sizing.max_order_krw is not supported yet",
        ),
        (
            lambda policy: policy["position_sizing"].update({"rounding_policy": "exchange_lot"}),
            "rounding_policy must be engine_float_no_exchange_lot_rounding",
        ),
    ],
)
def test_portfolio_policy_invalid_values_fail_clearly(mutator, message) -> None:
    payload = _manifest()
    policy = _portfolio_policy()
    mutator(policy)
    payload["portfolio_policy"] = policy

    with pytest.raises(ManifestValidationError, match=message):
        parse_manifest(payload)


def test_cash_buffer_policy_retain_one_percent_requires_matching_buy_fraction() -> None:
    payload = _manifest()
    policy = _portfolio_policy(buy_fraction=0.5)
    policy["position_sizing"]["cash_buffer_policy"] = "retain_1_percent_before_fees"
    payload["portfolio_policy"] = policy

    with pytest.raises(ManifestValidationError, match="retain_1_percent_before_fees requires buy_fraction == 0.99"):
        parse_manifest(payload)


def test_cash_buffer_policy_can_be_derived_from_buy_fraction() -> None:
    payload = _manifest()
    payload["portfolio_policy"] = _portfolio_policy(buy_fraction=0.5)

    manifest = parse_manifest(payload)

    assert manifest.portfolio_policy.position_sizing.cash_buffer_policy == "derived_from_buy_fraction_before_fees"


def test_manifest_parses_statistical_validation_and_binds_hash() -> None:
    payload = _manifest()
    payload["statistical_validation"] = _statistical_validation()

    manifest = parse_manifest(payload)
    baseline_hash = manifest.manifest_hash()

    assert manifest.statistical_validation is not None
    assert manifest.statistical_validation.required_for_promotion is True
    assert manifest.canonical_payload()["statistical_validation"]["primary_metric"] == "net_excess_return"

    changed = _manifest()
    changed["statistical_validation"] = _statistical_validation()
    changed["statistical_validation"]["gates"]["max_holdout_reuse_count"] = 2
    assert parse_manifest(changed).manifest_hash() != baseline_hash


def test_manifest_accepts_wrc_bootstrap_for_official_aligned_panel_generation() -> None:
    payload = _manifest()
    payload["statistical_validation"] = _statistical_validation()
    payload["statistical_validation"]["bootstrap"] = {
        "method": "white_reality_check_block_bootstrap",
        "n_bootstrap": 100,
        "block_length_policy": "fixed",
        "seed_policy": "derived_from_selection_universe_hash",
    }

    manifest = parse_manifest(payload)

    assert manifest.statistical_validation.bootstrap.method == "white_reality_check_block_bootstrap"


def test_production_bound_manifest_rejects_sharpe_like_primary_metric() -> None:
    payload = _production_manifest()
    payload["statistical_validation"]["primary_metric"] = "sharpe_like"

    with pytest.raises(
        ManifestValidationError,
        match="statistical_validation.primary_metric sharpe_like is not allowed for production-bound manifests",
    ):
        parse_manifest(payload)


def test_manifest_parses_stress_suite_and_binds_hash() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["period_ablation"] = {
        "calendar_years": "auto",
        "min_pass_ratio": 0.8,
        "min_return_retention_pct": 50.0,
    }
    payload["stress_suite"]["parameter_perturbation"] = {
        "relative_pct": [-0.2, -0.1, 0.1, 0.2],
        "numeric_params_only": True,
        "min_pass_ratio": 0.75,
    }

    manifest = parse_manifest(payload)
    baseline_hash = manifest.manifest_hash()

    assert manifest.stress_suite is not None
    assert manifest.stress_suite.required_for_promotion is True
    assert manifest.canonical_payload()["stress_suite"]["trade_removal"]["top_n_by_net_pnl"] == [1, 3]
    assert manifest.canonical_payload()["stress_suite"]["period_ablation"]["calendar_years"] == "auto"
    assert manifest.canonical_payload()["stress_suite"]["period_ablation"]["min_return_retention_pct"] == 50.0
    assert manifest.canonical_payload()["stress_suite"]["parameter_perturbation"]["min_pass_ratio"] == 0.75

    changed = _manifest()
    changed["stress_suite"] = _stress_suite()
    changed["stress_suite"]["period_ablation"] = {
        "calendar_years": "auto",
        "min_pass_ratio": 0.8,
        "min_return_retention_pct": 40.0,
    }
    assert parse_manifest(changed).manifest_hash() != baseline_hash


def test_manifest_rejects_unknown_stress_suite_fields() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["unexpected"] = True

    with pytest.raises(ManifestValidationError, match="stress_suite unsupported fields"):
        parse_manifest(payload)


def test_manifest_rejects_invalid_stress_top_n_list() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["trade_removal"]["top_n_by_net_pnl"] = [1, 1]

    with pytest.raises(ManifestValidationError, match="must not contain duplicates"):
        parse_manifest(payload)


def test_manifest_rejects_invalid_stress_probability_threshold() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["trade_order_monte_carlo"]["min_survival_probability"] = 1.5

    with pytest.raises(ManifestValidationError, match="min_survival_probability"):
        parse_manifest(payload)


def test_manifest_rejects_invalid_period_ablation_year_config() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["period_ablation"] = {"calendar_years": [2024, 2024], "min_pass_ratio": 0.8}

    with pytest.raises(ManifestValidationError, match="calendar_years must not contain duplicates"):
        parse_manifest(payload)


@pytest.mark.parametrize("value", [-1.0, 100.1, float("nan")])
def test_manifest_rejects_invalid_period_ablation_return_retention(value: float) -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["period_ablation"] = {
        "calendar_years": "auto",
        "min_pass_ratio": 0.8,
        "min_return_retention_pct": value,
    }

    with pytest.raises(ManifestValidationError, match="min_return_retention_pct"):
        parse_manifest(payload)


def test_manifest_rejects_unknown_period_ablation_field() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["period_ablation"] = {
        "calendar_years": "auto",
        "min_pass_ratio": 0.8,
        "unexpected": True,
    }

    with pytest.raises(ManifestValidationError, match="period_ablation unsupported fields"):
        parse_manifest(payload)


def test_manifest_rejects_invalid_parameter_perturbation_relative_pct() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["parameter_perturbation"] = {"relative_pct": [0.0], "numeric_params_only": True}

    with pytest.raises(ManifestValidationError, match="relative_pct values must be non-zero"):
        parse_manifest(payload)


def test_manifest_rejects_unknown_parameter_perturbation_field() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["parameter_perturbation"] = {"relative_pct": [0.1], "unexpected": True}

    with pytest.raises(ManifestValidationError, match="parameter_perturbation unsupported fields"):
        parse_manifest(payload)


def test_production_bound_manifest_rejects_disabled_stress_suite_gate() -> None:
    payload = _production_manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["required_for_promotion"] = False

    with pytest.raises(ManifestValidationError, match="stress_suite.required_for_promotion must be true"):
        parse_manifest(payload)


@pytest.mark.parametrize("deployment_tier", ["paper_candidate", "live_dry_run_candidate", "small_live_candidate"])
def test_production_bound_manifest_requires_stress_suite(deployment_tier: str) -> None:
    payload = _production_manifest()
    payload["deployment_tier"] = deployment_tier
    payload.pop("stress_suite", None)

    with pytest.raises(ManifestValidationError, match="stress_suite required for production-bound manifests"):
        parse_manifest(payload)


def test_research_only_manifest_can_omit_stress_suite() -> None:
    payload = _manifest()
    payload["deployment_tier"] = "research_only"
    payload.pop("stress_suite", None)

    manifest = parse_manifest(payload)

    assert manifest.stress_suite is None


def test_production_bound_manifest_requires_statistical_validation() -> None:
    payload = _production_manifest()
    payload.pop("statistical_validation")

    with pytest.raises(ManifestValidationError, match="statistical_validation required"):
        parse_manifest(payload)


def test_production_bound_manifest_rejects_malformed_statistical_validation() -> None:
    payload = _production_manifest()
    payload["statistical_validation"]["unexpected"] = True

    with pytest.raises(ManifestValidationError, match="statistical_validation unsupported fields"):
        parse_manifest(payload)


def test_production_bound_manifest_rejects_disabled_statistical_promotion_gate() -> None:
    payload = _production_manifest()
    payload["statistical_validation"]["required_for_promotion"] = False

    with pytest.raises(ManifestValidationError, match="required_for_promotion must be true"):
        parse_manifest(payload)


def test_manifest_parses_optional_metrics_v2_gate_fields() -> None:
    payload = _manifest()
    payload["acceptance_gate"].update(
        {
            "min_cagr_pct": 5.0,
            "min_expectancy_per_trade_krw": 100.0,
            "min_expectancy_per_trade_pct": 0.5,
            "max_exposure_time_pct": 80.0,
            "max_avg_holding_time_minutes": 60.0,
            "max_fee_drag_ratio": 0.01,
            "max_slippage_drag_ratio": 0.02,
            "reject_open_position_at_end": True,
            "metrics_contract_required": True,
        }
    )

    manifest = parse_manifest(payload)
    gate = manifest.acceptance_gate

    assert gate.min_cagr_pct == 5.0
    assert gate.min_expectancy_per_trade_krw == 100.0
    assert gate.max_exposure_time_pct == 80.0
    assert gate.reject_open_position_at_end is True
    assert manifest.canonical_payload()["acceptance_gate"]["metrics_contract_required"] is True


def test_manifest_rejects_unknown_acceptance_gate_fields() -> None:
    payload = _manifest()
    payload["acceptance_gate"]["unexpected_metric_gate"] = 1

    with pytest.raises(ManifestValidationError, match="acceptance_gate unsupported fields"):
        parse_manifest(payload)


def test_manifest_parses_execution_model_scenarios() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.001],
        "slippage_bps": [5, 10],
        "latency_ms": [0, 500],
        "partial_fill_rate": [0.0, 0.1],
        "order_failure_rate": [0.0],
        "market_order_extra_cost_bps": [0, 5],
        "scenario_policy": "must_pass_base_and_survive_stress",
        "seed": 42,
        "calibration_required": True,
    }

    manifest = parse_manifest(payload)

    assert manifest.execution_model.source == "execution_model"
    assert manifest.execution_model.calibration_required is True
    assert len(manifest.execution_model.scenarios) == 16
    assert {scenario.type for scenario in manifest.execution_model.scenarios} == {"stress"}


def test_execution_model_single_generated_scenario_defaults_to_single_scenario_policy() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": [0.0004],
        "slippage_bps": [10],
    }

    manifest = parse_manifest(payload)

    assert len(manifest.execution_model.scenarios) == 1
    assert manifest.execution_model.scenario_policy == "single_scenario"
    assert manifest.execution_model.scenarios[0].scenario_policy == "single_scenario"


def test_execution_model_multiple_generated_scenarios_defaults_to_base_and_stress_policy() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": [0.0004],
        "slippage_bps": [5, 20],
    }

    manifest = parse_manifest(payload)

    assert len(manifest.execution_model.scenarios) == 2
    assert manifest.execution_model.scenario_policy == "must_pass_base_and_survive_stress"
    assert [scenario.scenario_role for scenario in manifest.execution_model.scenarios] == ["base", "stress"]
    assert {scenario.scenario_role_source for scenario in manifest.execution_model.scenarios} == {"derived"}


def test_legacy_cost_model_manifest_keeps_legacy_single_pass_policy() -> None:
    manifest = parse_manifest(_manifest())

    assert manifest.execution_model.source == "legacy_cost_model"
    assert manifest.execution_model.scenario_policy == "legacy_cost_model_single_pass"


def test_manifest_supplied_scenario_role_is_applied_to_generated_scenarios() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": [0.0004],
        "slippage_bps": [5, 20],
        "scenario_role": "base",
    }

    manifest = parse_manifest(payload)

    assert {scenario.scenario_role for scenario in manifest.execution_model.scenarios} == {"base"}
    assert {scenario.scenario_role_source for scenario in manifest.execution_model.scenarios} == {"manifest"}


@pytest.mark.parametrize("role", ["base", "stress"])
def test_manifest_rejects_scalar_role_conflicting_with_base_and_stress_policy(role: str) -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": [0.0004],
        "slippage_bps": [5, 20],
        "scenario_policy": "must_pass_base_and_survive_stress",
        "scenario_role": role,
    }

    with pytest.raises(
        ManifestValidationError,
        match="execution_model.scenario_role conflicts with must_pass_base_and_survive_stress",
    ):
        parse_manifest(payload)


def test_manifest_allows_scalar_role_with_single_scenario_policy_for_legacy_parse_contract() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": [0.0004],
        "slippage_bps": [5, 20],
        "scenario_policy": "single_scenario",
        "scenario_role": "base",
    }

    manifest = parse_manifest(payload)

    assert manifest.execution_model.scenario_policy == "single_scenario"
    assert len(manifest.execution_model.scenarios) == 2
    assert {scenario.scenario_role for scenario in manifest.execution_model.scenarios} == {"base"}


def test_manifest_allows_derived_roles_with_base_and_stress_policy() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": [0.0004],
        "slippage_bps": [5, 20],
        "scenario_policy": "must_pass_base_and_survive_stress",
    }

    manifest = parse_manifest(payload)

    assert [scenario.scenario_role for scenario in manifest.execution_model.scenarios] == ["base", "stress"]
    assert {scenario.scenario_role_source for scenario in manifest.execution_model.scenarios} == {"derived"}


def test_manifest_rejects_invalid_scenario_role() -> None:
    payload = _manifest()
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": [0.0004],
        "slippage_bps": [5],
        "scenario_role": "primary",
    }

    with pytest.raises(ManifestValidationError, match="execution_model.scenario_role"):
        parse_manifest(payload)


def test_manifest_parses_valid_walk_forward_config() -> None:
    payload = _manifest()
    payload["walk_forward"] = {
        "train_window_days": 2,
        "test_window_days": 1,
        "step_days": 1,
        "min_windows": 1,
    }

    manifest = parse_manifest(payload)

    assert manifest.walk_forward is not None
    assert manifest.walk_forward.train_window_days == 2


def test_manifest_parses_regime_acceptance_gate() -> None:
    payload = _manifest()
    payload["acceptance_gate"]["regime_acceptance_gate"] = {
        "required": True,
        "min_trade_count_per_required_regime": 10,
        "required_regimes": ["uptrend"],
        "blocked_regimes": ["sideways_low_vol_volume_decreasing"],
        "blocked_regime_max_trade_count": 0,
        "blocked_regime_max_net_pnl_loss_krw": 0,
        "min_profit_factor_by_regime": {"uptrend": 1.2},
        "max_loss_share_by_single_regime": 0.4,
        "max_pnl_dependency_by_single_regime": 0.5,
    }

    manifest = parse_manifest(payload)

    gate = manifest.acceptance_gate.regime_acceptance_gate
    assert gate.required is True
    assert gate.required_regimes == ("uptrend",)
    assert gate.blocked_regimes == ("sideways_low_vol_volume_decreasing",)


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda payload: payload.pop("hypothesis"), "hypothesis"),
        (lambda payload: payload["dataset"].pop("validation"), "dataset.validation"),
        (lambda payload: payload.__setitem__("parameter_space", {}), "parameter_space"),
        (
            lambda payload: payload["dataset"]["train"].__setitem__("start", "2023-01-03"),
            "dataset.train.start",
        ),
        (
            lambda payload: payload["acceptance_gate"].__setitem__("min_trade_count", 0),
            "acceptance_gate.min_trade_count",
        ),
        (
            lambda payload: payload.__setitem__(
                "walk_forward",
                {"train_window_days": 0, "test_window_days": 1, "step_days": 1, "min_windows": 1},
            ),
            "walk_forward.train_window_days",
        ),
        (
            lambda payload: (
                payload["acceptance_gate"].__setitem__("walk_forward_required", True),
                payload.pop("walk_forward", None),
            ),
            "walk_forward is required",
        ),
    ],
)
def test_manifest_validation_rejects_invalid_contract(mutate, expected: str) -> None:
    payload = _manifest()
    mutate(payload)

    with pytest.raises(ManifestValidationError, match=expected):
        parse_manifest(payload)
