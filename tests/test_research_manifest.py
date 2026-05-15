from __future__ import annotations

import pytest

from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest


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


def _production_manifest() -> dict[str, object]:
    payload = _manifest()
    payload["deployment_tier"] = "paper_candidate"
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
    return payload


def test_manifest_parses_required_contract() -> None:
    manifest = parse_manifest(_manifest())

    assert manifest.experiment_id == "sma_filter_v1_2026_05"
    assert manifest.hypothesis
    assert manifest.manifest_hash().startswith("sha256:")
    assert manifest.execution_model.source == "legacy_cost_model"
    assert manifest.execution_model.scenarios[0].type == "fixed_bps"
    assert manifest.execution_model.scenarios[0].slippage_bps == 0.0


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


def test_manifest_parses_stress_suite_and_binds_hash() -> None:
    payload = _manifest()
    payload["stress_suite"] = _stress_suite()

    manifest = parse_manifest(payload)
    baseline_hash = manifest.manifest_hash()

    assert manifest.stress_suite is not None
    assert manifest.stress_suite.required_for_promotion is True
    assert manifest.canonical_payload()["stress_suite"]["trade_removal"]["top_n_by_net_pnl"] == [1, 3]

    changed = _manifest()
    changed["stress_suite"] = _stress_suite()
    changed["stress_suite"]["trade_order_monte_carlo"]["min_survival_probability"] = 0.9
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


def test_production_bound_manifest_rejects_disabled_stress_suite_gate() -> None:
    payload = _production_manifest()
    payload["stress_suite"] = _stress_suite()
    payload["stress_suite"]["required_for_promotion"] = False

    with pytest.raises(ManifestValidationError, match="stress_suite.required_for_promotion must be true"):
        parse_manifest(payload)


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
