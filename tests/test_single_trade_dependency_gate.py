from __future__ import annotations

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.metrics_contract import METRICS_SCHEMA_VERSION
from bithumb_bot.research.metrics_gate_policy import metrics_gate_policy_from_acceptance_gate, metrics_gate_policy_hash
from bithumb_bot.research.validation_protocol import _metrics_v2_gate_reasons


def _production_manifest() -> dict[str, object]:
    return {
        "experiment_id": "single_trade_dependency_test",
        "hypothesis": "Single-trade dependency is gated.",
        "strategy_name": "sma_with_filter",
        "deployment_tier": "paper_candidate",
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
            "SMA_SHORT": [7],
            "SMA_LONG": [30],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0012],
        },
        "cost_model": {"fee_rate": 0.0004, "slippage_bps": [10]},
        "execution_model": {
            "scenario_policy": "must_pass_base_and_survive_stress",
            "scenarios": [
                {
                    "scenario_role": "base",
                    "label": "realistic_bithumb_app_fee_0004",
                    "fee_rate": 0.0004,
                    "fee_source": "operator_declared_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "execution_calibration",
                    "promotable_as_base": True,
                },
                {
                    "scenario_role": "stress",
                    "label": "stress_fee_0025_slippage_20bps",
                    "fee_rate": 0.0025,
                    "fee_source": "stress_assumption",
                    "slippage_bps": 20,
                    "slippage_source": "stress_assumption",
                    "promotable_as_base": False,
                },
            ],
            "calibration_required": True,
        },
        "execution_timing": {
            "fill_reference_policy": "next_candle_open",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "candle_next_open",
        },
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
        "stress_suite": {
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
        },
    }


def _metrics(score: float) -> dict[str, object]:
    return {
        "metrics_schema_version": METRICS_SCHEMA_VERSION,
        "return_risk": {"cagr_pct": 1.0, "open_position_at_end": False},
        "trade_quality": {
            "expectancy_per_trade_krw": 1.0,
            "single_trade_dependency_score": score,
        },
        "time_exposure": {},
        "cost_execution": {},
    }


def test_single_trade_dependency_score_one_fails_production_bound_acceptance() -> None:
    manifest = parse_manifest(_production_manifest())

    reasons = _metrics_v2_gate_reasons(gate=manifest.acceptance_gate, metrics_v2=_metrics(1.0), prefix="")

    assert "max_single_trade_dependency_score_failed" in reasons


def test_single_trade_dependency_score_below_threshold_passes() -> None:
    manifest = parse_manifest(_production_manifest())

    reasons = _metrics_v2_gate_reasons(gate=manifest.acceptance_gate, metrics_v2=_metrics(0.5), prefix="")

    assert "max_single_trade_dependency_score_failed" not in reasons


def test_single_trade_dependency_threshold_is_in_metrics_policy_hash() -> None:
    manifest = parse_manifest(_production_manifest())
    policy = metrics_gate_policy_from_acceptance_gate(manifest.acceptance_gate)
    original_hash = metrics_gate_policy_hash(policy)

    changed = dict(policy)
    changed["max_single_trade_dependency_score"] = 0.5

    assert policy["max_single_trade_dependency_score"] == 0.8
    assert metrics_gate_policy_hash(changed) != original_hash
