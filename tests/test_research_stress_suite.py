from __future__ import annotations

import json
from datetime import datetime, timezone

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.metrics_contract import ClosedTradeRecord
from bithumb_bot.research.stress_suite import (
    StressSuiteContext,
    analyze_stress_suite,
    stress_suite_required_for_candidate,
    _trade_summary,
    validate_stress_suite_evidence_for_candidate,
)


def _contract_payload() -> dict[str, object]:
    return {
        "experiment_id": "stress_unit",
        "hypothesis": "stress suite unit manifest",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "unit",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
        },
        "parameter_space": {"SMA_SHORT": [2], "SMA_LONG": [4]},
        "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 90,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": False,
            "parameter_stability_required": False,
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
            "risk_adjusted_score": {
                "required_metrics": ["calmar"],
                "ranking": ["pass_gate", "max_calmar"],
            },
        },
    }


def _context() -> StressSuiteContext:
    return StressSuiteContext(
        manifest_hash="sha256:manifest",
        experiment_id="stress_unit",
        candidate_id="candidate_001",
        scenario_id="scenario_001",
        split_name="validation",
        parameter_values={"SMA_SHORT": 2, "SMA_LONG": 4},
    )


def _metrics_v2() -> dict[str, object]:
    return {
        "metrics_schema_version": 2,
        "return_risk": {"cagr_pct": 20.0, "max_drawdown_pct": 10.0},
        "trade_quality": {},
    }


def _trades(values: list[float]) -> tuple[ClosedTradeRecord, ...]:
    return tuple(
        ClosedTradeRecord(exit_ts=1_700_000_000_000 + index, net_pnl=value, entry_notional=100_000.0)
        for index, value in enumerate(values)
    )


def _trade(exit_year: int, net_pnl: float, index: int = 0) -> ClosedTradeRecord:
    ts = int(datetime(exit_year, 6, 1, tzinfo=timezone.utc).timestamp() * 1000) + index
    return ClosedTradeRecord(exit_ts=ts, net_pnl=net_pnl, entry_notional=100_000.0)


def test_top_n_trade_removal_fails_single_huge_winner_dependency() -> None:
    manifest = parse_manifest(_contract_payload())

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([100_000.0, -1_000.0, -1_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
    )

    assert result["gate_result"] == "FAIL"
    assert "stress_trade_removal_return_retention_failed" in result["fail_reasons"]
    json.dumps(result, allow_nan=False)


def test_distributed_profits_stress_suite_is_deterministic_and_passes() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["trade_removal"]["min_return_retention_pct"] = 40.0
    payload["stress_suite"]["trade_order_monte_carlo"]["ruin_max_drawdown_pct"] = 80.0
    manifest = parse_manifest(payload)

    kwargs = {
        "contract": manifest.stress_suite,
        "context": _context(),
        "original_metrics": {"return_pct": 10.0},
        "metrics_v2": _metrics_v2(),
        "closed_trades": _trades([10_000.0, 9_000.0, 8_000.0, -2_000.0, 7_000.0, -1_000.0]),
        "starting_cash": 1_000_000.0,
    }
    first = analyze_stress_suite(**kwargs)
    second = analyze_stress_suite(**kwargs)

    assert first == second
    assert first["gate_result"] == "PASS"
    assert first["stress_suite_hash"].startswith("sha256:")
    assert first["trade_order_monte_carlo"]["terminal_equity_p05"] is not None
    assert first["trade_order_monte_carlo"]["max_drawdown_pct_p95"] is not None
    assert first["trade_order_monte_carlo"]["limitations"] == [
        "monte_carlo_does_not_reconstruct_intratrade_equity_path",
        "monte_carlo_uses_closed_trade_pnl_not_bar_return_series",
    ]
    assert first["limitations"] == [
        "monte_carlo_does_not_reconstruct_intratrade_equity_path",
        "monte_carlo_uses_closed_trade_pnl_not_bar_return_series",
        "sharpe_unavailable_without_period_return_series",
        "sortino_unavailable_without_period_return_series",
    ]
    json.dumps(first, allow_nan=False)


def test_stress_suite_hash_binds_portfolio_policy_context_and_starting_cash() -> None:
    manifest = parse_manifest(_contract_payload())
    base_context = _context()
    policy_context = StressSuiteContext(
        manifest_hash=base_context.manifest_hash,
        experiment_id=base_context.experiment_id,
        candidate_id=base_context.candidate_id,
        scenario_id=base_context.scenario_id,
        split_name=base_context.split_name,
        parameter_values=base_context.parameter_values,
        portfolio_policy_hash="sha256:portfolio-a",
        simulation_policy_hash="sha256:simulation-a",
    )
    changed_context = StressSuiteContext(
        manifest_hash=base_context.manifest_hash,
        experiment_id=base_context.experiment_id,
        candidate_id=base_context.candidate_id,
        scenario_id=base_context.scenario_id,
        split_name=base_context.split_name,
        parameter_values=base_context.parameter_values,
        portfolio_policy_hash="sha256:portfolio-b",
        simulation_policy_hash="sha256:simulation-b",
    )

    first = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=policy_context,
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, 8_000.0, -2_000.0]),
        starting_cash=1_000_000.0,
    )
    changed_policy = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=changed_context,
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, 8_000.0, -2_000.0]),
        starting_cash=1_000_000.0,
    )
    changed_cash = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=policy_context,
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, 8_000.0, -2_000.0]),
        starting_cash=2_000_000.0,
    )

    assert first["context"]["portfolio_policy_hash"] == "sha256:portfolio-a"
    assert first["starting_cash"] == 1_000_000.0
    assert changed_policy["stress_suite_hash"] != first["stress_suite_hash"]
    assert changed_cash["stress_suite_hash"] != first["stress_suite_hash"]


def test_no_closed_trades_fails_with_stable_reason() -> None:
    manifest = parse_manifest(_contract_payload())

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 0.0},
        metrics_v2=_metrics_v2(),
        closed_trades=(),
        starting_cash=1_000_000.0,
    )

    assert result["gate_result"] == "FAIL"
    assert "stress_trade_removal_no_closed_trades" in result["fail_reasons"]
    assert "stress_monte_carlo_no_closed_trades" in result["fail_reasons"]


def test_risk_adjusted_score_required_calmar_missing_fails() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["trade_removal"]["min_return_retention_pct"] = 0.0
    payload["stress_suite"]["trade_order_monte_carlo"]["ruin_max_drawdown_pct"] = 80.0
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 1.0},
        metrics_v2={"metrics_schema_version": 2, "return_risk": {"cagr_pct": None, "max_drawdown_pct": 0.0}},
        closed_trades=_trades([10_000.0, -1_000.0, 9_000.0]),
        starting_cash=1_000_000.0,
    )

    assert result["risk_adjusted_score"]["status"] == "FAIL"
    assert "stress_risk_adjusted_calmar_missing" in result["fail_reasons"]


def test_required_sharpe_and_sortino_missing_fail_explicitly() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["trade_removal"]["min_return_retention_pct"] = 0.0
    payload["stress_suite"]["trade_order_monte_carlo"]["ruin_max_drawdown_pct"] = 80.0
    payload["stress_suite"]["risk_adjusted_score"]["required_metrics"] = ["sharpe", "sortino"]
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 1.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, -1_000.0, 9_000.0]),
        starting_cash=1_000_000.0,
    )

    assert result["risk_adjusted_score"]["sharpe_ratio"] is None
    assert result["risk_adjusted_score"]["sortino_ratio"] is None
    assert "stress_risk_adjusted_sharpe_missing" in result["fail_reasons"]
    assert "stress_risk_adjusted_sortino_missing" in result["fail_reasons"]


def test_optional_sharpe_sortino_limitations_do_not_fail_when_unrequired() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["trade_removal"]["min_return_retention_pct"] = 0.0
    payload["stress_suite"]["trade_order_monte_carlo"]["ruin_max_drawdown_pct"] = 80.0
    payload["stress_suite"]["risk_adjusted_score"]["required_metrics"] = ["calmar"]
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 1.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, -1_000.0, 9_000.0]),
        starting_cash=1_000_000.0,
    )

    assert result["risk_adjusted_score"]["status"] == "PASS"
    assert "stress_risk_adjusted_sharpe_missing" not in result["fail_reasons"]
    assert "stress_risk_adjusted_sortino_missing" not in result["fail_reasons"]
    assert "sharpe_unavailable_without_period_return_series" in result["risk_adjusted_score"]["limitations"]
    assert "sortino_unavailable_without_period_return_series" in result["risk_adjusted_score"]["limitations"]


def test_trade_order_monte_carlo_strict_drawdown_threshold_fails() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["trade_removal"]["min_return_retention_pct"] = 0.0
    payload["stress_suite"]["trade_order_monte_carlo"]["ruin_max_drawdown_pct"] = 0.01
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([20_000.0, -15_000.0, 20_000.0, -15_000.0, 20_000.0]),
        starting_cash=1_000_000.0,
    )

    assert result["trade_order_monte_carlo"]["status"] == "FAIL"
    assert "stress_monte_carlo_survival_probability_failed" in result["fail_reasons"]


def test_period_ablation_passes_leave_one_calendar_year_out() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["period_ablation"] = {
        "calendar_years": "auto",
        "min_pass_ratio": 0.8,
        "min_return_retention_pct": 20.0,
    }
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=(
            _trade(2023, 20_000.0, 1),
            _trade(2023, -5_000.0, 2),
            _trade(2024, 10_000.0, 3),
            _trade(2024, -5_000.0, 4),
        ),
        starting_cash=1_000_000.0,
    )

    assert result["period_ablation"]["status"] == "PASS"
    assert result["period_ablation"]["method"] == "leave_one_calendar_year_out_closed_trade_exit_year"
    assert result["period_ablation"]["calendar_years"] == [2023, 2024]
    assert result["period_ablation"]["min_return_retention_pct"] == 20.0
    assert result["period_ablation"]["pass_ratio"] == 1.0
    assert result["period_ablation"]["limitations"] == [
        "period_ablation_uses_closed_trade_exit_year_not_full_signal_rerun"
    ]
    json.dumps(result, allow_nan=False)


def test_period_ablation_fails_when_year_removal_destroys_return_retention() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["period_ablation"] = {
        "calendar_years": [2023, 2024],
        "min_pass_ratio": 1.0,
        "min_return_retention_pct": 50.0,
    }
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=(
            _trade(2023, 99_000.0, 1),
            _trade(2024, 1_000.0, 2),
        ),
        starting_cash=1_000_000.0,
    )

    assert result["period_ablation"]["status"] == "FAIL"
    assert result["period_ablation"]["pass_ratio"] == 0.5
    assert result["period_ablation"]["cases"][0]["return_retention_pct"] == 1.0
    assert result["period_ablation"]["cases"][0]["fail_reasons"] == [
        "stress_period_ablation_return_retention_failed"
    ]
    assert "stress_period_ablation_pass_ratio_failed" in result["fail_reasons"]
    assert "stress_period_ablation_return_retention_failed" in result["fail_reasons"]
    json.dumps(result, allow_nan=False)


def test_required_stress_suite_rejects_period_ablation_retention_failure() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["period_ablation"] = {
        "calendar_years": [2023, 2024],
        "min_pass_ratio": 1.0,
        "min_return_retention_pct": 50.0,
    }
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=(
            _trade(2023, 99_000.0, 1),
            _trade(2024, 1_000.0, 2),
        ),
        starting_cash=1_000_000.0,
    )
    candidate = {
        "stress_suite_required": True,
        "stress_suite_contract": manifest.stress_suite.as_dict(),
        "stress_suite_contract_hash": result["contract_hash"],
        "stress_suite_gate_result": result["gate_result"],
        "validation_stress_suite": dict(result),
        "final_holdout_present": False,
        "final_holdout_required_for_promotion": False,
    }

    assert result["gate_result"] == "FAIL"
    assert "stress_period_ablation_return_retention_failed" in result["fail_reasons"]
    assert "stress_suite_gate_not_passed" in validate_stress_suite_evidence_for_candidate(candidate, {})


def test_period_ablation_passes_when_return_retention_meets_threshold() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["period_ablation"] = {
        "calendar_years": [2023, 2024],
        "min_pass_ratio": 1.0,
        "min_return_retention_pct": 40.0,
    }
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=(
            _trade(2023, 30_000.0, 1),
            _trade(2024, 20_000.0, 2),
        ),
        starting_cash=1_000_000.0,
    )

    assert result["period_ablation"]["status"] == "PASS"
    assert result["period_ablation"]["pass_ratio"] == 1.0
    assert result["period_ablation"]["min_pass_ratio"] == 1.0
    assert result["period_ablation"]["min_return_retention_pct"] == 40.0
    assert result["period_ablation"]["fail_reasons"] == []
    json.dumps(result, allow_nan=False)


def test_period_ablation_fails_closed_without_usable_exit_timestamps() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["period_ablation"] = {"calendar_years": "auto", "min_pass_ratio": 0.8}
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=(ClosedTradeRecord(exit_ts=0, net_pnl=10_000.0, entry_notional=100_000.0),),
        starting_cash=1_000_000.0,
    )

    assert result["period_ablation"]["status"] == "FAIL"
    assert result["period_ablation"]["min_return_retention_pct"] == 50.0
    assert "stress_period_ablation_exit_timestamp_missing" in result["fail_reasons"]


def test_parameter_perturbation_passes_with_existing_grid_matches() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["parameter_perturbation"] = {"relative_pct": [-0.1, 0.1], "numeric_params_only": True}
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=StressSuiteContext(
            manifest_hash="sha256:manifest",
            experiment_id="stress_unit",
            candidate_id="candidate_base",
            scenario_id="scenario_001",
            split_name="validation",
            parameter_values={"SMA_SHORT": 10},
        ),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
        parameter_perturbation_candidates=(
            {
                "candidate_id": "candidate_low",
                "parameter_values": {"SMA_SHORT": 9},
                "validation_metrics": {"return_pct": 2.0, "max_drawdown_pct": 3.0},
                "final_holdout_metrics": {"return_pct": 1.0},
                "scenario_acceptance_gate_result": "PASS",
                "scenario_fail_reasons": [],
            },
            {
                "candidate_id": "candidate_high",
                "parameter_values": {"SMA_SHORT": 11},
                "validation_metrics": {"return_pct": 2.5, "max_drawdown_pct": 3.5},
                "final_holdout_metrics": {"return_pct": 1.5},
                "scenario_acceptance_gate_result": "PASS",
                "scenario_fail_reasons": [],
            },
        ),
    )

    assert result["parameter_perturbation"]["status"] == "PASS"
    assert result["parameter_perturbation"]["pass_ratio"] == 1.0
    assert result["parameter_perturbation"]["limitations"] == [
        "parameter_perturbation_uses_existing_grid_candidates_not_synthetic_reruns"
    ]


def test_parameter_perturbation_fails_when_grid_match_missing() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["parameter_perturbation"] = {
        "relative_pct": [-0.1, 0.1],
        "numeric_params_only": True,
        "min_pass_ratio": 1.0,
    }
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=StressSuiteContext(
            manifest_hash="sha256:manifest",
            experiment_id="stress_unit",
            candidate_id="candidate_base",
            scenario_id="scenario_001",
            split_name="validation",
            parameter_values={"SMA_SHORT": 10},
        ),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
        parameter_perturbation_candidates=(
            {
                "candidate_id": "candidate_low",
                "parameter_values": {"SMA_SHORT": 9},
                "validation_metrics": {"return_pct": 2.0, "max_drawdown_pct": 3.0},
                "scenario_acceptance_gate_result": "PASS",
                "scenario_fail_reasons": [],
            },
        ),
    )

    assert result["parameter_perturbation"]["status"] == "FAIL"
    assert "stress_parameter_perturbation_candidate_missing" in result["fail_reasons"]
    assert "stress_parameter_perturbation_pass_ratio_failed" in result["fail_reasons"]


def test_parameter_perturbation_fails_when_pass_ratio_below_threshold() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["parameter_perturbation"] = {
        "relative_pct": [-0.1, 0.1],
        "numeric_params_only": True,
        "min_pass_ratio": 1.0,
    }
    payload["stress_suite"].pop("trade_removal")
    payload["stress_suite"].pop("trade_order_monte_carlo")
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=StressSuiteContext(
            manifest_hash="sha256:manifest",
            experiment_id="stress_unit",
            candidate_id="candidate_base",
            scenario_id="scenario_001",
            split_name="validation",
            parameter_values={"SMA_SHORT": 10},
        ),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
        parameter_perturbation_candidates=(
            {
                "candidate_id": "candidate_low",
                "parameter_values": {"SMA_SHORT": 9},
                "validation_metrics": {"return_pct": 2.0, "max_drawdown_pct": 3.0},
                "scenario_acceptance_gate_result": "PASS",
                "scenario_fail_reasons": [],
            },
            {
                "candidate_id": "candidate_high",
                "parameter_values": {"SMA_SHORT": 11},
                "validation_metrics": {"return_pct": -2.0, "max_drawdown_pct": 20.0},
                "scenario_acceptance_gate_result": "FAIL",
                "scenario_fail_reasons": ["validation_return_not_positive"],
            },
        ),
    )

    assert result["parameter_perturbation"]["status"] == "FAIL"
    assert "stress_parameter_perturbation_constraint_invalid" in result["fail_reasons"]
    assert "stress_parameter_perturbation_pass_ratio_failed" in result["fail_reasons"]


def test_required_stress_evidence_validation_refuses_missing_and_hash_mismatch() -> None:
    manifest = parse_manifest(_contract_payload())
    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, 8_000.0, -2_000.0, 7_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
    )
    candidate = {
        "stress_suite_required": True,
        "stress_suite_contract": manifest.stress_suite.as_dict(),
        "stress_suite_contract_hash": result["contract_hash"],
        "stress_suite_gate_result": "PASS",
        "validation_stress_suite": dict(result),
        "final_holdout_present": False,
    }

    assert validate_stress_suite_evidence_for_candidate(candidate, {}) == []

    candidate["validation_stress_suite"]["gate_result"] = "FAIL"
    assert "stress_suite_hash_mismatch" in validate_stress_suite_evidence_for_candidate(candidate, {})

    candidate.pop("validation_stress_suite")
    assert "stress_suite_required_but_missing" in validate_stress_suite_evidence_for_candidate(candidate, {})


def test_production_bound_candidate_requires_stress_suite_even_when_flag_missing_or_false() -> None:
    missing = {
        "deployment_tier": "paper_candidate",
        "final_holdout_present": False,
        "final_holdout_required_for_promotion": False,
    }
    disabled = dict(missing, stress_suite_required=False)

    assert stress_suite_required_for_candidate(missing, {}) is True
    assert stress_suite_required_for_candidate(disabled, {}) is True
    assert "stress_suite_required_but_missing" in validate_stress_suite_evidence_for_candidate(missing, {})
    assert "stress_suite_required_but_missing" in validate_stress_suite_evidence_for_candidate(disabled, {})


def test_report_level_production_tier_requires_stress_suite_when_candidate_tier_missing() -> None:
    candidate = {
        "parameter_candidate_id": "candidate_legacy",
        "final_holdout_present": False,
        "final_holdout_required_for_promotion": False,
    }
    report = {"deployment_tier": "paper_candidate"}

    reasons = validate_stress_suite_evidence_for_candidate(candidate, report)

    assert stress_suite_required_for_candidate(candidate, report) is True
    assert reasons
    assert "stress_suite_contract_mismatch" in reasons
    assert "stress_suite_gate_not_passed" in reasons
    assert "stress_suite_hash_missing" in reasons
    assert "stress_suite_required_but_missing" in reasons


def test_research_only_candidate_does_not_require_stress_suite_without_flag() -> None:
    candidate = {
        "deployment_tier": "research_only",
        "final_holdout_present": False,
        "final_holdout_required_for_promotion": False,
    }

    assert stress_suite_required_for_candidate(candidate, {}) is False
    assert validate_stress_suite_evidence_for_candidate(candidate, {}) == []


def test_trade_summary_win_rate_uses_ratio_units() -> None:
    summary = _trade_summary(_trades([10_000.0, -5_000.0]), starting_cash=1_000_000.0)

    assert summary["win_rate"] == 0.5


def test_required_final_holdout_stress_evidence_is_fail_closed() -> None:
    manifest = parse_manifest(_contract_payload())
    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, 8_000.0, -2_000.0, 7_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
    )
    candidate = {
        "stress_suite_required": True,
        "stress_suite_contract": manifest.stress_suite.as_dict(),
        "stress_suite_contract_hash": result["contract_hash"],
        "stress_suite_gate_result": "PASS",
        "validation_stress_suite": dict(result),
        "final_holdout_present": True,
        "final_holdout_required_for_promotion": False,
    }

    reasons = validate_stress_suite_evidence_for_candidate(candidate, {})

    assert "final_holdout_stress_suite_required_but_missing" in reasons


def test_candidate_level_stress_contract_is_required_even_when_report_has_contract() -> None:
    manifest = parse_manifest(_contract_payload())
    contract = manifest.stress_suite.as_dict()
    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, 8_000.0, -2_000.0, 7_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
    )
    contract_hash = result["contract_hash"]
    candidate = {
        "stress_suite_required": True,
        "stress_suite_contract_hash": contract_hash,
        "stress_suite_gate_result": "PASS",
        "validation_stress_suite": dict(result),
        "final_holdout_present": False,
        "final_holdout_required_for_promotion": False,
    }

    reasons = validate_stress_suite_evidence_for_candidate(
        candidate,
        {"stress_suite_contract": contract, "stress_suite_contract_hash": contract_hash},
    )

    assert "stress_suite_contract_mismatch" in reasons
