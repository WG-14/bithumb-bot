from __future__ import annotations

import json

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.metrics_contract import ClosedTradeRecord
from bithumb_bot.research.stress_suite import (
    StressSuiteContext,
    analyze_stress_suite,
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


def test_declared_unimplemented_sections_fail_closed() -> None:
    payload = _contract_payload()
    payload["stress_suite"]["period_ablation"] = {"calendar_years": "auto", "min_pass_ratio": 0.8}
    payload["stress_suite"]["parameter_perturbation"] = {"relative_pct": [-0.1, 0.1], "numeric_params_only": True}
    manifest = parse_manifest(payload)

    result = analyze_stress_suite(
        contract=manifest.stress_suite,
        context=_context(),
        original_metrics={"return_pct": 10.0},
        metrics_v2=_metrics_v2(),
        closed_trades=_trades([10_000.0, 9_000.0, 8_000.0, -2_000.0, 7_000.0, -1_000.0]),
        starting_cash=1_000_000.0,
    )

    assert "stress_period_ablation_not_implemented" in result["fail_reasons"]
    assert "stress_parameter_perturbation_not_implemented" in result["fail_reasons"]


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
