from __future__ import annotations

import pytest

from bithumb_bot.research.execution_calibration import (
    ExecutionCalibrationError,
    build_calibration_artifact,
    compare_calibration_to_scenario,
    validate_calibration_artifact,
)
from bithumb_bot.execution_reality_contract import build_execution_reality_contract
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.validation_protocol import _execution_reality_contract


def test_calibration_artifact_schema_is_hash_validated() -> None:
    contract = _execution_contract()
    artifact = build_calibration_artifact(
        summary={
            "sample_count": 40,
            "median_slippage_vs_signal_bps": 4.0,
            "p90_slippage_vs_signal_bps": 12.0,
            "p95_slippage_vs_signal_bps": 18.0,
            "p95_submit_to_fill_ms": 1500,
            "partial_fill_rate": 0.02,
            "unfilled_rate": 0.01,
            "model_breach_rate": 0.03,
            "quality_gate_status": "PASS",
            "execution_reality_contract": contract,
            "execution_contract_hash": contract["execution_contract_hash"],
            "execution_contract_hashes": [contract["execution_contract_hash"]],
            "execution_contract_hash_present": True,
        },
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-03T00:00:00+00:00",
    )

    validated = validate_calibration_artifact(artifact)

    assert validated["artifact_type"] == "execution_cost_calibration"
    assert validated["content_hash"].startswith("sha256:")
    assert validated["recommended_research_cost_model"]["slippage_bps"]
    assert validated["execution_contract_hash"] == contract["execution_contract_hash"]


def test_calibration_hash_mismatch_is_rejected() -> None:
    artifact = build_calibration_artifact(
        summary={"sample_count": 1, "quality_gate_status": "PASS"},
        market="KRW-BTC",
        interval="1m",
    )
    artifact["sample_count"] = 2

    with pytest.raises(ExecutionCalibrationError, match="content_hash_mismatch"):
        validate_calibration_artifact(artifact)


def test_calibration_comparison_fails_when_observed_costs_exceed_assumptions() -> None:
    artifact = build_calibration_artifact(
        summary={
            "sample_count": 40,
            "p90_slippage_vs_signal_bps": 12.0,
            "p95_slippage_vs_signal_bps": 18.0,
            "p95_submit_to_fill_ms": 2500,
            "model_breach_rate": 0.0,
            "quality_gate_status": "PASS",
        },
        market="KRW-BTC",
        interval="1m",
    )

    result = compare_calibration_to_scenario(
        calibration=artifact,
        assumed_slippage_bps=10.0,
        assumed_latency_ms=3000,
    )

    assert result["status"] == "FAIL"
    assert "execution_calibration_p90_slippage_exceeds_assumption" in result["reasons"]


def test_calibration_comparison_fails_on_market_and_interval_mismatch() -> None:
    artifact = build_calibration_artifact(
        summary={"sample_count": 40, "quality_gate_status": "PASS"},
        market="KRW-ETH",
        interval="5m",
    )

    result = compare_calibration_to_scenario(
        calibration=artifact,
        assumed_slippage_bps=10.0,
        assumed_latency_ms=3000,
        expected_market="KRW-BTC",
        expected_interval="1m",
    )

    assert result["status"] == "FAIL"
    assert "execution_calibration_market_mismatch" in result["reasons"]
    assert "execution_calibration_interval_mismatch" in result["reasons"]


def test_required_calibration_without_content_hash_fails_closed() -> None:
    artifact = build_calibration_artifact(
        summary={"sample_count": 40, "quality_gate_status": "PASS"},
        market="KRW-BTC",
        interval="1m",
    )
    artifact.pop("content_hash")

    result = compare_calibration_to_scenario(
        calibration=artifact,
        assumed_slippage_bps=10.0,
        assumed_latency_ms=3000,
        expected_market="KRW-BTC",
        expected_interval="1m",
        require_content_hash=True,
    )

    assert result["status"] == "FAIL"
    assert "execution_calibration_content_hash_missing" in result["reasons"]


def _artifact(**overrides):
    summary = {
        "sample_count": 50,
        "median_slippage_vs_signal_bps": 1.0,
        "p90_slippage_vs_signal_bps": 2.0,
        "p95_slippage_vs_signal_bps": 3.0,
        "p95_submit_to_fill_ms": 100,
        "partial_fill_rate": 0.0,
        "unfilled_rate": 0.0,
        "model_breach_rate": 0.0,
        "quality_gate_status": "PASS",
    }
    summary.update(overrides)
    return build_calibration_artifact(
        summary=summary,
        market="KRW-BTC",
        interval="1m",
        generated_at="2026-05-07T00:00:00+00:00",
    )


def _compare(calibration, **overrides):
    kwargs = {
        "calibration": calibration,
        "assumed_slippage_bps": 5.0,
        "assumed_latency_ms": 200,
        "assumed_partial_fill_rate": 0.0,
        "assumed_order_failure_rate": 0.0,
        "expected_market": "KRW-BTC",
        "expected_interval": "1m",
        "require_content_hash": True,
        "min_sample_count": 30,
        "require_quality_gate_pass": True,
    }
    kwargs.update(overrides)
    return compare_calibration_to_scenario(**kwargs)


def test_calibration_fails_when_partial_fill_rate_exceeds_assumption() -> None:
    result = _compare(_artifact(partial_fill_rate=0.01))

    assert result["status"] == "FAIL"
    assert "execution_calibration_partial_fill_rate_exceeds_assumption" in result["reasons"]


def test_calibration_fails_when_unfilled_rate_exceeds_assumption() -> None:
    result = _compare(_artifact(unfilled_rate=0.02))

    assert result["status"] == "FAIL"
    assert "execution_calibration_unfilled_rate_exceeds_assumption" in result["reasons"]


def test_calibration_fails_when_sample_count_below_required() -> None:
    result = _compare(_artifact(sample_count=29))

    assert result["status"] == "FAIL"
    assert "execution_calibration_sample_count_below_required" in result["reasons"]


def test_calibration_fails_when_quality_gate_did_not_pass() -> None:
    result = _compare(_artifact(quality_gate_status="FAIL"))

    assert result["status"] == "FAIL"
    assert "execution_calibration_quality_gate_not_passed" in result["reasons"]


def test_calibration_passes_when_fill_rates_are_within_assumptions() -> None:
    result = _compare(
        _artifact(partial_fill_rate=0.01, unfilled_rate=0.02),
        assumed_partial_fill_rate=0.02,
        assumed_order_failure_rate=0.03,
    )

    assert result["status"] == "PASS"
    assert result["reasons"] == []
    assert result["observed_partial_fill_rate"] == 0.01
    assert result["observed_unfilled_rate"] == 0.02


def test_optional_warn_mode_missing_calibration_is_explicit() -> None:
    result = _compare(
        None,
        require_content_hash=False,
        min_sample_count=None,
        require_quality_gate_pass=False,
    )

    assert result["status"] == "MISSING"
    assert result["reasons"] == ["execution_calibration_missing"]


def test_calibration_artifact_binds_execution_contract_hash() -> None:
    contract = _execution_contract()
    artifact = _artifact(
        execution_reality_contract=contract,
        execution_contract_hash=contract["execution_contract_hash"],
        execution_contract_hashes=[contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )

    assert artifact["execution_contract_hash"] == contract["execution_contract_hash"]
    assert artifact["execution_reality_contract"] == contract


def test_calibration_contract_hash_does_not_self_reference_artifact_content_hash() -> None:
    observed_contract = _execution_contract(calibration_artifact_hash=None)
    artifact = _artifact(
        execution_reality_contract=observed_contract,
        execution_contract_hash=observed_contract["execution_contract_hash"],
        execution_contract_hashes=[observed_contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )
    expected_contract = _execution_contract(calibration_artifact_hash=artifact["content_hash"])

    assert expected_contract["execution_contract_hash"] == observed_contract["execution_contract_hash"]

    result = _compare(
        artifact,
        expected_execution_reality_contract=expected_contract,
        expected_calibration_artifact_hash=artifact["content_hash"],
    )

    assert result["status"] == "PASS"
    assert result["expected_execution_condition_contract_hash"] == observed_contract["execution_contract_hash"]
    assert result["artifact_execution_condition_contract_hash"] == observed_contract["execution_contract_hash"]
    assert result["expected_calibration_artifact_hash"] == artifact["content_hash"]
    assert result["artifact_content_hash"] == artifact["content_hash"]
    assert result["execution_condition_contract_mismatch"] is False
    assert result["calibration_artifact_lineage_mismatch"] is False


def test_compare_calibration_to_scenario_does_not_require_artifact_hash_inside_observed_contract_hash() -> None:
    observed_contract = _execution_contract(calibration_artifact_hash=None)
    artifact = _artifact(
        execution_reality_contract=observed_contract,
        execution_contract_hash=observed_contract["execution_contract_hash"],
        execution_contract_hashes=[observed_contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )
    expected_contract = _execution_contract(calibration_artifact_hash=artifact["content_hash"])

    result = _compare(artifact, expected_execution_reality_contract=expected_contract)

    assert result["status"] == "PASS"
    assert result["reasons"] == []


def test_compare_calibration_fails_on_execution_contract_hash_mismatch() -> None:
    contract = _execution_contract()
    other = _execution_contract(latency_model={"type": "fixed_bps", "latency_ms": 1})
    artifact = _artifact(
        execution_contract_hash=contract["execution_contract_hash"],
        execution_contract_hashes=[contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )

    result = _compare(artifact, expected_execution_contract_hash=other["execution_contract_hash"])

    assert result["status"] == "FAIL"
    assert "execution_calibration_contract_hash_mismatch" in result["reasons"]
    assert result["execution_condition_contract_mismatch"] is True


def test_compare_calibration_fails_when_contract_hash_required_but_missing() -> None:
    contract = _execution_contract()
    artifact = _artifact()

    result = _compare(artifact, expected_execution_contract_hash=contract["execution_contract_hash"])

    assert result["status"] == "FAIL"
    assert "execution_calibration_contract_hash_missing" in result["reasons"]


def test_compare_calibration_fails_on_mixed_execution_contract_hashes() -> None:
    first = _execution_contract()
    second = _execution_contract(latency_model={"type": "fixed_bps", "latency_ms": 1})
    artifact = _artifact(
        execution_contract_hash=None,
        execution_contract_hashes=[
            first["execution_contract_hash"],
            second["execution_contract_hash"],
        ],
        mixed_execution_contract_hashes=True,
    )

    result = _compare(artifact, expected_execution_contract_hash=first["execution_contract_hash"])

    assert result["status"] == "FAIL"
    assert "execution_calibration_mixed_contract_hashes" in result["reasons"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("fill_reference_policy", "latency_adjusted_orderbook"),
        ("missing_quote_policy", "fail"),
        ("latency_model", {"type": "fixed_bps", "latency_ms": 1}),
        ("top_of_book_required", True),
    ],
)
def test_compare_calibration_fails_when_non_calibration_execution_contract_field_differs(
    field: str,
    value: object,
) -> None:
    contract = _execution_contract()
    artifact = _artifact(
        execution_reality_contract=contract,
        execution_contract_hash=contract["execution_contract_hash"],
        execution_contract_hashes=[contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )
    expected = _execution_contract(**{field: value})

    result = _compare(artifact, expected_execution_reality_contract=expected)

    assert result["status"] == "FAIL"
    assert "execution_calibration_contract_hash_mismatch" in result["reasons"]


def test_compare_calibration_fails_when_fill_reference_policy_differs() -> None:
    contract = _execution_contract()
    artifact = _artifact(
        execution_reality_contract=contract,
        execution_contract_hash=contract["execution_contract_hash"],
        execution_contract_hashes=[contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )

    result = _compare(
        artifact,
        expected_execution_reality_contract=_execution_contract(fill_reference_policy="latency_adjusted_orderbook"),
    )

    assert result["status"] == "FAIL"
    assert "execution_calibration_contract_hash_mismatch" in result["reasons"]


def test_compare_calibration_fails_when_missing_quote_policy_differs() -> None:
    contract = _execution_contract()
    artifact = _artifact(
        execution_reality_contract=contract,
        execution_contract_hash=contract["execution_contract_hash"],
        execution_contract_hashes=[contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )

    result = _compare(
        artifact,
        expected_execution_reality_contract=_execution_contract(missing_quote_policy="fail"),
    )

    assert result["status"] == "FAIL"
    assert "execution_calibration_contract_hash_mismatch" in result["reasons"]


def test_compare_calibration_fails_when_latency_model_differs() -> None:
    contract = _execution_contract()
    artifact = _artifact(
        execution_reality_contract=contract,
        execution_contract_hash=contract["execution_contract_hash"],
        execution_contract_hashes=[contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )

    result = _compare(
        artifact,
        expected_execution_reality_contract=_execution_contract(latency_model={"type": "fixed_bps", "latency_ms": 1}),
    )

    assert result["status"] == "FAIL"
    assert "execution_calibration_contract_hash_mismatch" in result["reasons"]


def test_compare_calibration_fails_when_top_of_book_required_differs() -> None:
    contract = _execution_contract()
    artifact = _artifact(
        execution_reality_contract=contract,
        execution_contract_hash=contract["execution_contract_hash"],
        execution_contract_hashes=[contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )

    result = _compare(
        artifact,
        expected_execution_reality_contract=_execution_contract(top_of_book_required=True),
    )

    assert result["status"] == "FAIL"
    assert "execution_calibration_contract_hash_mismatch" in result["reasons"]


def test_research_backtest_accepts_matching_calibration_artifact_with_contract_hash() -> None:
    manifest = parse_manifest(
        {
            "experiment_id": "calibration_contract_hash_test",
            "hypothesis": "contract self reference regression",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "snap",
                "train": {"start": "2026-01-01", "end": "2026-01-01"},
                "validation": {"start": "2026-01-02", "end": "2026-01-02"},
            },
            "parameter_space": {"SMA_SHORT": [2], "SMA_LONG": [4], "SMA_FILTER_GAP_MIN_RATIO": [0.0]},
            "execution_model": {
                "scenario_policy": "single_scenario",
                "calibration_required": True,
                "calibration_strictness": "fail",
                "scenarios": [
                    {
                        "type": "fixed_bps",
                        "scenario_role": "base",
                        "fee_rate": 0.0004,
                        "slippage_bps": 5.0,
                        "latency_ms": 100,
                        "partial_fill_rate": 0.0,
                        "order_failure_rate": 0.0,
                        "label": "base",
                        "fee_source": "operator_declared_test_fee",
                        "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
                        "slippage_source": "test_calibration",
                        "promotable_as_base": False,
                    }
                ],
            },
            "execution_timing": {
                "fill_reference_policy": "next_candle_open",
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
            },
        }
    )
    scenario = manifest.execution_model.scenarios[0]
    observed_contract = _execution_reality_contract(
        manifest=manifest,
        scenario=scenario,
        calibration_hash=None,
        top_of_book_available=False,
    )
    artifact = _artifact(
        execution_reality_contract=observed_contract,
        execution_contract_hash=observed_contract["execution_contract_hash"],
        execution_contract_hashes=[observed_contract["execution_contract_hash"]],
        execution_contract_hash_present=True,
    )
    expected_contract = _execution_reality_contract(
        manifest=manifest,
        scenario=scenario,
        calibration_hash=artifact["content_hash"],
        top_of_book_available=False,
    )

    result = _compare(
        artifact,
        assumed_latency_ms=100,
        expected_execution_timing_policy=manifest.execution_timing.as_dict(),
        expected_execution_reality_contract=expected_contract,
        expected_calibration_artifact_hash=artifact["content_hash"],
    )

    assert result["status"] == "PASS"


def _execution_contract(**overrides):
    kwargs = {
        "fill_reference_policy": "next_candle_open",
        "missing_quote_policy": "warn",
        "min_execution_reality_level_for_promotion": "candle_next_open",
        "allow_same_candle_close_fill": False,
        "top_of_book_required": False,
        "latency_model": {"type": "fixed_bps", "latency_ms": 0},
        "partial_fill_model": {"type": "fixed_bps", "partial_fill_rate": 0.0},
        "order_failure_model": {"type": "fixed_bps", "order_failure_rate": 0.0},
        "fee_source": "operator_declared_test_fee",
        "slippage_source": "test_calibration",
        "calibration_required": True,
        "calibration_artifact_hash": "sha256:calibration",
    }
    kwargs.update(overrides)
    return build_execution_reality_contract(**kwargs)
