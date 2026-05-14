from __future__ import annotations

import pytest

from bithumb_bot.approved_profile import diff_profile_to_runtime
from bithumb_bot.execution_reality_contract import (
    build_execution_reality_contract,
    execution_condition_contract_hash,
    execution_contract_hash,
    unsupported_capability_reasons,
)
from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest


def _contract(**overrides):
    payload = build_execution_reality_contract(
        fill_reference_policy="next_candle_open",
        decision_guard_ms=500,
        max_quote_wait_ms=3000,
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="candle_next_open",
        allow_same_candle_close_fill=False,
        top_of_book_required=False,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test_fee",
        slippage_source="test_slippage",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
    )
    payload.update(overrides)
    payload["execution_contract_hash"] = execution_contract_hash(payload)
    return payload


def test_execution_contract_hash_is_stable_and_ignores_generated_timestamps() -> None:
    first = _contract(generated_at="2026-05-14T00:00:00+00:00")
    second = _contract(generated_at="2026-05-15T00:00:00+00:00")

    assert first["execution_contract_hash"] == second["execution_contract_hash"]
    assert execution_contract_hash(first) == execution_contract_hash(second)


@pytest.mark.parametrize(
    "field,value",
    [
        ("fill_reference_policy", "latency_adjusted_orderbook"),
        ("top_of_book_required", True),
        ("execution_reality_level", "latency_adjusted_top_of_book"),
    ],
)
def test_execution_contract_hash_changes_for_semantic_execution_fields(field: str, value: object) -> None:
    base = _contract()
    changed = _contract(**{field: value})

    assert changed["execution_contract_hash"] != base["execution_contract_hash"]


def test_execution_condition_hash_excludes_calibration_artifact_lineage() -> None:
    base = _contract(calibration_artifact_hash="sha256:first")
    changed = _contract(calibration_artifact_hash="sha256:second")

    assert execution_condition_contract_hash(base) == execution_condition_contract_hash(changed)
    assert execution_contract_hash(base) == execution_contract_hash(changed)


def _manifest_payload() -> dict[str, object]:
    return {
        "experiment_id": "sma_filter_contract_test",
        "hypothesis": "contract test",
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
        "cost_model": {"fee_rate": 0.0004, "slippage_bps": [10]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 50,
            "min_profit_factor": 1.0,
            "oos_return_must_be_positive": True,
            "parameter_stability_required": False,
        },
    }


def test_research_only_sma_candle_only_manifest_remains_allowed_but_limited() -> None:
    manifest = parse_manifest(_manifest_payload())

    assert manifest.deployment_tier == "research_only"
    assert manifest.dataset.top_of_book is None
    assert manifest.execution_timing.fill_reference_policy == "candle_close_legacy"


def test_production_bound_candle_close_manifest_fails_closed() -> None:
    payload = _manifest_payload()
    payload["deployment_tier"] = "paper_candidate"
    payload["execution_timing"] = {
        "fill_reference_policy": "candle_close_legacy",
        "allow_same_candle_close_fill": True,
        "min_execution_reality_level_for_promotion": "candle_close_optimistic",
    }

    with pytest.raises(ManifestValidationError) as exc:
        parse_manifest(payload)

    assert "production_execution_reference_price_candle_close_not_promotable" in str(exc.value)


def test_production_bound_top_of_book_policy_requires_top_of_book_dataset() -> None:
    payload = _manifest_payload()
    payload["deployment_tier"] = "paper_candidate"
    payload["execution_timing"] = {
        "fill_reference_policy": "latency_adjusted_orderbook",
        "missing_quote_policy": "fail",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "latency_adjusted_top_of_book",
    }

    with pytest.raises(ManifestValidationError) as exc:
        parse_manifest(payload)

    assert "production_top_of_book_required" in str(exc.value)


def test_unsupported_depth_requirement_fails_explicitly() -> None:
    payload = _manifest_payload()
    payload["execution_timing"] = {"depth_required": True}

    with pytest.raises(ManifestValidationError) as exc:
        parse_manifest(payload)

    assert "execution_depth_required_but_unavailable" in str(exc.value)


def test_top_of_book_only_contract_cannot_satisfy_depth_trade_tick_or_queue_requirements() -> None:
    contract = _contract(
        top_of_book_required=True,
        depth_required=True,
        trade_tick_required=True,
        queue_position_required=True,
        top_of_book_is_full_depth=False,
        depth_available=False,
        trade_ticks_available=False,
        queue_position_available=False,
    )

    reasons = unsupported_capability_reasons(contract)

    assert "execution_depth_required_but_unavailable" in reasons
    assert "execution_trade_ticks_required_but_unavailable" in reasons
    assert "execution_queue_position_required_but_unavailable" in reasons


def test_profile_runtime_execution_contract_mismatch_is_reason_coded() -> None:
    profile_contract = _contract()
    runtime_contract = _contract(fill_reference_policy="latency_adjusted_orderbook")
    profile = {
        "profile_schema_version": 1,
        "profile_mode": "paper",
        "source_promotion_content_hash": "sha256:promotion",
        "candidate_profile_hash": "sha256:candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "strategy_parameters": {},
        "cost_model": {},
        "base_cost_assumption": {
            "role": "base",
            "promotable_as_base": True,
            "label": "test",
            "fee_source": "test_fee",
            "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
            "slippage_source": "test_slippage",
            "fee_rate": 0.0004,
            "slippage_bps": 10,
        },
        "execution_model_source": "execution_model",
        "execution_model": {
            "source": "execution_model",
            "scenario_policy": "single_scenario",
            "calibration_required": True,
            "calibration_strictness": "fail",
            "scenarios": [
                {
                    "scenario_role": "base",
                    "cost_assumption": {
                        "role": "base",
                        "promotable_as_base": True,
                        "label": "test",
                        "fee_source": "test_fee",
                        "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
                        "slippage_source": "test_slippage",
                        "fee_rate": 0.0004,
                        "slippage_bps": 10,
                    },
                }
            ],
        },
        "execution_calibration_required": True,
        "execution_calibration_strictness": "fail",
        "execution_calibration_gate": {
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
            "artifact_hashes": ["sha256:calibration"],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "artifact_hash": "sha256:calibration",
                    "content_hash_present": True,
                    "quality_gate_status": "PASS",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "market": "KRW-BTC",
                    "expected_market": "KRW-BTC",
                    "interval": "1m",
                    "expected_interval": "1m",
                }
            ],
        },
        "production_calibration_policy_result": {
            "target": "paper_candidate",
            "production_bound": True,
            "required": True,
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
            "artifact_hashes": ["sha256:calibration"],
            "policy_source": "test",
            "operator_next_step": "none",
        },
        "execution_calibration_artifact_hash": "sha256:calibration",
        "execution_calibration_artifact_hashes": ["sha256:calibration"],
        "deployment_tier": "paper_candidate",
        "execution_reality_contract": profile_contract,
        "execution_contract_hash": profile_contract["execution_contract_hash"],
        "regime_policy": {
            "regime_classifier_version": "test",
            "allowed_regimes": [],
            "blocked_regimes": [],
        },
    }
    from bithumb_bot.approved_profile import compute_approved_profile_hash

    profile["profile_content_hash"] = compute_approved_profile_hash(profile)

    mismatches = diff_profile_to_runtime(profile, {"execution_reality_contract": runtime_contract})

    assert any(item.get("reason") == "execution_contract_hash_mismatch" for item in mismatches)
