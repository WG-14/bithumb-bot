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


def _production_bound_manifest_payload(**overrides: object) -> dict[str, object]:
    payload = _manifest_payload()
    payload.update(
        {
            "deployment_tier": "paper_candidate",
            "statistical_validation": _statistical_validation(),
            "stress_suite": _stress_suite(),
            "execution_model": {
                "type": "fixed_bps",
                "scenario_role": "base",
                "label": "operator_declared_base_cost",
                "fee_rate": 0.0004,
                "fee_source": "operator_declared_bithumb_app_fee",
                "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
                "slippage_bps": 5.0,
                "slippage_source": "execution_calibration_sample",
                "promotable_as_base": True,
                "calibration_required": True,
                "calibration_strictness": "fail",
            },
        }
    )
    payload.update(overrides)
    return payload


def _production_safe_execution_timing(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "fill_reference_policy": "next_candle_open",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "candle_next_open",
    }
    payload.update(overrides)
    return payload


def _assert_manifest_reasons(payload: dict[str, object], *expected: str) -> None:
    with pytest.raises(ManifestValidationError) as exc:
        parse_manifest(payload)
    message = str(exc.value)
    for reason in expected:
        assert reason in message


def test_research_only_sma_candle_only_manifest_remains_allowed_but_limited() -> None:
    manifest = parse_manifest(_manifest_payload())

    assert manifest.deployment_tier == "research_only"
    assert manifest.dataset.top_of_book is None
    assert manifest.execution_timing.fill_reference_policy == "candle_close_legacy"


def test_production_bound_manifest_without_execution_timing_fails_closed() -> None:
    payload = _production_bound_manifest_payload()

    _assert_manifest_reasons(
        payload,
        "production_execution_timing_required",
        "production_legacy_execution_timing_not_promotable",
        "production_execution_reference_price_candle_close_not_promotable",
        "production_same_candle_close_fill_not_allowed",
        "production_min_execution_reality_level_required",
    )


def test_production_bound_manifest_empty_execution_timing_fails_closed() -> None:
    payload = _production_bound_manifest_payload(execution_timing={})

    _assert_manifest_reasons(
        payload,
        "production_execution_timing_required",
        "production_min_execution_reality_level_required",
    )


def test_production_bound_manifest_explicit_candle_close_legacy_fails_closed() -> None:
    payload = _production_bound_manifest_payload(
        execution_timing={
            "fill_reference_policy": "candle_close_legacy",
            "allow_same_candle_close_fill": True,
            "min_execution_reality_level_for_promotion": "candle_close_optimistic",
        }
    )

    _assert_manifest_reasons(
        payload,
        "production_execution_reference_price_candle_close_not_promotable",
        "production_same_candle_close_fill_not_allowed",
        "production_execution_reality_level_below_required",
    )


def test_production_bound_manifest_same_candle_close_fill_fails_closed() -> None:
    payload = _production_bound_manifest_payload(
        execution_timing=_production_safe_execution_timing(allow_same_candle_close_fill=True)
    )

    _assert_manifest_reasons(payload, "production_same_candle_close_fill_not_allowed")


def test_production_bound_manifest_missing_min_execution_reality_level_fails_closed() -> None:
    payload = _production_bound_manifest_payload(
        execution_timing={
            "fill_reference_policy": "next_candle_open",
            "allow_same_candle_close_fill": False,
        }
    )

    _assert_manifest_reasons(
        payload,
        "production_execution_timing_required",
        "production_min_execution_reality_level_required",
    )


def test_research_only_manifest_without_execution_timing_keeps_legacy_compatibility() -> None:
    manifest = parse_manifest(_manifest_payload())

    assert manifest.deployment_tier == "research_only"
    assert manifest.execution_timing.source == "legacy_default"
    assert manifest.execution_timing.fill_reference_policy == "candle_close_legacy"
    assert manifest.execution_timing.allow_same_candle_close_fill is True
    assert manifest.execution_timing.min_execution_reality_level_for_promotion is None


def test_production_bound_next_candle_open_execution_timing_passes_manifest_policy() -> None:
    manifest = parse_manifest(
        _production_bound_manifest_payload(
            execution_timing=_production_safe_execution_timing()
        )
    )

    assert manifest.deployment_tier == "paper_candidate"
    assert manifest.execution_timing.fill_reference_policy == "next_candle_open"
    assert manifest.execution_timing.allow_same_candle_close_fill is False
    assert manifest.execution_timing.min_execution_reality_level_for_promotion == "candle_next_open"


def test_production_bound_orderbook_policy_min_level_below_reference_fails() -> None:
    payload = _production_bound_manifest_payload(
        execution_timing={
            "fill_reference_policy": "first_orderbook_after_decision",
            "missing_quote_policy": "fail",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "candle_next_open",
        }
    )
    dataset = dict(payload["dataset"])  # type: ignore[arg-type]
    dataset["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100.0,
    }
    payload["dataset"] = dataset

    _assert_manifest_reasons(payload, "production_execution_reality_level_below_policy_reference")


def test_production_bound_latency_orderbook_policy_min_level_below_reference_fails() -> None:
    payload = _production_bound_manifest_payload(
        execution_timing={
            "fill_reference_policy": "latency_adjusted_orderbook",
            "missing_quote_policy": "fail",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "top_of_book_after_decision",
        }
    )
    dataset = dict(payload["dataset"])  # type: ignore[arg-type]
    dataset["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100.0,
    }
    payload["dataset"] = dataset

    _assert_manifest_reasons(payload, "production_execution_reality_level_below_policy_reference")


def test_production_bound_candle_close_manifest_fails_closed() -> None:
    payload = _production_bound_manifest_payload()
    payload["execution_timing"] = {
        "fill_reference_policy": "candle_close_legacy",
        "allow_same_candle_close_fill": True,
        "min_execution_reality_level_for_promotion": "candle_close_optimistic",
    }

    with pytest.raises(ManifestValidationError) as exc:
        parse_manifest(payload)

    assert "production_execution_reference_price_candle_close_not_promotable" in str(exc.value)


def test_production_bound_top_of_book_policy_requires_top_of_book_dataset() -> None:
    payload = _production_bound_manifest_payload()
    payload["execution_timing"] = {
        "fill_reference_policy": "latency_adjusted_orderbook",
        "missing_quote_policy": "fail",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "latency_adjusted_top_of_book",
    }

    with pytest.raises(ManifestValidationError) as exc:
        parse_manifest(payload)

    assert "production_top_of_book_required" in str(exc.value)


@pytest.mark.parametrize(
    "top_of_book,reason",
    [
        (None, "production_top_of_book_required"),
        (
            {
                "source": "sqlite_orderbook_top_snapshots",
                "required": False,
                "missing_policy": "fail",
                "min_coverage_pct": 100.0,
            },
            "production_top_of_book_required",
        ),
        (
            {
                "source": "sqlite_orderbook_top_snapshots",
                "required": True,
                "missing_policy": "warn",
                "min_coverage_pct": 100.0,
            },
            "production_missing_quote_policy_must_fail",
        ),
        (
            {
                "source": "sqlite_orderbook_top_snapshots",
                "required": True,
                "missing_policy": "fail",
                "min_coverage_pct": 99.9,
            },
            "production_top_of_book_min_coverage_must_be_100",
        ),
    ],
)
def test_production_bound_orderbook_timing_requires_production_safe_dataset(
    top_of_book: dict[str, object] | None,
    reason: str,
) -> None:
    payload = _production_bound_manifest_payload(
        execution_timing={
            "fill_reference_policy": "first_orderbook_after_decision",
            "missing_quote_policy": "fail",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "top_of_book_after_decision",
        }
    )
    if top_of_book is not None:
        dataset = dict(payload["dataset"])  # type: ignore[arg-type]
        dataset["top_of_book"] = top_of_book
        payload["dataset"] = dataset

    _assert_manifest_reasons(payload, reason)


def test_production_bound_orderbook_timing_with_safe_top_of_book_dataset_passes() -> None:
    payload = _production_bound_manifest_payload(
        execution_timing={
            "fill_reference_policy": "first_orderbook_after_decision",
            "missing_quote_policy": "fail",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "top_of_book_after_decision",
        }
    )
    dataset = dict(payload["dataset"])  # type: ignore[arg-type]
    dataset["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100.0,
    }
    payload["dataset"] = dataset

    manifest = parse_manifest(payload)

    assert manifest.dataset.top_of_book is not None
    assert manifest.dataset.top_of_book.required is True
    assert manifest.dataset.top_of_book.missing_policy == "fail"
    assert manifest.execution_timing.fill_reference_policy == "first_orderbook_after_decision"


def test_production_bound_latency_orderbook_timing_with_safe_top_of_book_dataset_passes() -> None:
    payload = _production_bound_manifest_payload(
        execution_timing={
            "fill_reference_policy": "latency_adjusted_orderbook",
            "missing_quote_policy": "fail",
            "allow_same_candle_close_fill": False,
            "min_execution_reality_level_for_promotion": "latency_adjusted_top_of_book",
        }
    )
    dataset = dict(payload["dataset"])  # type: ignore[arg-type]
    dataset["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100.0,
    }
    payload["dataset"] = dataset

    manifest = parse_manifest(payload)

    assert manifest.dataset.top_of_book is not None
    assert manifest.dataset.top_of_book.required is True
    assert manifest.execution_timing.fill_reference_policy == "latency_adjusted_orderbook"


def test_invalid_production_bound_legacy_default_fails_before_report_generation() -> None:
    payload = _production_bound_manifest_payload()

    _assert_manifest_reasons(payload, "production_execution_timing_required")


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
