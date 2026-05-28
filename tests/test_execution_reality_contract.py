from __future__ import annotations

import pytest

from bithumb_bot.approved_profile import diff_profile_to_runtime
from bithumb_bot.execution_reality_contract import (
    build_execution_capability_contract,
    build_execution_reality_contract,
    execution_capability_contract_mismatch_reasons,
    execution_capability_contract_hash,
    execution_condition_contract_hash,
    execution_contract_hash,
    unsupported_capability_reasons,
    validate_execution_capability_contract,
)
from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin
from bithumb_bot.research.strategy_spec import (
    exit_policy_from_parameters,
    materialize_strategy_parameters,
    materialized_strategy_parameters_hash,
    strategy_parameter_source_map,
)


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


def test_execution_contract_hash_ignores_nested_top_of_book_observed_availability() -> None:
    left = build_execution_reality_contract(
        fill_reference_policy="first_orderbook_after_decision",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
        allow_same_candle_close_fill=False,
        top_of_book_required=True,
        top_of_book_available=True,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test_fee",
        slippage_source="test_slippage",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
        extra={"quote_evidence_available": True},
    )
    right = build_execution_reality_contract(
        fill_reference_policy="first_orderbook_after_decision",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
        allow_same_candle_close_fill=False,
        top_of_book_required=True,
        top_of_book_available=False,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test_fee",
        slippage_source="test_slippage",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
        extra={"quote_evidence_available": False},
    )

    assert left["execution_capability_contract"]["available_capabilities"]["top_of_book"] is True
    assert right["execution_capability_contract"]["available_capabilities"]["top_of_book"] is False
    assert execution_contract_hash(left) == execution_contract_hash(right)
    assert left["execution_contract_hash"] == right["execution_contract_hash"]


def test_execution_contract_hash_still_changes_for_semantic_top_of_book_required_change() -> None:
    base = build_execution_reality_contract(
        fill_reference_policy="next_candle_open",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="candle_next_open",
        allow_same_candle_close_fill=False,
        top_of_book_required=False,
        top_of_book_available=True,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test_fee",
        slippage_source="test_slippage",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
        extra={"quote_evidence_available": True},
    )
    changed = build_execution_reality_contract(
        fill_reference_policy="next_candle_open",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="candle_next_open",
        allow_same_candle_close_fill=False,
        top_of_book_required=True,
        top_of_book_available=True,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test_fee",
        slippage_source="test_slippage",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
        extra={"quote_evidence_available": True},
    )

    assert execution_contract_hash(base) != execution_contract_hash(changed)
    assert base["execution_contract_hash"] != changed["execution_contract_hash"]


def test_execution_capability_contract_hash_is_stable_and_ignores_generated_timestamps() -> None:
    first = build_execution_capability_contract(
        fill_reference_policy="next_candle_open",
        evidence_tier="candle_next_open",
    )
    second = dict(first)
    second["generated_at"] = "2026-05-16T00:00:00+00:00"

    assert execution_capability_contract_hash(first) == execution_capability_contract_hash(second)


@pytest.mark.parametrize(
    "overrides",
    [
        {"l2_depth_snapshot_required": True},
        {"l2_depth_snapshot_available": True},
        {"full_orderbook_depth_required": True},
        {"trade_ticks_required": True},
        {"queue_position_required": True},
        {"market_impact_model_required": True},
        {"evidence_tier": "latency_adjusted_top_of_book"},
    ],
)
def test_execution_capability_contract_hash_changes_for_semantic_fields(overrides: dict[str, object]) -> None:
    base = build_execution_capability_contract(
        fill_reference_policy="next_candle_open",
        evidence_tier="candle_next_open",
    )
    payload = {"fill_reference_policy": "next_candle_open", "evidence_tier": "candle_next_open"}
    payload.update(overrides)
    changed = build_execution_capability_contract(
        **payload,
    )

    assert changed["execution_capability_contract_hash"] != base["execution_capability_contract_hash"]


def test_reserved_future_evidence_tier_is_blocking_reason() -> None:
    capability = build_execution_capability_contract(
        fill_reference_policy="next_candle_open",
        evidence_tier="impact_model_calibrated",
    )
    contract = _contract(execution_capability_contract=capability)

    assert validate_execution_capability_contract(capability) == [
        "execution_evidence_tier_reserved_not_implemented"
    ]
    assert "execution_evidence_tier_reserved_not_implemented" in unsupported_capability_reasons(contract)


def test_unknown_evidence_tier_is_blocking_reason() -> None:
    capability = build_execution_capability_contract(
        fill_reference_policy="next_candle_open",
        evidence_tier="made_up_scalar_stress_tier",
    )
    contract = _contract(execution_capability_contract=capability)

    assert validate_execution_capability_contract(capability) == [
        "execution_evidence_tier_unsupported"
    ]
    assert "execution_evidence_tier_unsupported" in unsupported_capability_reasons(contract)


def test_l2_depth_rows_do_not_imply_full_orderbook_depth_capability() -> None:
    contract = build_execution_reality_contract(
        fill_reference_policy="first_orderbook_after_decision",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="top_of_book_after_decision",
        allow_same_candle_close_fill=False,
        top_of_book_required=True,
        top_of_book_available=True,
        top_of_book_is_full_depth=False,
        depth_required=True,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="test_fee",
        slippage_source="test_slippage",
        calibration_required=False,
        extra={
            "quote_evidence_available": True,
            "depth_available": True,
            "depth_available_semantics": "stored_l2_depth_complete_snapshots_exist_not_execution_model_used",
            "depth_evidence_available": True,
            "l2_depth_rows_available": True,
            "l2_depth_complete_snapshots_available": True,
            "depth_walk_execution_model_available": True,
            "depth_walk_execution_model_used": False,
            "full_orderbook_depth_available": False,
        },
    )

    capability = contract["execution_capability_contract"]
    assert contract["depth_available"] is True
    assert contract["l2_depth_complete_snapshots_available"] is True
    assert capability["available_capabilities"]["l2_depth_snapshot"] is True
    assert capability["strategy_required_capabilities"]["l2_depth_snapshot"] is True
    assert capability["available_capabilities"]["full_orderbook_depth"] is False
    assert capability["strategy_required_capabilities"]["full_orderbook_depth"] is False
    assert capability["available_capabilities"]["top_of_book_is_full_depth"] is False
    assert "execution_depth_required_but_unavailable" not in unsupported_capability_reasons(contract)


def test_l2_depth_walk_requires_l2_snapshot_not_full_orderbook_depth() -> None:
    capability = build_execution_capability_contract(
        fill_reference_policy="latency_adjusted_orderbook",
        top_of_book_required=True,
        top_of_book_available=True,
        evidence_tier="l2_depth_walk_no_queue",
        l2_depth_snapshot_available=True,
        full_orderbook_depth_available=False,
    )

    assert capability["strategy_required_capabilities"]["l2_depth_snapshot"] is True
    assert capability["strategy_required_capabilities"]["full_orderbook_depth"] is False
    assert capability["available_capabilities"]["l2_depth_snapshot"] is True
    assert capability["available_capabilities"]["full_orderbook_depth"] is False
    assert capability["unavailable_required_capabilities"] == []
    assert "full_orderbook_depth_unavailable" in capability["limitations"]
    assert "queue_position_unavailable" in capability["limitations"]
    assert "trade_ticks_unavailable" in capability["limitations"]
    assert "market_impact_model_unavailable" in capability["limitations"]
    assert "intra_candle_path_reconstruction_unavailable" in capability["limitations"]


def test_l2_depth_walk_missing_snapshot_is_specific_unavailable_capability() -> None:
    capability = build_execution_capability_contract(
        fill_reference_policy="latency_adjusted_orderbook",
        top_of_book_required=True,
        top_of_book_available=True,
        evidence_tier="l2_depth_walk_no_queue",
        l2_depth_snapshot_available=False,
        full_orderbook_depth_available=False,
    )
    contract = _contract(
        depth_required=True,
        depth_available=False,
        execution_reality_level="l2_depth_walk_no_queue",
        execution_capability_contract=capability,
    )

    assert capability["unavailable_required_capabilities"] == ["l2_depth_snapshot"]
    reasons = unsupported_capability_reasons(contract)
    assert "execution_l2_depth_snapshot_required_but_unavailable" in reasons
    assert "execution_depth_required_but_unavailable" not in reasons


def test_capability_validation_recomputes_required_availability_consistency() -> None:
    capability = build_execution_capability_contract(
        fill_reference_policy="first_orderbook_after_decision",
        top_of_book_required=True,
        top_of_book_available=False,
        evidence_tier="top_of_book_after_decision",
    )
    tampered = dict(capability)
    tampered["unavailable_required_capabilities"] = []

    reasons = validate_execution_capability_contract(tampered)

    assert "execution_capability_required_unavailable" in reasons
    assert "execution_capability_unavailable_required_capabilities_mismatch" in reasons


def test_capability_mismatch_ignores_only_top_of_book_unavailable_difference() -> None:
    expected = build_execution_capability_contract(
        fill_reference_policy="first_orderbook_after_decision",
        top_of_book_required=True,
        top_of_book_available=True,
        evidence_tier="top_of_book_after_decision",
    )
    observed = build_execution_capability_contract(
        fill_reference_policy="first_orderbook_after_decision",
        top_of_book_required=True,
        top_of_book_available=False,
        evidence_tier="top_of_book_after_decision",
    )

    assert not [
        item
        for item in execution_capability_contract_mismatch_reasons(expected=expected, observed=observed)
        if item.get("field") == "execution_capability_contract.unavailable_required_capabilities"
    ]

    bad = build_execution_capability_contract(
        fill_reference_policy="first_orderbook_after_decision",
        top_of_book_required=True,
        top_of_book_available=True,
        market_impact_model_required=True,
        evidence_tier="top_of_book_after_decision",
    )

    assert any(
        item.get("field") == "execution_capability_contract.unavailable_required_capabilities"
        for item in execution_capability_contract_mismatch_reasons(expected=expected, observed=bad)
    )


def test_capability_mismatch_detects_l2_depth_snapshot_availability() -> None:
    expected = build_execution_capability_contract(
        fill_reference_policy="latency_adjusted_orderbook",
        evidence_tier="l2_depth_walk_no_queue",
        l2_depth_snapshot_available=True,
    )
    observed = build_execution_capability_contract(
        fill_reference_policy="latency_adjusted_orderbook",
        evidence_tier="l2_depth_walk_no_queue",
        l2_depth_snapshot_available=False,
    )

    mismatches = execution_capability_contract_mismatch_reasons(expected=expected, observed=observed)

    assert any(
        item.get("field") == "execution_capability_contract.available_capabilities.l2_depth_snapshot"
        for item in mismatches
    )
    assert any(
        item.get("field") == "execution_capability_contract.unavailable_required_capabilities"
        for item in mismatches
    )


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


def _complete_runtime_bound_parameter_space(
    overrides: dict[str, list[object]] | None = None,
) -> dict[str, list[object]]:
    payload: dict[str, list[object]] = {
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
        "STRATEGY_ENTRY_SLIPPAGE_BPS": [5.0],
        "LIVE_FEE_RATE_ESTIMATE": [0.0004],
        "STRATEGY_EXIT_RULES": ["opposite_cross,max_holding_time"],
        "STRATEGY_EXIT_STOP_LOSS_RATIO": [0.0],
        "STRATEGY_EXIT_MAX_HOLDING_MIN": [0],
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": [0.0],
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": [0.0],
    }
    if overrides:
        payload.update(overrides)
    return payload


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
            "parameter_space": _complete_runtime_bound_parameter_space(),
            "portfolio_policy": _portfolio_policy(),
            "statistical_validation": _statistical_validation(),
            "stress_suite": _stress_suite(),
            "final_selection": _final_selection(),
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


def test_market_impact_required_fails_until_implemented() -> None:
    payload = _manifest_payload()
    payload["execution_timing"] = {"market_impact_required": True}

    with pytest.raises(ManifestValidationError, match="execution_market_impact_required_but_unavailable"):
        parse_manifest(payload)


def test_market_order_extra_cost_bps_does_not_satisfy_market_impact_required() -> None:
    payload = _manifest_payload()
    payload["execution_model"] = {
        "type": "stress",
        "fee_rate": 0.0004,
        "slippage_bps": 5.0,
        "market_order_extra_cost_bps": 25.0,
    }
    payload["execution_timing"] = {"market_impact_required": True}

    with pytest.raises(ManifestValidationError, match="execution_market_impact_required_but_unavailable"):
        parse_manifest(payload)


def test_depth_walk_manifest_type_is_wired_to_research_backtest_scenarios() -> None:
    payload = _manifest_payload()
    payload["execution_model"] = {
        "type": "depth_walk",
        "fee_rate": 0.0004,
        "slippage_bps": 0.0,
    }

    manifest = parse_manifest(payload)

    assert manifest.execution_model.scenarios[0].type == "depth_walk"


def test_l2_depth_walk_promotion_level_requires_depth_walk_scenario() -> None:
    payload = _manifest_payload()
    payload["execution_timing"] = _production_safe_execution_timing(
        fill_reference_policy="latency_adjusted_orderbook",
        min_execution_reality_level_for_promotion="l2_depth_walk_no_queue",
    )

    with pytest.raises(
        ManifestValidationError,
        match="execution_l2_depth_walk_required_but_depth_walk_scenario_missing",
    ):
        parse_manifest(payload)


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("depth_required", "execution_depth_required_but_unavailable"),
        ("trade_tick_required", "execution_trade_ticks_required_but_unavailable"),
        ("queue_position_required", "execution_queue_position_required_but_unavailable"),
        ("intra_candle_path_required", "execution_intra_candle_path_required_but_unavailable"),
    ],
)
def test_unsupported_execution_capability_gates_still_fail_closed(field: str, reason: str) -> None:
    payload = _manifest_payload()
    payload["execution_timing"] = {field: True}

    with pytest.raises(ManifestValidationError, match=reason):
        parse_manifest(payload)


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

    assert "execution_l2_depth_snapshot_required_but_unavailable" in reasons
    assert "execution_trade_ticks_required_but_unavailable" in reasons
    assert "execution_queue_position_required_but_unavailable" in reasons


def test_depth_availability_does_not_satisfy_queue_ticks_impact_or_intracandle() -> None:
    contract = build_execution_reality_contract(
        fill_reference_policy="latency_adjusted_orderbook",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="latency_adjusted_top_of_book",
        allow_same_candle_close_fill=False,
        top_of_book_required=True,
        top_of_book_available=True,
        depth_required=True,
        trade_tick_required=True,
        queue_position_required=True,
        market_impact_required=True,
        extra={
            "quote_evidence_available": True,
            "depth_available": True,
            "depth_evidence_available": True,
            "l2_depth_complete_snapshots_available": True,
            "trade_ticks_available": False,
            "queue_position_available": False,
            "market_impact_model_available": False,
            "intra_candle_path_required": True,
            "intra_candle_path_available": False,
        },
    )

    reasons = unsupported_capability_reasons(contract)

    assert "execution_l2_depth_snapshot_required_but_unavailable" not in reasons
    assert "execution_trade_ticks_required_but_unavailable" in reasons
    assert "execution_queue_position_required_but_unavailable" in reasons
    assert "execution_market_impact_required_but_unavailable" in reasons
    assert "execution_intra_candle_path_required_but_unavailable" in reasons


def test_profile_runtime_execution_contract_mismatch_is_reason_coded() -> None:
    profile_contract = _contract()
    runtime_contract = _contract(fill_reference_policy="latency_adjusted_orderbook")
    strategy_plugin = resolve_research_strategy_plugin("sma_with_filter")
    strategy_parameters = materialize_strategy_parameters(
        "sma_with_filter",
        {"SMA_SHORT": 7, "SMA_LONG": 30},
    )
    exit_policy = exit_policy_from_parameters("sma_with_filter", strategy_parameters)
    profile = {
        "profile_schema_version": 1,
        "profile_mode": "paper",
        "source_promotion_content_hash": "sha256:promotion",
        "candidate_profile_hash": "sha256:candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "strategy_name": "sma_with_filter",
        "strategy_plugin_contract": strategy_plugin.contract_payload(),
        "strategy_plugin_contract_hash": strategy_plugin.contract_hash(),
        "market": "KRW-BTC",
        "interval": "1m",
        "strategy_parameters": strategy_parameters,
        "effective_strategy_parameters": strategy_parameters,
        "effective_strategy_parameters_hash": materialized_strategy_parameters_hash(strategy_parameters),
            "strategy_parameter_source_map": strategy_parameter_source_map(
                "sma_with_filter",
                {"SMA_SHORT": 7, "SMA_LONG": 30},
            ),
        "exit_policy": exit_policy,
        "exit_policy_hash": sha256_prefixed(exit_policy),
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
