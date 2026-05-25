from __future__ import annotations

import json

import pytest

from bithumb_bot.canonical_decision import EMPTY_ORDER_RULES_HASH, runtime_decision_to_canonical_event, validate_canonical_decision_payload
from bithumb_bot.decision_equivalence import (
    compare_decision_export_artifacts,
    compare_decision_equivalence,
    compute_decision_export_hash,
    load_decision_export_artifact,
)
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.lot_native_simulation import LotNativeResearchPositionModel
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin
from bithumb_bot.strategy.base import StrategyDecision


def _decision(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "decision_contract_version": 1,
        "strategy_contract_version": "sma_strategy_v1",
        "strategy_name": "sma_with_filter",
        "profile_content_hash": "sha256:profile",
        "candidate_profile_hash": "sha256:candidate",
        "dataset_content_hash": "sha256:data",
        "db_data_fingerprint": "sha256:data",
        "market": "KRW-BTC",
        "interval": "1m",
        "signal_timestamp": "1714521660000",
        "candle_ts": 1_714_521_660_000,
        "through_ts_ms": 1_714_521_660_000,
        "candle_basis": "closed",
        "decision_ts": 1_714_521_720_000,
        "raw_signal": "BUY",
        "final_signal": "BUY",
        "side": "BUY",
        "blocked": False,
        "block_reason": "",
        "blocked_filters": [],
        "prev_s": 100.0,
        "prev_l": 101.0,
        "curr_s": 102.0,
        "curr_l": 101.0,
        "feature_hash": "sha256:feature",
        "gap_ratio": 0.01,
        "range_ratio": 0.02,
        "expected_edge_ratio": 0.01,
        "required_edge_ratio": 0.001,
        "fee_authority_hash": "sha256:fee_authority",
        "fee_model_hash": "sha256:fee",
        "slippage_model_hash": "sha256:slippage",
        "order_rules_hash": "sha256:order_rules",
        "market_regime": "uptrend",
        "regime_decision": "allowed",
        "regime_block_reason": "",
        "position_state_hash": "sha256:position",
        "entry_allowed": True,
        "exit_allowed": False,
        "dust_state": "flat",
        "effective_flat": True,
        "normalized_exposure_active": False,
        "exit_rule": "",
        "exit_reason": "",
        "exit_evaluations_hash": "sha256:exit",
        "execution_timing_policy_hash": "sha256:timing",
        "replay_fingerprint_hash": "sha256:replay",
    }
    payload.update(overrides)
    authority = dict(payload.get("position_authority") or {})
    authority.setdefault("state_class", "flat_no_dust_no_position")
    authority.setdefault("unsupported_reason", "")
    authority.setdefault("position_state_hash", payload["position_state_hash"])
    authority.setdefault("order_rules_hash", payload["order_rules_hash"])
    authority.setdefault("fee_authority_hash", payload["fee_authority_hash"])
    payload["position_authority"] = authority
    return payload


def _decision_v2(**overrides: object) -> dict[str, object]:
    payload = _decision(**overrides)
    for key in (
        "strategy_contract_version",
        "prev_s",
        "prev_l",
        "curr_s",
        "curr_l",
        "feature_hash",
        "gap_ratio",
        "range_ratio",
        "expected_edge_ratio",
        "required_edge_ratio",
    ):
        payload.pop(key, None)
    payload.update(
        {
            "decision_contract_version": 2,
            "strategy_version": "sma_with_filter.research_runtime_contract.v2",
            "strategy_decision_contract_version": "research_sma_decision_contract.v3_entry_exit_risk_exit",
            "feature_snapshot": {"short_sma": 102.0, "long_sma": 101.0},
            "feature_snapshot_hash": "sha256:feature_snapshot",
            "strategy_specific_payload": {"curr_s": 102.0, "curr_l": 101.0, "gap_ratio": 0.01},
            "strategy_diagnostics_namespace": "sma_with_filter",
            "strategy_diagnostics": {"cross": "golden"},
            "strategy_behavior_payload": {
                "strategy_name": "sma_with_filter",
                "raw_signal": payload.get("raw_signal"),
                "final_signal": payload.get("final_signal"),
                "strategy_specific_payload": {"curr_s": 102.0, "curr_l": 101.0, "gap_ratio": 0.01},
            },
            "strategy_behavior_hash": "sha256:strategy_behavior",
        }
    )
    payload.update(overrides)
    return payload


def _compare(research: dict[str, object], runtime: dict[str, object]):
    return compare_decision_equivalence(
        research_decisions=[research],
        runtime_decisions=[runtime],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )


def test_incomplete_canonical_payload_is_not_promotion_grade() -> None:
    payload = _decision()
    del payload["fee_model_hash"]

    validation = validate_canonical_decision_payload(payload)
    result = _compare(payload, payload)

    assert validation.promotion_grade is False
    assert "fee_model_hash" in validation.missing_fields
    assert result.ok is False
    assert result.report["promotion_grade_comparison"] is False
    assert result.report["canonical_incomplete_decision_count"] == 2
    assert "canonical_decision_required_field_missing" in result.report["reason_codes"]


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("profile_content_hash", "sha256:wrong", "decision_profile_hash_not_bound_to_report"),
        ("market", "KRW-ETH", "decision_market_not_bound_to_report"),
        ("interval", "5m", "decision_interval_not_bound_to_report"),
        ("dataset_content_hash", "sha256:wrong", "decision_data_fingerprint_not_bound_to_report"),
        ("db_data_fingerprint", "sha256:wrong", "decision_data_fingerprint_not_bound_to_report"),
    ],
)
def test_matching_decisions_still_fail_when_not_bound_to_report(
    field: str,
    value: object,
    reason: str,
) -> None:
    payload = _decision(**{field: value})
    if field in {"dataset_content_hash", "db_data_fingerprint"}:
        payload["dataset_content_hash"] = value
        payload["db_data_fingerprint"] = value

    result = _compare(payload, payload)

    assert result.ok is False
    assert reason in result.report["reason_codes"]


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("order_rules_hash", "sha256:other", "decision_order_rules_mismatch"),
        ("fee_model_hash", "sha256:other", "decision_fee_authority_mismatch"),
        ("slippage_model_hash", "sha256:other", "decision_slippage_model_mismatch"),
        ("position_state_hash", "sha256:other", "decision_position_dust_mismatch"),
        ("dust_state", "dust_only", "decision_position_dust_mismatch"),
        ("entry_allowed", False, "decision_position_dust_mismatch"),
        ("exit_allowed", True, "decision_position_dust_mismatch"),
        ("exit_rule", "max_holding_time", "decision_exit_rule_mismatch"),
        ("exit_reason", "exit by max holding time", "decision_exit_rule_mismatch"),
        ("exit_evaluations_hash", "sha256:other", "decision_exit_rule_mismatch"),
        ("regime_decision", "blocked", "decision_regime_mismatch"),
        ("candle_basis", "open", "decision_timestamp_candle_basis_mismatch"),
        ("execution_timing_policy_hash", "sha256:other", "decision_execution_timing_policy_mismatch"),
        ("raw_signal", "SELL", "decision_raw_signal_mismatch"),
        ("final_signal", "HOLD", "decision_final_signal_mismatch"),
        ("blocked_filters", ["cost_edge"], "decision_filter_block_reason_mismatch"),
    ],
)
def test_canonical_mutations_fail_with_operator_reason_codes(
    field: str,
    value: object,
    reason: str,
) -> None:
    result = _compare(_decision(), _decision(**{field: value}))

    assert result.ok is False
    assert reason in result.report["reason_codes"]


def test_empty_order_rules_hash_is_not_promotion_grade() -> None:
    result = _compare(_decision(order_rules_hash=EMPTY_ORDER_RULES_HASH), _decision(order_rules_hash=EMPTY_ORDER_RULES_HASH))

    assert result.ok is False
    assert "canonical_decision_empty_order_rules_hash" in result.report["reason_codes"]


def test_legacy_shallow_decisions_are_diagnostic_only() -> None:
    legacy = {
        "signal_timestamp": "1714521660000",
        "candle_basis": "closed",
        "side": "BUY",
        "strategy_name": "sma_with_filter",
        "profile_content_hash": "sha256:profile",
        "market": "KRW-BTC",
        "interval": "1m",
        "fee_model_hash": "sha256:fee",
        "slippage_model_hash": "sha256:slippage",
        "blocked": False,
        "block_reason": "",
    }

    result = _compare(legacy, legacy)

    assert result.ok is False
    assert result.report["comparison_contract_version"] == "legacy_shallow_v1"
    assert result.report["legacy_schema"] is True
    assert result.report["canonical_schema"] is False
    assert result.report["promotion_grade_comparison"] is False
    assert result.report["recommended_next_action"] == "regenerate_decisions_with_repo_owned_export_commands"


def test_canonical_v2_comparison_uses_strategy_behavior_hash_not_sma_fields() -> None:
    research = _decision_v2(strategy_specific_payload={"curr_s": 102.0})
    runtime = _decision_v2(strategy_specific_payload={"curr_s": 999.0})

    result = _compare(research, runtime)

    assert result.ok is True
    assert result.report["comparison_contract_version"] == "canonical_decision_v2"
    assert result.report["canonical_v2_schema"] is True


def test_canonical_v2_strategy_behavior_hash_mismatch_fails_with_reason() -> None:
    result = _compare(
        _decision_v2(),
        _decision_v2(strategy_behavior_hash="sha256:changed"),
    )

    assert result.ok is False
    assert "decision_strategy_behavior_hash_mismatch" in result.report["reason_codes"]


def test_canonical_v2_feature_snapshot_hash_is_diagnostic_not_equivalence_authority() -> None:
    result = _compare(
        _decision_v2(feature_snapshot_hash="sha256:research_feature_snapshot"),
        _decision_v2(feature_snapshot_hash="sha256:runtime_feature_snapshot"),
    )

    assert result.ok is True
    assert "decision_feature_mismatch" not in result.report["reason_codes"]


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("policy_contract_hash", "policy_contract_hash_mismatch"),
        ("policy_input_hash", "policy_input_hash_mismatch"),
        ("policy_decision_hash", "policy_decision_hash_mismatch"),
    ],
)
def test_canonical_v2_policy_hash_mismatches_have_explicit_reasons(
    field: str,
    reason: str,
) -> None:
    result = _compare(
        _decision_v2(**{field: "sha256:research_policy"}),
        _decision_v2(**{field: "sha256:runtime_policy"}),
    )

    assert result.ok is False
    assert reason in result.report["reason_codes"]


def test_mixed_canonical_contract_versions_fail_with_reason_code() -> None:
    result = _compare(_decision(), _decision_v2())

    assert result.ok is False
    assert "canonical_decision_contract_version_mismatch" in result.report["reason_codes"]


def _export_payload(source: str, decisions: list[dict[str, object]], **overrides: object) -> dict[str, object]:
    plugin = resolve_research_strategy_plugin("sma_with_filter")
    payload: dict[str, object] = {
        "schema_version": 1,
        "decision_contract_version": 1,
        "source": source,
        "profile_content_hash": "sha256:profile",
        "dataset_content_hash": "sha256:data",
        "db_data_fingerprint": "sha256:data",
        "market": "KRW-BTC",
        "interval": "1m",
        "decision_count": len(decisions),
        "promotion_grade_export": True,
        "strategy_plugin_contract": plugin.contract_payload(),
        "strategy_plugin_contract_hash": plugin.contract_hash(),
        "strategy_decision_contract_version": plugin.decision_contract_version,
        "recommended_next_action": "none",
        "decisions": decisions,
        "generated_at": "2026-05-08T00:00:00+00:00",
    }
    payload.update(overrides)
    payload["content_hash"] = compute_decision_export_hash(payload)
    return payload


def test_decision_export_artifacts_can_produce_promotion_grade_report(tmp_path) -> None:
    research_path = tmp_path / "research.json"
    runtime_path = tmp_path / "runtime.json"
    research_path.write_text(
        json.dumps(_export_payload("research", [_decision()]), sort_keys=True),
        encoding="utf-8",
    )
    runtime_path.write_text(
        json.dumps(_export_payload("runtime_replay", [_decision()]), sort_keys=True),
        encoding="utf-8",
    )

    result = compare_decision_export_artifacts(
        research_artifact=load_decision_export_artifact(research_path, expected_source="research"),
        runtime_artifact=load_decision_export_artifact(runtime_path, expected_source="runtime_replay"),
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert result.ok is True
    assert result.report["outcome"] == "PASS_POSITIVE_EQUIVALENCE"
    assert "claims_scope" in result.report
    assert "state_coverage_matrix" in result.report
    assert result.report["claims_scope"]["positive_equivalence_state_classes"] == ["flat_no_dust_no_position"]
    assert result.report["promotion_grade_comparison"] is True
    assert result.report["research_export_content_hash"].startswith("sha256:")
    assert result.report["runtime_export_content_hash"].startswith("sha256:")
    assert result.report["research_strategy_plugin_contract_hash"].startswith("sha256:")
    assert result.report["runtime_strategy_plugin_contract_hash"].startswith("sha256:")


@pytest.mark.parametrize(
    ("mutation", "error"),
    (
        (lambda payload: payload.pop("strategy_plugin_contract"), "decision_export_strategy_plugin_contract_missing"),
        (lambda payload: payload.pop("strategy_plugin_contract_hash"), "decision_export_strategy_plugin_contract_hash_missing"),
        (
            lambda payload: payload.__setitem__("strategy_plugin_contract_hash", "sha256:changed"),
            "decision_export_strategy_plugin_contract_hash_mismatch",
        ),
        (
            lambda payload: payload.pop("strategy_decision_contract_version"),
            "decision_export_strategy_decision_contract_version_missing",
        ),
    ),
)
def test_decision_export_loader_rejects_unbound_strategy_plugin_contract(tmp_path, mutation, error) -> None:
    path = tmp_path / "research.json"
    payload = _export_payload("research", [_decision()])
    mutation(payload)
    payload["content_hash"] = compute_decision_export_hash(payload)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    with pytest.raises(ValueError, match=error):
        load_decision_export_artifact(path, expected_source="research")


def test_decision_export_artifact_compare_fails_on_strategy_plugin_hash_mismatch(tmp_path) -> None:
    research_path = tmp_path / "research.json"
    runtime_path = tmp_path / "runtime.json"
    research_path.write_text(
        json.dumps(_export_payload("research", [_decision()]), sort_keys=True),
        encoding="utf-8",
    )
    runtime_payload = _export_payload("runtime_replay", [_decision()])
    runtime_contract = dict(runtime_payload["strategy_plugin_contract"])  # type: ignore[arg-type]
    runtime_contract["runner_qualname"] = "ChangedRunner"
    runtime_payload["strategy_plugin_contract"] = runtime_contract
    runtime_payload["strategy_plugin_contract_hash"] = sha256_prefixed(runtime_contract)
    runtime_payload["content_hash"] = compute_decision_export_hash(runtime_payload)
    runtime_path.write_text(json.dumps(runtime_payload, sort_keys=True), encoding="utf-8")

    result = compare_decision_export_artifacts(
        research_artifact=load_decision_export_artifact(research_path, expected_source="research"),
        runtime_artifact=load_decision_export_artifact(runtime_path, expected_source="runtime_replay"),
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert result.ok is False
    assert result.report["promotion_grade_comparison"] is False
    assert result.report["outcome"] == "FAIL_EXPORT_BINDING"
    assert "export_strategy_plugin_contract_hash_pair_mismatch" in result.report["reason_codes"]


def test_promotion_grade_decision_without_position_authority_fails_incomplete() -> None:
    payload = _decision()
    payload.pop("position_authority")

    result = _compare(payload, payload)

    assert result.ok is False
    assert result.report["outcome"] == "FAIL_INCOMPLETE_CANONICAL_PAYLOAD"
    assert result.report["promotion_grade_comparison"] is False
    assert "canonical_decision_position_authority_missing" in result.report["reason_codes"]


@pytest.mark.parametrize(
    ("authority_field", "reason"),
    [
        ("position_state_hash", "canonical_decision_position_authority_position_hash_mismatch"),
        ("order_rules_hash", "canonical_decision_position_authority_order_rules_hash_mismatch"),
        ("fee_authority_hash", "canonical_decision_position_authority_fee_authority_hash_mismatch"),
    ],
)
def test_promotion_grade_position_authority_hash_mismatches_fail_incomplete(
    authority_field: str,
    reason: str,
) -> None:
    authority = dict(_decision()["position_authority"])  # type: ignore[arg-type]
    authority[authority_field] = "sha256:other"
    payload = _decision(position_authority=authority)

    result = _compare(payload, payload)

    assert result.ok is False
    assert result.report["outcome"] == "FAIL_INCOMPLETE_CANONICAL_PAYLOAD"
    assert result.report["promotion_grade_comparison"] is False
    assert reason in result.report["reason_codes"]


def test_non_flat_runtime_state_is_fail_closed_not_positive_equivalence() -> None:
    runtime = _decision(
        position_state_hash="sha256:runtime_open_position",
        entry_allowed=False,
        exit_allowed=True,
        effective_flat=False,
        normalized_exposure_active=True,
        position_authority={
            "state_class": "open_exposure",
            "unsupported_reason": "research_model_lacks_lot_native_authority",
            "open_lot_count": 1,
            "sellable_executable_lot_count": 1,
            "terminal_state": "open_exposure",
        },
    )

    result = _compare(_decision(), runtime)

    assert result.ok is False
    assert result.report["outcome"] == "FAIL_CLOSED_UNMODELED_STATE"
    assert result.report["claims_scope"]["full_lifecycle_equivalence_supported"] is False
    assert "open_exposure" in result.report["claims_scope"]["unsupported_state_classes"]
    assert result.report["state_coverage_matrix"]["open_exposure"]["fail_closed_expected"] is True
    assert result.report["recommended_next_action"] == (
        "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
    )


def _lot_native_decision_for_state(state: str) -> dict[str, object]:
    model = LotNativeResearchPositionModel.flat().apply_buy_fill(qty=0.0002)
    if state == "reserved_exit_pending":
        model = model.submit_sell()
    snapshot = model.authority_snapshot(
        order_rules_hash="sha256:order_rules",
        fee_authority_hash="sha256:fee_authority",
    )
    return _decision(
        position_state_hash=snapshot.position_state_hash,
        entry_allowed=snapshot.entry_allowed,
        exit_allowed=snapshot.exit_allowed,
        dust_state="no_dust",
        effective_flat=snapshot.entry_allowed,
        normalized_exposure_active=snapshot.open_lot_count > 0,
        position_authority=snapshot.as_dict(),
    )


def test_modeled_lot_native_open_exposure_can_pass_positive_equivalence() -> None:
    state_class = "open_exposure"
    decision = _lot_native_decision_for_state(state_class)

    result = _compare(decision, decision)

    assert result.ok is True
    assert result.report["outcome"] == "PASS_POSITIVE_EQUIVALENCE"
    assert state_class in result.report["claims_scope"]["positive_equivalence_state_classes"]
    assert result.report["state_coverage_matrix"][state_class]["positive_equivalence_supported"] is True
    assert result.report["state_coverage_matrix"][state_class]["fail_closed_expected"] is False
    assert result.report["claims_scope"]["full_lifecycle_equivalence_supported"] is False


def test_reserved_exit_pending_scaffolded_state_fails_closed_without_repo_owned_replay_evidence() -> None:
    state_class = "reserved_exit_pending"
    decision = _lot_native_decision_for_state(state_class)

    result = _compare(decision, decision)

    assert result.ok is False
    assert result.report["outcome"] == "FAIL_CLOSED_UNMODELED_STATE"
    assert state_class not in result.report["claims_scope"]["positive_equivalence_state_classes"]
    assert state_class in result.report["claims_scope"]["unsupported_state_classes"]
    assert result.report["state_coverage_matrix"][state_class]["positive_equivalence_supported"] is False
    assert result.report["state_coverage_matrix"][state_class]["fail_closed_expected"] is True
    assert result.report["claims_scope"]["fail_closed_unmodeled_state_count"] == 2
    assert result.report["recommended_next_action"] == (
        "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
    )


def test_modeled_lot_native_position_authority_mismatch_fails_actual_drift() -> None:
    research = _lot_native_decision_for_state("open_exposure")
    runtime = _lot_native_decision_for_state("open_exposure")
    authority = dict(runtime["position_authority"])  # type: ignore[arg-type]
    authority["open_lot_count"] = 1
    runtime["position_authority"] = authority

    result = _compare(research, runtime)

    assert result.ok is False
    assert result.report["outcome"] == "FAIL_ACTUAL_DRIFT"
    assert "decision_position_authority_mismatch" in result.report["reason_codes"]


def test_incomplete_runtime_positive_state_missing_lot_fields_fails_closed() -> None:
    event = runtime_decision_to_canonical_event(
        StrategyDecision(
            signal="HOLD",
            reason="position held: no exit rule triggered",
            context={
                "strategy": "sma_with_filter",
                "ts": 1_714_521_660_000,
                "raw_signal": "HOLD",
                "final_signal": "HOLD",
                "policy_contract_hash": "sha256:contract",
                "policy_input_hash": "sha256:input",
                "policy_decision_hash": "sha256:decision",
                "prev_s": 100.0,
                "prev_l": 101.0,
                "curr_s": 102.0,
                "curr_l": 101.0,
                "gap_ratio": 0.01,
                "fee_authority": {
                    "bid_fee": 0.0,
                    "ask_fee": 0.0,
                    "fee_source": "test",
                    "degraded": False,
                    "degraded_reason": "",
                },
                "order_rules": {"source": "test", "min_qty": 0.0001},
                "position_gate": {
                    "raw_total_asset_qty": 0.0002,
                    "open_lot_count": 2,
                    "dust_tracking_lot_count": 0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_lot_count": 2,
                    "open_exposure_qty": 0.0002,
                    "dust_tracking_qty": 0.0,
                    "reserved_exit_qty": 0.0,
                    "terminal_state": "open_exposure",
                    "entry_allowed": False,
                    "exit_allowed": True,
                    "effective_flat": False,
                    "normalized_exposure_active": True,
                    "dust_state": "no_dust",
                    "has_any_position_residue": True,
                },
            },
        ),
        market="KRW-BTC",
        interval="1m",
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        db_data_fingerprint="sha256:data",
        through_ts_ms=1_714_521_660_000,
        execution_timing_policy_hash="sha256:timing",
        strategy_version="sma_with_filter.research_runtime_contract.v2",
        strategy_decision_contract_version="research_sma_decision_contract.v3_entry_exit_risk_exit",
    ).as_dict()

    result = _compare(event, event)

    assert result.ok is False
    assert result.report["outcome"] == "FAIL_CLOSED_UNMODELED_STATE"
    assert event["position_authority"]["state_class"] == "open_exposure"  # type: ignore[index]
    assert event["position_authority"]["unsupported_reason"] == "research_model_lacks_lot_native_authority"  # type: ignore[index]
    assert result.report["state_coverage_matrix"]["open_exposure"]["fail_closed_expected"] is True
    assert event["policy_contract_hash"] == "sha256:contract"
    assert event["policy_input_hash"] == "sha256:input"
    assert event["policy_decision_hash"] == "sha256:decision"


def test_unmodeled_lifecycle_plus_actual_signal_drift_returns_actual_drift() -> None:
    research = _decision(
        position_state_hash="sha256:research_open_position",
        entry_allowed=False,
        exit_allowed=True,
        effective_flat=False,
        normalized_exposure_active=True,
        position_authority={
            "state_class": "research_model_lacks_lot_native_authority",
            "unsupported_reason": "research_model_lacks_lot_native_authority",
            "position_state_hash": "sha256:research_open_position",
            "order_rules_hash": "sha256:order_rules",
            "fee_authority_hash": "sha256:fee_authority",
        },
    )
    runtime = _decision(
        raw_signal="SELL",
        final_signal="SELL",
        side="SELL",
        position_state_hash="sha256:runtime_open_position",
        entry_allowed=False,
        exit_allowed=True,
        effective_flat=False,
        normalized_exposure_active=True,
        position_authority={
            "state_class": "open_exposure",
            "unsupported_reason": "research_model_lacks_lot_native_authority",
            "position_state_hash": "sha256:runtime_open_position",
            "order_rules_hash": "sha256:order_rules",
            "fee_authority_hash": "sha256:fee_authority",
        },
    )

    result = _compare(research, runtime)

    assert result.report["outcome"] == "FAIL_ACTUAL_DRIFT"
    assert result.report["actual_semantic_drift_count"] == 1
    assert result.report["lifecycle_unmodeled_mismatch_count"] == 0


def test_missing_runtime_decision_is_actual_drift() -> None:
    result = compare_decision_equivalence(
        research_decisions=[_decision()],
        runtime_decisions=[],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    assert result.report["outcome"] == "FAIL_ACTUAL_DRIFT"
    assert result.report["actual_semantic_drift_count"] == 1


def test_matching_unsupported_non_flat_states_do_not_pass() -> None:
    unsupported = _decision(
        final_signal="HOLD",
        side="HOLD",
        position_state_hash="sha256:cash_qty_simulation",
        entry_allowed=False,
        exit_allowed=True,
        dust_state="research_not_modeled",
        effective_flat=False,
        normalized_exposure_active=True,
        position_authority={
            "state_class": "research_model_lacks_lot_native_authority",
            "unsupported_reason": "research_model_lacks_lot_native_authority",
            "research_position_model": "cash_qty_simulation_v1",
        },
    )

    result = _compare(unsupported, unsupported)

    assert result.ok is False
    assert result.report["mismatch_count"] == 0
    assert result.report["outcome"] == "FAIL_CLOSED_UNMODELED_STATE"
    assert result.report["claims_scope"]["fail_closed_unmodeled_state_count"] == 2
    assert result.report["recommended_next_action"] == (
        "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
    )


def test_fail_closed_unmodeled_state_never_recommends_none() -> None:
    unsupported = _decision(
        final_signal="HOLD",
        side="HOLD",
        position_state_hash="sha256:cash_qty_simulation",
        entry_allowed=False,
        exit_allowed=True,
        dust_state="research_not_modeled",
        effective_flat=False,
        normalized_exposure_active=True,
        position_authority={
            "state_class": "research_model_lacks_lot_native_authority",
            "unsupported_reason": "research_model_lacks_lot_native_authority",
            "research_position_model": "cash_qty_simulation_v1",
        },
    )

    result = _compare(unsupported, unsupported)

    assert result.ok is False
    assert result.report["mismatch_count"] == 0
    assert result.report["reason_codes"] == []
    assert result.report["outcome"] == "FAIL_CLOSED_UNMODELED_STATE"
    assert result.report["recommended_next_action"] != "none"
    assert result.report["recommended_next_action"] == (
        "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
    )


@pytest.mark.parametrize(
    ("fixture_name", "state_class", "expected_outcome"),
    [
        ("flat_no_dust_no_position", "flat_no_dust_no_position", "PASS_POSITIVE_EQUIVALENCE"),
        ("harmless_dust_effective_flat", "dust_only", "FAIL_CLOSED_UNMODELED_STATE"),
        ("blocking_dust", "dust_only", "FAIL_CLOSED_UNMODELED_STATE"),
        ("open_exposure_sellable", "open_exposure", "FAIL_CLOSED_UNMODELED_STATE"),
        ("reserved_exit_pending", "reserved_exit_pending", "FAIL_CLOSED_UNMODELED_STATE"),
        ("non_executable_position", "non_executable_position", "FAIL_CLOSED_UNMODELED_STATE"),
        ("partial_buy_pending", "open_exposure", "FAIL_CLOSED_UNMODELED_STATE"),
        ("partial_sell_reserved", "reserved_exit_pending", "FAIL_CLOSED_UNMODELED_STATE"),
        ("pending_sell_after_dataset_end", "reserved_exit_pending", "FAIL_CLOSED_UNMODELED_STATE"),
        ("unresolved_order_recovery_blocked", "recovery_blocked", "FAIL_CLOSED_UNMODELED_STATE"),
    ],
)
def test_state_coverage_matrix_fixtures_classify_expected_outcomes(
    fixture_name: str,
    state_class: str,
    expected_outcome: str,
) -> None:
    decision = _decision()
    if state_class != "flat_no_dust_no_position":
        decision.update(
            {
                "position_state_hash": f"sha256:{state_class}",
                "entry_allowed": False,
                "exit_allowed": state_class == "open_exposure",
                "dust_state": _fixture_dust_state(fixture_name=fixture_name, state_class=state_class),
                "effective_flat": False,
                "normalized_exposure_active": state_class in {"open_exposure", "reserved_exit_pending", "recovery_blocked"},
                "position_authority": {
                    "state_class": state_class,
                    "unsupported_reason": "research_model_lacks_lot_native_authority",
                    "position_state_hash": f"sha256:{state_class}",
                    "order_rules_hash": decision["order_rules_hash"],
                    "fee_authority_hash": decision["fee_authority_hash"],
                },
            }
        )

    result = _compare(decision, decision)

    assert result.report["outcome"] == expected_outcome
    assert result.ok is (expected_outcome == "PASS_POSITIVE_EQUIVALENCE")
    assert state_class in result.report["state_coverage_matrix"]


def _fixture_dust_state(*, fixture_name: str, state_class: str) -> str:
    if fixture_name == "harmless_dust_effective_flat":
        return "harmless_dust"
    if fixture_name == "blocking_dust":
        return "blocking_dust"
    if state_class == "dust_only":
        return "dust_only"
    return "flat"


def test_decision_export_loader_rejects_tampered_hash(tmp_path) -> None:
    path = tmp_path / "research.json"
    payload = _export_payload("research", [_decision()])
    payload["decision_count"] = 2
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    with pytest.raises(ValueError, match="decision_export_content_hash_mismatch"):
        load_decision_export_artifact(path, expected_source="research")


def test_decision_export_loader_rejects_decision_count_mismatch(tmp_path) -> None:
    path = tmp_path / "research.json"
    payload = _export_payload("research", [_decision()])
    payload["decision_count"] = 2
    payload["content_hash"] = compute_decision_export_hash(payload)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    with pytest.raises(ValueError, match="decision_export_decision_count_mismatch"):
        load_decision_export_artifact(path, expected_source="research")


def test_decision_export_loader_rejects_source_mismatch(tmp_path) -> None:
    path = tmp_path / "research.json"
    path.write_text(
        json.dumps(_export_payload("runtime_replay", [_decision()]), sort_keys=True),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="decision_export_source_mismatch"):
        load_decision_export_artifact(path, expected_source="research")
