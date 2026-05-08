from __future__ import annotations

import json

import pytest

from bithumb_bot.canonical_decision import EMPTY_ORDER_RULES_HASH, validate_canonical_decision_payload
from bithumb_bot.decision_equivalence import (
    compare_decision_export_artifacts,
    compare_decision_equivalence,
    compute_decision_export_hash,
    load_decision_export_artifact,
)


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
    assert result.report["recommended_next_action"] == "regenerate_decisions_with_canonical_schema_before_promotion"


def _export_payload(source: str, decisions: list[dict[str, object]], **overrides: object) -> dict[str, object]:
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
    assert result.report["promotion_grade_comparison"] is True
    assert result.report["research_export_content_hash"].startswith("sha256:")
    assert result.report["runtime_export_content_hash"].startswith("sha256:")


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
