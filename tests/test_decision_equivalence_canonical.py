from __future__ import annotations

import json

import pytest

from bithumb_bot.canonical_decision import (
    EMPTY_ORDER_RULES_HASH,
    PROMOTION_REQUIRED_CANONICAL_FIELDS,
    research_decision_to_canonical_event,
    runtime_decision_to_canonical_event,
    validate_canonical_decision_payload,
    validate_promotion_artifact,
)
from bithumb_bot.decision_equivalence import (
    compare_decision_export_artifacts,
    compare_decision_equivalence,
    compute_decision_export_hash,
    load_decision_export_artifact,
    promotion_grade_decision_equivalence_fail_reasons,
    require_promotion_grade_decision_equivalence,
)
from bithumb_bot.position_authority import classify_decision_position_state
from bithumb_bot.lifecycle_evidence import (
    AccountingReplayEvidence,
    CanonicalLifecycleEvidenceBundle,
    LiveSubmitResponseEvidence,
    PaperSubmitFillEvidence,
    PositionLifecycleSnapshotEvidence,
    ResearchSimulatedFillEvidence,
    validate_lifecycle_evidence_scope,
)
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.lot_native_simulation import LotNativeResearchPositionModel
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin
from bithumb_bot.strategy.base import StrategyDecision

VALID_SHA256 = "sha256:" + "b" * 64


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
    bundle_evidence = {
        "schema_version": 1,
        "authority_label": "ExecutionPlanBundle",
        "summary_authority": "ExecutionDecisionSummary",
        "submit_plan_authority": "ExecutionSubmitPlan",
        "planning_error": None,
    }
    summary_evidence = {
        "execution_engine": "research_virtual",
        "final_action": "ENTER_STRATEGY_POSITION",
        "submit_expected": True,
        "pre_submit_proof_status": "not_required",
        "block_reason": "none",
    }
    submit_evidence = {
        "side": "BUY",
        "source": "research_backtest",
        "authority": "strategy_execution_intent",
        "final_action": "ENTER_STRATEGY_POSITION",
        "qty": 1.0,
        "notional_krw": 100.0,
        "target_exposure_krw": 100.0,
        "current_effective_exposure_krw": 0.0,
        "delta_krw": 100.0,
        "submit_expected": True,
        "pre_submit_proof_status": "not_required",
        "block_reason": "none",
        "idempotency_key": None,
        "schema_version": 1,
        "authority_label": "ExecutionSubmitPlan.final_payload.v1",
    }
    submit_evidence["content_hash"] = sha256_prefixed(submit_evidence)
    payload.update(
        {
            "decision_contract_version": 2,
            "strategy_version": "sma_with_filter.research_runtime_contract.v2",
            "strategy_decision_contract_version": "research_sma_decision_contract.v3_entry_exit_risk_exit",
            "runtime_decision_request_hash": VALID_SHA256,
            "runtime_strategy_set_manifest_hash": VALID_SHA256,
            "approved_profile_hash": VALID_SHA256,
            "policy_input_hash": VALID_SHA256,
            "policy_decision_hash": VALID_SHA256,
            "policy_contract_hash": VALID_SHA256,
            "replay_fingerprint_hash": VALID_SHA256,
            "decision_input_bundle_hash": VALID_SHA256,
            "decision_input_contract_hash": VALID_SHA256,
            "decision_input_bundle_payload_hash": VALID_SHA256,
            "market_feature_hash": VALID_SHA256,
            "canonical_feature_projection_hash": VALID_SHA256,
            "final_exit_decision_input_hash": VALID_SHA256,
            "snapshot_projector_version": "sma_with_filter_snapshot_projector_v1",
            "snapshot_projector_hash": VALID_SHA256,
            "strategy_evaluation_provenance": {
                "decision_boundary": "StrategyDecisionService.evaluate",
            },
            "execution_summary_hash": sha256_prefixed(summary_evidence),
            "execution_submit_plan_hash": sha256_prefixed(submit_evidence),
            "execution_plan_bundle_hash": sha256_prefixed(bundle_evidence),
            "execution_plan_bundle_evidence": bundle_evidence,
            "typed_execution_summary_evidence": summary_evidence,
            "execution_submit_plan_evidence": submit_evidence,
            "final_action": "ENTER_STRATEGY_POSITION",
            "submit_expected": True,
            "pre_submit_proof_status": "not_required",
            "execution_block_reason": "none",
            "submit_plan_source": "research_backtest",
            "submit_plan_authority": "strategy_execution_intent",
            "execution_engine": "research_virtual",
            "execution_plan_bundle_present": True,
            "execution_evidence_source": "typed_execution_plan_bundle",
            "typed_execution_summary_present": True,
            "decision_authority_source": "DecisionEnvelope.strategy_decision",
            "compatibility_fallback": False,
            "legacy_context_planning_used": False,
            "artifact_grade": "promotion_candidate",
            "authority_plane": "typed_execution_plan_bundle",
            "runtime_replay_planning_error": "",
            "promotion_rejection_reason": "",
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


def _mark_repo_owned_report(report: dict[str, object]) -> dict[str, object]:
    report["repo_owned_export_artifacts"] = True
    report["legacy_or_unverified_export"] = False
    report["research_export_source"] = "research"
    report["runtime_export_source"] = "runtime_replay"
    report["research_export_content_hash"] = "sha256:research-export"
    report["runtime_export_content_hash"] = "sha256:runtime-export"
    report["research_export_path"] = "/tmp/research_decisions.json"
    report["runtime_export_path"] = "/tmp/runtime_decisions.json"
    report["post_export_canonical_artifact_equivalence"] = True
    return report


def _complete_lifecycle_evidence() -> CanonicalLifecycleEvidenceBundle:
    return CanonicalLifecycleEvidenceBundle(
        research_simulated_fills=(
            ResearchSimulatedFillEvidence(
                comparison_key="decision-1",
                signal_ts=1,
                decision_ts=2,
                side="BUY",
                requested_qty=1.0,
                requested_notional=100.0,
                filled_qty=1.0,
                filled_notional=100.0,
                avg_fill_price=100.0,
                fill_status="filled",
                model_hash="sha256:model",
            ),
        ),
        paper_submit_fills=(
            PaperSubmitFillEvidence(
                comparison_key="decision-1",
                client_order_id="client-1",
                exchange_order_id="paper-1",
                side="BUY",
                requested_qty=1.0,
                requested_notional=100.0,
                filled_qty=1.0,
                filled_notional=100.0,
                submit_hash="sha256:paper-submit",
                fill_hash="sha256:paper-fill",
            ),
        ),
        live_submit_responses=(
            LiveSubmitResponseEvidence(
                comparison_key="decision-1",
                client_order_id="client-1",
                exchange_order_id="live-1",
                side="BUY",
                accepted=True,
                submit_request_hash="sha256:live-request",
                response_hash="sha256:live-response",
            ),
        ),
        accounting_replays=(
            AccountingReplayEvidence(
                comparison_key="decision-1",
                replay_id="replay-1",
                replay_status="matched",
                ledger_hash="sha256:ledger",
                position_hash="sha256:position",
                realized_pnl_hash="sha256:pnl",
            ),
        ),
        position_lifecycle_snapshots=(
            PositionLifecycleSnapshotEvidence(
                comparison_key="decision-1",
                snapshot_ts=3,
                lifecycle_state="open_exposure",
                position_state_hash="sha256:position",
                open_lot_count=1,
                sellable_lot_count=1,
                dust_lot_count=0,
            ),
        ),
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


def test_canonical_v2_feature_drift_exposes_structured_diagnostics() -> None:
    result = _compare(
        _decision_v2(
            market_feature_hash="sha256:research_feature",
            strategy_specific_payload={
                "previous_cross_state": "below",
                "allow_initial_cross": False,
                "gap_ratio": 0.01,
                "volatility_ratio": 0.02,
                "overextended_ratio": 0.03,
                "market_regime_snapshot": {"composite_regime": "uptrend_normal_vol_unknown"},
            },
            execution_intent={"side": "BUY"},
            position_authority={
                "state_class": "flat_no_dust_no_position",
                "unsupported_reason": "",
                "terminal_state": "flat",
                "position_state_hash": "sha256:position",
                "order_rules_hash": "sha256:order_rules",
                "fee_authority_hash": "sha256:fee_authority",
            },
        ),
        _decision_v2(
            market_feature_hash="sha256:runtime_feature",
            strategy_specific_payload={
                "previous_cross_state": "above",
                "allow_initial_cross": True,
                "gap_ratio": 0.04,
                "volatility_ratio": 0.05,
                "overextended_ratio": 0.06,
                "market_regime_snapshot": {"composite_regime": "downtrend_normal_vol_unknown"},
            },
            execution_intent={"side": "SELL"},
            position_authority={
                "state_class": "flat_no_dust_no_position",
                "unsupported_reason": "",
                "terminal_state": "flat",
                "position_state_hash": "sha256:position",
                "order_rules_hash": "sha256:order_rules",
                "fee_authority_hash": "sha256:fee_authority",
            },
        ),
    )

    assert result.ok is False
    mismatch = result.report["mismatches"][0]
    diagnostics = mismatch["drift_diagnostics"]
    assert diagnostics["research"]["previous_cross_state"] == "below"
    assert diagnostics["runtime"]["allow_initial_cross"] is True
    assert diagnostics["research"]["gap_ratio"] == 0.01
    assert diagnostics["runtime"]["volatility_ratio"] == 0.05
    assert diagnostics["research"]["overextended_ratio"] == 0.03
    assert diagnostics["runtime"]["market_regime_snapshot"]["composite_regime"] == "downtrend_normal_vol_unknown"
    assert diagnostics["research"]["position_terminal_state"] == "flat"
    assert diagnostics["research"]["position_effective_flat"] is True
    assert diagnostics["research"]["position_dust_state"] == "flat"
    assert diagnostics["research"]["fee_authority_hash"] == "sha256:fee_authority"
    assert diagnostics["research"]["order_rules_hash"] == "sha256:order_rules"
    assert diagnostics["research"]["execution_intent"]["side"] == "BUY"
    assert diagnostics["runtime"]["final_signal"] == "BUY"
    assert diagnostics["research"]["policy_input_hash"] == VALID_SHA256
    assert diagnostics["research"]["policy_decision_hash"] == VALID_SHA256
    assert diagnostics["research"]["decision_input_bundle_hash"] == VALID_SHA256
    assert diagnostics["research"]["execution_submit_plan_hash"].startswith("sha256:")


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


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("execution_summary_hash", "sha256:other_summary", "execution_summary_hash_mismatch"),
        ("execution_submit_plan_hash", "sha256:other_plan", "execution_submit_plan_hash_mismatch"),
        ("final_action", "BLOCK_ORDER_RULE", "execution_final_action_mismatch"),
        ("submit_expected", False, "execution_submit_expected_mismatch"),
        ("pre_submit_proof_status", "failed", "execution_pre_submit_proof_status_mismatch"),
        ("execution_block_reason", "order_rule_blocked", "execution_block_reason_mismatch"),
        ("submit_plan_source", "runtime_replay", "execution_submit_plan_source_mismatch"),
        ("submit_plan_authority", "other_authority", "execution_submit_plan_authority_mismatch"),
        ("execution_engine", "lot_native", "execution_engine_mismatch"),
    ],
)
def test_canonical_v2_execution_plan_mismatches_have_execution_reasons(
    field: str,
    value: object,
    reason: str,
) -> None:
    baseline = {
        "execution_summary_hash": "sha256:summary",
        "execution_submit_plan_hash": "sha256:plan",
        "final_action": "ENTER_STRATEGY_POSITION",
        "submit_expected": True,
        "pre_submit_proof_status": "not_required",
        "execution_block_reason": "none",
        "submit_plan_source": "research_backtest",
        "submit_plan_authority": "strategy_execution_intent",
        "execution_engine": "research_virtual",
    }
    changed = dict(baseline)
    changed[field] = value
    result = _compare(
        _decision_v2(**baseline),
        _decision_v2(**changed),
    )

    assert result.ok is False
    assert reason in result.report["reason_codes"]


def test_signal_match_but_execution_submit_plan_mismatch_fails_execution_equivalence() -> None:
    result = _compare(
        _decision_v2(final_signal="BUY", side="BUY", execution_submit_plan_hash="sha256:plan_a"),
        _decision_v2(final_signal="BUY", side="BUY", execution_submit_plan_hash="sha256:plan_b"),
    )

    assert result.ok is False
    assert "decision_final_signal_mismatch" not in result.report["reason_codes"]
    assert "execution_submit_plan_hash_mismatch" in result.report["reason_codes"]
    assert result.report["claims_scope"]["execution_plan_equivalence_supported"] is True
    assert result.report["claims_scope"]["full_lifecycle_equivalence_supported"] is False
    assert result.report["execution_equivalence"]["claim_scope"] == "submit_plan_equivalence_only"
    assert result.report["execution_equivalence"]["submit_plan_equivalence_ok"] is False
    assert "execution_submit_plan_hash_mismatch" in result.report["execution_equivalence"]["fail_reasons"]


def test_promotion_grade_equivalence_gate_accepts_positive_canonical_v2_export_report() -> None:
    report = _mark_repo_owned_report(dict(_compare(_decision_v2(), _decision_v2()).report))

    require_promotion_grade_decision_equivalence(report)
    assert promotion_grade_decision_equivalence_fail_reasons(report) == ()


def test_promotion_grade_equivalence_gate_rejects_pre_export_only_report() -> None:
    report = dict(_compare(_decision_v2(), _decision_v2()).report)
    report["repo_owned_export_artifacts"] = True
    report["legacy_or_unverified_export"] = False
    report["research_export_source"] = "research"
    report["runtime_export_source"] = "runtime_replay"
    report["research_export_content_hash"] = "sha256:research-export"
    report["runtime_export_content_hash"] = "sha256:runtime-export"

    reasons = promotion_grade_decision_equivalence_fail_reasons(report)

    assert "decision_equivalence_post_export_canonical_artifact_missing" in reasons


def test_promotion_grade_equivalence_gate_rejects_execution_plan_drift() -> None:
    report = _compare(
        _decision_v2(execution_submit_plan_hash="sha256:research_plan"),
        _decision_v2(execution_submit_plan_hash="sha256:runtime_plan"),
    ).report

    reasons = promotion_grade_decision_equivalence_fail_reasons(report)

    assert "decision_equivalence_ok_not_true" in reasons
    assert "decision_equivalence_outcome_not_positive" in reasons
    assert "decision_equivalence_reason_codes_nonempty" in reasons
    with pytest.raises(ValueError, match="decision_equivalence_not_promotion_grade"):
        require_promotion_grade_decision_equivalence(report)


def test_promotion_grade_equivalence_gate_rejects_missing_execution_plan_binding() -> None:
    research = _decision_v2()
    runtime = _decision_v2()
    research.pop("execution_submit_plan_hash")
    runtime.pop("execution_submit_plan_hash")
    report = _compare(research, runtime).report

    reasons = promotion_grade_decision_equivalence_fail_reasons(report)

    assert "decision_equivalence_missing_execution_submit_plan_hash" in reasons
    assert "decision_equivalence_incomplete_canonical" in reasons
    assert report["execution_equivalence"]["submit_plan_equivalence_ok"] is False
    assert "execution_submit_plan_evidence_missing" in report["execution_equivalence"]["fail_reasons"]


def test_execution_equivalence_report_does_not_overclaim_lifecycle_scope() -> None:
    report = _compare(_decision_v2(), _decision_v2()).report

    assert report["claim_scope"] == "submit_plan_equivalence_only"
    assert report["submit_plan_equivalence_supported"] is True
    assert report["full_lifecycle_equivalence_supported"] is False
    assert report["simulated_fill_equivalence_supported"] is False
    assert report["live_submit_equivalence_supported"] is False
    assert report["accounting_replay_equivalence_supported"] is False
    assert "execution_lifecycle_scope_not_supported" in report["unsupported_lifecycle_reasons"]

    execution = report["execution_equivalence"]
    assert execution["ok"] is True
    assert execution["submit_plan_equivalence_supported"] is True
    assert execution["submit_plan_equivalence_ok"] is True
    assert execution["simulated_fill_equivalence_supported"] is False
    assert execution["live_submit_equivalence_supported"] is False
    assert execution["accounting_replay_equivalence_supported"] is False
    assert execution["full_lifecycle_equivalence_supported"] is False
    assert "execution_lifecycle_scope_not_supported" in execution["unsupported_lifecycle_reasons"]
    assert "fill_equivalence_evidence_missing" in execution["unsupported_lifecycle_reasons"]
    assert "live_submit_equivalence_evidence_missing" in execution["unsupported_lifecycle_reasons"]
    assert "accounting_replay_equivalence_missing" in execution["unsupported_lifecycle_reasons"]
    assert "position_lifecycle_equivalence_evidence_missing" in execution["unsupported_lifecycle_reasons"]
    assert report["scope_badge"] == "SUBMIT_PLAN_EQUIVALENCE_ONLY"
    assert execution["scope_badge"] == "SUBMIT_PLAN_EQUIVALENCE_ONLY"
    assert execution["full_lifecycle_scope_badge"] == "FULL_LIFECYCLE_EQUIVALENCE_UNSUPPORTED"


def test_dict_only_lifecycle_evidence_is_rejected_and_stays_submit_plan_scoped() -> None:
    report = compare_decision_equivalence(
        research_decisions=[_decision_v2()],
        runtime_decisions=[_decision_v2()],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
        lifecycle_evidence={
            "research_simulated_fills": [{"semantic_hash": "sha256:forged"}],
            "full_lifecycle_equivalence_supported": True,
        },
    ).report

    assert report["claim_scope"] == "submit_plan_equivalence_only"
    assert report["full_lifecycle_equivalence_supported"] is False
    assert "lifecycle_evidence_not_typed" in report["unsupported_lifecycle_reasons"]
    assert report["execution_equivalence"]["full_lifecycle_equivalence_supported"] is False


def test_partial_typed_lifecycle_evidence_does_not_upgrade_to_full_lifecycle() -> None:
    partial = CanonicalLifecycleEvidenceBundle(
        research_simulated_fills=_complete_lifecycle_evidence().research_simulated_fills,
    )

    validation = validate_lifecycle_evidence_scope(partial)
    report = compare_decision_equivalence(
        research_decisions=[_decision_v2()],
        runtime_decisions=[_decision_v2()],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
        lifecycle_evidence=partial,
    ).report

    assert validation.simulated_fill_equivalence_supported is True
    assert validation.full_lifecycle_equivalence_supported is False
    assert report["simulated_fill_equivalence_supported"] is True
    assert report["full_lifecycle_equivalence_supported"] is False
    assert report["claim_scope"] == "submit_plan_equivalence_only"
    assert "live_submit_equivalence_evidence_missing" in report["unsupported_lifecycle_reasons"]


@pytest.mark.parametrize(
    ("missing_group", "expected_reason"),
    (
        ("research_simulated_fills", "fill_equivalence_evidence_missing"),
        ("paper_submit_fills", "paper_submit_fill_equivalence_evidence_missing"),
        ("live_submit_responses", "live_submit_equivalence_evidence_missing"),
        ("accounting_replays", "accounting_replay_equivalence_missing"),
        ("position_lifecycle_snapshots", "position_lifecycle_equivalence_evidence_missing"),
    ),
)
def test_full_lifecycle_requires_all_typed_evidence_groups(
    missing_group: str,
    expected_reason: str,
) -> None:
    groups = {
        "research_simulated_fills": _complete_lifecycle_evidence().research_simulated_fills,
        "paper_submit_fills": _complete_lifecycle_evidence().paper_submit_fills,
        "live_submit_responses": _complete_lifecycle_evidence().live_submit_responses,
        "accounting_replays": _complete_lifecycle_evidence().accounting_replays,
        "position_lifecycle_snapshots": _complete_lifecycle_evidence().position_lifecycle_snapshots,
    }
    groups[missing_group] = ()

    validation = validate_lifecycle_evidence_scope(CanonicalLifecycleEvidenceBundle(**groups))

    assert validation.full_lifecycle_equivalence_supported is False
    assert expected_reason in validation.reason_codes
    assert "execution_lifecycle_scope_not_supported" in validation.reason_codes


def test_complete_typed_hash_bound_lifecycle_evidence_can_enable_stronger_claim_scope() -> None:
    evidence = _complete_lifecycle_evidence()

    validation = validate_lifecycle_evidence_scope(evidence)
    report = compare_decision_equivalence(
        research_decisions=[_decision_v2()],
        runtime_decisions=[_decision_v2()],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
        lifecycle_evidence=evidence,
    ).report

    assert validation.full_lifecycle_equivalence_supported is True
    assert report["claim_scope"] == "full_lifecycle_equivalence"
    assert report["scope_badge"] == "FULL_LIFECYCLE_EQUIVALENCE"
    assert report["full_lifecycle_equivalence_supported"] is True
    assert report["execution_equivalence"]["full_lifecycle_equivalence_supported"] is True
    assert report["claims_scope"]["paper_submit_fill_equivalence_supported"] is True
    assert report["unsupported_lifecycle_reasons"] == []


def test_promotion_grade_gate_rejects_full_lifecycle_claim_without_lifecycle_evidence() -> None:
    report = _compare(_decision_v2(), _decision_v2()).report
    report["claims_scope"] = {
        **report["claims_scope"],
        "full_lifecycle_equivalence_supported": True,
    }

    reasons = promotion_grade_decision_equivalence_fail_reasons(report)

    assert "decision_equivalence_full_lifecycle_equivalence_evidence_missing" in reasons


def test_promotion_rejects_legacy_context_planning() -> None:
    payload = _decision_v2(legacy_context_planning_used=True)

    validation = validate_promotion_artifact(payload)

    assert validation.promotion_grade is False
    assert "canonical_promotion_legacy_context_planning" in validation.reason_codes


def test_canonical_promotion_rejects_compatibility_fallback_context() -> None:
    payload = _decision_v2(compatibility_fallback=True)

    validation = validate_canonical_decision_payload(payload, promotion_grade=True)

    assert validation.promotion_grade is False
    assert "canonical_promotion_compatibility_fallback" in validation.reason_codes


def test_promotion_requires_typed_execution_plan_bundle() -> None:
    payload = _decision_v2(
        execution_plan_bundle_present=False,
        execution_plan_bundle_hash="",
        typed_execution_summary_present=False,
    )

    validation = validate_promotion_artifact(payload)

    assert validation.promotion_grade is False
    assert "execution_plan_bundle_present" in validation.missing_fields
    assert "execution_plan_bundle_hash" in validation.missing_fields
    assert "typed_execution_summary_present" in validation.missing_fields
    assert "canonical_promotion_execution_plan_bundle_missing" in validation.reason_codes
    assert "canonical_promotion_execution_plan_bundle_hash_missing" in validation.reason_codes
    assert "canonical_promotion_typed_execution_summary_missing" in validation.reason_codes


def test_promotion_rejects_legacy_context_authority_and_runtime_planning_error() -> None:
    payload = _decision_v2(
        decision_authority_source="legacy_context",
        runtime_replay_planning_error="runtime_replay_execution_readiness_unavailable",
    )

    validation = validate_promotion_artifact(payload)

    assert validation.promotion_grade is False
    assert "canonical_promotion_legacy_context_authority" in validation.reason_codes
    assert "canonical_promotion_runtime_replay_planning_error" in validation.reason_codes


def test_promotion_rejects_context_fallback_execution_evidence() -> None:
    payload = _decision_v2(
        execution_evidence_source="diagnostic_context_fallback",
        artifact_grade="diagnostic_only",
        authority_plane="compatibility_context",
        promotion_rejection_reason="context_fallback_execution_evidence",
    )

    validation = validate_promotion_artifact(payload)

    assert validation.promotion_grade is False
    assert "canonical_promotion_typed_execution_provenance_missing" in validation.reason_codes


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("runtime_decision_request_hash", "canonical_promotion_runtime_decision_request_hash_missing"),
        (
            "runtime_strategy_set_manifest_hash",
            "canonical_promotion_runtime_strategy_set_manifest_hash_missing",
        ),
        ("approved_profile_hash", "canonical_promotion_approved_profile_hash_missing"),
    ],
)
def test_promotion_rejects_missing_runtime_binding_hashes(field: str, reason: str) -> None:
    validation = validate_promotion_artifact(_decision_v2(**{field: ""}))

    assert validation.promotion_grade is False
    assert field in validation.missing_fields
    assert reason in validation.reason_codes


def test_promotion_accepts_fully_bound_typed_runtime_provenance() -> None:
    validation = validate_promotion_artifact(_decision_v2())

    assert validation.promotion_grade is True
    assert validation.reason_codes == ()


def test_policy_hashes_are_canonical_diagnostics_not_promotion_required() -> None:
    assert "policy_contract_hash" not in PROMOTION_REQUIRED_CANONICAL_FIELDS
    assert "policy_input_hash" not in PROMOTION_REQUIRED_CANONICAL_FIELDS
    assert "policy_decision_hash" not in PROMOTION_REQUIRED_CANONICAL_FIELDS


@pytest.mark.parametrize(
    "field",
    [
        "execution_summary_hash",
        "execution_submit_plan_hash",
        "execution_plan_bundle_hash",
        "final_action",
        "submit_expected",
        "pre_submit_proof_status",
        "execution_block_reason",
        "submit_plan_source",
        "submit_plan_authority",
        "execution_engine",
    ],
)
def test_promotion_canonical_validation_requires_execution_fields(field: str) -> None:
    payload = _decision_v2()
    payload.pop(field, None)

    validation = validate_canonical_decision_payload(payload)
    result = _compare(payload, payload)

    assert field in PROMOTION_REQUIRED_CANONICAL_FIELDS
    assert validation.promotion_grade is False
    assert field in validation.missing_fields
    assert result.ok is False
    assert result.report["outcome"] == "FAIL_INCOMPLETE_CANONICAL_PAYLOAD"


def test_runtime_canonical_export_includes_policy_hashes_from_context() -> None:
    event = runtime_decision_to_canonical_event(
        StrategyDecision(
            signal="BUY",
            reason="sma golden cross",
            context={
                "strategy": "sma_with_filter",
                "ts": 1_714_521_660_000,
                "raw_signal": "BUY",
                "final_signal": "BUY",
                "policy_contract_hash": "sha256:contract",
                "policy_input_hash": "sha256:input",
                "policy_decision_hash": "sha256:decision",
                "execution_decision": {
                    "execution_engine": "research_virtual",
                    "final_action": "STRATEGY_HOLD",
                    "submit_expected": False,
                    "pre_submit_proof_status": "not_required",
                    "block_reason": "position held: no exit rule triggered",
                    "buy_submit_plan": {
                        "side": "HOLD",
                        "source": "runtime_replay",
                        "authority": "typed_execution_summary",
                        "final_action": "STRATEGY_HOLD",
                        "qty": None,
                        "notional_krw": None,
                        "target_exposure_krw": None,
                        "current_effective_exposure_krw": None,
                        "delta_krw": None,
                        "submit_expected": False,
                        "pre_submit_proof_status": "not_required",
                        "block_reason": "position held: no exit rule triggered",
                        "idempotency_key": None,
                    },
                },
                "prev_s": 100.0,
                "prev_l": 101.0,
                "curr_s": 102.0,
                "curr_l": 101.0,
                "gap_ratio": 0.01,
                "fee_authority": {"fee_source": "test", "degraded": False},
                "order_rules": {"source": "test", "min_qty": 0.0001},
                "position_gate": {
                    "terminal_state": "flat",
                    "entry_allowed": True,
                    "exit_allowed": False,
                    "effective_flat": True,
                    "normalized_exposure_active": False,
                    "dust_state": "flat",
                    "raw_total_asset_qty": 0.0,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_lot_count": 0,
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

    assert event["policy_contract_hash"] == "sha256:contract"
    assert event["policy_input_hash"] == "sha256:input"
    assert event["policy_decision_hash"] == "sha256:decision"


def test_research_canonical_export_includes_policy_hashes() -> None:
    event = research_decision_to_canonical_event(
        _decision_v2(
            policy_contract_hash="sha256:contract",
            policy_input_hash="sha256:input",
            policy_decision_hash="sha256:decision",
        ),
        profile_content_hash="sha256:profile",
        dataset_content_hash="sha256:data",
        execution_timing_policy_hash="sha256:timing",
    ).as_dict()

    assert event["policy_contract_hash"] == "sha256:contract"
    assert event["policy_input_hash"] == "sha256:input"
    assert event["policy_decision_hash"] == "sha256:decision"


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
    assert result.report["post_export_canonical_artifact_equivalence"] is True
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
    payload = _decision_v2()
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
    authority = dict(_decision_v2()["position_authority"])  # type: ignore[arg-type]
    authority[authority_field] = "sha256:other"
    payload = _decision_v2(position_authority=authority)

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
                "execution_decision": {
                    "execution_engine": "research_virtual",
                    "final_action": "STRATEGY_HOLD",
                    "submit_expected": False,
                    "pre_submit_proof_status": "not_required",
                    "block_reason": "position held: no exit rule triggered",
                    "buy_submit_plan": {
                        "side": "HOLD",
                        "source": "runtime_replay",
                        "authority": "typed_execution_summary",
                        "final_action": "STRATEGY_HOLD",
                        "qty": None,
                        "notional_krw": None,
                        "target_exposure_krw": None,
                        "current_effective_exposure_krw": None,
                        "delta_krw": None,
                        "submit_expected": False,
                        "pre_submit_proof_status": "not_required",
                        "block_reason": "position held: no exit rule triggered",
                        "idempotency_key": None,
                    },
                },
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
    assert result.report["outcome"] == "FAIL_INCOMPLETE_CANONICAL_PAYLOAD"
    assert "canonical_promotion_typed_execution_provenance_missing" in result.report["reason_codes"]
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
    if state_class != "flat_no_dust_no_position":
        entry = result.report["state_coverage_matrix"][state_class]
        assert entry["fail_closed_expected"] is True
        assert entry["positive_equivalence_supported"] is False
        assert state_class in result.report["claims_scope"]["unsupported_state_classes"]
        assert result.report["outcome"] != "PASS_POSITIVE_EQUIVALENCE"


@pytest.mark.parametrize(
    ("fixture_name", "state_class", "dust_state", "positive_supported"),
    [
        ("flat_no_dust_no_position", "flat_no_dust_no_position", "flat", True),
        ("open_exposure", "open_exposure", "no_dust", True),
        ("reserved_exit_pending", "reserved_exit_pending", "no_dust", False),
        ("harmless_dust_effective_flat", "dust_only", "harmless_dust", False),
        ("blocking_dust", "dust_only", "blocking_dust", False),
        ("non_executable_position", "non_executable_position", "flat", False),
        ("recovery_blocked", "recovery_blocked", "flat", False),
    ],
)
def test_explicit_research_runtime_state_fixtures_are_classified_and_fail_closed_honestly(
    fixture_name: str,
    state_class: str,
    dust_state: str,
    positive_supported: bool,
) -> None:
    if positive_supported and state_class == "open_exposure":
        decision = _lot_native_decision_for_state("open_exposure")
    else:
        effective_flat = fixture_name == "harmless_dust_effective_flat"
        decision = _decision(
            final_signal="HOLD",
            side="HOLD",
            position_state_hash=f"sha256:{fixture_name}",
            entry_allowed=state_class == "flat_no_dust_no_position" or effective_flat,
            exit_allowed=state_class == "open_exposure",
            dust_state=dust_state,
            effective_flat=state_class == "flat_no_dust_no_position" or effective_flat,
            normalized_exposure_active=state_class in {"open_exposure", "reserved_exit_pending", "recovery_blocked"},
            position_authority={
                "state_class": state_class,
                "unsupported_reason": "" if state_class == "flat_no_dust_no_position" else "research_model_lacks_lot_native_authority",
                "position_state_hash": f"sha256:{fixture_name}",
                "order_rules_hash": "sha256:order_rules",
                "fee_authority_hash": "sha256:fee_authority",
                "research_position_model": "lot_native_simulation_v1" if positive_supported else "lot_native_simulation_v1_partial",
            },
        )

    assert classify_decision_position_state(decision, source="research")[0] == state_class
    assert classify_decision_position_state(decision, source="runtime")[0] == state_class

    result = _compare(decision, decision)
    matrix_entry = result.report["state_coverage_matrix"][state_class]

    if positive_supported:
        assert result.report["outcome"] == "PASS_POSITIVE_EQUIVALENCE"
        assert matrix_entry["positive_equivalence_supported"] is True
        assert matrix_entry["fail_closed_expected"] is False
    else:
        assert result.report["outcome"] == "FAIL_CLOSED_UNMODELED_STATE"
        assert result.report["full_lifecycle_equivalence_supported"] is False
        assert matrix_entry["positive_equivalence_supported"] is False
        assert matrix_entry["fail_closed_expected"] is True
        assert result.report["recommended_next_action"] == (
            "extend_research_lot_native_position_model_before_claiming_lifecycle_equivalence"
        )


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


def test_lifecycle_evidence_comparison_key_mismatch_stays_submit_plan_scoped() -> None:
    evidence = _complete_lifecycle_evidence()
    mismatched = CanonicalLifecycleEvidenceBundle(
        research_simulated_fills=evidence.research_simulated_fills,
        paper_submit_fills=evidence.paper_submit_fills,
        live_submit_responses=(
            LiveSubmitResponseEvidence(
                comparison_key="different-decision",
                client_order_id="client-1",
                exchange_order_id="live-1",
                side="BUY",
                accepted=True,
                submit_request_hash="sha256:live-request",
                response_hash="sha256:live-response",
            ),
        ),
        accounting_replays=evidence.accounting_replays,
        position_lifecycle_snapshots=evidence.position_lifecycle_snapshots,
    )

    report = compare_decision_equivalence(
        research_decisions=[_decision_v2()],
        runtime_decisions=[_decision_v2()],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
        lifecycle_evidence=mismatched,
    ).report

    assert report["full_lifecycle_equivalence_supported"] is False
    assert report["claim_scope"] == "submit_plan_equivalence_only"
    assert "lifecycle_evidence_comparison_keys_mismatch" in report["unsupported_lifecycle_reasons"]


def test_promotion_grade_gate_accepts_complete_typed_lifecycle_evidence_when_other_gates_pass() -> None:
    report = compare_decision_equivalence(
        research_decisions=[_decision_v2()],
        runtime_decisions=[_decision_v2()],
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
        lifecycle_evidence=_complete_lifecycle_evidence(),
    ).report
    report = _mark_repo_owned_report(dict(report))

    require_promotion_grade_decision_equivalence(report)


def test_decision_export_loader_rejects_source_mismatch(tmp_path) -> None:
    path = tmp_path / "research.json"
    path.write_text(
        json.dumps(_export_payload("runtime_replay", [_decision()]), sort_keys=True),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="decision_export_source_mismatch"):
        load_decision_export_artifact(path, expected_source="research")
