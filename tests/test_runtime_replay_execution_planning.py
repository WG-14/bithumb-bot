from __future__ import annotations

import sqlite3
from dataclasses import replace

from bithumb_bot.canonical_decision import (
    build_runtime_replay_execution_plan_bundle,
    runtime_decision_to_canonical_event,
    validate_promotion_artifact,
)
from bithumb_bot.core.sma_policy import EntryExecutionIntent, ExitExecutionIntent, PositionSnapshot, StrategyDecisionV2
from bithumb_bot.research.backtest_kernel import _execution_plan_evidence, _research_execution_plan_bundle
from bithumb_bot.runtime_sma_snapshot_builder import RuntimeSmaDecisionResult
from bithumb_bot.strategy.base import StrategyDecision


def _position_gate(*, open_exposure: bool = False) -> dict[str, object]:
    if open_exposure:
        return {
            "entry_allowed": False,
            "exit_allowed": True,
            "dust_state": "no_dust",
            "effective_flat": False,
            "normalized_exposure_active": True,
            "open_lot_count": 1,
            "dust_tracking_lot_count": 0,
            "reserved_exit_lot_count": 0,
            "sellable_executable_lot_count": 1,
            "sellable_executable_qty": 0.01,
            "open_exposure_qty": 0.01,
            "raw_total_asset_qty": 0.01,
            "has_any_position_residue": True,
            "has_executable_exposure": True,
            "terminal_state": "open_exposure",
        }
    return {
        "entry_allowed": True,
        "exit_allowed": False,
        "dust_state": "flat",
        "effective_flat": True,
        "normalized_exposure_active": False,
        "open_lot_count": 0,
        "dust_tracking_lot_count": 0,
        "sellable_executable_lot_count": 0,
        "has_any_position_residue": False,
    }


def _decision(*, raw_signal: str = "BUY", final_signal: str = "BUY") -> StrategyDecisionV2:
    is_sell = raw_signal == "SELL" or final_signal == "SELL"
    position = (
        PositionSnapshot(
            in_position=True,
            entry_allowed=False,
            exit_allowed=True,
            exit_block_reason="none",
            terminal_state="open_exposure",
            qty_open=0.01,
            raw_qty_open=0.01,
            raw_total_asset_qty=0.01,
            open_lot_count=1,
            sellable_executable_lot_count=1,
            dust_state="no_dust",
            effective_flat=False,
            has_executable_exposure=True,
            has_any_position_residue=True,
        )
        if is_sell
        else PositionSnapshot(
            in_position=False,
            entry_allowed=True,
            exit_allowed=False,
            terminal_state="flat",
            dust_state="flat",
            effective_flat=True,
        )
    )
    intent = (
        ExitExecutionIntent(
            side="SELL",
            intent="exit",
            pair="KRW-BTC",
            requires_execution_sizing=True,
        )
        if is_sell
        else EntryExecutionIntent(
            side="BUY",
            intent="enter",
            pair="KRW-BTC",
            requires_execution_sizing=True,
            budget_fraction_of_cash=0.5,
            max_budget_krw=100_000.0,
        )
    )
    return StrategyDecisionV2(
        strategy_name="sma_with_filter",
        raw_signal=raw_signal,  # type: ignore[arg-type]
        raw_reason="raw",
        entry_signal=final_signal,  # type: ignore[arg-type]
        entry_reason="entry",
        exit_signal="HOLD",
        exit_reason="no_exit",
        final_signal=final_signal,  # type: ignore[arg-type]
        final_reason="final",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=position,
        execution_intent=intent,
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"raw_signal": raw_signal, "final_signal": final_signal},
        policy_hash="sha256:pure",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )


def _runtime_result(*, raw_signal: str = "BUY", final_signal: str = "BUY") -> RuntimeSmaDecisionResult:
    decision = _decision(raw_signal=raw_signal, final_signal=final_signal)
    open_exposure = raw_signal == "SELL" or final_signal == "SELL"
    return RuntimeSmaDecisionResult(
        decision=decision,
        base_context={
            "strategy": "sma_with_filter",
            "position_gate": _position_gate(open_exposure=open_exposure),
            "position_state": (
                {"normalized_exposure": _position_gate(open_exposure=True)}
                if open_exposure
                else {}
            ),
            "fee_authority": {"bid_fee": 0.0, "ask_fee": 0.0, "fee_source": "test"},
            "order_rules": {"min_notional_krw": 5000.0},
            "features": {"fixture": "runtime_replay"},
        },
        position=object(),  # type: ignore[arg-type]
        exposure=object(),
        position_state=object(),
        candle_ts=123,
        market_price=10.0,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "test"},
    )


def _canonical_from_bundle(result: RuntimeSmaDecisionResult, bundle) -> dict[str, object]:
    decision = StrategyDecision(
        signal=result.decision.final_signal,  # type: ignore[arg-type]
        reason=result.decision.final_reason,
        context=dict(bundle.persistence_context),
    )
    return runtime_decision_to_canonical_event(
        decision,
        market="KRW-BTC",
        interval="1m",
        execution_plan_bundle=bundle,
        runtime_replay_planning_error=str(bundle.planning_error or ""),
    ).as_dict()


def test_runtime_replay_buy_submit_evidence_comes_from_typed_plan() -> None:
    result = _runtime_result(raw_signal="BUY", final_signal="BUY")
    readiness = {
        "cash_available": 500_000.0,
        "total_effective_exposure_notional_krw": 0.0,
        "residual_inventory_policy_allows_run": True,
    }
    bundle = build_runtime_replay_execution_plan_bundle(
        sqlite3.connect(":memory:"),
        result,
        readiness_payload=readiness,
    )

    canonical = _canonical_from_bundle(result, bundle)
    research_bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=500_000.0,
        buy_fraction=0.5,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=result.decision,
        candle_ts=123,
    )
    research_evidence = _execution_plan_evidence(research_bundle)

    assert bundle.submit_plan is not None
    assert canonical["submit_expected"] is True
    for key in (
        "execution_summary_hash",
        "execution_submit_plan_hash",
        "final_action",
        "submit_expected",
        "pre_submit_proof_status",
        "execution_block_reason",
        "submit_plan_source",
        "submit_plan_authority",
        "execution_engine",
    ):
        assert canonical[key] == research_evidence[key]
    assert canonical["decision_envelope_present"] is True
    assert canonical["execution_plan_bundle_present"] is True
    assert canonical["execution_plan_bundle_hash"] == bundle.content_hash()
    assert canonical["execution_evidence_source"] == "typed_execution_plan_bundle"
    assert canonical["typed_execution_summary_present"] is True
    assert canonical["artifact_grade"] == "promotion_candidate"
    assert canonical["authority_plane"] == "typed_execution_plan_bundle"
    assert canonical["decision_authority_source"] == "DecisionEnvelope.strategy_decision"
    assert canonical["persistence_context_authoritative"] == 0


def test_runtime_replay_blocked_plan_keeps_typed_block_reason() -> None:
    result = _runtime_result(raw_signal="BUY", final_signal="HOLD")
    bundle = build_runtime_replay_execution_plan_bundle(
        sqlite3.connect(":memory:"),
        result,
        readiness_payload={
            "cash_available": 500_000.0,
            "total_effective_exposure_notional_krw": 0.0,
            "residual_inventory_policy_allows_run": True,
        },
    )

    canonical = _canonical_from_bundle(result, bundle)

    assert canonical["submit_expected"] is False
    assert canonical["pre_submit_proof_status"] == "not_required"
    assert canonical["execution_block_reason"] != ""
    assert canonical["submit_plan_source"] == "strategy_position"
    assert canonical["submit_plan_authority"] in {
        "configured_strategy_order_size",
        "residual_inventory_delta",
    }
    assert canonical["execution_plan_bundle_present"] is True


def test_runtime_replay_missing_readiness_fails_closed_for_submit_signal() -> None:
    result = _runtime_result(raw_signal="BUY", final_signal="BUY")
    bundle = build_runtime_replay_execution_plan_bundle(
        sqlite3.connect(":memory:"),
        result,
        readiness_payload_builder=None,
    )

    canonical = _canonical_from_bundle(result, bundle)

    assert canonical["submit_expected"] is False
    assert canonical["final_action"] == "BLOCK_RUNTIME_REPLAY_EXECUTION"
    assert canonical["pre_submit_proof_status"] == "failed"
    assert canonical["execution_block_reason"] == "runtime_replay_execution_readiness_unavailable"
    assert canonical["execution_plan_bundle_present"] is False
    assert canonical["runtime_replay_planning_error"] == "runtime_replay_execution_readiness_unavailable"


def test_runtime_replay_submit_signal_ignores_dict_only_execution_authority() -> None:
    result = _runtime_result(raw_signal="BUY", final_signal="BUY")
    loose_context = dict(result.base_context)
    loose_context["execution_decision"] = {
        "final_action": "ENTER_STRATEGY_POSITION",
        "submit_expected": True,
        "pre_submit_proof_status": "not_required",
        "block_reason": "none",
        "buy_submit_plan": {
            "source": "loose_dict",
            "authority": "loose_dict",
            "submit_expected": True,
        },
    }
    decision = StrategyDecision(signal="BUY", reason="dict_only", context=loose_context)

    canonical = runtime_decision_to_canonical_event(
        decision,
        market="KRW-BTC",
        interval="1m",
    ).as_dict()

    assert canonical["submit_expected"] is False
    assert canonical["final_action"] == "BLOCK_RUNTIME_REPLAY_EXECUTION"
    assert canonical["execution_block_reason"] == "runtime_replay_execution_readiness_unavailable"
    assert canonical["submit_plan_source"] == "none"
    assert canonical["submit_plan_authority"] == "none"


def test_runtime_execution_plan_evidence_context_fallback_is_diagnostic_only() -> None:
    decision = StrategyDecision(
        signal="HOLD",
        reason="dict_only_hold",
        context={
            "strategy": "sma_with_filter",
            "execution_decision": {
                "execution_engine": "research_virtual",
                "final_action": "STRATEGY_HOLD",
                "submit_expected": False,
                "pre_submit_proof_status": "not_required",
                "block_reason": "none",
            },
            "decision_authority_source": "legacy_context",
            "execution_plan_bundle_present": False,
            "position_gate": _position_gate(),
            "fee_authority": {"fee_source": "test", "degraded": False},
            "order_rules": {"min_notional_krw": 5000.0},
        },
    )

    canonical = runtime_decision_to_canonical_event(
        decision,
        market="KRW-BTC",
        interval="1m",
    ).as_dict()
    validation = validate_promotion_artifact(canonical)

    assert canonical["execution_evidence_source"] == "diagnostic_context_fallback"
    assert canonical["artifact_grade"] == "diagnostic_only"
    assert canonical["authority_plane"] == "compatibility_context"
    assert canonical["promotion_rejection_reason"] == "context_fallback_execution_evidence"
    assert validation.promotion_grade is False
    assert "canonical_promotion_typed_execution_provenance_missing" in validation.reason_codes
