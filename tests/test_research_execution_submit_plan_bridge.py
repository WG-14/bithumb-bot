from __future__ import annotations

from dataclasses import replace

import pytest

from bithumb_bot.core.sma_policy import (
    EntryExecutionIntent,
    ExitExecutionIntent,
    PositionSnapshot,
    StrategyDecisionV2,
)
from bithumb_bot.execution_service import ExecutionDecisionSummary, ExecutionSubmitPlan, SignalExecutionRequest
from bithumb_bot.research.backtest_kernel import (
    ResearchExecutionContext,
    ResearchExecutionPlanBundle,
    ResearchVirtualExecutionService,
    _execution_plan_evidence,
    _research_execution_plan_bundle,
    execution_submit_plan_to_research_request,
)
from bithumb_bot.research.execution_model import FixedBpsExecutionModel
from bithumb_bot.research.execution_planner_stage import DefaultExecutionPlanner, ExecutionPlanningRequest
from bithumb_bot.research.execution_simulator_stage import DefaultExecutionSimulator, ExecutionSimulationRequest


def _typed_decision(*, raw_signal: str = "BUY", final_signal: str = "BUY") -> StrategyDecisionV2:
    is_sell = str(final_signal).upper() == "SELL"
    position_snapshot = (
        PositionSnapshot(
            in_position=True,
            entry_allowed=False,
            exit_allowed=True,
            entry_block_reason="position_has_executable_exposure",
            exit_block_reason="none",
            terminal_state="research_simulated_open_exposure",
            qty_open=0.25,
            raw_qty_open=0.25,
            raw_total_asset_qty=0.25,
            open_lot_count=2500,
            sellable_executable_lot_count=2500,
            dust_state="no_dust",
            effective_flat=False,
            has_executable_exposure=True,
            has_any_position_residue=True,
        )
        if is_sell
        else PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False)
    )
    execution_intent = (
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
        raw_signal=raw_signal,
        raw_reason="typed_raw",
        entry_signal="HOLD" if is_sell else final_signal,
        entry_reason="position_has_executable_exposure" if is_sell else "typed_entry",
        exit_signal="SELL" if is_sell else "HOLD",
        exit_reason="typed_exit" if is_sell else "no_exit",
        final_signal=final_signal,
        final_reason="typed_final",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule="unit_exit" if is_sell else None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=position_snapshot,
        execution_intent=execution_intent,
        entry_decision=object(),  # type: ignore[arg-type]
        trace={},
        policy_hash="sha256:pure",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )


def _plan(
    *,
    side: str,
    qty: float | None,
    notional_krw: float | None,
    submit_expected: bool = True,
    block_reason: str = "none",
) -> ExecutionSubmitPlan:
    return ExecutionSubmitPlan(
        side=side,
        source="research_backtest",
        authority="strategy_execution_intent",
        final_action="ENTER_STRATEGY_POSITION" if side == "BUY" else "EXIT_STRATEGY_POSITION",
        qty=qty,
        notional_krw=notional_krw,
        target_exposure_krw=notional_krw,
        current_effective_exposure_krw=0.0,
        delta_krw=notional_krw,
        submit_expected=submit_expected,
        pre_submit_proof_status="not_required",
        block_reason=block_reason,
        idempotency_key=None,
    )


def _request(plan: ExecutionSubmitPlan):
    return execution_submit_plan_to_research_request(
        submit_plan=plan,
        context=_context(),
        reference_price=10.0,
        fee_rate=0.001,
    )


def _context() -> ResearchExecutionContext:
    return ResearchExecutionContext(
        signal_ts=100,
        decision_ts=200,
        timing_fields={"submit_ts_assumption": 201},
        depth_fields={"depth_available": False},
    )


def _summary(plan: ExecutionSubmitPlan) -> ExecutionDecisionSummary:
    return ExecutionDecisionSummary(
        raw_signal=plan.side,
        final_signal=plan.side,
        final_action=plan.final_action,
        submit_expected=plan.submit_expected,
        pre_submit_proof_status=plan.pre_submit_proof_status,
        block_reason=plan.block_reason,
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=plan.target_exposure_krw,
        current_effective_exposure_krw=plan.current_effective_exposure_krw,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=plan.delta_krw,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=plan if plan.side == "BUY" else None,
        target_shadow_decision=None,
        target_submit_plan=plan if plan.side == "SELL" else None,
    )


def test_buy_submit_plan_produces_request_from_plan_notional() -> None:
    request = _request(_plan(side="BUY", qty=999.0, notional_krw=12345.0))

    assert request is not None
    assert request.side == "BUY"
    assert request.requested_notional == 12345.0
    assert request.requested_qty == 999.0


def test_sell_submit_plan_produces_request_from_plan_qty() -> None:
    request = _request(_plan(side="SELL", qty=0.25, notional_krw=2500.0))

    assert request is not None
    assert request.side == "SELL"
    assert request.requested_qty == 0.25
    assert request.requested_notional == 2500.0


def test_submit_not_expected_produces_no_research_fill_request() -> None:
    request = _request(
        _plan(
            side="BUY",
            qty=None,
            notional_krw=None,
            submit_expected=False,
            block_reason="research_zero_buy_notional",
        )
    )

    assert request is None


def test_blocked_submit_plan_produces_no_research_fill_request() -> None:
    request = _request(
        _plan(
            side="BUY",
            qty=1.0,
            notional_krw=10_000.0,
            submit_expected=True,
            block_reason="strategy_performance_gate_blocked",
        )
    )

    assert request is None


def test_research_backtest_bundle_blocks_zero_size_before_request() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=0.0,
        buy_fraction=1.0,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
        allow_compatibility_fallback=True,
        promotion_grade_required=False,
    )

    assert bundle.status == "BLOCKED"
    assert bundle.reason_code == "research_zero_buy_notional"
    assert bundle.submit_plan is not None
    assert bundle.submit_plan.submit_expected is False
    assert bundle.compatibility_fallback is True
    assert bundle.promotion_grade is False
    assert bundle.as_dict()["artifact_grade"] == "diagnostic_only"
    assert bundle.as_dict()["authority_plane"] == "diagnostic_research_compatibility_only"
    assert bundle.as_dict()["execution_evidence_source"] == "research_compatibility_fallback"
    assert bundle.as_dict()["live_authoritative"] is False
    assert bundle.submit_plan is not None
    assert bundle.submit_plan.as_dict()["artifact_grade"] == "diagnostic_only"
    assert bundle.submit_plan.as_dict()["authority_plane"] == "diagnostic_research_compatibility_only"
    assert bundle.recommended_next_action == "regenerate_research_decisions_with_typed_execution_submit_plan"
    assert execution_submit_plan_to_research_request(
        submit_plan=bundle.submit_plan,
        context=_context(),
        reference_price=10.0,
        fee_rate=0.001,
    ) is None


def test_research_backtest_bundle_blocks_hold_without_submit_plan() -> None:
    bundle = _research_execution_plan_bundle(
        side="HOLD",
        cash=1_000_000.0,
        buy_fraction=1.0,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
        block_reason="strategy_hold",
    )

    assert bundle.status == "BLOCKED"
    assert bundle.reason_code == "strategy_hold"
    assert bundle.submit_plan is None


def test_research_compatibility_submit_plan_is_disabled_without_explicit_flag() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=1_000_000.0,
        buy_fraction=1.0,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
    )

    assert bundle.status == "BLOCKED"
    assert bundle.reason_code == "research_compatibility_submit_plan_disabled"
    assert bundle.submit_plan is None
    assert bundle.compatibility_fallback is False


def test_research_compatibility_fallback_evidence_is_not_promotion_grade() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=1_000_000.0,
        buy_fraction=1.0,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
        allow_compatibility_fallback=True,
        promotion_grade_required=False,
    )

    evidence = _execution_plan_evidence(bundle)

    assert evidence["compatibility_fallback"] is True
    assert evidence["research_compatibility_execution_fallback"] is True
    assert evidence["promotion_grade"] is False
    assert evidence["artifact_grade"] == "diagnostic_only"
    assert evidence["authority_plane"] == "diagnostic_research_compatibility_only"
    assert evidence["execution_evidence_source"] == "research_compatibility_fallback"
    assert evidence["promotion_rejection_reason"] == "compatibility_or_diagnostic_execution_evidence_not_promotion_grade"
    assert evidence["live_authoritative"] is False
    assert evidence["execution_scope"] == "submit_plan_admission_only"
    assert evidence["scope_badge"] == "SUBMIT_PLAN_EQUIVALENCE_ONLY"
    assert evidence["recommended_next_action"] == "regenerate_research_decisions_with_typed_execution_submit_plan"


def test_research_compatibility_fallback_is_blocked_for_promotion_grade() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=1_000_000.0,
        buy_fraction=1.0,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
        allow_compatibility_fallback=True,
    )

    assert bundle.status == "BLOCKED"
    assert bundle.reason_code == "promotion_requires_typed_execution_submit_plan"
    assert bundle.submit_plan is None
    assert bundle.compatibility_fallback is False
    assert bundle.promotion_grade is False

    evidence = _execution_plan_evidence(bundle)

    assert evidence["promotion_grade"] is False
    assert evidence["execution_plan_reason_code"] == "promotion_requires_typed_execution_submit_plan"
    assert evidence["recommended_next_action"] == "regenerate_research_decisions_with_typed_execution_submit_plan"


def test_research_virtual_execution_service_public_input_is_submit_plan() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )

    fill = service.simulate_submit_plan(
        submit_plan=_plan(side="BUY", qty=None, notional_krw=12_000.0),
        context=_context(),
        reference_price=10.0,
    )

    assert fill is not None
    assert fill.side == "BUY"
    assert fill.requested_notional == 12_000.0


def test_research_virtual_execution_service_execute_accepts_typed_signal_request_boundary() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )
    plan = _plan(side="BUY", qty=None, notional_krw=12_000.0)

    fill = service.execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=100,
            market_price=10.0,
            execution_decision_summary=_summary(plan),
            research_execution_context=_context(),
        ),
    )

    assert fill is not None
    assert fill.side == "BUY"
    assert fill.requested_notional == 12_000.0


def test_research_virtual_execution_service_execute_rejects_untyped_research_context() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )
    plan = _plan(side="BUY", qty=None, notional_krw=12_000.0)

    with pytest.raises(ValueError, match="research_execution_context_not_typed"):
        service.execute(
            SignalExecutionRequest(
                signal="BUY",
                ts=100,
                market_price=10.0,
                execution_decision_summary=_summary(plan),
                research_execution_context={
                    "signal_ts": 100,
                    "decision_ts": 200,
                    "timing_fields": {},
                    "depth_fields": {},
                },
            ),
        )


def test_research_virtual_execution_service_execute_rejects_missing_typed_plan() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )

    with pytest.raises(ValueError, match="research_missing_typed_submit_plan"):
        service.execute(
            SignalExecutionRequest(
                signal="BUY",
                ts=100,
                market_price=10.0,
                research_execution_context=_context(),
            ),
        )


def test_research_virtual_execution_service_rejects_forged_dict_submit_plan() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )

    with pytest.raises(ValueError, match="research_submit_plan_not_typed"):
        service.simulate_submit_plan(
            submit_plan={"side": "BUY"},  # type: ignore[arg-type]
            context=_context(),
            reference_price=10.0,
        )


def test_research_virtual_execution_service_execute_rejects_forged_dict_submit_plan() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )
    summary = object.__new__(ExecutionDecisionSummary)
    for field_name, value in {
        "raw_signal": "BUY",
        "final_signal": "BUY",
        "final_action": "ENTER_STRATEGY_POSITION",
        "submit_expected": True,
        "pre_submit_proof_status": "not_required",
        "block_reason": "none",
        "strategy_sell_candidate": None,
        "residual_sell_candidate": None,
        "target_exposure_krw": 10_000.0,
        "current_effective_exposure_krw": 0.0,
        "tracked_residual_exposure_krw": None,
        "buy_delta_krw": 10_000.0,
        "residual_live_sell_mode": "block",
        "residual_buy_sizing_mode": "block",
        "residual_submit_plan": None,
        "buy_submit_plan": {"side": "BUY", "submit_expected": True},
        "target_shadow_decision": None,
        "target_submit_plan": None,
        "pre_trade_economics": None,
        "signal_flow": None,
    }.items():
        object.__setattr__(summary, field_name, value)

    with pytest.raises(ValueError, match="research_dict_only_submit_plan_not_authority:buy_submit_plan"):
        service.execute(
            SignalExecutionRequest(
                signal="BUY",
                ts=100,
                market_price=10.0,
                execution_decision_summary=summary,
                research_execution_context=_context(),
            ),
        )


def test_research_virtual_execution_service_blocked_plan_creates_no_fill() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )

    fill = service.simulate_submit_plan(
        submit_plan=_plan(
            side="BUY",
            qty=None,
            notional_krw=None,
            submit_expected=False,
            block_reason="research_zero_buy_notional",
        ),
        context=_context(),
        reference_price=10.0,
    )

    assert fill is None


def test_research_virtual_execution_service_execute_blocked_plan_creates_no_fill() -> None:
    service = ResearchVirtualExecutionService(
        execution_model=FixedBpsExecutionModel(fee_rate=0.001, slippage_bps=0.0),
        fee_rate=0.001,
    )
    plan = _plan(
        side="BUY",
        qty=None,
        notional_krw=None,
        submit_expected=False,
        block_reason="research_zero_buy_notional",
    )

    fill = service.execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=100,
            market_price=10.0,
            execution_decision_summary=_summary(plan),
            research_execution_context=_context(),
        ),
    )

    assert fill is None


def test_malformed_submit_plan_fails_closed_before_research_request() -> None:
    with pytest.raises(ValueError, match="research_submit_plan_not_typed"):
        execution_submit_plan_to_research_request(
            submit_plan={"side": "BUY"},  # type: ignore[arg-type]
            context=_context(),
            reference_price=10.0,
            fee_rate=0.001,
        )


def test_execution_simulator_is_independent_stage() -> None:
    simulator = DefaultExecutionSimulator()

    assert simulator.run(object()) is not None
    assert DefaultExecutionSimulator.__module__ == "bithumb_bot.research.execution_simulator_stage"


def test_execution_planner_is_independent_typed_plan_stage() -> None:
    class Candle:
        ts = 1_700_000_000_000
        close = 100.0

    class Ledger:
        cash = 1_000_000.0

    result = DefaultExecutionPlanner().plan(
        ExecutionPlanningRequest(
            candle=Candle(),
            event=object(),
            ledger=Ledger(),
            strategy_name="sma_with_filter",
            action="BUY",
            decision_reason="unit_buy",
            sellable_qty=0.0,
            buy_fraction=0.99,
            promotion_grade_policy_required=True,
            allow_execution_compatibility_fallback=False,
            policy_drives_execution=True,
            policy_decision=_typed_decision(final_signal="BUY"),
        )
    )

    assert result.plan_bundle.submit_plan is not None
    assert isinstance(result.plan_bundle.submit_plan, ExecutionSubmitPlan)
    assert result.evidence["typed_submit_plan"] is True
    assert result.evidence["typed_execution_boundary"] == "SignalExecutionRequest"


def test_execution_simulator_accepts_typed_request_boundary() -> None:
    request = ExecutionSimulationRequest(
        dataset=object(),
        candle=object(),
        candle_index=0,
        event=object(),
        ledger=object(),
        timing_policy=object(),
        execution_model=object(),
        fee_rate=0.001,
        strategy_name="unit",
        action="HOLD",
        decision_reason="strategy_hold",
        regime_snapshot={},
        decision_hash="sha256:test",
        sellable_qty=0.0,
        buy_fraction=0.0,
        promotion_grade_policy_required=True,
        allow_execution_compatibility_fallback=False,
        policy_drives_execution=True,
        policy_decision=None,
    )

    outcome = DefaultExecutionSimulator().execute(request)

    assert outcome.fill is None
    assert outcome.pending_fill is None


def test_direct_cash_fraction_is_not_request_authority_when_plan_exists() -> None:
    cash = 1_000_000.0
    legacy_buy_fraction = 0.5
    request = _request(_plan(side="BUY", qty=None, notional_krw=12_000.0))

    assert request is not None
    assert request.requested_notional == 12_000.0
    assert request.requested_notional != cash * legacy_buy_fraction


def test_research_buy_bundle_uses_typed_execution_planner_summary() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=500_000.0,
        buy_fraction=0.99,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=_typed_decision(final_signal="BUY"),
        candle_ts=987654321,
    )

    assert bundle.summary is not None
    assert bundle.submit_plan is not None
    assert bundle.submit_plan.submit_expected is True
    assert bundle.submit_plan.notional_krw == 100_000.0
    assert bundle.submit_plan.notional_krw != 500_000.0 * 0.99
    assert bundle.authority == bundle.submit_plan.authority
    assert bundle.compatibility_fallback is False


def test_research_typed_hold_blocks_without_fill_request() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=500_000.0,
        buy_fraction=1.0,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=_typed_decision(raw_signal="BUY", final_signal="HOLD"),
        candle_ts=123,
    )

    assert bundle.summary is not None
    assert bundle.submit_plan is not None
    assert bundle.submit_plan.submit_expected is False
    assert execution_submit_plan_to_research_request(
        submit_plan=bundle.submit_plan,
        context=_context(),
        reference_price=10.0,
        fee_rate=0.001,
    ) is None


def test_research_typed_missing_submit_plan_does_not_fall_back_to_compatibility_sizing() -> None:
    bundle = _research_execution_plan_bundle(
        side="SELL",
        cash=500_000.0,
        buy_fraction=1.0,
        sellable_qty=0.25,
        reference_price=10.0,
        policy_decision=_typed_decision(raw_signal="SELL", final_signal="SELL"),
        candle_ts=123,
        allow_compatibility_fallback=True,
        promotion_grade_required=False,
    )

    assert bundle.status == "BLOCKED"
    assert bundle.summary is not None
    assert bundle.submit_plan is None
    assert bundle.reason_code != "none"
    assert bundle.compatibility_fallback is False


def test_typed_sell_missing_typed_submit_plan_does_not_call_compatibility_in_promotion_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_called(**_: object) -> ExecutionSubmitPlan:
        raise AssertionError("compatibility submit planning must not run for typed promotion SELL")

    monkeypatch.setattr(
        "bithumb_bot.research.compatibility_execution_planning._research_execution_submit_plan",
        fail_if_called,
    )

    with pytest.raises(ValueError, match="research_submit_plan_missing|research_typed_submit_plan_missing"):
        _research_execution_plan_bundle(
            side="SELL",
            cash=500_000.0,
            buy_fraction=1.0,
            sellable_qty=0.25,
            reference_price=10.0,
            policy_decision=_typed_decision(raw_signal="SELL", final_signal="SELL"),
            candle_ts=123,
            promotion_grade_required=True,
            allow_compatibility_fallback=False,
        )


def test_diagnostic_submit_plan_payload_cannot_be_top_level_promotion_evidence() -> None:
    diagnostic_plan = replace(
        _plan(side="BUY", qty=1.0, notional_krw=10_000.0),
        extra_payload={
            "compatibility_fallback": True,
            "research_compatibility_execution_fallback": True,
            "promotion_grade": False,
            "artifact_grade": "diagnostic_only",
            "authority_plane": "diagnostic_research_compatibility_only",
            "execution_evidence_source": "research_compatibility_fallback",
            "live_authoritative": False,
        },
    )
    bundle = ResearchExecutionPlanBundle(
        submit_plan=diagnostic_plan,
        summary=_summary(diagnostic_plan),
        source=diagnostic_plan.source,
        authority=diagnostic_plan.authority,
        execution_engine="research_virtual",
        status="PLANNED",
        reason_code="none",
        compatibility_fallback=False,
        promotion_grade=True,
        recommended_next_action="none",
    )

    evidence = _execution_plan_evidence(bundle)

    assert evidence["execution_submit_plan_evidence"]["compatibility_fallback"] is True
    assert evidence["execution_submit_plan_evidence"]["artifact_grade"] == "diagnostic_only"
    assert evidence["compatibility_fallback"] is True
    assert evidence["research_compatibility_execution_fallback"] is True
    assert evidence["promotion_grade"] is False
    assert evidence["artifact_grade"] == "diagnostic_only"
    assert evidence["authority_plane"] == "diagnostic_research_compatibility_only"
    assert evidence["execution_evidence_source"] == "research_compatibility_fallback"
    assert evidence["promotion_rejection_reason"]
    assert evidence["recommended_next_action"] != "none"
    assert evidence["execution_plan_bundle_evidence"]["artifact_grade"] == "diagnostic_only"
