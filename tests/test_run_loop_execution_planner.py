from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.core.sma_policy import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionSubmitPlan,
    TypedExecutionPlanningInput,
)
from bithumb_bot.run_loop_compatibility import RunLoopCompatibilityPlanner
from bithumb_bot.run_loop_execution_planner import ExecutionPlanner


@dataclass(frozen=True)
class _Readiness:
    payload: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


def _summary(*, target_plan: ExecutionSubmitPlan | None = None) -> ExecutionDecisionSummary:
    return ExecutionDecisionSummary(
        raw_signal="BUY" if target_plan is not None else "HOLD",
        final_signal="BUY" if target_plan is not None else "HOLD",
        final_action="REBALANCE_TO_TARGET" if target_plan is not None else "STRATEGY_HOLD",
        submit_expected=target_plan is not None and target_plan.submit_expected,
        pre_submit_proof_status="passed" if target_plan is not None else "not_required",
        block_reason="none" if target_plan is not None else "raw_hold_no_entry_or_exit_signal",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=100_000.0 if target_plan is not None else None,
        current_effective_exposure_krw=0.0 if target_plan is not None else None,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=100_000.0 if target_plan is not None else None,
        residual_live_sell_mode="telemetry",
        residual_buy_sizing_mode="telemetry",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision={"target_policy_action": "use_existing_target"} if target_plan else None,
        target_submit_plan=target_plan,
    )


def _typed_decision(
    *,
    raw_signal: str = "BUY",
    final_signal: str = "BUY",
    final_reason: str = "typed_reason",
) -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name="sma_with_filter",
        raw_signal=raw_signal,
        raw_reason="typed_raw_reason",
        entry_signal=final_signal,
        entry_reason=final_reason,
        exit_signal="HOLD",
        exit_reason="no_exit",
        final_signal=final_signal,
        final_reason=final_reason,
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=EntryExecutionIntent(
            side="BUY",
            intent="enter",
            pair="KRW-BTC",
            requires_execution_sizing=True,
            budget_fraction_of_cash=1.0,
            max_budget_krw=100_000.0,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"final_signal": final_signal, "final_reason": final_reason},
        policy_hash="sha256:pure",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )


def test_diagnostic_legacy_equivalence_cannot_create_promotion_artifact() -> None:
    plan = ExecutionSubmitPlan(
        side="BUY",
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="REBALANCE_TO_TARGET",
        qty=0.001,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="target-plan-key",
    )
    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness(
            {
                "residual_inventory_state": "none",
                "policy_input_hash": "sha256:policy-input",
            }
        ),
        target_state_resolver=lambda *_args, **_kwargs: {
            "previous_target_exposure_krw": 0.0,
            "target_policy_metadata": {"target_origin": "runtime_state"},
        },
        summary_builder=lambda **_kwargs: _summary(target_plan=plan),
    )

    result = RunLoopCompatibilityPlanner(
        planner_factory=lambda: planner,
        runtime_handoff_fn=object(),
    ).plan_legacy_context(
        object(),
        decision_context={
            "strategy": "sma_with_filter",
            "policy_input_hash": "sha256:policy-input",
            "policy_decision_hash": "sha256:policy-decision",
        },
        signal="BUY",
        reason="cross_up",
        updated_ts=123,
        signal_handoff_fn=object(),
    )

    assert result.planning_error is None
    assert result.execution_decision_summary is not None
    assert result.execution_decision["target_submit_plan"]["source"] == "target_delta"  # type: ignore[index]
    assert result.context["execution_decision"] == result.execution_decision
    assert result.context["policy_input_hash"] == "sha256:policy-input"
    assert result.context["policy_decision_hash"] == "sha256:policy-decision"
    assert result.context["target_origin"] == "runtime_state"
    assert result.context["legacy_context_planning_used"] is True
    assert result.context["compatibility_fallback"] is True
    assert result.context["promotion_grade"] is False
    assert result.context["recommended_next_action"] == "regenerate_decision_with_typed_execution_authority"


def test_execution_planner_rejects_legacy_context_planning_even_when_requested() -> None:
    result = ExecutionPlanner().plan_strategy_decision(
        object(),
        decision_context={"strategy": "sma_with_filter"},
        signal="BUY",
        reason="cross_up",
        updated_ts=123,
        allow_legacy_context_planning=True,
    )

    assert result.execution_decision_summary is None
    assert result.context["promotion_grade"] is False
    assert result.context["execution_block_reason"] == "legacy_context_planning_diagnostic_only"


def test_run_loop_execution_planner_failure_returns_block_recovery_payload() -> None:
    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = RunLoopCompatibilityPlanner(
        planner_factory=lambda: planner,
        runtime_handoff_fn=object(),
    ).plan_legacy_context(
        object(),
        decision_context={"strategy": "sma_with_filter"},
        signal="BUY",
        reason="cross_up",
        updated_ts=123,
        signal_handoff_fn=object(),
    )

    assert result.execution_decision_summary is None
    assert result.execution_decision == {}
    assert result.context["execution_decision"] == result.execution_decision
    assert result.context["final_action"] == "BLOCK_RECOVERY"
    assert result.context["submit_expected"] is False
    assert result.context["pre_submit_proof_status"] == "failed"
    assert result.context["execution_block_reason"] == "execution_decision_unavailable"
    assert result.context["execution_decision_authoritative"] == 0
    assert result.context["promotion_grade"] is False
    assert result.planning_error == "RuntimeError: boom"


def test_plan_envelope_uses_typed_decision_over_conflicting_base_context() -> None:
    seen: dict[str, object] = {}

    def _summary_builder(**kwargs) -> ExecutionDecisionSummary:
        typed_input = kwargs["typed_input"]
        assert isinstance(typed_input, TypedExecutionPlanningInput)
        seen["final_signal"] = typed_input.strategy_decision.final_signal
        seen["market_price"] = typed_input.market_price
        return _summary()

    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness({"cash_available": 1_000_000.0}),
        target_state_resolver=lambda *_args, **_kwargs: {
            "previous_target_exposure_krw": 0.0,
            "target_policy_metadata": {},
        },
        summary_builder=_summary_builder,
    )
    envelope = DecisionEnvelope(
        strategy_decision=_typed_decision(final_signal="BUY"),
        candle_ts=123,
        market_price=10.0,
        base_context={"final_signal": "SELL", "signal": "SELL", "last_close": 999.0},
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "test"},
    )

    result = planner.plan_envelope(object(), envelope, updated_ts=456)

    assert result.planning_error is None
    assert seen == {"final_signal": "BUY", "market_price": 10.0}
    assert result.persistence_context["final_signal"] == "BUY"
    assert result.persistence_context["signal"] == "BUY"
    assert result.persistence_context["last_close"] == 10.0
    assert result.persistence_context["decision_authority_source"] == "DecisionEnvelope.strategy_decision"
    assert result.persistence_context["persistence_context_authoritative"] == 0
    assert result.persistence_context["execution_plan_bundle_present"] is True
    assert result.persistence_context["execution_plan_bundle"]["authority_label"] == "ExecutionPlanBundle"  # type: ignore[index]
    assert result.persistence_context["execution_plan_bundle"]["submit_plan_authority"] == "none"  # type: ignore[index]
    assert str(result.persistence_context["execution_plan_bundle_hash"]).startswith("sha256:")


def test_plan_envelope_freezes_mutable_base_context_before_planning() -> None:
    seen: dict[str, object] = {}

    def _summary_builder(**kwargs) -> ExecutionDecisionSummary:
        typed_input = kwargs["typed_input"]
        assert isinstance(typed_input, TypedExecutionPlanningInput)
        seen["final_signal"] = typed_input.strategy_decision.final_signal
        seen["observed_context_signal"] = typed_input.observability_context["signal"]
        return _summary()

    base_context: dict[str, object] = {"signal": "SELL", "nested": {"authority": "legacy"}}
    envelope = DecisionEnvelope(
        strategy_decision=_typed_decision(final_signal="BUY"),
        candle_ts=123,
        market_price=10.0,
        base_context=base_context,
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "test"},
    )
    base_context["signal"] = "HOLD"
    base_context["nested"] = {"authority": "mutated"}
    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness({"cash_available": 1_000_000.0}),
        target_state_resolver=lambda *_args, **_kwargs: {
            "previous_target_exposure_krw": 0.0,
            "target_policy_metadata": {},
        },
        summary_builder=_summary_builder,
    )

    result = planner.plan_envelope(object(), envelope, updated_ts=456)

    assert result.planning_error is None
    assert seen == {"final_signal": "BUY", "observed_context_signal": "BUY"}
    assert result.persistence_context["signal"] == "BUY"
    assert result.persistence_context["nested"] == {"authority": "legacy"}


def test_plan_envelope_submit_plan_is_selected_from_typed_summary_not_persistence_context() -> None:
    target_plan = ExecutionSubmitPlan(
        side="BUY",
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="REBALANCE_TO_TARGET",
        qty=0.001,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="target-plan-key",
    )

    def _persistence_context_builder(**kwargs) -> dict[str, object]:
        context = dict(kwargs["decision_context"])
        context["execution_decision"] = {
            "target_submit_plan": {
                **target_plan.as_dict(),
                "source": "legacy_context",
                "qty": 999.0,
            }
        }
        context["final_action"] = "LEGACY_MUTATION"
        context["submit_expected"] = False
        context["pre_submit_proof_status"] = "failed"
        context["execution_block_reason"] = "legacy_context_mutation"
        return context

    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness({"cash_available": 1_000_000.0}),
        target_state_resolver=lambda *_args, **_kwargs: {
            "previous_target_exposure_krw": 0.0,
            "target_policy_metadata": {},
        },
        summary_builder=lambda **_kwargs: _summary(target_plan=target_plan),
        persistence_context_builder=_persistence_context_builder,
    )
    envelope = DecisionEnvelope(
        strategy_decision=_typed_decision(final_signal="BUY"),
        candle_ts=123,
        market_price=10.0,
        base_context={},
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "test"},
    )

    result = planner.plan_envelope(object(), envelope, updated_ts=456)

    assert result.submit_plan is target_plan
    assert result.submit_plan.source == "target_delta"
    assert result.submit_plan.qty == 0.001
    assert result.persistence_context["execution_plan_bundle"]["primary_submit_plan"]["source"] == "target_delta"  # type: ignore[index]
    assert result.persistence_context["execution_plan_bundle"]["submit_plan_authority"] == "ExecutionSubmitPlan"  # type: ignore[index]
    assert result.persistence_context["execution_decision"]["target_submit_plan"]["source"] == "legacy_context"  # type: ignore[index]


def test_execution_plan_bundle_hash_is_stable_for_equivalent_bundles() -> None:
    plan = ExecutionSubmitPlan(
        side="BUY",
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="REBALANCE_TO_TARGET",
        qty=0.001,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="target-plan-key",
    )
    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: _Readiness({"cash_available": 1_000_000.0}),
        target_state_resolver=lambda *_args, **_kwargs: {
            "previous_target_exposure_krw": 0.0,
            "target_policy_metadata": {"target_origin": "runtime_state"},
        },
        summary_builder=lambda **_kwargs: _summary(target_plan=plan),
    )
    envelope = DecisionEnvelope(
        strategy_decision=_typed_decision(final_signal="BUY"),
        candle_ts=123,
        market_price=10.0,
        base_context={},
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "test"},
    )

    left = planner.plan_envelope(object(), envelope, updated_ts=456)
    right = planner.plan_envelope(object(), envelope, updated_ts=456)

    assert left.content_hash() == right.content_hash()
    assert left.persistence_context["execution_plan_bundle_hash"] == right.persistence_context["execution_plan_bundle_hash"]
    assert left.status is not None
    assert left.status.status == "PLANNED"


def test_plan_envelope_planning_error_returns_fail_closed_bundle_status() -> None:
    planner = ExecutionPlanner(
        readiness_snapshot_builder=lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    envelope = DecisionEnvelope(
        strategy_decision=_typed_decision(final_signal="BUY"),
        candle_ts=123,
        market_price=10.0,
        base_context={},
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "test"},
    )

    result = planner.plan_envelope(object(), envelope, updated_ts=456)

    assert result.summary is None
    assert result.submit_plan is None
    assert result.planning_error == "RuntimeError: boom"
    assert result.status is not None
    assert result.status.status == "ERROR"
    assert result.status.reason_code == "execution_planning_error"


def test_run_loop_execution_request_signal_uses_planned_authority() -> None:
    from bithumb_bot.engine import authoritative_execution_signal_for_trade, build_signal_execution_request

    context = {
        "signal": "HOLD",
        "final_signal": "HOLD",
        "authoritative_execution_signal": "BUY",
    }

    request = build_signal_execution_request(
        signal=authoritative_execution_signal_for_trade(context, fallback_signal="HOLD"),
        ts=123,
        market_price=10.0,
        strategy_name="multi_strategy",
        decision_id=1,
        decision_reason="allocated",
        exit_rule_name=None,
        execution_decision_summary=None,
        decision_context=context,
        execution_plan_bundle=None,
    )

    assert request.signal == "BUY"


def test_run_loop_execution_request_does_not_submit_representative_buy_when_planner_holds() -> None:
    from bithumb_bot.engine import authoritative_execution_signal_for_trade

    context = {
        "signal": "BUY",
        "final_signal": "BUY",
        "authoritative_execution_signal": "HOLD",
        "execution_block_reason": "allocator_hold",
    }

    assert authoritative_execution_signal_for_trade(context, fallback_signal="BUY") == "HOLD"
