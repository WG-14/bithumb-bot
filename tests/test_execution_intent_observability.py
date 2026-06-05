from __future__ import annotations

from dataclasses import fields, replace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.execution_service import (
    ExecutionReadinessPlanningInput,
    ExecutionTargetPlanningInput,
    TypedExecutionPlanningInput,
    build_typed_execution_decision_summary,
)
from bithumb_bot.portfolio_target import PortfolioTarget
from bithumb_bot.strategy_policy_contract import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2


@pytest.fixture(autouse=True)
def _restore_settings():
    old_values = {field.name: getattr(settings, field.name) for field in fields(type(settings))}
    yield
    for key, value in old_values.items():
        object.__setattr__(settings, key, value)


def _decision(*, budget_fraction: float, max_budget: float) -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name="sma_with_filter",
        raw_signal="BUY",
        raw_reason="raw_buy",
        entry_signal="BUY",
        entry_reason="entry_buy",
        exit_signal="HOLD",
        exit_reason="no_exit",
        final_signal="BUY",
        final_reason="final_buy",
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
            budget_fraction_of_cash=budget_fraction,
            max_budget_krw=max_budget,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"unit": "execution_intent_observability"},
        policy_hash="sha256:policy",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash=f"sha256:decision-{budget_fraction}-{max_budget}",
    )


def _typed_input(decision: StrategyDecisionV2) -> TypedExecutionPlanningInput:
    target = PortfolioTarget(
        pair="KRW-BTC",
        target_exposure_krw=100_000.0,
        target_qty=0.001,
        allocator_policy_name="unit_allocator",
        allocator_policy_version="1",
        allocator_config_hash="sha256:allocator-config",
        strategy_contribution_hash="sha256:strategy-contribution",
        allocation_input_hash="sha256:allocation-input",
        reason="unit_target",
        conflict_resolution={"selected_signal": "BUY"},
        authoritative=True,
        fail_closed_reason="none",
    )
    return TypedExecutionPlanningInput(
        strategy_decision=decision,
        candle_ts=123,
        market_price=100_000_000.0,
        readiness=ExecutionReadinessPlanningInput.from_payload(
            {
                "cash_available": 1_000_000.0,
                "min_qty": 0.0001,
                "qty_step": 0.0001,
                "min_notional_krw": 5_000.0,
                "broker_position_evidence": {
                    "broker_qty_known": True,
                    "broker_qty": 0.0,
                    "balance_source_stale": False,
                },
                "total_effective_exposure_notional_krw": 0.0,
            }
        ),
        target=ExecutionTargetPlanningInput(
            previous_target_exposure_krw=0.0,
            portfolio_target=target,
            portfolio_target_hash=target.content_hash(),
            allocation_decision_hash="sha256:allocation-decision",
            allocator_config_hash="sha256:allocator-config",
            strategy_contribution_hash="sha256:strategy-contribution",
        ),
    )


def test_raw_execution_intent_is_trace_only_in_typed_authority_payload() -> None:
    payload = _typed_input(_decision(budget_fraction=1.0, max_budget=70_000.0)).as_authority_payload()

    assert "execution_intent" not in payload
    assert payload["strategy_trace"]["execution_intent_authority"] == "non_authoritative_strategy_hint"  # type: ignore[index]
    assert payload["strategy_trace"]["execution_intent"]["max_budget_krw"] == pytest.approx(70_000.0)  # type: ignore[index]


def test_decision_envelope_persistence_marks_execution_intent_trace_only() -> None:
    envelope = DecisionEnvelope(
        strategy_decision=_decision(budget_fraction=1.0, max_budget=70_000.0),
        candle_ts=123,
        market_price=100_000_000.0,
        base_context={},
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 123},
        boundary={"phase": "unit"},
    )

    context = envelope.as_persistence_context()

    assert "execution_intent" not in context
    assert context["strategy_trace"]["execution_intent_authority"] == "non_authoritative_strategy_hint"  # type: ignore[index]


def test_execution_intent_cannot_affect_target_delta_submit_sizing() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "MAX_ORDER_KRW", 999_999.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    left = build_typed_execution_decision_summary(
        typed_input=_typed_input(_decision(budget_fraction=0.05, max_budget=5_000.0))
    )
    right = build_typed_execution_decision_summary(
        typed_input=_typed_input(_decision(budget_fraction=1.0, max_budget=900_000.0))
    )
    left_plan = left.typed_target_submit_plan()
    right_plan = right.typed_target_submit_plan()

    assert left_plan is not None
    assert right_plan is not None
    left_payload = left_plan.as_dict()
    right_payload = right_plan.as_dict()
    for key in ("side", "qty", "notional_krw", "target_exposure_krw", "delta_krw"):
        assert left_payload[key] == right_payload[key]
    assert left_payload["source"] == "target_delta"
    assert left_payload["authority"] == "canonical_target_delta_sizing"
    assert left_payload["portfolio_target_hash"] == _typed_input(
        _decision(budget_fraction=0.05, max_budget=5_000.0)
    ).target.portfolio_target_hash
    assert left_payload["allocation_decision_hash"] == "sha256:allocation-decision"
    assert left_payload["strategy_contribution_hash"] == "sha256:strategy-contribution"
    assert isinstance(left_payload["target_sizing"], dict)
    assert left_payload["order_rule_authority"]
    assert left_payload["submit_authority_mode"] == "live_real_order_target_delta_only"
    assert str(left_payload["submit_authority_policy_hash"]).startswith("sha256:")
    assert str(left_payload["risk_decision_hash"]).startswith("sha256:")
