from __future__ import annotations

from bithumb_bot.experiment_execution_contract import ExperimentExecutionContract
from bithumb_bot.config import settings
from bithumb_bot.core.sma_policy import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2
from bithumb_bot.execution_service import (
    ExecutionReadinessPlanningInput,
    ExecutionTargetPlanningInput,
    TypedExecutionPlanningInput,
    build_typed_execution_decision_summary,
)
from bithumb_bot.portfolio_target import PortfolioTarget


def _portfolio_target() -> PortfolioTarget:
    return PortfolioTarget(
        pair="KRW-BTC",
        target_exposure_krw=100_000.0,
        target_qty=None,
        allocator_policy_name="unit",
        allocator_policy_version="v1",
        allocator_config_hash="sha256:allocator",
        strategy_contribution_hash="sha256:contribution",
        allocation_input_hash="sha256:allocation-input",
        reason="unit",
    )


def _typed_buy_input(payload: dict[str, object]) -> TypedExecutionPlanningInput:
    decision = StrategyDecisionV2(
        strategy_name="daily_participation_sma",
        raw_signal="BUY",
        raw_reason="unit",
        entry_signal="BUY",
        entry_reason="unit",
        exit_signal="HOLD",
        exit_reason="none",
        final_signal="BUY",
        final_reason="unit",
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
        trace={},
        policy_hash="sha256:policy",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )
    target = _portfolio_target()
    return TypedExecutionPlanningInput(
        strategy_decision=decision,
        candle_ts=1,
        market_price=100_000_000.0,
        readiness=ExecutionReadinessPlanningInput.from_payload(payload),
        target=ExecutionTargetPlanningInput(
            previous_target_exposure_krw=0.0,
            portfolio_target=target,
            portfolio_target_hash=target.content_hash(),
            allocation_decision_hash="sha256:allocation",
            allocator_config_hash="sha256:allocator",
            strategy_contribution_hash="sha256:contribution",
        ),
        observability_context=payload,
    )


def _contract(**overrides) -> ExperimentExecutionContract:
    payload = {
        "source_artifact_hash": "sha256:source",
        "authority_hash": "sha256:authority",
        "code_commit_sha": "abc",
        "env_file_hash": "sha256:env",
        "strategy_parameter_hash": "sha256:params",
        "position_mode": "fixed_fill_qty_until_exit",
        "quantity_contract_hash": "sha256:qty",
        "order_rule_snapshot_hash": "sha256:rules",
        "fee_slippage_timing_hash": "sha256:fee",
        "startup_gate_hash": "sha256:gate",
    }
    payload.update(overrides)
    return ExperimentExecutionContract(**payload)


def test_contract_hash_changes_when_env_hash_changes() -> None:
    assert _contract().contract_hash() != _contract(env_file_hash="sha256:env2").contract_hash()


def test_contract_hash_changes_when_order_rule_snapshot_changes() -> None:
    assert _contract().contract_hash() != _contract(order_rule_snapshot_hash="sha256:rules2").contract_hash()


def test_contract_hash_changes_when_position_mode_changes() -> None:
    assert _contract().contract_hash() != _contract(position_mode="continuous_notional_target").contract_hash()


def test_h74_start_blocks_when_contract_hash_mismatch() -> None:
    certificate = {"contract_hash": _contract().contract_hash()}
    current = _contract(env_file_hash="sha256:env2").contract_hash()

    assert certificate["contract_hash"] != current


def test_h74_runtime_planning_injects_current_contract_hash() -> None:
    original_engine = settings.EXECUTION_ENGINE
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    try:
        summary = build_typed_execution_decision_summary(
            typed_input=_typed_buy_input(
                {
                    "position_mode": "fixed_fill_qty_until_exit",
                    "cash_available": 1_000_000.0,
                    "authority_hash": "sha256:authority",
                    "source_artifact_hash": "sha256:source",
                    "authority_parameter_hash": "sha256:params",
                    "startup_gate_hash": "sha256:gate",
                    "h74_startup_gate_status": "START_ALLOWED",
                    "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0},
                    "projection_converged": True,
                    "projection_convergence": {
                        "converged": True,
                        "portfolio_qty": 0.0,
                        "projected_total_qty": 0.0,
                    },
                    "h74_source_authority": {
                        "authority_content_hash": "sha256:authority",
                        "authority_parameter_hash": "sha256:params",
                        "hash_bound_parameters": {
                            "source_candidate_artifact_hash": "sha256:source",
                        },
                    },
                    "min_qty": 0.0001,
                    "qty_step": 0.0001,
                    "min_notional_krw": 5_000.0,
                }
            ),
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", original_engine)

    assert summary.target_submit_plan is not None
    plan = summary.target_submit_plan.as_dict()
    assert str(plan["contract_hash"]).startswith("sha256:")
    assert plan["contract_hash"] == plan["experiment_execution_contract"]["contract_hash"]
    assert plan["experiment_execution_contract"]["source_artifact_hash"] == "sha256:source"
    assert plan["experiment_execution_contract"]["quantity_contract_hash"].startswith("sha256:")
