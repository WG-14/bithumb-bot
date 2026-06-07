from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from bithumb_bot.config import LiveModeValidationError, settings, validate_runtime_strategy_set_selection
from bithumb_bot.execution_plan_batch import ExecutionPlanBatch, PairExecutionPlan
from bithumb_bot.runtime.execution_coordinator import ExecutionCoordinator
from bithumb_bot.runtime_strategy_set import RuntimeStrategySet, RuntimeStrategySpec, normalized_runtime_strategy_set_manifest


def test_multi_pair_runtime_strategy_set_fails_closed_before_decision() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [
                    {"strategy_name": "safe_hold", "pair": "KRW-BTC", "interval": "1m"},
                    {"strategy_name": "safe_hold", "pair": "KRW-ETH", "interval": "1m"},
                ],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError, match="multi_pair_runtime_unsupported"):
        validate_runtime_strategy_set_selection(cfg)


def test_multi_interval_runtime_strategy_set_fails_closed_before_decision() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [
                    {"strategy_name": "safe_hold", "pair": "KRW-BTC", "interval": "1m"},
                    {"strategy_name": "safe_hold", "pair": "KRW-BTC", "interval": "5m"},
                ],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError, match="single_interval_runtime_unsupported"):
        validate_runtime_strategy_set_selection(cfg)


def test_execution_coordinator_rejects_multi_pair_execution_plan_batch() -> None:
    submitted: list[str] = []
    batch = ExecutionPlanBatch(
        runtime_strategy_set_manifest_hash="sha256:manifest",
        allocation_decision_hash="sha256:allocation",
        budget_lock_hash="sha256:budget",
        batch_risk_decision_evidence={},
        pair_plans=(
            _pair_plan("KRW-BTC"),
            _pair_plan("KRW-ETH"),
        ),
    )

    result = ExecutionCoordinator(execution_engine_name="lot_native").execute_cycle(
        candle_ts=1,
        decision_id=1,
        execution_decision_summary=SimpleNamespace(submit_expected=True),
        execution_plan_bundle=SimpleNamespace(execution_plan_batch=batch),
        submit_invoker=lambda: submitted.append("submit"),
    )

    assert result.planning_status == "execution_plan_batch_single_pair_required"
    assert result.submit_expected is False
    assert submitted == []


def test_runtime_scope_manifest_declares_pair_only_target_state() -> None:
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(RuntimeStrategySpec("safe_hold", pair="KRW-BTC", interval="1m"),),
    )

    manifest = normalized_runtime_strategy_set_manifest(
        strategy_set=strategy_set,
        settings_obj=replace(settings, PAIR="KRW-BTC", INTERVAL="1m"),
    )

    assert manifest["target_position_state_scope"] == "pair_only"
    assert manifest["multi_pair_portfolio_supported"] is False
    assert manifest["multi_interval_runtime_supported"] is False


def _pair_plan(pair: str) -> PairExecutionPlan:
    return PairExecutionPlan(
        pair=pair,
        portfolio_target_hash=f"sha256:target:{pair}",
        execution_submit_plan_hash=f"sha256:submit:{pair}",
        idempotency_key=f"idem:{pair}",
        submit_authority_policy_hash="sha256:policy",
        pre_submit_risk_decision_hash="",
        pre_submit_risk_required=False,
        pre_submit_risk_not_required_reason="submit_not_expected",
    )
