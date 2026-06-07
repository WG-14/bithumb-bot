from __future__ import annotations

import pytest

from bithumb_bot.research.promotion_gate import build_candidate_profile
from bithumb_bot.research.strategy_registry import reload_research_strategy_plugins_for_tests
from bithumb_bot.research.strategy_spec import exit_policy_from_parameters
from bithumb_bot.strategy_decision_input import StrategyDecisionInputBundle
from bithumb_bot.strategy_policy_contract import ExecutionConstraintSnapshot, PositionSnapshot
from tests.fixtures.custom_exit_strategy_plugin import custom_exit_policy_materializer, custom_exit_provider


@pytest.fixture(autouse=True)
def _restore_plugins() -> None:
    reload_research_strategy_plugins_for_tests(providers=(custom_exit_provider,))
    yield
    reload_research_strategy_plugins_for_tests()


def test_custom_non_sma_exit_policy_materializes_without_sma_parameters() -> None:
    policy = exit_policy_from_parameters(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )

    assert policy["rules"] == ["trailing_stop"]
    assert "SMA_SHORT" not in policy


def test_custom_non_sma_exit_policy_survives_candidate_profile_build() -> None:
    materialized = custom_exit_policy_materializer(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )
    profile = build_candidate_profile(
        {
            "strategy_name": "custom_exit_canary",
            "parameter_values": {"TRAILING_STOP_RATIO": 0.03},
            "parameter_values_raw": {"TRAILING_STOP_RATIO": 0.03},
            **materialized,
        }
    )

    assert profile["exit_policy"]["rules"] == ["trailing_stop"]


def test_custom_non_sma_exit_policy_hash_changes_when_exit_threshold_changes() -> None:
    left = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.03})
    right = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.05})

    assert left["exit_policy_hash"] != right["exit_policy_hash"]
    assert left["exit_policy_config_hash"] != right["exit_policy_config_hash"]


def test_custom_non_sma_exit_policy_reaches_decision_input_bundle() -> None:
    materialized = custom_exit_policy_materializer(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )
    bundle = StrategyDecisionInputBundle.build(
        strategy_name="custom_exit_canary",
        market={"schema_version": 1, "close": 100.0},
        position=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        config={"schema_version": 1},
        execution_constraints=ExecutionConstraintSnapshot(
            fee_rate_for_decision=0.0,
            fee_authority_degraded_blocks_entry=False,
            fee_authority={"schema_version": 1},
            order_rules={"schema_version": 1},
        ),
        exit_policy_config=materialized["exit_policy_config"],
        materialized_parameters_hash="sha256:params",
        snapshot_projector_version="custom_exit_projector_v1",
        snapshot_projector_hash="sha256:projector",
        provenance={"exit_policy_hash": materialized["exit_policy_hash"]},
    )

    assert bundle.observability_payload()["exit_policy_hash"] == materialized["exit_policy_hash"]
    assert bundle.observability_payload()["exit_policy_config_hash"] == materialized["exit_policy_config_hash"]


def test_custom_non_sma_exit_policy_does_not_add_rule_to_core_whitelist() -> None:
    reload_research_strategy_plugins_for_tests()
    with pytest.raises(Exception, match="unsupported rule|materializer required"):
        exit_policy_from_parameters(
            "sma_with_filter",
            {
                "SMA_SHORT": 2,
                "SMA_LONG": 4,
                "STRATEGY_EXIT_RULES": "trailing_stop",
            },
        )
