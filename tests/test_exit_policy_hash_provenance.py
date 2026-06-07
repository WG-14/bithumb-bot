from __future__ import annotations

from dataclasses import replace

import pytest

from bithumb_bot.approved_profile import compute_approved_profile_hash
from bithumb_bot.research.promotion_gate import build_candidate_profile
from bithumb_bot.research.strategy_registry import reload_research_strategy_plugins_for_tests
from bithumb_bot.research.strategy_registry import StrategyRuntimeCapabilities
from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec
from bithumb_bot.config import settings
from bithumb_bot.strategy_decision_input import StrategyDecisionInputBundle
from bithumb_bot.strategy_policy_contract import ExecutionConstraintSnapshot, PositionSnapshot
from tests.fixtures.custom_exit_strategy_plugin import CUSTOM_EXIT_PLUGIN, custom_exit_policy_materializer


@pytest.fixture(autouse=True)
def _restore_plugins() -> None:
    reload_research_strategy_plugins_for_tests(providers=(_runtime_custom_exit_provider,))
    yield
    reload_research_strategy_plugins_for_tests()


def _runtime_custom_exit_provider():
    return (
        replace(
            CUSTOM_EXIT_PLUGIN,
            runtime_replay_builder=lambda *_args, **_kwargs: None,
            runtime_decision_adapter_factory=lambda: object(),
            policy_assembly_factory=lambda: object(),
            runtime_capabilities=StrategyRuntimeCapabilities(
                promotion_runtime_decisions_supported=True,
                runtime_replay_supported=True,
                live_dry_run_allowed=False,
                live_real_order_allowed=False,
                approved_profile_required=False,
                fail_closed_reason="custom_exit_canary_runtime_test",
            ),
            authoring_contract_kind="promotion_grade",
            promotion_extension_payload={"schema_version": 1, "test_only": True},
        ),
    )


def _settings():
    return replace(
        settings,
        MODE="paper",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=False,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_NAME="custom_exit_canary",
        TARGET_EXPOSURE_KRW=0.0,
        MAX_ORDER_KRW=0.0,
        RUNTIME_STRATEGY_SET_JSON="",
        ACTIVE_STRATEGIES="",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        STRATEGY_PARAMETERS_JSON="",
    )


def test_exit_policy_hash_changes_when_custom_exit_threshold_changes() -> None:
    left = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.03})
    right = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.05})

    assert left["exit_policy_hash"] != right["exit_policy_hash"]


def test_decision_input_bundle_hash_changes_when_exit_policy_config_changes() -> None:
    left = _bundle(0.03)
    right = _bundle(0.05)

    assert left.exit_policy_config_hash != right.exit_policy_config_hash
    assert left.decision_input_bundle_payload_hash != right.decision_input_bundle_payload_hash


def test_profile_hash_changes_when_custom_exit_policy_changes() -> None:
    left = build_candidate_profile(_candidate(0.03))
    right = build_candidate_profile(_candidate(0.05))

    assert compute_approved_profile_hash(left) != compute_approved_profile_hash(right)


def test_runtime_observability_contains_exit_policy_hash() -> None:
    request = RuntimeDecisionRequestBuilder(settings_obj=_settings()).build_for_spec(
        RuntimeStrategySpec(
            strategy_name="custom_exit_canary",
            pair="KRW-BTC",
            interval="1m",
            parameters={"TRAILING_STOP_RATIO": 0.03},
        ),
        through_ts_ms=1_700_000_000_000,
    )

    fields = request.observability_fields()
    assert fields["exit_policy_hash"] == request.exit_policy_hash
    assert fields["exit_policy_config_hash"] == request.exit_policy_config_hash


def _candidate(ratio: float) -> dict[str, object]:
    return {
        "strategy_name": "custom_exit_canary",
        "parameter_values": {"TRAILING_STOP_RATIO": ratio},
        "parameter_values_raw": {"TRAILING_STOP_RATIO": ratio},
    }


def _bundle(ratio: float) -> StrategyDecisionInputBundle:
    materialized = custom_exit_policy_materializer(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": ratio},
    )
    return StrategyDecisionInputBundle.build(
        strategy_name="custom_exit_canary",
        market={"schema_version": 1, "close": 100.0},
        position=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        config={"schema_version": 1},
        execution_constraints=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=materialized["exit_policy_config"],
        materialized_parameters_hash="sha256:params",
        snapshot_projector_version="custom_exit_projector_v1",
        snapshot_projector_hash="sha256:projector",
        provenance={"exit_policy_hash": materialized["exit_policy_hash"]},
    )
