from __future__ import annotations

from dataclasses import replace

import pytest

from bithumb_bot.runtime_strategy_decision import RuntimeDecisionRequest
from bithumb_bot.runtime_strategy_set import (
    RuntimeDecisionRequestBuilder,
    RuntimeStrategySpec,
    derive_strategy_instance_id,
)
from bithumb_bot.config import settings
from bithumb_bot.research.strategy_registry import StrategyRuntimeCapabilities
from bithumb_bot.research.strategy_registry import reload_research_strategy_plugins_for_tests
from tests.fixtures.custom_exit_strategy_plugin import custom_exit_policy_materializer


@pytest.fixture(autouse=True)
def _restore_plugins() -> None:
    reload_research_strategy_plugins_for_tests(providers=(_runtime_custom_exit_provider,))
    yield
    reload_research_strategy_plugins_for_tests()


def _runtime_custom_exit_provider():
    from tests.fixtures.custom_exit_strategy_plugin import CUSTOM_EXIT_PLUGIN

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


def _spec(*, ratio: float = 0.03) -> RuntimeStrategySpec:
    return RuntimeStrategySpec(
        strategy_name="custom_exit_canary",
        pair="KRW-BTC",
        interval="1m",
        parameters={"TRAILING_STOP_RATIO": ratio},
    )


def test_runtime_strategy_instance_contains_exit_policy_artifact_and_hash() -> None:
    instance = RuntimeDecisionRequestBuilder(settings_obj=_settings()).materialize_instance(_spec())

    payload = instance.as_dict()

    assert payload["exit_policy"]["rules"] == ["trailing_stop"]
    assert payload["exit_policy_hash"] == instance.exit_policy_hash
    assert payload["exit_policy_contract_hash"] == instance.exit_policy_contract_hash
    assert payload["exit_policy_config_hash"] == instance.exit_policy_config_hash


def test_runtime_decision_request_includes_exit_policy_hash() -> None:
    request = RuntimeDecisionRequestBuilder(settings_obj=_settings()).build_for_spec(
        _spec(),
        through_ts_ms=1_700_000_000_000,
    )

    fields = request.observability_fields()

    assert fields["exit_policy_hash"] == request.exit_policy_hash
    assert fields["exit_policy_config_hash"] == request.exit_policy_config_hash


def test_same_parameters_different_exit_policy_get_different_strategy_instance_ids() -> None:
    left = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.03})
    right = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.05})
    spec = _spec(ratio=0.03)

    assert derive_strategy_instance_id(spec, strategy_parameters_hash="sha256:params", exit_policy_hash=str(left["exit_policy_hash"])) != derive_strategy_instance_id(
        spec,
        strategy_parameters_hash="sha256:params",
        exit_policy_hash=str(right["exit_policy_hash"]),
    )


def test_no_exit_strategy_gets_canonical_no_exit_policy_hash() -> None:
    reload_research_strategy_plugins_for_tests()
    spec = RuntimeStrategySpec(
        strategy_name="buy_and_hold_baseline",
        pair="KRW-BTC",
        interval="1m",
        parameters={"BUY_HOLD_BUY_INDEX": 1, "BUY_HOLD_DECISION_REASON": "hold"},
    )
    instance = RuntimeDecisionRequestBuilder(settings_obj=_settings()).materialize_instance(spec)

    assert instance.exit_policy["rules"] == []
    assert instance.exit_policy_hash.startswith("sha256:")
    reload_research_strategy_plugins_for_tests(providers=(_runtime_custom_exit_provider,))


def test_runtime_fails_on_exit_policy_hash_mismatch() -> None:
    materialized = custom_exit_policy_materializer(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )
    spec = RuntimeStrategySpec(
        strategy_name="custom_exit_canary",
        pair="KRW-BTC",
        interval="1m",
        parameters={"TRAILING_STOP_RATIO": 0.03},
        exit_policy=materialized["exit_policy"],
        exit_policy_hash="sha256:wrong",
    )

    with pytest.raises(RuntimeError, match="runtime_exit_policy_hash_mismatch"):
        RuntimeDecisionRequestBuilder(settings_obj=_settings()).materialize_instance(spec)
