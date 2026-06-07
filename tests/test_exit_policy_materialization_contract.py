from __future__ import annotations

from dataclasses import replace

import pytest

from bithumb_bot.research.strategy_registry import (
    ResearchStrategyPlugin,
    StrategyRuntimeCapabilities,
    reload_research_strategy_plugins_for_tests,
    resolve_research_strategy_plugin,
)
from bithumb_bot.research.strategy_spec import (
    StrategySpecError,
    exit_policy_from_parameters,
    exit_policy_materialization_from_parameters,
)
from bithumb_bot.strategy_authoring import (
    PromotionGradeStrategyExtension,
    ReplayCompatibleStrategyExtension,
)
from tests.fixtures.custom_exit_strategy_plugin import (
    CUSTOM_EXIT_PLUGIN,
    CUSTOM_EXIT_SPEC,
    custom_exit_policy_materializer,
    custom_exit_provider,
)


@pytest.fixture(autouse=True)
def _restore_plugins() -> None:
    reload_research_strategy_plugins_for_tests(providers=(custom_exit_provider,))
    yield
    reload_research_strategy_plugins_for_tests()


def test_plugin_contract_exposes_exit_policy_materializer() -> None:
    payload = resolve_research_strategy_plugin("custom_exit_canary").contract_payload()

    assert payload["exit_policy_materializer_supported"] is True
    assert payload["exit_policy_materializer_module"] == "tests.fixtures.custom_exit_strategy_plugin"
    assert payload["exit_policy_materializer_qualname"] == "custom_exit_policy_materializer"
    assert payload["exit_policy_materializer_authority_scope"] == (
        "promotion_profile_runtime_live_authority"
    )


def test_promotion_extension_accepts_exit_policy_materializer() -> None:
    extension = PromotionGradeStrategyExtension(
        runtime_replay_builder=lambda *_args, **_kwargs: None,
        runtime_parameter_adapter=None,
        runtime_decision_adapter_factory=lambda: object(),
        policy_assembly_factory=lambda: object(),
        exit_policy_materializer=custom_exit_policy_materializer,
    )

    payload = extension.contract_payload()
    assert payload["exit_policy_materializer_supported"] is True
    assert payload["exit_policy_materializer_qualname"] == "custom_exit_policy_materializer"


def test_replay_extension_accepts_exit_policy_materializer() -> None:
    extension = ReplayCompatibleStrategyExtension(
        runtime_replay_builder=lambda *_args, **_kwargs: None,
        exit_policy_materializer=custom_exit_policy_materializer,
    )

    payload = extension.contract_payload()
    assert payload["exit_policy_materializer_supported"] is True
    assert payload["exit_policy_materializer_qualname"] == "custom_exit_policy_materializer"


def test_exit_policy_materializer_contract_hash_changes_when_hook_changes() -> None:
    def alternate_materializer(strategy_name, parameter_values):
        return custom_exit_policy_materializer(strategy_name, parameter_values)

    changed = replace(CUSTOM_EXIT_PLUGIN, exit_policy_materializer=alternate_materializer)

    assert CUSTOM_EXIT_PLUGIN.contract_hash() != changed.contract_hash()


def test_strategy_owned_exit_rule_does_not_require_core_rule_whitelist_change() -> None:
    policy = exit_policy_from_parameters(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )

    assert policy["rules"] == ["trailing_stop"]
    assert policy["strategy_rules"] == ["trailing_stop"]
    assert policy["common_rules"] == []


def test_exit_policy_from_parameters_dispatches_to_plugin_materializer() -> None:
    materialized = exit_policy_materialization_from_parameters(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.05},
    )

    assert materialized.exit_policy["trailing_stop"]["trailing_stop_ratio"] == 0.05
    assert materialized.exit_policy_source == "custom_exit_canary_materializer"
    assert materialized.exit_policy_hash.startswith("sha256:")


def test_custom_strategy_rule_is_not_added_to_core_supported_rule_set() -> None:
    reload_research_strategy_plugins_for_tests()
    with pytest.raises(StrategySpecError, match="unsupported rule"):
        exit_policy_from_parameters(
            "sma_with_filter",
            {
                "SMA_SHORT": 2,
                "SMA_LONG": 4,
                "STRATEGY_EXIT_RULES": "trailing_stop",
            },
        )


def test_non_empty_strategy_exit_schema_without_materializer_fails_closed() -> None:
    plugin = ResearchStrategyPlugin(
        name="custom_exit_canary",
        version=CUSTOM_EXIT_SPEC.strategy_version,
        spec=CUSTOM_EXIT_SPEC,
        required_data=CUSTOM_EXIT_SPEC.required_data,
        optional_data=(),
        runner=CUSTOM_EXIT_PLUGIN.runner,
        research_event_builder=CUSTOM_EXIT_PLUGIN.research_event_builder,
        runtime_replay_builder=None,
        runtime_parameter_adapter=None,
        decision_contract_version=CUSTOM_EXIT_SPEC.decision_contract_version,
        diagnostics_namespace="custom_exit_canary",
        runtime_capabilities=StrategyRuntimeCapabilities(
            promotion_runtime_decisions_supported=False,
            runtime_replay_supported=False,
            research_only=True,
            approved_profile_required=False,
            fail_closed_reason="test",
        ),
        authoring_contract_kind="research_only",
    )
    reload_research_strategy_plugins_for_tests(providers=(lambda: (plugin,),))

    with pytest.raises(StrategySpecError, match="materializer required"):
        exit_policy_from_parameters("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.03})
