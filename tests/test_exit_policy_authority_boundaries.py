from __future__ import annotations

from dataclasses import replace
import inspect

import pytest

from bithumb_bot.research.risk_gate_stage import DefaultRiskGate
from bithumb_bot.research.strategy_registry import ResearchStrategyPlugin, StrategyRuntimeCapabilities
from bithumb_bot.strategy_plugins.sma_with_filter_contract import SMA_DECISION_EVIDENCE_CONTRACT
from tests.fixtures.custom_exit_strategy_plugin import CUSTOM_EXIT_PLUGIN, CUSTOM_EXIT_SPEC


def test_exit_rule_factory_contract_scope_remains_research_exploratory_only() -> None:
    plugin = replace(CUSTOM_EXIT_PLUGIN, exit_rule_factory=lambda *_args: [])

    assert plugin.contract_payload()["exit_rule_factory_authority_scope"] == (
        "research_exploratory_compatibility_only"
    )


def test_live_eligible_plugin_with_only_exit_rule_factory_fails_closed() -> None:
    with pytest.raises(ValueError, match="exit policy materializer missing"):
        ResearchStrategyPlugin(
            name="custom_exit_canary",
            version=CUSTOM_EXIT_SPEC.strategy_version,
            spec=CUSTOM_EXIT_SPEC,
            required_data=CUSTOM_EXIT_SPEC.required_data,
            optional_data=(),
            runner=CUSTOM_EXIT_PLUGIN.runner,
            research_event_builder=CUSTOM_EXIT_PLUGIN.research_event_builder,
            runtime_replay_builder=lambda *_args, **_kwargs: None,
            runtime_parameter_adapter=None,
            runtime_decision_adapter_factory=lambda: object(),
            policy_assembly_factory=lambda: object(),
            exit_rule_factory=lambda *_args: [],
            decision_contract_version=CUSTOM_EXIT_SPEC.decision_contract_version,
            diagnostics_namespace="custom_exit_canary",
            runtime_capabilities=StrategyRuntimeCapabilities(
                promotion_runtime_decisions_supported=True,
                runtime_replay_supported=True,
                live_dry_run_allowed=True,
                live_real_order_allowed=False,
                approved_profile_required=True,
                fail_closed_reason="test",
            ),
            decision_evidence_contract=SMA_DECISION_EVIDENCE_CONTRACT,
        )


def test_typed_strategy_decision_does_not_reenter_exit_rule_factory() -> None:
    source = inspect.getsource(DefaultRiskGate.evaluate)

    assert "and strategy_decision is None" in source
    assert "plugin.exit_rule_factory(" in source


def test_research_exploratory_can_still_merge_common_and_plugin_exit_rules() -> None:
    plugin = replace(CUSTOM_EXIT_PLUGIN, exit_rule_factory=lambda *_args: [])

    assert plugin.exit_rule_factory is not None
    assert plugin.contract_payload()["exit_rule_factory_supported"] is True
