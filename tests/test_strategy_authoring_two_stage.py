from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.approved_profile import ApprovedProfileError, _require_runtime_replay_supported_strategy
from bithumb_bot.research.strategy_registry import (
    list_research_strategy_plugins,
    register_research_strategy_plugin,
    reload_research_strategy_plugins_for_tests,
    resolve_research_strategy,
    resolve_research_strategy_plugin,
    strategy_runtime_capability_issues,
)
from bithumb_bot.runtime_strategy_decision import get_runtime_decision_adapter
from bithumb_bot.strategy_authoring import ResearchOnlyStrategyPlugin
from bithumb_bot.strategy_plugins.threshold_research_only import THRESHOLD_RESEARCH_ONLY_PLUGIN
from tests.test_research_strategy_canary import _dataset


def test_lightweight_research_only_strategy_is_discoverable_without_runtime_boilerplate() -> None:
    assert isinstance(THRESHOLD_RESEARCH_ONLY_PLUGIN, ResearchOnlyStrategyPlugin)

    plugins = {plugin.name: plugin for plugin in list_research_strategy_plugins()}
    plugin = plugins["threshold_research_only"]
    payload = plugin.contract_payload()

    assert plugin.runtime_replay_builder is None
    assert plugin.runtime_parameter_adapter is None
    assert plugin.runtime_decision_adapter_factory is None
    assert plugin.policy_assembly_factory is None
    assert payload["authoring_contract_kind"] == "research_only"
    assert payload["promotion_grade"] is False
    assert payload["promotion_extension_missing_reason"] == "promotion_extension_missing"
    assert payload["recommended_next_action"] == "promote_strategy_contract"


def test_research_only_strategy_can_register_through_public_registry() -> None:
    try:
        register_research_strategy_plugin(THRESHOLD_RESEARCH_ONLY_PLUGIN, replace=True)
        plugin = resolve_research_strategy_plugin("threshold_research_only")

        assert plugin.authoring_contract_kind == "research_only"
        assert plugin.runtime_capabilities.fail_closed_reason == "promotion_extension_missing"
    finally:
        reload_research_strategy_plugins_for_tests()


def test_research_only_strategy_runs_and_emits_non_promotion_reproducibility_evidence() -> None:
    runner = resolve_research_strategy("threshold_research_only")

    result = runner(
        _dataset(),
        {"THRESHOLD_CLOSE_ABOVE": 102.0},
        0.001,
        0.0,
        None,
        None,
        None,
        None,
        None,
    )

    assert result.decisions
    first = result.decisions[0]
    assert first["strategy_name"] == "threshold_research_only"
    assert first["dataset_content_hash"].startswith("sha256:")
    assert first["strategy_spec_hash"].startswith("sha256:")
    assert first["strategy_plugin_contract_hash"].startswith("sha256:")
    assert first["decision_contract_hash"].startswith("sha256:")
    assert first["promotion_grade"] is False
    assert first["promotion_extension_missing_reason"] == "promotion_extension_missing"
    assert first["recommended_next_action"] == "promote_strategy_contract"
    assert result.resource_usage["common_decision_behavior_hash"].startswith("sha256:")
    assert result.resource_usage["strategy_behavior_hash"].startswith("sha256:")


def test_research_only_strategy_fails_closed_for_promotion_replay_and_live_modes() -> None:
    plugin = resolve_research_strategy_plugin("threshold_research_only")

    assert plugin.is_promotion_grade is False
    with pytest.raises(ApprovedProfileError, match="promotion_runtime_unsupported_for_strategy:threshold_research_only"):
        _require_runtime_replay_supported_strategy("threshold_research_only")
    assert get_runtime_decision_adapter("threshold_research_only") is None

    issues = strategy_runtime_capability_issues(
        "threshold_research_only",
        live_dry_run=True,
        live_real_order_armed=True,
        approved_profile_path="",
        require_promotion_runtime=True,
        require_runtime_replay=True,
        require_runtime_decision_adapter=True,
    )

    assert "promotion_runtime_unsupported_for_strategy:threshold_research_only:promotion_extension_missing" in issues
    assert "runtime_replay_unsupported_for_strategy:threshold_research_only:promotion_extension_missing" in issues
    assert "runtime_decision_adapter_unsupported_for_strategy:threshold_research_only:promotion_extension_missing" in issues
    assert "live_dry_run_not_allowed_for_strategy:threshold_research_only:promotion_extension_missing" in issues
    assert "live_real_order_not_allowed_for_strategy:threshold_research_only:promotion_extension_missing" in issues


def test_existing_promotion_grade_and_baseline_plugins_keep_expected_contracts() -> None:
    sma = resolve_research_strategy_plugin("sma_with_filter")
    canary = resolve_research_strategy_plugin("canary_non_sma")
    safe_hold = resolve_research_strategy_plugin("safe_hold")
    noop = resolve_research_strategy_plugin("noop_baseline")
    buy_hold = resolve_research_strategy_plugin("buy_and_hold_baseline")

    assert sma.contract_payload()["authoring_contract_kind"] == "promotion_grade"
    assert sma.is_promotion_grade is True
    assert sma.runtime_capabilities.runtime_replay_supported is True
    assert sma.runtime_decision_adapter_factory is not None
    assert sma.policy_assembly_factory is not None

    assert canary.contract_payload()["authoring_contract_kind"] == "promotion_grade"
    assert canary.is_promotion_grade is True
    assert canary.runtime_capabilities.live_dry_run_allowed is True
    assert canary.runtime_capabilities.live_real_order_allowed is False

    assert safe_hold.contract_payload()["authoring_contract_kind"] == "promotion_grade"
    assert safe_hold.is_promotion_grade is True
    assert safe_hold.runtime_capabilities.runtime_replay_supported is False
    assert safe_hold.runtime_capabilities.live_real_order_allowed is False

    assert noop.is_promotion_grade is False
    assert buy_hold.is_promotion_grade is False
    assert noop.runtime_capabilities.runtime_replay_supported is False
    assert buy_hold.runtime_capabilities.runtime_replay_supported is False


def test_authoring_docs_name_both_paths_and_strategy_spec_ownership() -> None:
    doc = Path("docs/strategy-plugin-authoring.md").read_text()

    assert "Fast Research Path" in doc
    assert "Promotion-Grade Path" in doc
    assert "StrategySpec Ownership" in doc
    assert "threshold_research_only" in doc
    assert "canary_non_sma` is not the minimal template" in doc
    assert "promotion_extension_missing" in doc
