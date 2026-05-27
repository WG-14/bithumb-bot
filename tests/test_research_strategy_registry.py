from __future__ import annotations

import json
import inspect
from dataclasses import replace
from pathlib import Path

import pytest

from bithumb_bot.research.strategy_registry import (
    DataCapabilityRequirement,
    TEST_TOP_OF_BOOK_REQUIRED_STRATEGY,
    ResearchStrategyRegistryError,
    RuntimeParameterAdapter,
    ResearchStrategyDataRequirements,
    research_strategy_data_requirements,
    resolve_research_strategy_plugin,
    resolve_research_strategy,
    runtime_strategy_parameter_env_keys,
)
import bithumb_bot.research.strategy_registry as strategy_registry
import bithumb_bot.research.validation_protocol as validation_protocol
from bithumb_bot.research.strategy_spec import exit_policy_from_parameters, strategy_spec_for_name
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.validation_protocol import ResearchValidationError, _validate_strategy_data_requirements


def test_research_strategy_registry_resolves_sma_with_filter() -> None:
    runner = resolve_research_strategy("sma_with_filter")

    assert callable(runner)
    plugin = resolve_research_strategy_plugin("sma_with_filter")
    assert plugin.name == "sma_with_filter"
    assert plugin.runner is runner
    assert plugin.spec is strategy_spec_for_name("sma_with_filter")
    assert plugin.runtime_replay_builder is not None
    assert plugin.contract_payload()["diagnostics_namespace"] == "sma_with_filter"
    assert plugin.contract_payload()["runtime_replay_supported"] is True
    assert plugin.contract_payload()["runner_module"] == "bithumb_bot.research.strategy_registry"
    assert plugin.contract_payload()["runner_qualname"] == "_run_sma_with_filter"
    assert plugin.contract_payload()["runtime_replay_builder_module"] == "bithumb_bot.research.sma_with_filter_plugin"
    assert plugin.contract_payload()["runtime_replay_builder_qualname"] == "build_runtime_replay_strategy"
    assert plugin.contract_payload()["runtime_parameter_adapter_supported"] is True
    assert plugin.contract_payload()["runtime_parameter_env_keys"] == list(
        runtime_strategy_parameter_env_keys("sma_with_filter")
    )
    assert "SMA_SHORT" in runtime_strategy_parameter_env_keys("sma_with_filter")
    assert plugin.contract_payload()["runtime_parameter_from_env_module"] == "bithumb_bot.research.sma_with_filter_plugin"
    assert plugin.contract_payload()["runtime_parameter_from_env_qualname"] == "runtime_parameters_from_env"
    assert (
        plugin.contract_payload()["runtime_parameter_from_settings_module"]
        == "bithumb_bot.research.sma_with_filter_plugin"
    )
    assert (
        plugin.contract_payload()["runtime_parameter_from_settings_qualname"]
        == "runtime_parameters_from_settings"
    )
    assert plugin.exit_rule_factory is not None
    assert plugin.contract_payload()["exit_rule_factory_supported"] is True
    assert plugin.contract_payload()["exit_rule_factory_module"] == "bithumb_bot.research.sma_with_filter_plugin"
    assert plugin.contract_payload()["exit_rule_factory_qualname"] == "exit_rule_factory"
    assert plugin.contract_hash() == resolve_research_strategy_plugin("sma_with_filter").contract_hash()
    assert plugin.contract_hash() == plugin.contract_hash()
    requirements = research_strategy_data_requirements("sma_with_filter")
    assert requirements.required_data == ("candles",)
    assert requirements.optional_data == ("top_of_book",)
    assert requirements.normalized_capabilities()[0].name == "candles"
    assert plugin.contract_payload()["data_capability_contract"]["schema_version"] == 1
    assert {"name": "candles", "required": True} in plugin.contract_payload()["data_capability_contract"][
        "capabilities"
    ]
    assert {"name": "top_of_book", "required": False} in plugin.contract_payload()["data_capability_contract"][
        "capabilities"
    ]


def test_data_capability_requirement_contract_supports_required_optional_and_coverage() -> None:
    requirements = ResearchStrategyDataRequirements(
        required_data=("candles",),
        optional_data=("top_of_book",),
        capabilities=(
            DataCapabilityRequirement(
                name="l2_depth_snapshot",
                required=True,
                min_coverage_pct=95.0,
                evidence_level="depth_walk",
                source="sqlite_orderbook_depth_snapshots",
                notes="required by depth-aware strategy",
            ),
            DataCapabilityRequirement(name="trade_ticks", required=False, evidence_level="tick_replay"),
        ),
    )

    payload = requirements.capability_contract_payload()

    assert payload["required_data"] == ["candles"]
    assert payload["optional_data"] == ["top_of_book"]
    assert {"name": "candles", "required": True} in payload["capabilities"]
    assert {"name": "top_of_book", "required": False} in payload["capabilities"]
    assert {
        "name": "l2_depth_snapshot",
        "required": True,
        "min_coverage_pct": 95.0,
        "evidence_level": "depth_walk",
        "source": "sqlite_orderbook_depth_snapshots",
        "notes": "required by depth-aware strategy",
    } in payload["capabilities"]
    assert {"name": "trade_ticks", "required": False, "evidence_level": "tick_replay"} in payload[
        "capabilities"
    ]


def test_research_strategy_registry_resolves_noop_baseline_as_independent_plugin() -> None:
    runner = resolve_research_strategy("noop_baseline")
    plugin = resolve_research_strategy_plugin("noop_baseline")
    sma_plugin = resolve_research_strategy_plugin("sma_with_filter")

    assert callable(runner)
    assert plugin.name == "noop_baseline"
    assert plugin.runner is runner
    assert plugin.spec is strategy_spec_for_name("noop_baseline")
    assert plugin.spec is not sma_plugin.spec
    assert plugin.contract_hash() != sma_plugin.contract_hash()
    assert plugin.runtime_replay_builder is None
    assert plugin.exit_rule_factory is None
    assert plugin.contract_payload()["exit_rule_factory_supported"] is False
    assert plugin.contract_payload()["exit_rule_factory_module"] is None
    assert plugin.contract_payload()["exit_rule_factory_qualname"] is None
    assert plugin.contract_payload()["runtime_replay_supported"] is False
    assert plugin.contract_payload()["runtime_parameter_adapter_supported"] is False
    assert plugin.contract_payload()["runtime_parameter_from_env_module"] is None
    assert plugin.contract_payload()["runtime_parameter_from_env_qualname"] is None
    assert plugin.contract_payload()["runtime_parameter_from_settings_module"] is None
    assert plugin.contract_payload()["runtime_parameter_from_settings_qualname"] is None
    assert plugin.contract_payload()["diagnostics_namespace"] == "noop_baseline"
    assert plugin.contract_payload()["runner_qualname"] == "_run_noop_baseline"
    requirements = research_strategy_data_requirements("noop_baseline")
    assert requirements.required_data == ("candles",)
    assert requirements.optional_data == ()


def test_buy_and_hold_contract_declares_no_runtime_parameter_adapter() -> None:
    plugin = resolve_research_strategy_plugin("buy_and_hold_baseline")
    payload = plugin.contract_payload()

    assert payload["runtime_parameter_adapter_supported"] is False
    assert payload["runtime_parameter_from_env_module"] is None
    assert payload["runtime_parameter_from_env_qualname"] is None
    assert payload["runtime_parameter_from_settings_module"] is None
    assert payload["runtime_parameter_from_settings_qualname"] is None


def test_non_sma_exit_policy_does_not_carry_opposite_cross_payload() -> None:
    policy = exit_policy_from_parameters(
        "buy_and_hold_baseline",
        {"BUY_HOLD_BUY_INDEX": 1, "BUY_HOLD_DECISION_REASON": "hold"},
    )

    assert policy["rules"] == []
    assert policy["common_rules"] == []
    assert policy["strategy_rules"] == []
    assert "opposite_cross" not in policy


def test_sma_exit_policy_classifies_common_and_strategy_rules() -> None:
    policy = exit_policy_from_parameters(
        "sma_with_filter",
        {
            "SMA_SHORT": 2,
            "SMA_LONG": 4,
            "STRATEGY_EXIT_RULES": "stop_loss,opposite_cross,max_holding_time",
            "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.01,
        },
    )

    assert policy["rules"] == ["stop_loss", "opposite_cross", "max_holding_time"]
    assert policy["common_rules"] == ["stop_loss", "max_holding_time"]
    assert policy["strategy_rules"] == ["opposite_cross"]
    assert policy["opposite_cross"]["enabled"] is True


def test_runtime_parameter_adapter_identity_is_contract_bound_and_deterministic() -> None:
    def alternate_from_env(_env):
        return {"SMA_SHORT": "2", "SMA_LONG": "4"}

    def alternate_from_settings(_cfg):
        return {"SMA_SHORT": 2, "SMA_LONG": 4}

    plugin = resolve_research_strategy_plugin("sma_with_filter")
    changed = replace(
        plugin,
        runtime_parameter_adapter=RuntimeParameterAdapter(
            from_env=alternate_from_env,
            from_settings=alternate_from_settings,
            env_keys=("ALT_KEY",),
        ),
    )

    assert plugin.contract_payload() != changed.contract_payload()
    assert plugin.contract_hash() != changed.contract_hash()
    assert changed.contract_payload()["runtime_parameter_from_env_module"] == __name__
    assert changed.contract_payload()["runtime_parameter_from_env_qualname"].endswith(
        ".<locals>.alternate_from_env"
    )
    assert changed.contract_payload()["runtime_parameter_from_settings_module"] == __name__
    assert changed.contract_payload()["runtime_parameter_from_settings_qualname"].endswith(
        ".<locals>.alternate_from_settings"
    )
    assert changed.contract_payload()["runtime_parameter_env_keys"] == ["ALT_KEY"]
    encoded = json.dumps(plugin.contract_payload(), sort_keys=True)
    assert "<function" not in encoded
    assert " object at 0x" not in encoded


def test_research_strategy_registry_rejects_unknown_strategy() -> None:
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy"):
        resolve_research_strategy("profit_hunter")
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy"):
        resolve_research_strategy_plugin("profit_hunter")


def test_sma_export_normalizer_is_not_imported_from_profile_cli() -> None:
    source = Path("src/bithumb_bot/research/sma_with_filter_plugin.py").read_text(encoding="utf-8")
    registry_source = inspect.getsource(strategy_registry)

    assert "from bithumb_bot.profile_cli" not in registry_source
    assert "from bithumb_bot.profile_cli" not in source
    assert "_sma_promotion_grade_research_export_decisions" not in registry_source
    assert "sma_promotion_grade_research_export_decisions" in source


def test_top_of_book_required_test_hook_is_private_by_name() -> None:
    assert TEST_TOP_OF_BOOK_REQUIRED_STRATEGY.startswith("__test_")
    assert TEST_TOP_OF_BOOK_REQUIRED_STRATEGY.endswith("__")
    requirements = research_strategy_data_requirements(TEST_TOP_OF_BOOK_REQUIRED_STRATEGY)

    assert requirements.required_data == ("candles", "top_of_book")


def test_required_data_preflight_fails_before_backtest_when_manifest_lacks_top_of_book() -> None:
    manifest = parse_manifest(
        {
            "experiment_id": "required_data_preflight",
            "hypothesis": "required data preflight fails before backtest execution",
            "strategy_name": TEST_TOP_OF_BOOK_REQUIRED_STRATEGY,
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "candles_only",
                "train": {"start": "2023-01-01", "end": "2023-01-01"},
                "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            },
            "parameter_space": {"SMA_SHORT": [2], "SMA_LONG": [4]},
            "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
            "acceptance_gate": {
                "min_trade_count": 1,
                "max_mdd_pct": 50,
                "min_profit_factor": 1.0,
                "oos_return_must_be_positive": True,
                "parameter_stability_required": False,
            },
        }
    )

    with pytest.raises(ResearchValidationError, match="research_data_requirement_top_of_book_missing"):
        _validate_strategy_data_requirements(manifest)


def test_required_l2_or_trade_tick_capability_fails_before_backtest(monkeypatch) -> None:
    manifest = parse_manifest(
        {
            "experiment_id": "required_data_capability_preflight",
            "hypothesis": "required capability preflight fails before backtest execution",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "candles_only",
                "train": {"start": "2023-01-01", "end": "2023-01-01"},
                "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            },
            "parameter_space": {"SMA_SHORT": [2], "SMA_LONG": [4]},
            "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
            "acceptance_gate": {
                "min_trade_count": 1,
                "max_mdd_pct": 50,
                "min_profit_factor": 1.0,
                "oos_return_must_be_positive": True,
                "parameter_stability_required": False,
            },
        }
    )

    monkeypatch.setattr(
        validation_protocol,
        "research_strategy_data_requirements",
        lambda _strategy_name: ResearchStrategyDataRequirements(
            required_data=("candles",),
            capabilities=(
                DataCapabilityRequirement(name="l2_depth_snapshot", required=True),
                DataCapabilityRequirement(name="trade_ticks", required=True),
            ),
        ),
    )

    with pytest.raises(
        ResearchValidationError,
        match="research_data_capability_missing:l2_depth_snapshot,trade_ticks",
    ):
        _validate_strategy_data_requirements(manifest)


def test_optional_data_capability_does_not_fail_preflight(monkeypatch) -> None:
    manifest = parse_manifest(
        {
            "experiment_id": "optional_data_capability_preflight",
            "hypothesis": "optional capability preflight does not fail execution",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "candles_only",
                "train": {"start": "2023-01-01", "end": "2023-01-01"},
                "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            },
            "parameter_space": {"SMA_SHORT": [2], "SMA_LONG": [4]},
            "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
            "acceptance_gate": {
                "min_trade_count": 1,
                "max_mdd_pct": 50,
                "min_profit_factor": 1.0,
                "oos_return_must_be_positive": True,
                "parameter_stability_required": False,
            },
        }
    )

    monkeypatch.setattr(
        validation_protocol,
        "research_strategy_data_requirements",
        lambda _strategy_name: ResearchStrategyDataRequirements(
            required_data=("candles",),
            capabilities=(DataCapabilityRequirement(name="trade_ticks", required=False),),
        ),
    )

    _validate_strategy_data_requirements(manifest)


def test_old_top_of_book_required_test_name_is_not_operator_supported() -> None:
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy"):
        resolve_research_strategy("top_of_book_required_test")
