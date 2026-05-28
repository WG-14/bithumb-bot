from __future__ import annotations

import ast
from pathlib import Path

from bithumb_bot.research.strategy_registry import list_research_strategy_plugins


ROOT = Path(__file__).resolve().parents[1]


def _source(relative: str) -> str:
    return (ROOT / relative).read_text()


def test_backtest_kernel_stays_strategy_neutral() -> None:
    source = _source("src/bithumb_bot/research/backtest_kernel.py")

    forbidden = (
        "sma_with_filter",
        "SMA_",
        "SmaPolicyConfig",
        "curr_s",
        "prev_s",
        "opposite_cross",
    )
    assert all(token not in source for token in forbidden)
    assert "ResearchDecisionEvent" in source
    assert "StrategyDecisionV2" in source
    assert "ExecutionSubmitPlan" in source


def test_production_strategy_decisions_go_through_canonical_service() -> None:
    allowed_files = {
        "src/bithumb_bot/strategy_decision_service.py",
        "src/bithumb_bot/runtime_strategy_decision.py",
        "src/bithumb_bot/strategy/sma_policy_strategy.py",
    }
    violations: list[str] = []
    for path in (ROOT / "src/bithumb_bot").rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if rel in allowed_files:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=rel)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "decide_snapshot":
                    violations.append(f"{rel}:{node.lineno}")

    assert violations == []


def test_runtime_production_modules_do_not_import_legacy_db_strategies() -> None:
    forbidden = {
        "LegacyDbStrategy",
        "create_legacy_db_strategy",
        "SmaCrossStrategy",
        "LegacySmaWithFilterDbAdapter",
    }
    allowed = {
        "src/bithumb_bot/compat/strategy.py",
        "src/bithumb_bot/compat/strategy_registry.py",
        "src/bithumb_bot/compat/sma_legacy_adapter.py",
        "src/bithumb_bot/run_loop_compatibility.py",
        "src/bithumb_bot/strategy/sma_legacy_adapter.py",
    }
    violations: list[str] = []
    for path in (ROOT / "src/bithumb_bot").rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if rel in allowed:
            continue
        source = path.read_text(encoding="utf-8-sig")
        for token in forbidden:
            if token in source:
                violations.append(f"{rel}:{token}")

    assert violations == []


def test_backtest_engine_is_compatibility_only_for_sma_event_generation() -> None:
    source = _source("src/bithumb_bot/research/backtest_engine.py")

    forbidden = (
        "SmaWithFilterDecisionAdapter",
        "_rolling_sma_values",
        "_rolling_close_range_ratios",
        "_overextended_return_ratios",
        "class Sma",
        "curr_s",
        "prev_s",
    )
    assert all(token not in source for token in forbidden)
    assert "Compatibility wrapper" in source


def test_backtest_runner_is_strategy_neutral() -> None:
    source = _source("src/bithumb_bot/research/backtest_runner.py")

    forbidden = (
        "sma_with_filter",
        "SMA_",
        "legacy_disabled_filter_defaults",
        "SmaWithFilter",
        "noop_baseline",
        "buy_and_hold_baseline",
    )
    assert all(token not in source for token in forbidden)
    assert "research_event_builder" in source
    assert "research_parameter_materializer" in source


def test_backtest_support_does_not_import_backtest_engine() -> None:
    source = _source("src/bithumb_bot/research/backtest_support.py")

    assert "backtest_engine" not in source


def test_strategy_registry_does_not_import_engine_owned_runners() -> None:
    source = _source("src/bithumb_bot/research/strategy_registry.py")

    forbidden = (
        "from .backtest_engine import",
        "run_sma_backtest",
        "run_noop_baseline_backtest",
        "run_buy_and_hold_baseline_backtest",
        "_rolling_sma_values",
        "_rolling_close_range_ratios",
        "_overextended_return_ratios",
        "build_sma_with_filter_research_events",
        "build_noop_baseline_events",
        "build_buy_and_hold_baseline_events",
        "_SMA_WITH_FILTER_PLUGIN",
        "_NOOP_BASELINE_PLUGIN",
        "_BUY_AND_HOLD_BASELINE_PLUGIN",
    )
    assert all(token not in source for token in forbidden)
    assert "ResearchStrategyPlugin(" not in source


def test_active_research_modules_do_not_import_common_types_from_backtest_engine() -> None:
    active_modules = (
        "src/bithumb_bot/research/validation_protocol.py",
    )
    for module in active_modules:
        source = _source(module)
        assert "from .backtest_engine import" not in source
        assert "backtest_engine import" not in source


def test_research_runnable_plugins_declare_event_builders_and_capabilities() -> None:
    for plugin in list_research_strategy_plugins():
        assert plugin.runtime_capabilities is not None
        payload = plugin.contract_payload()
        assert "research_event_builder_supported" in payload
        if payload["research_runnable"]:
            assert payload["research_event_builder_supported"] is True
            assert payload["research_event_builder_module"]


def test_non_sma_canary_uses_plugin_event_builder_contract() -> None:
    plugins = {plugin.name: plugin for plugin in list_research_strategy_plugins()}
    plugin = plugins["canary_non_sma"]
    payload = plugin.contract_payload()

    assert payload["research_event_builder_supported"] is True
    assert payload["research_event_builder_module"] == "bithumb_bot.strategy_plugins.canary_non_sma"
    assert payload["runner_module"] == "bithumb_bot.strategy_plugins.canary_non_sma"
