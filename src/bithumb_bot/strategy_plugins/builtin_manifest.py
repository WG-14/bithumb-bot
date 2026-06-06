from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class BuiltinStrategyPluginExport:
    module: str
    object_name: str

    @property
    def object_path(self) -> str:
        return f"{self.module}:{self.object_name}"


BUILTIN_STRATEGY_PLUGIN_EXPORTS: tuple[BuiltinStrategyPluginExport, ...] = (
    BuiltinStrategyPluginExport(
        "bithumb_bot.strategy_plugins.sma_with_filter_plugin",
        "SMA_WITH_FILTER_PLUGIN",
    ),
    BuiltinStrategyPluginExport(
        "bithumb_bot.strategy_plugins.baseline_plugins",
        "NOOP_BASELINE_PLUGIN",
    ),
    BuiltinStrategyPluginExport(
        "bithumb_bot.strategy_plugins.baseline_plugins",
        "BUY_AND_HOLD_BASELINE_PLUGIN",
    ),
    BuiltinStrategyPluginExport(
        "bithumb_bot.strategy_plugins.safe_hold_plugin",
        "SAFE_HOLD_PLUGIN",
    ),
    BuiltinStrategyPluginExport(
        "bithumb_bot.strategy_plugins.canary_non_sma",
        "CANARY_NON_SMA_PLUGIN",
    ),
    BuiltinStrategyPluginExport(
        "bithumb_bot.strategy_plugins.replay_threshold",
        "REPLAY_THRESHOLD_PLUGIN",
    ),
    BuiltinStrategyPluginExport(
        "bithumb_bot.strategy_plugins.threshold_research_only",
        "THRESHOLD_RESEARCH_ONLY_PLUGIN",
    ),
)


def iter_builtin_strategy_plugin_exports() -> tuple[BuiltinStrategyPluginExport, ...]:
    return BUILTIN_STRATEGY_PLUGIN_EXPORTS


def iter_builtin_strategy_plugins_from_manifest() -> Iterable[Any]:
    for plugin_export in BUILTIN_STRATEGY_PLUGIN_EXPORTS:
        module = import_module(plugin_export.module)
        yield getattr(module, plugin_export.object_name)
