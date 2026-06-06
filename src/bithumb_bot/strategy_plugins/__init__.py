from __future__ import annotations

from collections.abc import Iterable
from importlib import metadata
from typing import Any

from bithumb_bot.research.strategy_registry import ResearchStrategyPlugin
from bithumb_bot.strategy_authoring import (
    LiveEligibleStrategyPlugin,
    ReplayCompatibleStrategyPlugin,
    ResearchOnlyStrategyPlugin,
)
from bithumb_bot.strategy_plugins.builtin_manifest import iter_builtin_strategy_plugins_from_manifest


STRATEGY_PLUGIN_ENTRY_POINT_GROUP = "bithumb_bot.strategy_plugins"


StrategyPluginRegistration = (
    ResearchStrategyPlugin
    | ResearchOnlyStrategyPlugin
    | ReplayCompatibleStrategyPlugin
    | LiveEligibleStrategyPlugin
)


def iter_builtin_strategy_plugins() -> Iterable[StrategyPluginRegistration]:
    for loaded in iter_builtin_strategy_plugins_from_manifest():
        yield from _coerce_loaded_plugins(loaded)


def iter_entry_point_strategy_plugins() -> Iterable[ResearchStrategyPlugin]:
    entry_points = metadata.entry_points()
    if hasattr(entry_points, "select"):
        selected = entry_points.select(group=STRATEGY_PLUGIN_ENTRY_POINT_GROUP)
    elif isinstance(entry_points, dict):
        selected = entry_points.get(STRATEGY_PLUGIN_ENTRY_POINT_GROUP, ())
    else:
        selected = [
            item
            for item in entry_points
            if str(getattr(item, "group", STRATEGY_PLUGIN_ENTRY_POINT_GROUP))
            == STRATEGY_PLUGIN_ENTRY_POINT_GROUP
        ]
    for entry_point in sorted(
        selected,
        key=lambda item: (
            str(getattr(item, "name", "")),
            str(getattr(item, "value", "")),
        ),
    ):
        yield from coerce_loaded_strategy_plugins(entry_point.load())


def iter_discovered_strategy_plugins() -> Iterable[ResearchStrategyPlugin]:
    yield from iter_builtin_strategy_plugins()
    yield from iter_entry_point_strategy_plugins()


def _coerce_loaded_plugins(loaded: Any) -> Iterable[ResearchStrategyPlugin]:
    yield from coerce_loaded_strategy_plugins(loaded)


def coerce_loaded_strategy_plugins(loaded: Any) -> Iterable[ResearchStrategyPlugin]:
    if isinstance(loaded, ResearchStrategyPlugin):
        yield loaded
        return
    if isinstance(loaded, ResearchOnlyStrategyPlugin):
        yield loaded.to_research_strategy_plugin()
        return
    if isinstance(loaded, (ReplayCompatibleStrategyPlugin, LiveEligibleStrategyPlugin)):
        yield loaded.to_research_strategy_plugin()
        return
    candidate = loaded() if callable(loaded) else loaded
    if isinstance(candidate, ResearchStrategyPlugin):
        yield candidate
        return
    if isinstance(candidate, ResearchOnlyStrategyPlugin):
        yield candidate.to_research_strategy_plugin()
        return
    if isinstance(candidate, (ReplayCompatibleStrategyPlugin, LiveEligibleStrategyPlugin)):
        yield candidate.to_research_strategy_plugin()
        return
    for item in candidate:
        if not isinstance(item, ResearchStrategyPlugin):
            if isinstance(item, ResearchOnlyStrategyPlugin):
                yield item.to_research_strategy_plugin()
                continue
            if isinstance(item, (ReplayCompatibleStrategyPlugin, LiveEligibleStrategyPlugin)):
                yield item.to_research_strategy_plugin()
                continue
            raise TypeError(f"strategy_plugin_entry_point_returned_invalid_type:{type(item).__name__}")
        yield item
