from __future__ import annotations

from collections.abc import Iterable
from importlib import metadata
from typing import Any

from bithumb_bot.research.strategy_registry import ResearchStrategyPlugin
from bithumb_bot.strategy_authoring import ResearchOnlyStrategyPlugin


STRATEGY_PLUGIN_ENTRY_POINT_GROUP = "bithumb_bot.strategy_plugins"


StrategyPluginRegistration = ResearchStrategyPlugin | ResearchOnlyStrategyPlugin


def iter_builtin_strategy_plugins() -> Iterable[StrategyPluginRegistration]:
    from .baseline_plugins import BUY_AND_HOLD_BASELINE_PLUGIN, NOOP_BASELINE_PLUGIN
    from .canary_non_sma import CANARY_NON_SMA_PLUGIN
    from .safe_hold_plugin import SAFE_HOLD_PLUGIN
    from .sma_with_filter_plugin import SMA_WITH_FILTER_PLUGIN
    from .threshold_research_only import THRESHOLD_RESEARCH_ONLY_PLUGIN

    yield SMA_WITH_FILTER_PLUGIN
    yield NOOP_BASELINE_PLUGIN
    yield BUY_AND_HOLD_BASELINE_PLUGIN
    yield SAFE_HOLD_PLUGIN
    yield CANARY_NON_SMA_PLUGIN
    yield THRESHOLD_RESEARCH_ONLY_PLUGIN


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
        yield from _coerce_loaded_plugins(entry_point.load())


def iter_discovered_strategy_plugins() -> Iterable[ResearchStrategyPlugin]:
    yield from iter_builtin_strategy_plugins()
    yield from iter_entry_point_strategy_plugins()


def _coerce_loaded_plugins(loaded: Any) -> Iterable[ResearchStrategyPlugin]:
    if isinstance(loaded, ResearchStrategyPlugin):
        yield loaded
        return
    if isinstance(loaded, ResearchOnlyStrategyPlugin):
        yield loaded.to_research_strategy_plugin()
        return
    candidate = loaded() if callable(loaded) else loaded
    if isinstance(candidate, ResearchStrategyPlugin):
        yield candidate
        return
    if isinstance(candidate, ResearchOnlyStrategyPlugin):
        yield candidate.to_research_strategy_plugin()
        return
    for item in candidate:
        if not isinstance(item, ResearchStrategyPlugin):
            if isinstance(item, ResearchOnlyStrategyPlugin):
                yield item.to_research_strategy_plugin()
                continue
            raise TypeError(f"strategy_plugin_entry_point_returned_invalid_type:{type(item).__name__}")
        yield item
