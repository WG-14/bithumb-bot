from __future__ import annotations

from collections.abc import Iterable

from bithumb_bot.research.strategy_registry import ResearchStrategyPlugin


def iter_builtin_strategy_plugins() -> Iterable[ResearchStrategyPlugin]:
    from .canary_non_sma import CANARY_NON_SMA_PLUGIN

    yield CANARY_NON_SMA_PLUGIN

