from __future__ import annotations

import pytest

from bithumb_bot.research.strategy_registry import ResearchStrategyRegistryError, resolve_research_strategy


def test_research_strategy_registry_resolves_sma_with_filter() -> None:
    runner = resolve_research_strategy("sma_with_filter")

    assert callable(runner)


def test_research_strategy_registry_rejects_unknown_strategy() -> None:
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy"):
        resolve_research_strategy("profit_hunter")
