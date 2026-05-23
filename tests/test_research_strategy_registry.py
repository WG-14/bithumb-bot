from __future__ import annotations

import pytest

from bithumb_bot.research.strategy_registry import (
    TEST_TOP_OF_BOOK_REQUIRED_STRATEGY,
    ResearchStrategyRegistryError,
    research_strategy_data_requirements,
    resolve_research_strategy_plugin,
    resolve_research_strategy,
)
from bithumb_bot.research.strategy_spec import strategy_spec_for_name


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
    assert plugin.contract_hash() == resolve_research_strategy_plugin("sma_with_filter").contract_hash()
    requirements = research_strategy_data_requirements("sma_with_filter")
    assert requirements.required_data == ("candles",)
    assert requirements.optional_data == ("top_of_book",)


def test_research_strategy_registry_rejects_unknown_strategy() -> None:
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy"):
        resolve_research_strategy("profit_hunter")
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy"):
        resolve_research_strategy_plugin("profit_hunter")


def test_top_of_book_required_test_hook_is_private_by_name() -> None:
    assert TEST_TOP_OF_BOOK_REQUIRED_STRATEGY.startswith("__test_")
    assert TEST_TOP_OF_BOOK_REQUIRED_STRATEGY.endswith("__")
    requirements = research_strategy_data_requirements(TEST_TOP_OF_BOOK_REQUIRED_STRATEGY)

    assert requirements.required_data == ("candles", "top_of_book")


def test_old_top_of_book_required_test_name_is_not_operator_supported() -> None:
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy"):
        resolve_research_strategy("top_of_book_required_test")
