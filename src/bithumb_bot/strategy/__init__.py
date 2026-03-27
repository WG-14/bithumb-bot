from .base import Strategy, StrategyDecision
from .registry import create_strategy, list_strategies, register_strategy
from .sma import (
    SmaCrossStrategy,
    SmaWithFilterStrategy,
    create_sma_strategy,
    create_sma_with_filter_strategy,
)

register_strategy("sma_cross", create_sma_strategy)
register_strategy("sma_with_filter", create_sma_with_filter_strategy)

__all__ = [
    "Strategy",
    "StrategyDecision",
    "SmaCrossStrategy",
    "SmaWithFilterStrategy",
    "create_sma_strategy",
    "create_sma_with_filter_strategy",
    "register_strategy",
    "create_strategy",
    "list_strategies",
]
