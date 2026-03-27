from .base import Strategy, StrategyDecision
from .registry import create_strategy, list_strategies, register_strategy
from .sma import SmaCrossStrategy, create_sma_strategy

register_strategy("sma_cross", create_sma_strategy)

__all__ = [
    "Strategy",
    "StrategyDecision",
    "SmaCrossStrategy",
    "create_sma_strategy",
    "register_strategy",
    "create_strategy",
    "list_strategies",
]
