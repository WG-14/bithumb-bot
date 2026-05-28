"""Strategy policy facade.

Promotion-grade runtime strategy lifecycle behavior is exposed through
``ResearchStrategyPlugin`` manifests and plugin-bootstrapped runtime decision
adapters. Legacy DB-bound strategy APIs live under
``bithumb_bot.compat.strategy`` and are not exported here.
"""

from .base import PositionContext, StrategyDecision, StrategyPolicy
from .registry import (
    create_smoke_strategy_policy,
    list_smoke_strategy_policies,
    register_smoke_strategy_policy,
)
from .sma_policy_strategy import SmaWithFilterStrategy, create_sma_with_filter_strategy

register_smoke_strategy_policy("sma_with_filter", create_sma_with_filter_strategy)

__all__ = [
    "StrategyPolicy",
    "StrategyDecision",
    "PositionContext",
    "SmaWithFilterStrategy",
    "create_sma_with_filter_strategy",
    "register_smoke_strategy_policy",
    "create_smoke_strategy_policy",
    "list_smoke_strategy_policies",
]
