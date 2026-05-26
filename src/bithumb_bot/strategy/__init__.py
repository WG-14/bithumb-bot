from .base import LegacyDbStrategy, PositionContext, StrategyDecision, StrategyPolicy
from .registry import (
    create_legacy_strategy,
    create_strategy,
    create_strategy_policy,
    list_legacy_strategies,
    list_strategies,
    list_strategy_policies,
    register_legacy_strategy,
    register_strategy,
    register_strategy_policy,
)
from .sma import build_sma_with_filter_decision_from_normalized_db, decide_sma_with_filter_snapshot_from_db
from .sma_legacy_adapter import SmaCrossStrategy, create_sma_strategy
from .sma_policy_strategy import SmaWithFilterStrategy, create_sma_with_filter_strategy

register_legacy_strategy("sma_cross", create_sma_strategy)
register_strategy_policy("sma_with_filter", create_sma_with_filter_strategy)

__all__ = [
    "LegacyDbStrategy",
    "StrategyPolicy",
    "StrategyDecision",
    "PositionContext",
    "SmaCrossStrategy",
    "SmaWithFilterStrategy",
    "create_sma_strategy",
    "create_sma_with_filter_strategy",
    "build_sma_with_filter_decision_from_normalized_db",
    "decide_sma_with_filter_snapshot_from_db",
    "register_strategy_policy",
    "create_strategy_policy",
    "list_strategy_policies",
    "register_legacy_strategy",
    "create_legacy_strategy",
    "list_legacy_strategies",
    "register_strategy",
    "create_strategy",
    "list_strategies",
]
