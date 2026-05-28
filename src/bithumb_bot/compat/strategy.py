from __future__ import annotations

"""Legacy DB-bound strategy compatibility surface.

Promotion-grade research/runtime strategy decisions are owned by plugin
contracts and ``StrategyDecisionService``. Imports from this module are
explicitly non-production compatibility use.
"""

from bithumb_bot.compat.strategy_registry import (
    LegacyDbStrategy,
    create_legacy_db_strategy,
    create_legacy_strategy,
    create_strategy,
    list_legacy_db_strategies,
    list_legacy_strategies,
    list_strategies,
    register_legacy_db_strategy,
    register_legacy_strategy,
    register_strategy,
)
from bithumb_bot.compat.sma_legacy_adapter import (
    LegacySmaWithFilterDbAdapter,
    SmaCrossStrategy,
    create_legacy_sma_with_filter_db_adapter,
    create_sma_strategy,
)

register_legacy_db_strategy("sma_cross", create_sma_strategy)

__all__ = [
    "LegacyDbStrategy",
    "SmaCrossStrategy",
    "LegacySmaWithFilterDbAdapter",
    "create_legacy_sma_with_filter_db_adapter",
    "create_sma_strategy",
    "register_legacy_db_strategy",
    "create_legacy_db_strategy",
    "list_legacy_db_strategies",
    "register_legacy_strategy",
    "create_legacy_strategy",
    "list_legacy_strategies",
    "register_strategy",
    "create_strategy",
    "list_strategies",
]
