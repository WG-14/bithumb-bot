from __future__ import annotations

"""Production-facing SMA strategy facade.

Legacy DB-bound SMA classes and factories are intentionally exposed only from
``bithumb_bot.compat.strategy``.
"""

from .sma_policy_strategy import SmaWithFilterStrategy, create_sma_with_filter_strategy

__all__ = [
    "SmaWithFilterStrategy",
    "create_sma_with_filter_strategy",
]
