from __future__ import annotations

"""Compatibility facade for SMA strategy imports.

Promotion-grade ``sma_with_filter`` code lives in ``sma_policy_strategy``.
DB-bound SMA compatibility code lives in ``sma_legacy_adapter`` and is not a
promotion-grade execution boundary.
"""

from .sma_legacy_adapter import (
    SmaCrossStrategy,
    _compute_required_entry_edge_ratio,
    _evaluate_entry_edge_filter,
    _fee_authority_context,
    _resolve_strategy_fee_authority,
    build_sma_with_filter_decision_from_normalized_db,
    compute_signal,
    create_sma_strategy,
    create_sma_with_filter_strategy,
    decide_sma_with_filter_snapshot_from_db,
    get_effective_order_rules,
)
from .sma_policy_strategy import SmaWithFilterStrategy

__all__ = [
    "SmaCrossStrategy",
    "SmaWithFilterStrategy",
    "_compute_required_entry_edge_ratio",
    "_evaluate_entry_edge_filter",
    "_fee_authority_context",
    "_resolve_strategy_fee_authority",
    "build_sma_with_filter_decision_from_normalized_db",
    "compute_signal",
    "create_sma_strategy",
    "create_sma_with_filter_strategy",
    "decide_sma_with_filter_snapshot_from_db",
    "get_effective_order_rules",
]
