from __future__ import annotations

"""Compatibility import path for legacy DB-bound SMA adapters."""

from bithumb_bot.strategy.sma_legacy_adapter import (
    LEGACY_DB_BOUND_STRATEGY_STATUS,
    LegacySmaWithFilterDbAdapter,
    SmaCrossStrategy,
    _base_signal,
    _compute_gap_ratio,
    _compute_required_entry_edge_ratio,
    _evaluate_entry_edge_filter,
    _resolve_signal_strength_label,
    _sma,
    create_legacy_sma_with_filter_db_adapter,
    create_sma_strategy,
)

__all__ = [
    "LEGACY_DB_BOUND_STRATEGY_STATUS",
    "LegacySmaWithFilterDbAdapter",
    "SmaCrossStrategy",
    "_base_signal",
    "_compute_gap_ratio",
    "_compute_required_entry_edge_ratio",
    "_evaluate_entry_edge_filter",
    "_resolve_signal_strength_label",
    "_sma",
    "create_legacy_sma_with_filter_db_adapter",
    "create_sma_strategy",
]
