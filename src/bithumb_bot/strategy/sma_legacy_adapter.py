from __future__ import annotations

"""Deprecated compatibility shim for legacy DB-bound SMA adapters.

The implementation is owned by ``bithumb_bot.compat.sma_legacy_adapter``.
Production-facing strategy code must not import this module.
"""

from bithumb_bot.compat.sma_legacy_adapter import (
    LEGACY_DB_BOUND_STRATEGY_STATUS,
    LegacySmaWithFilterDbAdapter,
    SmaCrossStrategy,
    _base_signal,
    _compute_gap_ratio,
    _compute_required_entry_edge_ratio,
    _evaluate_entry_edge_filter,
    _resolve_signal_strength_label,
    _sma,
    compute_signal,
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
    "compute_signal",
    "create_legacy_sma_with_filter_db_adapter",
    "create_sma_strategy",
]
