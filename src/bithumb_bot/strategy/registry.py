from __future__ import annotations

"""Smoke-only registry for snapshot strategy policies.

Production runtime strategy lifecycle behavior is declared by
``ResearchStrategyPlugin`` in ``bithumb_bot.research.strategy_registry``.
Legacy DB-bound registries are isolated under ``bithumb_bot.compat``.
"""

from typing import Callable

from .base import StrategyPolicy

StrategyPolicyFactory = Callable[..., StrategyPolicy]

_POLICY_REGISTRY: dict[str, StrategyPolicyFactory] = {}


def _normalize_name(name: str) -> str:
    key = str(name or "").strip().lower()
    if not key:
        raise ValueError("strategy name must not be empty")
    return key


def register_smoke_strategy_policy(name: str, factory: StrategyPolicyFactory) -> None:
    _POLICY_REGISTRY[_normalize_name(name)] = factory


def create_smoke_strategy_policy(name: str, **kwargs) -> StrategyPolicy:
    key = _normalize_name(name)
    factory = _POLICY_REGISTRY.get(key)
    if factory is None:
        available = ", ".join(sorted(_POLICY_REGISTRY)) or "<none>"
        raise ValueError(f"strategy_policy_not_registered:{key}; available: {available}")
    policy = factory(**kwargs)
    if not hasattr(policy, "decide_snapshot"):
        raise TypeError(f"strategy_policy_invalid:{key}:missing_decide_snapshot")
    return policy


def list_smoke_strategy_policies() -> tuple[str, ...]:
    return tuple(sorted(_POLICY_REGISTRY))


def register_strategy_policy(name: str, factory: StrategyPolicyFactory) -> None:
    """Deprecated compatibility alias for smoke strategy policies only."""
    register_smoke_strategy_policy(name, factory)


def create_strategy_policy(name: str, **kwargs) -> StrategyPolicy:
    """Deprecated compatibility alias for smoke strategy policies only."""
    return create_smoke_strategy_policy(name, **kwargs)


def list_strategy_policies() -> tuple[str, ...]:
    """Deprecated compatibility alias for smoke strategy policies only."""
    return list_smoke_strategy_policies()
