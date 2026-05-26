from __future__ import annotations

from typing import Callable

from .base import LegacyDbStrategy, StrategyPolicy

StrategyPolicyFactory = Callable[..., StrategyPolicy]
LegacyStrategyFactory = Callable[..., LegacyDbStrategy]
StrategyFactory = Callable[..., LegacyDbStrategy]

_POLICY_REGISTRY: dict[str, StrategyPolicyFactory] = {}
_LEGACY_REGISTRY: dict[str, LegacyStrategyFactory] = {}


def _normalize_name(name: str) -> str:
    key = str(name or "").strip().lower()
    if not key:
        raise ValueError("strategy name must not be empty")
    return key


def register_strategy_policy(name: str, factory: StrategyPolicyFactory) -> None:
    _POLICY_REGISTRY[_normalize_name(name)] = factory


def create_strategy_policy(name: str, **kwargs) -> StrategyPolicy:
    key = _normalize_name(name)
    factory = _POLICY_REGISTRY.get(key)
    if factory is None:
        available = ", ".join(sorted(_POLICY_REGISTRY)) or "<none>"
        raise ValueError(f"strategy_policy_not_registered:{key}; available: {available}")
    policy = factory(**kwargs)
    if not hasattr(policy, "decide_snapshot"):
        raise TypeError(f"strategy_policy_invalid:{key}:missing_decide_snapshot")
    return policy


def list_strategy_policies() -> tuple[str, ...]:
    return tuple(sorted(_POLICY_REGISTRY))


def register_legacy_strategy(name: str, factory: LegacyStrategyFactory) -> None:
    _LEGACY_REGISTRY[_normalize_name(name)] = factory


def create_legacy_strategy(name: str, **kwargs) -> LegacyDbStrategy:
    key = _normalize_name(name)
    factory = _LEGACY_REGISTRY.get(key)
    if factory is None:
        available = ", ".join(sorted(_LEGACY_REGISTRY)) or "<none>"
        raise ValueError(f"legacy_db_strategy_not_registered:{key}; available: {available}")
    return factory(**kwargs)


def list_legacy_strategies() -> tuple[str, ...]:
    return tuple(sorted(_LEGACY_REGISTRY))


def register_strategy(name: str, factory: StrategyFactory) -> None:
    """Compatibility registration for DB-bound smoke strategies only."""
    register_legacy_strategy(name, factory)


def create_strategy(name: str, **kwargs) -> LegacyDbStrategy:
    """Compatibility creation for DB-bound smoke strategies only."""
    return create_legacy_strategy(name, **kwargs)


def list_strategies() -> tuple[str, ...]:
    return list_legacy_strategies()
