from __future__ import annotations

from typing import Callable

from .base import Strategy

StrategyFactory = Callable[..., Strategy]

_REGISTRY: dict[str, StrategyFactory] = {}


def register_strategy(name: str, factory: StrategyFactory) -> None:
    key = str(name or "").strip().lower()
    if not key:
        raise ValueError("strategy name must not be empty")
    _REGISTRY[key] = factory


def create_strategy(name: str, **kwargs) -> Strategy:
    key = str(name or "").strip().lower()
    factory = _REGISTRY.get(key)
    if factory is None:
        available = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise ValueError(f"unknown strategy={name!r}; available: {available}")
    return factory(**kwargs)


def list_strategies() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))
