from __future__ import annotations

"""Legacy DB-bound strategy registry for explicit compatibility callers."""

import sqlite3
from typing import Callable, Protocol

from bithumb_bot.strategy.base import StrategyDecision


class LegacyDbStrategy(Protocol):
    """Deprecated DB-bound strategy facade."""

    name: str

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None: ...


LegacyStrategyFactory = Callable[..., LegacyDbStrategy]
StrategyFactory = Callable[..., LegacyDbStrategy]

_LEGACY_REGISTRY: dict[str, LegacyStrategyFactory] = {}


def _normalize_name(name: str) -> str:
    key = str(name or "").strip().lower()
    if not key:
        raise ValueError("strategy name must not be empty")
    return key


def register_legacy_db_strategy(name: str, factory: LegacyStrategyFactory) -> None:
    _LEGACY_REGISTRY[_normalize_name(name)] = factory


def create_legacy_db_strategy(name: str, **kwargs) -> LegacyDbStrategy:
    key = _normalize_name(name)
    factory = _LEGACY_REGISTRY.get(key)
    if factory is None:
        available = ", ".join(sorted(_LEGACY_REGISTRY)) or "<none>"
        raise ValueError(f"legacy_db_strategy_not_registered:{key}; available: {available}")
    return factory(**kwargs)


def list_legacy_db_strategies() -> tuple[str, ...]:
    return tuple(sorted(_LEGACY_REGISTRY))


def register_legacy_strategy(name: str, factory: LegacyStrategyFactory) -> None:
    register_legacy_db_strategy(name, factory)


def create_legacy_strategy(name: str, **kwargs) -> LegacyDbStrategy:
    return create_legacy_db_strategy(name, **kwargs)


def list_legacy_strategies() -> tuple[str, ...]:
    return list_legacy_db_strategies()


def register_strategy(name: str, factory: StrategyFactory) -> None:
    register_legacy_db_strategy(name, factory)


def create_strategy(name: str, **kwargs) -> LegacyDbStrategy:
    return create_legacy_db_strategy(name, **kwargs)


def list_strategies() -> tuple[str, ...]:
    return list_legacy_db_strategies()
