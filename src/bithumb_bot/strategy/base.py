from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from bithumb_bot.strategy_policy_contract import (
    ExecutionConstraintSnapshot,
    PositionSnapshot,
    StrategyDecisionV2,
)

Signal = Literal["BUY", "SELL", "HOLD"]


@dataclass(frozen=True)
class StrategyDecision:
    """Legacy compatibility decision.

    This object may carry dict context for diagnostics or old DB-bound callers,
    but it is not promotion-grade execution authority. Runtime execution
    authority must come from ``StrategyDecisionV2`` through the typed execution
    planner and ``ExecutionSubmitPlan``.
    """

    signal: Signal
    reason: str
    context: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = dict(self.context)
        payload["signal"] = self.signal
        payload["reason"] = self.reason
        return payload


@dataclass(frozen=True)
class PositionContext:
    in_position: bool
    entry_ts: int | None = None
    entry_price: float | None = None
    qty_open: float = 0.0
    holding_time_sec: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_ratio: float = 0.0
    recent_signal_context: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "in_position": self.in_position,
            "entry_ts": self.entry_ts,
            "entry_price": self.entry_price,
            "qty_open": self.qty_open,
            "holding_time_sec": self.holding_time_sec,
            "unrealized_pnl": self.unrealized_pnl,
            "unrealized_pnl_ratio": self.unrealized_pnl_ratio,
            "recent_signal_context": dict(self.recent_signal_context),
        }


class LegacyDbStrategy(Protocol):
    """Deprecated DB-bound strategy facade.

    This protocol is compatibility-only. Promotion-grade strategy code should
    bind to ``StrategyPolicy`` and immutable snapshots instead of deciding from
    a mutable SQLite connection.
    """

    name: str

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None: ...


class StrategyPolicy(Protocol):
    """Promotion-grade snapshot strategy interface for final decisions.

    ``decide_snapshot`` returns the final strategy decision that execution
    planning may consume. Entry-only policy evaluation must use a strategy
    specific entry helper and must not be treated as executable authority.
    """

    name: str

    def decide_snapshot(
        self,
        *,
        market: object,
        position: PositionSnapshot,
        config: object,
        execution_context: ExecutionConstraintSnapshot,
        exit_policy_config: object | None = None,
        rule_sources: dict[str, str] | None = None,
    ) -> StrategyDecisionV2: ...
