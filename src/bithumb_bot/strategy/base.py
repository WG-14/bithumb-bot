from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

Signal = Literal["BUY", "SELL", "HOLD"]


@dataclass(frozen=True)
class StrategyDecision:
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


class Strategy(Protocol):
    name: str

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None: ...
