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


class Strategy(Protocol):
    name: str

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None: ...
