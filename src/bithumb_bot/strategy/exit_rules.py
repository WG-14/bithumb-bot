from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .base import PositionContext


@dataclass(frozen=True)
class ExitRuleDecision:
    should_exit: bool
    reason: str
    context: dict[str, object]


class ExitRule(Protocol):
    name: str

    def evaluate(
        self,
        *,
        position: PositionContext,
        candle_ts: int,
        market_price: float,
        signal_context: dict[str, object],
    ) -> ExitRuleDecision: ...


@dataclass(frozen=True)
class OppositeCrossExitRule:
    name: str = "opposite_cross"

    def evaluate(
        self,
        *,
        position: PositionContext,
        candle_ts: int,
        market_price: float,
        signal_context: dict[str, object],
    ) -> ExitRuleDecision:
        base_signal = str(signal_context.get("base_signal", "HOLD"))
        should_exit = position.in_position and base_signal == "SELL"
        return ExitRuleDecision(
            should_exit=bool(should_exit),
            reason="exit by opposite cross" if should_exit else "opposite cross not triggered",
            context={
                "rule": self.name,
                "base_signal": base_signal,
                "candle_ts": int(candle_ts),
                "market_price": float(market_price),
            },
        )


@dataclass(frozen=True)
class MaxHoldingTimeExitRule:
    max_holding_sec: float
    name: str = "max_holding_time"

    def evaluate(
        self,
        *,
        position: PositionContext,
        candle_ts: int,
        market_price: float,
        signal_context: dict[str, object],
    ) -> ExitRuleDecision:
        threshold = max(0.0, float(self.max_holding_sec))
        should_exit = bool(
            position.in_position
            and threshold > 0
            and float(position.holding_time_sec) >= threshold
        )
        return ExitRuleDecision(
            should_exit=should_exit,
            reason="exit by max holding time" if should_exit else "max holding time not triggered",
            context={
                "rule": self.name,
                "holding_time_sec": float(position.holding_time_sec),
                "threshold_sec": threshold,
                "candle_ts": int(candle_ts),
                "market_price": float(market_price),
            },
        )


def create_exit_rules(*, rule_names: list[str], max_holding_sec: float) -> list[ExitRule]:
    rules: list[ExitRule] = []
    for raw_name in rule_names:
        name = str(raw_name).strip().lower()
        if not name:
            continue
        if name == "opposite_cross":
            rules.append(OppositeCrossExitRule())
        elif name == "max_holding_time":
            rules.append(MaxHoldingTimeExitRule(max_holding_sec=float(max_holding_sec)))
        else:
            raise ValueError(f"unknown exit rule={raw_name!r}")
    return rules

