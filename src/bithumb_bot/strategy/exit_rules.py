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
    min_take_profit_ratio: float = 0.0
    live_fee_rate_estimate: float = 0.0
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
        opposite_cross_triggered = bool(position.in_position and base_signal == "SELL")
        required_take_profit_ratio = max(
            max(0.0, float(self.min_take_profit_ratio)),
            2.0 * max(0.0, float(self.live_fee_rate_estimate)),
        )
        unrealized_pnl_ratio = float(position.unrealized_pnl_ratio)
        in_micro_pnl_band = (-required_take_profit_ratio) <= unrealized_pnl_ratio < required_take_profit_ratio

        should_exit = bool(opposite_cross_triggered and not in_micro_pnl_band)
        if should_exit:
            reason = "exit by opposite cross"
        elif opposite_cross_triggered and in_micro_pnl_band:
            reason = "opposite cross deferred: minimum take-profit floor not met"
        else:
            reason = "opposite cross not triggered"
        return ExitRuleDecision(
            should_exit=should_exit,
            reason=reason,
            context={
                "rule": self.name,
                "base_signal": base_signal,
                "opposite_cross_triggered": opposite_cross_triggered,
                "deferred_by_min_take_profit_floor": bool(
                    opposite_cross_triggered and in_micro_pnl_band
                ),
                "unrealized_pnl_ratio": unrealized_pnl_ratio,
                "required_take_profit_ratio": required_take_profit_ratio,
                "configured_min_take_profit_ratio": max(0.0, float(self.min_take_profit_ratio)),
                "roundtrip_fee_ratio": 2.0 * max(0.0, float(self.live_fee_rate_estimate)),
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


def create_exit_rules(
    *,
    rule_names: list[str],
    max_holding_sec: float,
    min_take_profit_ratio: float,
    live_fee_rate_estimate: float,
) -> list[ExitRule]:
    rules: list[ExitRule] = []
    for raw_name in rule_names:
        name = str(raw_name).strip().lower()
        if not name:
            continue
        if name == "opposite_cross":
            rules.append(
                OppositeCrossExitRule(
                    min_take_profit_ratio=float(min_take_profit_ratio),
                    live_fee_rate_estimate=float(live_fee_rate_estimate),
                )
            )
        elif name == "max_holding_time":
            rules.append(MaxHoldingTimeExitRule(max_holding_sec=float(max_holding_sec)))
        else:
            raise ValueError(f"unknown exit rule={raw_name!r}")
    return rules
