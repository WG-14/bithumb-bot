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
    small_loss_tolerance_ratio: float = 0.0
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
        min_profit_floor = max(
            max(0.0, float(self.min_take_profit_ratio)),
            2.0 * max(0.0, float(self.live_fee_rate_estimate)),
        )
        unrealized_pnl_ratio = float(position.unrealized_pnl_ratio)
        resolved_small_loss_tolerance = max(0.0, float(self.small_loss_tolerance_ratio))
        is_small_loss = (-resolved_small_loss_tolerance) <= unrealized_pnl_ratio < 0.0
        is_small_gain = 0.0 <= unrealized_pnl_ratio < min_profit_floor
        filtered_by_pnl_floor = bool(opposite_cross_triggered and (is_small_loss or is_small_gain))
        filter_zone = "small_loss" if is_small_loss else "small_gain" if is_small_gain else "outside"

        should_exit = bool(opposite_cross_triggered and not filtered_by_pnl_floor)
        if should_exit:
            reason = "exit by opposite cross"
        elif filtered_by_pnl_floor:
            reason = f"opposite cross deferred: pnl in {filter_zone} noise band"
        else:
            reason = "opposite cross not triggered"
        return ExitRuleDecision(
            should_exit=should_exit,
            reason=reason,
            context={
                "rule": self.name,
                "base_signal": base_signal,
                "opposite_cross_triggered": opposite_cross_triggered,
                "filter_applied": filtered_by_pnl_floor,
                "deferred_by_min_take_profit_floor": filtered_by_pnl_floor,
                "unrealized_pnl_ratio": unrealized_pnl_ratio,
                "min_profit_floor": min_profit_floor,
                "required_take_profit_ratio": min_profit_floor,
                "configured_min_take_profit_ratio": max(0.0, float(self.min_take_profit_ratio)),
                "roundtrip_fee_ratio": 2.0 * max(0.0, float(self.live_fee_rate_estimate)),
                "small_loss_tolerance_ratio": resolved_small_loss_tolerance,
                "small_loss_tolerance_configured_ratio": max(
                    0.0, float(self.small_loss_tolerance_ratio)
                ),
                "small_loss_zone": is_small_loss,
                "small_gain_zone": is_small_gain,
                "filter_zone": filter_zone,
                "profit_floor_basis": {
                    "configured_min_take_profit_ratio": max(0.0, float(self.min_take_profit_ratio)),
                    "roundtrip_fee_ratio": 2.0 * max(0.0, float(self.live_fee_rate_estimate)),
                    "effective_min_profit_floor_ratio": min_profit_floor,
                },
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
    small_loss_tolerance_ratio: float,
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
                    small_loss_tolerance_ratio=float(small_loss_tolerance_ratio),
                )
            )
        elif name == "max_holding_time":
            rules.append(MaxHoldingTimeExitRule(max_holding_sec=float(max_holding_sec)))
        else:
            raise ValueError(f"unknown exit rule={raw_name!r}")
    return rules
