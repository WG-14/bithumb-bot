from __future__ import annotations

import math
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
class StopLossExitRule:
    stop_loss_ratio: float
    name: str = "stop_loss"

    def __post_init__(self) -> None:
        value = float(self.stop_loss_ratio)
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"stop_loss_ratio must be finite and >= 0, got {value!r}")

    def evaluate(
        self,
        *,
        position: PositionContext,
        candle_ts: int,
        market_price: float,
        signal_context: dict[str, object],
    ) -> ExitRuleDecision:
        threshold = float(self.stop_loss_ratio)
        unrealized_pnl_ratio = float(position.unrealized_pnl_ratio)
        should_exit = bool(
            position.in_position
            and threshold > 0.0
            and unrealized_pnl_ratio <= -threshold
        )
        return ExitRuleDecision(
            should_exit=should_exit,
            reason="exit by stop loss" if should_exit else "stop loss not triggered",
            context={
                "rule": self.name,
                "threshold_ratio": threshold,
                "unrealized_pnl_ratio": unrealized_pnl_ratio,
                "base_signal": str(signal_context.get("base_signal", "HOLD")),
                "raw_signal": str(signal_context.get("raw_signal", signal_context.get("base_signal", "HOLD"))),
                "entry_signal": str(signal_context.get("entry_signal", "HOLD")),
                "exit_signal": str(signal_context.get("exit_signal", signal_context.get("base_signal", "HOLD"))),
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
    stop_loss_ratio: float = 0.0,
) -> list[ExitRule]:
    rules: list[ExitRule] = []
    priority = {"stop_loss": 0, "max_holding_time": 1}
    normalized_names = [str(raw_name).strip().lower() for raw_name in rule_names if str(raw_name).strip()]
    unknown = [name for name in normalized_names if name not in priority]
    if unknown:
        raise ValueError(f"unknown exit rule={unknown[0]!r}")
    resolved_stop_loss_ratio = float(stop_loss_ratio)
    if not math.isfinite(resolved_stop_loss_ratio) or resolved_stop_loss_ratio < 0.0:
        raise ValueError(f"stop_loss_ratio must be finite and >= 0, got {resolved_stop_loss_ratio!r}")
    if resolved_stop_loss_ratio > 0.0 and "stop_loss" not in normalized_names:
        raise ValueError("stop_loss_ratio is positive but STRATEGY_EXIT_RULES does not include stop_loss")
    for name in sorted(dict.fromkeys(normalized_names), key=lambda item: priority[item]):
        if name == "stop_loss":
            rules.append(StopLossExitRule(stop_loss_ratio=resolved_stop_loss_ratio))
        elif name == "max_holding_time":
            rules.append(MaxHoldingTimeExitRule(max_holding_sec=float(max_holding_sec)))
    return rules


def merge_exit_rules(
    common_exit_rules: list[ExitRule],
    strategy_exit_rules: list[ExitRule],
) -> list[ExitRule]:
    """Preserve common risk exits while allowing plugin-owned strategy exits."""
    merged: list[ExitRule] = []
    seen: set[str] = set()
    common_names = {rule.name for rule in common_exit_rules}
    strategy_names = {rule.name for rule in strategy_exit_rules}
    if strategy_exit_rules and common_names <= strategy_names:
        ordered_sources = (strategy_exit_rules, common_exit_rules)
    else:
        ordered_sources = (common_exit_rules, strategy_exit_rules)
    for source in ordered_sources:
        for rule in source:
            if rule.name in seen:
                continue
            merged.append(rule)
            seen.add(rule.name)
    return merged


def create_sma_exit_rules(
    *,
    rule_names: list[str],
    max_holding_sec: float,
    min_take_profit_ratio: float,
    live_fee_rate_estimate: float,
    small_loss_tolerance_ratio: float,
    stop_loss_ratio: float = 0.0,
) -> list[ExitRule]:
    rules: list[ExitRule] = []
    priority = {"stop_loss": 0, "opposite_cross": 1, "max_holding_time": 2}
    normalized_names = [str(raw_name).strip().lower() for raw_name in rule_names if str(raw_name).strip()]
    unknown = [name for name in normalized_names if name not in priority]
    if unknown:
        raise ValueError(f"unknown exit rule={unknown[0]!r}")
    common_names = [name for name in normalized_names if name in {"stop_loss", "max_holding_time"}]
    common_rules = create_exit_rules(
        rule_names=common_names,
        max_holding_sec=max_holding_sec,
        stop_loss_ratio=stop_loss_ratio,
    )
    common_by_name = {rule.name: rule for rule in common_rules}
    for name in sorted(dict.fromkeys(normalized_names), key=lambda item: priority[item]):
        if name == "opposite_cross":
            rules.append(
                OppositeCrossExitRule(
                    min_take_profit_ratio=float(min_take_profit_ratio),
                    live_fee_rate_estimate=float(live_fee_rate_estimate),
                    small_loss_tolerance_ratio=float(small_loss_tolerance_ratio),
                )
            )
        else:
            rules.append(common_by_name[name])
    return rules
