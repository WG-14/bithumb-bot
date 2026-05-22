from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean
from typing import Any, Sequence

from bithumb_bot.market_regime import evaluate_live_regime_policy

from .market_regime import classify_sma_market_regime


@dataclass(frozen=True)
class SmaEntryDecision:
    base_signal: str
    base_reason: str
    entry_signal: str
    entry_reason: str
    prev_s: float
    prev_l: float
    curr_s: float
    curr_l: float
    gap_ratio: float
    volatility_ratio: float
    overextended_ratio: float
    blocked_filters: tuple[str, ...]
    gap_filter_enabled: bool
    volatility_filter_enabled: bool
    overextended_filter_enabled: bool
    gap_triggered: bool
    volatility_triggered: bool
    overextended_triggered: bool
    edge_filter_triggered: bool
    edge_filter_details: dict[str, float | bool]
    market_regime: dict[str, object]
    market_regime_triggered: bool
    candidate_regime_decision: dict[str, object]
    candidate_regime_triggered: bool
    filter_blocked: bool

    @property
    def final_signal(self) -> str:
        return self.entry_signal


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def compute_gap_ratio(*, curr_s: float, curr_l: float) -> float:
    return abs(safe_ratio(curr_s - curr_l, curr_l))


def base_signal(*, prev_s: float, prev_l: float, curr_s: float, curr_l: float) -> tuple[str, str]:
    if prev_s <= prev_l and curr_s > curr_l:
        return "BUY", "sma golden cross"
    if prev_s >= prev_l and curr_s < curr_l:
        return "SELL", "sma dead cross"
    return "HOLD", "sma no crossover"


def compute_required_entry_edge_ratio(
    *,
    slippage_bps: float,
    live_fee_rate_estimate: float,
    edge_buffer_ratio: float,
    strategy_min_expected_edge_ratio: float,
) -> tuple[float, float]:
    slippage_ratio = max(0.0, float(slippage_bps)) / 10_000.0
    roundtrip_fee_ratio = 2.0 * max(0.0, float(live_fee_rate_estimate))
    cost_floor_ratio = roundtrip_fee_ratio + slippage_ratio + max(0.0, float(edge_buffer_ratio))
    return cost_floor_ratio, max(cost_floor_ratio, max(0.0, float(strategy_min_expected_edge_ratio)))


def evaluate_entry_edge_filter(
    *,
    base_signal: str,
    gap_ratio: float,
    slippage_bps: float,
    live_fee_rate_estimate: float,
    edge_buffer_ratio: float,
    strategy_min_expected_edge_ratio: float,
    filter_enabled: bool = True,
) -> tuple[bool, dict[str, float | bool]]:
    cost_floor_ratio, required_edge_ratio = compute_required_entry_edge_ratio(
        slippage_bps=slippage_bps,
        live_fee_rate_estimate=live_fee_rate_estimate,
        edge_buffer_ratio=edge_buffer_ratio,
        strategy_min_expected_edge_ratio=strategy_min_expected_edge_ratio,
    )
    expected_edge_ratio = max(0.0, float(gap_ratio))
    signal_eligible = base_signal in ("BUY", "SELL")
    enabled = bool(filter_enabled) and signal_eligible
    blocked = enabled and expected_edge_ratio < required_edge_ratio
    return blocked, {
        "enabled": enabled,
        "configured_enabled": bool(filter_enabled),
        "signal_eligible": signal_eligible,
        "blocked": blocked,
        "expected_edge_ratio": expected_edge_ratio,
        "required_edge_ratio": required_edge_ratio,
        "cost_floor_ratio": cost_floor_ratio,
        "roundtrip_fee_ratio": 2.0 * max(0.0, float(live_fee_rate_estimate)),
        "slippage_ratio": max(0.0, float(slippage_bps)) / 10_000.0,
        "buffer_ratio": max(0.0, float(edge_buffer_ratio)),
        "min_expected_edge_ratio": max(0.0, float(strategy_min_expected_edge_ratio)),
    }


def evaluate_sma_entry_decision(
    *,
    closes: Sequence[float],
    prev_s: float,
    prev_l: float,
    curr_s: float,
    curr_l: float,
    min_gap_ratio: float,
    volatility_window: int,
    min_volatility_ratio: float,
    overextended_lookback: int,
    overextended_max_return_ratio: float,
    slippage_bps: float,
    live_fee_rate_estimate: float,
    entry_edge_buffer_ratio: float,
    cost_edge_enabled: bool,
    cost_edge_min_ratio: float,
    market_regime_enabled: bool,
    candidate_regime_policy: dict[str, object] | None = None,
    require_candidate_regime_policy: bool = False,
    fee_authority_degraded_blocks_entry: bool = False,
) -> SmaEntryDecision:
    close_values = [float(value) for value in closes]

    vol_window = max(1, int(volatility_window))
    vol_closes = close_values[-vol_window:]
    vol_mean = fmean(vol_closes) if vol_closes else 0.0
    volatility_ratio = safe_ratio(max(vol_closes) - min(vol_closes), vol_mean) if vol_closes else 0.0

    overext_lookback = max(1, int(overextended_lookback))
    base_close = close_values[-1 - overext_lookback] if len(close_values) > overext_lookback else 0.0
    overextended_ratio = abs(safe_ratio(close_values[-1] - base_close, base_close)) if close_values else 0.0
    market_regime_snapshot = classify_sma_market_regime(
        closes=close_values,
        short_sma=curr_s,
        long_sma=curr_l,
        volatility_window=vol_window,
        min_volatility_ratio=float(min_volatility_ratio),
        overextended_lookback=overext_lookback,
        overextended_max_return_ratio=float(overextended_max_return_ratio),
        min_trend_strength_ratio=float(min_gap_ratio),
    )
    return evaluate_sma_entry_decision_from_features(
        prev_s=prev_s,
        prev_l=prev_l,
        curr_s=curr_s,
        curr_l=curr_l,
        gap_ratio=compute_gap_ratio(curr_s=curr_s, curr_l=curr_l),
        volatility_ratio=volatility_ratio,
        overextended_ratio=overextended_ratio,
        market_regime_snapshot=market_regime_snapshot.as_dict(),
        min_gap_ratio=min_gap_ratio,
        min_volatility_ratio=min_volatility_ratio,
        overextended_max_return_ratio=overextended_max_return_ratio,
        slippage_bps=slippage_bps,
        live_fee_rate_estimate=live_fee_rate_estimate,
        entry_edge_buffer_ratio=entry_edge_buffer_ratio,
        cost_edge_enabled=cost_edge_enabled,
        cost_edge_min_ratio=cost_edge_min_ratio,
        market_regime_enabled=market_regime_enabled,
        candidate_regime_policy=candidate_regime_policy,
        require_candidate_regime_policy=require_candidate_regime_policy,
        fee_authority_degraded_blocks_entry=fee_authority_degraded_blocks_entry,
    )


def evaluate_sma_entry_decision_from_features(
    *,
    prev_s: float,
    prev_l: float,
    curr_s: float,
    curr_l: float,
    gap_ratio: float,
    volatility_ratio: float,
    overextended_ratio: float,
    market_regime_snapshot: dict[str, object],
    min_gap_ratio: float,
    min_volatility_ratio: float,
    overextended_max_return_ratio: float,
    slippage_bps: float,
    live_fee_rate_estimate: float,
    entry_edge_buffer_ratio: float,
    cost_edge_enabled: bool,
    cost_edge_min_ratio: float,
    market_regime_enabled: bool,
    candidate_regime_policy: dict[str, object] | None = None,
    require_candidate_regime_policy: bool = False,
    fee_authority_degraded_blocks_entry: bool = False,
) -> SmaEntryDecision:
    raw_signal, raw_reason = base_signal(prev_s=prev_s, prev_l=prev_l, curr_s=curr_s, curr_l=curr_l)
    gap_ratio = float(gap_ratio)
    volatility_ratio = float(volatility_ratio)
    overextended_ratio = float(overextended_ratio)

    gap_filter_enabled = float(min_gap_ratio) > 0
    volatility_filter_enabled = float(min_volatility_ratio) > 0
    overextended_filter_enabled = float(overextended_max_return_ratio) > 0
    gap_triggered = gap_filter_enabled and gap_ratio < float(min_gap_ratio)
    volatility_triggered = volatility_filter_enabled and volatility_ratio < float(min_volatility_ratio)
    overextended_triggered = (
        overextended_filter_enabled and overextended_ratio > float(overextended_max_return_ratio)
    )
    edge_filter_triggered, edge_filter_details = evaluate_entry_edge_filter(
        base_signal=raw_signal,
        gap_ratio=gap_ratio,
        slippage_bps=float(slippage_bps),
        live_fee_rate_estimate=float(live_fee_rate_estimate),
        edge_buffer_ratio=float(entry_edge_buffer_ratio),
        strategy_min_expected_edge_ratio=float(cost_edge_min_ratio),
        filter_enabled=bool(cost_edge_enabled),
    )

    blocked_filters: list[str] = []
    if gap_triggered:
        blocked_filters.append("gap")
    if volatility_triggered:
        blocked_filters.append("volatility")
    if overextended_triggered:
        blocked_filters.append("overextended")
    if edge_filter_triggered:
        blocked_filters.append("cost_edge")
    if raw_signal == "BUY" and fee_authority_degraded_blocks_entry:
        blocked_filters.append("fee_authority_degraded")

    market_regime_payload = dict(market_regime_snapshot)
    market_regime_allows_entry = bool(
        market_regime_payload.get(
            "allows_entry",
            market_regime_payload.get("allows_sma_entry", False),
        )
    )
    market_regime_triggered = bool(
        market_regime_enabled
        and raw_signal == "BUY"
        and not market_regime_allows_entry
    )
    candidate_regime_decision = evaluate_live_regime_policy(
        current_snapshot=market_regime_payload,
        candidate_policy=candidate_regime_policy,
    )
    candidate_regime_triggered = bool(
        raw_signal == "BUY"
        and (require_candidate_regime_policy or candidate_regime_policy is not None)
        and not bool(candidate_regime_decision.get("allowed"))
    )

    should_filter_entry = raw_signal == "BUY"
    entry_signal = raw_signal
    entry_reason = raw_reason
    if should_filter_entry and (blocked_filters or market_regime_triggered or candidate_regime_triggered):
        entry_signal = "HOLD"
        if "fee_authority_degraded" in blocked_filters:
            entry_reason = "fee_authority_degraded_live_entry_blocked"
        elif blocked_filters:
            entry_reason = f"filtered entry: {', '.join(blocked_filters)}"
        elif market_regime_triggered:
            entry_reason = f"market regime blocked: {market_regime_payload.get('block_reason')}"
        else:
            entry_reason = f"candidate regime blocked: {candidate_regime_decision.get('regime_block_reason')}"

    return SmaEntryDecision(
        base_signal=raw_signal,
        base_reason=raw_reason,
        entry_signal=entry_signal,
        entry_reason=entry_reason,
        prev_s=float(prev_s),
        prev_l=float(prev_l),
        curr_s=float(curr_s),
        curr_l=float(curr_l),
        gap_ratio=float(gap_ratio),
        volatility_ratio=float(volatility_ratio),
        overextended_ratio=float(overextended_ratio),
        blocked_filters=tuple(blocked_filters),
        gap_filter_enabled=bool(gap_filter_enabled),
        volatility_filter_enabled=bool(volatility_filter_enabled),
        overextended_filter_enabled=bool(overextended_filter_enabled),
        gap_triggered=bool(gap_triggered),
        volatility_triggered=bool(volatility_triggered),
        overextended_triggered=bool(overextended_triggered),
        edge_filter_triggered=bool(edge_filter_triggered),
        edge_filter_details=edge_filter_details,
        market_regime=market_regime_payload,
        market_regime_triggered=bool(market_regime_triggered),
        candidate_regime_decision=candidate_regime_decision,
        candidate_regime_triggered=bool(candidate_regime_triggered),
        filter_blocked=bool(should_filter_entry and blocked_filters),
    )
