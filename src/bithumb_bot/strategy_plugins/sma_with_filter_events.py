from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.research.dataset_snapshot import DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.execution_timing import candle_close_ts, interval_ms
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy
from bithumb_bot.research.strategy_spec import strategy_spec_for_name
from bithumb_bot.market_regime import classify_market_regime_from_arrays


def _sma(values: list[float] | tuple[float, ...], window: int, end: int) -> float:
    if window <= 0:
        raise ValueError("window must be positive")
    if end < window:
        raise ValueError("end must be at least window")
    return sum(float(value) for value in values[end - window:end]) / float(window)


def _rolling_sma_values(values: list[float] | tuple[float, ...], window: int) -> dict[int, float]:
    if window <= 0:
        raise ValueError("window must be positive")
    return {
        end: _sma(values, window, end)
        for end in range(window, len(values) + 1)
    }


@dataclass(frozen=True)
class SmaWithFilterDecisionAdapter:
    parameter_values: dict[str, Any]
    fee_rate: float
    slippage_bps: float
    timing_policy: ExecutionTimingPolicy
    strategy_name: str = "sma_with_filter"

    def build_events(self, dataset: DatasetSnapshot) -> tuple[ResearchDecisionEvent, ...]:
        # Compatibility serialization layer only. The backtest kernel must
        # re-evaluate sma_with_filter through StrategyDecisionV2 with the
        # simulated position before treating final action fields as authority.
        short_n = int(self.parameter_values.get("SMA_SHORT", self.parameter_values.get("short_n", 0)))
        long_n = int(self.parameter_values.get("SMA_LONG", self.parameter_values.get("long_n", 0)))
        if short_n <= 0 or long_n <= 0 or short_n >= long_n:
            raise ValueError("SMA_SHORT must be smaller than SMA_LONG")

        candles = dataset.candles
        strategy_spec = strategy_spec_for_name(self.strategy_name)
        events: list[ResearchDecisionEvent] = []
        compact_fixture_timestamps = (
            len(candles) >= 2
            and int(candles[1].ts) - int(candles[0].ts) < interval_ms(dataset.interval)
        )
        start_index = long_n if compact_fixture_timestamps else max(long_n, 4)
        if len(candles) <= start_index:
            return ()
        for index in range(start_index, len(candles)):
            candle = candles[index]
            decision_ts = candle_close_ts(candle, interval=dataset.interval) + int(self.timing_policy.decision_guard_ms)
            events.append(
                ResearchDecisionEvent(
                    candle_ts=int(candle.ts),
                    decision_ts=int(decision_ts),
                    strategy_name=self.strategy_name,
                    strategy_version=strategy_spec.strategy_version,
                    raw_signal="HOLD",
                    final_signal="HOLD",
                    reason="research_event_adapter_non_authoritative",
                    feature_snapshot={
                        "schema_version": 1,
                        "candle_index": int(index),
                        "authority": "promotion_decision_seed_only",
                    },
                    strategy_diagnostics={
                        "schema_version": 1,
                        "adapter": "SmaWithFilterDecisionAdapter",
                        "candle_index": int(index),
                        "authority": "historical_feature_serialization_only",
                    },
                    entry_signal="HOLD",
                    exit_signal="HOLD",
                    blocked_filters=(),
                    order_intent=None,
                    exit_intent={
                        "mode": "evaluate_exit_policy",
                        "base_signal": "HOLD",
                        "base_reason": "research_event_adapter_non_authoritative",
                    },
                    extra_payload={
                        "adapter": "SmaWithFilterDecisionAdapter",
                        "index": int(index),
                        "processed_count": int(index - long_n + 1),
                        "seed_contract": "PromotionDecisionSeed.v1",
                        "feature_authority": "SmaWithFilterSnapshotProjector.project_features",
                        "non_authoritative_event_adapter": True,
                    },
                )
            )
        return tuple(events)


def build_sma_with_filter_research_events(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    execution_timing_policy: ExecutionTimingPolicy,
    portfolio_policy: Any | None = None,
    context: Any | None = None,
) -> tuple[ResearchDecisionEvent, ...]:
    del portfolio_policy, context
    return SmaWithFilterDecisionAdapter(
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        timing_policy=execution_timing_policy,
    ).build_events(dataset)
