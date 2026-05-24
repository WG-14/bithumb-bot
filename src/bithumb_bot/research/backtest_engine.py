from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any, Callable

from bithumb_bot.market_regime import (
    RegimeCoverageRow,
    RegimePerformanceRow,
    aggregate_regime_coverage,
    aggregate_regime_performance,
    classify_market_regime_from_arrays,
)
from bithumb_bot.market_regime.thresholds import MarketRegimeThresholds
from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.position_authority import research_position_authority_snapshot
from bithumb_bot.sma_decision import evaluate_sma_entry_decision_from_features

from .dataset_snapshot import DatasetSnapshot
from .decision_event import ResearchDecisionEvent
from .execution_model import ExecutionFill, ExecutionModel, ExecutionRequest, FixedBpsExecutionModel, model_params_hash
from .execution_timing import (
    ExecutionReferenceEvent,
    SignalEvent,
    build_signal_event,
    candle_close_ts,
    resolve_execution_reference,
)
from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy, legacy_research_portfolio_policy
from .strategy_spec import (
    exit_policy_from_parameters,
    exit_policy_hash,
    materialize_strategy_parameters,
    strategy_spec_for_name,
)
from .metrics import ResearchMetrics
from .metrics_contract import (
    ClosedTradeRecord,
    EquityPoint,
    ExecutionRecord,
    MetricContractV2,
    PositionInterval,
    build_metrics_v2,
)


ProgressCallback = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class BacktestResourceLimits:
    max_runtime_s_per_candidate_split: float | None = None
    max_decisions_retained: int | None = None
    max_trades: int | None = None
    max_equity_points_retained: int | None = None
    max_rss_mb: float | None = None


@dataclass(frozen=True)
class BacktestHeartbeatPolicy:
    interval_s: float | None = None
    bar_interval: int | None = None


@dataclass
class BacktestRunContext:
    experiment_id: str = ""
    candidate_id: str = ""
    scenario_id: str = ""
    scenario_index: int | None = None
    split_name: str = ""
    report_detail: str = "full"
    resource_limits: BacktestResourceLimits = field(default_factory=BacktestResourceLimits)
    heartbeat: BacktestHeartbeatPolicy = field(default_factory=BacktestHeartbeatPolicy)
    progress_callback: ProgressCallback | None = None
    audit_trace: Any | None = None
    started_at: float = field(default_factory=time.perf_counter)


class BacktestResourceLimitExceeded(RuntimeError):
    def __init__(self, reason: str, evidence: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.evidence = evidence


@dataclass
class _BacktestAccumulator:
    context: BacktestRunContext
    total_candles: int
    diagnostics_namespace: str = "sma_with_filter"
    decision_count: int = 0
    signal_count: int = 0
    retained_decision_count: int = 0
    retained_equity_point_count: int = 0
    trade_count: int = 0
    closed_trade_count: int = 0
    period_start_ts: int | None = None
    period_end_ts: int | None = None
    active_bar_count: int = 0
    last_heartbeat_s: float = field(default_factory=time.perf_counter)
    last_heartbeat_bar: int = 0
    decision_hash_material: list[str] = field(default_factory=list)
    behavior_hash_material: list[dict[str, object]] = field(default_factory=list)
    common_behavior_hash_material: list[dict[str, object]] = field(default_factory=list)
    strategy_behavior_hash_material: list[dict[str, object]] = field(default_factory=list)
    trade_ledger_hash_material: list[dict[str, object]] = field(default_factory=list)
    equity_curve_hash_material: list[dict[str, object]] = field(default_factory=list)
    raw_sell_filter_blocked_while_in_position_count: int = 0
    raw_buy_filter_blocked_count: int = 0
    opposite_cross_triggered_count: int = 0
    opposite_cross_deferred_small_loss_count: int = 0
    opposite_cross_deferred_small_gain_count: int = 0
    stop_loss_exit_count: int = 0
    max_holding_exit_count: int = 0
    exit_filter_suppression_prevented_count: int = 0

    @property
    def report_detail(self) -> str:
        detail = str(self.context.report_detail or "full").strip().lower()
        return detail if detail in {"summary", "standard", "full"} else "full"

    def retain_full_detail(self) -> bool:
        return self.report_detail == "full"

    def retain_decision(self) -> bool:
        if self.report_detail == "full":
            return True
        limit = self.context.resource_limits.max_decisions_retained
        if limit is None:
            return True
        return self.retained_decision_count < int(limit)

    def retain_equity_point(self) -> bool:
        if self.report_detail == "full":
            return True
        limit = self.context.resource_limits.max_equity_points_retained
        if limit is None:
            return True
        return self.retained_equity_point_count < int(limit)

    def update_decision(self, payload: dict[str, object], retained: bool) -> None:
        self.decision_count += 1
        raw_signal = str(payload.get("raw_signal") or "").upper()
        if raw_signal in {"BUY", "SELL"}:
            self.signal_count += 1
        raw_filter_would_block = bool(
            payload.get("raw_filter_would_block", payload.get("entry_filter_blocked"))
        )
        entry_blocked = bool(payload.get("entry_blocked"))
        sellable_qty = float(payload.get("sellable_qty") or 0.0)
        if raw_signal == "BUY" and entry_blocked:
            self.raw_buy_filter_blocked_count += 1
        if raw_signal == "SELL" and raw_filter_would_block and sellable_qty > 1e-12:
            self.raw_sell_filter_blocked_while_in_position_count += 1
        if bool(payload.get("exit_filter_suppression_prevented")):
            self.exit_filter_suppression_prevented_count += 1
        for evaluation in payload.get("exit_evaluations") or []:
            if not isinstance(evaluation, dict):
                continue
            context = evaluation.get("context") if isinstance(evaluation.get("context"), dict) else {}
            rule = str(evaluation.get("rule") or context.get("rule") or "")
            if rule == "opposite_cross":
                if bool(context.get("opposite_cross_triggered")):
                    self.opposite_cross_triggered_count += 1
                if bool(context.get("filter_applied")):
                    zone = str(context.get("filter_zone") or "")
                    if zone == "small_loss":
                        self.opposite_cross_deferred_small_loss_count += 1
                    elif zone == "small_gain":
                        self.opposite_cross_deferred_small_gain_count += 1
            elif rule == "stop_loss" and bool(evaluation.get("triggered")):
                self.stop_loss_exit_count += 1
            elif rule == "max_holding_time" and bool(evaluation.get("triggered")):
                self.max_holding_exit_count += 1
        self.decision_hash_material.append(str(payload.get("replay_fingerprint_hash") or ""))
        self.behavior_hash_material.append(
            {
                "candle_ts": payload.get("candle_ts"),
                "raw_signal": payload.get("raw_signal"),
                "entry_signal": payload.get("entry_signal"),
                "exit_signal": payload.get("exit_signal"),
                "final_signal": payload.get("final_signal"),
                "entry_reason": payload.get("entry_reason"),
                "exit_rule": payload.get("exit_rule"),
                "exit_reason": payload.get("exit_reason"),
                "blocked_filters": payload.get("blocked_filters"),
                "regime_decision": payload.get("regime_decision"),
                "regime_block_reason": payload.get("regime_block_reason"),
            }
        )
        self.common_behavior_hash_material.append(
            {
                "candle_ts": payload.get("candle_ts"),
                "raw_signal": payload.get("raw_signal"),
                "final_signal": payload.get("final_signal"),
                "position_state_hash": payload.get("position_state_hash"),
                "execution_intent": payload.get("execution_intent"),
                "order_intent": payload.get("order_intent"),
                "exit_intent": payload.get("exit_intent"),
            }
        )
        strategy_namespace = str(
            payload.get("strategy_diagnostics_namespace")
            or payload.get("strategy_name")
            or self.diagnostics_namespace
        )
        self.strategy_behavior_hash_material.append(
            {
                "namespace": strategy_namespace,
                "payload": payload.get("strategy_behavior_payload")
                or payload.get("strategy_diagnostics")
                or {
                    "raw_signal": payload.get("raw_signal"),
                    "entry_signal": payload.get("entry_signal"),
                    "exit_signal": payload.get("exit_signal"),
                    "entry_reason": payload.get("entry_reason"),
                    "exit_rule": payload.get("exit_rule"),
                    "blocked_filters": payload.get("blocked_filters"),
                    "feature_hash": payload.get("feature_hash"),
                },
            }
        )
        if retained:
            self.retained_decision_count += 1

    def update_equity(self, *, retained: bool, ts: int, asset_qty: float) -> None:
        if self.period_start_ts is None:
            self.period_start_ts = int(ts)
        self.period_end_ts = int(ts)
        if float(asset_qty) > 1e-12:
            self.active_bar_count += 1
        if retained:
            self.retained_equity_point_count += 1

    def record_equity_point(self, *, ts: int, equity: float, cash: float, asset_qty: float) -> None:
        self.equity_curve_hash_material.append(
            {
                "ts": int(ts),
                "equity": round(float(equity), 12),
                "cash": round(float(cash), 12),
                "asset_qty": round(float(asset_qty), 12),
            }
        )

    def record_trade_ledger(self, trade: dict[str, object]) -> None:
        self.trade_ledger_hash_material.append(_trade_hash_payload(trade))

    def update_trades(self, trades: list[dict[str, object]]) -> None:
        self.trade_count = len(trades)
        self.closed_trade_count = sum(1 for trade in trades if str(trade.get("side") or "").upper() == "SELL")

    def maybe_emit_heartbeat(self, candles_processed: int) -> None:
        callback = self.context.progress_callback
        if callback is None:
            return
        now = time.perf_counter()
        interval = self.context.heartbeat.interval_s
        bar_interval = self.context.heartbeat.bar_interval
        by_time = interval is not None and now - self.last_heartbeat_s >= float(interval)
        by_bar = bar_interval is not None and int(bar_interval) > 0 and candles_processed - self.last_heartbeat_bar >= int(bar_interval)
        if not by_time and not by_bar:
            return
        self.last_heartbeat_s = now
        self.last_heartbeat_bar = candles_processed
        callback(self.heartbeat_payload(candles_processed=candles_processed))

    def heartbeat_payload(self, *, candles_processed: int) -> dict[str, Any]:
        return {
            "stage": "heartbeat",
            "experiment_id": self.context.experiment_id,
            "candidate_id": self.context.candidate_id,
            "scenario": self.context.scenario_id,
            "split": self.context.split_name,
            "candles_processed": int(candles_processed),
            "total_candles": int(self.total_candles),
            "signal_count": int(self.signal_count),
            "trade_count": int(self.trade_count),
            "closed_trade_count": int(self.closed_trade_count),
            "decision_count": int(self.decision_count),
            "retained_decision_count": int(self.retained_decision_count),
            "retained_equity_point_count": int(self.retained_equity_point_count),
            "elapsed_s": round(time.perf_counter() - self.context.started_at, 3),
            "rss_mb": _rss_mb(),
            "report_detail": self.report_detail,
        }

    def check_limits(self, *, candles_processed: int, trades: list[dict[str, object]]) -> None:
        self.update_trades(trades)
        limits = self.context.resource_limits
        reasons: list[str] = []
        elapsed = time.perf_counter() - self.context.started_at
        rss = _rss_mb()
        if limits.max_runtime_s_per_candidate_split is not None and elapsed > float(limits.max_runtime_s_per_candidate_split):
            reasons.append("max_runtime_exceeded")
        if limits.max_trades is not None and self.trade_count > int(limits.max_trades):
            reasons.append("max_trades_exceeded")
        if limits.max_rss_mb is not None and rss is not None and rss > float(limits.max_rss_mb):
            reasons.append("max_rss_exceeded")
        if not reasons:
            return
        evidence = self.heartbeat_payload(candles_processed=candles_processed)
        evidence.update({"status": "TRIPPED", "reasons": sorted(set(reasons))})
        if self.context.audit_trace is not None:
            evidence["audit_trace_index"] = self.context.audit_trace.complete(status="failed")
        raise BacktestResourceLimitExceeded("candidate_resource_limit_exceeded", evidence)

    def resource_usage(self, *, candles_processed: int) -> dict[str, Any]:
        payload = self.heartbeat_payload(candles_processed=candles_processed)
        payload.pop("stage", None)
        payload.pop("elapsed_s", None)
        payload.pop("rss_mb", None)
        payload["decision_hash"] = canonical_payload_hash(self.decision_hash_material)
        payload.update(_behavior_hashes(
            decision_material=self.behavior_hash_material,
            common_decision_material=self.common_behavior_hash_material,
            strategy_decision_material=self.strategy_behavior_hash_material,
            trade_material=self.trade_ledger_hash_material,
            equity_material=self.equity_curve_hash_material,
        ))
        payload["strategy_diagnostics"] = self.strategy_diagnostics(trades=[])
        return payload

    def metrics_summary_inputs(self, *, max_drawdown_pct: float) -> dict[str, Any]:
        elapsed_ms = (
            int(self.period_end_ts) - int(self.period_start_ts)
            if self.period_start_ts is not None and self.period_end_ts is not None
            else None
        )
        return {
            "summary_period_start_ts": self.period_start_ts,
            "summary_period_end_ts": self.period_end_ts,
            "summary_elapsed_ms": elapsed_ms,
            "summary_max_drawdown_pct": float(max_drawdown_pct),
            "summary_active_bar_count": int(self.active_bar_count),
        }

    def strategy_diagnostics(self, *, trades: list[dict[str, object]]) -> dict[str, object]:
        return _strategy_diagnostics_from_trades(
            namespace=self.diagnostics_namespace,
            trades=trades,
            raw_sell_filter_blocked_while_in_position_count=self.raw_sell_filter_blocked_while_in_position_count,
            raw_buy_filter_blocked_count=self.raw_buy_filter_blocked_count,
            opposite_cross_triggered_count=self.opposite_cross_triggered_count,
            opposite_cross_deferred_small_loss_count=self.opposite_cross_deferred_small_loss_count,
            opposite_cross_deferred_small_gain_count=self.opposite_cross_deferred_small_gain_count,
            stop_loss_exit_count=self.stop_loss_exit_count,
            max_holding_exit_count=self.max_holding_exit_count,
            exit_filter_suppression_prevented_count=self.exit_filter_suppression_prevented_count,
        )


@dataclass
class _RegimeCoverageAccumulator:
    total: int = 0
    counts: dict[str, dict[str, int]] = field(default_factory=dict)

    def update(self, snapshot: dict[str, object]) -> None:
        self.total += 1
        for dimension in ("price_regime", "volatility_bucket", "volume_bucket", "composite_regime"):
            bucket = str(snapshot.get(dimension) or "unknown")
            dimension_counts = self.counts.setdefault(dimension, {})
            dimension_counts[bucket] = dimension_counts.get(bucket, 0) + 1

    def coverage(self, *, trades: list[dict[str, object]]) -> tuple[RegimeCoverageRow, ...]:
        trade_counts: dict[tuple[str, str], int] = {}
        for trade in trades:
            if not _trade_is_effective(trade) or str(trade.get("side") or "").upper() != "BUY":
                continue
            snapshot = trade.get("entry_regime_snapshot")
            for dimension in ("price_regime", "volatility_bucket", "volume_bucket", "composite_regime"):
                regime = _regime_snapshot_value(snapshot, dimension)
                key = (dimension, regime)
                trade_counts[key] = trade_counts.get(key, 0) + 1
        rows: list[RegimeCoverageRow] = []
        for dimension in ("price_regime", "volatility_bucket", "volume_bucket", "composite_regime"):
            candle_counts = self.counts.get(dimension, {})
            regimes = sorted(set(candle_counts) | {regime for item_dimension, regime in trade_counts if item_dimension == dimension})
            for regime in regimes:
                candles = int(candle_counts.get(regime, 0))
                rows.append(
                    RegimeCoverageRow(
                        dimension=dimension,
                        regime=regime,
                        candle_count=candles,
                        candle_share=(candles / self.total) if self.total else 0.0,
                        trade_count=int(trade_counts.get((dimension, regime), 0)),
                    )
                )
        return tuple(rows)


def _regime_snapshot_value(snapshot: Any, key: str) -> str:
    if isinstance(snapshot, dict):
        return str(snapshot.get(key) or "unknown")
    return str(getattr(snapshot, key, "unknown") or "unknown")


def _trade_is_effective(trade: dict[str, object]) -> bool:
    if "is_portfolio_applied_trade" in trade:
        return bool(trade.get("is_portfolio_applied_trade"))
    if "is_effective_trade" in trade:
        return bool(trade.get("is_effective_trade"))
    execution = trade.get("execution")
    if isinstance(execution, dict):
        status = str(execution.get("fill_status") or "")
        return float(execution.get("filled_qty") or 0.0) > 0.0 and status in {"filled", "partial"}
    return float(trade.get("qty") or 0.0) > 0.0


@dataclass(frozen=True)
class BacktestRun:
    metrics: ResearchMetrics
    trades: tuple[dict[str, object], ...]
    candle_count: int
    warnings: tuple[str, ...]
    regime_performance: tuple[RegimePerformanceRow, ...] = ()
    regime_coverage: tuple[RegimeCoverageRow, ...] = ()
    execution_event_summary: dict[str, object] | None = None
    decisions: tuple[dict[str, object], ...] = ()
    equity_curve: tuple[EquityPoint, ...] = ()
    position_intervals: tuple[PositionInterval, ...] = ()
    closed_trades: tuple[ClosedTradeRecord, ...] = ()
    metrics_v2: MetricContractV2 | None = None
    resource_usage: dict[str, object] | None = None
    strategy_diagnostics: dict[str, object] | None = None
    retained_detail_summary: dict[str, object] | None = None
    audit_trace_index: dict[str, object] | None = None


@dataclass
class _PendingFill:
    fill: ExecutionFill
    trade_index: int
    side: str
    effective_ts: int
    qty: float
    fee: float
    slippage: float
    cash_delta: float
    entry_regime_snapshot: dict[str, object] | None = None
    exit_regime_snapshot: dict[str, object] | None = None


@dataclass(frozen=True)
class _ResearchPositionContext:
    in_position: bool
    entry_ts: int | None = None
    entry_price: float | None = None
    qty_open: float = 0.0
    holding_time_sec: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_ratio: float = 0.0


@dataclass(frozen=True)
class SmaWithFilterDecisionAdapter:
    parameter_values: dict[str, Any]
    fee_rate: float
    slippage_bps: float
    timing_policy: ExecutionTimingPolicy
    strategy_name: str = "sma_with_filter"

    def build_events(self, dataset: DatasetSnapshot) -> tuple[ResearchDecisionEvent, ...]:
        short_n = int(self.parameter_values.get("SMA_SHORT", self.parameter_values.get("short_n", 0)))
        long_n = int(self.parameter_values.get("SMA_LONG", self.parameter_values.get("long_n", 0)))
        if short_n <= 0 or long_n <= 0 or short_n >= long_n:
            raise ValueError("SMA_SHORT must be smaller than SMA_LONG")

        candles = dataset.candles
        if len(candles) < long_n + 2:
            return ()

        closes = [candle.close for candle in candles]
        highs = [candle.high for candle in candles]
        lows = [candle.low for candle in candles]
        volumes = [candle.volume for candle in candles]
        short_sma_values = _rolling_sma_values(closes, short_n)
        long_sma_values = _rolling_sma_values(closes, long_n)
        min_gap = float(
            self.parameter_values.get(
                "SMA_FILTER_GAP_MIN_RATIO",
                self.parameter_values.get("strategy_min_expected_edge_ratio", 0.0),
            )
        )
        min_range = float(self.parameter_values.get("SMA_FILTER_VOL_MIN_RANGE_RATIO", 0.0))
        volatility_window = max(1, int(self.parameter_values.get("SMA_FILTER_VOL_WINDOW", 10)))
        volume_window = max(1, int(self.parameter_values.get("SMA_FILTER_VOLUME_WINDOW", 10)))
        liquidity_window = max(1, int(self.parameter_values.get("SMA_FILTER_LIQUIDITY_WINDOW", 10)))
        overextended_lookback = max(1, int(self.parameter_values.get("SMA_FILTER_OVEREXT_LOOKBACK", 3)))
        overextended_max_return_ratio = float(self.parameter_values.get("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", 0.0))
        close_volatility_ratios = _rolling_close_range_ratios(closes, volatility_window)
        overextended_ratios = _overextended_return_ratios(closes, overextended_lookback)
        thresholds = MarketRegimeThresholds(
            min_trend_strength_ratio=max(0.0, min_gap),
            low_volatility_ratio=max(0.0, min_range),
        )
        strategy_spec = strategy_spec_for_name(self.strategy_name)
        events: list[ResearchDecisionEvent] = []
        prev_above: bool | None = None
        for index in range(long_n, len(candles)):
            candle = candles[index]
            prev_short = short_sma_values[index]
            prev_long = long_sma_values[index]
            curr_short = short_sma_values[index + 1]
            curr_long = long_sma_values[index + 1]
            if prev_short is None or prev_long is None or curr_short is None or curr_long is None:
                continue
            above = curr_short > curr_long
            regime_snapshot = classify_market_regime_from_arrays(
                closes=closes,
                highs=highs,
                lows=lows,
                volumes=volumes,
                index=index,
                short_sma=curr_short,
                long_sma=curr_long,
                volatility_window=volatility_window,
                volume_window=volume_window,
                liquidity_window=liquidity_window,
                thresholds=thresholds,
                overextended_lookback=overextended_lookback,
                overextended_max_return_ratio=overextended_max_return_ratio,
            ).as_dict()
            entry_decision = evaluate_sma_entry_decision_from_features(
                prev_s=prev_short,
                prev_l=prev_long,
                curr_s=curr_short,
                curr_l=curr_long,
                gap_ratio=abs((curr_short - curr_long) / curr_long) if curr_long != 0.0 else 0.0,
                volatility_ratio=close_volatility_ratios[index],
                overextended_ratio=overextended_ratios[index],
                market_regime_snapshot=regime_snapshot,
                min_gap_ratio=min_gap,
                min_volatility_ratio=min_range,
                overextended_max_return_ratio=overextended_max_return_ratio,
                slippage_bps=float(self.parameter_values.get("STRATEGY_ENTRY_SLIPPAGE_BPS", self.slippage_bps) or 0.0),
                live_fee_rate_estimate=float(self.parameter_values.get("LIVE_FEE_RATE_ESTIMATE") or self.fee_rate),
                entry_edge_buffer_ratio=float(self.parameter_values.get("ENTRY_EDGE_BUFFER_RATIO") or 0.0),
                cost_edge_enabled=bool(self.parameter_values.get("SMA_COST_EDGE_ENABLED", True)),
                cost_edge_min_ratio=float(self.parameter_values.get("SMA_COST_EDGE_MIN_RATIO") or 0.0),
                market_regime_enabled=bool(self.parameter_values.get("SMA_MARKET_REGIME_ENABLED", True)),
                candidate_regime_policy=None,
            )
            raw_signal = "HOLD"
            raw_reason = "sma no crossover"
            if prev_above is not None:
                if not prev_above and above:
                    raw_signal = "BUY"
                    raw_reason = "sma golden cross"
                elif prev_above and not above:
                    raw_signal = "SELL"
                    raw_reason = "sma dead cross"
            blocked_filters = tuple(entry_decision.blocked_filters) if raw_signal in {"BUY", "SELL"} else ()
            raw_filter_would_block = bool(
                raw_signal in {"BUY", "SELL"}
                and (
                    blocked_filters
                    or entry_decision.market_regime_triggered
                    or entry_decision.candidate_regime_triggered
                )
            )
            entry_filter_blocked = bool(raw_signal == "BUY" and raw_filter_would_block)
            entry_signal = "HOLD" if entry_filter_blocked else raw_signal
            feature_snapshot = _feature_snapshot(
                short_sma=curr_short,
                long_sma=curr_long,
                gap_ratio=entry_decision.gap_ratio,
                range_ratio=entry_decision.volatility_ratio,
                index=index,
            )
            decision_ts = candle_close_ts(candle, interval=dataset.interval) + int(self.timing_policy.decision_guard_ms)
            events.append(
                ResearchDecisionEvent(
                    candle_ts=int(candle.ts),
                    decision_ts=int(decision_ts),
                    strategy_name=self.strategy_name,
                    strategy_version=strategy_spec.strategy_version,
                    raw_signal=raw_signal,
                    final_signal=entry_signal,
                    reason=entry_decision.entry_reason if entry_filter_blocked else raw_reason,
                    feature_snapshot=feature_snapshot,
                    strategy_diagnostics={
                        "schema_version": 1,
                        "adapter": "SmaWithFilterDecisionAdapter",
                        "candle_index": int(index),
                        "raw_signal": raw_signal,
                        "entry_signal": entry_signal,
                        "raw_filter_would_block": raw_filter_would_block,
                        "blocked_filters": blocked_filters,
                        "market_regime_triggered": bool(entry_decision.market_regime_triggered),
                        "candidate_regime_triggered": bool(entry_decision.candidate_regime_triggered),
                    },
                    entry_signal=entry_signal,
                    exit_signal=raw_signal,
                    blocked_filters=blocked_filters,
                    order_intent=(
                        {"side": "BUY", "sizing": "portfolio_policy_fractional_cash"}
                        if entry_signal == "BUY"
                        else None
                    ),
                    exit_intent={
                        "mode": "evaluate_exit_policy",
                        "base_signal": raw_signal,
                        "base_reason": raw_reason,
                    },
                    extra_payload={
                        "adapter": "SmaWithFilterDecisionAdapter",
                        "index": int(index),
                        "processed_count": int(index - long_n + 1),
                        "prev_above": prev_above,
                        "above": above,
                        "prev_s": float(prev_short),
                        "prev_l": float(prev_long),
                        "curr_s": float(curr_short),
                        "curr_l": float(curr_long),
                        "regime_snapshot": regime_snapshot,
                        "entry_decision": entry_decision,
                        "raw_reason": raw_reason,
                        "raw_filter_would_block": raw_filter_would_block,
                        "entry_filter_blocked": entry_filter_blocked,
                    },
                )
            )
            prev_above = above
        return tuple(events)


def run_sma_backtest(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    return run_sma_backtest_via_kernel(
        dataset=dataset,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def run_sma_backtest_via_kernel(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    effective_parameters = _materialize_sma_backtest_parameters(
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    short_n = int(effective_parameters.get("SMA_SHORT", effective_parameters.get("short_n", 0)))
    long_n = int(effective_parameters.get("SMA_LONG", effective_parameters.get("long_n", 0)))
    if short_n <= 0 or long_n <= 0 or short_n >= long_n:
        raise ValueError("SMA_SHORT must be smaller than SMA_LONG")
    if len(dataset.candles) < long_n + 2:
        return _empty_kernel_compatible_backtest_result(
            dataset=dataset,
            parameter_stability_score=parameter_stability_score,
            portfolio_policy=portfolio_policy,
            context=context,
            diagnostics_namespace="sma_with_filter",
        )

    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    adapter = SmaWithFilterDecisionAdapter(
        parameter_values=effective_parameters,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        timing_policy=timing_policy,
    )
    from .backtest_kernel import run_decision_event_backtest as _run_decision_event_backtest

    return _run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values=effective_parameters,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        decision_events=adapter.build_events(dataset),
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _materialize_sma_backtest_parameters(
    *,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
) -> dict[str, Any]:
    effective_parameters = materialize_strategy_parameters(
        "sma_with_filter",
        parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    # Backward-compatible research-only behavior: minimal historical test and
    # diagnostic parameter sets exercised raw SMA crosses unless a filter was
    # explicitly part of the candidate. Production-bound manifests still fail
    # closed unless every runtime-bound behavior parameter is declared.
    legacy_disabled_filter_defaults = {
        "SMA_FILTER_GAP_MIN_RATIO": 0.0,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
        "SMA_COST_EDGE_ENABLED": False,
        "SMA_MARKET_REGIME_ENABLED": False,
    }
    for key, value in legacy_disabled_filter_defaults.items():
        if key not in parameter_values:
            effective_parameters[key] = value
    return effective_parameters


def _empty_kernel_compatible_backtest_result(
    *,
    dataset: DatasetSnapshot,
    parameter_stability_score: float | None,
    portfolio_policy: PortfolioPolicy | None,
    context: BacktestRunContext | None,
    diagnostics_namespace: str,
) -> BacktestRun:
    run_context = context or BacktestRunContext(report_detail="full")
    policy = portfolio_policy or legacy_research_portfolio_policy()
    starting_cash = float(policy.starting_cash_krw)
    initial_qty = float(policy.initial_position_qty)
    accumulator = _BacktestAccumulator(
        context=run_context,
        total_candles=len(dataset.candles),
        diagnostics_namespace=diagnostics_namespace,
    )
    audit_trace_index = _complete_audit_trace(run_context, status="completed")
    return BacktestRun(
        metrics=_empty_metrics(parameter_stability_score),
        metrics_v2=_empty_metrics_v2(starting_cash=starting_cash, initial_position_qty=initial_qty),
        trades=(),
        candle_count=len(dataset.candles),
        warnings=("not_enough_candles",),
        regime_performance=(),
        regime_coverage=(),
        execution_event_summary=empty_execution_event_summary(),
        resource_usage=accumulator.resource_usage(candles_processed=len(dataset.candles)),
        strategy_diagnostics=accumulator.strategy_diagnostics(trades=[]),
        retained_detail_summary=_retained_detail_summary(accumulator, retained_regime_snapshot_count=0),
        audit_trace_index=audit_trace_index,
    )


def _run_sma_backtest_legacy(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    """Transitional golden-reference path for SMA/kernel compatibility tests.

    Production and validation callers must use ``run_sma_backtest()``, which
    delegates to the common decision-event kernel.
    """
    from .strategy_registry import resolve_research_strategy_plugin

    strategy_plugin = resolve_research_strategy_plugin("sma_with_filter")
    strategy_spec = strategy_spec_for_name("sma_with_filter")
    effective_parameters = _materialize_sma_backtest_parameters(
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    active_exit_policy = exit_policy_from_parameters("sma_with_filter", effective_parameters)
    active_exit_policy_hash = exit_policy_hash(active_exit_policy)
    short_n = int(effective_parameters.get("SMA_SHORT", effective_parameters.get("short_n", 0)))
    long_n = int(effective_parameters.get("SMA_LONG", effective_parameters.get("long_n", 0)))
    if short_n <= 0 or long_n <= 0 or short_n >= long_n:
        raise ValueError("SMA_SHORT must be smaller than SMA_LONG")

    candles = dataset.candles
    run_context = context or BacktestRunContext(report_detail="full")
    policy = portfolio_policy or legacy_research_portfolio_policy()
    starting_cash = float(policy.starting_cash_krw)
    initial_qty = float(policy.initial_position_qty)
    buy_fraction = float(policy.position_sizing.buy_fraction)
    accumulator = _BacktestAccumulator(context=run_context, total_candles=len(candles))
    dataset_content_hash = dataset.content_hash()
    warnings: list[str] = []
    if len(candles) < long_n + 2:
        return _empty_kernel_compatible_backtest_result(
            dataset=dataset,
            parameter_stability_score=parameter_stability_score,
            portfolio_policy=portfolio_policy,
            context=context,
            diagnostics_namespace=strategy_plugin.diagnostics_namespace,
        )

    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    adapter = SmaWithFilterDecisionAdapter(
        parameter_values=effective_parameters,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        timing_policy=timing_policy,
    )
    decision_events = adapter.build_events(dataset)
    regime_snapshots: list[dict[str, object]] = []
    regime_coverage_accumulator = _RegimeCoverageAccumulator()
    cash = starting_cash
    qty = initial_qty
    entry_cost_basis = 0.0
    entry_regime_snapshot: dict[str, object] | None = None
    entry_ts: int | None = None
    entry_price: float | None = None
    entry_decision_hash: str | None = None
    open_trade_path: list[dict[str, float | int]] = []
    entry_fee = 0.0
    entry_slippage = 0.0
    peak = starting_cash
    max_drawdown = 0.0
    fee_total = 0.0
    slippage_total = 0.0
    trades: list[dict[str, object]] = []
    pending_fills: list[_PendingFill] = []
    decisions: list[dict[str, object]] = []
    equity_curve: list[EquityPoint] = []
    retain_initial_equity = accumulator.retain_equity_point()
    if retain_initial_equity:
        equity_curve.append(
            EquityPoint(
                ts=candle_close_ts(candles[0], interval=dataset.interval),
                equity=starting_cash,
                cash=starting_cash,
                asset_qty=initial_qty,
            )
        )
    accumulator.update_equity(
        retained=retain_initial_equity,
        ts=candle_close_ts(candles[0], interval=dataset.interval),
        asset_qty=initial_qty,
    )
    _trace_equity_mark(
        run_context,
        ts=candle_close_ts(candles[0], interval=dataset.interval),
        equity=starting_cash,
        cash=starting_cash,
        asset_qty=initial_qty,
    )
    closed_pnls: list[float] = []
    model = execution_model or FixedBpsExecutionModel(fee_rate=fee_rate, slippage_bps=slippage_bps)

    for event in decision_events:
        event_extra = event.extra_payload
        index = int(event_extra["index"])
        candle = candles[index]
        mark_boundary_ts = candle_close_ts(candle, interval=dataset.interval)
        decision_boundary_ts = int(event.decision_ts)
        cash, qty, entry_cost_basis, entry_regime_snapshot, entry_ts, entry_price, entry_decision_hash, open_trade_path, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=mark_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_ts=entry_ts,
            entry_price=entry_price,
            entry_decision_hash=entry_decision_hash,
            open_trade_path=open_trade_path,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        mark_cash = cash
        mark_qty = qty
        cash, qty, entry_cost_basis, entry_regime_snapshot, entry_ts, entry_price, entry_decision_hash, open_trade_path, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=decision_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_ts=entry_ts,
            entry_price=entry_price,
            entry_decision_hash=entry_decision_hash,
            open_trade_path=open_trade_path,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        prev_short = float(event_extra["prev_s"])
        prev_long = float(event_extra["prev_l"])
        curr_short = float(event_extra["curr_s"])
        curr_long = float(event_extra["curr_l"])
        regime_snapshot = dict(event_extra["regime_snapshot"])
        regime_coverage_accumulator.update(regime_snapshot)
        if accumulator.retain_full_detail():
            regime_snapshots.append(regime_snapshot)

        action = "HOLD"
        exit_rule = ""
        exit_reason = ""
        exit_evaluations: list[dict[str, object]] = []
        pending_buy_qty = sum(item.qty for item in pending_fills if item.side == "BUY")
        pending_sell_qty = sum(item.qty for item in pending_fills if item.side == "SELL")
        sellable_qty = max(0.0, qty - pending_sell_qty)
        entry_decision = event_extra["entry_decision"]
        raw_signal = str(event.raw_signal or "HOLD").upper()
        raw_reason = str(event_extra["raw_reason"])
        blocked_filters = list(event.blocked_filters)
        raw_filter_would_block = bool(event_extra["raw_filter_would_block"])
        entry_filter_blocked = bool(event_extra["entry_filter_blocked"])
        entry_signal = str(event.entry_signal or raw_signal).upper()
        entry_reason = event.reason
        gap_ratio = entry_decision.gap_ratio
        range_ratio = entry_decision.volatility_ratio
        if qty > 1e-12 and entry_price is not None:
            pnl_ratio = ((float(candle.close) - float(entry_price)) / float(entry_price)) if float(entry_price) > 0 else 0.0
            open_trade_path.append(
                {
                    "ts": int(candle.ts),
                    "close": float(candle.close),
                    "unrealized_pnl": (float(candle.close) - float(entry_price)) * float(qty),
                    "unrealized_pnl_pct": pnl_ratio * 100.0,
                }
            )
        if not entry_filter_blocked and event_extra["prev_above"] is not None:
            if entry_signal == "BUY" and qty <= 0.0 and pending_buy_qty <= 0.0:
                action = "BUY"
        if sellable_qty > 0.0:
            position = _ResearchPositionContext(
                in_position=True,
                entry_ts=entry_ts,
                entry_price=entry_price,
                qty_open=sellable_qty,
                holding_time_sec=(
                    max(0.0, (int(candle.ts) - int(entry_ts)) / 1000.0)
                    if entry_ts is not None
                    else 0.0
                ),
                unrealized_pnl=(
                    (float(candle.close) - float(entry_price)) * sellable_qty
                    if entry_price is not None
                    else 0.0
                ),
                unrealized_pnl_ratio=(
                    ((float(candle.close) - float(entry_price)) / float(entry_price))
                    if entry_price not in (None, 0.0)
                    else 0.0
                ),
            )
            for rule in _create_exit_rules(
                rule_names=list(active_exit_policy["rules"]),
                stop_loss_ratio=float(active_exit_policy["stop_loss"]["stop_loss_ratio"]),
                max_holding_sec=float(active_exit_policy["max_holding_time"]["max_holding_min"]) * 60.0,
                min_take_profit_ratio=float(active_exit_policy["opposite_cross"]["min_take_profit_ratio"]),
                live_fee_rate_estimate=float(effective_parameters.get("LIVE_FEE_RATE_ESTIMATE") or fee_rate),
                small_loss_tolerance_ratio=float(active_exit_policy["opposite_cross"]["small_loss_tolerance_ratio"]),
            ):
                result = rule.evaluate(
                    position=position,
                    candle_ts=int(candle.ts),
                    market_price=float(candle.close),
                    signal_context={
                        "base_signal": raw_signal,
                        "base_reason": raw_reason,
                        "entry_signal": entry_signal,
                        "exit_signal": raw_signal,
                        "curr_s": curr_short,
                        "curr_l": curr_long,
                    },
                )
                exit_evaluations.append(
                    {
                        "rule": rule.name,
                        "triggered": bool(result.should_exit),
                        "reason": result.reason,
                        "context": result.context,
                    }
                )
                if result.should_exit:
                    action = "SELL"
                    exit_rule = rule.name
                    exit_reason = result.reason
                    break
        protective_exit_overrode_entry = bool(
            raw_signal == "BUY"
            and action == "SELL"
            and exit_rule in {"stop_loss", "max_holding_time"}
        )
        entry_blocked = bool(raw_signal == "BUY" and action == "HOLD" and raw_filter_would_block)
        decision_payload = _research_decision_payload(
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=effective_parameters,
            strategy_name=strategy_plugin.name,
            strategy_spec=strategy_spec.as_dict(),
            strategy_spec_hash=strategy_spec.spec_hash(),
            strategy_plugin_contract=strategy_plugin.contract_payload(),
            strategy_plugin_contract_hash=strategy_plugin.contract_hash(),
            exit_policy=active_exit_policy,
            exit_policy_hash=active_exit_policy_hash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=policy,
            candle_ts=int(candle.ts),
            decision_ts=int(decision_boundary_ts),
            raw_signal=raw_signal,
            entry_signal=entry_signal,
            exit_signal=raw_signal,
            final_signal=action,
            raw_reason=raw_reason,
            blocked=bool(raw_signal in {"BUY", "SELL"} and action == "HOLD"),
            raw_filter_would_block=raw_filter_would_block,
            entry_blocked=entry_blocked,
            protective_exit_overrode_entry=protective_exit_overrode_entry,
            exit_filter_suppression_prevented=bool(
                raw_signal == "SELL"
                and raw_filter_would_block
                and sellable_qty > 1e-12
                and bool(exit_evaluations)
            ),
            blocked_filters=blocked_filters,
            prev_s=prev_short,
            prev_l=prev_long,
            curr_s=curr_short,
            curr_l=curr_long,
            gap_ratio=gap_ratio,
            range_ratio=range_ratio,
            regime_snapshot=regime_snapshot,
            entry_reason=entry_reason,
            market_regime_decision=entry_decision.candidate_regime_decision,
            market_regime_blocked=entry_decision.market_regime_triggered,
            candidate_regime_blocked=entry_decision.candidate_regime_triggered,
            qty=qty,
            sellable_qty=sellable_qty,
            exit_rule=exit_rule,
            exit_reason=exit_reason,
            exit_evaluations=exit_evaluations,
        )
        retain_decision = accumulator.retain_decision()
        if retain_decision:
            decisions.append(decision_payload)
        accumulator.update_decision(decision_payload, retained=retain_decision)
        _trace_decision(run_context, decision_payload)

        if action == "BUY":
            signal = build_signal_event(
                candle=candle,
                interval=dataset.interval,
                side="BUY",
                policy=timing_policy,
                feature_snapshot=_feature_snapshot(
                    short_sma=curr_short,
                    long_sma=curr_long,
                    gap_ratio=gap_ratio,
                    range_ratio=range_ratio,
                    index=index,
                ),
                regime_snapshot=regime_snapshot,
            )
            reference = resolve_execution_reference(
                dataset=dataset,
                signal=signal,
                signal_index=index,
                policy=timing_policy,
                model_latency_ms=_model_latency_ms(model),
            )
            if reference.fill_reference_price is None:
                fill = _failed_fill(
                    model=model,
                    signal=signal,
                    reference=reference,
                    timing_policy=timing_policy,
                    side="BUY",
                    fee_rate=fee_rate,
                    requested_notional=cash * buy_fraction,
                )
                warnings.extend(_execution_reference_warnings(fill))
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                _trace_execution(run_context, trades[-1])
                retain_equity = accumulator.retain_equity_point()
                peak, max_drawdown = _record_equity_mark(
                    equity_curve=equity_curve,
                    ts=mark_boundary_ts,
                    cash=mark_cash,
                    qty=mark_qty,
                    mark_price=candle.close,
                    peak=peak,
                    max_drawdown=max_drawdown,
                    retain=retain_equity,
                )
                accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
                _trace_equity_mark(
                    run_context,
                    ts=mark_boundary_ts,
                    equity=mark_cash + mark_qty * candle.close,
                    cash=mark_cash,
                    asset_qty=mark_qty,
                )
                accumulator.maybe_emit_heartbeat(int(event_extra["processed_count"]))
                accumulator.check_limits(candles_processed=int(event_extra["processed_count"]), trades=trades)
                continue
            spend = cash * buy_fraction
            fill = model.simulate(
                ExecutionRequest(
                    signal_ts=signal.signal_candle_start_ts,
                    decision_ts=signal.decision_ts,
                    side="BUY",
                    reference_price=float(reference.fill_reference_price),
                    requested_notional=spend,
                    fee_rate=fee_rate,
                    **_timing_request_fields(signal, reference, timing_policy),
                    **_depth_request_fields(
                        dataset=dataset,
                        reference=reference,
                        model=model,
                        timing_policy=timing_policy,
                    ),
                )
            )
            warnings.extend(_execution_reference_warnings(fill))
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                _trace_execution(run_context, trades[-1])
                retain_equity = accumulator.retain_equity_point()
                peak, max_drawdown = _record_equity_mark(
                    equity_curve=equity_curve,
                    ts=mark_boundary_ts,
                    cash=mark_cash,
                    qty=mark_qty,
                    mark_price=candle.close,
                    peak=peak,
                    max_drawdown=max_drawdown,
                    retain=retain_equity,
                )
                accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
                _trace_equity_mark(
                    run_context,
                    ts=mark_boundary_ts,
                    equity=mark_cash + mark_qty * candle.close,
                    cash=mark_cash,
                    asset_qty=mark_qty,
                )
                accumulator.maybe_emit_heartbeat(int(event_extra["processed_count"]))
                accumulator.check_limits(candles_processed=int(event_extra["processed_count"]), trades=trades)
                continue
            exec_price = float(fill.avg_fill_price)
            fee = fill.fee
            received_qty = fill.filled_qty
            actual_spend = (exec_price * received_qty) + fee
            reference_cost = float(fill.reference_price) * received_qty
            slipped_cost = exec_price * received_qty
            buy_slippage = max(0.0, slipped_cost - reference_cost)
            pending = _PendingFill(
                fill=fill,
                trade_index=len(trades),
                side="BUY",
                effective_ts=_fill_effective_ts(fill),
                qty=received_qty,
                fee=fee,
                slippage=buy_slippage,
                cash_delta=-actual_spend,
                entry_regime_snapshot=dict(regime_snapshot),
            )
            trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
            trades[-1]["entry_decision_hash"] = decision_payload.get("replay_fingerprint_hash")
            _trace_execution(run_context, trades[-1])
            if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                mark_cash += pending.cash_delta
                mark_qty += pending.qty
            pending_fills.append(pending)
            cash, qty, entry_cost_basis, entry_regime_snapshot, entry_ts, entry_price, entry_decision_hash, open_trade_path, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
                entry_ts=entry_ts,
                entry_price=entry_price,
                entry_decision_hash=entry_decision_hash,
                open_trade_path=open_trade_path,
                entry_fee=entry_fee,
                entry_slippage=entry_slippage,
                fee_total=fee_total,
                slippage_total=slippage_total,
                closed_pnls=closed_pnls,
            )
        elif action == "SELL":
            signal = build_signal_event(
                candle=candle,
                interval=dataset.interval,
                side="SELL",
                policy=timing_policy,
                feature_snapshot=_feature_snapshot(
                    short_sma=curr_short,
                    long_sma=curr_long,
                    gap_ratio=gap_ratio,
                    range_ratio=range_ratio,
                    index=index,
                ),
                regime_snapshot=regime_snapshot,
            )
            reference = resolve_execution_reference(
                dataset=dataset,
                signal=signal,
                signal_index=index,
                policy=timing_policy,
                model_latency_ms=_model_latency_ms(model),
            )
            if reference.fill_reference_price is None:
                fill = _failed_fill(
                    model=model,
                    signal=signal,
                    reference=reference,
                    timing_policy=timing_policy,
                    side="SELL",
                    fee_rate=fee_rate,
                    requested_qty=sellable_qty,
                )
                warnings.extend(_execution_reference_warnings(fill))
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                _trace_execution(run_context, trades[-1])
                retain_equity = accumulator.retain_equity_point()
                peak, max_drawdown = _record_equity_mark(
                    equity_curve=equity_curve,
                    ts=mark_boundary_ts,
                    cash=mark_cash,
                    qty=mark_qty,
                    mark_price=candle.close,
                    peak=peak,
                    max_drawdown=max_drawdown,
                    retain=retain_equity,
                )
                accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
                _trace_equity_mark(
                    run_context,
                    ts=mark_boundary_ts,
                    equity=mark_cash + mark_qty * candle.close,
                    cash=mark_cash,
                    asset_qty=mark_qty,
                )
                accumulator.maybe_emit_heartbeat(int(event_extra["processed_count"]))
                accumulator.check_limits(candles_processed=int(event_extra["processed_count"]), trades=trades)
                continue
            fill = model.simulate(
                ExecutionRequest(
                    signal_ts=signal.signal_candle_start_ts,
                    decision_ts=signal.decision_ts,
                    side="SELL",
                    reference_price=float(reference.fill_reference_price),
                    requested_qty=sellable_qty,
                    fee_rate=fee_rate,
                    **_timing_request_fields(signal, reference, timing_policy),
                    **_depth_request_fields(
                        dataset=dataset,
                        reference=reference,
                        model=model,
                        timing_policy=timing_policy,
                    ),
                )
            )
            warnings.extend(_execution_reference_warnings(fill))
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                _trace_execution(run_context, trades[-1])
                retain_equity = accumulator.retain_equity_point()
                peak, max_drawdown = _record_equity_mark(
                    equity_curve=equity_curve,
                    ts=mark_boundary_ts,
                    cash=mark_cash,
                    qty=mark_qty,
                    mark_price=candle.close,
                    peak=peak,
                    max_drawdown=max_drawdown,
                    retain=retain_equity,
                )
                accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
                _trace_equity_mark(
                    run_context,
                    ts=mark_boundary_ts,
                    equity=mark_cash + mark_qty * candle.close,
                    cash=mark_cash,
                    asset_qty=mark_qty,
                )
                accumulator.maybe_emit_heartbeat(int(event_extra["processed_count"]))
                accumulator.check_limits(candles_processed=int(event_extra["processed_count"]), trades=trades)
                continue
            exec_price = float(fill.avg_fill_price)
            sell_qty = fill.filled_qty
            gross = sell_qty * exec_price
            fee = fill.fee
            reference_proceeds = float(fill.reference_price) * sell_qty
            sell_slippage = max(0.0, reference_proceeds - gross)
            net_proceeds = gross - fee
            pending = _PendingFill(
                fill=fill,
                trade_index=len(trades),
                side="SELL",
                effective_ts=_fill_effective_ts(fill),
                qty=sell_qty,
                fee=fee,
                slippage=sell_slippage,
                cash_delta=net_proceeds,
                entry_regime_snapshot=entry_regime_snapshot,
                exit_regime_snapshot=dict(regime_snapshot),
            )
            trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
            trades[-1].update(
                _closed_trade_diagnostics(
                    entry_ts=entry_ts,
                    exit_ts=int(candle.ts),
                    entry_price=entry_price,
                    exit_price=exec_price,
                    entry_regime_snapshot=entry_regime_snapshot,
                    exit_regime_snapshot=dict(regime_snapshot),
                    exit_rule=exit_rule,
                    exit_reason=exit_reason,
                    path=open_trade_path,
                    entry_decision_hash=entry_decision_hash,
                    exit_decision_hash=str(decision_payload.get("replay_fingerprint_hash") or ""),
                )
            )
            _trace_execution(run_context, trades[-1])
            if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                mark_cash += pending.cash_delta
                mark_qty = max(0.0, mark_qty - pending.qty)
            pending_fills.append(pending)
            cash, qty, entry_cost_basis, entry_regime_snapshot, entry_ts, entry_price, entry_decision_hash, open_trade_path, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
                entry_ts=entry_ts,
                entry_price=entry_price,
                entry_decision_hash=entry_decision_hash,
                open_trade_path=open_trade_path,
                entry_fee=entry_fee,
                entry_slippage=entry_slippage,
                fee_total=fee_total,
                slippage_total=slippage_total,
                closed_pnls=closed_pnls,
            )

        retain_equity = accumulator.retain_equity_point()
        peak, max_drawdown = _record_equity_mark(
            equity_curve=equity_curve,
            ts=mark_boundary_ts,
            cash=mark_cash,
            qty=mark_qty,
            mark_price=candle.close,
            peak=peak,
            max_drawdown=max_drawdown,
            retain=retain_equity,
        )
        accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
        _trace_equity_mark(
            run_context,
            ts=mark_boundary_ts,
            equity=mark_cash + mark_qty * candle.close,
            cash=mark_cash,
            asset_qty=mark_qty,
        )
        accumulator.maybe_emit_heartbeat(int(event_extra["processed_count"]))
        accumulator.check_limits(candles_processed=int(event_extra["processed_count"]), trades=trades)

    last = candles[-1]
    last_mark_ts = candle_close_ts(last, interval=dataset.interval)
    cash, qty, entry_cost_basis, entry_regime_snapshot, entry_ts, entry_price, entry_decision_hash, open_trade_path, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
        pending_fills=pending_fills,
        trades=trades,
        boundary_ts=last_mark_ts,
        cash=cash,
        qty=qty,
        entry_cost_basis=entry_cost_basis,
        entry_regime_snapshot=entry_regime_snapshot,
        entry_ts=entry_ts,
        entry_price=entry_price,
        entry_decision_hash=entry_decision_hash,
        open_trade_path=open_trade_path,
        entry_fee=entry_fee,
        entry_slippage=entry_slippage,
        fee_total=fee_total,
        slippage_total=slippage_total,
        closed_pnls=closed_pnls,
    )
    _mark_pending_fills_at_end(pending_fills=pending_fills, trades=trades, final_mark_ts=last_mark_ts)
    final_equity = cash + qty * last.close
    retain_final_equity = accumulator.retain_equity_point()
    if retain_final_equity:
        equity_curve.append(
            EquityPoint(
                ts=last_mark_ts,
                equity=final_equity,
                cash=cash,
                asset_qty=qty,
            )
        )
    accumulator.update_equity(retained=retain_final_equity, ts=last_mark_ts, asset_qty=qty)
    _trace_equity_mark(
        run_context,
        ts=last_mark_ts,
        equity=final_equity,
        cash=cash,
        asset_qty=qty,
    )
    return_pct = ((final_equity / starting_cash) - 1.0) * 100.0 if starting_cash > 0.0 else 0.0
    metrics = _metrics(
        return_pct=return_pct,
        max_drawdown_pct=max_drawdown * 100.0,
        closed_pnls=closed_pnls,
        fee_total=fee_total,
        slippage_total=slippage_total,
        parameter_stability_score=parameter_stability_score,
    )
    position_intervals, closed_trade_records, execution_records, derived_open_cost_basis = _metrics_v2_ledgers_from_trades(
        trades=trades,
    )
    coverage = (
        aggregate_regime_coverage(snapshots=regime_snapshots, trades=trades)
        if accumulator.retain_full_detail()
        else regime_coverage_accumulator.coverage(trades=trades)
    )
    performance = aggregate_regime_performance(trades=trades, coverage=coverage, start_cash=starting_cash)
    execution_summary = execution_event_summary(trades)
    metrics_v2 = build_metrics_v2(
        starting_cash=starting_cash,
        final_cash=cash,
        final_asset_qty=qty,
        final_mark_price=last.close,
        final_open_cost_basis=entry_cost_basis if qty > 0.0 else derived_open_cost_basis,
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        execution_records=execution_records,
        **(
            {}
            if accumulator.retain_full_detail()
            else accumulator.metrics_summary_inputs(max_drawdown_pct=max_drawdown * 100.0)
        ),
    )
    if not accumulator.retain_full_detail():
        metrics_v2 = replace(
            metrics_v2,
            limitation_reasons=tuple(
                sorted(set(metrics_v2.limitation_reasons) | {"bounded_detail_equity_curve_not_retained"})
            ),
        )
    audit_trace_index = _complete_audit_trace(run_context, status="completed")
    accumulator.trade_ledger_hash_material = [_trade_hash_payload(trade) for trade in trades]
    accumulator.equity_curve_hash_material = [
        {
            "ts": int(point.ts),
            "equity": round(float(point.equity), 12),
            "cash": round(float(point.cash), 12),
            "asset_qty": round(float(point.asset_qty), 12),
        }
        for point in equity_curve
    ]
    strategy_diagnostics = accumulator.strategy_diagnostics(trades=trades)
    resource_usage = accumulator.resource_usage(candles_processed=max(0, len(candles) - long_n))
    resource_usage["strategy_diagnostics"] = strategy_diagnostics
    return BacktestRun(
        metrics=metrics,
        metrics_v2=metrics_v2,
        trades=tuple(trades),
        candle_count=len(candles),
        warnings=tuple(warnings),
        regime_performance=performance,
        regime_coverage=coverage,
        execution_event_summary=execution_summary,
        decisions=tuple(decisions),
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        resource_usage=resource_usage,
        strategy_diagnostics=strategy_diagnostics,
        retained_detail_summary=_retained_detail_summary(
            accumulator,
            retained_regime_snapshot_count=len(regime_snapshots),
        ),
        audit_trace_index=audit_trace_index,
    )


def run_noop_baseline_backtest(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    del execution_model
    from .strategy_registry import resolve_research_strategy_plugin

    strategy_plugin = resolve_research_strategy_plugin("noop_baseline")
    strategy_spec = strategy_spec_for_name("noop_baseline")
    effective_parameters = materialize_strategy_parameters(
        "noop_baseline",
        parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    start_index = max(0, int(effective_parameters.get("NOOP_DECISION_START_INDEX", 0)))
    decision_reason = str(effective_parameters.get("NOOP_DECISION_REASON") or "noop_baseline_hold")
    active_exit_policy = exit_policy_from_parameters("noop_baseline", effective_parameters)
    active_exit_policy_hash = exit_policy_hash(active_exit_policy)
    candles = dataset.candles
    run_context = context or BacktestRunContext(report_detail="full")
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    policy = portfolio_policy or legacy_research_portfolio_policy()
    starting_cash = float(policy.starting_cash_krw)
    initial_qty = float(policy.initial_position_qty)
    accumulator = _BacktestAccumulator(
        context=run_context,
        total_candles=len(candles),
        diagnostics_namespace=strategy_plugin.diagnostics_namespace,
    )
    dataset_content_hash = dataset.content_hash()
    if not candles:
        audit_trace_index = _complete_audit_trace(run_context, status="completed")
        return BacktestRun(
            metrics=_empty_metrics(parameter_stability_score),
            metrics_v2=_empty_metrics_v2(starting_cash=starting_cash, initial_position_qty=initial_qty),
            trades=(),
            candle_count=0,
            warnings=("not_enough_candles",),
            execution_event_summary=empty_execution_event_summary(),
            resource_usage=accumulator.resource_usage(candles_processed=0),
            strategy_diagnostics=_noop_strategy_diagnostics(decision_count=0),
            retained_detail_summary=_retained_detail_summary(accumulator, retained_regime_snapshot_count=0),
            audit_trace_index=audit_trace_index,
        )

    decisions: list[dict[str, object]] = []
    equity_curve: list[EquityPoint] = []
    cash = starting_cash
    qty = initial_qty
    peak = starting_cash
    max_drawdown = 0.0
    first_ts = candle_close_ts(candles[0], interval=dataset.interval)
    retain_initial_equity = accumulator.retain_equity_point()
    if retain_initial_equity:
        equity_curve.append(EquityPoint(ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty))
    accumulator.update_equity(retained=retain_initial_equity, ts=first_ts, asset_qty=qty)
    accumulator.record_equity_point(ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty)
    _trace_equity_mark(run_context, ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty)

    for index, candle in enumerate(candles):
        if index < start_index:
            continue
        mark_boundary_ts = candle_close_ts(candle, interval=dataset.interval)
        decision_boundary_ts = mark_boundary_ts + int(timing_policy.decision_guard_ms)
        feature_snapshot = {
            "candle_index": int(index),
            "close": float(candle.close),
            "start_index": int(start_index),
        }
        event = ResearchDecisionEvent(
            candle_ts=int(candle.ts),
            decision_ts=int(decision_boundary_ts),
            strategy_name=strategy_plugin.name,
            strategy_version=strategy_plugin.version,
            raw_signal="HOLD",
            final_signal="HOLD",
            reason=decision_reason,
            feature_snapshot=feature_snapshot,
            strategy_diagnostics={
                "schema_version": 1,
                "hold_decision_count": int(accumulator.decision_count + 1),
                "start_index": int(start_index),
            },
            entry_signal="HOLD",
            exit_signal="HOLD",
        )
        decision_payload = _research_decision_payload(
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=effective_parameters,
            strategy_name=strategy_plugin.name,
            strategy_spec=strategy_spec.as_dict(),
            strategy_spec_hash=strategy_spec.spec_hash(),
            strategy_plugin_contract=strategy_plugin.contract_payload(),
            strategy_plugin_contract_hash=strategy_plugin.contract_hash(),
            exit_policy=active_exit_policy,
            exit_policy_hash=active_exit_policy_hash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=policy,
            candle_ts=event.candle_ts,
            decision_ts=event.decision_ts,
            raw_signal=event.raw_signal,
            entry_signal=event.entry_signal or event.raw_signal,
            exit_signal=event.exit_signal or event.raw_signal,
            final_signal=event.final_signal,
            raw_reason=event.reason,
            blocked=False,
            raw_filter_would_block=False,
            entry_blocked=False,
            protective_exit_overrode_entry=False,
            exit_filter_suppression_prevented=False,
            blocked_filters=list(event.blocked_filters),
            prev_s=0.0,
            prev_l=0.0,
            curr_s=0.0,
            curr_l=0.0,
            gap_ratio=0.0,
            range_ratio=0.0,
            regime_snapshot={"composite_regime": "not_evaluated"},
            entry_reason=event.reason,
            market_regime_decision={"regime_decision": "not_configured"},
            market_regime_blocked=False,
            candidate_regime_blocked=False,
            qty=qty,
            sellable_qty=qty,
            exit_rule="",
            exit_reason="",
            exit_evaluations=[],
        )
        decision_payload.update(
            {
                "decision_event_schema_version": 1,
                "strategy_decision_contract_version": strategy_plugin.decision_contract_version,
                "raw_reason": event.reason,
                "feature_snapshot": dict(event.feature_snapshot),
                "strategy_diagnostics_namespace": strategy_plugin.diagnostics_namespace,
                "strategy_diagnostics": dict(event.strategy_diagnostics),
                "strategy_behavior_payload": {
                    "strategy_name": event.strategy_name,
                    "strategy_version": event.strategy_version,
                    "raw_signal": event.raw_signal,
                    "final_signal": event.final_signal,
                    "reason": event.reason,
                    "feature_snapshot": dict(event.feature_snapshot),
                },
                "execution_intent": "none",
                "order_intent": None,
                "exit_intent": None,
            }
        )
        retain_decision = accumulator.retain_decision()
        if retain_decision:
            decisions.append(decision_payload)
        accumulator.update_decision(decision_payload, retained=retain_decision)
        _trace_decision(run_context, decision_payload)
        mark_equity = cash + qty * float(candle.close)
        retain_equity = accumulator.retain_equity_point()
        peak, max_drawdown = _record_equity_mark(
            equity_curve=equity_curve,
            ts=mark_boundary_ts,
            cash=cash,
            qty=qty,
            mark_price=candle.close,
            peak=peak,
            max_drawdown=max_drawdown,
            retain=retain_equity,
        )
        accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=qty)
        accumulator.record_equity_point(ts=mark_boundary_ts, equity=mark_equity, cash=cash, asset_qty=qty)
        _trace_equity_mark(run_context, ts=mark_boundary_ts, equity=mark_equity, cash=cash, asset_qty=qty)
        accumulator.maybe_emit_heartbeat(index + 1)
        accumulator.check_limits(candles_processed=index + 1, trades=[])

    last = candles[-1]
    final_equity = cash + qty * float(last.close)
    return_pct = ((final_equity / starting_cash) - 1.0) * 100.0 if starting_cash > 0.0 else 0.0
    position_intervals, closed_trade_records, execution_records, derived_open_cost_basis = _metrics_v2_ledgers_from_trades(
        trades=[],
    )
    metrics_v2 = build_metrics_v2(
        starting_cash=starting_cash,
        final_cash=cash,
        final_asset_qty=qty,
        final_mark_price=last.close,
        final_open_cost_basis=derived_open_cost_basis,
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        execution_records=execution_records,
        **(
            {}
            if accumulator.retain_full_detail()
            else accumulator.metrics_summary_inputs(max_drawdown_pct=max_drawdown * 100.0)
        ),
    )
    if not accumulator.retain_full_detail():
        metrics_v2 = replace(
            metrics_v2,
            limitation_reasons=tuple(
                sorted(set(metrics_v2.limitation_reasons) | {"bounded_detail_equity_curve_not_retained"})
            ),
        )
    audit_trace_index = _complete_audit_trace(run_context, status="completed")
    strategy_diagnostics = _noop_strategy_diagnostics(
        decision_count=accumulator.decision_count,
        start_index=start_index,
    )
    resource_usage = accumulator.resource_usage(candles_processed=max(0, len(candles) - start_index))
    resource_usage["strategy_diagnostics"] = strategy_diagnostics
    return BacktestRun(
        metrics=_metrics(
            return_pct=return_pct,
            max_drawdown_pct=max_drawdown * 100.0,
            closed_pnls=[],
            fee_total=0.0,
            slippage_total=0.0,
            parameter_stability_score=parameter_stability_score,
        ),
        metrics_v2=metrics_v2,
        trades=(),
        candle_count=len(candles),
        warnings=(),
        regime_performance=(),
        regime_coverage=(),
        execution_event_summary=empty_execution_event_summary(),
        decisions=tuple(decisions),
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        resource_usage=resource_usage,
        strategy_diagnostics=strategy_diagnostics,
        retained_detail_summary=_retained_detail_summary(accumulator, retained_regime_snapshot_count=0),
        audit_trace_index=audit_trace_index,
    )


def run_buy_and_hold_baseline_backtest(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    from .strategy_registry import resolve_research_strategy_plugin

    strategy_plugin = resolve_research_strategy_plugin("buy_and_hold_baseline")
    effective_parameters = materialize_strategy_parameters(
        "buy_and_hold_baseline",
        parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    buy_index = max(0, int(effective_parameters.get("BUY_HOLD_BUY_INDEX", 0)))
    decision_reason = str(effective_parameters.get("BUY_HOLD_DECISION_REASON") or "buy_and_hold_architecture_canary")
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    events: list[ResearchDecisionEvent] = []
    for index, candle in enumerate(dataset.candles):
        action = "BUY" if index == buy_index else "HOLD"
        decision_ts = candle_close_ts(candle, interval=dataset.interval) + int(timing_policy.decision_guard_ms)
        feature_snapshot = {
            "candle_index": int(index),
            "buy_index": int(buy_index),
            "close": float(candle.close),
        }
        events.append(
            ResearchDecisionEvent(
                candle_ts=int(candle.ts),
                decision_ts=int(decision_ts),
                strategy_name=strategy_plugin.name,
                strategy_version=strategy_plugin.version,
                raw_signal=action,
                final_signal=action,
                reason=decision_reason if action == "BUY" else "buy_and_hold_after_entry_hold",
                feature_snapshot=feature_snapshot,
                strategy_diagnostics={
                    "schema_version": 1,
                    "buy_index": int(buy_index),
                    "candle_index": int(index),
                    "emitted_buy_intent": action == "BUY",
                },
                entry_signal=action if action == "BUY" else "HOLD",
                exit_signal="HOLD",
                order_intent=(
                    {
                        "side": "BUY",
                        "sizing": "portfolio_policy_fractional_cash",
                        "buy_fraction": float(
                            (portfolio_policy or legacy_research_portfolio_policy()).position_sizing.buy_fraction
                        ),
                    }
                    if action == "BUY"
                    else None
                ),
            )
        )
    from .backtest_kernel import run_decision_event_backtest as _run_decision_event_backtest

    return _run_decision_event_backtest(
        dataset=dataset,
        strategy_name=strategy_plugin.name,
        parameter_values=effective_parameters,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        decision_events=tuple(events),
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def run_decision_event_backtest(
    *,
    dataset: DatasetSnapshot,
    strategy_name: str,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    decision_events: tuple[ResearchDecisionEvent, ...],
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    """Execute strategy decision events through the shared research backtest kernel.

    Import new call sites from ``bithumb_bot.research.backtest_kernel``. This
    compatibility entrypoint remains for existing callers while the helper graph
    in this module is split into a standalone implementation module.
    """
    from .strategy_registry import resolve_research_strategy_plugin

    strategy_plugin = resolve_research_strategy_plugin(strategy_name)
    strategy_spec = strategy_spec_for_name(strategy_name)
    active_exit_policy = exit_policy_from_parameters(strategy_name, parameter_values)
    active_exit_policy_hash = exit_policy_hash(active_exit_policy)
    candles = dataset.candles
    run_context = context or BacktestRunContext(report_detail="full")
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()
    policy = portfolio_policy or legacy_research_portfolio_policy()
    model = execution_model or FixedBpsExecutionModel(fee_rate=fee_rate, slippage_bps=slippage_bps)
    starting_cash = float(policy.starting_cash_krw)
    cash = starting_cash
    qty = float(policy.initial_position_qty)
    buy_fraction = float(policy.position_sizing.buy_fraction)
    accumulator = _BacktestAccumulator(
        context=run_context,
        total_candles=len(candles),
        diagnostics_namespace=strategy_plugin.diagnostics_namespace,
    )
    if not candles:
        audit_trace_index = _complete_audit_trace(run_context, status="completed")
        return BacktestRun(
            metrics=_empty_metrics(parameter_stability_score),
            metrics_v2=_empty_metrics_v2(starting_cash=starting_cash, initial_position_qty=qty),
            trades=(),
            candle_count=0,
            warnings=("not_enough_candles",),
            execution_event_summary=empty_execution_event_summary(),
            resource_usage=accumulator.resource_usage(candles_processed=0),
            strategy_diagnostics=accumulator.strategy_diagnostics(trades=[]),
            retained_detail_summary=_retained_detail_summary(accumulator, retained_regime_snapshot_count=0),
            audit_trace_index=audit_trace_index,
        )

    dataset_content_hash = dataset.content_hash()
    candle_index_by_ts = {int(candle.ts): index for index, candle in enumerate(candles)}
    trades: list[dict[str, object]] = []
    decisions: list[dict[str, object]] = []
    equity_curve: list[EquityPoint] = []
    pending_fills: list[_PendingFill] = []
    warnings: list[str] = []
    closed_pnls: list[float] = []
    entry_cost_basis = 0.0
    entry_regime_snapshot: dict[str, object] | None = None
    entry_ts: int | None = None
    entry_price: float | None = None
    entry_decision_hash: str | None = None
    open_trade_path: list[dict[str, float | int]] = []
    entry_fee = 0.0
    entry_slippage = 0.0
    fee_total = 0.0
    slippage_total = 0.0
    peak = starting_cash
    max_drawdown = 0.0
    regime_snapshots: list[dict[str, object]] = []
    regime_coverage_accumulator = _RegimeCoverageAccumulator()

    first = candles[0]
    first_ts = candle_close_ts(first, interval=dataset.interval)
    retain_initial_equity = accumulator.retain_equity_point()
    if retain_initial_equity:
        equity_curve.append(EquityPoint(ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty))
    accumulator.update_equity(retained=retain_initial_equity, ts=first_ts, asset_qty=qty)
    _trace_equity_mark(run_context, ts=first_ts, equity=starting_cash, cash=cash, asset_qty=qty)

    for event_number, event in enumerate(decision_events, start=1):
        if event.strategy_name != strategy_plugin.name:
            raise ValueError(f"decision_event_strategy_mismatch:{event.strategy_name}")
        index = candle_index_by_ts.get(int(event.candle_ts))
        if index is None:
            raise ValueError(f"decision_event_candle_missing:{event.candle_ts}")
        candle = candles[index]
        mark_boundary_ts = candle_close_ts(candle, interval=dataset.interval)
        decision_boundary_ts = int(event.decision_ts)
        (
            cash,
            qty,
            entry_cost_basis,
            entry_regime_snapshot,
            entry_ts,
            entry_price,
            entry_decision_hash,
            open_trade_path,
            entry_fee,
            entry_slippage,
            fee_total,
            slippage_total,
        ) = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=mark_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_ts=entry_ts,
            entry_price=entry_price,
            entry_decision_hash=entry_decision_hash,
            open_trade_path=open_trade_path,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        mark_cash = cash
        mark_qty = qty
        (
            cash,
            qty,
            entry_cost_basis,
            entry_regime_snapshot,
            entry_ts,
            entry_price,
            entry_decision_hash,
            open_trade_path,
            entry_fee,
            entry_slippage,
            fee_total,
            slippage_total,
        ) = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=decision_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_ts=entry_ts,
            entry_price=entry_price,
            entry_decision_hash=entry_decision_hash,
            open_trade_path=open_trade_path,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        if qty > 1e-12 and entry_price is not None:
            pnl_ratio = (
                ((float(candle.close) - float(entry_price)) / float(entry_price))
                if float(entry_price) > 0
                else 0.0
            )
            open_trade_path.append(
                {
                    "ts": int(candle.ts),
                    "close": float(candle.close),
                    "unrealized_pnl": (float(candle.close) - float(entry_price)) * float(qty),
                    "unrealized_pnl_pct": pnl_ratio * 100.0,
                }
            )
        pending_buy_qty = sum(item.qty for item in pending_fills if item.side == "BUY")
        pending_sell_qty = sum(item.qty for item in pending_fills if item.side == "SELL")
        sellable_qty = max(0.0, qty - pending_sell_qty)
        event_extra = event.extra_payload if isinstance(event.extra_payload, dict) else {}
        event_feature_snapshot = dict(event.feature_snapshot)
        regime_snapshot = dict(
            event_extra.get("regime_snapshot")
            or {"composite_regime": "strategy_neutral_not_evaluated"}
        )
        regime_coverage_accumulator.update(regime_snapshot)
        if accumulator.retain_full_detail():
            regime_snapshots.append(regime_snapshot)
        entry_decision = event_extra.get("entry_decision")
        prev_s = float(event_extra.get("prev_s", 0.0) or 0.0)
        prev_l = float(event_extra.get("prev_l", 0.0) or 0.0)
        curr_s = float(
            event_extra.get("curr_s", event_feature_snapshot.get("short_sma", 0.0)) or 0.0
        )
        curr_l = float(
            event_extra.get("curr_l", event_feature_snapshot.get("long_sma", 0.0)) or 0.0
        )
        gap_ratio = float(
            event_feature_snapshot.get("gap_ratio", event_extra.get("gap_ratio", 0.0)) or 0.0
        )
        range_ratio = float(
            event_feature_snapshot.get("range_ratio", event_extra.get("range_ratio", 0.0)) or 0.0
        )
        raw_signal = str(event.raw_signal or "HOLD").upper()
        raw_reason = str(event_extra.get("raw_reason") or event.reason)
        raw_filter_would_block = bool(event_extra.get("raw_filter_would_block", bool(event.blocked_filters)))
        entry_filter_blocked = bool(event_extra.get("entry_filter_blocked", False))
        entry_signal = str(event.entry_signal or raw_signal).upper()
        market_regime_decision = (
            dict(getattr(entry_decision, "candidate_regime_decision"))
            if entry_decision is not None
            and isinstance(getattr(entry_decision, "candidate_regime_decision", None), dict)
            else {"regime_decision": "not_configured"}
        )
        market_regime_blocked = bool(
            getattr(entry_decision, "market_regime_triggered", False)
            if entry_decision is not None
            else False
        )
        candidate_regime_blocked = bool(
            getattr(entry_decision, "candidate_regime_triggered", False)
            if entry_decision is not None
            else False
        )
        requested_action = str(event.final_signal or "HOLD").upper()
        action = requested_action
        blocked = False
        block_reason = event.reason
        exit_evaluations: list[dict[str, object]] = []
        exit_rule = str((event.exit_intent or {}).get("exit_rule") or "") if event.exit_intent else ""
        exit_reason = str((event.exit_intent or {}).get("exit_reason") or "") if event.exit_intent else ""
        evaluates_exit_policy = bool(
            isinstance(event.exit_intent, dict)
            and str(event.exit_intent.get("mode") or "") == "evaluate_exit_policy"
        )
        if evaluates_exit_policy:
            action = "BUY" if requested_action == "BUY" else "HOLD"
            if sellable_qty > 1e-12:
                position = _ResearchPositionContext(
                    in_position=True,
                    entry_ts=entry_ts,
                    entry_price=entry_price,
                    qty_open=sellable_qty,
                    holding_time_sec=(
                        max(0.0, (int(candle.ts) - int(entry_ts)) / 1000.0)
                        if entry_ts is not None
                        else 0.0
                    ),
                    unrealized_pnl=(
                        (float(candle.close) - float(entry_price)) * sellable_qty
                        if entry_price is not None
                        else 0.0
                    ),
                    unrealized_pnl_ratio=(
                        ((float(candle.close) - float(entry_price)) / float(entry_price))
                        if entry_price not in (None, 0.0)
                        else 0.0
                    ),
                )
                for rule in _create_exit_rules(
                    rule_names=list(active_exit_policy["rules"]),
                    stop_loss_ratio=float(active_exit_policy.get("stop_loss", {}).get("stop_loss_ratio", 0.0)),
                    max_holding_sec=float(
                        active_exit_policy.get("max_holding_time", {}).get("max_holding_min", 0.0)
                    )
                    * 60.0,
                    min_take_profit_ratio=float(
                        active_exit_policy.get("opposite_cross", {}).get("min_take_profit_ratio", 0.0)
                    ),
                    live_fee_rate_estimate=float(parameter_values.get("LIVE_FEE_RATE_ESTIMATE") or fee_rate),
                    small_loss_tolerance_ratio=float(
                        active_exit_policy.get("opposite_cross", {}).get("small_loss_tolerance_ratio", 0.0)
                    ),
                ):
                    result = rule.evaluate(
                        position=position,
                        candle_ts=int(candle.ts),
                        market_price=float(candle.close),
                        signal_context={
                            "base_signal": raw_signal,
                            "base_reason": raw_reason,
                            "entry_signal": entry_signal,
                            "exit_signal": event.exit_signal or raw_signal,
                            "curr_s": curr_s,
                            "curr_l": curr_l,
                        },
                    )
                    exit_evaluations.append(
                        {
                            "rule": rule.name,
                            "triggered": bool(result.should_exit),
                            "reason": result.reason,
                            "context": result.context,
                        }
                    )
                    if result.should_exit:
                        action = "SELL"
                        exit_rule = rule.name
                        exit_reason = result.reason
                        break
        if action == "BUY" and (qty > 1e-12 or pending_buy_qty > 1e-12):
            action = "HOLD"
            blocked = True
            block_reason = "buy_blocked_existing_position_or_pending_buy"
        elif action == "SELL" and sellable_qty <= 1e-12:
            action = "HOLD"
            blocked = True
            block_reason = "sell_blocked_no_sellable_qty"
        elif action not in {"BUY", "SELL", "HOLD"}:
            raise ValueError(f"unsupported_decision_event_final_signal:{event.final_signal}")
        protective_exit_overrode_entry = bool(
            raw_signal == "BUY"
            and action == "SELL"
            and exit_rule in {"stop_loss", "max_holding_time"}
        )
        entry_blocked = bool(raw_signal == "BUY" and action == "HOLD" and raw_filter_would_block)
        exit_filter_suppression_prevented = bool(
            raw_signal == "SELL"
            and raw_filter_would_block
            and sellable_qty > 1e-12
            and bool(exit_evaluations)
        )
        decision_payload = _research_decision_payload(
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=parameter_values,
            strategy_name=strategy_plugin.name,
            strategy_spec=strategy_spec.as_dict(),
            strategy_spec_hash=strategy_spec.spec_hash(),
            strategy_plugin_contract=strategy_plugin.contract_payload(),
            strategy_plugin_contract_hash=strategy_plugin.contract_hash(),
            exit_policy=active_exit_policy,
            exit_policy_hash=active_exit_policy_hash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=policy,
            candle_ts=event.candle_ts,
            decision_ts=decision_boundary_ts,
            raw_signal=raw_signal,
            entry_signal=entry_signal,
            exit_signal=event.exit_signal or event.raw_signal,
            final_signal=action,
            raw_reason=raw_reason,
            blocked=bool(blocked or (raw_signal in {"BUY", "SELL"} and action == "HOLD")),
            raw_filter_would_block=raw_filter_would_block,
            entry_blocked=entry_blocked,
            protective_exit_overrode_entry=protective_exit_overrode_entry,
            exit_filter_suppression_prevented=exit_filter_suppression_prevented,
            blocked_filters=list(event.blocked_filters),
            prev_s=prev_s,
            prev_l=prev_l,
            curr_s=curr_s,
            curr_l=curr_l,
            gap_ratio=gap_ratio,
            range_ratio=range_ratio,
            regime_snapshot=regime_snapshot,
            entry_reason=block_reason,
            market_regime_decision=market_regime_decision,
            market_regime_blocked=market_regime_blocked,
            candidate_regime_blocked=candidate_regime_blocked,
            qty=qty,
            sellable_qty=sellable_qty,
            exit_rule=exit_rule,
            exit_reason=exit_reason,
            exit_evaluations=exit_evaluations,
        )
        decision_payload.update(
            {
                "decision_event_schema_version": 1,
                "strategy_decision_contract_version": strategy_plugin.decision_contract_version,
                "raw_reason": event.reason,
                "feature_snapshot": dict(event.feature_snapshot),
                "strategy_diagnostics_namespace": strategy_plugin.diagnostics_namespace,
                "strategy_diagnostics": dict(event.strategy_diagnostics),
                "strategy_behavior_payload": {
                    "strategy_name": event.strategy_name,
                    "strategy_version": event.strategy_version,
                    "raw_signal": event.raw_signal,
                    "final_signal": action,
                    "reason": event.reason,
                    "feature_snapshot": dict(event.feature_snapshot),
                    "strategy_diagnostics": dict(event.strategy_diagnostics),
                },
                "execution_intent": action.lower() if action in {"BUY", "SELL"} else "none",
                "order_intent": dict(event.order_intent) if event.order_intent is not None else None,
                "exit_intent": dict(event.exit_intent) if event.exit_intent is not None else None,
            }
        )
        retain_decision = accumulator.retain_decision()
        if retain_decision:
            decisions.append(decision_payload)
        accumulator.update_decision(decision_payload, retained=retain_decision)
        _trace_decision(run_context, decision_payload)

        if action in {"BUY", "SELL"}:
            side = action
            signal = build_signal_event(
                candle=candle,
                interval=dataset.interval,
                side=side,
                policy=timing_policy,
                feature_snapshot=dict(event.feature_snapshot),
                regime_snapshot=regime_snapshot,
            )
            reference = resolve_execution_reference(
                dataset=dataset,
                signal=signal,
                signal_index=index,
                policy=timing_policy,
                model_latency_ms=_model_latency_ms(model),
            )
            requested_notional = cash * buy_fraction if side == "BUY" else None
            requested_qty = sellable_qty if side == "SELL" else None
            if reference.fill_reference_price is None:
                fill = _failed_fill(
                    model=model,
                    signal=signal,
                    reference=reference,
                    timing_policy=timing_policy,
                    side=side,
                    fee_rate=fee_rate,
                    requested_qty=requested_qty,
                    requested_notional=requested_notional,
                )
            else:
                fill = model.simulate(
                    ExecutionRequest(
                        signal_ts=signal.signal_candle_start_ts,
                        decision_ts=signal.decision_ts,
                        side=side,
                        reference_price=float(reference.fill_reference_price),
                        requested_qty=requested_qty,
                        requested_notional=requested_notional,
                        fee_rate=fee_rate,
                        **_timing_request_fields(signal, reference, timing_policy),
                        **_depth_request_fields(
                            dataset=dataset,
                            reference=reference,
                            model=model,
                            timing_policy=timing_policy,
                        ),
                    )
                )
            warnings.extend(_execution_reference_warnings(fill))
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                _trace_execution(run_context, trades[-1])
            elif side == "BUY":
                exec_price = float(fill.avg_fill_price)
                fee = float(fill.fee)
                received_qty = float(fill.filled_qty)
                actual_spend = (exec_price * received_qty) + fee
                buy_slippage = max(0.0, (exec_price - float(fill.reference_price)) * received_qty)
                pending = _PendingFill(
                    fill=fill,
                    trade_index=len(trades),
                    side="BUY",
                    effective_ts=_fill_effective_ts(fill),
                    qty=received_qty,
                    fee=fee,
                    slippage=buy_slippage,
                    cash_delta=-actual_spend,
                    entry_regime_snapshot=regime_snapshot,
                )
                trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
                trades[-1]["entry_decision_hash"] = decision_payload.get("replay_fingerprint_hash")
                _trace_execution(run_context, trades[-1])
                if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                    mark_cash += pending.cash_delta
                    mark_qty += pending.qty
                pending_fills.append(pending)
            else:
                exec_price = float(fill.avg_fill_price)
                sell_qty = float(fill.filled_qty)
                gross = sell_qty * exec_price
                fee = float(fill.fee)
                sell_slippage = max(0.0, (float(fill.reference_price) - exec_price) * sell_qty)
                pending = _PendingFill(
                    fill=fill,
                    trade_index=len(trades),
                    side="SELL",
                    effective_ts=_fill_effective_ts(fill),
                    qty=sell_qty,
                    fee=fee,
                    slippage=sell_slippage,
                    cash_delta=gross - fee,
                    entry_regime_snapshot=entry_regime_snapshot,
                    exit_regime_snapshot=regime_snapshot,
                )
                trades.append(_pending_trade_from_fill(fill, cash=cash, asset_qty=qty))
                trades[-1].update(
                    _closed_trade_diagnostics(
                        entry_ts=entry_ts,
                        exit_ts=int(candle.ts),
                        entry_price=entry_price,
                        exit_price=exec_price,
                        entry_regime_snapshot=entry_regime_snapshot,
                        exit_regime_snapshot=regime_snapshot,
                        exit_rule=exit_rule,
                        exit_reason=exit_reason,
                        path=open_trade_path,
                        entry_decision_hash=entry_decision_hash,
                        exit_decision_hash=str(decision_payload.get("replay_fingerprint_hash") or ""),
                    )
                )
                _trace_execution(run_context, trades[-1])
                if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                    mark_cash += pending.cash_delta
                    mark_qty = max(0.0, mark_qty - pending.qty)
                pending_fills.append(pending)
            (
                cash,
                qty,
                entry_cost_basis,
                entry_regime_snapshot,
                entry_ts,
                entry_price,
                entry_decision_hash,
                open_trade_path,
                entry_fee,
                entry_slippage,
                fee_total,
                slippage_total,
            ) = _apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
                entry_ts=entry_ts,
                entry_price=entry_price,
                entry_decision_hash=entry_decision_hash,
                open_trade_path=open_trade_path,
                entry_fee=entry_fee,
                entry_slippage=entry_slippage,
                fee_total=fee_total,
                slippage_total=slippage_total,
                closed_pnls=closed_pnls,
            )

        retain_equity = accumulator.retain_equity_point()
        peak, max_drawdown = _record_equity_mark(
            equity_curve=equity_curve,
            ts=mark_boundary_ts,
            cash=mark_cash,
            qty=mark_qty,
            mark_price=candle.close,
            peak=peak,
            max_drawdown=max_drawdown,
            retain=retain_equity,
        )
        accumulator.update_equity(retained=retain_equity, ts=mark_boundary_ts, asset_qty=mark_qty)
        _trace_equity_mark(
            run_context,
            ts=mark_boundary_ts,
            equity=mark_cash + mark_qty * float(candle.close),
            cash=mark_cash,
            asset_qty=mark_qty,
        )
        accumulator.maybe_emit_heartbeat(event_number)
        accumulator.check_limits(candles_processed=event_number, trades=trades)

    last = candles[-1]
    last_mark_ts = candle_close_ts(last, interval=dataset.interval)
    (
        cash,
        qty,
        entry_cost_basis,
        entry_regime_snapshot,
        entry_ts,
        entry_price,
        entry_decision_hash,
        open_trade_path,
        entry_fee,
        entry_slippage,
        fee_total,
        slippage_total,
    ) = _apply_pending_fills(
        pending_fills=pending_fills,
        trades=trades,
        boundary_ts=last_mark_ts,
        cash=cash,
        qty=qty,
        entry_cost_basis=entry_cost_basis,
        entry_regime_snapshot=entry_regime_snapshot,
        entry_ts=entry_ts,
        entry_price=entry_price,
        entry_decision_hash=entry_decision_hash,
        open_trade_path=open_trade_path,
        entry_fee=entry_fee,
        entry_slippage=entry_slippage,
        fee_total=fee_total,
        slippage_total=slippage_total,
        closed_pnls=closed_pnls,
    )
    _mark_pending_fills_at_end(pending_fills=pending_fills, trades=trades, final_mark_ts=last_mark_ts)
    final_equity = cash + qty * float(last.close)
    retain_final_equity = accumulator.retain_equity_point()
    if retain_final_equity:
        equity_curve.append(EquityPoint(ts=last_mark_ts, equity=final_equity, cash=cash, asset_qty=qty))
    accumulator.update_equity(retained=retain_final_equity, ts=last_mark_ts, asset_qty=qty)
    _trace_equity_mark(run_context, ts=last_mark_ts, equity=final_equity, cash=cash, asset_qty=qty)
    return_pct = ((final_equity / starting_cash) - 1.0) * 100.0 if starting_cash > 0.0 else 0.0
    metrics = _metrics(
        return_pct=return_pct,
        max_drawdown_pct=max_drawdown * 100.0,
        closed_pnls=closed_pnls,
        fee_total=fee_total,
        slippage_total=slippage_total,
        parameter_stability_score=parameter_stability_score,
    )
    position_intervals, closed_trade_records, execution_records, derived_open_cost_basis = _metrics_v2_ledgers_from_trades(
        trades=trades,
    )
    coverage = (
        aggregate_regime_coverage(snapshots=regime_snapshots, trades=trades)
        if accumulator.retain_full_detail()
        else regime_coverage_accumulator.coverage(trades=trades)
    )
    performance = aggregate_regime_performance(trades=trades, coverage=coverage, start_cash=starting_cash)
    metrics_v2 = build_metrics_v2(
        starting_cash=starting_cash,
        final_cash=cash,
        final_asset_qty=qty,
        final_mark_price=last.close,
        final_open_cost_basis=entry_cost_basis if qty > 0.0 else derived_open_cost_basis,
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        execution_records=execution_records,
        **(
            {}
            if accumulator.retain_full_detail()
            else accumulator.metrics_summary_inputs(max_drawdown_pct=max_drawdown * 100.0)
        ),
    )
    if not accumulator.retain_full_detail():
        metrics_v2 = replace(
            metrics_v2,
            limitation_reasons=tuple(
                sorted(set(metrics_v2.limitation_reasons) | {"bounded_detail_equity_curve_not_retained"})
            ),
        )
    audit_trace_index = _complete_audit_trace(run_context, status="completed")
    accumulator.trade_ledger_hash_material = [_trade_hash_payload(trade) for trade in trades]
    accumulator.equity_curve_hash_material = [
        {
            "ts": int(point.ts),
            "equity": round(float(point.equity), 12),
            "cash": round(float(point.cash), 12),
            "asset_qty": round(float(point.asset_qty), 12),
        }
        for point in equity_curve
    ]
    strategy_diagnostics = accumulator.strategy_diagnostics(trades=trades)
    resource_usage = accumulator.resource_usage(candles_processed=len(decision_events))
    resource_usage["strategy_diagnostics"] = strategy_diagnostics
    return BacktestRun(
        metrics=metrics,
        metrics_v2=metrics_v2,
        trades=tuple(trades),
        candle_count=len(candles),
        warnings=tuple(warnings),
        regime_performance=performance,
        regime_coverage=coverage,
        execution_event_summary=execution_event_summary(trades),
        decisions=tuple(decisions),
        equity_curve=tuple(equity_curve),
        position_intervals=position_intervals,
        closed_trades=closed_trade_records,
        resource_usage=resource_usage,
        strategy_diagnostics=strategy_diagnostics,
        retained_detail_summary=_retained_detail_summary(
            accumulator,
            retained_regime_snapshot_count=len(regime_snapshots),
        ),
        audit_trace_index=audit_trace_index,
    )


def _noop_strategy_diagnostics(*, decision_count: int, start_index: int = 0) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "hold_decision_count": int(decision_count),
        "start_index": int(start_index),
    }
    payload["strategy_diagnostics_namespace"] = "noop_baseline"
    payload["strategy_specific_diagnostics"] = {"noop_baseline": dict(payload)}
    return payload


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


def _rolling_sma_values(values: list[float], n: int) -> list[float | None]:
    window = int(n)
    out: list[float | None] = [None] * (len(values) + 1)
    if window <= 0 or len(values) < window:
        return out
    rolling_sum = sum(values[:window])
    out[window] = rolling_sum / window
    for end in range(window + 1, len(values) + 1):
        rolling_sum += values[end - 1]
        rolling_sum -= values[end - window - 1]
        out[end] = rolling_sum / window
    return out


def _rolling_close_range_ratios(values: list[float], window: int) -> list[float]:
    window = max(1, int(window))
    out: list[float] = [0.0] * len(values)
    min_indexes: deque[int] = deque()
    max_indexes: deque[int] = deque()
    rolling_sum = 0.0
    for index, value in enumerate(values):
        value = float(value)
        rolling_sum += value
        stale_before = index - window + 1
        if index >= window:
            rolling_sum -= float(values[index - window])
        while min_indexes and min_indexes[0] < stale_before:
            min_indexes.popleft()
        while max_indexes and max_indexes[0] < stale_before:
            max_indexes.popleft()
        while min_indexes and float(values[min_indexes[-1]]) >= value:
            min_indexes.pop()
        while max_indexes and float(values[max_indexes[-1]]) <= value:
            max_indexes.pop()
        min_indexes.append(index)
        max_indexes.append(index)
        count = min(window, index + 1)
        mean = rolling_sum / count if count > 0 else 0.0
        out[index] = (
            ((float(values[max_indexes[0]]) - float(values[min_indexes[0]])) / mean)
            if mean != 0.0 and min_indexes and max_indexes
            else 0.0
        )
    return out


def _overextended_return_ratios(values: list[float], lookback: int) -> list[float]:
    lookback = max(1, int(lookback))
    out: list[float] = [0.0] * len(values)
    for index, value in enumerate(values):
        if index < lookback:
            continue
        base = float(values[index - lookback])
        out[index] = abs((float(value) - base) / base) if base != 0.0 else 0.0
    return out


def _create_exit_rules(**kwargs: Any):
    # Keep this local to avoid config -> approved_profile -> research -> strategy -> config imports.
    from bithumb_bot.strategy.exit_rules import create_exit_rules

    return create_exit_rules(**kwargs)


def _rss_mb() -> float | None:
    try:
        import resource

        rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None
    # Linux reports KiB; macOS reports bytes. AWS Linux is the reference runtime.
    if rss > 10_000_000:
        return round(rss / (1024.0 * 1024.0), 3)
    return round(rss / 1024.0, 3)


def _retained_detail_summary(
    accumulator: _BacktestAccumulator,
    *,
    retained_regime_snapshot_count: int,
) -> dict[str, object]:
    return {
        "report_detail": accumulator.report_detail,
        "decision_count": accumulator.decision_count,
        "retained_decision_count": accumulator.retained_decision_count,
        "retained_equity_point_count": accumulator.retained_equity_point_count,
        "retained_regime_snapshot_count": int(retained_regime_snapshot_count),
        "decision_hash": canonical_payload_hash(accumulator.decision_hash_material),
        **_behavior_hashes(
            decision_material=accumulator.behavior_hash_material,
            common_decision_material=accumulator.common_behavior_hash_material,
            strategy_decision_material=accumulator.strategy_behavior_hash_material,
            trade_material=accumulator.trade_ledger_hash_material,
            equity_material=accumulator.equity_curve_hash_material,
        ),
    }


def _trade_hash_payload(trade: dict[str, object]) -> dict[str, object]:
    execution = trade.get("execution") if isinstance(trade.get("execution"), dict) else {}
    return {
        "ts": trade.get("ts"),
        "side": trade.get("side"),
        "signal_ts": trade.get("signal_ts"),
        "decision_ts": trade.get("decision_ts"),
        "submit_ts_assumption": trade.get("submit_ts_assumption"),
        "fill_reference_ts": trade.get("fill_reference_ts"),
        "portfolio_effective_ts": trade.get("portfolio_effective_ts"),
        "price": trade.get("price"),
        "reference_price": execution.get("reference_price"),
        "avg_fill_price": execution.get("avg_fill_price"),
        "qty": trade.get("qty"),
        "filled_qty": execution.get("filled_qty"),
        "filled_notional": execution.get("filled_notional"),
        "remaining_qty": execution.get("remaining_qty"),
        "fill_status": execution.get("fill_status"),
        "fee": trade.get("fee"),
        "slippage_bps": execution.get("slippage_bps"),
        "cash": trade.get("cash"),
        "asset_qty": trade.get("asset_qty"),
        "pnl": trade.get("pnl"),
        "net_pnl": trade.get("net_pnl"),
        "closed_trade_pnl": trade.get("closed_trade_pnl"),
        "exit_rule": trade.get("exit_rule"),
        "exit_reason": trade.get("exit_reason"),
        "entry_decision_hash": trade.get("entry_decision_hash"),
        "exit_decision_hash": trade.get("exit_decision_hash"),
        "model_name": execution.get("model_name"),
        "model_version": execution.get("model_version"),
        "model_params_hash": execution.get("model_params_hash"),
    }


def _behavior_hashes(
    *,
    decision_material: list[dict[str, object]],
    common_decision_material: list[dict[str, object]] | None = None,
    strategy_decision_material: list[dict[str, object]] | None = None,
    trade_material: list[dict[str, object]],
    equity_material: list[dict[str, object]],
) -> dict[str, str]:
    decision_hash = canonical_payload_hash(decision_material)
    common_decision_hash = canonical_payload_hash(common_decision_material or [])
    strategy_decision_hash = canonical_payload_hash(strategy_decision_material or [])
    trade_hash = canonical_payload_hash(trade_material)
    equity_hash = canonical_payload_hash(equity_material)
    composite_hash = canonical_payload_hash(
        {
            "decision_behavior_hash": decision_hash,
            "trade_ledger_hash": trade_hash,
            "equity_curve_hash": equity_hash,
        }
    )
    composite_hash_v2 = canonical_payload_hash(
        {
            "common_decision_behavior_hash": common_decision_hash,
            "strategy_behavior_hash": strategy_decision_hash,
            "trade_ledger_hash": trade_hash,
            "equity_curve_hash": equity_hash,
        }
    )
    return {
        "decision_behavior_hash": decision_hash,
        "common_decision_behavior_hash": common_decision_hash,
        "strategy_behavior_hash": strategy_decision_hash,
        "trade_ledger_hash": trade_hash,
        "equity_curve_hash": equity_hash,
        "composite_behavior_hash": composite_hash,
        "composite_behavior_hash_v2": composite_hash_v2,
        "behavior_hash": composite_hash,
    }


def _trace_decision(context: BacktestRunContext, payload: dict[str, object]) -> None:
    sink = context.audit_trace
    if sink is None:
        return
    sink.write_decision(dict(payload))


def _trace_equity_mark(
    context: BacktestRunContext,
    *,
    ts: int,
    equity: float,
    cash: float,
    asset_qty: float,
) -> None:
    sink = context.audit_trace
    if sink is None:
        return
    sink.write_equity(
        {
            "ts": int(ts),
            "equity": float(equity),
            "cash": float(cash),
            "asset_qty": float(asset_qty),
        }
    )


def _trace_execution(context: BacktestRunContext, trade: dict[str, object]) -> None:
    sink = context.audit_trace
    if sink is None:
        return
    sink.write_execution(dict(trade))


def _complete_audit_trace(context: BacktestRunContext, *, status: str) -> dict[str, object] | None:
    sink = context.audit_trace
    if sink is None:
        return None
    return sink.complete(status=status)


def _record_equity_mark(
    *,
    equity_curve: list[EquityPoint],
    ts: int,
    cash: float,
    qty: float,
    mark_price: float,
    peak: float,
    max_drawdown: float,
    retain: bool = True,
) -> tuple[float, float]:
    equity = float(cash) + float(qty) * float(mark_price)
    if retain:
        equity_curve.append(
            EquityPoint(
                ts=int(ts),
                equity=equity,
                cash=float(cash),
                asset_qty=float(qty),
            )
        )
    peak = max(float(peak), equity)
    if peak > 0.0:
        max_drawdown = max(float(max_drawdown), (peak - equity) / peak)
    return peak, max_drawdown


def _fill_applies_to_mark(*, fill: Any, effective_ts: int, mark_boundary_ts: int) -> bool:
    if int(effective_ts) < int(mark_boundary_ts):
        return True
    if int(effective_ts) > int(mark_boundary_ts):
        return False
    return (
        bool(getattr(fill, "allow_same_candle_close_fill", False))
        and str(getattr(fill, "fill_reference_policy", "")) == "candle_close_legacy"
    )


def _apply_pending_fills(
    *,
    pending_fills: list[_PendingFill],
    trades: list[dict[str, object]],
    boundary_ts: int,
    cash: float,
    qty: float,
    entry_cost_basis: float,
    entry_regime_snapshot: dict[str, object] | None,
    entry_ts: int | None,
    entry_price: float | None,
    entry_decision_hash: str | None,
    open_trade_path: list[dict[str, float | int]],
    entry_fee: float,
    entry_slippage: float,
    fee_total: float,
    slippage_total: float,
    closed_pnls: list[float],
) -> tuple[
    float,
    float,
    float,
    dict[str, object] | None,
    int | None,
    float | None,
    str | None,
    list[dict[str, float | int]],
    float,
    float,
    float,
    float,
]:
    ready = sorted(
        [item for item in pending_fills if item.effective_ts <= int(boundary_ts)],
        key=lambda item: (item.effective_ts, item.trade_index),
    )
    for pending in ready:
        pending_fills.remove(pending)
        trade = trades[pending.trade_index]
        fill = pending.fill
        if pending.side == "BUY":
            cash += pending.cash_delta
            qty += pending.qty
            entry_cost_basis = abs(pending.cash_delta)
            entry_regime_snapshot = pending.entry_regime_snapshot
            entry_ts = int(pending.fill.signal_ts)
            entry_price = float(pending.fill.avg_fill_price or pending.fill.reference_price)
            entry_decision_hash = str(trade.get("entry_decision_hash") or "") or entry_decision_hash
            open_trade_path = []
            entry_fee = pending.fee
            entry_slippage = pending.slippage
            fee_total += pending.fee
            slippage_total += pending.slippage
            _mark_trade_applied(
                trade,
                cash=cash,
                asset_qty=qty,
                pnl=None,
                entry_regime_snapshot=entry_regime_snapshot,
                exit_regime_snapshot=None,
                net_pnl=None,
                fee_total=pending.fee,
                slippage_total=pending.slippage,
            )
        else:
            filled_fraction = pending.qty / qty if qty > 0.0 else 0.0
            pnl = pending.cash_delta - (entry_cost_basis * filled_fraction)
            cash += pending.cash_delta
            qty = max(0.0, qty - pending.qty)
            entry_cost_basis = entry_cost_basis * (1.0 - filled_fraction) if qty > 0.0 else 0.0
            fee_total += pending.fee
            slippage_total += pending.slippage
            if fill.fill_status in {"filled", "partial"}:
                closed_pnls.append(pnl)
            trade_fee_total = entry_fee + pending.fee
            trade_slippage_total = entry_slippage + pending.slippage
            _mark_trade_applied(
                trade,
                cash=cash,
                asset_qty=qty,
                pnl=pnl,
                entry_regime_snapshot=pending.entry_regime_snapshot,
                exit_regime_snapshot=pending.exit_regime_snapshot,
                net_pnl=pnl,
                fee_total=trade_fee_total,
                slippage_total=trade_slippage_total,
            )
            if qty <= 0.0:
                entry_regime_snapshot = None
                entry_ts = None
                entry_price = None
                entry_decision_hash = None
                open_trade_path = []
                entry_fee = 0.0
                entry_slippage = 0.0
    return (
        cash,
        qty,
        entry_cost_basis,
        entry_regime_snapshot,
        entry_ts,
        entry_price,
        entry_decision_hash,
        open_trade_path,
        entry_fee,
        entry_slippage,
        fee_total,
        slippage_total,
    )


def _timing_request_fields(
    signal: SignalEvent,
    reference: ExecutionReferenceEvent,
    policy: ExecutionTimingPolicy,
) -> dict[str, object]:
    fields = reference.request_fields()
    fields.update(
        {
            "signal_candle_start_ts": signal.signal_candle_start_ts,
            "signal_candle_close_ts": signal.signal_candle_close_ts,
            "signal_reference_price": signal.signal_reference_price,
            "signal_reference_source": signal.signal_reference_source,
            "allow_same_candle_close_fill": policy.allow_same_candle_close_fill,
            "quote_selection": policy.quote_selection,
            "fill_reference_policy": policy.fill_reference_policy,
            "top_of_book_source": reference.quote_source,
            "feature_snapshot": signal.feature_snapshot,
            "regime_snapshot": signal.regime_snapshot,
        }
    )
    return fields


def _depth_request_fields(
    *,
    dataset: DatasetSnapshot,
    reference: ExecutionReferenceEvent,
    model: ExecutionModel,
    timing_policy: ExecutionTimingPolicy,
) -> dict[str, object]:
    if getattr(model, "name", "") != "depth_walk":
        return {}
    target_ts = reference.fill_reference_ts
    if target_ts is None:
        target_ts = reference.submit_ts_assumption
    snapshot = dataset.first_depth_snapshot_after_or_equal(
        target_ts=int(target_ts),
        max_wait_ms=int(timing_policy.max_quote_wait_ms),
    )
    if snapshot is None:
        return {
            "orderbook_depth_snapshot": None,
            "orderbook_depth_ref": None,
            "depth_available": False,
            "depth_sufficient": False,
            "execution_liquidity_evidence_type": "l2_depth_walk_queue_unaware",
            "execution_realism_limitations": (
                "depth_snapshot_missing_for_depth_walk",
                "queue_position_unavailable",
                "market_impact_model_unavailable",
                "trade_ticks_unavailable",
                "intra_candle_path_reconstruction_unavailable",
            ),
        }
    return {
        "orderbook_depth_snapshot": snapshot,
        "orderbook_depth_ref": snapshot.depth_ref(),
        "depth_snapshot_ts": int(snapshot.ts),
        "depth_snapshot_age_ms": int(snapshot.ts) - int(target_ts),
        "depth_available": True,
        "execution_liquidity_evidence_type": "l2_depth_walk_queue_unaware",
        "execution_realism_limitations": (
            "queue_position_unavailable",
            "market_impact_model_unavailable",
            "trade_ticks_unavailable",
            "intra_candle_path_reconstruction_unavailable",
        ),
    }


def _feature_snapshot(
    *,
    short_sma: float,
    long_sma: float,
    gap_ratio: float,
    range_ratio: float,
    index: int,
) -> dict[str, object]:
    return {
        "short_sma": float(short_sma),
        "long_sma": float(long_sma),
        "gap_ratio": float(gap_ratio),
        "range_ratio": float(range_ratio),
        "candle_index": int(index),
    }


def _research_decision_payload(
    *,
    dataset: DatasetSnapshot,
    dataset_content_hash: str,
    parameter_values: dict[str, Any],
    strategy_name: str,
    strategy_spec: dict[str, Any],
    strategy_spec_hash: str,
    strategy_plugin_contract: dict[str, Any],
    strategy_plugin_contract_hash: str,
    exit_policy: dict[str, Any],
    exit_policy_hash: str,
    fee_rate: float,
    slippage_bps: float,
    timing_policy: ExecutionTimingPolicy,
    portfolio_policy: PortfolioPolicy,
    candle_ts: int,
    decision_ts: int,
    raw_signal: str,
    entry_signal: str,
    exit_signal: str,
    final_signal: str,
    raw_reason: str,
    blocked: bool,
    raw_filter_would_block: bool,
    entry_blocked: bool,
    protective_exit_overrode_entry: bool,
    exit_filter_suppression_prevented: bool,
    blocked_filters: list[str],
    prev_s: float,
    prev_l: float,
    curr_s: float,
    curr_l: float,
    gap_ratio: float,
    range_ratio: float,
    regime_snapshot: dict[str, object],
    entry_reason: str,
    market_regime_decision: dict[str, object],
    market_regime_blocked: bool,
    candidate_regime_blocked: bool,
    qty: float,
    sellable_qty: float,
    exit_rule: str = "",
    exit_reason: str = "",
    exit_evaluations: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    from bithumb_bot.research.lot_native_simulation import lot_native_model_from_quantities

    order_rules = {
        "source": "research_execution_model",
        "fee_rate": float(fee_rate),
        "slippage_bps": float(slippage_bps),
        "portfolio_policy_hash": portfolio_policy.policy_hash(),
        "position_sizing": portfolio_policy.position_sizing.as_dict(),
        "sizing": (
            f"cash_fraction_{portfolio_policy.position_sizing.buy_fraction:g}"
            "_or_full_sellable_qty"
        ),
    }
    fee_authority_hash = canonical_payload_hash({"source": "research_manifest", "fee_rate": float(fee_rate)})
    order_rules_hash = canonical_payload_hash(order_rules)
    fee_model_hash = canonical_payload_hash({"fee_rate": float(fee_rate)})
    slippage_model_hash = canonical_payload_hash({"slippage_bps": float(slippage_bps)})
    execution_timing_policy_hash = canonical_payload_hash(timing_policy.as_dict())
    portfolio_policy_hash = portfolio_policy.policy_hash()
    decision_contract_hash = canonical_payload_hash(
        {
            "dataset_content_hash": dataset_content_hash,
            "parameter_values": parameter_values,
            "candle_ts": int(candle_ts),
            "portfolio_policy_hash": portfolio_policy_hash,
            "execution_timing_policy_hash": execution_timing_policy_hash,
            "fee_model_hash": fee_model_hash,
            "slippage_model_hash": slippage_model_hash,
            "strategy_spec_hash": strategy_spec_hash,
            "exit_policy_hash": exit_policy_hash,
        }
    )
    lot_native_authority = lot_native_model_from_quantities(
        qty=float(qty),
        sellable_qty=float(sellable_qty),
    ).authority_snapshot(
        order_rules_hash=order_rules_hash,
        fee_authority_hash=fee_authority_hash,
    )
    flat_no_position = lot_native_authority.state_class == "flat_no_dust_no_position"
    position_state_hash = lot_native_authority.position_state_hash
    if lot_native_authority.unsupported_reason:
        legacy_authority = research_position_authority_snapshot(
            qty=float(qty),
            sellable_qty=float(sellable_qty),
            order_rules_hash=order_rules_hash,
            fee_authority_hash=fee_authority_hash,
            position_state_hash=canonical_payload_hash(
                {
                    "research_position_model": "cash_qty_simulation_v1",
                    "unsupported_reason": "research_model_lacks_lot_native_authority",
                    "qty": float(qty),
                    "sellable_qty": float(sellable_qty),
                }
            ),
        )
        position_state_hash = legacy_authority.position_state_hash
    else:
        legacy_authority = None
    payload = {
        "strategy_name": strategy_name,
        "strategy_spec": strategy_spec,
        "strategy_spec_hash": strategy_spec_hash,
        "strategy_plugin_contract": strategy_plugin_contract,
        "strategy_plugin_contract_hash": strategy_plugin_contract_hash,
        "exit_policy": exit_policy,
        "exit_policy_hash": exit_policy_hash,
        "market": dataset.market,
        "interval": dataset.interval,
        "signal_timestamp": str(candle_ts),
        "candle_ts": int(candle_ts),
        "through_ts_ms": int(candle_ts),
        "candle_basis": "research_closed_candle",
        "decision_ts": int(decision_ts),
        "raw_signal": raw_signal,
        "entry_signal": entry_signal,
        "exit_signal": exit_signal,
        "final_signal": final_signal,
        "side": final_signal,
        "entry_reason": str(entry_reason),
        "blocked": bool(blocked),
        "raw_filter_would_block": bool(raw_filter_would_block),
        "entry_blocked": bool(entry_blocked),
        "protective_exit_overrode_entry": bool(protective_exit_overrode_entry),
        # Legacy compatibility alias: for SELL this means filters would have
        # blocked the raw signal if entry filters governed exits.
        "entry_filter_blocked": bool(raw_filter_would_block),
        "exit_filter_suppression_prevented": bool(exit_filter_suppression_prevented),
        "entry_blocked_filters": tuple(blocked_filters),
        "block_reason": str(entry_reason) if blocked else "",
        "blocked_filters": tuple(blocked_filters),
        "sellable_qty": float(sellable_qty),
        "prev_s": float(prev_s),
        "prev_l": float(prev_l),
        "curr_s": float(curr_s),
        "curr_l": float(curr_l),
        "feature_hash": canonical_payload_hash(
            {
                "prev_s": prev_s,
                "prev_l": prev_l,
                "curr_s": curr_s,
                "curr_l": curr_l,
                "gap_ratio": gap_ratio,
                "range_ratio": range_ratio,
            }
        ),
        "gap_ratio": float(gap_ratio),
        "range_ratio": float(range_ratio),
        "expected_edge_ratio": float(gap_ratio),
        "required_edge_ratio": float(
            parameter_values.get(
                "SMA_FILTER_GAP_MIN_RATIO",
                parameter_values.get("strategy_min_expected_edge_ratio", 0.0),
            )
        ),
        "fee_authority_hash": fee_authority_hash,
        "fee_model_hash": fee_model_hash,
        "slippage_model_hash": slippage_model_hash,
        "order_rules_hash": order_rules_hash,
        "market_regime": str(regime_snapshot.get("composite_regime") or ""),
        "current_market_regime_snapshot": regime_snapshot,
        "current_regime": str(market_regime_decision.get("current_regime") or regime_snapshot.get("composite_regime") or ""),
        "regime_decision": market_regime_decision.get("regime_decision") or "not_configured",
        "regime_block_reason": market_regime_decision.get("regime_block_reason") or "",
        "market_regime_blocked": bool(market_regime_blocked),
        "candidate_regime_blocked": bool(candidate_regime_blocked),
        "position_state_hash": position_state_hash,
        "entry_allowed": bool(lot_native_authority.entry_allowed),
        "exit_allowed": bool(lot_native_authority.exit_allowed),
        "dust_state": "flat" if flat_no_position else (
            "research_not_modeled" if lot_native_authority.unsupported_reason else "no_dust"
        ),
        "effective_flat": bool(lot_native_authority.entry_allowed),
        "normalized_exposure_active": bool(lot_native_authority.open_lot_count > 0),
        "exit_rule": str(exit_rule or ""),
        "exit_reason": str(exit_reason or ""),
        "exit_evaluations_hash": canonical_payload_hash(
            {
                "raw_signal": raw_signal,
                "final_signal": final_signal,
                "position_qty": float(qty),
                "exit_rule": str(exit_rule or ""),
                "exit_reason": str(exit_reason or ""),
                "exit_evaluations": exit_evaluations or [],
                "exit_policy_hash": exit_policy_hash,
            }
        ),
        "exit_evaluations": exit_evaluations or [],
        "portfolio_policy_hash": portfolio_policy_hash,
        "execution_timing_policy_hash": execution_timing_policy_hash,
        "decision_contract_hash": decision_contract_hash,
        "replay_fingerprint_hash": decision_contract_hash,
    }
    payload["position_authority"] = (
        legacy_authority.as_dict() if legacy_authority is not None else lot_native_authority.as_dict()
    )
    return payload


def _model_latency_ms(model: ExecutionModel) -> int:
    try:
        return int(getattr(model, "latency_ms", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _failed_fill(
    *,
    model: ExecutionModel,
    signal: SignalEvent,
    reference: ExecutionReferenceEvent,
    timing_policy: ExecutionTimingPolicy,
    side: str,
    fee_rate: float,
    requested_qty: float | None = None,
    requested_notional: float | None = None,
) -> ExecutionFill:
    request_qty = float(requested_qty or 0.0)
    if request_qty <= 0.0 and requested_notional is not None and signal.signal_reference_price > 0:
        request_qty = float(requested_notional) / float(signal.signal_reference_price)
    return ExecutionFill(
        signal_ts=signal.signal_candle_start_ts,
        decision_ts=signal.decision_ts,
        submit_ts_assumption=reference.submit_ts_assumption,
        side=str(side).upper(),
        order_type="market",
        reference_price=float(signal.signal_reference_price),
        fill_reference_ts=reference.fill_reference_ts,
        fill_reference_price=reference.fill_reference_price,
        fill_reference_source=reference.fill_reference_source,
        signal_candle_start_ts=signal.signal_candle_start_ts,
        signal_candle_close_ts=signal.signal_candle_close_ts,
        signal_reference_price=signal.signal_reference_price,
        signal_reference_source=signal.signal_reference_source,
        quote_ts=reference.quote_ts,
        quote_age_ms=reference.quote_age_ms,
        quote_source=reference.quote_source,
        requested_qty=request_qty,
        filled_qty=0.0,
        remaining_qty=request_qty,
        avg_fill_price=None,
        fee=0.0,
        slippage_bps=0.0,
        latency_ms=_model_latency_ms(model),
        fill_status=_failed_fill_status(reference.failure_reason),
        model_name=getattr(model, "name", "unknown"),
        model_version=getattr(model, "version", "unknown"),
        model_params_hash=model_params_hash(model.params_payload()),
        best_bid=reference.best_bid,
        best_ask=reference.best_ask,
        spread_bps=reference.spread_bps,
        requested_notional=requested_notional,
        filled_notional=0.0,
        depth_available=False,
        depth_sufficient=False,
        queue_position_mode="unavailable",
        market_impact_mode="unavailable",
        execution_liquidity_evidence_type="top_of_book_quote_only" if reference.quote_ts is not None else "candle_only",
        execution_realism_limitations=(
            "full_orderbook_depth_unavailable",
            "queue_position_unavailable",
            "market_impact_model_unavailable",
        ),
        execution_reality_level=reference.execution_reality_level,
        allow_same_candle_close_fill=timing_policy.allow_same_candle_close_fill,
        quote_selection=timing_policy.quote_selection,
        fill_reference_policy=timing_policy.fill_reference_policy,
        top_of_book_source=reference.quote_source,
        top_of_book_is_full_depth=reference.top_of_book_is_full_depth,
        execution_reference_failure_reason=reference.failure_reason,
        latency_applied_to_reference=reference.latency_applied_to_reference,
        latency_applied_to_submit_ts=reference.latency_applied_to_submit_ts,
        latency_applied_to_fill_reference=reference.latency_applied_to_fill_reference,
        latency_reference_policy_warning=reference.latency_reference_policy_warning,
        feature_snapshot=signal.feature_snapshot,
        regime_snapshot=signal.regime_snapshot,
        intra_candle_policy=reference.intra_candle_policy,
    )


def _failed_fill_status(reason: str | None) -> str:
    if reason == "missing_quote_skipped":
        return "skipped"
    if reason == "missing_quote_warning":
        return "skipped_with_warning"
    return "failed"


def _trade(
    ts: int,
    side: str,
    price: float,
    qty: float,
    fee: float,
    cash: float,
    asset_qty: float,
    pnl: float | None,
    *,
    entry_regime_snapshot: dict[str, object] | None = None,
    exit_regime_snapshot: dict[str, object] | None = None,
    net_pnl: float | None = None,
    fee_total: float | None = None,
    slippage_total: float | None = None,
) -> dict[str, object]:
    entry_regime = None
    if entry_regime_snapshot is not None:
        entry_regime = entry_regime_snapshot.get("composite_regime")
    exit_regime = None
    if exit_regime_snapshot is not None:
        exit_regime = exit_regime_snapshot.get("composite_regime")
    return {
        "ts": int(ts),
        "side": side,
        "price": float(price),
        "qty": float(qty),
        "fee": float(fee),
        "cash": float(cash),
        "asset_qty": float(asset_qty),
        "closed_trade_pnl": pnl,
        "net_pnl": net_pnl,
        "fee_total": fee_total,
        "slippage_total": slippage_total,
        "entry_regime": entry_regime,
        "exit_regime": exit_regime,
        "entry_regime_snapshot": entry_regime_snapshot,
        "exit_regime_snapshot": exit_regime_snapshot,
    }


def _trade_from_fill(
    fill: Any,
    *,
    cash: float,
    asset_qty: float,
    pnl: float | None,
    entry_regime_snapshot: dict[str, object] | None = None,
    exit_regime_snapshot: dict[str, object] | None = None,
    net_pnl: float | None = None,
    fee_total: float | None = None,
    slippage_total: float | None = None,
) -> dict[str, object]:
    trade = _trade(
        fill.signal_ts,
        fill.side,
        float(fill.avg_fill_price) if fill.avg_fill_price is not None else float(fill.reference_price),
        float(fill.filled_qty),
        float(fill.fee),
        cash,
        asset_qty,
        pnl,
        entry_regime_snapshot=entry_regime_snapshot,
        exit_regime_snapshot=exit_regime_snapshot,
        net_pnl=net_pnl,
        fee_total=fee_total,
        slippage_total=slippage_total,
    )
    trade["signal_ts"] = fill.signal_ts
    trade["decision_ts"] = fill.decision_ts
    trade["submit_ts_assumption"] = fill.submit_ts_assumption
    trade["fill_ts"] = fill.fill_reference_ts
    trade["fill_reference_ts"] = fill.fill_reference_ts
    trade["event_ts_role"] = "signal_ts_legacy"
    trade["execution"] = fill.as_dict()
    _annotate_execution_record_type(trade, fill)
    trade["portfolio_effective_ts"] = fill.fill_reference_ts
    _annotate_portfolio_application(trade, fill, portfolio_applied=bool(trade["is_execution_filled"]))
    return trade


def _pending_trade_from_fill(fill: Any, *, cash: float, asset_qty: float) -> dict[str, object]:
    trade = _trade_from_fill(fill, cash=cash, asset_qty=asset_qty, pnl=None)
    trade["portfolio_effective_ts"] = _fill_effective_ts(fill)
    _annotate_portfolio_application(trade, fill, portfolio_applied=False)
    return trade


def _mark_trade_applied(
    trade: dict[str, object],
    *,
    cash: float,
    asset_qty: float,
    pnl: float | None,
    entry_regime_snapshot: dict[str, object] | None,
    exit_regime_snapshot: dict[str, object] | None,
    net_pnl: float | None,
    fee_total: float | None,
    slippage_total: float | None,
) -> None:
    entry_regime = entry_regime_snapshot.get("composite_regime") if entry_regime_snapshot is not None else None
    exit_regime = exit_regime_snapshot.get("composite_regime") if exit_regime_snapshot is not None else None
    trade.update(
        {
            "cash": float(cash),
            "asset_qty": float(asset_qty),
            "closed_trade_pnl": pnl,
            "net_pnl": net_pnl,
            "fee_total": fee_total,
            "slippage_total": slippage_total,
            "entry_regime": entry_regime,
            "exit_regime": exit_regime,
            "entry_regime_snapshot": entry_regime_snapshot,
            "exit_regime_snapshot": exit_regime_snapshot,
        }
    )
    _annotate_portfolio_application(trade, trade.get("execution") or {}, portfolio_applied=True)


def _fill_effective_ts(fill: Any) -> int:
    if fill.fill_reference_ts is not None:
        return int(fill.fill_reference_ts)
    return int(fill.submit_ts_assumption)


def _annotate_execution_record_type(trade: dict[str, object], fill: Any) -> None:
    status = str(getattr(fill, "fill_status", ""))
    is_filled = float(getattr(fill, "filled_qty", 0.0) or 0.0) > 0.0 and status in {"filled", "partial"}
    is_skipped = status in {"skipped", "skipped_with_warning"}
    is_failed = status == "failed"
    if is_skipped:
        record_type = "skipped_execution"
    elif is_failed:
        record_type = "failed_execution"
    elif is_filled:
        record_type = "portfolio_trade"
    else:
        record_type = "execution_attempt"
    trade["record_type"] = record_type
    trade["is_execution_attempt"] = True
    trade["is_execution_filled"] = is_filled
    trade["is_filled_trade"] = is_filled
    trade["is_skipped_execution"] = is_skipped
    trade["is_failed_execution"] = is_failed
    trade["is_portfolio_applied_trade"] = is_filled
    trade["is_effective_trade"] = is_filled
    trade["portfolio_application_status"] = "applied" if is_filled else "not_applicable"


def _annotate_portfolio_application(
    trade: dict[str, object],
    fill: Any,
    *,
    portfolio_applied: bool,
) -> None:
    if isinstance(fill, dict):
        status = str(fill.get("fill_status") or "")
        filled_qty = float(fill.get("filled_qty") or 0.0)
    else:
        status = str(getattr(fill, "fill_status", ""))
        filled_qty = float(getattr(fill, "filled_qty", 0.0) or 0.0)
    is_execution_filled = filled_qty > 0.0 and status in {"filled", "partial"}
    is_skipped = status in {"skipped", "skipped_with_warning"}
    is_failed = status == "failed"
    is_portfolio_trade = bool(is_execution_filled and portfolio_applied)
    if is_portfolio_trade:
        record_type = "portfolio_trade"
        application_status = "applied"
    elif is_execution_filled:
        record_type = "pending_execution"
        application_status = "pending"
    elif is_skipped:
        record_type = "skipped_execution"
        application_status = "not_applicable"
    elif is_failed:
        record_type = "failed_execution"
        application_status = "not_applicable"
    else:
        record_type = "execution_attempt"
        application_status = "not_applicable"
    trade.update(
        {
            "record_type": record_type,
            "is_execution_attempt": True,
            "is_execution_filled": is_execution_filled,
            "is_portfolio_applied_trade": is_portfolio_trade,
            "is_effective_trade": is_portfolio_trade,
            "is_filled_trade": is_portfolio_trade,
            "is_skipped_execution": is_skipped,
            "is_failed_execution": is_failed,
            "portfolio_applied": is_portfolio_trade,
            "portfolio_application_status": application_status,
        }
    )


def _mark_pending_fills_at_end(
    *,
    pending_fills: list[_PendingFill],
    trades: list[dict[str, object]],
    final_mark_ts: int,
) -> None:
    for pending in pending_fills:
        trade = trades[pending.trade_index]
        trade["pending_execution_at_end"] = True
        trade["pending_execution_after_dataset_end"] = int(pending.effective_ts) > int(final_mark_ts)
        trade["dataset_final_mark_ts"] = int(final_mark_ts)


def execution_event_summary(trades: Any) -> dict[str, object]:
    rows = [trade for trade in trades if isinstance(trade, dict)]
    attempts = [trade for trade in rows if bool(trade.get("is_execution_attempt"))]
    execution_filled = [trade for trade in rows if bool(trade.get("is_execution_filled"))]
    portfolio_applied = [trade for trade in rows if bool(trade.get("is_portfolio_applied_trade"))]
    pending = [
        trade
        for trade in rows
        if bool(trade.get("is_execution_filled")) and not bool(trade.get("is_portfolio_applied_trade"))
    ]
    skipped = [trade for trade in rows if bool(trade.get("is_skipped_execution"))]
    failed = [trade for trade in rows if bool(trade.get("is_failed_execution"))]
    closed = [
        trade
        for trade in portfolio_applied
        if str(trade.get("side") or "").upper() == "SELL"
    ]
    pending_at_end = [trade for trade in pending if bool(trade.get("pending_execution_at_end"))]
    pending_after_end = [trade for trade in pending if bool(trade.get("pending_execution_after_dataset_end"))]
    return {
        "execution_attempt_count": len(attempts),
        "execution_filled_count": len(execution_filled),
        "filled_execution_count": len(execution_filled),
        "portfolio_applied_trade_count": len(portfolio_applied),
        "pending_execution_count": len(pending),
        "skipped_execution_count": len(skipped),
        "failed_execution_count": len(failed),
        "closed_trade_count": len(closed),
        "pending_execution_at_end_count": len(pending_at_end),
        "pending_execution_after_dataset_end_count": len(pending_after_end),
        "execution_event_timeline_incomplete": bool(pending_after_end),
    }


def _strategy_diagnostics_from_trades(
    *,
    namespace: str = "sma_with_filter",
    trades: list[dict[str, object]],
    raw_sell_filter_blocked_while_in_position_count: int,
    raw_buy_filter_blocked_count: int,
    opposite_cross_triggered_count: int,
    opposite_cross_deferred_small_loss_count: int,
    opposite_cross_deferred_small_gain_count: int,
    stop_loss_exit_count: int,
    max_holding_exit_count: int,
    exit_filter_suppression_prevented_count: int,
) -> dict[str, object]:
    closed = [
        trade
        for trade in trades
        if isinstance(trade, dict)
        and bool(trade.get("is_portfolio_applied_trade"))
        and str(trade.get("side") or "").upper() == "SELL"
    ]
    exit_reason_distribution: dict[str, int] = {}
    mae_pct_by_trade: list[float] = []
    mfe_pct_by_trade: list[float] = []
    loss_holding_minutes: list[float] = []
    for trade in closed:
        reason = str(trade.get("exit_rule") or trade.get("exit_reason") or "unknown")
        exit_reason_distribution[reason] = exit_reason_distribution.get(reason, 0) + 1
        if trade.get("mae_pct") is not None:
            mae_pct_by_trade.append(float(trade.get("mae_pct") or 0.0))
        if trade.get("mfe_pct") is not None:
            mfe_pct_by_trade.append(float(trade.get("mfe_pct") or 0.0))
        pnl = trade.get("net_pnl") if trade.get("net_pnl") is not None else trade.get("closed_trade_pnl")
        if pnl is not None and float(pnl) < 0.0 and trade.get("holding_minutes") is not None:
            loss_holding_minutes.append(float(trade.get("holding_minutes") or 0.0))
    payload = {
        "schema_version": 1,
        "raw_sell_filter_blocked_while_in_position_count": int(raw_sell_filter_blocked_while_in_position_count),
        "raw_buy_filter_blocked_count": int(raw_buy_filter_blocked_count),
        "opposite_cross_triggered_count": int(opposite_cross_triggered_count),
        "opposite_cross_deferred_small_loss_count": int(opposite_cross_deferred_small_loss_count),
        "opposite_cross_deferred_small_gain_count": int(opposite_cross_deferred_small_gain_count),
        "stop_loss_exit_count": int(stop_loss_exit_count),
        "max_holding_exit_count": int(max_holding_exit_count),
        "exit_filter_suppression_prevented_count": int(exit_filter_suppression_prevented_count),
        "exit_reason_distribution": dict(sorted(exit_reason_distribution.items())),
        "mae_pct_by_trade": mae_pct_by_trade,
        "mfe_pct_by_trade": mfe_pct_by_trade,
        "p95_mae_pct": _percentile(mae_pct_by_trade, 0.95),
        "p05_mae_pct": _percentile(mae_pct_by_trade, 0.05),
        "p95_adverse_excursion_abs_pct": _percentile(
            [abs(value) for value in mae_pct_by_trade],
            0.95,
        ),
        "worst_trade_mae_pct": min(mae_pct_by_trade) if mae_pct_by_trade else None,
        "avg_loss_holding_minutes": (
            sum(loss_holding_minutes) / len(loss_holding_minutes)
            if loss_holding_minutes
            else None
        ),
    }
    strategy_specific = dict(payload)
    payload["strategy_diagnostics_namespace"] = str(namespace)
    payload["strategy_specific_diagnostics"] = {str(namespace): strategy_specific}
    return payload


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * float(percentile)))))
    return ordered[index]


def empty_execution_event_summary() -> dict[str, object]:
    return execution_event_summary(())


def _empty_metrics_v2(*, starting_cash: float | None = None, initial_position_qty: float = 0.0) -> MetricContractV2:
    cash = float(starting_cash if starting_cash is not None else legacy_research_portfolio_policy().starting_cash_krw)
    return build_metrics_v2(
        starting_cash=cash,
        final_cash=cash,
        final_asset_qty=float(initial_position_qty),
        final_mark_price=0.0,
        equity_curve=(),
        position_intervals=(),
        closed_trades=(),
        execution_records=(),
    )


def _metrics_v2_ledgers_from_trades(
    *,
    trades: list[dict[str, object]],
) -> tuple[tuple[PositionInterval, ...], tuple[ClosedTradeRecord, ...], tuple[ExecutionRecord, ...], float]:
    execution_records = tuple(_execution_record_from_trade(trade) for trade in trades if isinstance(trade, dict))
    applied = sorted(
        [trade for trade in trades if isinstance(trade, dict) and bool(trade.get("is_portfolio_applied_trade"))],
        key=lambda trade: (
            int(trade.get("portfolio_effective_ts") or trade.get("fill_ts") or trade.get("ts") or 0),
            str(trade.get("side") or ""),
        ),
    )
    intervals: list[PositionInterval] = []
    closed: list[ClosedTradeRecord] = []
    open_ts: int | None = None
    open_qty = 0.0
    open_cost_basis = 0.0
    for trade in applied:
        side = str(trade.get("side") or "").upper()
        ts = int(trade.get("portfolio_effective_ts") or trade.get("fill_ts") or trade.get("ts") or 0)
        qty = float(trade.get("qty") or 0.0)
        price = float(trade.get("price") or 0.0)
        fee = float(trade.get("fee") or 0.0)
        if side == "BUY" and qty > 0.0:
            if open_qty <= 1e-12:
                open_ts = ts
                open_cost_basis = 0.0
            open_qty += qty
            open_cost_basis += qty * price + fee
        elif side == "SELL" and qty > 0.0:
            basis_fraction = min(1.0, qty / open_qty) if open_qty > 1e-12 else 0.0
            allocated_basis = open_cost_basis * basis_fraction
            pnl = trade.get("net_pnl") if trade.get("net_pnl") is not None else trade.get("closed_trade_pnl")
            if pnl is not None:
                closed.append(
                    ClosedTradeRecord(
                        entry_ts=open_ts,
                        exit_ts=ts,
                        entry_notional=allocated_basis if allocated_basis > 0.0 else None,
                        net_pnl=float(pnl),
                        return_pct=(float(pnl) / allocated_basis * 100.0) if allocated_basis > 0.0 else None,
                        holding_minutes=(
                            float(trade.get("holding_minutes"))
                            if trade.get("holding_minutes") is not None
                            else None
                        ),
                        entry_price=(
                            float(trade.get("entry_price"))
                            if trade.get("entry_price") is not None
                            else None
                        ),
                        exit_price=(
                            float(trade.get("exit_price"))
                            if trade.get("exit_price") is not None
                            else None
                        ),
                        entry_regime=(
                            str(trade.get("entry_regime"))
                            if trade.get("entry_regime") is not None
                            else None
                        ),
                        exit_regime=(
                            str(trade.get("exit_regime"))
                            if trade.get("exit_regime") is not None
                            else None
                        ),
                        exit_rule=(
                            str(trade.get("exit_rule")) if trade.get("exit_rule") is not None else None
                        ),
                        exit_reason=(
                            str(trade.get("exit_reason")) if trade.get("exit_reason") is not None else None
                        ),
                        mae=float(trade.get("mae")) if trade.get("mae") is not None else None,
                        mfe=float(trade.get("mfe")) if trade.get("mfe") is not None else None,
                        mae_pct=float(trade.get("mae_pct")) if trade.get("mae_pct") is not None else None,
                        mfe_pct=float(trade.get("mfe_pct")) if trade.get("mfe_pct") is not None else None,
                        bars_to_mae=(
                            int(trade.get("bars_to_mae"))
                            if trade.get("bars_to_mae") is not None
                            else None
                        ),
                        bars_to_mfe=(
                            int(trade.get("bars_to_mfe"))
                            if trade.get("bars_to_mfe") is not None
                            else None
                        ),
                        unrealized_pnl_path_summary=(
                            dict(trade.get("unrealized_pnl_path_summary"))
                            if isinstance(trade.get("unrealized_pnl_path_summary"), dict)
                            else None
                        ),
                        entry_decision_hash=(
                            str(trade.get("entry_decision_hash"))
                            if trade.get("entry_decision_hash") is not None
                            else None
                        ),
                        exit_decision_hash=(
                            str(trade.get("exit_decision_hash"))
                            if trade.get("exit_decision_hash") is not None
                            else None
                        ),
                        fee_total=float(trade.get("fee_total") or fee),
                        slippage_total=float(trade.get("slippage_total") or 0.0),
                    )
                )
            open_qty = max(0.0, open_qty - qty)
            open_cost_basis = max(0.0, open_cost_basis - allocated_basis)
            if open_qty <= 1e-12:
                if open_ts is not None:
                    intervals.append(PositionInterval(open_ts=open_ts, close_ts=ts))
                open_ts = None
                open_cost_basis = 0.0
    if open_ts is not None:
        intervals.append(PositionInterval(open_ts=open_ts, close_ts=None))
    return tuple(intervals), tuple(closed), execution_records, open_cost_basis


def _execution_record_from_trade(trade: dict[str, object]) -> ExecutionRecord:
    execution = trade.get("execution") if isinstance(trade.get("execution"), dict) else {}
    assert isinstance(execution, dict)
    return ExecutionRecord(
        side=str(trade.get("side") or execution.get("side") or ""),
        status=str(execution.get("fill_status") or ""),
        filled_qty=float(execution.get("filled_qty") or trade.get("qty") or 0.0),
        price=(
            float(execution.get("avg_fill_price"))
            if execution.get("avg_fill_price") is not None
            else (float(trade.get("price")) if trade.get("price") is not None else None)
        ),
        fee=float(execution.get("fee") or trade.get("fee") or 0.0),
        slippage=float(_trade_execution_slippage(trade)),
        quote_age_ms=int(execution["quote_age_ms"]) if execution.get("quote_age_ms") is not None else None,
    )


def _trade_execution_slippage(trade: dict[str, object]) -> float:
    execution = trade.get("execution") if isinstance(trade.get("execution"), dict) else {}
    assert isinstance(execution, dict)
    status = str(execution.get("fill_status") or "")
    if status not in {"filled", "partial"}:
        return 0.0
    side = str(execution.get("side") or trade.get("side") or "").upper()
    qty = float(execution.get("filled_qty") or trade.get("qty") or 0.0)
    avg_price = execution.get("avg_fill_price")
    ref_price = execution.get("reference_price")
    if avg_price is None or ref_price is None or qty <= 0.0:
        return 0.0
    if side == "BUY":
        return max(0.0, (float(avg_price) - float(ref_price)) * qty)
    if side == "SELL":
        return max(0.0, (float(ref_price) - float(avg_price)) * qty)
    return 0.0


def _closed_trade_diagnostics(
    *,
    entry_ts: int | None,
    exit_ts: int,
    entry_price: float | None,
    exit_price: float,
    entry_regime_snapshot: dict[str, object] | None,
    exit_regime_snapshot: dict[str, object] | None,
    exit_rule: str,
    exit_reason: str,
    path: list[dict[str, float | int]],
    entry_decision_hash: str | None,
    exit_decision_hash: str,
) -> dict[str, object]:
    points = list(path)
    mae_point = min(points, key=lambda item: float(item.get("unrealized_pnl", 0.0)), default=None)
    mfe_point = max(points, key=lambda item: float(item.get("unrealized_pnl", 0.0)), default=None)
    entry_ts_int = int(entry_ts) if entry_ts is not None else None
    holding_minutes = (
        max(0.0, (int(exit_ts) - int(entry_ts_int)) / 60_000.0)
        if entry_ts_int is not None
        else None
    )
    return {
        "entry_ts": entry_ts_int,
        "exit_ts": int(exit_ts),
        "holding_minutes": holding_minutes,
        "entry_price": float(entry_price) if entry_price is not None else None,
        "exit_price": float(exit_price),
        "entry_regime": _regime_snapshot_value(entry_regime_snapshot, "composite_regime"),
        "exit_regime": _regime_snapshot_value(exit_regime_snapshot, "composite_regime"),
        "exit_rule": str(exit_rule or "unknown"),
        "exit_reason": str(exit_reason or "unknown"),
        "mae": float(mae_point.get("unrealized_pnl", 0.0)) if mae_point else 0.0,
        "mfe": float(mfe_point.get("unrealized_pnl", 0.0)) if mfe_point else 0.0,
        "mae_pct": float(mae_point.get("unrealized_pnl_pct", 0.0)) if mae_point else 0.0,
        "mfe_pct": float(mfe_point.get("unrealized_pnl_pct", 0.0)) if mfe_point else 0.0,
        "bars_to_mae": points.index(mae_point) if mae_point in points else None,
        "bars_to_mfe": points.index(mfe_point) if mfe_point in points else None,
        "unrealized_pnl_path_summary": {
            "point_count": len(points),
            "first": points[0] if points else None,
            "last": points[-1] if points else None,
            "mae_point": mae_point,
            "mfe_point": mfe_point,
        },
        "entry_decision_hash": str(entry_decision_hash or ""),
        "exit_decision_hash": str(exit_decision_hash or ""),
    }


def _execution_reference_warnings(fill: Any) -> list[str]:
    warnings: list[str] = []
    if getattr(fill, "execution_reference_failure_reason", None) == "missing_quote_warning":
        warnings.append("missing_quote_warning")
    if getattr(fill, "latency_reference_policy_warning", None):
        warnings.append(str(fill.latency_reference_policy_warning))
    return warnings


def _empty_metrics(parameter_stability_score: float | None) -> ResearchMetrics:
    return ResearchMetrics(
        return_pct=0.0,
        max_drawdown_pct=0.0,
        profit_factor=None,
        trade_count=0,
        win_rate=0.0,
        avg_win=None,
        avg_loss=None,
        fee_total=0.0,
        slippage_total=0.0,
        max_consecutive_losses=0,
        single_trade_dependency_score=None,
        parameter_stability_score=parameter_stability_score,
    )


def _metrics(
    *,
    return_pct: float,
    max_drawdown_pct: float,
    closed_pnls: list[float],
    fee_total: float,
    slippage_total: float,
    parameter_stability_score: float | None,
) -> ResearchMetrics:
    wins = [pnl for pnl in closed_pnls if pnl > 0.0]
    losses = [pnl for pnl in closed_pnls if pnl < 0.0]
    profit_factor_unbounded = bool(wins and not losses)
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else None
    largest_abs = max((abs(pnl) for pnl in closed_pnls), default=0.0)
    total_abs = sum(abs(pnl) for pnl in closed_pnls)
    return ResearchMetrics(
        return_pct=float(return_pct),
        max_drawdown_pct=float(max_drawdown_pct),
        profit_factor=profit_factor,
        trade_count=len(closed_pnls),
        win_rate=(len(wins) / len(closed_pnls)) if closed_pnls else 0.0,
        avg_win=(sum(wins) / len(wins)) if wins else None,
        avg_loss=(sum(losses) / len(losses)) if losses else None,
        fee_total=float(fee_total),
        slippage_total=float(slippage_total),
        max_consecutive_losses=_max_consecutive_losses(closed_pnls),
        single_trade_dependency_score=(largest_abs / total_abs) if total_abs > 0.0 else None,
        parameter_stability_score=parameter_stability_score,
        profit_factor_unbounded=profit_factor_unbounded,
    )


def _max_consecutive_losses(values: list[float]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value < 0.0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest
