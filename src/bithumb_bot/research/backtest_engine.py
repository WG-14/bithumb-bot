from __future__ import annotations

import time
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

from .dataset_snapshot import DatasetSnapshot
from .execution_model import ExecutionFill, ExecutionModel, ExecutionRequest, FixedBpsExecutionModel, model_params_hash
from .execution_timing import (
    ExecutionReferenceEvent,
    SignalEvent,
    build_signal_event,
    candle_close_ts,
    resolve_execution_reference,
)
from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy, legacy_research_portfolio_policy
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
        if str(payload.get("raw_signal") or "").upper() in {"BUY", "SELL"}:
            self.signal_count += 1
        self.decision_hash_material.append(str(payload.get("replay_fingerprint_hash") or ""))
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
    short_n = int(parameter_values.get("SMA_SHORT", parameter_values.get("short_n", 0)))
    long_n = int(parameter_values.get("SMA_LONG", parameter_values.get("long_n", 0)))
    min_gap = float(
        parameter_values.get(
            "SMA_FILTER_GAP_MIN_RATIO",
            parameter_values.get("strategy_min_expected_edge_ratio", 0.0),
        )
    )
    min_range = float(parameter_values.get("SMA_FILTER_VOL_MIN_RANGE_RATIO", 0.0))
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
        audit_trace_index = _complete_audit_trace(run_context, status="completed")
        return BacktestRun(
            metrics=_empty_metrics(parameter_stability_score),
            metrics_v2=_empty_metrics_v2(starting_cash=starting_cash, initial_position_qty=initial_qty),
            trades=(),
            candle_count=len(candles),
            warnings=("not_enough_candles",),
            regime_performance=(),
            regime_coverage=(),
            execution_event_summary=empty_execution_event_summary(),
            resource_usage=accumulator.resource_usage(candles_processed=len(candles)),
            retained_detail_summary=_retained_detail_summary(accumulator, retained_regime_snapshot_count=0),
            audit_trace_index=audit_trace_index,
        )

    closes = [candle.close for candle in candles]
    highs = [candle.high for candle in candles]
    lows = [candle.low for candle in candles]
    volumes = [candle.volume for candle in candles]
    regime_snapshots: list[dict[str, object]] = []
    regime_coverage_accumulator = _RegimeCoverageAccumulator()
    thresholds = MarketRegimeThresholds(
        min_trend_strength_ratio=max(0.0, min_gap),
        low_volatility_ratio=max(0.0, min_range),
    )
    cash = starting_cash
    qty = initial_qty
    entry_cost_basis = 0.0
    entry_regime_snapshot: dict[str, object] | None = None
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
    prev_above: bool | None = None
    model = execution_model or FixedBpsExecutionModel(fee_rate=fee_rate, slippage_bps=slippage_bps)
    timing_policy = execution_timing_policy or ExecutionTimingPolicy()

    for index in range(long_n, len(candles)):
        candle = candles[index]
        mark_boundary_ts = candle_close_ts(candle, interval=dataset.interval)
        decision_boundary_ts = mark_boundary_ts + int(timing_policy.decision_guard_ms)
        cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=mark_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        mark_cash = cash
        mark_qty = qty
        cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
            pending_fills=pending_fills,
            trades=trades,
            boundary_ts=decision_boundary_ts,
            cash=cash,
            qty=qty,
            entry_cost_basis=entry_cost_basis,
            entry_regime_snapshot=entry_regime_snapshot,
            entry_fee=entry_fee,
            entry_slippage=entry_slippage,
            fee_total=fee_total,
            slippage_total=slippage_total,
            closed_pnls=closed_pnls,
        )
        prev_short = _sma(closes, short_n, index)
        prev_long = _sma(closes, long_n, index)
        curr_short = _sma(closes, short_n, index + 1)
        curr_long = _sma(closes, long_n, index + 1)
        above = curr_short > curr_long
        gap_ratio = abs(curr_short - curr_long) / curr_long if curr_long > 0.0 else 0.0
        range_ratio = (candle.high - candle.low) / candle.close if candle.close > 0.0 else 0.0
        regime_snapshot = classify_market_regime_from_arrays(
            closes=closes,
            highs=highs,
            lows=lows,
            volumes=volumes,
            index=index,
            short_sma=curr_short,
            long_sma=curr_long,
            volatility_window=max(1, int(parameter_values.get("SMA_FILTER_VOL_WINDOW", 10))),
            volume_window=max(1, int(parameter_values.get("SMA_FILTER_VOLUME_WINDOW", 10))),
            liquidity_window=max(1, int(parameter_values.get("SMA_FILTER_LIQUIDITY_WINDOW", 10))),
            thresholds=thresholds,
            overextended_lookback=max(1, int(parameter_values.get("SMA_FILTER_OVEREXT_LOOKBACK", 3))),
            overextended_max_return_ratio=float(parameter_values.get("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", 0.0)),
        ).as_dict()
        regime_coverage_accumulator.update(regime_snapshot)
        if accumulator.retain_full_detail():
            regime_snapshots.append(regime_snapshot)

        action = "HOLD"
        raw_signal = "HOLD"
        raw_reason = "sma no crossover"
        pending_buy_qty = sum(item.qty for item in pending_fills if item.side == "BUY")
        pending_sell_qty = sum(item.qty for item in pending_fills if item.side == "SELL")
        sellable_qty = max(0.0, qty - pending_sell_qty)
        filter_blocked = False
        blocked_filters: list[str] = []
        if prev_above is not None:
            if not prev_above and above:
                raw_signal = "BUY"
                raw_reason = "sma golden cross"
            elif prev_above and not above:
                raw_signal = "SELL"
                raw_reason = "sma dead cross"
        if raw_signal in {"BUY", "SELL"} and gap_ratio < min_gap:
            filter_blocked = True
            blocked_filters.append("gap")
        if raw_signal in {"BUY", "SELL"} and range_ratio < min_range:
            filter_blocked = True
            blocked_filters.append("volatility")
        if not filter_blocked and prev_above is not None:
            if raw_signal == "BUY" and qty <= 0.0 and pending_buy_qty <= 0.0:
                action = "BUY"
            elif raw_signal == "SELL" and sellable_qty > 0.0:
                action = "SELL"
        decision_payload = _research_decision_payload(
            dataset=dataset,
            dataset_content_hash=dataset_content_hash,
            parameter_values=parameter_values,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            timing_policy=timing_policy,
            portfolio_policy=policy,
            candle_ts=int(candle.ts),
            decision_ts=int(decision_boundary_ts),
            raw_signal=raw_signal,
            final_signal=action,
            raw_reason=raw_reason,
            blocked=bool(raw_signal in {"BUY", "SELL"} and action == "HOLD"),
            blocked_filters=blocked_filters,
            prev_s=prev_short,
            prev_l=prev_long,
            curr_s=curr_short,
            curr_l=curr_long,
            gap_ratio=gap_ratio,
            range_ratio=range_ratio,
            regime_snapshot=regime_snapshot,
            qty=qty,
            sellable_qty=sellable_qty,
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
                prev_above = above
                accumulator.maybe_emit_heartbeat(index - long_n + 1)
                accumulator.check_limits(candles_processed=index - long_n + 1, trades=trades)
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
                prev_above = above
                accumulator.maybe_emit_heartbeat(index - long_n + 1)
                accumulator.check_limits(candles_processed=index - long_n + 1, trades=trades)
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
            _trace_execution(run_context, trades[-1])
            if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                mark_cash += pending.cash_delta
                mark_qty += pending.qty
            pending_fills.append(pending)
            cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
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
                prev_above = above
                accumulator.maybe_emit_heartbeat(index - long_n + 1)
                accumulator.check_limits(candles_processed=index - long_n + 1, trades=trades)
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
                prev_above = above
                accumulator.maybe_emit_heartbeat(index - long_n + 1)
                accumulator.check_limits(candles_processed=index - long_n + 1, trades=trades)
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
            _trace_execution(run_context, trades[-1])
            if _fill_applies_to_mark(fill=pending.fill, effective_ts=pending.effective_ts, mark_boundary_ts=mark_boundary_ts):
                mark_cash += pending.cash_delta
                mark_qty = max(0.0, mark_qty - pending.qty)
            pending_fills.append(pending)
            cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
                pending_fills=pending_fills,
                trades=trades,
                boundary_ts=decision_boundary_ts,
                cash=cash,
                qty=qty,
                entry_cost_basis=entry_cost_basis,
                entry_regime_snapshot=entry_regime_snapshot,
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
        prev_above = above
        accumulator.maybe_emit_heartbeat(index - long_n + 1)
        accumulator.check_limits(candles_processed=index - long_n + 1, trades=trades)

    last = candles[-1]
    last_mark_ts = candle_close_ts(last, interval=dataset.interval)
    cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total = _apply_pending_fills(
        pending_fills=pending_fills,
        trades=trades,
        boundary_ts=last_mark_ts,
        cash=cash,
        qty=qty,
        entry_cost_basis=entry_cost_basis,
        entry_regime_snapshot=entry_regime_snapshot,
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
        resource_usage=accumulator.resource_usage(candles_processed=max(0, len(candles) - long_n)),
        retained_detail_summary=_retained_detail_summary(
            accumulator,
            retained_regime_snapshot_count=len(regime_snapshots),
        ),
        audit_trace_index=audit_trace_index,
    )


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


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
    entry_fee: float,
    entry_slippage: float,
    fee_total: float,
    slippage_total: float,
    closed_pnls: list[float],
) -> tuple[float, float, float, dict[str, object] | None, float, float, float, float]:
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
                entry_fee = 0.0
                entry_slippage = 0.0
    return cash, qty, entry_cost_basis, entry_regime_snapshot, entry_fee, entry_slippage, fee_total, slippage_total


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
    fee_rate: float,
    slippage_bps: float,
    timing_policy: ExecutionTimingPolicy,
    portfolio_policy: PortfolioPolicy,
    candle_ts: int,
    decision_ts: int,
    raw_signal: str,
    final_signal: str,
    raw_reason: str,
    blocked: bool,
    blocked_filters: list[str],
    prev_s: float,
    prev_l: float,
    curr_s: float,
    curr_l: float,
    gap_ratio: float,
    range_ratio: float,
    regime_snapshot: dict[str, object],
    qty: float,
    sellable_qty: float,
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
        "strategy_name": "sma_with_filter",
        "market": dataset.market,
        "interval": dataset.interval,
        "signal_timestamp": str(candle_ts),
        "candle_ts": int(candle_ts),
        "through_ts_ms": int(candle_ts),
        "candle_basis": "research_closed_candle",
        "decision_ts": int(decision_ts),
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "side": final_signal,
        "blocked": bool(blocked),
        "block_reason": f"filtered entry: {', '.join(blocked_filters)}" if blocked_filters else (raw_reason if blocked else ""),
        "blocked_filters": tuple(blocked_filters),
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
        "fee_model_hash": canonical_payload_hash({"fee_rate": float(fee_rate)}),
        "slippage_model_hash": canonical_payload_hash({"slippage_bps": float(slippage_bps)}),
        "order_rules_hash": order_rules_hash,
        "market_regime": str(regime_snapshot.get("composite_regime") or ""),
        "regime_decision": "allowed",
        "regime_block_reason": "",
        "position_state_hash": position_state_hash,
        "entry_allowed": bool(lot_native_authority.entry_allowed),
        "exit_allowed": bool(lot_native_authority.exit_allowed),
        "dust_state": "flat" if flat_no_position else (
            "research_not_modeled" if lot_native_authority.unsupported_reason else "no_dust"
        ),
        "effective_flat": bool(lot_native_authority.entry_allowed),
        "normalized_exposure_active": bool(lot_native_authority.open_lot_count > 0),
        "exit_rule": "opposite_cross" if final_signal == "SELL" and raw_signal == "SELL" else "",
        "exit_reason": "exit by opposite cross" if final_signal == "SELL" and raw_signal == "SELL" else "",
        "exit_evaluations_hash": canonical_payload_hash(
            {"raw_signal": raw_signal, "final_signal": final_signal, "position_qty": float(qty)}
        ),
        "execution_timing_policy_hash": canonical_payload_hash(timing_policy.as_dict()),
        "replay_fingerprint_hash": canonical_payload_hash(
            {
                "dataset_content_hash": dataset_content_hash,
                "parameter_values": parameter_values,
                "candle_ts": int(candle_ts),
            }
        ),
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
