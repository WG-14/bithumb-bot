from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.market_regime import RegimeCoverageRow

from .backtest_engine import (
    BacktestRun,
    BacktestRunContext,
    apply_pending_fills,
    closed_trade_diagnostics,
    complete_audit_trace,
    create_exit_rules,
    depth_request_fields,
    empty_execution_event_summary,
    empty_metrics,
    empty_metrics_v2,
    execution_event_summary,
    execution_reference_warnings,
    failed_fill,
    fill_applies_to_mark,
    fill_effective_ts,
    mark_pending_fills_at_end,
    metrics,
    metrics_v2_ledgers_from_trades,
    model_latency_ms,
    pending_trade_from_fill,
    record_equity_mark,
    research_decision_payload,
    retained_detail_summary,
    timing_request_fields,
    trace_decision,
    trace_equity_mark,
    trace_execution,
    trade_from_fill,
    trade_hash_payload,
)
from .execution_model import ExecutionFill


@dataclass
class BacktestAccumulator:
    context: BacktestRunContext
    total_candles: int
    diagnostics_namespace: str
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
    strategy_diagnostic_counts: dict[str, int] = field(default_factory=dict)

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
        for key, value in _diagnostic_count_defaults(payload).items():
            self.strategy_diagnostic_counts.setdefault(key, int(value))
        for key, value in _diagnostic_count_increments(payload).items():
            self.strategy_diagnostic_counts[key] = (
                int(self.strategy_diagnostic_counts.get(key, 0)) + int(value)
            )
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
        self.trade_ledger_hash_material.append(trade_hash_payload(trade))

    def update_trades(self, trades: list[dict[str, object]]) -> None:
        self.trade_count = len(trades)
        self.closed_trade_count = sum(
            1 for trade in trades if str(trade.get("side") or "").upper() == "SELL"
        )

    def maybe_emit_heartbeat(self, candles_processed: int) -> None:
        callback = self.context.progress_callback
        if callback is None:
            return
        now = time.perf_counter()
        interval = self.context.heartbeat.interval_s
        bar_interval = self.context.heartbeat.bar_interval
        by_time = interval is not None and now - self.last_heartbeat_s >= float(interval)
        by_bar = (
            bar_interval is not None
            and int(bar_interval) > 0
            and candles_processed - self.last_heartbeat_bar >= int(bar_interval)
        )
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
        from .backtest_engine import BacktestResourceLimitExceeded

        self.update_trades(trades)
        limits = self.context.resource_limits
        reasons: list[str] = []
        elapsed = time.perf_counter() - self.context.started_at
        rss = _rss_mb()
        if (
            limits.max_runtime_s_per_candidate_split is not None
            and elapsed > float(limits.max_runtime_s_per_candidate_split)
        ):
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
        payload.update(
            _behavior_hashes(
                decision_material=self.behavior_hash_material,
                common_decision_material=self.common_behavior_hash_material,
                strategy_decision_material=self.strategy_behavior_hash_material,
                trade_material=self.trade_ledger_hash_material,
                equity_material=self.equity_curve_hash_material,
            )
        )
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
        payload = _generic_strategy_diagnostics_from_trades(
            namespace=self.diagnostics_namespace,
            trades=trades,
        )
        for key in sorted(self.strategy_diagnostic_counts):
            payload[key] = int(self.strategy_diagnostic_counts[key])
        strategy_specific = dict(payload)
        payload["strategy_specific_diagnostics"] = {self.diagnostics_namespace: strategy_specific}
        return payload


@dataclass
class RegimeCoverageAccumulator:
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
            regimes = sorted(
                set(candle_counts)
                | {regime for item_dimension, regime in trade_counts if item_dimension == dimension}
            )
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


@dataclass
class PendingFill:
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
class ResearchPositionContext:
    in_position: bool
    entry_ts: int | None = None
    entry_price: float | None = None
    qty_open: float = 0.0
    holding_time_sec: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_ratio: float = 0.0


def _diagnostic_count_defaults(payload: dict[str, object]) -> dict[str, int]:
    defaults = payload.get("strategy_diagnostic_count_defaults")
    if not isinstance(defaults, dict):
        return {}
    return {
        str(key): int(value)
        for key, value in defaults.items()
        if _diagnostic_key_is_public(str(key))
    }


def _diagnostic_count_increments(payload: dict[str, object]) -> dict[str, int]:
    counts = payload.get("strategy_diagnostic_counts")
    if not isinstance(counts, dict):
        return {}
    increments: dict[str, int] = {}
    for key, value in counts.items():
        normalized = str(key)
        if not _diagnostic_key_is_public(normalized):
            continue
        increments[normalized] = increments.get(normalized, 0) + int(value)
    return increments


def _diagnostic_key_is_public(key: str) -> bool:
    return bool(key) and not key.startswith("_")


def _generic_strategy_diagnostics_from_trades(
    *,
    namespace: str,
    trades: list[dict[str, object]],
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
        "strategy_diagnostics_namespace": str(namespace),
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
    payload["strategy_specific_diagnostics"] = {str(namespace): dict(payload)}
    return payload


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * float(percentile)))))
    return ordered[index]


def _behavior_hashes(
    *,
    decision_material: list[dict[str, object]],
    common_decision_material: list[dict[str, object]] | None,
    strategy_decision_material: list[dict[str, object]] | None,
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


def _rss_mb() -> float | None:
    try:
        import resource

        rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None
    if rss > 10_000_000:
        return round(rss / (1024.0 * 1024.0), 3)
    return round(rss / 1024.0, 3)


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
