from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from statistics import fmean
from typing import Any

from ..config import settings
from .base import PositionContext, StrategyDecision
from .exit_rules import ExitRule, create_exit_rules


def _load_signal_rows(
    conn: sqlite3.Connection,
    *,
    pair: str,
    interval: str,
    through_ts_ms: int | None,
) -> list[sqlite3.Row | tuple[Any, ...]]:
    query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
    params: list[object] = [pair, interval]
    if through_ts_ms is not None:
        query += " AND ts <= ?"
        params.append(int(through_ts_ms))
    query += " ORDER BY ts ASC"
    return conn.execute(query, tuple(params)).fetchall()


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _base_signal(*, prev_s: float, prev_l: float, curr_s: float, curr_l: float) -> tuple[str, str]:
    if prev_s <= prev_l and curr_s > curr_l:
        return "BUY", "sma golden cross"
    if prev_s >= prev_l and curr_s < curr_l:
        return "SELL", "sma dead cross"
    return "HOLD", "sma no crossover"


def _resolve_exit_rule_names(raw: str) -> list[str]:
    return [token.strip().lower() for token in str(raw or "").split(",") if token.strip()]


def _compute_entry_cost_floor_ratio(*, slippage_bps: float, live_fee_rate_estimate: float, buffer_ratio: float) -> float:
    slippage_ratio = max(0.0, float(slippage_bps)) / 10_000.0
    roundtrip_fee_ratio = 2.0 * max(0.0, float(live_fee_rate_estimate))
    return roundtrip_fee_ratio + slippage_ratio + max(0.0, float(buffer_ratio))


def _evaluate_entry_edge_filter(
    *,
    base_signal: str,
    gap_ratio: float,
    slippage_bps: float,
    live_fee_rate_estimate: float,
    edge_buffer_ratio: float,
    strategy_min_expected_edge_ratio: float,
) -> tuple[bool, dict[str, float | bool]]:
    cost_floor_ratio = _compute_entry_cost_floor_ratio(
        slippage_bps=slippage_bps,
        live_fee_rate_estimate=live_fee_rate_estimate,
        buffer_ratio=edge_buffer_ratio,
    )
    required_edge_ratio = max(cost_floor_ratio, max(0.0, float(strategy_min_expected_edge_ratio)))
    expected_edge_ratio = max(0.0, float(gap_ratio))
    enabled = base_signal in ("BUY", "SELL")
    blocked = enabled and expected_edge_ratio < required_edge_ratio
    return blocked, {
        "enabled": enabled,
        "blocked": blocked,
        "expected_edge_ratio": expected_edge_ratio,
        "required_edge_ratio": required_edge_ratio,
        "cost_floor_ratio": cost_floor_ratio,
        "roundtrip_fee_ratio": 2.0 * max(0.0, float(live_fee_rate_estimate)),
        "slippage_ratio": max(0.0, float(slippage_bps)) / 10_000.0,
        "buffer_ratio": max(0.0, float(edge_buffer_ratio)),
        "min_expected_edge_ratio": max(0.0, float(strategy_min_expected_edge_ratio)),
    }


def _load_position_context(
    conn: sqlite3.Connection,
    *,
    pair: str,
    candle_ts: int,
    market_price: float,
    signal_context: dict[str, Any],
) -> PositionContext:
    try:
        row = conn.execute(
            """
            SELECT
                MIN(entry_ts) AS entry_ts,
                SUM(entry_price * qty_open) / NULLIF(SUM(qty_open), 0.0) AS avg_entry_price,
                SUM(qty_open) AS qty_open
            FROM open_position_lots
            WHERE pair=? AND qty_open > 1e-12
            """,
            (pair,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None

    if row is None or row["entry_ts"] is None or row["qty_open"] is None:
        return PositionContext(in_position=False, recent_signal_context=dict(signal_context))

    entry_ts = int(row["entry_ts"])
    entry_price = float(row["avg_entry_price"])
    qty_open = float(row["qty_open"])
    holding_time_sec = max(0.0, (int(candle_ts) - entry_ts) / 1000.0)
    unrealized_pnl = (float(market_price) - entry_price) * qty_open
    unrealized_pnl_ratio = _safe_ratio(float(market_price) - entry_price, entry_price)

    return PositionContext(
        in_position=qty_open > 1e-12,
        entry_ts=entry_ts,
        entry_price=entry_price,
        qty_open=qty_open,
        holding_time_sec=holding_time_sec,
        unrealized_pnl=unrealized_pnl,
        unrealized_pnl_ratio=unrealized_pnl_ratio,
        recent_signal_context=dict(signal_context),
    )


def _apply_entry_exit_policy(
    *,
    base_signal: str,
    base_reason: str,
    base_context: dict[str, Any],
    position: PositionContext,
    exit_rules: list[ExitRule],
) -> StrategyDecision:
    if not position.in_position:
        return StrategyDecision(signal=base_signal, reason=base_reason, context=base_context)

    exit_results: list[dict[str, Any]] = []
    for rule in exit_rules:
        rule_result = rule.evaluate(
            position=position,
            candle_ts=int(base_context["ts"]),
            market_price=float(base_context["last_close"]),
            signal_context={
                "base_signal": base_signal,
                "base_reason": base_reason,
                "curr_s": base_context["curr_s"],
                "curr_l": base_context["curr_l"],
            },
        )
        exit_results.append(
            {
                "rule": rule.name,
                "triggered": bool(rule_result.should_exit),
                "reason": rule_result.reason,
                "context": rule_result.context,
            }
        )
        if rule_result.should_exit:
            context = dict(base_context)
            context["position"] = position.as_dict()
            context["exit"] = {
                "triggered": True,
                "rule": rule.name,
                "reason": rule_result.reason,
                "evaluations": exit_results,
            }
            return StrategyDecision(signal="SELL", reason=rule_result.reason, context=context)

    context = dict(base_context)
    context["position"] = position.as_dict()
    context["exit"] = {
        "triggered": False,
        "rule": None,
        "reason": "no exit rule triggered",
        "evaluations": exit_results,
    }
    return StrategyDecision(signal="HOLD", reason="position held: no exit rule triggered", context=context)


@dataclass(frozen=True)
class SmaCrossStrategy:
    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL
    exit_rule_names: list[str] = field(
        default_factory=lambda: _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    )
    exit_max_holding_min: int = settings.STRATEGY_EXIT_MAX_HOLDING_MIN
    exit_min_take_profit_ratio: float = settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
    exit_small_loss_tolerance_ratio: float = settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
    slippage_bps: float = settings.STRATEGY_ENTRY_SLIPPAGE_BPS
    live_fee_rate_estimate: float = settings.LIVE_FEE_RATE_ESTIMATE
    entry_edge_buffer_ratio: float = settings.ENTRY_EDGE_BUFFER_RATIO
    strategy_min_expected_edge_ratio: float = settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO

    name: str = "sma_cross"

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        if self.short_n >= self.long_n:
            raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

        rows = _load_signal_rows(
            conn,
            pair=self.pair,
            interval=self.interval,
            through_ts_ms=through_ts_ms,
        )
        if len(rows) < self.long_n + 2:
            return None

        closes = [float(r[1]) for r in rows]
        ts_list = [int(r[0]) for r in rows]
        end_prev = len(closes) - 1
        end_curr = len(closes)

        prev_s = _sma(closes, self.short_n, end_prev)
        prev_l = _sma(closes, self.long_n, end_prev)
        curr_s = _sma(closes, self.short_n, end_curr)
        curr_l = _sma(closes, self.long_n, end_curr)

        base_signal, base_reason = _base_signal(prev_s=prev_s, prev_l=prev_l, curr_s=curr_s, curr_l=curr_l)
        gap_ratio = abs(_safe_ratio(curr_s - curr_l, curr_l))
        edge_filter_triggered, edge_filter_details = _evaluate_entry_edge_filter(
            base_signal=base_signal,
            gap_ratio=gap_ratio,
            slippage_bps=float(self.slippage_bps),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            edge_buffer_ratio=float(self.entry_edge_buffer_ratio),
            strategy_min_expected_edge_ratio=float(self.strategy_min_expected_edge_ratio),
        )
        entry_signal = base_signal
        entry_reason = base_reason
        if edge_filter_triggered:
            entry_signal = "HOLD"
            entry_reason = "filtered entry: cost_edge"
        signal_context = {
            "strategy": self.name,
            "base_signal": base_signal,
            "base_reason": base_reason,
            "entry_signal": entry_signal,
            "entry_reason": entry_reason,
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
        }
        position = _load_position_context(
            conn,
            pair=self.pair,
            candle_ts=ts_list[-1],
            market_price=float(closes[-1]),
            signal_context=signal_context,
        )
        exit_rules = create_exit_rules(
            rule_names=self.exit_rule_names,
            max_holding_sec=float(self.exit_max_holding_min) * 60.0,
            min_take_profit_ratio=float(self.exit_min_take_profit_ratio),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            small_loss_tolerance_ratio=float(self.exit_small_loss_tolerance_ratio),
        )
        base_context = {
            "ts": ts_list[-1],
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
            "last_close": float(closes[-1]),
            "strategy": self.name,
            "gap_ratio": gap_ratio,
            "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
            "blocked_by_cost_filter": bool(edge_filter_triggered),
            "entry": {
                "base_signal": base_signal,
                "base_reason": base_reason,
                "entry_signal": entry_signal,
                "entry_reason": entry_reason,
            },
            "filters": {
                "cost_edge": {
                    "enabled": bool(edge_filter_details["enabled"]),
                    "passed": not bool(edge_filter_details["blocked"]),
                    "value": float(edge_filter_details["expected_edge_ratio"]),
                    "threshold": float(edge_filter_details["required_edge_ratio"]),
                    "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
                    "roundtrip_fee_ratio": float(edge_filter_details["roundtrip_fee_ratio"]),
                    "slippage_ratio": float(edge_filter_details["slippage_ratio"]),
                    "buffer_ratio": float(edge_filter_details["buffer_ratio"]),
                    "min_expected_edge_ratio": float(edge_filter_details["min_expected_edge_ratio"]),
                }
            },
        }
        return _apply_entry_exit_policy(
            base_signal=entry_signal,
            base_reason=entry_reason,
            base_context=base_context,
            position=position,
            exit_rules=exit_rules,
        )


@dataclass(frozen=True)
class SmaWithFilterStrategy:
    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL
    min_gap_ratio: float = settings.SMA_FILTER_GAP_MIN_RATIO
    volatility_window: int = settings.SMA_FILTER_VOL_WINDOW
    min_volatility_ratio: float = settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
    overextended_lookback: int = settings.SMA_FILTER_OVEREXT_LOOKBACK
    overextended_max_return_ratio: float = settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO
    slippage_bps: float = settings.STRATEGY_ENTRY_SLIPPAGE_BPS
    live_fee_rate_estimate: float = settings.LIVE_FEE_RATE_ESTIMATE
    entry_edge_buffer_ratio: float = settings.ENTRY_EDGE_BUFFER_RATIO
    strategy_min_expected_edge_ratio: float = settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO
    exit_rule_names: list[str] = field(
        default_factory=lambda: _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    )
    exit_max_holding_min: int = settings.STRATEGY_EXIT_MAX_HOLDING_MIN
    exit_min_take_profit_ratio: float = settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
    exit_small_loss_tolerance_ratio: float = settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO

    name: str = "sma_with_filter"

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        if self.short_n >= self.long_n:
            raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

        min_rows = max(
            self.long_n + 2,
            int(self.volatility_window),
            int(self.overextended_lookback) + 1,
        )
        rows = _load_signal_rows(
            conn,
            pair=self.pair,
            interval=self.interval,
            through_ts_ms=through_ts_ms,
        )
        if len(rows) < min_rows:
            return None

        closes = [float(r[1]) for r in rows]
        ts_list = [int(r[0]) for r in rows]

        end_prev = len(closes) - 1
        end_curr = len(closes)

        prev_s = _sma(closes, self.short_n, end_prev)
        prev_l = _sma(closes, self.long_n, end_prev)
        curr_s = _sma(closes, self.short_n, end_curr)
        curr_l = _sma(closes, self.long_n, end_curr)

        base_signal, base_reason = _base_signal(prev_s=prev_s, prev_l=prev_l, curr_s=curr_s, curr_l=curr_l)

        gap_ratio = abs(_safe_ratio(curr_s - curr_l, curr_l))

        vol_window = max(1, int(self.volatility_window))
        vol_closes = closes[-vol_window:]
        vol_mean = fmean(vol_closes)
        volatility_ratio = _safe_ratio((max(vol_closes) - min(vol_closes)), vol_mean)

        overext_lookback = max(1, int(self.overextended_lookback))
        base_close = closes[-1 - overext_lookback]
        overextended_ratio = abs(_safe_ratio(closes[-1] - base_close, base_close))

        gap_filter_enabled = float(self.min_gap_ratio) > 0
        volatility_filter_enabled = float(self.min_volatility_ratio) > 0
        overextended_filter_enabled = float(self.overextended_max_return_ratio) > 0

        gap_triggered = gap_filter_enabled and gap_ratio < float(self.min_gap_ratio)
        volatility_triggered = (
            volatility_filter_enabled and volatility_ratio < float(self.min_volatility_ratio)
        )
        overextended_triggered = (
            overextended_filter_enabled
            and overextended_ratio > float(self.overextended_max_return_ratio)
        )
        edge_filter_triggered, edge_filter_details = _evaluate_entry_edge_filter(
            base_signal=base_signal,
            gap_ratio=gap_ratio,
            slippage_bps=float(self.slippage_bps),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            edge_buffer_ratio=float(self.entry_edge_buffer_ratio),
            strategy_min_expected_edge_ratio=float(self.strategy_min_expected_edge_ratio),
        )

        blocked_filters = []
        if gap_triggered:
            blocked_filters.append("gap")
        if volatility_triggered:
            blocked_filters.append("volatility")
        if overextended_triggered:
            blocked_filters.append("overextended")
        if edge_filter_triggered:
            blocked_filters.append("cost_edge")

        should_filter_entry = base_signal in ("BUY", "SELL")
        entry_signal = base_signal
        entry_reason = base_reason
        if should_filter_entry and blocked_filters:
            entry_signal = "HOLD"
            entry_reason = f"filtered entry: {', '.join(blocked_filters)}"

        signal_context = {
            "strategy": self.name,
            "base_signal": base_signal,
            "base_reason": base_reason,
            "entry_signal": entry_signal,
            "entry_reason": entry_reason,
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
        }
        position = _load_position_context(
            conn,
            pair=self.pair,
            candle_ts=ts_list[-1],
            market_price=float(closes[-1]),
            signal_context=signal_context,
        )
        exit_rules = create_exit_rules(
            rule_names=self.exit_rule_names,
            max_holding_sec=float(self.exit_max_holding_min) * 60.0,
            min_take_profit_ratio=float(self.exit_min_take_profit_ratio),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            small_loss_tolerance_ratio=float(self.exit_small_loss_tolerance_ratio),
        )

        base_context = {
            "ts": ts_list[-1],
            "last_close": float(closes[-1]),
            "strategy": self.name,
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
            "features": {
                "prev_s": prev_s,
                "prev_l": prev_l,
                "curr_s": curr_s,
                "curr_l": curr_l,
                "sma_gap_ratio": gap_ratio,
                "volatility_range_ratio": volatility_ratio,
                "overextended_abs_return_ratio": overextended_ratio,
                "base_signal": base_signal,
                "base_reason": base_reason,
            },
            "filters": {
                "gap": {
                    "enabled": gap_filter_enabled,
                    "passed": not gap_triggered,
                    "threshold": float(self.min_gap_ratio),
                    "value": gap_ratio,
                },
                "volatility": {
                    "enabled": volatility_filter_enabled,
                    "passed": not volatility_triggered,
                    "window": vol_window,
                    "threshold": float(self.min_volatility_ratio),
                    "value": volatility_ratio,
                },
                "overextended": {
                    "enabled": overextended_filter_enabled,
                    "passed": not overextended_triggered,
                    "lookback": overext_lookback,
                    "threshold": float(self.overextended_max_return_ratio),
                    "value": overextended_ratio,
                },
                "cost_edge": {
                    "enabled": bool(edge_filter_details["enabled"]),
                    "passed": not bool(edge_filter_details["blocked"]),
                    "value": float(edge_filter_details["expected_edge_ratio"]),
                    "threshold": float(edge_filter_details["required_edge_ratio"]),
                    "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
                    "roundtrip_fee_ratio": float(edge_filter_details["roundtrip_fee_ratio"]),
                    "slippage_ratio": float(edge_filter_details["slippage_ratio"]),
                    "buffer_ratio": float(edge_filter_details["buffer_ratio"]),
                    "min_expected_edge_ratio": float(edge_filter_details["min_expected_edge_ratio"]),
                },
            },
            "filter_blocked": bool(should_filter_entry and blocked_filters),
            "blocked_filters": blocked_filters,
            "gap_ratio": gap_ratio,
            "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
            "blocked_by_cost_filter": bool(should_filter_entry and edge_filter_triggered),
            "entry": {"base_signal": base_signal, "base_reason": base_reason},
        }

        return _apply_entry_exit_policy(
            base_signal=entry_signal,
            base_reason=entry_reason,
            base_context=base_context,
            position=position,
            exit_rules=exit_rules,
        )


def create_sma_strategy(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
    exit_rule_names: list[str] | None = None,
    exit_max_holding_min: int | None = None,
    exit_min_take_profit_ratio: float | None = None,
    exit_small_loss_tolerance_ratio: float | None = None,
    slippage_bps: float | None = None,
    entry_edge_buffer_ratio: float | None = None,
    strategy_min_expected_edge_ratio: float | None = None,
    live_fee_rate_estimate: float | None = None,
) -> SmaCrossStrategy:
    return SmaCrossStrategy(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings.PAIR if pair is None else str(pair),
        interval=settings.INTERVAL if interval is None else str(interval),
        exit_rule_names=(
            _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
            if exit_rule_names is None
            else [str(name).strip().lower() for name in exit_rule_names if str(name).strip()]
        ),
        exit_max_holding_min=int(
            settings.STRATEGY_EXIT_MAX_HOLDING_MIN
            if exit_max_holding_min is None
            else exit_max_holding_min
        ),
        exit_min_take_profit_ratio=float(
            settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
            if exit_min_take_profit_ratio is None
            else exit_min_take_profit_ratio
        ),
        exit_small_loss_tolerance_ratio=float(
            settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
            if exit_small_loss_tolerance_ratio is None
            else exit_small_loss_tolerance_ratio
        ),
        slippage_bps=float(
            settings.STRATEGY_ENTRY_SLIPPAGE_BPS if slippage_bps is None else slippage_bps
        ),
        entry_edge_buffer_ratio=float(
            settings.ENTRY_EDGE_BUFFER_RATIO
            if entry_edge_buffer_ratio is None
            else entry_edge_buffer_ratio
        ),
        strategy_min_expected_edge_ratio=float(
            settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO
            if strategy_min_expected_edge_ratio is None
            else strategy_min_expected_edge_ratio
        ),
        live_fee_rate_estimate=float(
            settings.LIVE_FEE_RATE_ESTIMATE
            if live_fee_rate_estimate is None
            else live_fee_rate_estimate
        ),
    )


def create_sma_with_filter_strategy(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
    min_gap_ratio: float | None = None,
    volatility_window: int | None = None,
    min_volatility_ratio: float | None = None,
    overextended_lookback: int | None = None,
    overextended_max_return_ratio: float | None = None,
    slippage_bps: float | None = None,
    live_fee_rate_estimate: float | None = None,
    entry_edge_buffer_ratio: float | None = None,
    strategy_min_expected_edge_ratio: float | None = None,
    exit_rule_names: list[str] | None = None,
    exit_max_holding_min: int | None = None,
    exit_min_take_profit_ratio: float | None = None,
    exit_small_loss_tolerance_ratio: float | None = None,
) -> SmaWithFilterStrategy:
    return SmaWithFilterStrategy(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings.PAIR if pair is None else str(pair),
        interval=settings.INTERVAL if interval is None else str(interval),
        min_gap_ratio=float(
            settings.SMA_FILTER_GAP_MIN_RATIO if min_gap_ratio is None else min_gap_ratio
        ),
        volatility_window=int(
            settings.SMA_FILTER_VOL_WINDOW if volatility_window is None else volatility_window
        ),
        min_volatility_ratio=float(
            settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
            if min_volatility_ratio is None
            else min_volatility_ratio
        ),
        overextended_lookback=int(
            settings.SMA_FILTER_OVEREXT_LOOKBACK
            if overextended_lookback is None
            else overextended_lookback
        ),
        overextended_max_return_ratio=float(
            settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO
            if overextended_max_return_ratio is None
            else overextended_max_return_ratio
        ),
        slippage_bps=float(
            settings.STRATEGY_ENTRY_SLIPPAGE_BPS if slippage_bps is None else slippage_bps
        ),
        live_fee_rate_estimate=float(
            settings.LIVE_FEE_RATE_ESTIMATE
            if live_fee_rate_estimate is None
            else live_fee_rate_estimate
        ),
        entry_edge_buffer_ratio=float(
            settings.ENTRY_EDGE_BUFFER_RATIO
            if entry_edge_buffer_ratio is None
            else entry_edge_buffer_ratio
        ),
        strategy_min_expected_edge_ratio=float(
            settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO
            if strategy_min_expected_edge_ratio is None
            else strategy_min_expected_edge_ratio
        ),
        exit_rule_names=(
            _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
            if exit_rule_names is None
            else [str(name).strip().lower() for name in exit_rule_names if str(name).strip()]
        ),
        exit_max_holding_min=int(
            settings.STRATEGY_EXIT_MAX_HOLDING_MIN
            if exit_max_holding_min is None
            else exit_max_holding_min
        ),
        exit_min_take_profit_ratio=float(
            settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
            if exit_min_take_profit_ratio is None
            else exit_min_take_profit_ratio
        ),
        exit_small_loss_tolerance_ratio=float(
            settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
            if exit_small_loss_tolerance_ratio is None
            else exit_small_loss_tolerance_ratio
        ),
    )


def compute_signal(
    conn: sqlite3.Connection,
    short_n: int,
    long_n: int,
    *,
    through_ts_ms: int | None = None,
) -> dict[str, Any] | None:
    decision = create_sma_strategy(short_n=short_n, long_n=long_n).decide(
        conn,
        through_ts_ms=through_ts_ms,
    )
    if decision is None:
        return None
    return decision.as_dict()
