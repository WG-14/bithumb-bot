from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from typing import Any

from .decision_attribution import (
    DecisionAttribution,
    DecisionAttributionAccumulator,
    DecisionAttributionSummary,
    summarize_decision_attributions,
)
from .decision_contract import apply_decision_contract, build_replay_fingerprint
from .strategy.sma import (
    _base_signal,
    _compute_gap_ratio,
    _evaluate_entry_edge_filter,
    _resolve_signal_strength_label,
    _sma,
)
from .strategy_config import SmaStrategyConfig


@dataclass(frozen=True)
class StrategyReplayConfig:
    strategy_config: SmaStrategyConfig
    from_ts_ms: int | None = None
    to_ts_ms: int | None = None
    through_ts_ms: int | None = None
    max_candles: int | None = None


@dataclass(frozen=True)
class CandleReplayDataset:
    pair: str
    interval: str
    candles: tuple[tuple[int, float], ...]
    from_ts_ms: int | None
    to_ts_ms: int | None
    through_ts_ms: int | None
    max_candles: int | None
    source: str = "sqlite:candles"


@dataclass(frozen=True)
class StrategyReplayResult:
    config_id: str
    attribution_summary: DecisionAttributionSummary
    decision_count: int
    insufficient_candle_count: int
    candle_count: int


def _stable_payload(config: StrategyReplayConfig) -> dict[str, object]:
    return {
        "strategy_config": asdict(config.strategy_config),
        "from_ts_ms": config.from_ts_ms,
        "to_ts_ms": config.to_ts_ms,
        "through_ts_ms": config.through_ts_ms,
        "max_candles": config.max_candles,
    }


def build_strategy_replay_config_id(config: StrategyReplayConfig) -> str:
    raw = json.dumps(_stable_payload(config), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _upper_bound_ts(
    *,
    to_ts_ms: int | None,
    through_ts_ms: int | None,
) -> int | None:
    values = [
        int(value)
        for value in (to_ts_ms, through_ts_ms)
        if value is not None
    ]
    return min(values) if values else None


def load_replay_candles(
    conn: sqlite3.Connection,
    *,
    pair: str,
    interval: str,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    through_ts_ms: int | None = None,
    max_candles: int | None = None,
) -> CandleReplayDataset:
    if max_candles is not None and int(max_candles) <= 0:
        raise ValueError("max_candles must be positive")
    upper_bound_ts = _upper_bound_ts(to_ts_ms=to_ts_ms, through_ts_ms=through_ts_ms)
    params: list[object] = [str(pair), str(interval)]
    where = "WHERE pair=? AND interval=?"
    if from_ts_ms is not None:
        where += " AND ts >= ?"
        params.append(int(from_ts_ms))
    if upper_bound_ts is not None:
        where += " AND ts <= ?"
        params.append(int(upper_bound_ts))
    if max_candles is not None:
        params.append(int(max_candles))
        query = f"""
            SELECT ts, close
            FROM (
                SELECT ts, close
                FROM candles
                {where}
                ORDER BY ts DESC
                LIMIT ?
            )
            ORDER BY ts ASC
        """
    else:
        query = f"SELECT ts, close FROM candles {where} ORDER BY ts ASC"
    candles = tuple(
        (int(row[0]), float(row[1]))
        for row in conn.execute(query, tuple(params)).fetchall()
    )
    return CandleReplayDataset(
        pair=str(pair),
        interval=str(interval),
        candles=candles,
        from_ts_ms=None if from_ts_ms is None else int(from_ts_ms),
        to_ts_ms=None if to_ts_ms is None else int(to_ts_ms),
        through_ts_ms=None if through_ts_ms is None else int(through_ts_ms),
        max_candles=None if max_candles is None else int(max_candles),
    )


def _load_replay_candles(
    conn: sqlite3.Connection,
    *,
    strategy_config: SmaStrategyConfig,
    upper_bound_ts: int | None,
) -> list[tuple[int, float]]:
    """Compatibility wrapper for older tests/imports; prefer load_replay_candles."""
    query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
    params: list[object] = [strategy_config.pair, strategy_config.interval]
    if upper_bound_ts is not None:
        query += " AND ts <= ?"
        params.append(int(upper_bound_ts))
    query += " ORDER BY ts ASC"
    return [(int(row[0]), float(row[1])) for row in conn.execute(query, tuple(params)).fetchall()]


def _replay_decision_context(
    *,
    replay_config: StrategyReplayConfig,
    config_id: str,
    candle_ts: int,
    close: float,
    prev_s: float,
    prev_l: float,
    curr_s: float,
    curr_l: float,
) -> dict[str, Any]:
    strategy_config = replay_config.strategy_config
    raw_signal, base_reason = _base_signal(
        prev_s=prev_s,
        prev_l=prev_l,
        curr_s=curr_s,
        curr_l=curr_l,
    )
    gap_ratio = _compute_gap_ratio(curr_s=curr_s, curr_l=curr_l)
    edge_filter_triggered, edge_filter_details = _evaluate_entry_edge_filter(
        base_signal=raw_signal,
        gap_ratio=gap_ratio,
        slippage_bps=float(strategy_config.slippage_bps),
        live_fee_rate_estimate=float(strategy_config.live_fee_rate_estimate),
        edge_buffer_ratio=float(strategy_config.entry_edge_buffer_ratio),
        strategy_min_expected_edge_ratio=float(strategy_config.strategy_min_expected_edge_ratio),
    )
    final_signal = "HOLD" if edge_filter_triggered else raw_signal
    entry_reason = "filtered entry: cost_edge" if edge_filter_triggered else base_reason
    signal_strength_label = _resolve_signal_strength_label(
        base_signal=raw_signal,
        expected_edge_ratio=float(edge_filter_details["expected_edge_ratio"]),
        required_edge_ratio=float(edge_filter_details["required_edge_ratio"]),
    )
    extra_block_reasons = (
        [("strategy_filters", "cost_edge")] if edge_filter_triggered else []
    )
    context = {
        "strategy": "sma_replay",
        "replay_mode": "decision_attribution_only",
        "config_id": config_id,
        "experiment_fingerprint": config_id,
        "ts": int(candle_ts),
        "candle_ts": int(candle_ts),
        "last_close": float(close),
        "pair": strategy_config.pair,
        "interval": strategy_config.interval,
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "decision_type": (
            "BLOCKED_ENTRY"
            if raw_signal == "BUY" and edge_filter_triggered
            else "BLOCKED_EXIT"
            if raw_signal == "SELL" and edge_filter_triggered
            else final_signal
        ),
        "base_signal": raw_signal,
        "base_reason": base_reason,
        "entry_signal": final_signal,
        "entry_reason": entry_reason,
        "entry_block_reason": entry_reason if edge_filter_triggered else None,
        "prev_s": float(prev_s),
        "prev_l": float(prev_l),
        "curr_s": float(curr_s),
        "curr_l": float(curr_l),
        "gap_ratio": float(gap_ratio),
        "required_edge_ratio": float(edge_filter_details["required_edge_ratio"]),
        "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
        "blocked_by_cost_filter": bool(edge_filter_triggered),
        "signal_strength_label": signal_strength_label,
        "submit_expected": None,
        "features": {
            "prev_s": float(prev_s),
            "prev_l": float(prev_l),
            "curr_s": float(curr_s),
            "curr_l": float(curr_l),
            "sma_gap_ratio": float(gap_ratio),
            "base_signal": raw_signal,
            "base_reason": base_reason,
        },
        "signal_strength": {
            "label": signal_strength_label,
            "gap_ratio": float(gap_ratio),
            "required_edge_ratio": float(edge_filter_details["required_edge_ratio"]),
            "is_weak_cross": bool(signal_strength_label == "weak"),
        },
        "filters": {
            "cost_edge": {
                "enabled": bool(edge_filter_details["enabled"]),
                "configured_enabled": bool(edge_filter_details["configured_enabled"]),
                "signal_eligible": bool(edge_filter_details["signal_eligible"]),
                "passed": not bool(edge_filter_details["blocked"]),
                "value": float(edge_filter_details["expected_edge_ratio"]),
                "threshold": float(edge_filter_details["required_edge_ratio"]),
                "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
                "roundtrip_fee_ratio": float(edge_filter_details["roundtrip_fee_ratio"]),
                "slippage_ratio": float(edge_filter_details["slippage_ratio"]),
                "buffer_ratio": float(edge_filter_details["buffer_ratio"]),
                "min_expected_edge_ratio": float(edge_filter_details["min_expected_edge_ratio"]),
                "fee_authority_source": "strategy_replay_config",
                "fee_authority_degraded": False,
            },
        },
        "entry": {
            "base_signal": raw_signal,
            "base_reason": base_reason,
            "entry_signal": final_signal,
            "entry_reason": entry_reason,
            "allowed": final_signal == "BUY",
            "intent": {
                "pair": strategy_config.pair,
                "intent": "enter_open_exposure",
                "budget_model": "cash_fraction_capped_by_max_order_krw",
                "budget_fraction_of_cash": float(strategy_config.buy_fraction),
                "max_budget_krw": float(strategy_config.max_order_krw),
                "requires_execution_sizing": True,
            },
        },
        "replay_fingerprint": build_replay_fingerprint(
            strategy_name="sma_replay",
            pair=strategy_config.pair,
            interval=strategy_config.interval,
            candle_ts=int(candle_ts),
            through_ts_ms=replay_config.through_ts_ms,
            short_n=int(strategy_config.short_n),
            long_n=int(strategy_config.long_n),
            thresholds={
                "entry_edge_buffer_ratio": float(strategy_config.entry_edge_buffer_ratio),
                "strategy_min_expected_edge_ratio": float(
                    strategy_config.strategy_min_expected_edge_ratio
                ),
            },
            fee_authority={
                "fee_source": "strategy_replay_config",
                "degraded": False,
            },
            slippage_bps=float(strategy_config.slippage_bps),
            regime_version="not_applicable",
            order_sizing={
                "buy_fraction": float(strategy_config.buy_fraction),
                "max_order_krw": float(strategy_config.max_order_krw),
            },
        ),
    }
    return apply_decision_contract(
        context,
        final_action=final_signal,
        extra_block_reasons=extra_block_reasons,
    )


def _replay_decision_attribution(
    *,
    replay_config: StrategyReplayConfig,
    config_id: str,
    candle_ts: int,
    close: float,
    prev_s: float,
    prev_l: float,
    curr_s: float,
    curr_l: float,
) -> DecisionAttribution:
    del candle_ts, close
    strategy_config = replay_config.strategy_config
    raw_signal, base_reason = _base_signal(
        prev_s=prev_s,
        prev_l=prev_l,
        curr_s=curr_s,
        curr_l=curr_l,
    )
    gap_ratio = _compute_gap_ratio(curr_s=curr_s, curr_l=curr_l)
    edge_filter_triggered, edge_filter_details = _evaluate_entry_edge_filter(
        base_signal=raw_signal,
        gap_ratio=gap_ratio,
        slippage_bps=float(strategy_config.slippage_bps),
        live_fee_rate_estimate=float(strategy_config.live_fee_rate_estimate),
        edge_buffer_ratio=float(strategy_config.entry_edge_buffer_ratio),
        strategy_min_expected_edge_ratio=float(strategy_config.strategy_min_expected_edge_ratio),
    )
    final_signal = "HOLD" if edge_filter_triggered else raw_signal
    entry_reason = "filtered entry: cost_edge" if edge_filter_triggered else base_reason
    signal_strength_label = _resolve_signal_strength_label(
        base_signal=raw_signal,
        expected_edge_ratio=float(edge_filter_details["expected_edge_ratio"]),
        required_edge_ratio=float(edge_filter_details["required_edge_ratio"]),
    )
    decision_type = (
        "BLOCKED_ENTRY"
        if raw_signal == "BUY" and edge_filter_triggered
        else "BLOCKED_EXIT"
        if raw_signal == "SELL" and edge_filter_triggered
        else final_signal
    )
    return DecisionAttribution(
        raw_signal=raw_signal,
        final_signal=final_signal,
        decision_type=decision_type,
        base_reason=base_reason,
        entry_reason=entry_reason,
        entry_block_reason=entry_reason if edge_filter_triggered else None,
        primary_block_layer="strategy_filters" if edge_filter_triggered else "none",
        primary_block_reason="cost_edge" if edge_filter_triggered else "none",
        all_block_reasons=("strategy_filters.cost_edge",) if edge_filter_triggered else (),
        blocked_by_cost_filter=bool(edge_filter_triggered),
        blocked_by_fee_authority=False,
        blocked_by_position_gate=False,
        blocked_by_order_rule=False,
        blocked_by_performance_gate=False,
        gap_ratio=float(gap_ratio),
        required_edge_ratio=float(edge_filter_details["required_edge_ratio"]),
        signal_strength_label=signal_strength_label,
        submit_expected=None,
        execution_block_reason=None,
        target_block_reason=None,
        experiment_fingerprint=config_id,
    )


def replay_sma_strategy_decisions(
    conn: sqlite3.Connection,
    config: StrategyReplayConfig,
) -> StrategyReplayResult:
    dataset = load_replay_candles(
        conn,
        pair=config.strategy_config.pair,
        interval=config.strategy_config.interval,
        from_ts_ms=config.from_ts_ms,
        to_ts_ms=config.to_ts_ms,
        through_ts_ms=config.through_ts_ms,
        max_candles=config.max_candles,
    )
    return replay_sma_strategy_decisions_from_candles(dataset, config)


def replay_sma_strategy_decisions_from_candles(
    dataset: CandleReplayDataset,
    config: StrategyReplayConfig,
) -> StrategyReplayResult:
    strategy_config = config.strategy_config
    if int(strategy_config.short_n) >= int(strategy_config.long_n):
        raise ValueError("short_n must be smaller than long_n")
    if dataset.pair != strategy_config.pair:
        raise ValueError("dataset pair does not match strategy config")
    if dataset.interval != strategy_config.interval:
        raise ValueError("dataset interval does not match strategy config")

    config_id = build_strategy_replay_config_id(config)
    candles = dataset.candles
    replay_start_index = int(strategy_config.long_n) + 1
    if len(candles) < replay_start_index + 1:
        return StrategyReplayResult(
            config_id=config_id,
            attribution_summary=summarize_decision_attributions([]),
            decision_count=0,
            insufficient_candle_count=len(candles),
            candle_count=len(candles),
        )

    timestamps = [ts for ts, _close in candles]
    closes = [close for _ts, close in candles]
    accumulator = DecisionAttributionAccumulator()
    decision_count = 0
    for index in range(replay_start_index, len(candles)):
        candle_ts = timestamps[index]
        if config.from_ts_ms is not None and candle_ts < int(config.from_ts_ms):
            continue
        end_prev = index
        end_curr = index + 1
        accumulator.add(
            _replay_decision_attribution(
                replay_config=config,
                config_id=config_id,
                candle_ts=candle_ts,
                close=closes[index],
                prev_s=_sma(closes, int(strategy_config.short_n), end_prev),
                prev_l=_sma(closes, int(strategy_config.long_n), end_prev),
                curr_s=_sma(closes, int(strategy_config.short_n), end_curr),
                curr_l=_sma(closes, int(strategy_config.long_n), end_curr),
            )
        )
        decision_count += 1

    return StrategyReplayResult(
        config_id=config_id,
        attribution_summary=accumulator.summary(),
        decision_count=decision_count,
        insufficient_candle_count=0,
        candle_count=len(candles),
    )
