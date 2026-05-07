from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.market_regime import (
    RegimeCoverageRow,
    RegimePerformanceRow,
    aggregate_regime_coverage,
    aggregate_regime_performance,
    classify_market_regime,
)
from bithumb_bot.market_regime.thresholds import MarketRegimeThresholds

from .dataset_snapshot import DatasetSnapshot
from .execution_model import ExecutionModel, ExecutionRequest, FixedBpsExecutionModel
from .metrics import ResearchMetrics


START_CASH_KRW = 1_000_000.0
BUY_FRACTION = 0.99


@dataclass(frozen=True)
class BacktestRun:
    metrics: ResearchMetrics
    trades: tuple[dict[str, object], ...]
    candle_count: int
    warnings: tuple[str, ...]
    regime_performance: tuple[RegimePerformanceRow, ...] = ()
    regime_coverage: tuple[RegimeCoverageRow, ...] = ()


def run_sma_backtest(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
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
    warnings: list[str] = []
    if len(candles) < long_n + 2:
        return BacktestRun(
            metrics=_empty_metrics(parameter_stability_score),
            trades=(),
            candle_count=len(candles),
            warnings=("not_enough_candles",),
            regime_performance=(),
            regime_coverage=(),
        )

    closes = [candle.close for candle in candles]
    regime_snapshots: list[dict[str, object]] = []
    thresholds = MarketRegimeThresholds(
        min_trend_strength_ratio=max(0.0, min_gap),
        low_volatility_ratio=max(0.0, min_range),
    )
    cash = START_CASH_KRW
    qty = 0.0
    entry_cost_basis = 0.0
    entry_regime_snapshot: dict[str, object] | None = None
    entry_fee = 0.0
    entry_slippage = 0.0
    peak = START_CASH_KRW
    max_drawdown = 0.0
    fee_total = 0.0
    slippage_total = 0.0
    trades: list[dict[str, object]] = []
    closed_pnls: list[float] = []
    prev_above: bool | None = None
    model = execution_model or FixedBpsExecutionModel(fee_rate=fee_rate, slippage_bps=slippage_bps)

    for index in range(long_n, len(candles)):
        candle = candles[index]
        prev_short = _sma(closes, short_n, index)
        prev_long = _sma(closes, long_n, index)
        curr_short = _sma(closes, short_n, index + 1)
        curr_long = _sma(closes, long_n, index + 1)
        above = curr_short > curr_long
        gap_ratio = abs(curr_short - curr_long) / curr_long if curr_long > 0.0 else 0.0
        range_ratio = (candle.high - candle.low) / candle.close if candle.close > 0.0 else 0.0
        regime_snapshot = classify_market_regime(
            candles=candles[: index + 1],
            short_sma=curr_short,
            long_sma=curr_long,
            volatility_window=max(1, int(parameter_values.get("SMA_FILTER_VOL_WINDOW", 10))),
            thresholds=thresholds,
            overextended_lookback=max(1, int(parameter_values.get("SMA_FILTER_OVEREXT_LOOKBACK", 3))),
            overextended_max_return_ratio=float(parameter_values.get("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", 0.0)),
        ).as_dict()
        regime_snapshots.append(regime_snapshot)

        action = "HOLD"
        if gap_ratio >= min_gap and range_ratio >= min_range and prev_above is not None:
            if not prev_above and above and qty <= 0.0:
                action = "BUY"
            elif prev_above and not above and qty > 0.0:
                action = "SELL"

        if action == "BUY":
            spend = cash * BUY_FRACTION
            fill = model.simulate(
                ExecutionRequest(
                    signal_ts=candle.ts,
                    decision_ts=candle.ts,
                    side="BUY",
                    reference_price=candle.close,
                    requested_notional=spend,
                    fee_rate=fee_rate,
                    **_quote_request_fields(dataset, candle.ts),
                )
            )
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                prev_above = above
                continue
            exec_price = float(fill.avg_fill_price)
            fee = fill.fee
            received_qty = fill.filled_qty
            actual_spend = (exec_price * received_qty) + fee
            reference_cost = candle.close * received_qty
            slipped_cost = exec_price * received_qty
            buy_slippage = max(0.0, slipped_cost - reference_cost)
            slippage_total += buy_slippage
            cash -= actual_spend
            qty += received_qty
            entry_cost_basis = actual_spend
            entry_regime_snapshot = dict(regime_snapshot)
            entry_fee = fee
            entry_slippage = buy_slippage
            fee_total += fee
            trades.append(
                _trade_from_fill(
                    fill,
                    cash=cash,
                    asset_qty=qty,
                    pnl=None,
                    entry_regime_snapshot=entry_regime_snapshot,
                    exit_regime_snapshot=None,
                    net_pnl=None,
                    fee_total=fee,
                    slippage_total=buy_slippage,
                )
            )
        elif action == "SELL":
            fill = model.simulate(
                ExecutionRequest(
                    signal_ts=candle.ts,
                    decision_ts=candle.ts,
                    side="SELL",
                    reference_price=candle.close,
                    requested_qty=qty,
                    fee_rate=fee_rate,
                    **_quote_request_fields(dataset, candle.ts),
                )
            )
            if fill.fill_status == "failed" or fill.avg_fill_price is None or fill.filled_qty <= 0.0:
                trades.append(_trade_from_fill(fill, cash=cash, asset_qty=qty, pnl=None))
                prev_above = above
                continue
            exec_price = float(fill.avg_fill_price)
            sell_qty = fill.filled_qty
            gross = sell_qty * exec_price
            fee = fill.fee
            reference_proceeds = candle.close * sell_qty
            sell_slippage = max(0.0, reference_proceeds - gross)
            slippage_total += sell_slippage
            net_proceeds = gross - fee
            filled_fraction = sell_qty / qty if qty > 0.0 else 0.0
            pnl = net_proceeds - (entry_cost_basis * filled_fraction)
            cash += net_proceeds
            qty = max(0.0, qty - sell_qty)
            entry_cost_basis = entry_cost_basis * (1.0 - filled_fraction) if qty > 0.0 else 0.0
            fee_total += fee
            if fill.fill_status == "filled":
                closed_pnls.append(pnl)
            trade_fee_total = entry_fee + fee
            trade_slippage_total = entry_slippage + sell_slippage
            trades.append(
                _trade_from_fill(
                    fill,
                    cash=cash,
                    asset_qty=qty,
                    pnl=pnl,
                    entry_regime_snapshot=entry_regime_snapshot,
                    exit_regime_snapshot=dict(regime_snapshot),
                    net_pnl=pnl,
                    fee_total=trade_fee_total,
                    slippage_total=trade_slippage_total,
                )
            )
            if qty <= 0.0:
                entry_regime_snapshot = None
                entry_fee = 0.0
                entry_slippage = 0.0

        equity = cash + qty * candle.close
        peak = max(peak, equity)
        if peak > 0.0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
        prev_above = above

    last = candles[-1]
    final_equity = cash + qty * last.close
    return_pct = ((final_equity / START_CASH_KRW) - 1.0) * 100.0
    metrics = _metrics(
        return_pct=return_pct,
        max_drawdown_pct=max_drawdown * 100.0,
        closed_pnls=closed_pnls,
        fee_total=fee_total,
        slippage_total=slippage_total,
        parameter_stability_score=parameter_stability_score,
    )
    coverage = aggregate_regime_coverage(snapshots=regime_snapshots, trades=trades)
    performance = aggregate_regime_performance(trades=trades, coverage=coverage, start_cash=START_CASH_KRW)
    return BacktestRun(
        metrics=metrics,
        trades=tuple(trades),
        candle_count=len(candles),
        warnings=tuple(warnings),
        regime_performance=performance,
        regime_coverage=coverage,
    )


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


def _quote_request_fields(dataset: DatasetSnapshot, ts: int) -> dict[str, object]:
    quote = dataset.top_of_book_for_ts(ts)
    if quote is None:
        return {}
    return {
        "best_bid": quote.bid_price,
        "best_ask": quote.ask_price,
        "spread_bps": quote.spread_bps,
    }


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
    trade["execution"] = fill.as_dict()
    return trade


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
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else (float("inf") if wins else None)
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
