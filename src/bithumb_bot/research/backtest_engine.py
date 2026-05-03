from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .dataset_snapshot import DatasetSnapshot
from .metrics import ResearchMetrics


START_CASH_KRW = 1_000_000.0
BUY_FRACTION = 0.99


@dataclass(frozen=True)
class BacktestRun:
    metrics: ResearchMetrics
    trades: tuple[dict[str, object], ...]
    candle_count: int
    warnings: tuple[str, ...]


def run_sma_backtest(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
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
        )

    closes = [candle.close for candle in candles]
    cash = START_CASH_KRW
    qty = 0.0
    entry_cost_basis = 0.0
    peak = START_CASH_KRW
    max_drawdown = 0.0
    fee_total = 0.0
    slippage_total = 0.0
    trades: list[dict[str, object]] = []
    closed_pnls: list[float] = []
    prev_above: bool | None = None
    slip = float(slippage_bps) / 10_000.0

    for index in range(long_n, len(candles)):
        candle = candles[index]
        prev_short = _sma(closes, short_n, index)
        prev_long = _sma(closes, long_n, index)
        curr_short = _sma(closes, short_n, index + 1)
        curr_long = _sma(closes, long_n, index + 1)
        above = curr_short > curr_long
        gap_ratio = abs(curr_short - curr_long) / curr_long if curr_long > 0.0 else 0.0
        range_ratio = (candle.high - candle.low) / candle.close if candle.close > 0.0 else 0.0

        action = "HOLD"
        if gap_ratio >= min_gap and range_ratio >= min_range and prev_above is not None:
            if not prev_above and above and qty <= 0.0:
                action = "BUY"
            elif prev_above and not above and qty > 0.0:
                action = "SELL"

        if action == "BUY":
            spend = cash * BUY_FRACTION
            exec_price = candle.close * (1.0 + slip)
            fee = spend * fee_rate
            received_qty = (spend - fee) / exec_price if exec_price > 0.0 else 0.0
            reference_cost = candle.close * received_qty
            slipped_cost = exec_price * received_qty
            slippage_total += max(0.0, slipped_cost - reference_cost)
            cash -= spend
            qty += received_qty
            entry_cost_basis = spend
            fee_total += fee
            trades.append(_trade(candle.ts, "BUY", exec_price, received_qty, fee, cash, qty, None))
        elif action == "SELL":
            exec_price = candle.close * (1.0 - slip)
            sell_qty = qty
            gross = sell_qty * exec_price
            fee = gross * fee_rate
            reference_proceeds = candle.close * sell_qty
            slippage_total += max(0.0, reference_proceeds - gross)
            net_proceeds = gross - fee
            pnl = net_proceeds - entry_cost_basis
            cash += net_proceeds
            qty = 0.0
            entry_cost_basis = 0.0
            fee_total += fee
            closed_pnls.append(pnl)
            trades.append(_trade(candle.ts, "SELL", exec_price, sell_qty, fee, cash, qty, pnl))

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
    return BacktestRun(
        metrics=metrics,
        trades=tuple(trades),
        candle_count=len(candles),
        warnings=tuple(warnings),
    )


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


def _trade(
    ts: int,
    side: str,
    price: float,
    qty: float,
    fee: float,
    cash: float,
    asset_qty: float,
    pnl: float | None,
) -> dict[str, object]:
    return {
        "ts": int(ts),
        "side": side,
        "price": float(price),
        "qty": float(qty),
        "fee": float(fee),
        "cash": float(cash),
        "asset_qty": float(asset_qty),
        "closed_trade_pnl": pnl,
    }


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
