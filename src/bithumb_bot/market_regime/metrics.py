from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class RegimePerformanceRow:
    dimension: str
    regime: str
    trade_count: int
    candle_count: int
    candle_share: float
    gross_pnl: float
    net_pnl: float
    return_pct: float
    profit_factor: float | None
    win_rate: float
    expectancy: float | None
    max_drawdown: float
    max_consecutive_losses: int
    fee_drag: float
    slippage_drag: float
    single_trade_dependency_score: float | None

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class RegimeCoverageRow:
    dimension: str
    regime: str
    candle_count: int
    candle_share: float
    trade_count: int

    def as_dict(self) -> dict[str, object]:
        return self.__dict__.copy()


def _snapshot_value(snapshot: Any, key: str) -> str:
    if isinstance(snapshot, dict):
        return str(snapshot.get(key) or "unknown")
    return str(getattr(snapshot, key, "unknown") or "unknown")


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


def aggregate_regime_coverage(*, snapshots: Iterable[Any], trades: Iterable[dict[str, Any]]) -> tuple[RegimeCoverageRow, ...]:
    snapshot_list = list(snapshots)
    trade_list = list(trades)
    total = len(snapshot_list)
    rows: list[RegimeCoverageRow] = []
    for dimension in ("price_regime", "volatility_bucket", "volume_bucket", "composite_regime"):
        candle_counts: dict[str, int] = defaultdict(int)
        trade_counts: dict[str, int] = defaultdict(int)
        for snapshot in snapshot_list:
            candle_counts[_snapshot_value(snapshot, dimension)] += 1
        for trade in trade_list:
            if str(trade.get("side") or "").upper() == "BUY":
                trade_counts[_snapshot_value(trade.get("entry_regime_snapshot"), dimension)] += 1
        for regime in sorted(set(candle_counts) | set(trade_counts)):
            candles = candle_counts.get(regime, 0)
            rows.append(
                RegimeCoverageRow(
                    dimension=dimension,
                    regime=regime,
                    candle_count=candles,
                    candle_share=(candles / total) if total else 0.0,
                    trade_count=trade_counts.get(regime, 0),
                )
            )
    return tuple(rows)


def aggregate_regime_performance(
    *,
    trades: Iterable[dict[str, Any]],
    coverage: Iterable[RegimeCoverageRow],
    start_cash: float,
) -> tuple[RegimePerformanceRow, ...]:
    closed = [trade for trade in trades if str(trade.get("side") or "").upper() == "SELL"]
    coverage_lookup = {(row.dimension, row.regime): row for row in coverage}
    rows: list[RegimePerformanceRow] = []
    for dimension in ("price_regime", "volatility_bucket", "volume_bucket", "composite_regime"):
        regimes = {row.regime for row in coverage if row.dimension == dimension}
        regimes.update(_snapshot_value(trade.get("entry_regime_snapshot"), dimension) for trade in closed)
        for regime in sorted(regimes):
            values = [
                float(trade.get("net_pnl") if trade.get("net_pnl") is not None else trade.get("closed_trade_pnl") or 0.0)
                for trade in closed
                if _snapshot_value(trade.get("entry_regime_snapshot"), dimension) == regime
            ]
            fees = [
                float(trade.get("fee_total") if trade.get("fee_total") is not None else trade.get("fee") or 0.0)
                for trade in closed
                if _snapshot_value(trade.get("entry_regime_snapshot"), dimension) == regime
            ]
            slips = [
                float(trade.get("slippage_total") or 0.0)
                for trade in closed
                if _snapshot_value(trade.get("entry_regime_snapshot"), dimension) == regime
            ]
            wins = [value for value in values if value > 0.0]
            losses = [value for value in values if value < 0.0]
            total_abs = sum(abs(value) for value in values)
            largest_abs = max((abs(value) for value in values), default=0.0)
            gross_profit = sum(wins)
            gross_loss = abs(sum(losses))
            profit_factor = (gross_profit / gross_loss) if gross_loss > 0.0 else (None if not wins else None)
            running = 0.0
            peak = 0.0
            max_dd = 0.0
            for value in values:
                running += value
                peak = max(peak, running)
                max_dd = max(max_dd, peak - running)
            coverage_row = coverage_lookup.get((dimension, regime))
            rows.append(
                RegimePerformanceRow(
                    dimension=dimension,
                    regime=regime,
                    trade_count=len(values),
                    candle_count=coverage_row.candle_count if coverage_row else 0,
                    candle_share=coverage_row.candle_share if coverage_row else 0.0,
                    gross_pnl=sum(values) + sum(fees) + sum(slips),
                    net_pnl=sum(values),
                    return_pct=(sum(values) / float(start_cash)) * 100.0 if start_cash > 0.0 else 0.0,
                    profit_factor=profit_factor,
                    win_rate=(len(wins) / len(values)) if values else 0.0,
                    expectancy=(sum(values) / len(values)) if values else None,
                    max_drawdown=max_dd,
                    max_consecutive_losses=_max_consecutive_losses(values),
                    fee_drag=sum(fees),
                    slippage_drag=sum(slips),
                    single_trade_dependency_score=(largest_abs / total_abs) if total_abs > 0.0 else None,
                )
            )
    return tuple(rows)
