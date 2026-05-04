from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketRegimeThresholds:
    min_trend_strength_ratio: float = 0.0012
    strong_trend_strength_ratio: float = 0.006
    low_volatility_ratio: float = 0.003
    high_volatility_ratio: float = 0.02
    volume_decreasing_ratio: float = 0.8
    volume_increasing_ratio: float = 1.2
    thin_liquidity_ratio: float = 0.5
    thick_liquidity_ratio: float = 1.5
