from __future__ import annotations

from typing import Sequence

from bithumb_bot.market_regime import MARKET_REGIME_VERSION, MarketRegimeSnapshot
from bithumb_bot.market_regime.classifier import classify_sma_market_regime as _classify_sma_market_regime


def classify_sma_market_regime(
    *,
    closes: Sequence[float],
    short_sma: float,
    long_sma: float,
    volatility_window: int,
    min_volatility_ratio: float,
    overextended_lookback: int,
    overextended_max_return_ratio: float,
    min_trend_strength_ratio: float,
) -> MarketRegimeSnapshot:
    return _classify_sma_market_regime(
        closes=closes,
        short_sma=short_sma,
        long_sma=long_sma,
        volatility_window=volatility_window,
        min_volatility_ratio=min_volatility_ratio,
        overextended_lookback=overextended_lookback,
        overextended_max_return_ratio=overextended_max_return_ratio,
        min_trend_strength_ratio=min_trend_strength_ratio,
    )
