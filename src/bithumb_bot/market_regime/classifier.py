from __future__ import annotations

from statistics import fmean
from typing import Any, Sequence

from .schema import MARKET_REGIME_VERSION, MarketRegimeSnapshot
from .thresholds import MarketRegimeThresholds


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


def _value(item: Any, name: str, default: float = 0.0) -> float:
    if isinstance(item, dict):
        return float(item.get(name, default) or default)
    return float(getattr(item, name, default) or default)


def _bucket_ratio(value: float | None, *, low: float, high: float, low_label: str, normal_label: str, high_label: str) -> str:
    if value is None:
        return "unknown"
    if value < low:
        return low_label
    if value > high:
        return high_label
    return normal_label


def classify_market_regime(
    *,
    candles: Sequence[Any],
    short_sma: float | None = None,
    long_sma: float | None = None,
    volatility_window: int = 10,
    volume_window: int = 10,
    liquidity_window: int = 10,
    thresholds: MarketRegimeThresholds | None = None,
    overextended_lookback: int = 3,
    overextended_max_return_ratio: float = 0.0,
) -> MarketRegimeSnapshot:
    t = thresholds or MarketRegimeThresholds()
    normalized = list(candles)
    closes = [_value(candle, "close") for candle in normalized]
    highs = [_value(candle, "high", closes[index] if index < len(closes) else 0.0) for index, candle in enumerate(normalized)]
    lows = [_value(candle, "low", closes[index] if index < len(closes) else 0.0) for index, candle in enumerate(normalized)]
    volumes = [_value(candle, "volume") for candle in normalized]
    last_close = closes[-1] if closes else 0.0

    trend_delta = 0.0
    if short_sma is not None and long_sma is not None:
        trend_delta = float(short_sma) - float(long_sma)
    elif len(closes) >= 2:
        trend_delta = closes[-1] - closes[0]
    trend_base = float(long_sma) if long_sma and float(long_sma) > 0.0 else (closes[0] if closes else 0.0)
    trend_strength = abs(_safe_ratio(trend_delta, trend_base))
    trend_direction = 1 if trend_delta > 0.0 else (-1 if trend_delta < 0.0 else 0)

    min_trend = max(0.0, float(t.min_trend_strength_ratio))
    if last_close <= 0.0 or not closes:
        price_regime = "unknown"
    elif trend_strength < min_trend:
        price_regime = "sideways"
    elif trend_direction > 0:
        price_regime = "uptrend"
    elif trend_direction < 0:
        price_regime = "downtrend"
    else:
        price_regime = "sideways"

    if trend_strength < min_trend:
        trend_strength_bucket = "weak"
    elif trend_strength >= max(min_trend, float(t.strong_trend_strength_ratio)):
        trend_strength_bucket = "strong"
    else:
        trend_strength_bucket = "normal"

    vol_n = max(1, int(volatility_window))
    vol_highs = highs[-vol_n:]
    vol_lows = lows[-vol_n:]
    vol_base = last_close if last_close > 0.0 else (fmean(closes[-vol_n:]) if closes[-vol_n:] else 0.0)
    volatility_ratio = _safe_ratio(max(vol_highs) - min(vol_lows), vol_base) if vol_highs and vol_lows else 0.0
    volatility_bucket = _bucket_ratio(
        volatility_ratio,
        low=max(0.0, float(t.low_volatility_ratio)),
        high=max(float(t.low_volatility_ratio), float(t.high_volatility_ratio)),
        low_label="low_vol",
        normal_label="normal_vol",
        high_label="high_vol",
    )

    vol_window = max(1, int(volume_window))
    recent_volume = fmean(volumes[-vol_window:]) if volumes and any(value > 0.0 for value in volumes[-vol_window:]) else None
    prior = volumes[-(2 * vol_window) : -vol_window] if len(volumes) >= 2 * vol_window else volumes[:-vol_window]
    if not prior and 2 <= len(volumes) < 2 * vol_window:
        midpoint = len(volumes) // 2
        prior = volumes[:midpoint]
        recent_slice = volumes[midpoint:]
        recent_volume = fmean(recent_slice) if recent_slice and any(value > 0.0 for value in recent_slice) else recent_volume
    prior_volume = fmean(prior) if prior and any(value > 0.0 for value in prior) else None
    volume_ratio = (recent_volume / prior_volume) if recent_volume is not None and prior_volume and prior_volume > 0.0 else None
    volume_bucket = _bucket_ratio(
        volume_ratio,
        low=float(t.volume_decreasing_ratio),
        high=float(t.volume_increasing_ratio),
        low_label="volume_decreasing",
        normal_label="volume_normal",
        high_label="volume_increasing",
    )

    notionals = [max(0.0, closes[index]) * max(0.0, volumes[index]) for index in range(len(volumes))]
    liq_n = max(1, int(liquidity_window))
    recent_liquidity = fmean(notionals[-liq_n:]) if notionals and any(value > 0.0 for value in notionals[-liq_n:]) else None
    prior_liq_values = notionals[-(2 * liq_n) : -liq_n] if len(notionals) >= 2 * liq_n else notionals[:-liq_n]
    if not prior_liq_values and 2 <= len(notionals) < 2 * liq_n:
        midpoint = len(notionals) // 2
        prior_liq_values = notionals[:midpoint]
        recent_liq_values = notionals[midpoint:]
        recent_liquidity = fmean(recent_liq_values) if recent_liq_values and any(value > 0.0 for value in recent_liq_values) else recent_liquidity
    prior_liquidity = fmean(prior_liq_values) if prior_liq_values and any(value > 0.0 for value in prior_liq_values) else None
    liquidity_ratio = (
        recent_liquidity / prior_liquidity
        if recent_liquidity is not None and prior_liquidity and prior_liquidity > 0.0
        else None
    )
    liquidity_bucket = _bucket_ratio(
        liquidity_ratio,
        low=float(t.thin_liquidity_ratio),
        high=float(t.thick_liquidity_ratio),
        low_label="thin",
        normal_label="normal",
        high_label="thick",
    )

    overext_lookback = max(1, int(overextended_lookback))
    signed_overextension = 0.0
    if len(closes) > overext_lookback:
        signed_overextension = _safe_ratio(last_close - closes[-1 - overext_lookback], closes[-1 - overext_lookback])
    overextension_ratio = abs(signed_overextension)
    sma_gap_ratio = abs(_safe_ratio(float(short_sma or 0.0) - float(long_sma or 0.0), float(long_sma or 0.0))) if long_sma else None

    legacy_regime = "unknown"
    block_reason = "unknown_market_regime"
    if last_close <= 0.0:
        pass
    elif overextended_max_return_ratio > 0.0 and signed_overextension > float(overextended_max_return_ratio):
        legacy_regime = "overextended_up"
        block_reason = "overextended_up"
    elif volatility_bucket == "low_vol":
        legacy_regime = "low_vol"
        block_reason = "low_volatility"
    elif price_regime == "sideways" or trend_strength_bucket == "weak":
        legacy_regime = "chop"
        block_reason = "chop_market"
    elif price_regime == "uptrend":
        legacy_regime = "trend_up"
        block_reason = "none"
    elif price_regime == "downtrend":
        legacy_regime = "trend_down"
        block_reason = "downtrend"

    composite = "_".join([price_regime, volatility_bucket, volume_bucket])
    allows_sma_entry = bool(legacy_regime == "trend_up")
    return MarketRegimeSnapshot(
        version=MARKET_REGIME_VERSION,
        price_regime=price_regime,
        trend_strength_bucket=trend_strength_bucket,
        volatility_bucket=volatility_bucket,
        volume_bucket=volume_bucket,
        liquidity_bucket=liquidity_bucket,
        composite_regime=composite,
        allows_sma_entry=allows_sma_entry,
        block_reason=block_reason,
        trend_strength=float(trend_strength),
        trend_direction=int(trend_direction),
        volatility_ratio=float(volatility_ratio),
        volume_ratio=volume_ratio,
        liquidity_ratio=liquidity_ratio,
        sma_gap_ratio=sma_gap_ratio,
        legacy_regime=legacy_regime,
        inputs={
            "candle_count": len(normalized),
            "short_sma": short_sma,
            "long_sma": long_sma,
            "volatility_window": int(vol_n),
            "volume_window": int(vol_window),
            "liquidity_window": int(liq_n),
            "min_trend_strength_ratio": float(min_trend),
            "overextension_ratio": float(overextension_ratio),
            "overextended_lookback": int(overext_lookback),
            "overextended_max_return_ratio": float(overextended_max_return_ratio),
        },
    )


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
    candles = [
        {"close": float(close), "high": float(close), "low": float(close), "volume": 0.0}
        for close in closes
    ]
    return classify_market_regime(
        candles=candles,
        short_sma=float(short_sma),
        long_sma=float(long_sma),
        volatility_window=int(volatility_window),
        thresholds=MarketRegimeThresholds(
            min_trend_strength_ratio=max(0.0, float(min_trend_strength_ratio)),
            low_volatility_ratio=max(0.0, float(min_volatility_ratio)),
        ),
        overextended_lookback=int(overextended_lookback),
        overextended_max_return_ratio=float(overextended_max_return_ratio),
    )
