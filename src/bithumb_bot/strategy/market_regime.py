from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


MARKET_REGIME_VERSION = "market_regime_v1"


@dataclass(frozen=True)
class MarketRegimeSnapshot:
    version: str
    regime: str
    regime_score: float
    trend_strength: float
    trend_direction: int
    chop_score: float
    volatility_ratio: float
    volatility_state: str
    overextension_ratio: float
    allows_entry: bool
    block_reason: str
    inputs: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "regime": self.regime,
            "regime_score": float(self.regime_score),
            "trend_strength": float(self.trend_strength),
            "trend_direction": int(self.trend_direction),
            "chop_score": float(self.chop_score),
            "volatility_ratio": float(self.volatility_ratio),
            "volatility_state": self.volatility_state,
            "overextension_ratio": float(self.overextension_ratio),
            "allows_entry": bool(self.allows_entry),
            "block_reason": self.block_reason,
            "inputs": dict(self.inputs),
        }


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


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
    normalized_closes = [float(value) for value in closes]
    last_close = float(normalized_closes[-1]) if normalized_closes else 0.0
    trend_delta = float(short_sma) - float(long_sma)
    trend_strength = abs(_safe_ratio(trend_delta, float(long_sma)))
    trend_direction = 1 if trend_delta > 0.0 else (-1 if trend_delta < 0.0 else 0)

    vol_window = max(1, int(volatility_window))
    vol_closes = normalized_closes[-vol_window:]
    vol_denominator = last_close if last_close > 0.0 else (sum(vol_closes) / len(vol_closes) if vol_closes else 0.0)
    volatility_ratio = _safe_ratio(max(vol_closes) - min(vol_closes), vol_denominator) if vol_closes else 0.0
    min_volatility = max(0.0, float(min_volatility_ratio))
    volatility_state = "low" if min_volatility > 0.0 and volatility_ratio < min_volatility else "normal"

    overext_lookback = max(1, int(overextended_lookback))
    if len(normalized_closes) > overext_lookback:
        base_close = normalized_closes[-1 - overext_lookback]
        signed_overextension = _safe_ratio(last_close - base_close, base_close)
    else:
        base_close = None
        signed_overextension = 0.0
    overextension_ratio = abs(signed_overextension)
    overextended_threshold = max(0.0, float(overextended_max_return_ratio))

    min_trend = max(0.0, float(min_trend_strength_ratio))
    chop_score = max(0.0, 1.0 - _safe_ratio(trend_strength, min_trend)) if min_trend > 0.0 else 0.0

    regime = "unknown"
    block_reason = "unknown_market_regime"
    if long_sma <= 0.0 or last_close <= 0.0:
        regime = "unknown"
        block_reason = "unknown_market_regime"
    elif overextended_threshold > 0.0 and signed_overextension > overextended_threshold:
        regime = "overextended_up"
        block_reason = "overextended_up"
    elif volatility_state == "low":
        regime = "low_vol"
        block_reason = "low_volatility"
    elif min_trend > 0.0 and trend_strength < min_trend:
        regime = "chop"
        block_reason = "chop_market"
    elif trend_direction > 0:
        regime = "trend_up"
        block_reason = "none"
    elif trend_direction < 0:
        regime = "trend_down"
        block_reason = "downtrend"

    allows_entry = bool(regime == "trend_up")
    regime_score = trend_strength if regime == "trend_up" else (1.0 - chop_score if regime == "chop" else trend_strength)

    return MarketRegimeSnapshot(
        version=MARKET_REGIME_VERSION,
        regime=regime,
        regime_score=float(max(0.0, regime_score)),
        trend_strength=float(trend_strength),
        trend_direction=int(trend_direction),
        chop_score=float(chop_score),
        volatility_ratio=float(volatility_ratio),
        volatility_state=volatility_state,
        overextension_ratio=float(overextension_ratio),
        allows_entry=allows_entry,
        block_reason=block_reason,
        inputs={
            "short_sma": float(short_sma),
            "long_sma": float(long_sma),
            "last_close": float(last_close),
            "volatility_window": int(vol_window),
            "min_volatility_ratio": float(min_volatility),
            "overextended_lookback": int(overext_lookback),
            "overextended_max_return_ratio": float(overextended_threshold),
            "min_trend_strength_ratio": float(min_trend),
            "overextension_base_close": base_close,
        },
    )
