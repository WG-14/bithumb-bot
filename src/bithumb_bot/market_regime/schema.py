from __future__ import annotations

from dataclasses import dataclass
from typing import Any


MARKET_REGIME_VERSION = "market_regime_v2"


@dataclass(frozen=True)
class MarketRegimeSnapshot:
    version: str
    price_regime: str
    trend_strength_bucket: str
    volatility_bucket: str
    volume_bucket: str
    liquidity_bucket: str
    composite_regime: str
    allows_sma_entry: bool
    block_reason: str
    trend_strength: float
    trend_direction: int
    volatility_ratio: float
    volume_ratio: float | None
    liquidity_ratio: float | None
    sma_gap_ratio: float | None
    legacy_regime: str
    inputs: dict[str, Any]

    @property
    def regime(self) -> str:
        return self.legacy_regime

    @property
    def allows_entry(self) -> bool:
        return self.allows_sma_entry

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "regime_classifier_version": self.version,
            "price_regime": self.price_regime,
            "trend_strength_bucket": self.trend_strength_bucket,
            "volatility_bucket": self.volatility_bucket,
            "volume_bucket": self.volume_bucket,
            "liquidity_bucket": self.liquidity_bucket,
            "composite_regime": self.composite_regime,
            "allows_sma_entry": bool(self.allows_sma_entry),
            "block_reason": self.block_reason,
            "trend_strength": float(self.trend_strength),
            "trend_direction": int(self.trend_direction),
            "volatility_ratio": float(self.volatility_ratio),
            "volume_ratio": self.volume_ratio,
            "liquidity_ratio": self.liquidity_ratio,
            "sma_gap_ratio": self.sma_gap_ratio,
            "legacy_regime": self.legacy_regime,
            "inputs": dict(self.inputs),
            # Compatibility fields consumed by existing live decision tests.
            "regime": self.legacy_regime,
            "regime_score": float(self.trend_strength),
            "chop_score": float(max(0.0, 1.0 - self.trend_strength)),
            "volatility_state": "low" if self.volatility_bucket == "low_vol" else "normal",
            "overextension_ratio": float(self.inputs.get("overextension_ratio", 0.0) or 0.0),
            "allows_entry": bool(self.allows_sma_entry),
        }
