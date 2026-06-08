from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import fmean
from typing import Protocol

from bithumb_bot.market_regime import classify_market_regime_from_arrays
from bithumb_bot.research.dataset_snapshot import Candle
from bithumb_bot.research.hashing import sha256_prefixed


@dataclass(frozen=True)
class FeatureValue:
    name: str
    value: float | str | bool
    value_type: str
    feature_hash: str | None = None


class FeatureProvider(Protocol):
    name: str

    def compute(
        self,
        *,
        view: "AsOfCandleView",
    ) -> FeatureValue | None:
        ...


@dataclass(frozen=True)
class AsOfCandleView:
    candles: tuple[Candle, ...]
    index: int

    def __post_init__(self) -> None:
        if self.index < 0 or self.index >= len(self.candles):
            raise IndexError("index out of range")

    def candle(self, offset: int = 0) -> Candle:
        offset_int = int(offset)
        if offset_int > 0:
            raise ValueError("as-of candle view cannot access future candles")
        resolved = self.index + offset_int
        if resolved < 0:
            raise IndexError("offset before first candle")
        return self.candles[resolved]

    def history(self, length: int) -> tuple[Candle, ...] | None:
        n = int(length)
        if n <= 0:
            raise ValueError("history length must be positive")
        start = self.index - n + 1
        if start < 0:
            return None
        return self.candles[start : self.index + 1]


def _feature(name: str, value: float | str | bool, value_type: str) -> FeatureValue:
    return FeatureValue(
        name=name,
        value=value,
        value_type=value_type,
        feature_hash=sha256_prefixed({"name": name, "value": value, "value_type": value_type}),
    )


def _mean(values: tuple[float, ...]) -> float:
    return float(fmean(values))


def _std(values: tuple[float, ...]) -> float:
    if len(values) < 2:
        return 0.0
    avg = _mean(values)
    return sqrt(sum((value - avg) ** 2 for value in values) / len(values))


@dataclass(frozen=True)
class SmaGapProvider:
    name: str = "sma_gap"
    short_window: int = 5
    long_window: int = 20

    def compute(self, *, view: AsOfCandleView) -> FeatureValue | None:
        long_history = view.history(self.long_window)
        if long_history is None:
            return None
        short_history = view.history(self.short_window)
        if short_history is None:
            return None
        short_sma = _mean(tuple(float(candle.close) for candle in short_history))
        long_sma = _mean(tuple(float(candle.close) for candle in long_history))
        if long_sma == 0.0:
            return None
        return _feature(self.name, (short_sma - long_sma) / long_sma, "float")


@dataclass(frozen=True)
class RangeRatioProvider:
    name: str = "range_ratio"

    def compute(self, *, view: AsOfCandleView) -> FeatureValue | None:
        candle = view.candle()
        close = float(candle.close)
        if close <= 0.0:
            return None
        return _feature(self.name, (float(candle.high) - float(candle.low)) / close, "float")


@dataclass(frozen=True)
class VolumeRatioProvider:
    name: str = "volume_ratio"
    window: int = 10

    def compute(self, *, view: AsOfCandleView) -> FeatureValue | None:
        current = float(view.candle().volume)
        prior = view.history(self.window + 1)
        if prior is None:
            return None
        baseline_values = tuple(float(candle.volume) for candle in prior[:-1])
        baseline = _mean(baseline_values)
        if baseline <= 0.0:
            return None
        return _feature(self.name, current / baseline, "float")


@dataclass(frozen=True)
class BreakoutDistanceProvider:
    name: str = "breakout_distance"
    window: int = 20

    def compute(self, *, view: AsOfCandleView) -> FeatureValue | None:
        history = view.history(self.window)
        if history is None:
            return None
        current_close = float(view.candle().close)
        prior_high = max(float(candle.high) for candle in history[:-1])
        if prior_high <= 0.0:
            return None
        return _feature(self.name, (current_close - prior_high) / prior_high, "float")


@dataclass(frozen=True)
class RollingReturnProvider:
    name: str = "rolling_return"
    lookback: int = 5

    def compute(self, *, view: AsOfCandleView) -> FeatureValue | None:
        if view.index < self.lookback:
            return None
        current = float(view.candle().close)
        past = float(view.candle(-self.lookback).close)
        if past <= 0.0:
            return None
        return _feature(self.name, (current / past) - 1.0, "float")


@dataclass(frozen=True)
class ZScoreProvider:
    name: str = "zscore"
    window: int = 20

    def compute(self, *, view: AsOfCandleView) -> FeatureValue | None:
        history = view.history(self.window)
        if history is None:
            return None
        closes = tuple(float(candle.close) for candle in history)
        deviation = _std(closes)
        if deviation == 0.0:
            return _feature(self.name, 0.0, "float")
        return _feature(self.name, (float(view.candle().close) - _mean(closes)) / deviation, "float")


@dataclass(frozen=True)
class RegimeProvider:
    name: str = "regime"
    short_window: int = 5
    long_window: int = 20

    def compute(self, *, view: AsOfCandleView) -> FeatureValue | None:
        if view.index < 1:
            return None
        candles = view.candles[: view.index + 1]
        closes = tuple(float(candle.close) for candle in candles)
        highs = tuple(float(candle.high) for candle in candles)
        lows = tuple(float(candle.low) for candle in candles)
        volumes = tuple(float(candle.volume) for candle in candles)
        short_history = view.history(min(self.short_window, len(candles)))
        long_history = view.history(min(self.long_window, len(candles)))
        short_sma = _mean(tuple(float(candle.close) for candle in short_history)) if short_history else None
        long_sma = _mean(tuple(float(candle.close) for candle in long_history)) if long_history else None
        snapshot = classify_market_regime_from_arrays(
            closes=closes,
            highs=highs,
            lows=lows,
            volumes=volumes,
            index=view.index,
            short_sma=short_sma,
            long_sma=long_sma,
        )
        return _feature(self.name, snapshot.composite_regime, "str")


def feature_provider_for_name(name: str) -> FeatureProvider:
    providers: dict[str, FeatureProvider] = {
        provider.name: provider
        for provider in (
            SmaGapProvider(),
            RangeRatioProvider(),
            VolumeRatioProvider(),
            BreakoutDistanceProvider(),
            RollingReturnProvider(),
            ZScoreProvider(),
            RegimeProvider(),
        )
    }
    try:
        return providers[str(name).strip()]
    except KeyError as exc:
        allowed = ", ".join(sorted(providers))
        raise ValueError(f"unknown diagnostic feature={name!r}; allowed values: {allowed}") from exc


def feature_providers_for_names(names: tuple[str, ...]) -> tuple[FeatureProvider, ...]:
    if not names:
        raise ValueError("features must not be empty")
    return tuple(feature_provider_for_name(name) for name in names)
