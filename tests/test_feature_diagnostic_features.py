from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.research.dataset_snapshot import Candle
from bithumb_bot.research.feature_diagnostic_features import (
    AsOfCandleView,
    RollingReturnProvider,
    SmaGapProvider,
    VolumeRatioProvider,
    feature_provider_for_name,
)


ROOT = Path(__file__).resolve().parents[1]


def _candles(count: int = 30) -> tuple[Candle, ...]:
    return tuple(
        Candle(
            ts=index,
            open=100.0 + index,
            high=101.0 + index,
            low=99.0 + index,
            close=100.0 + index,
            volume=10.0 + index,
        )
        for index in range(count)
    )


def test_as_of_candle_view_rejects_positive_offset() -> None:
    view = AsOfCandleView(candles=_candles(), index=10)

    with pytest.raises(ValueError, match="future candles"):
        view.candle(1)


def test_sma_gap_uses_only_candles_at_or_before_index() -> None:
    candles = _candles()
    changed_future = list(candles)
    changed_future[25] = Candle(ts=25, open=999.0, high=999.0, low=999.0, close=999.0, volume=999.0)

    provider = SmaGapProvider(short_window=3, long_window=5)
    baseline = provider.compute(view=AsOfCandleView(candles=candles, index=10))
    future_changed = provider.compute(view=AsOfCandleView(candles=tuple(changed_future), index=10))

    assert baseline == future_changed


def test_volume_ratio_uses_only_candles_at_or_before_index() -> None:
    candles = _candles()
    changed_future = list(candles)
    changed_future[20] = Candle(ts=20, open=1.0, high=1.0, low=1.0, close=1.0, volume=1_000_000.0)

    provider = VolumeRatioProvider(window=3)
    baseline = provider.compute(view=AsOfCandleView(candles=candles, index=10))
    future_changed = provider.compute(view=AsOfCandleView(candles=tuple(changed_future), index=10))

    assert baseline == future_changed


def test_rolling_return_returns_none_until_lookback_available() -> None:
    provider = RollingReturnProvider(lookback=5)

    assert provider.compute(view=AsOfCandleView(candles=_candles(), index=4)) is None


def test_feature_provider_does_not_import_forward_targets() -> None:
    source = (ROOT / "src/bithumb_bot/research/feature_diagnostic_features.py").read_text(encoding="utf-8")

    assert "from bithumb_bot.research.forward_targets import ForwardTarget" not in source
    assert "ForwardTarget" not in source


def test_unknown_feature_name_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown diagnostic feature"):
        feature_provider_for_name("does_not_exist")
