from __future__ import annotations

import pytest

from bithumb_bot.research.dataset_snapshot import Candle
from bithumb_bot.research.forward_targets import compute_forward_target


def _candles() -> tuple[Candle, ...]:
    return (
        Candle(ts=0, open=10.0, high=11.0, low=9.0, close=10.0, volume=1.0),
        Candle(ts=1, open=12.0, high=13.0, low=11.0, close=12.5, volume=1.0),
        Candle(ts=2, open=13.0, high=16.0, low=10.0, close=15.0, volume=1.0),
        Candle(ts=3, open=15.0, high=17.0, low=8.0, close=14.0, volume=1.0),
        Candle(ts=4, open=14.0, high=99.0, low=1.0, close=13.0, volume=1.0),
    )


def test_forward_target_next_open_uses_next_candle_open_as_entry() -> None:
    target = compute_forward_target(candles=_candles(), index=0, horizon_steps=2)

    assert target is not None
    assert target.entry_ts == 1
    assert target.entry_price == 12.0


def test_forward_target_signal_close_uses_signal_candle_close_as_entry() -> None:
    target = compute_forward_target(
        candles=_candles(),
        index=0,
        horizon_steps=2,
        entry_price_mode="signal_close",
    )

    assert target is not None
    assert target.entry_ts == 0
    assert target.entry_price == 10.0


def test_forward_target_computes_mfe_from_highs_within_horizon() -> None:
    target = compute_forward_target(candles=_candles(), index=0, horizon_steps=2)

    assert target is not None
    assert target.mfe == pytest.approx((16.0 / 12.0) - 1.0)


def test_forward_target_computes_mae_from_lows_within_horizon() -> None:
    target = compute_forward_target(candles=_candles(), index=0, horizon_steps=2)

    assert target is not None
    assert target.mae == pytest.approx((10.0 / 12.0) - 1.0)


def test_forward_target_skips_when_horizon_exceeds_available_candles() -> None:
    assert compute_forward_target(candles=_candles(), index=3, horizon_steps=2) is None


def test_forward_target_rejects_unknown_entry_price_mode() -> None:
    with pytest.raises(ValueError, match="unknown entry_price_mode"):
        compute_forward_target(candles=_candles(), index=0, horizon_steps=1, entry_price_mode="unknown")
