from __future__ import annotations

from bithumb_bot.research.backtest_types import BacktestRunContext
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy
from bithumb_bot.strategy_plugins.daily_participation_sma import (
    build_daily_participation_sma_research_events,
    run_daily_participation_sma_backtest,
)


def _dataset() -> DatasetSnapshot:
    start = 1_704_031_200_000
    closes = (100.0, 101.0, 102.0, 103.0, 104.0, 105.0)
    return DatasetSnapshot(
        snapshot_id="daily_participation_fixture",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2024-01-01", "2024-01-01"),
        candles=tuple(
            Candle(ts=start + index * 60_000, open=close, high=close, low=close, close=close, volume=1.0)
            for index, close in enumerate(closes)
        ),
    )


def _params() -> dict[str, object]:
    return {
        "SMA_SHORT": 2,
        "SMA_LONG": 4,
        "DAILY_PARTICIPATION_ENABLED": True,
        "DAILY_PARTICIPATION_TIMEZONE": "Asia/Seoul",
        "DAILY_PARTICIPATION_COUNT_BASIS": "filled",
        "DAILY_PARTICIPATION_WINDOW_START_HOUR_KST": 0,
        "DAILY_PARTICIPATION_WINDOW_END_HOUR_KST": 24,
        "DAILY_PARTICIPATION_BUY_FRACTION": 0.05,
        "DAILY_PARTICIPATION_MAX_ORDER_KRW": 10000.0,
    }


def _run():
    return run_daily_participation_sma_backtest(
        _dataset(),
        _params(),
        fee_rate=0.001,
        slippage_bps=0.0,
        context=BacktestRunContext(report_detail="full"),
    )


def test_daily_participation_sma_research_builder_emits_fallback_buy() -> None:
    result = _run()

    fallback = [
        decision for decision in result.decisions
        if decision.get("entry_signal_source") == "daily_participation_fallback"
    ]
    assert fallback
    assert fallback[0]["final_signal"] == "BUY"
    assert fallback[0]["entry_signal_source"] == "daily_participation_fallback"


def test_seed_hold_event_is_not_final_authority_for_daily_strategy() -> None:
    events = build_daily_participation_sma_research_events(
        dataset=_dataset(),
        parameter_values=_params(),
        fee_rate=0.001,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(),
    )
    assert events
    assert {event.final_signal for event in events} == {"HOLD"}

    result = _run()

    assert any(decision.get("final_signal") == "BUY" for decision in result.decisions)


def test_daily_participation_backtest_uses_participation_sizing_in_trade_ledger() -> None:
    result = _run()

    trade = next(
        item for item in result.trades
        if item.get("side") == "BUY" and item.get("entry_signal_source") == "daily_participation_fallback"
    )

    assert trade["entry_signal_source"] == "daily_participation_fallback"
    assert trade["entry_sizing_source"] == "daily_participation_policy"
    assert float(trade["price"]) * float(trade["qty"]) <= 10000.0
    assert float(trade["price"]) * float(trade["qty"]) > 0.0
