from __future__ import annotations

from bithumb_bot.research.backtest_common import metrics_v2_ledgers_from_trades
from bithumb_bot.research.backtest_types import BacktestRunContext
from bithumb_bot.strategy_plugins.daily_participation_sma import run_daily_participation_sma_backtest
from tests.test_daily_participation_sma_backtest_integration import _dataset, _params


def test_daily_fallback_execution_record_keeps_entry_signal_source() -> None:
    _, _, records, _ = metrics_v2_ledgers_from_trades(
        trades=[
            {
                "side": "BUY",
                "qty": 1.0,
                "price": 100.0,
                "fee": 1.0,
                "fill_ts": 1_704_031_200_000,
                "entry_signal_source": "daily_participation_fallback",
                "execution": {
                    "side": "BUY",
                    "fill_status": "filled",
                    "filled_qty": 1.0,
                    "avg_fill_price": 100.0,
                    "fee": 1.0,
                    "fill_reference_ts": 1_704_031_200_000,
                },
            }
        ]
    )

    assert records[0].ts == 1_704_031_200_000
    assert records[0].entry_signal_source == "daily_participation_fallback"


def test_daily_fallback_backtest_trade_keeps_entry_signal_source() -> None:
    result = run_daily_participation_sma_backtest(
        _dataset(),
        _params(),
        fee_rate=0.001,
        slippage_bps=0.0,
        context=BacktestRunContext(report_detail="full"),
    )

    trade = next(item for item in result.trades if item.get("side") == "BUY")

    assert trade["entry_signal_source"] == "daily_participation_fallback"
    assert trade["entry_sizing_source"] == "daily_participation_policy"
    assert float(trade["price"]) * float(trade["qty"]) <= 10000.0
