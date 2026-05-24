from __future__ import annotations

from bithumb_bot.research.backtest_engine import BacktestRunContext, run_decision_event_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.experiment_manifest import DateRange


def test_decision_event_backtest_kernel_executes_buy_and_updates_portfolio() -> None:
    dataset = DatasetSnapshot(
        snapshot_id="kernel_contract",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=tuple(
            Candle(index * 60_000, 100.0 + index, 100.0 + index, 100.0 + index, 100.0 + index, 1.0)
            for index in range(4)
        ),
    )
    event = ResearchDecisionEvent(
        candle_ts=dataset.candles[1].ts,
        decision_ts=dataset.candles[1].ts + 60_000,
        strategy_name="buy_and_hold_baseline",
        strategy_version="buy_and_hold_baseline.research_contract.v1",
        raw_signal="BUY",
        final_signal="BUY",
        reason="kernel_contract_buy",
        feature_snapshot={"candle_index": 1, "close": dataset.candles[1].close},
        strategy_diagnostics={"schema_version": 1, "emitted_buy_intent": True},
        entry_signal="BUY",
        order_intent={"side": "BUY"},
    )

    result = run_decision_event_backtest(
        dataset=dataset,
        strategy_name="buy_and_hold_baseline",
        parameter_values={"BUY_HOLD_BUY_INDEX": 1, "BUY_HOLD_DECISION_REASON": "kernel_contract_buy"},
        fee_rate=0.001,
        slippage_bps=5.0,
        decision_events=(event,),
        context=BacktestRunContext(report_detail="full"),
    )

    assert result.trades
    assert result.trades[0]["side"] == "BUY"
    assert result.trades[0]["is_portfolio_applied_trade"] is True
    assert result.trades[0]["cash"] < 1_000_000.0
    assert result.trades[0]["asset_qty"] > 0.0
    assert result.execution_event_summary is not None
    assert result.execution_event_summary["execution_attempt_count"] == 1
    assert result.metrics_v2 is not None
    assert result.metrics_v2.cost_execution.filled_execution_count == 1
    assert result.metrics_v2.return_risk.open_position_at_end is True
    assert result.resource_usage is not None
    assert result.resource_usage["composite_behavior_hash_v2"].startswith("sha256:")
