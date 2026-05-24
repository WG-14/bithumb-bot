from __future__ import annotations

import inspect
from dataclasses import replace

from bithumb_bot.research import backtest_engine, backtest_kernel
import bithumb_bot.research.strategy_registry as strategy_registry
from bithumb_bot.research.backtest_engine import BacktestRunContext
from bithumb_bot.research.backtest_kernel import BacktestKernel, run_decision_event_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.strategy.exit_rules import create_exit_rules, create_sma_exit_rules


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


def test_decision_event_backtest_kernel_executes_sell_without_sma_fields() -> None:
    dataset = DatasetSnapshot(
        snapshot_id="kernel_sell_contract",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=tuple(
            Candle(index * 60_000, 100.0 + index, 100.0 + index, 100.0 + index, 100.0 + index, 1.0)
            for index in range(5)
        ),
    )
    events = (
        ResearchDecisionEvent(
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
        ),
        ResearchDecisionEvent(
            candle_ts=dataset.candles[3].ts,
            decision_ts=dataset.candles[3].ts + 60_000,
            strategy_name="buy_and_hold_baseline",
            strategy_version="buy_and_hold_baseline.research_contract.v1",
            raw_signal="SELL",
            final_signal="SELL",
            reason="kernel_contract_sell",
            feature_snapshot={"candle_index": 3, "close": dataset.candles[3].close},
            strategy_diagnostics={"schema_version": 1, "emitted_sell_intent": True},
            entry_signal="HOLD",
            exit_signal="SELL",
        ),
    )

    result = run_decision_event_backtest(
        dataset=dataset,
        strategy_name="buy_and_hold_baseline",
        parameter_values={"BUY_HOLD_BUY_INDEX": 1, "BUY_HOLD_DECISION_REASON": "kernel_contract_buy"},
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=events,
        context=BacktestRunContext(report_detail="full"),
    )

    assert [trade["side"] for trade in result.trades] == ["BUY", "SELL"]
    assert result.trades[-1]["is_portfolio_applied_trade"] is True
    assert result.execution_event_summary["execution_attempt_count"] == 2
    assert result.metrics_v2.cost_execution.filled_execution_count == 2
    assert result.metrics_v2.return_risk.open_position_at_end is False
    assert result.strategy_diagnostics["strategy_diagnostics_namespace"] == "buy_and_hold_baseline"
    assert result.resource_usage["common_decision_behavior_hash"].startswith("sha256:")
    assert result.resource_usage["strategy_behavior_hash"].startswith("sha256:")


def test_decision_event_backtest_kernel_evaluates_exit_intent_without_sma_fields() -> None:
    dataset = DatasetSnapshot(
        snapshot_id="kernel_exit_intent_contract",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=(
            Candle(0, 100.0, 100.0, 100.0, 100.0, 1.0),
            Candle(60_000, 100.0, 100.0, 100.0, 100.0, 1.0),
            Candle(120_000, 90.0, 90.0, 90.0, 90.0, 1.0),
            Candle(180_000, 90.0, 90.0, 90.0, 90.0, 1.0),
        ),
    )
    events = (
        ResearchDecisionEvent(
            candle_ts=dataset.candles[0].ts,
            decision_ts=dataset.candles[0].ts + 60_000,
            strategy_name="sma_with_filter",
            strategy_version="sma_with_filter.research_contract.v1",
            raw_signal="BUY",
            final_signal="BUY",
            reason="kernel_contract_entry",
            feature_snapshot={"candle_index": 0, "close": dataset.candles[0].close},
            strategy_diagnostics={"schema_version": 1, "adapter": "manual_kernel_contract"},
            entry_signal="BUY",
            order_intent={"side": "BUY"},
            exit_intent={"mode": "evaluate_exit_policy", "base_signal": "BUY"},
        ),
        ResearchDecisionEvent(
            candle_ts=dataset.candles[2].ts,
            decision_ts=dataset.candles[2].ts + 60_000,
            strategy_name="sma_with_filter",
            strategy_version="sma_with_filter.research_contract.v1",
            raw_signal="HOLD",
            final_signal="HOLD",
            reason="kernel_contract_hold",
            feature_snapshot={"candle_index": 2, "close": dataset.candles[2].close},
            strategy_diagnostics={"schema_version": 1, "adapter": "manual_kernel_contract"},
            entry_signal="HOLD",
            exit_signal="HOLD",
            exit_intent={"mode": "evaluate_exit_policy", "base_signal": "HOLD"},
        ),
    )

    result = run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 4,
            "STRATEGY_EXIT_RULES": "stop_loss",
            "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.05,
            "STRATEGY_EXIT_MAX_HOLDING_MIN": 0,
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0,
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=events,
        context=BacktestRunContext(report_detail="full"),
    )

    assert [trade["side"] for trade in result.trades] == ["BUY", "SELL"]
    assert result.decisions[-1]["raw_signal"] == "HOLD"
    assert result.decisions[-1]["final_signal"] == "SELL"
    assert result.decisions[-1]["exit_rule"] == "stop_loss"
    assert result.decisions[-1]["exit_evaluations"][0]["rule"] == "stop_loss"
    assert result.strategy_diagnostics["stop_loss_exit_count"] == 1
    assert result.resource_usage["common_decision_behavior_hash"].startswith("sha256:")
    assert result.resource_usage["composite_behavior_hash_v2"].startswith("sha256:")


def test_plugin_exit_factory_cannot_remove_common_stop_loss(monkeypatch) -> None:
    dataset = DatasetSnapshot(
        snapshot_id="kernel_exit_common_risk_preserved",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=(
            Candle(0, 100.0, 100.0, 100.0, 100.0, 1.0),
            Candle(60_000, 100.0, 100.0, 100.0, 100.0, 1.0),
            Candle(120_000, 90.0, 90.0, 90.0, 90.0, 1.0),
        ),
    )
    plugin = strategy_registry.resolve_research_strategy_plugin("sma_with_filter")
    changed = replace(plugin, exit_rule_factory=lambda _policy, _params, _fee: [])
    monkeypatch.setitem(strategy_registry._RESEARCH_STRATEGY_PLUGINS, "sma_with_filter", changed)
    events = (
        ResearchDecisionEvent(
            candle_ts=dataset.candles[0].ts,
            decision_ts=dataset.candles[0].ts + 60_000,
            strategy_name="sma_with_filter",
            strategy_version="sma_with_filter.research_contract.v1",
            raw_signal="BUY",
            final_signal="BUY",
            reason="entry",
            feature_snapshot={"close": 100.0},
            strategy_diagnostics={},
            entry_signal="BUY",
            order_intent={"side": "BUY"},
            exit_intent={"mode": "evaluate_exit_policy"},
        ),
        ResearchDecisionEvent(
            candle_ts=dataset.candles[2].ts,
            decision_ts=dataset.candles[2].ts + 60_000,
            strategy_name="sma_with_filter",
            strategy_version="sma_with_filter.research_contract.v1",
            raw_signal="HOLD",
            final_signal="HOLD",
            reason="hold",
            feature_snapshot={"close": 90.0},
            strategy_diagnostics={},
            entry_signal="HOLD",
            exit_signal="HOLD",
            exit_intent={"mode": "evaluate_exit_policy"},
        ),
    )

    result = run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 4,
            "STRATEGY_EXIT_RULES": "stop_loss",
            "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.05,
            "STRATEGY_EXIT_MAX_HOLDING_MIN": 0,
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0,
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=events,
        context=BacktestRunContext(report_detail="full"),
    )

    assert result.decisions[-1]["exit_rule"] == "stop_loss"
    assert result.decisions[-1]["exit_evaluations"][0]["rule"] == "stop_loss"
    assert result.decisions[-1]["exit_evaluations"][0]["rule_source"] == "common_risk"


def test_common_and_sma_exit_factories_keep_strategy_owned_opposite_cross_boundary() -> None:
    try:
        create_exit_rules(rule_names=["opposite_cross"], max_holding_sec=0.0)
    except ValueError as exc:
        assert "unknown exit rule='opposite_cross'" in str(exc)
    else:
        raise AssertionError("common create_exit_rules accepted opposite_cross")

    rules = create_sma_exit_rules(
        rule_names=["stop_loss", "opposite_cross", "max_holding_time"],
        max_holding_sec=60.0,
        min_take_profit_ratio=0.0,
        live_fee_rate_estimate=0.0,
        small_loss_tolerance_ratio=0.0,
        stop_loss_ratio=0.01,
    )

    assert [rule.name for rule in rules] == ["stop_loss", "opposite_cross", "max_holding_time"]


def test_backtest_kernel_class_preserves_decision_event_api_behavior() -> None:
    dataset = DatasetSnapshot(
        snapshot_id="kernel_class_contract",
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
        reason="kernel_class_contract_buy",
        feature_snapshot={"candle_index": 1, "close": dataset.candles[1].close},
        strategy_diagnostics={"schema_version": 1, "emitted_buy_intent": True},
        entry_signal="BUY",
        order_intent={"side": "BUY"},
    )
    kwargs = {
        "dataset": dataset,
        "strategy_name": "buy_and_hold_baseline",
        "parameter_values": {
            "BUY_HOLD_BUY_INDEX": 1,
            "BUY_HOLD_DECISION_REASON": "kernel_class_contract_buy",
        },
        "fee_rate": 0.001,
        "slippage_bps": 5.0,
        "decision_events": (event,),
    }

    via_function = run_decision_event_backtest(**kwargs, context=BacktestRunContext(report_detail="full"))
    via_class = BacktestKernel().run(**kwargs, context=BacktestRunContext(report_detail="full"))

    assert via_class.execution_event_summary == via_function.execution_event_summary
    assert via_class.metrics_v2.as_dict() == via_function.metrics_v2.as_dict()
    assert via_class.resource_usage["common_decision_behavior_hash"] == via_function.resource_usage[
        "common_decision_behavior_hash"
    ]
    assert via_class.resource_usage["trade_ledger_hash"] == via_function.resource_usage["trade_ledger_hash"]


def test_backtest_kernel_module_owns_decision_event_implementation() -> None:
    source = inspect.getsource(backtest_kernel.run_decision_event_backtest)
    implementation_source = inspect.getsource(backtest_kernel._run_decision_event_backtest_impl)

    assert "_run_decision_event_backtest_impl(" in source
    assert "from .backtest_engine import _run_decision_event_backtest_impl" not in source
    assert "Execute strategy decision events through the shared research backtest kernel" in implementation_source
    assert "resolve_research_strategy_plugin(strategy_name)" in implementation_source


def test_backtest_kernel_does_not_own_sma_specific_exit_rule_names() -> None:
    implementation_source = inspect.getsource(backtest_kernel._run_decision_event_backtest_impl)

    assert "opposite_cross" not in implementation_source
    assert "strategy_plugin.exit_rule_factory" in implementation_source
    assert "common_rules" in implementation_source


def test_backtest_engine_public_entrypoint_delegates_to_kernel_boundary() -> None:
    source = inspect.getsource(backtest_engine.run_decision_event_backtest)

    assert "Compatibility wrapper for the common backtest kernel boundary" in source
    assert "from .backtest_kernel import run_decision_event_backtest as _run_decision_event_backtest" in source
