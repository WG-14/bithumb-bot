from __future__ import annotations

import inspect
from dataclasses import replace
from pathlib import Path

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.research import backtest_engine, backtest_kernel, backtest_support
import bithumb_bot.research.strategy_registry as strategy_registry
from bithumb_bot.research.backtest_engine import BacktestRunContext
from bithumb_bot.research.backtest_kernel import BacktestKernel, run_decision_event_backtest
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.research.strategy_registry import (
    ResearchStrategyPlugin,
    StrategyRuntimeCapabilities,
)
from bithumb_bot.research.strategy_spec import StrategySpec
from bithumb_bot.strategy.exit_rules import create_exit_rules, create_sma_exit_rules
from bithumb_bot.strategy_policy_contract import (
    EntryExecutionIntent,
    ExitExecutionIntent,
    PositionSnapshot,
    StrategyDecisionV2,
)


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
    assert result.decisions[0]["execution_plan_bundle_present"] is True
    assert result.decisions[0]["execution_plan_status"] == "PLANNED"
    assert result.decisions[0]["submit_plan_source"] == "research_backtest"
    assert result.decisions[0]["execution_submit_plan_hash"].startswith("sha256:")
    assert result.execution_event_summary is not None
    assert result.execution_event_summary["execution_attempt_count"] == 1
    assert result.metrics_v2 is not None
    assert result.metrics_v2.cost_execution.filled_execution_count == 1
    assert result.metrics_v2.return_risk.open_position_at_end is True
    assert result.resource_usage is not None
    assert result.resource_usage["composite_behavior_hash_v2"].startswith("sha256:")


def test_decision_event_backtest_uses_typed_execution_service_boundary(monkeypatch) -> None:
    dataset = DatasetSnapshot(
        snapshot_id="kernel_typed_execution_service",
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
    calls = {"execute": 0}
    real_service = backtest_kernel.ResearchVirtualExecutionService

    class SpyResearchVirtualExecutionService(real_service):
        def execute(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls["execute"] += 1
            return super().execute(*args, **kwargs)

    monkeypatch.setattr(
        backtest_kernel,
        "ResearchVirtualExecutionService",
        SpyResearchVirtualExecutionService,
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

    assert calls["execute"] == 1
    assert result.trades
    assert result.decisions[0]["typed_execution_boundary"] == "SignalExecutionRequest"
    loop_source = inspect.getsource(backtest_kernel._run_decision_event_backtest_impl)
    assert ".simulate_submit_plan(" not in loop_source


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
    assert all(decision["execution_plan_bundle_present"] for decision in result.decisions)
    assert [decision["execution_plan_status"] for decision in result.decisions] == ["PLANNED", "PLANNED"]
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


def test_backtest_kernel_has_no_sma_specific_dependencies_or_branches() -> None:
    source = Path("src/bithumb_bot/research/backtest_kernel.py").read_text(encoding="utf-8")
    forbidden = (
        "bithumb_bot.core.sma_policy",
        "bithumb_bot.sma_decision",
        "evaluate_sma_",
        "SmaPolicyConfig",
        "\"sma_with_filter\"",
        "'sma_with_filter'",
        "curr_s",
        "curr_l",
        "prev_s",
        "prev_l",
        "from bithumb_bot.strategy.sma",
        "from .strategy.sma",
    )

    for text in forbidden:
        assert text not in source
    assert "research_policy_decision_builder" in source
    assert "from . import backtest_engine as _engine" not in source
    assert "=_engine." not in source.replace(" ", "")


def test_neutral_strategy_contracts_do_not_import_sma_policy() -> None:
    contract_source = Path("src/bithumb_bot/strategy_policy_contract.py").read_text(encoding="utf-8")
    base_source = Path("src/bithumb_bot/strategy/base.py").read_text(encoding="utf-8")

    assert "core.sma_policy" not in contract_source
    assert "core.sma_policy" not in base_source
    assert "SmaEntryDecision" not in contract_source
    assert "entry_decision: object | None" in contract_source


def test_backtest_accumulator_uses_generic_diagnostic_count_map() -> None:
    accumulator_fields = backtest_support.BacktestAccumulator.__dataclass_fields__
    accumulator_source = inspect.getsource(backtest_support.BacktestAccumulator)

    assert "strategy_diagnostic_counts" in accumulator_fields
    for field_name in (
        "opposite_cross_triggered_count",
        "stop_loss_exit_count",
        "max_holding_exit_count",
        "raw_buy_filter_blocked_count",
    ):
        assert field_name not in accumulator_fields
        assert field_name not in accumulator_source


def test_backtest_support_does_not_alias_private_engine_helpers() -> None:
    source = Path("src/bithumb_bot/research/backtest_support.py").read_text(encoding="utf-8")

    assert "from . import backtest_engine as _engine" not in source
    assert "_engine._" not in source


def test_common_accumulator_source_is_strategy_diagnostics_neutral() -> None:
    source = inspect.getsource(backtest_support.BacktestAccumulator)

    for text in (
        "opposite_cross",
        "stop_loss",
        "max_holding_time",
        "raw_buy_filter_blocked",
        "raw_sell_filter_blocked",
    ):
        assert text not in source


def test_strategy_registry_keeps_sma_helper_bodies_out_of_common_registry() -> None:
    source = Path("src/bithumb_bot/research/strategy_registry.py").read_text(encoding="utf-8")

    for helper_name in (
        "_sma_decision_payload_adapter",
        "_sma_exit_signal_context",
        "_sma_exit_rule_factory",
        "_sma_research_policy_decision_builder",
        "_sma_runtime_decision_adapter_factory",
        "_sma_single_replay_bundle_builder",
    ):
        assert helper_name not in source


def test_backtest_kernel_does_not_own_sma_specific_exit_rule_names() -> None:
    implementation_source = inspect.getsource(backtest_kernel._run_decision_event_backtest_impl)

    assert "opposite_cross" not in implementation_source
    assert "strategy_plugin.exit_rule_factory" in implementation_source
    assert "common_rules" in implementation_source


def test_backtest_engine_public_entrypoint_delegates_to_kernel_boundary() -> None:
    source = inspect.getsource(backtest_engine.run_decision_event_backtest)

    assert "Compatibility wrapper for the common backtest kernel boundary" in source
    assert "from .backtest_kernel import run_decision_event_backtest as _run_decision_event_backtest" in source


def test_non_sma_executable_plugin_runs_buy_sell_custom_exit_with_typed_intent(monkeypatch) -> None:
    strategy_name = "unit_non_sma_executable"
    spec = StrategySpec(
        strategy_name=strategy_name,
        strategy_version="unit_non_sma_executable.research_contract.v1",
        accepted_parameter_names=("UNIT_BUY_FRACTION",),
        required_parameter_names=(),
        behavior_affecting_parameter_names=("UNIT_BUY_FRACTION",),
        metadata_only_parameter_names=(),
        research_only_parameter_names=(),
        default_parameters={"UNIT_BUY_FRACTION": 0.5},
        decision_contract_version="unit_non_sma_executable_decision.v1",
        required_data=("candles",),
        optional_data=(),
        exit_policy_schema={"schema_version": 1, "rules": ("unit_custom_exit",)},
    )

    def _runner(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("test plugin runner is not used by this kernel test")

    def _policy_builder(
        *,
        event,
        dataset,
        candle_index,
        position,
        parameter_values,
        fee_rate,
        slippage_bps,
        active_exit_policy,
        buy_fraction=0.0,
    ):
        del dataset, fee_rate, slippage_bps, active_exit_policy
        fraction = float(parameter_values.get("UNIT_BUY_FRACTION") or buy_fraction or 0.5)
        trace = {
            "schema_version": 1,
            "strategy_name": strategy_name,
            "candle_index": int(candle_index),
            "position_terminal_state": position.terminal_state,
            "raw_filter_would_block": False,
            "entry_blocked": False,
            "namespace": "unit_non_sma",
        }
        if not position.in_position:
            final_signal = "BUY"
            final_reason = "unit_non_sma_entry"
            execution_intent = EntryExecutionIntent(
                side="BUY",
                intent="enter_unit_non_sma_position",
                pair="KRW-BTC",
                requires_execution_sizing=True,
                budget_fraction_of_cash=fraction,
                max_budget_krw=0.0,
            )
            exit_rule = None
            exit_evaluations = ()
        else:
            final_signal = "SELL"
            final_reason = "unit_custom_exit_triggered"
            execution_intent = ExitExecutionIntent(
                side="SELL",
                intent="exit_unit_non_sma_position",
                pair="KRW-BTC",
                requires_execution_sizing=True,
            )
            exit_rule = "unit_custom_exit"
            exit_evaluations = (
                {
                    "rule": "unit_custom_exit",
                    "rule_source": "plugin",
                    "triggered": True,
                    "reason": final_reason,
                    "context": {
                        "position_terminal_state": position.terminal_state,
                        "non_sma_rule": True,
                    },
                },
            )
        policy_input = {
            "event_ts": int(event.candle_ts),
            "position": position.terminal_state,
            "candle_index": int(candle_index),
            "parameters": {"UNIT_BUY_FRACTION": fraction},
        }
        policy_decision = {
            "final_signal": final_signal,
            "final_reason": final_reason,
            "exit_rule": exit_rule,
            "execution_intent": execution_intent.as_dict(),
        }
        return StrategyDecisionV2(
            strategy_name=strategy_name,
            raw_signal=final_signal,
            raw_reason=final_reason,
            entry_signal="BUY" if final_signal == "BUY" else "HOLD",
            entry_reason=final_reason if final_signal == "BUY" else "already_in_position",
            exit_signal="SELL" if final_signal == "SELL" else "HOLD",
            exit_reason=final_reason if final_signal == "SELL" else "no_exit",
            final_signal=final_signal,
            final_reason=final_reason,
            blocked_filters=(),
            entry_blocked=False,
            entry_block_reason=None,
            exit_rule=exit_rule,
            exit_evaluations=exit_evaluations,
            protective_exit_overrode_entry=False,
            exit_filter_suppression_prevented=False,
            position_snapshot=position,
            execution_intent=execution_intent,
            entry_decision=object(),
            trace=trace,
            policy_hash=canonical_payload_hash(
                {"policy_input": policy_input, "policy_decision": policy_decision}
            ),
            policy_contract_hash=canonical_payload_hash(
                {"strategy_name": strategy_name, "contract": "unit_non_sma_executable"}
            ),
            policy_input_hash=canonical_payload_hash(policy_input),
            policy_decision_hash=canonical_payload_hash(policy_decision),
        )

    plugin = ResearchStrategyPlugin(
        name=strategy_name,
        version=spec.strategy_version,
        spec=spec,
        required_data=spec.required_data,
        optional_data=spec.optional_data,
        runner=_runner,
        runtime_replay_builder=None,
        runtime_parameter_adapter=None,
        decision_contract_version=spec.decision_contract_version,
        diagnostics_namespace="unit_non_sma",
        research_policy_decision_builder=_policy_builder,
        runtime_capabilities=StrategyRuntimeCapabilities(
            promotion_runtime_decisions_supported=False,
            runtime_replay_supported=False,
            research_only=True,
            baseline_only=False,
            live_dry_run_allowed=False,
            live_real_order_allowed=False,
            approved_profile_required=False,
            fail_closed_reason="unit_non_sma_runtime_unsupported",
        ),
    )
    monkeypatch.setitem(strategy_registry._RESEARCH_STRATEGY_PLUGINS, strategy_name, plugin)

    dataset = DatasetSnapshot(
        snapshot_id="unit_non_sma_executable",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=tuple(
            Candle(index * 60_000, 100.0 + index, 101.0 + index, 99.0 + index, 100.0 + index, 1.0)
            for index in range(5)
        ),
    )
    events = (
        ResearchDecisionEvent(
            candle_ts=dataset.candles[1].ts,
            decision_ts=dataset.candles[1].ts + 60_000,
            strategy_name=strategy_name,
            strategy_version=spec.strategy_version,
            raw_signal="BUY",
            final_signal="BUY",
            reason="unit_non_sma_event_buy",
            feature_snapshot={"candle_index": 1, "feature_family": "unit_non_sma_close_only"},
            strategy_diagnostics={"schema_version": 1, "namespace": "unit_non_sma"},
            entry_signal="BUY",
            exit_signal="HOLD",
        ),
        ResearchDecisionEvent(
            candle_ts=dataset.candles[3].ts,
            decision_ts=dataset.candles[3].ts + 60_000,
            strategy_name=strategy_name,
            strategy_version=spec.strategy_version,
            raw_signal="HOLD",
            final_signal="HOLD",
            reason="unit_non_sma_event_exit_check",
            feature_snapshot={"candle_index": 3, "feature_family": "unit_non_sma_close_only"},
            strategy_diagnostics={"schema_version": 1, "namespace": "unit_non_sma"},
            entry_signal="HOLD",
            exit_signal="HOLD",
        ),
    )

    result = run_decision_event_backtest(
        dataset=dataset,
        strategy_name=strategy_name,
        parameter_values={"UNIT_BUY_FRACTION": 0.5},
        fee_rate=0.001,
        slippage_bps=2.0,
        decision_events=events,
        context=BacktestRunContext(report_detail="full"),
    )

    assert any(trade["side"] == "BUY" and trade["is_execution_filled"] for trade in result.trades)
    assert any(trade["side"] == "SELL" and trade["is_execution_filled"] for trade in result.trades)
    assert any(trade.get("exit_rule") == "unit_custom_exit" for trade in result.trades)
    assert result.decisions[0]["execution_intent_v2"]["side"] == "BUY"
    assert result.decisions[1]["execution_intent_v2"]["side"] == "SELL"
    assert result.decisions[1]["exit_rule"] == "unit_custom_exit"
    assert result.decisions[1]["research_policy_recomputed_with_simulated_position"] is True
    assert result.decisions[1]["research_policy_position_terminal_state"] == "research_simulated_open_exposure"
    assert result.strategy_diagnostics["strategy_diagnostics_namespace"] == "unit_non_sma"
    assert set(result.strategy_diagnostics["strategy_specific_diagnostics"]) == {"unit_non_sma"}
    for key in (
        "raw_buy_filter_blocked_count",
        "opposite_cross_triggered_count",
        "stop_loss_exit_count",
        "max_holding_exit_count",
    ):
        assert key not in result.strategy_diagnostics
    kernel_source = Path("src/bithumb_bot/research/backtest_kernel.py").read_text(encoding="utf-8")
    for text in ("curr_s", "curr_l", "SMA_SHORT", "SmaPolicyConfig"):
        assert text not in kernel_source
