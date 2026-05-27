from __future__ import annotations

import ast
import inspect
import sqlite3
import textwrap
from dataclasses import replace

import pytest

from bithumb_bot.core import sma_policy
from bithumb_bot.core.sma_policy import (
    EntryExecutionIntent,
    ExecutionConstraintSnapshot,
    MarketWindow,
    PositionSnapshot,
    SmaPolicyConfig,
    evaluate_sma_policy,
)
from bithumb_bot.canonical_decision import export_runtime_replay_decisions
from bithumb_bot.market_regime import MARKET_REGIME_VERSION
from bithumb_bot.research.backtest_engine import SmaWithFilterDecisionAdapter
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research import backtest_kernel
from bithumb_bot.research import sma_with_filter_plugin
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.experiment_manifest import (
    DateRange,
    ExecutionTimingPolicy,
    PortfolioPolicy,
    PositionSizingPolicy,
)
from bithumb_bot import engine
from bithumb_bot import runtime_position_state_normalizer
from bithumb_bot import runtime_sma_snapshot
from bithumb_bot import runtime_sma_snapshot_builder as runtime_sma
from bithumb_bot import runtime_strategy_decision
from bithumb_bot.runtime_adapters import sma_with_filter as runtime_sma_adapter
from bithumb_bot.strategy import sma as strategy_sma
from bithumb_bot.strategy.sma import (
    SmaCrossStrategy,
    SmaWithFilterStrategy,
    create_sma_with_filter_strategy,
)
from bithumb_bot.strategy.exit_rules import ExitPolicyConfig, evaluate_sma_exit_policy
from bithumb_bot.strategy.sma_decision_assembler import evaluate_sma_final_decision


def _policy_config() -> SmaPolicyConfig:
    return SmaPolicyConfig(
        strategy_name="sma_with_filter",
        short_n=2,
        long_n=3,
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        cost_edge_min_ratio=0.0,
        market_regime_enabled=False,
        buy_fraction=0.99,
        max_order_krw=100_000.0,
    )


def _market_window() -> MarketWindow:
    closes = (10.0, 10.0, 10.0, 10.0, 11.0)
    return MarketWindow(
        pair="BTC_KRW",
        interval="1m",
        candle_ts=1_700_000_240_000,
        closes=closes,
        prev_s=10.0,
        prev_l=10.0,
        curr_s=10.5,
        curr_l=10.333333333333334,
    )


def _flat_position() -> PositionSnapshot:
    return PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False)


def _open_position(**overrides: object) -> PositionSnapshot:
    payload = {
        "in_position": True,
        "entry_allowed": False,
        "exit_allowed": True,
        "entry_block_reason": "open_exposure",
        "terminal_state": "open_exposure",
        "entry_ts": 1_700_000_000_000,
        "entry_price": 10.0,
        "qty_open": 1.0,
        "holding_time_sec": 60.0,
        "unrealized_pnl": 1.0,
        "unrealized_pnl_ratio": 0.1,
        "raw_qty_open": 1.0,
        "raw_total_asset_qty": 1.0,
        "open_lot_count": 1,
        "sellable_executable_lot_count": 1,
        "effective_flat": False,
        "has_executable_exposure": True,
        "has_any_position_residue": True,
    }
    payload.update(overrides)
    return PositionSnapshot(**payload)  # type: ignore[arg-type]


def _exit_policy_config(**overrides: object) -> ExitPolicyConfig:
    payload = {
        "rule_names": ("stop_loss", "opposite_cross", "max_holding_time"),
        "stop_loss_ratio": 0.05,
        "max_holding_sec": 3_600.0,
        "min_take_profit_ratio": 0.0,
        "small_loss_tolerance_ratio": 0.0,
        "live_fee_rate_estimate": 0.0,
    }
    payload.update(overrides)
    return ExitPolicyConfig(**payload)  # type: ignore[arg-type]


def _allowing_policy() -> dict[str, object]:
    return {
        "regime_classifier_version": MARKET_REGIME_VERSION,
        "allowed_regimes": [
            "uptrend_high_vol_unknown",
            "uptrend_normal_vol_unknown",
            "uptrend_low_vol_unknown",
        ],
        "blocked_regimes": [],
        "regime_evidence": {},
    }


def test_evaluate_sma_policy_is_deterministic_for_same_snapshot() -> None:
    first = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    second = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert first == second
    assert first.policy_hash == second.policy_hash
    assert first.raw_signal == "BUY"
    assert first.final_signal == "BUY"


def test_evaluate_sma_policy_has_no_runtime_dependency_imports_or_side_effect_surfaces() -> None:
    source = inspect.getsource(sma_policy)
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])

    assert "sqlite3" not in imported_roots
    assert "time" not in imported_roots
    assert "bithumb_client" not in imported_roots
    assert "notifier" not in imported_roots
    assert "settings" not in source
    assert "conn." not in source
    assert "commit(" not in source


def test_evaluate_sma_policy_open_position_defers_exit_to_wrapper_without_entry_authority() -> None:
    decision = evaluate_sma_policy(
        market=_market_window(),
        position=PositionSnapshot(
            in_position=True,
            entry_allowed=False,
            exit_allowed=True,
            terminal_state="open_exposure",
            qty_open=1.0,
            open_lot_count=1,
            sellable_executable_lot_count=1,
            has_executable_exposure=True,
        ),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert decision.raw_signal == "BUY"
    assert decision.entry_signal == "BUY"
    assert decision.final_signal == "HOLD"
    assert decision.final_reason == "position held: exit policy evaluation required"
    assert decision.trace["position"]["terminal_state"] == "open_exposure"


def test_final_sma_decision_assembler_is_deterministic_and_hashes_policy_material() -> None:
    first = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    second = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    changed = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.001),
        exit_policy_config=_exit_policy_config(),
    )

    assert first == second
    assert first.final_signal == "BUY"
    assert isinstance(first.execution_intent, EntryExecutionIntent)
    assert first.execution_intent.as_dict() == {
        "schema_version": 1,
        "intent_version": 1,
        "side": "BUY",
        "intent": "enter_open_exposure",
        "pair": "BTC_KRW",
        "budget_model": "cash_fraction_capped_by_max_order_krw",
        "budget_fraction_of_cash": 0.99,
        "max_budget_krw": 100_000.0,
        "requires_execution_sizing": True,
    }
    assert first.policy_contract_hash == second.policy_contract_hash
    assert first.policy_input_hash == second.policy_input_hash
    assert first.policy_decision_hash == second.policy_decision_hash
    assert changed.policy_input_hash != first.policy_input_hash


def test_execution_intent_v1_serialization_is_stable() -> None:
    first = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    second = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )

    assert first.execution_intent is not None
    assert second.execution_intent is not None
    assert first.execution_intent.as_dict() == second.execution_intent.as_dict()
    assert first.policy_decision_hash == second.policy_decision_hash
    assert first.as_trace()["execution_intent"] == first.execution_intent.as_dict()


def test_policy_hashes_ignore_transient_fee_authority_timestamps() -> None:
    base_fee_authority = {
        "fee_source": "order_rules",
        "taker_bid_fee_rate": 0.001,
        "taker_ask_fee_rate": 0.001,
        "retrieved_at_sec": 1_700_000_000,
        "expires_at_sec": 1_700_000_300,
    }
    first = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(
            fee_rate_for_decision=0.001,
            fee_authority=base_fee_authority,
        ),
    )
    second = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(
            fee_rate_for_decision=0.001,
            fee_authority={
                **base_fee_authority,
                "retrieved_at_sec": 1_700_000_200,
                "expires_at_sec": 1_700_000_500,
            },
        ),
    )

    assert second.policy_input_hash == first.policy_input_hash
    assert second.policy_decision_hash == first.policy_decision_hash


def test_policy_hashes_normalize_research_runtime_comparable_terminal_states() -> None:
    runtime_flat = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    research_flat = evaluate_sma_policy(
        market=_market_window(),
        position=replace(_flat_position(), terminal_state="research_simulated_flat"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    runtime_open = evaluate_sma_policy(
        market=_market_window(),
        position=replace(_open_position(), terminal_state="open_exposure"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    research_open = evaluate_sma_policy(
        market=_market_window(),
        position=replace(_open_position(), terminal_state="research_simulated_open_exposure"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert research_flat.trace["position"]["terminal_state"] == "research_simulated_flat"
    assert runtime_flat.policy_input_hash == research_flat.policy_input_hash
    assert runtime_flat.policy_decision_hash == research_flat.policy_decision_hash
    assert runtime_flat.final_signal == research_flat.final_signal
    assert list(runtime_flat.blocked_filters) == list(research_flat.blocked_filters)
    assert runtime_flat.trace.get("execution_intent") == research_flat.trace.get("execution_intent")
    assert runtime_open.policy_input_hash == research_open.policy_input_hash
    assert runtime_open.policy_decision_hash == research_open.policy_decision_hash
    assert runtime_open.final_signal == research_open.final_signal
    assert list(runtime_open.blocked_filters) == list(research_open.blocked_filters)
    assert runtime_open.trace.get("execution_intent") == research_open.trace.get("execution_intent")

    runtime_final = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    research_final = evaluate_sma_final_decision(
        market=_market_window(),
        position=replace(_flat_position(), terminal_state="research_simulated_flat"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    assert runtime_final.policy_decision_hash == research_final.policy_decision_hash


def test_final_sma_decision_assembler_owns_opposite_cross_sell() -> None:
    decision = evaluate_sma_final_decision(
        market=MarketWindow(
            pair="BTC_KRW",
            interval="1m",
            candle_ts=1_700_000_240_000,
            closes=(12.0, 12.0, 12.0, 12.0, 11.0),
            prev_s=12.0,
            prev_l=11.5,
            curr_s=11.0,
            curr_l=11.5,
        ),
        position=_open_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(rule_names=("opposite_cross",), stop_loss_ratio=0.0),
    )

    assert decision.raw_signal == "SELL"
    assert decision.final_signal == "SELL"
    assert decision.exit_rule == "opposite_cross"
    assert decision.protective_exit_overrode_entry is False
    assert decision.exit_filter_suppression_prevented is False


def test_final_sma_decision_assembler_owns_protective_stop_loss_override() -> None:
    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=_open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(rule_names=("stop_loss",)),
    )

    assert decision.raw_signal == "BUY"
    assert decision.final_signal == "SELL"
    assert decision.exit_rule == "stop_loss"
    assert decision.protective_exit_overrode_entry is True


def test_final_sma_decision_assembler_owns_protective_max_holding_override() -> None:
    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=_open_position(holding_time_sec=7_200.0),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(
            rule_names=("max_holding_time",),
            max_holding_sec=3_600.0,
            stop_loss_ratio=0.0,
        ),
    )

    assert decision.raw_signal == "BUY"
    assert decision.final_signal == "SELL"
    assert decision.exit_rule == "max_holding_time"
    assert decision.protective_exit_overrode_entry is True


def test_snapshot_strategy_policy_decides_without_sqlite() -> None:
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    decision = strategy.decide_snapshot(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert decision.final_signal == "BUY"
    assert decision.policy_hash.startswith("sha256:")


def test_sma_strategy_snapshot_api_returns_final_decision_with_exit_policy() -> None:
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        exit_rule_names=["stop_loss"],
        exit_stop_loss_ratio=0.05,
    )

    entry_only = strategy.decide_entry_snapshot(
        market=_market_window(),
        position=_open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    final = strategy.decide_snapshot(
        market=_market_window(),
        position=_open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert entry_only.raw_signal == "BUY"
    assert final.final_signal == "SELL"
    assert final.exit_rule == "stop_loss"
    assert final.execution_intent is not None
    assert final.execution_intent.side == "SELL"


def _build_candle_db(closes: list[float]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE candles (
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            close REAL NOT NULL
        )
        """
    )
    base_ts = 1_700_000_000_000
    for idx, close in enumerate(closes):
        conn.execute(
            "INSERT INTO candles(ts, pair, interval, close) VALUES (?, ?, ?, ?)",
            (base_ts + idx * 60_000, "BTC_KRW", "1m", close),
        )
    conn.commit()
    return conn


def _dataset_from_closes(closes: list[float]) -> DatasetSnapshot:
    base_ts = 1_700_000_000_000
    candles = tuple(
        Candle(base_ts + index * 60_000, float(close), float(close), float(close), float(close), 1.0)
        for index, close in enumerate(closes)
    )
    return DatasetSnapshot(
        snapshot_id="sma_policy_boundary_unit",
        source="unit",
        market="BTC_KRW",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=candles,
    )


def test_live_wrapper_and_research_adapter_share_policy_entry_boundary() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    conn = _build_candle_db(closes)
    try:
        runtime_decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
            conn,
            create_sma_with_filter_strategy(
                short_n=2,
                long_n=3,
                pair="BTC_KRW",
                interval="1m",
                min_gap_ratio=0.0,
                volatility_window=3,
                min_volatility_ratio=0.0,
                overextended_lookback=1,
                overextended_max_return_ratio=0.0,
                slippage_bps=0.0,
                live_fee_rate_estimate=0.0,
                entry_edge_buffer_ratio=0.0,
                cost_edge_enabled=False,
                market_regime_enabled=False,
                candidate_regime_policy=_allowing_policy(),
            ),
        )
    finally:
        conn.close()

    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))
    research_event = events[-1]
    research_decision = sma_with_filter_plugin.research_policy_decision_builder(
        event=research_event,
        dataset=_dataset_from_closes(closes),
        candle_index=len(closes) - 1,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )

    assert runtime_decision is not None
    assert research_decision is not None
    assert runtime_decision.context["raw_signal"] == research_decision.raw_signal == "BUY"
    assert runtime_decision.context["entry_signal"] == research_decision.entry_signal == "BUY"
    assert runtime_decision.context["final_signal"] == research_decision.final_signal == "BUY"
    assert tuple(runtime_decision.context["blocked_filters"]) == research_decision.blocked_filters == ()
    assert runtime_decision.context["pure_policy_hash"].startswith("sha256:")
    assert "pure_policy_hash" not in research_event.extra_payload


def test_runtime_db_and_research_adapter_policy_input_hashes_are_non_comparable_without_live_constraints() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    conn = _build_candle_db(closes)
    try:
        runtime_decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
            conn,
            create_sma_with_filter_strategy(
                short_n=2,
                long_n=3,
                pair="BTC_KRW",
                interval="1m",
                min_gap_ratio=0.0,
                volatility_window=3,
                min_volatility_ratio=0.0,
                overextended_lookback=1,
                overextended_max_return_ratio=0.0,
                slippage_bps=0.0,
                live_fee_rate_estimate=0.0,
                entry_edge_buffer_ratio=0.0,
                cost_edge_enabled=False,
                market_regime_enabled=False,
                candidate_regime_policy=_allowing_policy(),
            ),
            through_ts_ms=1_700_000_240_000,
        )
    finally:
        conn.close()

    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))
    research_event = events[-1]
    research_decision = sma_with_filter_plugin.research_policy_decision_builder(
        event=research_event,
        dataset=_dataset_from_closes(closes),
        candle_index=len(closes) - 1,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )

    assert runtime_decision is not None
    assert research_decision is not None
    runtime_trace = runtime_decision.context["pure_policy_trace"]
    research_trace = research_decision.as_trace()
    assert runtime_trace["market"]["pair"] == research_trace["market"]["pair"] == "BTC_KRW"
    assert runtime_trace["market"]["interval"] == research_trace["market"]["interval"] == "1m"
    assert runtime_trace["market"]["candle_ts"] == research_trace["market"]["candle_ts"]
    assert runtime_trace["market"]["last_close"] == research_trace["market"]["last_close"]
    assert runtime_trace["final_signal"] == research_trace["final_signal"] == "BUY"
    assert runtime_trace["final_reason"] == research_trace["final_reason"]
    assert runtime_decision.context["policy_input_hash"].startswith("sha256:")
    assert runtime_decision.context["policy_decision_hash"].startswith("sha256:")
    assert research_decision.policy_input_hash.startswith("sha256:")
    assert research_decision.policy_decision_hash.startswith("sha256:")
    assert runtime_trace["execution_constraints"]["order_rules"] == (
        research_trace["execution_constraints"]["order_rules"]
    )
    assert runtime_trace["execution_constraints"]["fee_authority"] == (
        research_trace["execution_constraints"]["fee_authority"]
    )
    assert research_trace["position"]["terminal_state"] == "flat"


def test_research_adapter_does_not_override_policy_first_cross_when_prev_above_unknown() -> None:
    closes = [10.0, 10.0, 10.0, 11.0, 11.0]
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))

    first = events[0]
    decision = sma_with_filter_plugin.research_policy_decision_builder(
        event=first,
        dataset=_dataset_from_closes(closes),
        candle_index=3,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )
    assert decision is not None
    policy_trace = decision.as_trace()
    assert first.extra_payload["prev_above"] is None
    assert policy_trace["market"]["previous_cross_state"] == "unknown"
    assert policy_trace["market"]["allow_initial_cross"] is False
    assert policy_trace["raw_signal"] == "HOLD"
    assert policy_trace["entry_signal"] == "HOLD"
    assert policy_trace["final_signal"] == "HOLD"


def test_policy_can_allow_initial_cross_when_configured() -> None:
    decision = evaluate_sma_policy(
        market=MarketWindow(
            **{
                **_market_window().__dict__,
                "previous_cross_state": "unknown",
                "allow_initial_cross": True,
            }
        ),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert decision.raw_signal == "BUY"
    assert decision.final_signal == "BUY"


def test_shared_sma_exit_policy_is_deterministic_for_runtime_and_research_snapshots() -> None:
    market = MarketWindow(
        pair="BTC_KRW",
        interval="1m",
        candle_ts=1_700_000_240_000,
        closes=(95.0,),
        prev_s=100.0,
        prev_l=99.0,
        curr_s=98.0,
        curr_l=99.0,
    )
    config = ExitPolicyConfig(
        rule_names=("stop_loss", "opposite_cross", "max_holding_time"),
        stop_loss_ratio=0.04,
        max_holding_sec=600.0,
        min_take_profit_ratio=0.0,
        small_loss_tolerance_ratio=0.0,
        live_fee_rate_estimate=0.0,
    )
    runtime_snapshot = PositionSnapshot(
        in_position=True,
        entry_allowed=False,
        exit_allowed=True,
        terminal_state="open_exposure",
        entry_ts=1_700_000_000_000,
        entry_price=100.0,
        qty_open=1.0,
        holding_time_sec=240.0,
        unrealized_pnl=-5.0,
        unrealized_pnl_ratio=-0.05,
        open_lot_count=1,
        sellable_executable_lot_count=1,
        effective_flat=False,
        has_executable_exposure=True,
        has_any_position_residue=True,
    )
    research_snapshot = PositionSnapshot(
        **{
            **runtime_snapshot.__dict__,
            "terminal_state": "research_simulated_open_exposure",
        }
    )

    runtime_exit = evaluate_sma_exit_policy(
        position=runtime_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )
    research_exit = evaluate_sma_exit_policy(
        position=research_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )

    assert runtime_exit == research_exit
    assert runtime_exit.final_signal == "SELL"
    assert runtime_exit.rule == "stop_loss"
    assert runtime_exit.reason == "exit by stop loss"


def test_research_kernel_open_snapshot_matches_live_open_exit_policy_fields() -> None:
    market = MarketWindow(
        pair="BTC_KRW",
        interval="1m",
        candle_ts=1_700_000_240_000,
        closes=(95.0,),
        prev_s=100.0,
        prev_l=99.0,
        curr_s=98.0,
        curr_l=99.0,
    )
    config = ExitPolicyConfig(
        rule_names=("stop_loss", "opposite_cross", "max_holding_time"),
        stop_loss_ratio=0.04,
        max_holding_sec=600.0,
        min_take_profit_ratio=0.0,
        small_loss_tolerance_ratio=0.0,
        live_fee_rate_estimate=0.0,
    )
    research_snapshot = backtest_kernel._research_position_snapshot(
        qty=1.0,
        sellable_qty=1.0,
        pending_buy_qty=0.0,
        pending_sell_qty=0.0,
        entry_ts=1_700_000_000_000,
        entry_price=100.0,
        candle_ts=1_700_000_240_000,
        market_price=95.0,
    )
    live_snapshot = PositionSnapshot(
        **{
            **research_snapshot.__dict__,
            "terminal_state": "open_exposure",
        }
    )

    research_exit = evaluate_sma_exit_policy(
        position=research_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )
    live_exit = evaluate_sma_exit_policy(
        position=live_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )

    assert research_snapshot.terminal_state == "research_simulated_open_exposure"
    assert research_exit.final_signal == live_exit.final_signal == "SELL"
    assert research_exit.rule == live_exit.rule == "stop_loss"
    assert research_exit.reason == live_exit.reason == "exit by stop loss"


def test_runtime_decide_is_read_only_and_normalization_boundary_is_explicit() -> None:
    load_position_source = inspect.getsource(runtime_sma._load_position_context)
    normalizer_source = inspect.getsource(
        runtime_position_state_normalizer.PositionStateNormalizer.normalize_and_persist
    )
    builder_source = inspect.getsource(runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db)
    builder_impl_source = inspect.getsource(
        runtime_sma._build_sma_with_filter_runtime_decision_from_normalized_db_readonly_impl
    )
    orchestration_source = inspect.getsource(runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db)
    runtime_normalization_source = inspect.getsource(
        runtime_sma_adapter.normalize_position_state_before_strategy_decision
    )
    runtime_boundary_source = inspect.getsource(runtime_sma_snapshot.decide_sma_with_filter_snapshot_from_db)
    runtime_boundary_module_source = inspect.getsource(runtime_sma_snapshot)

    assert "mark_harmless_dust_positions" not in load_position_source
    assert "reclassify_non_executable_open_exposure" not in load_position_source
    assert "conn.commit()" not in load_position_source
    assert "mark_harmless_dust_positions" in normalizer_source
    assert "reclassify_non_executable_open_exposure" in normalizer_source
    assert "conn.commit()" in normalizer_source
    assert "readonly_decision_context(" in builder_source
    assert "_load_position_context(" in builder_impl_source
    assert "strategy.decide_snapshot(" in builder_impl_source
    assert "normalize_and_persist(" not in orchestration_source
    assert "normalize_and_persist(" in runtime_normalization_source
    assert "strategy.decide(" not in orchestration_source
    assert "_decide_from_normalized_db(" not in orchestration_source
    assert "build_sma_with_filter_runtime_decision_from_normalized_db(" in orchestration_source
    assert "_runtime_snapshot_from_db(" in runtime_boundary_source
    assert "decide_sma_with_filter_snapshot_from_db as _strategy_snapshot_from_db" not in runtime_boundary_module_source


def test_runtime_snapshot_builder_does_not_import_private_strategy_sma_helpers() -> None:
    tree = ast.parse(inspect.getsource(runtime_sma))
    forbidden_helpers = {
        "_safe_ratio",
        "_sma",
        "_build_entry_decision_context",
        "_build_position_gate_context",
        "_build_position_state_context",
        "_fee_authority_context",
        "_legacy_strategy_decision_from_sma_final_decision",
        "_live_armed_entry_fee_authority_blocks",
        "_resolve_strategy_fee_authority",
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        assert node.module not in {
            ".strategy.sma",
            "bithumb_bot.strategy.sma",
            "strategy.sma",
        }
        if node.module.endswith("strategy.sma"):
            imported = {alias.name for alias in node.names}
            assert imported.isdisjoint(forbidden_helpers)


def test_research_kernel_does_not_import_private_strategy_sma_helpers() -> None:
    source = inspect.getsource(backtest_kernel)
    tree = ast.parse(source)

    assert "from bithumb_bot.strategy import sma as runtime_sma" not in source
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            assert not node.module.endswith("strategy.sma")


def test_runtime_context_owns_sma_legacy_serialization_helpers() -> None:
    builder_source = inspect.getsource(runtime_sma.RuntimeSmaDecisionResult.legacy_strategy_decision)
    strategy_module_source = inspect.getsource(SmaWithFilterStrategy)

    assert "runtime_sma_context" in inspect.getsource(runtime_sma)
    assert "legacy_strategy_decision_from_sma_final_decision(" in builder_source
    assert "_legacy_strategy_decision_from_sma_final_decision(" not in builder_source
    assert "Promotion-grade snapshot SMA strategy" in strategy_module_source
    assert "def decide(" not in strategy_module_source


def test_strategy_sma_is_compatibility_facade_not_implementation_authority() -> None:
    module_source = inspect.getsource(strategy_sma)

    assert "Compatibility facade" in module_source
    assert "import sqlite3" not in module_source
    assert "class SmaWithFilterStrategy" not in module_source
    assert "class SmaCrossStrategy" not in module_source
    assert SmaWithFilterStrategy.__module__ == "bithumb_bot.strategy.sma_policy_strategy"
    assert "evaluate_sma_final_decision(" in inspect.getsource(SmaWithFilterStrategy.decide_snapshot)
    assert "evaluate_sma_policy(" in inspect.getsource(SmaWithFilterStrategy.decide_entry_snapshot)


def _called_function_names(module: object) -> set[str]:
    tree = ast.parse(inspect.getsource(module))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.add(node.func.attr)
    return names


def test_environment_adapters_do_not_call_final_sma_assembler_directly() -> None:
    forbidden = "evaluate_sma_final_decision"
    assert forbidden not in _called_function_names(runtime_sma)
    assert forbidden not in _called_function_names(sma_with_filter_plugin)
    assert forbidden not in _called_function_names(backtest_kernel)


def test_research_event_adapter_is_non_authoritative_source_boundary() -> None:
    source = inspect.getsource(SmaWithFilterDecisionAdapter.build_events)
    calls = _called_function_names(SmaWithFilterDecisionAdapter)
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0]))

    assert "evaluate_sma_policy" not in calls
    assert "evaluate_sma_final_decision" not in calls
    assert "pure_policy_trace" not in source
    assert events
    event = events[-1]
    assert event.raw_signal == "HOLD"
    assert event.final_signal == "HOLD"
    assert event.reason == "research_event_adapter_non_authoritative"
    assert "entry_decision" not in event.extra_payload
    assert "pure_policy_trace" not in event.extra_payload
    assert "pure_policy_hash" not in event.extra_payload


def test_live_and_research_policy_paths_call_same_strategy_entrypoint(monkeypatch) -> None:
    calls: list[str] = []
    original = SmaWithFilterStrategy.decide_snapshot

    def _counting_decide_snapshot(self, *args, **kwargs):
        calls.append(str(getattr(self, "name", "")))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(SmaWithFilterStrategy, "decide_snapshot", _counting_decide_snapshot)
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    conn = _build_candle_db(closes)
    try:
        runtime_result = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_240_000,
        )
    finally:
        conn.close()
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))
    research_result = sma_with_filter_plugin.research_policy_decision_builder(
        event=events[-1],
        dataset=_dataset_from_closes(closes),
        candle_index=len(closes) - 1,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )

    assert runtime_result is not None
    assert research_result is not None
    assert calls == ["sma_with_filter", "sma_with_filter"]


def test_live_research_equivalence_under_identical_policy_snapshots() -> None:
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        exit_rule_names=["stop_loss", "opposite_cross"],
    )
    live_position = _open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1)
    research_position = replace(
        live_position,
        terminal_state="research_simulated_open_exposure",
    )
    config = _policy_config()
    execution_context = ExecutionConstraintSnapshot(
        fee_rate_for_decision=0.0,
        fee_authority={"fee_source": "fixture", "taker_bid_fee_rate": 0.0},
        order_rules={"source": "fixture", "min_total": 5_000},
    )
    exit_config = _exit_policy_config(rule_names=("stop_loss", "opposite_cross"))

    live_decision = strategy.decide_snapshot(
        market=_market_window(),
        position=live_position,
        config=config,
        execution_context=execution_context,
        exit_policy_config=exit_config,
    )
    research_decision = strategy.decide_snapshot(
        market=_market_window(),
        position=research_position,
        config=config,
        execution_context=execution_context,
        exit_policy_config=exit_config,
    )

    assert research_decision.final_signal == live_decision.final_signal
    assert research_decision.raw_signal == live_decision.raw_signal
    assert research_decision.entry_signal == live_decision.entry_signal
    assert research_decision.exit_signal == live_decision.exit_signal
    assert research_decision.exit_rule == live_decision.exit_rule
    assert research_decision.execution_intent == live_decision.execution_intent
    assert research_decision.policy_input_hash == live_decision.policy_input_hash
    assert research_decision.policy_decision_hash == live_decision.policy_decision_hash
    assert research_decision.policy_contract_hash == live_decision.policy_contract_hash
    assert research_decision.exit_evaluations == live_decision.exit_evaluations


class _CommitCountingConnection:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.commit_count = 0

    def execute(self, *args, **kwargs):
        return self.conn.execute(*args, **kwargs)

    def commit(self) -> None:
        self.commit_count += 1
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def test_post_normalization_decision_path_does_not_commit(monkeypatch) -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    wrapped = _CommitCountingConnection(_build_candle_db(closes))

    try:
        strategy = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.0,
            entry_edge_buffer_ratio=0.0,
            cost_edge_enabled=False,
            market_regime_enabled=False,
            candidate_regime_policy=_allowing_policy(),
        )
        decision = runtime_sma.build_sma_with_filter_decision_from_normalized_db(
            wrapped,
            strategy,
        )
    finally:
        wrapped.close()

    assert decision is not None
    assert wrapped.commit_count == 0


def test_sma_cross_decide_does_not_run_position_normalizer_or_commit(monkeypatch) -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    wrapped = _CommitCountingConnection(_build_candle_db(closes))

    def _raise_mutating_normalizer(*args, **kwargs):
        raise AssertionError("SmaCrossStrategy.decide must not normalize mutable position state")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_mutating_normalizer,
    )

    try:
        decision = SmaCrossStrategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.0,
            entry_edge_buffer_ratio=0.0,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(wrapped, through_ts_ms=1_700_000_240_000)
    finally:
        wrapped.close()

    assert decision is not None
    assert wrapped.commit_count == 0


def test_runtime_snapshot_builder_after_normalization_is_read_only() -> None:
    closes = [10.0] * 11 + [11.0]
    conn = _build_candle_db(closes)
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    changes_before = conn.total_changes
    candles_before = conn.execute(
        "SELECT ts, pair, interval, close FROM candles ORDER BY ts"
    ).fetchall()

    try:
        result = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
        changes_after = conn.total_changes
        candles_after = conn.execute(
            "SELECT ts, pair, interval, close FROM candles ORDER BY ts"
        ).fetchall()
    finally:
        conn.close()

    assert result is not None
    assert changes_after == changes_before
    assert candles_after == candles_before
    assert result.decision.policy_input_hash.startswith("sha256:")
    assert result.decision.policy_decision_hash.startswith("sha256:")
    assert result.decision.policy_hash.startswith("sha256:")
    assert result.replay_fingerprint["strategy_name"] == "sma_with_filter"
    assert result.replay_fingerprint["through_ts_ms"] == 1_700_000_000_000 + 11 * 60_000
    assert result.boundary["normalization_boundary"] == (
        "engine.normalize_position_state_before_strategy_decision"
    )
    assert result.boundary["normalization_updated_count"] is None
    assert result.boundary["decision_boundary_phase"] == "post_normalization_decision"
    assert isinstance(result.boundary["post_normalization_read_only_guard"], dict)
    assert result.boundary["post_decision_total_changes_delta"] == 0
    assert result.runtime_decision_context.as_dict()["boundary"] == result.boundary


def test_post_normalization_read_only_guard_rejects_mutation(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    original_load_signal_rows = runtime_sma._load_signal_rows

    def _mutating_signal_rows(*args, **kwargs):
        conn.execute(
            "INSERT INTO candles(ts, pair, interval, close) VALUES (?, ?, ?, ?)",
            (1_800_000_000_000, "BTC_KRW", "1m", 99.0),
        )
        return original_load_signal_rows(*args, **kwargs)

    monkeypatch.setattr(runtime_sma, "_load_signal_rows", _mutating_signal_rows)

    try:
        with pytest.raises(RuntimeError, match="post_normalization_decision_readonly_violation"):
            runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db(
                conn,
                strategy,
                through_ts_ms=1_700_000_000_000 + 11 * 60_000,
            )
    finally:
        conn.close()



def test_load_position_context_does_not_commit() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    wrapped = _CommitCountingConnection(_build_candle_db(closes))

    try:
        runtime_sma._load_position_context(
            wrapped,
            pair="BTC_KRW",
            candle_ts=1_700_000_240_000,
            market_price=11.0,
            signal_context={"strategy": "sma_with_filter"},
            slippage_bps=0.0,
            entry_edge_buffer_ratio=0.0,
        )
    finally:
        wrapped.close()

    assert wrapped.commit_count == 0


def test_position_state_normalizer_is_the_commit_boundary(monkeypatch) -> None:
    wrapped = _CommitCountingConnection(_build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0]))
    monkeypatch.setattr(
        runtime_position_state_normalizer,
        "mark_harmless_dust_positions",
        lambda *args, **kwargs: 1,
    )
    monkeypatch.setattr(
        runtime_position_state_normalizer,
        "reclassify_non_executable_open_exposure",
        lambda *args, **kwargs: 0,
    )

    try:
        updated = runtime_position_state_normalizer.PositionStateNormalizer().normalize_and_persist(
            wrapped,
            pair="BTC_KRW",
            market_price=11.0,
            slippage_bps=0.0,
            entry_edge_buffer_ratio=0.0,
        )
    finally:
        wrapped.close()

    assert updated == 1
    assert wrapped.commit_count == 1


def _assert_no_sqlite_mutation_sql_or_commit(functions: tuple[object, ...]) -> None:
    mutating_sql = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "CREATE",
        "DROP",
        "ALTER",
        "VACUUM",
    }
    for function in functions:
        source = textwrap.dedent(inspect.getsource(function))
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    assert node.func.attr not in {"commit", "executemany", "executescript"}
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                first_token = node.value.strip().split(maxsplit=1)[0].upper() if node.value.strip() else ""
                assert first_token not in mutating_sql


def test_position_normalizer_is_the_only_runtime_decision_mutation_boundary() -> None:
    _assert_no_sqlite_mutation_sql_or_commit(
        (
            sma_policy.evaluate_sma_policy,
            SmaWithFilterStrategy.decide_snapshot,
            runtime_sma._load_signal_rows,
            runtime_sma._load_position_context,
            runtime_sma._policy_position_snapshot,
            runtime_sma._build_sma_with_filter_runtime_decision_from_normalized_db_readonly_impl,
            runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db,
            runtime_sma_snapshot.build_sma_with_filter_replay_bundle,
        )
    )

    normalizer_source = inspect.getsource(
        runtime_position_state_normalizer.PositionStateNormalizer.normalize_and_persist
    )
    orchestration_source = inspect.getsource(runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db)
    runtime_normalization_source = inspect.getsource(
        runtime_sma_adapter.normalize_position_state_before_strategy_decision
    )

    assert "mark_harmless_dust_positions(" in normalizer_source
    assert "reclassify_non_executable_open_exposure(" in normalizer_source
    assert "conn.commit()" in normalizer_source
    assert "normalize_and_persist(" not in orchestration_source
    assert "normalize_and_persist(" in runtime_normalization_source
    assert "build_sma_with_filter_runtime_decision_from_normalized_db(" in orchestration_source


def test_engine_orchestration_normalizes_before_snapshot_decision(monkeypatch) -> None:
    events: list[str] = []
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    old_pair = engine.settings.PAIR
    old_interval = engine.settings.INTERVAL

    def _normalize(conn, strategy, *, through_ts_ms=None, normalizer=None):
        events.append("normalize")
        return 0

    def _decide(conn, strategy, *, through_ts_ms=None):
        events.append("decision")
        return None

    monkeypatch.setattr(
        runtime_sma_adapter,
        "normalize_position_state_before_strategy_decision",
        _normalize,
    )
    monkeypatch.setattr(
        runtime_sma_adapter,
        "decide_sma_with_filter_runtime_snapshot_from_db",
        _decide,
    )

    try:
        object.__setattr__(engine.settings, "PAIR", "BTC_KRW")
        object.__setattr__(engine.settings, "INTERVAL", "1m")
        decision = engine.compute_strategy_decision_snapshot(
            conn,
            2,
            3,
            through_ts_ms=1_700_000_240_000,
            strategy_name="sma_with_filter",
        )
    finally:
        object.__setattr__(engine.settings, "PAIR", old_pair)
        object.__setattr__(engine.settings, "INTERVAL", old_interval)
        conn.close()

    assert decision is None
    assert events == ["normalize", "decision"]


def test_snapshot_orchestration_does_not_call_legacy_decide_facade(monkeypatch) -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_legacy_decide(*args, **kwargs):
        raise AssertionError("legacy decide facade was called")

    def _raise_legacy_normalized_db_decide(*args, **kwargs):
        raise AssertionError("legacy normalized DB strategy method was called")

    monkeypatch.setattr(SmaWithFilterStrategy, "decide", _raise_legacy_decide, raising=False)
    monkeypatch.setattr(
        SmaWithFilterStrategy,
        "_decide_from_normalized_db",
        _raise_legacy_normalized_db_decide,
        raising=False,
    )

    try:
        decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
            conn,
            strategy,
        )
    finally:
        conn.close()

    assert decision is not None
    assert decision.context["policy_decision_hash"].startswith("sha256:")


def test_runtime_sma_decision_helper_does_not_call_position_normalizer(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_mutating_normalizer(*args, **kwargs):
        raise AssertionError("runtime SMA decision helper must be read-only")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_mutating_normalizer,
    )

    try:
        result = runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert result is not None
    assert result.decision.policy_decision_hash.startswith("sha256:")


def test_replay_bundle_uses_read_only_normalized_builder(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_real_normalizer(*args, **kwargs):
        raise AssertionError("real normalizer should not run during replay bundle construction")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_real_normalizer,
    )

    try:
        bundle = runtime_sma_snapshot.build_sma_with_filter_replay_bundle(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert bundle is not None
    assert bundle["boundary_stages"]["snapshot_builder"] == (
        "runtime_sma_snapshot_builder.build_sma_with_filter_runtime_decision_from_normalized_db"
    )
    assert bundle["boundary_stages"]["pre_decision_normalization"] == (
        "engine.normalize_position_state_before_strategy_decision"
    )
    assert bundle["decision_context_schema_version"] == 1
    assert set(bundle["code_provenance"]) == {
        "schema_version",
        "source",
        "commit_sha",
        "dirty",
        "reason",
    }
    assert bundle["code_provenance"]["source"] in {"git", "unavailable"}
    assert bundle["final_typed_strategy_decision"]["policy_input_hash"] == bundle["policy_input_hash"]
    assert bundle["execution_decision_summary"]["final_signal"] == bundle["final_typed_strategy_decision"]["final_signal"]
    assert bundle["normalization_boundary"] == "engine.normalize_position_state_before_strategy_decision"
    assert bundle["normalization_updated_count"] is None
    assert bundle["decision_boundary_phase"] == "post_normalization_decision"
    assert isinstance(bundle["post_normalization_read_only_guard"], dict)
    assert bundle["post_decision_total_changes_delta"] == 0


def test_replay_decision_uses_read_only_normalizer_and_does_not_mutate_db(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    def _raise_mutating_normalizer(*args, **kwargs):
        raise AssertionError("mutating normalizer should not run during replay")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_mutating_normalizer,
    )
    changes_before = conn.total_changes

    try:
        bundle = runtime_sma_snapshot.build_sma_with_filter_replay_bundle(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
        changes_after = conn.total_changes
    finally:
        conn.close()

    assert bundle is not None
    assert changes_after == changes_before
    assert {
        "boundary_stages",
        "code_provenance",
        "market_snapshot",
        "position_snapshot",
        "policy_config",
        "execution_constraint_snapshot",
        "policy_input_hash",
        "policy_decision_hash",
        "pure_policy_hash",
        "replay_fingerprint",
        "final_typed_strategy_decision",
        "execution_decision_reconstructable",
        "execution_decision_reconstruction_reason",
    }.issubset(bundle)
    assert bundle["execution_decision_reconstructable"] is False
    assert bundle["execution_decision_reconstruction_reason"] == (
        "live_readiness_context_not_available_in_db_snapshot"
    )
    assert bundle["policy_input_hash"].startswith("sha256:")
    assert bundle["policy_decision_hash"].startswith("sha256:")
    assert bundle["pure_policy_hash"].startswith("sha256:")


def test_compute_signal_uses_direct_sma_with_filter_snapshot_path(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    events: list[str] = []
    original_builder = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db
    old_pair = engine.settings.PAIR
    old_interval = engine.settings.INTERVAL

    def _raise_legacy_decide(*args, **kwargs):
        raise AssertionError("legacy decide facade was called")

    def _raise_legacy_normalized_db_decide(*args, **kwargs):
        raise AssertionError("legacy normalized DB strategy method was called")

    def _builder(conn, strategy, *, through_ts_ms=None):
        events.append("builder")
        return original_builder(conn, strategy, through_ts_ms=through_ts_ms)

    monkeypatch.setattr(SmaWithFilterStrategy, "decide", _raise_legacy_decide, raising=False)
    monkeypatch.setattr(
        SmaWithFilterStrategy,
        "_decide_from_normalized_db",
        _raise_legacy_normalized_db_decide,
        raising=False,
    )
    monkeypatch.setattr(runtime_sma, "build_sma_with_filter_runtime_decision_from_normalized_db", _builder)

    try:
        object.__setattr__(engine.settings, "PAIR", "BTC_KRW")
        object.__setattr__(engine.settings, "INTERVAL", "1m")
        payload = engine.compute_signal(conn, 2, 3, strategy_name="sma_with_filter")
    finally:
        object.__setattr__(engine.settings, "PAIR", old_pair)
        object.__setattr__(engine.settings, "INTERVAL", old_interval)
        conn.close()

    assert payload is not None
    assert payload["strategy"] == "sma_with_filter"
    assert payload["policy_decision_hash"].startswith("sha256:")
    assert events == ["builder"]


def test_live_sma_handoff_does_not_serialize_legacy_dict_before_execution_summary(
    monkeypatch,
) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    old_pair = engine.settings.PAIR
    old_interval = engine.settings.INTERVAL

    def _raise_legacy_dict(self):
        raise AssertionError("legacy dict serialization should not be the runtime handoff")

    monkeypatch.setattr(runtime_sma.RuntimeSmaDecisionResult, "as_legacy_dict", _raise_legacy_dict)
    try:
        object.__setattr__(engine.settings, "PAIR", "BTC_KRW")
        object.__setattr__(engine.settings, "INTERVAL", "1m")
        handoff = engine.compute_signal_runtime_handoff(
            conn,
            2,
            3,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
            strategy_name="sma_with_filter",
        )
    finally:
        object.__setattr__(engine.settings, "PAIR", old_pair)
        object.__setattr__(engine.settings, "INTERVAL", old_interval)
        conn.close()

    assert isinstance(handoff, runtime_sma.RuntimeSmaDecisionResult)
    summary = engine.build_execution_decision_summary(
        decision_context=handoff.legacy_strategy_decision().context,
        readiness_payload={},
        raw_signal=handoff.decision.raw_signal,
        final_signal=handoff.decision.final_signal,
        final_reason=handoff.decision.final_reason,
    )
    assert summary.raw_signal == handoff.decision.raw_signal
    assert summary.final_signal == handoff.decision.final_signal


def test_typed_runtime_sma_result_preserves_policy_hashes_until_legacy_serialization() -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    try:
        result = runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert result is not None
    original_policy_decision_hash = result.decision.policy_decision_hash
    result.base_context["policy_decision_hash"] = "sha256:mutated_legacy_context"
    legacy_payload = result.as_legacy_dict()

    assert result.decision.policy_decision_hash == original_policy_decision_hash
    assert result.policy_hashes.policy_decision_hash == original_policy_decision_hash
    assert result.policy_observability["policy_decision_hash"] == original_policy_decision_hash
    assert legacy_payload["policy_decision_hash"] == original_policy_decision_hash
    assert legacy_payload["pure_policy_trace"]["policy_decision_hash"] == original_policy_decision_hash

    typed_context = result.runtime_decision_context
    serialized_context = typed_context.as_dict()
    serialized_context["policy_decision_hash"] = "sha256:mutated_serialized_context"
    serialized_context["blocked_filters"] = ["mutated"]
    assert typed_context.policy_decision_hash == original_policy_decision_hash
    assert list(typed_context.blocked_filters) == list(result.decision.blocked_filters)
    assert result.decision.policy_decision_hash == original_policy_decision_hash


def test_persistence_context_serializes_typed_policy_and_execution_summary_fields() -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    try:
        result = runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert result is not None
    decision_context = result.legacy_strategy_decision().context
    summary = engine.build_execution_decision_summary(
        decision_context=decision_context,
        readiness_payload={},
        raw_signal=result.decision.raw_signal,
        final_signal=result.decision.final_signal,
        final_reason=result.decision.final_reason,
    )
    persisted = engine.prepare_strategy_decision_persistence_context(
        decision_context=decision_context,
        execution_decision_summary=summary,
        readiness_payload={},
    )

    assert persisted["policy_contract_hash"] == result.decision.policy_contract_hash
    assert persisted["policy_input_hash"] == result.decision.policy_input_hash
    assert persisted["policy_decision_hash"] == result.decision.policy_decision_hash
    assert persisted["execution_decision"]["final_signal"] == summary.final_signal  # type: ignore[index]
    assert persisted["execution_decision"]["final_action"] == summary.final_action  # type: ignore[index]


def test_runtime_replay_export_uses_direct_sma_with_filter_snapshot_path(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    events: list[str] = []
    original_builder = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db
    strategy = create_sma_with_filter_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_legacy_decide(*args, **kwargs):
        raise AssertionError("legacy decide facade was called")

    def _raise_legacy_normalized_db_decide(*args, **kwargs):
        raise AssertionError("legacy normalized DB strategy method was called")

    def _builder(conn, strategy, *, through_ts_ms=None):
        events.append("builder")
        return original_builder(conn, strategy, through_ts_ms=through_ts_ms)

    monkeypatch.setattr(SmaWithFilterStrategy, "decide", _raise_legacy_decide, raising=False)
    monkeypatch.setattr(
        SmaWithFilterStrategy,
        "_decide_from_normalized_db",
        _raise_legacy_normalized_db_decide,
        raising=False,
    )
    monkeypatch.setattr(runtime_sma, "build_sma_with_filter_runtime_decision_from_normalized_db", _builder)

    try:
        events_out = export_runtime_replay_decisions(
            conn=conn,
            strategy=strategy,
            through_ts_list=[1_700_000_000_000 + 11 * 60_000],
            market="BTC_KRW",
            interval="1m",
        )
    finally:
        conn.close()

    assert len(events_out) == 1
    assert events_out[0]["strategy_name"] == "sma_with_filter"
    assert events == ["builder"]


def test_research_kernel_reevaluates_policy_with_flat_simulated_position() -> None:
    result = run_sma_backtest(
        dataset=_dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0]),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert result.decisions
    decision = result.decisions[-1]
    assert decision["research_policy_recomputed_with_simulated_position"] is True
    assert decision["research_policy_comparable"] is True
    assert decision["research_policy_position_terminal_state"] == "research_simulated_flat"
    assert decision["pure_policy_trace"]["position"]["terminal_state"] == "research_simulated_flat"
    assert decision["final_signal"] == decision["pure_policy_trace"]["final_signal"] == "BUY"


def test_research_kernel_missing_sma_policy_metadata_fails_closed_not_comparable() -> None:
    dataset = _dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0])
    event = ResearchDecisionEvent(
        candle_ts=dataset.candles[-1].ts,
        decision_ts=dataset.candles[-1].ts + 60_000,
        strategy_name="sma_with_filter",
        strategy_version="sma_with_filter.research_runtime_contract.v2",
        raw_signal="BUY",
        final_signal="BUY",
        reason="event-first buy must not be authoritative",
        feature_snapshot={},
        strategy_diagnostics={},
        entry_signal="BUY",
        exit_signal="BUY",
        exit_intent={"mode": "evaluate_exit_policy"},
        extra_payload={},
    )

    result = backtest_kernel.run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=(event,),
    )

    assert result.decisions
    decision = result.decisions[-1]
    assert decision["final_signal"] == "HOLD"
    assert decision["blocked"] is True
    assert decision["entry_reason"] == "research_policy_decision_missing_not_comparable"
    assert decision["research_policy_recomputed_with_simulated_position"] is False
    assert decision["research_policy_unsupported"] is True
    assert decision["research_policy_comparable"] is False
    assert decision["research_policy_unsupported_reason"] == (
        "research_policy_decision_missing_not_comparable"
    )


def test_research_kernel_open_position_exit_fields_come_from_policy_decision() -> None:
    dataset = _dataset_from_closes([12.0, 12.0, 12.0, 12.0, 11.0])
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(dataset)
    event = replace(
        events[-1],
        extra_payload={**events[-1].extra_payload, "prev_above": True},
    )

    result = backtest_kernel.run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=(event,),
        portfolio_policy=PortfolioPolicy(
            schema_version=1,
            starting_cash_krw=1_000_000.0,
            quote_currency="KRW",
            initial_position_qty=1.0,
            cash_interest_policy="zero",
            position_sizing=PositionSizingPolicy(
                type="fractional_cash",
                buy_fraction=0.99,
                sell_policy="sell_all_available_position",
                cash_buffer_policy="retain_1_percent_before_fees",
            ),
            source="unit_test",
        ),
    )

    assert result.decisions
    decision = result.decisions[-1]
    assert decision["research_policy_recomputed_with_simulated_position"] is True
    assert decision["final_signal"] == decision["pure_policy_trace"]["final_signal"]
    assert decision["exit_rule"] == decision["pure_policy_trace"]["exit_rule"] == "opposite_cross"
    assert decision["exit_filter_suppression_prevented"] == (
        decision["pure_policy_trace"]["exit_filter_suppression_prevented"]
    )


def test_research_pending_fill_snapshot_is_not_comparable_or_flat() -> None:
    snapshot = backtest_kernel._research_position_snapshot(
        qty=1.0,
        sellable_qty=1.0,
        pending_buy_qty=0.0,
        pending_sell_qty=1.0,
        entry_ts=1_700_000_000_000,
        entry_price=10.0,
        candle_ts=1_700_000_240_000,
        market_price=11.0,
    )

    assert snapshot.terminal_state == "research_pending_fill_not_policy_comparable"
    assert snapshot.entry_allowed is False
    assert snapshot.exit_allowed is False
    assert snapshot.effective_flat is True
    assert snapshot.entry_block_reason == "research_pending_fill_not_policy_comparable"
    assert snapshot.exit_block_reason == "research_pending_fill_not_policy_comparable"


def test_final_sma_decision_harmless_dust_is_explicit_effective_flat_for_entry() -> None:
    position = PositionSnapshot(
        in_position=False,
        entry_allowed=True,
        exit_allowed=False,
        exit_block_reason="dust_only_remainder",
        terminal_state="dust_only",
        raw_qty_open=0.00009629,
        raw_total_asset_qty=0.00009629,
        dust_tracking_lot_count=1,
        dust_classification="harmless_dust",
        dust_state="harmless_dust",
        effective_flat=True,
        has_any_position_residue=True,
        has_non_executable_residue=True,
        has_dust_only_remainder=True,
    )

    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=position,
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )

    assert decision.final_signal == "BUY"
    assert decision.position_snapshot.terminal_state == "dust_only"
    assert decision.position_snapshot.dust_classification == "harmless_dust"
    assert decision.position_snapshot.effective_flat is True
    assert decision.position_snapshot.has_dust_only_remainder is True


def test_final_sma_decision_blocking_dust_fails_closed_not_flat() -> None:
    position = PositionSnapshot(
        in_position=False,
        entry_allowed=False,
        exit_allowed=False,
        entry_block_reason="blocking_dust_not_tradable",
        exit_block_reason="dust_only_remainder",
        terminal_state="dust_only",
        raw_qty_open=0.0002,
        raw_total_asset_qty=0.0002,
        dust_tracking_lot_count=1,
        dust_classification="blocking_dust",
        dust_state="blocking_dust",
        effective_flat=False,
        has_any_position_residue=True,
        has_non_executable_residue=True,
        has_dust_only_remainder=True,
    )

    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=position,
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )

    assert decision.final_signal == "HOLD"
    assert decision.final_reason == "blocking_dust_not_tradable"
    assert decision.position_snapshot.terminal_state == "dust_only"
    assert decision.position_snapshot.dust_classification == "blocking_dust"
    assert decision.position_snapshot.effective_flat is False


def test_final_sma_decision_unsupported_states_fail_closed_not_flat() -> None:
    unsupported = (
        _open_position(
            exit_allowed=False,
            exit_block_reason="reserved_exit_pending",
            terminal_state="reserved_exit_pending",
            reserved_exit_lot_count=1,
            sellable_executable_lot_count=0,
        ),
        _open_position(
            exit_allowed=False,
            exit_block_reason="no_executable_exit_lot",
            terminal_state="non_executable_position",
            open_lot_count=0,
            sellable_executable_lot_count=0,
            has_executable_exposure=False,
            has_non_executable_residue=True,
        ),
        PositionSnapshot(
            in_position=False,
            entry_allowed=False,
            exit_allowed=False,
            entry_block_reason="authority_missing_recovery_required",
            exit_block_reason="authority_missing_recovery_required",
            terminal_state="authority_gap",
            has_any_position_residue=True,
        ),
        PositionSnapshot(
            in_position=False,
            entry_allowed=False,
            exit_allowed=False,
            entry_block_reason="recovery_required_present",
            exit_block_reason="recovery_required_present",
            terminal_state="recovery_required",
            has_any_position_residue=True,
        ),
    )

    for position in unsupported:
        decision = evaluate_sma_final_decision(
            market=_market_window(),
            position=position,
            config=_policy_config(),
            execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
            exit_policy_config=_exit_policy_config(),
        )

        assert decision.final_signal == "HOLD"
        assert decision.position_snapshot.terminal_state == position.terminal_state
        assert decision.final_reason in {
            position.entry_block_reason,
            position.exit_block_reason,
            "position held: no exit rule triggered",
        }
        assert decision.position_snapshot.terminal_state != "flat"


def test_research_adapter_placeholder_is_not_full_position_equivalence() -> None:
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0]))

    assert events[-1].extra_payload["non_authoritative_event_adapter"] is True
    assert "entry_decision" not in events[-1].extra_payload
    assert "pure_policy_hash" not in events[-1].extra_payload
    assert "pure_policy_trace" not in events[-1].extra_payload
