from __future__ import annotations

import ast
import inspect
import sqlite3

from bithumb_bot.core import sma_policy
from bithumb_bot.core.sma_policy import (
    ExecutionConstraintSnapshot,
    MarketWindow,
    PositionSnapshot,
    SmaPolicyConfig,
    evaluate_sma_policy,
)
from bithumb_bot.market_regime import MARKET_REGIME_VERSION
from bithumb_bot.research.backtest_engine import SmaWithFilterDecisionAdapter
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research import backtest_kernel
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange, ExecutionTimingPolicy
from bithumb_bot.strategy.sma import create_sma_with_filter_strategy
from bithumb_bot.strategy import sma as runtime_sma
from bithumb_bot.strategy.exit_rules import ExitPolicyConfig, evaluate_sma_exit_policy


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
        runtime_decision = create_sma_with_filter_strategy(
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
        ).decide(conn)
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

    assert runtime_decision is not None
    assert runtime_decision.context["raw_signal"] == research_event.raw_signal == "BUY"
    assert runtime_decision.context["entry_signal"] == research_event.entry_signal == "BUY"
    assert runtime_decision.context["final_signal"] == research_event.final_signal == "BUY"
    assert tuple(runtime_decision.context["blocked_filters"]) == research_event.blocked_filters == ()
    assert runtime_decision.context["pure_policy_hash"].startswith("sha256:")
    assert research_event.extra_payload["pure_policy_hash"].startswith("sha256:")


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
    policy_trace = first.extra_payload["pure_policy_trace"]
    assert first.extra_payload["prev_above"] is None
    assert policy_trace["market"]["previous_cross_state"] == "unknown"
    assert policy_trace["market"]["allow_initial_cross"] is False
    assert first.raw_signal == policy_trace["raw_signal"] == "HOLD"
    assert first.entry_signal == policy_trace["entry_signal"] == "HOLD"
    assert first.final_signal == policy_trace["final_signal"] == "HOLD"
    assert first.reason == policy_trace["final_reason"]


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
    normalizer_source = inspect.getsource(runtime_sma.PositionStateNormalizer.normalize_and_persist)
    decide_source = inspect.getsource(runtime_sma.SmaWithFilterStrategy.decide)
    orchestration_source = inspect.getsource(runtime_sma.decide_sma_with_filter_snapshot_from_db)

    assert "mark_harmless_dust_positions" not in load_position_source
    assert "reclassify_non_executable_open_exposure" not in load_position_source
    assert "conn.commit()" not in load_position_source
    assert "mark_harmless_dust_positions" in normalizer_source
    assert "reclassify_non_executable_open_exposure" in normalizer_source
    assert "conn.commit()" in normalizer_source
    assert "normalize_and_persist(" not in decide_source
    assert "_load_position_context(" in decide_source
    assert "normalize_and_persist(" in orchestration_source
    assert "strategy.decide(" in orchestration_source


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
        decision = create_sma_with_filter_strategy(
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
        ).decide(wrapped)
    finally:
        wrapped.close()

    assert decision is not None
    assert wrapped.commit_count == 0


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
    monkeypatch.setattr(runtime_sma, "mark_harmless_dust_positions", lambda *args, **kwargs: 1)
    monkeypatch.setattr(
        runtime_sma,
        "reclassify_non_executable_open_exposure",
        lambda *args, **kwargs: 0,
    )

    try:
        updated = runtime_sma.PositionStateNormalizer().normalize_and_persist(
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


def test_snapshot_orchestration_normalizes_before_policy_evaluation(monkeypatch) -> None:
    events: list[str] = []
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    original_decide_snapshot = runtime_sma.SmaWithFilterStrategy.decide_snapshot

    class _Normalizer:
        def normalize_and_persist(self, conn, **kwargs):
            events.append("normalize")
            return 0

    def _decide_snapshot(self, **kwargs):
        events.append("policy")
        return original_decide_snapshot(self, **kwargs)

    monkeypatch.setattr(runtime_sma.SmaWithFilterStrategy, "decide_snapshot", _decide_snapshot)

    try:
        decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
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
            normalizer=_Normalizer(),
        )
    finally:
        conn.close()

    assert decision is not None
    assert events == ["normalize", "policy"]


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
    assert decision["research_policy_position_terminal_state"] == "research_simulated_flat"
    assert decision["pure_policy_trace"]["position"]["terminal_state"] == "research_simulated_flat"


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

    placeholder_state = events[-1].extra_payload["pure_policy_trace"]["position"]["terminal_state"]
    assert placeholder_state == "research_event_adapter_position_deferred_to_kernel"
