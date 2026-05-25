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
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange, ExecutionTimingPolicy
from bithumb_bot.strategy.sma import create_sma_with_filter_strategy


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
