from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.strategy.sma import SmaCrossStrategy, create_sma_strategy
from bithumb_bot.strategy_config import (
    normalize_exit_rule_names,
    sma_strategy_config_from_settings,
)


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


@pytest.fixture
def settings_guard():
    names = (
        "SMA_SHORT",
        "SMA_LONG",
        "PAIR",
        "INTERVAL",
        "STRATEGY_EXIT_RULES",
        "STRATEGY_EXIT_MAX_HOLDING_MIN",
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
        "STRATEGY_ENTRY_SLIPPAGE_BPS",
        "LIVE_FEE_RATE_ESTIMATE",
        "ENTRY_EDGE_BUFFER_RATIO",
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
        "BUY_FRACTION",
        "MAX_ORDER_KRW",
    )
    original = {name: getattr(settings, name) for name in names}
    try:
        yield
    finally:
        for name, value in original.items():
            object.__setattr__(settings, name, value)


def test_sma_strategy_config_factory_preserves_settings_defaults(settings_guard) -> None:
    object.__setattr__(settings, "SMA_SHORT", 5)
    object.__setattr__(settings, "SMA_LONG", 13)
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "STRATEGY_EXIT_RULES", "opposite_cross, max_holding_time")
    object.__setattr__(settings, "STRATEGY_EXIT_MAX_HOLDING_MIN", 45)
    object.__setattr__(settings, "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", 0.012)
    object.__setattr__(settings, "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO", 0.003)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 7.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0015)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0007)
    object.__setattr__(settings, "STRATEGY_MIN_EXPECTED_EDGE_RATIO", 0.002)
    object.__setattr__(settings, "BUY_FRACTION", 0.42)
    object.__setattr__(settings, "MAX_ORDER_KRW", 55_000.0)

    config = sma_strategy_config_from_settings()

    assert config.short_n == 5
    assert config.long_n == 13
    assert config.pair == "BTC_KRW"
    assert config.interval == "1m"
    assert config.exit_rule_names == ("opposite_cross", "max_holding_time")
    assert config.exit_max_holding_min == 45
    assert config.exit_min_take_profit_ratio == pytest.approx(0.012)
    assert config.exit_small_loss_tolerance_ratio == pytest.approx(0.003)
    assert config.slippage_bps == pytest.approx(7.0)
    assert config.live_fee_rate_estimate == pytest.approx(0.0015)
    assert config.entry_edge_buffer_ratio == pytest.approx(0.0007)
    assert config.strategy_min_expected_edge_ratio == pytest.approx(0.002)
    assert config.buy_fraction == pytest.approx(0.42)
    assert config.max_order_krw == pytest.approx(55_000.0)


def test_existing_sma_constructor_behavior_is_preserved() -> None:
    strategy = SmaCrossStrategy(short_n=2, long_n=3)

    assert strategy.short_n == 2
    assert strategy.long_n == 3
    assert strategy.pair == settings.PAIR
    assert strategy.interval == settings.INTERVAL
    assert tuple(strategy.exit_rule_names) == normalize_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    assert strategy.buy_fraction == pytest.approx(float(settings.BUY_FRACTION))
    assert strategy.max_order_krw == pytest.approx(float(settings.MAX_ORDER_KRW))


def test_sma_from_config_preserves_stable_decision_context_fields() -> None:
    config = replace(
        sma_strategy_config_from_settings(short_n=2, long_n=3),
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        strategy_min_expected_edge_ratio=0.0,
    )
    direct = create_sma_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        strategy_min_expected_edge_ratio=0.0,
    )
    from_config = SmaCrossStrategy.from_config(config)
    conn_a = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    conn_b = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        direct_decision = direct.decide(conn_a)
        config_decision = from_config.decide(conn_b)
    finally:
        conn_a.close()
        conn_b.close()

    assert direct_decision is not None
    assert config_decision is not None
    assert config_decision.context["entry"]["intent"] == direct_decision.context["entry"]["intent"]
    assert config_decision.context["gap_ratio"] == pytest.approx(direct_decision.context["gap_ratio"])
    assert config_decision.context["signal_strength_label"] == direct_decision.context["signal_strength_label"]


def test_entry_intent_uses_config_values_without_mutating_settings(settings_guard) -> None:
    object.__setattr__(settings, "BUY_FRACTION", 0.99)
    object.__setattr__(settings, "MAX_ORDER_KRW", 999_999.0)
    config = replace(
        sma_strategy_config_from_settings(short_n=2, long_n=3),
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        strategy_min_expected_edge_ratio=0.0,
        buy_fraction=0.37,
        max_order_krw=12_345.0,
    )
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = SmaCrossStrategy.from_config(config).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.context["entry"]["intent"]["budget_fraction_of_cash"] == pytest.approx(0.37)
    assert decision.context["entry"]["intent"]["max_budget_krw"] == pytest.approx(12_345.0)


def test_position_lot_cost_context_uses_config_values_without_mutating_settings(settings_guard) -> None:
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 99.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.99)
    config = replace(
        sma_strategy_config_from_settings(short_n=2, long_n=3),
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=4.5,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0017,
        strategy_min_expected_edge_ratio=0.0,
    )
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = SmaCrossStrategy.from_config(config).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    costs = decision.context["position_lot_interpretation_costs"]
    assert costs["exit_slippage_bps"] == pytest.approx(4.5)
    assert costs["exit_buffer_ratio"] == pytest.approx(0.0017)
    assert settings.STRATEGY_ENTRY_SLIPPAGE_BPS == pytest.approx(99.0)
    assert settings.ENTRY_EDGE_BUFFER_RATIO == pytest.approx(0.99)


def test_invalid_sma_short_long_validation_remains() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="short"):
            SmaCrossStrategy(short_n=3, long_n=3).decide(conn)
    finally:
        conn.close()
