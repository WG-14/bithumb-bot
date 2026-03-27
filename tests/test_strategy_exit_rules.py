from __future__ import annotations

import os

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.strategy.base import PositionContext
from bithumb_bot.strategy.exit_rules import OppositeCrossExitRule
from bithumb_bot.strategy.sma import create_sma_strategy


def _insert_candles(conn, closes: list[float], *, base_ts: int = 1_700_000_000_000) -> int:
    for idx, close in enumerate(closes):
        ts = base_ts + idx * 60_000
        conn.execute(
            """
            INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
        )
    conn.commit()
    return base_ts + (len(closes) - 1) * 60_000


def _insert_open_position_lot(conn, *, entry_ts: int, entry_price: float, qty_open: float = 1.0) -> None:
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts,
            entry_price, qty_open, entry_fee_total, strategy_name, entry_decision_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            1,
            "entry_1",
            "fill_1",
            int(entry_ts),
            float(entry_price),
            float(qty_open),
            0.0,
            "sma_cross",
            None,
        ),
    )
    conn.commit()


def test_exit_rule_can_be_swapped_with_same_entry_signal(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")
    db_path = str(tmp_path / "exit_swap.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        candle_ts = _insert_candles(conn, [100.0, 100.0, 100.0, 100.0, 100.0])
        _insert_open_position_lot(conn, entry_ts=candle_ts - (20 * 60_000), entry_price=100.0)

        opposite_only = create_sma_strategy(
            short_n=2,
            long_n=3,
            exit_rule_names=["opposite_cross"],
            exit_max_holding_min=0,
        ).decide(conn)
        max_hold_only = create_sma_strategy(
            short_n=2,
            long_n=3,
            exit_rule_names=["max_holding_time"],
            exit_max_holding_min=10,
        ).decide(conn)
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert opposite_only is not None
    assert max_hold_only is not None
    assert opposite_only.signal == "HOLD"
    assert max_hold_only.signal == "SELL"
    assert max_hold_only.context["exit"]["rule"] == "max_holding_time"


def test_opposite_cross_exit_and_position_context_are_recorded(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")
    db_path = str(tmp_path / "exit_opposite_cross.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        candle_ts = _insert_candles(conn, [100.0, 100.0, 120.0, 120.0, 80.0])
        _insert_open_position_lot(conn, entry_ts=candle_ts - (2 * 60_000), entry_price=105.0)
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            exit_rule_names=["opposite_cross", "max_holding_time"],
            exit_max_holding_min=999,
        ).decide(conn)
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert decision is not None
    assert decision.signal == "SELL"
    assert decision.context["position"]["in_position"] is True
    assert decision.context["position"]["entry_ts"] == candle_ts - (2 * 60_000)
    assert decision.context["position"]["entry_price"] == 105.0
    assert decision.context["position"]["holding_time_sec"] >= 120.0
    assert "unrealized_pnl_ratio" in decision.context["position"]
    assert decision.context["exit"]["triggered"] is True
    assert decision.context["exit"]["rule"] == "opposite_cross"


def test_opposite_cross_is_deferred_when_pnl_is_below_take_profit_floor() -> None:
    rule = OppositeCrossExitRule(min_take_profit_ratio=0.002, live_fee_rate_estimate=0.0004)
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=0.001)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=100.1,
        signal_context={"base_signal": "SELL"},
    )

    assert decision.should_exit is False
    assert decision.context["deferred_by_min_take_profit_floor"] is True
    assert decision.context["required_take_profit_ratio"] == 0.002


def test_opposite_cross_exits_when_pnl_is_above_take_profit_floor() -> None:
    rule = OppositeCrossExitRule(min_take_profit_ratio=0.002, live_fee_rate_estimate=0.0004)
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=0.003)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=100.3,
        signal_context={"base_signal": "SELL"},
    )

    assert decision.should_exit is True
    assert decision.context["deferred_by_min_take_profit_floor"] is False


def test_max_holding_exit_is_not_blocked_by_take_profit_floor_when_opposite_cross_deferred(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")
    db_path = str(tmp_path / "exit_take_profit_floor_max_holding.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        candle_ts = _insert_candles(conn, [100.0, 100.0, 120.0, 120.0, 99.95])
        _insert_open_position_lot(conn, entry_ts=candle_ts - (20 * 60_000), entry_price=100.0)
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            exit_rule_names=["opposite_cross", "max_holding_time"],
            exit_max_holding_min=10,
            exit_min_take_profit_ratio=0.002,
            live_fee_rate_estimate=0.0004,
        ).decide(conn)
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert decision is not None
    assert decision.signal == "SELL"
    assert decision.context["exit"]["rule"] == "max_holding_time"
    assert decision.context["exit"]["evaluations"][0]["context"]["deferred_by_min_take_profit_floor"] is True


def test_live_fee_rate_raises_take_profit_floor_for_opposite_cross_exit() -> None:
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=0.003)
    signal_context: dict[str, object] = {"base_signal": "SELL"}

    low_fee_decision = OppositeCrossExitRule(
        min_take_profit_ratio=0.001,
        live_fee_rate_estimate=0.001,
    ).evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=100.3,
        signal_context=signal_context,
    )
    high_fee_decision = OppositeCrossExitRule(
        min_take_profit_ratio=0.001,
        live_fee_rate_estimate=0.002,
    ).evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=100.3,
        signal_context=signal_context,
    )

    assert low_fee_decision.should_exit is True
    assert high_fee_decision.should_exit is False
    assert low_fee_decision.context["required_take_profit_ratio"] == 0.002
    assert high_fee_decision.context["required_take_profit_ratio"] == 0.004
