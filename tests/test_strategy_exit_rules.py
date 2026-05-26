from __future__ import annotations

import os

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.strategy.base import PositionContext

from bithumb_bot.strategy.exit_rules import (
    MaxHoldingTimeExitRule,
    OppositeCrossExitRule,
    StopLossExitRule,
    create_exit_rules,
    create_sma_exit_rules,
    merge_exit_rules,
)
from bithumb_bot.strategy.sma import (
    create_legacy_sma_with_filter_db_adapter,
    create_sma_strategy,
)

create_sma_with_filter_strategy = create_legacy_sma_with_filter_db_adapter


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
            entry_price, qty_open, executable_lot_count, dust_tracking_lot_count,
            position_semantic_basis, position_state, entry_fee_total, strategy_name, entry_decision_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            1,
            "entry_1",
            "fill_1",
            int(entry_ts),
            float(entry_price),
            float(qty_open),
            1,
            0,
            "lot-native",
            "open_exposure",
            0.0,
            "sma_cross",
            None,
        ),
    )
    conn.commit()


def test_exit_rule_can_be_swapped_with_same_entry_signal(tmp_path, relaxed_test_order_rules) -> None:
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


def test_opposite_cross_exit_and_position_context_are_recorded(
    tmp_path,
    relaxed_test_order_rules,
) -> None:
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


def test_runtime_stop_loss_exits_while_raw_signal_is_hold(
    tmp_path,
    relaxed_test_order_rules,
) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")
    db_path = str(tmp_path / "exit_stop_loss_hold.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        candle_ts = _insert_candles(conn, [90.0, 90.0, 90.0, 90.0, 90.0])
        _insert_open_position_lot(conn, entry_ts=candle_ts - (2 * 60_000), entry_price=100.0)
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            exit_rule_names=["stop_loss", "opposite_cross", "max_holding_time"],
            exit_stop_loss_ratio=0.05,
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
    assert decision.context["raw_signal"] == "HOLD"
    assert decision.context["exit"]["rule"] == "stop_loss"


def test_stop_loss_rule_rejects_negative_ratio() -> None:
    with pytest.raises(ValueError, match="stop_loss_ratio"):
        StopLossExitRule(stop_loss_ratio=-0.01)


def test_positive_stop_loss_ratio_requires_stop_loss_rule() -> None:
    with pytest.raises(ValueError, match="does not include stop_loss"):
        create_sma_exit_rules(
            rule_names=["opposite_cross", "max_holding_time"],
            stop_loss_ratio=0.01,
            max_holding_sec=0.0,
            min_take_profit_ratio=0.0,
            live_fee_rate_estimate=0.0,
            small_loss_tolerance_ratio=0.0,
        )


def test_common_exit_factory_rejects_sma_owned_opposite_cross() -> None:
    with pytest.raises(ValueError, match="unknown exit rule='opposite_cross'"):
        create_exit_rules(
            rule_names=["stop_loss", "opposite_cross"],
            stop_loss_ratio=0.01,
            max_holding_sec=60.0,
        )


def test_sma_exit_factory_still_supports_plugin_owned_opposite_cross() -> None:
    rules = create_sma_exit_rules(
        rule_names=["stop_loss", "opposite_cross", "max_holding_time"],
        stop_loss_ratio=0.01,
        max_holding_sec=60.0,
        min_take_profit_ratio=0.0,
        live_fee_rate_estimate=0.0,
        small_loss_tolerance_ratio=0.0,
    )

    assert [rule.name for rule in rules] == ["stop_loss", "opposite_cross", "max_holding_time"]


def test_merge_exit_rules_preserves_common_stop_loss_when_plugin_returns_empty_list() -> None:
    common = [
        StopLossExitRule(stop_loss_ratio=0.05),
        MaxHoldingTimeExitRule(max_holding_sec=60.0),
    ]

    merged = merge_exit_rules(common_exit_rules=common, strategy_exit_rules=[])

    assert merged == common
    decision = merged[0].evaluate(
        position=PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=-0.06),
        candle_ts=1_700_000_000_000,
        market_price=94.0,
        signal_context={"base_signal": "HOLD"},
    )
    assert decision.should_exit is True
    assert decision.context["threshold_ratio"] == 0.05


def test_merge_exit_rules_preserves_common_max_holding_when_plugin_returns_custom_rule() -> None:
    class CustomExitRule:
        name = "custom_strategy_exit"

        def evaluate(self, *, position, candle_ts, market_price, signal_context):
            return StopLossExitRule(stop_loss_ratio=1.0).evaluate(
                position=position,
                candle_ts=candle_ts,
                market_price=market_price,
                signal_context=signal_context,
            )

    common = [MaxHoldingTimeExitRule(max_holding_sec=60.0)]
    custom = CustomExitRule()

    merged = merge_exit_rules(common_exit_rules=common, strategy_exit_rules=[custom])

    assert [rule.name for rule in merged] == ["max_holding_time", "custom_strategy_exit"]
    decision = merged[0].evaluate(
        position=PositionContext(
            in_position=True,
            entry_price=100.0,
            qty_open=1.0,
            holding_time_sec=60.0,
        ),
        candle_ts=1_700_000_000_000,
        market_price=100.0,
        signal_context={"base_signal": "HOLD"},
    )
    assert decision.should_exit is True


def test_merge_exit_rules_uses_common_rule_for_duplicate_common_name_while_preserving_sma_order() -> None:
    common_stop = StopLossExitRule(stop_loss_ratio=0.05)
    common_max_hold = MaxHoldingTimeExitRule(max_holding_sec=60.0)
    plugin_stop = StopLossExitRule(stop_loss_ratio=0.50)
    plugin_opposite = OppositeCrossExitRule()
    plugin_max_hold = MaxHoldingTimeExitRule(max_holding_sec=3600.0)

    merged = merge_exit_rules(
        common_exit_rules=[common_stop, common_max_hold],
        strategy_exit_rules=[plugin_stop, plugin_opposite, plugin_max_hold],
    )

    assert [rule.name for rule in merged] == ["stop_loss", "opposite_cross", "max_holding_time"]
    assert merged[0] is common_stop
    assert merged[1] is plugin_opposite
    assert merged[2] is common_max_hold


def test_runtime_raw_buy_open_position_checks_stop_loss_before_entry_gate(
    tmp_path,
    relaxed_test_order_rules,
) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")
    db_path = str(tmp_path / "exit_stop_loss_raw_buy.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        candle_ts = _insert_candles(conn, [10.0, 10.0, 10.0, 10.0, 11.0])
        _insert_open_position_lot(conn, entry_ts=candle_ts - (2 * 60_000), entry_price=12.0)
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            exit_rule_names=["stop_loss", "opposite_cross", "max_holding_time"],
            exit_stop_loss_ratio=0.05,
            exit_max_holding_min=999,
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=False,
            market_regime_enabled=False,
            candidate_regime_policy={"allowed": True},
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
    assert decision.context["raw_signal"] == "BUY"
    assert decision.context["entry_allowed"] is False
    assert decision.context["entry_blocked"] is False
    assert decision.context["protective_exit_overrode_entry"] is True
    assert decision.context["entry_block_reason"] is None
    assert decision.context["exit"]["rule"] == "stop_loss"


def test_runtime_raw_buy_open_position_checks_max_holding_before_entry_gate(
    tmp_path,
    relaxed_test_order_rules,
) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")
    db_path = str(tmp_path / "exit_max_holding_raw_buy.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        candle_ts = _insert_candles(conn, [10.0, 10.0, 10.0, 10.0, 11.0])
        _insert_open_position_lot(conn, entry_ts=candle_ts - (20 * 60_000), entry_price=10.0)
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            exit_rule_names=["stop_loss", "opposite_cross", "max_holding_time"],
            exit_stop_loss_ratio=0.05,
            exit_max_holding_min=10,
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=False,
            market_regime_enabled=False,
            candidate_regime_policy={"allowed": True},
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
    assert decision.context["raw_signal"] == "BUY"
    assert decision.context["entry_allowed"] is False
    assert decision.context["entry_blocked"] is False
    assert decision.context["protective_exit_overrode_entry"] is True
    assert decision.context["entry_block_reason"] is None
    assert decision.context["exit"]["rule"] == "max_holding_time"


def test_runtime_stop_loss_priority_over_opposite_cross_when_entry_filters_would_block(
    tmp_path,
    relaxed_test_order_rules,
) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")
    db_path = str(tmp_path / "exit_stop_loss_priority.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        candle_ts = _insert_candles(conn, [100.0, 100.0, 120.0, 120.0, 80.0])
        _insert_open_position_lot(conn, entry_ts=candle_ts - (2 * 60_000), entry_price=100.0)
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            min_gap_ratio=0.40,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=False,
            market_regime_enabled=False,
            candidate_regime_policy={"allowed": True},
            exit_rule_names=["opposite_cross", "stop_loss", "max_holding_time"],
            exit_stop_loss_ratio=0.05,
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
    assert decision.context["raw_signal"] == "SELL"
    assert decision.context["raw_filter_would_block"] is True
    assert decision.context["entry_blocked"] is False
    assert decision.context["protective_exit_overrode_entry"] is False
    assert decision.context["exit_filter_suppression_prevented"] is True
    assert decision.context["exit"]["rule"] == "stop_loss"
    assert [item["rule"] for item in decision.context["exit"]["evaluations"]] == ["stop_loss"]


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
    assert decision.context["filter_applied"] is True
    assert decision.context["deferred_by_min_take_profit_floor"] is True
    assert decision.context["min_profit_floor"] == 0.002
    assert decision.context["base_signal"] == "SELL"
    assert decision.context["unrealized_pnl_ratio"] == 0.001


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
    assert decision.context["filter_applied"] is False
    assert decision.context["deferred_by_min_take_profit_floor"] is False
    assert decision.context["min_profit_floor"] == 0.002


def test_opposite_cross_exits_on_large_loss_for_risk_defense() -> None:
    rule = OppositeCrossExitRule(min_take_profit_ratio=0.002, live_fee_rate_estimate=0.0004)
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=-0.01)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=99.0,
        signal_context={"base_signal": "SELL"},
    )

    assert decision.should_exit is True
    assert decision.context["filter_applied"] is False
    assert decision.context["small_loss_zone"] is False
    assert decision.context["small_gain_zone"] is False


def test_adverse_move_without_opposite_cross_or_max_holding_does_not_exit() -> None:
    rule = OppositeCrossExitRule(min_take_profit_ratio=0.0, live_fee_rate_estimate=0.0)
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=-0.10)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=90.0,
        signal_context={"base_signal": "HOLD"},
    )

    assert decision.should_exit is False
    assert decision.context["opposite_cross_triggered"] is False


def test_stop_loss_exits_on_adverse_move_without_raw_sell_signal() -> None:
    rule = StopLossExitRule(stop_loss_ratio=0.03)
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=-0.031)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=96.9,
        signal_context={"base_signal": "HOLD", "raw_signal": "HOLD", "entry_signal": "HOLD"},
    )

    assert decision.should_exit is True
    assert decision.reason == "exit by stop loss"
    assert decision.context["rule"] == "stop_loss"
    assert decision.context["base_signal"] == "HOLD"
    assert decision.context["threshold_ratio"] == 0.03


def test_opposite_cross_reason_context_include_expected_fields() -> None:
    rule = OppositeCrossExitRule(
        min_take_profit_ratio=0.002,
        live_fee_rate_estimate=0.0004,
        small_loss_tolerance_ratio=0.001,
    )
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=-0.001)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=99.9,
        signal_context={"base_signal": "SELL"},
    )

    assert decision.reason == "opposite cross deferred: pnl in small_loss noise band"
    assert decision.context["base_signal"] == "SELL"
    assert decision.context["unrealized_pnl_ratio"] == -0.001
    assert decision.context["min_profit_floor"] == 0.002
    assert decision.context["filter_applied"] is True
    assert decision.context["filter_zone"] == "small_loss"
    assert decision.context["profit_floor_basis"]["effective_min_profit_floor_ratio"] == 0.002


def test_opposite_cross_deferred_reason_marks_small_gain_zone() -> None:
    rule = OppositeCrossExitRule(min_take_profit_ratio=0.002, live_fee_rate_estimate=0.0004)
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=0.001)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=100.1,
        signal_context={"base_signal": "SELL"},
    )

    assert decision.should_exit is False
    assert decision.reason == "opposite cross deferred: pnl in small_gain noise band"
    assert decision.context["filter_zone"] == "small_gain"


def test_max_holding_exit_is_not_blocked_by_take_profit_floor_when_opposite_cross_deferred(
    tmp_path,
    relaxed_test_order_rules,
) -> None:
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
            exit_small_loss_tolerance_ratio=0.001,
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
    assert low_fee_decision.context["min_profit_floor"] == 0.002
    assert high_fee_decision.context["min_profit_floor"] == 0.004


def test_small_loss_tolerance_is_decoupled_from_min_profit_floor() -> None:
    rule = OppositeCrossExitRule(
        min_take_profit_ratio=0.004,
        live_fee_rate_estimate=0.0,
        small_loss_tolerance_ratio=0.001,
    )
    position = PositionContext(in_position=True, entry_price=100.0, qty_open=1.0, unrealized_pnl_ratio=-0.002)

    decision = rule.evaluate(
        position=position,
        candle_ts=1_700_000_000_000,
        market_price=99.8,
        signal_context={"base_signal": "SELL"},
    )

    assert decision.should_exit is True
    assert decision.context["filter_applied"] is False
    assert decision.context["small_loss_tolerance_ratio"] == 0.001
    assert decision.context["min_profit_floor"] == 0.004


def test_small_loss_and_small_gain_zones_are_separated_by_sign() -> None:
    rule = OppositeCrossExitRule(
        min_take_profit_ratio=0.002,
        live_fee_rate_estimate=0.0,
        small_loss_tolerance_ratio=0.001,
    )

    small_loss_decision = rule.evaluate(
        position=PositionContext(
            in_position=True,
            entry_price=100.0,
            qty_open=1.0,
            unrealized_pnl_ratio=-0.0005,
        ),
        candle_ts=1_700_000_000_000,
        market_price=99.95,
        signal_context={"base_signal": "SELL"},
    )
    small_gain_decision = rule.evaluate(
        position=PositionContext(
            in_position=True,
            entry_price=100.0,
            qty_open=1.0,
            unrealized_pnl_ratio=0.001,
        ),
        candle_ts=1_700_000_000_000,
        market_price=100.1,
        signal_context={"base_signal": "SELL"},
    )

    assert small_loss_decision.context["small_loss_zone"] is True
    assert small_loss_decision.context["small_gain_zone"] is False
    assert small_loss_decision.context["filter_zone"] == "small_loss"
    assert small_gain_decision.context["small_loss_zone"] is False
    assert small_gain_decision.context["small_gain_zone"] is True
    assert small_gain_decision.context["filter_zone"] == "small_gain"


def test_noise_band_boundary_comparisons_are_applied_as_expected() -> None:
    rule = OppositeCrossExitRule(
        min_take_profit_ratio=0.002,
        live_fee_rate_estimate=0.0,
        small_loss_tolerance_ratio=0.001,
    )

    at_negative_tolerance = rule.evaluate(
        position=PositionContext(
            in_position=True,
            entry_price=100.0,
            qty_open=1.0,
            unrealized_pnl_ratio=-0.001,
        ),
        candle_ts=1_700_000_000_000,
        market_price=99.9,
        signal_context={"base_signal": "SELL"},
    )
    at_zero = rule.evaluate(
        position=PositionContext(
            in_position=True,
            entry_price=100.0,
            qty_open=1.0,
            unrealized_pnl_ratio=0.0,
        ),
        candle_ts=1_700_000_000_000,
        market_price=100.0,
        signal_context={"base_signal": "SELL"},
    )
    at_min_profit_floor = rule.evaluate(
        position=PositionContext(
            in_position=True,
            entry_price=100.0,
            qty_open=1.0,
            unrealized_pnl_ratio=0.002,
        ),
        candle_ts=1_700_000_000_000,
        market_price=100.2,
        signal_context={"base_signal": "SELL"},
    )

    assert at_negative_tolerance.context["small_loss_zone"] is True
    assert at_negative_tolerance.should_exit is False
    assert at_zero.context["small_gain_zone"] is True
    assert at_zero.context["small_loss_zone"] is False
    assert at_zero.should_exit is False
    assert at_min_profit_floor.context["small_gain_zone"] is False
    assert at_min_profit_floor.should_exit is True


def test_common_exit_rule_factory_scope_is_strategy_neutral() -> None:
    rules = create_exit_rules(
        rule_names=["max_holding_time", "stop_loss"],
        max_holding_sec=60.0,
        stop_loss_ratio=0.03,
    )

    assert [rule.name for rule in rules] == ["stop_loss", "max_holding_time"]
    with pytest.raises(ValueError, match="unknown exit rule='opposite_cross'"):
        create_exit_rules(
            rule_names=["opposite_cross"],
            max_holding_sec=60.0,
        )


def test_sma_exit_rule_factory_owns_opposite_cross() -> None:
    rules = create_sma_exit_rules(
        rule_names=["max_holding_time", "opposite_cross", "stop_loss"],
        max_holding_sec=60.0,
        min_take_profit_ratio=0.002,
        live_fee_rate_estimate=0.0004,
        small_loss_tolerance_ratio=0.001,
        stop_loss_ratio=0.03,
    )

    assert [rule.name for rule in rules] == ["stop_loss", "opposite_cross", "max_holding_time"]
    with pytest.raises(ValueError, match="unknown exit rule='take_profit'"):
        create_sma_exit_rules(
            rule_names=["take_profit"],
            max_holding_sec=60.0,
            min_take_profit_ratio=0.002,
            live_fee_rate_estimate=0.0004,
            small_loss_tolerance_ratio=0.001,
        )
