from __future__ import annotations

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.strategy_performance import (
    fetch_strategy_performance_summary,
    evaluate_strategy_performance_gate,
)


def _insert_lifecycle(conn, *, idx: int, net_pnl: float, fee_total: float = 10.0, exit_rule_name: str = "opposite_cross") -> None:
    gross_pnl = float(net_pnl) + float(fee_total)
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
            gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, exit_rule_name
        ) VALUES (?, 'KRW-BTC', ?, ?, ?, ?, NULL, NULL, ?, ?, 0.0004, 100.0, 100.0, ?, ?, ?, 60.0, 'sma_with_filter', ?)
        """,
        (
            idx,
            idx,
            idx,
            f"entry-{idx}",
            f"exit-{idx}",
            1_710_000_000_000 + idx,
            1_710_000_060_000 + idx,
            gross_pnl,
            fee_total,
            net_pnl,
            exit_rule_name,
        ),
    )


def test_strategy_performance_summary_exposes_fee_adjusted_breakdown(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "performance-summary.sqlite"))
    try:
        _insert_lifecycle(conn, idx=1, net_pnl=-50.0, fee_total=45.0)
        _insert_lifecycle(conn, idx=2, net_pnl=20.0, fee_total=45.0, exit_rule_name="max_holding_time")
        conn.commit()

        summary = fetch_strategy_performance_summary(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        conn.close()

    assert summary.sample_count == 2
    assert summary.net_pnl == -30.0
    assert summary.fee_total == 90.0
    assert summary.expectancy_per_trade == -15.0
    assert summary.by_exit_rule_name["opposite_cross"]["net_pnl"] == -50.0


def test_strategy_performance_gate_blocks_negative_expectancy(tmp_path) -> None:
    old_min_sample = settings.LIVE_PERFORMANCE_GATE_MIN_SAMPLE
    old_enabled = settings.LIVE_PERFORMANCE_GATE_ENABLED
    conn = ensure_db(str(tmp_path / "performance-gate.sqlite"))
    try:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", True)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", 2)
        _insert_lifecycle(conn, idx=1, net_pnl=-50.0)
        _insert_lifecycle(conn, idx=2, net_pnl=10.0)
        conn.commit()

        gate = evaluate_strategy_performance_gate(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", old_min_sample)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", old_enabled)
        conn.close()

    assert gate.enabled is True
    assert gate.allowed is False
    assert "STRATEGY_EXPECTANCY_NEGATIVE" in gate.reason_code


def test_strategy_performance_gate_fails_closed_on_insufficient_sample(tmp_path) -> None:
    old_min_sample = settings.LIVE_PERFORMANCE_GATE_MIN_SAMPLE
    old_enabled = settings.LIVE_PERFORMANCE_GATE_ENABLED
    conn = ensure_db(str(tmp_path / "performance-gate-insufficient.sqlite"))
    try:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", True)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", 3)
        _insert_lifecycle(conn, idx=1, net_pnl=100.0)
        conn.commit()

        gate = evaluate_strategy_performance_gate(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", old_min_sample)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", old_enabled)
        conn.close()

    assert gate.allowed is False
    assert "STRATEGY_SAMPLE_INSUFFICIENT" in gate.reason_code
