from __future__ import annotations

import json
import sqlite3

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.reporting import cmd_strategy_report, fetch_strategy_performance_stats
from bithumb_bot.app import main as app_main

def _insert_sell_decision(conn, *, decision_ts: int, strategy_name: str, rule: str) -> None:
    conn.execute(
        """
        INSERT INTO strategy_decisions(
            decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
        ) VALUES (?, ?, 'SELL', 'exit', ?, 0, NULL, ?)
        """,
        (decision_ts, strategy_name, decision_ts, json.dumps({"exit": {"rule": rule}}, ensure_ascii=False)),
    )


def _insert_lifecycle(
    conn,
    *,
    lifecycle_id: int,
    strategy_name: str,
    pair: str,
    exit_ts: int,
    net_pnl: float,
    fee_total: float,
    holding_time_sec: float,
    exit_rule_name: str | None = None,
) -> None:
    gross_pnl = float(net_pnl) + float(fee_total)
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
            gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, entry_decision_id, exit_rule_name
        ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
        """,
        (
            lifecycle_id,
            pair,
            lifecycle_id,
            lifecycle_id,
            f"entry-{lifecycle_id}",
            f"exit-{lifecycle_id}",
            max(1, exit_ts - int(holding_time_sec * 1000)),
            exit_ts,
            1.0,
            100.0,
            100.0,
            gross_pnl,
            fee_total,
            net_pnl,
            holding_time_sec,
            strategy_name,
            exit_rule_name,
        ),
    )


def test_strategy_report_aggregates_strategy_and_exit_rule(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "strategy-report.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        _insert_sell_decision(conn, decision_ts=2_000, strategy_name="strategy_A", rule="opposite_cross")
        _insert_sell_decision(conn, decision_ts=3_000, strategy_name="strategy_B", rule="max_holding_time")

        _insert_lifecycle(
            conn,
            lifecycle_id=1,
            strategy_name="strategy_A",
            pair="BTC_KRW",
            exit_ts=2_100,
            net_pnl=100.0,
            fee_total=10.0,
            holding_time_sec=60.0,
            exit_rule_name="opposite_cross",
        )
        _insert_lifecycle(
            conn,
            lifecycle_id=2,
            strategy_name="strategy_A",
            pair="BTC_KRW",
            exit_ts=2_200,
            net_pnl=-40.0,
            fee_total=8.0,
            holding_time_sec=120.0,
            exit_rule_name="opposite_cross",
        )
        _insert_lifecycle(
            conn,
            lifecycle_id=3,
            strategy_name="strategy_B",
            pair="BTC_KRW",
            exit_ts=3_100,
            net_pnl=30.0,
            fee_total=5.0,
            holding_time_sec=30.0,
            exit_rule_name="max_holding_time",
        )
        conn.commit()

        stats = fetch_strategy_performance_stats(
            conn,
            group_by=("strategy_name", "exit_rule_name"),
        )
    finally:
        conn.close()

    assert len(stats) == 2
    by_strategy = {row.strategy_name: row for row in stats}

    a = by_strategy["strategy_A"]
    assert a.exit_rule_name == "opposite_cross"
    assert a.trade_count == 2
    assert a.win_rate == 0.5
    assert a.avg_gain == 100.0
    assert a.avg_loss == -40.0
    assert a.net_pnl == 60.0
    assert a.expectancy_per_trade == 30.0
    assert a.fee_total == 18.0
    assert a.holding_time_avg_sec == 90.0

    b = by_strategy["strategy_B"]
    assert b.exit_rule_name == "max_holding_time"
    assert b.trade_count == 1
    assert b.win_rate == 1.0
    assert b.expectancy_per_trade == 30.0

    cmd_strategy_report(
        strategy_name=None,
        exit_rule_name=None,
        pair=None,
        from_ts_ms=None,
        to_ts_ms=None,
        group_by=("strategy_name", "exit_rule_name"),
        as_json=False,
    )
    out = capsys.readouterr().out
    assert "[STRATEGY-PERFORMANCE-REPORT]" in out
    assert "strategy_A,opposite_cross" in out
    assert "strategy_B,max_holding_time" in out


def test_strategy_report_uses_lifecycle_exit_rule_without_decision_lookup(tmp_path, monkeypatch):
    db_path = str(tmp_path / "strategy-report-direct-exit-rule.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        _insert_lifecycle(
            conn,
            lifecycle_id=10,
            strategy_name="strategy_C",
            pair="BTC_KRW",
            exit_ts=4_100,
            net_pnl=12.0,
            fee_total=1.0,
            holding_time_sec=45.0,
            exit_rule_name="time_stop",
        )
        conn.commit()
        stats = fetch_strategy_performance_stats(conn, group_by=("strategy_name", "exit_rule_name"))
    finally:
        conn.close()

    assert len(stats) == 1
    assert stats[0].strategy_name == "strategy_C"
    assert stats[0].exit_rule_name == "time_stop"


def test_strategy_report_handles_empty_data_gracefully(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "strategy-report-empty.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    conn.close()

    cmd_strategy_report(
        strategy_name="does_not_exist",
        exit_rule_name=None,
        pair="BTC_KRW",
        from_ts_ms=1,
        to_ts_ms=2,
        group_by=("strategy_name", "exit_rule_name"),
        as_json=False,
    )
    out = capsys.readouterr().out
    assert "no matched trade_lifecycles rows" in out

def test_strategy_report_rejects_invalid_date_format(capsys):
    with pytest.raises(SystemExit) as excinfo:
        app_main(["strategy-report", "--from-date", "2026/03/01"])

    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "expected YYYY-MM-DD" in err


def test_strategy_report_rejects_reversed_date_range(capsys):
    with pytest.raises(SystemExit) as excinfo:
        app_main(["strategy-report", "--from-date", "2026-03-27", "--to-date", "2026-03-01"])

    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "--from-date must be earlier than or equal to --to-date" in err
