from __future__ import annotations

import json
import sqlite3

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.reporting import (
    cmd_strategy_report,
    fetch_attribution_quality_summary,
    fetch_filter_effectiveness_summary,
    fetch_lifecycle_close_summary,
    fetch_strategy_performance_stats,
)
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
    assert a.realized_gross_pnl == 78.0
    assert a.realized_net_pnl == 60.0
    assert a.expectancy_per_trade == 30.0
    assert a.fee_total == 18.0
    assert a.holding_time_avg_sec == 90.0
    assert a.exit_reason_linked_count == 0
    assert a.entry_reason_linked_count == 0

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
    assert "[STRATEGY-PERFORMANCE-REPORT (REALIZED PNL BASIS)]" in out
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
    assert stats[0].realized_gross_pnl == pytest.approx(13.0)
    assert stats[0].realized_net_pnl == pytest.approx(12.0)


def test_strategy_report_reason_summary_uses_linked_entry_and_exit_reason(tmp_path, monkeypatch):
    db_path = str(tmp_path / "strategy-report-reasons.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                id, decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            ) VALUES (11, 1000, 'strategy_D', 'BUY', 'entry', 1000, 0, NULL, ?)
            """,
            (json.dumps({"entry_reason": "filtered entry: cost_edge"}, ensure_ascii=False),),
        )
        _insert_lifecycle(
            conn,
            lifecycle_id=20,
            strategy_name="strategy_D",
            pair="BTC_KRW",
            exit_ts=4_200,
            net_pnl=5.0,
            fee_total=1.0,
            holding_time_sec=15.0,
            exit_rule_name="time_stop",
        )
        conn.execute("UPDATE trade_lifecycles SET entry_decision_id=11, exit_reason='max holding reached' WHERE id=20")
        conn.commit()
        stats = fetch_strategy_performance_stats(conn, group_by=("strategy_name",))
    finally:
        conn.close()

    assert len(stats) == 1
    assert stats[0].entry_reason_linked_count == 1
    assert stats[0].exit_reason_linked_count == 1
    assert stats[0].entry_reason_sample == "filtered entry: cost_edge"
    assert stats[0].exit_reason_sample == "max holding reached"


def test_lifecycle_close_summary_aggregates_by_exit_rule_and_entry_exit_combo(tmp_path, monkeypatch):
    db_path = str(tmp_path / "strategy-report-close-summary.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                id, decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            ) VALUES (101, 1000, 'strategy_E', 'BUY', 'entry', 1000, 0, NULL, ?)
            """,
            (json.dumps({"entry": {"rule": "sma_cross_entry"}}, ensure_ascii=False),),
        )
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                id, decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            ) VALUES (102, 2000, 'strategy_E', 'BUY', 'entry', 2000, 0, NULL, ?)
            """,
            (json.dumps({"entry": {"rule": "dip_buy_entry"}}, ensure_ascii=False),),
        )

        _insert_lifecycle(
            conn,
            lifecycle_id=30,
            strategy_name="strategy_E",
            pair="BTC_KRW",
            exit_ts=5_000,
            net_pnl=70.0,
            fee_total=3.0,
            holding_time_sec=100.0,
            exit_rule_name="opposite_cross",
        )
        _insert_lifecycle(
            conn,
            lifecycle_id=31,
            strategy_name="strategy_E",
            pair="BTC_KRW",
            exit_ts=6_000,
            net_pnl=-20.0,
            fee_total=2.0,
            holding_time_sec=140.0,
            exit_rule_name="opposite_cross",
        )
        _insert_lifecycle(
            conn,
            lifecycle_id=32,
            strategy_name="strategy_E",
            pair="BTC_KRW",
            exit_ts=7_000,
            net_pnl=15.0,
            fee_total=1.0,
            holding_time_sec=70.0,
            exit_rule_name="max_holding_time",
        )

        conn.execute(
            """
            UPDATE trade_lifecycles
            SET entry_decision_id=101, exit_reason='cross under triggered'
            WHERE id=30
            """
        )
        conn.execute(
            """
            UPDATE trade_lifecycles
            SET entry_decision_id=102, exit_reason='cross under triggered'
            WHERE id=31
            """
        )
        conn.execute(
            """
            UPDATE trade_lifecycles
            SET entry_decision_id=101, exit_reason='time stop reached'
            WHERE id=32
            """
        )
        conn.commit()

        by_exit_rule, by_entry_exit, notes = fetch_lifecycle_close_summary(conn, min_sample_size=2)
    finally:
        conn.close()

    opposite_cross_rows = [
        row
        for row in by_exit_rule
        if row.exit_rule_name == "opposite_cross" and row.exit_reason_bucket == "cross under triggered"
    ]
    assert len(opposite_cross_rows) == 1
    opposite = opposite_cross_rows[0]
    assert opposite.trade_count == 2
    assert opposite.win_rate == pytest.approx(0.5)
    assert opposite.realized_net_pnl == pytest.approx(50.0)
    assert opposite.avg_hold_time_sec == pytest.approx(120.0)

    combos = {
        (row.entry_rule_name, row.exit_rule_name, row.exit_reason_bucket): row
        for row in by_entry_exit
    }
    assert ("sma_cross_entry", "opposite_cross", "cross under triggered") in combos
    assert ("dip_buy_entry", "opposite_cross", "cross under triggered") in combos
    assert any("max_holding_time/time stop reached" in note for note in notes)


def test_lifecycle_close_summary_handles_legacy_missing_exit_reason_bucket(tmp_path, monkeypatch):
    db_path = str(tmp_path / "strategy-report-legacy-exit-reason.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        _insert_lifecycle(
            conn,
            lifecycle_id=40,
            strategy_name="strategy_F",
            pair="BTC_KRW",
            exit_ts=8_000,
            net_pnl=-5.0,
            fee_total=1.0,
            holding_time_sec=20.0,
            exit_rule_name="time_stop",
        )
        conn.execute("UPDATE trade_lifecycles SET exit_reason=NULL WHERE id=40")
        conn.commit()
        by_exit_rule, _by_entry_exit, notes = fetch_lifecycle_close_summary(conn, min_sample_size=2)
    finally:
        conn.close()

    legacy_rows = [
        row
        for row in by_exit_rule
        if row.exit_rule_name == "time_stop" and row.exit_reason_bucket == "<legacy_missing_exit_reason>"
    ]
    assert len(legacy_rows) == 1
    assert legacy_rows[0].trade_count == 1
    assert any("low-sample exit buckets present" in note for note in notes)


def test_strategy_report_prints_lifecycle_close_summary_block(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "strategy-report-lifecycle-close-print.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        _insert_lifecycle(
            conn,
            lifecycle_id=50,
            strategy_name="strategy_G",
            pair="BTC_KRW",
            exit_ts=9_000,
            net_pnl=10.0,
            fee_total=1.0,
            holding_time_sec=30.0,
            exit_rule_name="opposite_cross",
        )
        conn.execute("UPDATE trade_lifecycles SET exit_reason='cross under triggered' WHERE id=50")
        conn.commit()
    finally:
        conn.close()

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
    assert "[lifecycle_close_summary: by_exit_rule]" in out
    assert "opposite_cross,cross under triggered,1" in out
    assert "[filter_effectiveness]" in out


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


def test_strategy_report_schema_error_for_legacy_db_without_realized_columns(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "legacy-strategy-report.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            """
            CREATE TABLE trade_lifecycles (
                id INTEGER PRIMARY KEY,
                pair TEXT NOT NULL,
                strategy_name TEXT,
                exit_ts INTEGER NOT NULL
            )
            """
        )
        conn.commit()
        with pytest.raises(RuntimeError, match="missing required realized-pnl columns"):
            fetch_strategy_performance_stats(conn, group_by=("strategy_name", "exit_rule_name"))
    finally:
        conn.close()

    ensure_db(db_path).close()
    monkeypatch.setattr(
        "bithumb_bot.reporting.fetch_strategy_performance_stats",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("forced schema mismatch")),
    )
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
    assert "schema_error=forced schema mismatch" in out

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


def test_filter_effectiveness_summary_counts_single_multi_and_hold(tmp_path, monkeypatch):
    db_path = str(tmp_path / "strategy-report-filter-effectiveness.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES
                (60000, 'BTC_KRW', '1m', 100, 100, 100, 100, 1),
                (120000, 'BTC_KRW', '1m', 101, 101, 101, 101, 1),
                (180000, 'BTC_KRW', '1m', 98, 98, 98, 98, 1),
                (240000, 'BTC_KRW', '1m', 105, 105, 105, 105, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                id, decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            ) VALUES
                (1, 60000, 'strategy_H', 'HOLD', 'filtered entry: gap', 60000, 100, NULL, ?),
                (2, 120000, 'strategy_H', 'HOLD', 'filtered entry: gap,volatility', 120000, 101, NULL, ?),
                (3, 180000, 'strategy_H', 'BUY', 'entry', 180000, 98, NULL, ?),
                (4, 240000, 'strategy_H', 'HOLD', 'position held', 240000, 105, NULL, ?)
            """,
            (
                json.dumps(
                    {
                        "decision_type": "BLOCKED_ENTRY",
                        "base_signal": "BUY",
                        "pair": "BTC_KRW",
                        "interval": "1m",
                        "blocked_filters": ["gap"],
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "decision_type": "BLOCKED_ENTRY",
                        "base_signal": "BUY",
                        "pair": "BTC_KRW",
                        "interval": "1m",
                        "blocked_filters": ["gap", "volatility"],
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "decision_type": "BUY",
                        "base_signal": "BUY",
                        "pair": "BTC_KRW",
                        "interval": "1m",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "decision_type": "HOLD",
                        "base_signal": "HOLD",
                        "pair": "BTC_KRW",
                        "interval": "1m",
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        _insert_lifecycle(
            conn,
            lifecycle_id=60,
            strategy_name="strategy_H",
            pair="BTC_KRW",
            exit_ts=1200,
            net_pnl=3.0,
            fee_total=1.0,
            holding_time_sec=10.0,
            exit_rule_name="time_stop",
        )
        conn.execute("UPDATE trade_lifecycles SET entry_decision_id=3 WHERE id=60")
        conn.commit()

        summary = fetch_filter_effectiveness_summary(
            conn,
            strategy_name="strategy_H",
            pair="BTC_KRW",
            observation_window_bars=1,
            min_observation_sample=3,
        )
    finally:
        conn.close()

    assert summary.total_entry_candidates == 3
    assert summary.executed_entry_count == 1
    assert summary.blocked_entry_count == 2
    assert summary.hold_decision_count == 1
    assert summary.blocked_by_filter == {"gap": 2, "volatility": 1}
    assert summary.multi_filter_blocked_count == 1
    assert summary.observation.observed_count == 2
    assert summary.observation.avoided_loss_count == 1
    assert summary.observation.opportunity_missed_count == 1
    assert summary.observation.insufficient_sample is True
    assert summary.observation.return_distribution_bps["min_bps"] == pytest.approx(-297.029702970297)
    assert summary.observation.return_distribution_bps["max_bps"] == pytest.approx(100.0)
    assert summary.observation.blocked_outcome_by_filter["gap"]["blocked_count"] == 2
    assert summary.observation.blocked_outcome_by_filter["gap"]["avoided_loss_ratio"] == pytest.approx(0.5)
    assert summary.observation.blocked_outcome_by_filter["volatility"]["avoided_loss_ratio"] == pytest.approx(1.0)
    assert summary.observation.blocked_outcome_by_signal_strength["unknown"]["blocked_count"] == 2
    assert summary.observation.blocked_outcome_by_market_bucket["unknown"]["blocked_count"] == 2
    assert any("descriptive only" in note for note in summary.notes)
    assert any("explanatory observations" in note for note in summary.notes)


def test_strategy_report_json_includes_filter_effectiveness_and_insufficient_sample_warning(
    tmp_path, monkeypatch, capsys
):
    db_path = str(tmp_path / "strategy-report-filter-effectiveness-json.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES
                (2000, 'BTC_KRW', '1m', 100, 100, 100, 100, 1),
                (2060, 'BTC_KRW', '1m', 99, 99, 99, 99, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                id, decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            ) VALUES
                (21, 2000, 'strategy_I', 'HOLD', 'filtered entry: gap', 2000, 100, NULL, ?)
            """,
            (
                json.dumps(
                    {
                        "decision_type": "BLOCKED_ENTRY",
                        "base_signal": "BUY",
                        "pair": "BTC_KRW",
                        "interval": "1m",
                        "blocked_filters": ["gap"],
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_strategy_report(
        strategy_name="strategy_I",
        exit_rule_name=None,
        pair="BTC_KRW",
        from_ts_ms=None,
        to_ts_ms=None,
        group_by=("strategy_name", "exit_rule_name"),
        observation_window_bars=1,
        min_observation_sample=2,
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)
    section = payload["filter_effectiveness"]

    assert section["entry_candidate_summary"]["blocked_entry_count"] == 1
    assert section["entry_candidate_summary"]["blocked_by_filter"] == {"gap": 1}
    assert section["blocked_observation_window"]["insufficient_sample"] is True
    assert section["blocked_observation_window"]["return_distribution_bps"]["median_bps"] is None
    assert section["blocked_outcome_by_filter"]["gap"]["blocked_count"] == 1
    assert section["blocked_outcome_by_signal_strength"]["unknown"]["blocked_count"] == 1
    assert section["blocked_outcome_by_market_bucket"]["unknown"]["blocked_count"] == 1
    assert any("insufficient sample" in note for note in section["notes"])
    assert any("descriptive only" in note for note in section["notes"])
    attribution = payload["attribution_quality"]
    assert attribution["unattributed_trade_count"] == 0
    assert attribution["ambiguous_linkage_count"] == 0


def test_attribution_quality_summary_counts_unattributed_ambiguous_recovery_and_reason_buckets(tmp_path, monkeypatch):
    db_path = str(tmp_path / "attribution-quality.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
                entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
                gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, entry_decision_id,
                entry_decision_linkage, exit_decision_id, exit_reason, exit_rule_name
            ) VALUES
                (1, 'BTC_KRW', 1, 101, 'entry-1', 'exit-1', 'ef-1', 'xf-1', 1000, 2000, 1, 100, 101, 1, 0, 1, 60, 'strategy_Z', NULL, 'unattributed_no_strict_match', NULL, 'x', 'time_stop'),
                (2, 'BTC_KRW', 2, 102, 'entry-2', 'exit-2', 'ef-2', 'xf-2', 1000, 2000, 1, 100, 99, -1, 0, -1, 60, 'strategy_Z', NULL, 'ambiguous_multi_candidate', NULL, 'x', 'time_stop'),
                (3, 'BTC_KRW', 3, 103, 'entry-3', 'exit-3', 'ef-3', 'xf-3', 1000, 2000, 1, 100, 99, -1, 0, -1, 60, 'strategy_Z', NULL, 'degraded_recovery_unattributed', NULL, 'x', 'time_stop'),
                (4, 'BTC_KRW', 4, 104, 'entry-4', 'exit-4', 'ef-4', 'xf-4', 1000, 2000, 1, 100, 102, 2, 0, 2, 60, 'strategy_Z', NULL, NULL, NULL, 'x', 'time_stop'),
                (5, 'BTC_KRW', 5, 105, 'entry-5', 'exit-5', 'ef-5', 'xf-5', 1000, 2000, 1, 100, 103, 3, 0, 3, 60, 'strategy_Z', 77, 'direct', NULL, 'x', 'time_stop')
            """
        )
        conn.commit()
        summary = fetch_attribution_quality_summary(conn)
    finally:
        conn.close()

    assert summary.total_trade_count == 5
    assert summary.unattributed_trade_count == 4
    assert summary.ambiguous_linkage_count == 1
    assert summary.recovery_derived_attribution_count == 1
    assert summary.reason_buckets["missing_decision_id"] == 1
    assert summary.reason_buckets["multiple_candidate_decisions"] == 1
    assert summary.reason_buckets["legacy_incomplete_row"] == 1
    assert summary.reason_buckets["recovery_unresolved_linkage"] == 1
    assert any("unattributed trades present" in warning for warning in summary.warnings)
