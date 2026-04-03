from __future__ import annotations

import json

from bithumb_bot import runtime_state
from bithumb_bot.config import PATH_MANAGER, settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.app import main as app_main
from bithumb_bot.reporting import cmd_experiment_report, fetch_experiment_report_summary


def _insert_decision_with_context(conn, *, decision_id: int, candle_ts: int, volatility_ratio: float) -> None:
    context = {
        "decision_type": "BUY",
        "candle_ts": candle_ts,
        "volatility_ratio": volatility_ratio,
        "overextended_ratio": 0.03 if volatility_ratio >= 0.008 else 0.005,
    }
    conn.execute(
        """
        INSERT INTO strategy_decisions(
            id, decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
        ) VALUES (?, ?, 'strategy_exp', 'BUY', 'entry', ?, 0, NULL, ?)
        """,
        (decision_id, candle_ts, candle_ts, json.dumps(context, ensure_ascii=False)),
    )


def _insert_lifecycle(conn, *, lifecycle_id: int, entry_decision_id: int, exit_ts: int, net_pnl: float) -> None:
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
            gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, entry_decision_id, exit_decision_id
        ) VALUES (?, 'BTC_KRW', ?, ?, ?, ?, NULL, NULL, ?, ?, 1.0, 100.0, 100.0, ?, 0.0, ?, 60.0, 'strategy_exp', ?, NULL)
        """,
        (
            lifecycle_id,
            lifecycle_id,
            lifecycle_id,
            f"entry-{lifecycle_id}",
            f"exit-{lifecycle_id}",
            exit_ts - 60_000,
            exit_ts,
            net_pnl,
            net_pnl,
            entry_decision_id,
        ),
    )


def test_experiment_report_metrics_and_warnings(tmp_path, monkeypatch):
    db_path = str(tmp_path / "experiment-report.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        _insert_decision_with_context(conn, decision_id=1, candle_ts=1_710_000_000_000, volatility_ratio=0.010)
        _insert_decision_with_context(conn, decision_id=2, candle_ts=1_710_021_600_000, volatility_ratio=0.009)
        _insert_decision_with_context(conn, decision_id=3, candle_ts=1_710_043_200_000, volatility_ratio=0.009)
        _insert_decision_with_context(conn, decision_id=4, candle_ts=1_710_064_800_000, volatility_ratio=0.009)
        _insert_decision_with_context(conn, decision_id=5, candle_ts=1_710_086_400_000, volatility_ratio=0.001)

        _insert_lifecycle(conn, lifecycle_id=1, entry_decision_id=1, exit_ts=1_710_000_060_000, net_pnl=300.0)
        _insert_lifecycle(conn, lifecycle_id=2, entry_decision_id=2, exit_ts=1_710_021_660_000, net_pnl=-50.0)
        _insert_lifecycle(conn, lifecycle_id=3, entry_decision_id=3, exit_ts=1_710_043_260_000, net_pnl=-70.0)
        _insert_lifecycle(conn, lifecycle_id=4, entry_decision_id=4, exit_ts=1_710_064_860_000, net_pnl=-10.0)
        _insert_lifecycle(conn, lifecycle_id=5, entry_decision_id=5, exit_ts=1_710_086_460_000, net_pnl=5.0)
        conn.commit()

        summary = fetch_experiment_report_summary(
            conn,
            sample_threshold=10,
            top_n=1,
            concentration_warn_threshold=0.5,
            regime_skew_warn_threshold=0.7,
        )
    finally:
        conn.close()

    assert summary.realized_net_pnl == 175.0
    assert summary.trade_count == 5
    assert summary.win_rate == 0.4
    assert summary.expectancy_per_trade == 35.0
    assert summary.longest_losing_streak == 3
    assert summary.top_n_concentration == 300.0 / 435.0
    assert summary.sample_insufficient is True
    assert any("insufficient sample" in warning for warning in summary.warnings)
    assert any("concentrated pnl" in warning for warning in summary.warnings)
    assert any("regime skew" in warning for warning in summary.warnings)
    assert any(row.bucket.startswith("vol=high") for row in summary.regime_bucket_rows)
    assert len(summary.time_bucket_rows) >= 1


def test_experiment_report_command_writes_report_and_prints_warning(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "experiment-report-cmd.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={"remote_open_order_found": 0},
            now_epoch_sec=1000.0,
        )
        _insert_decision_with_context(conn, decision_id=10, candle_ts=1_710_000_000_000, volatility_ratio=0.010)
        _insert_lifecycle(conn, lifecycle_id=10, entry_decision_id=10, exit_ts=1_710_000_060_000, net_pnl=100.0)
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
                entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
                gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, entry_decision_id, entry_decision_linkage
            ) VALUES (?, 'BTC_KRW', ?, ?, ?, ?, NULL, NULL, ?, ?, 1.0, 100.0, 101.0, 1.0, 0.0, 1.0, 60.0, 'strategy_exp', ?, ?)
            """,
            (
                11,
                11,
                11,
                "entry-11",
                "exit-11",
                1_000_900,
                1_001_100,
                None,
                "ambiguous_multi_candidate",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_experiment_report(
        strategy_name=None,
        pair=None,
        from_ts_ms=None,
        to_ts_ms=None,
        sample_threshold=5,
        top_n=1,
        concentration_warn_threshold=0.5,
        regime_skew_warn_threshold=0.5,
        as_json=False,
    )
    out = capsys.readouterr().out
    assert "[EXPERIMENT-REPORT]" in out
    assert "[ATTRIBUTION-QUALITY]" in out
    assert "unresolved_attribution_count=1" in out
    assert "[WARNINGS]" in out
    assert "insufficient sample" in out
    assert "concentrated pnl" in out

    report_path = PATH_MANAGER.report_path("experiment_report")
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["attribution_quality"]["total_trade_count"] == 2
    assert payload["attribution_quality"]["unattributed_trade_count"] == 1
    assert payload["recovery_attribution_quality_signals"]["unresolved_attribution_count"] == 1
    assert payload["recovery_attribution_quality_signals"]["ambiguous_linkage_after_recent_reconcile"] is True


def test_experiment_report_cli_subcommand(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "experiment-report-cli.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        _insert_decision_with_context(conn, decision_id=20, candle_ts=1_710_000_000_000, volatility_ratio=0.001)
        _insert_lifecycle(conn, lifecycle_id=20, entry_decision_id=20, exit_ts=1_710_000_060_000, net_pnl=20.0)
        conn.commit()
    finally:
        conn.close()

    rc = app_main(["experiment-report", "--sample-threshold", "2", "--top-n", "1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "[EXPERIMENT-REPORT]" in out
