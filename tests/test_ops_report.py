from __future__ import annotations

import os

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, init_portfolio
from bithumb_bot.reporting import cmd_ops_report


def test_ops_report_with_strategy_and_trade_data(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "ops-report.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        init_portfolio(conn)
        conn.execute(
            """
            INSERT INTO order_intent_dedup(
                intent_key, symbol, side, strategy_context, intent_type, intent_ts, qty,
                client_order_id, order_status, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                "intent-1",
                "BTC_KRW",
                "BUY",
                "paper:sma_cross:1m",
                "ENTRY",
                1,
                0.001,
                "coid-1",
                "FILLED",
                1,
                1,
            ),
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-1", "ex-1", "FILLED", "BUY", 100000000.0, 0.001, 0.001, 1, 2),
        )
        conn.execute(
            """
            INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("coid-1", "fill-1", 3, 100000000.0, 0.001, 50.0),
        )
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, side, qty, price, submission_reason_code, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("coid-1", "submit_attempt_recorded", 4, "FILLED", "BUY", 0.001, 100000000.0, "SIGNAL_BUY", "submit ok"),
        )
        conn.execute(
            """
            INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (5, "BTC_KRW", "1m", "BUY", 100000000.0, 0.001, 50.0, 900000.0, 0.001, "paper fill"),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_ops_report(limit=5)
    out = capsys.readouterr().out

    assert "[OPS-REPORT]" in out
    assert "market=KRW-BTC" in out
    assert f"db_path={db_path}" in out
    assert "paper:sma_cross:1m,1,1,100000.00,0.00,50.00,-100050.00" in out
    assert "event=submit_attempt_recorded" in out
    assert "note=paper fill" in out


def test_ops_report_uses_env_db_path_without_hardcoded_path(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "env-db.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    conn.close()

    assert os.path.exists(db_path)
    cmd_ops_report(limit=1)
    out = capsys.readouterr().out
    assert "market=KRW-BTC" in out
    assert f"db_path={db_path}" in out
    assert "no strategy_context rows" in out
