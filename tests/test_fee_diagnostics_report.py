from __future__ import annotations

import json

from bithumb_bot.app import main as app_main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.reporting import cmd_fee_diagnostics, fetch_fee_diagnostics


def test_fee_diagnostics_metrics_are_computed_correctly(tmp_path, monkeypatch):
    db_path = str(tmp_path / "fee-diagnostics.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-1", "ex-1", "FILLED", "BUY", 100_000_000.0, 0.001, 0.001, 1, 1),
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-2", "ex-2", "FILLED", "SELL", 110_000_000.0, 0.001, 0.001, 2, 2),
        )
        conn.executemany(
            """
            INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("coid-1", "fill-1", 10, 100_000_000.0, 0.001, 40.0),  # 4.0 bps
                ("coid-2", "fill-2", 20, 110_000_000.0, 0.001, 0.0),   # 0 bps
            ],
        )
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
                entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
                gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, entry_decision_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "BTC_KRW",
                1,
                2,
                "coid-1",
                "coid-2",
                "fill-1",
                "fill-2",
                10,
                20,
                0.001,
                100_000_000.0,
                110_000_000.0,
                950.0,
                40.0,
                910.0,
                60.0,
                "strategy-test",
                None,
            ),
        )
        conn.commit()

        summary = fetch_fee_diagnostics(conn, fill_limit=10, roundtrip_limit=10, estimated_fee_rate=0.0005)
    finally:
        conn.close()

    assert summary.fill_count == 2
    assert summary.fee_zero_count == 1
    assert summary.fee_zero_ratio == 0.5
    assert summary.average_fee_rate == 40.0 / (100_000.0 + 110_000.0)
    assert summary.average_fee_bps == 2.0
    assert summary.median_fee_bps == 2.0
    assert summary.estimated_minus_actual_bps == (0.0005 - summary.average_fee_rate) * 10000.0
    assert summary.roundtrip_fee_total == 40.0
    assert summary.pnl_before_fee_total == 950.0
    assert summary.pnl_after_fee_total == 910.0
    assert summary.pnl_fee_drag_total == 40.0


def test_fee_diagnostics_handles_empty_data(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "fee-diagnostics-empty.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    conn.close()

    cmd_fee_diagnostics(fill_limit=5, roundtrip_limit=5, estimated_fee_rate=0.0004, as_json=False)
    out = capsys.readouterr().out
    assert "[FEE-DIAGNOSTICS]" in out
    assert "avg_fee_rate=-" in out
    assert "no fills found in the selected window" in out


def test_fee_diagnostics_cli_json_smoke(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "fee-diagnostics-cli.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    conn.close()

    app_main(["fee-diagnostics", "--fill-limit", "3", "--roundtrip-limit", "2", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["fill_window"]["limit"] == 3
    assert payload["roundtrip_window"]["limit"] == 2
    assert "fills" in payload
    assert "roundtrip" in payload
