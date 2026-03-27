from __future__ import annotations

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing


def _record_order(conn, *, client_order_id: str, side: str, qty_req: float, ts_ms: int) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side=side,
        qty_req=qty_req,
        submit_attempt_id=f"attempt_{client_order_id}",
        price=None,
        ts_ms=ts_ms,
        status="NEW",
    )


def test_trade_lifecycle_tracks_realized_pnl_fee_and_holding_time(tmp_path):
    conn = ensure_db(str(tmp_path / "lifecycle.sqlite"))
    base_ts = 1_700_000_000_000

    _record_order(conn, client_order_id="entry_1", side="BUY", qty_req=2.0, ts_ms=base_ts)
    buy_trade = apply_fill_and_trade(
        conn,
        client_order_id="entry_1",
        side="BUY",
        fill_id="fill_entry_1",
        fill_ts=base_ts,
        price=100.0,
        qty=2.0,
        fee=2.0,
        note="entry",
    )
    assert buy_trade is not None

    _record_order(conn, client_order_id="exit_1", side="SELL", qty_req=1.0, ts_ms=base_ts + 60_000)
    sell_trade_1 = apply_fill_and_trade(
        conn,
        client_order_id="exit_1",
        side="SELL",
        fill_id="fill_exit_1",
        fill_ts=base_ts + 60_000,
        price=110.0,
        qty=1.0,
        fee=1.0,
        note="exit_partial",
    )
    assert sell_trade_1 is not None

    _record_order(conn, client_order_id="exit_2", side="SELL", qty_req=1.0, ts_ms=base_ts + 120_000)
    sell_trade_2 = apply_fill_and_trade(
        conn,
        client_order_id="exit_2",
        side="SELL",
        fill_id="fill_exit_2",
        fill_ts=base_ts + 120_000,
        price=120.0,
        qty=1.0,
        fee=1.0,
        note="exit_final",
    )
    assert sell_trade_2 is not None

    rows = conn.execute(
        """
        SELECT
            entry_client_order_id,
            exit_client_order_id,
            matched_qty,
            gross_pnl,
            fee_total,
            net_pnl,
            holding_time_sec,
            entry_fill_id,
            exit_fill_id
        FROM trade_lifecycles
        ORDER BY id ASC
        """
    ).fetchall()
    open_lots = conn.execute("SELECT COUNT(*) FROM open_position_lots").fetchone()[0]
    conn.close()

    assert len(rows) == 2

    assert rows[0]["entry_client_order_id"] == "entry_1"
    assert rows[0]["exit_client_order_id"] == "exit_1"
    assert float(rows[0]["matched_qty"]) == pytest.approx(1.0)
    assert float(rows[0]["gross_pnl"]) == pytest.approx(10.0)
    assert float(rows[0]["fee_total"]) == pytest.approx(2.0)
    assert float(rows[0]["net_pnl"]) == pytest.approx(8.0)
    assert float(rows[0]["holding_time_sec"]) == pytest.approx(60.0)
    assert rows[0]["entry_fill_id"] == "fill_entry_1"
    assert rows[0]["exit_fill_id"] == "fill_exit_1"

    assert rows[1]["entry_client_order_id"] == "entry_1"
    assert rows[1]["exit_client_order_id"] == "exit_2"
    assert float(rows[1]["matched_qty"]) == pytest.approx(1.0)
    assert float(rows[1]["gross_pnl"]) == pytest.approx(20.0)
    assert float(rows[1]["fee_total"]) == pytest.approx(2.0)
    assert float(rows[1]["net_pnl"]) == pytest.approx(18.0)
    assert float(rows[1]["holding_time_sec"]) == pytest.approx(120.0)
    assert rows[1]["entry_fill_id"] == "fill_entry_1"
    assert rows[1]["exit_fill_id"] == "fill_exit_2"

    assert open_lots == 0


def test_schema_bootstrap_creates_lifecycle_tables(tmp_path):
    conn = ensure_db(str(tmp_path / "schema.sqlite"))
    lot_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(open_position_lots)").fetchall()}
    lifecycle_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(trade_lifecycles)").fetchall()}
    conn.close()

    assert "entry_client_order_id" in lot_cols
    assert "entry_fee_total" in lot_cols
    assert "gross_pnl" in lifecycle_cols
    assert "fee_total" in lifecycle_cols
    assert "net_pnl" in lifecycle_cols
    assert "holding_time_sec" in lifecycle_cols
    assert "entry_client_order_id" in lifecycle_cols
    assert "exit_client_order_id" in lifecycle_cols


def test_sell_without_known_entry_writes_unknown_lifecycle_row(tmp_path):
    conn = ensure_db(str(tmp_path / "unknown_entry.sqlite"))
    conn.execute(
        """
        INSERT OR REPLACE INTO portfolio(
            id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
        ) VALUES (1, 1000.0, 1.0, 1000.0, 0.0, 1.0, 0.0)
        """
    )
    _record_order(conn, client_order_id="orphan_sell", side="SELL", qty_req=0.5, ts_ms=1700000000000)
    apply_fill_and_trade(
        conn,
        client_order_id="orphan_sell",
        side="SELL",
        fill_id="fill_orphan_sell",
        fill_ts=1700000001000,
        price=100.0,
        qty=0.5,
        fee=0.1,
        note="reconcile legacy",
    )
    row = conn.execute(
        """
        SELECT entry_trade_id, entry_client_order_id, matched_qty, gross_pnl, fee_total, net_pnl
        FROM trade_lifecycles
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert int(row["entry_trade_id"]) == 0
    assert row["entry_client_order_id"] == "__unknown_entry__"
    assert float(row["matched_qty"]) == pytest.approx(0.5)
    assert float(row["gross_pnl"]) == pytest.approx(0.0)
    assert float(row["fee_total"]) == pytest.approx(0.1)
    assert float(row["net_pnl"]) == pytest.approx(-0.1)
