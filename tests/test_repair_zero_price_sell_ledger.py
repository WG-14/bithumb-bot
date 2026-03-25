from __future__ import annotations

import sqlite3

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, init_portfolio
from bithumb_bot.repair_zero_price_sell_ledger import run_repair


def _seed_contaminated_db(db_path: str) -> None:
    conn = ensure_db(db_path)
    try:
        init_portfolio(conn)

        orders = [
            ("buy_1", "B1", "FILLED", "BUY", 102247000.0, 9.777e-05, 9.777e-05, 1),
            ("recovery_C0101000002856175729", "C0101000002856175729", "FILLED", "SELL", 102247000.0, 9.777e-05, 9.777e-05, 2),
            ("buy_2", "B2", "FILLED", "BUY", 103032000.0, 9.709e-05, 9.709e-05, 3),
            ("live_1774265640000_sell_attempt_790346d1402e44d9", "C0101000002857593528", "FILLED", "SELL", 103032000.0, 9.709e-05, 9.709e-05, 4),
            ("buy_3", "B3", "FILLED", "BUY", 106143000.0, 9.433e-05, 9.433e-05, 5),
            ("recovery_C0101000002861024463", "C0101000002861024463", "FILLED", "SELL", 106143000.0, 9.433e-05, 9.433e-05, 6),
        ]
        for client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, ts in orders:
            conn.execute(
                """
                INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, ts, ts),
            )

        fills = [
            (13, "buy_1", "B1:aggregate:1", 1, 102247000.0, 9.777e-05, 0.0),
            (14, "recovery_C0101000002856175729", "C0101000002856175729:aggregate:1774230830000", 2, 0.0, 9.777e-05, 0.0),
            (32, "buy_2", "B2:aggregate:3", 3, 103032000.0, 9.709e-05, 0.0),
            (33, "live_1774265640000_sell_attempt_790346d1402e44d9", "C0101000002857593528:aggregate:1774265763000", 4, 0.0, 9.709e-05, 0.0),
            (98, "buy_3", "B3:aggregate:5", 5, 106143000.0, 9.433e-05, 0.0),
            (99, "recovery_C0101000002861024463", "C0101000002861024463:aggregate:1774352767000", 6, 0.0, 9.433e-05, 0.0),
        ]
        for row in fills:
            conn.execute(
                "INSERT INTO fills(id, client_order_id, fill_id, fill_ts, price, qty, fee) VALUES (?, ?, ?, ?, ?, ?, ?)",
                row,
            )

        trades = [
            (13, 1, "BUY", 102247000.0, 9.777e-05, 0.0, 990003.31081, 9.777e-05, "buy1"),
            (14, 2, "SELL", 102247000.0, 9.777e-05, 0.0, 990003.31081, 0.0, "reconcile recent exchange_order_id=C0101000002856175729"),
            (32, 3, "BUY", 103032000.0, 9.709e-05, 0.0, 980001.86318, 9.709e-05, "buy2"),
            (33, 4, "SELL", 103032000.0, 9.709e-05, 0.0, 979976.86318, 0.0, "reconcile recent exchange_order_id=C0101000002857593528"),
            (98, 5, "BUY", 106143000.0, 9.433e-05, 0.0, 969965.49308, 9.433e-05, "buy3"),
            (99, 6, "SELL", 106143000.0, 9.433e-05, 0.0, 969940.46308, 0.0, "reconcile recent exchange_order_id=C0101000002861024463"),
        ]
        for tid, ts, side, price, qty, fee, cash_after, asset_after, note in trades:
            conn.execute(
                """
                INSERT INTO trades(id, ts, pair, interval, side, price, qty, fee, cash_after, asset_after, note)
                VALUES (?, ?, 'BTC_KRW', '1m', ?, ?, ?, ?, ?, ?, ?)
                """,
                (tid, ts, side, price, qty, fee, cash_after, asset_after, note),
            )

        conn.execute(
            """
            UPDATE portfolio
            SET cash_krw=?, asset_qty=0.0, cash_available=?, cash_locked=0.0, asset_available=0.0, asset_locked=0.0
            WHERE id=1
            """,
            (969940.46308, 969940.46308),
        )
        conn.commit()
    finally:
        conn.close()


def test_repair_zero_price_sell_ledger(tmp_path) -> None:
    db_path = str(tmp_path / "repair.sqlite")
    _seed_contaminated_db(db_path)

    # dry-run must not modify
    assert run_repair(db_path=db_path, apply=False) == 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        assert float(conn.execute("SELECT price FROM fills WHERE id=14").fetchone()["price"]) == 0.0
    finally:
        conn.close()

    assert run_repair(db_path=db_path, apply=True, allow_no_backup=True) == 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        prices = [float(r["price"]) for r in conn.execute("SELECT price FROM fills WHERE id IN (14,33,99) ORDER BY id").fetchall()]
        assert prices == [102247000.0, 103032000.0, 106143000.0]

        cash = float(settings.START_CASH_KRW)
        qty = 0.0
        rows = conn.execute(
            """
            SELECT o.side, f.price, f.qty, f.fee
            FROM fills f
            JOIN orders o ON o.client_order_id=f.client_order_id
            ORDER BY f.fill_ts ASC, f.id ASC
            """
        ).fetchall()
        for row in rows:
            if row["side"] == "BUY":
                cash -= (float(row["price"]) * float(row["qty"])) + float(row["fee"])
                qty += float(row["qty"])
            else:
                cash += (float(row["price"]) * float(row["qty"])) - float(row["fee"])
                qty -= float(row["qty"])

        portfolio = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
        assert abs(float(portfolio["cash_krw"]) - cash) < 1e-8
        assert abs(float(portfolio["asset_qty"]) - qty) < 1e-12

        latest_trade = conn.execute("SELECT cash_after, asset_after FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        assert abs(float(latest_trade["cash_after"]) - float(portfolio["cash_krw"])) < 1e-8
        assert abs(float(latest_trade["asset_after"]) - float(portfolio["asset_qty"])) < 1e-12
    finally:
        conn.close()
