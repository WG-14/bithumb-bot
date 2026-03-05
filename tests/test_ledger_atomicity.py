import sqlite3

import bithumb_bot.broker.paper as paper
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, init_portfolio, get_portfolio


def test_paper_execute_is_atomic_on_failure(tmp_path, monkeypatch):
    db_path = tmp_path / "atomicity.sqlite"

    monkeypatch.setattr(paper, "ensure_db", lambda: ensure_db(str(db_path)))

    original_set_status = paper.set_status

    def failing_set_status(client_order_id: str, status: str, conn: sqlite3.Connection | None = None, **kwargs):
        original_set_status(client_order_id, status, conn=conn, **kwargs)
        raise RuntimeError("force rollback")

    monkeypatch.setattr(paper, "set_status", failing_set_status)

    conn = ensure_db(str(db_path))
    init_portfolio(conn)
    before_cash, before_qty = get_portfolio(conn)
    conn.close()

    try:
        paper.paper_execute("BUY", ts=1_700_000_000_000, price=100_000_000.0)
    except RuntimeError:
        pass

    conn = ensure_db(str(db_path))
    after_cash, after_qty = get_portfolio(conn)
    orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    fills = conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
    trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    conn.close()

    assert after_cash == before_cash == float(settings.START_CASH_KRW)
    assert after_qty == before_qty == 0.0
    assert orders == 0
    assert fills == 0
    assert trades == 0
