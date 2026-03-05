from __future__ import annotations

from pathlib import Path

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, set_portfolio
from bithumb_bot.broker import paper


def _set(attr: str, value):
    old = getattr(settings, attr)
    object.__setattr__(settings, attr, value)
    return old


def test_paper_execute_uses_orderbook_price_for_buy(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper.sqlite"))
    old_slip = _set("SLIPPAGE_BPS", 10.0)
    try:
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        monkeypatch.setattr(paper, "fetch_orderbook_top", lambda _: (104.0, 105.0))
        trade = paper.paper_execute("BUY", ts=1, price=999.0)

        assert trade is not None
        expected_fill = 105.0 * (1 + 10.0 / 10000.0)
        assert trade["price"] == expected_fill

        conn = ensure_db()
        t = conn.execute("SELECT price, note FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        o = conn.execute("SELECT price FROM orders ORDER BY id DESC LIMIT 1").fetchone()
        f = conn.execute("SELECT price FROM fills ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()

        assert t["price"] == expected_fill
        assert o["price"] == expected_fill
        assert f["price"] == expected_fill
        assert "signal_price=999.0" in t["note"]
    finally:
        _set("DB_PATH", old_db)
        _set("SLIPPAGE_BPS", old_slip)


def test_paper_execute_blocks_on_abnormal_spread(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper2.sqlite"))
    old_spread = _set("MAX_ORDERBOOK_SPREAD_BPS", 10.0)
    try:
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        monkeypatch.setattr(paper, "fetch_orderbook_top", lambda _: (100.0, 150.0))
        trade = paper.paper_execute("BUY", ts=1, price=101.0)
        assert trade is None

        conn = ensure_db()
        n = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        conn.close()
        assert n == 0
    finally:
        _set("DB_PATH", old_db)
        _set("MAX_ORDERBOOK_SPREAD_BPS", old_spread)
