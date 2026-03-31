from __future__ import annotations

from pathlib import Path

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, get_portfolio, set_portfolio
from bithumb_bot.broker import paper
from bithumb_bot.public_api_orderbook import BestQuote


def _set(attr: str, value):
    old = getattr(settings, attr)
    object.__setattr__(settings, attr, value)
    return old


def test_paper_execute_uses_orderbook_price_for_buy(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper.sqlite"))
    old_slip = _set("SLIPPAGE_BPS", 10.0)
    old_max_order = _set("MAX_ORDER_KRW", 0.0)
    old_paper_fee = _set("PAPER_FEE_RATE", 0.0025)
    try:
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        monkeypatch.setattr(
            paper,
            "fetch_orderbook_top",
            lambda _pair: BestQuote(market="KRW-BTC", bid_price=104.0, ask_price=105.0),
        )
        trade = paper.paper_execute("BUY", ts=1, price=999.0)

        assert trade is not None
        expected_fill = 105.0 * (1 + 10.0 / 10000.0)
        expected_fee = 1_000_000 * float(settings.BUY_FRACTION) * 0.0025
        assert trade["price"] == expected_fill
        assert trade["fee"] == expected_fee

        conn = ensure_db()
        t = conn.execute("SELECT price, fee, note FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        o = conn.execute("SELECT price FROM orders ORDER BY id DESC LIMIT 1").fetchone()
        f = conn.execute("SELECT price, fee FROM fills ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()

        assert t["price"] == expected_fill
        assert t["fee"] == expected_fee
        assert o["price"] == expected_fill
        assert f["price"] == expected_fill
        assert f["fee"] == expected_fee
        assert "signal_price=999.0" in t["note"]
    finally:
        _set("DB_PATH", old_db)
        _set("SLIPPAGE_BPS", old_slip)
        _set("MAX_ORDER_KRW", old_max_order)
        _set("PAPER_FEE_RATE", old_paper_fee)


def test_paper_execute_pnl_changes_when_paper_fee_rate_changes(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper_fee_compare.sqlite"))
    old_slip = _set("SLIPPAGE_BPS", 0.0)
    old_max_order = _set("MAX_ORDER_KRW", 0.0)
    old_buy_fraction = _set("BUY_FRACTION", 1.0)
    old_paper_fee = _set("PAPER_FEE_RATE", 0.0)
    try:
        monkeypatch.setattr(
            paper,
            "fetch_orderbook_top",
            lambda _pair: BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=100.0),
        )

        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        object.__setattr__(settings, "PAPER_FEE_RATE", 0.0)
        buy_trade_no_fee = paper.paper_execute("BUY", ts=1, price=100.0)
        sell_trade_no_fee = paper.paper_execute("SELL", ts=2, price=100.0)
        assert buy_trade_no_fee is not None
        assert sell_trade_no_fee is not None
        conn = ensure_db()
        cash_no_fee, _ = get_portfolio(conn)
        conn.close()

        object.__setattr__(settings, "DB_PATH", str(tmp_path / "paper_fee_compare_2.sqlite"))
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        object.__setattr__(settings, "PAPER_FEE_RATE", 0.005)
        buy_trade_fee = paper.paper_execute("BUY", ts=3, price=100.0)
        sell_trade_fee = paper.paper_execute("SELL", ts=4, price=100.0)
        assert buy_trade_fee is not None
        assert sell_trade_fee is not None
        conn = ensure_db()
        cash_with_fee, _ = get_portfolio(conn)
        conn.close()

        assert cash_with_fee < cash_no_fee
    finally:
        _set("DB_PATH", old_db)
        _set("SLIPPAGE_BPS", old_slip)
        _set("MAX_ORDER_KRW", old_max_order)
        _set("BUY_FRACTION", old_buy_fraction)
        _set("PAPER_FEE_RATE", old_paper_fee)


def test_paper_execute_blocks_on_abnormal_spread(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper2.sqlite"))
    old_spread = _set("MAX_ORDERBOOK_SPREAD_BPS", 10.0)
    try:
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        monkeypatch.setattr(
            paper,
            "fetch_orderbook_top",
            lambda _pair: BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=150.0),
        )
        trade = paper.paper_execute("BUY", ts=1, price=101.0)
        assert trade is None

        conn = ensure_db()
        n = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        conn.close()
        assert n == 0
    finally:
        _set("DB_PATH", old_db)
        _set("MAX_ORDERBOOK_SPREAD_BPS", old_spread)


def test_paper_execute_blocks_on_invalid_best_quote(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper-invalid-quote.sqlite"))
    try:
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        monkeypatch.setattr(
            paper,
            "fetch_orderbook_top",
            lambda _pair: BestQuote(market="KRW-BTC", bid_price=101.0, ask_price=100.0),
        )
        trade = paper.paper_execute("BUY", ts=1, price=101.0)
        assert trade is None
    finally:
        _set("DB_PATH", old_db)
