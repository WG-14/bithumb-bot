from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, get_portfolio, set_portfolio
from bithumb_bot.broker import paper
from bithumb_bot import runtime_state
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


def test_paper_execute_canonicalizes_legacy_pair_once_for_orderbook_and_ledger(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper_legacy_pair.sqlite"))
    old_pair = _set("PAIR", "BTC_KRW")
    old_slip = _set("SLIPPAGE_BPS", 0.0)
    old_max_order = _set("MAX_ORDER_KRW", 0.0)
    old_paper_fee = _set("PAPER_FEE_RATE", 0.0)
    try:
        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.0)
        conn.close()

        seen_markets: list[str] = []
        canonical_calls = {"count": 0}

        def _fake_fetch_orderbook_top(market: str):
            seen_markets.append(market)
            return BestQuote(market="KRW-BTC", bid_price=104.0, ask_price=105.0)

        real_canonical_market_with_raw = paper.canonical_market_with_raw

        def _spy_canonical_market_with_raw(market: str):
            canonical_calls["count"] += 1
            return real_canonical_market_with_raw(market)

        monkeypatch.setattr(paper, "fetch_orderbook_top", _fake_fetch_orderbook_top)
        monkeypatch.setattr(paper, "canonical_market_with_raw", _spy_canonical_market_with_raw)
        trade = paper.paper_execute("BUY", ts=1, price=999.0)

        assert trade is not None
        assert canonical_calls["count"] == 1
        assert seen_markets == ["KRW-BTC"]

        conn = ensure_db()
        trade_row = conn.execute("SELECT pair FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        event_row = conn.execute(
            "SELECT symbol FROM order_events WHERE event_type='intent_created' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        dedup_row = conn.execute(
            "SELECT symbol FROM order_intent_dedup ORDER BY updated_ts DESC LIMIT 1"
        ).fetchone()
        conn.close()

        assert trade_row is not None
        assert trade_row["pair"] == "KRW-BTC"
        assert event_row is not None
        assert event_row["symbol"] == "KRW-BTC"
        assert dedup_row is not None
        assert dedup_row["symbol"] == "KRW-BTC"
    finally:
        _set("DB_PATH", old_db)
        _set("PAIR", old_pair)
        _set("SLIPPAGE_BPS", old_slip)
        _set("MAX_ORDER_KRW", old_max_order)
        _set("PAPER_FEE_RATE", old_paper_fee)


def test_paper_execute_buy_allows_harmless_dust_effective_flat(tmp_path: Path, monkeypatch):
    old_db = _set("DB_PATH", str(tmp_path / "paper_harmless_dust.sqlite"))
    old_slip = _set("SLIPPAGE_BPS", 0.0)
    old_max_order = _set("MAX_ORDER_KRW", 20_000.0)
    old_paper_fee = _set("PAPER_FEE_RATE", 0.0)
    old_buy_fraction = _set("BUY_FRACTION", 1.0)
    try:
        monkeypatch.setattr(
            paper,
            "fetch_orderbook_top",
            lambda _pair: BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=100.0),
        )
        monkeypatch.setattr(
            runtime_state,
            "snapshot",
            lambda: SimpleNamespace(
                last_reconcile_metadata={
                    "dust_classification": "harmless_dust",
                    "dust_residual_present": 1,
                    "dust_residual_allow_resume": 1,
                    "dust_effective_flat": 1,
                    "dust_policy_reason": "matched_harmless_dust_resume_allowed",
                    "dust_partial_flatten_recent": 0,
                    "dust_partial_flatten_reason": "flatten_not_recent",
                    "dust_qty_gap_tolerance": 0.00005,
                    "dust_qty_gap_small": 1,
                    "dust_broker_qty": 0.00009193,
                    "dust_local_qty": 0.00009193,
                    "dust_delta_qty": 0.0,
                    "dust_min_qty": 0.0001,
                    "dust_min_notional_krw": 5_000.0,
                    "dust_latest_price": 100_000_000.0,
                    "dust_broker_notional_krw": 9_193.0,
                    "dust_local_notional_krw": 9_193.0,
                    "dust_broker_qty_is_dust": 1,
                    "dust_local_qty_is_dust": 1,
                    "dust_broker_notional_is_dust": 0,
                    "dust_local_notional_is_dust": 0,
                    "dust_residual_summary": (
                        "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                        "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
                    ),
                }
            ),
        )

        conn = ensure_db()
        set_portfolio(conn, cash_krw=1_000_000, asset_qty=0.00009193)
        conn.close()

        trade = paper.paper_execute("BUY", ts=1, price=100.0)
        assert trade is not None

        conn = ensure_db()
        row = conn.execute(
            "SELECT side, status, qty_req FROM orders ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()

        assert row is not None
        assert row["side"] == "BUY"
        assert row["status"] in {"NEW", "FILLED"}
        assert float(row["qty_req"]) > 0
    finally:
        _set("DB_PATH", old_db)
        _set("SLIPPAGE_BPS", old_slip)
        _set("MAX_ORDER_KRW", old_max_order)
        _set("PAPER_FEE_RATE", old_paper_fee)
        _set("BUY_FRACTION", old_buy_fraction)


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
