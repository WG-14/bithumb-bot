from __future__ import annotations

import sqlite3

from bithumb_bot.db_core import (
    ensure_db,
    get_portfolio,
    get_portfolio_breakdown,
    init_portfolio,
    set_portfolio,
    set_portfolio_breakdown,
)


def test_schema_bootstrap_creates_portfolio_split_columns(tmp_path):
    conn = ensure_db(str(tmp_path / "bootstrap.sqlite"))
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(portfolio)").fetchall()}
    conn.close()

    assert "cash_available" in cols
    assert "cash_locked" in cols
    assert "asset_available" in cols
    assert "asset_locked" in cols


def test_portfolio_read_write_supports_breakdown_shape(tmp_path):
    conn = ensure_db(str(tmp_path / "shape.sqlite"))
    init_portfolio(conn)

    set_portfolio_breakdown(
        conn,
        cash_available=123_000.0,
        cash_locked=7_000.0,
        asset_available=0.25,
        asset_locked=0.75,
    )

    cash_a, cash_l, asset_a, asset_l = get_portfolio_breakdown(conn)
    cash_total, asset_total = get_portfolio(conn)
    raw = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    conn.close()

    assert (cash_a, cash_l, asset_a, asset_l) == (123_000.0, 7_000.0, 0.25, 0.75)
    assert cash_total == 130_000.0
    assert asset_total == 1.0
    assert float(raw["cash_krw"]) == 130_000.0
    assert float(raw["asset_qty"]) == 1.0


def test_portfolio_schema_upgrade_backfills_from_legacy_aggregate_columns(tmp_path):
    db_path = tmp_path / "legacy.sqlite"

    legacy = sqlite3.connect(str(db_path))
    legacy.execute(
        """
        CREATE TABLE portfolio (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cash_krw REAL NOT NULL,
            asset_qty REAL NOT NULL
        )
        """
    )
    legacy.execute("INSERT INTO portfolio(id, cash_krw, asset_qty) VALUES (1, 456789.0, 0.42)")
    legacy.commit()
    legacy.close()

    conn = ensure_db(str(db_path))
    row = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    conn.close()

    assert float(row["cash_krw"]) == 456789.0
    assert float(row["asset_qty"]) == 0.42
    assert float(row["cash_available"]) == 456789.0
    assert float(row["cash_locked"]) == 0.0
    assert float(row["asset_available"]) == 0.42
    assert float(row["asset_locked"]) == 0.0


def test_set_portfolio_legacy_api_still_works(tmp_path):
    conn = ensure_db(str(tmp_path / "legacy_api.sqlite"))
    init_portfolio(conn)

    set_portfolio(conn, cash_krw=10_000.0, asset_qty=0.5)

    row = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    conn.close()

    assert float(row["cash_krw"]) == 10_000.0
    assert float(row["asset_qty"]) == 0.5
    assert float(row["cash_available"]) == 10_000.0
    assert float(row["cash_locked"]) == 0.0
    assert float(row["asset_available"]) == 0.5
    assert float(row["asset_locked"]) == 0.0
