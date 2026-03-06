from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import settings


def ensure_db(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or settings.DB_PATH

    try:
        p = Path(path)
        if str(p) != ":memory:":
            p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
    except Exception:
        pass

    ensure_schema(conn)
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candles (
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (ts, pair, interval)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cash_krw REAL NOT NULL,
            asset_qty REAL NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            fee REAL NOT NULL,
            cash_after REAL NOT NULL,
            asset_after REAL NOT NULL,
            note TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_risk (
            day_kst TEXT PRIMARY KEY,
            start_equity REAL NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT NOT NULL UNIQUE,
            exchange_order_id TEXT,
            status TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL,
            qty_req REAL NOT NULL,
            qty_filled REAL NOT NULL DEFAULT 0,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            last_error TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT NOT NULL,
            fill_ts INTEGER NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            fee REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )

    conn.commit()


def init_portfolio(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
    if row is None:
        had_tx = conn.in_transaction
        conn.execute(
            "INSERT INTO portfolio(id, cash_krw, asset_qty) VALUES (1, ?, 0.0)",
            (float(settings.START_CASH_KRW),),
        )
        if not had_tx:
            conn.commit()


def get_portfolio(conn: sqlite3.Connection) -> tuple[float, float]:
    init_portfolio(conn)
    row = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
    return float(row["cash_krw"]), float(row["asset_qty"])


def set_portfolio(conn: sqlite3.Connection, cash_krw: float, asset_qty: float) -> None:
    had_tx = conn.in_transaction
    conn.execute(
        "UPDATE portfolio SET cash_krw=?, asset_qty=? WHERE id=1",
        (float(cash_krw), float(asset_qty)),
    )
    if not had_tx:
        conn.commit()