# src/bithumb_bot/db_schema.py
from __future__ import annotations

import sqlite3


def ensure_schema(conn: sqlite3.Connection) -> None:
    # candles
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
        CREATE INDEX IF NOT EXISTS idx_candles_pair_interval_ts
        ON candles(pair, interval, ts)
        """
    )

    # portfolio (single-asset)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cash REAL NOT NULL,
            qty REAL NOT NULL
        )
        """
    )

    # trades
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            side TEXT NOT NULL,   -- BUY / SELL
            price REAL NOT NULL,
            qty REAL NOT NULL,
            fee REAL NOT NULL,
            cash REAL NOT NULL,
            note TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_cash_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            adjustment_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL DEFAULT 'external_cash_adjustment'
                CHECK (event_type = 'external_cash_adjustment'),
            event_ts INTEGER NOT NULL,
            currency TEXT NOT NULL,
            delta_amount REAL NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            broker_snapshot_basis TEXT NOT NULL,
            correlation_metadata TEXT,
            note TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )

    # risk
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_risk (
            day_kst TEXT PRIMARY KEY,
            start_equity REAL NOT NULL,
            baseline_cash_krw REAL,
            baseline_asset_qty REAL,
            baseline_mark_price REAL,
            baseline_mark_price_source TEXT,
            baseline_origin TEXT,
            baseline_balance_source TEXT,
            baseline_balance_observed_ts_ms INTEGER,
            baseline_reconcile_epoch_sec REAL,
            baseline_reconcile_reason_code TEXT,
            baseline_context TEXT,
            created_ts_ms INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evaluation_ts_ms INTEGER NOT NULL,
            day_kst TEXT NOT NULL,
            evaluation_origin TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            decision TEXT NOT NULL,
            max_daily_loss_krw REAL NOT NULL,
            start_equity REAL,
            current_equity REAL,
            loss_today REAL,
            current_cash_krw REAL,
            current_asset_qty REAL,
            mark_price REAL NOT NULL,
            mark_price_source TEXT NOT NULL,
            baseline_cash_krw REAL,
            baseline_asset_qty REAL,
            baseline_mark_price REAL,
            baseline_origin TEXT,
            baseline_balance_source TEXT,
            baseline_balance_observed_ts_ms INTEGER,
            current_source TEXT,
            current_balance_source TEXT,
            current_balance_observed_ts_ms INTEGER,
            current_reconcile_epoch_sec REAL,
            current_reconcile_reason_code TEXT,
            local_cash_krw REAL,
            local_asset_qty REAL,
            broker_cash_krw REAL,
            broker_asset_qty REAL,
            mismatch_summary TEXT,
            details_json TEXT
        )
        """
    )

    conn.commit()


def init_portfolio(conn: sqlite3.Connection, start_cash: float) -> None:
    row = conn.execute("SELECT cash, qty FROM portfolio WHERE id=1").fetchone()
    if row:
        return
    conn.execute("INSERT INTO portfolio(id, cash, qty) VALUES (1, ?, 0.0)", (float(start_cash),))
    conn.commit()
