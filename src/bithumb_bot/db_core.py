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


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(row[1]) for row in cols}
    if column not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


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
            asset_qty REAL NOT NULL,
            cash_available REAL NOT NULL DEFAULT 0,
            cash_locked REAL NOT NULL DEFAULT 0,
            asset_available REAL NOT NULL DEFAULT 0,
            asset_locked REAL NOT NULL DEFAULT 0
        )
        """
    )

    _ensure_column(conn, "portfolio", "cash_available", "cash_available REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "portfolio", "cash_locked", "cash_locked REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "portfolio", "asset_available", "asset_available REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "portfolio", "asset_locked", "asset_locked REAL NOT NULL DEFAULT 0")

    # One-time backfill for pre-existing DBs that had only aggregate columns.
    conn.execute(
        """
        UPDATE portfolio
        SET
            cash_available = cash_krw,
            cash_locked = 0,
            asset_available = asset_qty,
            asset_locked = 0
        WHERE
            ABS(cash_available) < 1e-12
            AND ABS(cash_locked) < 1e-12
            AND ABS(asset_available) < 1e-12
            AND ABS(asset_locked) < 1e-12
            AND (ABS(cash_krw) >= 1e-12 OR ABS(asset_qty) >= 1e-12)
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
        CREATE TABLE IF NOT EXISTS bot_health (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            trading_enabled INTEGER NOT NULL DEFAULT 1,
            halt_new_orders_blocked INTEGER NOT NULL DEFAULT 0,
            halt_reason_code TEXT,
            halt_state_unresolved INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            last_candle_age_sec REAL,
            retry_at_epoch_sec REAL,
            last_disable_reason TEXT,
            unresolved_open_order_count INTEGER NOT NULL DEFAULT 0,
            oldest_unresolved_order_age_sec REAL,
            recovery_required_count INTEGER NOT NULL DEFAULT 0,
            last_reconcile_epoch_sec REAL,
            last_reconcile_status TEXT,
            last_reconcile_error TEXT,
            last_reconcile_reason_code TEXT,
            last_reconcile_metadata TEXT,
            last_cancel_open_orders_epoch_sec REAL,
            last_cancel_open_orders_trigger TEXT,
            last_cancel_open_orders_status TEXT,
            last_cancel_open_orders_summary TEXT,
            startup_gate_reason TEXT,
            updated_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )

    _ensure_column(
        conn,
        "bot_health",
        "halt_new_orders_blocked",
        "halt_new_orders_blocked INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(conn, "bot_health", "halt_reason_code", "halt_reason_code TEXT")
    _ensure_column(
        conn,
        "bot_health",
        "halt_state_unresolved",
        "halt_state_unresolved INTEGER NOT NULL DEFAULT 0",
    )

    _ensure_column(
        conn,
        "bot_health",
        "unresolved_open_order_count",
        "unresolved_open_order_count INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "bot_health",
        "oldest_unresolved_order_age_sec",
        "oldest_unresolved_order_age_sec REAL",
    )
    _ensure_column(
        conn,
        "bot_health",
        "recovery_required_count",
        "recovery_required_count INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_reconcile_epoch_sec",
        "last_reconcile_epoch_sec REAL",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_reconcile_status",
        "last_reconcile_status TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_reconcile_error",
        "last_reconcile_error TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_reconcile_reason_code",
        "last_reconcile_reason_code TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_reconcile_metadata",
        "last_reconcile_metadata TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_cancel_open_orders_epoch_sec",
        "last_cancel_open_orders_epoch_sec REAL",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_cancel_open_orders_trigger",
        "last_cancel_open_orders_trigger TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_cancel_open_orders_status",
        "last_cancel_open_orders_status TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_cancel_open_orders_summary",
        "last_cancel_open_orders_summary TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "startup_gate_reason",
        "startup_gate_reason TEXT",
    )

    conn.execute(
        """
        INSERT INTO bot_health (
            id,
            trading_enabled,
            halt_new_orders_blocked,
            halt_reason_code,
            halt_state_unresolved,
            error_count,
            last_candle_age_sec,
            retry_at_epoch_sec,
            last_disable_reason,
            unresolved_open_order_count,
            oldest_unresolved_order_age_sec,
            recovery_required_count,
            last_reconcile_epoch_sec,
            last_reconcile_status,
            last_reconcile_error,
            last_reconcile_reason_code,
            last_reconcile_metadata,
            last_cancel_open_orders_epoch_sec,
            last_cancel_open_orders_trigger,
            last_cancel_open_orders_status,
            last_cancel_open_orders_summary,
            startup_gate_reason
        )
        VALUES (1, 1, 0, NULL, 0, 0, NULL, NULL, NULL, 0, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        ON CONFLICT(id) DO NOTHING
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT NOT NULL UNIQUE,
            submit_attempt_id TEXT,
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

    _ensure_column(conn, "orders", "submit_attempt_id", "submit_attempt_id TEXT")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT NOT NULL,
            fill_id TEXT,
            fill_ts INTEGER NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            fee REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )

    _ensure_column(conn, "fills", "fill_id", "fill_id TEXT")

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_client_fill_id
        ON fills(client_order_id, fill_id)
        WHERE fill_id IS NOT NULL
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_ts INTEGER NOT NULL,
            order_status TEXT,
            exchange_order_id TEXT,
            fill_id TEXT,
            qty REAL,
            price REAL,
            message TEXT,
            symbol TEXT,
            side TEXT,
            submit_ts INTEGER,
            payload_fingerprint TEXT,
            broker_response_summary TEXT,
            exception_class TEXT,
            timeout_flag INTEGER,
            exchange_order_id_obtained INTEGER,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )

    _ensure_column(conn, "order_events", "symbol", "symbol TEXT")
    _ensure_column(conn, "order_events", "side", "side TEXT")
    _ensure_column(conn, "order_events", "submit_ts", "submit_ts INTEGER")
    _ensure_column(conn, "order_events", "payload_fingerprint", "payload_fingerprint TEXT")
    _ensure_column(conn, "order_events", "broker_response_summary", "broker_response_summary TEXT")
    _ensure_column(conn, "order_events", "exception_class", "exception_class TEXT")
    _ensure_column(conn, "order_events", "timeout_flag", "timeout_flag INTEGER")
    _ensure_column(conn, "order_events", "exchange_order_id_obtained", "exchange_order_id_obtained INTEGER")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_events_client_ts
        ON order_events(client_order_id, event_ts, id)
        """
    )

    conn.commit()


def init_portfolio(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT id FROM portfolio WHERE id=1").fetchone()
    if row is None:
        had_tx = conn.in_transaction
        conn.execute(
            """
            INSERT INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, ?, 0.0, ?, 0.0, 0.0, 0.0)
            """,
            (float(settings.START_CASH_KRW), float(settings.START_CASH_KRW)),
        )
        if not had_tx:
            conn.commit()


def get_portfolio_breakdown(conn: sqlite3.Connection) -> tuple[float, float, float, float]:
    init_portfolio(conn)
    row = conn.execute(
        "SELECT cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    return (
        float(row["cash_available"]),
        float(row["cash_locked"]),
        float(row["asset_available"]),
        float(row["asset_locked"]),
    )


def get_portfolio(conn: sqlite3.Connection) -> tuple[float, float]:
    cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
    return cash_available + cash_locked, asset_available + asset_locked


def set_portfolio(
    conn: sqlite3.Connection,
    cash_krw: float,
    asset_qty: float,
    *,
    cash_locked: float = 0.0,
    asset_locked: float = 0.0,
) -> None:
    set_portfolio_breakdown(
        conn,
        cash_available=float(cash_krw),
        cash_locked=float(cash_locked),
        asset_available=float(asset_qty),
        asset_locked=float(asset_locked),
    )


def set_portfolio_breakdown(
    conn: sqlite3.Connection,
    *,
    cash_available: float,
    cash_locked: float,
    asset_available: float,
    asset_locked: float,
) -> None:
    init_portfolio(conn)
    cash_total = float(cash_available) + float(cash_locked)
    asset_total = float(asset_available) + float(asset_locked)
    had_tx = conn.in_transaction
    conn.execute(
        """
        UPDATE portfolio
        SET
            cash_krw=?,
            asset_qty=?,
            cash_available=?,
            cash_locked=?,
            asset_available=?,
            asset_locked=?
        WHERE id=1
        """,
        (
            cash_total,
            asset_total,
            float(cash_available),
            float(cash_locked),
            float(asset_available),
            float(asset_locked),
        ),
    )
    if not had_tx:
        conn.commit()
