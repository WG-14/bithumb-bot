from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .config import resolve_db_path, settings
from .sqlite_resilience import configure_connection
from .decision_context import normalize_strategy_decision_context


def ensure_db(db_path: str | None = None) -> sqlite3.Connection:
    path = resolve_db_path(db_path or settings.DB_PATH)

    try:
        p = Path(path)
        if str(p) != ":memory:":
            p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    try:
        configure_connection(conn)
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
            client_order_id TEXT,
            strategy_name TEXT,
            entry_decision_id INTEGER,
            exit_decision_id INTEGER,
            exit_reason TEXT,
            exit_rule_name TEXT,
            note TEXT
        )
        """
    )
    _ensure_column(conn, "trades", "client_order_id", "client_order_id TEXT")
    _ensure_column(conn, "trades", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "trades", "entry_decision_id", "entry_decision_id INTEGER")
    _ensure_column(conn, "trades", "exit_decision_id", "exit_decision_id INTEGER")
    _ensure_column(conn, "trades", "exit_reason", "exit_reason TEXT")
    _ensure_column(conn, "trades", "exit_rule_name", "exit_rule_name TEXT")

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
            halt_policy_stage TEXT NOT NULL DEFAULT 'SAFE_HALT_REVIEW_ONLY',
            halt_policy_block_new_orders INTEGER NOT NULL DEFAULT 1,
            halt_policy_attempt_cancel_open_orders INTEGER NOT NULL DEFAULT 1,
            halt_policy_auto_liquidate_positions INTEGER NOT NULL DEFAULT 0,
            halt_position_present INTEGER NOT NULL DEFAULT 0,
            halt_open_orders_present INTEGER NOT NULL DEFAULT 0,
            halt_operator_action_required INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            last_candle_age_sec REAL,
            last_candle_status TEXT NOT NULL DEFAULT 'waiting_first_sync',
            last_candle_sync_epoch_sec REAL,
            last_candle_ts_ms INTEGER,
            last_processed_candle_ts_ms INTEGER,
            last_candle_status_detail TEXT,
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
            last_flatten_position_epoch_sec REAL,
            last_flatten_position_status TEXT,
            last_flatten_position_summary TEXT,
            emergency_flatten_blocked INTEGER NOT NULL DEFAULT 0,
            emergency_flatten_block_reason TEXT,
            startup_gate_reason TEXT,
            resume_gate_blocked INTEGER NOT NULL DEFAULT 0,
            resume_gate_reason TEXT,
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
        "halt_policy_stage",
        "halt_policy_stage TEXT NOT NULL DEFAULT 'SAFE_HALT_REVIEW_ONLY'",
    )
    _ensure_column(
        conn,
        "bot_health",
        "halt_policy_block_new_orders",
        "halt_policy_block_new_orders INTEGER NOT NULL DEFAULT 1",
    )
    _ensure_column(
        conn,
        "bot_health",
        "halt_policy_attempt_cancel_open_orders",
        "halt_policy_attempt_cancel_open_orders INTEGER NOT NULL DEFAULT 1",
    )
    _ensure_column(
        conn,
        "bot_health",
        "halt_policy_auto_liquidate_positions",
        "halt_policy_auto_liquidate_positions INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "bot_health",
        "halt_position_present",
        "halt_position_present INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "bot_health",
        "halt_open_orders_present",
        "halt_open_orders_present INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "bot_health",
        "halt_operator_action_required",
        "halt_operator_action_required INTEGER NOT NULL DEFAULT 0",
    )

    _ensure_column(
        conn,
        "bot_health",
        "last_candle_status",
        "last_candle_status TEXT NOT NULL DEFAULT 'waiting_first_sync'",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_candle_sync_epoch_sec",
        "last_candle_sync_epoch_sec REAL",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_candle_ts_ms",
        "last_candle_ts_ms INTEGER",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_processed_candle_ts_ms",
        "last_processed_candle_ts_ms INTEGER",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_candle_status_detail",
        "last_candle_status_detail TEXT",
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
        "last_flatten_position_epoch_sec",
        "last_flatten_position_epoch_sec REAL",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_flatten_position_status",
        "last_flatten_position_status TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "last_flatten_position_summary",
        "last_flatten_position_summary TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "emergency_flatten_blocked",
        "emergency_flatten_blocked INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "bot_health",
        "emergency_flatten_block_reason",
        "emergency_flatten_block_reason TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "startup_gate_reason",
        "startup_gate_reason TEXT",
    )
    _ensure_column(
        conn,
        "bot_health",
        "resume_gate_blocked",
        "resume_gate_blocked INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "bot_health",
        "resume_gate_reason",
        "resume_gate_reason TEXT",
    )

    conn.execute(
        """
        INSERT INTO bot_health (id)
        VALUES (1)
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
            strategy_name TEXT,
            entry_decision_id INTEGER,
            exit_decision_id INTEGER,
            decision_reason TEXT,
            exit_rule_name TEXT,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            last_error TEXT
        )
        """
    )

    _ensure_column(conn, "orders", "submit_attempt_id", "submit_attempt_id TEXT")
    _ensure_column(conn, "orders", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "orders", "entry_decision_id", "entry_decision_id INTEGER")
    _ensure_column(conn, "orders", "exit_decision_id", "exit_decision_id INTEGER")
    _ensure_column(conn, "orders", "decision_reason", "decision_reason TEXT")
    _ensure_column(conn, "orders", "exit_rule_name", "exit_rule_name TEXT")

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
            reference_price REAL,
            slippage_bps REAL,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )

    _ensure_column(conn, "fills", "fill_id", "fill_id TEXT")
    _ensure_column(conn, "fills", "reference_price", "reference_price REAL")
    _ensure_column(conn, "fills", "slippage_bps", "slippage_bps REAL")

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
            submit_attempt_id TEXT,
            mode TEXT,
            intent_ts INTEGER,
            submit_ts INTEGER,
            payload_fingerprint TEXT,
            broker_response_summary TEXT,
            submission_reason_code TEXT,
            exception_class TEXT,
            timeout_flag INTEGER,
            submit_evidence TEXT,
            exchange_order_id_obtained INTEGER,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )

    _ensure_column(conn, "order_events", "symbol", "symbol TEXT")
    _ensure_column(conn, "order_events", "side", "side TEXT")
    _ensure_column(conn, "order_events", "submit_attempt_id", "submit_attempt_id TEXT")
    _ensure_column(conn, "order_events", "mode", "mode TEXT")
    _ensure_column(conn, "order_events", "intent_ts", "intent_ts INTEGER")
    _ensure_column(conn, "order_events", "submit_ts", "submit_ts INTEGER")
    _ensure_column(conn, "order_events", "payload_fingerprint", "payload_fingerprint TEXT")
    _ensure_column(conn, "order_events", "broker_response_summary", "broker_response_summary TEXT")
    _ensure_column(conn, "order_events", "submission_reason_code", "submission_reason_code TEXT")
    _ensure_column(conn, "order_events", "exception_class", "exception_class TEXT")
    _ensure_column(conn, "order_events", "timeout_flag", "timeout_flag INTEGER")
    _ensure_column(conn, "order_events", "submit_evidence", "submit_evidence TEXT")
    _ensure_column(conn, "order_events", "exchange_order_id_obtained", "exchange_order_id_obtained INTEGER")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_events_client_ts
        ON order_events(client_order_id, event_ts, id)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_intent_dedup (
            intent_key TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            strategy_context TEXT NOT NULL,
            intent_type TEXT NOT NULL,
            intent_ts INTEGER NOT NULL,
            qty REAL,
            client_order_id TEXT NOT NULL,
            order_status TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            last_error TEXT
        )
        """
    )

    _ensure_column(conn, "order_intent_dedup", "qty", "qty REAL")
    _ensure_column(conn, "order_intent_dedup", "last_error", "last_error TEXT")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_intent_dedup_lookup
        ON order_intent_dedup(symbol, side, intent_ts, updated_ts)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_ts INTEGER NOT NULL,
            strategy_name TEXT NOT NULL,
            signal TEXT NOT NULL,
            reason TEXT NOT NULL,
            candle_ts INTEGER,
            market_price REAL,
            confidence REAL,
            context_json TEXT NOT NULL
        )
        """
    )

    _ensure_column(conn, "strategy_decisions", "candle_ts", "candle_ts INTEGER")
    _ensure_column(conn, "strategy_decisions", "market_price", "market_price REAL")
    _ensure_column(conn, "strategy_decisions", "confidence", "confidence REAL")
    _ensure_column(conn, "strategy_decisions", "context_json", "context_json TEXT NOT NULL DEFAULT '{}'")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_decisions_lookup
        ON strategy_decisions(strategy_name, decision_ts, signal)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS open_position_lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            entry_trade_id INTEGER NOT NULL,
            entry_client_order_id TEXT NOT NULL,
            entry_fill_id TEXT,
            entry_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            qty_open REAL NOT NULL,
            entry_fee_total REAL NOT NULL DEFAULT 0,
            strategy_name TEXT,
            entry_decision_id INTEGER,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )

    _ensure_column(conn, "open_position_lots", "entry_fill_id", "entry_fill_id TEXT")
    _ensure_column(conn, "open_position_lots", "entry_fee_total", "entry_fee_total REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "open_position_lots", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "open_position_lots", "entry_decision_id", "entry_decision_id INTEGER")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_open_position_lots_pair_ts
        ON open_position_lots(pair, entry_ts, id)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_lifecycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            entry_trade_id INTEGER NOT NULL,
            exit_trade_id INTEGER NOT NULL,
            entry_client_order_id TEXT NOT NULL,
            exit_client_order_id TEXT NOT NULL,
            entry_fill_id TEXT,
            exit_fill_id TEXT,
            entry_ts INTEGER NOT NULL,
            exit_ts INTEGER NOT NULL,
            matched_qty REAL NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            gross_pnl REAL NOT NULL,
            fee_total REAL NOT NULL,
            net_pnl REAL NOT NULL,
            holding_time_sec REAL NOT NULL,
            strategy_name TEXT,
            entry_decision_id INTEGER,
            exit_decision_id INTEGER,
            exit_reason TEXT,
            exit_rule_name TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )

    _ensure_column(conn, "trade_lifecycles", "entry_fill_id", "entry_fill_id TEXT")
    _ensure_column(conn, "trade_lifecycles", "exit_fill_id", "exit_fill_id TEXT")
    _ensure_column(conn, "trade_lifecycles", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "trade_lifecycles", "entry_decision_id", "entry_decision_id INTEGER")
    _ensure_column(conn, "trade_lifecycles", "exit_decision_id", "exit_decision_id INTEGER")
    _ensure_column(conn, "trade_lifecycles", "exit_reason", "exit_reason TEXT")
    _ensure_column(conn, "trade_lifecycles", "exit_rule_name", "exit_rule_name TEXT")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_trade_lifecycles_exit_trade
        ON trade_lifecycles(exit_trade_id, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_trade_lifecycles_entry_trade
        ON trade_lifecycles(entry_trade_id, id)
        """
    )

    conn.commit()


def record_strategy_decision(
    conn: sqlite3.Connection,
    *,
    decision_ts: int,
    strategy_name: str,
    signal: str,
    reason: str,
    candle_ts: int | None,
    market_price: float | None,
    context: dict[str, Any] | None,
    confidence: float | None = None,
) -> int:
    normalized_context = normalize_strategy_decision_context(
        context=context,
        signal=str(signal),
        reason=str(reason),
        strategy_name=str(strategy_name),
        pair=str(settings.PAIR),
        interval=str(settings.INTERVAL),
        decision_ts=int(decision_ts),
        candle_ts=None if candle_ts is None else int(candle_ts),
        market_price=None if market_price is None else float(market_price),
    )
    row = conn.execute(
        """
        INSERT INTO strategy_decisions(
            decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(decision_ts),
            str(strategy_name),
            str(signal),
            str(reason),
            None if candle_ts is None else int(candle_ts),
            None if market_price is None else float(market_price),
            None if confidence is None else float(confidence),
            json.dumps(normalized_context, ensure_ascii=False, sort_keys=True),
        ),
    )
    return int(row.lastrowid)


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
