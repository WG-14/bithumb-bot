from __future__ import annotations

import hashlib
import math
import json
import sqlite3
from decimal import Decimal, ROUND_HALF_EVEN
from collections.abc import Iterable
from typing import Any, Callable

from .config import prepare_db_path_for_connection, settings
from .dust import OPEN_EXPOSURE_LOT_STATE, lot_state_quantity_contract
from .sqlite_resilience import configure_connection
from .decision_context import normalize_strategy_decision_context


# The lot-state contract is intentionally tiny and safety-critical:
# open_exposure is the sellable inventory base, while dust_tracking is
# operator evidence only. Keep the schema aligned with that routing contract.
_OPEN_POSITION_LOT_STATES = tuple(lot_state_quantity_contract().keys())
EXTERNAL_CASH_ADJUSTMENT_EVENT_TYPE = "external_cash_adjustment"
_CASH_QUANTUM = Decimal("0.00000001")
_ASSET_QUANTUM = Decimal("0.000000000001")


def _as_finite_decimal(value: float | int | str, *, field: str) -> Decimal:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid accounting value for {field}: {value}") from exc
    if not math.isfinite(numeric):
        raise RuntimeError(f"invalid non-finite accounting value for {field}: {value}")
    return Decimal(str(numeric))


def normalize_cash_amount(value: float | int | str) -> float:
    return float(_as_finite_decimal(value, field="cash").quantize(_CASH_QUANTUM, rounding=ROUND_HALF_EVEN))


def normalize_asset_qty(value: float | int | str) -> float:
    return float(_as_finite_decimal(value, field="asset").quantize(_ASSET_QUANTUM, rounding=ROUND_HALF_EVEN))


def normalize_portfolio_breakdown(
    *,
    cash_available: float,
    cash_locked: float,
    asset_available: float,
    asset_locked: float,
) -> tuple[float, float, float, float]:
    return (
        normalize_cash_amount(cash_available),
        normalize_cash_amount(cash_locked),
        normalize_asset_qty(asset_available),
        normalize_asset_qty(asset_locked),
    )


def portfolio_cash_total(*, cash_available: float, cash_locked: float) -> float:
    return normalize_cash_amount(normalize_cash_amount(cash_available) + normalize_cash_amount(cash_locked))


def portfolio_asset_total(*, asset_available: float, asset_locked: float) -> float:
    return normalize_asset_qty(normalize_asset_qty(asset_available) + normalize_asset_qty(asset_locked))


def _consume_locked_then_available(
    locked: float,
    available: float,
    amount: float,
    *,
    field: str,
    normalize_amount: Callable[[float | int | str], float],
) -> tuple[float, float]:
    eps = 1e-12

    def _normalize_tiny_negative(value: float, *, tolerance: float) -> float:
        if -tolerance < value < 0.0:
            return 0.0
        return value

    def _float_tolerance(*values: float) -> float:
        finite_values = [abs(float(v)) for v in values if math.isfinite(float(v))]
        scale = max(finite_values) if finite_values else 0.0
        scale = max(scale, 1.0)
        return max(eps, math.ulp(scale) * 4)

    remaining = normalize_amount(amount)
    locked_after = normalize_amount(locked)
    available_after = normalize_amount(available)
    tolerance = _float_tolerance(locked_after, available_after, remaining, locked, available)

    locked_after = _normalize_tiny_negative(locked_after, tolerance=tolerance)
    available_after = _normalize_tiny_negative(available_after, tolerance=tolerance)

    from_locked = min(locked_after, remaining)
    locked_after = normalize_amount(locked_after - from_locked)
    remaining = normalize_amount(remaining - from_locked)

    if remaining > eps:
        available_after = normalize_amount(available_after - remaining)

    locked_after = _normalize_tiny_negative(locked_after, tolerance=tolerance)
    available_after = _normalize_tiny_negative(available_after, tolerance=tolerance)
    if locked_after < -tolerance or available_after < -tolerance:
        raise RuntimeError(
            f"negative {field} after fill: available={available_after}, locked={locked_after}, needed={amount}, tolerance={tolerance}"
        )
    return max(locked_after, 0.0), max(available_after, 0.0)


def calculate_fill_portfolio_snapshot(
    *,
    cash_available: float,
    cash_locked: float,
    asset_available: float,
    asset_locked: float,
    side: str,
    price: float,
    qty: float,
    fee: float | None,
) -> tuple[float, float, float, float, float, float]:
    cash_available_n, cash_locked_n, asset_available_n, asset_locked_n = normalize_portfolio_breakdown(
        cash_available=cash_available,
        cash_locked=cash_locked,
        asset_available=asset_available,
        asset_locked=asset_locked,
    )
    price_n = normalize_cash_amount(price)
    qty_n = normalize_asset_qty(qty)
    fee_n = normalize_cash_amount(0.0 if fee is None else fee)

    if side == "BUY":
        spend = normalize_cash_amount(price_n * qty_n + fee_n)
        cash_locked_after, cash_available_after = _consume_locked_then_available(
            cash_locked_n,
            cash_available_n,
            spend,
            field="cash",
            normalize_amount=normalize_cash_amount,
        )
        asset_available_after = normalize_asset_qty(asset_available_n + qty_n)
        asset_locked_after = asset_locked_n
    elif side == "SELL":
        cash_available_after = normalize_cash_amount(cash_available_n + (price_n * qty_n) - fee_n)
        cash_locked_after = cash_locked_n
        asset_locked_after, asset_available_after = _consume_locked_then_available(
            asset_locked_n,
            asset_available_n,
            qty_n,
            field="asset",
            normalize_amount=normalize_asset_qty,
        )
    else:
        raise RuntimeError(f"invalid fill side: {side}")

    cash_after = normalize_cash_amount(cash_available_after + cash_locked_after)
    asset_after = normalize_asset_qty(asset_available_after + asset_locked_after)
    return (
        cash_available_after,
        cash_locked_after,
        asset_available_after,
        asset_locked_after,
        cash_after,
        asset_after,
    )


def replay_fill_portfolio_snapshot(
    *,
    cash_available: float,
    cash_locked: float,
    asset_available: float,
    asset_locked: float,
    rows: Iterable[tuple[str, float, float, float | None]],
) -> tuple[float, float, float, float, float, float]:
    cash_available_n, cash_locked_n, asset_available_n, asset_locked_n = normalize_portfolio_breakdown(
        cash_available=cash_available,
        cash_locked=cash_locked,
        asset_available=asset_available,
        asset_locked=asset_locked,
    )
    cash_after = portfolio_cash_total(cash_available=cash_available_n, cash_locked=cash_locked_n)
    asset_after = portfolio_asset_total(asset_available=asset_available_n, asset_locked=asset_locked_n)

    for side, price, qty, fee in rows:
        (
            cash_available_n,
            cash_locked_n,
            asset_available_n,
            asset_locked_n,
            cash_after,
            asset_after,
        ) = calculate_fill_portfolio_snapshot(
            cash_available=cash_available_n,
            cash_locked=cash_locked_n,
            asset_available=asset_available_n,
            asset_locked=asset_locked_n,
            side=side,
            price=price,
            qty=qty,
            fee=fee,
        )

    return (
        cash_available_n,
        cash_locked_n,
        asset_available_n,
        asset_locked_n,
        cash_after,
        asset_after,
    )


def ensure_db(db_path: str | None = None, *, ensure_schema_ready: bool = True) -> sqlite3.Connection:
    path = prepare_db_path_for_connection(db_path or settings.DB_PATH, mode=settings.MODE)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    try:
        configure_connection(conn)
    except Exception:
        pass

    if ensure_schema_ready:
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
    _ensure_column(conn, "trades", "client_order_id", "client_order_id TEXT")
    _ensure_column(conn, "trades", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "trades", "entry_decision_id", "entry_decision_id INTEGER")
    _ensure_column(conn, "trades", "exit_decision_id", "exit_decision_id INTEGER")
    _ensure_column(conn, "trades", "exit_reason", "exit_reason TEXT")
    _ensure_column(conn, "trades", "exit_rule_name", "exit_rule_name TEXT")

    _ensure_column(conn, "external_cash_adjustments", "adjustment_key", "adjustment_key TEXT")
    _ensure_column(
        conn,
        "external_cash_adjustments",
        "event_type",
        "event_type TEXT NOT NULL DEFAULT 'external_cash_adjustment'",
    )
    _ensure_column(conn, "external_cash_adjustments", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "external_cash_adjustments", "currency", "currency TEXT NOT NULL DEFAULT 'KRW'")
    _ensure_column(conn, "external_cash_adjustments", "delta_amount", "delta_amount REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "external_cash_adjustments", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "external_cash_adjustments", "reason", "reason TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "external_cash_adjustments",
        "broker_snapshot_basis",
        "broker_snapshot_basis TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "external_cash_adjustments", "correlation_metadata", "correlation_metadata TEXT")
    _ensure_column(conn, "external_cash_adjustments", "note", "note TEXT")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_external_cash_adjustments_event_ts
        ON external_cash_adjustments(event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_external_cash_adjustments_key
        ON external_cash_adjustments(adjustment_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_external_cash_adjustments_currency_ts
        ON external_cash_adjustments(currency, event_ts, id)
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
        CREATE TABLE IF NOT EXISTS order_suppressions (
            suppression_key TEXT PRIMARY KEY,
            event_kind TEXT NOT NULL,
            event_ts INTEGER NOT NULL,
            mode TEXT NOT NULL,
            strategy_context TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            signal TEXT NOT NULL,
            side TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            reason TEXT NOT NULL,
            requested_qty REAL,
            normalized_qty REAL,
            market_price REAL,
            decision_id INTEGER,
            decision_reason TEXT,
            exit_rule_name TEXT,
            dust_present INTEGER NOT NULL DEFAULT 0,
            dust_allow_resume INTEGER NOT NULL DEFAULT 0,
            dust_effective_flat INTEGER NOT NULL DEFAULT 0,
            dust_state TEXT,
            dust_action TEXT,
            dust_signature TEXT,
            qty_below_min INTEGER NOT NULL DEFAULT 0,
            normalized_non_positive INTEGER NOT NULL DEFAULT 0,
            normalized_below_min INTEGER NOT NULL DEFAULT 0,
            notional_below_min INTEGER NOT NULL DEFAULT 0,
            summary TEXT,
            context_json TEXT,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            seen_count INTEGER NOT NULL DEFAULT 1
        )
        """
    )

    _ensure_column(conn, "order_suppressions", "requested_qty", "requested_qty REAL")
    _ensure_column(conn, "order_suppressions", "normalized_qty", "normalized_qty REAL")
    _ensure_column(conn, "order_suppressions", "market_price", "market_price REAL")
    _ensure_column(conn, "order_suppressions", "decision_id", "decision_id INTEGER")
    _ensure_column(conn, "order_suppressions", "decision_reason", "decision_reason TEXT")
    _ensure_column(conn, "order_suppressions", "exit_rule_name", "exit_rule_name TEXT")
    _ensure_column(conn, "order_suppressions", "dust_present", "dust_present INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_suppressions", "dust_allow_resume", "dust_allow_resume INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_suppressions", "dust_effective_flat", "dust_effective_flat INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_suppressions", "dust_state", "dust_state TEXT")
    _ensure_column(conn, "order_suppressions", "dust_action", "dust_action TEXT")
    _ensure_column(conn, "order_suppressions", "dust_signature", "dust_signature TEXT")
    _ensure_column(conn, "order_suppressions", "qty_below_min", "qty_below_min INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_suppressions", "normalized_non_positive", "normalized_non_positive INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_suppressions", "normalized_below_min", "normalized_below_min INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_suppressions", "notional_below_min", "notional_below_min INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_suppressions", "summary", "summary TEXT")
    _ensure_column(conn, "order_suppressions", "context_json", "context_json TEXT")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_suppressions_lookup
        ON order_suppressions(mode, strategy_name, signal, side, updated_ts)
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
            position_state TEXT NOT NULL DEFAULT '{open_state}' CHECK (position_state IN ({allowed_states})),
            entry_fee_total REAL NOT NULL DEFAULT 0,
            strategy_name TEXT,
            entry_decision_id INTEGER,
            entry_decision_linkage TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """.format(
            open_state=OPEN_EXPOSURE_LOT_STATE,
            allowed_states=', '.join(repr(state) for state in _OPEN_POSITION_LOT_STATES),
        )
    )

    _ensure_column(
        conn,
        "open_position_lots",
        "position_state",
        f"position_state TEXT NOT NULL DEFAULT '{OPEN_EXPOSURE_LOT_STATE}'",
    )
    _ensure_column(conn, "open_position_lots", "entry_fill_id", "entry_fill_id TEXT")
    _ensure_column(conn, "open_position_lots", "entry_fee_total", "entry_fee_total REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "open_position_lots", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "open_position_lots", "entry_decision_id", "entry_decision_id INTEGER")
    _ensure_column(conn, "open_position_lots", "entry_decision_linkage", "entry_decision_linkage TEXT")
    conn.execute(
        """
        UPDATE open_position_lots
        SET position_state=?
        WHERE position_state IS NULL OR TRIM(position_state)=''
        """
        ,
        (OPEN_EXPOSURE_LOT_STATE,),
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_open_position_lots_pair_ts
        ON open_position_lots(pair, entry_ts, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_open_position_lots_pair_state_ts
        ON open_position_lots(pair, position_state, entry_ts, id)
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
            entry_decision_linkage TEXT,
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
    _ensure_column(conn, "trade_lifecycles", "entry_decision_linkage", "entry_decision_linkage TEXT")
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
    return normalize_portfolio_breakdown(
        cash_available=float(row["cash_available"]),
        cash_locked=float(row["cash_locked"]),
        asset_available=float(row["asset_available"]),
        asset_locked=float(row["asset_locked"]),
    )


def get_portfolio(conn: sqlite3.Connection) -> tuple[float, float]:
    cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
    return portfolio_cash_total(cash_available=cash_available, cash_locked=cash_locked), portfolio_asset_total(
        asset_available=asset_available,
        asset_locked=asset_locked,
    )


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
    cash_available_n, cash_locked_n, asset_available_n, asset_locked_n = normalize_portfolio_breakdown(
        cash_available=cash_available,
        cash_locked=cash_locked,
        asset_available=asset_available,
        asset_locked=asset_locked,
    )
    cash_total = portfolio_cash_total(cash_available=cash_available_n, cash_locked=cash_locked_n)
    asset_total = portfolio_asset_total(asset_available=asset_available_n, asset_locked=asset_locked_n)
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
            cash_available_n,
            cash_locked_n,
            asset_available_n,
            asset_locked_n,
        ),
    )
    if not had_tx:
        conn.commit()


def _external_cash_adjustment_key(
    *,
    currency: str,
    delta_amount: float,
    source: str,
    reason: str,
    broker_snapshot_basis: str,
    correlation_metadata: str | None,
    note: str | None,
) -> str:
    payload = {
        "event_type": EXTERNAL_CASH_ADJUSTMENT_EVENT_TYPE,
        "currency": str(currency).strip().upper(),
        "delta_amount": f"{float(delta_amount):.12g}",
        "source": str(source).strip(),
        "reason": str(reason).strip(),
        "broker_snapshot_basis": str(broker_snapshot_basis).strip(),
        "correlation_metadata": str(correlation_metadata or "").strip(),
        "note": str(note or "").strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def record_external_cash_adjustment(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    currency: str,
    delta_amount: float,
    source: str,
    reason: str,
    broker_snapshot_basis: dict[str, Any] | str,
    correlation_metadata: dict[str, Any] | str | None = None,
    note: str | None = None,
    adjustment_key: str | None = None,
) -> dict[str, Any] | None:
    currency_value = str(currency).strip().upper()
    if currency_value != "KRW":
        raise RuntimeError(f"unsupported external cash adjustment currency: {currency_value}")

    basis_text = (
        json.dumps(broker_snapshot_basis, ensure_ascii=False, sort_keys=True)
        if isinstance(broker_snapshot_basis, dict)
        else str(broker_snapshot_basis)
    )
    correlation_text = (
        json.dumps(correlation_metadata, ensure_ascii=False, sort_keys=True)
        if isinstance(correlation_metadata, dict)
        else (str(correlation_metadata) if correlation_metadata is not None else None)
    )
    source_text = str(source).strip()
    reason_text = str(reason).strip()
    if not source_text:
        raise RuntimeError("external cash adjustment source is required")
    if not reason_text:
        raise RuntimeError("external cash adjustment reason is required")
    if not basis_text.strip():
        raise RuntimeError("external cash adjustment basis is required")

    init_portfolio(conn)
    had_tx = conn.in_transaction
    key = adjustment_key or _external_cash_adjustment_key(
        currency=currency_value,
        delta_amount=float(delta_amount),
        source=source_text,
        reason=reason_text,
        broker_snapshot_basis=basis_text,
        correlation_metadata=correlation_text,
        note=note,
    )

    existing = conn.execute(
        """
        SELECT id, adjustment_key, event_ts, currency, delta_amount, source, reason,
               broker_snapshot_basis, correlation_metadata, note
        FROM external_cash_adjustments
        WHERE adjustment_key=?
        """,
        (key,),
    ).fetchone()
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "adjustment_key": str(existing["adjustment_key"]),
            "event_ts": int(existing["event_ts"]),
            "currency": str(existing["currency"]),
            "delta_amount": float(existing["delta_amount"]),
            "source": str(existing["source"]),
            "reason": str(existing["reason"]),
            "broker_snapshot_basis": str(existing["broker_snapshot_basis"]),
            "correlation_metadata": (
                str(existing["correlation_metadata"])
                if existing["correlation_metadata"] is not None
                else None
            ),
            "note": str(existing["note"]) if existing["note"] is not None else None,
            "created": False,
        }

    portfolio_row = conn.execute(
        """
        SELECT cash_krw, cash_available, cash_locked
        FROM portfolio
        WHERE id=1
        """
    ).fetchone()
    if portfolio_row is None:
        raise RuntimeError("portfolio row missing while recording external cash adjustment")

    cash_available = normalize_cash_amount(portfolio_row["cash_available"])
    cash_locked = normalize_cash_amount(portfolio_row["cash_locked"])
    delta_amount_value = normalize_cash_amount(delta_amount)
    new_cash_available = normalize_cash_amount(cash_available + delta_amount_value)
    new_cash_total = portfolio_cash_total(cash_available=new_cash_available, cash_locked=cash_locked)
    if new_cash_total < -1e-8:
        raise RuntimeError(
            "external cash adjustment would make cash negative: "
            f"cash_before={float(portfolio_row['cash_krw']):.12g} delta={float(delta_amount):.12g} "
            f"cash_after={new_cash_total:.12g}"
        )

    cursor = conn.execute(
        """
        INSERT INTO external_cash_adjustments(
            adjustment_key, event_type, event_ts, currency, delta_amount, source, reason,
            broker_snapshot_basis, correlation_metadata, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            EXTERNAL_CASH_ADJUSTMENT_EVENT_TYPE,
            int(event_ts),
            currency_value,
            delta_amount_value,
            source_text,
            reason_text,
            basis_text,
            correlation_text,
            note,
        ),
    )
    conn.execute(
        """
        UPDATE portfolio
        SET cash_krw=?, cash_available=?
        WHERE id=1
        """,
        (
            new_cash_total,
            new_cash_available,
        ),
    )
    if not had_tx:
        conn.commit()

    return {
        "id": int(cursor.lastrowid),
        "adjustment_key": key,
        "event_ts": int(event_ts),
        "currency": currency_value,
        "delta_amount": delta_amount_value,
        "source": source_text,
        "reason": reason_text,
        "broker_snapshot_basis": basis_text,
        "correlation_metadata": correlation_text,
        "note": note,
        "created": True,
    }


def get_external_cash_adjustment_summary(conn: sqlite3.Connection) -> dict[str, float | int | str | None]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS adjustment_count, COALESCE(SUM(delta_amount), 0.0) AS adjustment_total
        FROM external_cash_adjustments
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT adjustment_key, event_ts, currency, delta_amount, source, reason, broker_snapshot_basis, correlation_metadata, note
        FROM external_cash_adjustments
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "adjustment_count": int(row["adjustment_count"] if row else 0),
        "adjustment_total": float(row["adjustment_total"] if row else 0.0),
        "last_event_ts": int(last["event_ts"]) if last is not None else None,
        "last_adjustment_key": str(last["adjustment_key"]) if last is not None else None,
        "last_currency": str(last["currency"]) if last is not None else None,
        "last_delta_amount": float(last["delta_amount"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_reason": str(last["reason"]) if last is not None else None,
        "last_broker_snapshot_basis": str(last["broker_snapshot_basis"]) if last is not None else None,
        "last_correlation_metadata": (
            str(last["correlation_metadata"]) if last is not None and last["correlation_metadata"] is not None else None
        ),
        "last_note": str(last["note"]) if last is not None and last["note"] is not None else None,
    }
