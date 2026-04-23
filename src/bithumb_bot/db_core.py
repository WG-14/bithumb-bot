from __future__ import annotations

import hashlib
import math
import json
import sqlite3
from decimal import Decimal, ROUND_HALF_EVEN
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Callable

from .config import prepare_db_path_for_connection, settings
from .dust import OPEN_EXPOSURE_LOT_STATE, lot_state_quantity_contract
from .sqlite_resilience import configure_connection
from .decision_context import (
    materialize_strategy_decision_context,
    normalize_strategy_decision_context,
)


# The lot-state contract is intentionally tiny and safety-critical:
# open_exposure is the sellable inventory base, while dust_tracking is
# operator evidence only. Keep the schema aligned with that routing contract.
_OPEN_POSITION_LOT_STATES = tuple(lot_state_quantity_contract().keys())
EXTERNAL_CASH_ADJUSTMENT_EVENT_TYPE = "external_cash_adjustment"
EXTERNAL_POSITION_ADJUSTMENT_EVENT_TYPE = "external_position_adjustment"
MANUAL_FLAT_ACCOUNTING_REPAIR_EVENT_TYPE = "manual_flat_accounting_repair"
FEE_GAP_ACCOUNTING_REPAIR_EVENT_TYPE = "fee_gap_accounting_repair"
FEE_PENDING_ACCOUNTING_REPAIR_EVENT_TYPE = "fee_pending_accounting_repair"
POSITION_AUTHORITY_REPAIR_EVENT_TYPE = "position_authority_repair"
BROKER_FILL_OBSERVATION_EVENT_TYPE = "broker_fill_observation"
ACCOUNTING_PROJECTION_MODEL = "authoritative_accounting_projection_v1"
AUTHORITATIVE_ACCOUNTING_EVENT_FAMILIES = (
    "fills",
    "external_cash_adjustments",
    "manual_flat_accounting_repairs",
    "external_position_adjustments",
)
DIAGNOSTIC_ACCOUNTING_EVENT_FAMILIES = (
    "broker_fill_observations",
    "position_authority_repairs",
    "fee_gap_accounting_repairs",
    "fee_pending_accounting_repairs",
)
_CASH_QUANTUM = Decimal("0.00000001")
_ASSET_QUANTUM = Decimal("0.000000000001")
FEE_ACCOUNTING_COMPLETE_EPS = 1e-12


@dataclass(frozen=True)
class FillAccountingIncidentVerdict:
    fill_key: str
    client_order_id: str
    fill_id: str | None
    fill_ts: int
    price: float
    qty: float
    authoritative_fill_present: bool
    final_fee_applied: bool
    authoritative_fill_row_id: int | None
    authoritative_fill_fee: float | None
    latest_observation_id: int | None
    latest_observation_event_ts: int | None
    latest_observation_fee_status: str | None
    latest_observation_accounting_status: str | None
    latest_observation_source: str | None
    repair_present: bool
    repair_count: int
    latest_repair_id: int | None
    canonical_incident_state: str
    incident_scope: str
    active_issue: bool
    raw_observation_count: int
    fee_pending_observation_count: int
    accounting_complete_observation_count: int
    evidence: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "fill_key": self.fill_key,
            "client_order_id": self.client_order_id,
            "fill_id": self.fill_id,
            "fill_ts": self.fill_ts,
            "price": self.price,
            "qty": self.qty,
            "authoritative_fill_present": bool(self.authoritative_fill_present),
            "final_fee_applied": bool(self.final_fee_applied),
            "authoritative_fill_row_id": self.authoritative_fill_row_id,
            "authoritative_fill_fee": self.authoritative_fill_fee,
            "latest_observation_id": self.latest_observation_id,
            "latest_observation_event_ts": self.latest_observation_event_ts,
            "latest_observation_fee_status": self.latest_observation_fee_status,
            "latest_observation_accounting_status": self.latest_observation_accounting_status,
            "latest_observation_source": self.latest_observation_source,
            "repair_present": bool(self.repair_present),
            "repair_count": int(self.repair_count),
            "latest_repair_id": self.latest_repair_id,
            "canonical_incident_state": self.canonical_incident_state,
            "incident_scope": self.incident_scope,
            "active_issue": bool(self.active_issue),
            "raw_observation_count": int(self.raw_observation_count),
            "fee_pending_observation_count": int(self.fee_pending_observation_count),
            "accounting_complete_observation_count": int(self.accounting_complete_observation_count),
            "evidence": dict(self.evidence),
        }


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


def _ensure_open_position_lot_invariant_triggers(conn: sqlite3.Connection) -> None:
    invariant_check = """
        SELECT CASE
            WHEN COALESCE(NEW.executable_lot_count, 0) < 0
                OR COALESCE(NEW.dust_tracking_lot_count, 0) < 0
                THEN RAISE(ABORT, 'open_position_lots negative lot counts are not allowed')
            WHEN COALESCE(NEW.position_semantic_basis, '') = 'lot-native'
                 AND COALESCE(NEW.qty_open, 0.0) > 1e-12
                 AND (
                    (NEW.position_state = 'open_exposure'
                        AND (
                            COALESCE(NEW.executable_lot_count, 0) <= 0
                            OR COALESCE(NEW.dust_tracking_lot_count, 0) != 0
                        ))
                    OR
                    (NEW.position_state = 'dust_tracking'
                        AND (
                            COALESCE(NEW.executable_lot_count, 0) != 0
                            OR COALESCE(NEW.dust_tracking_lot_count, 0) <= 0
                        ))
                 )
                THEN RAISE(ABORT, 'open_position_lots lot-native state/count mismatch')
            WHEN COALESCE(NEW.position_semantic_basis, '') = 'lot-native'
                 AND COALESCE(NEW.qty_open, 0.0) > 1e-12
                 AND COALESCE(NEW.internal_lot_size, 0.0) > 1e-12
                 AND NEW.position_state = 'open_exposure'
                 AND ABS(
                    COALESCE(NEW.qty_open, 0.0)
                    - (COALESCE(NEW.executable_lot_count, 0) * COALESCE(NEW.internal_lot_size, 0.0))
                 ) > 1e-12
                THEN RAISE(ABORT, 'open_position_lots executable qty must match lot authority')
            WHEN COALESCE(NEW.position_semantic_basis, '') = 'lot-native'
                 AND COALESCE(NEW.qty_open, 0.0) > 1e-12
                 AND COALESCE(NEW.internal_lot_size, 0.0) > 1e-12
                 AND NEW.position_state = 'dust_tracking'
                 AND (
                    COALESCE(NEW.qty_open, 0.0)
                    - (COALESCE(NEW.dust_tracking_lot_count, 0) * COALESCE(NEW.internal_lot_size, 0.0))
                 ) > 1e-12
                THEN RAISE(ABORT, 'open_position_lots dust qty must not exceed lot authority')
            WHEN COALESCE(NEW.qty_open, 0.0) <= 1e-12
                 AND (
                    COALESCE(NEW.executable_lot_count, 0) != 0
                    OR COALESCE(NEW.dust_tracking_lot_count, 0) != 0
                 )
                THEN RAISE(ABORT, 'open_position_lots zero qty rows must not keep lot authority')
        END
    """
    conn.execute("DROP TRIGGER IF EXISTS trg_open_position_lots_validate_insert")
    conn.execute("DROP TRIGGER IF EXISTS trg_open_position_lots_validate_update")
    conn.execute(
        f"""
        CREATE TRIGGER trg_open_position_lots_validate_insert
        BEFORE INSERT ON open_position_lots
        FOR EACH ROW
        BEGIN
            {invariant_check};
        END
        """
    )
    conn.execute(
        f"""
        CREATE TRIGGER trg_open_position_lots_validate_update
        BEFORE UPDATE ON open_position_lots
        FOR EACH ROW
        BEGIN
            {invariant_check};
        END
        """
    )


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manual_flat_accounting_repairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repair_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL DEFAULT 'manual_flat_accounting_repair'
                CHECK (event_type = 'manual_flat_accounting_repair'),
            event_ts INTEGER NOT NULL,
            asset_qty_delta REAL NOT NULL,
            cash_delta REAL NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            repair_basis TEXT NOT NULL,
            note TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_position_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            adjustment_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL DEFAULT 'external_position_adjustment'
                CHECK (event_type = 'external_position_adjustment'),
            event_ts INTEGER NOT NULL,
            asset_qty_delta REAL NOT NULL,
            cash_delta REAL NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            adjustment_basis TEXT NOT NULL,
            note TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fee_gap_accounting_repairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repair_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL DEFAULT 'fee_gap_accounting_repair'
                CHECK (event_type = 'fee_gap_accounting_repair'),
            event_ts INTEGER NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            repair_basis TEXT NOT NULL,
            note TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fee_pending_accounting_repairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repair_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL DEFAULT 'fee_pending_accounting_repair'
                CHECK (event_type = 'fee_pending_accounting_repair'),
            event_ts INTEGER NOT NULL,
            client_order_id TEXT NOT NULL,
            exchange_order_id TEXT,
            fill_id TEXT,
            fill_ts INTEGER NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            fee REAL NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            repair_basis TEXT NOT NULL,
            note TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_authority_repairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repair_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL DEFAULT 'position_authority_repair'
                CHECK (event_type = 'position_authority_repair'),
            event_ts INTEGER NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            repair_basis TEXT NOT NULL,
            note TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_authority_projection_publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publication_key TEXT NOT NULL UNIQUE,
            publication_type TEXT NOT NULL DEFAULT 'portfolio_projection_publish'
                CHECK (publication_type = 'portfolio_projection_publish'),
            pair TEXT NOT NULL,
            target_trade_id INTEGER NOT NULL,
            event_ts INTEGER NOT NULL,
            source TEXT NOT NULL,
            publish_basis TEXT NOT NULL,
            note TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_fill_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL DEFAULT 'broker_fill_observation'
                CHECK (event_type = 'broker_fill_observation'),
            event_ts INTEGER NOT NULL,
            client_order_id TEXT NOT NULL,
            exchange_order_id TEXT,
            fill_id TEXT,
            fill_ts INTEGER NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            fee REAL,
            fee_status TEXT NOT NULL,
            accounting_status TEXT NOT NULL,
            source TEXT NOT NULL,
            parse_warnings TEXT,
            raw_payload TEXT,
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
    _ensure_column(conn, "manual_flat_accounting_repairs", "repair_key", "repair_key TEXT")
    _ensure_column(
        conn,
        "manual_flat_accounting_repairs",
        "event_type",
        "event_type TEXT NOT NULL DEFAULT 'manual_flat_accounting_repair'",
    )
    _ensure_column(conn, "manual_flat_accounting_repairs", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(
        conn,
        "manual_flat_accounting_repairs",
        "asset_qty_delta",
        "asset_qty_delta REAL NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "manual_flat_accounting_repairs",
        "cash_delta",
        "cash_delta REAL NOT NULL DEFAULT 0",
    )
    _ensure_column(conn, "manual_flat_accounting_repairs", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "manual_flat_accounting_repairs", "reason", "reason TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "manual_flat_accounting_repairs",
        "repair_basis",
        "repair_basis TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "manual_flat_accounting_repairs", "note", "note TEXT")
    _ensure_column(conn, "external_position_adjustments", "adjustment_key", "adjustment_key TEXT")
    _ensure_column(
        conn,
        "external_position_adjustments",
        "event_type",
        "event_type TEXT NOT NULL DEFAULT 'external_position_adjustment'",
    )
    _ensure_column(conn, "external_position_adjustments", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(
        conn,
        "external_position_adjustments",
        "asset_qty_delta",
        "asset_qty_delta REAL NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "external_position_adjustments",
        "cash_delta",
        "cash_delta REAL NOT NULL DEFAULT 0",
    )
    _ensure_column(conn, "external_position_adjustments", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "external_position_adjustments", "reason", "reason TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "external_position_adjustments",
        "adjustment_basis",
        "adjustment_basis TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "external_position_adjustments", "note", "note TEXT")
    _ensure_column(conn, "fee_gap_accounting_repairs", "repair_key", "repair_key TEXT")
    _ensure_column(
        conn,
        "fee_gap_accounting_repairs",
        "event_type",
        "event_type TEXT NOT NULL DEFAULT 'fee_gap_accounting_repair'",
    )
    _ensure_column(conn, "fee_gap_accounting_repairs", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "fee_gap_accounting_repairs", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "fee_gap_accounting_repairs", "reason", "reason TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "fee_gap_accounting_repairs",
        "repair_basis",
        "repair_basis TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "fee_gap_accounting_repairs", "note", "note TEXT")
    _ensure_column(conn, "fee_pending_accounting_repairs", "repair_key", "repair_key TEXT")
    _ensure_column(
        conn,
        "fee_pending_accounting_repairs",
        "event_type",
        "event_type TEXT NOT NULL DEFAULT 'fee_pending_accounting_repair'",
    )
    _ensure_column(conn, "fee_pending_accounting_repairs", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "fee_pending_accounting_repairs", "client_order_id", "client_order_id TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "fee_pending_accounting_repairs", "exchange_order_id", "exchange_order_id TEXT")
    _ensure_column(conn, "fee_pending_accounting_repairs", "fill_id", "fill_id TEXT")
    _ensure_column(conn, "fee_pending_accounting_repairs", "fill_ts", "fill_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "fee_pending_accounting_repairs", "price", "price REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "fee_pending_accounting_repairs", "qty", "qty REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "fee_pending_accounting_repairs", "fee", "fee REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "fee_pending_accounting_repairs", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "fee_pending_accounting_repairs", "reason", "reason TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "fee_pending_accounting_repairs",
        "repair_basis",
        "repair_basis TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "fee_pending_accounting_repairs", "note", "note TEXT")
    _ensure_column(conn, "position_authority_repairs", "repair_key", "repair_key TEXT")
    _ensure_column(
        conn,
        "position_authority_repairs",
        "event_type",
        "event_type TEXT NOT NULL DEFAULT 'position_authority_repair'",
    )
    _ensure_column(conn, "position_authority_repairs", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "position_authority_repairs", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "position_authority_repairs", "reason", "reason TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "position_authority_repairs",
        "repair_basis",
        "repair_basis TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "position_authority_repairs", "note", "note TEXT")
    _ensure_column(
        conn,
        "broker_fill_observations",
        "event_type",
        "event_type TEXT NOT NULL DEFAULT 'broker_fill_observation'",
    )
    _ensure_column(conn, "broker_fill_observations", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "broker_fill_observations", "client_order_id", "client_order_id TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "broker_fill_observations", "exchange_order_id", "exchange_order_id TEXT")
    _ensure_column(conn, "broker_fill_observations", "fill_id", "fill_id TEXT")
    _ensure_column(conn, "broker_fill_observations", "fill_ts", "fill_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "broker_fill_observations", "side", "side TEXT NOT NULL DEFAULT 'UNKNOWN'")
    _ensure_column(conn, "broker_fill_observations", "price", "price REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "broker_fill_observations", "qty", "qty REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "broker_fill_observations", "fee", "fee REAL")
    _ensure_column(conn, "broker_fill_observations", "fee_status", "fee_status TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "broker_fill_observations",
        "accounting_status",
        "accounting_status TEXT NOT NULL DEFAULT 'observed'",
    )
    _ensure_column(conn, "broker_fill_observations", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "broker_fill_observations", "parse_warnings", "parse_warnings TEXT")
    _ensure_column(conn, "broker_fill_observations", "raw_payload", "raw_payload TEXT")

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
        CREATE INDEX IF NOT EXISTS idx_manual_flat_accounting_repairs_event_ts
        ON manual_flat_accounting_repairs(event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_manual_flat_accounting_repairs_key
        ON manual_flat_accounting_repairs(repair_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_external_position_adjustments_event_ts
        ON external_position_adjustments(event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_external_position_adjustments_key
        ON external_position_adjustments(adjustment_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fee_gap_accounting_repairs_event_ts
        ON fee_gap_accounting_repairs(event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_broker_fill_observations_client_ts
        ON broker_fill_observations(client_order_id, event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_broker_fill_observations_accounting_status
        ON broker_fill_observations(accounting_status, event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_fee_gap_accounting_repairs_key
        ON fee_gap_accounting_repairs(repair_key)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_fee_pending_accounting_repairs_key
        ON fee_pending_accounting_repairs(repair_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fee_pending_accounting_repairs_fill
        ON fee_pending_accounting_repairs(client_order_id, fill_id, fill_ts)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_position_authority_repairs_event_ts
        ON position_authority_repairs(event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_position_authority_repairs_key
        ON position_authority_repairs(repair_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_position_authority_projection_publications_pair_event_ts
        ON position_authority_projection_publications(pair, event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_position_authority_projection_publications_target
        ON position_authority_projection_publications(pair, target_trade_id, event_ts, id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_position_authority_projection_publications_key
        ON position_authority_projection_publications(publication_key)
        """
    )

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
    _ensure_column(conn, "daily_risk", "baseline_cash_krw", "baseline_cash_krw REAL")
    _ensure_column(conn, "daily_risk", "baseline_asset_qty", "baseline_asset_qty REAL")
    _ensure_column(conn, "daily_risk", "baseline_mark_price", "baseline_mark_price REAL")
    _ensure_column(conn, "daily_risk", "baseline_mark_price_source", "baseline_mark_price_source TEXT")
    _ensure_column(conn, "daily_risk", "baseline_origin", "baseline_origin TEXT")
    _ensure_column(conn, "daily_risk", "baseline_balance_source", "baseline_balance_source TEXT")
    _ensure_column(
        conn,
        "daily_risk",
        "baseline_balance_observed_ts_ms",
        "baseline_balance_observed_ts_ms INTEGER",
    )
    _ensure_column(conn, "daily_risk", "baseline_reconcile_epoch_sec", "baseline_reconcile_epoch_sec REAL")
    _ensure_column(
        conn,
        "daily_risk",
        "baseline_reconcile_reason_code",
        "baseline_reconcile_reason_code TEXT",
    )
    _ensure_column(conn, "daily_risk", "baseline_context", "baseline_context TEXT")
    _ensure_column(conn, "daily_risk", "created_ts_ms", "created_ts_ms INTEGER")

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
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_evaluations_day_eval_ts
        ON risk_evaluations(day_kst, evaluation_ts_ms, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_evaluations_reason_code
        ON risk_evaluations(reason_code, evaluation_ts_ms, id)
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

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_rule_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT NOT NULL,
            fetched_ts INTEGER NOT NULL,
            source_mode TEXT NOT NULL,
            fallback_used INTEGER NOT NULL DEFAULT 0,
            fallback_reason_code TEXT,
            fallback_reason_summary TEXT,
            rule_signature TEXT NOT NULL,
            rules_json TEXT NOT NULL,
            source_json TEXT NOT NULL,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    _ensure_column(conn, "order_rule_snapshots", "market", "market TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "order_rule_snapshots", "fetched_ts", "fetched_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_rule_snapshots", "source_mode", "source_mode TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "order_rule_snapshots", "fallback_used", "fallback_used INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "order_rule_snapshots", "fallback_reason_code", "fallback_reason_code TEXT")
    _ensure_column(conn, "order_rule_snapshots", "fallback_reason_summary", "fallback_reason_summary TEXT")
    _ensure_column(conn, "order_rule_snapshots", "rule_signature", "rule_signature TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "order_rule_snapshots", "rules_json", "rules_json TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(conn, "order_rule_snapshots", "source_json", "source_json TEXT NOT NULL DEFAULT '{}'")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_order_rule_snapshots_market_signature
        ON order_rule_snapshots(market, rule_signature)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_rule_snapshots_market_fetched
        ON order_rule_snapshots(market, fetched_ts DESC, id DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS private_stream_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_name TEXT NOT NULL,
            dedupe_key TEXT NOT NULL UNIQUE,
            event_ts INTEGER NOT NULL,
            client_order_id TEXT,
            exchange_order_id TEXT,
            order_status TEXT,
            fill_id TEXT,
            qty REAL,
            price REAL,
            payload_json TEXT NOT NULL,
            applied INTEGER NOT NULL DEFAULT 0,
            applied_status TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    _ensure_column(conn, "private_stream_events", "stream_name", "stream_name TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "private_stream_events", "dedupe_key", "dedupe_key TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "private_stream_events", "event_ts", "event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "private_stream_events", "client_order_id", "client_order_id TEXT")
    _ensure_column(conn, "private_stream_events", "exchange_order_id", "exchange_order_id TEXT")
    _ensure_column(conn, "private_stream_events", "order_status", "order_status TEXT")
    _ensure_column(conn, "private_stream_events", "fill_id", "fill_id TEXT")
    _ensure_column(conn, "private_stream_events", "qty", "qty REAL")
    _ensure_column(conn, "private_stream_events", "price", "price REAL")
    _ensure_column(conn, "private_stream_events", "payload_json", "payload_json TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(conn, "private_stream_events", "applied", "applied INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "private_stream_events", "applied_status", "applied_status TEXT")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_private_stream_events_dedupe
        ON private_stream_events(dedupe_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_private_stream_events_lookup
        ON private_stream_events(stream_name, event_ts DESC, id DESC)
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
            order_type TEXT,
            price REAL,
            qty_req REAL NOT NULL,
            qty_filled REAL NOT NULL DEFAULT 0,
            strategy_name TEXT,
            entry_decision_id INTEGER,
            exit_decision_id INTEGER,
            decision_reason TEXT,
            exit_rule_name TEXT,
            internal_lot_size REAL,
            effective_min_trade_qty REAL,
            qty_step REAL,
            min_notional_krw REAL,
            intended_lot_count INTEGER,
            executable_lot_count INTEGER,
            final_intended_qty REAL,
            final_submitted_qty REAL,
            decision_reason_code TEXT,
            local_intent_state TEXT,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            last_error TEXT
        )
        """
    )

    _ensure_column(conn, "orders", "submit_attempt_id", "submit_attempt_id TEXT")
    _ensure_column(conn, "orders", "order_type", "order_type TEXT")
    _ensure_column(conn, "orders", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "orders", "entry_decision_id", "entry_decision_id INTEGER")
    _ensure_column(conn, "orders", "exit_decision_id", "exit_decision_id INTEGER")
    _ensure_column(conn, "orders", "decision_reason", "decision_reason TEXT")
    _ensure_column(conn, "orders", "exit_rule_name", "exit_rule_name TEXT")
    _ensure_column(conn, "orders", "internal_lot_size", "internal_lot_size REAL")
    _ensure_column(conn, "orders", "effective_min_trade_qty", "effective_min_trade_qty REAL")
    _ensure_column(conn, "orders", "qty_step", "qty_step REAL")
    _ensure_column(conn, "orders", "min_notional_krw", "min_notional_krw REAL")
    _ensure_column(conn, "orders", "intended_lot_count", "intended_lot_count INTEGER")
    _ensure_column(conn, "orders", "executable_lot_count", "executable_lot_count INTEGER")
    _ensure_column(conn, "orders", "final_intended_qty", "final_intended_qty REAL")
    _ensure_column(conn, "orders", "final_submitted_qty", "final_submitted_qty REAL")
    _ensure_column(conn, "orders", "decision_reason_code", "decision_reason_code TEXT")
    _ensure_column(conn, "orders", "local_intent_state", "local_intent_state TEXT")

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
            intended_lot_count INTEGER,
            executable_lot_count INTEGER,
            internal_lot_size REAL,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )

    _ensure_column(conn, "fills", "fill_id", "fill_id TEXT")
    _ensure_column(conn, "fills", "reference_price", "reference_price REAL")
    _ensure_column(conn, "fills", "slippage_bps", "slippage_bps REAL")
    _ensure_column(conn, "fills", "intended_lot_count", "intended_lot_count INTEGER")
    _ensure_column(conn, "fills", "executable_lot_count", "executable_lot_count INTEGER")
    _ensure_column(conn, "fills", "internal_lot_size", "internal_lot_size REAL")

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
            order_type TEXT,
            submit_attempt_id TEXT,
            mode TEXT,
            intent_ts INTEGER,
            submit_ts INTEGER,
            payload_fingerprint TEXT,
            broker_response_summary TEXT,
            submission_reason_code TEXT,
            exception_class TEXT,
            timeout_flag INTEGER,
            submit_phase TEXT,
            submit_plan_id TEXT,
            signed_request_id TEXT,
            submission_id TEXT,
            confirmation_id TEXT,
            submit_evidence TEXT,
            exchange_order_id_obtained INTEGER,
            internal_lot_size REAL,
            effective_min_trade_qty REAL,
            qty_step REAL,
            min_notional_krw REAL,
            intended_lot_count INTEGER,
            executable_lot_count INTEGER,
            final_intended_qty REAL,
            final_submitted_qty REAL,
            decision_reason_code TEXT,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )

    _ensure_column(conn, "order_events", "symbol", "symbol TEXT")
    _ensure_column(conn, "order_events", "side", "side TEXT")
    _ensure_column(conn, "order_events", "order_type", "order_type TEXT")
    _ensure_column(conn, "order_events", "submit_attempt_id", "submit_attempt_id TEXT")
    _ensure_column(conn, "order_events", "mode", "mode TEXT")
    _ensure_column(conn, "order_events", "intent_ts", "intent_ts INTEGER")
    _ensure_column(conn, "order_events", "submit_ts", "submit_ts INTEGER")
    _ensure_column(conn, "order_events", "payload_fingerprint", "payload_fingerprint TEXT")
    _ensure_column(conn, "order_events", "broker_response_summary", "broker_response_summary TEXT")
    _ensure_column(conn, "order_events", "submission_reason_code", "submission_reason_code TEXT")
    _ensure_column(conn, "order_events", "exception_class", "exception_class TEXT")
    _ensure_column(conn, "order_events", "timeout_flag", "timeout_flag INTEGER")
    _ensure_column(conn, "order_events", "submit_phase", "submit_phase TEXT")
    _ensure_column(conn, "order_events", "submit_plan_id", "submit_plan_id TEXT")
    _ensure_column(conn, "order_events", "signed_request_id", "signed_request_id TEXT")
    _ensure_column(conn, "order_events", "submission_id", "submission_id TEXT")
    _ensure_column(conn, "order_events", "confirmation_id", "confirmation_id TEXT")
    _ensure_column(conn, "order_events", "submit_evidence", "submit_evidence TEXT")
    _ensure_column(conn, "order_events", "exchange_order_id_obtained", "exchange_order_id_obtained INTEGER")
    _ensure_column(conn, "order_events", "internal_lot_size", "internal_lot_size REAL")
    _ensure_column(conn, "order_events", "effective_min_trade_qty", "effective_min_trade_qty REAL")
    _ensure_column(conn, "order_events", "qty_step", "qty_step REAL")
    _ensure_column(conn, "order_events", "min_notional_krw", "min_notional_krw REAL")
    _ensure_column(conn, "order_events", "intended_lot_count", "intended_lot_count INTEGER")
    _ensure_column(conn, "order_events", "executable_lot_count", "executable_lot_count INTEGER")
    _ensure_column(conn, "order_events", "final_intended_qty", "final_intended_qty REAL")
    _ensure_column(conn, "order_events", "final_submitted_qty", "final_submitted_qty REAL")
    _ensure_column(conn, "order_events", "decision_reason_code", "decision_reason_code TEXT")

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
            intended_lot_count INTEGER,
            executable_lot_count INTEGER,
            client_order_id TEXT NOT NULL,
            order_status TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            last_error TEXT
        )
        """
    )

    _ensure_column(conn, "order_intent_dedup", "qty", "qty REAL")
    _ensure_column(conn, "order_intent_dedup", "intended_lot_count", "intended_lot_count INTEGER")
    _ensure_column(conn, "order_intent_dedup", "executable_lot_count", "executable_lot_count INTEGER")
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
            internal_lot_size REAL,
            effective_min_trade_qty REAL,
            qty_step REAL,
            min_notional_krw REAL,
            intended_lot_count INTEGER,
            executable_lot_count INTEGER,
            final_intended_qty REAL,
            final_submitted_qty REAL,
            decision_reason_code TEXT,
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
    _ensure_column(conn, "order_suppressions", "internal_lot_size", "internal_lot_size REAL")
    _ensure_column(conn, "order_suppressions", "effective_min_trade_qty", "effective_min_trade_qty REAL")
    _ensure_column(conn, "order_suppressions", "qty_step", "qty_step REAL")
    _ensure_column(conn, "order_suppressions", "min_notional_krw", "min_notional_krw REAL")
    _ensure_column(conn, "order_suppressions", "intended_lot_count", "intended_lot_count INTEGER")
    _ensure_column(conn, "order_suppressions", "executable_lot_count", "executable_lot_count INTEGER")
    _ensure_column(conn, "order_suppressions", "final_intended_qty", "final_intended_qty REAL")
    _ensure_column(conn, "order_suppressions", "final_submitted_qty", "final_submitted_qty REAL")
    _ensure_column(conn, "order_suppressions", "decision_reason_code", "decision_reason_code TEXT")
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
            executable_lot_count INTEGER NOT NULL DEFAULT 0,
            dust_tracking_lot_count INTEGER NOT NULL DEFAULT 0,
            lot_semantic_version INTEGER,
            internal_lot_size REAL,
            lot_min_qty REAL,
            lot_qty_step REAL,
            lot_min_notional_krw REAL,
            lot_max_qty_decimals INTEGER,
            lot_rule_source_mode TEXT,
            position_semantic_basis TEXT NOT NULL DEFAULT 'lot-native',
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
    _ensure_column(
        conn,
        "open_position_lots",
        "executable_lot_count",
        "executable_lot_count INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        conn,
        "open_position_lots",
        "dust_tracking_lot_count",
        "dust_tracking_lot_count INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(conn, "open_position_lots", "lot_semantic_version", "lot_semantic_version INTEGER")
    _ensure_column(conn, "open_position_lots", "internal_lot_size", "internal_lot_size REAL")
    _ensure_column(conn, "open_position_lots", "lot_min_qty", "lot_min_qty REAL")
    _ensure_column(conn, "open_position_lots", "lot_qty_step", "lot_qty_step REAL")
    _ensure_column(conn, "open_position_lots", "lot_min_notional_krw", "lot_min_notional_krw REAL")
    _ensure_column(conn, "open_position_lots", "lot_max_qty_decimals", "lot_max_qty_decimals INTEGER")
    _ensure_column(conn, "open_position_lots", "lot_rule_source_mode", "lot_rule_source_mode TEXT")
    _ensure_column(
        conn,
        "open_position_lots",
        "position_semantic_basis",
        "position_semantic_basis TEXT NOT NULL DEFAULT 'lot-native'",
    )
    _ensure_column(conn, "open_position_lots", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "open_position_lots", "entry_decision_id", "entry_decision_id INTEGER")
    _ensure_column(conn, "open_position_lots", "entry_decision_linkage", "entry_decision_linkage TEXT")
    conn.execute(
        """
        UPDATE open_position_lots
        SET position_semantic_basis='lot-native'
        WHERE position_semantic_basis IS NULL OR TRIM(position_semantic_basis)=''
        """
    )
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
        UPDATE open_position_lots
        SET
            executable_lot_count = CASE
                WHEN position_state = ? AND qty_open > 1e-12 AND COALESCE(executable_lot_count, 0) <= 0
                    THEN 1
                ELSE COALESCE(executable_lot_count, 0)
            END,
            dust_tracking_lot_count = CASE
                WHEN position_state = ? AND qty_open > 1e-12 AND COALESCE(dust_tracking_lot_count, 0) <= 0
                    THEN 1
                ELSE COALESCE(dust_tracking_lot_count, 0)
            END
        """,
        (
            OPEN_EXPOSURE_LOT_STATE,
            "dust_tracking",
        ),
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
    _ensure_open_position_lot_invariant_triggers(conn)

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
            json.dumps(
                materialize_strategy_decision_context(normalized_context),
                ensure_ascii=False,
                sort_keys=True,
            ),
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


def _manual_flat_accounting_repair_key(
    *,
    event_ts: int,
    asset_qty_delta: float,
    cash_delta: float,
    source: str,
    reason: str,
    repair_basis: str,
    note: str | None,
) -> str:
    payload = {
        "event_type": MANUAL_FLAT_ACCOUNTING_REPAIR_EVENT_TYPE,
        "event_ts": int(event_ts),
        "asset_qty_delta": f"{normalize_asset_qty(asset_qty_delta):.12f}",
        "cash_delta": f"{normalize_cash_amount(cash_delta):.8f}",
        "source": str(source).strip(),
        "reason": str(reason).strip(),
        "repair_basis": str(repair_basis).strip(),
        "note": str(note or "").strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _external_position_adjustment_key(
    *,
    event_ts: int,
    asset_qty_delta: float,
    cash_delta: float,
    source: str,
    reason: str,
    adjustment_basis: str,
    note: str | None,
) -> str:
    payload = {
        "event_type": EXTERNAL_POSITION_ADJUSTMENT_EVENT_TYPE,
        "event_ts": int(event_ts),
        "asset_qty_delta": f"{normalize_asset_qty(asset_qty_delta):.12f}",
        "cash_delta": f"{normalize_cash_amount(cash_delta):.8f}",
        "source": str(source).strip(),
        "reason": str(reason).strip(),
        "adjustment_basis": str(adjustment_basis).strip(),
        "note": str(note or "").strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _fee_gap_accounting_repair_key(
    *,
    source: str,
    reason: str,
    repair_basis: str,
    note: str | None,
) -> str:
    payload = {
        "event_type": FEE_GAP_ACCOUNTING_REPAIR_EVENT_TYPE,
        "source": str(source).strip(),
        "reason": str(reason).strip(),
        "repair_basis": str(repair_basis).strip(),
        "note": str(note or "").strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _fee_pending_accounting_repair_key(
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    source: str,
    reason: str,
    repair_basis: str,
    note: str | None,
) -> str:
    payload = {
        "event_type": FEE_PENDING_ACCOUNTING_REPAIR_EVENT_TYPE,
        "client_order_id": str(client_order_id).strip(),
        "fill_id": str(fill_id or "").strip(),
        "fill_ts": int(fill_ts),
        "price": f"{normalize_cash_amount(price):.8f}",
        "qty": f"{normalize_asset_qty(qty):.12f}",
        "fee": f"{normalize_cash_amount(fee):.8f}",
        "source": str(source).strip(),
        "reason": str(reason).strip(),
        "repair_basis": str(repair_basis).strip(),
        "note": str(note or "").strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _position_authority_repair_key(
    *,
    source: str,
    reason: str,
    repair_basis: str,
    note: str | None,
) -> str:
    payload = {
        "event_type": POSITION_AUTHORITY_REPAIR_EVENT_TYPE,
        "source": str(source).strip(),
        "reason": str(reason).strip(),
        "repair_basis": str(repair_basis).strip(),
        "note": str(note or "").strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def existing_fill_fee_complete(existing_fill: Any | None) -> bool:
    if existing_fill is None:
        return False
    try:
        fee = existing_fill["fee"]
    except (KeyError, IndexError, TypeError):
        fee = None
    if fee is None:
        return False
    try:
        return float(fee) > FEE_ACCOUNTING_COMPLETE_EPS
    except (TypeError, ValueError):
        return False


def load_matching_accounted_fill(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
) -> sqlite3.Row | None:
    client_order_id_text = str(client_order_id or "").strip()
    fill_id_text = str(fill_id or "").strip()
    if fill_id_text:
        row = conn.execute(
            """
            SELECT id, client_order_id, fill_id, fill_ts, price, qty, fee
            FROM fills
            WHERE client_order_id=? AND fill_id=?
            LIMIT 1
            """,
            (client_order_id_text, fill_id_text),
        ).fetchone()
        if row is not None:
            return row
    return conn.execute(
        """
        SELECT id, client_order_id, fill_id, fill_ts, price, qty, fee
        FROM fills
        WHERE client_order_id=? AND fill_ts=? AND ABS(price-?) < 1e-12 AND ABS(qty-?) < 1e-12
        LIMIT 1
        """,
        (client_order_id_text, int(fill_ts), float(price), float(qty)),
    ).fetchone()


def _fill_incident_identity(
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
) -> tuple[str, object, object, object, object]:
    client_order_id_text = str(client_order_id or "").strip()
    fill_id_text = str(fill_id or "").strip()
    if fill_id_text:
        return ("fill_id", client_order_id_text, fill_id_text, None, None)
    return ("fill_terms", client_order_id_text, int(fill_ts), float(price), float(qty))


def _fill_incident_key(identity: tuple[str, object, object, object, object]) -> str:
    if identity[0] == "fill_id":
        return f"client_order_id={identity[1]}|fill_id={identity[2]}"
    return f"client_order_id={identity[1]}|fill_ts={identity[2]}|price={identity[3]}|qty={identity[4]}"


def _matching_fee_pending_repairs(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
) -> list[sqlite3.Row]:
    client_order_id_text = str(client_order_id or "").strip()
    fill_id_text = str(fill_id or "").strip()
    if fill_id_text:
        rows = conn.execute(
            """
            SELECT id, repair_key, event_ts, client_order_id, exchange_order_id,
                   fill_id, fill_ts, price, qty, fee, source, reason
            FROM fee_pending_accounting_repairs
            WHERE client_order_id=? AND fill_id=?
            ORDER BY event_ts ASC, id ASC
            """,
            (client_order_id_text, fill_id_text),
        ).fetchall()
        if rows:
            return list(rows)
    return list(
        conn.execute(
            """
            SELECT id, repair_key, event_ts, client_order_id, exchange_order_id,
                   fill_id, fill_ts, price, qty, fee, source, reason
            FROM fee_pending_accounting_repairs
            WHERE client_order_id=?
              AND fill_ts=?
              AND ABS(price-?) < 1e-12
              AND ABS(qty-?) < 1e-12
            ORDER BY event_ts ASC, id ASC
            """,
            (client_order_id_text, int(fill_ts), float(price), float(qty)),
        ).fetchall()
    )


def build_fill_accounting_incident_projection(conn: sqlite3.Connection) -> list[FillAccountingIncidentVerdict]:
    observations = conn.execute(
        """
        SELECT id, event_ts, client_order_id, exchange_order_id, fill_id, fill_ts,
               side, price, qty, fee, fee_status, accounting_status, source,
               parse_warnings
        FROM broker_fill_observations
        ORDER BY event_ts ASC, id ASC
        """
    ).fetchall()
    grouped: dict[tuple[str, object, object, object, object], list[sqlite3.Row]] = {}
    for row in observations:
        identity = _fill_incident_identity(
            client_order_id=str(row["client_order_id"]),
            fill_id=(str(row["fill_id"]) if row["fill_id"] is not None else None),
            fill_ts=int(row["fill_ts"] or 0),
            price=float(row["price"] or 0.0),
            qty=float(row["qty"] or 0.0),
        )
        grouped.setdefault(identity, []).append(row)

    verdicts: list[FillAccountingIncidentVerdict] = []
    for identity, rows in grouped.items():
        latest = rows[-1]
        fill_id = str(latest["fill_id"]) if latest["fill_id"] is not None and str(latest["fill_id"]).strip() else None
        fill_ts = int(latest["fill_ts"] or 0)
        price = float(latest["price"] or 0.0)
        qty = float(latest["qty"] or 0.0)
        client_order_id = str(latest["client_order_id"])
        existing_fill = load_matching_accounted_fill(
            conn,
            client_order_id=client_order_id,
            fill_id=fill_id,
            fill_ts=fill_ts,
            price=price,
            qty=qty,
        )
        final_fee_applied = existing_fill_fee_complete(existing_fill)
        repairs = _matching_fee_pending_repairs(
            conn,
            client_order_id=client_order_id,
            fill_id=fill_id,
            fill_ts=fill_ts,
            price=price,
            qty=qty,
        )
        repair_present = bool(repairs)
        latest_accounting_status = str(latest["accounting_status"] or "").strip()
        latest_fee_status = str(latest["fee_status"] or "").strip()
        fee_pending_count = sum(1 for row in rows if str(row["accounting_status"] or "") == "fee_pending")
        accounting_complete_count = sum(
            1 for row in rows if str(row["accounting_status"] or "") == "accounting_complete"
        )

        if latest_accounting_status == "fee_pending" and not final_fee_applied and not repair_present:
            canonical_state = "active_fee_pending"
            incident_scope = "active_blocking"
            active_issue = True
        elif latest_accounting_status == "fee_pending" and final_fee_applied:
            canonical_state = "already_accounted_observation_stale"
            incident_scope = "historical_context"
            active_issue = False
        elif latest_accounting_status == "fee_pending" and repair_present:
            canonical_state = "repaired" if final_fee_applied else "ambiguous"
            incident_scope = "historical_context" if final_fee_applied else "active_blocking"
            active_issue = not final_fee_applied
        elif latest_accounting_status == "accounting_complete" and repair_present:
            canonical_state = "repaired"
            incident_scope = "historical_context"
            active_issue = False
        elif latest_accounting_status == "accounting_complete":
            canonical_state = "none"
            incident_scope = "historical_context"
            active_issue = False
        else:
            canonical_state = "none"
            incident_scope = "historical_context"
            active_issue = False

        latest_repair = repairs[-1] if repairs else None
        verdicts.append(
            FillAccountingIncidentVerdict(
                fill_key=_fill_incident_key(identity),
                client_order_id=client_order_id,
                fill_id=fill_id,
                fill_ts=fill_ts,
                price=price,
                qty=qty,
                authoritative_fill_present=existing_fill is not None,
                final_fee_applied=final_fee_applied,
                authoritative_fill_row_id=(int(existing_fill["id"]) if existing_fill is not None else None),
                authoritative_fill_fee=(
                    float(existing_fill["fee"]) if existing_fill is not None and existing_fill["fee"] is not None else None
                ),
                latest_observation_id=int(latest["id"]),
                latest_observation_event_ts=int(latest["event_ts"] or 0),
                latest_observation_fee_status=latest_fee_status or None,
                latest_observation_accounting_status=latest_accounting_status or None,
                latest_observation_source=str(latest["source"] or "").strip() or None,
                repair_present=repair_present,
                repair_count=len(repairs),
                latest_repair_id=(int(latest_repair["id"]) if latest_repair is not None else None),
                canonical_incident_state=canonical_state,
                incident_scope=incident_scope,
                active_issue=active_issue,
                raw_observation_count=len(rows),
                fee_pending_observation_count=fee_pending_count,
                accounting_complete_observation_count=accounting_complete_count,
                evidence={
                    "matching_logic": "fill_id_or_client_order_id_fill_ts_price_qty",
                    "latest_observation_status": latest_accounting_status,
                    "raw_observation_ids": [int(row["id"]) for row in rows],
                    "fee_pending_observation_ids": [
                        int(row["id"]) for row in rows if str(row["accounting_status"] or "") == "fee_pending"
                    ],
                    "accounting_complete_observation_ids": [
                        int(row["id"])
                        for row in rows
                        if str(row["accounting_status"] or "") == "accounting_complete"
                    ],
                },
            )
        )
    return verdicts


def summarize_fill_accounting_incident_projection(conn: sqlite3.Connection) -> dict[str, object]:
    verdicts = build_fill_accounting_incident_projection(conn)
    active = [v for v in verdicts if v.active_issue]
    stale = [v for v in verdicts if v.canonical_incident_state == "already_accounted_observation_stale"]
    repaired = [v for v in verdicts if v.canonical_incident_state == "repaired"]
    complete = [
        v for v in verdicts if v.latest_observation_accounting_status == "accounting_complete" and not v.active_issue
    ]
    return {
        "projection_kind": "fill_accounting_incident_projection",
        "incident_count": len(verdicts),
        "active_fee_pending_count": sum(1 for v in active if v.canonical_incident_state == "active_fee_pending"),
        "active_issue_count": len(active),
        "already_accounted_observation_stale_count": len(stale),
        "repaired_count": len(repaired),
        "latest_accounting_complete_count": len(complete),
        "verdicts": [v.as_dict() for v in verdicts],
    }


def compute_accounting_replay(conn: sqlite3.Connection) -> dict[str, object]:
    init_portfolio(conn)
    total_fee = 0.0
    external_cash_adjustment_total = 0.0
    external_cash_adjustment_count = 0
    manual_flat_repair_cash_total = 0.0
    manual_flat_repair_asset_total = 0.0
    manual_flat_repair_count = 0
    external_position_adjustment_cash_total = 0.0
    external_position_adjustment_asset_total = 0.0
    external_position_adjustment_count = 0
    fee_gap_repair_count = 0
    broker_fill_observation_count = 0
    broker_fill_fee_pending_count = 0
    broker_fill_accounting_complete_count = 0
    broker_fill_fee_candidate_order_level_count = 0
    broker_fill_missing_fee_count = 0
    broker_fill_zero_reported_fee_count = 0
    broker_fill_invalid_fee_count = 0
    broker_fill_latest_unresolved_fee_pending_count = 0
    broker_fill_latest_accounting_complete_count = 0
    fee_pending_repair_count = 0
    position_authority_repair_count = 0
    dup_fill_count = 0

    seen_fill_keys: set[tuple[str, int, float, float]] = set()
    fills = conn.execute(
        """
        SELECT f.client_order_id, f.fill_ts, f.price, f.qty, f.fee, o.side
        FROM fills f
        JOIN orders o ON o.client_order_id = f.client_order_id
        ORDER BY f.fill_ts ASC, f.id ASC
        """
    ).fetchall()

    fill_rows: list[tuple[str, float, float, float | None]] = []
    for row in fills:
        key = (
            str(row["client_order_id"]),
            int(row["fill_ts"]),
            float(row["price"]),
            float(row["qty"]),
        )
        if key in seen_fill_keys:
            dup_fill_count += 1
        seen_fill_keys.add(key)

        fee = float(row["fee"]) if row["fee"] is not None else None
        total_fee += float(fee or 0.0)
        fill_rows.append((str(row["side"]), float(row["price"]), float(row["qty"]), fee))

    cash_available, cash_locked, asset_available, asset_locked, cash, qty = replay_fill_portfolio_snapshot(
        cash_available=settings.START_CASH_KRW,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
        rows=fill_rows,
    )

    adjustments = conn.execute(
        """
        SELECT adjustment_key, delta_amount
        FROM external_cash_adjustments
        ORDER BY event_ts ASC, id ASC
        """
    ).fetchall()
    seen_adjustment_keys: set[str] = set()
    for row in adjustments:
        key = str(row["adjustment_key"])
        if key in seen_adjustment_keys:
            continue
        seen_adjustment_keys.add(key)
        delta_amount = normalize_cash_amount(row["delta_amount"])
        external_cash_adjustment_total = normalize_cash_amount(external_cash_adjustment_total + delta_amount)
        external_cash_adjustment_count += 1
        cash = normalize_cash_amount(cash + delta_amount)
        cash_available = normalize_cash_amount(cash_available + delta_amount)

    repairs = conn.execute(
        """
        SELECT repair_key, cash_delta, asset_qty_delta
        FROM manual_flat_accounting_repairs
        ORDER BY event_ts ASC, id ASC
        """
    ).fetchall()
    seen_repair_keys: set[str] = set()
    for row in repairs:
        key = str(row["repair_key"])
        if key in seen_repair_keys:
            continue
        seen_repair_keys.add(key)
        cash_delta = normalize_cash_amount(row["cash_delta"])
        asset_qty_delta = normalize_asset_qty(row["asset_qty_delta"])
        manual_flat_repair_cash_total = normalize_cash_amount(manual_flat_repair_cash_total + cash_delta)
        manual_flat_repair_asset_total = normalize_asset_qty(manual_flat_repair_asset_total + asset_qty_delta)
        manual_flat_repair_count += 1
        cash = normalize_cash_amount(cash + cash_delta)
        cash_available = normalize_cash_amount(cash_available + cash_delta)
        qty = normalize_asset_qty(qty + asset_qty_delta)
        asset_available = normalize_asset_qty(asset_available + asset_qty_delta)

    position_adjustments = conn.execute(
        """
        SELECT adjustment_key, cash_delta, asset_qty_delta
        FROM external_position_adjustments
        ORDER BY event_ts ASC, id ASC
        """
    ).fetchall()
    seen_position_adjustment_keys: set[str] = set()
    for row in position_adjustments:
        key = str(row["adjustment_key"])
        if key in seen_position_adjustment_keys:
            continue
        seen_position_adjustment_keys.add(key)
        cash_delta = normalize_cash_amount(row["cash_delta"])
        asset_qty_delta = normalize_asset_qty(row["asset_qty_delta"])
        external_position_adjustment_cash_total = normalize_cash_amount(
            external_position_adjustment_cash_total + cash_delta
        )
        external_position_adjustment_asset_total = normalize_asset_qty(
            external_position_adjustment_asset_total + asset_qty_delta
        )
        external_position_adjustment_count += 1
        cash = normalize_cash_amount(cash + cash_delta)
        cash_available = normalize_cash_amount(cash_available + cash_delta)
        qty = normalize_asset_qty(qty + asset_qty_delta)
        asset_available = normalize_asset_qty(asset_available + asset_qty_delta)

    fee_gap_repairs = conn.execute(
        """
        SELECT repair_key
        FROM fee_gap_accounting_repairs
        ORDER BY event_ts ASC, id ASC
        """
    ).fetchall()
    seen_fee_gap_repair_keys: set[str] = set()
    for row in fee_gap_repairs:
        key = str(row["repair_key"])
        if key in seen_fee_gap_repair_keys:
            continue
        seen_fee_gap_repair_keys.add(key)
        fee_gap_repair_count += 1

    observation_summary = conn.execute(
        """
        SELECT
            COUNT(*) AS observation_count,
            COALESCE(SUM(CASE WHEN accounting_status='fee_pending' THEN 1 ELSE 0 END), 0) AS fee_pending_count,
            COALESCE(SUM(CASE WHEN accounting_status='accounting_complete' THEN 1 ELSE 0 END), 0) AS accounting_complete_count,
            COALESCE(SUM(CASE WHEN fee_status='order_level_candidate' THEN 1 ELSE 0 END), 0) AS fee_candidate_order_level_count,
            COALESCE(SUM(CASE WHEN fee_status='missing' THEN 1 ELSE 0 END), 0) AS missing_fee_count,
            COALESCE(SUM(CASE WHEN fee_status='zero_reported' THEN 1 ELSE 0 END), 0) AS zero_reported_fee_count,
            COALESCE(SUM(CASE WHEN fee_status IN ('empty', 'invalid', 'unparseable') THEN 1 ELSE 0 END), 0) AS invalid_fee_count
        FROM broker_fill_observations
        """
    ).fetchone()
    if observation_summary is not None:
        broker_fill_observation_count = int(observation_summary["observation_count"] or 0)
        broker_fill_fee_pending_count = int(observation_summary["fee_pending_count"] or 0)
        broker_fill_accounting_complete_count = int(observation_summary["accounting_complete_count"] or 0)
        broker_fill_fee_candidate_order_level_count = int(observation_summary["fee_candidate_order_level_count"] or 0)
        broker_fill_missing_fee_count = int(observation_summary["missing_fee_count"] or 0)
        broker_fill_zero_reported_fee_count = int(observation_summary["zero_reported_fee_count"] or 0)
        broker_fill_invalid_fee_count = int(observation_summary["invalid_fee_count"] or 0)

    incident_projection = summarize_fill_accounting_incident_projection(conn)
    broker_fill_latest_unresolved_fee_pending_count = int(incident_projection["active_issue_count"])
    broker_fill_latest_accounting_complete_count = int(incident_projection["latest_accounting_complete_count"])

    fee_pending_repair_summary = conn.execute(
        "SELECT COUNT(*) AS repair_count FROM fee_pending_accounting_repairs"
    ).fetchone()
    if fee_pending_repair_summary is not None:
        fee_pending_repair_count = int(fee_pending_repair_summary["repair_count"] or 0)

    position_authority_repair_summary = conn.execute(
        "SELECT COUNT(*) AS repair_count FROM position_authority_repairs"
    ).fetchone()
    if position_authority_repair_summary is not None:
        position_authority_repair_count = int(position_authority_repair_summary["repair_count"] or 0)

    return {
        "replay_cash": cash,
        "replay_qty": qty,
        "replay_cash_available": cash_available,
        "replay_cash_locked": cash_locked,
        "replay_asset_available": asset_available,
        "replay_asset_locked": asset_locked,
        "fee_total": total_fee,
        "external_cash_adjustment_count": external_cash_adjustment_count,
        "external_cash_adjustment_total": external_cash_adjustment_total,
        "manual_flat_accounting_repair_count": manual_flat_repair_count,
        "manual_flat_accounting_repair_cash_total": manual_flat_repair_cash_total,
        "manual_flat_accounting_repair_asset_total": manual_flat_repair_asset_total,
        "external_position_adjustment_count": external_position_adjustment_count,
        "external_position_adjustment_cash_total": external_position_adjustment_cash_total,
        "external_position_adjustment_asset_total": external_position_adjustment_asset_total,
        "fee_gap_accounting_repair_count": fee_gap_repair_count,
        "broker_fill_observation_count": broker_fill_observation_count,
        "broker_fill_fee_pending_count": broker_fill_fee_pending_count,
        "broker_fill_accounting_complete_count": broker_fill_accounting_complete_count,
        "broker_fill_fee_candidate_order_level_count": broker_fill_fee_candidate_order_level_count,
        "broker_fill_missing_fee_count": broker_fill_missing_fee_count,
        "broker_fill_zero_reported_fee_count": broker_fill_zero_reported_fee_count,
        "broker_fill_invalid_fee_count": broker_fill_invalid_fee_count,
        "broker_fill_latest_unresolved_fee_pending_count": broker_fill_latest_unresolved_fee_pending_count,
        "broker_fill_latest_accounting_complete_count": broker_fill_latest_accounting_complete_count,
        "unresolved_fee_state": broker_fill_latest_unresolved_fee_pending_count > 0,
        "fill_accounting_incident_projection": incident_projection,
        "fill_accounting_active_issue_count": int(incident_projection["active_issue_count"]),
        "fill_accounting_already_accounted_observation_stale_count": int(
            incident_projection["already_accounted_observation_stale_count"]
        ),
        "fill_accounting_repaired_incident_count": int(incident_projection["repaired_count"]),
        "fee_pending_accounting_repair_count": fee_pending_repair_count,
        "position_authority_repair_count": position_authority_repair_count,
        "dup_fill_count": dup_fill_count,
        "projection_model": ACCOUNTING_PROJECTION_MODEL,
        "projection_kind": "authoritative_accounting_projection",
        "included_event_families": AUTHORITATIVE_ACCOUNTING_EVENT_FAMILIES,
        "diagnostic_event_families": DIAGNOSTIC_ACCOUNTING_EVENT_FAMILIES,
        "omitted_event_families": DIAGNOSTIC_ACCOUNTING_EVENT_FAMILIES,
    }


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


def record_manual_flat_accounting_repair(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    asset_qty_delta: float,
    cash_delta: float,
    source: str,
    reason: str,
    repair_basis: dict[str, Any] | str,
    note: str | None = None,
    repair_key: str | None = None,
) -> dict[str, Any]:
    basis_text = (
        json.dumps(repair_basis, ensure_ascii=False, sort_keys=True)
        if isinstance(repair_basis, dict)
        else str(repair_basis)
    )
    source_text = str(source).strip()
    reason_text = str(reason).strip()
    asset_qty_delta_value = normalize_asset_qty(asset_qty_delta)
    cash_delta_value = normalize_cash_amount(cash_delta)
    if abs(asset_qty_delta_value) <= 1e-12 and abs(cash_delta_value) <= 1e-8:
        raise RuntimeError("manual-flat accounting repair delta is zero")

    key = repair_key or _manual_flat_accounting_repair_key(
        event_ts=int(event_ts),
        asset_qty_delta=asset_qty_delta_value,
        cash_delta=cash_delta_value,
        source=source_text,
        reason=reason_text,
        repair_basis=basis_text,
        note=note,
    )

    existing = conn.execute(
        """
        SELECT id, repair_key, event_ts, asset_qty_delta, cash_delta, source, reason, repair_basis, note
        FROM manual_flat_accounting_repairs
        WHERE repair_key=?
        """,
        (key,),
    ).fetchone()
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "repair_key": str(existing["repair_key"]),
            "event_ts": int(existing["event_ts"]),
            "asset_qty_delta": float(existing["asset_qty_delta"]),
            "cash_delta": float(existing["cash_delta"]),
            "source": str(existing["source"]),
            "reason": str(existing["reason"]),
            "repair_basis": str(existing["repair_basis"]),
            "note": str(existing["note"]) if existing["note"] is not None else None,
            "created": False,
        }

    had_tx = conn.in_transaction
    cursor = conn.execute(
        """
        INSERT INTO manual_flat_accounting_repairs(
            repair_key, event_type, event_ts, asset_qty_delta, cash_delta, source, reason, repair_basis, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            MANUAL_FLAT_ACCOUNTING_REPAIR_EVENT_TYPE,
            int(event_ts),
            asset_qty_delta_value,
            cash_delta_value,
            source_text,
            reason_text,
            basis_text,
            note,
        ),
    )
    if not had_tx:
        conn.commit()

    return {
        "id": int(cursor.lastrowid),
        "repair_key": key,
        "event_ts": int(event_ts),
        "asset_qty_delta": asset_qty_delta_value,
        "cash_delta": cash_delta_value,
        "source": source_text,
        "reason": reason_text,
        "repair_basis": basis_text,
        "note": note,
        "created": True,
    }


def record_external_position_adjustment(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    asset_qty_delta: float,
    cash_delta: float,
    source: str,
    reason: str,
    adjustment_basis: dict[str, Any] | str,
    note: str | None = None,
    adjustment_key: str | None = None,
) -> dict[str, Any]:
    basis_text = (
        json.dumps(adjustment_basis, ensure_ascii=False, sort_keys=True)
        if isinstance(adjustment_basis, dict)
        else str(adjustment_basis)
    )
    source_text = str(source).strip()
    reason_text = str(reason).strip()
    asset_qty_delta_value = normalize_asset_qty(asset_qty_delta)
    cash_delta_value = normalize_cash_amount(cash_delta)
    if abs(asset_qty_delta_value) <= 1e-12 and abs(cash_delta_value) <= 1e-8:
        raise RuntimeError("external position adjustment delta is zero")

    key = adjustment_key or _external_position_adjustment_key(
        event_ts=int(event_ts),
        asset_qty_delta=asset_qty_delta_value,
        cash_delta=cash_delta_value,
        source=source_text,
        reason=reason_text,
        adjustment_basis=basis_text,
        note=note,
    )

    existing = conn.execute(
        """
        SELECT id, adjustment_key, event_ts, asset_qty_delta, cash_delta, source, reason, adjustment_basis, note
        FROM external_position_adjustments
        WHERE adjustment_key=?
        """,
        (key,),
    ).fetchone()
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "adjustment_key": str(existing["adjustment_key"]),
            "event_ts": int(existing["event_ts"]),
            "asset_qty_delta": float(existing["asset_qty_delta"]),
            "cash_delta": float(existing["cash_delta"]),
            "source": str(existing["source"]),
            "reason": str(existing["reason"]),
            "adjustment_basis": str(existing["adjustment_basis"]),
            "note": str(existing["note"]) if existing["note"] is not None else None,
            "created": False,
        }

    had_tx = conn.in_transaction
    cursor = conn.execute(
        """
        INSERT INTO external_position_adjustments(
            adjustment_key, event_type, event_ts, asset_qty_delta, cash_delta, source, reason, adjustment_basis, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            EXTERNAL_POSITION_ADJUSTMENT_EVENT_TYPE,
            int(event_ts),
            asset_qty_delta_value,
            cash_delta_value,
            source_text,
            reason_text,
            basis_text,
            note,
        ),
    )
    if not had_tx:
        conn.commit()

    return {
        "id": int(cursor.lastrowid),
        "adjustment_key": key,
        "event_ts": int(event_ts),
        "asset_qty_delta": asset_qty_delta_value,
        "cash_delta": cash_delta_value,
        "source": source_text,
        "reason": reason_text,
        "adjustment_basis": basis_text,
        "note": note,
        "created": True,
    }


def get_external_position_adjustment_summary(conn: sqlite3.Connection) -> dict[str, float | int | str | None]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS adjustment_count,
            COALESCE(SUM(asset_qty_delta), 0.0) AS asset_qty_total,
            COALESCE(SUM(cash_delta), 0.0) AS cash_total
        FROM external_position_adjustments
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT adjustment_key, event_ts, asset_qty_delta, cash_delta, source, reason, adjustment_basis, note
        FROM external_position_adjustments
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "adjustment_count": int(row["adjustment_count"] if row else 0),
        "asset_qty_total": float(row["asset_qty_total"] if row else 0.0),
        "cash_total": float(row["cash_total"] if row else 0.0),
        "last_event_ts": int(last["event_ts"]) if last is not None else None,
        "last_adjustment_key": str(last["adjustment_key"]) if last is not None else None,
        "last_asset_qty_delta": float(last["asset_qty_delta"]) if last is not None else None,
        "last_cash_delta": float(last["cash_delta"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_reason": str(last["reason"]) if last is not None else None,
        "last_adjustment_basis": str(last["adjustment_basis"]) if last is not None else None,
        "last_note": str(last["note"]) if last is not None and last["note"] is not None else None,
    }


def get_manual_flat_accounting_repair_summary(conn: sqlite3.Connection) -> dict[str, float | int | str | None]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS repair_count,
            COALESCE(SUM(asset_qty_delta), 0.0) AS asset_qty_total,
            COALESCE(SUM(cash_delta), 0.0) AS cash_total
        FROM manual_flat_accounting_repairs
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT repair_key, event_ts, asset_qty_delta, cash_delta, source, reason, repair_basis, note
        FROM manual_flat_accounting_repairs
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "repair_count": int(row["repair_count"] if row else 0),
        "asset_qty_total": float(row["asset_qty_total"] if row else 0.0),
        "cash_total": float(row["cash_total"] if row else 0.0),
        "last_event_ts": int(last["event_ts"]) if last is not None else None,
        "last_repair_key": str(last["repair_key"]) if last is not None else None,
        "last_asset_qty_delta": float(last["asset_qty_delta"]) if last is not None else None,
        "last_cash_delta": float(last["cash_delta"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_reason": str(last["reason"]) if last is not None else None,
        "last_repair_basis": str(last["repair_basis"]) if last is not None else None,
        "last_note": str(last["note"]) if last is not None and last["note"] is not None else None,
    }


def record_broker_fill_observation(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    client_order_id: str,
    exchange_order_id: str | None,
    fill_id: str | None,
    fill_ts: int,
    side: str,
    price: float,
    qty: float,
    fee: float | None,
    fee_status: str,
    accounting_status: str,
    source: str,
    parse_warnings: Iterable[str] | str | None = None,
    raw_payload: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    client_order_id_text = str(client_order_id or "").strip()
    if not client_order_id_text:
        raise RuntimeError("broker fill observation client_order_id is required")
    side_text = str(side or "").strip().upper()
    if side_text not in {"BUY", "SELL"}:
        raise RuntimeError(f"broker fill observation side is invalid: {side}")
    fee_status_text = str(fee_status or "").strip() or "unknown"
    accounting_status_text = str(accounting_status or "").strip() or "observed"
    source_text = str(source or "").strip() or "unknown"
    warnings_text: str | None
    if parse_warnings is None:
        warnings_text = None
    elif isinstance(parse_warnings, str):
        warnings_text = parse_warnings
    else:
        warnings_text = json.dumps([str(item) for item in parse_warnings], ensure_ascii=False, sort_keys=True)
    raw_payload_text: str | None
    if raw_payload is None:
        raw_payload_text = None
    elif isinstance(raw_payload, str):
        raw_payload_text = raw_payload
    else:
        raw_payload_text = json.dumps(raw_payload, ensure_ascii=False, sort_keys=True)

    had_tx = conn.in_transaction
    cursor = conn.execute(
        """
        INSERT INTO broker_fill_observations(
            event_type, event_ts, client_order_id, exchange_order_id, fill_id,
            fill_ts, side, price, qty, fee, fee_status, accounting_status,
            source, parse_warnings, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            BROKER_FILL_OBSERVATION_EVENT_TYPE,
            int(event_ts),
            client_order_id_text,
            str(exchange_order_id or "").strip() or None,
            str(fill_id or "").strip() or None,
            int(fill_ts),
            side_text,
            float(price),
            float(qty),
            (float(fee) if fee is not None else None),
            fee_status_text,
            accounting_status_text,
            source_text,
            warnings_text,
            raw_payload_text,
        ),
    )
    if not had_tx:
        conn.commit()
    return {
        "id": int(cursor.lastrowid),
        "event_ts": int(event_ts),
        "client_order_id": client_order_id_text,
        "exchange_order_id": str(exchange_order_id or "").strip() or None,
        "fill_id": str(fill_id or "").strip() or None,
        "fee_status": fee_status_text,
        "accounting_status": accounting_status_text,
        "source": source_text,
    }


def get_broker_fill_observation_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS observation_count,
            COALESCE(SUM(CASE WHEN accounting_status='fee_pending' THEN 1 ELSE 0 END), 0) AS fee_pending_count,
            COALESCE(SUM(CASE WHEN accounting_status='accounting_complete' THEN 1 ELSE 0 END), 0) AS accounting_complete_count,
            COALESCE(SUM(CASE WHEN fee_status='order_level_candidate' THEN 1 ELSE 0 END), 0) AS fee_candidate_order_level_count,
            COALESCE(SUM(CASE WHEN fee_status='missing' THEN 1 ELSE 0 END), 0) AS missing_fee_count,
            COALESCE(SUM(CASE WHEN fee_status='zero_reported' THEN 1 ELSE 0 END), 0) AS zero_reported_fee_count,
            COALESCE(SUM(CASE WHEN fee_status IN ('empty', 'invalid', 'unparseable') THEN 1 ELSE 0 END), 0) AS invalid_fee_count
        FROM broker_fill_observations
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT event_ts, client_order_id, exchange_order_id, fill_id, side, price, qty,
               fee, fee_status, accounting_status, source, parse_warnings
        FROM broker_fill_observations
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "observation_count": int(row["observation_count"] if row else 0),
        "fee_pending_count": int(row["fee_pending_count"] if row else 0),
        "accounting_complete_count": int(row["accounting_complete_count"] if row else 0),
        "fee_candidate_order_level_count": int(row["fee_candidate_order_level_count"] if row else 0),
        "missing_fee_count": int(row["missing_fee_count"] if row else 0),
        "zero_reported_fee_count": int(row["zero_reported_fee_count"] if row else 0),
        "invalid_fee_count": int(row["invalid_fee_count"] if row else 0),
        "last_event_ts": int(last["event_ts"]) if last is not None else None,
        "last_client_order_id": str(last["client_order_id"]) if last is not None else None,
        "last_exchange_order_id": str(last["exchange_order_id"]) if last is not None and last["exchange_order_id"] is not None else None,
        "last_fill_id": str(last["fill_id"]) if last is not None and last["fill_id"] is not None else None,
        "last_side": str(last["side"]) if last is not None else None,
        "last_price": float(last["price"]) if last is not None else None,
        "last_qty": float(last["qty"]) if last is not None else None,
        "last_fee": float(last["fee"]) if last is not None and last["fee"] is not None else None,
        "last_fee_status": str(last["fee_status"]) if last is not None else None,
        "last_accounting_status": str(last["accounting_status"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_parse_warnings": str(last["parse_warnings"]) if last is not None and last["parse_warnings"] is not None else None,
    }


def record_fee_gap_accounting_repair(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    source: str,
    reason: str,
    repair_basis: dict[str, Any] | str,
    note: str | None = None,
    repair_key: str | None = None,
) -> dict[str, Any]:
    basis_text = (
        json.dumps(repair_basis, ensure_ascii=False, sort_keys=True)
        if isinstance(repair_basis, dict)
        else str(repair_basis)
    )
    source_text = str(source).strip()
    reason_text = str(reason).strip()
    if not source_text:
        raise RuntimeError("fee-gap accounting repair source is required")
    if not reason_text:
        raise RuntimeError("fee-gap accounting repair reason is required")
    if not basis_text.strip():
        raise RuntimeError("fee-gap accounting repair basis is required")

    key = repair_key or _fee_gap_accounting_repair_key(
        source=source_text,
        reason=reason_text,
        repair_basis=basis_text,
        note=note,
    )
    existing = conn.execute(
        """
        SELECT id, repair_key, event_ts, source, reason, repair_basis, note
        FROM fee_gap_accounting_repairs
        WHERE repair_key=?
        """,
        (key,),
    ).fetchone()
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "repair_key": str(existing["repair_key"]),
            "event_ts": int(existing["event_ts"]),
            "source": str(existing["source"]),
            "reason": str(existing["reason"]),
            "repair_basis": str(existing["repair_basis"]),
            "note": str(existing["note"]) if existing["note"] is not None else None,
            "created": False,
        }

    had_tx = conn.in_transaction
    cursor = conn.execute(
        """
        INSERT INTO fee_gap_accounting_repairs(
            repair_key, event_type, event_ts, source, reason, repair_basis, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            FEE_GAP_ACCOUNTING_REPAIR_EVENT_TYPE,
            int(event_ts),
            source_text,
            reason_text,
            basis_text,
            note,
        ),
    )
    if not had_tx:
        conn.commit()

    return {
        "id": int(cursor.lastrowid),
        "repair_key": key,
        "event_ts": int(event_ts),
        "source": source_text,
        "reason": reason_text,
        "repair_basis": basis_text,
        "note": note,
        "created": True,
    }


def get_fee_gap_accounting_repair_summary(conn: sqlite3.Connection) -> dict[str, float | int | str | None]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS repair_count
        FROM fee_gap_accounting_repairs
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT repair_key, event_ts, source, reason, repair_basis, note
        FROM fee_gap_accounting_repairs
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "repair_count": int(row["repair_count"] if row else 0),
        "last_event_ts": int(last["event_ts"]) if last is not None else None,
        "last_repair_key": str(last["repair_key"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_reason": str(last["reason"]) if last is not None else None,
        "last_repair_basis": str(last["repair_basis"]) if last is not None else None,
        "last_note": str(last["note"]) if last is not None and last["note"] is not None else None,
    }


def record_position_authority_repair(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    source: str,
    reason: str,
    repair_basis: dict[str, Any] | str,
    note: str | None = None,
    repair_key: str | None = None,
) -> dict[str, Any]:
    basis_text = (
        json.dumps(repair_basis, ensure_ascii=False, sort_keys=True)
        if isinstance(repair_basis, dict)
        else str(repair_basis)
    )
    source_text = str(source).strip()
    reason_text = str(reason).strip()
    if not source_text:
        raise RuntimeError("position authority repair source is required")
    if not reason_text:
        raise RuntimeError("position authority repair reason is required")
    if not basis_text.strip():
        raise RuntimeError("position authority repair basis is required")

    key = repair_key or _position_authority_repair_key(
        source=source_text,
        reason=reason_text,
        repair_basis=basis_text,
        note=note,
    )
    existing = conn.execute(
        """
        SELECT id, repair_key, event_ts, source, reason, repair_basis, note
        FROM position_authority_repairs
        WHERE repair_key=?
        """,
        (key,),
    ).fetchone()
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "repair_key": str(existing["repair_key"]),
            "event_ts": int(existing["event_ts"]),
            "source": str(existing["source"]),
            "reason": str(existing["reason"]),
            "repair_basis": str(existing["repair_basis"]),
            "note": str(existing["note"]) if existing["note"] is not None else None,
            "created": False,
        }

    had_tx = conn.in_transaction
    cursor = conn.execute(
        """
        INSERT INTO position_authority_repairs(
            repair_key, event_type, event_ts, source, reason, repair_basis, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            POSITION_AUTHORITY_REPAIR_EVENT_TYPE,
            int(event_ts),
            source_text,
            reason_text,
            basis_text,
            note,
        ),
    )
    if not had_tx:
        conn.commit()

    return {
        "id": int(cursor.lastrowid),
        "repair_key": key,
        "event_ts": int(event_ts),
        "source": source_text,
        "reason": reason_text,
        "repair_basis": basis_text,
        "note": note,
        "created": True,
    }


def _position_authority_projection_publication_key(
    *,
    pair: str,
    target_trade_id: int,
    source: str,
    publish_basis: str,
    note: str | None,
) -> str:
    hasher = hashlib.sha256()
    hasher.update(str(pair).strip().encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(str(int(target_trade_id)).encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(str(source).strip().encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update(str(publish_basis).strip().encode("utf-8"))
    hasher.update(b"\x1f")
    hasher.update((note or "").strip().encode("utf-8"))
    return hasher.hexdigest()


def record_position_authority_projection_publication(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    pair: str,
    target_trade_id: int,
    source: str,
    publish_basis: dict[str, Any] | str,
    note: str | None = None,
    publication_key: str | None = None,
) -> dict[str, Any]:
    pair_text = str(pair).strip()
    source_text = str(source).strip()
    basis_text = (
        json.dumps(publish_basis, ensure_ascii=False, sort_keys=True)
        if isinstance(publish_basis, dict)
        else str(publish_basis)
    )
    if not pair_text:
        raise RuntimeError("position authority projection publication pair is required")
    if int(target_trade_id) <= 0:
        raise RuntimeError("position authority projection publication target_trade_id is required")
    if not source_text:
        raise RuntimeError("position authority projection publication source is required")
    if not basis_text.strip():
        raise RuntimeError("position authority projection publication basis is required")

    key = publication_key or _position_authority_projection_publication_key(
        pair=pair_text,
        target_trade_id=int(target_trade_id),
        source=source_text,
        publish_basis=basis_text,
        note=note,
    )
    existing = conn.execute(
        """
        SELECT id, publication_key, publication_type, pair, target_trade_id, event_ts, source, publish_basis, note
        FROM position_authority_projection_publications
        WHERE publication_key=?
        """,
        (key,),
    ).fetchone()
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "publication_key": str(existing["publication_key"]),
            "publication_type": str(existing["publication_type"]),
            "pair": str(existing["pair"]),
            "target_trade_id": int(existing["target_trade_id"]),
            "event_ts": int(existing["event_ts"]),
            "source": str(existing["source"]),
            "publish_basis": str(existing["publish_basis"]),
            "note": str(existing["note"]) if existing["note"] is not None else None,
            "created": False,
        }

    had_tx = conn.in_transaction
    cursor = conn.execute(
        """
        INSERT INTO position_authority_projection_publications(
            publication_key, publication_type, pair, target_trade_id, event_ts, source, publish_basis, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            "portfolio_projection_publish",
            pair_text,
            int(target_trade_id),
            int(event_ts),
            source_text,
            basis_text,
            note,
        ),
    )
    if not had_tx:
        conn.commit()

    return {
        "id": int(cursor.lastrowid),
        "publication_key": key,
        "publication_type": "portfolio_projection_publish",
        "pair": pair_text,
        "target_trade_id": int(target_trade_id),
        "event_ts": int(event_ts),
        "source": source_text,
        "publish_basis": basis_text,
        "note": note,
        "created": True,
    }


def get_position_authority_repair_summary(conn: sqlite3.Connection) -> dict[str, float | int | str | None]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS repair_count
        FROM position_authority_repairs
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT repair_key, event_ts, source, reason, repair_basis, note
        FROM position_authority_repairs
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "repair_count": int(row["repair_count"] if row else 0),
        "last_event_ts": int(last["event_ts"]) if last is not None else None,
        "last_repair_key": str(last["repair_key"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_reason": str(last["reason"]) if last is not None else None,
        "last_repair_basis": str(last["repair_basis"]) if last is not None else None,
        "last_note": str(last["note"]) if last is not None and last["note"] is not None else None,
    }


def record_fee_pending_accounting_repair(
    conn: sqlite3.Connection,
    *,
    event_ts: int,
    client_order_id: str,
    exchange_order_id: str | None,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    source: str,
    reason: str,
    repair_basis: dict[str, Any] | str,
    note: str | None = None,
    repair_key: str | None = None,
) -> dict[str, Any]:
    client_order_id_text = str(client_order_id or "").strip()
    if not client_order_id_text:
        raise RuntimeError("fee-pending accounting repair client_order_id is required")
    basis_text = (
        json.dumps(repair_basis, ensure_ascii=False, sort_keys=True)
        if isinstance(repair_basis, dict)
        else str(repair_basis)
    )
    source_text = str(source).strip()
    reason_text = str(reason).strip()
    if not source_text:
        raise RuntimeError("fee-pending accounting repair source is required")
    if not reason_text:
        raise RuntimeError("fee-pending accounting repair reason is required")
    if not basis_text.strip():
        raise RuntimeError("fee-pending accounting repair basis is required")

    fee_value = normalize_cash_amount(fee)
    if fee_value < 0:
        raise RuntimeError("fee-pending accounting repair fee must be non-negative")
    key = repair_key or _fee_pending_accounting_repair_key(
        client_order_id=client_order_id_text,
        fill_id=fill_id,
        fill_ts=int(fill_ts),
        price=float(price),
        qty=float(qty),
        fee=fee_value,
        source=source_text,
        reason=reason_text,
        repair_basis=basis_text,
        note=note,
    )
    existing = conn.execute(
        """
        SELECT id, repair_key, event_ts, client_order_id, exchange_order_id, fill_id,
               fill_ts, price, qty, fee, source, reason, repair_basis, note
        FROM fee_pending_accounting_repairs
        WHERE repair_key=?
        """,
        (key,),
    ).fetchone()
    if existing is not None:
        return {
            "id": int(existing["id"]),
            "repair_key": str(existing["repair_key"]),
            "event_ts": int(existing["event_ts"]),
            "client_order_id": str(existing["client_order_id"]),
            "exchange_order_id": str(existing["exchange_order_id"]) if existing["exchange_order_id"] is not None else None,
            "fill_id": str(existing["fill_id"]) if existing["fill_id"] is not None else None,
            "fill_ts": int(existing["fill_ts"]),
            "price": float(existing["price"]),
            "qty": float(existing["qty"]),
            "fee": float(existing["fee"]),
            "source": str(existing["source"]),
            "reason": str(existing["reason"]),
            "repair_basis": str(existing["repair_basis"]),
            "note": str(existing["note"]) if existing["note"] is not None else None,
            "created": False,
        }

    had_tx = conn.in_transaction
    cursor = conn.execute(
        """
        INSERT INTO fee_pending_accounting_repairs(
            repair_key, event_type, event_ts, client_order_id, exchange_order_id,
            fill_id, fill_ts, price, qty, fee, source, reason, repair_basis, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            FEE_PENDING_ACCOUNTING_REPAIR_EVENT_TYPE,
            int(event_ts),
            client_order_id_text,
            str(exchange_order_id or "").strip() or None,
            str(fill_id or "").strip() or None,
            int(fill_ts),
            float(price),
            float(qty),
            fee_value,
            source_text,
            reason_text,
            basis_text,
            note,
        ),
    )
    if not had_tx:
        conn.commit()

    return {
        "id": int(cursor.lastrowid),
        "repair_key": key,
        "event_ts": int(event_ts),
        "client_order_id": client_order_id_text,
        "exchange_order_id": str(exchange_order_id or "").strip() or None,
        "fill_id": str(fill_id or "").strip() or None,
        "fill_ts": int(fill_ts),
        "price": float(price),
        "qty": float(qty),
        "fee": fee_value,
        "source": source_text,
        "reason": reason_text,
        "repair_basis": basis_text,
        "note": note,
        "created": True,
    }


def get_fee_pending_accounting_repair_summary(conn: sqlite3.Connection) -> dict[str, float | int | str | None]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS repair_count
        FROM fee_pending_accounting_repairs
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT repair_key, event_ts, client_order_id, exchange_order_id, fill_id,
               fill_ts, fee, source, reason, repair_basis, note
        FROM fee_pending_accounting_repairs
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return {
        "repair_count": int(row["repair_count"] if row else 0),
        "last_event_ts": int(last["event_ts"]) if last is not None else None,
        "last_repair_key": str(last["repair_key"]) if last is not None else None,
        "last_client_order_id": str(last["client_order_id"]) if last is not None else None,
        "last_exchange_order_id": (
            str(last["exchange_order_id"]) if last is not None and last["exchange_order_id"] is not None else None
        ),
        "last_fill_id": str(last["fill_id"]) if last is not None and last["fill_id"] is not None else None,
        "last_fill_ts": int(last["fill_ts"]) if last is not None else None,
        "last_fee": float(last["fee"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_reason": str(last["reason"]) if last is not None else None,
        "last_repair_basis": str(last["repair_basis"]) if last is not None else None,
        "last_note": str(last["note"]) if last is not None and last["note"] is not None else None,
    }


@dataclass(frozen=True)
class OrderRuleSnapshotRecord:
    market: str
    fetched_ts: int
    source_mode: str
    fallback_used: bool
    fallback_reason_code: str
    fallback_reason_summary: str
    rule_signature: str
    rules_json: str
    source_json: str


def record_order_rule_snapshot(
    conn: sqlite3.Connection,
    *,
    market: str,
    fetched_ts: int,
    source_mode: str,
    fallback_used: bool,
    fallback_reason_code: str | None,
    fallback_reason_summary: str | None,
    rules_payload: dict[str, Any],
    source_payload: dict[str, str],
) -> OrderRuleSnapshotRecord:
    rules_json = json.dumps(rules_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    source_json = json.dumps(source_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    signature_payload = {
        "market": str(market),
        "source_mode": str(source_mode),
        "fallback_used": bool(fallback_used),
        "fallback_reason_code": str(fallback_reason_code or ""),
        "fallback_reason_summary": str(fallback_reason_summary or ""),
        "rules_json": rules_json,
        "source_json": source_json,
    }
    rule_signature = hashlib.sha256(
        json.dumps(signature_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    conn.execute(
        """
        INSERT OR IGNORE INTO order_rule_snapshots(
            market,
            fetched_ts,
            source_mode,
            fallback_used,
            fallback_reason_code,
            fallback_reason_summary,
            rule_signature,
            rules_json,
            source_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(market),
            int(fetched_ts),
            str(source_mode),
            1 if fallback_used else 0,
            (str(fallback_reason_code) if fallback_reason_code else None),
            (str(fallback_reason_summary) if fallback_reason_summary else None),
            rule_signature,
            rules_json,
            source_json,
        ),
    )
    return OrderRuleSnapshotRecord(
        market=str(market),
        fetched_ts=int(fetched_ts),
        source_mode=str(source_mode),
        fallback_used=bool(fallback_used),
        fallback_reason_code=str(fallback_reason_code or ""),
        fallback_reason_summary=str(fallback_reason_summary or ""),
        rule_signature=rule_signature,
        rules_json=rules_json,
        source_json=source_json,
    )


def fetch_latest_order_rule_snapshot(
    conn: sqlite3.Connection,
    *,
    market: str | None = None,
) -> OrderRuleSnapshotRecord | None:
    if market:
        row = conn.execute(
            """
            SELECT market, fetched_ts, source_mode, fallback_used, fallback_reason_code,
                   fallback_reason_summary, rule_signature, rules_json, source_json
            FROM order_rule_snapshots
            WHERE market=?
            ORDER BY fetched_ts DESC, id DESC
            LIMIT 1
            """,
            (str(market),),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT market, fetched_ts, source_mode, fallback_used, fallback_reason_code,
                   fallback_reason_summary, rule_signature, rules_json, source_json
            FROM order_rule_snapshots
            ORDER BY fetched_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    return OrderRuleSnapshotRecord(
        market=str(row["market"]),
        fetched_ts=int(row["fetched_ts"]),
        source_mode=str(row["source_mode"]),
        fallback_used=bool(int(row["fallback_used"])),
        fallback_reason_code=str(row["fallback_reason_code"] or ""),
        fallback_reason_summary=str(row["fallback_reason_summary"] or ""),
        rule_signature=str(row["rule_signature"]),
        rules_json=str(row["rules_json"]),
        source_json=str(row["source_json"]),
    )


def record_private_stream_event(
    conn: sqlite3.Connection,
    *,
    stream_name: str,
    dedupe_key: str,
    event_ts: int,
    client_order_id: str | None,
    exchange_order_id: str | None,
    order_status: str | None,
    fill_id: str | None,
    qty: float | None,
    price: float | None,
    payload: dict[str, Any],
) -> bool:
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO private_stream_events(
            stream_name,
            dedupe_key,
            event_ts,
            client_order_id,
            exchange_order_id,
            order_status,
            fill_id,
            qty,
            price,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(stream_name),
            str(dedupe_key),
            int(event_ts),
            (str(client_order_id) if client_order_id else None),
            (str(exchange_order_id) if exchange_order_id else None),
            (str(order_status) if order_status else None),
            (str(fill_id) if fill_id else None),
            (float(qty) if qty is not None else None),
            (float(price) if price is not None else None),
            payload_json,
        ),
    )
    return bool(cursor.rowcount)


def mark_private_stream_event_applied(
    conn: sqlite3.Connection,
    *,
    dedupe_key: str,
    applied: bool,
    applied_status: str,
) -> None:
    conn.execute(
        """
        UPDATE private_stream_events
        SET applied=?, applied_status=?
        WHERE dedupe_key=?
        """,
        (
            1 if applied else 0,
            str(applied_status),
            str(dedupe_key),
        ),
    )
