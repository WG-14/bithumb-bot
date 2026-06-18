from __future__ import annotations

import hashlib
import math
import json
import sqlite3
from decimal import Decimal, ROUND_HALF_EVEN
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .config import prepare_db_path_for_connection, resolve_db_path_for_connection, settings
from .dust import OPEN_EXPOSURE_LOT_STATE, lot_state_quantity_contract
from .sqlite_resilience import configure_connection
from .decision_context import (
    materialize_strategy_decision_context,
    normalize_strategy_decision_context,
)
from .canonical_decision import sha256_prefixed
from .risk_decision import (
    RISK_BUDGET_LEGACY_MARKER,
    RISK_BUDGET_SEMANTICS,
    build_risk_decision_artifact,
)
from .target_position import (
    ACTUAL_PAIR_TARGET_AUTHORITY,
    ACTUAL_PAIR_TARGET_AUTHORITY_SCOPE,
    ACTUAL_PAIR_TARGET_SOURCE,
    TargetPositionState,
    build_actual_pair_target_provenance,
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
OPERATIONAL_SCHEMA_VERSION = 1
OPERATIONAL_SCHEMA_META_KEY = "operational_schema"
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
FILL_FEE_ACCOUNTING_STATUS_FINALIZED = "fee_finalized"
FILL_FEE_ACCOUNTING_STATUS_PENDING = "principal_applied_fee_pending"
FILL_FEE_ACCOUNTING_STATUS_BLOCKED = "fee_validation_blocked"
FEE_PENDING_REPAIR_PROVENANCE_ORDER_LEVEL_ALLOCATED = "order_level_paid_fee_validated_allocated"
FEE_PENDING_REPAIR_FEE_ROUNDING_TOLERANCE_KRW = 0.01


class SchemaValidationError(RuntimeError):
    pass


REQUIRED_RUNTIME_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "portfolio": (
        "id",
        "cash_krw",
        "asset_qty",
        "cash_available",
        "cash_locked",
        "asset_available",
        "asset_locked",
    ),
    "trades": (
        "id",
        "ts",
        "pair",
        "interval",
        "side",
        "price",
        "qty",
        "fee",
        "cash_after",
        "asset_after",
    ),
    "orders": (
        "id",
        "client_order_id",
        "status",
        "side",
        "qty_req",
        "qty_filled",
        "local_intent_state",
    ),
    "fills": (
        "id",
        "client_order_id",
        "fill_ts",
        "price",
        "qty",
        "fee",
        "fee_accounting_status",
        "trade_id",
    ),
    "order_events": ("id", "client_order_id", "event_type", "event_ts"),
    "bot_health": ("id", "recovery_required_count", "unresolved_open_order_count", "startup_gate_reason"),
    "open_position_lots": (
        "id",
        "pair",
        "entry_trade_id",
        "qty_open",
        "executable_lot_count",
        "dust_tracking_lot_count",
        "position_semantic_basis",
        "position_state",
    ),
    "trade_lifecycles": ("id", "entry_trade_id", "exit_trade_id", "matched_qty", "net_pnl"),
    "external_cash_adjustments": ("id", "adjustment_key", "event_type", "event_ts", "delta_amount"),
    "manual_flat_accounting_repairs": ("id", "repair_key", "event_type", "event_ts", "repair_basis"),
    "external_position_adjustments": ("id", "adjustment_key", "event_type", "event_ts", "adjustment_basis"),
    "fee_gap_accounting_repairs": ("id", "repair_key", "event_type", "event_ts", "repair_basis"),
    "fee_pending_accounting_repairs": ("id", "repair_key", "event_type", "event_ts", "repair_basis"),
    "position_authority_repairs": ("id", "repair_key", "event_type", "event_ts", "repair_basis"),
    "position_authority_projection_publications": (
        "id",
        "publication_key",
        "publication_type",
        "pair",
        "target_trade_id",
        "publish_basis",
    ),
    "broker_fill_observations": (
        "id",
        "event_type",
        "event_ts",
        "client_order_id",
        "fill_ts",
        "price",
        "qty",
        "accounting_status",
    ),
    "schema_meta": ("key", "schema_version", "schema_fingerprint", "accounting_projection_model", "updated_ts"),
    "runtime_strategy_decision_bundle": (
        "id",
        "candle_ts",
        "pair",
        "interval",
        "runtime_strategy_set_manifest_id",
        "strategy_set_manifest_hash",
        "bundle_hash",
        "result_count",
        "created_ts",
    ),
    "runtime_strategy_set_manifest": (
        "id",
        "manifest_hash",
        "source",
        "market_scope_json",
        "active_strategy_count",
        "single_pair_runtime_enforced",
        "execution_config_hash",
        "risk_config_hash",
        "manifest_json",
        "created_ts",
    ),
    "runtime_strategy_decision_result": (
        "id",
        "bundle_id",
        "strategy_instance_id",
        "strategy_name",
        "raw_signal",
        "final_signal",
        "final_reason",
        "market_price",
        "runtime_decision_request_hash",
        "strategy_parameters_hash",
        "approved_profile_hash",
        "runtime_contract_hash",
        "plugin_contract_hash",
        "policy_input_hash",
        "policy_decision_hash",
        "replay_fingerprint_hash",
        "decision_hash",
        "full_decision_json",
    ),
    "portfolio_allocation_decision": (
        "id",
        "bundle_id",
        "runtime_strategy_set_manifest_id",
        "runtime_strategy_set_manifest_hash",
        "allocation_decision_hash",
        "allocation_input_hash",
        "allocator_config_hash",
        "strategy_contribution_hash",
        "selected_signal",
        "selected_priority",
        "authoritative",
        "primary_block_reason",
        "reason",
        "conflict_resolution_json",
        "allocation_decision_json",
    ),
    "strategy_contribution": (
        "id",
        "allocation_id",
        "strategy_instance_id",
        "strategy_name",
        "pair",
        "signal_direction",
        "priority",
        "weight",
        "desired_exposure_krw",
        "risk_budget_krw",
        "preference_hash",
        "reason",
        "contribution_json",
    ),
    "portfolio_target": (
        "id",
        "allocation_id",
        "pair",
        "target_exposure_krw",
        "target_qty",
        "authoritative",
        "fail_closed_reason",
        "final_portfolio_target_hash",
        "conflict_resolution_json",
        "target_json",
    ),
    "execution_plan": (
        "id",
        "allocation_id",
        "runtime_strategy_set_manifest_id",
        "runtime_strategy_set_manifest_hash",
        "portfolio_target_hash",
        "execution_plan_bundle_hash",
        "execution_submit_plan_hash",
        "submit_plan_side",
        "submit_plan_qty",
        "submit_plan_notional_krw",
        "submit_plan_idempotency_key",
        "submit_plan_source",
        "submit_plan_authority",
        "submit_expected",
        "final_action",
        "block_reason",
        "status",
        "execution_plan_bundle_json",
        "execution_submit_plan_json",
        "execution_plan_batch_hash",
        "execution_plan_batch_id",
    ),
}

DIAGNOSTIC_RUNTIME_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "execution_quality_events": (
        "client_order_id",
        "canonical_execution_kind",
        "semantic_evidence_quality",
        "market_equivalent",
        "execution_contract_hash",
        "remaining_notional_krw",
        "material_partial_fill_flag",
        "remaining_qty_materiality_reason",
    ),
}
REQUIRED_RUNTIME_TRIGGERS = (
    "trg_open_position_lots_validate_insert",
    "trg_open_position_lots_validate_update",
)


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
    authoritative_fill_fee_accounting_status: str | None
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
            "authoritative_fill_fee_accounting_status": self.authoritative_fill_fee_accounting_status,
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


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> dict[str, sqlite3.Row | tuple[Any, ...]]:
    if table not in _table_names(conn):
        return {}
    return {str(row[1]): row for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _trigger_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
    return {str(row[0]) for row in rows}


def _row_value(row: sqlite3.Row | tuple[Any, ...], key: str, index: int) -> Any:
    if isinstance(row, sqlite3.Row):
        return row[key]
    return row[index]


def _preflight_unsupported_schema(conn: sqlite3.Connection) -> None:
    columns = set(_table_columns(conn, "portfolio"))
    if not columns:
        return
    current_aggregate = {"cash_krw", "asset_qty"}
    old_aggregate = {"cash", "qty"}
    if old_aggregate.issubset(columns) and not current_aggregate.issubset(columns):
        raise SchemaValidationError(
            "unsupported legacy DB schema detected: portfolio(cash, qty). "
            "This schema cannot be opened for trading, reporting, or repair. "
            "Restore a current DB backup or run an explicit reviewed migration that maps "
            "cash->cash_krw and qty->asset_qty after preserving a DB backup."
        )
    if not current_aggregate.issubset(columns):
        missing = ", ".join(sorted(current_aggregate - columns))
        present = ", ".join(sorted(columns))
        raise SchemaValidationError(
            "malformed DB schema detected: portfolio table is missing required aggregate "
            f"column(s): {missing}; present_columns={present}. "
            "Restore a valid DB backup or run a reviewed repair before operating."
        )


def _ensure_schema_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            schema_fingerprint TEXT NOT NULL,
            accounting_projection_model TEXT NOT NULL,
            updated_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    _ensure_column(conn, "schema_meta", "schema_version", "schema_version INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "schema_meta", "schema_fingerprint", "schema_fingerprint TEXT NOT NULL DEFAULT ''")
    _ensure_column(
        conn,
        "schema_meta",
        "accounting_projection_model",
        "accounting_projection_model TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(conn, "schema_meta", "updated_ts", "updated_ts INTEGER NOT NULL DEFAULT 0")


def runtime_schema_fingerprint(conn: sqlite3.Connection) -> str:
    payload: dict[str, object] = {"tables": {}, "triggers": sorted(_trigger_names(conn))}
    tables_payload: dict[str, object] = {}
    for table in sorted(REQUIRED_RUNTIME_TABLE_COLUMNS):
        column_rows = _table_columns(conn, table)
        columns_payload: list[dict[str, object]] = []
        for column in sorted(column_rows):
            row = column_rows[column]
            columns_payload.append(
                {
                    "name": str(row[1]),
                    "type": str(row[2]),
                    "notnull": int(row[3]),
                    "pk": int(row[5]),
                }
            )
        tables_payload[table] = columns_payload
    payload["tables"] = tables_payload
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _runtime_schema_errors(conn: sqlite3.Connection, *, require_metadata: bool) -> list[str]:
    errors: list[str] = []
    tables = _table_names(conn)
    for table, required_columns in REQUIRED_RUNTIME_TABLE_COLUMNS.items():
        if table not in tables:
            errors.append(f"missing required table: {table}")
            continue
        columns = set(_table_columns(conn, table))
        missing = sorted(set(required_columns) - columns)
        if missing:
            errors.append(f"table {table} missing required column(s): {', '.join(missing)}")

    portfolio_columns = set(_table_columns(conn, "portfolio"))
    if {"cash", "qty"}.issubset(portfolio_columns) and not {"cash_krw", "asset_qty"}.issubset(portfolio_columns):
        errors.append("unsupported legacy table shape: portfolio(cash, qty)")

    triggers = _trigger_names(conn)
    missing_triggers = sorted(set(REQUIRED_RUNTIME_TRIGGERS) - triggers)
    if missing_triggers:
        errors.append(f"missing required trigger(s): {', '.join(missing_triggers)}")

    if "portfolio" in tables and {"id", "cash_krw", "asset_qty", "cash_available", "cash_locked", "asset_available", "asset_locked"}.issubset(portfolio_columns):
        rows = conn.execute(
            """
            SELECT id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            FROM portfolio
            """
        ).fetchall()
        for row in rows:
            row_id = int(_row_value(row, "id", 0))
            cash_total = normalize_cash_amount(
                float(_row_value(row, "cash_available", 3)) + float(_row_value(row, "cash_locked", 4))
            )
            asset_total = normalize_asset_qty(
                float(_row_value(row, "asset_available", 5)) + float(_row_value(row, "asset_locked", 6))
            )
            if row_id != 1:
                errors.append(f"portfolio contains invalid row id={row_id}; only id=1 is supported")
            if abs(normalize_cash_amount(float(_row_value(row, "cash_krw", 1))) - cash_total) > 1e-8:
                errors.append(
                    f"portfolio cash total mismatch for id={row_id}: "
                    "cash_krw must equal cash_available + cash_locked"
                )
            if abs(normalize_asset_qty(float(_row_value(row, "asset_qty", 2))) - asset_total) > 1e-12:
                errors.append(
                    f"portfolio asset total mismatch for id={row_id}: "
                    "asset_qty must equal asset_available + asset_locked"
                )

    if require_metadata and "schema_meta" in tables:
        row = conn.execute(
            """
            SELECT schema_version, schema_fingerprint, accounting_projection_model
            FROM schema_meta
            WHERE key=?
            """,
            (OPERATIONAL_SCHEMA_META_KEY,),
        ).fetchone()
        current_fingerprint = runtime_schema_fingerprint(conn)
        if row is None:
            errors.append("schema_meta missing operational_schema row")
        else:
            schema_version = int(_row_value(row, "schema_version", 0))
            schema_fingerprint = str(_row_value(row, "schema_fingerprint", 1))
            projection_model = str(_row_value(row, "accounting_projection_model", 2))
            if schema_version != OPERATIONAL_SCHEMA_VERSION:
                errors.append(
                    f"schema_meta version mismatch: expected={OPERATIONAL_SCHEMA_VERSION} got={schema_version}"
                )
            if projection_model != ACCOUNTING_PROJECTION_MODEL:
                errors.append(
                    "schema_meta accounting projection model mismatch: "
                    f"expected={ACCOUNTING_PROJECTION_MODEL} got={projection_model}"
                )
            if schema_fingerprint != current_fingerprint:
                errors.append("schema_meta fingerprint mismatch")

    return errors


def _update_schema_metadata(conn: sqlite3.Connection) -> None:
    fingerprint = runtime_schema_fingerprint(conn)
    conn.execute(
        """
        INSERT INTO schema_meta(key, schema_version, schema_fingerprint, accounting_projection_model, updated_ts)
        VALUES (?, ?, ?, ?, strftime('%s', 'now'))
        ON CONFLICT(key) DO UPDATE SET
            schema_version=excluded.schema_version,
            schema_fingerprint=excluded.schema_fingerprint,
            accounting_projection_model=excluded.accounting_projection_model,
            updated_ts=excluded.updated_ts
        """,
        (
            OPERATIONAL_SCHEMA_META_KEY,
            OPERATIONAL_SCHEMA_VERSION,
            fingerprint,
            ACCOUNTING_PROJECTION_MODEL,
        ),
    )


def assert_current_schema(conn: sqlite3.Connection) -> None:
    errors = _runtime_schema_errors(conn, require_metadata=True)
    if errors:
        raise SchemaValidationError(
            "DB schema validation failed before runtime operation: "
            + "; ".join(errors)
            + ". Restore a current DB backup or run a reviewed DB repair/migration before operating."
        )


def build_runtime_schema_diagnostics(conn: sqlite3.Connection) -> dict[str, object]:
    tables = _table_names(conn)
    missing_tables = sorted(set(REQUIRED_RUNTIME_TABLE_COLUMNS) - tables)
    missing_columns: dict[str, list[str]] = {}
    for table, required_columns in REQUIRED_RUNTIME_TABLE_COLUMNS.items():
        if table not in tables:
            continue
        columns = set(_table_columns(conn, table))
        missing = sorted(set(required_columns) - columns)
        if missing:
            missing_columns[table] = missing

    diagnostic_missing_tables = sorted(set(DIAGNOSTIC_RUNTIME_TABLE_COLUMNS) - tables)
    diagnostic_missing_columns: dict[str, list[str]] = {}
    for table, required_columns in DIAGNOSTIC_RUNTIME_TABLE_COLUMNS.items():
        if table not in tables:
            continue
        columns = set(_table_columns(conn, table))
        missing = sorted(set(required_columns) - columns)
        if missing:
            diagnostic_missing_columns[table] = missing
    diagnostic_status = "PASS"
    diagnostic_recommended_command = "diagnostic_schema_current"
    if diagnostic_missing_tables or diagnostic_missing_columns:
        diagnostic_status = "WARN"
        diagnostic_recommended_command = "execution-quality-report"

    portfolio_columns = set(_table_columns(conn, "portfolio"))
    legacy_schema_detected = bool(
        {"cash", "qty"}.issubset(portfolio_columns) and not {"cash_krw", "asset_qty"}.issubset(portfolio_columns)
    )
    malformed_portfolio = bool(portfolio_columns and not {"cash_krw", "asset_qty"}.issubset(portfolio_columns))
    missing_triggers = sorted(set(REQUIRED_RUNTIME_TRIGGERS) - _trigger_names(conn))
    errors = _runtime_schema_errors(conn, require_metadata=True)
    schema_meta = None
    if "schema_meta" in tables:
        row = conn.execute(
            """
            SELECT key, schema_version, schema_fingerprint, accounting_projection_model, updated_ts
            FROM schema_meta
            WHERE key=?
            """,
            (OPERATIONAL_SCHEMA_META_KEY,),
        ).fetchone()
        if row is not None:
            schema_meta = {
                "key": _row_value(row, "key", 0),
                "schema_version": _row_value(row, "schema_version", 1),
                "schema_fingerprint": _row_value(row, "schema_fingerprint", 2),
                "accounting_projection_model": _row_value(row, "accounting_projection_model", 3),
                "updated_ts": _row_value(row, "updated_ts", 4),
            }
    observed_schema_version = None
    observed_accounting_projection_model = None
    if schema_meta is not None:
        observed_schema_version = schema_meta["schema_version"]
        observed_accounting_projection_model = schema_meta["accounting_projection_model"]
    recommendation = "schema_current"
    if legacy_schema_detected:
        recommendation = "restore_current_backup_or_run_reviewed_legacy_cash_qty_migration_before_operating"
    elif malformed_portfolio or missing_tables or missing_columns or missing_triggers:
        recommendation = "restore_valid_backup_or_run_reviewed_db_repair_before_operating"
    elif errors:
        recommendation = "run_db_initialization_or_review_schema_metadata_before_operating"

    return {
        "status": "PASS" if not errors else "FAIL",
        "expected_schema_version": OPERATIONAL_SCHEMA_VERSION,
        "observed_schema_version": observed_schema_version,
        "schema_version": OPERATIONAL_SCHEMA_VERSION,
        "expected_accounting_projection_model": ACCOUNTING_PROJECTION_MODEL,
        "observed_accounting_projection_model": observed_accounting_projection_model,
        "accounting_projection_model": ACCOUNTING_PROJECTION_MODEL,
        "schema_fingerprint": runtime_schema_fingerprint(conn),
        "schema_meta": schema_meta,
        "legacy_schema_detected": legacy_schema_detected,
        "malformed_portfolio_detected": malformed_portfolio,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "missing_triggers": missing_triggers,
        "validation_errors": errors,
        "recommended_action": recommendation,
        "diagnostic_schema_status": diagnostic_status,
        "diagnostic_missing_tables": diagnostic_missing_tables,
        "diagnostic_missing_columns": diagnostic_missing_columns,
        "diagnostic_recommended_command": diagnostic_recommended_command,
    }


def diagnose_db_path(db_path: str | None = None) -> dict[str, object]:
    path = resolve_db_path_for_connection(db_path or settings.DB_PATH, mode=settings.MODE)
    if path != ":memory:" and not Path(path).exists():
        return {
            "status": "FAIL",
            "db_path": path,
            "exists": False,
            "validation_errors": ["database file does not exist"],
            "recommended_action": "run canonical initialization with bithumb_bot.db_core.ensure_db before operating",
        }
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return {"db_path": path, "exists": True, **build_runtime_schema_diagnostics(conn)}
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(row[1]) for row in cols}
    if column not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _json_dumps_stable(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_loads_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    loaded = json.loads(value)
    return loaded if isinstance(loaded, dict) else {}


def _strategy_scope_from_decision_context(context_json: str | None) -> tuple[str | None, str | None]:
    try:
        context = _json_loads_object(context_json)
    except json.JSONDecodeError:
        return (None, None)
    instance_id = str(context.get("strategy_instance_id") or "").strip()
    manifest_hash = str(context.get("runtime_strategy_set_manifest_hash") or "").strip()
    if not instance_id:
        ids = context.get("allocation_selected_strategy_instance_ids")
        if isinstance(ids, list) and len(ids) == 1:
            instance_id = str(ids[0] or "").strip()
    return (instance_id or None, manifest_hash or None)


def _backfill_trade_lifecycle_strategy_scope(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT tl.id, sd.context_json
        FROM trade_lifecycles tl
        JOIN strategy_decisions sd ON sd.id = tl.entry_decision_id
        WHERE (tl.strategy_instance_id IS NULL OR TRIM(tl.strategy_instance_id) = '')
           OR (tl.runtime_strategy_set_manifest_hash IS NULL OR TRIM(tl.runtime_strategy_set_manifest_hash) = '')
        """
    ).fetchall()
    for row in rows:
        instance_id, manifest_hash = _strategy_scope_from_decision_context(row["context_json"])
        conn.execute(
            """
            UPDATE trade_lifecycles
            SET
                strategy_instance_id = COALESCE(NULLIF(strategy_instance_id, ''), ?),
                runtime_strategy_set_manifest_hash = COALESCE(NULLIF(runtime_strategy_set_manifest_hash, ''), ?)
            WHERE id=?
            """,
            (instance_id, manifest_hash, int(row["id"])),
        )


def _strategy_decision_experiment_context(*, strategy_name: str) -> dict[str, Any]:
    payload = {
        "strategy_name": str(strategy_name).strip().lower(),
        "market": str(settings.PAIR),
        "interval": str(settings.INTERVAL),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "experiment_id": digest,
        "experiment_fingerprint": digest,
        "experiment_fingerprint_version": "experiment_fingerprint_v1",
        "experiment_fingerprint_inputs": payload,
    }


def _ensure_multi_strategy_artifact_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_dependency_manifest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manifest_hash TEXT NOT NULL UNIQUE,
            mode TEXT NOT NULL,
            execution_engine TEXT NOT NULL,
            broker_factory_identity TEXT NOT NULL,
            execution_service_identity TEXT NOT NULL,
            notification_service_identity TEXT NOT NULL,
            manifest_json TEXT NOT NULL,
            created_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_runtime_dependency_manifest_hash
        ON runtime_dependency_manifest(manifest_hash)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_strategy_set_manifest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manifest_hash TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL,
            market_scope_json TEXT NOT NULL DEFAULT '{}',
            active_strategy_count INTEGER NOT NULL,
            single_pair_runtime_enforced INTEGER NOT NULL,
            execution_config_hash TEXT,
            risk_config_hash TEXT,
            manifest_json TEXT NOT NULL,
            created_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_runtime_strategy_set_manifest_hash
        ON runtime_strategy_set_manifest(manifest_hash)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_strategy_decision_bundle (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candle_ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            runtime_strategy_set_manifest_id INTEGER,
            strategy_set_manifest_hash TEXT NOT NULL,
            bundle_hash TEXT NOT NULL UNIQUE,
            result_count INTEGER NOT NULL,
            created_ts INTEGER NOT NULL,
            FOREIGN KEY(runtime_strategy_set_manifest_id) REFERENCES runtime_strategy_set_manifest(id)
        )
        """
    )
    _ensure_column(conn, "runtime_strategy_decision_bundle", "runtime_strategy_set_manifest_id", "runtime_strategy_set_manifest_id INTEGER")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_runtime_strategy_bundle_candle
        ON runtime_strategy_decision_bundle(candle_ts, pair, interval)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_runtime_strategy_bundle_hash
        ON runtime_strategy_decision_bundle(bundle_hash)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_strategy_decision_result (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bundle_id INTEGER NOT NULL,
            strategy_instance_id TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            raw_signal TEXT NOT NULL,
            final_signal TEXT NOT NULL,
            final_reason TEXT NOT NULL,
            market_price REAL,
            runtime_decision_request_hash TEXT,
            strategy_parameters_hash TEXT,
            approved_profile_hash TEXT,
            runtime_contract_hash TEXT,
            plugin_contract_hash TEXT,
            policy_input_hash TEXT,
            policy_decision_hash TEXT,
            replay_fingerprint_hash TEXT NOT NULL,
            decision_hash TEXT NOT NULL,
            full_decision_json TEXT NOT NULL,
            FOREIGN KEY(bundle_id) REFERENCES runtime_strategy_decision_bundle(id)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_strategy_result_bundle_instance
        ON runtime_strategy_decision_result(bundle_id, strategy_instance_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_runtime_strategy_result_instance
        ON runtime_strategy_decision_result(strategy_instance_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_runtime_strategy_result_decision_hash
        ON runtime_strategy_decision_result(decision_hash)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_allocation_decision (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bundle_id INTEGER NOT NULL,
            allocation_decision_hash TEXT NOT NULL UNIQUE,
            allocation_input_hash TEXT NOT NULL,
            allocator_config_hash TEXT NOT NULL,
            strategy_contribution_hash TEXT NOT NULL,
            selected_signal TEXT NOT NULL DEFAULT '',
            selected_priority INTEGER,
            authoritative INTEGER NOT NULL,
            primary_block_reason TEXT NOT NULL,
            reason TEXT NOT NULL,
            conflict_resolution_json TEXT NOT NULL,
            allocation_decision_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(bundle_id) REFERENCES runtime_strategy_decision_bundle(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_allocation_bundle
        ON portfolio_allocation_decision(bundle_id)
        """
    )
    _ensure_column(
        conn,
        "portfolio_allocation_decision",
        "allocation_decision_json",
        "allocation_decision_json TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "portfolio_allocation_decision", "runtime_strategy_set_manifest_id", "runtime_strategy_set_manifest_id INTEGER")
    _ensure_column(conn, "portfolio_allocation_decision", "runtime_strategy_set_manifest_hash", "runtime_strategy_set_manifest_hash TEXT")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_allocation_hash
        ON portfolio_allocation_decision(allocation_decision_hash)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_contribution (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            allocation_id INTEGER NOT NULL,
            strategy_instance_id TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            pair TEXT NOT NULL,
            signal_direction TEXT NOT NULL,
            priority INTEGER NOT NULL,
            weight REAL NOT NULL,
            desired_exposure_krw REAL,
            risk_budget_krw REAL,
            preference_hash TEXT NOT NULL,
            reason TEXT NOT NULL,
            contribution_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(allocation_id) REFERENCES portfolio_allocation_decision(id)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_contribution_allocation_instance
        ON strategy_contribution(allocation_id, strategy_instance_id)
        """
    )
    _ensure_column(
        conn,
        "strategy_contribution",
        "contribution_json",
        "contribution_json TEXT NOT NULL DEFAULT '{}'",
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_contribution_instance
        ON strategy_contribution(strategy_instance_id)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_target (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            allocation_id INTEGER NOT NULL,
            pair TEXT NOT NULL,
            target_exposure_krw REAL,
            target_qty REAL,
            authoritative INTEGER NOT NULL,
            fail_closed_reason TEXT NOT NULL,
            final_portfolio_target_hash TEXT NOT NULL UNIQUE,
            conflict_resolution_json TEXT NOT NULL,
            target_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(allocation_id) REFERENCES portfolio_allocation_decision(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_target_allocation
        ON portfolio_target(allocation_id)
        """
    )
    _ensure_column(conn, "portfolio_target", "target_json", "target_json TEXT NOT NULL DEFAULT '{}'")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_portfolio_target_hash
        ON portfolio_target(final_portfolio_target_hash)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_plan (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            allocation_id INTEGER NOT NULL,
            portfolio_target_hash TEXT,
            execution_plan_bundle_hash TEXT NOT NULL,
            execution_submit_plan_hash TEXT,
            submit_plan_side TEXT,
            submit_plan_qty REAL,
            submit_plan_notional_krw REAL,
            submit_plan_idempotency_key TEXT,
            submit_plan_source TEXT,
            submit_plan_authority TEXT,
            submit_expected INTEGER NOT NULL,
            final_action TEXT NOT NULL,
            block_reason TEXT NOT NULL,
            status TEXT NOT NULL,
            execution_plan_bundle_json TEXT NOT NULL DEFAULT '{}',
            execution_submit_plan_json TEXT,
            FOREIGN KEY(allocation_id) REFERENCES portfolio_allocation_decision(id)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_plan_allocation_bundle
        ON execution_plan(allocation_id, execution_plan_bundle_hash)
        """
    )
    _ensure_column(
        conn,
        "execution_plan",
        "execution_plan_bundle_json",
        "execution_plan_bundle_json TEXT NOT NULL DEFAULT '{}'",
    )
    _ensure_column(conn, "execution_plan", "runtime_strategy_set_manifest_id", "runtime_strategy_set_manifest_id INTEGER")
    _ensure_column(conn, "execution_plan", "runtime_strategy_set_manifest_hash", "runtime_strategy_set_manifest_hash TEXT")
    _ensure_column(conn, "execution_plan", "execution_submit_plan_json", "execution_submit_plan_json TEXT")
    _ensure_column(conn, "execution_plan", "submit_plan_side", "submit_plan_side TEXT")
    _ensure_column(conn, "execution_plan", "submit_plan_qty", "submit_plan_qty REAL")
    _ensure_column(conn, "execution_plan", "submit_plan_notional_krw", "submit_plan_notional_krw REAL")
    _ensure_column(conn, "execution_plan", "submit_plan_idempotency_key", "submit_plan_idempotency_key TEXT")
    _ensure_column(conn, "execution_plan", "submit_plan_source", "submit_plan_source TEXT")
    _ensure_column(conn, "execution_plan", "submit_plan_authority", "submit_plan_authority TEXT")
    _ensure_column(conn, "execution_plan", "execution_plan_batch_hash", "execution_plan_batch_hash TEXT")
    _ensure_column(conn, "execution_plan", "execution_plan_batch_id", "execution_plan_batch_id TEXT")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_plan_bundle_hash
        ON execution_plan(execution_plan_bundle_hash)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_plan_submit_hash
        ON execution_plan(execution_submit_plan_hash)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_plan_batch (
            batch_hash TEXT PRIMARY KEY,
            batch_id TEXT NOT NULL,
            runtime_strategy_set_manifest_hash TEXT NOT NULL,
            allocation_decision_hash TEXT NOT NULL,
            budget_lock_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            batch_json TEXT NOT NULL,
            created_ts INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_plan_batch_manifest
        ON execution_plan_batch(runtime_strategy_set_manifest_hash, allocation_decision_hash)
        """
    )


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
    _preflight_unsupported_schema(conn)

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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orderbook_top_snapshots (
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            bid_price REAL NOT NULL,
            ask_price REAL NOT NULL,
            spread_bps REAL NOT NULL,
            source TEXT NOT NULL,
            observed_at_epoch_sec REAL,
            PRIMARY KEY (ts, pair, source)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_orderbook_top_pair_ts
        ON orderbook_top_snapshots(pair, ts)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orderbook_depth_levels (
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('bid', 'ask')),
            level_index INTEGER NOT NULL CHECK (level_index >= 0),
            price REAL NOT NULL CHECK (price > 0),
            size REAL NOT NULL CHECK (size > 0),
            cumulative_size REAL NOT NULL CHECK (cumulative_size > 0),
            cumulative_notional REAL NOT NULL CHECK (cumulative_notional > 0),
            source TEXT NOT NULL,
            observed_at_epoch_sec REAL,
            PRIMARY KEY (ts, pair, source, side, level_index)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_orderbook_depth_pair_ts
        ON orderbook_depth_levels(pair, ts)
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
            fee_source TEXT NOT NULL DEFAULT 'unknown',
            fee_confidence TEXT NOT NULL DEFAULT 'unknown',
            accounting_status TEXT NOT NULL,
            source TEXT NOT NULL,
            fee_provenance TEXT,
            fee_validation_reason TEXT,
            fee_validation_checks TEXT,
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
    _ensure_column(conn, "broker_fill_observations", "fee_source", "fee_source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "broker_fill_observations", "fee_confidence", "fee_confidence TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(
        conn,
        "broker_fill_observations",
        "accounting_status",
        "accounting_status TEXT NOT NULL DEFAULT 'observed'",
    )
    _ensure_column(conn, "broker_fill_observations", "source", "source TEXT NOT NULL DEFAULT 'unknown'")
    _ensure_column(conn, "broker_fill_observations", "fee_provenance", "fee_provenance TEXT")
    _ensure_column(conn, "broker_fill_observations", "fee_validation_reason", "fee_validation_reason TEXT")
    _ensure_column(conn, "broker_fill_observations", "fee_validation_checks", "fee_validation_checks TEXT")
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
    for name, ddl in (
        ("risk_input_hash", "risk_input_hash TEXT"),
        ("risk_policy_hash", "risk_policy_hash TEXT"),
        ("risk_evidence_hash", "risk_evidence_hash TEXT"),
        ("risk_decision_hash", "risk_decision_hash TEXT"),
        ("risk_reason_code", "risk_reason_code TEXT"),
        ("risk_status", "risk_status TEXT"),
        ("risk_evaluation_point", "risk_evaluation_point TEXT"),
        ("risk_state_source", "risk_state_source TEXT"),
        ("effective_risk_limits_json", "effective_risk_limits_json TEXT"),
    ):
        _ensure_column(conn, "risk_evaluations", name, ddl)

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
            pair TEXT,
            order_type TEXT,
            price REAL,
            qty_req REAL NOT NULL,
            qty_filled REAL NOT NULL DEFAULT 0,
            strategy_name TEXT,
            strategy_instance_id TEXT,
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
    _ensure_column(conn, "orders", "pair", "pair TEXT")
    _ensure_column(conn, "orders", "order_type", "order_type TEXT")
    _ensure_column(conn, "orders", "strategy_name", "strategy_name TEXT")
    _ensure_column(conn, "orders", "strategy_instance_id", "strategy_instance_id TEXT")
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
            fee_accounting_status TEXT NOT NULL DEFAULT 'fee_finalized',
            observed_fee_status TEXT NOT NULL DEFAULT 'complete',
            observed_fee_source TEXT NOT NULL DEFAULT 'trade_level_fee',
            observed_fee_confidence TEXT NOT NULL DEFAULT 'authoritative',
            observed_fee_provenance TEXT,
            observed_fee_validation_reason TEXT,
            observed_fee_validation_checks TEXT,
            trade_id INTEGER,
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
    _ensure_column(
        conn,
        "fills",
        "fee_accounting_status",
        "fee_accounting_status TEXT NOT NULL DEFAULT 'fee_finalized'",
    )
    _ensure_column(
        conn,
        "fills",
        "observed_fee_status",
        "observed_fee_status TEXT NOT NULL DEFAULT 'complete'",
    )
    _ensure_column(
        conn,
        "fills",
        "observed_fee_source",
        "observed_fee_source TEXT NOT NULL DEFAULT 'trade_level_fee'",
    )
    _ensure_column(
        conn,
        "fills",
        "observed_fee_confidence",
        "observed_fee_confidence TEXT NOT NULL DEFAULT 'authoritative'",
    )
    _ensure_column(conn, "fills", "observed_fee_provenance", "observed_fee_provenance TEXT")
    _ensure_column(conn, "fills", "observed_fee_validation_reason", "observed_fee_validation_reason TEXT")
    _ensure_column(conn, "fills", "observed_fee_validation_checks", "observed_fee_validation_checks TEXT")
    _ensure_column(conn, "fills", "trade_id", "trade_id INTEGER")
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
        CREATE TABLE IF NOT EXISTS fill_trade_linkage_repairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repair_key TEXT NOT NULL UNIQUE,
            fill_row_id INTEGER NOT NULL,
            client_order_id TEXT NOT NULL,
            fill_id TEXT,
            candidate_trade_id INTEGER NOT NULL,
            applied_ts INTEGER NOT NULL,
            status TEXT NOT NULL,
            repair_basis TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fill_trade_linkage_repairs_fill_row
        ON fill_trade_linkage_repairs(fill_row_id, candidate_trade_id)
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
        CREATE TABLE IF NOT EXISTS execution_quality_events (
            execution_trace_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT NOT NULL UNIQUE,
            submit_attempt_id TEXT,
            decision_id INTEGER,
            strategy_name TEXT,
            mode TEXT,
            market TEXT,
            side TEXT,
            order_type TEXT,
            exchange TEXT,
            submit_contract_kind TEXT,
            canonical_execution_kind TEXT,
            semantic_evidence_quality TEXT NOT NULL DEFAULT 'legacy_unverified',
            market_equivalent INTEGER NOT NULL DEFAULT 0,
            legacy_unknown_order_type INTEGER NOT NULL DEFAULT 0,
            unsupported_unknown_order_type INTEGER NOT NULL DEFAULT 0,
            exchange_order_id TEXT,
            execution_reality_contract TEXT,
            execution_contract_hash TEXT,
            execution_contract_hash_valid INTEGER,
            execution_contract_mismatch_reason TEXT,
            signal_ts_ms INTEGER,
            signal_reference_price REAL,
            signal_best_bid REAL,
            signal_best_ask REAL,
            signal_spread_bps REAL,
            submit_plan_ts_ms INTEGER,
            submit_sent_ts_ms INTEGER,
            submit_response_ts_ms INTEGER,
            submit_reference_price REAL,
            submit_best_bid REAL,
            submit_best_ask REAL,
            submit_spread_bps REAL,
            first_fill_ts_ms INTEGER,
            last_fill_ts_ms INTEGER,
            avg_fill_price REAL,
            filled_qty REAL NOT NULL DEFAULT 0,
            requested_qty REAL,
            remaining_qty REAL,
            remaining_notional_krw REAL,
            internal_target_remaining_qty REAL,
            internal_target_residue_material INTEGER NOT NULL DEFAULT 0,
            internal_target_residue_reason TEXT NOT NULL DEFAULT 'not_yet_computed',
            exchange_submit_notional_krw REAL,
            exchange_spent_quote_krw REAL,
            exchange_remaining_quote_krw REAL,
            exchange_fill_completion_ratio REAL,
            qty_step REAL,
            effective_min_trade_qty REAL,
            min_notional_krw REAL,
            fee REAL,
            realized_fee_rate REAL,
            submit_latency_ms INTEGER,
            response_latency_ms INTEGER,
            first_fill_latency_ms INTEGER,
            full_fill_latency_ms INTEGER,
            slippage_vs_signal_bps REAL,
            slippage_vs_submit_ref_bps REAL,
            slippage_vs_best_quote_bps REAL,
            fill_ratio REAL,
            partial_fill_flag INTEGER NOT NULL DEFAULT 0,
            unfilled_flag INTEGER NOT NULL DEFAULT 0,
            material_partial_fill_flag INTEGER NOT NULL DEFAULT 0,
            material_unfilled_flag INTEGER NOT NULL DEFAULT 0,
            remaining_qty_materiality_reason TEXT NOT NULL DEFAULT 'not_yet_computed',
            quality_status TEXT NOT NULL,
            quality_reason TEXT NOT NULL,
            backtest_assumed_slippage_bps REAL,
            model_breach_flag INTEGER,
            created_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id)
        )
        """
    )
    for column, ddl in (
        ("submit_attempt_id", "submit_attempt_id TEXT"),
        ("decision_id", "decision_id INTEGER"),
        ("strategy_name", "strategy_name TEXT"),
        ("mode", "mode TEXT"),
        ("market", "market TEXT"),
        ("side", "side TEXT"),
        ("order_type", "order_type TEXT"),
        ("exchange", "exchange TEXT"),
        ("submit_contract_kind", "submit_contract_kind TEXT"),
        ("canonical_execution_kind", "canonical_execution_kind TEXT"),
        ("semantic_evidence_quality", "semantic_evidence_quality TEXT NOT NULL DEFAULT 'legacy_unverified'"),
        ("market_equivalent", "market_equivalent INTEGER NOT NULL DEFAULT 0"),
        ("legacy_unknown_order_type", "legacy_unknown_order_type INTEGER NOT NULL DEFAULT 0"),
        ("unsupported_unknown_order_type", "unsupported_unknown_order_type INTEGER NOT NULL DEFAULT 0"),
        ("exchange_order_id", "exchange_order_id TEXT"),
        ("execution_reality_contract", "execution_reality_contract TEXT"),
        ("execution_contract_hash", "execution_contract_hash TEXT"),
        ("execution_contract_hash_valid", "execution_contract_hash_valid INTEGER"),
        ("execution_contract_mismatch_reason", "execution_contract_mismatch_reason TEXT"),
        ("signal_ts_ms", "signal_ts_ms INTEGER"),
        ("signal_reference_price", "signal_reference_price REAL"),
        ("signal_best_bid", "signal_best_bid REAL"),
        ("signal_best_ask", "signal_best_ask REAL"),
        ("signal_spread_bps", "signal_spread_bps REAL"),
        ("submit_plan_ts_ms", "submit_plan_ts_ms INTEGER"),
        ("submit_sent_ts_ms", "submit_sent_ts_ms INTEGER"),
        ("submit_response_ts_ms", "submit_response_ts_ms INTEGER"),
        ("submit_reference_price", "submit_reference_price REAL"),
        ("submit_best_bid", "submit_best_bid REAL"),
        ("submit_best_ask", "submit_best_ask REAL"),
        ("submit_spread_bps", "submit_spread_bps REAL"),
        ("first_fill_ts_ms", "first_fill_ts_ms INTEGER"),
        ("last_fill_ts_ms", "last_fill_ts_ms INTEGER"),
        ("avg_fill_price", "avg_fill_price REAL"),
        ("filled_qty", "filled_qty REAL NOT NULL DEFAULT 0"),
        ("requested_qty", "requested_qty REAL"),
        ("remaining_qty", "remaining_qty REAL"),
        ("remaining_notional_krw", "remaining_notional_krw REAL"),
        ("internal_target_remaining_qty", "internal_target_remaining_qty REAL"),
        ("internal_target_residue_material", "internal_target_residue_material INTEGER NOT NULL DEFAULT 0"),
        (
            "internal_target_residue_reason",
            "internal_target_residue_reason TEXT NOT NULL DEFAULT 'not_yet_computed'",
        ),
        ("exchange_submit_notional_krw", "exchange_submit_notional_krw REAL"),
        ("exchange_spent_quote_krw", "exchange_spent_quote_krw REAL"),
        ("exchange_remaining_quote_krw", "exchange_remaining_quote_krw REAL"),
        ("exchange_fill_completion_ratio", "exchange_fill_completion_ratio REAL"),
        ("qty_step", "qty_step REAL"),
        ("effective_min_trade_qty", "effective_min_trade_qty REAL"),
        ("min_notional_krw", "min_notional_krw REAL"),
        ("fee", "fee REAL"),
        ("realized_fee_rate", "realized_fee_rate REAL"),
        ("submit_latency_ms", "submit_latency_ms INTEGER"),
        ("response_latency_ms", "response_latency_ms INTEGER"),
        ("first_fill_latency_ms", "first_fill_latency_ms INTEGER"),
        ("full_fill_latency_ms", "full_fill_latency_ms INTEGER"),
        ("slippage_vs_signal_bps", "slippage_vs_signal_bps REAL"),
        ("slippage_vs_submit_ref_bps", "slippage_vs_submit_ref_bps REAL"),
        ("slippage_vs_best_quote_bps", "slippage_vs_best_quote_bps REAL"),
        ("fill_ratio", "fill_ratio REAL"),
        ("partial_fill_flag", "partial_fill_flag INTEGER NOT NULL DEFAULT 0"),
        ("unfilled_flag", "unfilled_flag INTEGER NOT NULL DEFAULT 0"),
        ("material_partial_fill_flag", "material_partial_fill_flag INTEGER NOT NULL DEFAULT 0"),
        ("material_unfilled_flag", "material_unfilled_flag INTEGER NOT NULL DEFAULT 0"),
        (
            "remaining_qty_materiality_reason",
            "remaining_qty_materiality_reason TEXT NOT NULL DEFAULT 'not_yet_computed'",
        ),
        ("quality_status", "quality_status TEXT NOT NULL DEFAULT 'insufficient_evidence'"),
        ("quality_reason", "quality_reason TEXT NOT NULL DEFAULT 'not_yet_computed'"),
        ("backtest_assumed_slippage_bps", "backtest_assumed_slippage_bps REAL"),
        ("model_breach_flag", "model_breach_flag INTEGER"),
        ("created_ts", "created_ts INTEGER NOT NULL DEFAULT 0"),
        ("updated_ts", "updated_ts INTEGER NOT NULL DEFAULT 0"),
    ):
        _ensure_column(conn, "execution_quality_events", column, ddl)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_quality_market_mode_ts
        ON execution_quality_events(market, mode, submit_sent_ts_ms, client_order_id)
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
            runtime_strategy_decision_bundle_id INTEGER,
            portfolio_allocation_decision_id INTEGER,
            portfolio_target_id INTEGER,
            execution_plan_id INTEGER,
            strategy_decision_projection_type TEXT,
            strategy_decisions_authority TEXT,
            context_json TEXT NOT NULL
        )
        """
    )

    _ensure_column(conn, "strategy_decisions", "candle_ts", "candle_ts INTEGER")
    _ensure_column(conn, "strategy_decisions", "market_price", "market_price REAL")
    _ensure_column(conn, "strategy_decisions", "confidence", "confidence REAL")
    _ensure_column(conn, "strategy_decisions", "runtime_strategy_decision_bundle_id", "runtime_strategy_decision_bundle_id INTEGER")
    _ensure_column(conn, "strategy_decisions", "portfolio_allocation_decision_id", "portfolio_allocation_decision_id INTEGER")
    _ensure_column(conn, "strategy_decisions", "portfolio_target_id", "portfolio_target_id INTEGER")
    _ensure_column(conn, "strategy_decisions", "execution_plan_id", "execution_plan_id INTEGER")
    _ensure_column(conn, "strategy_decisions", "strategy_decision_projection_type", "strategy_decision_projection_type TEXT")
    _ensure_column(conn, "strategy_decisions", "strategy_decisions_authority", "strategy_decisions_authority TEXT")
    _ensure_column(conn, "strategy_decisions", "context_json", "context_json TEXT NOT NULL DEFAULT '{}'")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_decisions_lookup
        ON strategy_decisions(strategy_name, decision_ts, signal)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_decisions_artifact_chain
        ON strategy_decisions(runtime_strategy_decision_bundle_id, portfolio_allocation_decision_id, portfolio_target_id, execution_plan_id)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS target_position_state (
            pair TEXT PRIMARY KEY,
            target_exposure_krw REAL NOT NULL,
            target_qty REAL NOT NULL,
            last_signal TEXT NOT NULL,
            last_decision_id INTEGER,
            last_reference_price REAL NOT NULL,
            updated_ts INTEGER NOT NULL
        )
        """
    )

    _ensure_column(conn, "target_position_state", "target_exposure_krw", "target_exposure_krw REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "target_position_state", "target_qty", "target_qty REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "target_position_state", "last_signal", "last_signal TEXT NOT NULL DEFAULT 'HOLD'")
    _ensure_column(conn, "target_position_state", "last_decision_id", "last_decision_id INTEGER")
    _ensure_column(conn, "target_position_state", "last_reference_price", "last_reference_price REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "target_position_state", "updated_ts", "updated_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "target_position_state", "target_origin", "target_origin TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "target_position_state", "adoption_reason", "adoption_reason TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "target_position_state", "adopted_broker_qty", "adopted_broker_qty REAL")
    _ensure_column(conn, "target_position_state", "adopted_broker_exposure_krw", "adopted_broker_exposure_krw REAL")
    _ensure_column(conn, "target_position_state", "created_from_signal", "created_from_signal TEXT NOT NULL DEFAULT ''")
    _ensure_column(
        conn,
        "target_position_state",
        "actual_target_authority",
        f"actual_target_authority TEXT NOT NULL DEFAULT '{ACTUAL_PAIR_TARGET_AUTHORITY}'",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "actual_target_authority_scope",
        (
            "actual_target_authority_scope TEXT NOT NULL DEFAULT "
            f"'{ACTUAL_PAIR_TARGET_AUTHORITY_SCOPE}'"
        ),
    )
    _ensure_column(
        conn,
        "target_position_state",
        "actual_target_source",
        "actual_target_source TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "runtime_strategy_set_manifest_hash",
        "runtime_strategy_set_manifest_hash TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "runtime_strategy_decision_bundle_hash",
        "runtime_strategy_decision_bundle_hash TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "portfolio_allocation_decision_hash",
        "portfolio_allocation_decision_hash TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "portfolio_target_hash",
        "portfolio_target_hash TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "execution_plan_batch_hash",
        "execution_plan_batch_hash TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "execution_submit_plan_hash",
        "execution_submit_plan_hash TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "actual_target_provenance_hash",
        "actual_target_provenance_hash TEXT NOT NULL DEFAULT ''",
    )
    _ensure_column(
        conn,
        "target_position_state",
        "actual_target_provenance_json",
        "actual_target_provenance_json TEXT NOT NULL DEFAULT '{}'",
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_virtual_target_state (
            strategy_instance_id TEXT NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            scope_key_hash TEXT NOT NULL,
            runtime_contract_hash TEXT NOT NULL,
            virtual_target_exposure_krw REAL NOT NULL,
            virtual_target_qty REAL,
            lifecycle_state TEXT NOT NULL,
            last_signal TEXT NOT NULL,
            updated_ts INTEGER NOT NULL,
            evidence_hash TEXT,
            state_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY(strategy_instance_id, pair, interval, scope_key_hash)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_virtual_target_state_scope
        ON strategy_virtual_target_state(scope_key_hash, pair, interval)
        """
    )
    _ensure_column(conn, "strategy_virtual_target_state", "strategy_name", "strategy_name TEXT NOT NULL DEFAULT ''")

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
        CREATE TABLE IF NOT EXISTS account_balances (
            currency TEXT PRIMARY KEY,
            available REAL NOT NULL DEFAULT 0,
            locked REAL NOT NULL DEFAULT 0,
            total REAL NOT NULL DEFAULT 0,
            authority_source TEXT NOT NULL DEFAULT 'multi_asset_ledger_v1',
            updated_ts INTEGER NOT NULL DEFAULT 0,
            evidence_hash TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pair_positions (
            pair TEXT PRIMARY KEY,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            available_qty REAL NOT NULL DEFAULT 0,
            locked_qty REAL NOT NULL DEFAULT 0,
            total_qty REAL NOT NULL DEFAULT 0,
            authority_source TEXT NOT NULL DEFAULT 'multi_asset_ledger_v1',
            updated_ts INTEGER NOT NULL DEFAULT 0,
            evidence_hash TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS budget_locks (
            lock_hash TEXT PRIMARY KEY,
            currency TEXT NOT NULL,
            pair TEXT,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            released_ts INTEGER,
            evidence_hash TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_locks (
            lock_hash TEXT PRIMARY KEY,
            pair TEXT NOT NULL,
            currency TEXT NOT NULL,
            client_order_id TEXT,
            exchange_order_id TEXT,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            released_ts INTEGER,
            evidence_hash TEXT
        )
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
    _ensure_column(conn, "trade_lifecycles", "strategy_instance_id", "strategy_instance_id TEXT")
    _ensure_column(
        conn,
        "trade_lifecycles",
        "runtime_strategy_set_manifest_hash",
        "runtime_strategy_set_manifest_hash TEXT",
    )
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
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_trade_lifecycles_strategy_instance_pair_exit
        ON trade_lifecycles(strategy_instance_id, pair, exit_ts, id)
        """
    )
    _backfill_trade_lifecycle_strategy_scope(conn)

    _ensure_multi_strategy_artifact_schema(conn)
    _ensure_schema_meta_table(conn)
    structural_errors = _runtime_schema_errors(conn, require_metadata=False)
    if structural_errors:
        raise SchemaValidationError(
            "DB schema validation failed during initialization: "
            + "; ".join(structural_errors)
            + ". Restore a current DB backup or run a reviewed DB repair/migration before operating."
        )
    _update_schema_metadata(conn)
    conn.commit()
    assert_current_schema(conn)


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
    runtime_strategy_decision_bundle_id: int | None = None,
    portfolio_allocation_decision_id: int | None = None,
    portfolio_target_id: int | None = None,
    execution_plan_id: int | None = None,
    strategy_decision_projection_type: str | None = None,
    strategy_decisions_authority: str | None = None,
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
    normalized_context = {
        **normalized_context,
        **_strategy_decision_experiment_context(strategy_name=str(strategy_name)),
    }
    row = conn.execute(
        """
        INSERT INTO strategy_decisions(
            decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence,
            runtime_strategy_decision_bundle_id, portfolio_allocation_decision_id,
            portfolio_target_id, execution_plan_id, strategy_decision_projection_type,
            strategy_decisions_authority, context_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(decision_ts),
            str(strategy_name),
            str(signal),
            str(reason),
            None if candle_ts is None else int(candle_ts),
            None if market_price is None else float(market_price),
            None if confidence is None else float(confidence),
            None if runtime_strategy_decision_bundle_id is None else int(runtime_strategy_decision_bundle_id),
            None if portfolio_allocation_decision_id is None else int(portfolio_allocation_decision_id),
            None if portfolio_target_id is None else int(portfolio_target_id),
            None if execution_plan_id is None else int(execution_plan_id),
            strategy_decision_projection_type,
            strategy_decisions_authority,
            json.dumps(
                materialize_strategy_decision_context(normalized_context),
                ensure_ascii=False,
                sort_keys=True,
            ),
        ),
    )
    return int(row.lastrowid)


def _runtime_result_instance_id(result: object) -> str:
    base_context = getattr(result, "base_context", {})
    if isinstance(base_context, Mapping):
        value = str(base_context.get("strategy_instance_id") or "").strip()
        if value:
            return value
    replay = getattr(result, "replay_fingerprint", {})
    if isinstance(replay, Mapping):
        value = str(replay.get("strategy_instance_id") or "").strip()
        if value:
            return value
    decision = getattr(result, "decision", None)
    return str(getattr(decision, "strategy_name", "") or "").strip().lower()


def _runtime_result_full_decision_payload(result: object) -> dict[str, Any]:
    decision = getattr(result, "decision", None)
    base_context = getattr(result, "base_context", {})
    replay = getattr(result, "replay_fingerprint", {})
    trace = decision.as_trace() if hasattr(decision, "as_trace") else {}
    payload = {
        "schema_version": 1,
        "strategy_instance_id": _runtime_result_instance_id(result),
        "strategy_name": str(getattr(decision, "strategy_name", "") or "").strip().lower(),
        "candle_ts": int(getattr(result, "candle_ts", 0) or 0),
        "market_price": float(getattr(result, "market_price", 0.0) or 0.0),
        "raw_signal": str(getattr(decision, "raw_signal", "HOLD") or "HOLD").upper(),
        "final_signal": str(getattr(decision, "final_signal", "HOLD") or "HOLD").upper(),
        "final_reason": str(getattr(decision, "final_reason", "") or ""),
        "policy_input_hash": str(getattr(decision, "policy_input_hash", "") or ""),
        "policy_decision_hash": str(getattr(decision, "policy_decision_hash", "") or ""),
        "base_context": dict(base_context) if isinstance(base_context, Mapping) else {},
        "replay_fingerprint": dict(replay) if isinstance(replay, Mapping) else {},
        "trace": trace if isinstance(trace, dict) else {},
    }
    payload["decision_hash"] = sha256_prefixed(
        {key: value for key, value in payload.items() if key != "decision_hash"}
    )
    return payload


def record_runtime_strategy_decision_bundle(
    conn: sqlite3.Connection,
    *,
    result_bundle: object,
    pair: str,
    interval: str,
    created_ts: int,
    settings_obj: object | None = None,
    manifest_payload: Mapping[str, Any] | None = None,
    runtime_strategy_set_manifest_id: int | None = None,
    runtime_strategy_set_manifest_hash: str | None = None,
) -> dict[str, Any]:
    """Persist typed runtime strategy results as first-class replay artifacts."""
    bundle_hash = str(result_bundle.content_hash())
    strategy_set = getattr(result_bundle, "strategy_set")
    manifest_refs = record_runtime_strategy_set_manifest(
        conn,
        strategy_set=strategy_set,
        created_ts=created_ts,
        settings_obj=settings_obj,
        manifest_payload=manifest_payload,
    )
    expected_manifest_hash = str(runtime_strategy_set_manifest_hash or "").strip()
    if expected_manifest_hash and expected_manifest_hash != str(manifest_refs["runtime_strategy_set_manifest_hash"]):
        raise RuntimeError("runtime_strategy_set_manifest_hash_mismatch")
    expected_manifest_id = runtime_strategy_set_manifest_id
    if expected_manifest_id is not None and int(expected_manifest_id) != int(manifest_refs["runtime_strategy_set_manifest_id"]):
        raise RuntimeError("runtime_strategy_set_manifest_hash_mismatch")
    strategy_set_manifest_hash = str(manifest_refs["runtime_strategy_set_manifest_hash"])
    results = tuple(getattr(result_bundle, "results"))
    conn.execute(
        """
        INSERT OR IGNORE INTO runtime_strategy_decision_bundle(
            candle_ts, pair, interval, runtime_strategy_set_manifest_id,
            strategy_set_manifest_hash, bundle_hash,
            result_count, created_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(getattr(result_bundle, "candle_ts")),
            str(pair),
            str(interval),
            int(manifest_refs["runtime_strategy_set_manifest_id"]),
            strategy_set_manifest_hash,
            bundle_hash,
            len(results),
            int(created_ts),
        ),
    )
    row = conn.execute(
        "SELECT id FROM runtime_strategy_decision_bundle WHERE bundle_hash=?",
        (bundle_hash,),
    ).fetchone()
    if row is None:
        raise RuntimeError("runtime_strategy_decision_bundle_persist_failed")
    bundle_id = int(row["id"])

    result_ids: dict[str, int] = {}
    for result in results:
        decision = getattr(result, "decision", None)
        base_context = getattr(result, "base_context", {})
        replay = getattr(result, "replay_fingerprint", {})
        base = dict(base_context) if isinstance(base_context, Mapping) else {}
        replay_payload = dict(replay) if isinstance(replay, Mapping) else {}
        full_decision = _runtime_result_full_decision_payload(result)
        decision_hash = str(full_decision["decision_hash"])
        instance_id = _runtime_result_instance_id(result)
        conn.execute(
            """
            INSERT OR IGNORE INTO runtime_strategy_decision_result(
                bundle_id, strategy_instance_id, strategy_name, raw_signal, final_signal,
                final_reason, market_price, runtime_decision_request_hash,
                strategy_parameters_hash, approved_profile_hash, runtime_contract_hash,
                plugin_contract_hash, policy_input_hash, policy_decision_hash,
                replay_fingerprint_hash, decision_hash, full_decision_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bundle_id,
                instance_id,
                str(getattr(decision, "strategy_name", "") or "").strip().lower(),
                str(getattr(decision, "raw_signal", "HOLD") or "HOLD").upper(),
                str(getattr(decision, "final_signal", "HOLD") or "HOLD").upper(),
                str(getattr(decision, "final_reason", "") or ""),
                float(getattr(result, "market_price", 0.0) or 0.0),
                base.get("runtime_decision_request_hash"),
                base.get("strategy_parameters_hash"),
                base.get("approved_profile_hash"),
                base.get("runtime_contract_hash"),
                base.get("plugin_contract_hash"),
                str(getattr(decision, "policy_input_hash", "") or ""),
                str(getattr(decision, "policy_decision_hash", "") or ""),
                sha256_prefixed(replay_payload),
                decision_hash,
                _json_dumps_stable(full_decision),
            ),
        )
        result_row = conn.execute(
            """
            SELECT id FROM runtime_strategy_decision_result
            WHERE bundle_id=? AND strategy_instance_id=?
            """,
            (bundle_id, instance_id),
        ).fetchone()
        if result_row is not None:
            result_ids[instance_id] = int(result_row["id"])
    return {
        "runtime_strategy_decision_bundle_id": bundle_id,
        "runtime_strategy_decision_bundle_hash": bundle_hash,
        "runtime_strategy_decision_result_ids": result_ids,
        "runtime_strategy_set_manifest_hash": strategy_set_manifest_hash,
        "runtime_strategy_set_manifest_id": manifest_refs["runtime_strategy_set_manifest_id"],
        "runtime_strategy_set_source": str(getattr(strategy_set, "source", "")),
    }


def record_runtime_strategy_set_manifest(
    conn: sqlite3.Connection,
    *,
    strategy_set: object | None = None,
    created_ts: int,
    settings_obj: object | None = None,
    manifest_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from .runtime_strategy_set import normalized_runtime_strategy_set_manifest

    if manifest_payload is not None:
        manifest = dict(manifest_payload)
    else:
        if strategy_set is None:
            raise RuntimeError("runtime_strategy_set_manifest_source_missing")
        manifest = normalized_runtime_strategy_set_manifest(
            strategy_set=strategy_set,
            **({} if settings_obj is None else {"settings_obj": settings_obj}),
        )
    manifest_hash = str(manifest.get("runtime_strategy_set_manifest_hash") or "")
    if not manifest_hash:
        raise RuntimeError("runtime_strategy_set_manifest_hash_missing")
    hash_payload = dict(manifest)
    recorded = str(hash_payload.pop("runtime_strategy_set_manifest_hash", "") or "")
    replayed = sha256_prefixed(hash_payload)
    if recorded != replayed or manifest_hash != replayed:
        raise RuntimeError("runtime_strategy_set_manifest_hash_mismatch")
    market_scope = manifest.get("market_scope")
    conn.execute(
        """
        INSERT OR IGNORE INTO runtime_strategy_set_manifest(
            manifest_hash, source, market_scope_json, active_strategy_count,
            single_pair_runtime_enforced, execution_config_hash, risk_config_hash,
            manifest_json, created_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            manifest_hash,
            str(manifest.get("source") or ""),
            _json_dumps_stable(market_scope if isinstance(market_scope, dict) else {}),
            int(manifest.get("active_strategy_count") or 0),
            1 if bool(manifest.get("single_pair_runtime_enforced")) else 0,
            str(manifest.get("execution_config_hash") or ""),
            str(manifest.get("risk_config_hash") or ""),
            _json_dumps_stable(manifest),
            int(created_ts),
        ),
    )
    row = conn.execute(
        "SELECT id FROM runtime_strategy_set_manifest WHERE manifest_hash=?",
        (manifest_hash,),
    ).fetchone()
    if row is None:
        raise RuntimeError("runtime_strategy_set_manifest_persist_failed")
    return {
        "runtime_strategy_set_manifest_id": int(row["id"]),
        "runtime_strategy_set_manifest_hash": manifest_hash,
    }


def record_runtime_dependency_manifest(
    conn: sqlite3.Connection,
    *,
    manifest_payload: Mapping[str, Any],
    created_ts: int,
) -> dict[str, Any]:
    manifest = dict(manifest_payload)
    manifest_hash = str(manifest.get("runtime_dependency_manifest_hash") or "")
    if not manifest_hash:
        raise RuntimeError("runtime_dependency_manifest_hash_missing")
    hash_payload = dict(manifest)
    recorded = str(hash_payload.pop("runtime_dependency_manifest_hash", "") or "")
    replayed = sha256_prefixed(hash_payload)
    if recorded != replayed or manifest_hash != replayed:
        raise RuntimeError("runtime_dependency_manifest_hash_mismatch")
    conn.execute(
        """
        INSERT OR IGNORE INTO runtime_dependency_manifest(
            manifest_hash, mode, execution_engine, broker_factory_identity,
            execution_service_identity, notification_service_identity,
            manifest_json, created_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            manifest_hash,
            str(manifest.get("mode") or ""),
            str(manifest.get("execution_engine") or ""),
            str(manifest.get("broker_factory_identity") or ""),
            str(manifest.get("execution_service_identity") or ""),
            str(manifest.get("notification_service_identity") or ""),
            _json_dumps_stable(manifest),
            int(created_ts),
        ),
    )
    row = conn.execute(
        "SELECT id FROM runtime_dependency_manifest WHERE manifest_hash=?",
        (manifest_hash,),
    ).fetchone()
    if row is None:
        raise RuntimeError("runtime_dependency_manifest_persist_failed")
    return {
        "runtime_dependency_manifest_id": int(row["id"]),
        "runtime_dependency_manifest_hash": manifest_hash,
    }


def record_portfolio_allocation_decision(
    conn: sqlite3.Connection,
    *,
    bundle_id: int,
    allocation_decision: dict[str, Any],
) -> dict[str, Any]:
    allocation_hash = str(allocation_decision.get("allocation_decision_hash") or "")
    if not allocation_hash:
        raise RuntimeError("allocation_decision_hash_missing")
    conflict = allocation_decision.get("conflict_resolution")
    conflict_payload = dict(conflict) if isinstance(conflict, dict) else {}
    targets = allocation_decision.get("targets")
    target_payloads = list(targets) if isinstance(targets, list) else []
    contributions = allocation_decision.get("contributions")
    contribution_payloads = list(contributions) if isinstance(contributions, list) else []
    selected_signal = ""
    selected_priority: int | None = None
    if target_payloads and isinstance(target_payloads[0], dict):
        target_conflict = target_payloads[0].get("conflict_resolution")
        if isinstance(target_conflict, dict):
            selected_signal = str(target_conflict.get("selected_signal") or "")
            raw_priority = target_conflict.get("selected_priority")
            selected_priority = None if raw_priority is None else int(raw_priority)
    bundle_manifest_row = conn.execute(
        "SELECT runtime_strategy_set_manifest_id, strategy_set_manifest_hash FROM runtime_strategy_decision_bundle WHERE id=?",
        (int(bundle_id),),
    ).fetchone()
    manifest_id = None if bundle_manifest_row is None else bundle_manifest_row["runtime_strategy_set_manifest_id"]
    manifest_hash = "" if bundle_manifest_row is None else str(bundle_manifest_row["strategy_set_manifest_hash"] or "")
    allocation_decision = {
        **allocation_decision,
        "runtime_strategy_set_manifest_id": manifest_id,
        "runtime_strategy_set_manifest_hash": manifest_hash,
    }
    conn.execute(
        """
        INSERT OR IGNORE INTO portfolio_allocation_decision(
            bundle_id, runtime_strategy_set_manifest_id, runtime_strategy_set_manifest_hash,
            allocation_decision_hash, allocation_input_hash,
            allocator_config_hash, strategy_contribution_hash, selected_signal,
            selected_priority, authoritative, primary_block_reason, reason,
            conflict_resolution_json, allocation_decision_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(bundle_id),
            manifest_id,
            manifest_hash,
            allocation_hash,
            str(allocation_decision.get("allocation_input_hash") or ""),
            str(allocation_decision.get("allocator_config_hash") or ""),
            str(allocation_decision.get("strategy_contribution_hash") or ""),
            selected_signal,
            selected_priority,
            1 if bool(allocation_decision.get("authoritative")) else 0,
            str(allocation_decision.get("primary_block_reason") or ""),
            str(allocation_decision.get("reason") or ""),
            _json_dumps_stable(conflict_payload),
            _json_dumps_stable(allocation_decision),
        ),
    )
    row = conn.execute(
        "SELECT id FROM portfolio_allocation_decision WHERE allocation_decision_hash=?",
        (allocation_hash,),
    ).fetchone()
    if row is None:
        raise RuntimeError("portfolio_allocation_decision_persist_failed")
    allocation_id = int(row["id"])

    for contribution in contribution_payloads:
        if not isinstance(contribution, dict):
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO strategy_contribution(
                allocation_id, strategy_instance_id, strategy_name, pair, signal_direction,
                priority, weight, desired_exposure_krw, risk_budget_krw, preference_hash,
                reason, contribution_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                allocation_id,
                str(contribution.get("strategy_instance_id") or ""),
                str(contribution.get("strategy_name") or ""),
                str(contribution.get("pair") or ""),
                str(contribution.get("signal_direction") or ""),
                int(contribution.get("priority") or 0),
                float(contribution.get("weight") or 0.0),
                contribution.get("desired_exposure_krw"),
                contribution.get("risk_budget_krw"),
                str(contribution.get("preference_hash") or ""),
                str(contribution.get("reason") or ""),
                _json_dumps_stable(contribution),
            ),
        )

    target_ids: dict[str, int] = {}
    for target in target_payloads:
        if not isinstance(target, dict):
            continue
        target_hash = str(target.get("final_portfolio_target_hash") or "")
        target_conflict = target.get("conflict_resolution")
        conn.execute(
            """
            INSERT OR IGNORE INTO portfolio_target(
                allocation_id, pair, target_exposure_krw, target_qty, authoritative,
                fail_closed_reason, final_portfolio_target_hash, conflict_resolution_json,
                target_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                allocation_id,
                str(target.get("pair") or ""),
                target.get("target_exposure_krw"),
                target.get("target_qty"),
                1 if bool(target.get("authoritative")) else 0,
                str(target.get("fail_closed_reason") or ""),
                target_hash,
                _json_dumps_stable(target_conflict if isinstance(target_conflict, dict) else {}),
                _json_dumps_stable(target),
            ),
        )
        target_row = conn.execute(
            "SELECT id FROM portfolio_target WHERE final_portfolio_target_hash=?",
            (target_hash,),
        ).fetchone()
        if target_row is not None:
            target_ids[str(target.get("pair") or "")] = int(target_row["id"])
    singular_target_id: int | None = None
    singular_target_hash = ""
    singular_reason = "portfolio_target_singular_not_available"
    if len(target_payloads) == 1 and len(target_ids) == 1 and isinstance(target_payloads[0], dict):
        first_target = target_payloads[0]
        pair = str(first_target.get("pair") or "")
        singular_target_id = target_ids.get(pair)
        singular_target_hash = str(first_target.get("final_portfolio_target_hash") or "")
        singular_reason = "exactly_one_portfolio_target"
    elif len(target_payloads) > 1:
        singular_reason = "multiple_portfolio_targets_singular_compatibility_fail_closed"
    return {
        "portfolio_allocation_decision_id": allocation_id,
        "allocation_decision_hash": allocation_hash,
        "portfolio_target_ids": target_ids,
        "portfolio_target_id": singular_target_id,
        "portfolio_target_hash": singular_target_hash,
        "portfolio_target_singular_available": singular_target_id is not None,
        "portfolio_target_singular_reason": singular_reason,
        "runtime_strategy_set_manifest_id": manifest_id,
        "runtime_strategy_set_manifest_hash": manifest_hash,
    }


def record_execution_plan(
    conn: sqlite3.Connection,
    *,
    allocation_id: int,
    portfolio_target_hash: str | None,
    execution_plan_bundle: object,
) -> dict[str, Any]:
    bundle_payload = execution_plan_bundle.as_dict()
    bundle_hash = str(execution_plan_bundle.content_hash())
    execution_plan_batch = getattr(execution_plan_bundle, "execution_plan_batch", None)
    execution_plan_batch_hash = None
    execution_plan_batch_id = None
    if execution_plan_batch is not None:
        batch_payload = execution_plan_batch.as_dict()
        execution_plan_batch_hash = str(batch_payload.get("batch_hash") or "")
        execution_plan_batch_id = str(batch_payload.get("batch_id") or "")
    submit_plan = getattr(execution_plan_bundle, "submit_plan", None)
    submit_payload = submit_plan.as_dict() if submit_plan is not None and hasattr(submit_plan, "as_dict") else None
    submit_hash = None
    if submit_payload is not None:
        submit_hash = (
            str(submit_plan.content_hash())
            if hasattr(submit_plan, "content_hash")
            else sha256_prefixed(submit_payload)
        )
        submit_payload = {**submit_payload, "submit_plan_hash": submit_hash}
    status = getattr(execution_plan_bundle, "status", None)
    status_text = ""
    if status is not None and hasattr(status, "status"):
        status_text = str(status.status)
    elif isinstance(bundle_payload.get("status"), dict):
        status_text = str(bundle_payload["status"].get("status") or "")
    allocation_manifest_row = conn.execute(
        """
        SELECT runtime_strategy_set_manifest_id, runtime_strategy_set_manifest_hash
        FROM portfolio_allocation_decision WHERE id=?
        """,
        (int(allocation_id),),
    ).fetchone()
    manifest_id = None if allocation_manifest_row is None else allocation_manifest_row["runtime_strategy_set_manifest_id"]
    manifest_hash = "" if allocation_manifest_row is None else str(allocation_manifest_row["runtime_strategy_set_manifest_hash"] or "")
    target_rows = conn.execute(
        """
        SELECT final_portfolio_target_hash
        FROM portfolio_target
        WHERE allocation_id=?
        ORDER BY pair, id
        """,
        (int(allocation_id),),
    ).fetchall()
    if len(target_rows) == 1:
        authoritative_target_hash = str(target_rows[0]["final_portfolio_target_hash"] or "")
        if str(portfolio_target_hash or "") != authoritative_target_hash:
            raise RuntimeError("portfolio_target_hash_not_planner_validated_runtime_pair_target")
    elif len(target_rows) > 1 and str(portfolio_target_hash or ""):
        raise RuntimeError("portfolio_target_hash_singular_requires_exactly_one_target")
    bundle_payload = {
        **bundle_payload,
        "runtime_strategy_set_manifest_id": manifest_id,
        "runtime_strategy_set_manifest_hash": manifest_hash,
    }
    conn.execute(
        """
        INSERT OR IGNORE INTO execution_plan(
            allocation_id, runtime_strategy_set_manifest_id, runtime_strategy_set_manifest_hash,
            portfolio_target_hash, execution_plan_bundle_hash,
            execution_submit_plan_hash, submit_plan_side, submit_plan_qty,
            submit_plan_notional_krw, submit_plan_idempotency_key, submit_plan_source,
            submit_plan_authority, submit_expected, final_action, block_reason,
            status, execution_plan_bundle_json, execution_submit_plan_json,
            execution_plan_batch_hash, execution_plan_batch_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(allocation_id),
            manifest_id,
            manifest_hash,
            portfolio_target_hash,
            bundle_hash,
            submit_hash,
            None if submit_payload is None else str(submit_payload.get("side") or ""),
            None if submit_payload is None or submit_payload.get("qty") is None else float(submit_payload.get("qty") or 0.0),
            None if submit_payload is None or submit_payload.get("notional_krw") is None else float(submit_payload.get("notional_krw") or 0.0),
            None if submit_payload is None else submit_payload.get("idempotency_key"),
            None if submit_payload is None else str(submit_payload.get("source") or ""),
            None if submit_payload is None else str(submit_payload.get("authority") or ""),
            1 if bool(getattr(submit_plan, "submit_expected", False)) else 0,
            str(getattr(submit_plan, "final_action", "") or ""),
            str(getattr(submit_plan, "block_reason", "") or ""),
            status_text,
            _json_dumps_stable(bundle_payload),
            None if submit_payload is None else _json_dumps_stable(submit_payload),
            execution_plan_batch_hash,
            execution_plan_batch_id,
        ),
    )
    row = conn.execute(
        """
        SELECT id FROM execution_plan
        WHERE allocation_id=? AND execution_plan_bundle_hash=?
        """,
        (int(allocation_id), bundle_hash),
    ).fetchone()
    if row is None:
        raise RuntimeError("execution_plan_persist_failed")
    return {
        "execution_plan_id": int(row["id"]),
        "execution_plan_bundle_hash": bundle_hash,
        "execution_submit_plan_hash": submit_hash,
        "execution_plan_batch_hash": execution_plan_batch_hash,
        "execution_plan_batch_id": execution_plan_batch_id,
        "runtime_strategy_set_manifest_id": manifest_id,
        "runtime_strategy_set_manifest_hash": manifest_hash,
    }


def record_execution_plan_batch(
    conn: sqlite3.Connection,
    *,
    execution_plan_batch: object,
    created_ts: int,
) -> dict[str, Any]:
    from .execution_plan_batch import ExecutionPlanBatch

    if not isinstance(execution_plan_batch, ExecutionPlanBatch):
        raise TypeError("execution_plan_batch_required")
    payload = execution_plan_batch.as_dict()
    batch_hash = str(payload["batch_hash"])
    conn.execute(
        """
        INSERT OR IGNORE INTO execution_plan_batch(
            batch_hash, batch_id, runtime_strategy_set_manifest_hash,
            allocation_decision_hash, budget_lock_hash, status, batch_json, created_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch_hash,
            str(payload["batch_id"]),
            execution_plan_batch.runtime_strategy_set_manifest_hash,
            execution_plan_batch.allocation_decision_hash,
            execution_plan_batch.budget_lock_hash,
            execution_plan_batch.status,
            _json_dumps_stable(payload),
            int(created_ts),
        ),
    )
    return {
        "execution_plan_batch_hash": batch_hash,
        "execution_plan_batch_id": str(payload["batch_id"]),
    }


def update_execution_plan_final_submit_payload(
    conn: sqlite3.Connection,
    *,
    final_submit_payload: Mapping[str, Any],
    persistence_status: str = "final_broker_bound_payload",
) -> dict[str, Any]:
    payload = dict(final_submit_payload)
    submit_hash = str(payload.get("submit_plan_hash") or "").strip()
    if not submit_hash:
        raise RuntimeError("final_submit_payload_submit_plan_hash_missing")
    payload.setdefault("final_submit_payload_persistence_status", str(persistence_status))
    payload_hash = sha256_prefixed(payload)
    payload["final_submit_payload_hash"] = payload_hash
    cur = conn.execute(
        """
        UPDATE execution_plan
        SET
            execution_submit_plan_json=?,
            execution_submit_plan_hash=?,
            submit_plan_side=?,
            submit_plan_qty=?,
            submit_plan_notional_krw=?,
            submit_plan_idempotency_key=?,
            submit_plan_source=?,
            submit_plan_authority=?,
            submit_expected=?,
            final_action=?,
            block_reason=?
        WHERE execution_submit_plan_hash=?
        """,
        (
            _json_dumps_stable(payload),
            submit_hash,
            str(payload.get("side") or ""),
            None if payload.get("qty") is None else float(payload.get("qty") or 0.0),
            None
            if payload.get("notional_krw") is None
            else float(payload.get("notional_krw") or 0.0),
            payload.get("idempotency_key"),
            str(payload.get("source") or ""),
            str(payload.get("authority") or ""),
            1 if bool(payload.get("submit_expected")) else 0,
            str(payload.get("final_action") or ""),
            str(payload.get("block_reason") or ""),
            submit_hash,
        ),
    )
    if cur.rowcount <= 0:
        return {
            "updated": False,
            "reason": "execution_plan_not_found_for_submit_plan_hash",
            "execution_submit_plan_hash": submit_hash,
            "final_submit_payload_hash": payload_hash,
        }
    return {
        "updated": True,
        "execution_submit_plan_hash": submit_hash,
        "final_submit_payload_hash": payload_hash,
        "updated_row_count": int(cur.rowcount),
    }


def replay_allocation_decision_hash(conn: sqlite3.Connection, allocation_id: int) -> str:
    row = conn.execute(
        "SELECT allocation_decision_json FROM portfolio_allocation_decision WHERE id=?",
        (int(allocation_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError("portfolio_allocation_decision_not_found")
    payload = _json_loads_object(str(row["allocation_decision_json"]))
    recorded = str(payload.pop("allocation_decision_hash", "") or "")
    payload.pop("runtime_strategy_set_manifest_id", None)
    payload.pop("runtime_strategy_set_manifest_hash", None)
    replayed = sha256_prefixed(payload)
    if recorded and recorded != replayed:
        raise RuntimeError("portfolio_allocation_decision_hash_mismatch")
    return replayed


def replay_runtime_strategy_set_manifest(conn: sqlite3.Connection, manifest_id: int) -> str:
    row = conn.execute(
        "SELECT manifest_hash, manifest_json FROM runtime_strategy_set_manifest WHERE id=?",
        (int(manifest_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError("runtime_strategy_set_manifest_not_found")
    payload = _json_loads_object(str(row["manifest_json"]))
    recorded = str(payload.pop("runtime_strategy_set_manifest_hash", "") or "")
    replayed = sha256_prefixed(payload)
    if recorded and recorded != replayed:
        raise RuntimeError("runtime_strategy_set_manifest_hash_mismatch")
    stored = str(row["manifest_hash"] or "")
    if stored and stored != replayed:
        raise RuntimeError("runtime_strategy_set_manifest_record_hash_mismatch")
    return replayed


def replay_manifest_request_hashes(conn: sqlite3.Connection, manifest_id: int) -> dict[str, str]:
    row = conn.execute(
        "SELECT manifest_json FROM runtime_strategy_set_manifest WHERE id=?",
        (int(manifest_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError("runtime_strategy_set_manifest_not_found")
    manifest = _json_loads_object(str(row["manifest_json"]))
    instances = manifest.get("active_instances")
    if not isinstance(instances, list) or not instances:
        raise RuntimeError("runtime_strategy_set_manifest_instances_missing")
    result: dict[str, str] = {}
    for instance in instances:
        if not isinstance(instance, dict):
            raise RuntimeError("runtime_strategy_set_manifest_instance_invalid")
        instance_id = str(instance.get("strategy_instance_id") or "")
        if not instance_id:
            raise RuntimeError("runtime_strategy_set_manifest_instance_missing")
        request_hash = str(instance.get("runtime_decision_request_hash") or "")
        if not request_hash.startswith("sha256:"):
            raise RuntimeError("runtime_strategy_set_manifest_request_hash_missing")
        result[instance_id] = request_hash
    return result


def replay_portfolio_target_hash(conn: sqlite3.Connection, portfolio_target_id: int) -> str:
    row = conn.execute(
        "SELECT target_json FROM portfolio_target WHERE id=?",
        (int(portfolio_target_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError("portfolio_target_not_found")
    payload = _json_loads_object(str(row["target_json"]))
    recorded = str(payload.pop("final_portfolio_target_hash", "") or "")
    replayed = sha256_prefixed(payload)
    if recorded and recorded != replayed:
        raise RuntimeError("portfolio_target_hash_mismatch")
    return replayed


def replay_execution_submit_plan_hash(conn: sqlite3.Connection, execution_plan_id: int) -> str | None:
    row = conn.execute(
        "SELECT execution_submit_plan_json, execution_submit_plan_hash FROM execution_plan WHERE id=?",
        (int(execution_plan_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError("execution_plan_not_found")
    if row["execution_submit_plan_json"] is None:
        return None
    payload = json.loads(str(row["execution_submit_plan_json"]))
    replayed = str(payload.get("submit_plan_hash") or "") or sha256_prefixed(payload)
    recorded = row["execution_submit_plan_hash"]
    if recorded is not None and str(recorded) != replayed:
        raise RuntimeError("execution_submit_plan_hash_mismatch")
    return replayed


def _strategy_contribution_payload_from_row(row: sqlite3.Row) -> dict[str, Any]:
    default_risk_decision = build_risk_decision_artifact(
        risk_budget_krw=row["risk_budget_krw"],
        decision_context="strategy_contribution",
    )
    payload = {
        "schema_version": 1,
        "strategy_instance_id": str(row["strategy_instance_id"] or ""),
        "strategy_name": str(row["strategy_name"] or ""),
        "pair": str(row["pair"] or ""),
        "signal_direction": str(row["signal_direction"] or ""),
        "priority": int(row["priority"] or 0),
        "weight": float(row["weight"] or 0.0),
        "preference_hash": str(row["preference_hash"] or ""),
        "desired_exposure_krw": row["desired_exposure_krw"],
        "risk_budget_krw": row["risk_budget_krw"],
        "max_target_exposure_krw": None,
        "pre_cap_weighted_target_exposure_krw": None,
        "exposure_cap_applied": False,
        "exposure_cap_source": "none",
        "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
        "exposure_boundary_artifact": default_risk_decision,
        "exposure_boundary_artifact_hash": default_risk_decision["exposure_boundary_artifact_hash"],
        "legacy_non_authoritative_exposure_risk_decision": default_risk_decision,
        "legacy_non_authoritative_exposure_risk_decision_hash": default_risk_decision[
            "exposure_boundary_artifact_hash"
        ],
        "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
        "scope_key_hash": "",
        "runtime_scope_key": None,
        "virtual_lifecycle_evidence": None,
        "virtual_lifecycle_transition_hash": None,
        "virtual_lifecycle_before_hash": None,
        "virtual_lifecycle_after_hash": None,
        "virtual_lifecycle_evidence_hash": None,
        "virtual_lifecycle_authority": None,
        "virtual_lifecycle_live_submit_authority": None,
        "strategy_risk_policy": None,
        "strategy_risk_snapshot": None,
        "strategy_risk_profile": None,
        "strategy_risk_decision": None,
        "strategy_risk_decision_hash": None,
        "strategy_risk_policy_hash": None,
        "strategy_risk_input_hash": None,
        "strategy_risk_evidence_hash": None,
        "strategy_risk_status": None,
        "strategy_risk_reason_code": None,
        "strategy_risk_state_source": None,
        "reason": str(row["reason"] or ""),
    }
    raw_json = str(row["contribution_json"] or "").strip()
    if raw_json:
        try:
            stored_payload = json.loads(raw_json)
        except json.JSONDecodeError:
            stored_payload = None
        if isinstance(stored_payload, dict):
            for key in (
                "max_target_exposure_krw",
                "pre_cap_weighted_target_exposure_krw",
                "exposure_cap_applied",
                "exposure_cap_source",
                "risk_budget_semantics",
                "exposure_boundary_artifact",
                "exposure_boundary_artifact_hash",
                "legacy_non_authoritative_exposure_risk_decision",
                "legacy_non_authoritative_exposure_risk_decision_hash",
                "risk_budget_legacy_marker",
                "scope_key_hash",
                "runtime_scope_key",
                "virtual_lifecycle_evidence",
                "virtual_lifecycle_transition_hash",
                "virtual_lifecycle_before_hash",
                "virtual_lifecycle_after_hash",
                "virtual_lifecycle_evidence_hash",
                "virtual_lifecycle_authority",
                "virtual_lifecycle_live_submit_authority",
                "strategy_risk_policy",
                "strategy_risk_snapshot",
                "strategy_risk_profile",
                "strategy_risk_decision",
                "strategy_risk_decision_hash",
                "strategy_risk_policy_hash",
                "strategy_risk_input_hash",
                "strategy_risk_evidence_hash",
                "strategy_risk_status",
                "strategy_risk_reason_code",
                "strategy_risk_state_source",
            ):
                payload[key] = stored_payload.get(key, payload[key])
    return payload


def _portfolio_target_payload_from_row(row: sqlite3.Row) -> dict[str, Any]:
    default_risk_decision = build_risk_decision_artifact(
        max_target_exposure_krw=None,
        exposure_cap_source="none",
        decision_context="portfolio_target",
    )
    payload = {
        "schema_version": 1,
        "pair": str(row["pair"] or ""),
        "target_exposure_krw": row["target_exposure_krw"],
        "max_target_exposure_krw": row["target_exposure_krw"],
        "pre_cap_weighted_target_exposure_krw": None,
        "exposure_cap_krw": None,
        "exposure_cap_applied": False,
        "exposure_cap_source": "none",
        "target_qty": row["target_qty"],
        "allocator_policy_name": "",
        "allocator_policy_version": "",
        "allocator_config_hash": "",
        "strategy_contribution_hash": "",
        "allocation_input_hash": "",
        "reason": "",
        "conflict_resolution": _json_loads_object(str(row["conflict_resolution_json"] or "{}")),
        "authoritative": bool(row["authoritative"]),
        "fail_closed_reason": str(row["fail_closed_reason"] or ""),
        "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
        "exposure_boundary_artifact": default_risk_decision,
        "exposure_boundary_artifact_hash": default_risk_decision["exposure_boundary_artifact_hash"],
        "legacy_non_authoritative_exposure_risk_decision": default_risk_decision,
        "legacy_non_authoritative_exposure_risk_decision_hash": default_risk_decision[
            "exposure_boundary_artifact_hash"
        ],
        "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
    }
    target_json = _json_loads_object(str(row["target_json"] or "{}"))
    if target_json.get("final_portfolio_target_hash") and target_json.get("portfolio_risk_decision"):
        return target_json
    for key in (
        "allocator_policy_name",
        "allocator_policy_version",
        "allocator_config_hash",
        "strategy_contribution_hash",
        "allocation_input_hash",
        "reason",
        "max_target_exposure_krw",
        "pre_cap_weighted_target_exposure_krw",
        "exposure_cap_krw",
        "exposure_cap_applied",
        "exposure_cap_source",
        "risk_budget_semantics",
        "exposure_boundary_artifact",
        "exposure_boundary_artifact_hash",
        "legacy_non_authoritative_exposure_risk_decision",
        "legacy_non_authoritative_exposure_risk_decision_hash",
        "risk_budget_legacy_marker",
    ):
        payload[key] = target_json.get(key, payload[key])
    payload["final_portfolio_target_hash"] = sha256_prefixed(payload)
    return payload


def rebuild_portfolio_target_from_allocation(
    conn: sqlite3.Connection,
    allocation_id: int,
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT *
        FROM portfolio_target
        WHERE allocation_id=?
        ORDER BY pair, id
        """,
        (int(allocation_id),),
    ).fetchall()
    if not rows:
        raise RuntimeError("portfolio_target_not_found")
    if len(rows) != 1:
        raise RuntimeError("portfolio_target_singular_requires_exactly_one_target")
    row = rows[0]
    payload = _portfolio_target_payload_from_row(row)
    recorded = str(row["final_portfolio_target_hash"] or "")
    if recorded and str(payload["final_portfolio_target_hash"]) != recorded:
        raise RuntimeError("portfolio_target_rebuild_hash_mismatch")
    return payload


def replay_portfolio_target_from_allocation(
    conn: sqlite3.Connection,
    allocation_id: int,
) -> str:
    return str(rebuild_portfolio_target_from_allocation(conn, allocation_id)["final_portfolio_target_hash"])


def rebuild_allocation_decision_from_bundle(
    conn: sqlite3.Connection,
    bundle_id: int,
) -> dict[str, Any]:
    allocation = conn.execute(
        "SELECT * FROM portfolio_allocation_decision WHERE bundle_id=?",
        (int(bundle_id),),
    ).fetchone()
    if allocation is None:
        raise RuntimeError("portfolio_allocation_decision_not_found")
    allocation_id = int(allocation["id"])
    contribution_rows = conn.execute(
        """
        SELECT *
        FROM strategy_contribution
        WHERE allocation_id=?
        ORDER BY pair, strategy_instance_id, id
        """,
        (allocation_id,),
    ).fetchall()
    contributions = [_strategy_contribution_payload_from_row(row) for row in contribution_rows]
    contribution_hash = sha256_prefixed(contributions)
    recorded_contribution_hash = str(allocation["strategy_contribution_hash"] or "")
    if recorded_contribution_hash and contribution_hash != recorded_contribution_hash:
        raise RuntimeError("strategy_contribution_rebuild_hash_mismatch")
    target_rows = conn.execute(
        """
        SELECT *
        FROM portfolio_target
        WHERE allocation_id=?
        ORDER BY pair, id
        """,
        (allocation_id,),
    ).fetchall()
    targets = [_portfolio_target_payload_from_row(row) for row in target_rows]
    for row, target in zip(target_rows, targets, strict=True):
        recorded_target_hash = str(row["final_portfolio_target_hash"] or "")
        if recorded_target_hash and str(target["final_portfolio_target_hash"]) != recorded_target_hash:
            raise RuntimeError("portfolio_target_rebuild_hash_mismatch")
    conflict_count = sum(
        int(dict(target.get("conflict_resolution") or {}).get("conflict_count") or 0)
        for target in targets
    )
    conflict_resolution = {
        "policy": _json_loads_object(str(allocation["conflict_resolution_json"] or "{}")).get(
            "policy", "fail_closed_equal_priority"
        ),
        "target_count": len(targets),
        "blocked_target_count": sum(1 for target in targets if not bool(target.get("authoritative"))),
        "conflict_count": conflict_count,
    }
    risk_decision = build_risk_decision_artifact(
        decision_context="portfolio_allocation_decision"
    )
    payload = {
        "schema_version": 1,
        "allocation_input_hash": str(allocation["allocation_input_hash"] or ""),
        "allocator_config_hash": str(allocation["allocator_config_hash"] or ""),
        "strategy_contribution_hash": contribution_hash,
        "targets": targets,
        "contributions": contributions,
        "conflict_resolution": conflict_resolution,
        "reason": str(allocation["reason"] or ""),
        "authoritative": bool(allocation["authoritative"]),
        "primary_block_reason": str(allocation["primary_block_reason"] or ""),
        "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
        "exposure_boundary_artifact": risk_decision,
        "exposure_boundary_artifact_hash": risk_decision["exposure_boundary_artifact_hash"],
        "legacy_non_authoritative_exposure_risk_decision": risk_decision,
        "legacy_non_authoritative_exposure_risk_decision_hash": risk_decision[
            "exposure_boundary_artifact_hash"
        ],
        "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
    }
    payload["allocation_decision_hash"] = sha256_prefixed(payload)
    recorded = str(allocation["allocation_decision_hash"] or "")
    if recorded and str(payload["allocation_decision_hash"]) != recorded:
        raise RuntimeError("portfolio_allocation_decision_rebuild_hash_mismatch")
    return payload


def replay_allocation_decision_from_bundle(
    conn: sqlite3.Connection,
    bundle_id: int,
) -> str:
    return str(rebuild_allocation_decision_from_bundle(conn, bundle_id)["allocation_decision_hash"])


def rebuild_execution_submit_plan_from_execution_plan(
    conn: sqlite3.Connection,
    execution_plan_id: int,
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM execution_plan WHERE id=?",
        (int(execution_plan_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError("execution_plan_not_found")
    if row["execution_submit_plan_json"] is None:
        return None
    payload = _json_loads_object(str(row["execution_submit_plan_json"]))
    payload.update(
        {
            "side": row["submit_plan_side"],
            "qty": row["submit_plan_qty"],
            "notional_krw": row["submit_plan_notional_krw"],
            "idempotency_key": row["submit_plan_idempotency_key"],
            "source": row["submit_plan_source"],
            "authority": row["submit_plan_authority"],
            "submit_expected": bool(row["submit_expected"]),
            "final_action": row["final_action"],
            "block_reason": row["block_reason"],
        }
    )
    replayed = str(payload.get("submit_plan_hash") or "") or sha256_prefixed(payload)
    recorded = row["execution_submit_plan_hash"]
    if recorded is not None and str(recorded) != replayed:
        raise RuntimeError("execution_submit_plan_rebuild_hash_mismatch")
    return payload


def replay_execution_submit_plan_from_execution_plan(
    conn: sqlite3.Connection,
    execution_plan_id: int,
) -> str | None:
    payload = rebuild_execution_submit_plan_from_execution_plan(conn, execution_plan_id)
    if payload is None:
        return None
    return str(payload.get("submit_plan_hash") or "") or sha256_prefixed(payload)


def load_target_position_state(conn: sqlite3.Connection, *, pair: str) -> TargetPositionState | None:
    row = conn.execute(
        """
        SELECT pair, target_exposure_krw, target_qty, last_signal, last_decision_id,
               last_reference_price, updated_ts, target_origin, adoption_reason,
               adopted_broker_qty, adopted_broker_exposure_krw, created_from_signal,
               actual_target_authority, actual_target_authority_scope,
               actual_target_source, runtime_strategy_set_manifest_hash,
               runtime_strategy_decision_bundle_hash, portfolio_allocation_decision_hash,
               portfolio_target_hash, execution_plan_batch_hash,
               execution_submit_plan_hash, actual_target_provenance_hash,
               actual_target_provenance_json
        FROM target_position_state
        WHERE pair=?
        """,
        (str(pair),),
    ).fetchone()
    if row is None:
        return None
    return TargetPositionState(
        pair=str(row["pair"]),
        target_exposure_krw=float(row["target_exposure_krw"] or 0.0),
        target_qty=float(row["target_qty"] or 0.0),
        last_signal=str(row["last_signal"] or "HOLD"),
        last_decision_id=(None if row["last_decision_id"] is None else int(row["last_decision_id"])),
        last_reference_price=float(row["last_reference_price"] or 0.0),
        updated_ts=int(row["updated_ts"] or 0),
        target_origin=str(row["target_origin"] or ""),
        adoption_reason=str(row["adoption_reason"] or ""),
        adopted_broker_qty=(
            None if row["adopted_broker_qty"] is None else float(row["adopted_broker_qty"])
        ),
        adopted_broker_exposure_krw=(
            None
            if row["adopted_broker_exposure_krw"] is None
            else float(row["adopted_broker_exposure_krw"])
        ),
        created_from_signal=str(row["created_from_signal"] or ""),
        actual_target_authority=str(row["actual_target_authority"] or ACTUAL_PAIR_TARGET_AUTHORITY),
        actual_target_authority_scope=str(row["actual_target_authority_scope"] or ACTUAL_PAIR_TARGET_AUTHORITY_SCOPE),
        actual_target_source=str(row["actual_target_source"] or ""),
        runtime_strategy_set_manifest_hash=str(row["runtime_strategy_set_manifest_hash"] or ""),
        runtime_strategy_decision_bundle_hash=str(row["runtime_strategy_decision_bundle_hash"] or ""),
        portfolio_allocation_decision_hash=str(row["portfolio_allocation_decision_hash"] or ""),
        portfolio_target_hash=str(row["portfolio_target_hash"] or ""),
        execution_plan_batch_hash=str(row["execution_plan_batch_hash"] or ""),
        execution_submit_plan_hash=str(row["execution_submit_plan_hash"] or ""),
        actual_target_provenance_hash=str(row["actual_target_provenance_hash"] or ""),
        actual_target_provenance_json=str(row["actual_target_provenance_json"] or "{}"),
    )


def upsert_target_position_state(
    conn: sqlite3.Connection,
    *,
    pair: str,
    target_exposure_krw: float,
    target_qty: float,
    last_signal: str,
    last_decision_id: int | None,
    last_reference_price: float,
    updated_ts: int,
    target_origin: str = "",
    adoption_reason: str = "",
    adopted_broker_qty: float | None = None,
    adopted_broker_exposure_krw: float | None = None,
    created_from_signal: str = "",
    runtime_strategy_set_manifest_hash: str = "",
    runtime_strategy_decision_bundle_hash: str = "",
    portfolio_allocation_decision_hash: str = "",
    portfolio_target_hash: str = "",
    execution_plan_batch_hash: str = "",
    execution_submit_plan_hash: str = "",
    actual_target_source: str = ACTUAL_PAIR_TARGET_SOURCE,
) -> None:
    provenance = build_actual_pair_target_provenance(
        pair=str(pair),
        runtime_strategy_set_manifest_hash=runtime_strategy_set_manifest_hash,
        runtime_strategy_decision_bundle_hash=runtime_strategy_decision_bundle_hash,
        portfolio_allocation_decision_hash=portfolio_allocation_decision_hash,
        portfolio_target_hash=portfolio_target_hash,
        execution_plan_batch_hash=execution_plan_batch_hash,
        execution_submit_plan_hash=execution_submit_plan_hash,
        source=actual_target_source,
    )
    conn.execute(
        """
        INSERT INTO target_position_state(
            pair, target_exposure_krw, target_qty, last_signal, last_decision_id,
            last_reference_price, updated_ts, target_origin, adoption_reason,
            adopted_broker_qty, adopted_broker_exposure_krw, created_from_signal,
            actual_target_authority, actual_target_authority_scope,
            actual_target_source, runtime_strategy_set_manifest_hash,
            runtime_strategy_decision_bundle_hash, portfolio_allocation_decision_hash,
            portfolio_target_hash, execution_plan_batch_hash, execution_submit_plan_hash,
            actual_target_provenance_hash, actual_target_provenance_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pair) DO UPDATE SET
            target_exposure_krw=excluded.target_exposure_krw,
            target_qty=excluded.target_qty,
            last_signal=excluded.last_signal,
            last_decision_id=excluded.last_decision_id,
            last_reference_price=excluded.last_reference_price,
            updated_ts=excluded.updated_ts,
            target_origin=excluded.target_origin,
            adoption_reason=excluded.adoption_reason,
            adopted_broker_qty=excluded.adopted_broker_qty,
            adopted_broker_exposure_krw=excluded.adopted_broker_exposure_krw,
            created_from_signal=excluded.created_from_signal,
            actual_target_authority=excluded.actual_target_authority,
            actual_target_authority_scope=excluded.actual_target_authority_scope,
            actual_target_source=excluded.actual_target_source,
            runtime_strategy_set_manifest_hash=excluded.runtime_strategy_set_manifest_hash,
            runtime_strategy_decision_bundle_hash=excluded.runtime_strategy_decision_bundle_hash,
            portfolio_allocation_decision_hash=excluded.portfolio_allocation_decision_hash,
            portfolio_target_hash=excluded.portfolio_target_hash,
            execution_plan_batch_hash=excluded.execution_plan_batch_hash,
            execution_submit_plan_hash=excluded.execution_submit_plan_hash,
            actual_target_provenance_hash=excluded.actual_target_provenance_hash,
            actual_target_provenance_json=excluded.actual_target_provenance_json
        """,
        (
            str(pair),
            float(target_exposure_krw),
            float(target_qty),
            str(last_signal or "HOLD").upper(),
            None if last_decision_id is None else int(last_decision_id),
            float(last_reference_price),
            int(updated_ts),
            str(target_origin or ""),
            str(adoption_reason or ""),
            None if adopted_broker_qty is None else float(adopted_broker_qty),
            (
                None
                if adopted_broker_exposure_krw is None
                else float(adopted_broker_exposure_krw)
            ),
            str(created_from_signal or ""),
            ACTUAL_PAIR_TARGET_AUTHORITY,
            ACTUAL_PAIR_TARGET_AUTHORITY_SCOPE,
            str(actual_target_source or ACTUAL_PAIR_TARGET_SOURCE),
            str(runtime_strategy_set_manifest_hash or ""),
            str(runtime_strategy_decision_bundle_hash or ""),
            str(portfolio_allocation_decision_hash or ""),
            str(portfolio_target_hash or ""),
            str(execution_plan_batch_hash or ""),
            str(execution_submit_plan_hash or ""),
            str(provenance["actual_target_provenance_hash"]),
            _json_dumps_stable(provenance),
        ),
    )


def upsert_strategy_virtual_target_state(conn: sqlite3.Connection, state: object) -> None:
    from .virtual_target_state import StrategyVirtualTargetState

    if not isinstance(state, StrategyVirtualTargetState):
        raise TypeError("strategy_virtual_target_state_required")
    payload = state.as_dict()
    conn.execute(
        """
        INSERT INTO strategy_virtual_target_state(
            strategy_instance_id, strategy_name, pair, interval, scope_key_hash,
            runtime_contract_hash, virtual_target_exposure_krw, virtual_target_qty,
            lifecycle_state, last_signal, updated_ts, evidence_hash, state_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(strategy_instance_id, pair, interval, scope_key_hash) DO UPDATE SET
            strategy_name=excluded.strategy_name,
            runtime_contract_hash=excluded.runtime_contract_hash,
            virtual_target_exposure_krw=excluded.virtual_target_exposure_krw,
            virtual_target_qty=excluded.virtual_target_qty,
            lifecycle_state=excluded.lifecycle_state,
            last_signal=excluded.last_signal,
            updated_ts=excluded.updated_ts,
            evidence_hash=excluded.evidence_hash,
            state_json=excluded.state_json
        """,
        (
            state.strategy_instance_id,
            state.strategy_name,
            state.pair,
            state.interval,
            state.scope_key_hash,
            state.runtime_contract_hash,
            state.virtual_target_exposure_krw,
            state.virtual_target_qty,
            state.lifecycle_state,
            state.last_signal,
            int(state.updated_ts),
            state.evidence_hash,
            _json_dumps_stable(payload),
        ),
    )


def load_strategy_virtual_target_state(
    conn: sqlite3.Connection,
    *,
    strategy_instance_id: str,
    pair: str,
    interval: str,
    scope_key_hash: str,
):
    from .virtual_target_state import StrategyVirtualTargetState

    row = conn.execute(
        """
        SELECT state_json
        FROM strategy_virtual_target_state
        WHERE strategy_instance_id=? AND pair=? AND interval=? AND scope_key_hash=?
        """,
        (strategy_instance_id, pair, interval, scope_key_hash),
    ).fetchone()
    if row is None:
        return None
    payload = _json_loads_object(row["state_json"])
    return StrategyVirtualTargetState(
        strategy_instance_id=str(payload["strategy_instance_id"]),
        strategy_name=str(payload.get("strategy_name") or ""),
        pair=str(payload["pair"]),
        interval=str(payload["interval"]),
        scope_key_hash=str(payload["scope_key_hash"]),
        runtime_contract_hash=str(payload["runtime_contract_hash"]),
        virtual_target_exposure_krw=float(payload["virtual_target_exposure_krw"]),
        virtual_target_qty=(
            None if payload.get("virtual_target_qty") is None else float(payload["virtual_target_qty"])
        ),
        lifecycle_state=str(payload["lifecycle_state"]),
        last_signal=str(payload["last_signal"]),
        updated_ts=int(payload["updated_ts"]),
        evidence_hash=str(payload.get("evidence_hash") or ""),
    )


def upsert_account_balance(
    conn: sqlite3.Connection,
    *,
    currency: str,
    available: float,
    locked: float,
    updated_ts: int,
    evidence_hash: str = "",
) -> None:
    available_n = float(available)
    locked_n = float(locked)
    if available_n < 0.0 or locked_n < 0.0:
        raise RuntimeError("multi_asset_balance_negative")
    conn.execute(
        """
        INSERT INTO account_balances(currency, available, locked, total, updated_ts, evidence_hash)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(currency) DO UPDATE SET
            available=excluded.available,
            locked=excluded.locked,
            total=excluded.total,
            updated_ts=excluded.updated_ts,
            evidence_hash=excluded.evidence_hash
        """,
        (
            str(currency).strip().upper(),
            available_n,
            locked_n,
            available_n + locked_n,
            int(updated_ts),
            str(evidence_hash or "").strip(),
        ),
    )


def upsert_pair_position(
    conn: sqlite3.Connection,
    *,
    pair: str,
    base_currency: str,
    quote_currency: str,
    available_qty: float,
    locked_qty: float,
    updated_ts: int,
    evidence_hash: str = "",
) -> None:
    available_n = float(available_qty)
    locked_n = float(locked_qty)
    if available_n < 0.0 or locked_n < 0.0:
        raise RuntimeError("multi_asset_position_negative")
    conn.execute(
        """
        INSERT INTO pair_positions(
            pair, base_currency, quote_currency, available_qty, locked_qty, total_qty,
            updated_ts, evidence_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pair) DO UPDATE SET
            base_currency=excluded.base_currency,
            quote_currency=excluded.quote_currency,
            available_qty=excluded.available_qty,
            locked_qty=excluded.locked_qty,
            total_qty=excluded.total_qty,
            updated_ts=excluded.updated_ts,
            evidence_hash=excluded.evidence_hash
        """,
        (
            str(pair).strip(),
            str(base_currency).strip().upper(),
            str(quote_currency).strip().upper(),
            available_n,
            locked_n,
            available_n + locked_n,
            int(updated_ts),
            str(evidence_hash or "").strip(),
        ),
    )


def create_or_get_budget_lock(
    conn: sqlite3.Connection,
    *,
    currency: str,
    pair: str,
    amount: float,
    reason: str,
    created_ts: int,
    idempotency_key: str,
    evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    amount_n = float(amount)
    if amount_n < 0.0:
        raise RuntimeError("budget_lock_negative_amount")
    evidence_payload = {
        "schema_version": 1,
        "lock_table": "budget_locks",
        "currency": str(currency).strip().upper(),
        "pair": str(pair or "").strip(),
        "amount": amount_n,
        "reason": str(reason or "").strip(),
        "idempotency_key": str(idempotency_key or "").strip(),
        "evidence": dict(evidence or {}),
    }
    lock_hash = sha256_prefixed(evidence_payload)
    evidence_hash = sha256_prefixed(evidence_payload)
    conn.execute(
        """
        INSERT OR IGNORE INTO budget_locks(
            lock_hash, currency, pair, amount, status, reason, created_ts, evidence_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lock_hash,
            evidence_payload["currency"],
            evidence_payload["pair"],
            amount_n,
            "active",
            evidence_payload["reason"],
            int(created_ts),
            evidence_hash,
        ),
    )
    row = conn.execute(
        "SELECT lock_hash, status, evidence_hash FROM budget_locks WHERE lock_hash=?",
        (lock_hash,),
    ).fetchone()
    if row is None:
        raise RuntimeError("budget_lock_persist_failed")
    return {
        "lock_hash": str(row["lock_hash"]),
        "lock_type": "quote_budget",
        "lock_status": str(row["status"]),
        "evidence_hash": str(row["evidence_hash"] or ""),
    }


def create_or_get_order_lock(
    conn: sqlite3.Connection,
    *,
    pair: str,
    currency: str,
    amount: float,
    reason: str,
    created_ts: int,
    idempotency_key: str,
    evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    amount_n = float(amount)
    if amount_n < 0.0:
        raise RuntimeError("order_lock_negative_amount")
    evidence_payload = {
        "schema_version": 1,
        "lock_table": "order_locks",
        "pair": str(pair or "").strip(),
        "currency": str(currency).strip().upper(),
        "amount": amount_n,
        "reason": str(reason or "").strip(),
        "idempotency_key": str(idempotency_key or "").strip(),
        "evidence": dict(evidence or {}),
    }
    lock_hash = sha256_prefixed(evidence_payload)
    evidence_hash = sha256_prefixed(evidence_payload)
    conn.execute(
        """
        INSERT OR IGNORE INTO order_locks(
            lock_hash, pair, currency, amount, status, reason, created_ts, evidence_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lock_hash,
            evidence_payload["pair"],
            evidence_payload["currency"],
            amount_n,
            "active",
            evidence_payload["reason"],
            int(created_ts),
            evidence_hash,
        ),
    )
    row = conn.execute(
        "SELECT lock_hash, status, evidence_hash FROM order_locks WHERE lock_hash=?",
        (lock_hash,),
    ).fetchone()
    if row is None:
        raise RuntimeError("order_lock_persist_failed")
    return {
        "lock_hash": str(row["lock_hash"]),
        "lock_type": "base_order",
        "lock_status": str(row["status"]),
        "evidence_hash": str(row["evidence_hash"] or ""),
    }


def multi_asset_ledger_authority_status(conn: sqlite3.Connection) -> dict[str, Any]:
    tables = {
        "account_balances",
        "pair_positions",
        "budget_locks",
        "order_locks",
    }
    existing = {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    missing = sorted(tables.difference(existing))
    counts = {
        table: (
            0
            if table in missing
            else int(conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"])
        )
        for table in tables
    }
    stale_or_missing_evidence: list[str] = []
    if not missing:
        for table in ("account_balances", "pair_positions"):
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table} WHERE COALESCE(updated_ts, 0) <= 0 OR COALESCE(evidence_hash, '') = ''"
            ).fetchone()
            if int(row["count"]) > 0:
                stale_or_missing_evidence.append(table)
        for table in ("budget_locks", "order_locks"):
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS count FROM {table}
                WHERE status NOT IN ('active', 'released', 'submit_failed', 'broker_error', 'reconcile_failed', 'recovered')
                   OR COALESCE(evidence_hash, '') = ''
                """
            ).fetchone()
            if int(row["count"]) > 0:
                stale_or_missing_evidence.append(table)
    authority_verified = bool(not missing and not stale_or_missing_evidence and counts["account_balances"] > 0)
    return {
        "schema_version": 1,
        "authority_model": "multi_asset_ledger_v1",
        "status": "present" if not missing else "missing",
        "authority_verification_status": (
            "verified" if authority_verified else ("present_unverified" if not missing else "missing")
        ),
        "missing_tables": missing,
        "table_counts": counts,
        "evidence_freshness_status": "pass" if not stale_or_missing_evidence else "fail",
        "stale_or_missing_evidence_tables": stale_or_missing_evidence,
        "lock_consistency_status": "pass" if not any(table in stale_or_missing_evidence for table in ("budget_locks", "order_locks")) else "fail",
        "reconcile_status": "not_multi_pair_verified",
        "authority_verified": authority_verified,
        "portfolio_id_1_multi_pair_live_authority": False,
        "live_multi_pair_enablement": "fail_closed_until_scoped_batch_ledger_authority_verified",
        "fail_closed_reason": (
            "multi_asset_ledger_authority_missing"
            if missing
            else "multi_asset_ledger_authority_unverified"
            if not authority_verified
            else "multi_pair_runtime_unsupported"
        ),
    }


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
        fee_accounting_status = str(existing_fill["fee_accounting_status"] or "").strip()
    except (KeyError, IndexError, TypeError):
        fee_accounting_status = ""
    if fee_accounting_status:
        return fee_accounting_status == FILL_FEE_ACCOUNTING_STATUS_FINALIZED
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
            SELECT id, client_order_id, fill_id, fill_ts, price, qty, fee,
                   fee_accounting_status, observed_fee_status, observed_fee_source,
                   observed_fee_confidence, observed_fee_provenance,
                   observed_fee_validation_reason, observed_fee_validation_checks,
                   trade_id
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
        SELECT id, client_order_id, fill_id, fill_ts, price, qty, fee,
               fee_accounting_status, observed_fee_status, observed_fee_source,
               observed_fee_confidence, observed_fee_provenance,
               observed_fee_validation_reason, observed_fee_validation_checks,
               trade_id
        FROM fills
        WHERE client_order_id=? AND fill_ts=? AND ABS(price-?) < 1e-12 AND ABS(qty-?) < 1e-12
        LIMIT 1
        """,
        (client_order_id_text, int(fill_ts), float(price), float(qty)),
    ).fetchone()


def fill_fee_accounting_status(existing_fill: Any | None) -> str:
    if existing_fill is None:
        return ""
    try:
        status = str(existing_fill["fee_accounting_status"] or "").strip()
    except (KeyError, IndexError, TypeError):
        status = ""
    if status:
        return status
    return (
        FILL_FEE_ACCOUNTING_STATUS_FINALIZED
        if existing_fill_fee_complete(existing_fill)
        else FILL_FEE_ACCOUNTING_STATUS_PENDING
    )


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
            WHERE client_order_id=?
              AND fill_id=?
              AND fill_ts=?
              AND ABS(price-?) < 1e-12
              AND ABS(qty-?) < 1e-12
              AND fee > ?
            ORDER BY event_ts ASC, id ASC
            """,
            (
                client_order_id_text,
                fill_id_text,
                int(fill_ts),
                float(price),
                float(qty),
                FEE_ACCOUNTING_COMPLETE_EPS,
            ),
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
              AND fee > ?
            ORDER BY event_ts ASC, id ASC
            """,
            (
                client_order_id_text,
                int(fill_ts),
                float(price),
                float(qty),
                FEE_ACCOUNTING_COMPLETE_EPS,
            ),
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
        authoritative_fill_fee_accounting_status = fill_fee_accounting_status(existing_fill) or None
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

        if latest_accounting_status == "fee_pending" and repair_present:
            canonical_state = "repaired"
            incident_scope = "historical_context"
            active_issue = False
        elif existing_fill is None and latest_accounting_status == "fee_pending":
            canonical_state = "unapplied_principal_pending"
            incident_scope = "active_blocking"
            active_issue = True
        elif authoritative_fill_fee_accounting_status == FILL_FEE_ACCOUNTING_STATUS_PENDING:
            canonical_state = FILL_FEE_ACCOUNTING_STATUS_PENDING
            incident_scope = "active_degraded"
            active_issue = False
        elif authoritative_fill_fee_accounting_status == FILL_FEE_ACCOUNTING_STATUS_BLOCKED:
            canonical_state = FILL_FEE_ACCOUNTING_STATUS_BLOCKED
            incident_scope = "active_blocking"
            active_issue = True
        elif latest_accounting_status == "fee_pending" and final_fee_applied:
            canonical_state = "already_accounted_observation_stale"
            incident_scope = "historical_context"
            active_issue = False
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
                authoritative_fill_fee_accounting_status=authoritative_fill_fee_accounting_status,
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
                    "matching_repair_ids": [int(row["id"]) for row in repairs],
                    "matching_repair_fees": [float(row["fee"]) for row in repairs],
                },
            )
        )
    return verdicts


def summarize_fill_accounting_incident_projection(conn: sqlite3.Connection) -> dict[str, object]:
    verdicts = build_fill_accounting_incident_projection(conn)
    active = [v for v in verdicts if v.active_issue]
    stale = [v for v in verdicts if v.canonical_incident_state == "already_accounted_observation_stale"]
    repaired = [v for v in verdicts if v.canonical_incident_state == "repaired"]
    unapplied = [v for v in verdicts if v.canonical_incident_state == "unapplied_principal_pending"]
    principal_applied_pending = [
        v for v in verdicts if v.canonical_incident_state == FILL_FEE_ACCOUNTING_STATUS_PENDING
    ]
    fee_validation_blocked = [
        v for v in verdicts if v.canonical_incident_state == FILL_FEE_ACCOUNTING_STATUS_BLOCKED
    ]
    finalized = [v for v in verdicts if v.authoritative_fill_fee_accounting_status == FILL_FEE_ACCOUNTING_STATUS_FINALIZED]
    complete = [
        v for v in verdicts if v.latest_observation_accounting_status == "accounting_complete" and not v.active_issue
    ]
    return {
        "projection_kind": "fill_accounting_incident_projection",
        "incident_count": len(verdicts),
        "active_fee_pending_count": len(unapplied),
        "unapplied_principal_pending_count": len(unapplied),
        "principal_applied_fee_pending_count": len(principal_applied_pending),
        "fee_validation_blocked_count": len(fee_validation_blocked),
        "fee_finalized_count": len(finalized),
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
            COALESCE(SUM(CASE WHEN fee_validation_checks LIKE '%"expected_fee_rate_match": false%' THEN 1 ELSE 0 END), 0)
                AS expected_fee_rate_mismatch_count,
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
    fee_source: str = "unknown",
    fee_confidence: str = "unknown",
    accounting_status: str,
    source: str,
    fee_provenance: str | None = None,
    fee_validation_reason: str | None = None,
    fee_validation_checks: dict[str, object] | None = None,
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
    fee_source_text = str(fee_source or "").strip() or "unknown"
    fee_confidence_text = str(fee_confidence or "").strip() or "unknown"
    accounting_status_text = str(accounting_status or "").strip() or "observed"
    source_text = str(source or "").strip() or "unknown"
    fee_provenance_text = str(fee_provenance or "").strip() or None
    fee_validation_reason_text = str(fee_validation_reason or "").strip() or None
    fee_validation_checks_text = (
        json.dumps(fee_validation_checks, ensure_ascii=False, sort_keys=True)
        if isinstance(fee_validation_checks, dict)
        else None
    )
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
            fill_ts, side, price, qty, fee, fee_status, fee_source, fee_confidence,
            accounting_status, source, fee_provenance, fee_validation_reason,
            fee_validation_checks, parse_warnings, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            fee_source_text,
            fee_confidence_text,
            accounting_status_text,
            source_text,
            fee_provenance_text,
            fee_validation_reason_text,
            fee_validation_checks_text,
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
        "fee_source": fee_source_text,
        "fee_confidence": fee_confidence_text,
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
            COALESCE(SUM(CASE WHEN fee_validation_checks LIKE '%"expected_fee_rate_match": false%' THEN 1 ELSE 0 END), 0)
                AS expected_fee_rate_mismatch_count,
            COALESCE(SUM(CASE WHEN fee_status='missing' THEN 1 ELSE 0 END), 0) AS missing_fee_count,
            COALESCE(SUM(CASE WHEN fee_status='zero_reported' THEN 1 ELSE 0 END), 0) AS zero_reported_fee_count,
            COALESCE(SUM(CASE WHEN fee_status IN ('empty', 'invalid', 'unparseable') THEN 1 ELSE 0 END), 0) AS invalid_fee_count
        FROM broker_fill_observations
        """
    ).fetchone()
    last = conn.execute(
        """
        SELECT event_ts, client_order_id, exchange_order_id, fill_id, side, price, qty,
               fee, fee_status, fee_source, fee_confidence, accounting_status, source,
               fee_provenance, fee_validation_reason, fee_validation_checks, parse_warnings,
               raw_payload
        FROM broker_fill_observations
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    fee_evidence_diagnostics: dict[str, Any] = {
        "primary_blocker": "none",
        "affected_client_order_id": None,
        "affected_exchange_order_id": None,
        "affected_fill_id": None,
        "affected_fill_ids": [],
        "order_paid_fee": None,
        "sum_observed_or_applied_fill_fee": None,
        "fee_delta": None,
        "proposed_repair_fee": None,
        "fee_provenance": None,
        "non_authoritative_fill_ids": [],
        "complete_fill_set_available": False,
        "deterministic_allocation_available": False,
        "operator_confirmation_required": False,
        "safe_to_repair": False,
        "why_safe": [],
        "why_not_safe": [],
        "idempotency_key": None,
        "recommended_command": None,
        "expected_after": None,
        "rollback_or_backup": None,
        "post_apply_verification_commands": [],
        "recommended_safe_next_action": "none",
    }
    if last is not None:
        last_client_order_id = str(last["client_order_id"])
        last_exchange_order_id = (
            str(last["exchange_order_id"]) if last["exchange_order_id"] is not None else None
        )
        related = conn.execute(
            """
            SELECT fill_id, fill_ts, side, price, qty, fee, fee_status, accounting_status,
                   fee_source, fee_confidence, fee_provenance, fee_validation_reason,
                   fee_validation_checks, raw_payload
            FROM broker_fill_observations
            WHERE client_order_id=?
              AND COALESCE(exchange_order_id, '')=COALESCE(?, '')
            ORDER BY event_ts ASC, id ASC
            """,
            (last_client_order_id, last_exchange_order_id),
        ).fetchall()
        fill_ids = [
            str(item["fill_id"])
            for item in related
            if item["fill_id"] is not None and str(item["fill_id"]).strip()
        ]
        non_authoritative_fill_ids = [
            str(item["fill_id"])
            for item in related
            if item["fill_id"] is not None
            and str(item["fill_id"]).strip()
            and str(item["accounting_status"] or "") != "accounting_complete"
        ]
        observed_fee_sum = normalize_cash_amount(
            sum(float(item["fee"]) for item in related if item["fee"] is not None)
        )
        order_paid_fee: float | None = None
        complete_fill_set_available = False
        allocated_fee_sum_match = False
        for item in related:
            checks_raw = item["fee_validation_checks"]
            if checks_raw:
                try:
                    checks = json.loads(str(checks_raw))
                except json.JSONDecodeError:
                    checks = {}
                if isinstance(checks, dict):
                    complete_fill_set_available = complete_fill_set_available or bool(
                        checks.get("complete_fill_set")
                    )
                    allocated_fee_sum_match = allocated_fee_sum_match or bool(
                        checks.get("allocated_fee_sum_match")
                    )
            raw_payload = item["raw_payload"]
            if raw_payload and order_paid_fee is None:
                try:
                    payload = json.loads(str(raw_payload))
                except json.JSONDecodeError:
                    payload = {}
                if isinstance(payload, dict):
                    order_fee_fields = payload.get("order_fee_fields")
                    paid_fee_raw = None
                    if isinstance(order_fee_fields, dict):
                        paid_fee_raw = order_fee_fields.get("paid_fee")
                    if paid_fee_raw is None:
                        paid_fee_raw = payload.get("paid_fee")
                    try:
                        order_paid_fee = float(paid_fee_raw) if paid_fee_raw not in (None, "") else None
                    except (TypeError, ValueError):
                        order_paid_fee = None
        fee_delta = None
        if order_paid_fee is not None:
            fee_delta = normalize_cash_amount(float(order_paid_fee) - float(observed_fee_sum))
        has_fee_pending = bool(non_authoritative_fill_ids)
        deterministic_allocation_available = bool(
            has_fee_pending
            and order_paid_fee is not None
            and complete_fill_set_available
            and (allocated_fee_sum_match or abs(float(fee_delta or 0.0)) > FEE_ACCOUNTING_COMPLETE_EPS)
        )
        if has_fee_pending:
            primary_blocker = "fee_evidence_non_authoritative"
            if deterministic_allocation_available:
                recommended_action = "repair-plan can derive a deterministic fee-pending accounting repair candidate"
            elif order_paid_fee is not None:
                recommended_action = "review retained order_paid_fee and fill evidence before manual repair"
            else:
                recommended_action = "obtain authoritative exchange fee evidence before repair or resume"
        else:
            primary_blocker = "none"
            recommended_action = "none"
        proposed_fee = normalize_cash_amount(fee_delta or 0.0) if fee_delta is not None else None
        affected_fill_id = non_authoritative_fill_ids[0] if len(non_authoritative_fill_ids) == 1 else None
        idempotency_key = (
            f"fee_pending_accounting_repair:{last_client_order_id}:{affected_fill_id}:{proposed_fee:.2f}"
            if affected_fill_id and proposed_fee is not None
            else None
        )
        why_not_safe: list[str] = []
        why_safe: list[str] = []
        pending_row = None
        if len(non_authoritative_fill_ids) != 1:
            why_not_safe.append(f"pending_fill_count={len(non_authoritative_fill_ids)}")
        elif affected_fill_id:
            pending_row = next((item for item in related if str(item["fill_id"] or "") == affected_fill_id), None)
            why_safe.append("exact_pending_fill_identified")
        if not deterministic_allocation_available:
            why_not_safe.append("deterministic_allocation_unavailable")
        else:
            why_safe.append("deterministic_order_level_paid_fee_allocation_available")
        if not complete_fill_set_available:
            why_not_safe.append("complete_fill_set_missing")
        else:
            why_safe.append("complete_fill_set_available")
        if order_paid_fee is None:
            why_not_safe.append("order_paid_fee_missing")
        if proposed_fee is None or proposed_fee <= FEE_ACCOUNTING_COMPLETE_EPS:
            why_not_safe.append("proposed_fee_not_positive")
        if order_paid_fee is not None and proposed_fee is not None:
            reconstructed = normalize_cash_amount(observed_fee_sum + proposed_fee)
            if abs(reconstructed - normalize_cash_amount(order_paid_fee)) > FEE_PENDING_REPAIR_FEE_ROUNDING_TOLERANCE_KRW:
                why_not_safe.append("fee_delta_mismatches_order_paid_fee")
            else:
                why_safe.append("proposed_fee_reconciles_to_order_paid_fee")
        if pending_row is not None and proposed_fee is not None:
            notional = float(pending_row["price"] or 0.0) * float(pending_row["qty"] or 0.0)
            expected_fee = normalize_cash_amount(notional * float(settings.LIVE_FEE_RATE_ESTIMATE))
            tolerance = max(FEE_PENDING_REPAIR_FEE_ROUNDING_TOLERANCE_KRW, expected_fee * 0.25)
            if notional <= 0.0:
                why_not_safe.append("pending_fill_notional_invalid")
            elif abs(float(proposed_fee) - expected_fee) > tolerance:
                why_not_safe.append("proposed_fee_outside_fee_rate_tolerance")
            else:
                why_safe.append("proposed_fee_within_fee_rate_tolerance")
        duplicate_repair_count = 0
        if affected_fill_id and proposed_fee is not None:
            duplicate_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM fee_pending_accounting_repairs
                WHERE client_order_id=?
                  AND fill_id=?
                  AND ABS(fee-?) <= ?
                """,
                (
                    last_client_order_id,
                    affected_fill_id,
                    float(proposed_fee),
                    FEE_PENDING_REPAIR_FEE_ROUNDING_TOLERANCE_KRW,
                ),
            ).fetchone()
            duplicate_repair_count = int(duplicate_row["cnt"] or 0) if duplicate_row else 0
            if duplicate_repair_count:
                why_not_safe.append("duplicate_fee_pending_accounting_repair_exists")
            else:
                why_safe.append("no_duplicate_fee_pending_accounting_repair")
        order_blocker_row = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'ACCOUNTING_PENDING', 'CANCEL_REQUESTED') THEN 1 ELSE 0 END), 0)
                    AS unresolved_open_order_count,
                COALESCE(SUM(CASE WHEN status='SUBMIT_UNKNOWN' THEN 1 ELSE 0 END), 0) AS submit_unknown_count,
                COALESCE(SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END), 0) AS recovery_required_count
            FROM orders
            """
        ).fetchone()
        unresolved_open_order_count = int(order_blocker_row["unresolved_open_order_count"] or 0) if order_blocker_row else 0
        submit_unknown_count = int(order_blocker_row["submit_unknown_count"] or 0) if order_blocker_row else 0
        recovery_required_count = int(order_blocker_row["recovery_required_count"] or 0) if order_blocker_row else 0
        if unresolved_open_order_count or submit_unknown_count or recovery_required_count:
            why_not_safe.append(
                "unresolved_orders_present:"
                f"open={unresolved_open_order_count},submit_unknown={submit_unknown_count},"
                f"recovery_required={recovery_required_count}"
            )
        else:
            why_safe.append("no_unresolved_orders")
        safe_to_repair = bool(has_fee_pending and not why_not_safe)
        recommended_command = None
        if safe_to_repair and affected_fill_id and proposed_fee is not None:
            recommended_command = (
                "uv run python bot.py fee-pending-accounting-repair "
                f"--client-order-id {last_client_order_id} "
                f"--fill-id {affected_fill_id} "
                f"--fee {proposed_fee:.2f} "
                f"--fee-provenance {FEE_PENDING_REPAIR_PROVENANCE_ORDER_LEVEL_ALLOCATED} "
                "--apply --yes"
            )
        fee_evidence_diagnostics = {
            "primary_blocker": primary_blocker,
            "affected_client_order_id": last_client_order_id,
            "affected_exchange_order_id": last_exchange_order_id,
            "affected_fill_id": affected_fill_id,
            "affected_fill_ids": fill_ids,
            "order_paid_fee": order_paid_fee,
            "sum_observed_or_applied_fill_fee": observed_fee_sum if related else None,
            "fee_delta": fee_delta,
            "proposed_repair_fee": proposed_fee,
            "fee_provenance": FEE_PENDING_REPAIR_PROVENANCE_ORDER_LEVEL_ALLOCATED if safe_to_repair else None,
            "non_authoritative_fill_ids": non_authoritative_fill_ids,
            "complete_fill_set_available": bool(complete_fill_set_available),
            "deterministic_allocation_available": deterministic_allocation_available,
            "operator_confirmation_required": bool(has_fee_pending and not deterministic_allocation_available),
            "safe_to_repair": safe_to_repair,
            "why_safe": list(dict.fromkeys(why_safe)),
            "why_not_safe": list(dict.fromkeys(why_not_safe)),
            "duplicate_repair_count": duplicate_repair_count,
            "unresolved_open_order_count": unresolved_open_order_count,
            "submit_unknown_count": submit_unknown_count,
            "recovery_required_count": recovery_required_count,
            "idempotency_key": idempotency_key,
            "recommended_command": recommended_command,
            "expected_after": (
                "fee finalization recorded through fee_pending_accounting_repair; "
                "broker fill unresolved fee-pending count should decrease"
                if safe_to_repair
                else None
            ),
            "rollback_or_backup": f"backup/{settings.MODE}/db/ via scripts/backup_sqlite.sh before apply",
            "post_apply_verification_commands": [
                "uv run python bot.py audit-ledger",
                "uv run python bot.py recovery-report",
                "uv run python bot.py restart-checklist",
                "uv run python bot.py health",
            ],
            "recommended_safe_next_action": recommended_action,
        }
    return {
        "observation_count": int(row["observation_count"] if row else 0),
        "fee_pending_count": int(row["fee_pending_count"] if row else 0),
        "accounting_complete_count": int(row["accounting_complete_count"] if row else 0),
        "fee_candidate_order_level_count": int(row["fee_candidate_order_level_count"] if row else 0),
        "expected_fee_rate_mismatch_count": int(row["expected_fee_rate_mismatch_count"] if row else 0),
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
        "last_fee_source": str(last["fee_source"]) if last is not None else None,
        "last_fee_confidence": str(last["fee_confidence"]) if last is not None else None,
        "last_accounting_status": str(last["accounting_status"]) if last is not None else None,
        "last_source": str(last["source"]) if last is not None else None,
        "last_fee_provenance": str(last["fee_provenance"]) if last is not None and last["fee_provenance"] is not None else None,
        "last_fee_validation_reason": str(last["fee_validation_reason"]) if last is not None and last["fee_validation_reason"] is not None else None,
        "last_fee_validation_checks": str(last["fee_validation_checks"]) if last is not None and last["fee_validation_checks"] is not None else None,
        "last_parse_warnings": str(last["parse_warnings"]) if last is not None and last["parse_warnings"] is not None else None,
        "fee_evidence_diagnostics": fee_evidence_diagnostics,
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


_UNTRUSTED_ORDER_RULE_FALLBACK_REASON_PREFIXES = (
    "AUTH_",
    "ACCOUNTS_AUTH_",
    "TRANSPORT_",
    "SERVER_",
    "RATE_LIMITED",
    "TEMPORARY",
)
_TRUSTED_ORDER_RULE_SOURCE_MODES = frozenset({"exchange", "merged"})


def order_rule_snapshot_trust_level(record: OrderRuleSnapshotRecord | None) -> str:
    if record is None:
        return "missing"
    source_mode = str(record.source_mode or "").strip().lower()
    reason = str(record.fallback_reason_code or "").strip().upper()
    if bool(record.fallback_used) and any(reason.startswith(prefix) for prefix in _UNTRUSTED_ORDER_RULE_FALLBACK_REASON_PREFIXES):
        return "auth_failed_quarantine"
    if bool(record.fallback_used):
        return "local_fallback_untrusted"
    if source_mode not in _TRUSTED_ORDER_RULE_SOURCE_MODES:
        return "untrusted_source_mode"
    if reason and any(reason.startswith(prefix) for prefix in _UNTRUSTED_ORDER_RULE_FALLBACK_REASON_PREFIXES):
        return "quarantined_failure"
    return "trusted_exchange_verified"


def order_rule_snapshot_is_trusted_baseline(record: OrderRuleSnapshotRecord | None) -> bool:
    return order_rule_snapshot_trust_level(record) == "trusted_exchange_verified"


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


def fetch_latest_trusted_order_rule_snapshot(
    conn: sqlite3.Connection,
    *,
    market: str | None = None,
) -> OrderRuleSnapshotRecord | None:
    params: tuple[str, ...]
    market_clause = ""
    if market:
        market_clause = "AND market=?"
        params = (str(market),)
    else:
        params = ()
    rows = conn.execute(
        f"""
        SELECT market, fetched_ts, source_mode, fallback_used, fallback_reason_code,
               fallback_reason_summary, rule_signature, rules_json, source_json
        FROM order_rule_snapshots
        WHERE COALESCE(fallback_used, 0)=0
          AND COALESCE(source_mode, '') IN ('exchange', 'merged')
          AND (
              COALESCE(fallback_reason_code, '')=''
              AND COALESCE(fallback_reason_summary, '')=''
          )
          {market_clause}
        ORDER BY fetched_ts DESC, id DESC
        LIMIT 1
        """,
        params,
    ).fetchall()
    if not rows:
        return None
    row = rows[0]
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
