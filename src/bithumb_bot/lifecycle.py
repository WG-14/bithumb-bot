from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, replace
from typing import Any

from .config import settings
from .db_core import normalize_asset_qty
from .dust import (
    DUST_TRACKING_LOT_STATE,
    OPEN_EXPOSURE_LOT_STATE,
    DustClassification,
    DustDisplayContext,
    DustState,
    ExecutableLot,
    build_dust_display_context,
    build_executable_lot,
    is_strictly_below_min_qty,
)
from .lot_model import build_market_lot_rules, lot_count_to_qty, qty_to_executable_lot_count
from .markets import parse_user_market_input

OPEN_POSITION_STATE = OPEN_EXPOSURE_LOT_STATE
DUST_TRACKING_STATE = DUST_TRACKING_LOT_STATE


_ENTRY_DECISION_FALLBACK_LOOKBACK_MS = 15 * 60 * 1000
# BUY fill attribution states are persisted in entry_decision_linkage.
# The order below matters: direct linked decision takes precedence over
# fallback classification, and fallback classification must stay specific
# enough to explain why the BUY fill was or was not linked.
ENTRY_DECISION_LINKAGE_DIRECT = "direct"
ENTRY_DECISION_LINKAGE_STRICT_SINGLE_FALLBACK = "fallback_strict_match"
ENTRY_DECISION_LINKAGE_AMBIGUOUS_MULTI_CANDIDATE = "ambiguous_multi_candidate"
ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH = "unattributed_no_strict_match"
ENTRY_DECISION_LINKAGE_UNATTRIBUTED_MISSING_STRATEGY = "unattributed_missing_strategy"
ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED = "degraded_recovery_unattributed"
LOT_SEMANTIC_VERSION_V1 = 1


@dataclass(frozen=True)
class LotDefinitionSnapshot:
    semantic_version: int | None
    internal_lot_size: float | None
    min_qty: float | None
    qty_step: float | None
    min_notional_krw: float | None
    max_qty_decimals: int | None
    source_mode: str

    @property
    def is_authoritative(self) -> bool:
        return (
            self.semantic_version is not None
            and self.internal_lot_size is not None
            and float(self.internal_lot_size) > 0.0
        )

    def as_dict(self) -> dict[str, float | int | str | bool | None]:
        return {
            "semantic_version": None if self.semantic_version is None else int(self.semantic_version),
            "internal_lot_size": None if self.internal_lot_size is None else float(self.internal_lot_size),
            "min_qty": None if self.min_qty is None else float(self.min_qty),
            "qty_step": None if self.qty_step is None else float(self.qty_step),
            "min_notional_krw": None if self.min_notional_krw is None else float(self.min_notional_krw),
            "max_qty_decimals": None if self.max_qty_decimals is None else int(self.max_qty_decimals),
            "source_mode": str(self.source_mode or ""),
            "is_authoritative": bool(self.is_authoritative),
        }


@dataclass(frozen=True)
class PositionLotSnapshot:
    """Recovery-facing lot summary with explicit lot-native exposure counts.

    The executable semantic authority is the lot state/count layer. The qty
    fields remain available as raw or compatibility quantities for accounting,
    reporting, and broker reconciliation.
    """

    raw_open_exposure_qty: float
    executable_open_exposure_qty: float
    dust_tracking_qty: float
    raw_total_asset_qty: float
    open_lot_count: int
    dust_tracking_lot_count: int
    effective_min_trade_qty: float
    exit_non_executable_reason: str
    position_semantic_basis: str
    lot_definition: LotDefinitionSnapshot | None = None

    @property
    def total_holdings_qty(self) -> float:
        return float(self.raw_total_asset_qty)

    @property
    def executable_exposure_qty(self) -> float:
        return float(self.executable_open_exposure_qty)

    @property
    def tracked_dust_qty(self) -> float:
        return float(self.dust_tracking_qty)

    @property
    def semantic_basis(self) -> str:
        return str(self.position_semantic_basis or "lot-native")

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "semantic_basis": self.semantic_basis,
            "position_semantic_basis": self.semantic_basis,
            "raw_open_exposure_qty": float(self.raw_open_exposure_qty),
            "raw_total_asset_qty": float(self.raw_total_asset_qty),
            "total_holdings_qty": float(self.total_holdings_qty),
            "executable_open_exposure_qty": float(self.executable_open_exposure_qty),
            "executable_exposure_qty": float(self.executable_exposure_qty),
            "dust_tracking_qty": float(self.dust_tracking_qty),
            "tracked_dust_qty": float(self.tracked_dust_qty),
            "open_exposure_lot_count": int(self.open_lot_count),
            "open_lot_count": int(self.open_lot_count),
            "dust_tracking_lot_count": int(self.dust_tracking_lot_count),
            "effective_min_trade_qty": float(self.effective_min_trade_qty),
            "exit_non_executable_reason": self.exit_non_executable_reason,
        }
        if self.lot_definition is not None:
            payload["lot_definition"] = self.lot_definition.as_dict()
            payload["lot_semantic_version"] = self.lot_definition.semantic_version
            payload["internal_lot_size"] = self.lot_definition.internal_lot_size
        return payload


@dataclass(frozen=True)
class ResidualLotRowSnapshot:
    lot_id: int
    entry_trade_id: int
    qty_open: float
    position_state: str
    source_mode: str
    entry_decision_linkage: str
    internal_lot_size: float
    lot_min_qty: float
    lot_min_notional_krw: float
    near_lot_delta: float | None
    below_exchange_min_qty: bool
    below_exchange_min_notional: bool
    estimated_notional_krw: float | None
    classes: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "lot_id": int(self.lot_id),
            "entry_trade_id": int(self.entry_trade_id),
            "qty_open": float(self.qty_open),
            "position_state": str(self.position_state or ""),
            "source_mode": str(self.source_mode or ""),
            "entry_decision_linkage": str(self.entry_decision_linkage or ""),
            "internal_lot_size": float(self.internal_lot_size),
            "lot_min_qty": float(self.lot_min_qty),
            "lot_min_notional_krw": float(self.lot_min_notional_krw),
            "near_lot_delta": (
                None if self.near_lot_delta is None else float(self.near_lot_delta)
            ),
            "below_exchange_min_qty": bool(self.below_exchange_min_qty),
            "below_exchange_min_notional": bool(self.below_exchange_min_notional),
            "estimated_notional_krw": (
                None if self.estimated_notional_krw is None else float(self.estimated_notional_krw)
            ),
            "classes": list(self.classes),
        }


@dataclass(frozen=True)
class ResidualInventorySnapshot:
    residual_qty: float
    residual_notional_krw: float | None
    residual_lot_count: int
    residual_classes: tuple[str, ...]
    exchange_sellable: bool
    strategy_sellable: bool
    material_residual: bool
    rows: tuple[ResidualLotRowSnapshot, ...]

    @property
    def explainable(self) -> bool:
        if not self.rows:
            return False
        unresolved_classes = {"UNCLASSIFIED_RESIDUAL"}
        allowed_linkages = {"direct", "degraded_recovery_unattributed"}
        allowed_sources = {
            "ledger",
            "full_projection_rebuild_portfolio_anchor",
            "portfolio_anchored_repair",
        }
        return all(
            all(value not in unresolved_classes for value in row.classes)
            and str(row.entry_decision_linkage or "") in allowed_linkages
            and str(row.source_mode or "") in allowed_sources
            for row in self.rows
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "residual_qty": float(self.residual_qty),
            "residual_notional_krw": (
                None if self.residual_notional_krw is None else float(self.residual_notional_krw)
            ),
            "residual_lot_count": int(self.residual_lot_count),
            "residual_classes": list(self.residual_classes),
            "exchange_sellable": bool(self.exchange_sellable),
            "strategy_sellable": bool(self.strategy_sellable),
            "material_residual": bool(self.material_residual),
            "explainable": bool(self.explainable),
            "rows": [row.as_dict() for row in self.rows],
        }


@dataclass(frozen=True)
class ProjectionReplayResult:
    pair: str
    replayed_trade_count: int
    replayed_buy_count: int
    replayed_sell_count: int
    deleted_open_position_lot_count: int
    deleted_trade_lifecycle_count: int
    lot_snapshot_before: PositionLotSnapshot
    lot_snapshot_after: PositionLotSnapshot

    def as_dict(self) -> dict[str, object]:
        return {
            "pair": self.pair,
            "replayed_trade_count": int(self.replayed_trade_count),
            "replayed_buy_count": int(self.replayed_buy_count),
            "replayed_sell_count": int(self.replayed_sell_count),
            "deleted_open_position_lot_count": int(self.deleted_open_position_lot_count),
            "deleted_trade_lifecycle_count": int(self.deleted_trade_lifecycle_count),
            "lot_snapshot_before": self.lot_snapshot_before.as_dict(),
            "lot_snapshot_after": self.lot_snapshot_after.as_dict(),
        }


@dataclass(frozen=True)
class ExecutionQuantityAuthority:
    source: str
    submitted_qty: float
    open_exposure_qty: float | None
    dust_tracking_qty: float | None
    terminal_asset_after: float | None
    terminal_flat: bool
    decision_reason_code: str | None
    evidence_source: str

    @property
    def is_target_delta_terminal_flat(self) -> bool:
        return (
            self.source == "target_delta"
            and self.terminal_flat
            and self.submitted_qty > 1e-12
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "submitted_qty": float(self.submitted_qty),
            "open_exposure_qty": self.open_exposure_qty,
            "dust_tracking_qty": self.dust_tracking_qty,
            "terminal_asset_after": self.terminal_asset_after,
            "terminal_flat": bool(self.terminal_flat),
            "decision_reason_code": self.decision_reason_code,
            "evidence_source": self.evidence_source,
        }


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_float(*values: object) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return normalize_asset_qty(float(value))
        except (TypeError, ValueError):
            continue
    return None


def _first_text(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def resolve_execution_quantity_authority(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    trade_id: int | None = None,
) -> ExecutionQuantityAuthority:
    """Resolve narrow SELL quantity authority for terminal-flat projection closure.

    This intentionally avoids replacing lot-native SELL authority. It only
    recognizes target-delta terminal-flat evidence so lifecycle replay can close
    dust projection rows that were included in a broker-verified target-delta
    liquidation fill.
    """

    trade_row = None
    if trade_id is not None:
        trade_row = conn.execute(
            """
            SELECT id, side, qty, asset_after, client_order_id
            FROM trades
            WHERE id=?
            LIMIT 1
            """,
            (int(trade_id),),
        ).fetchone()
    if trade_row is None:
        trade_row = conn.execute(
            """
            SELECT id, side, qty, asset_after, client_order_id
            FROM trades
            WHERE client_order_id=?
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            (str(client_order_id),),
        ).fetchone()

    order_row = conn.execute(
        """
        SELECT
            client_order_id, side, qty_req, qty_filled, final_submitted_qty,
            decision_reason_code
        FROM orders
        WHERE client_order_id=?
        LIMIT 1
        """,
        (str(client_order_id),),
    ).fetchone()
    event_row = conn.execute(
        """
        SELECT
            submit_evidence, qty, final_submitted_qty, decision_reason_code,
            submission_reason_code, event_type
        FROM order_events
        WHERE client_order_id=?
          AND submit_evidence IS NOT NULL
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """,
        (str(client_order_id),),
    ).fetchone()
    evidence = _json_object(_row_value(event_row, "submit_evidence", 0)) if event_row is not None else {}

    decision_reason_code = _first_text(
        evidence.get("decision_reason_code"),
        evidence.get("decision_reason"),
        _row_value(order_row, "decision_reason_code", 5),
        _row_value(event_row, "decision_reason_code", 3),
        _row_value(event_row, "submission_reason_code", 4),
    )
    submit_qty_source = _first_text(
        evidence.get("sell_qty_basis_source"),
        evidence.get("submit_qty_source"),
        evidence.get("authority"),
        evidence.get("source"),
    )
    source = "target_delta" if (
        submit_qty_source == "target_position_delta"
        or evidence.get("authority") == "target_position_delta"
        or evidence.get("source") == "target_delta"
        or decision_reason_code == "target_delta_rebalance"
    ) else "lot_native"

    submitted_qty = _first_float(
        evidence.get("final_submitted_qty"),
        evidence.get("order_qty"),
        evidence.get("normalized_qty"),
        evidence.get("observed_submit_payload_qty"),
        evidence.get("submit_payload_qty"),
        _row_value(order_row, "final_submitted_qty", 4),
        _row_value(order_row, "qty_filled", 3),
        _row_value(order_row, "qty_req", 2),
        _row_value(event_row, "final_submitted_qty", 2),
        _row_value(event_row, "qty", 1),
        _row_value(trade_row, "qty", 2),
    ) or 0.0
    open_exposure_qty = _first_float(
        evidence.get("sell_open_exposure_qty"),
        evidence.get("open_exposure_qty"),
    )
    dust_tracking_qty = _first_float(
        evidence.get("sell_dust_tracking_qty"),
        evidence.get("dust_tracking_qty"),
    )
    terminal_asset_after = _first_float(_row_value(trade_row, "asset_after", 3))
    trade_side = str(_row_value(trade_row, "side", 1) or "").upper()
    terminal_flat = bool(trade_side == "SELL" and terminal_asset_after is not None and abs(terminal_asset_after) <= 1e-12)
    evidence_source = "order_events.submit_evidence" if evidence else (
        "orders+trades" if order_row is not None or trade_row is not None else "missing"
    )
    return ExecutionQuantityAuthority(
        source=source,
        submitted_qty=float(submitted_qty),
        open_exposure_qty=open_exposure_qty,
        dust_tracking_qty=dust_tracking_qty,
        terminal_asset_after=terminal_asset_after,
        terminal_flat=terminal_flat,
        decision_reason_code=decision_reason_code,
        evidence_source=evidence_source,
    )


def _lot_definition_from_rules(*, lot_rules: object) -> LotDefinitionSnapshot:
    return LotDefinitionSnapshot(
        semantic_version=LOT_SEMANTIC_VERSION_V1,
        internal_lot_size=float(getattr(lot_rules, "lot_size", 0.0) or 0.0),
        min_qty=float(getattr(lot_rules, "min_qty", 0.0) or 0.0),
        qty_step=float(getattr(lot_rules, "qty_step", 0.0) or 0.0),
        min_notional_krw=float(getattr(lot_rules, "min_notional_krw", 0.0) or 0.0),
        max_qty_decimals=int(getattr(lot_rules, "max_qty_decimals", 0) or 0),
        source_mode=str(getattr(lot_rules, "source_mode", "ledger") or "ledger"),
    )


def _build_fill_lot_rules(*, pair: str, market_price: float) -> object:
    """Build deterministic lot rules for fill lifecycle accounting.

    The lifecycle layer must not depend on a live order-rules fetch to split or
    consume lot-native exposure. Use the local configuration fallback inputs so
    ledger semantics stay stable even in offline tests and recovery flows.
    """

    fallback_rules = type(
        "_LifecycleLotRules",
        (object,),
        {
            "min_qty": float(settings.LIVE_MIN_ORDER_QTY),
            "qty_step": float(settings.LIVE_ORDER_QTY_STEP),
            "min_notional_krw": float(settings.MIN_ORDER_NOTIONAL_KRW),
            "max_qty_decimals": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        },
    )()
    return build_market_lot_rules(
        market_id=str(pair),
        market_price=float(market_price),
        rules=fallback_rules,
        source_mode="ledger",
    )


def _read_fill_lot_rules_override(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
) -> dict[str, float | int] | None:
    row = conn.execute(
        """
        SELECT
            COALESCE(f.internal_lot_size, o.internal_lot_size) AS internal_lot_size,
            COALESCE(o.effective_min_trade_qty, f.internal_lot_size, o.internal_lot_size) AS effective_min_trade_qty,
            COALESCE(o.qty_step, f.internal_lot_size, o.internal_lot_size) AS qty_step,
            COALESCE(o.min_notional_krw, 0.0) AS min_notional_krw,
            COALESCE(o.executable_lot_count, f.executable_lot_count, 0) AS executable_lot_count,
            COALESCE(o.intended_lot_count, f.intended_lot_count, 0) AS intended_lot_count
        FROM orders o
        LEFT JOIN fills f
          ON f.client_order_id=o.client_order_id
         AND (
              (? IS NOT NULL AND f.fill_id=?)
              OR (
                    f.fill_ts=?
                AND ABS(f.price-?) < 1e-12
                AND ABS(f.qty-?) < 1e-12
              )
         )
        WHERE o.client_order_id=?
        ORDER BY f.id DESC
        LIMIT 1
        """,
        (
            fill_id,
            fill_id,
            int(fill_ts),
            float(price),
            float(qty),
            str(client_order_id),
        ),
    ).fetchone()
    if row is None:
        return None
    internal_lot_size = float(row["internal_lot_size"] or 0.0)
    executable_lot_count = int(row["executable_lot_count"] or 0)
    if internal_lot_size <= 1e-12 or executable_lot_count <= 0:
        return None
    return {
        "internal_lot_size": internal_lot_size,
        "effective_min_trade_qty": max(0.0, float(row["effective_min_trade_qty"] or internal_lot_size)),
        "qty_step": max(0.0, float(row["qty_step"] or internal_lot_size)),
        "min_notional_krw": max(0.0, float(row["min_notional_krw"] or 0.0)),
        "executable_lot_count": executable_lot_count,
        "intended_lot_count": int(row["intended_lot_count"] or executable_lot_count),
    }


def _row_executable_lot_count(row: object, *, qty_open: float, lot_rules: object) -> int:
    raw_count = int(_row_value(row, "executable_lot_count", 7) or 0)
    if raw_count > 0:
        return raw_count
    # Do not infer executable-lot authority from qty alone. Legacy rows that
    # lack an executable lot count must fail closed rather than silently
    # recreating executable exposure semantics.
    return 0


def _row_dust_tracking_lot_count(row: object, *, qty_open: float) -> int:
    raw_count = int(_row_value(row, "dust_tracking_lot_count", 8) or 0)
    if raw_count > 0:
        return raw_count
    # Dust tracking is operator evidence only; qty without explicit dust state
    # is not authoritative enough to recreate dust semantics.
    return 0


def _row_value(row: object, key: str, index: int) -> object | None:
    """Read a SQLite result row by key when available, otherwise by position.

    Lifecycle paths can be called with either ``sqlite3.Row`` or plain tuples
    depending on how the connection was created. Keep those callers working
    without forcing a global row-factory policy here.
    """

    if row is None:
        return None
    if hasattr(row, "keys"):
        try:
            return row[key]  # type: ignore[index]
        except (KeyError, IndexError, TypeError):
            pass
    try:
        return row[index]  # type: ignore[index]
    except (IndexError, KeyError, TypeError):
        return None


def _row_float_local(row: Any, key: str, default: float = 0.0) -> float:
    value = _row_value(row, key, -1)
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return default


def _row_int_local(row: Any, key: str, default: int = 0) -> int:
    value = _row_value(row, key, -1)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _row_text_local(row: Any, key: str, default: str = "") -> str:
    value = _row_value(row, key, -1)
    return str(value or default)


def _read_authoritative_lot_definition_snapshot(
    conn: sqlite3.Connection,
    *,
    pair: str,
) -> LotDefinitionSnapshot | None:
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT
                lot_semantic_version,
                internal_lot_size,
                lot_min_qty,
                lot_qty_step,
                lot_min_notional_krw,
                lot_max_qty_decimals,
                lot_rule_source_mode
            FROM open_position_lots
            WHERE pair=?
              AND qty_open > 1e-12
              AND COALESCE(position_semantic_basis, '')='lot-native'
              AND (
                    COALESCE(executable_lot_count, 0) > 0
                    OR COALESCE(dust_tracking_lot_count, 0) > 0
                  )
            """,
            (str(pair),),
        ).fetchall()
    except sqlite3.OperationalError:
        return None

    if not rows:
        return None

    snapshots: list[LotDefinitionSnapshot] = []
    for row in rows:
        semantic_version_raw = _row_value(row, "lot_semantic_version", 0)
        internal_lot_size_raw = _row_value(row, "internal_lot_size", 1)
        min_qty_raw = _row_value(row, "lot_min_qty", 2)
        qty_step_raw = _row_value(row, "lot_qty_step", 3)
        min_notional_krw_raw = _row_value(row, "lot_min_notional_krw", 4)
        max_qty_decimals_raw = _row_value(row, "lot_max_qty_decimals", 5)
        source_mode_raw = _row_value(row, "lot_rule_source_mode", 6)
        snapshot = LotDefinitionSnapshot(
            semantic_version=None if semantic_version_raw is None else int(semantic_version_raw),
            internal_lot_size=None if internal_lot_size_raw is None else float(internal_lot_size_raw),
            min_qty=None if min_qty_raw is None else float(min_qty_raw),
            qty_step=None if qty_step_raw is None else float(qty_step_raw),
            min_notional_krw=None if min_notional_krw_raw is None else float(min_notional_krw_raw),
            max_qty_decimals=None if max_qty_decimals_raw is None else int(max_qty_decimals_raw),
            source_mode=str(source_mode_raw or ""),
        )
        has_any_snapshot_metadata = any(
            value not in (None, "")
            for value in (
                semantic_version_raw,
                internal_lot_size_raw,
                min_qty_raw,
                qty_step_raw,
                min_notional_krw_raw,
                max_qty_decimals_raw,
                source_mode_raw,
            )
        )
        if has_any_snapshot_metadata and not snapshot.is_authoritative:
            return None
        if snapshot.is_authoritative:
            snapshots.append(snapshot)

    if snapshots:
        first = snapshots[0]
        if not any(snapshot != first for snapshot in snapshots[1:]):
            return first

    evidence_snapshot = _read_authoritative_lot_definition_from_accounted_buy_evidence(
        conn,
        pair=str(pair),
    )
    if evidence_snapshot is not None:
        return evidence_snapshot

    try:
        executable_rows = conn.execute(
            """
            SELECT qty_open, executable_lot_count
            FROM open_position_lots
            WHERE pair=?
              AND qty_open > 1e-12
              AND COALESCE(position_semantic_basis, '')='lot-native'
              AND COALESCE(executable_lot_count, 0) > 0
              AND COALESCE(dust_tracking_lot_count, 0) = 0
            """,
            (str(pair),),
        ).fetchall()
    except sqlite3.OperationalError:
        return None

    if not executable_rows:
        try:
            dust_rows = conn.execute(
                """
                SELECT qty_open, dust_tracking_lot_count
                FROM open_position_lots
                WHERE pair=?
                  AND qty_open > 1e-12
                  AND COALESCE(position_semantic_basis, '')='lot-native'
                  AND COALESCE(executable_lot_count, 0) = 0
                  AND COALESCE(dust_tracking_lot_count, 0) > 0
                """,
                (str(pair),),
            ).fetchall()
        except sqlite3.OperationalError:
            return None
        if not dust_rows:
            return None
        implied_lot_sizes = []
        for row in dust_rows:
            qty_open = float(_row_value(row, "qty_open", 0) or 0.0)
            dust_lot_count = int(_row_value(row, "dust_tracking_lot_count", 1) or 0)
            if qty_open <= 1e-12 or dust_lot_count <= 0:
                return None
            implied_lot_sizes.append(qty_open / float(dust_lot_count))
        source_mode = "derived_from_dust_row_qty"
    else:
        implied_lot_sizes = []
        for row in executable_rows:
            qty_open = float(_row_value(row, "qty_open", 0) or 0.0)
            executable_lot_count = int(_row_value(row, "executable_lot_count", 1) or 0)
            if qty_open <= 1e-12 or executable_lot_count <= 0:
                return None
            implied_lot_sizes.append(qty_open / float(executable_lot_count))
        source_mode = "derived_from_row_qty"

    baseline_lot_size = implied_lot_sizes[0]
    if baseline_lot_size <= 1e-12:
        return None
    if any(abs(lot_size - baseline_lot_size) > 1e-12 for lot_size in implied_lot_sizes[1:]):
        return None

    return LotDefinitionSnapshot(
        semantic_version=0,
        internal_lot_size=float(baseline_lot_size),
        min_qty=None,
        qty_step=None,
        min_notional_krw=None,
        max_qty_decimals=None,
        source_mode=source_mode,
    )


def _read_authoritative_lot_definition_from_accounted_buy_evidence(
    conn: sqlite3.Connection,
    *,
    pair: str,
) -> LotDefinitionSnapshot | None:
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT
                COALESCE(f.internal_lot_size, o.internal_lot_size) AS internal_lot_size,
                COALESCE(o.effective_min_trade_qty, f.internal_lot_size, o.internal_lot_size) AS min_qty,
                COALESCE(o.qty_step, f.internal_lot_size, o.internal_lot_size) AS qty_step,
                COALESCE(o.min_notional_krw, 0.0) AS min_notional_krw
            FROM trades t
            LEFT JOIN fills f
              ON f.client_order_id=t.client_order_id
             AND f.fill_ts=t.ts
             AND ABS(f.price-t.price) < 1e-12
             AND ABS(f.qty-t.qty) < 1e-12
            LEFT JOIN orders o
              ON o.client_order_id=t.client_order_id
            WHERE t.pair=?
              AND t.side='BUY'
              AND COALESCE(f.internal_lot_size, o.internal_lot_size, 0.0) > 1e-12
            """,
            (str(pair),),
        ).fetchall()
    except sqlite3.OperationalError:
        return None

    if not rows:
        return None

    snapshots: list[LotDefinitionSnapshot] = []
    for row in rows:
        internal_lot_size = float(_row_value(row, "internal_lot_size", 0) or 0.0)
        if internal_lot_size <= 1e-12:
            continue
        snapshots.append(
            LotDefinitionSnapshot(
                semantic_version=LOT_SEMANTIC_VERSION_V1,
                internal_lot_size=internal_lot_size,
                min_qty=float(_row_value(row, "min_qty", 1) or 0.0),
                qty_step=float(_row_value(row, "qty_step", 2) or 0.0),
                min_notional_krw=float(_row_value(row, "min_notional_krw", 3) or 0.0),
                max_qty_decimals=None,
                source_mode="accounted_buy_evidence",
            )
        )

    if not snapshots:
        return None

    first = snapshots[0]
    if any(snapshot != first for snapshot in snapshots[1:]):
        return None
    return first


def _persist_missing_lot_definition_snapshot(
    conn: sqlite3.Connection,
    *,
    pair: str,
    lot_definition: LotDefinitionSnapshot | None,
) -> None:
    if lot_definition is None or not lot_definition.is_authoritative:
        return
    try:
        conn.execute(
            """
            UPDATE open_position_lots
            SET lot_semantic_version=?,
                internal_lot_size=?,
                lot_min_qty=?,
                lot_qty_step=?,
                lot_min_notional_krw=?,
                lot_max_qty_decimals=?,
                lot_rule_source_mode=?
            WHERE pair=?
              AND qty_open > 1e-12
              AND COALESCE(position_semantic_basis, '')='lot-native'
              AND (
                    COALESCE(executable_lot_count, 0) > 0
                    OR COALESCE(dust_tracking_lot_count, 0) > 0
                  )
              AND lot_semantic_version IS NULL
              AND internal_lot_size IS NULL
              AND lot_min_qty IS NULL
              AND lot_qty_step IS NULL
              AND lot_min_notional_krw IS NULL
              AND lot_max_qty_decimals IS NULL
              AND lot_rule_source_mode IS NULL
              AND (
                    (
                        position_state='open_exposure'
                        AND ABS(
                            COALESCE(qty_open, 0.0)
                            - (COALESCE(executable_lot_count, 0) * ?)
                        ) <= 1e-12
                    )
                    OR
                    (
                        position_state='dust_tracking'
                        AND (
                            COALESCE(qty_open, 0.0)
                            - (COALESCE(dust_tracking_lot_count, 0) * ?)
                        ) <= 1e-12
                    )
                  )
            """,
            (
                lot_definition.semantic_version,
                lot_definition.internal_lot_size,
                lot_definition.min_qty,
                lot_definition.qty_step,
                lot_definition.min_notional_krw,
                lot_definition.max_qty_decimals,
                lot_definition.source_mode,
                str(pair),
                float(lot_definition.internal_lot_size or 0.0),
                float(lot_definition.internal_lot_size or 0.0),
            ),
        )
    except (sqlite3.OperationalError, sqlite3.IntegrityError):
        return


def _load_strategy_for_decision_id(conn: sqlite3.Connection, *, decision_id: int) -> str | None:
    row = conn.execute(
        """
        SELECT strategy_name
        FROM strategy_decisions
        WHERE id=?
        LIMIT 1
        """,
        (int(decision_id),),
    ).fetchone()
    strategy_name = _row_value(row, "strategy_name", 0)
    if strategy_name is None:
        return None
    return str(strategy_name)


def _extract_pair_from_context(context: object) -> str | None:
    if not isinstance(context, dict):
        return None

    candidate_paths = (
        ("pair",),
        ("market",),
        ("position_state", "normalized_exposure", "pair"),
        ("position_state", "pair"),
    )
    for path in candidate_paths:
        current: object = context
        for key in path:
            if not isinstance(current, dict) or key not in current:
                current = None
                break
            current = current[key]
        if current is None:
            continue
        text = str(current).strip()
        if text:
            return text
    return None


def _normalize_pair_for_match(pair: object) -> str | None:
    text = str(pair or "").strip()
    if not text:
        return None
    try:
        return parse_user_market_input(text)
    except Exception:
        return text.upper()


def _find_entry_decision(
    conn: sqlite3.Connection,
    *,
    fill_ts: int,
    pair: str,
    strategy_name: str | None,
) -> tuple[int | None, str | None, str]:
    """Resolve the BUY decision that should be attributed to a fill.

    Precedence is intentionally narrow and stable:

    1. direct linked decision: an explicit ``entry_decision_id`` always wins.
    2. strict single fallback: when no direct link exists, filter by
       ``strategy_name`` + ``signal='BUY'`` + ``decision_ts <= fill_ts`` within
       the fallback window, then require a strict pair match in the decision
       context.
    3. ambiguous multi candidate: more than one strict pair match in the window.
    4. unattributed no strict match: no strict pair match in the window.

    The pair is the final strict gate. ``strategy_name``/``signal``/``decision_ts``
    form the coarse candidate pool, and ``fill_ts`` is the upper bound so a BUY
    fill never attaches to a later decision.
    """
    if strategy_name is None or not str(strategy_name).strip():
        return None, None, ENTRY_DECISION_LINKAGE_UNATTRIBUTED_MISSING_STRATEGY

    lower_ts = max(0, int(fill_ts) - _ENTRY_DECISION_FALLBACK_LOOKBACK_MS)
    rows = conn.execute(
        """
        SELECT id, strategy_name, context_json
        FROM strategy_decisions
        WHERE signal='BUY'
          AND strategy_name=?
          AND decision_ts BETWEEN ? AND ?
        ORDER BY decision_ts DESC, id DESC
        """,
        (str(strategy_name), lower_ts, int(fill_ts)),
    ).fetchall()

    if not rows:
        return None, str(strategy_name), ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH

    normalized_pair = _normalize_pair_for_match(pair)
    strict_rows: list[sqlite3.Row] = []
    for row in rows:
        try:
            context = json.loads(str(_row_value(row, "context_json", 2) or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(context, dict):
            continue
        candidate_pair = _normalize_pair_for_match(_extract_pair_from_context(context))
        if candidate_pair is None or normalized_pair is None:
            continue
        if candidate_pair == normalized_pair:
            strict_rows.append(row)
            if len(strict_rows) > 1:
                break

    if not strict_rows:
        return None, str(strategy_name), ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH
    if len(strict_rows) > 1:
        return None, str(strategy_name), ENTRY_DECISION_LINKAGE_AMBIGUOUS_MULTI_CANDIDATE

    row = strict_rows[0]
    return (
        int(_row_value(row, "id", 0) or 0),
        str(_row_value(row, "strategy_name", 1) or ""),
        ENTRY_DECISION_LINKAGE_STRICT_SINGLE_FALLBACK,
    )


def apply_fill_lifecycle(
    conn: sqlite3.Connection,
    *,
    side: str,
    pair: str,
    trade_id: int,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    strategy_name: str | None = None,
    entry_decision_id: int | None = None,
    exit_decision_id: int | None = None,
    exit_reason: str | None = None,
    exit_rule_name: str | None = None,
    allow_entry_decision_fallback: bool = True,
) -> None:
    if side == "BUY":
        # BUY fills are persisted as lot-native exposure plus explicit dust.
        # The stored executable quantity is the exact executable lot multiple;
        # the non-executable remainder is tracked separately as dust evidence.
        lot_rules = _build_fill_lot_rules(pair=pair, market_price=price)
        override = _read_fill_lot_rules_override(
            conn,
            client_order_id=client_order_id,
            fill_id=fill_id,
            fill_ts=int(fill_ts),
            price=float(price),
            qty=float(qty),
        )
        if override is not None:
            lot_rules = replace(
                lot_rules,
                lot_size=float(override["internal_lot_size"]),
                executable_min_qty=float(override["internal_lot_size"]),
                dust_threshold=float(override["internal_lot_size"]),
                min_qty=float(override["effective_min_trade_qty"]),
                qty_step=float(override["qty_step"]),
                min_notional_krw=float(override["min_notional_krw"]),
            )
        lot_definition = _lot_definition_from_rules(lot_rules=lot_rules)
        split = lot_rules.split_qty(float(qty))
        fill_lot = ExecutableLot(
            raw_qty=float(qty),
            executable_qty=float(split.executable_qty),
            dust_qty=float(split.dust_qty),
            effective_min_trade_qty=float(split.executable_min_qty),
            min_qty=float(lot_rules.min_qty),
            qty_step=float(lot_rules.qty_step),
            min_notional_krw=float(lot_rules.min_notional_krw),
            exit_price_floor=None,
            exit_fee_ratio=0.0,
            exit_slippage_ratio=0.0,
            exit_buffer_ratio=0.0,
            exit_non_executable_reason=str(split.non_executable_reason),
        )
        executable_lot_count = int(split.lot_count if split.executable else 0)
        dust_lot_count = 1 if fill_lot.dust_qty > 1e-12 else 0
        resolved_entry_decision_id = entry_decision_id
        resolved_strategy_name = strategy_name
        resolved_entry_decision_linkage = (
            ENTRY_DECISION_LINKAGE_DIRECT
            if resolved_entry_decision_id is not None
            else ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH
        )
        if resolved_entry_decision_id is not None and resolved_strategy_name is None:
            resolved_strategy_name = _load_strategy_for_decision_id(conn, decision_id=int(resolved_entry_decision_id))
        if resolved_entry_decision_id is None and allow_entry_decision_fallback:
            lookup_decision_id, lookup_strategy_name, lookup_linkage = _find_entry_decision(
                conn,
                fill_ts=int(fill_ts),
                pair=str(pair),
                strategy_name=resolved_strategy_name,
            )
            resolved_entry_decision_id = lookup_decision_id
            if resolved_strategy_name is None:
                resolved_strategy_name = lookup_strategy_name
            resolved_entry_decision_linkage = lookup_linkage
        elif resolved_entry_decision_id is None and not allow_entry_decision_fallback:
            resolved_entry_decision_linkage = ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED
        total_fill_qty = max(0.0, float(qty))
        executable_qty = max(0.0, float(fill_lot.executable_qty))
        dust_qty = max(0.0, float(fill_lot.dust_qty))
        if executable_qty > 1e-12:
            executable_fee = float(fee) * (executable_qty / total_fill_qty) if total_fill_qty > 1e-12 else float(fee)
            conn.execute(
                """
                INSERT INTO open_position_lots(
                    pair,
                    entry_trade_id,
                    entry_client_order_id,
                    entry_fill_id,
                    entry_ts,
                    entry_price,
                    qty_open,
                    executable_lot_count,
                    dust_tracking_lot_count,
                    lot_semantic_version,
                    internal_lot_size,
                    lot_min_qty,
                    lot_qty_step,
                    lot_min_notional_krw,
                    lot_max_qty_decimals,
                    lot_rule_source_mode,
                    position_semantic_basis,
                    position_state,
                    entry_fee_total,
                    strategy_name,
                    entry_decision_id,
                    entry_decision_linkage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(pair),
                    int(trade_id),
                    str(client_order_id),
                    fill_id,
                    int(fill_ts),
                    float(price),
                    executable_qty,
                    executable_lot_count,
                    0,
                    lot_definition.semantic_version,
                    lot_definition.internal_lot_size,
                    lot_definition.min_qty,
                    lot_definition.qty_step,
                    lot_definition.min_notional_krw,
                    lot_definition.max_qty_decimals,
                    lot_definition.source_mode,
                    "lot-native",
                    OPEN_EXPOSURE_LOT_STATE,
                    float(executable_fee),
                    resolved_strategy_name,
                    resolved_entry_decision_id,
                    resolved_entry_decision_linkage,
                ),
            )
        if dust_qty > 1e-12:
            dust_fee = float(fee) - (float(fee) * (executable_qty / total_fill_qty) if total_fill_qty > 1e-12 else float(fee))
            conn.execute(
                """
                INSERT INTO open_position_lots(
                    pair,
                    entry_trade_id,
                    entry_client_order_id,
                    entry_fill_id,
                    entry_ts,
                    entry_price,
                    qty_open,
                    executable_lot_count,
                    dust_tracking_lot_count,
                    lot_semantic_version,
                    internal_lot_size,
                    lot_min_qty,
                    lot_qty_step,
                    lot_min_notional_krw,
                    lot_max_qty_decimals,
                    lot_rule_source_mode,
                    position_semantic_basis,
                    position_state,
                    entry_fee_total,
                    strategy_name,
                    entry_decision_id,
                    entry_decision_linkage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(pair),
                    int(trade_id),
                    str(client_order_id),
                    fill_id,
                    int(fill_ts),
                        float(price),
                        dust_qty,
                        0,
                        dust_lot_count or 1,
                        lot_definition.semantic_version,
                        lot_definition.internal_lot_size,
                        lot_definition.min_qty,
                        lot_definition.qty_step,
                        lot_definition.min_notional_krw,
                        lot_definition.max_qty_decimals,
                        lot_definition.source_mode,
                    "lot-native",
                    DUST_TRACKING_LOT_STATE,
                    float(dust_fee),
                    resolved_strategy_name,
                    resolved_entry_decision_id,
                    resolved_entry_decision_linkage,
                ),
            )
        return

    if side != "SELL":
        raise RuntimeError(f"unsupported lifecycle side: {side}")

    # SELL lifecycle normally consumes only the sellable open_exposure path.
    # This is post-fill accounting/matching only; SELL decision eligibility and
    # SELL sizing must already have been decided from
    # position_state.normalized_exposure.sellable_executable_lot_count.
    # dust_tracking lots remain operator evidence and are not matched by
    # ordinary lot-native SELLs. The only exception below is a target-delta
    # terminal-flat SELL with explicit quantity evidence that the submitted fill
    # closed both executable exposure and tracked dust.
    lot_snapshot_before_sell = summarize_position_lots(conn, pair=str(pair))
    quantity_authority = resolve_execution_quantity_authority(
        conn,
        client_order_id=str(client_order_id),
        trade_id=int(trade_id),
    )
    lot_rules = _build_fill_lot_rules(pair=pair, market_price=price)
    lot_definition = _read_authoritative_lot_definition_snapshot(conn, pair=str(pair))
    _persist_missing_lot_definition_snapshot(conn, pair=str(pair), lot_definition=lot_definition)
    if lot_definition is not None and lot_definition.internal_lot_size is not None:
        lot_rules = replace(
            lot_rules,
            lot_size=float(lot_definition.internal_lot_size),
            executable_min_qty=float(lot_definition.internal_lot_size),
            dust_threshold=float(lot_definition.internal_lot_size),
            min_qty=(
                float(lot_definition.min_qty)
                if lot_definition.min_qty is not None
                else float(lot_rules.min_qty)
            ),
            qty_step=(
                float(lot_definition.qty_step)
                if lot_definition.qty_step is not None
                else float(lot_rules.qty_step)
            ),
            min_notional_krw=(
                float(lot_definition.min_notional_krw)
                if lot_definition.min_notional_krw is not None
                else float(lot_rules.min_notional_krw)
            ),
            max_qty_decimals=(
                int(lot_definition.max_qty_decimals)
                if lot_definition.max_qty_decimals is not None
                else int(lot_rules.max_qty_decimals)
            ),
            source_mode=str(lot_definition.source_mode or lot_rules.source_mode),
        )
    rows = _fetch_sellable_open_exposure_lots(conn, pair=str(pair))

    remaining_lots = int(qty_to_executable_lot_count(qty=float(qty), lot_rules=lot_rules))
    if remaining_lots <= 0:
        return

    total_exit_qty = lot_count_to_qty(lot_count=remaining_lots, lot_size=float(lot_rules.lot_size))
    eps = 1e-12
    for row in rows:
        if remaining_lots <= 0:
            break

        lot = row
        lot_qty = float(_row_value(lot, "qty_open", 6) or 0.0)
        lot_count = _row_executable_lot_count(lot, qty_open=lot_qty, lot_rules=lot_rules)
        if lot_count <= 0:
            continue
        matched_lots = min(lot_count, remaining_lots)
        if matched_lots <= 0:
            continue
        matched_qty = lot_count_to_qty(lot_count=matched_lots, lot_size=float(lot_rules.lot_size))

        entry_fee_total = float(_row_value(lot, "entry_fee_total", 10) or 0.0)
        entry_fee_alloc = (entry_fee_total * (matched_qty / lot_qty)) if lot_qty > eps else 0.0
        exit_fee_alloc = float(fee) * (matched_qty / total_exit_qty)

        gross_pnl = (float(price) - float(_row_value(lot, "entry_price", 5) or 0.0)) * matched_qty
        fee_total = entry_fee_alloc + exit_fee_alloc
        net_pnl = gross_pnl - fee_total
        holding_time_seconds = max(0.0, (int(fill_ts) - int(_row_value(lot, "entry_ts", 4) or 0)) / 1000.0)

        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                pair,
                entry_trade_id,
                exit_trade_id,
                entry_client_order_id,
                exit_client_order_id,
                entry_fill_id,
                exit_fill_id,
                entry_ts,
                exit_ts,
                matched_qty,
                entry_price,
                exit_price,
                gross_pnl,
                fee_total,
                net_pnl,
                holding_time_sec,
                strategy_name,
                entry_decision_id,
                entry_decision_linkage,
                exit_decision_id,
                exit_reason,
                exit_rule_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pair),
                int(_row_value(lot, "entry_trade_id", 1) or 0),
                int(trade_id),
                str(_row_value(lot, "entry_client_order_id", 2) or ""),
                str(client_order_id),
                _row_value(lot, "entry_fill_id", 3),
                fill_id,
                int(_row_value(lot, "entry_ts", 4) or 0),
                int(fill_ts),
                float(matched_qty),
                float(_row_value(lot, "entry_price", 5) or 0.0),
                float(price),
                float(gross_pnl),
                float(fee_total),
                float(net_pnl),
                float(holding_time_seconds),
                strategy_name or _row_value(lot, "strategy_name", 11),
                entry_decision_id if entry_decision_id is not None else _row_value(lot, "entry_decision_id", 12),
                (
                    ENTRY_DECISION_LINKAGE_DIRECT
                    if entry_decision_id is not None
                    else str(_row_value(lot, "entry_decision_linkage", 13) or "")
                ),
                exit_decision_id,
                exit_reason,
                exit_rule_name,
            ),
        )

        remaining_lot_count = max(0, lot_count - matched_lots)
        qty_open_after = lot_count_to_qty(lot_count=remaining_lot_count, lot_size=float(lot_rules.lot_size))
        fee_remaining = max(0.0, entry_fee_total - entry_fee_alloc)
        conn.execute(
            """
            UPDATE open_position_lots
            SET qty_open=?, executable_lot_count=?, entry_fee_total=?
            WHERE id=?
            """,
            (
                qty_open_after,
                remaining_lot_count,
                fee_remaining,
                int(_row_value(lot, "id", 0) or 0),
            ),
        )

        remaining_lots -= matched_lots

    if remaining_lots > 0:
        remaining_qty = lot_count_to_qty(lot_count=remaining_lots, lot_size=float(lot_rules.lot_size))
        fallback_exit_fee = float(fee) * (remaining_qty / total_exit_qty) if total_exit_qty > eps else 0.0
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                pair,
                entry_trade_id,
                exit_trade_id,
                entry_client_order_id,
                exit_client_order_id,
                entry_fill_id,
                exit_fill_id,
                entry_ts,
                exit_ts,
                matched_qty,
                entry_price,
                exit_price,
                gross_pnl,
                fee_total,
                net_pnl,
                holding_time_sec,
                strategy_name,
                entry_decision_id,
                entry_decision_linkage,
                exit_decision_id,
                exit_reason,
                exit_rule_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pair),
                0,
                int(trade_id),
                "__unknown_entry__",
                str(client_order_id),
                None,
                fill_id,
                int(fill_ts),
                int(fill_ts),
                float(remaining_qty),
                float(price),
                float(price),
                0.0,
                float(fallback_exit_fee),
                float(-fallback_exit_fee),
                0.0,
                strategy_name,
                entry_decision_id,
                "unattributed_unknown_entry",
                exit_decision_id,
                exit_reason,
                exit_rule_name,
            ),
        )

    conn.execute(
        """
        DELETE FROM open_position_lots
        WHERE pair=?
          AND position_state=?
          AND qty_open <= ?
          AND COALESCE(executable_lot_count, 0) <= 0
        """,
        (str(pair), OPEN_EXPOSURE_LOT_STATE, eps),
    )
    _close_terminal_flat_dust_tracking_lots(
        conn,
        pair=str(pair),
        authority=quantity_authority,
        open_exposure_qty_before=float(lot_snapshot_before_sell.raw_open_exposure_qty),
        dust_tracking_qty_before=float(lot_snapshot_before_sell.dust_tracking_qty),
    )


def rebuild_lifecycle_projections_from_trades(
    conn: sqlite3.Connection,
    *,
    pair: str,
    allow_entry_decision_fallback: bool = False,
) -> ProjectionReplayResult:
    """Rebuild lot/lifecycle projections from accounted trade rows.

    Authoritative fill accounting is stored in orders/fills/trades. The
    open-position lot table and lifecycle table are projections and must be
    reproducible from the accounted trade sequence during recovery/repair.
    """

    pair_text = str(pair)
    before = summarize_position_lots(conn, pair=pair_text)
    lot_delete = conn.execute(
        "DELETE FROM open_position_lots WHERE pair=?",
        (pair_text,),
    )
    lifecycle_delete = conn.execute(
        "DELETE FROM trade_lifecycles WHERE pair=?",
        (pair_text,),
    )
    rows = conn.execute(
        """
        SELECT
            t.id AS trade_id,
            t.ts AS fill_ts,
            t.pair,
            t.side,
            t.price,
            t.qty,
            t.fee,
            t.client_order_id,
            t.strategy_name,
            t.entry_decision_id,
            t.exit_decision_id,
            t.exit_reason,
            t.exit_rule_name,
            f.fill_id
        FROM trades t
        LEFT JOIN fills f
          ON f.client_order_id=t.client_order_id
         AND f.fill_ts=t.ts
         AND ABS(f.price-t.price) < 1e-12
         AND ABS(f.qty-t.qty) < 1e-12
        WHERE t.pair=?
          AND t.side IN ('BUY', 'SELL')
        ORDER BY t.ts ASC, t.id ASC
        """,
        (pair_text,),
    ).fetchall()

    buy_count = 0
    sell_count = 0
    for row in rows:
        side = str(row["side"] or "").upper()
        if side == "BUY":
            buy_count += 1
        elif side == "SELL":
            sell_count += 1
        apply_fill_lifecycle(
            conn,
            side=side,
            pair=pair_text,
            trade_id=int(row["trade_id"]),
            client_order_id=str(row["client_order_id"]),
            fill_id=(str(row["fill_id"]) if row["fill_id"] is not None else None),
            fill_ts=int(row["fill_ts"]),
            price=float(row["price"]),
            qty=float(row["qty"]),
            fee=float(row["fee"] or 0.0),
            strategy_name=(str(row["strategy_name"]) if row["strategy_name"] is not None else None),
            entry_decision_id=(int(row["entry_decision_id"]) if row["entry_decision_id"] is not None else None),
            exit_decision_id=(int(row["exit_decision_id"]) if row["exit_decision_id"] is not None else None),
            exit_reason=(str(row["exit_reason"]) if row["exit_reason"] is not None else None),
            exit_rule_name=(str(row["exit_rule_name"]) if row["exit_rule_name"] is not None else None),
            allow_entry_decision_fallback=allow_entry_decision_fallback,
        )

    _apply_published_portfolio_projection_adjustments(conn, pair=pair_text)

    after = summarize_position_lots(conn, pair=pair_text)
    return ProjectionReplayResult(
        pair=pair_text,
        replayed_trade_count=len(rows),
        replayed_buy_count=buy_count,
        replayed_sell_count=sell_count,
        deleted_open_position_lot_count=max(0, int(lot_delete.rowcount or 0)),
        deleted_trade_lifecycle_count=max(0, int(lifecycle_delete.rowcount or 0)),
        lot_snapshot_before=before,
        lot_snapshot_after=after,
    )


def apply_portfolio_anchored_projection_repair_basis(
    conn: sqlite3.Connection,
    *,
    pair: str,
    repair_basis: dict[str, object],
) -> None:
    """Apply an explicit projection-only repair backed by broker/portfolio evidence.

    This does not create accounting trades. It removes false executable
    authority for one target BUY and, when the verified portfolio still leaves
    a sub-lot remainder attributable to that target, persists that remainder as
    dust-tracking evidence.
    """

    target_trade_id = int(repair_basis.get("target_trade_id") or 0)
    if target_trade_id <= 0:
        raise RuntimeError("portfolio projection repair target_trade_id missing")
    target_remainder_qty = normalize_asset_qty(float(repair_basis.get("target_remainder_qty") or 0.0))
    internal_lot_size = float(repair_basis.get("canonical_internal_lot_size") or 0.0)
    if internal_lot_size <= 1e-12:
        raise RuntimeError("portfolio projection repair canonical lot size missing")
    if target_remainder_qty >= internal_lot_size - 1e-12:
        raise RuntimeError("portfolio projection repair remainder is still executable")

    row = conn.execute(
        """
        SELECT
            t.id AS trade_id,
            t.client_order_id,
            t.ts AS fill_ts,
            t.price,
            t.fee,
            t.strategy_name,
            t.entry_decision_id,
            f.fill_id,
            COALESCE(o.effective_min_trade_qty, ?) AS effective_min_trade_qty,
            COALESCE(o.qty_step, ?) AS qty_step,
            COALESCE(o.min_notional_krw, 0.0) AS min_notional_krw
        FROM trades t
        LEFT JOIN fills f
          ON f.client_order_id=t.client_order_id
         AND f.fill_ts=t.ts
         AND ABS(f.price-t.price) < 1e-12
        LEFT JOIN orders o
          ON o.client_order_id=t.client_order_id
        WHERE t.id=? AND t.pair=? AND t.side='BUY'
        LIMIT 1
        """,
        (
            float(internal_lot_size),
            float(internal_lot_size),
            int(target_trade_id),
            str(pair),
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("portfolio projection repair target BUY disappeared")

    conn.execute(
        "DELETE FROM open_position_lots WHERE pair=? AND entry_trade_id=?",
        (str(pair), int(target_trade_id)),
    )
    conn.execute(
        """
        DELETE FROM trade_lifecycles
        WHERE pair=?
          AND (
                entry_trade_id=?
                OR exit_trade_id IN (
                    SELECT id FROM trades
                    WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))
                )
              )
        """,
        (
            str(pair),
            int(target_trade_id),
            str(pair),
            int(row["fill_ts"]),
            int(row["fill_ts"]),
            int(target_trade_id),
        ),
    )
    if target_remainder_qty <= 1e-12:
        return

    total_qty = normalize_asset_qty(float(repair_basis.get("target_qty") or 0.0))
    fee_total = float(row["fee"] or 0.0)
    dust_fee = fee_total * (target_remainder_qty / total_qty) if total_qty > 1e-12 else 0.0
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_fill_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            lot_semantic_version,
            internal_lot_size,
            lot_min_qty,
            lot_qty_step,
            lot_min_notional_krw,
            lot_max_qty_decimals,
            lot_rule_source_mode,
            position_semantic_basis,
            position_state,
            entry_fee_total,
            strategy_name,
            entry_decision_id,
            entry_decision_linkage
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(pair),
            int(target_trade_id),
            str(row["client_order_id"]),
            (str(row["fill_id"]) if row["fill_id"] is not None else None),
            int(row["fill_ts"]),
            float(row["price"]),
            float(target_remainder_qty),
            0,
            1,
            LOT_SEMANTIC_VERSION_V1,
            float(internal_lot_size),
            float(row["effective_min_trade_qty"] or internal_lot_size),
            float(row["qty_step"] or internal_lot_size),
            float(row["min_notional_krw"] or 0.0),
            int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
            "portfolio_anchored_repair",
            "lot-native",
            DUST_TRACKING_LOT_STATE,
            float(dust_fee),
            (str(row["strategy_name"]) if row["strategy_name"] is not None else None),
            (int(row["entry_decision_id"]) if row["entry_decision_id"] is not None else None),
            ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED,
        ),
    )


def _published_portfolio_projection_matches_current_candidate(
    conn: sqlite3.Connection,
    *,
    pair: str,
    publish_basis: dict[str, object],
) -> bool:
    from .position_authority_state import build_position_authority_assessment

    target_trade_id = int(publish_basis.get("target_trade_id") or 0)
    target_remainder_qty = normalize_asset_qty(float(publish_basis.get("target_remainder_qty") or 0.0))
    portfolio_qty = normalize_asset_qty(float(publish_basis.get("portfolio_qty") or 0.0))
    if target_trade_id <= 0:
        return False

    assessment = build_position_authority_assessment(conn, pair=pair)
    return bool(
        assessment.get("needs_portfolio_projection_repair")
        and bool(assessment.get("projection_repair_covers_excess"))
        and int(assessment.get("target_trade_id") or 0) == target_trade_id
        and abs(
            normalize_asset_qty(float(assessment.get("portfolio_target_remainder_qty") or 0.0)) - target_remainder_qty
        )
        <= 1e-12
        and abs(normalize_asset_qty(float(assessment.get("portfolio_qty") or 0.0)) - portfolio_qty) <= 1e-12
    )


def _apply_published_portfolio_projection_adjustments(conn: sqlite3.Connection, *, pair: str) -> int:
    from .position_authority_state import build_lot_projection_convergence

    try:
        rows = conn.execute(
            """
            SELECT publish_basis
            FROM position_authority_projection_publications
            WHERE pair=?
            ORDER BY event_ts ASC, id ASC
            """,
            (str(pair),),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    applied = 0
    for index, row in enumerate(rows, start=1):
        try:
            basis = json.loads(str(_row_value(row, "publish_basis", 0) or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(basis, dict):
            continue
        if not _published_portfolio_projection_matches_current_candidate(conn, pair=pair, publish_basis=basis):
            continue
        savepoint_name = f"position_authority_publication_{index}"
        conn.execute(f"SAVEPOINT {savepoint_name}")
        apply_portfolio_anchored_projection_repair_basis(conn, pair=pair, repair_basis=basis)
        convergence = build_lot_projection_convergence(conn, pair=pair)
        if not bool(convergence.get("converged")):
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            continue
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        applied += 1
    return applied


def mark_harmless_dust_positions(
    conn: sqlite3.Connection,
    *,
    pair: str,
    dust_metadata: DustDisplayContext | DustClassification | str | dict[str, object] | None,
) -> int:
    dust_context = (
        dust_metadata
        if isinstance(dust_metadata, DustDisplayContext)
        else build_dust_display_context(dust_metadata)
    )
    dust = dust_context.classification
    if not (
        dust.present
        and dust.classification == DustState.HARMLESS_DUST.value
        and dust_context.effective_flat_due_to_harmless_dust
    ):
        return 0

    min_qty = max(0.0, float(dust.min_qty))
    if min_qty <= 0.0:
        return 0

    candidate_rows = conn.execute(
        """
        SELECT id, qty_open
        FROM open_position_lots
        WHERE pair=?
          AND position_state=?
          AND qty_open > 1e-12
        ORDER BY entry_ts ASC, id ASC
        """,
        (
            str(pair),
            OPEN_EXPOSURE_LOT_STATE,
        ),
    ).fetchall()

    updated_count = 0
    for row in candidate_rows:
        # The boundary is strict: qty_open == min_qty stays open_exposure.
        # Only strict sub-min residues are reclassified to dust_tracking.
        if not is_strictly_below_min_qty(qty_open=float(_row_value(row, "qty_open", 1) or 0.0), min_qty=min_qty):
            continue
        conn.execute(
            """
            UPDATE open_position_lots
            SET position_state=?,
                position_semantic_basis='lot-native',
                executable_lot_count=0,
                dust_tracking_lot_count=CASE
                    WHEN COALESCE(executable_lot_count, 0) > 0 THEN executable_lot_count
                    ELSE 1
                END
            WHERE id=?
            """,
            (
                DUST_TRACKING_LOT_STATE,
                int(_row_value(row, "id", 0) or 0),
            ),
        )
        updated_count += 1
    return updated_count


def summarize_position_lots(
    conn: sqlite3.Connection,
    *,
    pair: str,
    executable_lot: ExecutableLot | None = None,
) -> PositionLotSnapshot:
    try:
        open_row = conn.execute(
            """
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(executable_lot_count, 0) > 0
                                 AND COALESCE(dust_tracking_lot_count, 0) = 0 THEN qty_open
                            ELSE 0.0
                        END
                    ),
                    0.0
                ),
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(executable_lot_count, 0) > 0
                                 AND COALESCE(dust_tracking_lot_count, 0) = 0 THEN executable_lot_count
                            ELSE 0
                        END
                    ),
                    0
                )
            FROM open_position_lots
            WHERE pair=?
            """,
            (str(pair),),
        ).fetchone()
        dust_row = conn.execute(
            """
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(dust_tracking_lot_count, 0) > 0
                                 AND COALESCE(executable_lot_count, 0) = 0 THEN qty_open
                            ELSE 0.0
                        END
                    ),
                    0.0
                ),
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(dust_tracking_lot_count, 0) > 0
                                 AND COALESCE(executable_lot_count, 0) = 0 THEN dust_tracking_lot_count
                            ELSE 0
                        END
                    ),
                    0
                )
            FROM open_position_lots
            WHERE pair=?
            """,
            (str(pair),),
        ).fetchone()
    except sqlite3.OperationalError:
        try:
            open_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(qty_open), 0.0),
                    COALESCE(SUM(CASE WHEN COALESCE(executable_lot_count, 0) > 0 THEN executable_lot_count ELSE 0 END), 0)
                FROM open_position_lots
                WHERE pair=? AND position_state=? AND COALESCE(executable_lot_count, 0) > 0
                """,
                (str(pair), OPEN_EXPOSURE_LOT_STATE),
            ).fetchone()
            dust_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(qty_open), 0.0),
                    COALESCE(SUM(CASE WHEN COALESCE(dust_tracking_lot_count, 0) > 0 THEN dust_tracking_lot_count ELSE 0 END), 0)
                FROM open_position_lots
                WHERE pair=? AND position_state=? AND COALESCE(dust_tracking_lot_count, 0) > 0
                """,
                (str(pair), DUST_TRACKING_LOT_STATE),
            ).fetchone()
        except (sqlite3.OperationalError, AssertionError):
            open_row = (0.0, 0)
            dust_row = (0.0, 0)
    except AssertionError:
        open_row = (0.0, 0)
        dust_row = (0.0, 0)
    raw_open_qty = max(0.0, float(open_row[0] if open_row is not None else 0.0))
    tracked_dust_qty = max(0.0, float(dust_row[0] if dust_row is not None else 0.0))
    open_lot_count = max(0, int(open_row[1] if open_row is not None else 0))
    dust_lot_count = max(0, int(dust_row[1] if dust_row is not None else 0))
    try:
        lot_definition = _read_authoritative_lot_definition_snapshot(conn, pair=str(pair))
    except (sqlite3.OperationalError, AssertionError):
        lot_definition = None
    if executable_lot is None:
        executable_qty = 0.0
        effective_min_trade_qty = (
            0.0
            if lot_definition is None or lot_definition.min_qty is None
            else float(lot_definition.min_qty)
        )
        if open_lot_count > 0:
            exit_non_executable_reason = "none"
        elif dust_lot_count > 0:
            exit_non_executable_reason = "dust_only_remainder"
        else:
            exit_non_executable_reason = "no_executable_open_lots"
    else:
        executable_qty = float(executable_lot.executable_qty)
        effective_min_trade_qty = float(executable_lot.effective_min_trade_qty)
        exit_non_executable_reason = str(executable_lot.exit_non_executable_reason)
    return PositionLotSnapshot(
        raw_open_exposure_qty=raw_open_qty,
        executable_open_exposure_qty=float(executable_qty),
        dust_tracking_qty=max(0.0, tracked_dust_qty + (0.0 if executable_lot is None else float(executable_lot.dust_qty))),
        raw_total_asset_qty=max(0.0, raw_open_qty + tracked_dust_qty),
        open_lot_count=max(0, open_lot_count),
        dust_tracking_lot_count=max(0, dust_lot_count),
        effective_min_trade_qty=float(effective_min_trade_qty),
        exit_non_executable_reason=exit_non_executable_reason,
        position_semantic_basis="lot-native",
        lot_definition=lot_definition,
    )


def _residual_classification_tolerance(
    *,
    internal_lot_size: float,
    qty_step: float,
    max_qty_decimals: int | None,
) -> float:
    decimal_unit = 0.0
    if max_qty_decimals is not None and int(max_qty_decimals) > 0:
        decimal_unit = 10 ** (-int(max_qty_decimals))
    return max(
        1e-12,
        decimal_unit * 20.0,
        abs(float(qty_step or 0.0)) * 0.002,
        abs(float(internal_lot_size or 0.0)) * 0.001,
        2e-7,
    )


def summarize_non_executable_residuals(
    conn: sqlite3.Connection,
    *,
    pair: str,
) -> ResidualInventorySnapshot:
    try:
        rows = conn.execute(
            """
            SELECT
                id,
                entry_trade_id,
                qty_open,
                position_state,
                COALESCE(lot_rule_source_mode, '') AS lot_rule_source_mode,
                COALESCE(entry_decision_linkage, '') AS entry_decision_linkage,
                COALESCE(internal_lot_size, 0.0) AS internal_lot_size,
                COALESCE(lot_min_qty, 0.0) AS lot_min_qty,
                COALESCE(lot_qty_step, 0.0) AS lot_qty_step,
                COALESCE(lot_min_notional_krw, 0.0) AS lot_min_notional_krw,
                COALESCE(lot_max_qty_decimals, 0) AS lot_max_qty_decimals,
                entry_price
            FROM open_position_lots
            WHERE pair=?
              AND qty_open > 1e-12
              AND COALESCE(executable_lot_count, 0) = 0
              AND COALESCE(dust_tracking_lot_count, 0) > 0
            ORDER BY entry_ts ASC, id ASC
            """,
            (str(pair),),
        ).fetchall()
    except (sqlite3.OperationalError, AssertionError):
        rows = []

    residual_rows: list[ResidualLotRowSnapshot] = []
    residual_classes: set[str] = set()
    total_qty = 0.0
    total_notional = 0.0
    notional_known = False
    aggregate_min_qty = 0.0
    aggregate_min_notional = 0.0

    for row in rows:
        qty_open = max(0.0, _row_float_local(row, "qty_open"))
        internal_lot_size = max(0.0, _row_float_local(row, "internal_lot_size"))
        min_qty = max(0.0, _row_float_local(row, "lot_min_qty"))
        qty_step = max(0.0, _row_float_local(row, "lot_qty_step"))
        min_notional_krw = max(0.0, _row_float_local(row, "lot_min_notional_krw"))
        max_qty_decimals = _row_int_local(row, "lot_max_qty_decimals")
        entry_price = _row_float_local(row, "entry_price")
        source_mode = _row_text_local(row, "lot_rule_source_mode")
        entry_decision_linkage = _row_text_local(row, "entry_decision_linkage")
        tolerance = _residual_classification_tolerance(
            internal_lot_size=internal_lot_size,
            qty_step=qty_step,
            max_qty_decimals=max_qty_decimals,
        )
        near_lot_delta = (
            None
            if internal_lot_size <= 1e-12
            else normalize_asset_qty(abs(qty_open - internal_lot_size))
        )
        below_exchange_min_qty = bool(min_qty > 0.0 and qty_open + 1e-12 < min_qty)
        estimated_notional_krw = (
            qty_open * entry_price if entry_price > 0.0 else None
        )
        below_exchange_min_notional = bool(
            estimated_notional_krw is not None
            and min_notional_krw > 0.0
            and estimated_notional_krw + 1e-9 < min_notional_krw
        )
        classes: list[str] = []
        if near_lot_delta is not None and near_lot_delta <= tolerance:
            classes.append("NEAR_LOT_RESIDUAL")
        if source_mode == "ledger":
            classes.append("LEDGER_SPLIT_RESIDUAL")
        if source_mode in {
            "full_projection_rebuild_portfolio_anchor",
            "portfolio_anchored_repair",
        }:
            classes.append("PORTFOLIO_ANCHOR_RESIDUAL")
        if entry_decision_linkage.startswith("degraded_recovery_"):
            classes.append("DEGRADED_RECOVERY_RESIDUAL")
        if below_exchange_min_qty or below_exchange_min_notional:
            classes.append("TRUE_DUST")
        if not classes:
            classes.append("UNCLASSIFIED_RESIDUAL")

        residual_rows.append(
            ResidualLotRowSnapshot(
                lot_id=_row_int_local(row, "id"),
                entry_trade_id=_row_int_local(row, "entry_trade_id"),
                qty_open=qty_open,
                position_state=_row_text_local(row, "position_state"),
                source_mode=source_mode,
                entry_decision_linkage=entry_decision_linkage,
                internal_lot_size=internal_lot_size,
                lot_min_qty=min_qty,
                lot_min_notional_krw=min_notional_krw,
                near_lot_delta=near_lot_delta,
                below_exchange_min_qty=below_exchange_min_qty,
                below_exchange_min_notional=below_exchange_min_notional,
                estimated_notional_krw=estimated_notional_krw,
                classes=tuple(classes),
            )
        )
        residual_classes.update(classes)
        total_qty = normalize_asset_qty(total_qty + qty_open)
        if estimated_notional_krw is not None:
            total_notional += estimated_notional_krw
            notional_known = True
        aggregate_min_qty = max(aggregate_min_qty, min_qty)
        aggregate_min_notional = max(aggregate_min_notional, min_notional_krw)

    residual_notional_krw = total_notional if notional_known else None
    exchange_sellable = bool(
        total_qty > 1e-12
        and aggregate_min_qty > 0.0
        and total_qty + 1e-12 >= aggregate_min_qty
        and (
            aggregate_min_notional <= 0.0
            or (
                residual_notional_krw is not None
                and residual_notional_krw + 1e-9 >= aggregate_min_notional
            )
        )
    )
    material_residual = bool(
        total_qty > 1e-12
        and (
            exchange_sellable
            or any(
                value in residual_classes
                for value in (
                    "NEAR_LOT_RESIDUAL",
                    "PORTFOLIO_ANCHOR_RESIDUAL",
                    "DEGRADED_RECOVERY_RESIDUAL",
                    "UNCLASSIFIED_RESIDUAL",
                )
            )
        )
    )
    return ResidualInventorySnapshot(
        residual_qty=total_qty,
        residual_notional_krw=residual_notional_krw,
        residual_lot_count=len(residual_rows),
        residual_classes=tuple(sorted(residual_classes)),
        exchange_sellable=exchange_sellable,
        strategy_sellable=False,
        material_residual=material_residual,
        rows=tuple(residual_rows),
    )


def summarize_reserved_exit_qty(
    conn: sqlite3.Connection,
    *,
    pair: str,
) -> float:
    """Return remaining qty already reserved by unresolved SELL orders."""

    query_with_symbol = """
        SELECT COALESCE(SUM(MAX(qty_req - qty_filled, 0.0)), 0.0) AS reserved_exit_qty
        FROM orders
        WHERE symbol=?
          AND side='SELL'
          AND status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
    """
    query_without_symbol = """
        SELECT COALESCE(SUM(MAX(qty_req - qty_filled, 0.0)), 0.0) AS reserved_exit_qty
        FROM orders
        WHERE side='SELL'
          AND status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
    """
    try:
        row = conn.execute(query_with_symbol, (str(pair),)).fetchone()
    except sqlite3.OperationalError:
        try:
            row = conn.execute(query_without_symbol).fetchone()
        except sqlite3.OperationalError:
            return 0.0
    if row is None:
        return 0.0
    try:
        value = row["reserved_exit_qty"] if hasattr(row, "keys") else row[0]
        return max(0.0, float(value or 0.0))
    except (TypeError, ValueError, IndexError, KeyError):
        return 0.0


def reclassify_non_executable_open_exposure(
    conn: sqlite3.Connection,
    *,
    pair: str,
    executable_lot: ExecutableLot,
) -> int:
    if executable_lot.executable_qty > 1e-12:
        return 0
    if executable_lot.raw_qty <= 1e-12:
        return 0
    result = conn.execute(
        """
        UPDATE open_position_lots
        SET position_state=?,
            position_semantic_basis='lot-native',
            dust_tracking_lot_count=CASE
                WHEN COALESCE(executable_lot_count, 0) > 0 THEN executable_lot_count
                ELSE 1
            END,
            executable_lot_count=0
        WHERE pair=?
          AND position_state=?
          AND qty_open > 1e-12
        """,
        (DUST_TRACKING_LOT_STATE, str(pair), OPEN_EXPOSURE_LOT_STATE),
    )
    return int(result.rowcount or 0)


def _fetch_sellable_open_exposure_lots(
    conn: sqlite3.Connection,
    *,
    pair: str,
) -> list[sqlite3.Row]:
    """Return lots that can actually be sold.

    Only `open_exposure` lots are eligible. `dust_tracking` lots are operator
    evidence and must not be counted as sellable inventory.
    """

    return conn.execute(
        """
        SELECT
            id,
            entry_trade_id,
            entry_client_order_id,
            entry_fill_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_state,
            entry_fee_total,
            strategy_name,
            entry_decision_id,
            entry_decision_linkage
        FROM open_position_lots
        WHERE pair=? AND position_state=? AND COALESCE(executable_lot_count, 0) > 0
        ORDER BY entry_ts ASC, id ASC
        """,
        (str(pair), OPEN_EXPOSURE_LOT_STATE),
    ).fetchall()


def _close_terminal_flat_dust_tracking_lots(
    conn: sqlite3.Connection,
    *,
    pair: str,
    authority: ExecutionQuantityAuthority,
    open_exposure_qty_before: float,
    dust_tracking_qty_before: float,
) -> int:
    """Close dust projection rows only for proven target-delta terminal flat SELLs."""

    if dust_tracking_qty_before <= 1e-12:
        return 0
    if not authority.is_target_delta_terminal_flat:
        return 0

    evidence_open_qty = (
        float(authority.open_exposure_qty)
        if authority.open_exposure_qty is not None
        else float(open_exposure_qty_before)
    )
    evidence_dust_qty = (
        float(authority.dust_tracking_qty)
        if authority.dust_tracking_qty is not None
        else float(dust_tracking_qty_before)
    )
    expected_closed_qty = normalize_asset_qty(evidence_open_qty + evidence_dust_qty)
    actual_closed_qty = normalize_asset_qty(float(authority.submitted_qty))
    if expected_closed_qty <= 1e-12:
        return 0
    if actual_closed_qty + 1e-12 < expected_closed_qty:
        return 0
    if abs(evidence_dust_qty - dust_tracking_qty_before) > 1e-12:
        return 0

    result = conn.execute(
        """
        DELETE FROM open_position_lots
        WHERE pair=?
          AND position_state=?
          AND qty_open > 1e-12
          AND COALESCE(executable_lot_count, 0) = 0
          AND COALESCE(dust_tracking_lot_count, 0) > 0
        """,
        (str(pair), DUST_TRACKING_LOT_STATE),
    )
    return int(result.rowcount or 0)
