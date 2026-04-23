from __future__ import annotations

import json
import sqlite3
from typing import Any

from .config import settings
from .db_core import normalize_asset_qty
from .lot_model import build_quantity_contract_snapshot
from .position_authority_incidents import PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON


_EPS = 1e-12
PARTIAL_CLOSE_RESIDUAL_REPAIR_REASON = "partial_close_residual_authority_normalization"


def _row_float(row: Any, key: str, default: float = 0.0) -> float:
    if row is None:
        return default
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return default


def _row_int(row: Any, key: str, default: int = 0) -> int:
    if row is None:
        return default
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _row_text(row: Any, key: str, default: str = "") -> str:
    if row is None:
        return default
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return str(value or default)


def build_lot_projection_convergence(conn, *, pair: str | None = None) -> dict[str, Any]:
    """Summarize whether the persisted lot projection matches portfolio holdings.

    This is a convergence guard only. It must not be used to recover SELL
    authority from aggregate qty; executable authority remains the
    open_exposure lot-count path.
    """

    pair_text = str(pair or settings.PAIR)
    portfolio_row = conn.execute(
        "SELECT asset_qty, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    portfolio_qty = 0.0
    if portfolio_row is not None:
        try:
            keys = portfolio_row.keys()
        except AttributeError:
            keys = ()
        if "asset_available" in keys:
            portfolio_qty = normalize_asset_qty(
                _row_float(portfolio_row, "asset_available") + _row_float(portfolio_row, "asset_locked")
            )
        else:
            portfolio_qty = normalize_asset_qty(_row_float(portfolio_row, "asset_qty"))

    try:
        lot_row = conn.execute(
            """
            SELECT
                COUNT(*) AS lot_row_count,
                COALESCE(SUM(qty_open), 0.0) AS projected_total_qty,
                COALESCE(SUM(CASE WHEN position_state='open_exposure' THEN qty_open ELSE 0.0 END), 0.0)
                    AS open_exposure_qty,
                COALESCE(SUM(CASE WHEN position_state='dust_tracking' THEN qty_open ELSE 0.0 END), 0.0)
                    AS dust_tracking_qty,
                COALESCE(SUM(executable_lot_count), 0) AS executable_lot_count,
                COALESCE(SUM(dust_tracking_lot_count), 0) AS dust_tracking_lot_count
            FROM open_position_lots
            WHERE pair=? AND qty_open > 1e-12
            """,
            (pair_text,),
        ).fetchone()
    except AssertionError:
        return {
            "pair": pair_text,
            "available": False,
            "converged": True,
            "reason": "projection_convergence_unavailable",
            "portfolio_qty": portfolio_qty,
            "projected_total_qty": 0.0,
            "portfolio_delta_qty": 0.0,
            "projected_qty_excess": 0.0,
            "projected_qty_shortfall": 0.0,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.0,
            "decomposed_total_qty": 0.0,
            "lot_row_count": 0,
            "executable_lot_count": 0,
            "dust_tracking_lot_count": 0,
        }

    lot_row_count = _row_int(lot_row, "lot_row_count")
    projected_total_qty = normalize_asset_qty(_row_float(lot_row, "projected_total_qty"))
    open_exposure_qty = normalize_asset_qty(_row_float(lot_row, "open_exposure_qty"))
    dust_tracking_qty = normalize_asset_qty(_row_float(lot_row, "dust_tracking_qty"))
    executable_lot_count = _row_int(lot_row, "executable_lot_count")
    dust_tracking_lot_count = _row_int(lot_row, "dust_tracking_lot_count")
    decomposed_total_qty = normalize_asset_qty(open_exposure_qty + dust_tracking_qty)
    portfolio_delta_qty = normalize_asset_qty(projected_total_qty - portfolio_qty)
    projected_qty_excess = normalize_asset_qty(max(0.0, portfolio_delta_qty))
    projected_qty_shortfall = normalize_asset_qty(max(0.0, -portfolio_delta_qty))

    reasons: list[str] = []
    if abs(portfolio_delta_qty) > _EPS:
        reasons.append(
            "portfolio_projection_qty_mismatch="
            f"projected_total_qty={projected_total_qty:.12f},portfolio_qty={portfolio_qty:.12f}"
        )
    if abs(projected_total_qty - decomposed_total_qty) > _EPS:
        reasons.append(
            "projection_decomposition_mismatch="
            f"projected_total_qty={projected_total_qty:.12f},"
            f"open_plus_dust_qty={decomposed_total_qty:.12f}"
        )
    if open_exposure_qty > _EPS and executable_lot_count <= 0:
        reasons.append("open_exposure_qty_without_executable_lots")
    if open_exposure_qty <= _EPS and executable_lot_count > 0:
        reasons.append("executable_lots_without_open_exposure_qty")
    if dust_tracking_qty > _EPS and dust_tracking_lot_count <= 0:
        reasons.append("dust_tracking_qty_without_dust_lots")
    if dust_tracking_qty <= _EPS and dust_tracking_lot_count > 0:
        reasons.append("dust_lots_without_dust_tracking_qty")

    return {
        "pair": pair_text,
        "available": True,
        "converged": not reasons,
        "reason": "none" if not reasons else ";".join(reasons),
        "portfolio_qty": portfolio_qty,
        "projected_total_qty": projected_total_qty,
        "portfolio_delta_qty": portfolio_delta_qty,
        "projected_qty_excess": projected_qty_excess,
        "projected_qty_shortfall": projected_qty_shortfall,
        "open_exposure_qty": open_exposure_qty,
        "dust_tracking_qty": dust_tracking_qty,
        "decomposed_total_qty": decomposed_total_qty,
        "lot_row_count": lot_row_count,
        "executable_lot_count": executable_lot_count,
        "dust_tracking_lot_count": dust_tracking_lot_count,
    }


def _matching_partial_close_residual_repair_present(
    conn,
    *,
    target_trade_id: int,
    sell_trade_ids: list[int],
    expected_residual_qty: float,
) -> bool:
    if target_trade_id <= 0 or not sell_trade_ids:
        return False
    rows = conn.execute(
        """
        SELECT repair_basis
        FROM position_authority_repairs
        WHERE reason=?
        ORDER BY event_ts DESC, id DESC
        LIMIT 20
        """,
        (PARTIAL_CLOSE_RESIDUAL_REPAIR_REASON,),
    ).fetchall()
    expected_sell_ids = [int(value) for value in sell_trade_ids]
    for row in rows:
        try:
            basis = json.loads(str(row["repair_basis"]))
        except (TypeError, ValueError, json.JSONDecodeError, KeyError, IndexError):
            continue
        if int(basis.get("target_trade_id") or 0) != int(target_trade_id):
            continue
        basis_sell_ids = [int(value) for value in basis.get("sell_trade_ids") or []]
        if basis_sell_ids != expected_sell_ids:
            continue
        try:
            basis_residual_qty = normalize_asset_qty(float(basis.get("expected_residual_qty") or 0.0))
        except (TypeError, ValueError):
            continue
        if abs(basis_residual_qty - normalize_asset_qty(expected_residual_qty)) <= _EPS:
            return True
    return False


def _matching_portfolio_projection_repair_present(
    conn,
    *,
    target_trade_id: int,
    target_remainder_qty: float,
    portfolio_qty: float,
) -> bool:
    if target_trade_id <= 0:
        return False
    rows = conn.execute(
        """
        SELECT repair_basis
        FROM position_authority_repairs
        WHERE reason=?
        ORDER BY event_ts DESC, id DESC
        LIMIT 20
        """,
        (PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON,),
    ).fetchall()
    expected_remainder = normalize_asset_qty(target_remainder_qty)
    expected_portfolio = normalize_asset_qty(portfolio_qty)
    for row in rows:
        try:
            basis = json.loads(str(row["repair_basis"]))
        except (TypeError, ValueError, json.JSONDecodeError, KeyError, IndexError):
            continue
        if int(basis.get("target_trade_id") or 0) != int(target_trade_id):
            continue
        try:
            basis_remainder = normalize_asset_qty(float(basis.get("target_remainder_qty") or 0.0))
            basis_portfolio = normalize_asset_qty(float(basis.get("portfolio_qty") or 0.0))
        except (TypeError, ValueError):
            continue
        if abs(basis_remainder - expected_remainder) <= _EPS and abs(basis_portfolio - expected_portfolio) <= _EPS:
            return True
    return False


def _repair_event_status(*, recorded: bool, state_converged: bool) -> str:
    if not recorded:
        return "none"
    if state_converged:
        return "recorded_and_state_converged"
    return "recorded_but_not_current_state_proof"


def _partial_close_residual_state_converged(
    conn,
    *,
    pair: str,
    target_trade_id: int,
    sell_trade_ids: list[int],
    expected_residual_qty: float,
    expected_closed_qty: float,
    target_total_qty: float,
    target_open_qty: float,
    target_dust_qty: float,
    target_executable_lot_count: int,
    target_dust_lot_count: int,
    target_min_internal_lot_size: float,
    target_max_internal_lot_size: float,
    expected_internal_lot_size: float,
) -> bool:
    """Return True when current tables already reflect the post-replay state.

    A partial-close residual replay is only complete when the present authority
    rows and lifecycle rows have converged. A historical repair event alone is
    evidence, not proof that the current DB state is still converged.
    """

    if target_trade_id <= 0 or not sell_trade_ids:
        return False
    expected_residual = normalize_asset_qty(expected_residual_qty)
    expected_closed = normalize_asset_qty(expected_closed_qty)
    if expected_residual <= _EPS or expected_closed <= _EPS:
        return False
    if abs(normalize_asset_qty(target_total_qty) - expected_residual) > _EPS:
        return False
    if abs(normalize_asset_qty(target_dust_qty) - expected_residual) > _EPS:
        return False
    if abs(normalize_asset_qty(target_open_qty)) > _EPS:
        return False
    if int(target_executable_lot_count) != 0 or int(target_dust_lot_count) <= 0:
        return False
    if expected_internal_lot_size > _EPS and (
        abs(float(target_min_internal_lot_size) - float(expected_internal_lot_size)) > _EPS
        or abs(float(target_max_internal_lot_size) - float(expected_internal_lot_size)) > _EPS
    ):
        return False

    placeholders = ",".join("?" for _ in sell_trade_ids)
    try:
        lifecycle_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS lifecycle_count,
                COALESCE(SUM(matched_qty), 0.0) AS matched_qty
            FROM trade_lifecycles
            WHERE pair=?
              AND entry_trade_id=?
              AND exit_trade_id IN ({placeholders})
            """,
            (str(pair), int(target_trade_id), *[int(value) for value in sell_trade_ids]),
        ).fetchone()
    except (AssertionError, sqlite3.OperationalError):
        return False

    lifecycle_count = _row_int(lifecycle_row, "lifecycle_count")
    lifecycle_matched_qty = normalize_asset_qty(_row_float(lifecycle_row, "matched_qty"))
    return bool(lifecycle_count > 0 and abs(lifecycle_matched_qty - expected_closed) <= _EPS)


def build_position_authority_assessment(conn, *, pair: str | None = None) -> dict[str, Any]:
    """Classify position-authority recovery state from one DB snapshot.

    This is read-only and deliberately separates observed/accounted BUY
    evidence from executable lot authority. It detects the repairable incident
    class where the latest accounted BUY still exists as one dust-only lot even
    though order/fill lot metadata says executable exposure should exist.
    """

    pair_text = str(pair or settings.PAIR)
    try:
        latest_buy = conn.execute(
            """
            SELECT
                t.id AS trade_id,
                t.client_order_id,
                t.ts AS fill_ts,
                t.price,
                t.qty,
                t.fee,
                t.strategy_name,
                t.entry_decision_id,
                f.fill_id,
                f.intended_lot_count AS fill_intended_lot_count,
                f.executable_lot_count AS fill_executable_lot_count,
                f.internal_lot_size AS fill_internal_lot_size,
                o.status AS order_status,
                o.qty_filled AS order_qty_filled,
                o.intended_lot_count AS order_intended_lot_count,
                o.executable_lot_count AS order_executable_lot_count,
                o.internal_lot_size AS order_internal_lot_size
            FROM trades t
            LEFT JOIN fills f
              ON f.client_order_id=t.client_order_id
             AND f.fill_ts=t.ts
             AND ABS(f.price-t.price) < 1e-12
             AND ABS(f.qty-t.qty) < 1e-12
            LEFT JOIN orders o
              ON o.client_order_id=t.client_order_id
            WHERE t.pair=? AND t.side='BUY'
            ORDER BY t.ts DESC, t.id DESC
            LIMIT 1
            """,
            (pair_text,),
        ).fetchone()
    except (AssertionError, sqlite3.OperationalError):
        return {
            "needs_correction": False,
            "safe_to_correct": False,
            "reason": "position_authority_assessment_unavailable",
            "target_trade_id": None,
            "recommended_action": "review_recovery_report",
        }

    if latest_buy is None:
        return {
            "needs_correction": False,
            "safe_to_correct": False,
            "reason": "latest_accounted_buy_missing",
            "target_trade_id": None,
            "recommended_action": "review_recovery_report",
        }

    target_trade_id = _row_int(latest_buy, "trade_id")
    client_order_id = _row_text(latest_buy, "client_order_id")
    fill_id = _row_text(latest_buy, "fill_id") or None
    fill_ts = _row_int(latest_buy, "fill_ts")
    fill_qty = normalize_asset_qty(_row_float(latest_buy, "qty"))
    fill_price = _row_float(latest_buy, "price")
    order_status = _row_text(latest_buy, "order_status", "unknown")
    canonical_lot_size = _row_float(latest_buy, "fill_internal_lot_size")
    if canonical_lot_size <= _EPS:
        canonical_lot_size = _row_float(latest_buy, "order_internal_lot_size")
    canonical_executable_lot_count = _row_int(latest_buy, "fill_executable_lot_count")
    if canonical_executable_lot_count <= 0:
        canonical_executable_lot_count = _row_int(latest_buy, "order_executable_lot_count")
    canonical_intended_lot_count = _row_int(latest_buy, "fill_intended_lot_count")
    if canonical_intended_lot_count <= 0:
        canonical_intended_lot_count = _row_int(latest_buy, "order_intended_lot_count")

    lot_row = conn.execute(
        """
        SELECT
            COUNT(*) AS lot_row_count,
            COALESCE(SUM(qty_open), 0.0) AS total_qty_open,
            COALESCE(SUM(CASE WHEN position_state='open_exposure' THEN qty_open ELSE 0.0 END), 0.0)
                AS open_exposure_qty,
            COALESCE(SUM(CASE WHEN position_state='dust_tracking' THEN qty_open ELSE 0.0 END), 0.0)
                AS dust_tracking_qty,
            COALESCE(SUM(executable_lot_count), 0) AS executable_lot_count,
            COALESCE(SUM(dust_tracking_lot_count), 0) AS dust_tracking_lot_count,
            COALESCE(MAX(internal_lot_size), 0.0) AS max_internal_lot_size,
            COALESCE(MIN(internal_lot_size), 0.0) AS min_internal_lot_size
        FROM open_position_lots
        WHERE pair=?
          AND entry_trade_id=?
          AND qty_open > 1e-12
        """,
        (pair_text, target_trade_id),
    ).fetchone()
    sell_after_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt, COALESCE(SUM(qty), 0.0) AS qty
        FROM trades
        WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))
        """,
        (pair_text, fill_ts, fill_ts, target_trade_id),
    ).fetchone()
    sell_after_rows = conn.execute(
        """
        SELECT id, client_order_id, ts, price, qty, fee
        FROM trades
        WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))
        ORDER BY ts ASC, id ASC
        """,
        (pair_text, fill_ts, fill_ts, target_trade_id),
    ).fetchall()
    other_active_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt, COALESCE(SUM(qty_open), 0.0) AS qty
        FROM open_position_lots
        WHERE pair=?
          AND qty_open > 1e-12
          AND entry_trade_id != ?
        """,
        (pair_text, target_trade_id),
    ).fetchone()
    portfolio_row = conn.execute(
        "SELECT asset_qty, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    portfolio_qty = 0.0
    if portfolio_row is not None:
        try:
            keys = portfolio_row.keys()
        except AttributeError:
            keys = ()
        if "asset_available" in keys:
            portfolio_qty = normalize_asset_qty(
                _row_float(portfolio_row, "asset_available") + _row_float(portfolio_row, "asset_locked")
            )
        else:
            portfolio_qty = normalize_asset_qty(_row_float(portfolio_row, "asset_qty"))

    lot_row_count = _row_int(lot_row, "lot_row_count")
    target_total_qty = normalize_asset_qty(_row_float(lot_row, "total_qty_open"))
    target_open_qty = normalize_asset_qty(_row_float(lot_row, "open_exposure_qty"))
    target_dust_qty = normalize_asset_qty(_row_float(lot_row, "dust_tracking_qty"))
    target_executable_lot_count = _row_int(lot_row, "executable_lot_count")
    target_dust_lot_count = _row_int(lot_row, "dust_tracking_lot_count")
    target_min_internal_lot_size = _row_float(lot_row, "min_internal_lot_size")
    target_max_internal_lot_size = _row_float(lot_row, "max_internal_lot_size")
    sell_after_count = _row_int(sell_after_row, "cnt")
    sell_after_qty = normalize_asset_qty(_row_float(sell_after_row, "qty"))
    sell_trade_ids = [_row_int(row, "id") for row in sell_after_rows]
    other_active_lot_count = _row_int(other_active_row, "cnt")
    other_active_qty = normalize_asset_qty(_row_float(other_active_row, "qty"))
    projection_convergence = build_lot_projection_convergence(conn, pair=pair_text)
    canonical_executable_qty = normalize_asset_qty(
        float(canonical_lot_size) * float(max(0, canonical_executable_lot_count))
    )

    conflicting_dust_authority = bool(
        lot_row_count > 0
        and target_executable_lot_count <= 0
        and target_dust_lot_count > 0
        and target_dust_qty > _EPS
        and canonical_executable_lot_count > 0
        and canonical_lot_size > _EPS
    )
    target_qty_matches_fill = bool(abs(target_total_qty - fill_qty) <= 1e-12)
    portfolio_matches_target = bool(portfolio_qty <= _EPS or abs(portfolio_qty - target_total_qty) <= 1e-12)
    expected_residual_qty = normalize_asset_qty(max(0.0, fill_qty - sell_after_qty))
    projected_total_qty = normalize_asset_qty(target_total_qty + other_active_qty)
    projected_qty_excess = normalize_asset_qty(max(0.0, projected_total_qty - portfolio_qty))
    portfolio_target_remainder_qty = normalize_asset_qty(max(0.0, portfolio_qty - other_active_qty))
    projection_repair_removable_qty = normalize_asset_qty(max(0.0, target_total_qty - portfolio_target_remainder_qty))
    projection_repair_covers_excess = bool(projected_qty_excess <= projection_repair_removable_qty + _EPS)
    partial_close_residual_candidate = bool(
        conflicting_dust_authority
        and sell_after_count > 0
        and canonical_executable_qty > _EPS
        and abs(sell_after_qty - canonical_executable_qty) <= _EPS
        and expected_residual_qty > _EPS
        and abs(target_total_qty - expected_residual_qty) <= _EPS
        and abs(target_open_qty) <= _EPS
        and target_dust_lot_count > 0
        and other_active_lot_count == 0
        and abs(other_active_qty) <= _EPS
        and portfolio_matches_target
    )
    portfolio_projection_divergence_candidate = bool(
        lot_row_count > 0
        and target_open_qty > _EPS
        and target_executable_lot_count > 0
        and canonical_executable_lot_count > 0
        and canonical_lot_size > _EPS
        and sell_after_count == 0
        and portfolio_qty > _EPS
        and projected_qty_excess > _EPS
        and portfolio_target_remainder_qty < canonical_lot_size - _EPS
        and portfolio_target_remainder_qty <= target_total_qty + _EPS
    )
    residual_normalization_recorded = _matching_partial_close_residual_repair_present(
        conn,
        target_trade_id=target_trade_id,
        sell_trade_ids=sell_trade_ids,
        expected_residual_qty=expected_residual_qty,
    )
    residual_state_converged = _partial_close_residual_state_converged(
        conn,
        pair=pair_text,
        target_trade_id=target_trade_id,
        sell_trade_ids=sell_trade_ids,
        expected_residual_qty=expected_residual_qty,
        expected_closed_qty=sell_after_qty,
        target_total_qty=target_total_qty,
        target_open_qty=target_open_qty,
        target_dust_qty=target_dust_qty,
        target_executable_lot_count=target_executable_lot_count,
        target_dust_lot_count=target_dust_lot_count,
        target_min_internal_lot_size=target_min_internal_lot_size,
        target_max_internal_lot_size=target_max_internal_lot_size,
        expected_internal_lot_size=canonical_lot_size,
    )
    needs_residual_normalization = bool(
        partial_close_residual_candidate and not residual_state_converged
    )
    portfolio_projection_repair_recorded = _matching_portfolio_projection_repair_present(
        conn,
        target_trade_id=target_trade_id,
        target_remainder_qty=portfolio_target_remainder_qty,
        portfolio_qty=portfolio_qty,
    )
    portfolio_projection_state_converged = bool(
        portfolio_projection_repair_recorded
        and lot_row_count > 0
        and abs(target_total_qty - portfolio_target_remainder_qty) <= _EPS
        and abs(target_open_qty) <= _EPS
        and abs(target_dust_qty - portfolio_target_remainder_qty) <= _EPS
        and target_executable_lot_count == 0
        and target_dust_lot_count > 0
        and target_min_internal_lot_size > _EPS
        and abs(target_min_internal_lot_size - canonical_lot_size) <= _EPS
        and abs(target_max_internal_lot_size - canonical_lot_size) <= _EPS
    )
    needs_correction = bool(
        conflicting_dust_authority
        and not partial_close_residual_candidate
        and not portfolio_projection_state_converged
    )
    needs_portfolio_projection_repair = bool(portfolio_projection_divergence_candidate)

    projection_internal_lot_size = 0.0
    projection_semantic_version: int | None = None
    if target_min_internal_lot_size > _EPS and abs(target_max_internal_lot_size - target_min_internal_lot_size) <= _EPS:
        projection_internal_lot_size = float(target_min_internal_lot_size)
        projection_semantic_version = 1

    authoritative_contract = build_quantity_contract_snapshot(
        requested_qty=float(fill_qty),
        exchange_constrained_qty=float(fill_qty),
        internal_lot_size=(canonical_lot_size if canonical_lot_size > _EPS else None),
        intended_lot_count=(canonical_intended_lot_count if canonical_intended_lot_count > 0 else None),
        executable_lot_count=int(canonical_executable_lot_count),
        residual_reason=("dust_only_remainder" if fill_qty - canonical_executable_qty > _EPS else "none"),
        provenance="accounted_buy_evidence",
        semantic_version=(1 if canonical_lot_size > _EPS else None),
    )
    projection_contract = build_quantity_contract_snapshot(
        requested_qty=float(target_total_qty),
        exchange_constrained_qty=float(target_total_qty),
        internal_lot_size=(projection_internal_lot_size if projection_internal_lot_size > _EPS else None),
        intended_lot_count=(
            target_executable_lot_count + target_dust_lot_count
            if lot_row_count > 0
            else None
        ),
        executable_lot_count=int(target_executable_lot_count),
        residual_reason=(
            "dust_tracking_projection"
            if target_dust_qty > _EPS
            else ("open_exposure_projection" if target_open_qty > _EPS else "none")
        ),
        provenance="open_position_lots_projection",
        semantic_version=projection_semantic_version,
    )

    diagnostic_flags: list[str] = []
    if not bool(projection_convergence.get("converged")):
        diagnostic_flags.append("projection_diverged")
    if (
        (lot_row_count > 1 and target_executable_lot_count <= 0 and target_dust_lot_count > 1)
        or (
            other_active_lot_count > 1
            and other_active_qty > _EPS
            and target_total_qty > _EPS
        )
    ):
        diagnostic_flags.append("historical_fragmentation")
    if conflicting_dust_authority or (
        authoritative_contract.internal_lot_size is not None
        and projection_contract.internal_lot_size is not None
        and abs(
            float(authoritative_contract.internal_lot_size)
            - float(projection_contract.internal_lot_size)
        ) > _EPS
    ):
        diagnostic_flags.append("semantic_contract_mismatch")
    if needs_portfolio_projection_repair and not projection_repair_covers_excess:
        diagnostic_flags.append("unsafe_auto_repair")
    alignment_state = diagnostic_flags[0] if diagnostic_flags else "same_truth"

    blockers: list[str] = []
    if not needs_correction and not needs_residual_normalization and not needs_portfolio_projection_repair:
        blockers.append("no_repairable_authority_conflict")
    if sell_after_count > 0 and not needs_residual_normalization and not needs_portfolio_projection_repair:
        blockers.append(f"sell_after_target_buy={sell_after_count}")
    if not target_qty_matches_fill and not needs_residual_normalization and not needs_portfolio_projection_repair:
        blockers.append(
            f"target_lot_qty_fill_mismatch=target_qty={target_total_qty:.12f},fill_qty={fill_qty:.12f}"
        )
    if not portfolio_matches_target and not needs_portfolio_projection_repair:
        blockers.append(
            f"portfolio_target_qty_mismatch=portfolio_qty={portfolio_qty:.12f},target_qty={target_total_qty:.12f}"
        )
    if order_status not in {"FILLED", "PARTIAL", "NEW", "unknown"}:
        blockers.append(f"order_status={order_status}")
    if needs_portfolio_projection_repair and not projection_repair_covers_excess:
        blockers.append(
            "projection_excess_outside_target="
            f"projected_qty_excess={projected_qty_excess:.12f},"
            f"repair_removable_qty={projection_repair_removable_qty:.12f},"
            f"other_active_qty={other_active_qty:.12f}"
        )

    safe_to_normalize_residual = bool(needs_residual_normalization and not blockers)
    safe_to_repair_portfolio_projection = bool(needs_portfolio_projection_repair and not blockers)
    safe_to_correct = bool((needs_correction and not blockers) or safe_to_normalize_residual)
    if safe_to_normalize_residual or safe_to_correct:
        repair_action_state = "safe_to_apply_now"
    elif safe_to_repair_portfolio_projection:
        repair_action_state = "safe_to_apply_now"
    elif needs_portfolio_projection_repair and any(
        str(item).startswith("projection_excess_outside_target=") for item in blockers
    ):
        repair_action_state = "inspect_only"
    elif needs_correction or needs_residual_normalization or needs_portfolio_projection_repair:
        repair_action_state = "blocked_pending_evidence"
    else:
        repair_action_state = "not_applicable"
    if safe_to_normalize_residual:
        reason = "partial-close residual authority normalization applicable"
    elif safe_to_repair_portfolio_projection:
        reason = "portfolio-anchored projection repair requires broker/portfolio evidence gates"
    elif safe_to_correct:
        reason = "position authority correction applicable"
    else:
        reason = ", ".join(blockers)
    residual_repair_event_status = _repair_event_status(
        recorded=residual_normalization_recorded,
        state_converged=residual_state_converged,
    )
    portfolio_projection_repair_event_status = _repair_event_status(
        recorded=portfolio_projection_repair_recorded,
        state_converged=portfolio_projection_state_converged,
    )
    truth_model = {
        "canonical_truth_source": "orders_fills_trades_plus_portfolio",
        "projection_truth_source": "open_position_lots_materialized_projection",
        "projection_role": "rebuildable_materialized_view",
        "repair_event_role": "historical_evidence_not_current_state_proof",
        "portfolio_asset_qty": float(portfolio_qty),
        "projected_total_qty": float(projected_total_qty),
        "projection_delta_qty": float(projected_total_qty - portfolio_qty),
        "projected_qty_excess": float(projected_qty_excess),
        "projected_qty_shortfall": float(max(0.0, portfolio_qty - projected_total_qty)),
        "projection_converged": bool(projection_convergence.get("converged")),
        "projection_non_convergence_reason": str(projection_convergence.get("reason") or "none"),
        "alignment_state": alignment_state,
        "repair_action_state": repair_action_state,
        "inspect_only": bool(repair_action_state == "inspect_only"),
        "residual_repair_event_status": residual_repair_event_status,
        "portfolio_projection_repair_event_status": portfolio_projection_repair_event_status,
    }
    return {
        "incident_class": (
            "PROJECTION_PORTFOLIO_DIVERGENCE"
            if needs_portfolio_projection_repair
            else (
                "PROJECTION_RESIDUAL_DIVERGENCE"
                if needs_residual_normalization
                else ("LOT_AUTHORITY_CONFLICT" if needs_correction else "NONE")
            )
        ),
        "needs_correction": needs_correction,
        "needs_residual_normalization": needs_residual_normalization,
        "needs_portfolio_projection_repair": needs_portfolio_projection_repair,
        "safe_to_correct": safe_to_correct,
        "safe_to_normalize_residual": safe_to_normalize_residual,
        "safe_to_repair_portfolio_projection": safe_to_repair_portfolio_projection,
        "reason": reason,
        "recommended_action": (
            "apply_rebuild_position_authority"
            if safe_to_correct or safe_to_repair_portfolio_projection
            else "review_recovery_report"
        ),
        "repair_mode": (
            "portfolio_projection_repair"
            if needs_portfolio_projection_repair
            else ("residual_normalization" if needs_residual_normalization else "correction")
        ),
        "repair_reason": (
            PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON
            if needs_portfolio_projection_repair
            else None
        ),
        "target_trade_id": target_trade_id,
        "target_client_order_id": client_order_id,
        "target_fill_id": fill_id,
        "target_fill_ts": fill_ts,
        "target_price": fill_price,
        "target_qty": fill_qty,
        "target_order_status": order_status,
        "canonical_internal_lot_size": canonical_lot_size,
        "canonical_intended_lot_count": canonical_intended_lot_count,
        "canonical_executable_lot_count": canonical_executable_lot_count,
        "canonical_executable_qty": canonical_executable_qty,
        "authoritative_quantity_contract": authoritative_contract.as_dict(),
        "projection_quantity_contract": projection_contract.as_dict(),
        "alignment_state": alignment_state,
        "diagnostic_flags": diagnostic_flags,
        "repair_action_state": repair_action_state,
        "existing_lot_rows": lot_row_count,
        "existing_total_qty": target_total_qty,
        "existing_open_exposure_qty": target_open_qty,
        "existing_dust_tracking_qty": target_dust_qty,
        "existing_executable_lot_count": target_executable_lot_count,
        "existing_dust_tracking_lot_count": target_dust_lot_count,
        "existing_min_internal_lot_size": target_min_internal_lot_size,
        "existing_max_internal_lot_size": target_max_internal_lot_size,
        "sell_after_target_buy_count": sell_after_count,
        "sell_after_target_buy_qty": sell_after_qty,
        "sell_trade_ids": sell_trade_ids,
        "expected_residual_qty": expected_residual_qty,
        "projected_total_qty": projected_total_qty,
        "projected_qty_excess": projected_qty_excess,
        "projection_repair_removable_qty": projection_repair_removable_qty,
        "projection_repair_covers_excess": projection_repair_covers_excess,
        "portfolio_target_remainder_qty": portfolio_target_remainder_qty,
        "partial_close_residual_candidate": partial_close_residual_candidate,
        "portfolio_projection_divergence_candidate": portfolio_projection_divergence_candidate,
        "portfolio_projection_repair_recorded": portfolio_projection_repair_recorded,
        "portfolio_projection_state_converged": portfolio_projection_state_converged,
        "projection_convergence": projection_convergence,
        "projection_state_converged": bool(projection_convergence.get("converged")),
        "projection_non_convergence_reason": str(projection_convergence.get("reason") or "none"),
        "residual_normalization_recorded": residual_normalization_recorded,
        "residual_repair_event_present": residual_normalization_recorded,
        "residual_repair_event_status": residual_repair_event_status,
        "residual_state_converged": residual_state_converged,
        "portfolio_projection_repair_event_status": portfolio_projection_repair_event_status,
        "other_active_lot_count": other_active_lot_count,
        "other_active_qty": other_active_qty,
        "portfolio_qty": portfolio_qty,
        "blockers": blockers,
        "truth_model": truth_model,
    }
