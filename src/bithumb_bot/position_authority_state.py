from __future__ import annotations

from typing import Any

from .config import settings
from .db_core import normalize_asset_qty


_EPS = 1e-12


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


def build_position_authority_assessment(conn, *, pair: str | None = None) -> dict[str, Any]:
    """Classify position-authority recovery state from one DB snapshot.

    This is read-only and deliberately separates observed/accounted BUY
    evidence from executable lot authority. It detects the repairable incident
    class where the latest accounted BUY still exists as one dust-only lot even
    though order/fill lot metadata says executable exposure should exist.
    """

    pair_text = str(pair or settings.PAIR)
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
        "SELECT COUNT(*) AS cnt FROM trades WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))",
        (pair_text, fill_ts, fill_ts, target_trade_id),
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
    sell_after_count = _row_int(sell_after_row, "cnt")

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
    needs_correction = bool(conflicting_dust_authority)

    blockers: list[str] = []
    if not needs_correction:
        blockers.append("no_repairable_authority_conflict")
    if sell_after_count > 0:
        blockers.append(f"sell_after_target_buy={sell_after_count}")
    if not target_qty_matches_fill:
        blockers.append(
            f"target_lot_qty_fill_mismatch=target_qty={target_total_qty:.12f},fill_qty={fill_qty:.12f}"
        )
    if not portfolio_matches_target:
        blockers.append(
            f"portfolio_target_qty_mismatch=portfolio_qty={portfolio_qty:.12f},target_qty={target_total_qty:.12f}"
        )
    if order_status not in {"FILLED", "PARTIAL", "NEW", "unknown"}:
        blockers.append(f"order_status={order_status}")

    safe_to_correct = bool(needs_correction and not blockers)
    reason = "position authority correction applicable" if safe_to_correct else ", ".join(blockers)
    return {
        "needs_correction": needs_correction,
        "safe_to_correct": safe_to_correct,
        "reason": reason,
        "recommended_action": (
            "apply_rebuild_position_authority" if safe_to_correct else "review_recovery_report"
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
        "existing_lot_rows": lot_row_count,
        "existing_total_qty": target_total_qty,
        "existing_open_exposure_qty": target_open_qty,
        "existing_dust_tracking_qty": target_dust_qty,
        "existing_executable_lot_count": target_executable_lot_count,
        "existing_dust_tracking_lot_count": target_dust_lot_count,
        "existing_min_internal_lot_size": _row_float(lot_row, "min_internal_lot_size"),
        "existing_max_internal_lot_size": _row_float(lot_row, "max_internal_lot_size"),
        "sell_after_target_buy_count": sell_after_count,
        "portfolio_qty": portfolio_qty,
        "blockers": blockers,
    }
