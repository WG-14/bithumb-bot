from __future__ import annotations

import logging
import math
import sqlite3
from typing import Any

from .config import settings
from .db_core import ensure_db, get_portfolio_breakdown, init_portfolio, set_portfolio_breakdown
from .lifecycle import apply_fill_lifecycle
from .notifier import format_event, notify
from .observability import record_fill_fee_anomaly
from .oms import add_fill, create_order, set_exchange_order_id, set_status

_LOG = logging.getLogger(__name__)


def order_fill_tolerance(qty_req: float | None = None) -> float:
    base = max(1e-12, abs(float(settings.LIVE_ORDER_QTY_STEP or 0.0)) * 0.51)
    if qty_req is None:
        return base
    return max(base, abs(float(qty_req)) * 1e-9)


def record_order_if_missing(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    qty_req: float,
    submit_attempt_id: str | None = None,
    price: float | None,
    ts_ms: int,
    status: str = "NEW",
) -> None:
    exists = conn.execute(
        "SELECT 1 FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if exists:
        return
    create_order(
        client_order_id=client_order_id,
        side=side,
        qty_req=qty_req,
        submit_attempt_id=submit_attempt_id,
        price=price,
        status=status,
        ts_ms=ts_ms,
        conn=conn,
    )


def _fill_exists(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
) -> bool:
    if fill_id:
        row = conn.execute(
            """
            SELECT 1 FROM fills
            WHERE client_order_id=? AND fill_id=?
            LIMIT 1
            """,
            (client_order_id, fill_id),
        ).fetchone()
        if row is not None:
            return True

    row = conn.execute(
        """
        SELECT 1 FROM fills
        WHERE client_order_id=? AND fill_ts=? AND ABS(price-?) < 1e-12 AND ABS(qty-?) < 1e-12
        LIMIT 1
        """,
        (client_order_id, int(fill_ts), float(price), float(qty)),
    ).fetchone()
    return row is not None


def _aggregate_fill_duplicate_reason(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    qty: float,
) -> str | None:
    fill_id_text = str(fill_id or "")
    if ":aggregate:" not in fill_id_text:
        return None

    row = conn.execute(
        """
        SELECT COUNT(*) AS fill_count, COALESCE(SUM(qty), 0.0) AS total_qty
        FROM fills
        WHERE client_order_id=?
        """,
        (client_order_id,),
    ).fetchone()
    if row is None:
        return None

    fill_count = int(row["fill_count"] or 0)
    existing_total_qty = float(row["total_qty"] or 0.0)
    tol = order_fill_tolerance(existing_total_qty if existing_total_qty > 0 else qty)
    if fill_count <= 0 or abs(existing_total_qty - float(qty)) > tol:
        return None
    return (
        "aggregate_snapshot_already_accounted "
        f"fill_id={fill_id_text} existing_total_qty={existing_total_qty:.12g} "
        f"incoming_qty={float(qty):.12g} tolerance={tol:.12g}"
    )


def apply_fill_and_trade(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    note: str | None = None,
) -> dict[str, Any] | None:
    eps = 1e-12

    if qty <= 0:
        raise RuntimeError(f"invalid fill qty for {client_order_id}: {qty}")
    if price <= 0:
        raise RuntimeError(f"invalid fill price for {client_order_id}: {price}")
    if fee < 0:
        raise RuntimeError(f"invalid fill fee for {client_order_id}: {fee}")
    if side not in ("BUY", "SELL"):
        raise RuntimeError(f"invalid fill side for {client_order_id}: {side}")

    price_value = float(price)
    qty_value = float(qty)
    fee_value = float(fee)
    fill_id_value = fill_id or "-"
    notional_value = price_value * qty_value if math.isfinite(price_value) and math.isfinite(qty_value) else 0.0
    fee_ratio_value: float | None = None
    if notional_value > eps and math.isfinite(fee_value):
        fee_ratio_value = fee_value / notional_value

    min_notional = max(0.0, float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW))
    min_fee_ratio = max(0.0, float(settings.LIVE_FILL_FEE_RATIO_MIN))
    max_fee_ratio = max(min_fee_ratio, float(settings.LIVE_FILL_FEE_RATIO_MAX))
    should_check_live_fee_anomaly = (
        settings.MODE == "live"
        and math.isfinite(notional_value)
        and notional_value >= min_notional
    )
    if should_check_live_fee_anomaly and abs(fee_value) <= eps:
        record_fill_fee_anomaly(
            anomaly_type="zero_fee",
            mode=settings.MODE,
            client_order_id=client_order_id,
            fill_id=fill_id_value,
            side=side,
            price=price_value,
            qty=qty_value,
            notional=notional_value,
            fee=fee_value,
            fee_ratio=fee_ratio_value,
            min_notional=min_notional,
            min_fee_ratio=min_fee_ratio,
            max_fee_ratio=max_fee_ratio,
        )
    elif (
        should_check_live_fee_anomaly
        and fee_ratio_value is not None
        and math.isfinite(fee_ratio_value)
        and (fee_ratio_value < min_fee_ratio or fee_ratio_value > max_fee_ratio)
    ):
        record_fill_fee_anomaly(
            anomaly_type="fee_ratio_outlier",
            mode=settings.MODE,
            client_order_id=client_order_id,
            fill_id=fill_id_value,
            side=side,
            price=price_value,
            qty=qty_value,
            notional=notional_value,
            fee=fee_value,
            fee_ratio=fee_ratio_value,
            min_notional=min_notional,
            min_fee_ratio=min_fee_ratio,
            max_fee_ratio=max_fee_ratio,
        )

    init_portfolio(conn)
    duplicate_reason: str | None = None
    if _fill_exists(conn, client_order_id=client_order_id, fill_id=fill_id, fill_ts=fill_ts, price=price, qty=qty):
        duplicate_reason = (
            "existing_fill_identity "
            f"client_order_id={client_order_id} fill_id={fill_id or '-'} fill_ts={int(fill_ts)} "
            f"price={float(price):.12g} qty={float(qty):.12g}"
        )
    else:
        duplicate_reason = _aggregate_fill_duplicate_reason(
            conn,
            client_order_id=client_order_id,
            fill_id=fill_id,
            qty=qty,
        )
    if duplicate_reason is not None:
        _LOG.info("fill_duplicate_skipped %s", duplicate_reason)
        return None

    order = conn.execute(
        "SELECT qty_req, qty_filled FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if order is not None:
        qty_req = float(order["qty_req"])
        qty_filled = float(order["qty_filled"])
        fill_tol = order_fill_tolerance(qty_req)
        projected_qty = qty_filled + float(qty)
        _LOG.info(
            "fill_apply_candidate client_order_id=%s fill_id=%s side=%s requested_qty=%.12g existing_filled_qty=%.12g incoming_fill_qty=%.12g projected_qty=%.12g tolerance=%.12g",
            client_order_id,
            fill_id or "-",
            side,
            qty_req,
            qty_filled,
            float(qty),
            projected_qty,
            fill_tol,
        )
        if projected_qty > qty_req + fill_tol:
            raise RuntimeError(
                f"overfill detected for {client_order_id}: existing={qty_filled}, fill={qty}, requested={qty_req}, tolerance={fill_tol}"
            )

    cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)

    def _consume_locked_then_available(locked: float, available: float, amount: float, *, field: str) -> tuple[float, float]:
        remaining = float(amount)
        locked_after = float(locked)
        available_after = float(available)

        from_locked = min(locked_after, remaining)
        locked_after -= from_locked
        remaining -= from_locked

        if remaining > eps:
            available_after -= remaining

        if locked_after < -eps or available_after < -eps:
            raise RuntimeError(
                f"negative {field} after fill for {client_order_id}: available={available_after}, locked={locked_after}, needed={amount}"
            )
        return max(locked_after, 0.0), max(available_after, 0.0)

    if side == "BUY":
        spend = (price * qty) + fee
        cash_locked_after, cash_available_after = _consume_locked_then_available(
            cash_locked,
            cash_available,
            spend,
            field="cash",
        )
        asset_available_after = asset_available + qty
        asset_locked_after = asset_locked
    else:
        cash_available_after = cash_available + (price * qty) - fee
        cash_locked_after = cash_locked
        asset_locked_after, asset_available_after = _consume_locked_then_available(
            asset_locked,
            asset_available,
            qty,
            field="asset",
        )

    cash_after = cash_available_after + cash_locked_after
    asset_after = asset_available_after + asset_locked_after

    if cash_after < -eps:
        raise RuntimeError(f"negative cash after fill for {client_order_id}: {cash_after}")
    if asset_after < -eps:
        raise RuntimeError(f"negative asset after fill for {client_order_id}: {asset_after}")

    add_fill(
        client_order_id=client_order_id,
        fill_id=fill_id,
        fill_ts=fill_ts,
        price=price,
        qty=qty,
        fee=fee,
        conn=conn,
    )

    set_portfolio_breakdown(
        conn,
        cash_available=max(cash_available_after, 0.0),
        cash_locked=max(cash_locked_after, 0.0),
        asset_available=max(asset_available_after, 0.0),
        asset_locked=max(asset_locked_after, 0.0),
    )
    trade_row = conn.execute(
        """
        INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(fill_ts),
            settings.PAIR,
            settings.INTERVAL,
            side,
            float(price),
            float(qty),
            float(fee),
            float(cash_after),
            float(asset_after),
            note,
        ),
    )
    trade_id = int(trade_row.lastrowid)
    apply_fill_lifecycle(
        conn,
        side=side,
        pair=settings.PAIR,
        trade_id=trade_id,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fill_ts=int(fill_ts),
        price=float(price),
        qty=float(qty),
        fee=float(fee),
    )
    notify(
        format_event(
            "fill_applied",
            pair=settings.PAIR,
            side=side,
            qty=float(qty),
            price=float(price),
            client_order_id=client_order_id,
            fill_id=fill_id,
        )
    )
    return {
        "ts": int(fill_ts),
        "side": side,
        "price": float(price),
        "qty": float(qty),
        "fee": float(fee),
        "cash": float(cash_after),
        "asset": float(asset_after),
        "client_order_id": client_order_id,
    }


def update_order_snapshot(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    exchange_order_id: str | None,
    status: str,
) -> None:
    if exchange_order_id:
        set_exchange_order_id(client_order_id, exchange_order_id, conn=conn)
    set_status(client_order_id, status, conn=conn)
