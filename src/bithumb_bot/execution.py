from __future__ import annotations

import json
import logging
import math
import sqlite3
from typing import Any

from .config import settings
from .db_core import (
    FILL_FEE_ACCOUNTING_STATUS_BLOCKED,
    FILL_FEE_ACCOUNTING_STATUS_FINALIZED,
    FILL_FEE_ACCOUNTING_STATUS_PENDING,
    calculate_fill_portfolio_snapshot,
    compute_accounting_replay,
    ensure_db,
    fill_fee_accounting_status,
    get_portfolio_breakdown,
    init_portfolio,
    load_matching_accounted_fill,
    set_portfolio_breakdown,
)
from .lifecycle import apply_fill_lifecycle
from .notifier import format_event, notify
from .observability import format_log_kv, record_fill_fee_anomaly
from .oms import add_fill, create_order, set_exchange_order_id, set_status

_LOG = logging.getLogger(__name__)


class LiveFillFeeValidationError(RuntimeError):
    """Raised when a materially sized live fill cannot be persisted safely."""


def order_fill_tolerance(qty_req: float | None = None) -> float:
    """Return the fill matching tolerance used by ledger-side overfill checks.

    This tolerance is for fill dedupe / overfill detection only. It should not
    be used to round order quantities; order sizing belongs to the submitter
    (for example, paper execution rounds down before the fill is applied).
    """
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
    symbol: str | None = None,
    strategy_name: str | None = None,
    entry_decision_id: int | None = None,
    exit_decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
    local_intent_state: str | None = None,
    ts_ms: int | None = None,
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
        symbol=symbol,
        strategy_name=strategy_name,
        entry_decision_id=entry_decision_id,
        exit_decision_id=exit_decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
        local_intent_state=local_intent_state,
        status=status,
        ts_ms=(int(ts_ms) if ts_ms is not None else None),
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


def _fill_identity_key(*, client_order_id: str, fill_ts: int, price: float, qty: float) -> tuple[str, int, float, float]:
    return (str(client_order_id), int(fill_ts), float(price), float(qty))


def _assign_fill_trade_id(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    trade_id: int,
) -> None:
    if fill_id:
        conn.execute(
            "UPDATE fills SET trade_id=? WHERE client_order_id=? AND fill_id=?",
            (int(trade_id), str(client_order_id), str(fill_id)),
        )
        return
    conn.execute(
        """
        UPDATE fills
        SET trade_id=?
        WHERE id=(
            SELECT id
            FROM fills
            WHERE client_order_id=?
              AND fill_ts=?
              AND ABS(price-?) < 1e-12
              AND ABS(qty-?) < 1e-12
            ORDER BY id DESC
            LIMIT 1
        )
        """,
        (int(trade_id), str(client_order_id), int(fill_ts), float(price), float(qty)),
    )


def _rebuild_trade_snapshots_from_fills(conn: sqlite3.Connection) -> None:
    fill_rows = conn.execute(
        """
        SELECT f.id, f.trade_id, f.client_order_id, f.fill_ts, f.price, f.qty, f.fee, o.side
        FROM fills f
        JOIN orders o ON o.client_order_id=f.client_order_id
        ORDER BY f.fill_ts ASC, f.id ASC
        """
    ).fetchall()
    fill_by_trade_id: dict[int, Any] = {}
    fill_queues: dict[tuple[str, int, float, float], list[Any]] = {}
    for row in fill_rows:
        trade_id = int(row["trade_id"] or 0)
        if trade_id > 0:
            fill_by_trade_id[trade_id] = row
            continue
        fill_queues.setdefault(
            _fill_identity_key(
                client_order_id=str(row["client_order_id"]),
                fill_ts=int(row["fill_ts"]),
                price=float(row["price"]),
                qty=float(row["qty"]),
            ),
            [],
        ).append(row)

    trade_rows = conn.execute(
        """
        SELECT id, client_order_id, ts, side, price, qty, fee
        FROM trades
        ORDER BY ts ASC, id ASC
        """
    ).fetchall()

    cash_available = float(settings.START_CASH_KRW)
    cash_locked = 0.0
    asset_available = 0.0
    asset_locked = 0.0
    updates: list[tuple[float, float, float, int]] = []
    for trade in trade_rows:
        trade_id = int(trade["id"])
        matched_fill = fill_by_trade_id.get(trade_id)
        if matched_fill is None:
            matched_fill = next(
                iter(
                    fill_queues.get(
                        _fill_identity_key(
                            client_order_id=str(trade["client_order_id"]),
                            fill_ts=int(trade["ts"]),
                            price=float(trade["price"]),
                            qty=float(trade["qty"]),
                        ),
                        [],
                    )
                ),
                None,
            )
            if matched_fill is not None:
                queue = fill_queues[_fill_identity_key(
                    client_order_id=str(trade["client_order_id"]),
                    fill_ts=int(trade["ts"]),
                    price=float(trade["price"]),
                    qty=float(trade["qty"]),
                )]
                queue.pop(0)
                if not queue:
                    fill_queues.pop(
                        _fill_identity_key(
                            client_order_id=str(trade["client_order_id"]),
                            fill_ts=int(trade["ts"]),
                            price=float(trade["price"]),
                            qty=float(trade["qty"]),
                        ),
                        None,
                    )
        effective_fee = (
            float(matched_fill["fee"])
            if matched_fill is not None and matched_fill["fee"] is not None
            else float(trade["fee"] or 0.0)
        )
        (
            cash_available,
            cash_locked,
            asset_available,
            asset_locked,
            cash_after,
            asset_after,
        ) = calculate_fill_portfolio_snapshot(
            cash_available=cash_available,
            cash_locked=cash_locked,
            asset_available=asset_available,
            asset_locked=asset_locked,
            side=str(trade["side"]),
            price=float(trade["price"]),
            qty=float(trade["qty"]),
            fee=effective_fee,
        )
        updates.append((effective_fee, float(cash_after), float(asset_after), trade_id))

    if updates:
        conn.executemany(
            "UPDATE trades SET fee=?, cash_after=?, asset_after=? WHERE id=?",
            updates,
        )


def _apply_fill_and_trade_core(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee_to_apply: float,
    fee_accounting_status: str,
    observed_fee_status: str,
    observed_fee_source: str,
    observed_fee_confidence: str,
    observed_fee_provenance: str | None,
    observed_fee_validation_reason: str | None,
    observed_fee_validation_checks: dict[str, object] | None,
    strategy_name: str | None = None,
    entry_decision_id: int | None = None,
    exit_decision_id: int | None = None,
    exit_reason: str | None = None,
    exit_rule_name: str | None = None,
    note: str | None = None,
    pair: str | None = None,
    signal_ts: int | None = None,
    allow_entry_decision_fallback: bool = True,
) -> dict[str, Any] | None:
    price_value = float(price)
    qty_value = float(qty)
    fee_value = float(fee_to_apply)

    if qty_value <= 0:
        raise RuntimeError(f"invalid fill qty for {client_order_id}: {qty}")
    if price_value <= 0:
        raise RuntimeError(f"invalid fill price for {client_order_id}: {price}")
    if fee_value < 0:
        raise RuntimeError(f"invalid fill fee for {client_order_id}: {fee_value}")
    if side not in ("BUY", "SELL"):
        raise RuntimeError(f"invalid fill side for {client_order_id}: {side}")

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
        """
        SELECT
            exchange_order_id,
            qty_req,
            qty_filled,
            strategy_name,
            entry_decision_id,
            exit_decision_id,
            decision_reason,
            exit_rule_name
        FROM orders
        WHERE client_order_id=?
        """,
        (client_order_id,),
    ).fetchone()
    order_strategy_name: str | None = None
    order_exchange_order_id: str | None = None
    order_entry_decision_id: int | None = None
    order_exit_decision_id: int | None = None
    order_decision_reason: str | None = None
    order_exit_rule_name: str | None = None
    submit_qty = float(qty)
    if order is not None:
        order_exchange_order_id = str(order["exchange_order_id"]) if order["exchange_order_id"] is not None else None
        qty_req = float(order["qty_req"])
        qty_filled = float(order["qty_filled"])
        submit_qty = float(qty_req)
        order_strategy_name = str(order["strategy_name"]) if order["strategy_name"] is not None else None
        order_entry_decision_id = int(order["entry_decision_id"]) if order["entry_decision_id"] is not None else None
        order_exit_decision_id = int(order["exit_decision_id"]) if order["exit_decision_id"] is not None else None
        order_decision_reason = str(order["decision_reason"]) if order["decision_reason"] is not None else None
        order_exit_rule_name = str(order["exit_rule_name"]) if order["exit_rule_name"] is not None else None
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
    (
        cash_available_after,
        cash_locked_after,
        asset_available_after,
        asset_locked_after,
        cash_after,
        asset_after,
    ) = calculate_fill_portfolio_snapshot(
        cash_available=cash_available,
        cash_locked=cash_locked,
        asset_available=asset_available,
        asset_locked=asset_locked,
        side=side,
        price=price,
        qty=qty,
        fee=fee_value,
    )

    add_fill(
        client_order_id=client_order_id,
        fill_id=fill_id,
        fill_ts=fill_ts,
        price=price,
        qty=qty,
        fee=fee_value,
        fee_accounting_status=fee_accounting_status,
        observed_fee_status=observed_fee_status,
        observed_fee_source=observed_fee_source,
        observed_fee_confidence=observed_fee_confidence,
        observed_fee_provenance=observed_fee_provenance,
        observed_fee_validation_reason=observed_fee_validation_reason,
        observed_fee_validation_checks=observed_fee_validation_checks,
        conn=conn,
    )

    set_portfolio_breakdown(
        conn,
        cash_available=max(cash_available_after, 0.0),
        cash_locked=max(cash_locked_after, 0.0),
        asset_available=max(asset_available_after, 0.0),
        asset_locked=max(asset_locked_after, 0.0),
    )
    effective_strategy_name = strategy_name or order_strategy_name
    effective_entry_decision_id = entry_decision_id if entry_decision_id is not None else order_entry_decision_id
    effective_exit_decision_id = exit_decision_id if exit_decision_id is not None else order_exit_decision_id
    effective_exit_reason = exit_reason or order_decision_reason
    effective_exit_rule_name = exit_rule_name or order_exit_rule_name

    trade_pair = pair or settings.PAIR
    effective_note = note
    if fee_accounting_status != FILL_FEE_ACCOUNTING_STATUS_FINALIZED:
        suffix = (
            f"fee_accounting_status={fee_accounting_status};"
            f"observed_fee_status={observed_fee_status};"
            f"observed_fee_source={observed_fee_source}"
        )
        effective_note = f"{note}; {suffix}" if note else suffix
    trade_row = conn.execute(
        """
        INSERT INTO trades(
            ts, pair, interval, side, price, qty, fee, cash_after, asset_after,
            client_order_id, strategy_name, entry_decision_id, exit_decision_id, exit_reason, exit_rule_name, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(fill_ts),
            trade_pair,
            settings.INTERVAL,
            side,
            float(price),
            float(qty),
            float(fee_value),
            float(cash_after),
            float(asset_after),
            str(client_order_id),
            effective_strategy_name,
            effective_entry_decision_id,
            effective_exit_decision_id,
            effective_exit_reason if side == "SELL" else None,
            effective_exit_rule_name if side == "SELL" else None,
            effective_note,
        ),
    )
    trade_id = int(trade_row.lastrowid)
    _assign_fill_trade_id(
        conn,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fill_ts=int(fill_ts),
        price=price_value,
        qty=qty_value,
        trade_id=trade_id,
    )
    apply_fill_lifecycle(
        conn,
        side=side,
        pair=trade_pair,
        trade_id=trade_id,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fill_ts=int(fill_ts),
        price=float(price),
        qty=float(qty),
        fee=float(fee_value),
        strategy_name=effective_strategy_name,
        entry_decision_id=effective_entry_decision_id,
        exit_decision_id=effective_exit_decision_id,
        exit_reason=(effective_exit_reason if side == "SELL" else None),
        exit_rule_name=(effective_exit_rule_name if side == "SELL" else None),
        allow_entry_decision_fallback=allow_entry_decision_fallback,
    )
    fill_signal_ts = int(signal_ts if signal_ts is not None else fill_ts)
    filled_qty = float(qty)
    _LOG.info(
        format_log_kv(
            "[ACCOUNTING] trade_applied",
            mode=settings.MODE,
            client_order_id=client_order_id,
            exchange_order_id=order_exchange_order_id or "-",
            signal_ts=fill_signal_ts,
            candle_ts=fill_signal_ts,
            side=side,
            submit_qty=submit_qty,
            filled_qty=filled_qty,
            post_trade_cash=float(cash_after),
            post_trade_asset=float(asset_after),
            fill_id=fill_id or "-",
            trade_id=trade_id,
        )
    )
    notify(
        format_event(
            "fill_applied",
            pair=trade_pair,
            side=side,
            qty=filled_qty,
            price=float(price),
            client_order_id=client_order_id,
            exchange_order_id=order_exchange_order_id or "-",
            signal_ts=fill_signal_ts,
            candle_ts=fill_signal_ts,
            submit_qty=submit_qty,
            filled_qty=filled_qty,
            post_trade_cash=float(cash_after),
            post_trade_asset=float(asset_after),
            fill_id=fill_id,
        )
    )
    return {
        "ts": int(fill_ts),
        "signal_ts": fill_signal_ts,
        "candle_ts": fill_signal_ts,
        "side": side,
        "price": float(price),
        "qty": filled_qty,
        "filled_qty": filled_qty,
        "submit_qty": submit_qty,
        "fee": float(fee_value),
        "cash": float(cash_after),
        "asset": float(asset_after),
        "post_trade_cash": float(cash_after),
        "post_trade_asset": float(asset_after),
        "client_order_id": client_order_id,
        "exchange_order_id": order_exchange_order_id,
        "fee_accounting_status": fee_accounting_status,
        "principal_applied": True,
    }


def apply_fill_and_trade(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float | None,
    strategy_name: str | None = None,
    entry_decision_id: int | None = None,
    exit_decision_id: int | None = None,
    exit_reason: str | None = None,
    exit_rule_name: str | None = None,
    note: str | None = None,
    pair: str | None = None,
    signal_ts: int | None = None,
    allow_entry_decision_fallback: bool = True,
) -> dict[str, Any] | None:
    eps = 1e-12
    fee_value = 0.0 if fee is None else float(fee)
    fill_id_value = fill_id or "-"
    notional_value = float(price) * float(qty) if math.isfinite(float(price)) and math.isfinite(float(qty)) else 0.0
    material_notional_threshold = max(0.0, float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW))
    if settings.MODE == "live" and fee is None and math.isfinite(notional_value) and notional_value >= material_notional_threshold:
        raise LiveFillFeeValidationError(
            "missing fill fee for materially sized live fill: "
            f"client_order_id={client_order_id} fill_id={fill_id_value} notional={notional_value:.12g}"
        )
    if (
        settings.MODE == "live"
        and math.isfinite(notional_value)
        and notional_value >= material_notional_threshold
        and abs(fee_value) <= eps
    ):
        raise LiveFillFeeValidationError(
            "zero fill fee blocked for materially sized live fill: "
            f"client_order_id={client_order_id} fill_id={fill_id_value} notional={notional_value:.12g}"
        )
    fee_ratio_value: float | None = None
    if notional_value > eps and math.isfinite(fee_value):
        fee_ratio_value = fee_value / notional_value

    min_notional = material_notional_threshold
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
            price=float(price),
            qty=float(qty),
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
            price=float(price),
            qty=float(qty),
            notional=notional_value,
            fee=fee_value,
            fee_ratio=fee_ratio_value,
            min_notional=min_notional,
            min_fee_ratio=min_fee_ratio,
            max_fee_ratio=max_fee_ratio,
        )

    return _apply_fill_and_trade_core(
        conn,
        client_order_id=client_order_id,
        side=side,
        fill_id=fill_id,
        fill_ts=fill_ts,
        price=price,
        qty=qty,
        fee_to_apply=fee_value,
        fee_accounting_status=FILL_FEE_ACCOUNTING_STATUS_FINALIZED,
        observed_fee_status="complete",
        observed_fee_source="trade_level_fee",
        observed_fee_confidence="authoritative",
        observed_fee_provenance="trade_level_fee_present",
        observed_fee_validation_reason="accounting_complete",
        observed_fee_validation_checks=None,
        strategy_name=strategy_name,
        entry_decision_id=entry_decision_id,
        exit_decision_id=exit_decision_id,
        exit_reason=exit_reason,
        exit_rule_name=exit_rule_name,
        note=note,
        pair=pair,
        signal_ts=signal_ts,
        allow_entry_decision_fallback=allow_entry_decision_fallback,
    )


def apply_fill_principal_with_pending_fee(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    side: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float | None,
    fee_status: str | None,
    fee_source: str | None = None,
    fee_confidence: str | None = None,
    fee_provenance: str | None = None,
    fee_validation_reason: str | None = None,
    fee_validation_checks: dict[str, object] | None = None,
    strategy_name: str | None = None,
    entry_decision_id: int | None = None,
    exit_decision_id: int | None = None,
    exit_reason: str | None = None,
    exit_rule_name: str | None = None,
    note: str | None = None,
    pair: str | None = None,
    signal_ts: int | None = None,
    allow_entry_decision_fallback: bool = True,
) -> dict[str, Any] | None:
    from .fee_observation import classify_fee_evaluation

    evaluation = classify_fee_evaluation(
        fee=fee,
        fee_status=fee_status,
        price=price,
        qty=qty,
        material_notional_threshold=float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW),
        fee_source=fee_source,
        fee_confidence=fee_confidence,
        provenance=fee_provenance,
        reason=fee_validation_reason,
        checks=fee_validation_checks,
    )
    target_fee_status = (
        FILL_FEE_ACCOUNTING_STATUS_BLOCKED
        if evaluation.accounting_eligibility == "blocked"
        else FILL_FEE_ACCOUNTING_STATUS_PENDING
    )
    if evaluation.accounting_status == "accounting_complete" and evaluation.fee is not None:
        return apply_fill_and_trade(
            conn,
            client_order_id=client_order_id,
            side=side,
            fill_id=fill_id,
            fill_ts=fill_ts,
            price=price,
            qty=qty,
            fee=evaluation.fee,
            strategy_name=strategy_name,
            entry_decision_id=entry_decision_id,
            exit_decision_id=exit_decision_id,
            exit_reason=exit_reason,
            exit_rule_name=exit_rule_name,
            note=note,
            pair=pair,
            signal_ts=signal_ts,
            allow_entry_decision_fallback=allow_entry_decision_fallback,
        )
    return _apply_fill_and_trade_core(
        conn,
        client_order_id=client_order_id,
        side=side,
        fill_id=fill_id,
        fill_ts=fill_ts,
        price=price,
        qty=qty,
        fee_to_apply=0.0,
        fee_accounting_status=target_fee_status,
        observed_fee_status=evaluation.fee_status,
        observed_fee_source=evaluation.fee_source,
        observed_fee_confidence=evaluation.fee_confidence,
        observed_fee_provenance=evaluation.provenance,
        observed_fee_validation_reason=evaluation.reason,
        observed_fee_validation_checks=evaluation.checks,
        strategy_name=strategy_name,
        entry_decision_id=entry_decision_id,
        exit_decision_id=exit_decision_id,
        exit_reason=exit_reason,
        exit_rule_name=exit_rule_name,
        note=note,
        pair=pair,
        signal_ts=signal_ts,
        allow_entry_decision_fallback=allow_entry_decision_fallback,
    )


def finalize_fill_fee(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float,
    fee_status: str = "operator_confirmed",
    fee_source: str = "operator_confirmed",
    fee_confidence: str = "authoritative",
    fee_provenance: str | None = None,
    fee_validation_reason: str | None = None,
    fee_validation_checks: dict[str, object] | None = None,
) -> dict[str, Any]:
    fee_value = float(fee)
    if not math.isfinite(fee_value) or fee_value < 0.0:
        raise RuntimeError(f"invalid finalized fee for {client_order_id}: {fee}")
    existing_fill = load_matching_accounted_fill(
        conn,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fill_ts=fill_ts,
        price=price,
        qty=qty,
    )
    if existing_fill is None:
        raise RuntimeError(
            "cannot finalize fee for missing accounted fill: "
            f"client_order_id={client_order_id} fill_id={fill_id or '-'}"
        )
    current_fee_status = fill_fee_accounting_status(existing_fill)
    current_fee = float(existing_fill["fee"] or 0.0)
    if current_fee_status == FILL_FEE_ACCOUNTING_STATUS_FINALIZED:
        if abs(current_fee - fee_value) <= 1e-12:
            return {
                "updated": False,
                "already_finalized": True,
                "fill_id": fill_id,
                "client_order_id": client_order_id,
                "fee": current_fee,
            }
        raise RuntimeError(
            "fee already finalized with a different value: "
            f"client_order_id={client_order_id} fill_id={fill_id or '-'} existing_fee={current_fee} new_fee={fee_value}"
        )

    conn.execute(
        """
        UPDATE fills
        SET fee=?,
            fee_accounting_status=?,
            observed_fee_status=?,
            observed_fee_source=?,
            observed_fee_confidence=?,
            observed_fee_provenance=?,
            observed_fee_validation_reason=?,
            observed_fee_validation_checks=?
        WHERE id=?
        """,
        (
            fee_value,
            FILL_FEE_ACCOUNTING_STATUS_FINALIZED,
            str(fee_status or "operator_confirmed"),
            str(fee_source or "operator_confirmed"),
            str(fee_confidence or "authoritative"),
            (str(fee_provenance).strip() if fee_provenance else None),
            (str(fee_validation_reason).strip() if fee_validation_reason else None),
            (
                json.dumps(fee_validation_checks, ensure_ascii=False, sort_keys=True)
                if isinstance(fee_validation_checks, dict)
                else None
            ),
            int(existing_fill["id"]),
        ),
    )
    _rebuild_trade_snapshots_from_fills(conn)
    replay = compute_accounting_replay(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=float(replay["replay_cash_available"]),
        cash_locked=float(replay["replay_cash_locked"]),
        asset_available=float(replay["replay_asset_available"]),
        asset_locked=float(replay["replay_asset_locked"]),
    )
    return {
        "updated": True,
        "already_finalized": False,
        "client_order_id": client_order_id,
        "fill_id": fill_id,
        "fee": fee_value,
        "previous_fee": current_fee,
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
