from __future__ import annotations

import re

from .broker.base import Broker, BrokerFill, BrokerOrder
from .db_core import ensure_db, get_portfolio_breakdown, init_portfolio, set_portfolio_breakdown
from .execution import apply_fill_and_trade, record_order_if_missing
from .oms import get_open_orders, set_exchange_order_id, set_status
from . import runtime_state
from .notifier import format_event, notify


LOCAL_RECONCILE_STATUSES = ("PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN")


def assert_no_open_orders() -> None:
    open_orders = get_open_orders()
    if open_orders:
        raise RuntimeError(f"Open orders exist (resume required): {open_orders}")


def _safe_recovery_client_order_id(*, tag: str, exchange_order_id: str | None, ts: int) -> str:
    base = exchange_order_id or f"{tag}_{ts}"
    clean = re.sub(r"[^a-zA-Z0-9_-]", "_", str(base))[:64]
    return f"recovery_{clean}"


def _record_unmatched_recent_activity(
    conn,
    *,
    exchange_order_id: str | None,
    side: str,
    qty_req: float,
    ts_ms: int,
    status: str,
    message: str,
) -> None:
    oid = _safe_recovery_client_order_id(tag="recent", exchange_order_id=exchange_order_id, ts=ts_ms)
    record_order_if_missing(
        conn,
        client_order_id=oid,
        side=(side if side in ("BUY", "SELL") else "BUY"),
        qty_req=max(0.0, float(qty_req)),
        price=None,
        ts_ms=int(ts_ms),
        status="SUBMIT_UNKNOWN",
    )
    if exchange_order_id:
        set_exchange_order_id(oid, str(exchange_order_id), conn=conn)
    set_status(oid, status, last_error=message, conn=conn)


def _sync_recent_order_activity(conn, recent_orders: list[BrokerOrder]) -> None:
    local_rows = conn.execute(
        "SELECT client_order_id, exchange_order_id FROM orders"
    ).fetchall()
    by_exchange_id = {str(r["exchange_order_id"]): str(r["client_order_id"]) for r in local_rows if r["exchange_order_id"]}
    by_client_order_id = {str(r["client_order_id"]): str(r["client_order_id"]) for r in local_rows}

    for remote in recent_orders:
        remote_exchange_id = str(remote.exchange_order_id or "")
        remote_client_order_id = str(remote.client_order_id or "")

        local_id = by_exchange_id.get(remote_exchange_id)
        if local_id is None and remote_client_order_id:
            local_id = by_client_order_id.get(remote_client_order_id)

        if local_id:
            if remote_exchange_id:
                set_exchange_order_id(local_id, remote_exchange_id, conn=conn)
                by_exchange_id[remote_exchange_id] = local_id
            set_status(local_id, remote.status, conn=conn)
            continue

        _record_unmatched_recent_activity(
            conn,
            exchange_order_id=(remote_exchange_id or None),
            side=remote.side,
            qty_req=remote.qty_req,
            ts_ms=remote.updated_ts,
            status="RECOVERY_REQUIRED",
            message="unmatched recent remote order detected; manual recovery required",
        )


def _apply_recent_fills(conn, recent_fills: list[BrokerFill]) -> None:
    local_rows = conn.execute(
        "SELECT client_order_id, exchange_order_id, side, qty_req, qty_filled FROM orders"
    ).fetchall()
    by_exchange_id = {str(r["exchange_order_id"]): r for r in local_rows if r["exchange_order_id"]}
    by_client_order_id = {str(r["client_order_id"]): r for r in local_rows}

    for fill in recent_fills:
        remote_exchange_id = str(fill.exchange_order_id or "")
        remote_client_order_id = str(fill.client_order_id or "")

        local = by_exchange_id.get(remote_exchange_id)
        if local is None and remote_client_order_id:
            local = by_client_order_id.get(remote_client_order_id)

        if local is None:
            _record_unmatched_recent_activity(
                conn,
                exchange_order_id=(remote_exchange_id or None),
                side="BUY",
                qty_req=fill.qty,
                ts_ms=fill.fill_ts,
                status="RECOVERY_REQUIRED",
                message="unmatched recent remote fill detected; manual recovery required",
            )
            continue

        local_id = str(local["client_order_id"])
        if remote_exchange_id:
            set_exchange_order_id(local_id, remote_exchange_id, conn=conn)

        apply_fill_and_trade(
            conn,
            client_order_id=local_id,
            side=str(local["side"]),
            fill_id=fill.fill_id,
            fill_ts=fill.fill_ts,
            price=fill.price,
            qty=fill.qty,
            fee=fill.fee,
            note=f"reconcile recent exchange_order_id={remote_exchange_id or '<none>'}",
        )

        order_row = conn.execute(
            "SELECT qty_req, qty_filled FROM orders WHERE client_order_id=?",
            (local_id,),
        ).fetchone()
        if order_row is None:
            continue
        qty_req = float(order_row["qty_req"])
        qty_filled = float(order_row["qty_filled"])
        if qty_req > 0 and qty_filled >= qty_req - 1e-12:
            set_status(local_id, "FILLED", conn=conn)
        elif qty_filled > 1e-12:
            set_status(local_id, "PARTIAL", conn=conn)


def reconcile_with_broker(broker: Broker) -> None:
    conn = ensure_db()
    try:
        init_portfolio(conn)

        placeholders = ",".join("?" for _ in LOCAL_RECONCILE_STATUSES)
        local_open = conn.execute(
            f"SELECT client_order_id, exchange_order_id, side, qty_req, status FROM orders WHERE status IN ({placeholders})",
            LOCAL_RECONCILE_STATUSES,
        ).fetchall()
        for row in local_open:
            oid = row["client_order_id"]
            if row["status"] == "SUBMIT_UNKNOWN" and not row["exchange_order_id"]:
                reason = "submit_unknown without exchange_order_id; manual recovery required"
                set_status(
                    oid,
                    "RECOVERY_REQUIRED",
                    last_error=reason,
                    conn=conn,
                )
                notify(
                    format_event(
                        "recovery_required_transition",
                        client_order_id=oid,
                        side=row["side"],
                        status="RECOVERY_REQUIRED",
                        reason=reason,
                    )
                )
                continue

            remote = broker.get_order(client_order_id=oid, exchange_order_id=row["exchange_order_id"])
            if remote.exchange_order_id:
                set_exchange_order_id(oid, remote.exchange_order_id, conn=conn)
                notify(
                    format_event(
                        "exchange_order_id_attached",
                        client_order_id=oid,
                        exchange_order_id=remote.exchange_order_id,
                        side=row["side"],
                        status=remote.status,
                        reason="reconcile",
                    )
                )
            prev_status = row["status"]
            set_status(oid, remote.status, conn=conn)
            if prev_status != remote.status:
                notify(
                    format_event(
                        "reconcile_status_change",
                        client_order_id=oid,
                        exchange_order_id=remote.exchange_order_id,
                        side=row["side"],
                        status=remote.status,
                        reason=f"from={prev_status}",
                    )
                )
            fills = broker.get_fills(client_order_id=oid, exchange_order_id=remote.exchange_order_id)
            for fill in fills:
                apply_fill_and_trade(
                    conn,
                    client_order_id=oid,
                    side=row["side"],
                    fill_id=fill.fill_id,
                    fill_ts=fill.fill_ts,
                    price=fill.price,
                    qty=fill.qty,
                    fee=fill.fee,
                    note=f"reconcile exchange_order_id={remote.exchange_order_id}",
                )

        remote_open = broker.get_open_orders()
        known_exchange_ids = {
            str(r["exchange_order_id"])
            for r in conn.execute(
                "SELECT exchange_order_id FROM orders WHERE exchange_order_id IS NOT NULL"
            ).fetchall()
        }
        for remote in remote_open:
            exid = str(remote.exchange_order_id or "")
            if not exid or exid in known_exchange_ids:
                continue
            oid = f"remote_{exid}"
            record_order_if_missing(
                conn,
                client_order_id=oid,
                side=remote.side,
                qty_req=remote.qty_req,
                price=remote.price,
                ts_ms=remote.created_ts,
                status="SUBMIT_UNKNOWN",
            )
            set_exchange_order_id(oid, exid, conn=conn)
            set_status(oid, remote.status, last_error="stray remote open order detected", conn=conn)
            notify(
                format_event(
                    "reconcile_status_change",
                    client_order_id=oid,
                    exchange_order_id=exid,
                    side=remote.side,
                    status=remote.status,
                    reason="stray remote open order detected",
                )
            )

        _sync_recent_order_activity(conn, broker.get_recent_orders(limit=100))
        _apply_recent_fills(conn, broker.get_recent_fills(limit=100))

        bal = broker.get_balance()
        _, local_cash_locked, _, local_asset_locked = get_portfolio_breakdown(conn)
        has_open_orders = bool(local_open) or bool(remote_open)

        cash_locked = float(bal.cash_locked)
        asset_locked = float(bal.asset_locked)
        if has_open_orders and cash_locked <= 1e-12 and local_cash_locked > 1e-12:
            cash_locked = local_cash_locked
        if has_open_orders and asset_locked <= 1e-12 and local_asset_locked > 1e-12:
            asset_locked = local_asset_locked

        set_portfolio_breakdown(
            conn,
            cash_available=bal.cash_available,
            cash_locked=cash_locked,
            asset_available=bal.asset_available,
            asset_locked=asset_locked,
        )
        conn.commit()
    except Exception as e:
        runtime_state.record_reconcile_result(
            success=False,
            error=f"{type(e).__name__}: {e}",
        )
        runtime_state.refresh_open_order_health()
        raise
    else:
        runtime_state.record_reconcile_result(success=True)
        runtime_state.refresh_open_order_health()
    finally:
        conn.close()


def recover_order_with_exchange_id(
    broker: Broker,
    *,
    client_order_id: str,
    exchange_order_id: str,
) -> None:
    conn = ensure_db()
    order_found = False
    try:
        order = conn.execute(
            "SELECT client_order_id, side FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if order is None:
            raise RuntimeError(f"unknown client_order_id: {client_order_id}")

        order_found = True
        side = str(order["side"])
        set_exchange_order_id(client_order_id, exchange_order_id, conn=conn)

        remote = broker.get_order(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
        resolved_exchange_order_id = str(remote.exchange_order_id or exchange_order_id)
        if resolved_exchange_order_id:
            set_exchange_order_id(client_order_id, resolved_exchange_order_id, conn=conn)

        fills = broker.get_fills(
            client_order_id=client_order_id,
            exchange_order_id=resolved_exchange_order_id,
        )
        for fill in fills:
            apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee,
                note=f"manual recovery exchange_order_id={resolved_exchange_order_id}",
            )

        if remote.status in LOCAL_RECONCILE_STATUSES:
            raise RuntimeError(f"order still unresolved after recovery: status={remote.status}")

        set_status(client_order_id, remote.status, conn=conn)
        conn.commit()
    except Exception as e:
        if order_found:
            set_status(
                client_order_id,
                "RECOVERY_REQUIRED",
                last_error=f"manual recovery failed: {type(e).__name__}: {e}",
                conn=conn,
            )
            conn.commit()
        raise
    finally:
        conn.close()
        runtime_state.refresh_open_order_health()


def cancel_open_orders_with_broker(broker: Broker) -> dict[str, int | list[str]]:
    conn = ensure_db()
    try:
        remote_open = broker.get_open_orders()
        if not remote_open:
            return {
                "remote_open_count": 0,
                "canceled_count": 0,
                "matched_local_count": 0,
                "stray_canceled_count": 0,
                "failed_count": 0,
                "stray_messages": [],
                "error_messages": [],
            }

        local_by_exchange_id: dict[str, str] = {}
        local_by_client_order_id: dict[str, str] = {}
        rows = conn.execute(
            "SELECT client_order_id, exchange_order_id FROM orders"
        ).fetchall()
        for row in rows:
            local_id = str(row["client_order_id"])
            local_by_client_order_id[local_id] = local_id
            if row["exchange_order_id"]:
                local_by_exchange_id[str(row["exchange_order_id"])] = local_id

        canceled_count = 0
        matched_local_count = 0
        stray_canceled_count = 0
        failed_count = 0
        stray_messages: list[str] = []
        error_messages: list[str] = []

        for remote in remote_open:
            remote_exchange_id = str(remote.exchange_order_id or "")
            remote_client_order_id = str(remote.client_order_id or "")
            local_id = local_by_exchange_id.get(remote_exchange_id)
            if local_id is None and remote_client_order_id:
                local_id = local_by_client_order_id.get(remote_client_order_id)

            cancel_client_order_id = local_id or remote_client_order_id or f"remote_{remote_exchange_id or 'unknown'}"

            try:
                broker.cancel_order(
                    client_order_id=cancel_client_order_id,
                    exchange_order_id=remote.exchange_order_id,
                )
                canceled_count += 1
            except Exception as e:
                failed_count += 1
                target = remote_exchange_id or cancel_client_order_id
                error_messages.append(f"failed to cancel {target}: {type(e).__name__}: {e}")
                continue

            if local_id:
                if remote_exchange_id:
                    set_exchange_order_id(local_id, remote_exchange_id, conn=conn)
                set_status(local_id, "CANCELED", conn=conn)
                matched_local_count += 1
            else:
                stray_canceled_count += 1
                stray_messages.append(
                    f"stray remote order canceled exchange_order_id={remote_exchange_id or '<none>'} side={remote.side} qty={remote.qty_req}"
                )

        conn.commit()
        return {
            "remote_open_count": len(remote_open),
            "canceled_count": canceled_count,
            "matched_local_count": matched_local_count,
            "stray_canceled_count": stray_canceled_count,
            "failed_count": failed_count,
            "stray_messages": stray_messages,
            "error_messages": error_messages,
        }
    finally:
        conn.close()
