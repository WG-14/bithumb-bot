from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from ..db_core import mark_private_stream_event_applied, record_private_stream_event
from ..execution import LiveFillFeeValidationError, apply_fill_and_trade
from ..oms import record_status_transition, set_exchange_order_id, set_status
from .myorder_events import NormalizedMyOrderEvent, normalize_myorder_event_payload


@dataclass(frozen=True)
class MyOrderIngestResult:
    dedupe_key: str
    accepted: bool
    applied: bool
    action: str
    client_order_id: str
    exchange_order_id: str
    status: str


def _find_local_order(conn: sqlite3.Connection, event: NormalizedMyOrderEvent):
    if event.client_order_id:
        return conn.execute(
            """
            SELECT client_order_id, exchange_order_id, side, status
            FROM orders
            WHERE client_order_id=?
            ORDER BY updated_ts DESC
            LIMIT 1
            """,
            (event.client_order_id,),
        ).fetchone()
    if event.exchange_order_id:
        row = conn.execute(
            """
            SELECT client_order_id, exchange_order_id, side, status
            FROM orders
            WHERE exchange_order_id=?
            ORDER BY updated_ts DESC
            LIMIT 1
            """,
            (event.exchange_order_id,),
        ).fetchone()
        if row is not None:
            return row
    return None


def ingest_myorder_event(
    conn: sqlite3.Connection,
    *,
    payload: dict[str, object],
    strategy_name: str | None = None,
) -> MyOrderIngestResult:
    event = normalize_myorder_event_payload(payload)
    accepted = record_private_stream_event(
        conn,
        stream_name="myorder",
        dedupe_key=event.dedupe_key,
        event_ts=event.event_ts_ms,
        client_order_id=event.client_order_id or None,
        exchange_order_id=event.exchange_order_id or None,
        order_status=event.status or None,
        fill_id=event.fill_id or None,
        qty=event.qty,
        price=event.price,
        payload=event.raw_payload,
    )
    if not accepted:
        return MyOrderIngestResult(
            dedupe_key=event.dedupe_key,
            accepted=False,
            applied=False,
            action="duplicate_event",
            client_order_id=event.client_order_id,
            exchange_order_id=event.exchange_order_id,
            status=event.status,
        )

    row = _find_local_order(conn, event)
    if row is None:
        mark_private_stream_event_applied(
            conn,
            dedupe_key=event.dedupe_key,
            applied=False,
            applied_status="no_local_order_match",
        )
        return MyOrderIngestResult(
            dedupe_key=event.dedupe_key,
            accepted=True,
            applied=False,
            action="no_local_order_match",
            client_order_id=event.client_order_id,
            exchange_order_id=event.exchange_order_id,
            status=event.status,
        )

    client_order_id = str(row["client_order_id"] or event.client_order_id or "")
    existing_exchange_order_id = str(row["exchange_order_id"] or "")
    exchange_order_id = str(existing_exchange_order_id or event.exchange_order_id or "")
    side = str(row["side"] or event.side or "").upper()
    applied = False

    if event.exchange_order_id and event.exchange_order_id != existing_exchange_order_id:
        set_exchange_order_id(client_order_id, event.exchange_order_id, conn=conn)
        exchange_order_id = event.exchange_order_id
        applied = True

    if event.status in {"PARTIAL", "FILLED"} and event.qty is not None and event.price is not None and event.fill_id:
        try:
            trade = apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=event.fill_id,
                fill_ts=int(event.event_ts_ms),
                price=float(event.price),
                qty=float(event.qty),
                fee=0.0,
                strategy_name=strategy_name,
                note="myorder private stream",
                allow_entry_decision_fallback=False,
            )
        except LiveFillFeeValidationError as exc:
            reason = f"myorder fill fee validation blocked ledger apply; manual recovery required ({exc})"
            current_status = str(row["status"] or "")
            record_status_transition(
                client_order_id,
                from_status=current_status,
                to_status="RECOVERY_REQUIRED",
                reason=reason,
                conn=conn,
            )
            set_status(
                client_order_id,
                "RECOVERY_REQUIRED",
                last_error=reason,
                conn=conn,
            )
            mark_private_stream_event_applied(
                conn,
                dedupe_key=event.dedupe_key,
                applied=True,
                applied_status="recovery_required",
            )
            return MyOrderIngestResult(
                dedupe_key=event.dedupe_key,
                accepted=True,
                applied=True,
                action="recovery_required",
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                status="RECOVERY_REQUIRED",
            )
        applied = applied or trade is not None

    if event.status in {"NEW", "PARTIAL", "FILLED", "CANCEL_REQUESTED", "FAILED"}:
        current_status = str(row["status"] or "")
        if event.status != current_status:
            set_status(client_order_id, event.status, conn=conn)
            applied = True

    mark_private_stream_event_applied(
        conn,
        dedupe_key=event.dedupe_key,
        applied=applied,
        applied_status="applied" if applied else "recorded_only",
    )
    return MyOrderIngestResult(
        dedupe_key=event.dedupe_key,
        accepted=True,
        applied=applied,
        action="applied" if applied else "recorded_only",
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        status=event.status,
    )
