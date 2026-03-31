from __future__ import annotations

import time

from . import runtime_state
from .broker.live import normalize_order_qty, validate_order, validate_pretrade
from .config import settings
from .db_core import ensure_db, init_portfolio
from .marketdata import fetch_orderbook_top, validated_best_quote_prices
from .notifier import notify
from .observability import safety_event
from .reason_codes import EMERGENCY_FLATTEN_FAILED, EMERGENCY_FLATTEN_STARTED, EMERGENCY_FLATTEN_SUCCEEDED


def flatten_btc_position(*, broker, dry_run: bool = False, trigger: str = "operator") -> dict[str, object]:
    conn = ensure_db()
    try:
        init_portfolio(conn)
        row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
    finally:
        conn.close()

    qty = float(row["asset_qty"] if row is not None else 0.0)
    if qty <= 1e-12:
        summary = {
            "status": "no_position",
            "qty": qty,
            "dry_run": int(bool(dry_run)),
            "trigger": trigger,
        }
        runtime_state.record_flatten_position_result(status="no_position", summary=summary)
        return summary

    runtime_state.record_flatten_position_result(
        status="started",
        summary={
            "qty": qty,
            "dry_run": int(bool(dry_run)),
            "side": "SELL",
            "symbol": settings.PAIR,
            "trigger": trigger,
        },
    )
    notify(
        safety_event(
            "flatten_position_started",
            reason_code=EMERGENCY_FLATTEN_STARTED,
            side="SELL",
            symbol=settings.PAIR,
            qty=qty,
            dry_run=1 if dry_run else 0,
            trigger=trigger,
        )
    )

    if dry_run:
        summary = {
            "status": "dry_run",
            "qty": qty,
            "dry_run": 1,
            "side": "SELL",
            "symbol": settings.PAIR,
            "trigger": trigger,
        }
        runtime_state.record_flatten_position_result(status="dry_run", summary=summary)
        return summary

    client_order_id = f"flatten_{int(time.time() * 1000)}"
    try:
        quote = fetch_orderbook_top(settings.PAIR)
        bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
        market_price = float(bid)

        normalized_qty = normalize_order_qty(qty=qty, market_price=market_price)
        validate_order(signal="SELL", side="SELL", qty=normalized_qty, market_price=market_price)
        validate_pretrade(broker=broker, side="SELL", qty=normalized_qty, market_price=market_price)

        order = broker.place_order(client_order_id=client_order_id, side="SELL", qty=normalized_qty, price=None)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        summary = {
            "status": "failed",
            "qty": qty,
            "side": "SELL",
            "symbol": settings.PAIR,
            "error": err,
            "trigger": trigger,
        }
        runtime_state.record_flatten_position_result(status="failed", summary=summary)
        notify(
            safety_event(
                "flatten_position_failed",
                reason_code=EMERGENCY_FLATTEN_FAILED,
                side="SELL",
                symbol=settings.PAIR,
                qty=qty,
                error=err,
                trigger=trigger,
            )
        )
        return summary

    summary = {
        "status": "submitted",
        "qty": normalized_qty,
        "side": "SELL",
        "symbol": settings.PAIR,
        "client_order_id": client_order_id,
        "exchange_order_id": str(order.exchange_order_id or "-"),
        "order_status": str(order.status or "-"),
        "trigger": trigger,
    }
    runtime_state.record_flatten_position_result(status="submitted", summary=summary)
    notify(
        safety_event(
            "flatten_position_submitted",
            reason_code=EMERGENCY_FLATTEN_SUCCEEDED,
            side="SELL",
            symbol=settings.PAIR,
            qty=qty,
            client_order_id=client_order_id,
            exchange_order_id=str(order.exchange_order_id or "-"),
            status=str(order.status or "-"),
            trigger=trigger,
        )
    )
    return summary
