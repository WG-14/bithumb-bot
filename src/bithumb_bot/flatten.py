from __future__ import annotations

import math
import time

from . import runtime_state
from .config import settings
from .db_core import ensure_db, init_portfolio
from .dust import build_dust_display_context, build_position_state_model
from .marketdata import fetch_orderbook_top, validated_best_quote_prices
from .notifier import notify
from .observability import safety_event
from .reason_codes import EMERGENCY_FLATTEN_FAILED, EMERGENCY_FLATTEN_STARTED, EMERGENCY_FLATTEN_SUCCEEDED
from .lifecycle import summarize_position_lots


def _normalize_flatten_qty(*, qty: float, market_price: float) -> float:
    normalized_qty = max(0.0, float(qty))
    qty_step = max(0.0, float(settings.LIVE_ORDER_QTY_STEP))
    if qty_step > 0:
        normalized_qty = math.floor((normalized_qty / qty_step) + 1e-12) * qty_step
    max_qty_decimals = max(0, int(settings.LIVE_ORDER_MAX_QTY_DECIMALS))
    if max_qty_decimals > 0:
        scale = 10**max_qty_decimals
        normalized_qty = math.floor((normalized_qty * scale) + 1e-12) / scale
    if normalized_qty <= 0:
        raise ValueError(f"invalid order qty: {normalized_qty}")
    min_qty = max(0.0, float(settings.LIVE_MIN_ORDER_QTY))
    if min_qty > 0 and normalized_qty + 1e-12 < min_qty:
        raise ValueError(f"order qty below minimum: {normalized_qty:.12f} < {min_qty:.12f}")
    min_notional = max(0.0, float(settings.MIN_ORDER_NOTIONAL_KRW))
    if min_notional > 0 and (normalized_qty * float(market_price)) + 1e-12 < min_notional:
        raise ValueError(
            f"order notional below minimum (SELL): {(normalized_qty * float(market_price)):.2f} < {min_notional:.2f}"
        )
    return normalized_qty


def _validate_flatten_pretrade(*, broker, qty: float) -> None:
    balance = broker.get_balance()
    required_asset = float(qty)
    available_asset = float(balance.asset_available)
    if available_asset + 1e-12 < required_asset:
        raise ValueError(
            f"insufficient available asset: need={required_asset:.12f} avail={available_asset:.12f}"
        )


def flatten_btc_position(*, broker, dry_run: bool = False, trigger: str = "operator") -> dict[str, object]:
    state_snapshot = runtime_state.snapshot()
    conn = ensure_db()
    try:
        init_portfolio(conn)
        row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    qty = float(row["asset_qty"] if row is not None else 0.0)
    dust_context = build_dust_display_context(state_snapshot.last_reconcile_metadata)
    open_exposure_qty = float(lot_snapshot.raw_open_exposure_qty)
    open_lot_count = int(lot_snapshot.open_lot_count)
    dust_tracking_qty = float(lot_snapshot.dust_tracking_qty)
    dust_tracking_lot_count = int(lot_snapshot.dust_tracking_lot_count)
    if (
        open_lot_count <= 0
        and dust_tracking_lot_count <= 0
        and qty > 1e-12
        and float(dust_context.raw_holdings.broker_qty) <= 1e-12
    ):
        open_exposure_qty = float(qty)
        open_lot_count = 1
    position_state = build_position_state_model(
        raw_qty_open=qty,
        metadata_raw=state_snapshot.last_reconcile_metadata,
        raw_total_asset_qty=max(
            qty,
            float(lot_snapshot.raw_total_asset_qty),
            float(dust_context.raw_holdings.broker_qty),
        ),
        open_exposure_qty=open_exposure_qty,
        dust_tracking_qty=dust_tracking_qty,
        open_lot_count=open_lot_count,
        dust_tracking_lot_count=dust_tracking_lot_count,
    )
    normalized_exposure = position_state.normalized_exposure
    if not normalized_exposure.has_executable_exposure:
        summary = {
            "status": "no_position",
            "qty": float(normalized_exposure.open_exposure_qty),
            "raw_total_asset_qty": float(normalized_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(normalized_exposure.open_exposure_qty),
            "tracked_dust_qty": float(normalized_exposure.dust_tracking_qty),
            "terminal_state": str(normalized_exposure.terminal_state),
            "dry_run": int(bool(dry_run)),
            "trigger": trigger,
        }
        runtime_state.record_flatten_position_result(status="no_position", summary=summary)
        return summary

    runtime_state.record_flatten_position_result(
        status="started",
        summary={
            "qty": float(normalized_exposure.open_exposure_qty),
            "raw_total_asset_qty": float(normalized_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(normalized_exposure.open_exposure_qty),
            "tracked_dust_qty": float(normalized_exposure.dust_tracking_qty),
            "terminal_state": str(normalized_exposure.terminal_state),
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
            "qty": float(normalized_exposure.open_exposure_qty),
            "raw_total_asset_qty": float(normalized_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(normalized_exposure.open_exposure_qty),
            "tracked_dust_qty": float(normalized_exposure.dust_tracking_qty),
            "terminal_state": str(normalized_exposure.terminal_state),
            "dry_run": 1,
            "side": "SELL",
            "symbol": settings.PAIR,
            "trigger": trigger,
        }
        runtime_state.record_flatten_position_result(status="dry_run", summary=summary)
        return summary

    client_order_id = f"flatten_{int(time.time() * 1000)}"
    normalized_qty = float(normalized_exposure.open_exposure_qty)
    try:
        quote = fetch_orderbook_top(settings.PAIR)
        bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
        market_price = float(bid)

        normalized_qty = _normalize_flatten_qty(qty=normalized_qty, market_price=market_price)
        _validate_flatten_pretrade(broker=broker, qty=normalized_qty)

        order = broker.place_order(client_order_id=client_order_id, side="SELL", qty=normalized_qty, price=None)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        summary = {
            "status": "failed",
            "qty": float(normalized_qty),
            "raw_total_asset_qty": float(normalized_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(normalized_exposure.open_exposure_qty),
            "tracked_dust_qty": float(normalized_exposure.dust_tracking_qty),
            "terminal_state": str(normalized_exposure.terminal_state),
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
        "raw_total_asset_qty": float(normalized_exposure.raw_total_asset_qty),
        "executable_exposure_qty": float(normalized_exposure.open_exposure_qty),
        "tracked_dust_qty": float(normalized_exposure.dust_tracking_qty),
        "terminal_state": str(normalized_exposure.terminal_state),
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
