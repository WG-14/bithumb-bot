from __future__ import annotations

import math
import inspect
import time

from . import runtime_state
from .config import settings
from .decision_context import resolve_canonical_position_exposure_snapshot
from .db_core import ensure_db, init_portfolio
from .dust import build_dust_display_context, build_position_state_model
from .marketdata import fetch_orderbook_top, validated_best_quote_prices
from .notifier import notify
from .observability import safety_event
from .order_sizing import SellExecutionAuthority, build_sell_execution_sizing
from .reason_codes import EMERGENCY_FLATTEN_FAILED, EMERGENCY_FLATTEN_STARTED, EMERGENCY_FLATTEN_SUCCEEDED
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from .execution_models import OrderIntent
from .broker.order_submit import plan_place_order


def _resolve_flatten_sell_authority(
    *,
    position_state,
):
    canonical_exposure = resolve_canonical_position_exposure_snapshot(
        {"position_state": position_state.as_dict()}
    )
    return (
        canonical_exposure,
        int(canonical_exposure.sellable_executable_lot_count),
        bool(canonical_exposure.exit_allowed),
        str(canonical_exposure.exit_block_reason or "no_executable_exit_lot"),
    )


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
        reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    finally:
        conn.close()

    qty = float(row["asset_qty"] if row is not None else 0.0)
    dust_context = build_dust_display_context(state_snapshot.last_reconcile_metadata)
    open_exposure_qty = float(lot_snapshot.raw_open_exposure_qty)
    open_lot_count = int(lot_snapshot.open_lot_count)
    dust_tracking_qty = float(lot_snapshot.dust_tracking_qty)
    dust_tracking_lot_count = int(lot_snapshot.dust_tracking_lot_count)
    # Flatten is a SELL-capable path, so qty-only holdings stay observational.
    # Without explicit lot-native executable authority, we must suppress.
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
        reserved_exit_qty=reserved_exit_qty,
        open_lot_count=open_lot_count,
        dust_tracking_lot_count=dust_tracking_lot_count,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    canonical_exposure, sellable_executable_lot_count, exit_allowed, exit_block_reason = _resolve_flatten_sell_authority(
        position_state=position_state,
    )
    terminal_state = str(position_state.normalized_exposure.terminal_state)
    if (not exit_allowed) or sellable_executable_lot_count < 1:
        summary = {
            "status": "no_position",
            "qty": float(canonical_exposure.open_exposure_qty),
            "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
            "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
            "terminal_state": terminal_state,
            "dry_run": int(bool(dry_run)),
            "trigger": trigger,
        }
        runtime_state.record_flatten_position_result(status="no_position", summary=summary)
        return summary

    exit_sizing = build_sell_execution_sizing(
        pair=settings.PAIR,
        market_price=1.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=int(sellable_executable_lot_count),
            exit_allowed=bool(exit_allowed),
            exit_block_reason=str(exit_block_reason),
        ),
        lot_definition=lot_snapshot.lot_definition,
    )

    runtime_state.record_flatten_position_result(
        status="started",
        summary={
            "qty": float(canonical_exposure.open_exposure_qty),
            "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
            "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
            "terminal_state": terminal_state,
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
            "qty": float(exit_sizing.executable_qty),
            "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
            "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
            "terminal_state": terminal_state,
            "dry_run": 1,
            "side": "SELL",
            "symbol": settings.PAIR,
            "trigger": trigger,
        }
        runtime_state.record_flatten_position_result(status="dry_run", summary=summary)
        return summary

    client_order_id = f"flatten_{int(time.time() * 1000)}"
    normalized_qty = float(exit_sizing.executable_qty)
    try:
        quote = fetch_orderbook_top(settings.PAIR)
        bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
        market_price = float(bid)

        normalized_qty = _normalize_flatten_qty(qty=normalized_qty, market_price=market_price)
        _validate_flatten_pretrade(broker=broker, qty=normalized_qty)

        place_order_kwargs = {
            "client_order_id": client_order_id,
            "side": "SELL",
            "qty": normalized_qty,
            "price": None,
        }
        place_order_params = inspect.signature(broker.place_order).parameters
        if "submit_plan" in place_order_params:
            submit_plan = plan_place_order(
                broker,
                intent=OrderIntent(
                    client_order_id=client_order_id,
                    market=settings.PAIR,
                    side="SELL",
                    normalized_side="ask",
                    qty=float(normalized_qty),
                    price=None,
                    created_ts=int(time.time() * 1000),
                    market_price_hint=market_price,
                    trace_id=client_order_id,
                ),
                skip_qty_revalidation=True,
            )
            place_order_kwargs["submit_plan"] = submit_plan
        order = broker.place_order(**place_order_kwargs)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        summary = {
            "status": "failed",
            "qty": float(normalized_qty),
            "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
            "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
            "terminal_state": terminal_state,
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
        "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
        "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
        "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
        "terminal_state": terminal_state,
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
