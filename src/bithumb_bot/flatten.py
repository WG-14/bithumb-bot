from __future__ import annotations

import math
import inspect
import json
import time
import uuid

from . import runtime_state
from . import db_core
from .config import settings
from .decision_context import resolve_canonical_position_exposure_snapshot
from .db_core import ensure_db, init_portfolio
from .marketdata import fetch_orderbook_top, validated_best_quote_prices
from .notifier import notify
from .observability import safety_event
from .position_state_snapshot import build_canonical_position_snapshot
from .runtime_readiness import compute_runtime_readiness_snapshot
from .execution import record_order_if_missing
from .oms import (
    payload_fingerprint,
    record_submit_attempt,
    record_submit_started,
    record_status_transition,
    set_exchange_order_id,
    set_status,
)
from .order_sizing import SellExecutionAuthority, build_sell_execution_sizing
from .reason_codes import EMERGENCY_FLATTEN_FAILED, EMERGENCY_FLATTEN_STARTED, EMERGENCY_FLATTEN_SUCCEEDED
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


def _flatten_submit_evidence(
    *,
    client_order_id: str,
    submit_attempt_id: str,
    trigger: str,
    qty: float,
    market_price: float | None,
    phase: str,
    status: str,
    exchange_order_id: str | None = None,
    error: str | None = None,
) -> str:
    return json.dumps(
        {
            "client_order_id": client_order_id,
            "submit_attempt_id": submit_attempt_id,
            "submit_path": "operator_flatten",
            "trigger": trigger,
            "symbol": settings.PAIR,
            "side": "SELL",
            "qty": float(qty),
            "price": None,
            "reference_price": float(market_price) if market_price is not None else None,
            "phase": phase,
            "status": status,
            "exchange_order_id": exchange_order_id,
            "error": error,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _stage_flatten_submit_intent(
    *,
    client_order_id: str,
    trigger: str,
    qty: float,
    market_price: float,
    lot_snapshot,
    exit_sizing,
) -> tuple[str, str, int]:
    submit_attempt_id = f"{client_order_id}:submit:{uuid.uuid4().hex[:8]}"
    ts = int(time.time() * 1000)
    payload_hash = payload_fingerprint(
        {
            "client_order_id": client_order_id,
            "submit_attempt_id": submit_attempt_id,
            "symbol": settings.PAIR,
            "side": "SELL",
            "qty": float(qty),
            "price": None,
            "trigger": trigger,
            "submit_path": "operator_flatten",
        }
    )
    evidence = _flatten_submit_evidence(
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        trigger=trigger,
        qty=qty,
        market_price=market_price,
        phase="pre_submit",
        status="PENDING_SUBMIT",
    )
    conn = db_core.ensure_db()
    try:
        record_order_if_missing(
            conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=settings.PAIR,
            side="SELL",
            qty_req=float(qty),
            price=None,
            strategy_name="operator_flatten",
            exit_rule_name=trigger,
            order_type="market",
            internal_lot_size=float(exit_sizing.internal_lot_size),
            effective_min_trade_qty=float(exit_sizing.effective_min_trade_qty),
            qty_step=float(settings.LIVE_ORDER_QTY_STEP),
            min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            intended_lot_count=int(exit_sizing.intended_lot_count),
            executable_lot_count=int(exit_sizing.executable_lot_count),
            final_intended_qty=float(exit_sizing.executable_qty),
            final_submitted_qty=float(qty),
            decision_reason_code="operator_flatten",
            local_intent_state="PENDING_SUBMIT",
            ts_ms=ts,
            status="PENDING_SUBMIT",
        )
        record_submit_started(
            client_order_id,
            conn=conn,
            submit_attempt_id=submit_attempt_id,
            symbol=settings.PAIR,
            side="SELL",
            qty=float(qty),
            mode=settings.MODE,
            message=f"operator flatten submit staged before broker dispatch; trigger={trigger}",
        )
        record_submit_attempt(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=settings.PAIR,
            side="SELL",
            qty=float(qty),
            price=float(market_price),
            submit_ts=ts,
            payload_fingerprint=payload_hash,
            broker_response_summary="operator_flatten_pre_submit_journaled",
            submission_reason_code="operator_flatten_pre_submit_journaled",
            exception_class=None,
            timeout_flag=False,
            submit_evidence=evidence,
            exchange_order_id_obtained=False,
            order_status="PENDING_SUBMIT",
            submit_phase="operator_pre_submit",
            submit_plan_id=f"{submit_attempt_id}:plan",
            signed_request_id=f"{submit_attempt_id}:signed",
            submission_id=f"{submit_attempt_id}:submission",
            confirmation_id=f"{submit_attempt_id}:confirmation",
            event_type="submit_attempt_preflight",
            message=f"operator flatten trigger={trigger}",
            order_type="market",
            internal_lot_size=float(exit_sizing.internal_lot_size),
            effective_min_trade_qty=float(exit_sizing.effective_min_trade_qty),
            qty_step=float(settings.LIVE_ORDER_QTY_STEP),
            min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            intended_lot_count=int(exit_sizing.intended_lot_count),
            executable_lot_count=int(exit_sizing.executable_lot_count),
            final_intended_qty=float(exit_sizing.executable_qty),
            final_submitted_qty=float(qty),
            decision_reason_code="operator_flatten",
        )
        conn.commit()
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        conn.close()
    return submit_attempt_id, payload_hash, ts


def _record_flatten_submit_ack(
    *,
    client_order_id: str,
    submit_attempt_id: str,
    payload_hash: str,
    trigger: str,
    qty: float,
    market_price: float,
    order,
) -> None:
    exchange_order_id = str(getattr(order, "exchange_order_id", "") or "")
    order_status = str(getattr(order, "status", "") or "NEW")
    ts = int(time.time() * 1000)
    evidence = _flatten_submit_evidence(
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        trigger=trigger,
        qty=qty,
        market_price=market_price,
        phase="broker_ack",
        status=order_status,
        exchange_order_id=exchange_order_id or None,
    )
    conn = db_core.ensure_db()
    try:
        if exchange_order_id:
            set_exchange_order_id(client_order_id, exchange_order_id, conn=conn)
        record_submit_attempt(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=settings.PAIR,
            side="SELL",
            qty=float(qty),
            price=float(market_price),
            submit_ts=ts,
            payload_fingerprint=payload_hash,
            broker_response_summary=f"operator_flatten_ack status={order_status} exchange_order_id={exchange_order_id or '-'}",
            submission_reason_code="operator_flatten_ack",
            exception_class=None,
            timeout_flag=False,
            submit_evidence=evidence,
            exchange_order_id_obtained=bool(exchange_order_id),
            order_status=order_status,
            submit_phase="broker_ack",
            submit_plan_id=f"{submit_attempt_id}:plan",
            signed_request_id=f"{submit_attempt_id}:signed",
            submission_id=f"{submit_attempt_id}:submission",
            confirmation_id=f"{submit_attempt_id}:confirmation",
            event_type="submit_attempt_acknowledged",
            order_type="market",
        )
        set_status(client_order_id, order_status, conn=conn)
        conn.commit()
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        conn.close()


def _mark_flatten_submit_unknown(
    *,
    client_order_id: str,
    submit_attempt_id: str,
    reason: str,
) -> None:
    conn = db_core.ensure_db()
    try:
        row = conn.execute(
            "SELECT status FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if row is not None and str(row["status"]) != "SUBMIT_UNKNOWN":
            record_status_transition(
                client_order_id,
                from_status=str(row["status"] or "UNKNOWN"),
                to_status="SUBMIT_UNKNOWN",
                reason=reason,
                conn=conn,
            )
            set_status(client_order_id, "SUBMIT_UNKNOWN", last_error=reason, conn=conn)
        conn.commit()
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        conn.close()


def flatten_btc_position(*, broker, dry_run: bool = False, trigger: str = "operator") -> dict[str, object]:
    state_snapshot = runtime_state.snapshot()
    conn = ensure_db()
    try:
        init_portfolio(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        if trigger == "operator":
            if int(readiness.recovery_required_count or 0) > 0:
                return {
                    "status": "blocked",
                    "reason": "recovery_required_orders_present",
                    "recovery_stage": readiness.recovery_stage,
                    "recovery_required_count": int(readiness.recovery_required_count or 0),
                    "recommended_command": readiness.recommended_command,
                    "closeout_allowed": False,
                    "dry_run": int(bool(dry_run)),
                    "trigger": trigger,
                }
            if int(readiness.open_order_count or 0) > 0:
                return {
                    "status": "blocked",
                    "reason": "unresolved_orders_present",
                    "recovery_stage": readiness.recovery_stage,
                    "open_order_count": int(readiness.open_order_count or 0),
                    "recommended_command": readiness.recommended_command,
                    "closeout_allowed": False,
                    "dry_run": int(bool(dry_run)),
                    "trigger": trigger,
                }
        unapplied_principal_pending_count = int(
            (readiness.fill_accounting_incident_summary or {}).get("unapplied_principal_pending_count") or 0
        )
        if unapplied_principal_pending_count > 0:
            return {
                "status": "blocked",
                "reason": "unapplied_principal_pending",
                "recovery_stage": readiness.recovery_stage,
                "unapplied_principal_pending_count": unapplied_principal_pending_count,
                "recommended_command": "uv run python bot.py recovery-report",
                "closeout_allowed": False,
                "dry_run": int(bool(dry_run)),
                "trigger": trigger,
            }
        fee_validation_blocked_count = int(
            (readiness.fill_accounting_incident_summary or {}).get("fee_validation_blocked_count") or 0
        )
        if trigger == "operator" and fee_validation_blocked_count > 0:
            return {
                "status": "blocked",
                "reason": "fee_validation_blocked",
                "recovery_stage": readiness.recovery_stage,
                "fee_validation_blocked_count": fee_validation_blocked_count,
                "recommended_command": readiness.recommended_command,
                "closeout_allowed": False,
                "dry_run": int(bool(dry_run)),
                "trigger": trigger,
            }
        row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
        qty = float(row["asset_qty"] if row is not None else 0.0)
        snapshot = build_canonical_position_snapshot(
            conn,
            metadata_raw=state_snapshot.last_reconcile_metadata,
            pair=settings.PAIR,
            portfolio_asset_qty=qty,
        )
    finally:
        conn.close()

    qty = snapshot.portfolio_asset_qty
    position_state = snapshot.position_state
    canonical_exposure, sellable_executable_lot_count, exit_allowed, exit_block_reason = _resolve_flatten_sell_authority(
        position_state=position_state,
    )
    terminal_state = str(position_state.normalized_exposure.terminal_state)
    if (not exit_allowed) or sellable_executable_lot_count < 1:
        summary = {
            "status": "no_position",
            "reason": str(exit_block_reason),
            "qty": float(canonical_exposure.open_exposure_qty),
            "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
            "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
            "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
            "terminal_state": terminal_state,
            "execution_flat": True,
            "closeout_allowed": False,
            "sellable_executable_lot_count": int(sellable_executable_lot_count),
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
        lot_definition=snapshot.lot_snapshot.lot_definition,
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
    submit_attempt_id: str | None = None
    try:
        quote = fetch_orderbook_top(settings.PAIR)
        bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
        market_price = float(bid)

        normalized_qty = _normalize_flatten_qty(qty=normalized_qty, market_price=market_price)
        _validate_flatten_pretrade(broker=broker, qty=normalized_qty)
        submit_attempt_id, payload_hash, _submit_ts = _stage_flatten_submit_intent(
            client_order_id=client_order_id,
            trigger=trigger,
            qty=normalized_qty,
            market_price=market_price,
            lot_snapshot=snapshot.lot_snapshot,
            exit_sizing=exit_sizing,
        )

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
        _record_flatten_submit_ack(
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            payload_hash=payload_hash,
            trigger=trigger,
            qty=normalized_qty,
            market_price=market_price,
            order=order,
        )
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        if submit_attempt_id is not None:
            _mark_flatten_submit_unknown(
                client_order_id=client_order_id,
                submit_attempt_id=submit_attempt_id,
                reason=f"operator flatten submit outcome unknown after pre-submit journal: {err}",
            )
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
