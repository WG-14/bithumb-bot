from __future__ import annotations

import math
import inspect
import json
import time
import uuid
from decimal import Decimal, ROUND_FLOOR
from types import SimpleNamespace

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


BROKER_CONFIRMED_RESIDUAL_CLOSEOUT = "broker_confirmed_residual_closeout"
FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL = "full_closeout_would_leave_residual"
MANUAL_EXCHANGE_CLOSEOUT_OR_RULE_UPDATE = "manual_exchange_closeout_or_rule_update"
_BROKER_OPEN_STATUSES = {
    "NEW",
    "PARTIAL",
    "PENDING_SUBMIT",
    "SUBMIT_UNKNOWN",
    "ACCOUNTING_PENDING",
    "CANCEL_REQUESTED",
    "OPEN",
    "WAIT",
    "WATCH",
}
_QTY_EPS = 1e-12


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
        normalized_qty = math.floor((normalized_qty / qty_step) + 1e-9) * qty_step
    max_qty_decimals = max(0, int(settings.LIVE_ORDER_MAX_QTY_DECIMALS))
    if max_qty_decimals > 0:
        scale = 10**max_qty_decimals
        normalized_qty = math.floor((normalized_qty * scale) + 1e-9) / scale
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


def _decimal_from_float(value: float) -> Decimal:
    parsed = Decimal(str(float(value)))
    if not parsed.is_finite():
        raise ValueError(f"invalid non-finite qty: {value}")
    return parsed


def _floor_qty_to_max_decimals(*, qty: float) -> float:
    max_qty_decimals = max(0, int(settings.LIVE_ORDER_MAX_QTY_DECIMALS))
    parsed = max(Decimal("0"), _decimal_from_float(qty))
    if max_qty_decimals > 0:
        parsed = parsed.quantize(Decimal("1").scaleb(-max_qty_decimals), rounding=ROUND_FLOOR)
    return max(0.0, float(parsed))


def _qty_matches_configured_step(*, qty: float) -> bool:
    qty_step = max(0.0, float(settings.LIVE_ORDER_QTY_STEP))
    if qty_step <= 0.0:
        return True
    parsed_qty = _decimal_from_float(qty)
    parsed_step = _decimal_from_float(qty_step)
    if parsed_step <= 0:
        return True
    stepped = (parsed_qty / parsed_step).to_integral_value(rounding=ROUND_FLOOR) * parsed_step
    return abs(float(parsed_qty - stepped)) <= _QTY_EPS


def _validate_flatten_qty_limits(*, qty: float, market_price: float) -> None:
    normalized_qty = max(0.0, float(qty))
    if normalized_qty <= 0:
        raise ValueError(f"invalid order qty: {normalized_qty}")
    min_qty = max(0.0, float(settings.LIVE_MIN_ORDER_QTY))
    if min_qty > 0 and normalized_qty + _QTY_EPS < min_qty:
        raise ValueError(f"order qty below minimum: {normalized_qty:.12f} < {min_qty:.12f}")
    min_notional = max(0.0, float(settings.MIN_ORDER_NOTIONAL_KRW))
    if min_notional > 0 and (normalized_qty * float(market_price)) + _QTY_EPS < min_notional:
        raise ValueError(
            f"order notional below minimum (SELL): {(normalized_qty * float(market_price)):.2f} < {min_notional:.2f}"
        )


def _clean_account_closeout_metrics(
    *,
    raw_total_asset_qty: float,
    planned_sell_qty: float,
    market_price: float,
) -> dict[str, object]:
    raw_qty = max(0.0, float(raw_total_asset_qty))
    planned_qty = max(0.0, float(planned_sell_qty))
    residual_qty = max(0.0, raw_qty - planned_qty)
    if residual_qty <= _QTY_EPS:
        residual_qty = 0.0
    else:
        residual_qty = round(residual_qty, 12)
    return {
        "raw_total_asset_qty": float(raw_qty),
        "planned_sell_qty": float(planned_qty),
        "estimated_residual_qty": float(residual_qty),
        "estimated_residual_notional_krw": float(residual_qty * float(market_price)),
        "clean_account_after_sell": bool(residual_qty <= _QTY_EPS),
    }


def _plan_exact_clean_closeout_qty(
    *,
    raw_total_asset_qty: float,
    market_price: float,
) -> tuple[float, dict[str, object]]:
    raw_qty = max(0.0, float(raw_total_asset_qty))
    step_planned_qty = _normalize_flatten_qty(qty=raw_qty, market_price=market_price)
    exact_qty = _floor_qty_to_max_decimals(qty=raw_qty)
    step_metrics = _clean_account_closeout_metrics(
        raw_total_asset_qty=raw_qty,
        planned_sell_qty=step_planned_qty,
        market_price=market_price,
    )
    exact_metrics = _clean_account_closeout_metrics(
        raw_total_asset_qty=raw_qty,
        planned_sell_qty=exact_qty,
        market_price=market_price,
    )
    if (
        not bool(exact_metrics["clean_account_after_sell"])
        or not _qty_matches_configured_step(qty=exact_qty)
    ):
        metrics = {
            **step_metrics,
            "closeout_allowed": False,
            "closeout_reason_code": FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL,
            "recommended_action": MANUAL_EXCHANGE_CLOSEOUT_OR_RULE_UPDATE,
            "max_decimal_closeout_qty": float(exact_qty),
            "configured_qty_step": float(settings.LIVE_ORDER_QTY_STEP),
            "configured_max_qty_decimals": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        }
        raise ValueError(json.dumps(metrics, sort_keys=True, separators=(",", ":")))
    _validate_flatten_qty_limits(qty=exact_qty, market_price=market_price)
    return exact_qty, {
        **exact_metrics,
        "step_floor_planned_sell_qty": float(step_planned_qty),
        "closeout_allowed": True,
        "closeout_reason_code": BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
        "recommended_action": None,
        "configured_qty_step": float(settings.LIVE_ORDER_QTY_STEP),
        "configured_max_qty_decimals": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    }


def _block_full_closeout_would_leave_residual(
    *,
    raw_total_asset_qty: float,
    planned_sell_qty: float,
    market_price: float,
) -> None:
    metrics = _clean_account_closeout_metrics(
        raw_total_asset_qty=raw_total_asset_qty,
        planned_sell_qty=planned_sell_qty,
        market_price=market_price,
    )
    metrics.update(
        {
            "closeout_allowed": False,
            "closeout_reason_code": FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL,
            "recommended_action": MANUAL_EXCHANGE_CLOSEOUT_OR_RULE_UPDATE,
            "configured_qty_step": float(settings.LIVE_ORDER_QTY_STEP),
            "configured_max_qty_decimals": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        }
    )
    raise ValueError(json.dumps(metrics, sort_keys=True, separators=(",", ":")))


def _plan_operator_clean_account_qty(
    *,
    default_planned_qty: float,
    raw_total_asset_qty: float,
    market_price: float,
    broker,
) -> tuple[float, dict[str, object]]:
    metrics = _clean_account_closeout_metrics(
        raw_total_asset_qty=raw_total_asset_qty,
        planned_sell_qty=default_planned_qty,
        market_price=market_price,
    )
    if bool(metrics["clean_account_after_sell"]):
        return float(default_planned_qty), {
            **metrics,
            "closeout_allowed": True,
            "closeout_reason_code": "operator_flatten",
            "recommended_action": None,
            "configured_qty_step": float(settings.LIVE_ORDER_QTY_STEP),
            "configured_max_qty_decimals": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        }

    balance = broker.get_balance()
    broker_asset_available = max(0.0, float(balance.asset_available))
    raw_qty = max(0.0, float(raw_total_asset_qty))
    tolerance = max(_QTY_EPS, float(settings.LIVE_MIN_ORDER_QTY or 0.0) * 1e-6)
    if abs(broker_asset_available - raw_qty) > tolerance:
        _block_full_closeout_would_leave_residual(
            raw_total_asset_qty=raw_qty,
            planned_sell_qty=default_planned_qty,
            market_price=market_price,
        )
    exact_qty, exact_metrics = _plan_exact_clean_closeout_qty(
        raw_total_asset_qty=broker_asset_available,
        market_price=market_price,
    )
    return float(exact_qty), {
        **exact_metrics,
        "broker_asset_available": float(broker_asset_available),
        "closeout_reason_code": BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
    }


def _validate_flatten_pretrade(*, broker, qty: float) -> None:
    balance = broker.get_balance()
    required_asset = float(qty)
    available_asset = float(balance.asset_available)
    if available_asset + 1e-12 < required_asset:
        raise ValueError(
            f"insufficient available asset: need={required_asset:.12f} avail={available_asset:.12f}"
        )


def _validate_flatten_db_schema() -> None:
    diagnostics = db_core.diagnose_db_path(settings.DB_PATH)
    if str(diagnostics.get("status") or "") != "PASS":
        errors = diagnostics.get("validation_errors") or []
        detail = "; ".join(str(item) for item in errors) if isinstance(errors, list) else str(errors)
        raise ValueError(f"db schema validation failed: {detail or 'status not PASS'}")


def _broker_open_orders(broker) -> list[object]:
    get_recent_orders_for_recovery = getattr(broker, "get_recent_orders_for_recovery", None)
    if callable(get_recent_orders_for_recovery):
        return list(get_recent_orders_for_recovery(limit=100, market=settings.PAIR))
    get_open_orders = getattr(broker, "get_open_orders", None)
    if callable(get_open_orders):
        return list(get_open_orders(exchange_order_ids=(), client_order_ids=()))
    raise ValueError("broker open-order verification unavailable")


def _assert_no_broker_open_orders(broker) -> int:
    open_orders = [
        order
        for order in _broker_open_orders(broker)
        if str(getattr(order, "status", "") or "").strip().upper() in _BROKER_OPEN_STATUSES
    ]
    if open_orders:
        raise ValueError(f"broker unresolved/open orders present: count={len(open_orders)}")
    return 0


def _is_residual_closeout_state(*, terminal_state: str, canonical_exposure) -> bool:
    if str(terminal_state) == "dust_only":
        return True
    return bool(
        getattr(canonical_exposure, "has_dust_only_remainder", False)
        and getattr(canonical_exposure, "effective_flat", False)
    )


def _residual_quantities_match(*, broker_asset_available: float, canonical_exposure) -> bool:
    raw_total_asset_qty = max(0.0, float(getattr(canonical_exposure, "raw_total_asset_qty", 0.0) or 0.0))
    tracked_dust_qty = max(0.0, float(getattr(canonical_exposure, "dust_tracking_qty", 0.0) or 0.0))
    expected_qty = raw_total_asset_qty if raw_total_asset_qty > 0.0 else tracked_dust_qty
    tolerance = max(1e-12, float(settings.LIVE_MIN_ORDER_QTY or 0.0) * 1e-6)
    return expected_qty > 0.0 and abs(float(broker_asset_available) - expected_qty) <= tolerance


def _build_residual_exit_sizing(*, qty: float, snapshot) -> object:
    lot_definition = snapshot.lot_snapshot.lot_definition
    internal_lot_size = float(getattr(lot_definition, "internal_lot_size", 0.0) or 0.0)
    effective_min_trade_qty = float(getattr(lot_definition, "min_qty", 0.0) or settings.LIVE_MIN_ORDER_QTY)
    return SimpleNamespace(
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        intended_lot_count=0,
        executable_lot_count=0,
        executable_qty=float(qty),
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
    reason_code: str = "operator_flatten",
) -> str:
    return json.dumps(
        {
            "client_order_id": client_order_id,
            "submit_attempt_id": submit_attempt_id,
            "submit_path": "operator_flatten",
            "reason_code": reason_code,
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
    reason_code: str = "operator_flatten",
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
            "reason_code": reason_code,
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
        reason_code=reason_code,
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
            decision_reason_code=reason_code,
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
            submission_reason_code=reason_code,
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
            decision_reason_code=reason_code,
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
    reason_code: str = "operator_flatten",
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
        reason_code=reason_code,
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
            submission_reason_code=reason_code,
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
    (
        canonical_exposure,
        sellable_executable_lot_count,
        exit_allowed,
        exit_block_reason,
    ) = _resolve_flatten_sell_authority(position_state=position_state)
    terminal_state = str(position_state.normalized_exposure.terminal_state)
    if (not exit_allowed) or sellable_executable_lot_count < 1:
        if trigger == "operator" and _is_residual_closeout_state(
            terminal_state=terminal_state,
            canonical_exposure=canonical_exposure,
        ):
            try:
                quote = fetch_orderbook_top(settings.PAIR)
                bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
                market_price = float(bid)
                balance = broker.get_balance()
                broker_asset_available = max(0.0, float(balance.asset_available))
                if not _residual_quantities_match(
                    broker_asset_available=broker_asset_available,
                    canonical_exposure=canonical_exposure,
                ):
                    raise LookupError(
                        "broker residual does not match local dust evidence: "
                        f"broker_asset_available={broker_asset_available:.12f} "
                        f"raw_total_asset_qty={float(canonical_exposure.raw_total_asset_qty):.12f}"
                    )
                normalized_qty, clean_closeout_metrics = _plan_exact_clean_closeout_qty(
                    raw_total_asset_qty=broker_asset_available,
                    market_price=market_price,
                )
                _validate_flatten_db_schema()
                broker_open_order_count = _assert_no_broker_open_orders(broker)
                if not dry_run:
                    if settings.MODE != "live":
                        raise ValueError("MODE=live is required for residual closeout")
                    if bool(settings.LIVE_DRY_RUN):
                        raise ValueError("LIVE_DRY_RUN=false is required for residual closeout submit")
                    if not bool(settings.LIVE_REAL_ORDER_ARMED):
                        raise ValueError("LIVE_REAL_ORDER_ARMED=true is required for residual closeout submit")
                    if bool(settings.KILL_SWITCH):
                        raise ValueError("KILL_SWITCH=false is required for residual closeout submit")
                    _validate_flatten_pretrade(broker=broker, qty=normalized_qty)

                exit_sizing = _build_residual_exit_sizing(qty=normalized_qty, snapshot=snapshot)
                runtime_state.record_flatten_position_result(
                    status="started" if not dry_run else "dry_run",
                    summary={
                        "status": "dry_run" if dry_run else "started",
                        "reason": BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
                        "qty": float(normalized_qty),
                        **clean_closeout_metrics,
                        "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
                        "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
                        "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
                        "broker_asset_available": float(broker_asset_available),
                        "reference_bid": float(market_price),
                        "terminal_state": terminal_state,
                        "execution_flat": True,
                        "closeout_allowed": True,
                        "broker_open_order_count": int(broker_open_order_count),
                        "sellable_executable_lot_count": int(sellable_executable_lot_count),
                        "dry_run": int(bool(dry_run)),
                        "side": "SELL",
                        "symbol": settings.PAIR,
                        "trigger": trigger,
                    },
                )
                if dry_run:
                    return {
                        "status": "dry_run",
                        "reason": BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
                        "qty": float(normalized_qty),
                        **clean_closeout_metrics,
                        "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
                        "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
                        "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
                        "broker_asset_available": float(broker_asset_available),
                        "reference_bid": float(market_price),
                        "terminal_state": terminal_state,
                        "execution_flat": True,
                        "closeout_allowed": True,
                        "sellable_executable_lot_count": int(sellable_executable_lot_count),
                        "dry_run": 1,
                        "side": "SELL",
                        "symbol": settings.PAIR,
                        "trigger": trigger,
                    }

                client_order_id = f"flatten_{int(time.time() * 1000)}"
                submit_attempt_id: str | None = None
                try:
                    submit_attempt_id, payload_hash, _submit_ts = _stage_flatten_submit_intent(
                        client_order_id=client_order_id,
                        trigger=trigger,
                        qty=normalized_qty,
                        market_price=market_price,
                        lot_snapshot=snapshot.lot_snapshot,
                        exit_sizing=exit_sizing,
                        reason_code=BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
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
                        reason_code=BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
                    )
                except Exception as exc:
                    err = f"{type(exc).__name__}: {exc}"
                    if submit_attempt_id is not None:
                        _mark_flatten_submit_unknown(
                            client_order_id=client_order_id,
                            submit_attempt_id=submit_attempt_id,
                            reason=(
                                "operator residual closeout submit outcome unknown after "
                                f"pre-submit journal: {err}"
                            ),
                        )
                    summary = {
                        "status": "failed",
                        "reason": BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
                        "qty": float(normalized_qty),
                        **clean_closeout_metrics,
                        "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
                        "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
                        "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
                        "broker_asset_available": float(broker_asset_available),
                        "terminal_state": terminal_state,
                        "side": "SELL",
                        "symbol": settings.PAIR,
                        "error": err,
                        "trigger": trigger,
                    }
                    runtime_state.record_flatten_position_result(status="failed", summary=summary)
                    return summary

                summary = {
                    "status": "submitted",
                    "reason": BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
                    "qty": normalized_qty,
                    **clean_closeout_metrics,
                    "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
                    "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
                    "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
                    "broker_asset_available": float(broker_asset_available),
                    "terminal_state": terminal_state,
                    "side": "SELL",
                    "symbol": settings.PAIR,
                    "client_order_id": client_order_id,
                    "exchange_order_id": str(order.exchange_order_id or "-"),
                    "order_status": str(order.status or "-"),
                    "trigger": trigger,
                }
                runtime_state.record_flatten_position_result(status="submitted", summary=summary)
                return summary
            except LookupError:
                pass
            except ValueError as exc:
                blocked_metrics: dict[str, object] = {}
                try:
                    parsed = json.loads(str(exc))
                    if isinstance(parsed, dict):
                        blocked_metrics = parsed
                except (TypeError, ValueError, json.JSONDecodeError):
                    blocked_metrics = {}
                reason = str(blocked_metrics.get("closeout_reason_code") or exc)
                summary = {
                    "status": "blocked",
                    "reason": reason,
                    "closeout_reason_code": str(
                        blocked_metrics.get("closeout_reason_code") or BROKER_CONFIRMED_RESIDUAL_CLOSEOUT
                    ),
                    "recommended_action": blocked_metrics.get("recommended_action"),
                    "qty": 0.0,
                    **blocked_metrics,
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
                runtime_state.record_flatten_position_result(status="blocked", summary=summary)
                return summary

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
    operator_planned_qty = float(exit_sizing.executable_qty)
    clean_closeout_metrics: dict[str, object] = {}
    if trigger == "operator":
        try:
            quote = fetch_orderbook_top(settings.PAIR)
            bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
            market_price = float(bid)
            default_planned_qty = _normalize_flatten_qty(qty=float(exit_sizing.executable_qty), market_price=market_price)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            summary = {
                "status": "failed",
                "qty": 0.0,
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
            return summary
        try:
            operator_planned_qty, clean_closeout_metrics = _plan_operator_clean_account_qty(
                default_planned_qty=default_planned_qty,
                raw_total_asset_qty=float(canonical_exposure.raw_total_asset_qty),
                market_price=market_price,
                broker=broker,
            )
        except ValueError as exc:
            blocked_metrics: dict[str, object] = {}
            try:
                parsed = json.loads(str(exc))
                if isinstance(parsed, dict):
                    blocked_metrics = parsed
            except (TypeError, ValueError, json.JSONDecodeError):
                blocked_metrics = {}
            reason = str(blocked_metrics.get("closeout_reason_code") or exc)
            summary = {
                "status": "blocked",
                "reason": reason,
                "qty": 0.0,
                **blocked_metrics,
                "raw_total_asset_qty": float(canonical_exposure.raw_total_asset_qty),
                "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
                "tracked_dust_qty": float(canonical_exposure.dust_tracking_qty),
                "terminal_state": terminal_state,
                "execution_flat": False,
                "closeout_allowed": False,
                "sellable_executable_lot_count": int(sellable_executable_lot_count),
                "dry_run": int(bool(dry_run)),
                "side": "SELL",
                "symbol": settings.PAIR,
                "trigger": trigger,
            }
            runtime_state.record_flatten_position_result(status="blocked", summary=summary)
            return summary

    runtime_state.record_flatten_position_result(
        status="started",
        summary={
            "qty": float(operator_planned_qty),
            **clean_closeout_metrics,
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
            qty=operator_planned_qty,
            dry_run=1 if dry_run else 0,
            trigger=trigger,
        )
    )

    if dry_run:
        summary = {
            "status": "dry_run",
            "qty": float(operator_planned_qty),
            **clean_closeout_metrics,
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
    normalized_qty = float(operator_planned_qty)
    submit_attempt_id: str | None = None
    try:
        quote = fetch_orderbook_top(settings.PAIR)
        bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
        market_price = float(bid)
        if trigger != "operator":
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
            **clean_closeout_metrics,
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
        **clean_closeout_metrics,
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
            qty=normalized_qty,
            client_order_id=client_order_id,
            exchange_order_id=str(order.exchange_order_id or "-"),
            status=str(order.status or "-"),
            trigger=trigger,
        )
    )
    return summary
