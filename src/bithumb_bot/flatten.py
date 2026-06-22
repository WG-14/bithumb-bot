from __future__ import annotations

import math
import inspect
import json
import time
import uuid
from types import SimpleNamespace

from . import runtime_state
from .canonical_decision import sha256_prefixed
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
from .broker.order_payloads import build_order_payload_from_plan
from .broker import order_rules
from .operator_closeout import (
    COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT,
    build_operator_clean_closeout_contract,
    validate_clean_closeout_contract_for_submit,
)
from .quantity_contract import ExchangeQuantityContract


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


def _operator_blocked_json_summary(
    *,
    reason: str,
    dry_run: bool,
    trigger: str,
    recommended_action: str | None = MANUAL_EXCHANGE_CLOSEOUT_OR_RULE_UPDATE,
    recommended_command: str | None = None,
    quantity_authority: dict[str, object] | None = None,
    quantity_authority_unavailable_reason: str | None = None,
    **fields: object,
) -> dict[str, object]:
    summary = {
        "status": "blocked",
        "command_intent": COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT,
        "market": settings.PAIR,
        "symbol": settings.PAIR,
        "side": "SELL",
        "dry_run": int(bool(dry_run)),
        "trigger": trigger,
        "closeout_allowed": False,
        "block_reason": str(reason),
        "reason": str(reason),
        "recommended_action": recommended_action,
        "recommended_command": recommended_command,
    }
    if quantity_authority is not None:
        summary["quantity_authority"] = dict(quantity_authority)
    if quantity_authority_unavailable_reason is not None:
        summary["quantity_authority_unavailable_reason"] = str(
            quantity_authority_unavailable_reason
        )
    summary.update(fields)
    return summary


def _operator_blocked_contract_fields(summary: dict[str, object]) -> dict[str, object]:
    reserved = {
        "status",
        "command_intent",
        "market",
        "symbol",
        "side",
        "dry_run",
        "trigger",
        "closeout_allowed",
        "block_reason",
        "reason",
        "recommended_action",
        "recommended_command",
        "quantity_authority",
        "raw_total_asset_qty",
        "executable_exposure_qty",
        "tracked_dust_qty",
        "broker_asset_available",
        "reference_bid",
        "terminal_state",
        "execution_flat",
        "sellable_executable_lot_count",
        "qty",
    }
    return {key: value for key, value in summary.items() if key not in reserved}


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


def _resolve_operator_quantity_contract_and_rules(*, market: str):
    try:
        resolution = order_rules.get_effective_order_rules(market)
        return ExchangeQuantityContract.from_rule_resolution(resolution, market=market), resolution.rules
    except Exception:
        rules = order_rules.DerivedOrderConstraints(
            market_id=market,
            bid_min_total_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            ask_min_total_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            bid_price_unit=1.0,
            ask_price_unit=1.0,
            order_types=("limit", "price", "market"),
            bid_types=("price",),
            ask_types=("limit", "market"),
            order_sides=("bid", "ask"),
            min_qty=float(settings.LIVE_MIN_ORDER_QTY),
            qty_step=float(settings.LIVE_ORDER_QTY_STEP),
            min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
            max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        )
        return (
            ExchangeQuantityContract.local_fallback(
                market=market,
                min_qty=float(settings.LIVE_MIN_ORDER_QTY),
                min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
                max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
                configured_qty_step=float(settings.LIVE_ORDER_QTY_STEP),
            ),
            rules,
        )


def _clean_closeout_summary_from_contract(
    contract,
    *,
    allowed_reason: str = BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
) -> dict[str, object]:
    contract_payload = contract.as_dict()
    quantity_authority = dict(contract_payload.get("quantity_authority") or {})
    submit_payload_preview = dict(contract_payload.get("submit_payload_preview") or {})
    reason = (
        allowed_reason
        if bool(contract.closeout_allowed)
        else str(contract.block_reason or FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL)
    )
    summary = {
        **contract_payload,
        "closeout_reason_code": reason,
        "reason": reason,
        "quantity_rule_source_mode": quantity_authority.get("quantity_rule_source_mode"),
        "min_qty_source": quantity_authority.get("min_qty_source"),
        "qty_step_source": quantity_authority.get("qty_step_source"),
        "max_qty_decimals_source": quantity_authority.get("max_qty_decimals_source"),
        "qty_step_authority_level": quantity_authority.get("qty_step_authority_level"),
        "quantity_contract_complete": quantity_authority.get("quantity_contract_complete"),
        "quantity_contract_recommended_action": quantity_authority.get(
            "quantity_contract_recommended_action"
        ),
        **submit_payload_preview,
    }
    summary.pop("status", None)
    return summary


def _operator_closeout_submit_evidence(
    *,
    clean_closeout_metrics: dict[str, object],
    raw_total_asset_qty: float,
    covered_open_exposure_qty: float,
    covered_dust_tracking_qty: float,
    broker_qty_after: float | None = None,
    portfolio_qty_after: float | None = None,
) -> dict[str, object]:
    planned_qty = float(clean_closeout_metrics.get("planned_sell_qty") or 0.0)
    payload_volume = clean_closeout_metrics.get("payload_volume")
    evidence = {
        "authority_type": COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT,
        "command_intent": COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT,
        "reason_code": BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
        "raw_total_asset_qty": float(raw_total_asset_qty),
        "tracked_dust_qty": float(covered_dust_tracking_qty),
        "planned_sell_qty": planned_qty,
        "planned_qty": planned_qty,
        "submitted_qty": planned_qty,
        "payload_volume": float(payload_volume) if payload_volume is not None else None,
        "clean_account_after_sell": bool(clean_closeout_metrics.get("clean_account_after_sell")),
        "estimated_residual_qty": float(clean_closeout_metrics.get("estimated_residual_qty") or 0.0),
        "covered_dust_tracking_qty": float(covered_dust_tracking_qty),
        "covered_open_exposure_qty": float(covered_open_exposure_qty),
        "broker_qty_after": broker_qty_after,
        "portfolio_qty_after": portfolio_qty_after,
    }
    evidence["operator_closeout_contract_hash"] = sha256_prefixed(evidence)
    return evidence


def plan_operator_clean_account_closeout_from_flatten_context(
    *,
    broker,
    raw_total_asset_qty: float,
    market_price: float,
    dry_run: bool,
    client_order_id: str,
):
    quantity_contract, resolved_rules = _resolve_operator_quantity_contract_and_rules(
        market=settings.PAIR,
    )
    balance = broker.get_balance()
    broker_asset_available = max(0.0, float(balance.asset_available))
    closeout_contract = build_operator_clean_closeout_contract(
        broker=broker,
        market=settings.PAIR,
        raw_total_asset_qty=float(raw_total_asset_qty),
        broker_asset_available=broker_asset_available,
        market_price=float(market_price),
        quantity_contract=quantity_contract,
        dry_run=dry_run,
        plan_place_order_fn=plan_place_order,
        rules=resolved_rules,
        client_order_id=client_order_id,
    )
    return closeout_contract, resolved_rules


def _build_validated_clean_closeout_submit_plan(
    *,
    broker,
    contract,
    rules,
    client_order_id: str,
    market_price: float,
):
    latest_balance = broker.get_balance()
    submit_plan = plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id=client_order_id,
            market=settings.PAIR,
            side="SELL",
            normalized_side="ask",
            qty=float(contract.planned_sell_qty),
            price=None,
            created_ts=int(time.time() * 1000),
            market_price_hint=float(market_price),
            trace_id=client_order_id,
        ),
        rules=rules,
        skip_qty_revalidation=True,
    )
    validate_clean_closeout_contract_for_submit(
        contract,
        submitted_qty=float(submit_plan.submitted_qty),
        broker_asset_available=float(latest_balance.asset_available),
    )
    if abs(float(submit_plan.submitted_qty) - float(contract.planned_sell_qty)) > _QTY_EPS:
        raise ValueError("operator clean closeout submit plan qty does not match contract")
    payload_plan = build_order_payload_from_plan(plan=submit_plan)
    payload_volume = payload_plan.payload.get("volume")
    if payload_volume is None or abs(float(payload_volume) - float(contract.planned_sell_qty)) > _QTY_EPS:
        raise ValueError("operator clean closeout payload volume does not match contract")
    if not bool(contract.clean_account_after_sell) or float(contract.estimated_residual_qty) > _QTY_EPS:
        raise ValueError("operator clean closeout contract is not clean-account proof")
    return submit_plan


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
    raw_total_asset_qty = max(0.0, float(getattr(canonical_exposure, "raw_total_asset_qty", 0.0) or 0.0))
    tracked_dust_qty = max(0.0, float(getattr(canonical_exposure, "dust_tracking_qty", 0.0) or 0.0))
    if raw_total_asset_qty <= _QTY_EPS and tracked_dust_qty <= _QTY_EPS:
        return False
    return bool(
        str(terminal_state) == "dust_only"
        and getattr(canonical_exposure, "has_dust_only_remainder", False)
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
    closeout_contract_evidence: dict[str, object] | None = None,
) -> str:
    evidence: dict[str, object] = {
        "client_order_id": client_order_id,
        "submit_attempt_id": submit_attempt_id,
        "submit_path": "operator_flatten",
        "reason_code": reason_code,
        "trigger": trigger,
        "symbol": settings.PAIR,
        "side": "SELL",
        "qty": float(qty),
        "submitted_qty": float(qty),
        "price": None,
        "reference_price": float(market_price) if market_price is not None else None,
        "phase": phase,
        "status": status,
        "exchange_order_id": exchange_order_id,
        "error": error,
    }
    if closeout_contract_evidence:
        evidence.update(dict(closeout_contract_evidence))
    if (
        evidence.get("command_intent") == COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT
        and evidence.get("planned_sell_qty") is not None
        and evidence.get("payload_volume") is not None
        and abs(float(evidence["planned_sell_qty"]) - float(evidence["payload_volume"])) > _QTY_EPS
    ):
        raise ValueError("operator clean closeout payload volume does not match planned sell qty")
    if evidence.get("command_intent") == COMMAND_INTENT_OPERATOR_CLEAN_ACCOUNT_CLOSEOUT:
        proof_payload = {
            key: value
            for key, value in evidence.items()
            if key not in {"operator_closeout_contract_hash", "evidence_hash"}
        }
        evidence.setdefault("operator_closeout_contract_hash", sha256_prefixed(proof_payload))
        evidence.setdefault("evidence_hash", evidence["operator_closeout_contract_hash"])
    return json.dumps(
        evidence,
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
    closeout_contract_evidence: dict[str, object] | None = None,
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
        closeout_contract_evidence=closeout_contract_evidence,
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
    closeout_contract_evidence: dict[str, object] | None = None,
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
        closeout_contract_evidence=closeout_contract_evidence,
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
        residual_disposition = getattr(readiness, "residual_disposition", None)
        if (
            trigger == "operator"
            and residual_disposition is not None
            and getattr(residual_disposition, "disposition", "") == "TRACKED_NON_EXECUTABLE"
        ):
            summary = {
                "status": "tracked_non_executable_residual",
                "reason": "sub_min_qty_residual_tracked",
                "residual_disposition": "TRACKED_NON_EXECUTABLE",
                "residual_reason_code": "sub_min_qty_residual_tracked",
                "operator_action_required": False,
                "recommended_action": "none",
                "recommended_command": None,
                "manual_exchange_action_required": False,
                "submit_expected": False,
                "closeout_allowed": False,
                "flatten_required": False,
                "qty": 0.0,
                "raw_total_asset_qty": float(readiness.residual_inventory.residual_qty),
                "executable_exposure_qty": float(
                    readiness.position_state.normalized_exposure.open_exposure_qty
                ),
                "tracked_dust_qty": float(readiness.residual_inventory.residual_qty),
                "terminal_state": str(readiness.position_state.normalized_exposure.terminal_state),
                "execution_flat": True,
                "sellable_executable_lot_count": int(
                    readiness.position_state.normalized_exposure.sellable_executable_lot_count
                ),
                "quantity_rule_authority": getattr(
                    residual_disposition, "quantity_rule_authority", "unknown"
                ),
                "broker_local_projection_state": getattr(
                    residual_disposition, "broker_local_projection_state", "unknown"
                ),
                "dry_run": int(bool(dry_run)),
                "side": "SELL",
                "symbol": settings.PAIR,
                "trigger": trigger,
            }
            runtime_state.record_flatten_position_result(
                status="tracked_non_executable_residual",
                summary=summary,
            )
            return summary
        if trigger == "operator":
            if int(readiness.recovery_required_count or 0) > 0:
                return _operator_blocked_json_summary(
                    reason="recovery_required_orders_present",
                    dry_run=dry_run,
                    trigger=trigger,
                    recommended_command=readiness.recommended_command,
                    quantity_authority_unavailable_reason=(
                        "precondition_blocked_before_quantity_authority_resolution"
                    ),
                    recovery_stage=readiness.recovery_stage,
                    recovery_required_count=int(readiness.recovery_required_count or 0),
                )
            if int(readiness.open_order_count or 0) > 0:
                return _operator_blocked_json_summary(
                    reason="unresolved_orders_present",
                    dry_run=dry_run,
                    trigger=trigger,
                    recommended_command=readiness.recommended_command,
                    quantity_authority_unavailable_reason=(
                        "precondition_blocked_before_quantity_authority_resolution"
                    ),
                    recovery_stage=readiness.recovery_stage,
                    open_order_count=int(readiness.open_order_count or 0),
                )
        unapplied_principal_pending_count = int(
            (readiness.fill_accounting_incident_summary or {}).get("unapplied_principal_pending_count") or 0
        )
        if unapplied_principal_pending_count > 0:
            if trigger == "operator":
                return _operator_blocked_json_summary(
                    reason="unapplied_principal_pending",
                    dry_run=dry_run,
                    trigger=trigger,
                    recommended_command="uv run python bot.py recovery-report",
                    quantity_authority_unavailable_reason=(
                        "precondition_blocked_before_quantity_authority_resolution"
                    ),
                    recovery_stage=readiness.recovery_stage,
                    unapplied_principal_pending_count=unapplied_principal_pending_count,
                )
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
            return _operator_blocked_json_summary(
                reason="fee_validation_blocked",
                dry_run=dry_run,
                trigger=trigger,
                recommended_command=readiness.recommended_command,
                quantity_authority_unavailable_reason=(
                    "precondition_blocked_before_quantity_authority_resolution"
                ),
                recovery_stage=readiness.recovery_stage,
                fee_validation_blocked_count=fee_validation_blocked_count,
            )
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
                client_order_id = f"flatten_{int(time.time() * 1000)}"
                closeout_contract, resolved_rules = plan_operator_clean_account_closeout_from_flatten_context(
                    broker=broker,
                    raw_total_asset_qty=broker_asset_available,
                    market_price=market_price,
                    dry_run=dry_run,
                    client_order_id=client_order_id,
                )
                clean_closeout_metrics = _clean_closeout_summary_from_contract(closeout_contract)
                normalized_qty = float(closeout_contract.planned_sell_qty)
                if not closeout_contract.closeout_allowed:
                    block_reason = str(
                        closeout_contract.block_reason or FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL
                    )
                    summary = _operator_blocked_json_summary(
                        reason=block_reason,
                        dry_run=dry_run,
                        trigger=trigger,
                        recommended_action=closeout_contract.recommended_action,
                        quantity_authority=closeout_contract.quantity_authority,
                        qty=0.0,
                        **_operator_blocked_contract_fields(clean_closeout_metrics),
                        raw_total_asset_qty=float(canonical_exposure.raw_total_asset_qty),
                        executable_exposure_qty=float(canonical_exposure.open_exposure_qty),
                        tracked_dust_qty=float(canonical_exposure.dust_tracking_qty),
                        broker_asset_available=float(broker_asset_available),
                        reference_bid=float(market_price),
                        terminal_state=terminal_state,
                        execution_flat=True,
                        sellable_executable_lot_count=int(sellable_executable_lot_count),
                    )
                    runtime_state.record_flatten_position_result(status="blocked", summary=summary)
                    return summary
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

                submit_attempt_id: str | None = None
                try:
                    closeout_submit_evidence = _operator_closeout_submit_evidence(
                        clean_closeout_metrics=clean_closeout_metrics,
                        raw_total_asset_qty=float(canonical_exposure.raw_total_asset_qty),
                        covered_open_exposure_qty=float(canonical_exposure.open_exposure_qty),
                        covered_dust_tracking_qty=float(canonical_exposure.dust_tracking_qty),
                        broker_qty_after=0.0,
                        portfolio_qty_after=0.0,
                    )
                    submit_attempt_id, payload_hash, _submit_ts = _stage_flatten_submit_intent(
                        client_order_id=client_order_id,
                        trigger=trigger,
                        qty=normalized_qty,
                        market_price=market_price,
                        lot_snapshot=snapshot.lot_snapshot,
                        exit_sizing=exit_sizing,
                        reason_code=BROKER_CONFIRMED_RESIDUAL_CLOSEOUT,
                        closeout_contract_evidence=closeout_submit_evidence,
                    )
                    place_order_kwargs = {
                        "client_order_id": client_order_id,
                        "side": "SELL",
                        "qty": normalized_qty,
                        "price": None,
                    }
                    place_order_params = inspect.signature(broker.place_order).parameters
                    if "submit_plan" in place_order_params:
                        submit_plan = _build_validated_clean_closeout_submit_plan(
                            broker=broker,
                            contract=closeout_contract,
                            rules=resolved_rules,
                            client_order_id=client_order_id,
                            market_price=market_price,
                        )
                        place_order_kwargs["submit_plan"] = submit_plan
                    else:
                        raise ValueError("broker.place_order submit_plan support required for operator clean closeout")
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
                        closeout_contract_evidence=closeout_submit_evidence,
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
                summary = _operator_blocked_json_summary(
                    reason=reason,
                    dry_run=dry_run,
                    trigger=trigger,
                recommended_action=blocked_metrics.get("recommended_action")
                or MANUAL_EXCHANGE_CLOSEOUT_OR_RULE_UPDATE,
                quantity_authority_unavailable_reason="contract_build_failed_before_quantity_authority",
                closeout_reason_code=str(
                    blocked_metrics.get("closeout_reason_code") or BROKER_CONFIRMED_RESIDUAL_CLOSEOUT
                ),
                qty=0.0,
                **_operator_blocked_contract_fields(blocked_metrics),
                raw_total_asset_qty=float(canonical_exposure.raw_total_asset_qty),
                    executable_exposure_qty=float(canonical_exposure.open_exposure_qty),
                    tracked_dust_qty=float(canonical_exposure.dust_tracking_qty),
                    terminal_state=terminal_state,
                    execution_flat=True,
                    sellable_executable_lot_count=int(sellable_executable_lot_count),
                )
                runtime_state.record_flatten_position_result(status="blocked", summary=summary)
                return summary

        raw_total_asset_qty = float(canonical_exposure.raw_total_asset_qty)
        tracked_dust_qty = float(canonical_exposure.dust_tracking_qty)
        min_qty = max(0.0, float(settings.LIVE_MIN_ORDER_QTY or 0.0))
        should_report_operator_block = (
            trigger == "operator"
            and (raw_total_asset_qty > 0.0 or tracked_dust_qty > 0.0)
            and (terminal_state != "dust_only" or raw_total_asset_qty + _QTY_EPS >= min_qty)
            and min_qty > 0.0
        )
        if should_report_operator_block:
            summary = _operator_blocked_json_summary(
                reason=str(exit_block_reason),
                dry_run=dry_run,
                trigger=trigger,
                recommended_action=MANUAL_EXCHANGE_CLOSEOUT_OR_RULE_UPDATE,
                quantity_authority_unavailable_reason="no_executable_exit_lot_before_quantity_authority_resolution",
                qty=float(canonical_exposure.open_exposure_qty),
                raw_total_asset_qty=raw_total_asset_qty,
                executable_exposure_qty=float(canonical_exposure.open_exposure_qty),
                tracked_dust_qty=tracked_dust_qty,
                terminal_state=terminal_state,
                execution_flat=True,
                sellable_executable_lot_count=int(sellable_executable_lot_count),
            )
            runtime_state.record_flatten_position_result(status="blocked", summary=summary)
            return summary
        summary = {
            "status": "no_position",
            "reason": str(exit_block_reason),
            "qty": float(canonical_exposure.open_exposure_qty),
            "raw_total_asset_qty": raw_total_asset_qty,
            "executable_exposure_qty": float(canonical_exposure.open_exposure_qty),
            "tracked_dust_qty": tracked_dust_qty,
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
    operator_closeout_contract = None
    operator_resolved_rules = None
    operator_market_price: float | None = None
    client_order_id = f"flatten_{int(time.time() * 1000)}"
    if trigger == "operator":
        try:
            quote = fetch_orderbook_top(settings.PAIR)
            bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
            operator_market_price = float(bid)
            operator_closeout_contract, operator_resolved_rules = plan_operator_clean_account_closeout_from_flatten_context(
                broker=broker,
                raw_total_asset_qty=float(canonical_exposure.raw_total_asset_qty),
                market_price=operator_market_price,
                dry_run=dry_run,
                client_order_id=client_order_id,
            )
            clean_closeout_metrics = _clean_closeout_summary_from_contract(operator_closeout_contract)
            operator_planned_qty = float(operator_closeout_contract.planned_sell_qty)
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
        if not bool(operator_closeout_contract.closeout_allowed):
            reason = str(operator_closeout_contract.block_reason or FULL_CLOSEOUT_WOULD_LEAVE_RESIDUAL)
            summary = _operator_blocked_json_summary(
                reason=reason,
                dry_run=dry_run,
                trigger=trigger,
                recommended_action=operator_closeout_contract.recommended_action,
                quantity_authority=operator_closeout_contract.quantity_authority,
                qty=0.0,
                **_operator_blocked_contract_fields(clean_closeout_metrics),
                raw_total_asset_qty=float(canonical_exposure.raw_total_asset_qty),
                executable_exposure_qty=float(canonical_exposure.open_exposure_qty),
                tracked_dust_qty=float(canonical_exposure.dust_tracking_qty),
                terminal_state=terminal_state,
                execution_flat=False,
                sellable_executable_lot_count=int(sellable_executable_lot_count),
            )
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

    normalized_qty = float(operator_planned_qty)
    submit_attempt_id: str | None = None
    try:
        if trigger == "operator" and operator_market_price is not None:
            market_price = float(operator_market_price)
        else:
            quote = fetch_orderbook_top(settings.PAIR)
            bid, _ask = validated_best_quote_prices(quote, requested_market=settings.PAIR)
            market_price = float(bid)
        if trigger != "operator":
            normalized_qty = _normalize_flatten_qty(qty=normalized_qty, market_price=market_price)
        if trigger == "operator":
            if settings.MODE != "live":
                raise ValueError("MODE=live is required for operator clean closeout submit")
            if bool(settings.LIVE_DRY_RUN):
                raise ValueError("LIVE_DRY_RUN=false is required for operator clean closeout submit")
            if not bool(settings.LIVE_REAL_ORDER_ARMED):
                raise ValueError("LIVE_REAL_ORDER_ARMED=true is required for operator clean closeout submit")
            if bool(settings.KILL_SWITCH):
                raise ValueError("KILL_SWITCH=false is required for operator clean closeout submit")
            if operator_closeout_contract is None or operator_resolved_rules is None:
                raise ValueError("operator clean closeout contract missing before submit")
            _assert_no_broker_open_orders(broker)
        _validate_flatten_pretrade(broker=broker, qty=normalized_qty)
        closeout_submit_evidence = None
        submit_reason_code = "operator_flatten"
        if trigger == "operator":
            closeout_submit_evidence = _operator_closeout_submit_evidence(
                clean_closeout_metrics=clean_closeout_metrics,
                raw_total_asset_qty=float(canonical_exposure.raw_total_asset_qty),
                covered_open_exposure_qty=float(canonical_exposure.open_exposure_qty),
                covered_dust_tracking_qty=float(canonical_exposure.dust_tracking_qty),
                broker_qty_after=0.0,
                portfolio_qty_after=0.0,
            )
            submit_reason_code = BROKER_CONFIRMED_RESIDUAL_CLOSEOUT
        submit_attempt_id, payload_hash, _submit_ts = _stage_flatten_submit_intent(
            client_order_id=client_order_id,
            trigger=trigger,
            qty=normalized_qty,
            market_price=market_price,
            lot_snapshot=snapshot.lot_snapshot,
            exit_sizing=exit_sizing,
            reason_code=submit_reason_code,
            closeout_contract_evidence=closeout_submit_evidence,
        )

        place_order_kwargs = {
            "client_order_id": client_order_id,
            "side": "SELL",
            "qty": normalized_qty,
            "price": None,
        }
        place_order_params = inspect.signature(broker.place_order).parameters
        if "submit_plan" in place_order_params:
            if trigger == "operator":
                submit_plan = _build_validated_clean_closeout_submit_plan(
                    broker=broker,
                    contract=operator_closeout_contract,
                    rules=operator_resolved_rules,
                    client_order_id=client_order_id,
                    market_price=market_price,
                )
                normalized_qty = float(submit_plan.submitted_qty)
                place_order_kwargs["qty"] = normalized_qty
            else:
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
        elif trigger == "operator":
            raise ValueError("broker.place_order submit_plan support required for operator clean closeout")
        order = broker.place_order(**place_order_kwargs)
        _record_flatten_submit_ack(
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            payload_hash=payload_hash,
            trigger=trigger,
            qty=normalized_qty,
            market_price=market_price,
            order=order,
            reason_code=submit_reason_code,
            closeout_contract_evidence=closeout_submit_evidence,
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
