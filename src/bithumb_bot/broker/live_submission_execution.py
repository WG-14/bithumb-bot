from __future__ import annotations

import time
from dataclasses import replace

from ..config import settings
from ..execution import LiveFillFeeValidationError, apply_fill_and_trade
from ..notifier import format_event, notify
from ..observability import format_log_kv, safety_event
from ..oms import (
    TERMINAL_ORDER_STATUSES,
    build_client_order_id,
    build_order_intent_key,
    claim_order_intent_dedup,
    evaluate_unresolved_order_gate,
    payload_fingerprint,
    record_submit_blocked,
    set_status,
    update_order_intent_dedup,
)
from ..reason_codes import (
    RISKY_ORDER_BLOCK,
    SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
    SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
)
from ..risk import evaluate_order_submission_halt
from .live_submit_planning import build_live_submit_plan
from .live_submit_orchestrator import (
    LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
    StandardSubmitPipelineRequest,
    record_standard_submit_planning_failure,
    run_standard_submit_pipeline,
)


def execute_live_submission_and_application(
    live_module,
    *,
    broker,
    signal: str,
    ts: int,
    market_price: float,
    position_state,
    submission_ready,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
):
    intent = submission_ready.intent
    feasibility = submission_ready.feasibility
    conn = position_state.conn
    decision_observability = position_state.decision_observability
    submit_attempt_id = live_module._submit_attempt_id()
    client_order_id = live_module._client_order_id(ts=ts, side=feasibility.side, submit_attempt_id=submit_attempt_id)
    strategy_context = live_module._order_intent_strategy_context()
    intent_type = live_module._order_intent_type(side=feasibility.side)
    lot_sizing = feasibility.entry_sizing if feasibility.side == "BUY" else feasibility.exit_sizing
    intent_key = build_order_intent_key(
        symbol=settings.PAIR,
        side=feasibility.side,
        strategy_context=strategy_context,
        intent_ts=int(ts),
        intent_type=intent_type,
        qty=feasibility.normalized_qty,
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
    )

    reference_price: float | None = None
    top_of_book_summary: dict[str, float | str] | None = None
    if feasibility.reference_quote is not None:
        reference_price = float(feasibility.reference_quote["reference_price"])
        top_of_book_summary = {
            "bid": float(feasibility.reference_quote["bid"]),
            "ask": float(feasibility.reference_quote["ask"]),
            "spread": float(feasibility.reference_quote["ask"]) - float(feasibility.reference_quote["bid"]),
            "reference_ts": live_module._format_epoch_ts(float(feasibility.reference_quote["reference_ts_epoch_sec"])),
            "reference_source": str(feasibility.reference_quote["reference_source"]),
        }
    else:
        try:
            reference_quote = live_module._load_live_reference_quote(pair=settings.PAIR)
            reference_price = float(reference_quote["reference_price"])
            top_of_book_summary = {
                "bid": float(reference_quote["bid"]),
                "ask": float(reference_quote["ask"]),
                "spread": float(reference_quote["ask"]) - float(reference_quote["bid"]),
                "reference_ts": live_module._format_epoch_ts(float(reference_quote["reference_ts_epoch_sec"])),
                "reference_source": str(reference_quote["reference_source"]),
            }
        except ValueError as exc:
            reference_price = None
            top_of_book_summary = {"error": str(exc).removeprefix("reference price unavailable: ")}

    blocked, reason = evaluate_order_submission_halt(
        conn,
        ts_ms=int(ts),
        now_ms=int(time.time() * 1000),
        cash=float(position_state.cash),
        qty=float(position_state.portfolio_qty),
        price=float(market_price),
    )
    if blocked:
        gate_blocked, reason_code, gate_reason = evaluate_unresolved_order_gate(
            conn,
            now_ms=int(time.time() * 1000),
            max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
        )
        if gate_blocked:
            blocked_client_order_id = build_client_order_id(
                mode="live",
                side=feasibility.side,
                intent_ts=int(ts),
                submit_attempt_id=submit_attempt_id,
            )
            gate_reason = (
                f"category=unresolved_risk_gate;"
                f"reason_detail_code={reason_code};"
                f"reason={gate_reason}"
            )
            live_module.RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] unresolved risk gate",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side=feasibility.side,
                    reason=gate_reason,
                    sell_failure_category=SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
                    sell_failure_detail=SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
                    reason_detail_code=reason_code,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                    open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                    dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                    submit_qty_source=decision_observability["submit_qty_source"],
                    position_state_source=decision_observability["position_state_source"],
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            live_module._block_new_submission_for_unresolved_risk(
                conn=conn,
                client_order_id=blocked_client_order_id,
                side=feasibility.side,
                qty=feasibility.normalized_qty,
                ts=ts,
                reason_code=reason_code,
                reason=gate_reason,
            )
            conn.commit()
            return None

        live_module.RUN_LOG.info(
            format_log_kv(
                "[ORDER_SKIP] submission halt",
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                signal=signal,
                side=feasibility.side,
                reason=f"category=submission_halt;reason_detail_code=submission_halt;reason={reason}",
                sell_failure_category=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                sell_failure_detail=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                reason_detail_code="submission_halt",
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                submit_qty_source=decision_observability["submit_qty_source"],
                position_state_source=decision_observability["position_state_source"],
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            )
        )
        notify(f"live order placement blocked ({feasibility.side}): category=submission_halt;reason={reason}")
        return None

    existing = conn.execute(
        "SELECT status FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if existing is not None:
        existing_status = str(existing["status"])
        if existing_status in TERMINAL_ORDER_STATUSES:
            reason = f"duplicate submit blocked: terminal status {existing_status}"
            live_module.RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] duplicate client order id",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side=feasibility.side,
                    reason=reason,
                    client_order_id=client_order_id,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                    open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                    dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                    submit_qty_source=decision_observability["submit_qty_source"],
                    position_state_source=decision_observability["position_state_source"],
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            record_submit_blocked(client_order_id, status=existing_status, reason=reason, conn=conn)
            notify(
                safety_event(
                    "order_submit_blocked",
                    client_order_id=client_order_id,
                    submit_attempt_id=submit_attempt_id,
                    side=feasibility.side,
                    status=existing_status,
                    reason_code=RISKY_ORDER_BLOCK,
                    signal_ts=int(ts),
                    decision_ts=int(ts),
                    decision_id=str(submit_attempt_id),
                    reason=reason,
                )
            )
            conn.commit()
            return None

    claimed, existing_intent = claim_order_intent_dedup(
        conn,
        intent_key=intent_key,
        client_order_id=client_order_id,
        symbol=settings.PAIR,
        side=feasibility.side,
        strategy_context=strategy_context,
        intent_type=intent_type,
        intent_ts=int(ts),
        qty=feasibility.normalized_qty,
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
        order_status="PENDING_SUBMIT",
    )
    if not claimed:
        existing_client_order_id = (
            str(existing_intent["client_order_id"])
            if existing_intent is not None and existing_intent["client_order_id"] is not None
            else "-"
        )
        existing_status = (
            str(existing_intent["order_status"])
            if existing_intent is not None and existing_intent["order_status"] is not None
            else "UNKNOWN"
        )
        skip_reason = (
            f"duplicate intent already recorded "
            f"existing_client_order_id={existing_client_order_id} existing_status={existing_status}"
        )
        live_module.RUN_LOG.info(
            format_log_kv(
                "[ORDER_SKIP] duplicate order intent",
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                mode=settings.MODE,
                symbol=settings.PAIR,
                side=feasibility.side,
                qty=f"{float(feasibility.normalized_qty):.12f}",
                intent_ts=int(ts),
                intent_key=intent_key,
                reason=skip_reason,
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            )
        )
        notify(
            format_event(
                "order_intent_dedup_skip",
                symbol=settings.PAIR,
                side=feasibility.side,
                qty=float(feasibility.normalized_qty),
                intent_ts=int(ts),
                client_order_id=client_order_id,
                dedup_key=intent_key,
                skip_reason=skip_reason,
                existing_client_order_id=existing_client_order_id,
                existing_status=existing_status,
            )
        )
        conn.commit()
        return None

    live_module.RUN_LOG.info(
        format_log_kv(
            "[ORDER_DECISION] submit order intent",
            mode=settings.MODE,
            symbol=settings.PAIR,
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            signal=signal,
            side=feasibility.side,
            market_price=market_price,
            position_qty=float(intent.order_qty),
            order_qty=feasibility.order_qty,
            normalized_qty=feasibility.normalized_qty,
            submit_payload_qty=float(feasibility.normalized_qty),
            reference_price=reference_price,
            client_order_id=client_order_id,
            intent_ts=int(ts),
            intent_key=intent_key,
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
            normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
            open_exposure_qty=float(decision_observability["open_exposure_qty"]),
            dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
            submit_qty_source=feasibility.submit_qty_source,
            position_state_source=str(decision_observability["position_state_source"]),
            decision_reason_code=str(lot_sizing.decision_reason_code),
            budget_krw=float(getattr(lot_sizing, "budget_krw", 0.0)),
            requested_qty=float(lot_sizing.requested_qty),
            internal_lot_size=float(lot_sizing.internal_lot_size),
            effective_min_trade_qty=float(lot_sizing.effective_min_trade_qty),
            min_qty=float(lot_sizing.min_qty),
            qty_step=float(lot_sizing.qty_step),
            min_notional_krw=float(lot_sizing.min_notional_krw),
            intended_lot_count=int(lot_sizing.intended_lot_count),
            executable_lot_count=int(lot_sizing.executable_lot_count),
            internal_lot_is_exchange_inflated=1 if bool(getattr(lot_sizing, "internal_lot_is_exchange_inflated", False)) else 0,
            internal_lot_would_block_buy=1 if bool(getattr(lot_sizing, "internal_lot_would_block_buy", False)) else 0,
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            top_of_book=top_of_book_summary,
        )
    )

    sell_truth_source_fields = live_module._sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=feasibility.submit_qty_source,
    )
    submit_truth_source_fields: dict[str, object] = dict(sell_truth_source_fields)
    submit_observability_fields: dict[str, object]
    sell_observability: dict[str, object]
    if feasibility.side == "SELL":
        canonical_sell_submit = live_module._CanonicalSellSubmitObservability(
            submit_qty_source=feasibility.submit_qty_source,
            submit_lot_source=str(
                decision_observability.get("sell_submit_lot_source")
                or decision_observability.get("submit_lot_source")
                or live_module._CANONICAL_SELL_SUBMIT_LOT_SOURCE
            ),
            submit_lot_count=int(decision_observability.get("sell_submit_lot_count") or 0),
            normalized_qty=feasibility.normalized_qty,
            position_state_source=str(decision_observability["position_state_source"]),
            position_state_source_truth_source=sell_truth_source_fields["position_state_source_truth_source"],
            submit_qty_source_truth_source=sell_truth_source_fields["submit_qty_source_truth_source"],
            submit_lot_source_truth_source=sell_truth_source_fields["submit_lot_source_truth_source"],
        )
        observed_sell_telemetry = live_module._ObservedSellSubmitTelemetry(
            position_qty=float(intent.order_qty),
            submit_payload_qty=feasibility.normalized_qty,
            raw_total_asset_qty=position_state.raw_total_asset_qty,
            open_exposure_qty=float(position_state.open_exposure_qty),
            dust_tracking_qty=float(position_state.dust_tracking_qty),
            sell_qty_basis_qty=float(decision_observability.get("sell_qty_basis_qty") or position_state.open_exposure_qty),
            sell_qty_basis_source=str(feasibility.submit_qty_source or decision_observability.get("sell_qty_basis_source") or "-"),
            sell_qty_basis_qty_truth_source=sell_truth_source_fields["sell_qty_basis_qty_truth_source"],
            sell_qty_basis_source_truth_source=sell_truth_source_fields["sell_qty_basis_source_truth_source"],
            sell_qty_boundary_kind=str(decision_observability.get("sell_qty_boundary_kind") or "none"),
            sell_qty_boundary_kind_truth_source=sell_truth_source_fields["sell_qty_boundary_kind_truth_source"],
            sell_normalized_exposure_qty_truth_source=sell_truth_source_fields["sell_normalized_exposure_qty_truth_source"],
            sell_open_exposure_qty_truth_source=sell_truth_source_fields["sell_open_exposure_qty_truth_source"],
            sell_dust_tracking_qty_truth_source=sell_truth_source_fields["sell_dust_tracking_qty_truth_source"],
        )
        sell_observability = live_module._sell_submit_observability_fields(
            decision_observability=decision_observability,
            canonical_submit=canonical_sell_submit,
            observed_inputs=observed_sell_telemetry,
            sell_failure_category="none",
            sell_failure_detail="none",
        )
        submit_observability_fields = dict(sell_observability)
    else:
        submit_truth_source_fields = {
            key: sell_truth_source_fields[key]
            for key in (
                "entry_allowed_truth_source",
                "effective_flat_truth_source",
                "raw_qty_open_truth_source",
                "raw_total_asset_qty_truth_source",
                "position_qty_truth_source",
                "submit_payload_qty_truth_source",
                "normalized_exposure_active_truth_source",
                "normalized_exposure_qty_truth_source",
                "open_exposure_qty_truth_source",
                "dust_tracking_qty_truth_source",
                "submit_qty_source_truth_source",
                "position_state_source_truth_source",
            )
        }
        submit_observability_fields = {
            "observed_position_qty": float(intent.order_qty),
            "submit_payload_qty": float(feasibility.normalized_qty),
            "submit_qty_source": feasibility.submit_qty_source,
            "position_state_source": str(decision_observability["position_state_source"]),
            "raw_total_asset_qty": float(position_state.raw_total_asset_qty),
            "open_exposure_qty": float(position_state.open_exposure_qty),
            "dust_tracking_qty": float(position_state.dust_tracking_qty),
        }
        sell_observability = {}

    payload_hash = payload_fingerprint(
        {
            "client_order_id": client_order_id,
            "submit_attempt_id": submit_attempt_id,
            "symbol": settings.PAIR,
            "side": feasibility.side,
            "qty": float(feasibility.normalized_qty),
            "price": reference_price,
            "submit_ts": int(ts),
        }
    )
    request = StandardSubmitPipelineRequest(
        conn=conn,
        submit_plan=None,
        signal=signal,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        side=feasibility.side,
        order_qty=float(feasibility.order_qty),
        position_qty=float(intent.order_qty),
        qty=float(feasibility.normalized_qty),
        ts=int(ts),
        intent_key=intent_key,
        market_price=float(market_price),
        raw_total_asset_qty=float(position_state.raw_total_asset_qty),
        open_exposure_qty=float(position_state.open_exposure_qty),
        dust_tracking_qty=float(position_state.dust_tracking_qty),
        effective_rules=position_state.effective_rules,
        submit_qty_source=feasibility.submit_qty_source,
        position_state_source=str(decision_observability["position_state_source"]),
        reference_price=reference_price,
        top_of_book_summary=top_of_book_summary,
        strategy_name=(strategy_name or settings.STRATEGY_NAME),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        order_type=("price" if feasibility.side == "BUY" else "market"),
        contract_profile=LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
        payload_hash=payload_hash,
        internal_lot_size=float(lot_sizing.internal_lot_size),
        effective_min_trade_qty=float(lot_sizing.effective_min_trade_qty),
        qty_step=float(lot_sizing.qty_step),
        min_notional_krw=float(lot_sizing.min_notional_krw),
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
        final_intended_qty=float(feasibility.order_qty),
        final_submitted_qty=float(feasibility.normalized_qty),
        decision_reason_code=str(lot_sizing.decision_reason_code),
        submit_truth_source_fields=submit_truth_source_fields,
        submit_observability_fields=submit_observability_fields,
        sell_observability=sell_observability,
    )
    try:
        submit_plan = build_live_submit_plan(
            broker=broker,
            client_order_id=client_order_id,
            side=feasibility.side,
            qty=float(feasibility.normalized_qty),
            ts=int(ts),
            effective_rules=position_state.effective_rules,
            reference_price=reference_price,
        )
    except Exception as error:
        record_standard_submit_planning_failure(request=request, error=error)
        return None
    order = run_standard_submit_pipeline(
        broker=broker,
        request=replace(request, submit_plan=submit_plan),
    )
    if order is None:
        return None

    fills = broker.get_fills(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
    try:
        fills_to_apply = live_module._aggregate_fills_for_apply(
            fills=fills,
            client_order_id=client_order_id,
            exchange_order_id=order.exchange_order_id,
            side=feasibility.side,
            context="_submit_via_standard_path",
        )
    except (live_module.FillFeeStrictModeError, LiveFillFeeValidationError) as exc:
        from_status = str(order.status or "NEW")
        live_module._mark_recovery_required(
            conn=conn,
            client_order_id=client_order_id,
            side=feasibility.side,
            from_status=from_status,
            reason=str(exc),
        )
        update_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            order_status="RECOVERY_REQUIRED",
        )
        conn.commit()
        live_module.RUN_LOG.error(
            format_log_kv(
                "[FILL_AGG] strict mode blocked aggregate; transitioned to recovery required",
                client_order_id=client_order_id,
                exchange_order_id=order.exchange_order_id or live_module.UNSET_EVENT_FIELD,
                side=feasibility.side,
                from_status=from_status,
                reason=str(exc),
            )
        )
        return None

    trade = None
    for fill in fills_to_apply:
        trade = apply_fill_and_trade(
            conn,
            client_order_id=client_order_id,
            side=feasibility.side,
            fill_id=fill.fill_id,
            fill_ts=fill.fill_ts,
            price=fill.price,
            qty=fill.qty,
            fee=fill.fee,
            strategy_name=(strategy_name or settings.STRATEGY_NAME),
            entry_decision_id=(decision_id if feasibility.side == "BUY" else None),
            exit_decision_id=(decision_id if feasibility.side == "SELL" else None),
            exit_reason=(decision_reason if feasibility.side == "SELL" else None),
            exit_rule_name=(exit_rule_name if feasibility.side == "SELL" else None),
            note=f"live exchange_order_id={order.exchange_order_id}",
            signal_ts=int(ts),
        ) or trade

    refreshed = broker.get_order(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
    set_status(client_order_id, refreshed.status, conn=conn)
    update_order_intent_dedup(
        conn,
        intent_key=intent_key,
        client_order_id=client_order_id,
        order_status=refreshed.status,
    )
    conn.commit()
    return trade
