from __future__ import annotations

import math


def record_sell_dust_unsellable(
    live_module,
    *,
    conn,
    state,
    ts: int,
    market_price: float,
    canonical_sell,
    diagnostic_qty,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    dust_details: dict[str, float | int | str] | None = None,
    decision_observability: dict[str, object],
    allow_decision_suppression: bool = True,
) -> bool:
    canonical_submit_lot_source = live_module._require_canonical_sell_submit_lot_source(
        submit_qty_source=canonical_sell.submit_qty_source,
        context="sell dust suppression",
    )
    position_qty = float(diagnostic_qty.observed_position_qty)
    dust_details = dust_details or live_module._build_sell_dust_unsellable_details(
        qty=position_qty,
        market_price=market_price,
    )
    if dust_details is None:
        return False
    dust_details = live_module._normalize_sell_dust_details(details=dust_details, market_price=market_price)

    exit_non_executable_reason = str(
        canonical_sell.exit_block_reason
        or decision_observability.get("exit_non_executable_reason")
        or decision_observability.get("exit_block_reason")
        or (
            decision_observability.get("position_state", {})
            if isinstance(decision_observability.get("position_state"), dict)
            else {}
        ).get("normalized_exposure", {})
        if isinstance(
            (
                decision_observability.get("position_state", {})
                if isinstance(decision_observability.get("position_state"), dict)
                else {}
            ).get("normalized_exposure", {}),
            dict,
        )
        else ""
    ).strip()
    suppress_as_decision = allow_decision_suppression and exit_non_executable_reason in {
        "dust_only_remainder",
        "no_executable_exit_lot",
    }
    resolved_exit_block_reason = str(
        exit_non_executable_reason
        or canonical_sell.exit_block_reason
        or decision_observability.get("exit_block_reason")
        or "-"
    ).strip() or "-"
    terminal_state = str(decision_observability.get("terminal_state") or "-")
    if terminal_state == "dust_only":
        resolved_exit_block_reason = "dust_only_remainder"
    elif terminal_state == "flat":
        resolved_exit_block_reason = "no_position"
    elif resolved_exit_block_reason in {"legacy_lot_metadata_missing", "no_executable_exit_lot"}:
        resolved_exit_block_reason = (
            "dust_only_remainder"
            if position_qty > live_module.POSITION_EPSILON
            else "no_position"
        )
    reason_code = (
        live_module.DUST_RESIDUAL_SUPPRESSED
        if suppress_as_decision
        else live_module.DUST_RESIDUAL_UNSELLABLE
    )
    sell_failure_category = live_module._classify_sell_failure_category(
        reason_code=reason_code,
        dust_details=dust_details,
    )
    sell_failure_detail = live_module._sell_failure_detail_from_observability(
        sell_failure_category=sell_failure_category,
        dust_details=dust_details,
    )
    truth_sources = live_module._decision_truth_sources_payload(decision_observability)
    guard_action = "block_sell_position_dust"
    if dust_details.get("dust_scope") == "remainder_after_sell":
        guard_action = "block_sell_remainder_dust"
    dust_message = (
        f"{'decision_suppressed:exit_suppressed_by_quantity_rule;' if suppress_as_decision else ''}"
        f"exit_non_executable_reason={exit_non_executable_reason or 'none'};"
        f"category={sell_failure_category};detail={sell_failure_detail};"
        f"state={dust_details['state']};"
        f"terminal_state={terminal_state};"
        f"exit_block_reason={resolved_exit_block_reason};"
        f"operator_action={dust_details['operator_action']};"
        f"guard_action={guard_action};"
        f"position_qty={float(position_qty):.12f};"
        f"normalized_qty={float(dust_details['normalized_qty']):.12f};"
        f"min_qty={float(dust_details['min_qty']):.12f};"
        f"sell_notional_krw={float(dust_details['sell_notional_krw']):.2f};"
        f"min_notional_krw={float(dust_details['min_notional_krw']):.2f};"
        f"dust_scope={dust_details.get('dust_scope') or 'position_qty'}"
    )
    strategy_name_value = strategy_name or live_module.settings.STRATEGY_NAME
    strategy_context = (
        f"{live_module.settings.MODE}:{strategy_name_value}:{live_module.settings.INTERVAL}"
    )
    suppression_key = live_module.build_order_suppression_key(
        mode=live_module.settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        dust_signature=str(dust_details["dust_signature"]),
        requested_qty=float(position_qty),
        normalized_qty=float(dust_details["normalized_qty"]),
        market_price=float(market_price),
    )
    sell_truth_source_fields = live_module._sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=live_module._CANONICAL_SELL_SUBMIT_QTY_SOURCE,
    )
    resolved_sell_submit_qty_source = live_module._CANONICAL_SELL_SUBMIT_QTY_SOURCE
    sell_qty_basis_qty = live_module._resolve_non_authoritative_sell_basis_qty(
        decision_observability=decision_observability,
        open_exposure_qty=diagnostic_qty.open_exposure_qty,
    )
    sell_qty_basis_source = str(
        decision_observability.get("sell_qty_basis_source")
        or live_module._CANONICAL_SELL_SUBMIT_LOT_SOURCE
        or canonical_submit_lot_source.value
        or "-"
    )
    sell_qty_boundary_kind = live_module._sell_qty_boundary_kind_from_dust_details(
        dust_details=dust_details
    )
    suppression_context = {
        "signal": "SELL",
        "side": "SELL",
        "market_price": float(market_price),
        "reason_code": reason_code,
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
        "sell_submit_qty_source": resolved_sell_submit_qty_source,
        "observed_sell_qty_basis_qty": float(sell_qty_basis_qty),
        "sell_qty_basis_source": sell_qty_basis_source,
        "sell_qty_boundary_kind": sell_qty_boundary_kind,
        "terminal_state": terminal_state,
        "exit_block_reason": resolved_exit_block_reason,
        "sell_normalized_exposure_qty": 0.0,
        "raw_total_asset_qty": float(diagnostic_qty.raw_total_asset_qty),
        "sell_open_exposure_qty": float(diagnostic_qty.open_exposure_qty),
        "sell_dust_tracking_qty": float(diagnostic_qty.dust_tracking_qty),
        "observed_position_qty": float(position_qty),
        "submit_payload_qty": 0.0,
        "normalized_qty": float(dust_details["normalized_qty"]),
        "effective_min_trade_qty": float(
            decision_observability.get("effective_min_trade_qty") or 0.0
        ),
        "exit_non_executable_reason": str(
            decision_observability.get("exit_non_executable_reason")
            or dust_details.get("dust_scope")
            or "no_executable_exit_lot"
        ),
        "min_qty": float(dust_details["min_qty"]),
        "sell_notional_krw": float(dust_details["sell_notional_krw"]),
        "min_notional_krw": float(dust_details["min_notional_krw"]),
        "decision_truth_sources": truth_sources,
        **{f"{key}_truth_source": value for key, value in truth_sources.items()},
        **sell_truth_source_fields,
        "qty_below_min": int(dust_details["qty_below_min"]),
        "normalized_non_positive": int(dust_details["normalized_non_positive"]),
        "normalized_below_min": int(dust_details["normalized_below_min"]),
        "notional_below_min": int(dust_details["notional_below_min"]),
        "qty_step": float(dust_details["qty_step"]),
        "max_qty_decimals": int(dust_details["max_qty_decimals"]),
        "dust_scope": str(dust_details.get("dust_scope") or "position_qty"),
        "requested_qty": float(
            dust_details.get("requested_qty", dust_details["position_qty"])
        ),
        "remainder_qty": float(dust_details.get("remainder_qty", 0.0)),
        "remainder_notional_krw": float(
            dust_details.get("remainder_notional_krw", 0.0)
        ),
        "broker_full_qty": float(
            dust_details.get("broker_full_qty", dust_details["position_qty"])
        ),
        "broker_full_remainder_qty": float(
            dust_details.get("broker_full_remainder_qty", 0.0)
        ),
        "broker_full_remainder_notional_krw": float(
            dust_details.get("broker_full_remainder_notional_krw", 0.0)
        ),
        "broker_volume_decimals": int(
            dust_details.get(
                "broker_volume_decimals",
                live_module.BROKER_MARKET_SELL_QTY_DECIMALS,
            )
        ),
        "suppression_key": suppression_key,
        "summary": dust_message,
    }
    live_module.record_order_suppression(
        conn=conn,
        suppression_key=suppression_key,
        event_kind="decision_suppressed",
        mode=live_module.settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        reason="decision_suppressed:exit_suppressed_by_quantity_rule",
        requested_qty=float(position_qty),
        normalized_qty=float(dust_details["normalized_qty"]),
        market_price=float(market_price),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        dust_present=True,
        dust_allow_resume=False,
        dust_effective_flat=False,
        dust_state=str(dust_details["state"]),
        dust_action=str(dust_details["operator_action"]),
        dust_signature=str(dust_details["dust_signature"]),
        qty_below_min=bool(dust_details["qty_below_min"]),
        normalized_non_positive=bool(dust_details["normalized_non_positive"]),
        normalized_below_min=bool(dust_details["normalized_below_min"]),
        notional_below_min=bool(dust_details["notional_below_min"]),
        summary=dust_message,
        context=suppression_context,
    )
    live_module.RUN_LOG.info(
        live_module.format_log_kv(
            "[ORDER_SKIP] exit quantity suppressed"
            if suppress_as_decision
            else "[ORDER_SKIP] dust residual unsellable",
            side="SELL",
            signal="SELL",
            reason_code=reason_code,
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(decision_id) if decision_id is not None else "-",
            sell_failure_category=sell_failure_category,
            sell_failure_detail=sell_failure_detail,
            state=dust_details["state"],
            operator_action=dust_details["operator_action"],
            position_qty=position_qty,
            sell_qty_basis_qty=sell_qty_basis_qty,
            sell_qty_basis_source=sell_qty_basis_source,
            sell_qty_boundary_kind=sell_qty_boundary_kind,
            normalized_qty=dust_details["normalized_qty"],
            min_qty=dust_details["min_qty"],
            sell_notional_krw=dust_details["sell_notional_krw"],
            min_notional_krw=dust_details["min_notional_krw"],
        )
    )
    live_module.notify(
        live_module.safety_event(
            "decision_suppressed",
            client_order_id=live_module.UNSET_EVENT_FIELD,
            submit_attempt_id=live_module.UNSET_EVENT_FIELD,
            exchange_order_id=live_module.UNSET_EVENT_FIELD,
            reason_code=reason_code,
            side="SELL",
            status="SUPPRESSED",
            dust_state=str(dust_details["notify_dust_state"]),
            dust_action=str(dust_details["notify_dust_action"]),
            dust_new_orders_allowed="1" if bool(dust_details["new_orders_allowed"]) else "0",
            dust_resume_allowed="1" if bool(dust_details["resume_allowed"]) else "0",
            dust_treat_as_flat="1" if bool(dust_details["treat_as_flat"]) else "0",
            dust_event_state=str(dust_details["state"]),
            operator_action=str(dust_details["operator_action"]),
            dust_qty_below_min=str(dust_details["qty_below_min"]),
            dust_notional_below_min=str(dust_details["notional_below_min"]),
            reason=dust_message,
        )
    )
    return True


def record_sell_no_executable_exit_suppression(
    live_module,
    *,
    conn,
    state,
    ts: int,
    market_price: float,
    canonical_sell,
    diagnostic_qty,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    decision_observability: dict[str, object],
    exit_sizing: object | None = None,
) -> bool:
    nested_exit_block_reason = ""
    position_state = decision_observability.get("position_state")
    if isinstance(position_state, dict):
        normalized_exposure = position_state.get("normalized_exposure")
        if isinstance(normalized_exposure, dict):
            nested_exit_block_reason = str(
                normalized_exposure.get("exit_block_reason") or ""
            ).strip()
    exit_non_executable_reason = str(
        getattr(exit_sizing, "block_reason", "")
        if exit_sizing is not None
        else decision_observability.get("exit_non_executable_reason")
        or decision_observability.get("exit_block_reason")
        or nested_exit_block_reason
        or ""
    ).strip()
    if exit_non_executable_reason not in {"no_executable_exit_lot", "dust_only_remainder"}:
        return False

    strategy_name_value = strategy_name or live_module.settings.STRATEGY_NAME
    strategy_context = (
        f"{live_module.settings.MODE}:{strategy_name_value}:{live_module.settings.INTERVAL}"
    )
    reason_code = live_module.DUST_RESIDUAL_SUPPRESSED
    suppression_outcome = "execution_suppressed"
    sell_failure_category = live_module._classify_sell_failure_category(
        reason_code=reason_code
    )
    sell_failure_detail = live_module._sell_failure_detail_from_observability(
        sell_failure_category=sell_failure_category
    )
    truth_sources = live_module._decision_truth_sources_payload(decision_observability)
    sell_truth_source_fields = live_module._sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=canonical_sell.submit_qty_source,
    )
    exit_sizing_allowed = (
        bool(getattr(exit_sizing, "allowed", False))
        if exit_sizing is not None
        else False
    )
    exit_sizing_block_reason = str(
        getattr(exit_sizing, "block_reason", exit_non_executable_reason)
        if exit_sizing is not None
        else exit_non_executable_reason
    )
    exit_sizing_decision_reason_code = str(
        getattr(
            exit_sizing,
            "decision_reason_code",
            "exit_suppressed_by_quantity_rule",
        )
        if exit_sizing is not None
        else "exit_suppressed_by_quantity_rule"
    )
    intended_lot_count = int(
        getattr(exit_sizing, "intended_lot_count", 0) if exit_sizing is not None else 0
    )
    executable_lot_count = int(
        getattr(exit_sizing, "executable_lot_count", 0)
        if exit_sizing is not None
        else 0
    )
    executable_qty = float(
        getattr(exit_sizing, "executable_qty", 0.0) if exit_sizing is not None else 0.0
    )
    internal_lot_size = float(
        getattr(exit_sizing, "internal_lot_size", 0.0)
        if exit_sizing is not None
        else 0.0
    )
    effective_min_trade_qty = float(
        getattr(exit_sizing, "effective_min_trade_qty", 0.0)
        if exit_sizing is not None
        else 0.0
    )
    min_qty = float(
        getattr(exit_sizing, "min_qty", 0.0) if exit_sizing is not None else 0.0
    )
    min_notional_krw = float(
        getattr(exit_sizing, "min_notional_krw", 0.0)
        if exit_sizing is not None
        else 0.0
    )
    requested_qty = float(canonical_sell.sellable_executable_qty)
    normalized_qty = 0.0
    suppression_key = live_module.build_order_suppression_key(
        mode=live_module.settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        dust_signature=exit_non_executable_reason,
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=float(market_price),
    )
    suppression_context = {
        "signal": "SELL",
        "side": "SELL",
        "market_price": float(market_price),
        "reason_code": reason_code,
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
        "sell_submit_qty_source": live_module._CANONICAL_SELL_SUBMIT_QTY_SOURCE,
        "sell_submit_lot_source": live_module._CANONICAL_SELL_SUBMIT_LOT_SOURCE,
        "sell_submit_lot_count": executable_lot_count,
        "sell_submit_lot_source_truth_source": str(
            decision_observability.get("sell_submit_lot_source_truth_source") or "-"
        ),
        "sell_submit_lot_count_truth_source": str(
            decision_observability.get("sell_submit_lot_count_truth_source") or "-"
        ),
        "observed_sell_qty_basis_qty": live_module._resolve_non_authoritative_sell_basis_qty(
            decision_observability=decision_observability,
            open_exposure_qty=diagnostic_qty.open_exposure_qty,
        ),
        "sell_qty_basis_source": str(
            decision_observability.get("sell_qty_basis_source")
            or canonical_sell.submit_qty_source
            or live_module._CANONICAL_SELL_SUBMIT_QTY_SOURCE
        ),
        "submit_lot_source": live_module._CANONICAL_SELL_SUBMIT_LOT_SOURCE,
        "sell_qty_boundary_kind": str(
            decision_observability.get("sell_qty_boundary_kind") or "none"
        ),
        "exit_non_executable_reason": exit_non_executable_reason,
        "exit_sizing_allowed": exit_sizing_allowed,
        "exit_sizing_block_reason": exit_sizing_block_reason,
        "exit_sizing_decision_reason_code": exit_sizing_decision_reason_code,
        "internal_lot_size": internal_lot_size,
        "intended_lot_count": intended_lot_count,
        "executable_lot_count": executable_lot_count,
        "sell_normalized_exposure_qty": 0.0,
        "raw_total_asset_qty": float(diagnostic_qty.raw_total_asset_qty),
        "sell_open_exposure_qty": float(diagnostic_qty.open_exposure_qty),
        "sell_dust_tracking_qty": float(diagnostic_qty.dust_tracking_qty),
        "observed_position_qty": float(diagnostic_qty.observed_position_qty),
        "submit_payload_qty": 0.0,
        "normalized_qty": normalized_qty,
        "effective_min_trade_qty": effective_min_trade_qty,
        "min_qty": min_qty,
        "sell_notional_krw": requested_qty * float(market_price),
        "min_notional_krw": min_notional_krw,
        "decision_truth_sources": truth_sources,
        **{f"{key}_truth_source": value for key, value in truth_sources.items()},
        **sell_truth_source_fields,
        "strategy_name": strategy_name_value,
        "strategy_context": strategy_context,
        "decision_id": decision_id,
        "decision_reason": decision_reason,
        "exit_rule_name": exit_rule_name,
        "suppression_outcome": suppression_outcome,
        "suppression_reason_code": reason_code,
        "suppression_reason": "decision_suppressed:exit_suppressed_by_quantity_rule",
        "suppression_summary": (
            f"suppression_outcome={suppression_outcome};"
            "decision_suppressed:exit_suppressed_by_quantity_rule;"
            f"exit_non_executable_reason={exit_non_executable_reason};"
            f"sellable_executable_lot_count={executable_lot_count};"
            f"sellable_executable_qty={executable_qty:.12f}"
        ),
        "suppression_key": suppression_key,
    }
    live_module.record_order_suppression(
        conn=conn,
        suppression_key=suppression_key,
        event_kind="decision_suppressed",
        mode=live_module.settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        reason="decision_suppressed:exit_suppressed_by_quantity_rule",
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=float(market_price),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        dust_present=True,
        dust_allow_resume=False,
        dust_effective_flat=False,
        dust_state=str(
            decision_observability.get("dust_classification")
            or decision_observability.get("dust_state")
            or "-"
        ),
        dust_action="manual_review_before_resume",
        dust_signature=exit_non_executable_reason,
        summary=(
            f"suppression_outcome={suppression_outcome};"
            "decision_suppressed:exit_suppressed_by_quantity_rule;"
            f"exit_non_executable_reason={exit_non_executable_reason};"
            f"sellable_executable_qty={executable_qty:.12f}"
        ),
        context=suppression_context,
    )
    live_module.RUN_LOG.info(
        live_module.format_log_kv(
            "[ORDER_SKIP] exit quantity suppressed",
            side="SELL",
            signal="SELL",
            reason_code=reason_code,
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(decision_id) if decision_id is not None else "-",
            sell_failure_category=sell_failure_category,
            sell_failure_detail=sell_failure_detail,
            exit_non_executable_reason=exit_non_executable_reason,
            sellable_executable_lot_count=executable_lot_count,
            sellable_executable_qty=executable_qty,
        )
    )
    live_module.notify(
        live_module.safety_event(
            "decision_suppressed",
            client_order_id=live_module.UNSET_EVENT_FIELD,
            submit_attempt_id=live_module.UNSET_EVENT_FIELD,
            exchange_order_id=live_module.UNSET_EVENT_FIELD,
            reason_code=reason_code,
            side="SELL",
            status="SUPPRESSED",
            dust_state=str(
                decision_observability.get("dust_classification")
                or decision_observability.get("dust_state")
                or "-"
            ),
            dust_action="manual_review_before_resume",
            dust_new_orders_allowed="0",
            dust_resume_allowed="0",
            dust_treat_as_flat="0",
            dust_event_state=exit_non_executable_reason,
            operator_action="manual_review_before_resume",
            dust_qty_below_min="0",
            dust_notional_below_min="0",
            reason=(
                f"suppression_outcome={suppression_outcome};"
                "decision_suppressed:exit_suppressed_by_quantity_rule;"
                f"exit_non_executable_reason={exit_non_executable_reason}"
            ),
        )
    )
    return True


def record_harmless_dust_exit_suppression(
    live_module,
    *,
    conn,
    state,
    signal: str,
    side: str,
    requested_qty: float,
    market_price: float,
    normalized_qty: float,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    submit_qty_source: str | None = None,
    position_state_source: str | None = None,
    raw_total_asset_qty: float | None = None,
    open_exposure_qty: float | None = None,
    dust_tracking_qty: float | None = None,
) -> bool:
    if side != "SELL":
        return False

    decision_observability = live_module._load_strategy_decision_observability(
        conn=conn,
        decision_id=decision_id,
        fallback_signal=signal,
    )
    truth_sources = live_module._decision_truth_sources_payload(decision_observability)
    dust_context = live_module.build_dust_display_context(state.last_reconcile_metadata)
    dust = dust_context.classification
    dust_view = dust_context.operator_view
    harmless_dust_effective_flat = bool(
        dust.present
        and dust.classification == live_module.DustState.HARMLESS_DUST.value
        and dust_view.resume_allowed
        and dust_view.treat_as_flat
    )
    if not harmless_dust_effective_flat:
        return False

    requested_qty = float(requested_qty)
    normalized_qty = float(normalized_qty)
    market_price = float(market_price)
    boundary_sellable_qty = bool(
        live_module._sell_qty_is_min_qty_boundary_rounding_case(
            qty=requested_qty,
            min_qty=dust.min_qty,
        )
        and (
            dust.min_notional_krw <= 0
            or (requested_qty * market_price) >= dust.min_notional_krw
        )
    )
    if boundary_sellable_qty:
        return False
    qty_below_min = bool(dust.min_qty > 0 and requested_qty < dust.min_qty)
    normalized_non_positive = not math.isfinite(normalized_qty) or normalized_qty <= 0
    normalized_below_min = bool(
        dust.min_qty > 0 and normalized_qty > 0 and normalized_qty < dust.min_qty
    )
    notional_below_min = bool(
        dust.min_notional_krw > 0
        and normalized_qty > 0
        and (normalized_qty * market_price) < dust.min_notional_krw
    )
    suppression_scope = (
        "harmless_dust_below_min"
        if any(
            (
                qty_below_min,
                normalized_non_positive,
                normalized_below_min,
                notional_below_min,
            )
        )
        else "harmless_dust_effective_flat"
    )

    strategy_name_value = strategy_name or live_module.settings.STRATEGY_NAME
    strategy_context = (
        f"{live_module.settings.MODE}:{strategy_name_value}:{live_module.settings.INTERVAL}"
    )
    suppression_reason = "decision_suppressed:harmless_dust_exit"
    suppression_outcome = "execution_suppressed"
    sell_failure_category = live_module._classify_sell_failure_category(
        reason_code=live_module.DUST_RESIDUAL_SUPPRESSED
    )
    sell_failure_detail = live_module._sell_failure_detail_from_observability(
        sell_failure_category=sell_failure_category,
        dust_details={
            "dust_scope": suppression_scope,
            "normalized_non_positive": normalized_non_positive,
            "qty_below_min": qty_below_min,
            "normalized_below_min": normalized_below_min,
            "notional_below_min": notional_below_min,
        },
    )
    suppression_summary = (
        f"suppression_outcome={suppression_outcome};"
        f"{suppression_reason};{dust_context.compact_summary};"
        f"base_signal={decision_observability['base_signal']};final_signal={decision_observability['final_signal']};"
        f"entry_allowed={1 if bool(decision_observability['entry_allowed']) else 0};"
        f"effective_flat={1 if bool(decision_observability['effective_flat']) else 0};"
        f"normalized_exposure_active={1 if bool(decision_observability['normalized_exposure_active']) else 0};"
        f"normalized_exposure_qty={float(decision_observability['normalized_exposure_qty']):.12f};"
        f"raw_qty_open={float(decision_observability['raw_qty_open']):.12f};"
        f"effective_flat_due_to_harmless_dust={1 if dust_context.effective_flat_due_to_harmless_dust else 0};"
        f"entry_allowed_truth_source={decision_observability['entry_allowed_truth_source']};"
        f"effective_flat_truth_source={decision_observability['effective_flat_truth_source']};"
        f"suppression_scope={suppression_scope};"
        f"requested_qty={requested_qty:.12f};normalized_qty={normalized_qty:.12f};"
        f"market_price={market_price:.8f};qty_below_min={1 if qty_below_min else 0};"
        f"normalized_non_positive={1 if normalized_non_positive else 0};"
        f"normalized_below_min={1 if normalized_below_min else 0};"
        f"notional_below_min={1 if notional_below_min else 0}"
    )
    suppression_key = live_module.build_order_suppression_key(
        mode=live_module.settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal=signal,
        side=side,
        reason_code=live_module.DUST_RESIDUAL_SUPPRESSED,
        dust_signature=str(dust.summary),
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=market_price,
    )
    suppression_submit_qty_source = live_module._harmless_dust_suppression_submit_qty_source(
        submit_qty_source
    )
    suppression_truth_source_fields = live_module._sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=suppression_submit_qty_source,
    )
    suppression_sell_qty_basis_qty = live_module._resolve_non_authoritative_sell_basis_qty(
        decision_observability=decision_observability,
        open_exposure_qty=open_exposure_qty,
    )
    suppression_sell_qty_basis_source = str(
        decision_observability.get("sell_qty_basis_source")
        or live_module._CANONICAL_SELL_SUBMIT_LOT_SOURCE
        or suppression_submit_qty_source
        or submit_qty_source
        or "-"
    )
    suppression_sell_submit_lot_source = str(
        decision_observability.get("sell_submit_lot_source")
        or live_module._CANONICAL_SELL_SUBMIT_LOT_SOURCE
    )
    suppression_sell_submit_lot_count = int(
        decision_observability.get("sell_submit_lot_count")
        or decision_observability.get("sellable_executable_lot_count")
        or 0
    )
    suppression_sell_qty_boundary_kind = (
        "min_qty" if suppression_scope == "harmless_dust_below_min" else "dust_mismatch"
    )
    suppression_context = {
        **dust_context.fields,
        "signal": "SELL",
        "side": side,
        "requested_qty": requested_qty,
        "normalized_qty": normalized_qty,
        "market_price": market_price,
        "observed_position_qty": float(requested_qty),
        "operator_action": dust_view.operator_action,
        "dust_action": dust_view.operator_action,
        "submit_qty_source": suppression_submit_qty_source,
        "submit_payload_qty": 0.0,
        "sell_submit_qty_source": suppression_submit_qty_source,
        "sell_submit_lot_source": suppression_sell_submit_lot_source,
        "sell_submit_lot_count": suppression_sell_submit_lot_count,
        "observed_sell_qty_basis_qty": float(suppression_sell_qty_basis_qty),
        "sell_qty_basis_source": suppression_sell_qty_basis_source,
        "sell_qty_boundary_kind": suppression_sell_qty_boundary_kind,
        "sell_qty_basis_qty_truth_source": str(
            decision_observability.get("sell_qty_basis_qty_truth_source") or "-"
        ),
        "sell_qty_basis_source_truth_source": str(
            decision_observability.get("sell_qty_basis_source_truth_source") or "-"
        ),
        "sell_qty_boundary_kind_truth_source": str(
            decision_observability.get("sell_qty_boundary_kind_truth_source") or "-"
        ),
        "sell_normalized_exposure_qty": float(normalized_qty),
        "sell_open_exposure_qty": float(
            normalized_qty if open_exposure_qty is None else open_exposure_qty
        ),
        "sell_dust_tracking_qty": float(0.0 if dust_tracking_qty is None else dust_tracking_qty),
        "decision_truth_sources": truth_sources,
        **{f"{key}_truth_source": value for key, value in truth_sources.items()},
        **suppression_truth_source_fields,
        "sell_submit_lot_source_truth_source": str(
            decision_observability.get("sell_submit_lot_source_truth_source") or "-"
        ),
        "sell_submit_lot_count_truth_source": str(
            decision_observability.get("sell_submit_lot_count_truth_source") or "-"
        ),
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
        "base_signal": decision_observability["base_signal"],
        "final_signal": decision_observability["final_signal"],
        "entry_allowed": bool(decision_observability["entry_allowed"]),
        "effective_flat": bool(decision_observability["effective_flat"]),
        "raw_qty_open": float(decision_observability["raw_qty_open"]),
        "raw_total_asset_qty": float(
            raw_total_asset_qty or decision_observability["raw_total_asset_qty"]
        ),
        "open_exposure_qty": float(
            open_exposure_qty or decision_observability["open_exposure_qty"]
        ),
        "dust_tracking_qty": float(
            dust_tracking_qty or decision_observability["dust_tracking_qty"]
        ),
        "normalized_exposure_active": bool(
            decision_observability["normalized_exposure_active"]
        ),
        "normalized_exposure_qty": float(decision_observability["normalized_exposure_qty"]),
        "entry_allowed_truth_source": str(
            decision_observability["entry_allowed_truth_source"]
        ),
        "effective_flat_truth_source": str(
            decision_observability["effective_flat_truth_source"]
        ),
        "strategy_name": strategy_name_value,
        "strategy_context": strategy_context,
        "decision_id": decision_id,
        "decision_reason": decision_reason,
        "exit_rule_name": exit_rule_name,
        "suppression_outcome": suppression_outcome,
        "suppression_reason_code": live_module.DUST_RESIDUAL_SUPPRESSED,
        "suppression_reason": suppression_reason,
        "suppression_summary": suppression_summary,
        "suppression_key": suppression_key,
        "effective_flat_due_to_harmless_dust": bool(
            dust_context.effective_flat_due_to_harmless_dust
        ),
        "suppression_scope": suppression_scope,
        "decision_observability": decision_observability,
    }
    live_module.record_order_suppression(
        conn=conn,
        suppression_key=suppression_key,
        event_kind="decision_suppressed",
        mode=live_module.settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal=signal,
        side=side,
        reason_code=live_module.DUST_RESIDUAL_SUPPRESSED,
        reason=suppression_reason,
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=market_price,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        dust_present=bool(dust.present),
        dust_allow_resume=bool(dust.allow_resume),
        dust_effective_flat=bool(dust.effective_flat),
        dust_state=dust_view.state,
        dust_action=dust_view.operator_action,
        dust_signature=str(dust.summary),
        qty_below_min=qty_below_min,
        normalized_non_positive=normalized_non_positive,
        normalized_below_min=normalized_below_min,
        notional_below_min=notional_below_min,
        summary=suppression_summary,
        context=suppression_context,
    )
    live_module.RUN_LOG.info(
        live_module.format_log_kv(
            "[ORDER_SKIP] harmless dust exit suppressed",
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            signal=signal,
            side=side,
            reason_code=live_module.DUST_RESIDUAL_SUPPRESSED,
            sell_failure_category=sell_failure_category,
            sell_failure_detail=sell_failure_detail,
            state=dust_view.state,
            operator_action=dust_view.operator_action,
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1
            if bool(decision_observability["normalized_exposure_active"])
            else 0,
            normalized_exposure_qty=float(
                decision_observability["normalized_exposure_qty"]
            ),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            suppression_scope=suppression_scope,
            requested_qty=requested_qty,
            normalized_qty=normalized_qty,
            sell_qty_basis_qty=suppression_sell_qty_basis_qty,
            sell_qty_basis_source=suppression_sell_qty_basis_source,
            sell_qty_boundary_kind=suppression_sell_qty_boundary_kind,
            market_price=market_price,
            dust_signature=suppression_key,
        )
    )
    live_module.notify(
        live_module.safety_event(
            "decision_suppressed",
            client_order_id=live_module.UNSET_EVENT_FIELD,
            submit_attempt_id=live_module.UNSET_EVENT_FIELD,
            exchange_order_id=live_module.UNSET_EVENT_FIELD,
            reason_code=live_module.DUST_RESIDUAL_SUPPRESSED,
            side=side,
            status="SUPPRESSED",
            reason=suppression_summary,
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1
            if bool(decision_observability["normalized_exposure_active"])
            else 0,
            normalized_exposure_qty=float(
                decision_observability["normalized_exposure_qty"]
            ),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            dust_state=dust_view.state,
            dust_action=dust_view.operator_action,
            dust_residual_present=1 if dust.present else 0,
            dust_residual_allow_resume=1 if dust.allow_resume else 0,
            dust_effective_flat=1 if dust.effective_flat else 0,
            effective_flat_due_to_harmless_dust=1
            if dust_context.effective_flat_due_to_harmless_dust
            else 0,
            qty_below_min=1 if qty_below_min else 0,
            normalized_non_positive=1 if normalized_non_positive else 0,
            normalized_below_min=1 if normalized_below_min else 0,
            notional_below_min=1 if notional_below_min else 0,
            suppression_scope=suppression_scope,
            dust_signature=suppression_key,
        )
    )
    return True
