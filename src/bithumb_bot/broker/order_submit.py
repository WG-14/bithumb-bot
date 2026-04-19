from __future__ import annotations

import logging
from dataclasses import replace

from ..config import settings
from ..execution_planner import build_submit_plan, resolve_submit_price_tick_policy
from ..execution_models import (
    OrderConfirmation,
    OrderIntent,
    SignedOrderRequest,
    SubmitPlan,
)
from ..observability import format_log_kv
from .base import BrokerOrder, BrokerRejectError
from .live_order_contract import ORDER_SUBMIT_DISPATCH_AUTHORITY
from .order_payloads import build_order_payload_from_plan, normalize_order_side, validate_client_order_id
from .order_serialization import truncate_volume
from .bithumb_execution import execute_signed_order_request

RUN_LOG = logging.getLogger("bithumb_bot.run")


def fetch_orderbook_top(pair: str):
    from .bithumb import fetch_orderbook_top as bithumb_fetch_orderbook_top

    return bithumb_fetch_orderbook_top(pair)


def validated_best_quote_ask_price(quote, *, requested_market: str):
    from .bithumb import validated_best_quote_ask_price as bithumb_validated_best_quote_ask_price

    return bithumb_validated_best_quote_ask_price(quote, requested_market=requested_market)


class PlaceOrderSubmissionFlow:
    def __init__(
        self,
        *,
        intent: OrderIntent,
        plan: SubmitPlan,
        signed_request: SignedOrderRequest,
    ) -> None:
        self.intent = intent
        self.plan = plan
        self.signed_request = signed_request


def plan_place_order(
    broker,
    *,
    intent: OrderIntent,
    rules=None,
    skip_qty_revalidation: bool = False,
) -> SubmitPlan:
    volume_truncator = getattr(broker, "_truncate_volume", None)
    return build_submit_plan(
        intent=intent,
        rules=rules,
        fetch_order_rules=__import__("bithumb_bot.broker.order_rules", fromlist=["get_effective_order_rules"]).get_effective_order_rules,
        fetch_top_of_book=fetch_orderbook_top,
        resolve_best_ask=lambda quote, market: validated_best_quote_ask_price(quote, requested_market=market),
        truncate_volume=(
            (lambda qty: volume_truncator(float(qty)))
            if callable(volume_truncator)
            else (lambda qty: truncate_volume(float(qty)))
        ),
        skip_qty_revalidation=skip_qty_revalidation,
    )


def build_place_order_payload(broker, *, plan: SubmitPlan) -> SignedOrderRequest:
    planned_payload = build_order_payload_from_plan(
        plan=plan,
    )

    canonical_payload = type(broker._private_api)._query_string(planned_payload.payload)
    RUN_LOG.info(
        format_log_kv(
            "[ORDER_SUBMIT] validated payload",
            market=planned_payload.payload.get("market"),
            side=plan.intent.normalized_side,
            order_type=planned_payload.payload.get("order_type"),
            chance_validation_order_type=plan.chance_validation_order_type,
            supported_order_types=",".join(plan.chance_supported_order_types) or "-",
            submit_field=planned_payload.exchange_submit_field,
            volume=planned_payload.payload.get("volume"),
            price=planned_payload.payload.get("price"),
            client_order_id=plan.intent.client_order_id,
            requested_qty=float(plan.requested_qty),
            exchange_constrained_qty=float(plan.exchange_constrained_qty),
            lifecycle_executable_qty=float(plan.lifecycle_executable_qty),
            submitted_qty=float(plan.submitted_qty),
            rejected_qty_remainder=float(plan.rejected_qty_remainder),
            unused_budget_krw=float(plan.unused_budget_krw),
            submit_qty_authority=plan.submit_qty_authority,
            lifecycle_non_executable_reason=str(plan.lifecycle_non_executable_reason or "none"),
            internal_lot_qty=float(plan.internal_lot_qty),
            exchange_submit_qty=float(plan.exchange_submit_qty),
            exchange_submit_notional_krw=planned_payload.exchange_submit_notional_krw if planned_payload.exchange_submit_notional_krw is not None else "",
            dust_qty=float(plan.qty_split.dust_qty),
            lot_count=int(plan.qty_split.lot_count),
            lot_size=float(plan.lot_rules.lot_size),
            submit_price_tick_applies=1 if plan.submit_price_tick_policy.applies else 0,
            submit_price_tick_unit=float(plan.submit_price_tick_policy.price_unit),
            submit_price_tick_reason=plan.submit_price_tick_policy.reason,
            canonical_query_string=canonical_payload,
            payload_fields=",".join(planned_payload.payload.keys()),
        )
    )
    return SignedOrderRequest(
        intent=plan.intent,
        plan=plan,
        payload=planned_payload.payload,
        submit_contract_context=planned_payload.submit_contract_context,
        exchange_submit_field=planned_payload.exchange_submit_field,
        exchange_submit_notional_krw=planned_payload.exchange_submit_notional_krw,
        exchange_submit_qty=float(plan.exchange_submit_qty),
        internal_lot_qty=float(plan.internal_lot_qty),
        canonical_payload=canonical_payload,
        trace_id=plan.trace_id,
        plan_id=plan.plan_id,
        request_id=f"{plan.trace_id or plan.intent.client_order_id}:signed_request",
        phase_identity="signed_request",
        phase_result="signed",
    )


def execute_place_order(
    broker,
    *,
    plan: SubmitPlan,
    signed_request: SignedOrderRequest,
    now: int,
    retry_safe: bool = False,
) -> OrderConfirmation:
    return execute_signed_order_request(
        broker,
        plan=plan,
        signed_request=signed_request,
        now=now,
        retry_safe=retry_safe,
    )


def build_place_order_submission_flow(
    broker,
    *,
    plan: SubmitPlan | None = None,
    validated_client_order_id: str | None = None,
    side: str | None = None,
    qty: float | None = None,
    price: float | None = None,
    buy_price_none_submit_contract=None,
    now: int | None = None,
) -> PlaceOrderSubmissionFlow:
    if plan is None:
        if validated_client_order_id is None or side is None or qty is None or now is None:
            raise BrokerRejectError(
                "place order submission flow requires either explicit SubmitPlan or validated order fields"
            )
        normalized_client_order_id = validate_client_order_id(validated_client_order_id)
        normalized_side = normalize_order_side(side)
        order_side = "BUY" if normalized_side == "bid" else "SELL"
        plan = plan_place_order(
            broker,
            intent=OrderIntent(
                client_order_id=normalized_client_order_id,
                market=settings.PAIR,
                side=order_side,
                normalized_side=normalized_side,
                qty=float(qty),
                price=price,
                created_ts=int(now),
                submit_contract=buy_price_none_submit_contract,
                trace_id=normalized_client_order_id,
            ),
        )
    signed_request = replace(
        build_place_order_payload(broker, plan=plan),
        dispatch_authority=ORDER_SUBMIT_DISPATCH_AUTHORITY,
    )
    return PlaceOrderSubmissionFlow(
        intent=plan.intent,
        plan=plan,
        signed_request=signed_request,
    )


def run_place_order_submission_flow(
    broker,
    *,
    flow: PlaceOrderSubmissionFlow,
) -> BrokerOrder:
    confirmation = execute_place_order(
        broker,
        plan=flow.plan,
        signed_request=flow.signed_request,
        now=flow.intent.created_ts,
    )
    return BrokerOrder(
        confirmation.client_order_id,
        confirmation.exchange_order_id,
        confirmation.side,
        confirmation.status,
        confirmation.price,
        confirmation.qty,
        confirmation.filled_qty,
        confirmation.created_ts,
        confirmation.updated_ts,
        confirmation.raw,
        confirmation.submit_contract_context,
    )
