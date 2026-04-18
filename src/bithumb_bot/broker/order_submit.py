from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import ROUND_DOWN
from typing import Any

from ..config import settings
from ..lot_model import DUST_POSITION_EPS, build_market_lot_rules, lot_count_to_qty
from ..observability import format_log_kv
from .base import BrokerOrder, BrokerRejectError, BrokerTemporaryError
from .order_payloads import build_order_payload, normalize_order_side

RUN_LOG = logging.getLogger("bithumb_bot.run")


def fetch_orderbook_top(pair: str):
    from .bithumb import fetch_orderbook_top as bithumb_fetch_orderbook_top

    return bithumb_fetch_orderbook_top(pair)


def validated_best_quote_ask_price(quote, *, requested_market: str):
    from .bithumb import validated_best_quote_ask_price as bithumb_validated_best_quote_ask_price

    return bithumb_validated_best_quote_ask_price(quote, requested_market=requested_market)


@dataclass(frozen=True)
class SubmitPriceTickPolicy:
    applies: bool
    price_unit: float
    reason: str


@dataclass(frozen=True)
class PlaceOrderPlan:
    validated_client_order_id: str
    market: str
    normalized_side: str
    order_side: str
    requested_qty: float
    requested_price: float | None
    rules: Any
    buy_price_none_submit_contract: Any | None
    chance_validation_order_type: str
    chance_supported_order_types: tuple[str, ...]
    exchange_submit_field_hint: str
    submit_contract_context: dict[str, object]
    submit_price_tick_policy: SubmitPriceTickPolicy
    effective_market_price: float | None
    lot_rules: Any
    qty_split: Any
    internal_lot_qty: float
    exchange_submit_qty: float


@dataclass(frozen=True)
class PlaceOrderPayloadPlan:
    payload: dict[str, object]
    submit_contract_context: dict[str, object]
    exchange_submit_field: str
    exchange_submit_notional_krw: float | None
    exchange_submit_qty: float
    internal_lot_qty: float
    canonical_payload: str


@dataclass(frozen=True)
class PlaceOrderSubmissionFlow:
    plan: PlaceOrderPlan
    payload_plan: PlaceOrderPayloadPlan
    side: str
    price: float | None
    now: int


def resolve_submit_price_tick_policy(
    *,
    order_side: str,
    price: float | None,
    rules,
) -> SubmitPriceTickPolicy:
    from .order_rules import side_price_unit

    if price is None and str(order_side).upper() == "SELL":
        return SubmitPriceTickPolicy(
            applies=False,
            price_unit=0.0,
            reason="market_sell_price_tick_non_applicable",
        )
    if price is None:
        return SubmitPriceTickPolicy(
            applies=True,
            price_unit=float(side_price_unit(rules=rules, side=order_side)),
            reason="market_buy_notional_price_unit",
        )
    return SubmitPriceTickPolicy(
        applies=True,
        price_unit=float(side_price_unit(rules=rules, side=order_side)),
        reason="limit_price_unit",
    )


def plan_place_order(
    broker,
    *,
    validated_client_order_id: str,
    normalized_side: str,
    market: str,
    side: str,
    qty: float,
    price: float | None,
    buy_price_none_submit_contract=None,
) -> PlaceOrderPlan:
    from .order_rules import (
        BuyPriceNoneSubmitContract,
        get_effective_order_rules,
        serialize_buy_price_none_submit_contract,
        supported_order_types_for_chance_validation,
        validate_buy_price_none_order_chance_contract,
        validate_buy_price_none_submit_contract,
        validate_order_chance_support,
    )

    submit_contract_context: dict[str, object] = {}
    try:
        order_rules_resolution = get_effective_order_rules(market)
        rules = order_rules_resolution.rules
        order_side = "BUY" if normalized_side == "bid" else "SELL"
        buy_submit_contract = (
            buy_price_none_submit_contract
            if price is None and normalized_side == "bid"
            else None
        )
        if buy_submit_contract is not None and not isinstance(
            buy_submit_contract,
            BuyPriceNoneSubmitContract,
        ):
            raise BrokerRejectError(
                "BUY price=None submit contract invalid before broker dispatch"
            )
        if buy_submit_contract is None and price is None and normalized_side == "bid":
            raise BrokerRejectError(
                "BUY price=None submit contract missing before broker dispatch"
            )

        chance_validation_order_type = "limit"
        chance_supported_order_types: tuple[str, ...] = ()
        if buy_submit_contract is not None:
            chance_validation_order_type = buy_submit_contract.chance_validation_order_type
            chance_supported_order_types = buy_submit_contract.chance_supported_order_types
        elif price is None:
            chance_validation_order_type = "market"
            chance_supported_order_types = supported_order_types_for_chance_validation(
                side=order_side,
                rules=rules,
            )
        else:
            chance_supported_order_types = supported_order_types_for_chance_validation(
                side=order_side,
                rules=rules,
            )

        exchange_submit_field_hint = (
            buy_submit_contract.exchange_submit_field
            if buy_submit_contract is not None
            else "volume"
        )
        if buy_submit_contract is not None:
            submit_contract_context = serialize_buy_price_none_submit_contract(
                buy_submit_contract,
                market=market,
                order_side=order_side,
            )
        else:
            submit_contract_context = {
                "chance_validation_order_type": chance_validation_order_type,
                "chance_supported_order_types": list(chance_supported_order_types),
                "exchange_submit_field": exchange_submit_field_hint,
                "exchange_order_type": chance_validation_order_type,
                "exchange_submit_notional_krw": None,
                "exchange_submit_qty": None,
                "internal_executable_qty": None,
                "market": market,
                "order_side": order_side,
            }

        RUN_LOG.info(
            format_log_kv(
                "[ORDER_SUBMIT] chance contract",
                market=market,
                side=normalized_side,
                client_order_id=validated_client_order_id,
                chance_validation_order_type=chance_validation_order_type,
                supported_order_types=",".join(chance_supported_order_types) or "-",
                buy_price_none_allowed=(
                    "-"
                    if buy_submit_contract is None
                    else int(buy_submit_contract.resolution.allowed)
                ),
                buy_price_none_decision_basis=(
                    "-"
                    if buy_submit_contract is None
                    else buy_submit_contract.resolution.decision_basis
                ),
                buy_price_none_alias_used=(
                    "-"
                    if buy_submit_contract is None
                    else int(buy_submit_contract.resolution.alias_used)
                ),
                buy_price_none_block_reason=(
                    "-"
                    if buy_submit_contract is None
                    else (buy_submit_contract.resolution.block_reason or "-")
                ),
                submit_field=exchange_submit_field_hint,
            )
        )

        submit_price_tick_policy = resolve_submit_price_tick_policy(
            order_side=order_side,
            price=price,
            rules=rules,
        )
        if buy_submit_contract is not None:
            validate_buy_price_none_order_chance_contract(
                rules=rules,
                submit_contract=buy_submit_contract,
            )
            validate_buy_price_none_submit_contract(
                submit_contract=buy_submit_contract,
            )
        else:
            validate_order_chance_support(
                rules=rules,
                side=side,
                order_type=chance_validation_order_type,
            )

        if price is None and normalized_side == "ask":
            broker_precision_qty = broker._truncate_volume(float(qty))
            if abs(float(qty) - broker_precision_qty) > DUST_POSITION_EPS:
                raise BrokerRejectError(
                    "qty requires explicit lot normalization before submit: "
                    f"raw_qty={format(float(qty), 'f')} broker_precision_qty={format(broker_precision_qty, 'f')}"
                )

        effective_market_price: float | None = price
        if price is None and normalized_side == "bid":
            try:
                quote = fetch_orderbook_top(market)
                effective_market_price = validated_best_quote_ask_price(quote, requested_market=market)
            except Exception as exc:
                raise BrokerTemporaryError(
                    "market buy blocked: failed to load validated best ask "
                    f"market={market} client_order_id={validated_client_order_id} cause={type(exc).__name__}: {exc}"
                ) from exc

        lot_rules = build_market_lot_rules(
            market_id=market,
            market_price=effective_market_price,
            rules=rules,
            exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
            exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
            exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
            source_mode="exchange",
        )
        has_explicit_qty_controls = any(
            hasattr(rules, field_name)
            for field_name in ("min_qty", "qty_step", "max_qty_decimals")
        )
        qty_split = lot_rules.split_qty(float(qty))
        if has_explicit_qty_controls and qty_split.executable is False:
            raise BrokerRejectError(
                f"{normalized_side.lower()} qty suppressed by quantity rule: "
                f"reason={qty_split.non_executable_reason} raw_qty={format(qty_split.requested_qty, 'f')} "
                f"lot_size={format(lot_rules.lot_size, 'f')} dust_qty={format(qty_split.dust_qty, 'f')} "
                f"lot_count={qty_split.lot_count} client_order_id={validated_client_order_id}"
            )
        if has_explicit_qty_controls and qty_split.dust_qty > DUST_POSITION_EPS:
            raise BrokerRejectError(
                f"qty requires explicit lot normalization before submit: "
                f"raw_qty={format(qty_split.requested_qty, 'f')} lot_size={format(lot_rules.lot_size, 'f')} "
                f"lot_count={qty_split.lot_count} dust_qty={format(qty_split.dust_qty, 'f')}"
            )

        internal_lot_qty = (
            lot_count_to_qty(lot_count=qty_split.lot_count, lot_size=lot_rules.lot_size)
            if has_explicit_qty_controls
            else float(qty)
        )

        return PlaceOrderPlan(
            validated_client_order_id=validated_client_order_id,
            market=market,
            normalized_side=normalized_side,
            order_side=order_side,
            requested_qty=float(qty),
            requested_price=price,
            rules=rules,
            buy_price_none_submit_contract=buy_submit_contract,
            chance_validation_order_type=chance_validation_order_type,
            chance_supported_order_types=chance_supported_order_types,
            exchange_submit_field_hint=exchange_submit_field_hint,
            submit_contract_context=submit_contract_context,
            submit_price_tick_policy=submit_price_tick_policy,
            effective_market_price=effective_market_price,
            lot_rules=lot_rules,
            qty_split=qty_split,
            internal_lot_qty=float(internal_lot_qty),
            exchange_submit_qty=float(internal_lot_qty),
        )
    except BrokerRejectError as exc:
        exc_submit_contract_context = getattr(exc, "submit_contract_context", None)
        setattr(
            exc,
            "submit_contract_context",
            dict(exc_submit_contract_context)
            if isinstance(exc_submit_contract_context, dict)
            else dict(submit_contract_context),
        )
        raise


def build_place_order_payload(broker, *, plan: PlaceOrderPlan) -> PlaceOrderPayloadPlan:
    from .order_rules import (
        normalize_limit_price_for_side,
        serialize_buy_price_none_submit_contract,
        side_min_total_krw,
    )

    exchange_submit_notional_krw: float | None = None
    exchange_submit_field = "volume"
    if plan.requested_price is None:
        if plan.normalized_side == "bid":
            exchange_submit_field = plan.exchange_submit_field_hint
            exchange_submit_notional = broker._decimal_from_value(plan.effective_market_price) * broker._decimal_from_value(plan.internal_lot_qty)
            bid_price_unit = broker._decimal_from_value(plan.submit_price_tick_policy.price_unit)
            if bid_price_unit > 0:
                exchange_submit_notional = (exchange_submit_notional / bid_price_unit).to_integral_value(rounding=ROUND_DOWN) * bid_price_unit
            min_total = side_min_total_krw(rules=plan.rules, side=plan.order_side)
            if min_total > 0 and exchange_submit_notional < broker._decimal_from_value(min_total):
                raise BrokerRejectError(
                    "order notional below side minimum for market BUY: "
                    f"side={plan.order_side} notional={format(exchange_submit_notional, 'f')} min_total={min_total:.8f}"
                )
            exchange_submit_notional_krw = float(exchange_submit_notional)
            payload = build_order_payload(
                market=plan.market,
                side=plan.normalized_side,
                ord_type=plan.buy_price_none_submit_contract.exchange_order_type,
                price=broker._format_krw_amount(exchange_submit_notional),
                client_order_id=plan.validated_client_order_id,
            )
        else:
            payload = build_order_payload(
                market=plan.market,
                side=plan.normalized_side,
                ord_type="market",
                volume=broker._format_volume(plan.exchange_submit_qty),
                client_order_id=plan.validated_client_order_id,
            )
    else:
        requested_limit_price = broker._decimal_from_value(plan.requested_price)
        if requested_limit_price <= 0:
            raise BrokerRejectError(f"limit price must be > 0 (got {plan.requested_price})")

        price_unit = plan.submit_price_tick_policy.price_unit
        normalized_limit_price = broker._decimal_from_value(
            normalize_limit_price_for_side(
                price=float(requested_limit_price),
                side=plan.order_side,
                rules=plan.rules,
            )
        )
        if normalized_limit_price <= 0:
            raise BrokerRejectError(
                "limit price normalization produced non-positive executable price: "
                f"side={plan.order_side} requested={format(requested_limit_price, 'f')} "
                f"price_unit={price_unit:.8f} normalized={format(normalized_limit_price, 'f')}"
            )

        exchange_submit_notional = normalized_limit_price * broker._decimal_from_value(plan.internal_lot_qty)
        min_total = side_min_total_krw(rules=plan.rules, side=plan.order_side)
        if min_total > 0 and exchange_submit_notional < broker._decimal_from_value(min_total):
            raise BrokerRejectError(
                "order notional below side minimum for limit order: "
                f"side={plan.order_side} notional={format(exchange_submit_notional, 'f')} min_total={min_total:.8f}"
            )
        payload = build_order_payload(
            market=plan.market,
            side=plan.normalized_side,
            ord_type="limit",
            volume=broker._format_volume(plan.exchange_submit_qty),
            price=broker._format_krw_amount(normalized_limit_price),
            client_order_id=plan.validated_client_order_id,
        )

    if plan.buy_price_none_submit_contract is not None:
        executed_submit_contract = plan.buy_price_none_submit_contract.with_execution_fields(
            exchange_submit_notional_krw=exchange_submit_notional_krw,
            exchange_submit_qty=(
                float(plan.exchange_submit_qty)
                if exchange_submit_field == "volume"
                else None
            ),
            internal_executable_qty=float(plan.internal_lot_qty),
        )
        submit_contract_context = serialize_buy_price_none_submit_contract(
            executed_submit_contract,
            market=plan.market,
            order_side=plan.order_side,
        )
    else:
        submit_contract_context = dict(plan.submit_contract_context)
        submit_contract_context.update(
            {
                "exchange_submit_field": exchange_submit_field,
                "exchange_order_type": str(payload.get("order_type") or plan.chance_validation_order_type),
                "exchange_submit_notional_krw": exchange_submit_notional_krw,
                "exchange_submit_qty": float(plan.exchange_submit_qty) if exchange_submit_field == "volume" else None,
                "internal_executable_qty": float(plan.internal_lot_qty),
            }
        )

    canonical_payload = type(broker._private_api)._query_string(payload)
    RUN_LOG.info(
        format_log_kv(
            "[ORDER_SUBMIT] validated payload",
            market=payload.get("market"),
            side=plan.normalized_side,
            order_type=payload.get("order_type"),
            chance_validation_order_type=plan.chance_validation_order_type,
            supported_order_types=",".join(plan.chance_supported_order_types) or "-",
            submit_field=exchange_submit_field,
            volume=payload.get("volume"),
            price=payload.get("price"),
            client_order_id=plan.validated_client_order_id,
            requested_qty=float(plan.requested_qty),
            internal_lot_qty=float(plan.internal_lot_qty),
            exchange_submit_qty=float(plan.exchange_submit_qty),
            exchange_submit_notional_krw=exchange_submit_notional_krw if exchange_submit_notional_krw is not None else "",
            dust_qty=float(plan.qty_split.dust_qty),
            lot_count=int(plan.qty_split.lot_count),
            lot_size=float(plan.lot_rules.lot_size),
            submit_price_tick_applies=1 if plan.submit_price_tick_policy.applies else 0,
            submit_price_tick_unit=float(plan.submit_price_tick_policy.price_unit),
            submit_price_tick_reason=plan.submit_price_tick_policy.reason,
            canonical_query_string=canonical_payload,
            payload_fields=",".join(payload.keys()),
        )
    )
    return PlaceOrderPayloadPlan(
        payload=payload,
        submit_contract_context=submit_contract_context,
        exchange_submit_field=exchange_submit_field,
        exchange_submit_notional_krw=exchange_submit_notional_krw,
        exchange_submit_qty=float(plan.exchange_submit_qty),
        internal_lot_qty=float(plan.internal_lot_qty),
        canonical_payload=canonical_payload,
    )


def execute_place_order(
    broker,
    *,
    plan: PlaceOrderPlan,
    payload_plan: PlaceOrderPayloadPlan,
    side: str,
    price: float | None,
    now: int,
) -> BrokerOrder:
    data = broker._submit_validated_order_payload(
        payload_plan=payload_plan,
        retry_safe=False,
    )
    if not isinstance(data, dict):
        raise BrokerRejectError(f"unexpected /v2/orders payload type: {type(data).__name__}")
    response_row = data.get("data") if isinstance(data.get("data"), dict) else data
    resolved_client_order_id, resolved_exchange_order_id = broker._resolve_order_identifiers(
        response_row if isinstance(response_row, dict) else {},
        fallback_client_order_id=plan.validated_client_order_id,
        allow_coid_alias=True,
        context="/v2/orders submit response",
    )
    if not resolved_exchange_order_id:
        raise BrokerRejectError(f"missing order id from /v2/orders response: {data}")
    if resolved_client_order_id and resolved_client_order_id != plan.validated_client_order_id:
        raise BrokerRejectError(
            "order submit response client_order_id mismatch: "
            f"requested={plan.validated_client_order_id} response={resolved_client_order_id}"
        )
    raw = broker._raw_v2_order_fields(
        response_row if isinstance(response_row, dict) else {},
        fallback_client_order_id=plan.validated_client_order_id,
    )
    raw.setdefault("market", payload_plan.payload.get("market"))
    raw.setdefault("order_type", payload_plan.payload.get("order_type"))
    raw.setdefault("ord_type", payload_plan.payload.get("order_type"))
    return BrokerOrder(
        plan.validated_client_order_id,
        resolved_exchange_order_id,
        side,
        "NEW",
        price,
        float(payload_plan.internal_lot_qty),
        0.0,
        now,
        now,
        raw,
        dict(payload_plan.submit_contract_context),
    )


def build_place_order_submission_flow(
    broker,
    *,
    validated_client_order_id: str,
    side: str,
    qty: float,
    price: float | None,
    buy_price_none_submit_contract,
    now: int,
) -> PlaceOrderSubmissionFlow:
    normalized_side = normalize_order_side(side)
    plan = plan_place_order(
        broker,
        validated_client_order_id=validated_client_order_id,
        normalized_side=normalized_side,
        market=broker._market(),
        side=side,
        qty=float(qty),
        price=price,
        buy_price_none_submit_contract=buy_price_none_submit_contract,
    )
    payload_plan = build_place_order_payload(broker, plan=plan)
    return PlaceOrderSubmissionFlow(
        plan=plan,
        payload_plan=payload_plan,
        side=side,
        price=price,
        now=now,
    )


def run_place_order_submission_flow(
    broker,
    *,
    flow: PlaceOrderSubmissionFlow,
) -> BrokerOrder:
    return execute_place_order(
        broker,
        plan=flow.plan,
        payload_plan=flow.payload_plan,
        side=flow.side,
        price=flow.price,
        now=flow.now,
    )
