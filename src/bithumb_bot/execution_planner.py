from __future__ import annotations

import math
from collections.abc import Callable

from .config import settings
from .execution_models import OrderIntent, SubmitPlan, SubmitPriceTickPolicy
from .lot_model import DUST_POSITION_EPS, build_market_lot_rules, lot_count_to_qty
from .broker.base import BrokerRejectError, BrokerTemporaryError


def resolve_submit_price_tick_policy(
    *,
    order_side: str,
    price: float | None,
    rules,
) -> SubmitPriceTickPolicy:
    from .broker.order_rules import side_price_unit

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


def build_submit_plan(
    *,
    intent: OrderIntent,
    rules=None,
    fetch_order_rules: Callable[[str], object],
    fetch_top_of_book: Callable[[str], object],
    resolve_best_ask: Callable[[object, str], float],
    truncate_volume: Callable[[float], float],
    skip_qty_revalidation: bool = False,
) -> SubmitPlan:
    from .broker.order_rules import (
        BuyPriceNoneSubmitContract,
        normalize_limit_price_for_side,
        resolve_buy_price_none_resolution,
        serialize_buy_price_none_submit_contract,
        side_min_total_krw,
        supported_order_types_for_chance_validation,
        validate_buy_price_none_order_chance_contract,
        validate_buy_price_none_submit_contract,
        validate_order_chance_support,
    )

    submit_contract_context: dict[str, object] = {}
    trace_id = str(intent.trace_id or intent.client_order_id)
    try:
        resolved_rules = rules
        if resolved_rules is None:
            order_rules_resolution = fetch_order_rules(intent.market)
            resolved_rules = order_rules_resolution.rules
        order_side = intent.order_side
        buy_submit_contract = None
        provided_submit_contract = intent.submit_contract
        if intent.price is None and intent.normalized_side == "bid":
            if provided_submit_contract is None:
                raise BrokerRejectError("BUY price=None submit contract missing before broker dispatch")
            if not isinstance(provided_submit_contract, BuyPriceNoneSubmitContract):
                raise BrokerRejectError("BUY price=None submit contract invalid before broker dispatch")
            buy_submit_contract = provided_submit_contract

        chance_validation_order_type = "limit"
        chance_supported_order_types: tuple[str, ...] = ()
        if buy_submit_contract is not None:
            chance_validation_order_type = buy_submit_contract.chance_validation_order_type
            chance_supported_order_types = buy_submit_contract.chance_supported_order_types
        elif intent.price is None:
            chance_validation_order_type = "market"
            chance_supported_order_types = supported_order_types_for_chance_validation(
                side=order_side,
                rules=resolved_rules,
            )
        else:
            chance_supported_order_types = supported_order_types_for_chance_validation(
                side=order_side,
                rules=resolved_rules,
            )

        exchange_submit_field = (
            buy_submit_contract.exchange_submit_field
            if buy_submit_contract is not None
            else "volume"
        )
        exchange_order_type = (
            buy_submit_contract.exchange_order_type
            if buy_submit_contract is not None
            else chance_validation_order_type
        )
        if buy_submit_contract is not None:
            submit_contract_context = serialize_buy_price_none_submit_contract(
                buy_submit_contract,
                market=intent.market,
                order_side=order_side,
            )
            submit_contract_context["submit_contract_kind"] = "market_buy_notional"
        else:
            submit_contract_context = {
                "submit_contract_kind": (
                    "market_qty"
                    if chance_validation_order_type == "market"
                    else "limit_qty_price"
                ),
                "chance_validation_order_type": chance_validation_order_type,
                "chance_supported_order_types": list(chance_supported_order_types),
                "exchange_submit_field": exchange_submit_field,
                "exchange_order_type": exchange_order_type,
                "exchange_submit_notional_krw": None,
                "exchange_submit_qty": None,
                "internal_executable_qty": None,
                "market": intent.market,
                "order_side": order_side,
            }

        submit_price_tick_policy = resolve_submit_price_tick_policy(
            order_side=order_side,
            price=intent.price,
            rules=resolved_rules,
        )
        if buy_submit_contract is not None:
            current_buy_price_none_resolution = resolve_buy_price_none_resolution(
                rules=resolved_rules,
            )
            validate_order_chance_support(
                rules=resolved_rules,
                side="BUY",
                order_type=buy_submit_contract.chance_validation_order_type,
                buy_price_none_resolution=current_buy_price_none_resolution,
            )
            validate_buy_price_none_order_chance_contract(
                rules=resolved_rules,
                submit_contract=buy_submit_contract,
            )
            validate_buy_price_none_submit_contract(
                submit_contract=buy_submit_contract,
            )
        else:
            validate_order_chance_support(
                rules=resolved_rules,
                side=intent.side,
                order_type=chance_validation_order_type,
            )

        if intent.price is None and intent.normalized_side == "ask":
            broker_precision_qty = truncate_volume(float(intent.qty))
            if abs(float(intent.qty) - broker_precision_qty) > DUST_POSITION_EPS:
                raise BrokerRejectError(
                    "qty requires explicit lot normalization before submit: "
                    f"raw_qty={format(float(intent.qty), 'f')} broker_precision_qty={format(broker_precision_qty, 'f')}"
                )

        effective_market_price: float | None = intent.price
        if intent.price is None and intent.normalized_side == "bid":
            if intent.market_price_hint is not None:
                effective_market_price = float(intent.market_price_hint)
            else:
                try:
                    quote = fetch_top_of_book(intent.market)
                    effective_market_price = resolve_best_ask(quote, intent.market)
                except Exception as exc:
                    raise BrokerTemporaryError(
                        "market buy blocked: failed to load validated best ask "
                        f"market={intent.market} client_order_id={intent.client_order_id} cause={type(exc).__name__}: {exc}"
                    ) from exc

        lot_rules = build_market_lot_rules(
            market_id=intent.market,
            market_price=effective_market_price,
            rules=resolved_rules,
            exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
            exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
            exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
            source_mode="exchange",
        )
        has_explicit_qty_controls = any(
            hasattr(resolved_rules, field_name)
            for field_name in ("min_qty", "qty_step", "max_qty_decimals")
        )
        qty_split = lot_rules.split_qty(float(intent.qty))
        if not skip_qty_revalidation and has_explicit_qty_controls and qty_split.executable is False:
            raise BrokerRejectError(
                f"{intent.normalized_side.lower()} qty suppressed by quantity rule: "
                f"reason={qty_split.non_executable_reason} raw_qty={format(qty_split.requested_qty, 'f')} "
                f"lot_size={format(lot_rules.lot_size, 'f')} dust_qty={format(qty_split.dust_qty, 'f')} "
                f"lot_count={qty_split.lot_count} client_order_id={intent.client_order_id}"
            )
        if not skip_qty_revalidation and has_explicit_qty_controls and qty_split.dust_qty > DUST_POSITION_EPS:
            raise BrokerRejectError(
                f"qty requires explicit lot normalization before submit: "
                f"raw_qty={format(qty_split.requested_qty, 'f')} lot_size={format(lot_rules.lot_size, 'f')} "
                f"lot_count={qty_split.lot_count} dust_qty={format(qty_split.dust_qty, 'f')}"
            )

        internal_lot_qty = (
            lot_count_to_qty(lot_count=qty_split.lot_count, lot_size=lot_rules.lot_size)
            if has_explicit_qty_controls
            else float(intent.qty)
        )
        exchange_submit_qty = float(internal_lot_qty)
        exchange_submit_price: float | None = None
        exchange_submit_volume: float | None = None
        exchange_submit_notional_krw: float | None = None

        if intent.price is None and intent.normalized_side == "bid":
            if effective_market_price is None:
                raise BrokerRejectError("market BUY planning requires effective market price")
            exchange_submit_notional_krw = float(effective_market_price) * float(internal_lot_qty)
            price_unit = float(submit_price_tick_policy.price_unit)
            if price_unit > 0:
                exchange_submit_notional_krw = math.floor(exchange_submit_notional_krw / price_unit) * price_unit
            min_total = side_min_total_krw(rules=resolved_rules, side=order_side)
            if min_total > 0 and exchange_submit_notional_krw < float(min_total):
                raise BrokerRejectError(
                    "order notional below side minimum for market BUY: "
                    f"side={order_side} notional={exchange_submit_notional_krw:.8f} min_total={min_total:.8f}"
                )
            exchange_submit_price = float(exchange_submit_notional_krw)
        elif intent.price is None:
            exchange_submit_volume = float(exchange_submit_qty)
        else:
            normalized_limit_price = float(
                normalize_limit_price_for_side(
                    price=float(intent.price),
                    side=order_side,
                    rules=resolved_rules,
                )
            )
            if normalized_limit_price <= 0:
                raise BrokerRejectError(
                    "limit price normalization produced non-positive executable price: "
                    f"side={order_side} requested={float(intent.price):.8f} "
                    f"price_unit={submit_price_tick_policy.price_unit:.8f} normalized={normalized_limit_price:.8f}"
                )
            exchange_submit_notional_krw = normalized_limit_price * float(internal_lot_qty)
            min_total = side_min_total_krw(rules=resolved_rules, side=order_side)
            if min_total > 0 and exchange_submit_notional_krw < float(min_total):
                raise BrokerRejectError(
                    "order notional below side minimum for limit order: "
                    f"side={order_side} notional={exchange_submit_notional_krw:.8f} min_total={min_total:.8f}"
                )
            exchange_submit_price = normalized_limit_price
            exchange_submit_volume = float(exchange_submit_qty)

        return SubmitPlan(
            intent=intent,
            rules=resolved_rules,
            chance_validation_order_type=chance_validation_order_type,
            chance_supported_order_types=chance_supported_order_types,
            exchange_submit_field=exchange_submit_field,
            exchange_order_type=exchange_order_type,
            exchange_submit_price=exchange_submit_price,
            exchange_submit_volume=exchange_submit_volume,
            exchange_submit_notional_krw=exchange_submit_notional_krw,
            submit_contract_context=submit_contract_context,
            submit_price_tick_policy=submit_price_tick_policy,
            effective_market_price=effective_market_price,
            lot_rules=lot_rules,
            qty_split=qty_split,
            internal_lot_qty=float(internal_lot_qty),
            exchange_submit_qty=float(exchange_submit_qty),
            buy_price_none_submit_contract=buy_submit_contract,
            trace_id=trace_id,
            plan_id=f"{trace_id}:plan",
            phase_identity="planning",
            phase_result="planned",
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
