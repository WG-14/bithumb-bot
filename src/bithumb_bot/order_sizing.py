from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from dataclasses import dataclass
from typing import Any

from .config import settings
from .dust import DUST_POSITION_EPS
from .dust import build_executable_lot
from .fee_authority import (
    FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON,
    FeeAuthoritySnapshot,
    build_fee_authority_snapshot,
)
from .broker.order_rules import get_effective_order_rules
from .lifecycle import LotDefinitionSnapshot
from .lot_model import build_market_lot_rules, lot_count_to_qty

_DECIMAL_ZERO = Decimal("0")

BUY_BLOCK_REASON_NON_POSITIVE_ENTRY_BUDGET = "non_positive_entry_budget"
BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS = "entry_min_notional_miss"
BUY_BLOCK_REASON_ENTRY_QTY_ROUNDED_TO_ZERO = "entry_qty_rounded_to_zero_after_exchange_constraints"


@dataclass(frozen=True)
class EntryExecutionIntent:
    pair: str
    intent: str
    budget_model: str
    budget_fraction_of_cash: float
    max_budget_krw: float
    requires_execution_sizing: bool


@dataclass(frozen=True)
class BuyExecutionAuthority:
    """Canonical BUY authority surface for execution sizing handoff.

    BUY entry authorization remains an upstream position-state decision.
    This typed object is carried through sizing as informational provenance
    only: it records the upstream gate outcome and truth source, but it is not
    itself the BUY quantity gate and it does not override exchange-constrained
    sizing outcomes.
    """

    entry_allowed: bool
    entry_allowed_truth_source: str


@dataclass(frozen=True)
class ExecutionSizingPlan:
    side: str
    allowed: bool
    block_reason: str
    decision_reason_code: str
    gross_budget_krw: float
    budget_krw: float
    exposure_offset_krw: float
    requested_qty: float
    exchange_constrained_qty: float
    lifecycle_executable_qty: float
    executable_qty: float
    rejected_qty_remainder: float
    unused_budget_krw: float
    internal_lot_size: float
    intended_lot_count: int
    executable_lot_count: int
    qty_source: str
    effective_min_trade_qty: float
    min_qty: float
    qty_step: float
    min_notional_krw: float
    non_executable_reason: str
    buy_authority: BuyExecutionAuthority | None = None
    internal_lot_is_exchange_inflated: bool = False
    internal_lot_would_block_buy: bool = False
    fee_authority: FeeAuthoritySnapshot | None = None
    fee_authority_source: str = "unresolved"
    fee_authority_degraded: bool = False
    fee_rate_used: float = 0.0
    residual_inventory_qty: float = 0.0
    residual_inventory_notional_krw: float = 0.0
    total_effective_exposure_qty: float = 0.0
    total_effective_exposure_notional_krw: float = 0.0
    buy_sizing_residual_adjusted: bool = False


@dataclass(frozen=True)
class SellExecutionAuthority:
    """Canonical SELL authority surface for execution sizing.

    SELL decision eligibility and sizing must come from
    `position_state.normalized_exposure.sellable_executable_lot_count`.
    The SELL path must accept that lot-native authority, not raw aggregate qty
    and not lifecycle/accounting lot matching. Any qty used for execution
    remains derived from this lot-native input.
    """

    sellable_executable_lot_count: int
    exit_allowed: bool
    exit_block_reason: str


def _sell_decision_reason_code(*, exit_block_reason: str) -> str:
    """Return the canonical sell decision outcome code.

    The lot-derived executable quantity remains the source of truth. This helper
    only names the reason execution was suppressed after lot sizing already
    finished.
    """

    normalized = str(exit_block_reason or "").strip()
    if normalized in {"", "none", "no_executable_exit_lot"}:
        return "exit_suppressed_by_quantity_rule"
    return normalized


def _decimal_from_number(value: object) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid numeric value: {value}") from exc
    if not parsed.is_finite():
        raise ValueError(f"invalid non-finite numeric value: {value}")
    return parsed


def _build_default_entry_execution_intent(*, pair: str) -> EntryExecutionIntent:
    return EntryExecutionIntent(
        pair=str(pair),
        intent="enter_open_exposure",
        budget_model="cash_fraction_capped_by_max_order_krw",
        budget_fraction_of_cash=float(settings.BUY_FRACTION),
        max_budget_krw=float(settings.MAX_ORDER_KRW),
        requires_execution_sizing=True,
    )


def _exchange_qty_step(*, qty_step: float, min_qty: float, max_qty_decimals: int) -> float:
    normalized_step = max(0.0, float(qty_step))
    if normalized_step > DUST_POSITION_EPS:
        return normalized_step
    normalized_min_qty = max(0.0, float(min_qty))
    if normalized_min_qty > DUST_POSITION_EPS:
        return normalized_min_qty
    normalized_decimals = max(0, int(max_qty_decimals))
    if normalized_decimals > 0:
        return 1.0 / (10**normalized_decimals)
    return 0.0


def _floor_qty_to_exchange_constraints(*, qty: float, qty_step: float, max_qty_decimals: int) -> float:
    normalized_qty = max(_DECIMAL_ZERO, _decimal_from_number(qty))
    normalized_step = max(_DECIMAL_ZERO, _decimal_from_number(qty_step))
    if normalized_step > _DECIMAL_ZERO:
        normalized_qty = (normalized_qty / normalized_step).to_integral_value(rounding=ROUND_FLOOR) * normalized_step
    quantizer = Decimal("1").scaleb(-max(0, int(max_qty_decimals))) if int(max_qty_decimals) > 0 else None
    if quantizer is not None:
        normalized_qty = normalized_qty.quantize(quantizer, rounding=ROUND_FLOOR)
    return max(0.0, float(normalized_qty))


def _ceil_qty_to_exchange_constraints(*, qty: float, qty_step: float, max_qty_decimals: int) -> float:
    normalized_qty = max(_DECIMAL_ZERO, _decimal_from_number(qty))
    normalized_step = max(_DECIMAL_ZERO, _decimal_from_number(qty_step))
    if normalized_step > _DECIMAL_ZERO:
        normalized_qty = (normalized_qty / normalized_step).to_integral_value(rounding=ROUND_CEILING) * normalized_step
    quantizer = Decimal("1").scaleb(-max(0, int(max_qty_decimals))) if int(max_qty_decimals) > 0 else None
    if quantizer is not None:
        normalized_qty = normalized_qty.quantize(quantizer, rounding=ROUND_CEILING)
    return max(0.0, float(normalized_qty))


def compute_feasible_entry_lot_count(
    *,
    budget_krw: float,
    market_price: float,
    lot_rules,
) -> int:
    budget = max(0.0, float(budget_krw))
    price = float(market_price)
    lot_size = float(getattr(lot_rules, "lot_size", 0.0) or 0.0)
    if budget <= 0.0 or not math.isfinite(price) or price <= 0.0 or lot_size <= 0.0:
        return 0

    lot_notional = price * lot_size
    if not math.isfinite(lot_notional) or lot_notional <= 0.0:
        return 0

    lot_count = Decimal(str(budget)) / Decimal(str(lot_notional))
    return max(0, int(lot_count.to_integral_value(rounding=ROUND_FLOOR)))


def compute_feasible_exit_lot_count(
    *,
    sellable_qty: float,
    lot_rules,
) -> int:
    requested_qty = max(0.0, float(sellable_qty))
    lot_size = float(getattr(lot_rules, "lot_size", 0.0) or 0.0)
    if requested_qty <= 0.0 or lot_size <= 0.0:
        return 0
    return max(0, int(lot_rules.quantize_to_lot_count(qty=requested_qty, rounding=ROUND_FLOOR)))


def _parse_entry_execution_intent(
    *,
    pair: str,
    entry_intent: EntryExecutionIntent | dict[str, Any] | None,
) -> EntryExecutionIntent:
    if isinstance(entry_intent, EntryExecutionIntent):
        return entry_intent
    default_intent = _build_default_entry_execution_intent(pair=pair)
    if not isinstance(entry_intent, dict):
        return default_intent
    return EntryExecutionIntent(
        pair=str(entry_intent.get("pair") or default_intent.pair),
        intent=str(entry_intent.get("intent") or "enter_open_exposure"),
        budget_model=str(
            entry_intent.get("budget_model")
            or default_intent.budget_model
        ),
        budget_fraction_of_cash=max(
            0.0,
            float(
                entry_intent.get(
                    "budget_fraction_of_cash",
                    default_intent.budget_fraction_of_cash,
                )
            ),
        ),
        max_budget_krw=max(
            0.0,
            float(entry_intent.get("max_budget_krw", default_intent.max_budget_krw)),
        ),
        requires_execution_sizing=bool(
            entry_intent.get(
                "requires_execution_sizing",
                default_intent.requires_execution_sizing,
            )
        ),
    )


def _parse_buy_execution_authority(
    *,
    authority: BuyExecutionAuthority | None,
) -> BuyExecutionAuthority | None:
    if isinstance(authority, BuyExecutionAuthority):
        return authority
    return None


def build_buy_execution_sizing(
    *,
    pair: str,
    cash_krw: float,
    market_price: float,
    fee_rate: float | None = None,
    entry_intent: EntryExecutionIntent | dict[str, Any] | None = None,
    authority: BuyExecutionAuthority | None = None,
    existing_exposure_qty: float = 0.0,
    residual_inventory_qty: float = 0.0,
    residual_inventory_notional_krw: float | None = None,
) -> ExecutionSizingPlan:
    resolved_intent = _parse_entry_execution_intent(pair=pair, entry_intent=entry_intent)
    resolved_authority = _parse_buy_execution_authority(authority=authority)
    gross_budget = max(0.0, float(cash_krw)) * float(resolved_intent.budget_fraction_of_cash)
    if float(resolved_intent.max_budget_krw) > 0:
        gross_budget = min(gross_budget, float(resolved_intent.max_budget_krw))
    residual_qty = max(0.0, float(residual_inventory_qty))
    executable_exposure_qty = max(0.0, float(existing_exposure_qty))
    residual_notional = (
        max(0.0, float(residual_inventory_notional_krw))
        if residual_inventory_notional_krw is not None
        else residual_qty * max(0.0, float(market_price))
    )
    executable_exposure_notional = executable_exposure_qty * max(0.0, float(market_price))
    total_effective_exposure_qty = executable_exposure_qty + residual_qty
    total_effective_exposure_notional = executable_exposure_notional + residual_notional
    exposure_offset_krw = total_effective_exposure_notional
    adjusted_budget = max(0.0, float(gross_budget) - float(exposure_offset_krw))
    residual_adjusted = bool(residual_qty > DUST_POSITION_EPS or residual_notional > 0.0)
    if adjusted_budget <= 0.0:
        return ExecutionSizingPlan(
            side="BUY",
            allowed=False,
            block_reason=BUY_BLOCK_REASON_NON_POSITIVE_ENTRY_BUDGET,
            decision_reason_code=BUY_BLOCK_REASON_NON_POSITIVE_ENTRY_BUDGET,
            gross_budget_krw=float(gross_budget),
            budget_krw=float(adjusted_budget),
            exposure_offset_krw=float(exposure_offset_krw),
            requested_qty=0.0,
            exchange_constrained_qty=0.0,
            lifecycle_executable_qty=0.0,
            executable_qty=0.0,
            rejected_qty_remainder=0.0,
            unused_budget_krw=0.0,
            internal_lot_size=0.0,
            intended_lot_count=0,
            executable_lot_count=0,
            qty_source=(
                "residual_inventory_delta"
                if residual_adjusted
                else "entry.intent_budget_krw"
            ),
            effective_min_trade_qty=0.0,
            min_qty=0.0,
            qty_step=0.0,
            min_notional_krw=0.0,
            non_executable_reason=BUY_BLOCK_REASON_NON_POSITIVE_ENTRY_BUDGET,
            buy_authority=resolved_authority,
            residual_inventory_qty=float(residual_qty),
            residual_inventory_notional_krw=float(residual_notional),
            total_effective_exposure_qty=float(total_effective_exposure_qty),
            total_effective_exposure_notional_krw=float(total_effective_exposure_notional),
            buy_sizing_residual_adjusted=residual_adjusted,
        )
    if not math.isfinite(float(market_price)) or float(market_price) <= 0.0:
        return ExecutionSizingPlan(
            side="BUY",
            allowed=False,
            block_reason="invalid_market_price",
            decision_reason_code="invalid_market_price",
            gross_budget_krw=float(gross_budget),
            budget_krw=float(adjusted_budget),
            exposure_offset_krw=float(exposure_offset_krw),
            requested_qty=0.0,
            exchange_constrained_qty=0.0,
            lifecycle_executable_qty=0.0,
            executable_qty=0.0,
            rejected_qty_remainder=0.0,
            unused_budget_krw=0.0,
            internal_lot_size=0.0,
            intended_lot_count=0,
            executable_lot_count=0,
            qty_source=(
                "residual_inventory_delta"
                if residual_adjusted
                else "entry.intent_budget_krw"
            ),
            effective_min_trade_qty=0.0,
            min_qty=0.0,
            qty_step=0.0,
            min_notional_krw=0.0,
            non_executable_reason="invalid_market_price",
            buy_authority=resolved_authority,
            residual_inventory_qty=float(residual_qty),
            residual_inventory_notional_krw=float(residual_notional),
            total_effective_exposure_qty=float(total_effective_exposure_qty),
            total_effective_exposure_notional_krw=float(total_effective_exposure_notional),
            buy_sizing_residual_adjusted=residual_adjusted,
        )
    resolution = get_effective_order_rules(pair)
    rules = resolution.rules
    fee_authority = build_fee_authority_snapshot(resolution, config_fallback_fee_rate=fee_rate)
    if settings.MODE == "live" and not bool(settings.LIVE_DRY_RUN) and bool(settings.LIVE_REAL_ORDER_ARMED):
        if not fee_authority.live_entry_allowed():
            return ExecutionSizingPlan(
                side="BUY",
                allowed=False,
                block_reason=FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON,
                decision_reason_code=FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON,
                gross_budget_krw=float(gross_budget),
                budget_krw=float(adjusted_budget),
                exposure_offset_krw=float(exposure_offset_krw),
                requested_qty=0.0,
                exchange_constrained_qty=0.0,
                lifecycle_executable_qty=0.0,
                executable_qty=0.0,
                rejected_qty_remainder=0.0,
                unused_budget_krw=0.0,
                internal_lot_size=0.0,
                intended_lot_count=0,
                executable_lot_count=0,
                qty_source=(
                    "residual_inventory_delta"
                    if residual_adjusted
                    else "entry.intent_budget_krw"
                ),
                effective_min_trade_qty=0.0,
                min_qty=float(rules.min_qty),
                qty_step=float(rules.qty_step),
                min_notional_krw=float(rules.min_notional_krw),
                non_executable_reason=FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON,
                buy_authority=resolved_authority,
                fee_authority=fee_authority,
                fee_authority_source=fee_authority.fee_source,
                fee_authority_degraded=True,
                fee_rate_used=float(fee_authority.taker_bid_fee_rate),
                residual_inventory_qty=float(residual_qty),
                residual_inventory_notional_krw=float(residual_notional),
                total_effective_exposure_qty=float(total_effective_exposure_qty),
                total_effective_exposure_notional_krw=float(total_effective_exposure_notional),
                buy_sizing_residual_adjusted=residual_adjusted,
            )
    lot_rules = build_market_lot_rules(
        market_id=pair,
        market_price=float(market_price),
        rules=rules,
        exit_fee_ratio=float(fee_authority.taker_ask_fee_rate),
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    requested_qty = float(_decimal_from_number(adjusted_budget) / _decimal_from_number(market_price))
    effective_qty_step = _exchange_qty_step(
        qty_step=float(rules.qty_step),
        min_qty=float(rules.min_qty),
        max_qty_decimals=int(rules.max_qty_decimals),
    )
    executable_qty = _floor_qty_to_exchange_constraints(
        qty=float(requested_qty),
        qty_step=float(effective_qty_step),
        max_qty_decimals=int(rules.max_qty_decimals),
    )
    executable_lot = build_executable_lot(
        qty=float(executable_qty),
        market_price=float(market_price),
        min_qty=float(rules.min_qty),
        qty_step=float(rules.qty_step),
        min_notional_krw=float(rules.min_notional_krw),
        max_qty_decimals=int(rules.max_qty_decimals),
        exit_fee_ratio=float(fee_authority.taker_ask_fee_rate),
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    effective_min_trade_qty = float(executable_lot.effective_min_trade_qty)
    lifecycle_executable_qty = float(executable_lot.executable_qty)
    rejected_qty_remainder = max(0.0, float(requested_qty) - float(executable_qty))
    unused_budget_krw = float(rejected_qty_remainder) * float(market_price)
    intended_lot_count = max(0, int(lot_rules.quantize_to_lot_count(qty=float(requested_qty), rounding=ROUND_FLOOR)))
    executable_lot_count = max(0, int(lot_rules.quantize_to_lot_count(qty=float(executable_qty), rounding=ROUND_FLOOR)))
    internal_lot_is_exchange_inflated = bool(float(lot_rules.lot_size) > float(effective_min_trade_qty) + DUST_POSITION_EPS)
    internal_lot_would_block_buy = bool(
        executable_qty > DUST_POSITION_EPS
        and intended_lot_count <= 0
        and lot_count_to_qty(lot_count=intended_lot_count, lot_size=lot_rules.lot_size) <= DUST_POSITION_EPS
    )
    if executable_qty <= DUST_POSITION_EPS:
        allowed = False
        entry_reason = BUY_BLOCK_REASON_ENTRY_QTY_ROUNDED_TO_ZERO
    elif float(rules.min_qty) > 0.0 and executable_qty + DUST_POSITION_EPS < float(rules.min_qty):
        allowed = False
        entry_reason = "entry_min_qty_miss"
    elif float(rules.min_notional_krw) > 0.0 and (executable_qty * float(market_price)) + DUST_POSITION_EPS < float(rules.min_notional_krw):
        allowed = False
        entry_reason = BUY_BLOCK_REASON_ENTRY_MIN_NOTIONAL_MISS
    elif executable_lot.executable_qty <= DUST_POSITION_EPS:
        allowed = False
        entry_reason = str(executable_lot.exit_non_executable_reason or "no_executable_exit_lot")
    else:
        allowed = True
        entry_reason = "none"
    return ExecutionSizingPlan(
        side="BUY",
        allowed=allowed,
        block_reason="none" if allowed else entry_reason,
        decision_reason_code="none" if allowed else entry_reason,
        gross_budget_krw=float(gross_budget),
        budget_krw=float(adjusted_budget),
        exposure_offset_krw=float(exposure_offset_krw),
        requested_qty=float(requested_qty),
        exchange_constrained_qty=float(executable_qty),
        lifecycle_executable_qty=float(lifecycle_executable_qty),
        executable_qty=float(executable_qty if allowed else 0.0),
        rejected_qty_remainder=float(rejected_qty_remainder),
        unused_budget_krw=float(unused_budget_krw),
        internal_lot_size=float(lot_rules.lot_size),
        intended_lot_count=int(intended_lot_count),
        executable_lot_count=int(executable_lot_count if allowed else 0),
        qty_source=(
            "residual_inventory_delta"
            if residual_adjusted
            else "entry.intent_budget_exchange_constraints"
        ),
        effective_min_trade_qty=float(effective_min_trade_qty),
        min_qty=float(lot_rules.min_qty),
        qty_step=float(lot_rules.qty_step),
        min_notional_krw=float(lot_rules.min_notional_krw),
        non_executable_reason="executable" if allowed else entry_reason,
        buy_authority=resolved_authority,
        internal_lot_is_exchange_inflated=internal_lot_is_exchange_inflated,
        internal_lot_would_block_buy=internal_lot_would_block_buy,
        fee_authority=fee_authority,
        fee_authority_source=fee_authority.fee_source,
        fee_authority_degraded=fee_authority.degraded,
        fee_rate_used=float(fee_authority.taker_bid_fee_rate),
        residual_inventory_qty=float(residual_qty),
        residual_inventory_notional_krw=float(residual_notional),
        total_effective_exposure_qty=float(total_effective_exposure_qty),
        total_effective_exposure_notional_krw=float(total_effective_exposure_notional),
        buy_sizing_residual_adjusted=residual_adjusted,
    )


def build_sell_execution_sizing(
    *,
    pair: str,
    market_price: float,
    authority: SellExecutionAuthority,
    lot_definition: LotDefinitionSnapshot | None = None,
) -> ExecutionSizingPlan:
    if lot_definition is not None and lot_definition.is_authoritative:
        internal_lot_size = float(lot_definition.internal_lot_size or 0.0)
        effective_min_trade_qty = float(lot_definition.min_qty or 0.0)
        min_qty = float(lot_definition.min_qty or 0.0)
        qty_step = float(lot_definition.qty_step or 0.0)
        min_notional_krw = float(lot_definition.min_notional_krw or 0.0)
    else:
        resolution = get_effective_order_rules(pair)
        rules = resolution.rules
        fee_authority = build_fee_authority_snapshot(resolution)
        lot_rules = build_market_lot_rules(
            market_id=pair,
            market_price=float(market_price),
            rules=rules,
            exit_fee_ratio=float(fee_authority.taker_ask_fee_rate),
            exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
            exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
        )
        internal_lot_size = float(lot_rules.lot_size)
        effective_min_trade_qty = float(lot_rules.executable_min_qty)
        min_qty = float(lot_rules.min_qty)
        qty_step = float(lot_rules.qty_step)
        min_notional_krw = float(lot_rules.min_notional_krw)
    if lot_definition is not None and lot_definition.is_authoritative:
        fee_authority = None
    intended_lot_count = max(0, int(authority.sellable_executable_lot_count))
    requested_qty = lot_count_to_qty(
        lot_count=intended_lot_count,
        lot_size=float(internal_lot_size),
    )
    executable_qty = float(requested_qty)
    allowed = bool(authority.exit_allowed) and intended_lot_count >= 1 and executable_qty > DUST_POSITION_EPS
    block_reason = "none" if allowed else str(authority.exit_block_reason or "no_executable_exit_lot")
    if not allowed and block_reason in {"", "none"}:
        block_reason = "no_executable_exit_lot"
    return ExecutionSizingPlan(
        side="SELL",
        allowed=allowed,
        block_reason=block_reason,
        decision_reason_code="none" if allowed else _sell_decision_reason_code(exit_block_reason=block_reason),
        gross_budget_krw=0.0,
        budget_krw=0.0,
        exposure_offset_krw=0.0,
        requested_qty=float(requested_qty),
        exchange_constrained_qty=float(requested_qty),
        lifecycle_executable_qty=float(executable_qty if allowed else 0.0),
        executable_qty=float(executable_qty if allowed else 0.0),
        rejected_qty_remainder=0.0,
        unused_budget_krw=0.0,
        internal_lot_size=float(internal_lot_size),
        intended_lot_count=int(intended_lot_count),
        executable_lot_count=int(intended_lot_count if allowed else 0),
        qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
        effective_min_trade_qty=float(effective_min_trade_qty),
        min_qty=float(min_qty),
        qty_step=float(qty_step),
        min_notional_krw=float(min_notional_krw),
        non_executable_reason="executable" if allowed else block_reason,
        fee_authority=fee_authority,
        fee_authority_source="lot_definition" if fee_authority is None else fee_authority.fee_source,
        fee_authority_degraded=False if fee_authority is None else fee_authority.degraded,
        fee_rate_used=0.0 if fee_authority is None else float(fee_authority.taker_ask_fee_rate),
    )
