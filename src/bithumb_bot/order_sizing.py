from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from dataclasses import dataclass
from typing import Any

from .config import settings
from .dust import DUST_POSITION_EPS
from .broker.order_rules import get_effective_order_rules
from .lifecycle import LotDefinitionSnapshot
from .lot_model import build_market_lot_rules, lot_count_to_qty

_DECIMAL_ZERO = Decimal("0")


@dataclass(frozen=True)
class EntryExecutionIntent:
    pair: str
    intent: str
    budget_model: str
    budget_fraction_of_cash: float
    max_budget_krw: float
    requires_execution_sizing: bool


@dataclass(frozen=True)
class ExecutionSizingPlan:
    side: str
    allowed: bool
    block_reason: str
    decision_reason_code: str
    budget_krw: float
    requested_qty: float
    executable_qty: float
    internal_lot_size: float
    intended_lot_count: int
    executable_lot_count: int
    qty_source: str
    effective_min_trade_qty: float
    min_qty: float
    qty_step: float
    min_notional_krw: float
    non_executable_reason: str


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


def build_buy_execution_sizing(
    *,
    pair: str,
    cash_krw: float,
    market_price: float,
    fee_rate: float | None = None,
    entry_intent: EntryExecutionIntent | dict[str, Any] | None = None,
) -> ExecutionSizingPlan:
    resolved_intent = _parse_entry_execution_intent(pair=pair, entry_intent=entry_intent)
    gross_budget = max(0.0, float(cash_krw)) * float(resolved_intent.budget_fraction_of_cash)
    if float(resolved_intent.max_budget_krw) > 0:
        gross_budget = min(gross_budget, float(resolved_intent.max_budget_krw))
    if gross_budget <= 0.0 or not math.isfinite(float(market_price)) or float(market_price) <= 0.0:
        return ExecutionSizingPlan(
            side="BUY",
            allowed=False,
            block_reason="non_positive_entry_budget",
            decision_reason_code="entry_suppressed_by_budget",
            budget_krw=float(gross_budget),
            requested_qty=0.0,
            executable_qty=0.0,
            internal_lot_size=0.0,
            intended_lot_count=0,
            executable_lot_count=0,
            qty_source="entry.intent_budget_krw",
            effective_min_trade_qty=0.0,
            min_qty=0.0,
            qty_step=0.0,
            min_notional_krw=0.0,
            non_executable_reason="non_positive_entry_budget",
        )
    rules = get_effective_order_rules(pair).rules
    lot_rules = build_market_lot_rules(
        market_id=pair,
        market_price=float(market_price),
        rules=rules,
        exit_fee_ratio=0.0,
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    requested_qty = float(_decimal_from_number(gross_budget) / _decimal_from_number(market_price))
    intended_lot_count = compute_feasible_entry_lot_count(
        budget_krw=float(gross_budget),
        market_price=float(market_price),
        lot_rules=lot_rules,
    )
    executable_qty = lot_count_to_qty(lot_count=intended_lot_count, lot_size=lot_rules.lot_size)
    allowed = bool(intended_lot_count >= 1 and executable_qty > DUST_POSITION_EPS)
    entry_reason = "none" if allowed else "no_executable_entry_lot"
    return ExecutionSizingPlan(
        side="BUY",
        allowed=allowed,
        block_reason="none" if allowed else entry_reason,
        decision_reason_code="none" if allowed else entry_reason,
        budget_krw=float(gross_budget),
        requested_qty=float(requested_qty),
        executable_qty=float(executable_qty if allowed else 0.0),
        internal_lot_size=float(lot_rules.lot_size),
        intended_lot_count=int(intended_lot_count),
        executable_lot_count=int(intended_lot_count if allowed else 0),
        qty_source="entry.intent_lot_count",
        effective_min_trade_qty=float(lot_rules.executable_min_qty),
        min_qty=float(lot_rules.min_qty),
        qty_step=float(lot_rules.qty_step),
        min_notional_krw=float(lot_rules.min_notional_krw),
        non_executable_reason="executable" if allowed else "no_executable_entry_lot",
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
        rules = get_effective_order_rules(pair).rules
        lot_rules = build_market_lot_rules(
            market_id=pair,
            market_price=float(market_price),
            rules=rules,
            exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
            exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
            exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
        )
        internal_lot_size = float(lot_rules.lot_size)
        effective_min_trade_qty = float(lot_rules.executable_min_qty)
        min_qty = float(lot_rules.min_qty)
        qty_step = float(lot_rules.qty_step)
        min_notional_krw = float(lot_rules.min_notional_krw)
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
        budget_krw=0.0,
        requested_qty=float(requested_qty),
        executable_qty=float(executable_qty if allowed else 0.0),
        internal_lot_size=float(internal_lot_size),
        intended_lot_count=int(intended_lot_count),
        executable_lot_count=int(intended_lot_count if allowed else 0),
        qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
        effective_min_trade_qty=float(effective_min_trade_qty),
        min_qty=float(min_qty),
        qty_step=float(qty_step),
        min_notional_krw=float(min_notional_krw),
        non_executable_reason="executable" if allowed else block_reason,
    )
