from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .config import settings
from .dust import DUST_POSITION_EPS, build_executable_lot
from .broker.order_rules import get_effective_order_rules


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
    budget_krw: float
    requested_qty: float
    executable_qty: float
    qty_source: str
    effective_min_trade_qty: float
    min_qty: float
    qty_step: float
    min_notional_krw: float
    non_executable_reason: str


def _build_default_entry_execution_intent(*, pair: str) -> EntryExecutionIntent:
    return EntryExecutionIntent(
        pair=str(pair),
        intent="enter_open_exposure",
        budget_model="cash_fraction_capped_by_max_order_krw",
        budget_fraction_of_cash=float(settings.BUY_FRACTION),
        max_budget_krw=float(settings.MAX_ORDER_KRW),
        requires_execution_sizing=True,
    )


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
    normalized_fee_rate = max(0.0, float(settings.LIVE_FEE_RATE_ESTIMATE if fee_rate is None else fee_rate))
    net_budget = gross_budget / (1.0 + normalized_fee_rate) if gross_budget > 0.0 else 0.0
    if gross_budget <= 0.0 or not math.isfinite(float(market_price)) or float(market_price) <= 0.0:
        return ExecutionSizingPlan(
            side="BUY",
            allowed=False,
            block_reason="non_positive_entry_budget",
            budget_krw=float(gross_budget),
            requested_qty=0.0,
            executable_qty=0.0,
            qty_source="entry.intent_budget_krw",
            effective_min_trade_qty=0.0,
            min_qty=0.0,
            qty_step=0.0,
            min_notional_krw=0.0,
            non_executable_reason="non_positive_entry_budget",
        )
    rules = get_effective_order_rules(pair).rules
    requested_qty = net_budget / float(market_price)
    executable_lot = build_executable_lot(
        qty=requested_qty,
        market_price=float(market_price),
        min_qty=float(rules.min_qty),
        qty_step=float(rules.qty_step),
        min_notional_krw=float(rules.min_notional_krw),
        max_qty_decimals=int(rules.max_qty_decimals),
        exit_fee_ratio=normalized_fee_rate,
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    allowed = executable_lot.executable_qty > DUST_POSITION_EPS
    return ExecutionSizingPlan(
        side="BUY",
        allowed=allowed,
        block_reason="none" if allowed else str(executable_lot.exit_non_executable_reason),
        budget_krw=float(gross_budget),
        requested_qty=float(requested_qty),
        executable_qty=float(executable_lot.executable_qty),
        qty_source="entry.intent_budget_krw",
        effective_min_trade_qty=float(executable_lot.effective_min_trade_qty),
        min_qty=float(rules.min_qty),
        qty_step=float(rules.qty_step),
        min_notional_krw=float(rules.min_notional_krw),
        non_executable_reason=str(executable_lot.exit_non_executable_reason),
    )


def build_sell_execution_sizing(
    *,
    pair: str,
    market_price: float,
    sellable_qty: float,
    exit_allowed: bool,
    exit_block_reason: str,
) -> ExecutionSizingPlan:
    rules = get_effective_order_rules(pair).rules
    requested_qty = max(0.0, float(sellable_qty))
    executable_lot = build_executable_lot(
        qty=requested_qty,
        market_price=float(market_price),
        min_qty=float(rules.min_qty),
        qty_step=float(rules.qty_step),
        min_notional_krw=float(rules.min_notional_krw),
        max_qty_decimals=int(rules.max_qty_decimals),
        exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    allowed = bool(exit_allowed) and executable_lot.executable_qty > DUST_POSITION_EPS
    block_reason = "none" if allowed else str(exit_block_reason or executable_lot.exit_non_executable_reason)
    return ExecutionSizingPlan(
        side="SELL",
        allowed=allowed,
        block_reason=block_reason,
        budget_krw=0.0,
        requested_qty=float(requested_qty),
        executable_qty=float(executable_lot.executable_qty),
        qty_source="position_state.normalized_exposure.sellable_executable_qty",
        effective_min_trade_qty=float(executable_lot.effective_min_trade_qty),
        min_qty=float(rules.min_qty),
        qty_step=float(rules.qty_step),
        min_notional_krw=float(rules.min_notional_krw),
        non_executable_reason=str(executable_lot.exit_non_executable_reason),
    )
