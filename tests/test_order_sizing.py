from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.lifecycle import LotDefinitionSnapshot, LOT_SEMANTIC_VERSION_V1
from bithumb_bot.order_sizing import (
    BuyExecutionAuthority,
    SellExecutionAuthority,
    build_buy_execution_sizing,
    build_sell_execution_sizing,
)


@pytest.fixture
def sizing_rule_overrides():
    original = {
        "BUY_FRACTION": float(settings.BUY_FRACTION),
        "MAX_ORDER_KRW": float(settings.MAX_ORDER_KRW),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
        "LIVE_FEE_RATE_ESTIMATE": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
    }
    object.__setattr__(settings, "BUY_FRACTION", 0.5)
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    try:
        yield
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_buy_execution_sizing_finalizes_order_qty_from_entry_budget(sizing_rule_overrides) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=20_000_000.0,
    )

    assert plan.side == "BUY"
    assert plan.allowed is True
    assert plan.qty_source == "entry.intent_budget_exchange_constraints"
    assert plan.budget_krw == pytest.approx(10000.0)
    assert plan.requested_qty == pytest.approx(0.0005)
    assert plan.internal_lot_size == pytest.approx(0.0004)
    assert plan.intended_lot_count == 1
    assert plan.executable_lot_count == 1
    assert plan.executable_qty == pytest.approx(0.0005)
    assert plan.decision_reason_code == "none"


# BUY execution authority and sizing handoff.

def test_buy_execution_sizing_consumes_entry_intent_and_still_finalizes_qty_in_sizing(
    sizing_rule_overrides,
) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=10_000_000.0,
        entry_intent={
            "pair": "BTC_KRW",
            "intent": "enter_open_exposure",
            "budget_model": "cash_fraction_capped_by_max_order_krw",
            "budget_fraction_of_cash": 0.25,
            "max_budget_krw": 5000.0,
            "requires_execution_sizing": True,
        },
    )

    assert plan.allowed is True
    assert plan.budget_krw == pytest.approx(5000.0)
    assert plan.requested_qty == pytest.approx(0.0005)
    assert plan.executable_qty == pytest.approx(0.0005)
    assert plan.internal_lot_size > plan.executable_qty
    assert plan.intended_lot_count == 0
    assert plan.executable_lot_count == 0
    assert plan.block_reason == "none"
    assert plan.decision_reason_code == "none"


def test_buy_execution_sizing_exposes_buffered_internal_lot_without_restoring_buy_gate(
    sizing_rule_overrides,
) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=10_000_000.0,
        entry_intent={
            "pair": "BTC_KRW",
            "intent": "enter_open_exposure",
            "budget_model": "cash_fraction_capped_by_max_order_krw",
            "budget_fraction_of_cash": 0.25,
            "max_budget_krw": 5000.0,
            "requires_execution_sizing": True,
        },
    )

    assert plan.allowed is True
    assert plan.executable_qty == pytest.approx(0.0005)
    assert plan.internal_lot_is_exchange_inflated is True
    assert plan.internal_lot_would_block_buy is True
    assert plan.intended_lot_count == 0
    assert plan.executable_lot_count == 0


def test_buy_execution_sizing_returns_direct_reason_for_non_positive_budget(
    sizing_rule_overrides,
) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=0.0,
        market_price=20_000_000.0,
    )

    assert plan.allowed is False
    assert plan.block_reason == "non_positive_entry_budget"
    assert plan.decision_reason_code == "non_positive_entry_budget"


def test_buy_execution_sizing_returns_direct_reason_for_min_notional_miss(
    sizing_rule_overrides,
) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=20_000_000.0,
        entry_intent={
            "pair": "BTC_KRW",
            "intent": "enter_open_exposure",
            "budget_model": "cash_fraction_capped_by_max_order_krw",
            "budget_fraction_of_cash": 0.2,
            "max_budget_krw": 4000.0,
            "requires_execution_sizing": True,
        },
    )

    assert plan.allowed is False
    assert plan.requested_qty == pytest.approx(0.0002)
    assert plan.block_reason == "entry_min_notional_miss"
    assert plan.decision_reason_code == "entry_min_notional_miss"


def test_buy_execution_sizing_returns_direct_reason_when_exchange_rounding_zeroes_qty(
    sizing_rule_overrides,
) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=1_000.0,
        market_price=20_000_000.0,
    )

    assert plan.allowed is False
    assert plan.requested_qty == pytest.approx(0.000025)
    assert plan.executable_qty == pytest.approx(0.0)
    assert plan.block_reason == "entry_qty_rounded_to_zero_after_exchange_constraints"
    assert plan.decision_reason_code == "entry_qty_rounded_to_zero_after_exchange_constraints"


def test_buy_execution_sizing_preserves_typed_buy_authority_handoff(
    sizing_rule_overrides,
) -> None:
    authority = BuyExecutionAuthority(
        entry_allowed=True,
        entry_allowed_truth_source="position_state.normalized_exposure.entry_allowed",
    )

    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=20_000_000.0,
        authority=authority,
    )

    assert plan.allowed is True
    assert plan.buy_authority == authority
    assert plan.buy_authority is authority


def test_buy_execution_authority_is_informational_handoff_only(
    sizing_rule_overrides,
) -> None:
    authority = BuyExecutionAuthority(
        entry_allowed=False,
        entry_allowed_truth_source="position_state.normalized_exposure.entry_allowed",
    )

    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=20_000_000.0,
        authority=authority,
    )

    assert plan.allowed is True
    assert plan.executable_qty == pytest.approx(0.0005)
    assert plan.buy_authority is authority


def test_buy_execution_sizing_does_not_reserve_fee_budget_before_qty_rounding(sizing_rule_overrides) -> None:
    plan = build_buy_execution_sizing(
        pair="BTC_KRW",
        cash_krw=20000.0,
        market_price=10_000.0,
        fee_rate=0.1,
    )

    assert plan.side == "BUY"
    assert plan.allowed is True
    assert plan.budget_krw == pytest.approx(10000.0)
    assert plan.requested_qty == pytest.approx(10000.0 / 10_000.0)
    assert plan.executable_qty <= plan.requested_qty
    assert plan.executable_qty == pytest.approx(plan.requested_qty)


# SELL execution authority and lot-native submit sizing.

def test_sell_execution_sizing_finalizes_order_qty_from_sellable_inventory(sizing_rule_overrides) -> None:
    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=308,
            exit_allowed=True,
            exit_block_reason="none",
        ),
    )

    assert plan.side == "SELL"
    assert plan.allowed is True
    assert plan.qty_source == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert plan.requested_qty == pytest.approx(0.1232)
    assert plan.executable_qty == pytest.approx(0.1232)
    assert plan.internal_lot_size == pytest.approx(0.0004)
    assert plan.intended_lot_count == 308
    assert plan.executable_lot_count == 308
    assert plan.decision_reason_code == "none"


def test_sell_execution_sizing_uses_suppression_reason_code_when_quantity_rule_blocks_exit(sizing_rule_overrides) -> None:
    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=0,
            exit_allowed=False,
            exit_block_reason="no_executable_exit_lot",
        ),
    )

    assert plan.side == "SELL"
    assert plan.allowed is False
    assert plan.requested_qty == pytest.approx(0.0)
    assert plan.executable_qty == pytest.approx(0.0)
    assert plan.block_reason == "no_executable_exit_lot"
    assert plan.decision_reason_code == "exit_suppressed_by_quantity_rule"
    assert plan.non_executable_reason == "no_executable_exit_lot"


def test_sell_execution_sizing_uses_canonical_sellable_lot_count_when_qty_rounds_to_zero(sizing_rule_overrides) -> None:
    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=2,
            exit_allowed=True,
            exit_block_reason="none",
        ),
    )

    assert plan.side == "SELL"
    assert plan.allowed is True
    assert plan.requested_qty == pytest.approx(0.0008)
    assert plan.executable_qty == pytest.approx(0.0008)
    assert plan.intended_lot_count == 2
    assert plan.executable_lot_count == 2
    assert plan.block_reason == "none"
    assert plan.decision_reason_code == "none"


def test_sell_execution_sizing_prefers_persisted_lot_definition_over_current_rules(
    sizing_rule_overrides,
) -> None:
    persisted_lot_definition = LotDefinitionSnapshot(
        semantic_version=LOT_SEMANTIC_VERSION_V1,
        internal_lot_size=0.0004,
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
        source_mode="ledger",
    )
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 6)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 100000.0)

    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=2,
            exit_allowed=True,
            exit_block_reason="none",
        ),
        lot_definition=persisted_lot_definition,
    )

    assert plan.allowed is True
    assert plan.requested_qty == pytest.approx(0.0008)
    assert plan.executable_qty == pytest.approx(0.0008)
    assert plan.internal_lot_size == pytest.approx(0.0004)
    assert plan.min_qty == pytest.approx(0.0001)
    assert plan.qty_step == pytest.approx(0.0001)
    assert plan.min_notional_krw == pytest.approx(5000.0)


@pytest.mark.lot_native_regression_gate
def test_sell_execution_sizing_requires_canonical_sell_authority_inputs(sizing_rule_overrides) -> None:
    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=0,
            exit_allowed=True,
            exit_block_reason="none",
        ),
    )

    assert plan.allowed is False
    assert plan.requested_qty == pytest.approx(0.0)
    assert plan.executable_qty == pytest.approx(0.0)
    assert plan.block_reason == "no_executable_exit_lot"
    assert plan.decision_reason_code == "exit_suppressed_by_quantity_rule"
