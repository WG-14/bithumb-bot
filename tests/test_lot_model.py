from __future__ import annotations

import pytest

from bithumb_bot.broker.order_rules import DerivedOrderConstraints
from bithumb_bot.lot_model import (
    build_market_lot_rules,
    is_executable_exit_qty,
    lot_count_to_qty,
    quantize_to_lot_count,
)


def _rules() -> DerivedOrderConstraints:
    return DerivedOrderConstraints(
        market_id="KRW-BTC",
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        bid_price_unit=10.0,
        ask_price_unit=10.0,
        order_types=("limit", "price", "market"),
        order_sides=("bid", "ask"),
        bid_fee=0.0005,
        ask_fee=0.0005,
        maker_bid_fee=0.0004,
        maker_ask_fee=0.0004,
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
    )


def test_market_lot_rules_quantize_exact_lot_count() -> None:
    lot_rules = build_market_lot_rules(
        market_id="KRW-BTC",
        market_price=20_000_000.0,
        rules=_rules(),
        exit_fee_ratio=0.0,
        exit_slippage_bps=0.0,
        exit_buffer_ratio=0.0,
    )

    assert lot_rules.lot_size > 0
    assert quantize_to_lot_count(qty=lot_rules.lot_size * 3, lot_size=lot_rules.lot_size) == 3
    assert lot_count_to_qty(lot_count=3, lot_size=lot_rules.lot_size) == pytest.approx(lot_rules.lot_size * 3)
    assert is_executable_exit_qty(qty=lot_rules.lot_size * 3, lot_rules=lot_rules) is True


def test_market_lot_rules_split_sub_lot_qty_into_dust_only_remainder() -> None:
    lot_rules = build_market_lot_rules(
        market_id="KRW-BTC",
        market_price=20_000_000.0,
        rules=_rules(),
        exit_fee_ratio=0.0,
        exit_slippage_bps=0.0,
        exit_buffer_ratio=0.0,
    )

    split = lot_rules.split_qty(lot_rules.lot_size / 2)

    assert split.executable is False
    assert split.lot_count == 0
    assert split.executable_qty == pytest.approx(0.0)
    assert split.dust_qty == pytest.approx(lot_rules.lot_size / 2)
    assert split.non_executable_reason == "dust_only_remainder"


def test_market_lot_rules_split_exact_lot_qty_is_executable() -> None:
    lot_rules = build_market_lot_rules(
        market_id="KRW-BTC",
        market_price=20_000_000.0,
        rules=_rules(),
        exit_fee_ratio=0.0,
        exit_slippage_bps=0.0,
        exit_buffer_ratio=0.0,
    )

    split = lot_rules.split_qty(lot_rules.lot_size * 2)

    assert split.executable is True
    assert split.lot_count == 2
    assert split.executable_qty == pytest.approx(lot_rules.lot_size * 2)
    assert split.dust_qty == pytest.approx(0.0)
