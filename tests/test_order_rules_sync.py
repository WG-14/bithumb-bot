from __future__ import annotations

from bithumb_bot.broker import order_rules
from bithumb_bot.config import settings


import pytest


@pytest.fixture(autouse=True)
def _reset_settings():
    old = {
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
    }
    yield
    for key, value in old.items():
        object.__setattr__(settings, key, value)



def test_get_effective_order_rules_uses_auto_values_when_metadata_available(monkeypatch):
    order_rules._cached_rules.clear()

    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.OrderRules(
            min_qty=0.001,
            qty_step=0.001,
            min_notional_krw=10000.0,
            max_qty_decimals=3,
        ),
    )

    resolved = order_rules.get_effective_order_rules("BTC_KRW")

    assert resolved.rules.min_qty == 0.001
    assert resolved.rules.qty_step == 0.001
    assert resolved.rules.min_notional_krw == 10000.0
    assert resolved.rules.max_qty_decimals == 3
    assert resolved.source["min_qty"] == "auto"
    assert resolved.source["qty_step"] == "auto"
    assert resolved.source["min_notional_krw"] == "auto"
    assert resolved.source["max_qty_decimals"] == "auto"


def test_get_effective_order_rules_falls_back_to_manual_when_metadata_fetch_fails(monkeypatch):
    order_rules._cached_rules.clear()

    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0002)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0005)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 6000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 5)

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    warnings: list[str] = []
    monkeypatch.setattr(order_rules, "notify", lambda msg: warnings.append(msg))

    resolved = order_rules.get_effective_order_rules("BTC_KRW")

    assert resolved.rules.min_qty == 0.0002
    assert resolved.rules.qty_step == 0.0005
    assert resolved.rules.min_notional_krw == 6000.0
    assert resolved.rules.max_qty_decimals == 5
    assert all(source == "manual" for source in resolved.source.values())
    assert warnings
    assert "auto-sync failed" in warnings[0]
