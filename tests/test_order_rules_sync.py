from __future__ import annotations

from bithumb_bot.broker import order_rules
from bithumb_bot.config import settings


import pytest


@pytest.fixture(autouse=True)
def _stub_market_canonicalization(monkeypatch):
    normalize = lambda pair: str(pair).replace("_", "-").upper()
    monkeypatch.setattr(order_rules, "build_order_rules_market", normalize)
    monkeypatch.setattr(order_rules, "canonical_market_id", normalize)


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


def _doc_order_chance_payload(*, market: str = "KRW-BTC") -> dict[str, object]:
    return {
        "bid_fee": "0.0025",
        "ask_fee": "0.0025",
        "maker_bid_fee": "0.0025",
        "maker_ask_fee": "0.0025",
        "market": {
            "id": market,
            "order_types": ["limit", "price", "market"],
            "order_sides": ["ask", "bid"],
            "bid": {"price_unit": "1", "min_total": "5000"},
            "ask": {"price_unit": "1", "min_total": "5000"},
        },
    }


def test_fetch_exchange_order_rules_strict_parse_accepts_documented_payload(monkeypatch):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: _doc_order_chance_payload(market=market)})(),
    )

    rules = order_rules.fetch_exchange_order_rules("BTC_KRW")

    assert rules.market_id == "BTC-KRW"
    assert rules.bid_min_total_krw == 5000.0
    assert rules.ask_min_total_krw == 5000.0
    assert rules.bid_price_unit == 1.0
    assert rules.ask_price_unit == 1.0
    assert rules.order_types == ("limit", "price", "market")
    assert rules.order_sides == ("ask", "bid")
    assert rules.bid_fee == 0.0025
    assert rules.ask_fee == 0.0025
    assert rules.maker_bid_fee == 0.0025
    assert rules.maker_ask_fee == 0.0025
    assert rules.min_notional_krw == 5000.0
    assert rules.min_qty == 0.0
    assert rules.qty_step == 0.0
    assert rules.max_qty_decimals == 0


def test_parse_order_chance_response_transforms_raw_payload():
    parsed = order_rules.parse_order_chance_response(_doc_order_chance_payload(), requested_market="KRW-BTC")

    assert parsed.market_id == "KRW-BTC"
    assert parsed.bid.price_unit == 1.0
    assert parsed.bid.min_total == 5000.0
    assert parsed.ask.price_unit == 1.0
    assert parsed.ask.min_total == 5000.0
    assert parsed.bid_fee == 0.0025
    assert parsed.ask_fee == 0.0025
    assert parsed.maker_bid_fee == 0.0025
    assert parsed.maker_ask_fee == 0.0025


def test_derive_order_rules_from_chance_preserves_bid_ask_split():
    response = order_rules.parse_order_chance_response(
        _doc_order_chance_payload(market="KRW-BTC") | {"market": _doc_order_chance_payload()["market"] | {"bid": {"price_unit": "10", "min_total": "5100"}, "ask": {"price_unit": "1", "min_total": "5000"}}},
        requested_market="KRW-BTC",
    )

    rules = order_rules.derive_order_rules_from_chance(response)

    assert rules.bid_min_total_krw == 5100.0
    assert rules.ask_min_total_krw == 5000.0
    assert rules.bid_price_unit == 10.0
    assert rules.ask_price_unit == 1.0
    assert rules.min_notional_krw == 5100.0


def test_fetch_exchange_order_rules_fails_on_market_id_mismatch(monkeypatch):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: _doc_order_chance_payload(market="KRW-ETH")})(),
    )

    with pytest.raises(order_rules.OrderChanceSchemaError, match="market.id mismatch"):
        order_rules.fetch_exchange_order_rules("KRW-BTC")


def test_fetch_exchange_order_rules_fails_when_bid_min_total_missing(monkeypatch):
    payload = _doc_order_chance_payload()
    del payload["market"]["bid"]["min_total"]
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: payload})(),
    )

    with pytest.raises(order_rules.OrderChanceSchemaError, match="response.market.bid.min_total"):
        order_rules.fetch_exchange_order_rules("KRW-BTC")


def test_fetch_exchange_order_rules_fails_when_ask_price_unit_missing(monkeypatch):
    payload = _doc_order_chance_payload()
    del payload["market"]["ask"]["price_unit"]
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: payload})(),
    )

    with pytest.raises(order_rules.OrderChanceSchemaError, match="response.market.ask.price_unit"):
        order_rules.fetch_exchange_order_rules("KRW-BTC")


def test_fetch_exchange_order_rules_works_without_undocumented_guess_fields(monkeypatch):
    payload = _doc_order_chance_payload()
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: payload})(),
    )

    rules = order_rules.fetch_exchange_order_rules("KRW-BTC")

    assert rules.min_notional_krw == 5000.0


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
            market_id="KRW-BTC",
            bid_min_total_krw=10000.0,
            ask_min_total_krw=11000.0,
            bid_price_unit=1.0,
            ask_price_unit=1.0,
            order_types=("limit", "price", "market"),
            order_sides=("ask", "bid"),
            bid_fee=0.0025,
            ask_fee=0.0025,
            maker_bid_fee=0.0020,
            maker_ask_fee=0.0020,
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
    assert resolved.source["min_qty"] == "unsupported_by_doc"
    assert resolved.source["qty_step"] == "unsupported_by_doc"
    assert resolved.source["min_notional_krw"] == "chance_doc"
    assert resolved.source["max_qty_decimals"] == "unsupported_by_doc"
    assert resolved.source["bid_min_total_krw"] == "chance_doc"
    assert resolved.source["ask_min_total_krw"] == "chance_doc"
    assert resolved.source["bid_price_unit"] == "chance_doc"
    assert resolved.source["ask_price_unit"] == "chance_doc"
    assert resolved.source["order_types"] == "chance_doc"
    assert resolved.source["order_sides"] == "chance_doc"
    assert resolved.source["bid_fee"] == "chance_doc"
    assert resolved.source["ask_fee"] == "chance_doc"
    assert resolved.source["maker_bid_fee"] == "chance_doc"
    assert resolved.source["maker_ask_fee"] == "chance_doc"


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
    assert resolved.source["min_qty"] == "manual_config"
    assert resolved.source["qty_step"] == "manual_config"
    assert resolved.source["min_notional_krw"] == "manual_config"
    assert resolved.source["max_qty_decimals"] == "manual_config"
    assert resolved.source["bid_min_total_krw"] == "unsupported_by_doc"
    assert warnings
    assert "auto-sync failed" in warnings[0]
