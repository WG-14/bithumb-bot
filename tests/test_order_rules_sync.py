from __future__ import annotations

from bithumb_bot.broker import order_rules
from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, fetch_latest_order_rule_snapshot


import pytest


@pytest.fixture(autouse=True)
def _reset_settings():
    old = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
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
            "bid_types": ["limit", "price", "market"],
            "ask_types": ["limit", "price", "market"],
            "order_sides": ["ask", "bid"],
            "bid": {"price_unit": "1", "min_total": "5000"},
            "ask": {"price_unit": "1", "min_total": "5000"},
        },
    }


@pytest.fixture
def valid_doc_shaped_response() -> dict[str, object]:
    return _doc_order_chance_payload()


@pytest.fixture
def missing_required_field_response() -> dict[str, object]:
    payload = _doc_order_chance_payload()
    del payload["market"]["bid"]["min_total"]
    return payload


@pytest.fixture
def market_mismatch_response() -> dict[str, object]:
    return _doc_order_chance_payload(market="KRW-ETH")


@pytest.fixture
def malformed_decimal_field_response() -> dict[str, object]:
    payload = _doc_order_chance_payload()
    payload["market"]["ask"]["price_unit"] = "not-a-decimal"
    return payload


@pytest.fixture
def extra_undocumented_field_response() -> dict[str, object]:
    payload = _doc_order_chance_payload()
    payload["legacy_guess_min_qty"] = "0.001"
    payload["market"]["legacy_guess_qty_step"] = "0.0001"
    return payload


def test_fetch_exchange_order_rules_strict_parse_accepts_documented_payload(monkeypatch, valid_doc_shaped_response):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: valid_doc_shaped_response | {"market": valid_doc_shaped_response["market"] | {"id": market}}})(),
    )

    rules = order_rules.fetch_exchange_order_rules("BTC-KRW")

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
    assert not hasattr(rules, "min_notional_krw")
    assert not hasattr(rules, "min_qty")
    assert not hasattr(rules, "qty_step")
    assert not hasattr(rules, "max_qty_decimals")


def test_parse_order_chance_response_transforms_raw_payload(valid_doc_shaped_response):
    parsed = order_rules.parse_order_chance_response(valid_doc_shaped_response, requested_market="KRW-BTC")

    assert parsed.market_id == "KRW-BTC"
    assert parsed.bid_types == ("limit", "price", "market")
    assert parsed.ask_types == ("limit", "price", "market")
    assert parsed.bid.price_unit == 1.0
    assert parsed.bid.min_total == 5000.0
    assert parsed.ask.price_unit == 1.0
    assert parsed.ask.min_total == 5000.0
    assert parsed.bid_fee == 0.0025
    assert parsed.ask_fee == 0.0025
    assert parsed.maker_bid_fee == 0.0025
    assert parsed.maker_ask_fee == 0.0025


def test_parse_order_chance_response_allows_missing_price_unit(valid_doc_shaped_response):
    payload = valid_doc_shaped_response.copy()
    market = dict(payload["market"])
    market["bid"] = {"min_total": "5000"}
    market["ask"] = {"min_total": "5000"}
    payload["market"] = market

    parsed = order_rules.parse_order_chance_response(payload, requested_market="KRW-BTC")

    assert parsed.bid.price_unit is None
    assert parsed.ask.price_unit is None
    derived = order_rules.derive_order_rules_from_chance(parsed)
    assert derived.bid_price_unit == 0.0
    assert derived.ask_price_unit == 0.0
    assert derived.bid_min_total_krw == 5000.0
    assert derived.ask_min_total_krw == 5000.0


def test_parse_order_chance_response_rejects_noncanonical_requested_market(valid_doc_shaped_response):
    with pytest.raises(ValueError, match="canonical QUOTE-BASE"):
        order_rules.parse_order_chance_response(valid_doc_shaped_response, requested_market="BTC_KRW")


def test_parse_order_chance_response_does_not_depend_on_public_market_registry(valid_doc_shaped_response):
    parsed = order_rules.parse_order_chance_response(valid_doc_shaped_response, requested_market="KRW-BTC")

    assert parsed.market_id == "KRW-BTC"


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
    assert not hasattr(rules, "min_notional_krw")


def test_derive_order_rules_from_chance_preserves_side_specific_order_types() -> None:
    payload = _doc_order_chance_payload()
    payload["market"] = payload["market"] | {
        "order_types": ["limit"],
        "bid_types": ["limit", "price"],
        "ask_types": ["limit", "market"],
    }

    response = order_rules.parse_order_chance_response(payload, requested_market="KRW-BTC")
    rules = order_rules.derive_order_rules_from_chance(response)

    assert rules.order_types == ("limit",)
    assert rules.bid_types == ("limit", "price")
    assert rules.ask_types == ("limit", "market")


def test_fetch_exchange_order_rules_fails_on_market_id_mismatch(monkeypatch, market_mismatch_response):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: market_mismatch_response})(),
    )

    with pytest.raises(order_rules.OrderChanceMarketMismatchError, match="market.id mismatch"):
        order_rules.fetch_exchange_order_rules("KRW-BTC")


def test_fetch_exchange_order_rules_rejects_noncanonical_request_market(monkeypatch, valid_doc_shaped_response):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: valid_doc_shaped_response})(),
    )
    with pytest.raises(order_rules.OrderChanceSchemaError, match="must be canonical QUOTE-BASE"):
        order_rules.fetch_exchange_order_rules("BTC_KRW")


def test_fetch_exchange_order_rules_fails_when_required_field_is_missing(monkeypatch, missing_required_field_response):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: missing_required_field_response})(),
    )

    with pytest.raises(order_rules.OrderChanceSchemaError, match="response.market.bid.min_total"):
        order_rules.fetch_exchange_order_rules("KRW-BTC")


def test_fetch_exchange_order_rules_fails_when_decimal_field_is_malformed(monkeypatch, malformed_decimal_field_response):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: malformed_decimal_field_response})(),
    )

    with pytest.raises(order_rules.OrderChanceSchemaError, match="response.market.ask.price_unit must be numeric"):
        order_rules.fetch_exchange_order_rules("KRW-BTC")


def test_fetch_exchange_order_rules_ignores_extra_undocumented_fields(monkeypatch, extra_undocumented_field_response):
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: extra_undocumented_field_response})(),
    )

    rules = order_rules.fetch_exchange_order_rules("KRW-BTC")

    assert rules.bid_min_total_krw == 5000.0
    assert rules.ask_min_total_krw == 5000.0


def test_get_effective_order_rules_reports_schema_violation_even_with_manual_fallback(monkeypatch, missing_required_field_response):
    order_rules._cached_rules.clear()

    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0002)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0005)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 6000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 5)

    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: missing_required_field_response})(),
    )

    warnings: list[str] = []
    monkeypatch.setattr(order_rules, "notify", lambda msg: warnings.append(msg))

    resolved = order_rules.get_effective_order_rules("KRW-BTC")

    assert resolved.rules.min_notional_krw == 6000.0
    assert resolved.source["min_notional_krw"] == "local_fallback"
    assert warnings
    assert "OrderChanceSchemaError" in warnings[0]
    assert "response.market.bid.min_total" in warnings[0]


def test_get_effective_order_rules_rejects_invalid_live_fallback_config(monkeypatch):
    order_rules._cached_rules.clear()

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(BrokerRejectError, match="live order rule fallback invalid"):
        order_rules.get_effective_order_rules("KRW-BTC")


def test_get_effective_order_rules_uses_auto_values_when_metadata_available(monkeypatch):
    order_rules._cached_rules.clear()

    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.ExchangeDerivedConstraints(
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
        ),
    )

    resolved = order_rules.get_effective_order_rules("KRW-BTC")

    assert resolved.rules.min_qty == 0.0001
    assert resolved.rules.qty_step == 0.0001
    assert resolved.rules.min_notional_krw == 5000.0
    assert resolved.rules.max_qty_decimals == 4
    assert resolved.source["min_qty"] == "local_fallback"
    assert resolved.source["qty_step"] == "local_fallback"
    assert resolved.source["min_notional_krw"] == "local_fallback"
    assert resolved.source["max_qty_decimals"] == "local_fallback"
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
    assert resolved.source["ruleset"] == "merged"


def test_get_effective_order_rules_marks_price_unit_source_missing_when_absent(monkeypatch):
    order_rules._cached_rules.clear()

    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)

    payload = _doc_order_chance_payload()
    payload["market"]["bid"] = {"min_total": "5000"}
    payload["market"]["ask"] = {"min_total": "5000"}
    monkeypatch.setattr(
        order_rules,
        "BithumbBroker",
        lambda: type("_StubBroker", (), {"get_order_chance": lambda _self, market: payload | {"market": payload["market"] | {"id": market}}})(),
    )

    resolved = order_rules.get_effective_order_rules("KRW-BTC")

    assert resolved.rules.bid_min_total_krw == 5000.0
    assert resolved.rules.ask_min_total_krw == 5000.0
    assert resolved.rules.bid_price_unit == 0.0
    assert resolved.rules.ask_price_unit == 0.0
    assert resolved.source["bid_min_total_krw"] == "chance_doc"
    assert resolved.source["ask_min_total_krw"] == "chance_doc"
    assert resolved.source["order_types"] == "chance_doc"
    assert resolved.source["order_sides"] == "chance_doc"
    assert resolved.source["bid_fee"] == "chance_doc"
    assert resolved.source["ask_fee"] == "chance_doc"
    assert resolved.source["maker_bid_fee"] == "chance_doc"
    assert resolved.source["maker_ask_fee"] == "chance_doc"
    assert resolved.source["bid_price_unit"] == "missing"
    assert resolved.source["ask_price_unit"] == "missing"


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

    resolved = order_rules.get_effective_order_rules("KRW-BTC")

    assert resolved.rules.min_qty == 0.0002
    assert resolved.rules.qty_step == 0.0005
    assert resolved.rules.min_notional_krw == 6000.0
    assert resolved.rules.max_qty_decimals == 5
    assert resolved.source["min_qty"] == "local_fallback"
    assert resolved.source["qty_step"] == "local_fallback"
    assert resolved.source["min_notional_krw"] == "local_fallback"
    assert resolved.source["max_qty_decimals"] == "local_fallback"
    assert resolved.source["bid_min_total_krw"] == "unsupported_by_doc"
    assert resolved.source["ruleset"] == "merged"
    assert resolved.fallback_used is True
    assert resolved.fallback_reason_code == "UNRECOVERABLE"
    assert resolved.fallback_reason_summary == "unclassified private API failure; operator investigation required"
    assert "RuntimeError: boom" in resolved.fallback_reason_detail
    assert "order-rule auto-sync unavailable" in resolved.fallback_risk
    assert warnings
    assert "using local fallback only" in warnings[0]
    assert "reason_code=UNRECOVERABLE" in warnings[0]
    assert "risk=order-rule auto-sync unavailable" in warnings[0]


def test_get_effective_order_rules_cached_result_preserves_source_metadata(monkeypatch):
    order_rules._cached_rules.clear()
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0003)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0007)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 9000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 6)

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.ExchangeDerivedConstraints(
            market_id="KRW-BTC",
            bid_min_total_krw=7000.0,
            ask_min_total_krw=7100.0,
            bid_price_unit=1.0,
            ask_price_unit=1.0,
            order_types=("limit",),
            order_sides=("ask", "bid"),
            bid_fee=0.0025,
            ask_fee=0.0025,
            maker_bid_fee=0.0020,
            maker_ask_fee=0.0020,
        ),
    )

    first = order_rules.get_effective_order_rules("KRW-BTC")
    second = order_rules.get_effective_order_rules("KRW-BTC")

    assert first.source["bid_min_total_krw"] == "chance_doc"
    assert second.source["bid_min_total_krw"] == "chance_doc"
    assert first.source["min_qty"] == "local_fallback"
    assert second.source["min_qty"] == "local_fallback"


def test_get_effective_order_rules_persists_durable_snapshot(monkeypatch, tmp_path):
    order_rules._cached_rules.clear()
    object.__setattr__(settings, "DB_PATH", str((tmp_path / "rules.sqlite").resolve()))
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0003)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0007)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 9000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 6)

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.ExchangeDerivedConstraints(
            market_id="KRW-BTC",
            bid_min_total_krw=7000.0,
            ask_min_total_krw=7100.0,
            bid_price_unit=1.0,
            ask_price_unit=1.0,
            order_types=("limit",),
            order_sides=("ask", "bid"),
            bid_fee=0.0025,
            ask_fee=0.0025,
            maker_bid_fee=0.0020,
            maker_ask_fee=0.0020,
        ),
    )

    resolved = order_rules.get_effective_order_rules("KRW-BTC")
    conn = ensure_db(str((tmp_path / "rules.sqlite").resolve()))
    try:
        snapshot = fetch_latest_order_rule_snapshot(conn, market="KRW-BTC")
    finally:
        conn.close()

    assert resolved.snapshot_persisted is True
    assert snapshot is not None
    assert snapshot.market == "KRW-BTC"
    assert snapshot.source_mode == "merged"
    assert snapshot.fallback_used is False
    assert '"bid_min_total_krw":7000.0' in snapshot.rules_json
    assert '"min_qty":"local_fallback"' in snapshot.source_json
    assert '"source_mode":"merged"' in snapshot.source_json


def test_get_effective_order_rules_separates_exchange_and_local_fallback_provenance(monkeypatch, tmp_path):
    order_rules._cached_rules.clear()

    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.ExchangeDerivedConstraints(
            market_id="KRW-BTC",
            bid_min_total_krw=7000.0,
            ask_min_total_krw=7100.0,
            bid_price_unit=1.0,
            ask_price_unit=1.0,
            order_types=("limit", "price", "market"),
            order_sides=("ask", "bid"),
            bid_fee=0.0025,
            ask_fee=0.0025,
            maker_bid_fee=0.0020,
            maker_ask_fee=0.0020,
        ),
    )

    resolved = order_rules.get_effective_order_rules("KRW-BTC")

    assert resolved.source_mode == "merged"
    assert resolved.exchange_source["bid_min_total_krw"] == "chance_doc"
    assert resolved.exchange_source["ask_price_unit"] == "chance_doc"
    assert resolved.local_fallback_source["min_qty"] == "local_fallback"
    assert resolved.local_fallback_source["qty_step"] == "local_fallback"
    assert resolved.source["ruleset"] == "merged"
    assert "exchange_source_json" in resolved.source
    assert "local_fallback_source_json" in resolved.source


def test_side_min_total_krw_prefers_bid_ask_from_doc() -> None:
    rules = order_rules.OrderRules(
        bid_min_total_krw=5500.0,
        ask_min_total_krw=5000.0,
        min_notional_krw=7000.0,
    )

    assert order_rules.side_min_total_krw(rules=rules, side="BUY") == 5500.0
    assert order_rules.side_min_total_krw(rules=rules, side="SELL") == 5000.0
    assert order_rules.side_min_total_krw(rules=rules, side="UNKNOWN") == 7000.0


def test_rule_source_for_defaults_to_missing_for_unknown_values() -> None:
    assert order_rules.rule_source_for("bid_min_total_krw", None) == "missing"
    assert order_rules.rule_source_for("bid_min_total_krw", {"bid_min_total_krw": "auto"}) == "missing"


def test_required_rule_source_issues_requires_chance_doc_for_side_constraints() -> None:
    issues = order_rules.required_rule_source_issues(
        {
            "bid_min_total_krw": "unsupported_by_doc",
            "ask_min_total_krw": "missing",
            "bid_price_unit": "chance_doc",
            "ask_price_unit": "local_fallback",
        }
    )
    assert len(issues) == 3
    assert "bid_min_total_krw source must be chance_doc" in issues[0]
    assert "ask_min_total_krw source must be chance_doc" in issues[1]

    relaxed = order_rules.required_rule_source_issues(
        {
            "bid_min_total_krw": "chance_doc",
            "ask_min_total_krw": "chance_doc",
            "bid_price_unit": "missing",
            "ask_price_unit": "local_fallback",
        },
        require_price_unit_sources=False,
    )
    assert relaxed == []


def test_optional_rule_source_warnings_reports_price_unit_gaps() -> None:
    warnings = order_rules.optional_rule_source_warnings(
        {
            "bid_price_unit": "missing",
            "ask_price_unit": "local_fallback",
        }
    )
    assert len(warnings) == 2
    assert "bid_price_unit source is missing" in warnings[0]
    assert "ask_price_unit source is local_fallback" in warnings[1]


def test_side_price_unit_and_limit_price_normalization_are_side_aware() -> None:
    rules = order_rules.OrderRules(
        bid_price_unit=10.0,
        ask_price_unit=0.5,
    )

    assert order_rules.side_price_unit(rules=rules, side="BUY") == 10.0
    assert order_rules.side_price_unit(rules=rules, side="SELL") == 0.5
    assert order_rules.side_price_unit(rules=rules, side="UNKNOWN") == 0.0

    assert order_rules.normalize_limit_price_for_side(price=1003.0, side="BUY", rules=rules) == 1000.0
    assert order_rules.normalize_limit_price_for_side(price=1003.0, side="SELL", rules=rules) == 1003.0


def test_validate_order_chance_support_rejects_unsupported_side_and_type() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit",),
        order_sides=("bid",),
    )

    with pytest.raises(BrokerRejectError, match="rejected order side before submit"):
        order_rules.validate_order_chance_support(rules=rules, side="SELL", order_type="limit")

    with pytest.raises(BrokerRejectError, match="rejected order type before submit"):
        order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="market")


def test_validate_order_chance_support_allows_supported_side_and_type() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "price", "market"),
        bid_types=("limit", "price", "market"),
        ask_types=("limit", "price", "market"),
        order_sides=("bid", "ask"),
    )

    order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="price")
    order_rules.validate_order_chance_support(rules=rules, side="SELL", order_type="market")


def test_validate_order_chance_support_blocks_buy_price_none_when_chance_only_advertises_market() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "market"),
        order_sides=("bid", "ask"),
    )

    with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support"):
        order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="price")

    with pytest.raises(BrokerRejectError, match="rejected order type before submit"):
        order_rules.validate_order_chance_support(rules=rules, side="SELL", order_type="price")


def test_validate_order_chance_support_prefers_side_specific_order_types() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit",),
        bid_types=("limit", "price"),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
    )

    order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="price")
    order_rules.validate_order_chance_support(rules=rules, side="SELL", order_type="market")

    with pytest.raises(BrokerRejectError, match="rejected order type before submit"):
        order_rules.validate_order_chance_support(rules=rules, side="SELL", order_type="price")


def test_validate_order_chance_support_uses_shared_market_fallback_only_when_bid_types_missing() -> None:
    side_specific_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "market"),
        bid_types=("limit",),
        order_sides=("bid", "ask"),
    )

    with pytest.raises(BrokerRejectError, match="buy_price_none_unsupported"):
        order_rules.validate_order_chance_support(rules=side_specific_rules, side="BUY", order_type="price")

    fallback_rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "market"),
        order_sides=("bid", "ask"),
    )

    with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support"):
        order_rules.validate_order_chance_support(rules=fallback_rules, side="BUY", order_type="price")


def test_resolve_buy_price_none_resolution_default_mode_is_fail_closed() -> None:
    cases = (
        (("price",), True, "", "raw"),
        (("limit", "price"), True, "", "raw"),
        (("limit",), False, "buy_price_none_unsupported", "raw"),
        (("market",), False, "buy_price_none_requires_explicit_price_support", "raw"),
    )

    for bid_types, allowed, block_reason, decision_basis in cases:
        resolution = order_rules.resolve_buy_price_none_resolution(
            rules=order_rules.DerivedOrderConstraints(
                bid_types=bid_types,
                order_sides=("bid", "ask"),
            )
        )
        assert resolution.allowed is allowed
        assert resolution.alias_used is False
        assert resolution.alias_policy == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
        assert resolution.resolved_order_type == "price"
        assert resolution.block_reason == block_reason
        assert resolution.decision_basis == decision_basis


def test_buy_price_none_market_only_default_policy_blocks_without_alias_exception() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("limit", "market"),
        bid_types=("market",),
        order_sides=("bid", "ask"),
    )

    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)
    diagnostic_fields = order_rules.build_buy_price_none_diagnostic_fields(
        rules=rules,
        resolution=resolution,
    )
    submit_context = order_rules.build_buy_price_none_submit_contract_context(
        rules=rules,
        resolution=resolution,
    )

    assert resolution.allowed is False
    assert resolution.alias_used is False
    assert resolution.alias_policy == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert resolution.block_reason == "buy_price_none_requires_explicit_price_support"
    assert diagnostic_fields["alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert submit_context["buy_price_none_alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY

    with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support"):
        order_rules.validate_order_chance_support(
            rules=rules,
            side="BUY",
            order_type="price",
            buy_price_none_resolution=resolution,
        )


@pytest.mark.parametrize(
    ("rules", "support_source", "raw_supported_types"),
    (
        (
            order_rules.DerivedOrderConstraints(
                order_types=("limit", "market"),
                bid_types=("market",),
                order_sides=("bid", "ask"),
            ),
            "bid_types",
            ("market",),
        ),
        (
            order_rules.DerivedOrderConstraints(
                order_types=("limit", "market"),
                order_sides=("bid", "ask"),
            ),
            "order_types",
            ("limit", "market"),
        ),
    ),
    ids=("side_specific_market_only", "shared_market_only_fallback"),
)
def test_buy_price_none_market_only_has_no_enabled_compatibility_path(
    rules: order_rules.DerivedOrderConstraints,
    support_source: str,
    raw_supported_types: tuple[str, ...],
) -> None:
    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)
    diagnostic_fields = order_rules.build_buy_price_none_diagnostic_fields(
        rules=rules,
        resolution=resolution,
    )
    submit_context = order_rules.build_buy_price_none_submit_contract_context(
        rules=rules,
        resolution=resolution,
    )

    assert resolution.allowed is False
    assert resolution.decision_basis == "raw"
    assert resolution.alias_used is False
    assert resolution.alias_policy == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert resolution.block_reason == "buy_price_none_requires_explicit_price_support"
    assert resolution.raw_supported_types == raw_supported_types
    assert resolution.support_source == support_source
    assert diagnostic_fields["alias_used"] is False
    assert diagnostic_fields["alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert diagnostic_fields["support_source"] == support_source
    assert submit_context["buy_price_none_allowed"] is False
    assert submit_context["buy_price_none_decision_outcome"] == "block"
    assert submit_context["buy_price_none_alias_used"] is False
    assert submit_context["buy_price_none_alias_policy"] == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert submit_context["buy_price_none_support_source"] == support_source
    assert submit_context["buy_price_none_raw_supported_types"] == list(raw_supported_types)


def test_buy_price_none_default_path_does_not_allow_implicit_market_alias_without_gate() -> None:
    rules = order_rules.DerivedOrderConstraints(
        order_types=("market",),
        order_sides=("bid", "ask"),
    )

    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)
    diagnostic_fields = order_rules.build_buy_price_none_diagnostic_fields(
        rules=rules,
        resolution=resolution,
    )
    submit_context = order_rules.build_buy_price_none_submit_contract_context(
        rules=rules,
        resolution=resolution,
    )

    assert resolution.allowed is False
    assert resolution.decision_basis == "raw"
    assert resolution.alias_used is False
    assert resolution.alias_policy == order_rules.BUY_PRICE_NONE_ALIAS_POLICY
    assert resolution.block_reason == "buy_price_none_requires_explicit_price_support"
    assert resolution.support_source == "order_types"
    assert resolution.raw_supported_types == ("market",)
    assert diagnostic_fields["allowed"] is False
    assert diagnostic_fields["alias_used"] is False
    assert diagnostic_fields["support_source"] == "order_types"
    assert submit_context["buy_price_none_allowed"] is False
    assert submit_context["buy_price_none_decision_outcome"] == "block"
    assert submit_context["buy_price_none_alias_used"] is False
    assert submit_context["buy_price_none_support_source"] == "order_types"
    assert submit_context["buy_price_none_raw_supported_types"] == ["market"]

    with pytest.raises(BrokerRejectError, match="buy_price_none_requires_explicit_price_support"):
        order_rules.validate_order_chance_support(
            rules=rules,
            side="BUY",
            order_type="price",
            buy_price_none_resolution=resolution,
        )


@pytest.mark.parametrize(
    ("bid_types", "allowed", "block_reason"),
    (
        (("price",), True, ""),
        (("limit", "price"), True, ""),
        (("limit",), False, "buy_price_none_unsupported"),
        (("market",), False, "buy_price_none_requires_explicit_price_support"),
    ),
)
def test_validate_order_chance_support_buy_price_none_default_matrix(
    bid_types: tuple[str, ...],
    allowed: bool,
    block_reason: str,
) -> None:
    rules = order_rules.DerivedOrderConstraints(
        bid_types=bid_types,
        order_sides=("bid", "ask"),
    )

    if allowed:
        order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="price")
        return

    with pytest.raises(BrokerRejectError, match=block_reason):
        order_rules.validate_order_chance_support(rules=rules, side="BUY", order_type="price")


@pytest.mark.parametrize(
    ("rules", "expected_allowed", "expected_block_reason"),
    (
        (
            order_rules.DerivedOrderConstraints(
                order_types=("limit",),
                bid_types=("limit", "price"),
                order_sides=("bid", "ask"),
            ),
            True,
            "",
        ),
        (
            order_rules.DerivedOrderConstraints(
                order_types=("limit", "market"),
                bid_types=("market",),
                order_sides=("bid", "ask"),
            ),
            False,
            "buy_price_none_requires_explicit_price_support",
        ),
    ),
    ids=("allow_explicit_price_support", "block_market_only_support"),
)
def test_buy_price_none_diagnostic_fields_share_submit_contract_decision(
    rules: order_rules.DerivedOrderConstraints,
    expected_allowed: bool,
    expected_block_reason: str,
) -> None:
    resolution = order_rules.resolve_buy_price_none_resolution(rules=rules)
    diagnostic_fields = order_rules.build_buy_price_none_diagnostic_fields(
        rules=rules,
        resolution=resolution,
    )
    submit_context = order_rules.build_buy_price_none_submit_contract_context(
        rules=rules,
        resolution=resolution,
    )

    assert diagnostic_fields["raw_buy_supported_types"] == submit_context["buy_price_none_raw_supported_types"]
    assert diagnostic_fields["support_source"] == submit_context["buy_price_none_support_source"]
    assert diagnostic_fields["resolved_order_type"] == submit_context["buy_price_none_resolved_order_type"]
    assert diagnostic_fields["submit_field"] == submit_context["exchange_submit_field"]
    assert diagnostic_fields["allowed"] is expected_allowed
    assert diagnostic_fields["allowed"] == submit_context["buy_price_none_allowed"]
    assert diagnostic_fields["decision_outcome"] == submit_context["buy_price_none_decision_outcome"]
    assert diagnostic_fields["decision_basis"] == submit_context["buy_price_none_decision_basis"]
    assert diagnostic_fields["alias_used"] == submit_context["buy_price_none_alias_used"]
    assert diagnostic_fields["alias_policy"] == submit_context["buy_price_none_alias_policy"]
    assert diagnostic_fields["block_reason"] == (expected_block_reason or "-")
    assert diagnostic_fields["block_reason"] == (submit_context["buy_price_none_block_reason"] or "-")
