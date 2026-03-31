from __future__ import annotations

import httpx
import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.order_rules import build_order_rules_market
from bithumb_bot.config import settings
from bithumb_bot.marketdata import fetch_orderbook_top, validated_best_quote_ask_price
from bithumb_bot.public_api_orderbook import BestQuote


class _OrderbookClient:
    requests: list[dict[str, object]] = []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, path: str, params=None):
        type(self).requests.append({"path": path, "params": params})
        req = httpx.Request("GET", f"https://api.bithumb.com{path}")
        return httpx.Response(
            200,
            json=[{"market": "KRW-BTC", "orderbook_units": [{"bid_price": "100.0", "ask_price": "101.0"}]}],
            request=req,
        )


def test_fetch_orderbook_top_uses_canonical_market_source(monkeypatch):
    _OrderbookClient.requests = []
    monkeypatch.setattr("httpx.Client", _OrderbookClient)
    monkeypatch.setattr("bithumb_bot.marketdata.canonical_market_id", lambda market: "KRW-BTC")

    quote = fetch_orderbook_top("btc_krw")

    assert quote.bid_price == 100.0
    assert quote.ask_price == 101.0
    assert quote.market == "KRW-BTC"
    assert _OrderbookClient.requests == [{"path": "/v1/orderbook", "params": {"markets": "KRW-BTC"}}]


def test_validated_best_quote_ask_price_returns_ask_for_matching_market() -> None:
    quote = BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0)
    assert validated_best_quote_ask_price(quote, requested_market="btc_krw") == 101.0


def test_validated_best_quote_ask_price_rejects_non_positive_ask() -> None:
    quote = BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=0.0)
    with pytest.raises(RuntimeError, match="invalid ask"):
        validated_best_quote_ask_price(quote, requested_market="KRW-BTC")


def test_build_order_rules_market_uses_canonical_market_source(monkeypatch):
    seen: list[str] = []

    def _fake_canonical(market: str) -> str:
        seen.append(market)
        return "KRW-BTC"

    monkeypatch.setattr("bithumb_bot.broker.order_rules.canonical_market_id", _fake_canonical)

    assert build_order_rules_market("btc_krw") == "KRW-BTC"
    assert seen == ["btc_krw"]


def test_broker_order_chance_and_payload_use_same_canonical_market(monkeypatch):
    original_pair = settings.PAIR
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_key = settings.BITHUMB_API_KEY
    original_secret = settings.BITHUMB_API_SECRET

    object.__setattr__(settings, "PAIR", "btc_krw")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")

    broker = BithumbBroker()
    seen: list[str] = []

    def _fake_canonical(market: str) -> str:
        seen.append(market)
        return "KRW-BTC"

    chance_call: dict[str, object] = {}
    submit_call: dict[str, object] = {}

    def _fake_get(endpoint, params, retry_safe=False):
        chance_call["endpoint"] = endpoint
        chance_call["params"] = params
        return {"market": {"bid": {"min_total": "5000"}}}

    def _fake_post(endpoint, payload, retry_safe=False):
        submit_call["endpoint"] = endpoint
        submit_call["payload"] = payload
        return {"uuid": "order-1"}

    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", _fake_canonical)
    orderbook_markets: list[str] = []

    def _fake_orderbook(market: str) -> BestQuote:
        orderbook_markets.append(market)
        return BestQuote(market=market, bid_price=100.0, ask_price=101.0)

    monkeypatch.setattr("bithumb_bot.broker.bithumb.fetch_orderbook_top", _fake_orderbook)
    monkeypatch.setattr(broker, "_get_private", _fake_get)
    monkeypatch.setattr(broker, "_post_private", _fake_post)

    try:
        broker.get_order_chance()
        broker.place_order(client_order_id="cid-1", side="BUY", qty=1.0, price=None)

        assert chance_call == {"endpoint": "/v1/orders/chance", "params": {"market": "KRW-BTC"}}
        assert submit_call["endpoint"] == "/v2/orders"
        assert submit_call["payload"]["market"] == "KRW-BTC"
        assert seen == ["btc_krw", "btc_krw"]
        assert orderbook_markets == ["KRW-BTC"]
    finally:
        object.__setattr__(settings, "PAIR", original_pair)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "BITHUMB_API_KEY", original_key)
        object.__setattr__(settings, "BITHUMB_API_SECRET", original_secret)


def test_broker_pair_and_market_share_canonical_source(monkeypatch):
    original_pair = settings.PAIR
    object.__setattr__(settings, "PAIR", "btc_krw")
    seen: list[str] = []

    def _fake_canonical(market: str) -> str:
        seen.append(market)
        return "KRW-BTC"

    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", _fake_canonical)
    broker = BithumbBroker()

    try:
        assert broker._market() == "KRW-BTC"
        assert broker._pair() == ("BTC", "KRW")
        assert seen == ["btc_krw", "btc_krw"]
    finally:
        object.__setattr__(settings, "PAIR", original_pair)
