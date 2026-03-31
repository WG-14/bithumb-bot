from __future__ import annotations

import pytest

from bithumb_bot.public_api import PublicApiSchemaError
from bithumb_bot.public_api_orderbook import (
    BestQuote,
    OrderbookSnapshot,
    OrderbookUnit,
    extract_top_quotes,
    fetch_orderbook_top,
    fetch_orderbook_snapshots,
    parse_orderbook_top,
    parse_orderbook_snapshots,
)


def test_parse_orderbook_snapshots_preserves_units() -> None:
    payload = [
        {
            "market": "KRW-BTC",
            "orderbook_units": [
                {"ask_price": "101.0", "bid_price": "100.0"},
                {"ask_price": "102.0", "bid_price": "99.5"},
            ],
        }
    ]

    snapshots = parse_orderbook_snapshots(payload)

    assert snapshots == [
        OrderbookSnapshot(
            market="KRW-BTC",
            orderbook_units=(
                OrderbookUnit(bid_price=100.0, ask_price=101.0),
                OrderbookUnit(bid_price=99.5, ask_price=102.0),
            ),
        )
    ]


def test_parse_orderbook_snapshots_rejects_missing_market() -> None:
    with pytest.raises(PublicApiSchemaError, match="field=market"):
        parse_orderbook_snapshots([{"orderbook_units": [{"ask_price": 1, "bid_price": 1}]}])


def test_parse_orderbook_snapshots_rejects_missing_orderbook_units() -> None:
    with pytest.raises(PublicApiSchemaError, match="where=orderbook_units"):
        parse_orderbook_snapshots([{"market": "KRW-BTC"}])


def test_parse_orderbook_snapshots_rejects_empty_orderbook_units() -> None:
    with pytest.raises(PublicApiSchemaError, match="where=orderbook_units"):
        parse_orderbook_snapshots([{"market": "KRW-BTC", "orderbook_units": []}])


@pytest.mark.parametrize("bad_field", ["bid_price", "ask_price"])
def test_parse_orderbook_snapshots_rejects_non_numeric_prices(bad_field: str) -> None:
    unit = {"ask_price": "101.0", "bid_price": "100.0"}
    unit[bad_field] = "not-a-number"
    with pytest.raises(PublicApiSchemaError, match="orderbook schema mismatch"):
        parse_orderbook_snapshots([{"market": "KRW-BTC", "orderbook_units": [unit]}])


def test_extract_top_quotes_returns_best_quote() -> None:
    snapshots = [
        OrderbookSnapshot(
            market="KRW-BTC",
            orderbook_units=(
                OrderbookUnit(bid_price=100.0, ask_price=101.0),
                OrderbookUnit(bid_price=99.5, ask_price=102.0),
            ),
        )
    ]
    assert extract_top_quotes(snapshots) == [
        BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0)
    ]


def test_parse_orderbook_top_compatibility_wrapper() -> None:
    payload = [{"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]}]
    assert parse_orderbook_top(payload) == [BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0)]


def test_fetch_orderbook_snapshot_and_top_compatibility(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [{"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]}]

    def _fake_get_public_json(_client, path, params):
        assert path == "/v1/orderbook"
        assert params == {"markets": "KRW-BTC"}
        return payload

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json", _fake_get_public_json)
    assert fetch_orderbook_snapshots(client=object(), market="KRW-BTC") == [
        OrderbookSnapshot(
            market="KRW-BTC",
            orderbook_units=(OrderbookUnit(bid_price=100.0, ask_price=101.0),),
        )
    ]
    assert fetch_orderbook_top(client=object(), market="KRW-BTC") == [
        BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0)
    ]


def test_fetch_orderbook_snapshots_accepts_canonical_market_match(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [{"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]}]

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json", lambda *_args, **_kwargs: payload)

    assert fetch_orderbook_snapshots(client=object(), market="btc_krw") == [
        OrderbookSnapshot(
            market="KRW-BTC",
            orderbook_units=(OrderbookUnit(bid_price=100.0, ask_price=101.0),),
        )
    ]


def test_fetch_orderbook_snapshots_rejects_when_requested_market_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = [{"market": "KRW-ETH", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]}]

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json", lambda *_args, **_kwargs: payload)

    with pytest.raises(PublicApiSchemaError, match="orderbook response market mismatch") as exc:
        fetch_orderbook_snapshots(client=object(), market="KRW-BTC")

    message = str(exc.value)
    assert "endpoint=/v1/orderbook" in message
    assert "requested_markets=['KRW-BTC']" in message
    assert "returned_markets=['KRW-ETH']" in message


def test_fetch_orderbook_snapshots_rejects_multiple_returned_markets(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]},
        {"market": "KRW-ETH", "orderbook_units": [{"ask_price": "201.0", "bid_price": "200.0"}]},
    ]

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json", lambda *_args, **_kwargs: payload)

    with pytest.raises(PublicApiSchemaError, match="orderbook response market mismatch") as exc:
        fetch_orderbook_snapshots(client=object(), market="KRW-BTC")

    assert "returned_count=2" in str(exc.value)


def test_fetch_orderbook_snapshots_rejects_empty_response_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json", lambda *_args, **_kwargs: [])

    with pytest.raises(PublicApiSchemaError, match="expected=non-empty list where=root"):
        fetch_orderbook_snapshots(client=object(), market="KRW-BTC")


def test_fetch_orderbook_top_rejects_mixed_market_response(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]},
        {"market": "KRW-XRP", "orderbook_units": [{"ask_price": "51.0", "bid_price": "50.0"}]},
    ]
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json", lambda *_args, **_kwargs: payload)

    with pytest.raises(PublicApiSchemaError, match="orderbook response market mismatch"):
        fetch_orderbook_top(client=object(), market="KRW-BTC")
