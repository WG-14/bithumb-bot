from __future__ import annotations

import pytest

from bithumb_bot.public_api import PublicApiSchemaError
from bithumb_bot.public_api_orderbook import (
    BestQuote,
    OrderbookSnapshot,
    OrderbookUnit,
    extract_top_quotes,
    fetch_orderbook_top,
    fetch_orderbook_tops,
    fetch_orderbook_snapshot,
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

    def _fake_get_public_json_with_retry(_client, path, params, max_retries):
        assert path == "/v1/orderbook"
        assert params == {"markets": "KRW-BTC"}
        assert max_retries == 3
        return payload

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", _fake_get_public_json_with_retry)
    assert fetch_orderbook_snapshot(client=object(), market="KRW-BTC") == OrderbookSnapshot(
        market="KRW-BTC",
        orderbook_units=(OrderbookUnit(bid_price=100.0, ask_price=101.0),),
    )
    assert fetch_orderbook_top(client=object(), market="KRW-BTC") == [
        BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0)
    ]


def test_fetch_orderbook_snapshots_serializes_multi_markets(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]},
        {"market": "KRW-ETH", "orderbook_units": [{"ask_price": "201.0", "bid_price": "200.0"}]},
    ]

    captured: dict[str, object] = {}

    def _fake_get_public_json_with_retry(_client, path, params, max_retries):
        captured["path"] = path
        captured["params"] = params
        captured["max_retries"] = max_retries
        return payload

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", _fake_get_public_json_with_retry)

    snapshots = fetch_orderbook_snapshots(client=object(), markets=["KRW-BTC", "KRW-ETH", "KRW-BTC"])

    assert captured["path"] == "/v1/orderbook"
    assert captured["params"] == {"markets": "KRW-BTC,KRW-ETH"}
    assert captured["max_retries"] == 3
    assert [s.market for s in snapshots] == ["KRW-BTC", "KRW-ETH"]


def test_fetch_orderbook_snapshots_rejects_when_requested_market_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = [{"market": "KRW-ETH", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]}]

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: payload)

    with pytest.raises(PublicApiSchemaError, match="orderbook response market mismatch") as exc:
        fetch_orderbook_snapshots(client=object(), markets=["KRW-BTC", "KRW-ETH"])

    message = str(exc.value)
    assert "endpoint=/v1/orderbook" in message
    assert "requested_markets=['KRW-BTC', 'KRW-ETH']" in message
    assert "returned_markets=['KRW-ETH']" in message


def test_fetch_orderbook_snapshots_rejects_multiple_returned_markets(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]},
        {"market": "KRW-ETH", "orderbook_units": [{"ask_price": "201.0", "bid_price": "200.0"}]},
    ]

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: payload)

    with pytest.raises(PublicApiSchemaError, match="orderbook response market mismatch") as exc:
        fetch_orderbook_snapshots(client=object(), markets=["KRW-BTC"])

    assert "returned_count=2" in str(exc.value)


def test_fetch_orderbook_snapshots_rejects_duplicate_return_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]},
        {"market": "BTC_KRW", "orderbook_units": [{"ask_price": "102.0", "bid_price": "99.0"}]},
    ]

    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: payload)

    with pytest.raises(PublicApiSchemaError, match="schema validation failed"):
        fetch_orderbook_snapshots(client=object(), markets=["KRW-BTC"])


def test_fetch_orderbook_snapshots_rejects_empty_response_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: [])

    with pytest.raises(PublicApiSchemaError, match="expected=non-empty list where=root"):
        fetch_orderbook_snapshots(client=object(), markets=["KRW-BTC"])


def test_fetch_orderbook_top_rejects_mixed_market_response(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]},
        {"market": "KRW-XRP", "orderbook_units": [{"ask_price": "51.0", "bid_price": "50.0"}]},
    ]
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: payload)

    with pytest.raises(PublicApiSchemaError, match="orderbook response market mismatch"):
        fetch_orderbook_top(client=object(), market="KRW-BTC")


def test_fetch_orderbook_tops_multi_market(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        {"market": "KRW-BTC", "orderbook_units": [{"ask_price": "101.0", "bid_price": "100.0"}]},
        {"market": "KRW-ETH", "orderbook_units": [{"ask_price": "201.0", "bid_price": "200.0"}]},
    ]
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: payload)

    tops = fetch_orderbook_tops(client=object(), markets=["KRW-BTC", "KRW-ETH"])
    assert tops == [
        BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0),
        BestQuote(market="KRW-ETH", bid_price=200.0, ask_price=201.0),
    ]


def test_fetch_orderbook_snapshots_rejects_noncanonical_requested_market(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: [])
    with pytest.raises(ValueError, match="canonical QUOTE-BASE"):
        fetch_orderbook_snapshots(client=object(), markets=["BTC_KRW"])

def test_fetch_orderbook_snapshots_rejects_bare_symbol_requested_market(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: [])
    with pytest.raises(ValueError, match="canonical QUOTE-BASE"):
        fetch_orderbook_snapshots(client=object(), markets=["BTC"])


def test_fetch_orderbook_snapshots_includes_context_in_schema_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bithumb_bot.public_api_orderbook.get_public_json_with_retry", lambda *_args, **_kwargs: [])

    with pytest.raises(PublicApiSchemaError, match="endpoint=/v1/orderbook") as exc:
        fetch_orderbook_snapshots(client=object(), markets=["KRW-BTC"])

    message = str(exc.value)
    assert "requested_markets=['KRW-BTC']" in message


def test_fetch_orderbook_snapshots_rejects_string_markets_argument() -> None:
    with pytest.raises(TypeError, match="sequence of market identifiers"):
        fetch_orderbook_snapshots(client=object(), markets="KRW-BTC")
