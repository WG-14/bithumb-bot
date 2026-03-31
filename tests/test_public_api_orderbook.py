from __future__ import annotations

import pytest

from bithumb_bot.public_api import PublicApiSchemaError
from bithumb_bot.public_api_orderbook import (
    OrderbookTop,
    parse_orderbook_top,
)


def test_parse_orderbook_top_extracts_best_bid_ask() -> None:
    payload = [
        {
            "market": "KRW-BTC",
            "orderbook_units": [
                {"ask_price": "101.0", "bid_price": "100.0"},
                {"ask_price": "102.0", "bid_price": "99.5"},
            ],
        }
    ]

    snapshots = parse_orderbook_top(payload)

    assert snapshots == [OrderbookTop(market="KRW-BTC", bid_price=100.0, ask_price=101.0)]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        [{"market": "KRW-BTC", "orderbook_units": []}],
        [{"market": "", "orderbook_units": [{"ask_price": 1, "bid_price": 1}]}],
    ],
)
def test_parse_orderbook_top_rejects_schema_mismatch(payload) -> None:
    with pytest.raises(PublicApiSchemaError, match="orderbook schema mismatch"):
        parse_orderbook_top(payload)
