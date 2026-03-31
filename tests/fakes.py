from __future__ import annotations

from bithumb_bot.public_api_orderbook import BestQuote


class FakeMarketData:
    """Minimal deterministic market-data fixture for hermetic tests."""

    def __init__(self, *, bid: float = 100.0, ask: float = 101.0) -> None:
        self.bid = float(bid)
        self.ask = float(ask)

    def fetch_orderbook_top(self, pair: str) -> BestQuote:
        return BestQuote(market=pair, bid_price=self.bid, ask_price=self.ask)
