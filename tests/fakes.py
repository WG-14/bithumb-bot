from __future__ import annotations


class FakeMarketData:
    """Minimal deterministic market-data fixture for hermetic tests."""

    def __init__(self, *, bid: float = 100.0, ask: float = 101.0) -> None:
        self.bid = float(bid)
        self.ask = float(ask)

    def fetch_orderbook_top(self, _pair: str) -> tuple[float, float]:
        return (self.bid, self.ask)
