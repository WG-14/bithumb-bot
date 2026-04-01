from __future__ import annotations

from collections import deque

import pytest

from bithumb_bot.broker.balance_source import BalanceSnapshot
from bithumb_bot.broker.base import BrokerTemporaryError
from bithumb_bot.broker.myasset_ws import (
    MyAssetSchemaMismatchError,
    MyAssetStreamStaleError,
    MyAssetWsBalanceSource,
    build_myasset_subscribe_request,
    parse_myasset_message,
)


def _valid_payload(*, timestamp: int = 1712123456000, stream_type: str = "snapshot") -> dict[str, object]:
    return {
        "type": "myAsset",
        "stream_type": stream_type,
        "timestamp": timestamp,
        "asset_timestamp": timestamp,
        "assets": [
            {"currency": "KRW", "balance": "1000000", "locked": "1000"},
            {"currency": "BTC", "balance": "0.1", "locked": "0.01"},
        ],
    }


def test_build_myasset_subscribe_request() -> None:
    payload = build_myasset_subscribe_request(ticket="live-balance")
    assert payload == {"type": "myAsset", "ticket": "live-balance"}


def test_parse_myasset_message_success() -> None:
    parsed = parse_myasset_message(_valid_payload(), order_currency="BTC", payment_currency="KRW")
    assert parsed.stream_type == "snapshot"
    assert parsed.timestamp_ms == 1712123456000
    assert float(parsed.pair_balances.cash_balance) == 1_000_000
    assert float(parsed.pair_balances.asset_locked) == 0.01


@pytest.mark.parametrize(
    "payload, pattern",
    [
        ({"type": "ticker"}, "unexpected type"),
        ({"type": "myAsset", "stream_type": "snapshot", "timestamp": 1, "asset_timestamp": 1}, "assets must be array"),
        (_valid_payload(stream_type="invalid"), "invalid stream_type"),
        ({**_valid_payload(), "assets": [{"currency": "KRW", "balance": "abc", "locked": "0"}]}, "invalid numeric field 'balance'"),
        ({**_valid_payload(), "assets": [{"currency": "KRW", "balance": "1", "locked": "-1"}]}, "invalid non-negative numeric field 'locked'"),
    ],
)
def test_parse_myasset_message_validation_errors(payload: dict[str, object], pattern: str) -> None:
    with pytest.raises(MyAssetSchemaMismatchError, match=pattern):
        parse_myasset_message(payload, order_currency="BTC", payment_currency="KRW")


class _FakeConn:
    def __init__(self, messages: list[object], *, fail_once: bool = False) -> None:
        self.messages = deque(messages)
        self.fail_once = fail_once
        self.open_calls = 0
        self.sent: list[dict[str, object]] = []
        self.closed = False

    def open(self) -> None:
        self.open_calls += 1

    def send_json(self, payload: dict[str, object]) -> None:
        self.sent.append(payload)

    def recv_json(self, *, timeout_sec: float) -> object:
        if self.fail_once:
            self.fail_once = False
            raise TimeoutError("simulated disconnect")
        if not self.messages:
            raise TimeoutError(f"timeout({timeout_sec})")
        return self.messages.popleft()

    def close(self) -> None:
        self.closed = True


def test_myasset_balance_source_reconnects_and_resubscribes() -> None:
    first = _FakeConn([_valid_payload()], fail_once=True)
    second = _FakeConn([_valid_payload(timestamp=1712123457000, stream_type="realtime")])
    conns = deque([first, second])

    source = MyAssetWsBalanceSource(
        connection_factory=lambda: conns.popleft(),
        order_currency="BTC",
        payment_currency="KRW",
        now_ms=lambda: 1712123457001,
        stale_after_ms=60_000,
        recv_timeout_sec=1.0,
        subscribe_ticket="ticket-1",
    )

    snap = source.fetch_snapshot()

    assert isinstance(snap, BalanceSnapshot)
    assert snap.observed_ts_ms == 1712123457000
    assert first.sent == [{"type": "myAsset", "ticket": "ticket-1"}]
    assert second.sent == [{"type": "myAsset", "ticket": "ticket-1"}]


def test_myasset_balance_source_stale_stream_detected() -> None:
    source = MyAssetWsBalanceSource(
        connection_factory=lambda: _FakeConn([_valid_payload(timestamp=1000)]),
        order_currency="BTC",
        payment_currency="KRW",
        now_ms=lambda: 1000 + 20_000,
        stale_after_ms=5_000,
        recv_timeout_sec=0.5,
    )

    with pytest.raises(MyAssetStreamStaleError, match="stream stale"):
        source.fetch_snapshot()


def test_myasset_balance_source_wraps_transport_error() -> None:
    source = MyAssetWsBalanceSource(
        connection_factory=lambda: _FakeConn([], fail_once=True),
        order_currency="BTC",
        payment_currency="KRW",
        now_ms=lambda: 1000,
        stale_after_ms=0,
        recv_timeout_sec=0.1,
    )
    with pytest.raises(BrokerTemporaryError, match="websocket snapshot fetch failed"):
        source.fetch_snapshot()
