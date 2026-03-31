from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from bithumb_bot.config import settings
from bithumb_bot.marketdata import cmd_candles, cmd_sync
from bithumb_bot.marketdata import fetch_orderbook_top as fetch_marketdata_orderbook_top
from bithumb_bot.public_api import PublicApiSchemaError
from bithumb_bot.public_api_orderbook import BestQuote
from bithumb_bot.public_api_minute_candles import MinuteCandle


class _DummyClient:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def _settings_guard(tmp_path: Path):
    old_db_path = settings.DB_PATH
    old_pair = settings.PAIR
    old_interval = settings.INTERVAL

    object.__setattr__(settings, "DB_PATH", str(tmp_path / "candles.sqlite"))
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "INTERVAL", "1m")

    try:
        yield
    finally:
        object.__setattr__(settings, "DB_PATH", old_db_path)
        object.__setattr__(settings, "PAIR", old_pair)
        object.__setattr__(settings, "INTERVAL", old_interval)


def _sample_candle(*, utc: str, timestamp: int = 1_743_379_299_999) -> MinuteCandle:
    return MinuteCandle(
        market="KRW-BTC",
        candle_date_time_utc=utc,
        candle_date_time_kst="2026-03-31T09:00:00",
        opening_price=101.0,
        high_price=110.0,
        low_price=99.0,
        trade_price=108.0,
        timestamp=timestamp,
        candle_acc_trade_price=15000.0,
        candle_acc_trade_volume=0.321,
    )


def test_cmd_sync_uses_minute_candle_layer_and_maps_db_fields(monkeypatch, _settings_guard) -> None:
    candles = [
        _sample_candle(utc="2026-03-31T00:00:00", timestamp=1_111_111_111_111),
        _sample_candle(utc="2026-03-31T00:01:00", timestamp=1_111_111_199_999),
    ]

    called: dict[str, object] = {}

    def fake_fetch(client, *, market: str, minute_unit: int, count: int, to=None):
        called["market"] = market
        called["minute_unit"] = minute_unit
        called["count"] = count
        called["to"] = to
        return candles

    monkeypatch.setattr("bithumb_bot.marketdata.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.marketdata.to_v1_market", lambda pair: "KRW-BTC")
    monkeypatch.setattr("bithumb_bot.marketdata.fetch_minute_candles", fake_fetch)

    cmd_sync(quiet=True, limit=2)

    assert called == {
        "market": "KRW-BTC",
        "minute_unit": 1,
        "count": 2,
        "to": None,
    }

    with sqlite3.connect(settings.DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT ts, pair, interval, open, high, low, close, volume
            FROM candles
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()

    assert row is not None
    # ts uses normalized candle bucket time (candle_date_time_utc), not trade tick timestamp.
    assert int(row[0]) == 1_774_915_260_000
    assert row[1] == "KRW-BTC"
    assert row[2] == "1m"
    assert float(row[3]) == 101.0
    assert float(row[4]) == 110.0
    assert float(row[5]) == 99.0
    assert float(row[6]) == 108.0
    assert float(row[7]) == 0.321


def test_cmd_sync_handles_empty_candle_response(monkeypatch, _settings_guard) -> None:
    monkeypatch.setattr("bithumb_bot.marketdata.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.marketdata.to_v1_market", lambda pair: "KRW-BTC")
    monkeypatch.setattr("bithumb_bot.marketdata.fetch_minute_candles", lambda *args, **kwargs: [])

    cmd_sync(quiet=True, limit=10)

    assert not Path(settings.DB_PATH).exists()


def test_cmd_sync_propagates_schema_mismatch(monkeypatch, _settings_guard) -> None:
    monkeypatch.setattr("bithumb_bot.marketdata.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.marketdata.to_v1_market", lambda pair: "KRW-BTC")

    def bad_schema(*args, **kwargs):
        raise PublicApiSchemaError("minute candle schema mismatch")

    monkeypatch.setattr("bithumb_bot.marketdata.fetch_minute_candles", bad_schema)

    with pytest.raises(PublicApiSchemaError, match="schema mismatch"):
        cmd_sync(quiet=True, limit=10)


def test_cmd_sync_fails_for_unsupported_interval(monkeypatch, _settings_guard) -> None:
    object.__setattr__(settings, "INTERVAL", "2m")

    with pytest.raises(ValueError, match="unsupported minute interval"):
        cmd_sync(quiet=True, limit=10)


def test_cmd_candles_uses_minute_candle_layer(monkeypatch, capsys, _settings_guard) -> None:
    candles = [_sample_candle(utc="2026-03-31T00:00:00")]

    monkeypatch.setattr("bithumb_bot.marketdata.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.marketdata.to_v1_market", lambda pair: "KRW-BTC")
    monkeypatch.setattr("bithumb_bot.marketdata.fetch_minute_candles", lambda *args, **kwargs: candles)

    cmd_candles(limit=1)

    captured = capsys.readouterr()
    assert "[CANDLES KRW-BTC 1m] last 1" in captured.out
    assert "MinuteCandle" in captured.out


def test_marketdata_orderbook_fetch_uses_public_retry_path(monkeypatch, _settings_guard) -> None:
    captured: dict[str, object] = {}

    def _fake_orderbook_fetch(client, *, market: str, max_retries: int):
        captured["client_timeout"] = client.timeout
        captured["market"] = market
        captured["max_retries"] = max_retries
        return [BestQuote(market=market, bid_price=100.0, ask_price=101.0)]

    monkeypatch.setattr("bithumb_bot.marketdata.fetch_public_orderbook_top", _fake_orderbook_fetch)
    monkeypatch.setattr("bithumb_bot.marketdata.to_v1_market", lambda _pair: "KRW-BTC")

    quote = fetch_marketdata_orderbook_top("BTC_KRW")
    assert quote.market == "KRW-BTC"
    assert quote.bid_price == 100.0
    assert quote.ask_price == 101.0
    assert quote.observed_at_epoch_sec is not None
    assert quote.source == "bithumb_public_v1_orderbook"
    assert captured["market"] == "KRW-BTC"
    assert int(float(captured["client_timeout"].connect)) == 10
    assert captured["max_retries"] == 3


def test_marketdata_orderbook_fetch_fails_when_public_quote_market_mismatch(monkeypatch, _settings_guard) -> None:
    monkeypatch.setattr(
        "bithumb_bot.marketdata.fetch_public_orderbook_top",
        lambda *_args, **_kwargs: [BestQuote(market="KRW-ETH", bid_price=100.0, ask_price=101.0)],
    )
    monkeypatch.setattr("bithumb_bot.marketdata.to_v1_market", lambda _pair: "KRW-BTC")

    with pytest.raises(RuntimeError, match="market mismatch"):
        fetch_marketdata_orderbook_top("BTC_KRW")
