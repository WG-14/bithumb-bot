from __future__ import annotations

import random
import time
from datetime import UTC, datetime
from dataclasses import replace
import math

import httpx

from .config import settings
from .db_core import ensure_db
from .markets import parse_documented_market_code, canonical_market_id
from .notifier import notify
from .public_api_minute_candles import (
    MinuteCandle,
    fetch_minute_candles,
    interval_to_minute_unit,
)
from .public_api_orderbook import BestQuote, fetch_orderbook_tops as fetch_public_orderbook_tops
from .public_api_ticker import fetch_ticker_single


BASE_URL = "https://api.bithumb.com"

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_HTTP_RETRIES = 4
BASE_BACKOFF_SEC = 1.0
MAX_BACKOFF_SEC = 10.0
JITTER_SEC = 0.5
ORDERBOOK_FETCH_TIMEOUT_SEC = 10.0
ORDERBOOK_FETCH_MAX_RETRIES = 3


def _sleep_backoff(attempt: int) -> None:
    # attempt: 0,1,2... -> 1,2,4... seconds + jitter
    backoff = min(MAX_BACKOFF_SEC, BASE_BACKOFF_SEC * (2 ** attempt))
    backoff += random.uniform(0.0, JITTER_SEC)
    time.sleep(backoff)


def _get_with_retry(client: httpx.Client, path: str, params: dict[str, object] | None = None) -> httpx.Response:
    last_error: Exception | None = None

    for attempt in range(MAX_HTTP_RETRIES + 1):
        try:
            resp = client.get(path, params=params)

            # retryable status는 재시도
            if resp.status_code in RETRYABLE_STATUS_CODES:
                if attempt < MAX_HTTP_RETRIES:
                    notify(
                        f"http retry {attempt + 1}/{MAX_HTTP_RETRIES} "
                        f"status={resp.status_code} path={path}"
                    )
                    _sleep_backoff(attempt)
                    continue
                resp.raise_for_status()

            resp.raise_for_status()
            return resp

        except httpx.RequestError as exc:
            # 네트워크/타임아웃 등
            last_error = exc
            if attempt >= MAX_HTTP_RETRIES:
                break
            notify(
                f"http retry {attempt + 1}/{MAX_HTTP_RETRIES} "
                f"path={path} error={exc}"
            )
            _sleep_backoff(attempt)

        except httpx.HTTPStatusError as exc:
            # raise_for_status()에서 나온 에러(대부분 non-retryable)
            code = exc.response.status_code if exc.response is not None else None
            if code in RETRYABLE_STATUS_CODES and attempt < MAX_HTTP_RETRIES:
                last_error = exc
                notify(
                    f"http retry {attempt + 1}/{MAX_HTTP_RETRIES} "
                    f"status={code} path={path} error={exc}"
                )
                _sleep_backoff(attempt)
                continue
            raise

    raise RuntimeError(f"http request failed after retries: {path}") from last_error


def fetch_orderbook_tops(pairs: list[str]) -> list[BestQuote]:
    markets = [canonical_market_id(pair) for pair in pairs]
    with httpx.Client(base_url=BASE_URL, timeout=ORDERBOOK_FETCH_TIMEOUT_SEC) as c:
        quotes = fetch_public_orderbook_tops(c, markets=markets, max_retries=ORDERBOOK_FETCH_MAX_RETRIES)

    quote_by_market = {parse_documented_market_code(quote.market): quote for quote in quotes}
    resolved: list[BestQuote] = []
    for market in markets:
        key = parse_documented_market_code(market)
        if key not in quote_by_market:
            returned_markets = sorted(quote_by_market.keys())
            raise RuntimeError(
                "orderbook top market mismatch "
                f"requested_market={market!r} returned_markets={returned_markets!r}"
            )
        quote = quote_by_market[key]
        bid = float(quote.bid_price)
        ask = float(quote.ask_price)
        if not math.isfinite(bid) or not math.isfinite(ask) or bid <= 0 or ask <= 0 or bid > ask:
            raise RuntimeError(
                "orderbook top invalid quote "
                f"market={market!r} bid={bid!r} ask={ask!r}"
            )
        resolved.append(
            replace(
                quote,
                bid_price=bid,
                ask_price=ask,
                observed_at_epoch_sec=time.time(),
                source="bithumb_public_v1_orderbook",
            )
        )
    return resolved


def fetch_orderbook_top(pair: str | None = None) -> BestQuote:
    market = canonical_market_id(pair or settings.PAIR)
    return fetch_orderbook_tops([market])[0]


def validated_best_quote_prices(
    quote: BestQuote,
    *,
    requested_market: str | None = None,
) -> tuple[float, float]:
    requested = str(requested_market or quote.market)
    if parse_documented_market_code(quote.market) != parse_documented_market_code(requested):
        raise RuntimeError(
            "orderbook top market mismatch "
            f"requested_market={requested!r} returned_market={quote.market!r}"
        )
    bid = float(quote.bid_price)
    ask = float(quote.ask_price)
    if not math.isfinite(bid) or not math.isfinite(ask) or bid <= 0 or ask <= 0 or bid > ask:
        raise RuntimeError(
            "orderbook top invalid quote "
            f"market={requested!r} bid={bid!r} ask={ask!r}"
        )
    return bid, ask


def validated_best_quote_ask_price(quote: BestQuote, *, requested_market: str | None = None) -> float:
    _, ask = validated_best_quote_prices(quote, requested_market=requested_market)
    return ask


def _candle_key_ts_ms(candle: MinuteCandle) -> int:
    """
    Build canonical DB candle key timestamp (candle bucket start, UTC epoch ms).

    Bithumb minute candle payload includes both:
    - `timestamp`: trade-tick millisecond timestamp within candle window
    - `candle_date_time_utc`: canonical candle bucket time

    The engine and strategy logic treat `candles.ts` as candle bucket start,
    so we must persist `candle_date_time_utc` as the DB key and never the raw
    trade snapshot `timestamp`.
    """
    dt = datetime.fromisoformat(candle.candle_date_time_utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return int(dt.timestamp() * 1000)


def _minute_candle_to_db_row(candle: MinuteCandle) -> tuple[int, str, str, float, float, float, float, float]:
    return (
        _candle_key_ts_ms(candle),
        candle.market,
        settings.INTERVAL,
        candle.opening_price,
        candle.high_price,
        candle.low_price,
        candle.trade_price,
        candle.candle_acc_trade_volume,
    )


def cmd_sync(quiet: bool = False, limit: int = 200) -> None:
    minute_unit = interval_to_minute_unit(settings.INTERVAL)
    market = canonical_market_id(settings.PAIR)
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        candles = fetch_minute_candles(
            client,
            market=market,
            minute_unit=minute_unit,
            count=max(1, int(limit)),
        )

    if not candles:
        if not quiet:
            print("[SYNC] no data")
        return

    rows = candles[-limit:]

    conn = ensure_db()
    try:
        inserted = 0
        for candle in rows:
            cur = conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _minute_candle_to_db_row(candle),
            )
            inserted += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    if not quiet:
        print(f"[SYNC] upserted {len(rows)} rows -> {settings.DB_PATH}")


def cmd_ticker() -> None:
    market = canonical_market_id(settings.PAIR)
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        d = fetch_ticker_single(client, market=market)
    print(
        f"[TICKER {d.market}] trade_price={d.trade_price} high={d.high_price} "
        f"low={d.low_price} volume_24h={d.acc_trade_volume_24h}"
    )


def cmd_candles(limit: int = 5) -> None:
    minute_unit = interval_to_minute_unit(settings.INTERVAL)
    market = canonical_market_id(settings.PAIR)
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        rows = fetch_minute_candles(
            client,
            market=market,
            minute_unit=minute_unit,
            count=max(1, int(limit)),
        )
    print(f"[CANDLES {market} {settings.INTERVAL}] last {limit}")
    for row in rows[-limit:]:
        print(row)
