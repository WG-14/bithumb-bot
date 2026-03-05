from __future__ import annotations

import random
import time
from typing import Any

import httpx

from .config import settings
from .db_core import ensure_db
from .notifier import notify


BASE_URL = "https://api.bithumb.com"

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_HTTP_RETRIES = 4
BASE_BACKOFF_SEC = 1.0
MAX_BACKOFF_SEC = 10.0
JITTER_SEC = 0.5


def _sleep_backoff(attempt: int) -> None:
    # attempt: 0,1,2... -> 1,2,4... seconds + jitter
    backoff = min(MAX_BACKOFF_SEC, BASE_BACKOFF_SEC * (2 ** attempt))
    backoff += random.uniform(0.0, JITTER_SEC)
    time.sleep(backoff)


def _get_with_retry(
    client: httpx.Client,
    path: str,
    params: dict[str, Any] | None = None,
) -> httpx.Response:
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


def fetch_json(path: str) -> dict[str, Any]:
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as c:
        r = _get_with_retry(c, path)
        return r.json()


def to_v1_market(pair: str) -> str:
    """
    BTC_KRW -> KRW-BTC
    """
    if "_" not in pair:
        return pair
    base, quote = pair.split("_", 1)
    return f"{quote}-{base}"


def fetch_orderbook_top(pair: str | None = None) -> tuple[float, float]:
    market = to_v1_market(pair or settings.PAIR)
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as c:
        r = _get_with_retry(c, "/v1/orderbook", params={"markets": market})
        payload = r.json()

    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"empty orderbook payload: {payload}")

    units = payload[0].get("orderbook_units")
    if not isinstance(units, list) or not units:
        raise RuntimeError(f"orderbook_units missing: {payload[0]}")

    best = units[0]
    bid = float(best.get("bid_price", 0.0))
    ask = float(best.get("ask_price", 0.0))
    return bid, ask


def cmd_sync(quiet: bool = False, limit: int = 200) -> None:
    """
    Public candlestick -> DB(candles)
    Bithumb public candlestick returns list rows:
      [timestamp, open, close, high, low, volume] (strings)
    """
    data = fetch_json(f"/public/candlestick/{settings.PAIR}/{settings.INTERVAL}")
    if str(data.get("status")) != "0000":
        raise RuntimeError(data)

    rows = data.get("data", [])
    if not rows:
        if not quiet:
            print("[SYNC] no data")
        return

    rows = rows[-limit:]

    conn = ensure_db()
    try:
        inserted = 0
        for r in rows:
            ts = int(float(r[0]))  # ms
            o = float(r[1])
            c = float(r[2])
            h = float(r[3])
            l = float(r[4])
            v = float(r[5])

            cur = conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, o, h, l, c, v),
            )
            inserted += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    if not quiet:
        print(f"[SYNC] upserted {len(rows)} rows -> {settings.DB_PATH}")


def cmd_ticker() -> None:
    data = fetch_json(f"/public/ticker/{settings.PAIR}")
    if str(data.get("status")) != "0000":
        raise RuntimeError(data)

    d = data["data"]
    print(
        f"[TICKER {settings.PAIR}] close={d.get('closing_price')} high={d.get('max_price')} "
        f"low={d.get('min_price')} volume={d.get('units_traded')} at_raw={d.get('date')}"
    )


def cmd_candles(limit: int = 5) -> None:
    data = fetch_json(f"/public/candlestick/{settings.PAIR}/{settings.INTERVAL}")
    if str(data.get("status")) != "0000":
        raise RuntimeError(data)

    rows = data.get("data", [])[-limit:]
    print(f"[CANDLES {settings.PAIR} {settings.INTERVAL}] last {limit}")
    for row in rows:
        print(row)