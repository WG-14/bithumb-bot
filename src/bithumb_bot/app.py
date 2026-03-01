from .config import settings
import os
import time
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

import httpx
from dotenv import load_dotenv

# .env를 "main.py 위치" 기준으로 로드
load_dotenv(Path(__file__).with_name(".env"))

BASE_URL = "https://api.bithumb.com"
PAIR = os.getenv("PAIR", "BTC_KRW")
INTERVAL = os.getenv("INTERVAL", "1m")
DB_PATH = os.getenv("DB_PATH", "data/bithumb.sqlite")

SMA_SHORT = int(os.getenv("SMA_SHORT", "7"))
SMA_LONG = int(os.getenv("SMA_LONG", "30"))

MODE = settings.MODE
PAIR = settings.PAIR
INTERVAL = settings.INTERVAL
EVERY = settings.EVERY

SMA_SHORT = settings.SMA_SHORT
SMA_LONG = settings.SMA_LONG
COOLDOWN_MIN = settings.COOLDOWN_MIN
MIN_GAP = settings.MIN_GAP

DB_PATH = settings.DB_PATH
START_CASH_KRW = settings.START_CASH_KRW
BUY_FRACTION = settings.BUY_FRACTION
FEE_RATE = settings.FEE_RATE

MAX_ORDER_KRW = settings.MAX_ORDER_KRW
MAX_DAILY_LOSS_KRW = settings.MAX_DAILY_LOSS_KRW
MAX_OPEN_POSITIONS = settings.MAX_OPEN_POSITIONS
KILL_SWITCH = settings.KILL_SWITCH
KILL_SWITCH_LIQUIDATE = settings.KILL_SWITCH_LIQUIDATE

def fetch_json(path: str):
    url = f"{BASE_URL}{path}"
    with httpx.Client(timeout=10) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()


def ensure_db(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(p)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candles (
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            ts INTEGER NOT NULL,          -- epoch ms
            open REAL NOT NULL,
            close REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (pair, interval, ts)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY CHECK (id=1),
            cash_krw REAL NOT NULL,
            asset_qty REAL NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            side TEXT NOT NULL,           -- BUY / SELL
            price REAL NOT NULL,
            qty REAL NOT NULL,
            fee REAL NOT NULL,
            cash_after REAL NOT NULL,
            asset_after REAL NOT NULL
        )
        """
    )

    conn.commit()
    return conn


def init_portfolio(conn: sqlite3.Connection):
    row = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
    if row is None:
        conn.execute("INSERT INTO portfolio(id, cash_krw, asset_qty) VALUES (1, ?, ?)", (START_CASH_KRW, 0.0))
        conn.commit()


def get_portfolio(conn: sqlite3.Connection):
    init_portfolio(conn)
    cash, qty = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
    return float(cash), float(qty)


def set_portfolio(conn: sqlite3.Connection, cash: float, qty: float):
    conn.execute("UPDATE portfolio SET cash_krw=?, asset_qty=? WHERE id=1", (cash, qty))
    conn.commit()


def kst_str(ts_ms: int) -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(kst).strftime("%Y-%m-%d %H:%M:%S %Z")


def parse_interval_sec(interval: str) -> int:
    # "1m", "5m", "1h" 지원
    interval = interval.strip().lower()
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600
    raise ValueError(f"지원하지 않는 INTERVAL: {interval} (예: 1m, 5m, 1h)")


def cmd_ticker():
    data = fetch_json(f"/public/ticker/{PAIR}")
    if data.get("status") != "0000":
        raise RuntimeError(data)

    d = data["data"]
    print(
        f"[TICKER {PAIR}] close={d.get('closing_price')} high={d.get('max_price')} "
        f"low={d.get('min_price')} volume={d.get('units_traded')} at_raw={d.get('date')}"
    )


def cmd_candles(limit: int):
    data = fetch_json(f"/public/candlestick/{PAIR}/{INTERVAL}")
    if data.get("status") != "0000":
        raise RuntimeError(data)

    rows = data["data"][-limit:]
    print(f"[CANDLES {PAIR} {INTERVAL}] last {limit}")
    for row in rows:
        print(row)


def cmd_sync(quiet: bool = False):
    """캔들을 받아서 DB에 누적 저장"""
    data = fetch_json(f"/public/candlestick/{PAIR}/{INTERVAL}")
    if data.get("status") != "0000":
        raise RuntimeError(data)

    rows = data["data"]
    conn = ensure_db(DB_PATH)

    with conn:
        for row in rows:
            ts = int(row[0])
            o = float(row[1]); c = float(row[2]); h = float(row[3]); l = float(row[4]); v = float(row[5])
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(pair, interval, ts, open, close, high, low, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (PAIR, INTERVAL, ts, o, c, h, l, v),
            )

    cur = conn.execute(
        "SELECT MIN(ts), MAX(ts), COUNT(*) FROM candles WHERE pair=? AND interval=?",
        (PAIR, INTERVAL),
    )
    min_ts, max_ts, cnt = cur.fetchone()
    conn.close()

    if not quiet:
        print(f"[SYNC {PAIR} {INTERVAL}] fetched={len(rows)} upserted={len(rows)} db_count={cnt}")
        if min_ts and max_ts:
            print(f"  range: {kst_str(int(min_ts))}  ~  {kst_str(int(max_ts))}")


def load_recent(conn: sqlite3.Connection, need: int):
    rows = conn.execute(
        """
        SELECT ts, close
        FROM candles
        WHERE pair=? AND interval=?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (PAIR, INTERVAL, need),
    ).fetchall()

    if len(rows) < need:
        return None

    rows = list(reversed(rows))  # ASC
    closes = [float(r[1]) for r in rows]
    return rows, closes


def sma(values, n: int):
    return sum(values[-n:]) / n


def compute_signal(short_n: int, long_n: int):
    if short_n >= long_n:
        raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

    need = long_n + 2  # 직전/현재 비교하려면 +1, 안전하게 +2
    conn = ensure_db(DB_PATH)
    rows_closes = load_recent(conn, need)
    conn.close()

    if rows_closes is None:
        return None

    rows, closes = rows_closes
    prev = closes[:-1]
    curr = closes

    prev_s = sma(prev, short_n)
    prev_l = sma(prev, long_n)
    curr_s = sma(curr, short_n)
    curr_l = sma(curr, long_n)

    last_ts = int(rows[-1][0])

    signal = "HOLD"
    if prev_s <= prev_l and curr_s > curr_l:
        signal = "BUY"
    elif prev_s >= prev_l and curr_s < curr_l:
        signal = "SELL"

    return {
        "ts": last_ts,
        "prev_s": prev_s,
        "prev_l": prev_l,
        "curr_s": curr_s,
        "curr_l": curr_l,
        "signal": signal,
        "last_close": float(closes[-1]),
    }


def cmd_signal(short_n: int, long_n: int):
    r = compute_signal(short_n, long_n)
    if r is None:
        print(f"[SIGNAL] 데이터가 부족해. 먼저 sync를 실행해줘.")
        return

    print(f"[SIGNAL {PAIR} {INTERVAL}] at {kst_str(r['ts'])}")
    print(f"  SMA(short={short_n}) prev={r['prev_s']:.2f} curr={r['curr_s']:.2f}")
    print(f"  SMA(long ={long_n}) prev={r['prev_l']:.2f} curr={r['curr_l']:.2f}")
    print(f"  last_close={r['last_close']:.2f}")
    print(f"  => {r['signal']}")


def cmd_explain(short_n: int, long_n: int):
    """왜 HOLD/BUY/SELL이 나왔는지 '마지막 구간 숫자'를 눈으로 보게 해줌"""
    need = long_n + 2
    conn = ensure_db(DB_PATH)
    rows_closes = load_recent(conn, need)
    conn.close()

    if rows_closes is None:
        print(f"[EXPLAIN] 데이터가 부족해. need={need}")
        return

    rows, closes = rows_closes
    print(f"[EXPLAIN {PAIR} {INTERVAL}] last {need} closes (시간순)")
    for (ts, close) in rows:
        print(f"  {kst_str(int(ts))}  close={float(close):.2f}")

    r = compute_signal(short_n, long_n)
    print("")
    print("계산 요약:")
    print(f"  prev short SMA = 평균(직전 {short_n}개 close)")
    print(f"  prev long  SMA = 평균(직전 {long_n}개 close)")
    print(f"  curr short SMA = 평균(현재 {short_n}개 close)")
    print(f"  curr long  SMA = 평균(현재 {long_n}개 close)")
    print("")
    print(f"  prev_s={r['prev_s']:.2f}, prev_l={r['prev_l']:.2f}")
    print(f"  curr_s={r['curr_s']:.2f}, curr_l={r['curr_l']:.2f}")
    print(f"  => signal={r['signal']}")


def paper_execute(signal: str, ts: int, price: float):
    """BUY/SELL 신호를 '가짜 매매'로 실행해서 portfolio/trades에 기록"""
    conn = ensure_db(DB_PATH)
    init_portfolio(conn)
    cash, qty = get_portfolio(conn)

    side = None
    trade_qty = 0.0
    fee = 0.0

    if signal == "BUY" and qty <= 0.0:
        spend = cash * BUY_FRACTION
        if spend <= 0:
            conn.close()
            return None

        fee = spend * FEE_RATE
        spend_net = spend - fee
        trade_qty = spend_net / price

        cash_after = cash - spend
        qty_after = qty + trade_qty
        side = "BUY"

    elif signal == "SELL" and qty > 0.0:
        proceeds = qty * price
        fee = proceeds * FEE_RATE
        cash_after = cash + (proceeds - fee)
        qty_after = 0.0
        trade_qty = qty
        side = "SELL"
    else:
        conn.close()
        return None

    with conn:
        set_portfolio(conn, cash_after, qty_after)
        conn.execute(
            """
            INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, PAIR, INTERVAL, side, price, trade_qty, fee, cash_after, qty_after),
        )

    conn.close()
    return {"side": side, "price": price, "qty": trade_qty, "fee": fee, "cash": cash_after, "asset": qty_after}


def cmd_status():
    conn = ensure_db(DB_PATH)
    init_portfolio(conn)
    cash, qty = get_portfolio(conn)

    row = conn.execute(
        "SELECT close, ts FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
        (PAIR, INTERVAL),
    ).fetchone()
    conn.close()

    if row is None:
        print("[STATUS] 캔들이 없음. 먼저 sync 실행")
        return

    last_close = float(row[0])
    ts = int(row[1])

    equity = cash + qty * last_close
    print(f"[STATUS {PAIR} {INTERVAL}] at {kst_str(ts)}")
    print(f"  cash_krw={cash:,.0f}")
    print(f"  asset_qty={qty:.8f}")
    print(f"  last_close={last_close:,.0f}")
    print(f"  equity={equity:,.0f} KRW")


def cmd_trades(limit: int):
    conn = ensure_db(DB_PATH)
    rows = conn.execute(
        """
        SELECT ts, side, price, qty, fee, cash_after, asset_after
        FROM trades
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    print(f"[TRADES] last {limit}")
    for ts, side, price, qty, fee, cash_a, asset_a in reversed(rows):
        print(
            f"  {kst_str(int(ts))} {side:4s} price={float(price):,.0f} qty={float(qty):.8f} "
            f"fee={float(fee):,.0f} cash={float(cash_a):,.0f} asset={float(asset_a):.8f}"
        )


def cmd_run(short_n: int, long_n: int):
    sec = parse_interval_sec(INTERVAL)
    print(f"[RUN] MODE={MODE} PAIR={PAIR} INTERVAL={INTERVAL} (every {sec}s) short={short_n} long={long_n}")
    print("중지: Ctrl+C")

    try:
        while True:
            # 캔들 마감 직후를 노리고 약간 늦게 실행(2초)
            now = time.time()
            sleep_s = sec - (now % sec) + 2
            time.sleep(sleep_s)

            cmd_sync(quiet=True)
            r = compute_signal(short_n, long_n)
            if r is None:
                print("[RUN] 데이터 부족. sync가 쌓이면 다시 계산됨.")
                continue

            print(f"[RUN] {kst_str(r['ts'])} close={r['last_close']:,.0f}  "
                f"SMA{short_n}={r['curr_s']:.2f}  SMA{long_n}={r['curr_l']:.2f}  => {r['signal']}")

            if MODE == "paper" and r["signal"] in ("BUY", "SELL"):
                trade = paper_execute(r["signal"], r["ts"], r["last_close"])
                if trade:
                    print(f"  [PAPER] {trade['side']} qty={trade['qty']:.8f} price={trade['price']:,.0f} "
                        f"fee={trade['fee']:,.0f} cash={trade['cash']:,.0f} asset={trade['asset']:.8f}")
    except KeyboardInterrupt:
        print("\n[RUN] stopped by user (Ctrl+C)")
        return

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=False)

    sub.add_parser("ticker")

    c = sub.add_parser("candles")
    c.add_argument("--limit", type=int, default=5)

    sub.add_parser("sync")

    s = sub.add_parser("signal")
    s.add_argument("--short", type=int, default=SMA_SHORT)
    s.add_argument("--long", type=int, default=SMA_LONG)

    e = sub.add_parser("explain")
    e.add_argument("--short", type=int, default=SMA_SHORT)
    e.add_argument("--long", type=int, default=SMA_LONG)

    st = sub.add_parser("status")

    t = sub.add_parser("trades")
    t.add_argument("--limit", type=int, default=20)

    r = sub.add_parser("run")
    r.add_argument("--short", type=int, default=SMA_SHORT)
    r.add_argument("--long", type=int, default=SMA_LONG)

    args = p.parse_args()

    if args.cmd in (None, "ticker"):
        cmd_ticker()
    elif args.cmd == "candles":
        cmd_candles(args.limit)
    elif args.cmd == "sync":
        cmd_sync()
    elif args.cmd == "signal":
        cmd_signal(args.short, args.long)
    elif args.cmd == "explain":
        cmd_explain(args.short, args.long)
    elif args.cmd == "status":
        cmd_status()
    elif args.cmd == "trades":
        cmd_trades(args.limit)
    elif args.cmd == "run":
        cmd_run(args.short, args.long)

