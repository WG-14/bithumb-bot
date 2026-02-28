import os
import time
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

import httpx
from dotenv import load_dotenv

# .env를 bot.py 위치 기준으로 로드
load_dotenv(Path(__file__).with_name(".env"))

BASE_URL = "https://api.bithumb.com"

PAIR = os.getenv("PAIR", "BTC_KRW")
INTERVAL = os.getenv("INTERVAL", "1m")
DB_PATH = os.getenv("DB_PATH", "data/bithumb.sqlite")

MODE = os.getenv("MODE", "paper").lower()                     # paper / live(미사용)
ENTRY_MODE = os.getenv("ENTRY_MODE", "cross").lower()         # cross / regime

START_CASH_KRW = float(os.getenv("START_CASH_KRW", "1000000"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0004"))
BUY_FRACTION = float(os.getenv("BUY_FRACTION", "0.99"))

SMA_SHORT = int(os.getenv("SMA_SHORT", "7"))
SMA_LONG = int(os.getenv("SMA_LONG", "30"))
COOLDOWN_BARS = int(os.getenv("COOLDOWN_BARS", "0"))
MIN_GAP = float(os.getenv("MIN_GAP", "0.0"))  # 예: 0.0003 = 0.03%

# ---------- utilities ----------
def fetch_json(path: str):
    url = f"{BASE_URL}{path}"
    with httpx.Client(timeout=10) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()


def ensure_db() -> sqlite3.Connection:
    p = Path(DB_PATH)
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
    row = conn.execute("SELECT 1 FROM portfolio WHERE id=1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO portfolio(id, cash_krw, asset_qty) VALUES (1, ?, ?)",
            (START_CASH_KRW, 0.0),
        )
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
    interval = interval.strip().lower()
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600
    raise ValueError(f"지원하지 않는 INTERVAL: {interval} (예: 1m, 5m, 1h)")


def sma(values, n: int) -> float:
    return sum(values[-n:]) / n


# ---------- core: data ----------
def sync_candles(need_long: int):
    """최근 캔들을 받아 DB에 upsert (long 기준으로 충분히 저장)"""
    data = fetch_json(f"/public/candlestick/{PAIR}/{INTERVAL}")
    if data.get("status") != "0000":
        raise RuntimeError(data)

    rows = data["data"]

    # long 보다 넉넉히(교차/평균 계산용)
    keep = max(need_long + 200, 400)
    rows = rows[-keep:]

    conn = ensure_db()
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

    cnt, max_ts = conn.execute(
        "SELECT COUNT(*), MAX(ts) FROM candles WHERE pair=? AND interval=?",
        (PAIR, INTERVAL),
    ).fetchone()
    conn.close()
    return int(cnt), (int(max_ts) if max_ts else None)


def compute_signal(short_n: int, long_n: int):
    """
    cross 모드 신호:
      BUY  = (직전 short<=long) and (현재 short>long)
      SELL = (직전 short>=long) and (현재 short<long)
      else HOLD

    regime 모드 판단용:
      above = (현재 short > long)
    """
    if short_n >= long_n:
        raise ValueError("short는 long보다 작아야 해. 예: 2/5, 3/10, 7/30")

    need = long_n + 2  # 직전/현재 비교용
    conn = ensure_db()
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
    conn.close()

    if len(rows) < need:
        return None

    rows = list(reversed(rows))  # 시간순
    closes = [float(r[1]) for r in rows]

    prev = closes[:-1]
    curr = closes

    prev_s = sma(prev, short_n)
    prev_l = sma(prev, long_n)
    curr_s = sma(curr, short_n)
    curr_l = sma(curr, long_n)

    signal = "HOLD"
    if prev_s <= prev_l and curr_s > curr_l:
        signal = "BUY"
    elif prev_s >= prev_l and curr_s < curr_l:
        signal = "SELL"

    return {
        "ts": int(rows[-1][0]),
        "last_close": float(closes[-1]),
        "prev_s": prev_s,
        "prev_l": prev_l,
        "curr_s": curr_s,
        "curr_l": curr_l,
        "signal": signal,            # cross용
        "above": (curr_s > curr_l),  # regime용
    }


# ---------- core: paper trading ----------
def paper_execute(side: str, ts: int, price: float):
    """BUY/SELL을 페이퍼 트레이드로 실행"""
    conn = ensure_db()
    init_portfolio(conn)
    cash, qty = get_portfolio(conn)

    if side == "BUY" and qty <= 0.0:
        spend = cash * BUY_FRACTION
        if spend <= 0:
            conn.close()
            return None

        fee = spend * FEE_RATE
        spend_net = spend - fee
        buy_qty = spend_net / price

        cash_after = cash - spend
        qty_after = qty + buy_qty

        with conn:
            set_portfolio(conn, cash_after, qty_after)
            conn.execute(
                """
                INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after)
                VALUES (?, ?, ?, 'BUY', ?, ?, ?, ?, ?)
                """,
                (ts, PAIR, INTERVAL, price, buy_qty, fee, cash_after, qty_after),
            )
        conn.close()
        return ("BUY", buy_qty, fee, cash_after, qty_after)

    if side == "SELL" and qty > 0.0:
        proceeds = qty * price
        fee = proceeds * FEE_RATE

        cash_after = cash + (proceeds - fee)
        qty_after = 0.0

        with conn:
            set_portfolio(conn, cash_after, qty_after)
            conn.execute(
                """
                INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after)
                VALUES (?, ?, ?, 'SELL', ?, ?, ?, ?, ?)
                """,
                (ts, PAIR, INTERVAL, price, qty, fee, cash_after, qty_after),
            )
        conn.close()
        return ("SELL", qty, fee, cash_after, qty_after)

    conn.close()
    return None


# ---------- commands ----------
def cmd_status():
    conn = ensure_db()
    init_portfolio(conn)
    cash, qty = get_portfolio(conn)

    row = conn.execute(
        "SELECT close, ts FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
        (PAIR, INTERVAL),
    ).fetchone()
    conn.close()

    if not row:
        print("[STATUS] 캔들 없음. run을 잠깐이라도 실행해줘.")
        return

    last_close, ts = float(row[0]), int(row[1])
    equity = cash + qty * last_close

    print(f"[STATUS {PAIR} {INTERVAL}] at {kst_str(ts)}")
    print(f"  cash_krw={cash:,.0f}")
    print(f"  asset_qty={qty:.8f}")
    print(f"  last_close={last_close:,.0f}")
    print(f"  equity={equity:,.0f} KRW")


def cmd_trades(limit: int):
    conn = ensure_db()
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


def cmd_run(short_n: int, long_n: int, entry_mode: str):
    sec = parse_interval_sec(INTERVAL)
    print(
        f"[RUN] MODE={MODE} ENTRY={entry_mode} PAIR={PAIR} INTERVAL={INTERVAL} "
        f"every={sec}s short={short_n} long={long_n} cooldown={COOLDOWN_BARS} min_gap={MIN_GAP}"
    )
    print("중지: Ctrl+C")

    last_processed_ts = None
    cooldown_left = 0

    try:
        while True:
            # 캔들 마감 직후를 노리고 약간 늦게(2초)
            now = time.time()
            sleep_s = sec - (now % sec) + 2
            time.sleep(sleep_s)

            cnt, _ = sync_candles(long_n)
            r = compute_signal(short_n, long_n)
            if r is None:
                print(f"[RUN] 데이터 부족 (db_count={cnt})")
                continue

            # 같은 캔들은 재처리하지 않기
            if last_processed_ts == r["ts"]:
                continue
            last_processed_ts = r["ts"]

            # 현재 포트폴리오 확인
            conn = ensure_db()
            init_portfolio(conn)
            cash, qty = get_portfolio(conn)
            conn.close()

            # -----------------------------
            # (1) 필터 1: 쿨다운
            # -----------------------------
            if cooldown_left > 0:
                cooldown_left -= 1
                action = "HOLD"
            else:
                # -----------------------------
                # (2) 필터 2: SMA 간격(min-gap)
                # -----------------------------
                gap = abs(r["curr_s"] - r["curr_l"]) / r["curr_l"]  # 비율
                if gap < MIN_GAP:
                    action = "HOLD"
                else:
                    # -----------------------------
                    # (3) 기존 entry_mode 로직 (여기가 “action 결정”)
                    # -----------------------------
                    if entry_mode == "cross":
                        action = r["signal"]  # BUY/SELL/HOLD (교차 순간)
                    elif entry_mode == "regime":
                        # short>long이면 롱 상태 유지, short<long이면 현금 상태 유지
                        if r["above"] and qty <= 0.0:
                            action = "BUY"
                        elif (not r["above"]) and qty > 0.0:
                            action = "SELL"
                        else:
                            action = "HOLD"
                    else:
                        raise ValueError("entry_mode must be 'cross' or 'regime'")

            print(
                f"[RUN] {kst_str(r['ts'])} close={r['last_close']:,.0f} "
                f"SMA{short_n}={r['curr_s']:.2f} SMA{long_n}={r['curr_l']:.2f} "
                f"(cross={r['signal']}) cooldown_left={cooldown_left} => {action}"
            )

            if MODE == "paper" and action in ("BUY", "SELL"):
                trade = paper_execute(action, r["ts"], r["last_close"])
                if trade:
                    # ✅ 거래가 실제로 발생했을 때만 쿨다운 시작
                    cooldown_left = COOLDOWN_BARS
                    side, t_qty, fee, cash_after, asset_after = trade
                    print(
                        f"  [PAPER] {side} qty={t_qty:.8f} fee={fee:,.0f} "
                        f"cash={cash_after:,.0f} asset={asset_after:.8f}"
                    )

    except KeyboardInterrupt:
        print("\n[RUN] stopped (Ctrl+C)")

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status")

    t = sub.add_parser("trades")
    t.add_argument("--limit", type=int, default=20)

    r = sub.add_parser("run")
    r.add_argument("--short", type=int, default=SMA_SHORT)
    r.add_argument("--long", type=int, default=SMA_LONG)
    r.add_argument("--entry", choices=["cross", "regime"], default=ENTRY_MODE)

    args = p.parse_args()

    if args.cmd == "status":
        cmd_status()
    elif args.cmd == "trades":
        cmd_trades(args.limit)
    elif args.cmd == "run":
        cmd_run(args.short, args.long, args.entry)


if __name__ == "__main__":
    main()