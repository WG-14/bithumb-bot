from .config import settings
from .risk import evaluate_buy_guardrails
from .broker.paper import paper_execute
from .strategy.sma import compute_signal
import os
import time
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from .marketdata import cmd_sync, cmd_ticker, cmd_candles
from .db_core import ensure_db, init_portfolio, get_portfolio, set_portfolio
from .utils_time import kst_str, parse_interval_sec

import httpx

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


def cmd_signal(short_n: int, long_n: int):
    conn = ensure_db(DB_PATH)
    r = compute_signal(conn, short_n, long_n)
    conn.close()
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

    conn = ensure_db(DB_PATH)
    r = compute_signal(conn, short_n, long_n)
    conn.close()
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
        SELECT ts, side, price, qty, fee, cash_after, asset_after, note
        FROM trades
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    print(f"[TRADES] last {limit}")
    for ts, side, price, qty, fee, cash_a, asset_a, note in reversed(rows):
        note_s = (note or "")
        print(
            f"  {kst_str(int(ts))} {side:4s} price={float(price):,.0f} qty={float(qty):.8f} "
            f"fee={float(fee):,.0f} cash={float(cash_a):,.0f} asset={float(asset_a):.8f} "
            f"note={note_s}"
        )

def cmd_orders(limit: int = 50):
    conn = ensure_db()
    rows = conn.execute(
        """
        SELECT client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts
        FROM orders
        ORDER BY created_ts DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    print(f"[ORDERS] last {limit}")
    for r in reversed(rows):
        print(dict(r))


def cmd_fills(limit: int = 50):
    conn = ensure_db()
    rows = conn.execute(
        """
        SELECT client_order_id, fill_ts, price, qty, fee
        FROM fills
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    print(f"[FILLS] last {limit}")
    for r in reversed(rows):
        print(dict(r))


def cmd_audit():
    conn = ensure_db(DB_PATH)
    init_portfolio(conn)

    errors: list[str] = []

    portfolio = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
    if portfolio is None:
        errors.append("portfolio row(id=1) missing")
    else:
        cash_krw = float(portfolio["cash_krw"])
        asset_qty = float(portfolio["asset_qty"])
        if cash_krw < 0:
            errors.append(f"portfolio.cash_krw is negative: {cash_krw}")
        if asset_qty < 0:
            errors.append(f"portfolio.asset_qty is negative: {asset_qty}")

    filled_without_qty = conn.execute(
        """
        SELECT client_order_id, qty_filled
        FROM orders
        WHERE status='FILLED' AND qty_filled <= 0
        """
    ).fetchall()
    for row in filled_without_qty:
        errors.append(
            f"order {row['client_order_id']} has FILLED status but qty_filled={float(row['qty_filled'])}"
        )

    orphan_fills = conn.execute(
        """
        SELECT f.id, f.client_order_id
        FROM fills f
        LEFT JOIN orders o ON o.client_order_id = f.client_order_id
        WHERE o.client_order_id IS NULL
        """
    ).fetchall()
    for row in orphan_fills:
        errors.append(f"fill id={row['id']} references missing order {row['client_order_id']}")

    bad_buy_snapshots = conn.execute(
        """
        SELECT id, side, qty, fee, cash_after, asset_after
        FROM trades
        WHERE side='BUY' AND (cash_after + fee < 0 OR asset_after < qty)
        """
    ).fetchall()
    for row in bad_buy_snapshots:
        errors.append(
            "trade id={id} BUY snapshot impossible: cash_after={cash_after}, fee={fee}, asset_after={asset_after}, qty={qty}".format(
                id=row['id'],
                cash_after=float(row['cash_after']),
                fee=float(row['fee']),
                asset_after=float(row['asset_after']),
                qty=float(row['qty']),
            )
        )

    bad_sell_snapshots = conn.execute(
        """
        SELECT id, side, qty, cash_after, asset_after
        FROM trades
        WHERE side='SELL' AND (cash_after < 0 OR asset_after > qty)
        """
    ).fetchall()
    for row in bad_sell_snapshots:
        errors.append(
            "trade id={id} SELL snapshot impossible: cash_after={cash_after}, asset_after={asset_after}, qty={qty}".format(
                id=row['id'],
                cash_after=float(row['cash_after']),
                asset_after=float(row['asset_after']),
                qty=float(row['qty']),
            )
        )

    last_trade = conn.execute(
        "SELECT cash_after, asset_after FROM trades ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if last_trade is not None and portfolio is not None:
        if abs(float(last_trade["cash_after"]) - float(portfolio["cash_krw"])) > 1e-8:
            errors.append(
                f"latest trade cash_after={float(last_trade['cash_after'])} != portfolio.cash_krw={float(portfolio['cash_krw'])}"
            )
        if abs(float(last_trade["asset_after"]) - float(portfolio["asset_qty"])) > 1e-12:
            errors.append(
                f"latest trade asset_after={float(last_trade['asset_after'])} != portfolio.asset_qty={float(portfolio['asset_qty'])}"
            )

    conn.close()

    if errors:
        print("[AUDIT] FAILED")
        for err in errors:
            print(f"  - {err}")
        raise SystemExit(1)

    print("[AUDIT] OK")

def cmd_run(short_n: int, long_n: int):
    from .engine import run_loop
    run_loop(short_n, long_n)
    
def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=False)

    sub.add_parser("ticker")

    o = sub.add_parser("orders")
    o.add_argument("--limit", type=int, default=50)

    f = sub.add_parser("fills")
    f.add_argument("--limit", type=int, default=50)

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

    sub.add_parser("audit")
    sub.add_parser("check")

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
    elif args.cmd in ("audit", "check"):
        cmd_audit()
    elif args.cmd == "trades":
        cmd_trades(args.limit)
    elif args.cmd == "orders":
        cmd_orders(args.limit)
    elif args.cmd == "fills":
        cmd_fills(args.limit)
    elif args.cmd == "run":
        cmd_run(args.short, args.long)

# === CLI entrypoint & routing (add at bottom of file) ===
import sys
import argparse


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bithumb-bot")
    sub = p.add_subparsers(dest="cmd", required=True)

    # audit
    sub.add_parser("audit", help="Check DB invariants / ledger consistency")

    # fills (너 파일에 cmd_fills(limit=50) 가 있으니 연결)
    fills = sub.add_parser("fills", help="Print recent fills")
    fills.add_argument("--limit", type=int, default=50)

    # (옵션) 너 app.py에 있는 다른 cmd_*들도 같은 방식으로 추가하면 됨.
    # 예시:
    # orders = sub.add_parser("orders", help="Print recent orders")
    # orders.add_argument("--limit", type=int, default=50)

    return p


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "audit":
        cmd_audit()
        return 0

    if args.cmd == "fills":
        cmd_fills(limit=args.limit)
        return 0

    # 이론상 여기 올 일 없음(required=True라서)
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())