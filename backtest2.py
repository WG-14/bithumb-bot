import os
import argparse
import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bithumb_bot.paths import PathManager

load_dotenv(Path(__file__).with_name(".env"))

path_manager = PathManager.from_env(PROJECT_ROOT)
_db_path_env = os.getenv("DB_PATH", "")
if _db_path_env.strip():
    _db_candidate = Path(_db_path_env).expanduser()
    if not _db_candidate.is_absolute():
        raise ValueError(f"DB_PATH must be absolute for backtest2.py (got relative path: {_db_path_env!r})")
    DB_PATH = str(_db_candidate.resolve())
else:
    DB_PATH = str(path_manager.primary_db_path())
PAIR = os.getenv("PAIR", "BTC_KRW")
INTERVAL = os.getenv("INTERVAL", "1m")  # DB에 저장된 interval (보통 1m)

START_CASH_KRW = float(os.getenv("START_CASH_KRW", "1000000"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0004"))
BUY_FRACTION = float(os.getenv("BUY_FRACTION", "0.99"))

KST = timezone(timedelta(hours=9))


def kst_str(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def load_candles(interval: str):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT ts, close
        FROM candles
        WHERE pair=? AND interval=?
        ORDER BY ts ASC
        """,
        (PAIR, interval),
    ).fetchall()
    conn.close()
    return [(int(ts), float(close)) for ts, close in rows]


def resample_last_close(candles, minutes: int):
    """n분봉으로 묶어서 각 구간의 '마지막 close'만 남김 (SMA용)"""
    if minutes <= 1:
        return candles
    bucket_ms = minutes * 60 * 1000

    out = []
    cur_bucket = None
    last = None
    for ts, close in candles:
        b = ts // bucket_ms
        if cur_bucket is None:
            cur_bucket = b
        if b != cur_bucket:
            out.append(last)  # 이전 버킷 마지막 close
            cur_bucket = b
        last = (ts, close)
    if last is not None:
        out.append(last)
    return out


def sma(values, n: int) -> float:
    return sum(values[-n:]) / n


def backtest(short_n: int, long_n: int, entry: str, cooldown: int, min_gap: float, slippage_bps: float, resample_min: int):
    if short_n >= long_n:
        raise ValueError("short는 long보다 작아야 해. 예: 2/5, 3/10, 7/30")

    candles = load_candles(INTERVAL)
    if len(candles) < long_n + 10:
        raise RuntimeError(f"데이터 부족: candles={len(candles)} (long={long_n})")

    candles = resample_last_close(candles, resample_min)
    closes = [c for _, c in candles]
    ts_list = [t for t, _ in candles]

    if len(candles) < long_n + 10:
        raise RuntimeError(f"리샘플 후 데이터 부족: candles={len(candles)} (long={long_n})")

    cash = START_CASH_KRW
    qty = 0.0

    peak = START_CASH_KRW
    max_dd = 0.0

    trades = []
    total_fee = 0.0

    slip = slippage_bps / 10000.0  # bps -> fraction

    def do_buy(ts, price):
        nonlocal cash, qty, total_fee
        if qty > 0:
            return
        spend = cash * BUY_FRACTION
        if spend <= 0:
            return
        # 슬리피지: 매수는 더 비싸게 산다고 가정
        exec_price = price * (1.0 + slip)

        fee = spend * FEE_RATE
        spend_net = spend - fee
        buy_qty = spend_net / exec_price

        cash -= spend
        qty += buy_qty
        total_fee += fee
        trades.append((ts, "BUY", exec_price, buy_qty, fee, cash, qty))

    def do_sell(ts, price):
        nonlocal cash, qty, total_fee
        if qty <= 0:
            return
        # 슬리피지: 매도는 더 싸게 판다고 가정
        exec_price = price * (1.0 - slip)

        proceeds = qty * exec_price
        fee = proceeds * FEE_RATE

        cash += (proceeds - fee)
        total_fee += fee
        sell_qty = qty
        qty = 0.0
        trades.append((ts, "SELL", exec_price, sell_qty, fee, cash, qty))

    # 롤링 합
    s_sum = sum(closes[:short_n])
    l_sum = sum(closes[:long_n])

    prev_above = None
    cooldown_left = 0

    for i in range(long_n - 1, len(candles)):
        price = closes[i]
        ts = ts_list[i]

        if i >= short_n:
            s_sum += closes[i] - closes[i - short_n]
        if i >= long_n:
            l_sum += closes[i] - closes[i - long_n]

        if i < long_n - 1 or i < short_n - 1:
            continue

        s_sma = s_sum / short_n
        l_sma = l_sum / long_n
        above = s_sma > l_sma

        # 필터: SMA 간격이 너무 작으면(노이즈) 거래하지 않기
        gap = abs(s_sma - l_sma) / l_sma  # 비율
        if gap < min_gap:
            action = "HOLD"
        else:
            action = "HOLD"
            if cooldown_left > 0:
                action = "HOLD"
            else:
                if entry == "regime":
                    if above and qty <= 0:
                        action = "BUY"
                    elif (not above) and qty > 0:
                        action = "SELL"
                else:  # cross
                    if prev_above is not None:
                        if (not prev_above) and above:
                            action = "BUY"
                        elif prev_above and (not above):
                            action = "SELL"

        if action == "BUY":
            do_buy(ts, price)
            cooldown_left = cooldown
        elif action == "SELL":
            do_sell(ts, price)
            cooldown_left = cooldown
        else:
            if cooldown_left > 0:
                cooldown_left -= 1

        equity = cash + qty * price
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

        prev_above = above

    final_equity = cash + qty * closes[-1]
    ret = (final_equity / START_CASH_KRW - 1.0) * 100.0

    return {
        "count_candles": len(candles),
        "start_ts": ts_list[0],
        "end_ts": ts_list[-1],
        "trade_count": len(trades),
        "total_fee": total_fee,
        "final_equity": final_equity,
        "return_pct": ret,
        "max_dd_pct": max_dd * 100.0,
        "end_cash": cash,
        "end_qty": qty,
        "last_price": closes[-1],
        "trades": trades,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--short", type=int, required=True)
    ap.add_argument("--long", type=int, required=True)
    ap.add_argument("--entry", choices=["cross", "regime"], default="cross")

    ap.add_argument("--resample", type=int, default=1, help="n분봉으로 묶기(1=그대로, 5=5분봉)")
    ap.add_argument("--cooldown", type=int, default=0, help="거래 후 N봉 동안 재거래 금지")
    ap.add_argument("--min-gap", type=float, default=0.0, help="SMA 간격 최소 비율(예: 0.0005=0.05%%)")
    ap.add_argument("--slippage-bps", type=float, default=0.0, help="슬리피지 bps(예: 5=0.05%%)")
    ap.add_argument("--show-trades", type=int, default=10)

    args = ap.parse_args()

    r = backtest(
        args.short, args.long, args.entry,
        cooldown=args.cooldown,
        min_gap=args.min_gap,
        slippage_bps=args.slippage_bps,
        resample_min=args.resample,
    )

    print(f"[BACKTEST2] PAIR={PAIR} interval_in_db={INTERVAL} entry={args.entry} short={args.short} long={args.long}")
    print(f"  resample={args.resample}m cooldown={args.cooldown} min_gap={args.min_gap} slippage={args.slippage_bps}bps")
    print(f"  candles={r['count_candles']}  range={kst_str(r['start_ts'])} ~ {kst_str(r['end_ts'])}")
    print(f"  trades={r['trade_count']}  total_fee={r['total_fee']:,.0f} KRW")
    print(f"  final_equity={r['final_equity']:,.0f} KRW  return={r['return_pct']:.3f}%  maxDD={r['max_dd_pct']:.3f}%")
    print(f"  end: cash={r['end_cash']:,.0f} qty={r['end_qty']:.8f} last_price={r['last_price']:,.0f}")

    n = max(0, args.show_trades)
    if n:
        print(f"\n[TRADES] last {n}")
        for ts, side, price, qty, fee, cash, asset in r["trades"][-n:]:
            print(f"  {kst_str(ts)} {side:4s} price={price:,.0f} qty={qty:.8f} fee={fee:,.0f} cash={cash:,.0f} asset={asset:.8f}")


if __name__ == "__main__":
    main()
