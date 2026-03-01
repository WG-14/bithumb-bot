# src/bithumb_bot/engine.py
from __future__ import annotations

import time
from typing import Callable, Any

from .config import settings



def parse_interval_sec(interval: str) -> int:
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600
    raise ValueError(f"Unsupported interval: {interval}")


def run_loop(short_n: int, long_n: int) -> None:
    sec = parse_interval_sec(settings.INTERVAL)
    print(f"[RUN] MODE={settings.MODE} PAIR={settings.PAIR} INTERVAL={settings.INTERVAL} (every {sec}s) short={short_n} long={long_n}")
    print("중지: Ctrl+C")

    try:
        while True:
            now = time.time()
            sleep_s = sec - (now % sec) + 2
            time.sleep(sleep_s)

            cmd_sync(quiet=True)
            # compute_signal은 conn을 받는 버전이면 여기서 conn 열어 전달해야 함
            # 네 현재 compute_signal이 conn 없이 동작하도록 app에서 감싸고 있을 수 있어.
            r = compute_signal(short_n, long_n)

            if r is None:
                print("[RUN] 데이터 부족. sync가 쌓이면 다시 계산됨.")
                continue

            print(f"[RUN] {kst_str(r['ts'])} close={r['last_close']:,.0f}  "
                  f"SMA{short_n}={r['curr_s']:.2f}  SMA{long_n}={r['curr_l']:.2f}  => {r['signal']}")

            if settings.MODE == "paper" and r["signal"] in ("BUY", "SELL"):
                trade = paper_execute(r["signal"], r["ts"], r["last_close"])
                if trade:
                    print(f"  [PAPER] {trade['side']} qty={trade['qty']:.8f} price={trade['price']:,.0f} "
                          f"fee={trade['fee']:,.0f} cash={trade['cash']:,.0f} asset={trade['asset']:.8f}")

    except KeyboardInterrupt:
        print("\n[RUN] stopped by user (Ctrl+C)")