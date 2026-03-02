from __future__ import annotations

import time

from .config import settings
from .marketdata import cmd_sync
from .strategy.sma import compute_signal
from .broker.paper import paper_execute
from .db_core import ensure_db
from .utils_time import kst_str, parse_interval_sec


def run_loop(short_n: int, long_n: int) -> None:
    from .recovery import assert_no_open_orders
    assert_no_open_orders()
    
    sec = parse_interval_sec(settings.INTERVAL)
    print(f"[RUN] MODE={settings.MODE} PAIR={settings.PAIR} INTERVAL={settings.INTERVAL} (every {sec}s) short={short_n} long={long_n}")
    print("중지: Ctrl+C")

    try:
        while True:
            now = time.time()
            sleep_s = sec - (now % sec) + 2
            time.sleep(sleep_s)

            cmd_sync(quiet=True)

            conn = ensure_db()
            r = compute_signal(conn, short_n, long_n)
            conn.close()

            if r is None:
                print("[RUN] 데이터 부족. sync가 쌓이면 다시 계산됨.")
                continue

            print(
                f"[RUN] {kst_str(r['ts'])} close={r['last_close']:,.0f}  "
                f"SMA{short_n}={r['curr_s']:.2f}  SMA{long_n}={r['curr_l']:.2f}  => {r['signal']}"
            )

            if settings.MODE == "paper" and r["signal"] in ("BUY", "SELL"):
                trade = paper_execute(r["signal"], r["ts"], r["last_close"])
                if trade:
                    print(
                        f"  [PAPER] {trade['side']} qty={trade['qty']:.8f} price={trade['price']:,.0f} "
                        f"fee={trade['fee']:,.0f} cash={trade['cash']:,.0f} asset={trade['asset']:.8f}"
                    )

    except KeyboardInterrupt:
        print("\n[RUN] stopped by user (Ctrl+C)")