from __future__ import annotations

import time

from .config import settings
from .marketdata import cmd_sync
from .strategy.sma import compute_signal
from .broker.paper import paper_execute
from .broker.live import live_execute_signal
from .broker.bithumb import BithumbBroker
from .db_core import ensure_db
from .utils_time import kst_str, parse_interval_sec
from .notifier import notify
from . import runtime_state


FAILSAFE_RETRY_DELAY_SEC = 180


def get_health_status() -> dict[str, float | int | bool | None]:
    state = runtime_state.snapshot()
    return {
        "last_candle_age_sec": state.last_candle_age_sec,
        "error_count": state.error_count,
        "trading_enabled": state.trading_enabled,
        "retry_at_epoch_sec": state.retry_at_epoch_sec,
    }


def run_loop(short_n: int, long_n: int) -> None:
    from .recovery import reconcile_with_broker

    broker = None
    if settings.MODE == "live":
        broker = BithumbBroker()
        reconcile_with_broker(broker)

    sec = parse_interval_sec(settings.INTERVAL)
    print(f"[RUN] MODE={settings.MODE} PAIR={settings.PAIR} INTERVAL={settings.INTERVAL} (every {sec}s) short={short_n} long={long_n}")
    print("중지: Ctrl+C")
    fail_count = 0
    MAX_FAILS = 5

    try:
        while True:
            tick_now = time.time()
            sleep_s = sec - (tick_now % sec) + 2
            time.sleep(sleep_s)
            now = time.time()

            state = runtime_state.snapshot()
            if (not state.trading_enabled) and state.retry_at_epoch_sec and now < state.retry_at_epoch_sec:
                wait_sec = int(state.retry_at_epoch_sec - now)
                print(f"[RUN] failsafe active. trading paused for {wait_sec}s")
                continue
            if (not state.trading_enabled) and state.retry_at_epoch_sec and now >= state.retry_at_epoch_sec:
                runtime_state.enable_trading()
                notify("failsafe retry window reached, attempting auto-resume")

            try:
                cmd_sync(quiet=True)
                conn = ensure_db()
                row = conn.execute(
                    "SELECT ts FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
                    (settings.PAIR, settings.INTERVAL),
                ).fetchone()
                conn.close()

                if row is None:
                    notify("no candles after sync")
                    continue

                last_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
                candle_age_sec = max(0.0, (time.time() * 1000 - last_ts) / 1000)
                runtime_state.set_last_candle_age_sec(candle_age_sec)

                fail_count = 0
                runtime_state.set_error_count(fail_count)
            except Exception as e:
                fail_count += 1
                runtime_state.set_error_count(fail_count)
                notify(f"sync failed ({fail_count}/{MAX_FAILS}): {e}")
                if fail_count >= MAX_FAILS:
                    retry_at = time.time() + FAILSAFE_RETRY_DELAY_SEC
                    runtime_state.disable_trading_until(retry_at)
                    notify(
                        f"failsafe enabled after consecutive sync failures. "
                        f"trading paused until epoch={int(retry_at)}"
                    )
                continue

            stale_cutoff_sec = sec * 2
            if candle_age_sec > stale_cutoff_sec:
                notify(
                    f"stale candle detected: age={candle_age_sec:.1f}s > {stale_cutoff_sec}s; order blocked"
                )
                continue

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

            if r["signal"] not in ("BUY", "SELL"):
                continue

            trade = None
            if settings.MODE == "paper":
                trade = paper_execute(r["signal"], r["ts"], r["last_close"])
            elif settings.MODE == "live" and broker is not None:
                trade = live_execute_signal(broker, r["signal"], r["ts"], r["last_close"])

            if trade:
                print(
                    f"  [{settings.MODE.upper()}] {trade['side']} qty={trade['qty']:.8f} price={trade['price']:,.0f} "
                    f"fee={trade['fee']:,.0f} cash={trade['cash']:,.0f} asset={trade['asset']:.8f}"
                )

    except KeyboardInterrupt:
        print("\n[RUN] stopped by user (Ctrl+C)")
