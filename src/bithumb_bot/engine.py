from __future__ import annotations

import math
import time
from dataclasses import dataclass

from .config import settings, validate_live_mode_preflight
from .marketdata import cmd_sync
from .strategy.sma import compute_signal
from .broker.paper import paper_execute
from .broker.live import live_execute_signal
from .broker.bithumb import BithumbBroker
from .broker.base import BrokerError
from .db_core import ensure_db
from .utils_time import kst_str, parse_interval_sec
from .notifier import format_event, notify
from . import runtime_state
from .risk import evaluate_daily_loss_breach


FAILSAFE_RETRY_DELAY_SEC = 180
STARTUP_RECOVERY_GATE_PREFIX = "startup safety gate"


@dataclass(frozen=True)
class HaltReason:
    code: str
    detail: str


def _halt_reason(code: str, detail: str) -> HaltReason:
    return HaltReason(code=code, detail=detail)

LIVE_UNRESOLVED_ORDER_STATUSES = (
    "PENDING_SUBMIT",
    "NEW",
    "PARTIAL",
    "SUBMIT_UNKNOWN",
    "RECOVERY_REQUIRED",
)


def _get_open_order_snapshot(now_ms: int) -> tuple[int, float | None]:
    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in LIVE_UNRESOLVED_ORDER_STATUSES)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS open_count, MIN(created_ts) AS oldest_created_ts
            FROM orders
            WHERE status IN ({placeholders})
            """,
            LIVE_UNRESOLVED_ORDER_STATUSES,
        ).fetchone()
        open_count = int(row["open_count"])
        oldest_created_ts = (
            int(row["oldest_created_ts"])
            if row["oldest_created_ts"] is not None
            else None
        )
        if open_count <= 0 or oldest_created_ts is None:
            return 0, None
        age_sec = max(0.0, (now_ms - oldest_created_ts) / 1000)
        return open_count, age_sec
    finally:
        conn.close()


def _mark_open_orders_recovery_required(reason: str, now_ms: int) -> int:
    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in LIVE_UNRESOLVED_ORDER_STATUSES)
        res = conn.execute(
            f"""
            UPDATE orders
            SET status='RECOVERY_REQUIRED', updated_ts=?, last_error=?
            WHERE status IN ({placeholders})
            """,
            (now_ms, reason, *LIVE_UNRESOLVED_ORDER_STATUSES),
        )
        conn.commit()
        return int(res.rowcount or 0)
    finally:
        conn.close()


def get_health_status() -> dict[str, float | int | bool | str | None]:
    state = runtime_state.snapshot()
    return {
        "last_candle_age_sec": state.last_candle_age_sec,
        "error_count": state.error_count,
        "trading_enabled": state.trading_enabled,
        "retry_at_epoch_sec": state.retry_at_epoch_sec,
        "last_disable_reason": state.last_disable_reason,
        "halt_new_orders_blocked": state.halt_new_orders_blocked,
        "halt_reason_code": state.halt_reason_code,
        "halt_state_unresolved": state.halt_state_unresolved,
        "unresolved_open_order_count": state.unresolved_open_order_count,
        "oldest_unresolved_order_age_sec": state.oldest_unresolved_order_age_sec,
        "recovery_required_count": state.recovery_required_count,
        "last_reconcile_epoch_sec": state.last_reconcile_epoch_sec,
        "last_reconcile_status": state.last_reconcile_status,
        "last_reconcile_error": state.last_reconcile_error,
        "last_cancel_open_orders_epoch_sec": state.last_cancel_open_orders_epoch_sec,
        "last_cancel_open_orders_trigger": state.last_cancel_open_orders_trigger,
        "last_cancel_open_orders_status": state.last_cancel_open_orders_status,
        "last_cancel_open_orders_summary": state.last_cancel_open_orders_summary,
        "startup_gate_reason": state.startup_gate_reason,
    }



def evaluate_startup_safety_gate() -> str | None:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()

    conn = ensure_db()
    try:
        submit_unknown_without_exchange_row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM orders
            WHERE status='SUBMIT_UNKNOWN'
              AND (exchange_order_id IS NULL OR TRIM(exchange_order_id)='')
            """
        ).fetchone()
        stray_remote_open_row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM orders
            WHERE client_order_id LIKE 'remote_%'
              AND status IN ('PENDING_SUBMIT','NEW','PARTIAL','SUBMIT_UNKNOWN','RECOVERY_REQUIRED')
            """
        ).fetchone()
    finally:
        conn.close()

    reasons: list[str] = []
    if state.unresolved_open_order_count > 0:
        reasons.append(f"unresolved_open_orders={state.unresolved_open_order_count}")
    if state.recovery_required_count > 0:
        reasons.append(f"recovery_required_orders={state.recovery_required_count}")

    submit_unknown_without_exchange_count = int(
        submit_unknown_without_exchange_row["cnt"] if submit_unknown_without_exchange_row else 0
    )
    if submit_unknown_without_exchange_count > 0:
        reasons.append(
            "submit_unknown_without_exchange_id="
            f"{submit_unknown_without_exchange_count}"
        )

    stray_remote_open_count = int(stray_remote_open_row["cnt"] if stray_remote_open_row else 0)
    if stray_remote_open_count > 0:
        reasons.append(f"stray_remote_open_orders={stray_remote_open_count}")

    if not reasons:
        runtime_state.set_startup_gate_reason(None)
        return None

    reason = f"{STARTUP_RECOVERY_GATE_PREFIX}: " + ", ".join(reasons)
    runtime_state.set_startup_gate_reason(reason)
    return reason

def _halt_trading(reason: HaltReason, *, unresolved: bool = False) -> None:
    runtime_state.disable_trading_until(
        float("inf"),
        reason=reason.detail,
        reason_code=reason.code,
        halt_new_orders_blocked=True,
        unresolved=unresolved,
    )
    notify(format_event("trading_halted", status="HALTED", reason=reason.detail, reason_code=reason.code))


def _attempt_open_order_cancellation(broker: BithumbBroker, trigger: str) -> bool:
    from .recovery import cancel_open_orders_with_broker

    try:
        summary = cancel_open_orders_with_broker(broker)
    except Exception as e:
        runtime_state.record_cancel_open_orders_result(
            trigger=trigger,
            status="error",
            summary={"error": f"{type(e).__name__}: {e}"},
        )
        notify(
            f"emergency open-order cancellation failed ({trigger}): "
            f"{type(e).__name__}: {e}"
        )
        return False

    remote_open_count = int(summary["remote_open_count"])
    canceled_count = int(summary["canceled_count"])
    failed_count = int(summary["failed_count"])
    notify(
        "emergency open-order cancellation pass "
        f"({trigger}): remote_open={remote_open_count} "
        f"canceled={canceled_count} failed={failed_count}"
    )

    for message in summary["stray_messages"]:
        notify(message)
    for message in summary["error_messages"]:
        notify(message)

    status = "partial" if failed_count > 0 else "ok"
    runtime_state.record_cancel_open_orders_result(trigger=trigger, status=status, summary=summary)

    if failed_count > 0:
        notify("emergency stop remains halted: open-order cancellation incomplete")
        return False
    return True


def run_loop(short_n: int, long_n: int) -> None:
    from .recovery import reconcile_with_broker

    validate_live_mode_preflight(settings)

    broker = None
    if settings.MODE == "live":
        broker = BithumbBroker()
        try:
            reconcile_with_broker(broker)
        except Exception as e:
            _halt_trading(_halt_reason("INITIAL_RECONCILE_FAILED", f"initial reconcile failed ({type(e).__name__}): {e}"), unresolved=True)
            return

        startup_gate_reason = evaluate_startup_safety_gate()
        if startup_gate_reason is not None:
            _halt_trading(_halt_reason("STARTUP_SAFETY_GATE", startup_gate_reason), unresolved=True)
            return

    sec = parse_interval_sec(settings.INTERVAL)
    print(
        f"[RUN] MODE={settings.MODE} PAIR={settings.PAIR} "
        f"INTERVAL={settings.INTERVAL} (every {sec}s) short={short_n} long={long_n}"
    )
    print("중지: Ctrl+C")
    fail_count = 0
    MAX_FAILS = 5
    last_open_order_reconcile_at: float | None = None

    try:
        while True:
            tick_now = time.time()
            sleep_s = sec - (tick_now % sec) + 2
            time.sleep(sleep_s)
            now = time.time()

            state = runtime_state.snapshot()
            if (not state.trading_enabled) and state.retry_at_epoch_sec:
                if math.isinf(state.retry_at_epoch_sec):
                    print("[RUN] trading halted indefinitely. exiting run loop.")
                    return
                if now < state.retry_at_epoch_sec:
                    wait_sec = max(0, int(state.retry_at_epoch_sec - now))
                    print(f"[RUN] failsafe active. trading paused for {wait_sec}s")
                    continue
                runtime_state.enable_trading()
                notify("failsafe retry window reached, attempting auto-resume")

            try:
                cmd_sync(quiet=True)
                conn = ensure_db()
                try:
                    row = conn.execute(
                        "SELECT ts, close FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
                        (settings.PAIR, settings.INTERVAL),
                    ).fetchone()
                finally:
                    conn.close()

                if row is None:
                    notify("no candles after sync")
                    continue

                last_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
                last_close = float(row["close"] if hasattr(row, "keys") else row[1])
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
                        "failsafe enabled after consecutive sync failures. "
                        f"trading paused until epoch={int(retry_at)}"
                    )
                continue

            stale_cutoff_sec = sec * 2
            if candle_age_sec > stale_cutoff_sec:
                notify(
                    f"stale candle detected: age={candle_age_sec:.1f}s > "
                    f"{stale_cutoff_sec}s; order blocked"
                )
                continue

            if settings.MODE == "live" and broker is not None:
                if settings.KILL_SWITCH:
                    canceled_ok = _attempt_open_order_cancellation(
                        broker, trigger="kill-switch"
                    )
                    if not canceled_ok:
                        _halt_trading(
                            _halt_reason("KILL_SWITCH", "KILL_SWITCH=ON; emergency cancellation failed"),
                            unresolved=True,
                        )
                    else:
                        _halt_trading(
                            _halt_reason("KILL_SWITCH", "KILL_SWITCH=ON; emergency cancellation attempted"),
                            unresolved=False,
                        )
                    continue

                conn = ensure_db()
                try:
                    portfolio = conn.execute(
                        "SELECT cash_krw, asset_qty FROM portfolio WHERE id=1"
                    ).fetchone()
                    if portfolio is not None:
                        # Use latest candle close as the mark price for daily-loss evaluation.
                        blocked, reason = evaluate_daily_loss_breach(
                            conn,
                            ts_ms=int(now * 1000),
                            cash=float(portfolio["cash_krw"]),
                            qty=float(portfolio["asset_qty"]),
                            price=float(last_close),
                        )
                        if blocked:
                            canceled_ok = _attempt_open_order_cancellation(
                                broker, trigger="daily-loss-halt"
                            )
                            suffix = (
                                "emergency cancellation attempted"
                                if canceled_ok
                                else "emergency cancellation failed"
                            )
                            _halt_trading(
                                _halt_reason("DAILY_LOSS_LIMIT", f"{reason}; {suffix}"),
                                unresolved=not canceled_ok,
                            )
                            continue
                finally:
                    conn.close()

                open_count, oldest_open_age_sec = _get_open_order_snapshot(int(now * 1000))
                if open_count > 0:
                    min_reconcile_sec = max(
                        1, int(settings.OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC)
                    )
                    if (
                        last_open_order_reconcile_at is None
                        or (now - last_open_order_reconcile_at) >= min_reconcile_sec
                    ):
                        try:
                            reconcile_with_broker(broker)
                            last_open_order_reconcile_at = now
                        except Exception as e:
                            _halt_trading(
                                _halt_reason(
                                    "PERIODIC_RECONCILE_FAILED",
                                    f"periodic reconcile failed ({type(e).__name__}): {e}",
                                ),
                                unresolved=True,
                            )
                            continue

                    open_count, oldest_open_age_sec = _get_open_order_snapshot(
                        int(now * 1000)
                    )
                    if open_count > 0 and oldest_open_age_sec is not None:
                        max_age_sec = max(1, int(settings.MAX_OPEN_ORDER_AGE_SEC))
                        if oldest_open_age_sec > max_age_sec:
                            reason = (
                                "stale unresolved open order detected: "
                                f"age={oldest_open_age_sec:.1f}s > {max_age_sec}s"
                            )
                            marked = _mark_open_orders_recovery_required(
                                reason, int(now * 1000)
                            )
                            canceled_ok = _attempt_open_order_cancellation(
                                broker, trigger="stale-open-order-halt"
                            )
                            if not canceled_ok:
                                _halt_trading(
                                    _halt_reason(
                                        "STALE_OPEN_ORDER",
                                        f"{reason}; marked={marked} recovery_required; emergency cancellation failed",
                                    ),
                                    unresolved=True,
                                )
                            else:
                                _halt_trading(
                                    _halt_reason(
                                        "STALE_OPEN_ORDER",
                                        f"{reason}; marked={marked} recovery_required; emergency cancellation attempted",
                                    ),
                                    unresolved=True,
                                )
                            continue

                    if open_count > 0:
                        notify("unresolved open order exists; skip new order placement")
                        continue

            conn = ensure_db()
            r = compute_signal(conn, short_n, long_n)
            conn.close()

            if r is None:
                print("[RUN] 데이터 부족. sync가 쌓이면 다시 계산됨.")
                continue

            print(
                f"[RUN] {kst_str(r['ts'])} close={r['last_close']:,.0f}  "
                f"SMA{short_n}={r['curr_s']:.2f}  "
                f"SMA{long_n}={r['curr_l']:.2f}  => {r['signal']}"
            )

            if r["signal"] not in ("BUY", "SELL"):
                continue

            trade = None
            if settings.MODE == "paper":
                trade = paper_execute(r["signal"], r["ts"], r["last_close"])
            elif settings.MODE == "live" and broker is not None:
                try:
                    trade = live_execute_signal(
                        broker, r["signal"], r["ts"], r["last_close"]
                    )
                except BrokerError as e:
                    _halt_trading(
                        _halt_reason(
                            "LIVE_EXECUTION_BROKER_ERROR",
                            f"live execution broker error ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue
                except Exception as e:
                    _halt_trading(
                        _halt_reason(
                            "LIVE_EXECUTION_FAILED",
                            f"live execution failed ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue
                try:
                    reconcile_with_broker(broker)
                except Exception as e:
                    _halt_trading(
                        _halt_reason(
                            "POST_TRADE_RECONCILE_FAILED",
                            f"reconcile failed ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue

            if trade:
                print(
                    f"  [{settings.MODE.upper()}] {trade['side']} "
                    f"qty={trade['qty']:.8f} price={trade['price']:,.0f} "
                    f"fee={trade['fee']:,.0f} cash={trade['cash']:,.0f} "
                    f"asset={trade['asset']:.8f}"
                )

    except KeyboardInterrupt:
        print("\n[RUN] stopped by user (Ctrl+C)")