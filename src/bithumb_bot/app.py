from .config import settings
from .risk import evaluate_buy_guardrails
from .broker.paper import paper_execute
from .strategy.sma import compute_signal
import os
import time
import argparse
import sqlite3
import math
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from .marketdata import cmd_sync, cmd_ticker, cmd_candles
from .db_core import ensure_db, init_portfolio, get_portfolio_breakdown
from .utils_time import kst_str, parse_interval_sec
from .engine import evaluate_resume_eligibility, get_health_status
from .recovery import (
    cancel_open_orders_with_broker,
    load_recent_order_lifecycle,
    reconcile_with_broker,
    recover_order_with_exchange_id,
)
from .runtime_state import disable_trading_until, enable_trading, refresh_open_order_health
from .notifier import notify
from .observability import safety_event
from . import runtime_state
from .oms import OPEN_ORDER_STATUSES

import httpx

MODE = settings.MODE
PAIR = settings.PAIR
INTERVAL = settings.INTERVAL
EVERY = settings.EVERY

SMA_SHORT = settings.SMA_SHORT
SMA_LONG = settings.SMA_LONG
COOLDOWN_MIN = settings.COOLDOWN_MIN
MIN_GAP = settings.MIN_GAP

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
    conn = conn = ensure_db()
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
    conn = ensure_db()
    rows_closes = load_recent(conn, need)
    conn.close()

    if rows_closes is None:
        print(f"[EXPLAIN] 데이터가 부족해. need={need}")
        return

    rows, closes = rows_closes
    print(f"[EXPLAIN {PAIR} {INTERVAL}] last {need} closes (시간순)")
    for (ts, close) in rows:
        print(f"  {kst_str(int(ts))}  close={float(close):.2f}")

    conn = ensure_db()
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
    conn = ensure_db()
    init_portfolio(conn)
    cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
    cash = cash_available + cash_locked
    qty = asset_available + asset_locked

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
    print(f"  cash_krw={cash:,.0f} (available={cash_available:,.0f}, locked={cash_locked:,.0f})")
    print(f"  asset_qty={qty:.8f} (available={asset_available:.8f}, locked={asset_locked:.8f})")
    print(f"  last_close={last_close:,.0f}")
    print(f"  equity={equity:,.0f} KRW")


def cmd_trades(limit: int):
    conn = ensure_db()
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
    conn = ensure_db()
    init_portfolio(conn)

    errors: list[str] = []

    portfolio = conn.execute("SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1").fetchone()
    if portfolio is None:
        errors.append("portfolio row(id=1) missing")
    else:
        cash_krw = float(portfolio["cash_krw"])
        asset_qty = float(portfolio["asset_qty"])
        cash_available = float(portfolio["cash_available"])
        cash_locked = float(portfolio["cash_locked"])
        asset_available = float(portfolio["asset_available"])
        asset_locked = float(portfolio["asset_locked"])
        if cash_krw < 0:
            errors.append(f"portfolio.cash_krw is negative: {cash_krw}")
        if asset_qty < 0:
            errors.append(f"portfolio.asset_qty is negative: {asset_qty}")
        if cash_available < 0:
            errors.append(f"portfolio.cash_available is negative: {cash_available}")
        if cash_locked < 0:
            errors.append(f"portfolio.cash_locked is negative: {cash_locked}")
        if asset_available < 0:
            errors.append(f"portfolio.asset_available is negative: {asset_available}")
        if asset_locked < 0:
            errors.append(f"portfolio.asset_locked is negative: {asset_locked}")
        if abs((cash_available + cash_locked) - cash_krw) > 1e-8:
            errors.append(
                f"portfolio cash split mismatch: available+locked={cash_available + cash_locked} != cash_krw={cash_krw}"
            )
        if abs((asset_available + asset_locked) - asset_qty) > 1e-12:
            errors.append(
                f"portfolio asset split mismatch: available+locked={asset_available + asset_locked} != asset_qty={asset_qty}"
            )

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
                id=row["id"],
                cash_after=float(row["cash_after"]),
                fee=float(row["fee"]),
                asset_after=float(row["asset_after"]),
                qty=float(row["qty"]),
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
                id=row["id"],
                cash_after=float(row["cash_after"]),
                asset_after=float(row["asset_after"]),
                qty=float(row["qty"]),
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
    from .run_lock import RunLockError, acquire_run_lock

    try:
        with acquire_run_lock():
            run_loop(short_n, long_n)
    except RunLockError as e:
        notify(
            safety_event(
                "run_lock_conflict",
                client_order_id="-",
                submit_attempt_id="-",
                exchange_order_id="-",
                reason_code="RUN_LOCK_CONFLICT",
                alert_kind="run_lock_conflict",
                reason=str(e),
            )
        )
        print(f"[RUN] {e}")
        raise SystemExit(1) from e


def cmd_health() -> None:
    refresh_open_order_health()
    health = get_health_status()
    submit_unknown_count = 0
    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS submit_unknown_count FROM orders WHERE status='SUBMIT_UNKNOWN'"
        ).fetchone()
    finally:
        conn.close()
    if row is not None:
        submit_unknown_count = int(row["submit_unknown_count"] or 0)

    current_halt_reason = "none"
    if health["halt_reason_code"] or health["last_disable_reason"]:
        current_halt_reason = (
            f"code={health['halt_reason_code'] or '-'} "
            f"reason={health['last_disable_reason'] or '-'}"
        )

    reconcile_latest = "none"
    if health["last_reconcile_status"]:
        reconcile_latest = (
            f"epoch_sec={health['last_reconcile_epoch_sec'] if health['last_reconcile_epoch_sec'] is not None else '-'} "
            f"status={health['last_reconcile_status']} "
            f"reason_code={health['last_reconcile_reason_code'] or '-'}"
        )

    print("[HEALTH]")
    print("  [RISK-SNAPSHOT]")
    print(
        "    "
        f"unresolved_open_order_count={health['unresolved_open_order_count']} "
        f"recovery_required_count={health['recovery_required_count']} "
        f"submit_unknown_count={submit_unknown_count}"
    )
    print(f"    current_halt_reason={current_halt_reason}")
    print(f"    reconcile_latest={reconcile_latest}")
    print(f"  last_candle_age_sec={health['last_candle_age_sec']}")
    print(f"  error_count={health['error_count']}")
    print(f"  trading_enabled={health['trading_enabled']}")
    print(f"  retry_at_epoch_sec={health['retry_at_epoch_sec']}")
    print(f"  unresolved_open_order_count={health['unresolved_open_order_count']}")
    print(f"  oldest_unresolved_order_age_sec={health['oldest_unresolved_order_age_sec']}")
    print(f"  recovery_required_count={health['recovery_required_count']}")
    print(f"  last_reconcile_epoch_sec={health['last_reconcile_epoch_sec']}")
    print(f"  last_reconcile_status={health['last_reconcile_status']}")
    print(f"  last_reconcile_error={health['last_reconcile_error']}")
    print(f"  last_reconcile_reason_code={health['last_reconcile_reason_code']}")
    print(f"  last_reconcile_metadata={health['last_reconcile_metadata']}")
    print(f"  last_disable_reason={health['last_disable_reason']}")
    print(f"  halt_new_orders_blocked={health['halt_new_orders_blocked']}")
    print(f"  halt_reason_code={health['halt_reason_code']}")
    print(f"  halt_state_unresolved={health['halt_state_unresolved']}")
    print(f"  last_cancel_open_orders_epoch_sec={health['last_cancel_open_orders_epoch_sec']}")
    print(f"  last_cancel_open_orders_trigger={health['last_cancel_open_orders_trigger']}")
    print(f"  last_cancel_open_orders_status={health['last_cancel_open_orders_status']}")
    print(f"  last_cancel_open_orders_summary={health['last_cancel_open_orders_summary']}")
    print(f"  startup_gate_reason={health['startup_gate_reason']}")


def _eod_price_for_day(conn: sqlite3.Connection, day: str) -> float | None:
    row = conn.execute(
        """
        SELECT close
        FROM candles
        WHERE pair=? AND interval=? AND strftime('%Y-%m-%d', ts/1000, 'unixepoch', '+9 hours')=?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (PAIR, INTERVAL, day),
    ).fetchone()
    if row is None:
        return None
    return float(row["close"])


def _ledger_replay(conn: sqlite3.Connection) -> dict[str, float | int | bool]:
    init_portfolio(conn)
    cash = float(settings.START_CASH_KRW)
    qty = 0.0
    total_fee = 0.0
    dup_fill_count = 0

    seen_fill_keys: set[tuple[str, int, float, float]] = set()
    fills = conn.execute(
        """
        SELECT f.client_order_id, f.fill_ts, f.price, f.qty, f.fee, o.side
        FROM fills f
        JOIN orders o ON o.client_order_id = f.client_order_id
        ORDER BY f.fill_ts ASC, f.id ASC
        """
    ).fetchall()

    for row in fills:
        key = (
            str(row["client_order_id"]),
            int(row["fill_ts"]),
            float(row["price"]),
            float(row["qty"]),
        )
        if key in seen_fill_keys:
            dup_fill_count += 1
        seen_fill_keys.add(key)

        fill_price = float(row["price"])
        fill_qty = float(row["qty"])
        fee = float(row["fee"])
        side = str(row["side"])
        total_fee += fee

        if side == "BUY":
            cash -= (fill_price * fill_qty) + fee
            qty += fill_qty
        elif side == "SELL":
            cash += (fill_price * fill_qty) - fee
            qty -= fill_qty

    p = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    portfolio_cash = float(p["cash_available"]) + float(p["cash_locked"]) if p else 0.0
    portfolio_qty = float(p["asset_available"]) + float(p["asset_locked"]) if p else 0.0
    consistent = math.isclose(cash, portfolio_cash, abs_tol=1e-6) and math.isclose(qty, portfolio_qty, abs_tol=1e-10)

    return {
        "replay_cash": cash,
        "replay_qty": qty,
        "portfolio_cash": portfolio_cash,
        "portfolio_qty": portfolio_qty,
        "fee_total": total_fee,
        "dup_fill_count": dup_fill_count,
        "consistent": consistent,
    }


def cmd_audit_ledger() -> None:
    conn = ensure_db()
    try:
        replay = _ledger_replay(conn)
    finally:
        conn.close()

    print("[AUDIT-LEDGER]")
    print(f"  replay_cash={float(replay['replay_cash']):,.3f}")
    print(f"  replay_qty={float(replay['replay_qty']):.10f}")
    print(f"  portfolio_cash={float(replay['portfolio_cash']):,.3f}")
    print(f"  portfolio_qty={float(replay['portfolio_qty']):.10f}")
    print(f"  fee_total={float(replay['fee_total']):,.3f}")
    print(f"  dup_fill_count={int(replay['dup_fill_count'])}")

    if not bool(replay["consistent"]):
        print("[AUDIT-LEDGER] FAILED: replay result mismatches portfolio")
        raise SystemExit(1)

    print("[AUDIT-LEDGER] OK")


def cmd_report(days: int) -> None:
    conn = ensure_db()
    try:
        now_kst = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9))).date()
        start_day = now_kst - timedelta(days=days - 1)
        rows: list[tuple[str, float, float, float, float, float, float]] = []

        prev_end = float(settings.START_CASH_KRW)
        peak = prev_end
        mdd = 0.0
        slippage_bps = float(settings.SLIPPAGE_BPS)

        for i in range(days):
            day = (start_day + timedelta(days=i)).isoformat()
            agg = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN side='SELL' THEN (price * qty) ELSE 0 END), 0) AS sell_notional,
                    COALESCE(SUM(CASE WHEN side='BUY' THEN (price * qty) ELSE 0 END), 0) AS buy_notional,
                    COALESCE(SUM(fee), 0) AS fee_total
                FROM trades
                WHERE strftime('%Y-%m-%d', ts/1000, 'unixepoch', '+9 hours')=?
                """,
                (day,),
            ).fetchone()

            day_fee = float(agg["fee_total"])
            realized = float(agg["sell_notional"]) - float(agg["buy_notional"]) - day_fee
            slippage_cost = (float(agg["sell_notional"]) + float(agg["buy_notional"])) * (slippage_bps / 10000.0)

            eod = conn.execute(
                """
                SELECT cash_after, asset_after
                FROM trades
                WHERE strftime('%Y-%m-%d', ts/1000, 'unixepoch', '+9 hours')=?
                ORDER BY ts DESC, id DESC
                LIMIT 1
                """,
                (day,),
            ).fetchone()

            if eod is None:
                end_equity = prev_end
                unrealized = 0.0
            else:
                eod_price = _eod_price_for_day(conn, day)
                if eod_price is None:
                    eod_price = float(eod["cash_after"]) if float(eod["asset_after"]) == 0 else 0.0
                end_equity = float(eod["cash_after"]) + (float(eod["asset_after"]) * float(eod_price))
                unrealized = end_equity - prev_end - realized

            drawdown = 0.0
            if peak > 0:
                drawdown = (peak - end_equity) / peak
            peak = max(peak, end_equity)
            mdd = max(mdd, drawdown)

            rows.append((day, prev_end, end_equity, realized, unrealized, day_fee, slippage_cost))
            prev_end = end_equity

        expected_bars = int((24 * 60 * 60 / parse_interval_sec(INTERVAL)) * days)
        candle_count = int(
            conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM candles
                WHERE pair=? AND interval=? AND ts >= ?
                """,
                (
                    PAIR,
                    INTERVAL,
                    int(datetime.combine(start_day, datetime.min.time(), tzinfo=timezone(timedelta(hours=9))).timestamp() * 1000),
                ),
            ).fetchone()["c"]
        )
        missing_rate = 1.0 - (candle_count / expected_bars) if expected_bars > 0 else 0.0

        replay = _ledger_replay(conn)
        dup_orders = int(
            conn.execute(
                "SELECT COALESCE(SUM(cnt-1),0) FROM (SELECT client_order_id, COUNT(*) cnt FROM orders GROUP BY client_order_id HAVING COUNT(*)>1)"
            ).fetchone()[0]
        )
    finally:
        conn.close()

    print(f"[REPORT] days={days} pair={PAIR} interval={INTERVAL}")
    print("day,start_equity,end_equity,realized,unrealized,fee,slippage_est")
    for day, start_eq, end_eq, realized, unrealized, fee_total, slip in rows:
        print(f"{day},{start_eq:.2f},{end_eq:.2f},{realized:.2f},{unrealized:.2f},{fee_total:.2f},{slip:.2f}")

    target_daily = 0.001
    avg_daily = ((rows[-1][2] / rows[0][1]) ** (1 / len(rows)) - 1) if rows and rows[0][1] > 0 else 0.0
    gate_missing = missing_rate <= 0.05
    gate_dup = dup_orders == 0 and int(replay["dup_fill_count"]) == 0
    gate_consistency = bool(replay["consistent"])
    gate_mdd = mdd <= 0.20
    gate_pnl = avg_daily >= target_daily
    gate_pass = gate_missing and gate_dup and gate_consistency and gate_mdd and gate_pnl

    print("[SUMMARY]")
    print(f"  avg_daily_return={avg_daily * 100:.4f}%")
    print(f"  mdd={mdd * 100:.2f}%")
    print(f"  missing_candle_rate={missing_rate * 100:.2f}%")
    print(f"  fee_total={float(replay['fee_total']):,.2f}")
    print(f"  ledger_consistent={replay['consistent']}")
    print(f"  duplicate_orders={dup_orders}, duplicate_fills={int(replay['dup_fill_count'])}")
    print("[GATE]")
    print(f"  data_missing_rate<=5%: {'PASS' if gate_missing else 'FAIL'}")
    print(f"  duplicate_orders/fills==0: {'PASS' if gate_dup else 'FAIL'}")
    print(f"  ledger_consistency: {'PASS' if gate_consistency else 'FAIL'}")
    print(f"  drawdown<=20%: {'PASS' if gate_mdd else 'FAIL'}")
    print(f"  avg_daily_return>=0.10%: {'PASS' if gate_pnl else 'FAIL'}")
    print(f"  => {'PASS' if gate_pass else 'FAIL'}")


def cmd_cancel_open_orders() -> None:
    if settings.MODE != "live":
        print(f"[CANCEL-OPEN-ORDERS] skipped: MODE={settings.MODE} (live only)")
        return

    from .broker.bithumb import BithumbBroker

    broker = BithumbBroker()
    summary = cancel_open_orders_with_broker(broker)
    status = "partial" if int(summary["failed_count"]) > 0 else "ok"
    runtime_state.record_cancel_open_orders_result(
        trigger="operator-command",
        status=status,
        summary=summary,
    )

    print("[CANCEL-OPEN-ORDERS]")
    print(f"  remote_open_count={summary['remote_open_count']}")
    print(f"  canceled_count={summary['canceled_count']}")
    print(f"  matched_local_count={summary['matched_local_count']}")
    print(f"  stray_canceled_count={summary['stray_canceled_count']}")
    print(f"  failed_count={summary['failed_count']}")
    for msg in summary["stray_messages"]:
        print(f"  - {msg}")
    for msg in summary["error_messages"]:
        print(f"  - {msg}")


def _last_reconcile_failed(state) -> bool:
    status = str(getattr(state, "last_reconcile_status", "") or "").upper()
    return status in {"FAILED", "ERROR"}


def _load_recovery_report(
    *,
    oldest_limit: int = 5,
) -> dict[str, int | float | str | bool | None | list[dict[str, str | float | bool]]]:
    conn = conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
        unresolved_row = conn.execute(
            f"""
            SELECT COUNT(*) AS unresolved_count, MIN(created_ts) AS oldest_created_ts
            FROM orders
            WHERE status IN ({placeholders})
            """,
            OPEN_ORDER_STATUSES,
        ).fetchone()
        recovery_required_row = conn.execute(
            "SELECT COUNT(*) AS recovery_required_count FROM orders WHERE status='RECOVERY_REQUIRED'"
        ).fetchone()
        submit_unknown_row = conn.execute(
            "SELECT COUNT(*) AS submit_unknown_count FROM orders WHERE status='SUBMIT_UNKNOWN'"
        ).fetchone()
        oldest_rows = conn.execute(
            f"""
            SELECT client_order_id, status, exchange_order_id, created_ts, last_error
            FROM orders
            WHERE status IN ({placeholders})
            ORDER BY created_ts ASC
            LIMIT ?
            """,
            (*OPEN_ORDER_STATUSES, oldest_limit),
        ).fetchall()
        recovery_required_rows = conn.execute(
            """
            SELECT client_order_id, status, exchange_order_id, created_ts, last_error
            FROM orders
            WHERE status='RECOVERY_REQUIRED'
            ORDER BY created_ts ASC
            LIMIT ?
            """,
            (oldest_limit,),
        ).fetchall()
        health_row = conn.execute(
            """
            SELECT
                halt_reason_code,
                halt_state_unresolved,
                last_disable_reason,
                last_reconcile_epoch_sec,
                last_reconcile_status,
                last_reconcile_error,
                last_reconcile_reason_code,
                last_reconcile_metadata
            FROM bot_health
            WHERE id=1
            """
        ).fetchone()
        recent_order_lifecycle = load_recent_order_lifecycle(conn, limit=oldest_limit)
    finally:
        conn.close()

    unresolved_count = int(unresolved_row["unresolved_count"] if unresolved_row else 0)
    recovery_required_count = int(recovery_required_row["recovery_required_count"] if recovery_required_row else 0)
    submit_unknown_count = int(submit_unknown_row["submit_unknown_count"] if submit_unknown_row else 0)

    oldest_created_ts = unresolved_row["oldest_created_ts"] if unresolved_row else None
    oldest_age_sec = None
    if unresolved_count > 0 and oldest_created_ts is not None:
        oldest_age_sec = max(0.0, (time.time() * 1000 - float(oldest_created_ts)) / 1000)

    now_ms = time.time() * 1000

    oldest_orders: list[dict[str, str | float]] = []
    for row in oldest_rows:
        last_error = str(row["last_error"] or "").strip()
        oldest_orders.append(
            {
                "client_order_id": str(row["client_order_id"]),
                "status": str(row["status"]),
                "exchange_order_id": str(row["exchange_order_id"] or "-"),
                "age_sec": max(0.0, (now_ms - float(row["created_ts"])) / 1000),
                "last_error": (last_error[:60] + "...") if len(last_error) > 60 else (last_error or "-"),
            }
        )

    recovery_required_orders: list[dict[str, str | float]] = []
    for row in recovery_required_rows:
        last_error = str(row["last_error"] or "").strip()
        recovery_required_orders.append(
            {
                "client_order_id": str(row["client_order_id"]),
                "status": str(row["status"]),
                "exchange_order_id": str(row["exchange_order_id"] or "-"),
                "age_sec": max(0.0, (now_ms - float(row["created_ts"])) / 1000),
                "last_error": (last_error[:60] + "...") if len(last_error) > 60 else (last_error or "-"),
            }
        )

    last_reconcile_summary = "none"
    if health_row and health_row["last_reconcile_status"]:
        pieces = [
            f"status={health_row['last_reconcile_status']}",
            f"reason_code={health_row['last_reconcile_reason_code'] or '-'}",
        ]
        if health_row["last_reconcile_epoch_sec"] is not None:
            pieces.append(f"epoch_sec={float(health_row['last_reconcile_epoch_sec']):.3f}")
            pieces.append(f"age_sec={max(0.0, time.time() - float(health_row['last_reconcile_epoch_sec'])):.1f}")
        if health_row["last_reconcile_error"]:
            pieces.append(f"error={health_row['last_reconcile_error']}")
        last_reconcile_summary = " ".join(pieces)

    recent_halt_reason = "none"
    if health_row and (health_row["halt_reason_code"] or health_row["last_disable_reason"]):
        recent_halt_reason = (
            f"code={health_row['halt_reason_code'] or '-'} "
            f"reason={health_row['last_disable_reason'] or '-'} "
            f"unresolved={1 if bool(health_row['halt_state_unresolved']) else 0}"
        )

    unprocessed_remote_open_orders = 0
    balance_split_mismatch_summary = "none"
    if health_row and health_row["last_reconcile_metadata"]:
        try:
            reconcile_meta = json.loads(str(health_row["last_reconcile_metadata"]))
        except json.JSONDecodeError:
            reconcile_meta = {}
        raw_count = reconcile_meta.get("remote_open_order_found", 0)
        try:
            unprocessed_remote_open_orders = max(0, int(raw_count))
        except (TypeError, ValueError):
            unprocessed_remote_open_orders = 0
        raw_mismatch_summary = str(reconcile_meta.get("balance_split_mismatch_summary") or "").strip()
        if raw_mismatch_summary:
            balance_split_mismatch_summary = raw_mismatch_summary

    resume_allowed, blockers = evaluate_resume_eligibility()
    blocker_list: list[dict[str, str | bool]] = [
        {"code": b.code, "detail": b.detail, "overridable": bool(b.overridable)}
        for b in blockers
    ]
    non_overridable_blockers = [b for b in blocker_list if not bool(b["overridable"])]
    primary_blocker_code = str(blocker_list[0]["code"]) if blocker_list else "-"
    blocker_summary = (
        f"total={len(blocker_list)} "
        f"non_overridable={len(non_overridable_blockers)} "
        f"overridable={len(blocker_list) - len(non_overridable_blockers)}"
    )

    if bool(resume_allowed):
        operator_next_action = "resume_now"
        recommended_command = "uv run python bot.py resume"
        recommended_next_action = "No active blocker. Resume trading now."
        resume_blocked_reason = "none"
    elif blocker_list and all(bool(b["overridable"]) for b in blocker_list):
        operator_next_action = "review_and_force_resume"
        recommended_command = "uv run python bot.py resume --force"
        recommended_next_action = "Review overridable blockers and force resume only if risk is accepted."
        resume_blocked_reason = "resume blocked by overridable blockers"
    elif recovery_required_count > 0:
        operator_next_action = "manual_recovery_required"
        recommended_command = "uv run python bot.py recover-order --client-order-id <id>"
        recommended_next_action = "Recover RECOVERY_REQUIRED orders before attempting resume."
        resume_blocked_reason = "resume blocked by RECOVERY_REQUIRED orders"
    else:
        operator_next_action = "investigate_blockers"
        recommended_command = "uv run python bot.py recovery-report --json"
        recommended_next_action = "Investigate non-overridable blockers and clear the root cause first."
        resume_blocked_reason = "resume blocked by non-overridable safety blockers"

    active_blocker_summary = "none"
    if blocker_list:
        active_blocker_summary = " | ".join(
            f"{b['code']}(overridable={1 if bool(b['overridable']) else 0})"
            for b in blocker_list[:3]
        )

    risk_level = "low"
    if recovery_required_count > 0 or non_overridable_blockers:
        risk_level = "high"
    elif unresolved_count > 0 or blocker_list:
        risk_level = "medium"

    state = runtime_state.snapshot()

    return {
        "unresolved_count": unresolved_count,
        "recovery_required_count": recovery_required_count,
        "submit_unknown_count": submit_unknown_count,
        "oldest_unresolved_age_sec": oldest_age_sec,
        "oldest_orders": oldest_orders,
        "last_reconcile_summary": last_reconcile_summary,
        "recent_halt_reason": recent_halt_reason,
        "unprocessed_remote_open_orders": unprocessed_remote_open_orders,
        "balance_split_mismatch_summary": balance_split_mismatch_summary,
        "trading_enabled": bool(state.trading_enabled),
        "resume_allowed": bool(resume_allowed),
        "force_resume_allowed": all(bool(b.overridable) for b in blockers),
        "blockers": blocker_list,
        "blocker_summary": blocker_summary,
        "active_blocker_summary": active_blocker_summary,
        "risk_level": risk_level,
        "primary_blocker_code": primary_blocker_code,
        "non_overridable_blockers": non_overridable_blockers,
        "unresolved_summary": oldest_orders,
        "recovery_required_summary": recovery_required_orders,
        "operator_next_action": operator_next_action,
        "recommended_next_action": recommended_next_action,
        "resume_blocked_reason": resume_blocked_reason,
        "recommended_command": recommended_command,
        "recent_order_lifecycle": recent_order_lifecycle,
    }


def cmd_recovery_report(*, as_json: bool = False) -> None:
    report = _load_recovery_report()
    if as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return

    print("[RECOVERY-REPORT]")
    print("  [P1] order_recovery_status")
    print(f"    unresolved_count={report['unresolved_count']}")
    print(f"    recovery_required_count={report['recovery_required_count']}")
    print(f"    submit_unknown_count={report['submit_unknown_count']}")
    print("  [P2] resume_eligibility")
    print(f"    resume_allowed={1 if bool(report['resume_allowed']) else 0}")
    print(f"    force_resume_allowed={1 if bool(report['force_resume_allowed']) else 0}")
    blockers = report.get("blockers") or []
    print(f"    blocker_summary={report['blocker_summary']}")
    print(f"    active_blocker_summary={report['active_blocker_summary']}")
    print(f"    risk_level={report['risk_level']}")
    print(f"    primary_blocker_code={report['primary_blocker_code']}")
    for blocker in blockers:
        print(
            "    - "
            f"code={blocker['code']} "
            f"overridable={1 if bool(blocker['overridable']) else 0} "
            f"detail={blocker['detail']}"
        )
    print("  [P3] balance_mismatch")
    print(f"    summary={report['balance_split_mismatch_summary']}")
    print("  [P4] last_reconcile_summary")
    print(f"    {report['last_reconcile_summary']}")
    print("  [P5] recent_halt_reason")
    print(f"    {report['recent_halt_reason']}")
    print("  [P6] operator_next_action")
    print(f"    action={report['operator_next_action']}")
    print(f"    recommended_next_action={report['recommended_next_action']}")
    print(f"    resume_blocked_reason={report['resume_blocked_reason']}")
    print(f"    command={report['recommended_command']}")
    print("    hint=check blocker code then run command")
    print("  [P7] unprocessed_remote_open_orders")
    print(f"    count={report['unprocessed_remote_open_orders']}")
    lifecycle = report.get("recent_order_lifecycle") or []
    if lifecycle:
        print(f"  [P8] recent_order_lifecycle(top {len(lifecycle)}):")
        for item in lifecycle:
            print(
                "    - "
                f"client_order_id={item['client_order_id']} "
                f"intent_ts={item['intent_ts']} "
                f"submit_ts={item['submit_ts']} "
                f"correlation={item['correlation']} "
                f"mapping={item['mapping_status']} "
                f"state={item['state']} "
                f"unresolved={item['unresolved']}"
            )

    if report["oldest_unresolved_age_sec"] is None:
        print("  oldest_unresolved_age_sec=none")
    else:
        print(f"  oldest_unresolved_age_sec={float(report['oldest_unresolved_age_sec']):.1f}")

    oldest_orders = report.get("oldest_orders") or []
    if oldest_orders:
        print(f"  oldest_unresolved_orders(top {len(oldest_orders)}):")
        for item in oldest_orders:
            print(
                "    - "
                f"client_order_id={item['client_order_id']} "
                f"status={item['status']} "
                f"exchange_order_id={item['exchange_order_id']} "
                f"age_sec={float(item['age_sec']):.1f} "
                f"last_error={item['last_error']}"
            )

    recovery_required_orders = report.get("recovery_required_summary") or []
    if recovery_required_orders:
        print(f"  recovery_required_orders(top {len(recovery_required_orders)}):")
        for item in recovery_required_orders:
            print(
                "    - "
                f"client_order_id={item['client_order_id']} "
                f"exchange_order_id={item['exchange_order_id']} "
                f"age_sec={float(item['age_sec']):.1f} "
                f"reason={item['last_error']}"
            )

def _last_reconcile_failed(state) -> bool:
    status = str(getattr(state, "last_reconcile_status", "") or "").upper()
    return status in {"FAILED", "ERROR"}

def cmd_pause() -> None:
    runtime_state.enter_halt(
        reason_code="MANUAL_PAUSE",
        reason="manual operator pause",
        unresolved=False,
    )
    print("[PAUSE] trading disabled via persistent runtime state")


def cmd_resume(force: bool = False) -> None:
    if settings.MODE == "live":
        from .broker.bithumb import BithumbBroker

        reconcile_with_broker(BithumbBroker())

    eligible, resume_blocks = evaluate_resume_eligibility()

    if (not eligible) and (not force):
        print("[RESUME] refused:")
        print("  blocking_reasons:")
        for blocker in resume_blocks:
            print(
                "  - "
                f"code={blocker.code} "
                f"overridable={1 if bool(blocker.overridable) else 0} "
                f"detail={blocker.detail}"
            )
        print("  run `uv run python bot.py recovery-report` for details")
        print("  or resume explicitly with `uv run python bot.py resume --force`")
        raise SystemExit(1)

    if force and resume_blocks:
        non_overridable_blocks = [b for b in resume_blocks if not bool(b.overridable)]
        if non_overridable_blocks:
            print("[RESUME] refused: force override denied")
            print("  non_overridable_blockers:")
            for blocker in non_overridable_blocks:
                print(
                    "  - "
                    f"code={blocker.code} "
                    f"overridable={1 if bool(blocker.overridable) else 0} "
                    f"detail={blocker.detail}"
                )
            print("  run `uv run python bot.py recovery-report` for details")
            raise SystemExit(1)

    enable_trading()
    if force and resume_blocks:
        block_summary = "; ".join(
            f"{blocker.code}[overridable={1 if bool(blocker.overridable) else 0}]:{blocker.detail}"
            for blocker in resume_blocks
        )
        print(f"[RESUME] forced: trading enabled despite blocks={block_summary}")
        print("[RESUME] override_applied=1 override_reason=operator_force_resume")
    else:
        print("[RESUME] trading enabled")


def cmd_reconcile() -> None:
    if settings.MODE != "live":
        print(f"[RECONCILE] skipped: MODE={settings.MODE} (live only)")
        return

    from .broker.bithumb import BithumbBroker

    reconcile_with_broker(BithumbBroker())
    print("[RECONCILE] completed one live reconciliation pass")


def cmd_recover_order(*, client_order_id: str, exchange_order_id: str) -> None:
    if settings.MODE != "live":
        print(f"[RECOVER-ORDER] skipped: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    from .broker.bithumb import BithumbBroker

    disable_trading_until(float("inf"), reason="manual recovery in progress")
    try:
        recover_order_with_exchange_id(
            BithumbBroker(),
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
    except Exception as e:
        disable_trading_until(float("inf"), reason="manual recovery failed; resume required")
        print(f"[RECOVER-ORDER] failed: {type(e).__name__}: {e}")
        print("  order remains RECOVERY_REQUIRED; inspect and retry")
        raise SystemExit(1)

    disable_trading_until(float("inf"), reason="manual recovery completed; explicit resume required")
    print("[RECOVER-ORDER] completed")
    print("  trading remains disabled; run `uv run python bot.py resume` when ready")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bithumb-bot")
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

    sub.add_parser("status")
    sub.add_parser("audit")
    sub.add_parser("check")
    sub.add_parser("health")
    sub.add_parser("audit-ledger")
    sub.add_parser("cancel-open-orders")
    sub.add_parser("pause")

    resume = sub.add_parser("resume")
    resume.add_argument("--force", action="store_true")

    sub.add_parser("reconcile")
    recovery_report = sub.add_parser("recovery-report")
    recovery_report.add_argument("--json", action="store_true")
    recover_order = sub.add_parser("recover-order")
    recover_order.add_argument("--client-order-id", required=True)
    recover_order.add_argument("--exchange-order-id", required=True)

    report = sub.add_parser("report")
    report.add_argument("--days", type=int, default=30)

    t = sub.add_parser("trades")
    t.add_argument("--limit", type=int, default=20)

    r = sub.add_parser("run")
    r.add_argument("--short", type=int, default=SMA_SHORT)
    r.add_argument("--long", type=int, default=SMA_LONG)

    args = p.parse_args(argv)

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
    elif args.cmd == "health":
        cmd_health()
    elif args.cmd == "trades":
        cmd_trades(args.limit)
    elif args.cmd == "orders":
        cmd_orders(args.limit)
    elif args.cmd == "fills":
        cmd_fills(args.limit)
    elif args.cmd == "report":
        cmd_report(max(1, int(args.days)))
    elif args.cmd == "audit-ledger":
        cmd_audit_ledger()
    elif args.cmd == "cancel-open-orders":
        cmd_cancel_open_orders()
    elif args.cmd == "pause":
        cmd_pause()
    elif args.cmd == "resume":
        cmd_resume(force=bool(args.force))
    elif args.cmd == "reconcile":
        cmd_reconcile()
    elif args.cmd == "recovery-report":
        cmd_recovery_report(as_json=bool(args.json))
    elif args.cmd == "recover-order":
        cmd_recover_order(
            client_order_id=str(args.client_order_id),
            exchange_order_id=str(args.exchange_order_id),
        )
    elif args.cmd == "run":
        cmd_run(args.short, args.long)
    else:
        p.print_help()
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
