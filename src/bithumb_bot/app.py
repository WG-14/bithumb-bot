from .config import (
    LiveModeValidationError,
    ModeValidationError,
    PATH_MANAGER,
    settings,
    validate_live_mode_preflight,
    validate_mode_or_raise,
)
from .risk import evaluate_buy_guardrails
from .broker.paper import paper_execute
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
from .engine import compute_signal, evaluate_resume_eligibility, get_health_status, maybe_clear_stale_initial_reconcile_halt
from .recovery import (
    cancel_open_orders_with_broker,
    load_recent_order_lifecycle,
    reconcile_with_broker,
    recover_order_with_exchange_id,
)
from .runtime_state import disable_trading_until, enable_trading, refresh_open_order_health
from .notifier import notify
from .observability import safety_event
from .broker.order_rules import get_effective_order_rules, rule_source_for
from .broker import bithumb as bithumb_broker_module
from .broker.base import BrokerBalance, BrokerOrder
from . import runtime_state
from .oms import OPEN_ORDER_STATUSES
from .flatten import flatten_btc_position
from .markets import canonical_market_with_raw
from .reporting import cmd_fee_diagnostics, cmd_ops_report, cmd_strategy_report, parse_kst_date_range_to_ts_ms
from .storage_io import write_json_atomic

import httpx

MODE = settings.MODE
PAIR = settings.PAIR
MARKET, RAW_SYMBOL = canonical_market_with_raw(PAIR)
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
DEFAULT_BITHUMB_BROKER_CLASS = bithumb_broker_module.BithumbBroker


def _format_rule_value_with_source(*, field: str, value: object, source: dict[str, str] | None) -> str:
    return f"{value} (source={rule_source_for(field, source)})"


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

    raw_suffix = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[SIGNAL {MARKET} {INTERVAL}{raw_suffix}] at {kst_str(r['ts'])}")
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
    raw_suffix = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[EXPLAIN {MARKET} {INTERVAL}{raw_suffix}] last {need} closes (시간순)")
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
    raw_suffix = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[STATUS {MARKET} {INTERVAL}{raw_suffix}] at {kst_str(ts)}")
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
        with acquire_run_lock(Path(settings.RUN_LOCK_PATH)):
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
    halt_reason_for_summary = "none"
    if health["halt_reason_code"] or health["last_disable_reason"]:
        halt_reason_for_summary = str(health["halt_reason_code"] or "-")
        current_halt_reason = (
            f"code={health['halt_reason_code'] or '-'} "
            f"reason={health['last_disable_reason'] or '-'}"
        )

    open_order_count = 0
    remote_open_order_count: int | None = None
    position_summary = "flat"
    portfolio_conn = ensure_db()
    try:
        open_order_row = portfolio_conn.execute(
            """
            SELECT COUNT(*) AS open_order_count
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'RECOVERY_REQUIRED')
            """
        ).fetchone()
        if open_order_row is not None:
            open_order_count = int(open_order_row["open_order_count"] or 0)
        portfolio_row = portfolio_conn.execute(
            "SELECT asset_qty FROM portfolio WHERE id=1"
        ).fetchone()
        if portfolio_row is not None and abs(float(portfolio_row["asset_qty"] or 0.0)) > 1e-12:
            position_summary = f"long_qty={float(portfolio_row['asset_qty']):.8f}"
    finally:
        portfolio_conn.close()

    reconcile_metadata_raw = health.get("last_reconcile_metadata")
    if reconcile_metadata_raw:
        try:
            reconcile_metadata = json.loads(str(reconcile_metadata_raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            reconcile_metadata = {}
        raw_remote_open_count = reconcile_metadata.get("remote_open_order_found")
        if raw_remote_open_count is not None:
            try:
                remote_open_order_count = max(0, int(raw_remote_open_count))
            except (TypeError, ValueError):
                remote_open_order_count = None

    state_label = "running"
    if bool(health["halt_new_orders_blocked"]):
        state_label = "halted"
    elif not bool(health["trading_enabled"]):
        state_label = "paused"

    resume_allowed, resume_blockers = evaluate_resume_eligibility()
    resume_blocker_codes = [str(blocker.code) for blocker in resume_blockers]
    can_resume_label = "true" if bool(resume_allowed) else "false"
    blockers_label = ", ".join(resume_blocker_codes) if resume_blocker_codes else "none"
    unsafe_reasons: list[str] = []
    if not bool(resume_allowed):
        for blocker in resume_blockers[:3]:
            unsafe_reasons.append(str(blocker.code))
        if not unsafe_reasons:
            unsafe_reasons.append("RESUME_BLOCKED")
    if unsafe_reasons:
        resume_safety = f"unsafe ({', '.join(unsafe_reasons)})"
    else:
        resume_safety = "safe"

    recommended_commands = "uv run python bot.py recovery-report"
    if health["startup_gate_reason"]:
        halt_reason_for_summary = "STARTUP_SAFETY_GATE"
        recommended_commands = "uv run python bot.py reconcile | uv run python bot.py recovery-report"
    elif bool(health.get("emergency_flatten_blocked")):
        halt_reason_for_summary = "EMERGENCY_FLATTEN_UNRESOLVED"
        recommended_commands = "uv run python bot.py flatten-position | uv run python bot.py recovery-report"
    elif health["recovery_required_count"] > 0:
        recommended_commands = (
            "uv run python bot.py recover-order --client-order-id <id>"
            " | uv run python bot.py recovery-report"
        )
    elif halt_reason_for_summary == "KILL_SWITCH":
        recommended_commands = "uv run python bot.py recovery-report | uv run python bot.py resume"

    has_critical_state = bool(
        health["startup_gate_reason"]
        or health["halt_new_orders_blocked"]
        or bool(health.get("emergency_flatten_blocked"))
        or health["recovery_required_count"] > 0
    )

    reconcile_latest = "none"
    if health["last_reconcile_status"]:
        reconcile_latest = (
            f"epoch_sec={health['last_reconcile_epoch_sec'] if health['last_reconcile_epoch_sec'] is not None else '-'} "
            f"status={health['last_reconcile_status']} "
            f"reason_code={health['last_reconcile_reason_code'] or '-'}"
        )

    candle_status = str(health.get("last_candle_status") or "unknown")
    candle_age = health.get("last_candle_age_sec")
    candle_observed = health.get("last_candle_sync_epoch_sec")
    candle_ts_ms = health.get("last_candle_ts_ms")
    candle_detail = health.get("last_candle_status_detail")

    print("[HEALTH]")
    print("  [HALT-RECOVERY-STATUS]")
    print(
        "    "
        f"state={state_label} "
        f"trading_enabled={1 if bool(health['trading_enabled']) else 0} "
        f"halt_new_orders_blocked={1 if bool(health['halt_new_orders_blocked']) else 0}"
    )
    print(f"    reason={current_halt_reason}")
    print(
        "    "
        f"unresolved_open_order_count={health['unresolved_open_order_count']} "
        f"open_order_count={open_order_count} "
        f"recovery_required_count={health['recovery_required_count']}"
    )
    if remote_open_order_count is None:
        print("    broker_open_order_count=unknown")
    else:
        print(f"    broker_open_order_count={remote_open_order_count}")
    print(f"    position={position_summary}")
    print(f"    can_resume={can_resume_label}")
    print(f"    blockers={blockers_label}")
    print(f"    resume_safety={resume_safety}")

    print("  [RISK-SNAPSHOT]")
    print(
        "    "
        f"unresolved_open_order_count={health['unresolved_open_order_count']} "
        f"recovery_required_count={health['recovery_required_count']} "
        f"submit_unknown_count={submit_unknown_count}"
    )
    print(f"    current_halt_reason={current_halt_reason}")
    print(f"    reconcile_latest={reconcile_latest}")
    if has_critical_state:
        print("  [CRITICAL-OPERATOR-SUMMARY]")
        print(
            "    "
            f"halt_reason={halt_reason_for_summary} "
            f"unresolved_order_count={health['unresolved_open_order_count']} "
            f"open_order_count={open_order_count} "
            f"position={position_summary}"
        )
        print(f"    next_commands={recommended_commands}")
    print("  [ORDER-RULE-SNAPSHOT]")
    try:
        resolved_rules = get_effective_order_rules(PAIR)
        rules = resolved_rules.rules
        source = resolved_rules.source or {}
        print(
            "    "
            f"min_qty={_format_rule_value_with_source(field='min_qty', value=rules.min_qty, source=source)} "
            f"qty_step={_format_rule_value_with_source(field='qty_step', value=rules.qty_step, source=source)} "
            f"min_notional_krw={_format_rule_value_with_source(field='min_notional_krw', value=rules.min_notional_krw, source=source)} "
            f"max_qty_decimals={_format_rule_value_with_source(field='max_qty_decimals', value=rules.max_qty_decimals, source=source)}"
        )
        print(
            "    "
            f"BUY(min_total_krw={_format_rule_value_with_source(field='bid_min_total_krw', value=rules.bid_min_total_krw, source=source)}, "
            f"price_unit={_format_rule_value_with_source(field='bid_price_unit', value=rules.bid_price_unit, source=source)}) "
            f"SELL(min_total_krw={_format_rule_value_with_source(field='ask_min_total_krw', value=rules.ask_min_total_krw, source=source)}, "
            f"price_unit={_format_rule_value_with_source(field='ask_price_unit', value=rules.ask_price_unit, source=source)})"
        )
    except Exception as exc:
        print(f"    failed_to_load={type(exc).__name__}: {exc}")
    print(
        "  "
        f"last_candle_age_sec={candle_age} "
        f"(status={candle_status}, sync_epoch_sec={candle_observed if candle_observed is not None else '-'}, "
        f"candle_ts_ms={candle_ts_ms if candle_ts_ms is not None else '-'}, "
        f"detail={candle_detail or '-'})"
    )
    print(f"  last_candle_status={candle_status}")
    print(f"  last_candle_sync_epoch_sec={candle_observed}")
    print(f"  last_candle_ts_ms={candle_ts_ms}")
    print(f"  last_candle_status_detail={candle_detail}")
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
    print(f"  emergency_flatten_blocked={health.get('emergency_flatten_blocked')}")
    print(f"  emergency_flatten_block_reason={health.get('emergency_flatten_block_reason')}")
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



def _execution_quality_summary(conn: sqlite3.Connection, *, start_day: str) -> dict[str, float | int | None]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS fill_count,
            SUM(CASE WHEN reference_price IS NOT NULL AND slippage_bps IS NOT NULL THEN 1 ELSE 0 END) AS measured_count,
            AVG(slippage_bps) AS avg_slippage_bps,
            MAX(slippage_bps) AS worst_slippage_bps,
            MIN(slippage_bps) AS best_slippage_bps
        FROM fills
        WHERE strftime('%Y-%m-%d', fill_ts/1000, 'unixepoch', '+9 hours') >= ?
        """,
        (start_day,),
    ).fetchone()
    return {
        "fill_count": int(row["fill_count"] if row else 0),
        "measured_count": int(row["measured_count"] if row and row["measured_count"] is not None else 0),
        "avg_slippage_bps": (float(row["avg_slippage_bps"]) if row and row["avg_slippage_bps"] is not None else None),
        "worst_slippage_bps": (float(row["worst_slippage_bps"]) if row and row["worst_slippage_bps"] is not None else None),
        "best_slippage_bps": (float(row["best_slippage_bps"]) if row and row["best_slippage_bps"] is not None else None),
    }

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
        exec_quality = _execution_quality_summary(conn, start_day=start_day.isoformat())
        dup_orders = int(
            conn.execute(
                "SELECT COALESCE(SUM(cnt-1),0) FROM (SELECT client_order_id, COUNT(*) cnt FROM orders GROUP BY client_order_id HAVING COUNT(*)>1)"
            ).fetchone()[0]
        )
    finally:
        conn.close()

    raw_symbol_detail = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[REPORT] days={days} market={MARKET}{raw_symbol_detail} interval={INTERVAL}")
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
    print("[EXECUTION-QUALITY]")
    print(f"  fills={int(exec_quality['fill_count'])} measured={int(exec_quality['measured_count'])}")
    if exec_quality['avg_slippage_bps'] is None:
        print("  avg_slippage_bps=NA worst_slippage_bps=NA best_slippage_bps=NA")
    else:
        print(
            "  "
            f"avg_slippage_bps={float(exec_quality['avg_slippage_bps']):.3f} "
            f"worst_slippage_bps={float(exec_quality['worst_slippage_bps']):.3f} "
            f"best_slippage_bps={float(exec_quality['best_slippage_bps']):.3f}"
        )
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

    from .broker.bithumb import BithumbBroker, classify_private_api_error

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
    print(f"  cancel_accepted_count={summary['cancel_accepted_count']}")
    print(f"  canceled_count={summary['canceled_count']}")
    print(f"  cancel_confirm_pending_count={summary['cancel_confirm_pending_count']}")
    print(f"  matched_local_count={summary['matched_local_count']}")
    print(f"  stray_canceled_count={summary['stray_canceled_count']}")
    print(f"  failed_count={summary['failed_count']}")
    for msg in summary["stray_messages"]:
        print(f"  - {msg}")
    for msg in summary["error_messages"]:
        print(f"  - {msg}")


def cmd_broker_diagnose() -> None:
    if settings.MODE != "live":
        print(f"[BROKER-DIAGNOSE] failed: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    from .broker.bithumb import BithumbBroker, classify_private_api_error

    broker = BithumbBroker()
    checks: list[dict[str, str | bool]] = []
    account_validation_reason = "not_checked"
    account_validation_last_failure_reason = "none"

    def add_check(name: str, status: str, detail: str, *, critical: bool) -> None:
        checks.append({"name": name, "status": status, "detail": detail, "critical": critical})

    live_armed = settings.MODE == "live" and not settings.LIVE_DRY_RUN
    add_check(
        "live execution mode",
        "PASS",
        f"MODE={settings.MODE} LIVE_DRY_RUN={settings.LIVE_DRY_RUN} armed={live_armed}",
        critical=True,
    )
    add_check(
        "order submit routing",
        "PASS",
        "price=None => /v2/orders market/price order, price set => /v2/orders limit order",
        critical=True,
    )
    add_check(
        "order lookup path",
        "PASS",
        "get_order reads /v1/order directly; open/recent snapshots use /v1/orders",
        critical=True,
    )

    try:
        validate_live_mode_preflight(settings)
        add_check("config/env loaded", "PASS", "live preflight validation passed", critical=True)
    except LiveModeValidationError as e:
        add_check("config/env loaded", "FAIL", str(e), critical=True)

    balance = None
    try:
        balance = broker.get_balance()
        account_validation_reason = "ok"
        add_check("broker authentication", "PASS", "private API reachable", critical=True)
    except Exception as e:
        code, summary = classify_private_api_error(e)
        if code in {"AUTH_SIGN", "PERMISSION"}:
            account_validation_reason = "auth failure"
        elif code == "TEMPORARY":
            account_validation_reason = "transport failure"
        elif "missing quote currency row" in str(e).lower() or "missing base currency row" in str(e).lower():
            account_validation_reason = "required currency missing"
        elif "duplicate currency row" in str(e).lower():
            account_validation_reason = "duplicate currency"
        elif "schema mismatch" in str(e).lower():
            account_validation_reason = "schema mismatch"
        detail = f"private API failed [{code}] {summary} ({type(e).__name__}: {e})"
        add_check("broker authentication", "FAIL", detail, critical=True)
        add_check("balance query", "FAIL", detail, critical=True)

    if balance is not None:
        balance_summary = (
            f"cash_available={balance.cash_available:,.0f} cash_locked={balance.cash_locked:,.0f} "
            f"asset_available={balance.asset_available:.8f} asset_locked={balance.asset_locked:.8f}"
        )
        add_check("balance query", "PASS", balance_summary, critical=True)

    try:
        open_orders = broker.get_open_orders()
        add_check("open order query", "PASS", f"count={len(open_orders)}", critical=False)
    except Exception as e:
        add_check(
            "open order query",
            "WARN",
            f"snapshot failed ({type(e).__name__}: {e})",
            critical=False,
        )

    try:
        rr = get_effective_order_rules(PAIR)
        rules = rr.rules
        source = rr.source or {}
        add_check(
            "symbol/order rule query",
            "PASS",
            (
                f"min_qty={_format_rule_value_with_source(field='min_qty', value=rules.min_qty, source=source)} "
                f"qty_step={_format_rule_value_with_source(field='qty_step', value=rules.qty_step, source=source)} "
                f"min_notional_krw={_format_rule_value_with_source(field='min_notional_krw', value=rules.min_notional_krw, source=source)} "
                f"max_qty_decimals={_format_rule_value_with_source(field='max_qty_decimals', value=rules.max_qty_decimals, source=source)} "
                f"bid_min_total_krw={_format_rule_value_with_source(field='bid_min_total_krw', value=rules.bid_min_total_krw, source=source)} "
                f"ask_min_total_krw={_format_rule_value_with_source(field='ask_min_total_krw', value=rules.ask_min_total_krw, source=source)} "
                f"bid_price_unit={_format_rule_value_with_source(field='bid_price_unit', value=rules.bid_price_unit, source=source)} "
                f"ask_price_unit={_format_rule_value_with_source(field='ask_price_unit', value=rules.ask_price_unit, source=source)}"
            ),
            critical=False,
        )
    except Exception as e:
        add_check(
            "symbol/order rule query",
            "WARN",
            f"lookup failed ({type(e).__name__}: {e})",
            critical=False,
        )

    notifier_enabled = os.getenv("NOTIFIER_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "y",
    }
    has_notifier_target = any(
        [
            os.getenv("NOTIFIER_WEBHOOK_URL", "").strip(),
            os.getenv("SLACK_WEBHOOK_URL", "").strip(),
            os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
            and os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        ]
    )
    if not notifier_enabled:
        add_check("notifier configured", "WARN", "NOTIFIER_ENABLED=false", critical=False)
    elif has_notifier_target:
        add_check("notifier configured", "PASS", "delivery target detected", critical=False)
    else:
        add_check("notifier configured", "WARN", "no webhook/chat target configured", critical=False)

    try:
        conn = ensure_db()
        try:
            conn.execute("SAVEPOINT live_readiness_probe")
            conn.execute(
                "INSERT OR REPLACE INTO daily_risk(day_kst, start_equity) VALUES ('__live_readiness_probe__', 0.0)"
            )
            conn.execute("ROLLBACK TO live_readiness_probe")
            conn.execute("RELEASE live_readiness_probe")
            add_check("DB writable", "PASS", f"path={settings.DB_PATH}", critical=True)
        finally:
            conn.close()
    except Exception as e:
        add_check("DB writable", "FAIL", f"db write probe failed ({type(e).__name__}: {e})", critical=True)

    account_diag_raw = getattr(broker, "get_accounts_validation_diagnostics", lambda: {})()
    if isinstance(account_diag_raw, dict):
        account_validation_reason = str(account_diag_raw.get("reason") or account_validation_reason)
        account_validation_last_failure_reason = str(account_diag_raw.get("last_failure_reason") or "none")
        row_count = int(account_diag_raw.get("row_count") or 0)
        currencies = ",".join(str(item) for item in list(account_diag_raw.get("currencies") or [])[:20]) or "-"
        missing = ",".join(str(item) for item in list(account_diag_raw.get("missing_required_currencies") or [])[:10]) or "-"
        duplicate = ",".join(str(item) for item in list(account_diag_raw.get("duplicate_currencies") or [])[:10]) or "-"
        add_check(
            "accounts validation diagnostic",
            "PASS" if account_validation_reason == "ok" else "WARN",
            (
                f"reason={account_validation_reason} row_count={row_count} "
                f"currencies={currencies} missing_required_currencies={missing} duplicate_currencies={duplicate} "
                f"last_success={account_diag_raw.get('last_success_reason') or '-'} "
                f"last_failure={account_validation_last_failure_reason}"
            ),
            critical=False,
        )

    fail_count = sum(1 for check in checks if check["status"] == "FAIL")
    warn_count = sum(1 for check in checks if check["status"] == "WARN")
    pass_count = sum(1 for check in checks if check["status"] == "PASS")
    overall_status = "FAIL" if fail_count else ("WARN" if warn_count else "PASS")

    print("[BROKER-READINESS]")
    print(f"  market={MARKET}")
    if RAW_SYMBOL:
        print(f"  raw_symbol={RAW_SYMBOL}")
    print(f"  summary: pass={pass_count} warn={warn_count} fail={fail_count} overall={overall_status}")
    for check in checks:
        print(f"  - [{check['status']}] {check['name']}: {check['detail']}")

    if fail_count:
        raise SystemExit(1)


def _last_reconcile_failed(state) -> bool:
    status = str(getattr(state, "last_reconcile_status", "") or "").upper()
    return status in {"FAILED", "ERROR"}


def _safe_recent_broker_orders_snapshot(*, limit: int = 100) -> tuple[list[object], str | None]:
    if settings.MODE != "live":
        return [], "broker snapshot unavailable in non-live mode"
    try:
        from .broker.bithumb import BithumbBroker, classify_private_api_error

        return BithumbBroker().get_recent_orders(limit=limit), None
    except Exception as e:
        return [], f"failed to load recent broker orders: {type(e).__name__}: {e}"


def _build_recovery_candidates(*, local_order: dict[str, str | float], recent_orders: list[object]) -> list[dict[str, str | float | int]]:
    side = str(local_order["side"])
    qty_req = float(local_order["qty_req"])
    local_price_raw = local_order.get("price")
    local_price = float(local_price_raw) if local_price_raw is not None else None
    created_ts = int(local_order["created_ts"])
    submit_ts = int(local_order.get("submit_evidence_attempted_ts") or created_ts)

    ranked: list[tuple[int, dict[str, str | float | int | str]]] = []
    for remote in recent_orders:
        exchange_order_id = str(getattr(remote, "exchange_order_id", "") or "")
        if not exchange_order_id:
            continue

        remote_side = str(getattr(remote, "side", "") or "")
        remote_qty_req = float(getattr(remote, "qty_req", 0.0) or 0.0)
        remote_price_raw = getattr(remote, "price", None)
        remote_price = float(remote_price_raw) if remote_price_raw is not None else None
        remote_status = str(getattr(remote, "status", "") or "-")
        remote_created_ts = int(getattr(remote, "created_ts", 0) or 0)
        remote_updated_ts = int(getattr(remote, "updated_ts", 0) or 0)

        score = 0
        reasons: list[str] = []
        if str(getattr(remote, "client_order_id", "") or "") == str(local_order["client_order_id"]):
            score += 4
            reasons.append("same client_order_id")
        if remote_side == side:
            score += 3
            reasons.append("same side")
        else:
            reasons.append("side mismatch")

        qty_gap = abs(remote_qty_req - qty_req)
        qty_gap_pct = (qty_gap / max(qty_req, 1e-12)) * 100.0
        qty_tolerance = max(1e-12, max(qty_req, remote_qty_req) * 0.03)
        if qty_gap <= max(1e-12, max(qty_req, remote_qty_req) * 0.01):
            score += 3
            reasons.append("very close qty")
        elif qty_gap <= qty_tolerance:
            score += 2
            reasons.append("close qty")

        ts_gap_sec = abs(remote_created_ts - submit_ts) / 1000 if remote_created_ts > 0 else float("inf")
        if ts_gap_sec <= 90:
            score += 3
            reasons.append("close submit timestamp")
        elif ts_gap_sec <= 300:
            score += 2
            reasons.append("near submit timestamp")
        elif ts_gap_sec <= 600:
            score += 1
            reasons.append("recent timestamp")

        price_gap_pct: float | None = None
        strong_price_match = False
        if local_price is not None and remote_price is not None and local_price > 0:
            price_gap_pct = abs(remote_price - local_price) / local_price * 100.0
            if price_gap_pct <= 0.2:
                score += 2
                strong_price_match = True
                reasons.append("very close price")
            elif price_gap_pct <= 1.0:
                score += 1
                reasons.append("close price")
            else:
                score -= 2
                reasons.append("price mismatch")

        if remote_status in {"PARTIAL", "FILLED", "NEW"}:
            score += 1
            reasons.append(f"status={remote_status}")

        if score < 4:
            continue

        side_match = remote_side == side
        qty_match = qty_gap <= qty_tolerance
        high_confidence = (
            score >= 8
            and side_match
            and qty_match
            and ts_gap_sec <= 300
            and (local_price is None or remote_price is None or strong_price_match)
        )
        likely_fill_match = float(getattr(remote, "qty_filled", 0.0) or 0.0) > 1e-12 or remote_status in {"PARTIAL", "FILLED"}

        ranked.append(
            (
                score,
                {
                    "exchange_order_id": exchange_order_id,
                    "side": remote_side or "-",
                    "qty": remote_qty_req,
                    "filled_qty": float(getattr(remote, "qty_filled", 0.0) or 0.0),
                    "status": remote_status,
                    "price": remote_price,
                    "created_ts": remote_created_ts,
                    "updated_ts": remote_updated_ts,
                    "score": score,
                    "qty_gap_pct": qty_gap_pct,
                    "time_gap_sec": ts_gap_sec,
                    "price_gap_pct": price_gap_pct,
                    "match_reason": " + ".join(reasons),
                    "high_confidence": 1 if high_confidence else 0,
                    "likely_fill_match": 1 if likely_fill_match else 0,
                },
            )
        )

    ranked.sort(key=lambda item: (-item[0], -int(item[1]["updated_ts"]), -int(item[1]["created_ts"])))
    return [item[1] for item in ranked]




def _load_submission_evidence(conn, *, client_order_id: str, submit_attempt_id: str | None) -> dict[str, str | int | bool | None]:
    event = None
    if submit_attempt_id:
        event = conn.execute(
            """
            SELECT submit_ts, event_ts, order_status, submission_reason_code,
                   timeout_flag, exchange_order_id_obtained, exception_class
            FROM order_events
            WHERE client_order_id=?
              AND submit_attempt_id=?
              AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (client_order_id, submit_attempt_id),
        ).fetchone()

    if event is None:
        event = conn.execute(
            """
            SELECT submit_ts, event_ts, order_status, submission_reason_code,
                   timeout_flag, exchange_order_id_obtained, exception_class
            FROM order_events
            WHERE client_order_id=?
              AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (client_order_id,),
        ).fetchone()

    intent = conn.execute(
        """
        SELECT intent_ts, event_ts
        FROM order_events
        WHERE client_order_id=?
          AND event_type='intent_created'
        ORDER BY id DESC
        LIMIT 1
        """,
        (client_order_id,),
    ).fetchone()

    attempted_locally = bool(event is not None or intent is not None)
    attempted_ts = None
    if event is not None:
        attempted_ts = int(event["submit_ts"] if event["submit_ts"] is not None else event["event_ts"])
    elif intent is not None:
        attempted_ts = int(intent["intent_ts"] if intent["intent_ts"] is not None else intent["event_ts"])

    request_likely_sent = "unknown"
    if event is not None:
        timeout_flag = bool(event["timeout_flag"]) if event["timeout_flag"] is not None else False
        exchange_id_obtained = bool(event["exchange_order_id_obtained"]) if event["exchange_order_id_obtained"] is not None else False
        if timeout_flag or exchange_id_obtained:
            request_likely_sent = "yes"
        elif str(event["order_status"] or "").strip() in {"NEW", "PARTIAL", "FILLED", "CANCELED", "FAILED", "SUBMIT_UNKNOWN"}:
            request_likely_sent = "yes"

    attempted_desc = "local intent evidence unavailable"
    if event is not None:
        attempted_desc = (
            f"submit_attempt_recorded status={event['order_status'] or '-'} "
            f"reason_code={event['submission_reason_code'] or '-'} "
            f"timeout={1 if bool(event['timeout_flag']) else 0} "
            f"exchange_order_id_obtained={1 if bool(event['exchange_order_id_obtained']) else 0}"
        )
    elif intent is not None:
        attempted_desc = "intent_created recorded but no submit_attempt_recorded evidence"

    return {
        "submit_attempt_id": submit_attempt_id,
        "attempted_locally": attempted_locally,
        "attempted_ts": attempted_ts,
        "attempted_desc": attempted_desc,
        "request_likely_sent": request_likely_sent,
    }
def _load_recovery_report(
    *,
    oldest_limit: int = 5,
) -> dict[str, int | float | str | bool | None | list[dict[str, str | float | bool]]]:
    maybe_clear_stale_initial_reconcile_halt()
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
            SELECT client_order_id, submit_attempt_id, status, exchange_order_id, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
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
        submission_evidence_by_order_id: dict[str, dict[str, str | int | bool | None]] = {}
        for row in oldest_rows:
            submit_attempt_id = str(row["submit_attempt_id"] or "").strip()
            submission_evidence_by_order_id[str(row["client_order_id"])] = _load_submission_evidence(
                conn,
                client_order_id=str(row["client_order_id"]),
                submit_attempt_id=(submit_attempt_id or None),
            )
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
    candidate_local_orders: list[dict[str, str | float]] = []
    for row in oldest_rows:
        last_error = str(row["last_error"] or "").strip()
        evidence = submission_evidence_by_order_id.get(str(row["client_order_id"]), {})
        local_order = {
            "client_order_id": str(row["client_order_id"]),
            "status": str(row["status"]),
            "exchange_order_id": str(row["exchange_order_id"] or "-"),
            "side": str(row["side"] or "-"),
            "price": (float(row["price"]) if row["price"] is not None else None),
            "qty_req": float(row["qty_req"] or 0.0),
            "qty_filled": float(row["qty_filled"] or 0.0),
            "created_ts": int(row["created_ts"]),
            "updated_ts": int(row["updated_ts"]),
            "age_sec": max(0.0, (now_ms - float(row["created_ts"])) / 1000),
            "last_error": (last_error[:60] + "...") if len(last_error) > 60 else (last_error or "-"),
            "submit_evidence_attempted_locally": bool(evidence["attempted_locally"]),
            "submit_evidence_attempted_ts": int(evidence["attempted_ts"] or row["created_ts"]),
            "submit_evidence_attempted_desc": str(evidence["attempted_desc"]),
            "submit_evidence_request_likely_sent": str(evidence["request_likely_sent"]),
        }
        oldest_orders.append(
            {
                "client_order_id": str(local_order["client_order_id"]),
                "status": str(local_order["status"]),
                "exchange_order_id": str(local_order["exchange_order_id"]),
                "age_sec": float(local_order["age_sec"]),
                "last_error": str(local_order["last_error"]),
            }
        )
        candidate_local_orders.append(local_order)

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
    can_resume = bool(resume_allowed)
    blocker_codes = [str(b["code"]) for b in blocker_list]
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

    def _next_action_for_blocker(code: str) -> str:
        if code == "STARTUP_SAFETY_GATE_BLOCKED":
            if recovery_required_count > 0:
                return "uv run python bot.py recover-order --client-order-id <id>"
            if submit_unknown_count > 0:
                return "uv run python bot.py reconcile"
            return "uv run python bot.py recovery-report"
        if code == "LAST_RECONCILE_FAILED":
            return "uv run python bot.py reconcile"
        if code == "HALT_RISK_OPEN_POSITION":
            return "uv run python bot.py flatten-position"
        if code in {"HALT_STATE_UNRESOLVED", "EMERGENCY_FLATTEN_UNRESOLVED"}:
            return "uv run python bot.py restart-checklist"
        return recommended_command

    blocker_summary_view: list[dict[str, str]] = []
    for blocker in blocker_list[:3]:
        code = str(blocker["code"])
        evidence = str(blocker["detail"])
        if code == "STARTUP_SAFETY_GATE_BLOCKED":
            evidence = (
                f"unresolved={unresolved_count} "
                f"submit_unknown={submit_unknown_count} "
                f"recovery_required={recovery_required_count}; "
                f"{evidence}"
            )
        blocker_summary_view.append(
            {
                "blocker": code,
                "evidence": evidence,
                "recommended_next_action": _next_action_for_blocker(code),
            }
        )

    if not blocker_summary_view:
        blocker_summary_view.append(
            {
                "blocker": "none",
                "evidence": "resume gates clear",
                "recommended_next_action": "uv run python bot.py resume",
            }
        )

    recent_orders_snapshot, broker_snapshot_error = _safe_recent_broker_orders_snapshot(limit=100)
    candidate_report: list[dict[str, object]] = []
    for local_order in candidate_local_orders:
        candidates = _build_recovery_candidates(local_order=local_order, recent_orders=recent_orders_snapshot)
        plausible_candidates = [c for c in candidates if int(c.get("high_confidence") or 0) == 1]
        likely_candidate = plausible_candidates[0] if len(plausible_candidates) == 1 else None
        if not candidates:
            outcome = "no_candidate"
            next_action = "No likely broker match found. Keep order unresolved, run reconcile, and verify exchange history manually before recover-order."
        elif len(plausible_candidates) > 1:
            outcome = "multiple_plausible_candidates"
            next_action = "Multiple high-confidence broker matches detected. Keep unresolved and perform manual broker/order-event review before recover-order."
        elif len(plausible_candidates) == 1:
            outcome = "single_plausible_candidate"
            next_action = "Exactly one high-confidence broker match found. Operator should validate and run recover-order with the suggested exchange_order_id."
        else:
            outcome = "weak_candidates_only"
            next_action = "Only weak broker candidates found. Keep unresolved and verify order/fill history before manual recover-order."

        candidate_report.append(
            {
                "client_order_id": local_order["client_order_id"],
                "local_status": local_order["status"],
                "local_side": local_order["side"],
                "local_qty": local_order["qty_req"],
                "local_created_ts": local_order["created_ts"],
                "attempted_locally": bool(local_order["submit_evidence_attempted_locally"]),
                "attempted_ts": int(local_order["submit_evidence_attempted_ts"]),
                "attempted_summary": str(local_order["submit_evidence_attempted_desc"]),
                "request_likely_sent": str(local_order["submit_evidence_request_likely_sent"]),
                "candidate_outcome": outcome,
                "plausible_candidate_count": len(plausible_candidates),
                "likely_broker_match": bool(likely_candidate is not None),
                "likely_broker_exchange_order_id": (
                    str(likely_candidate["exchange_order_id"]) if likely_candidate is not None else None
                ),
                "likely_broker_match_kind": (
                    "order_or_fill" if likely_candidate is not None and int(likely_candidate.get("likely_fill_match") or 0) == 1
                    else ("order" if likely_candidate is not None else "none")
                ),
                "candidates": candidates[:5],
                "next_action_hint": next_action,
            }
        )

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
        "emergency_flatten_blocked": bool(state.emergency_flatten_blocked),
        "emergency_flatten_block_reason": state.emergency_flatten_block_reason,
        "resume_allowed": bool(resume_allowed),
        "can_resume": can_resume,
        "resume_blockers": blocker_codes,
        "force_resume_allowed": all(bool(b.overridable) for b in blockers),
        "blockers": blocker_list,
        "blocker_summary": blocker_summary,
        "active_blocker_summary": active_blocker_summary,
        "blocker_summary_view": blocker_summary_view,
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
        "recovery_candidates": candidate_report,
        "broker_recent_orders_snapshot_error": broker_snapshot_error,
    }


def cmd_recovery_report(*, as_json: bool = False) -> None:
    report = _load_recovery_report()
    write_json_atomic(PATH_MANAGER.recovery_report_path(), report)
    if as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return

    print("[RECOVERY-REPORT]")
    print("  [P0] blocker_summary_view")
    for item in report.get("blocker_summary_view") or []:
        print(
            "    - "
            f"blocker={item['blocker']} "
            f"evidence={item['evidence']} "
            f"recommended_next_action={item['recommended_next_action']}"
        )
    print("  [P1] order_recovery_status")
    print(f"    unresolved_count={report['unresolved_count']}")
    print(f"    recovery_required_count={report['recovery_required_count']}")
    print(f"    submit_unknown_count={report['submit_unknown_count']}")
    print("  [P2] resume_eligibility")
    print(f"    resume_allowed={1 if bool(report['resume_allowed']) else 0}")
    print(f"    can_resume={'true' if bool(report['can_resume']) else 'false'}")
    resume_blockers = report.get("resume_blockers") or []
    print(f"    blockers={', '.join(str(b) for b in resume_blockers) if resume_blockers else 'none'}")
    print(f"    force_resume_allowed={1 if bool(report['force_resume_allowed']) else 0}")
    blockers = report.get("blockers") or []
    print(f"    blocker_summary={report['blocker_summary']}")
    print(f"    active_blocker_summary={report['active_blocker_summary']}")
    print(f"    risk_level={report['risk_level']}")
    print(f"    primary_blocker_code={report['primary_blocker_code']}")
    print(f"    emergency_flatten_blocked={1 if bool(report.get('emergency_flatten_blocked')) else 0}")
    print(f"    emergency_flatten_block_reason={report.get('emergency_flatten_block_reason') or 'none'}")
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

    print("  [P9] recovery_candidates")
    broker_snapshot_error = str(report.get("broker_recent_orders_snapshot_error") or "")
    if broker_snapshot_error:
        print(f"    broker_snapshot={broker_snapshot_error}")
    recovery_candidates = report.get("recovery_candidates") or []
    for item in recovery_candidates:
        print(
            "    - "
            f"local_order_id={item['client_order_id']} "
            f"local_status={item['local_status']} "
            f"candidate_outcome={item['candidate_outcome']}"
        )
        print(
            "      "
            f"attempted_locally={1 if bool(item.get('attempted_locally')) else 0} "
            f"attempted_ts={item.get('attempted_ts')} "
            f"request_likely_sent={item.get('request_likely_sent')}"
        )
        print(f"      attempted_summary={item.get('attempted_summary')}")
        print(
            "      "
            f"likely_broker_match={1 if bool(item.get('likely_broker_match')) else 0} "
            f"likely_broker_exchange_order_id={item.get('likely_broker_exchange_order_id') or '-'} "
            f"likely_broker_match_kind={item.get('likely_broker_match_kind') or 'none'}"
        )
        candidates = item.get("candidates") or []
        if not candidates:
            print("      candidate_exchange_orders=none")
        else:
            for candidate in candidates:
                print(
                    "      * "
                    f"exchange_order_id={candidate['exchange_order_id']} "
                    f"side={candidate['side']} "
                    f"qty={float(candidate['qty']):.8f} "
                    f"filled_qty={float(candidate['filled_qty']):.8f} "
                    f"status={candidate['status']} "
                    f"created_ts={candidate['created_ts']} "
                    f"updated_ts={candidate['updated_ts']} "
                    f"score={int(candidate['score'])} "
                    f"qty_gap_pct={float(candidate['qty_gap_pct']):.3f} "
                    f"time_gap_sec={float(candidate['time_gap_sec']):.1f} "
                    f"price_gap_pct={("-" if candidate['price_gap_pct'] is None else f'{float(candidate["price_gap_pct"]):.3f}')} "
                    f"reason={candidate['match_reason']}"
                )
        print(f"      next_action={item['next_action_hint']}")


def _load_restart_safety_checklist() -> list[tuple[str, bool, str]]:
    maybe_clear_stale_initial_reconcile_halt()
    report = _load_recovery_report()
    state = runtime_state.snapshot()

    conn = ensure_db()
    try:
        open_row = conn.execute(
            """
            SELECT COUNT(*) AS open_count
            FROM orders
            WHERE status IN ({})
              AND status != 'RECOVERY_REQUIRED'
            """.format(",".join("?" for _ in OPEN_ORDER_STATUSES)),
            OPEN_ORDER_STATUSES,
        ).fetchone()
        portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
    finally:
        conn.close()

    unresolved_count = int(report.get("unresolved_count") or 0)
    recovery_required_count = int(report.get("recovery_required_count") or 0)
    open_order_count = int(open_row["open_count"] if open_row else 0)
    asset_qty = float(portfolio_row["asset_qty"] if portfolio_row else 0.0)

    last_reconcile_summary = str(report.get("last_reconcile_summary") or "none")
    last_reconcile_ok = (
        last_reconcile_summary == "none" or "status=ok" in last_reconcile_summary.lower()
    )

    halt_reason = str(report.get("recent_halt_reason") or "none")
    halt_clear = (
        not state.halt_new_orders_blocked
        and not state.halt_state_unresolved
        and halt_reason == "none"
    )

    return [
        (
            "unresolved/recovery-required orders",
            unresolved_count == 0 and recovery_required_count == 0,
            (
                f"unresolved={unresolved_count} "
                f"recovery_required={recovery_required_count}"
            ),
        ),
        (
            "open orders",
            open_order_count == 0,
            f"open_orders={open_order_count}",
        ),
        (
            "open position",
            asset_qty <= 1e-12,
            f"asset_qty={asset_qty:.12f}",
        ),
        (
            "halt state",
            halt_clear,
            (
                f"halt_blocked={1 if state.halt_new_orders_blocked else 0} "
                f"halt_unresolved={1 if state.halt_state_unresolved else 0} "
                f"detail={halt_reason}"
            ),
        ),
        (
            "last reconcile",
            last_reconcile_ok,
            last_reconcile_summary,
        ),
    ]


def cmd_restart_checklist() -> None:
    checklist = _load_restart_safety_checklist()
    blocked = [item for item in checklist if not item[1]]

    print("[RESTART-SAFETY-CHECKLIST]")
    for label, ok, detail in checklist:
        status = "PASS" if ok else "BLOCKED"
        print(f"  - {status:<7} {label}: {detail}")
    print(f"  safe_to_resume={1 if not blocked else 0}")

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


def _build_live_broker():
    return bithumb_broker_module.BithumbBroker()


def _running_under_pytest() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ


class _OfflineTestReconcileBroker:
    """Safe broker used by operator-command tests unless they opt into live deps."""

    def get_order(
        self,
        *,
        client_order_id: str,
        exchange_order_id: str | None = None,
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            side="BUY",
            status="NEW",
            price=None,
            qty_req=0.0,
            qty_filled=0.0,
            created_ts=0,
            updated_ts=0,
        )

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list:
        return []

    def get_open_orders(self) -> list:
        return []

    def get_recent_orders(self, *, limit: int = 100) -> list:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=0.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )


def _default_live_reconcile_dependencies(*, broker_factory=None, reconcile_fn=None):
    resolved_reconcile_fn = reconcile_fn or reconcile_with_broker
    if broker_factory is not None:
        return broker_factory, resolved_reconcile_fn

    current_broker_class = bithumb_broker_module.BithumbBroker
    if _running_under_pytest() and current_broker_class is DEFAULT_BITHUMB_BROKER_CLASS:
        return _OfflineTestReconcileBroker, resolved_reconcile_fn

    return _build_live_broker, resolved_reconcile_fn


def _run_live_reconcile(*, broker_factory=None, reconcile_fn=None) -> None:
    resolved_broker_factory, resolved_reconcile_fn = _default_live_reconcile_dependencies(
        broker_factory=broker_factory,
        reconcile_fn=reconcile_fn,
    )
    resolved_reconcile_fn(resolved_broker_factory())


def cmd_resume(
    force: bool = False,
    *,
    broker_factory=None,
    reconcile_fn=None,
) -> None:
    if settings.MODE == "live":
        _run_live_reconcile(
            broker_factory=broker_factory,
            reconcile_fn=reconcile_fn,
        )

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


def cmd_reconcile(*, broker_factory=None, reconcile_fn=None) -> None:
    if settings.MODE != "live":
        print(f"[RECONCILE] skipped: MODE={settings.MODE} (live only)")
        return

    _run_live_reconcile(
        broker_factory=broker_factory,
        reconcile_fn=reconcile_fn,
    )
    print("[RECONCILE] completed one live reconciliation pass")


def cmd_flatten_position(*, dry_run: bool = False) -> None:
    if settings.MODE != "live":
        print(f"[FLATTEN-POSITION] skipped: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    if not dry_run:
        try:
            validate_live_mode_preflight(settings)
        except LiveModeValidationError as e:
            print(f"[FLATTEN-POSITION] failed: {e}")
            raise SystemExit(1)

    from .broker.bithumb import BithumbBroker, classify_private_api_error

    broker = BithumbBroker()
    summary = flatten_btc_position(broker=broker, dry_run=dry_run, trigger="operator")
    status = str(summary.get("status") or "")
    qty = float(summary.get("qty") or 0.0)

    if status == "no_position":
        print("[FLATTEN-POSITION] no position to flatten (BTC qty=0)")
        return

    print(f"[FLATTEN-POSITION] target=BTC side=SELL qty={qty:.8f} dry_run={1 if dry_run else 0}")
    if status == "dry_run":
        print("[FLATTEN-POSITION] dry-run: submit skipped")
        return

    if status == "failed":
        err = str(summary.get("error") or "unknown flatten failure")
        print(f"[FLATTEN-POSITION] failed: {err}")
        raise SystemExit(1)

    print(
        "[FLATTEN-POSITION] submitted "
        f"client_order_id={str(summary.get('client_order_id') or '-')} "
        f"exchange_order_id={str(summary.get('exchange_order_id') or '-')} "
        f"status={str(summary.get('order_status') or '-')}"
    )


def _build_recover_order_preview(*, client_order_id: str, exchange_order_id: str) -> dict[str, object]:
    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT client_order_id, status, exchange_order_id, qty_filled, last_error
            FROM orders
            WHERE client_order_id=?
            """,
            (client_order_id,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return {
            "exists": False,
            "safe_to_apply": False,
            "target_client_order_id": client_order_id,
            "target_exchange_order_id": exchange_order_id,
            "current_status": "UNKNOWN",
            "current_exchange_order_id": "-",
            "proposed_action": "manual_recover_with_exchange_id",
            "state_changes": ["none (client_order_id not found)"],
        }

    current_status = str(row["status"] or "UNKNOWN")
    return {
        "exists": True,
        "safe_to_apply": current_status == "RECOVERY_REQUIRED",
        "target_client_order_id": client_order_id,
        "target_exchange_order_id": exchange_order_id,
        "current_status": current_status,
        "current_exchange_order_id": str(row["exchange_order_id"] or "-"),
        "proposed_action": "manual_recover_with_exchange_id",
        "state_changes": [
            f"exchange_order_id -> {exchange_order_id}",
            "fetch remote order + fills and apply missing fills",
            "final order status updated from broker snapshot",
            "trading remains disabled until explicit resume",
        ],
        "last_error": str(row["last_error"] or "-"),
        "qty_filled": float(row["qty_filled"] or 0.0),
    }


def cmd_recover_order(*, client_order_id: str, exchange_order_id: str, dry_run: bool = False, confirm: bool = False) -> None:
    if settings.MODE != "live":
        print(f"[RECOVER-ORDER] skipped: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    preview = _build_recover_order_preview(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )
    print("[RECOVER-ORDER] preview")
    print(
        "  target_order_id="
        f"{preview['target_client_order_id']} exchange_order_id={preview['target_exchange_order_id']}"
    )
    print(
        "  current_known_state="
        f"status={preview['current_status']} "
        f"exchange_order_id={preview['current_exchange_order_id']}"
    )
    print(f"  proposed_recovery_action={preview['proposed_action']}")
    print("  important_state_changes:")
    for change in preview.get("state_changes", []):
        print(f"    - {change}")

    if dry_run:
        print("[RECOVER-ORDER] dry-run: no changes applied")
        return

    if not bool(preview.get("safe_to_apply")):
        print("[RECOVER-ORDER] refused: unsafe recovery request")
        print("  reason=client_order_id must exist and be RECOVERY_REQUIRED")
        raise SystemExit(1)

    if not confirm:
        print("[RECOVER-ORDER] confirmation required: re-run with --yes to apply")
        raise SystemExit(1)

    from .broker.bithumb import BithumbBroker, classify_private_api_error

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
    sub.add_parser(
        "health",
        help="show health summary (staleness/errors/trading state/recovery)",
        description="Show health summary for limited unattended operation checks.",
    )
    sub.add_parser("audit-ledger")
    sub.add_parser(
        "cancel-open-orders",
        help="cancel all remote open orders in live mode",
        description="Cancel all remote open orders (live mode only).",
    )
    sub.add_parser(
        "broker-diagnose",
        help="read-only live broker/API diagnostics",
        description="Run read-only live broker diagnostics (no order create/cancel).",
    )
    sub.add_parser(
        "pause",
        help="persistently pause new trading",
        description="Persistently disable trading until explicit resume.",
    )
    flatten = sub.add_parser(
        "flatten-position",
        help="emergency flatten open position",
        description="Flatten current position for emergency exposure reduction.",
    )
    flatten.add_argument("--dry-run", action="store_true")

    resume = sub.add_parser(
        "resume",
        help="resume trading if safety checks pass",
        description="Resume trading with safety gates; use --force only as a last resort.",
    )
    resume.add_argument("--force", action="store_true")

    sub.add_parser(
        "reconcile",
        help="reconcile local/exchange order state",
        description="Run order-state reconciliation with the broker.",
    )
    recovery_report = sub.add_parser(
        "recovery-report",
        help="show unresolved/recovery-required order report",
        description="Show unresolved/recovery-required orders and resume blockers.",
    )
    recovery_report.add_argument("--json", action="store_true")
    sub.add_parser(
        "restart-checklist",
        help="print restart safety checklist before resume",
        description="Print restart safety checklist for operator restart verification.",
    )
    recover_order = sub.add_parser("recover-order")
    recover_order.add_argument("--client-order-id", required=True)
    recover_order.add_argument("--exchange-order-id", required=True)
    recover_order.add_argument("--dry-run", action="store_true")
    recover_order.add_argument("--yes", action="store_true")

    report = sub.add_parser("report")
    report.add_argument("--days", type=int, default=30)

    t = sub.add_parser("trades")
    t.add_argument("--limit", type=int, default=20)

    ops = sub.add_parser("ops-report", help="operator observability report")
    ops.add_argument("--limit", type=int, default=20)

    fee_diag = sub.add_parser(
        "fee-diagnostics",
        help="validate real fee application against recent fills/roundtrips",
    )
    fee_diag.add_argument("--fill-limit", type=int, default=100)
    fee_diag.add_argument("--roundtrip-limit", type=int, default=50)
    fee_diag.add_argument(
        "--estimated-fee-rate",
        type=float,
        default=None,
        help="expected fee rate (default: FEE_RATE setting)",
    )
    fee_diag.add_argument("--json", action="store_true")

    strategy_report = sub.add_parser(
        "strategy-report",
        help="strategy performance comparison report",
        description="Aggregate trade_lifecycles by strategy/exit-rule/date range for experiments.",
    )
    strategy_report.add_argument("--strategy-name")
    strategy_report.add_argument("--exit-rule-name")
    strategy_report.add_argument("--pair")
    strategy_report.add_argument("--from-date", help="KST date (YYYY-MM-DD)")
    strategy_report.add_argument("--to-date", help="KST date (YYYY-MM-DD)")
    strategy_report.add_argument(
        "--group-by",
        default="strategy_name,exit_rule_name",
        help="comma-separated axes: strategy_name,exit_rule_name,pair",
    )
    strategy_report.add_argument("--json", action="store_true")

    r = sub.add_parser("run")
    r.add_argument("--short", type=int, default=SMA_SHORT)
    r.add_argument("--long", type=int, default=SMA_LONG)

    args = p.parse_args(argv)

    try:
        validate_mode_or_raise(settings.MODE)
    except ModeValidationError as e:
        print(f"[MODE] {e}")
        raise SystemExit(1) from e

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
    elif args.cmd == "ops-report":
        cmd_ops_report(limit=max(1, int(args.limit)))
    elif args.cmd == "fee-diagnostics":
        cmd_fee_diagnostics(
            fill_limit=max(1, int(args.fill_limit)),
            roundtrip_limit=max(1, int(args.roundtrip_limit)),
            estimated_fee_rate=args.estimated_fee_rate,
            as_json=bool(args.json),
        )
    elif args.cmd == "strategy-report":
        try:
            from_ts_ms, to_ts_ms = parse_kst_date_range_to_ts_ms(
                from_date=args.from_date,
                to_date=args.to_date,
            )
        except ValueError:
            p.error("invalid date format for --from-date/--to-date; expected YYYY-MM-DD")

        if from_ts_ms is not None and to_ts_ms is not None and from_ts_ms > to_ts_ms:
            p.error("--from-date must be earlier than or equal to --to-date")

        group_by = tuple(part.strip() for part in str(args.group_by or "").split(",") if part.strip())
        cmd_strategy_report(
            strategy_name=args.strategy_name,
            exit_rule_name=args.exit_rule_name,
            pair=args.pair,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            group_by=group_by,
            as_json=bool(args.json),
        )
    elif args.cmd == "report":
        cmd_report(max(1, int(args.days)))
    elif args.cmd == "audit-ledger":
        cmd_audit_ledger()
    elif args.cmd == "cancel-open-orders":
        cmd_cancel_open_orders()
    elif args.cmd == "broker-diagnose":
        cmd_broker_diagnose()
    elif args.cmd == "pause":
        cmd_pause()
    elif args.cmd == "resume":
        cmd_resume(force=bool(args.force))
    elif args.cmd == "flatten-position":
        cmd_flatten_position(dry_run=bool(args.dry_run))
    elif args.cmd == "reconcile":
        cmd_reconcile()
    elif args.cmd == "recovery-report":
        cmd_recovery_report(as_json=bool(args.json))
    elif args.cmd == "restart-checklist":
        cmd_restart_checklist()
    elif args.cmd == "recover-order":
        cmd_recover_order(
            client_order_id=str(args.client_order_id),
            exchange_order_id=str(args.exchange_order_id),
            dry_run=bool(args.dry_run),
            confirm=bool(args.yes),
        )
    elif args.cmd == "run":
        cmd_run(args.short, args.long)
    else:
        p.print_help()
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
