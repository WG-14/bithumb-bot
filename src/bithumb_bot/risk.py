# src/bithumb_bot/risk.py
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta

from .config import settings
from .oms import evaluate_unresolved_order_gate

KST = timezone(timedelta(hours=9))
POSITION_EPSILON = 1e-12


def _day_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=KST)
    return dt.strftime("%Y-%m-%d")


def _ensure_daily_risk_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_risk (
            day_kst TEXT PRIMARY KEY,
            start_equity REAL NOT NULL
        )
        """
    )
    conn.commit()


def _get_or_set_start_equity(conn: sqlite3.Connection, day_kst: str, equity: float) -> float:
    row = conn.execute("SELECT start_equity FROM daily_risk WHERE day_kst=?", (day_kst,)).fetchone()
    if row:
        return float(row[0])
    conn.execute("INSERT INTO daily_risk(day_kst, start_equity) VALUES (?, ?)", (day_kst, float(equity)))
    conn.commit()
    return float(equity)


def _count_orders_today(conn: sqlite3.Connection, ts_ms: int) -> int:
    day = _day_kst(ts_ms)
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM orders
        WHERE strftime('%Y-%m-%d', created_ts/1000, 'unixepoch', '+9 hours')=?
        """,
        (day,),
    ).fetchone()
    return int(row["cnt"] if hasattr(row, "keys") else row[0])


def evaluate_buy_guardrails(
    conn: sqlite3.Connection,
    ts_ms: int,
    cash: float,
    qty: float,
    price: float,
) -> tuple[bool, str]:
    """
    Returns (blocked, reason)
    - Kill switch
    - Max open position (single-position model)
    - Daily loss limit (optional)
    - Daily order count limit (optional)
    """
    if settings.KILL_SWITCH:
        return True, "KILL_SWITCH=ON"

    if settings.MAX_OPEN_POSITIONS <= 1 and qty > POSITION_EPSILON:
        return True, "duplicate entry blocked"

    if settings.MAX_DAILY_ORDER_COUNT > 0:
        today_orders = _count_orders_today(conn, ts_ms)
        if today_orders >= settings.MAX_DAILY_ORDER_COUNT:
            return True, f"daily order count limit exceeded ({today_orders}/{settings.MAX_DAILY_ORDER_COUNT})"

    blocked, reason = _daily_loss_exceeded(conn, ts_ms, cash, qty, price)
    if blocked:
        return True, reason

    return False, "ok"


def _daily_loss_exceeded(conn: sqlite3.Connection, ts_ms: int, cash: float, qty: float, price: float) -> tuple[bool, str]:
    if settings.MAX_DAILY_LOSS_KRW <= 0:
        return False, "ok"

    _ensure_daily_risk_table(conn)
    day = _day_kst(ts_ms)
    equity = float(cash) + float(qty) * float(price)
    start_equity = _get_or_set_start_equity(conn, day, equity)
    loss_today = max(0.0, start_equity - equity)
    if loss_today >= settings.MAX_DAILY_LOSS_KRW:
        return True, f"daily loss limit exceeded ({loss_today:,.0f}/{settings.MAX_DAILY_LOSS_KRW:,.0f} KRW)"

    return False, "ok"


def evaluate_daily_loss_breach(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    cash: float,
    qty: float,
    price: float,
) -> tuple[bool, str]:
    """Returns whether current portfolio equity already breached the daily loss limit."""
    return _daily_loss_exceeded(conn, ts_ms, cash, qty, price)


def evaluate_order_submission_halt(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    now_ms: int,
    cash: float,
    qty: float,
    price: float,
) -> tuple[bool, str]:
    """Shared hard-stop checks before placing any new order."""
    if settings.KILL_SWITCH:
        return True, "KILL_SWITCH=ON"

    blocked, reason = evaluate_daily_loss_breach(
        conn,
        ts_ms=ts_ms,
        cash=cash,
        qty=qty,
        price=price,
    )
    if blocked:
        return True, reason

    blocked, _, reason = evaluate_unresolved_order_gate(
        conn,
        now_ms=now_ms,
        max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
    )
    if blocked:
        return True, reason

    return False, "ok"
