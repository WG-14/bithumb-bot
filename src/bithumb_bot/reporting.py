from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .config import settings
from .db_core import ensure_db
from .utils_time import kst_str


@dataclass
class StrategyStat:
    strategy_context: str
    order_count: int
    fill_count: int
    buy_notional: float
    sell_notional: float
    fee_total: float

    @property
    def pnl_proxy(self) -> float:
        return self.sell_notional - self.buy_notional - self.fee_total


def _fetch_strategy_stats(conn: sqlite3.Connection) -> list[StrategyStat]:
    rows = conn.execute(
        """
        SELECT
            oid.strategy_context AS strategy_context,
            COUNT(DISTINCT o.client_order_id) AS order_count,
            COUNT(f.id) AS fill_count,
            COALESCE(SUM(CASE WHEN o.side='BUY' THEN (f.price * f.qty) ELSE 0 END), 0) AS buy_notional,
            COALESCE(SUM(CASE WHEN o.side='SELL' THEN (f.price * f.qty) ELSE 0 END), 0) AS sell_notional,
            COALESCE(SUM(f.fee), 0) AS fee_total
        FROM order_intent_dedup oid
        LEFT JOIN orders o ON o.client_order_id = oid.client_order_id
        LEFT JOIN fills f ON f.client_order_id = o.client_order_id
        GROUP BY oid.strategy_context
        ORDER BY order_count DESC, fill_count DESC, oid.strategy_context ASC
        """
    ).fetchall()
    return [
        StrategyStat(
            strategy_context=str(r["strategy_context"]),
            order_count=int(r["order_count"] or 0),
            fill_count=int(r["fill_count"] or 0),
            buy_notional=float(r["buy_notional"] or 0.0),
            sell_notional=float(r["sell_notional"] or 0.0),
            fee_total=float(r["fee_total"] or 0.0),
        )
        for r in rows
    ]


def _fetch_recent_flow(conn: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            oe.event_ts,
            oe.client_order_id,
            oe.event_type,
            oe.order_status,
            oe.side,
            oe.price,
            oe.qty,
            oe.submission_reason_code,
            oe.message,
            oid.strategy_context
        FROM order_events oe
        LEFT JOIN order_intent_dedup oid ON oid.client_order_id = oe.client_order_id
        ORDER BY oe.event_ts DESC, oe.id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()


def _fetch_recent_trade_ops(conn: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT ts, side, price, qty, fee, cash_after, asset_after, note
        FROM trades
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()


def _fmt_float(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def cmd_ops_report(*, limit: int = 20) -> None:
    conn = ensure_db()
    try:
        strategy_stats = _fetch_strategy_stats(conn)
        recent_flow = _fetch_recent_flow(conn, limit=max(1, int(limit)))
        recent_trades = _fetch_recent_trade_ops(conn, limit=max(1, int(limit)))
    finally:
        conn.close()

    print("[OPS-REPORT]")
    print(f"  mode={settings.MODE} pair={settings.PAIR} interval={settings.INTERVAL} db_path={settings.DB_PATH}")

    print("\n[STRATEGY-SUMMARY]")
    if not strategy_stats:
        print("  no strategy_context rows in order_intent_dedup")
        print("  tip: strategy_context 기반 집계는 주문 intent dedup 데이터가 있어야 계산됩니다.")
    else:
        print("  strategy_context,order_count,fill_count,buy_notional,sell_notional,fee_total,pnl_proxy")
        for stat in strategy_stats:
            print(
                "  "
                f"{stat.strategy_context},{stat.order_count},{stat.fill_count},"
                f"{stat.buy_notional:.2f},{stat.sell_notional:.2f},{stat.fee_total:.2f},{stat.pnl_proxy:.2f}"
            )

    print("\n[RECENT-STRATEGY-ORDER-FILL-FLOW]")
    if not recent_flow:
        print("  no order_events rows")
    else:
        for row in reversed(recent_flow):
            ts = kst_str(int(row["event_ts"]))
            strategy_context = str(row["strategy_context"] or "<unknown>")
            message = str(row["message"] or "")
            if len(message) > 80:
                message = f"{message[:77]}..."
            print(
                "  "
                f"{ts} strategy={strategy_context} cid={row['client_order_id']} "
                f"event={row['event_type']} status={row['order_status'] or '-'} side={row['side'] or '-'} "
                f"qty={_fmt_float(float(row['qty'] or 0.0), 8)} price={_fmt_float(float(row['price'] or 0.0), 0)} "
                f"reason={row['submission_reason_code'] or '-'} note={message or '-'}"
            )

    print("\n[RECENT-TRADES-OPERATIONS]")
    if not recent_trades:
        print("  no trades rows")
    else:
        fee_total = 0.0
        for row in reversed(recent_trades):
            fee = float(row["fee"] or 0.0)
            fee_total += fee
            print(
                "  "
                f"{kst_str(int(row['ts']))} {row['side']:4s} "
                f"price={_fmt_float(float(row['price']), 0)} qty={_fmt_float(float(row['qty']), 8)} "
                f"fee={_fmt_float(fee, 2)} cash_after={_fmt_float(float(row['cash_after']), 2)} "
                f"asset_after={_fmt_float(float(row['asset_after']), 8)} note={row['note'] or '-'}"
            )
        print(f"  fee_total(last {len(recent_trades)} trades)={_fmt_float(fee_total, 2)}")

    print("\n[KNOWN-LIMITATIONS/TODO]")
    print("  - trades 테이블에 strategy_context/client_order_id가 없어 전략별 확정 손익(realized PnL)은 직접 합산할 수 없습니다.")
    print("  - 현재는 fills+orders 기반 notional/fee로 pnl_proxy(sell-buy-fee)를 제공합니다.")
    print("  - TODO: trades에 strategy_context 또는 client_order_id를 저장하면 전략별 realized/unrealized PnL 정확도를 높일 수 있습니다.")
