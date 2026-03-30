from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from statistics import median

from .config import PATH_MANAGER, settings
from .db_core import ensure_db
from .storage_io import write_json_atomic
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


@dataclass
class StrategyPerformanceStat:
    strategy_name: str
    exit_rule_name: str
    pair: str
    trade_count: int
    win_rate: float
    avg_gain: float
    avg_loss: float
    net_pnl: float
    expectancy_per_trade: float
    fee_total: float
    holding_time_avg_sec: float | None
    holding_time_min_sec: float | None
    holding_time_max_sec: float | None


@dataclass
class FeeDiagnosticSummary:
    fill_count: int
    fills_with_notional: int
    fee_zero_count: int
    fee_zero_ratio: float
    average_fee_rate: float | None
    average_fee_bps: float | None
    median_fee_bps: float | None
    estimated_fee_rate: float
    estimated_minus_actual_bps: float | None
    total_fee_recent_fills: float
    total_notional_recent_fills: float
    roundtrip_count: int
    roundtrip_fee_total: float
    pnl_before_fee_total: float
    pnl_after_fee_total: float
    pnl_fee_drag_total: float
    notes: list[str]


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


def _fetch_recent_fills_with_side(conn: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            f.fill_ts,
            f.client_order_id,
            o.side,
            f.price,
            f.qty,
            f.fee
        FROM fills f
        LEFT JOIN orders o ON o.client_order_id = f.client_order_id
        ORDER BY f.id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()


def _fetch_recent_trade_lifecycles(conn: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, pair, strategy_name, gross_pnl, fee_total, net_pnl, entry_ts, exit_ts
        FROM trade_lifecycles
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()


def summarize_fee_diagnostics(
    recent_fills: list[sqlite3.Row],
    *,
    estimated_fee_rate: float,
    recent_lifecycles: list[sqlite3.Row],
) -> FeeDiagnosticSummary:
    fill_count = len(recent_fills)
    fee_zero_count = 0
    total_fee = 0.0
    total_notional = 0.0
    per_fill_fee_bps: list[float] = []

    for row in recent_fills:
        fee = float(row["fee"] or 0.0)
        if abs(fee) <= 1e-12:
            fee_zero_count += 1
        price = float(row["price"] or 0.0)
        qty = float(row["qty"] or 0.0)
        notional = max(0.0, price * qty)
        total_fee += fee
        if notional > 0:
            total_notional += notional
            per_fill_fee_bps.append((fee / notional) * 10000.0)

    average_fee_rate = (total_fee / total_notional) if total_notional > 0 else None
    average_fee_bps = (sum(per_fill_fee_bps) / len(per_fill_fee_bps)) if per_fill_fee_bps else None
    median_fee_bps = median(per_fill_fee_bps) if per_fill_fee_bps else None
    fee_zero_ratio = (fee_zero_count / fill_count) if fill_count > 0 else 0.0
    estimated_minus_actual_bps = (
        (estimated_fee_rate - average_fee_rate) * 10000.0 if average_fee_rate is not None else None
    )

    roundtrip_count = len(recent_lifecycles)
    pnl_before_fee_total = sum(float(row["gross_pnl"] or 0.0) for row in recent_lifecycles)
    roundtrip_fee_total = sum(float(row["fee_total"] or 0.0) for row in recent_lifecycles)
    pnl_after_fee_total = sum(float(row["net_pnl"] or 0.0) for row in recent_lifecycles)
    pnl_fee_drag_total = pnl_before_fee_total - pnl_after_fee_total

    notes: list[str] = []
    if fill_count == 0:
        notes.append("no fills found in the selected window")
    if fill_count > 0 and total_notional <= 0:
        notes.append("fills exist but all notional values were non-positive")
    if roundtrip_count == 0:
        notes.append("no trade_lifecycles rows found for roundtrip fee/pnl diagnostics")

    return FeeDiagnosticSummary(
        fill_count=fill_count,
        fills_with_notional=len(per_fill_fee_bps),
        fee_zero_count=fee_zero_count,
        fee_zero_ratio=fee_zero_ratio,
        average_fee_rate=average_fee_rate,
        average_fee_bps=average_fee_bps,
        median_fee_bps=median_fee_bps,
        estimated_fee_rate=float(estimated_fee_rate),
        estimated_minus_actual_bps=estimated_minus_actual_bps,
        total_fee_recent_fills=total_fee,
        total_notional_recent_fills=total_notional,
        roundtrip_count=roundtrip_count,
        roundtrip_fee_total=roundtrip_fee_total,
        pnl_before_fee_total=pnl_before_fee_total,
        pnl_after_fee_total=pnl_after_fee_total,
        pnl_fee_drag_total=pnl_fee_drag_total,
        notes=notes,
    )


def fetch_fee_diagnostics(
    conn: sqlite3.Connection,
    *,
    fill_limit: int,
    roundtrip_limit: int,
    estimated_fee_rate: float,
) -> FeeDiagnosticSummary:
    recent_fills = _fetch_recent_fills_with_side(conn, limit=max(1, int(fill_limit)))
    recent_lifecycles = _fetch_recent_trade_lifecycles(conn, limit=max(1, int(roundtrip_limit)))
    return summarize_fee_diagnostics(
        recent_fills,
        estimated_fee_rate=float(estimated_fee_rate),
        recent_lifecycles=recent_lifecycles,
    )


def _fmt_rate(value: float | None, *, as_bps: bool = False) -> str:
    if value is None:
        return "-"
    if as_bps:
        return f"{value:.3f} bps"
    return f"{value:.6f}"


def cmd_fee_diagnostics(
    *,
    fill_limit: int = 100,
    roundtrip_limit: int = 50,
    estimated_fee_rate: float | None = None,
    as_json: bool = False,
) -> None:
    estimate = (
        settings.LIVE_FEE_RATE_ESTIMATE
        if estimated_fee_rate is None and settings.MODE == "live"
        else settings.PAPER_FEE_RATE
        if estimated_fee_rate is None
        else float(estimated_fee_rate)
    )
    conn = ensure_db()
    try:
        summary = fetch_fee_diagnostics(
            conn,
            fill_limit=fill_limit,
            roundtrip_limit=roundtrip_limit,
            estimated_fee_rate=estimate,
        )
    finally:
        conn.close()

    payload = {
        "db_path": settings.DB_PATH,
        "mode": settings.MODE,
        "pair": settings.PAIR,
        "fill_window": {"limit": max(1, int(fill_limit)), "count": summary.fill_count},
        "roundtrip_window": {"limit": max(1, int(roundtrip_limit)), "count": summary.roundtrip_count},
        "fills": {
            "average_fee_rate": summary.average_fee_rate,
            "average_fee_bps": summary.average_fee_bps,
            "median_fee_bps": summary.median_fee_bps,
            "fee_zero_count": summary.fee_zero_count,
            "fee_zero_ratio": summary.fee_zero_ratio,
            "fills_with_notional": summary.fills_with_notional,
            "total_fee": summary.total_fee_recent_fills,
            "total_notional": summary.total_notional_recent_fills,
        },
        "fee_model_validation": {
            "estimated_fee_rate": summary.estimated_fee_rate,
            "estimated_minus_actual_bps": summary.estimated_minus_actual_bps,
        },
        "roundtrip": {
            "total_fee": summary.roundtrip_fee_total,
            "pnl_before_fee": summary.pnl_before_fee_total,
            "pnl_after_fee": summary.pnl_after_fee_total,
            "pnl_fee_drag": summary.pnl_fee_drag_total,
        },
        "notes": summary.notes,
    }

    write_json_atomic(PATH_MANAGER.fee_diagnostics_report_path(), payload)

    if as_json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return

    print("[FEE-DIAGNOSTICS]")
    print(
        "  "
        f"mode={settings.MODE} pair={settings.PAIR} db_path={settings.DB_PATH} "
        f"fills(last={max(1, int(fill_limit))}) roundtrips(last={max(1, int(roundtrip_limit))})"
    )
    print("\n[FILL-FEE-SUMMARY]")
    print(
        "  "
        f"avg_fee_rate={_fmt_rate(summary.average_fee_rate)} "
        f"avg_fee_bps={_fmt_rate(summary.average_fee_bps, as_bps=True)} "
        f"median_fee_bps={_fmt_rate(summary.median_fee_bps, as_bps=True)}"
    )
    print(
        "  "
        f"fee_zero={summary.fee_zero_count}/{summary.fill_count} ({summary.fee_zero_ratio:.2%}) "
        f"fills_with_notional={summary.fills_with_notional} "
        f"total_fee={_fmt_float(summary.total_fee_recent_fills, 2)} "
        f"total_notional={_fmt_float(summary.total_notional_recent_fills, 2)}"
    )
    print("\n[FEE-MODEL-VALIDATION]")
    print(
        "  "
        f"estimated_fee_rate={summary.estimated_fee_rate:.6f} "
        f"estimated_minus_actual_bps={_fmt_rate(summary.estimated_minus_actual_bps, as_bps=True)}"
    )
    print("\n[ROUNDTRIP-FEE-AND-PNL]")
    print(
        "  "
        f"roundtrip_count={summary.roundtrip_count} "
        f"fee_total={_fmt_float(summary.roundtrip_fee_total, 2)} "
        f"pnl_before_fee={_fmt_float(summary.pnl_before_fee_total, 2)} "
        f"pnl_after_fee={_fmt_float(summary.pnl_after_fee_total, 2)} "
        f"pnl_fee_drag={_fmt_float(summary.pnl_fee_drag_total, 2)}"
    )
    if summary.notes:
        print("\n[NOTES]")
        for note in summary.notes:
            print(f"  - {note}")

def _fmt_float(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def parse_kst_date_range_to_ts_ms(*, from_date: str | None, to_date: str | None) -> tuple[int | None, int | None]:
    if from_date is None and to_date is None:
        return None, None

    kst = timezone(timedelta(hours=9))
    start_ts: int | None = None
    end_ts: int | None = None

    if from_date:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=kst)
        start_ts = int(from_dt.timestamp() * 1000)

    if to_date:
        to_dt = datetime.strptime(to_date, "%Y-%m-%d").replace(tzinfo=kst)
        to_dt = datetime.combine(to_dt.date(), time.max, tzinfo=kst)
        end_ts = int(to_dt.timestamp() * 1000)

    return start_ts, end_ts


def _normalize_group_by(group_by: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    allowed = {"strategy_name", "exit_rule_name", "pair"}
    normalized = []
    for item in group_by or ("strategy_name", "exit_rule_name"):
        key = str(item).strip().lower()
        if key in allowed and key not in normalized:
            normalized.append(key)
    if not normalized:
        normalized = ["strategy_name", "exit_rule_name"]
    return tuple(normalized)


def fetch_strategy_performance_stats(
    conn: sqlite3.Connection,
    *,
    strategy_name: str | None = None,
    exit_rule_name: str | None = None,
    pair: str | None = None,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    group_by: tuple[str, ...] | list[str] | None = None,
) -> list[StrategyPerformanceStat]:
    group_axes = _normalize_group_by(group_by)

    lifecycle_base = """
        SELECT
            tl.id,
            COALESCE(tl.strategy_name, '<unknown>') AS strategy_name,
            COALESCE(tl.pair, '<unknown>') AS pair,
            tl.exit_ts,
            tl.net_pnl,
            tl.fee_total,
            tl.holding_time_sec,
            COALESCE(
                json_extract(
                    (
                        SELECT sd.context_json
                        FROM strategy_decisions sd
                        WHERE sd.signal='SELL'
                          AND sd.decision_ts <= tl.exit_ts
                          AND (tl.strategy_name IS NULL OR sd.strategy_name = tl.strategy_name)
                        ORDER BY sd.decision_ts DESC, sd.id DESC
                        LIMIT 1
                    ),
                    '$.exit.rule'
                ),
                '<unknown>'
            ) AS exit_rule_name
        FROM trade_lifecycles tl
    """

    filters: list[str] = []
    params: list[object] = []

    if strategy_name:
        filters.append("COALESCE(tl.strategy_name, '<unknown>') = ?")
        params.append(str(strategy_name))
    if pair:
        filters.append("COALESCE(tl.pair, '<unknown>') = ?")
        params.append(str(pair))
    if from_ts_ms is not None:
        filters.append("tl.exit_ts >= ?")
        params.append(int(from_ts_ms))
    if to_ts_ms is not None:
        filters.append("tl.exit_ts <= ?")
        params.append(int(to_ts_ms))

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    cte = f"WITH lifecycle_base AS ({lifecycle_base} {where_clause})"

    post_filters: list[str] = []
    if exit_rule_name:
        post_filters.append("exit_rule_name = ?")
        params.append(str(exit_rule_name))

    post_where = f"WHERE {' AND '.join(post_filters)}" if post_filters else ""

    dims_expr = {
        "strategy_name": "strategy_name",
        "exit_rule_name": "exit_rule_name",
        "pair": "pair",
    }

    select_dims = [f"{dims_expr[axis]} AS {axis}" for axis in group_axes]
    group_dims = [dims_expr[axis] for axis in group_axes]

    for axis in ("strategy_name", "exit_rule_name", "pair"):
        if axis not in group_axes:
            fallback = "'<all>'"
            if axis == "pair":
                fallback = "'<all>'"
            select_dims.append(f"{fallback} AS {axis}")

    select_dim_sql = ",\n            ".join(select_dims)
    group_by_sql = ", ".join(group_dims)

    query = f"""
        {cte}
        SELECT
            {select_dim_sql},
            COUNT(*) AS trade_count,
            COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0) AS win_count,
            COALESCE(AVG(CASE WHEN net_pnl > 0 THEN net_pnl ELSE NULL END), 0.0) AS avg_gain,
            COALESCE(AVG(CASE WHEN net_pnl < 0 THEN net_pnl ELSE NULL END), 0.0) AS avg_loss,
            COALESCE(SUM(net_pnl), 0.0) AS net_pnl,
            COALESCE(SUM(fee_total), 0.0) AS fee_total,
            AVG(holding_time_sec) AS holding_time_avg_sec,
            MIN(holding_time_sec) AS holding_time_min_sec,
            MAX(holding_time_sec) AS holding_time_max_sec
        FROM lifecycle_base
        {post_where}
        GROUP BY {group_by_sql}
        ORDER BY trade_count DESC, strategy_name ASC, exit_rule_name ASC, pair ASC
    """

    rows = conn.execute(query, tuple(params)).fetchall()

    stats: list[StrategyPerformanceStat] = []
    for row in rows:
        trade_count = int(row["trade_count"] or 0)
        win_count = int(row["win_count"] or 0)
        avg_gain = float(row["avg_gain"] or 0.0)
        avg_loss = float(row["avg_loss"] or 0.0)
        win_rate = (win_count / trade_count) if trade_count > 0 else 0.0
        loss_rate = 1.0 - win_rate if trade_count > 0 else 0.0
        expectancy = (win_rate * avg_gain) + (loss_rate * avg_loss)

        stats.append(
            StrategyPerformanceStat(
                strategy_name=str(row["strategy_name"]),
                exit_rule_name=str(row["exit_rule_name"]),
                pair=str(row["pair"]),
                trade_count=trade_count,
                win_rate=win_rate,
                avg_gain=avg_gain,
                avg_loss=avg_loss,
                net_pnl=float(row["net_pnl"] or 0.0),
                expectancy_per_trade=expectancy,
                fee_total=float(row["fee_total"] or 0.0),
                holding_time_avg_sec=(
                    None if row["holding_time_avg_sec"] is None else float(row["holding_time_avg_sec"])
                ),
                holding_time_min_sec=(
                    None if row["holding_time_min_sec"] is None else float(row["holding_time_min_sec"])
                ),
                holding_time_max_sec=(
                    None if row["holding_time_max_sec"] is None else float(row["holding_time_max_sec"])
                ),
            )
        )
    return stats


def cmd_strategy_report(
    *,
    strategy_name: str | None,
    exit_rule_name: str | None,
    pair: str | None,
    from_ts_ms: int | None,
    to_ts_ms: int | None,
    group_by: tuple[str, ...] | list[str] | None,
    as_json: bool = False,
) -> None:
    conn = ensure_db()
    try:
        stats = fetch_strategy_performance_stats(
            conn,
            strategy_name=strategy_name,
            exit_rule_name=exit_rule_name,
            pair=pair,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            group_by=group_by,
        )
    finally:
        conn.close()

    normalized_group_by = _normalize_group_by(group_by)

    payload = {
        "group_by": list(normalized_group_by),
        "filters": {
            "strategy_name": strategy_name,
            "exit_rule_name": exit_rule_name,
            "pair": pair,
            "from_ts_ms": from_ts_ms,
            "to_ts_ms": to_ts_ms,
        },
        "rows": [
            {
                "strategy_name": stat.strategy_name,
                "exit_rule_name": stat.exit_rule_name,
                "pair": stat.pair,
                "trade_count": stat.trade_count,
                "win_rate": stat.win_rate,
                "average_gain": stat.avg_gain,
                "average_loss": stat.avg_loss,
                "net_pnl": stat.net_pnl,
                "expectancy_per_trade": stat.expectancy_per_trade,
                "fee_total": stat.fee_total,
                "holding_time": {
                    "avg_sec": stat.holding_time_avg_sec,
                    "min_sec": stat.holding_time_min_sec,
                    "max_sec": stat.holding_time_max_sec,
                },
            }
            for stat in stats
        ],
        "notes": [] if stats else ["no trade_lifecycles rows matched the given filters"],
    }
    write_json_atomic(PATH_MANAGER.strategy_validation_report_path(), payload)

    if as_json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return

    print("[STRATEGY-PERFORMANCE-REPORT]")
    print(
        "  "
        f"group_by={','.join(normalized_group_by)} "
        f"strategy_name={strategy_name or '<all>'} "
        f"exit_rule_name={exit_rule_name or '<all>'} "
        f"pair={pair or '<all>'} "
        f"from_ts_ms={from_ts_ms if from_ts_ms is not None else '<none>'} "
        f"to_ts_ms={to_ts_ms if to_ts_ms is not None else '<none>'}"
    )

    if not stats:
        print("  no matched trade_lifecycles rows")
        print("  tip: 실행 구간/전략명/청산 규칙 필터를 완화하거나 lifecycle 데이터 생성 여부를 확인하세요.")
        return

    print(
        "  "
        "strategy_name,exit_rule_name,pair,trade_count,win_rate,average_gain,average_loss,"
        "net_pnl,expectancy_per_trade,fee_total,holding_avg_sec,holding_min_sec,holding_max_sec"
    )
    for stat in stats:
        holding_avg = "-" if stat.holding_time_avg_sec is None else f"{stat.holding_time_avg_sec:.2f}"
        holding_min = "-" if stat.holding_time_min_sec is None else f"{stat.holding_time_min_sec:.2f}"
        holding_max = "-" if stat.holding_time_max_sec is None else f"{stat.holding_time_max_sec:.2f}"
        print(
            "  "
            f"{stat.strategy_name},{stat.exit_rule_name},{stat.pair},{stat.trade_count},"
            f"{stat.win_rate:.4f},{stat.avg_gain:.2f},{stat.avg_loss:.2f},{stat.net_pnl:.2f},"
            f"{stat.expectancy_per_trade:.2f},{stat.fee_total:.2f},{holding_avg},{holding_min},{holding_max}"
        )


def cmd_ops_report(*, limit: int = 20) -> None:
    conn = ensure_db()
    try:
        strategy_stats = _fetch_strategy_stats(conn)
        recent_flow = _fetch_recent_flow(conn, limit=max(1, int(limit)))
        recent_trades = _fetch_recent_trade_ops(conn, limit=max(1, int(limit)))
        fee_summary = fetch_fee_diagnostics(
            conn,
            fill_limit=max(1, int(limit)),
            roundtrip_limit=max(1, int(limit)),
            estimated_fee_rate=float(settings.FEE_RATE),
        )
    finally:
        conn.close()

    payload = {
        "mode": settings.MODE,
        "pair": settings.PAIR,
        "interval": settings.INTERVAL,
        "db_path": settings.DB_PATH,
        "strategy_summary": [
            {
                "strategy_context": stat.strategy_context,
                "order_count": stat.order_count,
                "fill_count": stat.fill_count,
                "buy_notional": stat.buy_notional,
                "sell_notional": stat.sell_notional,
                "fee_total": stat.fee_total,
                "pnl_proxy": stat.pnl_proxy,
            }
            for stat in strategy_stats
        ],
        "recent_flow": [dict(row) for row in recent_flow],
        "recent_trades": [dict(row) for row in recent_trades],
        "fee_diagnostics_snapshot": {
            "fill_count": fee_summary.fill_count,
            "fee_zero_count": fee_summary.fee_zero_count,
            "fee_zero_ratio": fee_summary.fee_zero_ratio,
            "average_fee_bps": fee_summary.average_fee_bps,
            "median_fee_bps": fee_summary.median_fee_bps,
            "estimated_minus_actual_bps": fee_summary.estimated_minus_actual_bps,
            "roundtrip_count": fee_summary.roundtrip_count,
            "roundtrip_fee_total": fee_summary.roundtrip_fee_total,
            "pnl_before_fee_total": fee_summary.pnl_before_fee_total,
            "pnl_after_fee_total": fee_summary.pnl_after_fee_total,
        },
    }
    write_json_atomic(PATH_MANAGER.ops_report_path(), payload)

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
    print("\n[FEE-DIAGNOSTICS-SNAPSHOT]")
    print(
        "  "
        f"fills={fee_summary.fill_count} fee_zero={fee_summary.fee_zero_count} ({fee_summary.fee_zero_ratio:.2%}) "
        f"avg_fee_bps={_fmt_rate(fee_summary.average_fee_bps, as_bps=True)} "
        f"median_fee_bps={_fmt_rate(fee_summary.median_fee_bps, as_bps=True)} "
        f"est_minus_actual_bps={_fmt_rate(fee_summary.estimated_minus_actual_bps, as_bps=True)}"
    )
    print(
        "  "
        f"roundtrip_count={fee_summary.roundtrip_count} "
        f"roundtrip_fee_total={_fmt_float(fee_summary.roundtrip_fee_total, 2)} "
        f"pnl_before_fee={_fmt_float(fee_summary.pnl_before_fee_total, 2)} "
        f"pnl_after_fee={_fmt_float(fee_summary.pnl_after_fee_total, 2)}"
    )
