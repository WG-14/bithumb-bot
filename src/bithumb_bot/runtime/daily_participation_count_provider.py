from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from bithumb_bot.strategy.daily_participation_policy import (
    DailyParticipationCountSnapshot,
    DailyParticipationPolicyConfig,
    TIMESTAMP_FIELD_BY_BASIS,
    kst_day,
)


def build_runtime_daily_count_snapshot_from_sqlite(
    *,
    conn: sqlite3.Connection,
    config: DailyParticipationPolicyConfig,
    decision_ts: int,
    pair: str,
) -> DailyParticipationCountSnapshot:
    day = kst_day(decision_ts, config.timezone)
    start_ms, end_ms = _day_bounds_ms(day, config.timezone)
    try:
        rows = _runtime_rows(
            conn=conn,
            config=config,
            pair=pair,
            start_ms=start_ms,
            end_ms=min(end_ms, int(decision_ts)),
        )
    except sqlite3.Error as exc:
        return DailyParticipationCountSnapshot(
            count_basis=config.count_basis,
            timezone=config.timezone,
            kst_day=day,
            count_for_kst_day=0,
            timestamp_field=TIMESTAMP_FIELD_BY_BASIS[config.count_basis],
            source="sqlite_runtime_data_provider",
            rows=(),
            fail_closed_reason=f"daily_participation_runtime_count_source_unavailable:{type(exc).__name__}",
        )
    return DailyParticipationCountSnapshot(
        count_basis=config.count_basis,
        timezone=config.timezone,
        kst_day=day,
        count_for_kst_day=len(rows),
        timestamp_field=TIMESTAMP_FIELD_BY_BASIS[config.count_basis],
        source="sqlite_runtime_data_provider",
        rows=tuple(rows),
    )


def _runtime_rows(
    *,
    conn: sqlite3.Connection,
    config: DailyParticipationPolicyConfig,
    pair: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, object]]:
    if config.count_basis in {"intent", "submit_expected", "submitted"}:
        columns = _table_columns(conn, "orders")
        required = {"side", "created_ts", "status"}
        if not required.issubset(columns):
            raise sqlite3.OperationalError("orders_daily_count_columns_missing")
        where = "side='BUY' AND created_ts>=? AND created_ts<?"
        params: list[object] = [start_ms, end_ms]
        if "pair" in columns and pair:
            where += " AND pair=?"
            params.append(pair)
        if config.count_basis in {"submit_expected", "submitted"}:
            where += " AND status NOT IN ('rejected','failed','canceled','cancelled')"
        rows = conn.execute(f"SELECT created_ts AS ts, status FROM orders WHERE {where}", tuple(params)).fetchall()
        return [_row_dict(row) for row in rows]
    if config.count_basis == "filled":
        columns = _table_columns(conn, "fills")
        if not {"fill_ts", "qty"}.issubset(columns):
            raise sqlite3.OperationalError("fills_daily_count_columns_missing")
        rows = conn.execute(
            "SELECT fill_ts AS ts, qty FROM fills WHERE fill_ts>=? AND fill_ts<? AND qty>0",
            (start_ms, end_ms),
        ).fetchall()
        return [_row_dict(row) for row in rows]
    columns = _table_columns(conn, "trades")
    if not {"ts", "side"}.issubset(columns):
        raise sqlite3.OperationalError("trades_daily_count_columns_missing")
    where = "side='SELL' AND ts>=? AND ts<?"
    params = [start_ms, end_ms]
    if "pair" in columns and pair:
        where += " AND pair=?"
        params.append(pair)
    rows = conn.execute(f"SELECT ts, side FROM trades WHERE {where}", tuple(params)).fetchall()
    return [_row_dict(row) for row in rows]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1] if not hasattr(row, "keys") else row["name"]) for row in rows}


def _row_dict(row: Any) -> dict[str, object]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return {"ts": row[0]}


def _day_bounds_ms(day: str, timezone_name: str) -> tuple[int, int]:
    tz = ZoneInfo("Asia/Seoul" if timezone_name == "KST" else timezone_name)
    start = datetime.fromisoformat(day).replace(tzinfo=tz)
    end = datetime.fromordinal(start.date().toordinal() + 1).replace(tzinfo=tz)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)
