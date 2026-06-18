from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from bithumb_bot.core.sma_policy import _stable_hash
from bithumb_bot.strategy.daily_participation_events import (
    ParticipationEvent,
    SOURCE_CONTRACT_VERSION,
    participation_event_set_hash,
    source_contract_hash,
)
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
    strategy_instance_id: str = "",
    strategy_name: str = "daily_participation_sma",
) -> DailyParticipationCountSnapshot:
    day = kst_day(decision_ts, config.timezone)
    start_ms, end_ms = _day_bounds_ms(day, config.timezone)
    source = "sqlite_runtime_data_provider"
    source_version = SOURCE_CONTRACT_VERSION
    try:
        events = _runtime_events(
            conn=conn,
            config=config,
            pair=pair,
            strategy_instance_id=strategy_instance_id,
            strategy_name=strategy_name,
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
            source=source,
            rows=(),
            fail_closed_reason=f"daily_participation_runtime_count_source_unavailable:{type(exc).__name__}",
            pair=pair,
            strategy_instance_id=strategy_instance_id,
            source_contract_version=source_version,
        )
    rows = tuple(_event_row(event) for event in events)
    return DailyParticipationCountSnapshot(
        count_basis=config.count_basis,
        timezone=config.timezone,
        kst_day=day,
        count_for_kst_day=len(events),
        timestamp_field=TIMESTAMP_FIELD_BY_BASIS[config.count_basis],
        source=source,
        rows=rows,
        pair=pair,
        strategy_instance_id=strategy_instance_id,
        event_set_hash=participation_event_set_hash(events),
        source_contract_hash=source_contract_hash(source=source, source_contract_version=source_version),
        query_contract_hash=_stable_hash(
            {
                "schema_version": 1,
                "query_contract": "daily_participation_sqlite_count.v1",
                "count_basis": config.count_basis,
                "pair": pair,
                "strategy_instance_id": strategy_instance_id,
                "strategy_name": strategy_name,
                "kst_day": day,
            }
        ),
        source_contract_version=source_version,
    )


def _runtime_events(
    *,
    conn: sqlite3.Connection,
    config: DailyParticipationPolicyConfig,
    pair: str,
    strategy_instance_id: str,
    strategy_name: str,
    start_ms: int,
    end_ms: int,
) -> tuple[ParticipationEvent, ...]:
    scope_instance = str(strategy_instance_id or "").strip()
    scope_pair = str(pair or "").strip()
    scope_strategy = str(strategy_name or "").strip().lower()
    if not scope_instance:
        raise sqlite3.OperationalError("daily_participation_strategy_instance_scope_missing")
    if not scope_pair:
        raise sqlite3.OperationalError("daily_participation_pair_scope_missing")
    if config.count_basis in {"intent", "submit_expected", "submitted"}:
        columns = _table_columns(conn, "orders")
        required = {"client_order_id", "side", "created_ts", "status", "pair", "strategy_name", "strategy_instance_id"}
        if not required.issubset(columns):
            raise sqlite3.OperationalError("orders_daily_count_columns_missing")
        where = (
            "UPPER(side)='BUY' AND created_ts>=? AND created_ts<? "
            "AND pair=? AND LOWER(strategy_name)=? AND strategy_instance_id=?"
        )
        params: list[object] = [start_ms, end_ms, scope_pair, scope_strategy, scope_instance]
        if config.count_basis in {"submit_expected", "submitted"}:
            where += " AND status NOT IN ('rejected','failed','canceled','cancelled')"
        rows = conn.execute(
            f"SELECT client_order_id, created_ts AS ts, status, side, pair, strategy_name, strategy_instance_id "
            f"FROM orders WHERE {where}",
            tuple(params),
        ).fetchall()
        return tuple(
            ParticipationEvent(
                event_id=f"order:{row['client_order_id']}:{config.count_basis}",
                strategy_instance_id=str(row["strategy_instance_id"] or ""),
                strategy_name=str(row["strategy_name"] or ""),
                pair=str(row["pair"] or ""),
                side=str(row["side"] or ""),
                lifecycle_stage=config.count_basis,
                event_ts=int(row["ts"]),
                count_basis=config.count_basis,
                client_order_id=str(row["client_order_id"] or ""),
                source="sqlite_runtime_data_provider",
            )
            for row in rows
        )
    if config.count_basis == "filled":
        fill_columns = _table_columns(conn, "fills")
        order_columns = _table_columns(conn, "orders")
        required_fills = {"client_order_id", "fill_id", "fill_ts", "qty"}
        required_orders = {"client_order_id", "side", "pair", "strategy_name", "strategy_instance_id"}
        if not required_fills.issubset(fill_columns):
            raise sqlite3.OperationalError("fills_daily_count_columns_missing")
        if not required_orders.issubset(order_columns):
            raise sqlite3.OperationalError("orders_daily_count_scope_columns_missing")
        rows = conn.execute(
            """
            SELECT
                f.client_order_id,
                COALESCE(f.fill_id, '') AS fill_id,
                f.fill_ts AS ts,
                f.qty AS qty,
                o.side AS side,
                o.pair AS pair,
                o.strategy_name AS strategy_name,
                o.strategy_instance_id AS strategy_instance_id
            FROM fills f
            JOIN orders o ON o.client_order_id = f.client_order_id
            WHERE f.fill_ts>=?
              AND f.fill_ts<?
              AND f.qty>0
              AND UPPER(o.side)='BUY'
              AND o.pair=?
              AND LOWER(o.strategy_name)=?
              AND o.strategy_instance_id=?
              AND f.client_order_id IS NOT NULL
              AND TRIM(f.client_order_id) <> ''
            """,
            (start_ms, end_ms, scope_pair, scope_strategy, scope_instance),
        ).fetchall()
        return tuple(
            ParticipationEvent(
                event_id=f"fill:{row['client_order_id']}:{row['fill_id'] or row['ts']}",
                strategy_instance_id=str(row["strategy_instance_id"] or ""),
                strategy_name=str(row["strategy_name"] or ""),
                pair=str(row["pair"] or ""),
                side=str(row["side"] or ""),
                lifecycle_stage="filled",
                event_ts=int(row["ts"]),
                count_basis="filled",
                client_order_id=str(row["client_order_id"] or ""),
                fill_id=str(row["fill_id"] or ""),
                source="sqlite_runtime_data_provider",
            )
            for row in rows
        )
    columns = _table_columns(conn, "trades")
    required = {"ts", "side", "pair", "strategy_name", "strategy_instance_id"}
    if not required.issubset(columns):
        raise sqlite3.OperationalError("trades_daily_count_columns_missing")
    rows = conn.execute(
        """
        SELECT id, ts, side, pair, strategy_name, strategy_instance_id
        FROM trades
        WHERE UPPER(side)='SELL'
          AND ts>=?
          AND ts<?
          AND pair=?
          AND LOWER(strategy_name)=?
          AND strategy_instance_id=?
        """,
        (start_ms, end_ms, scope_pair, scope_strategy, scope_instance),
    ).fetchall()
    return tuple(
        ParticipationEvent(
            event_id=f"trade:{row['id']}",
            strategy_instance_id=str(row["strategy_instance_id"] or ""),
            strategy_name=str(row["strategy_name"] or ""),
            pair=str(row["pair"] or ""),
            side=str(row["side"] or ""),
            lifecycle_stage="closed_trade",
            event_ts=int(row["ts"]),
            count_basis="closed_trade",
            order_id=str(row["id"] or ""),
            source="sqlite_runtime_data_provider",
        )
        for row in rows
    )


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1] if not hasattr(row, "keys") else row["name"]) for row in rows}


def _row_dict(row: Any) -> dict[str, object]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return {"ts": row[0]}


def _event_row(event: ParticipationEvent) -> dict[str, object]:
    payload = event.as_dict()
    payload["basis"] = event.count_basis
    payload["ts"] = int(event.event_ts)
    return payload


def _day_bounds_ms(day: str, timezone_name: str) -> tuple[int, int]:
    tz = ZoneInfo("Asia/Seoul" if timezone_name == "KST" else timezone_name)
    start = datetime.fromisoformat(day).replace(tzinfo=tz)
    end = datetime.fromordinal(start.date().toordinal() + 1).replace(tzinfo=tz)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)
