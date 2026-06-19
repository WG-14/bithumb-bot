from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .research.hashing import sha256_prefixed


KST = timezone(timedelta(hours=9))


H74_OBSERVATION_REPORT_FIELDS = (
    "observation_start",
    "observation_end",
    "eligible_kst_days",
    "daily_buy_intent_count",
    "daily_buy_submitted_count",
    "daily_buy_filled_count",
    "duplicate_entry_block_count",
    "claim_pending_count",
    "claim_fulfilled_count",
    "claim_terminal_failed_count",
    "max_holding_exit_due_count",
    "max_holding_exit_filled_count",
    "exit_delay_seconds_p50",
    "exit_delay_seconds_max",
    "fee_total_krw",
    "observed_fee_bps",
    "slippage_bps_avg",
    "broker_local_mismatch_count",
    "manual_intervention_count",
)


def build_h74_observation_report(
    *,
    conn: sqlite3.Connection | None = None,
    days: int = 7,
    now: datetime | None = None,
    observation_start: datetime | None = None,
    observation_end: datetime | None = None,
    authority_hash: str | None = None,
    strategy_instance_id: str | None = None,
    participation_policy_hash: str | None = None,
    pair: str = "KRW-BTC",
    interval: str = "1m",
) -> dict[str, Any]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    end = (observation_end or current).astimezone(timezone.utc)
    start = (observation_start or (end - timedelta(days=int(days)))).astimezone(timezone.utc)
    required_kst_days = _required_kst_days(start, end)
    payload: dict[str, Any] = {
        "artifact_type": "h74_live_observation_report",
        "strategy_name": "daily_participation_sma",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
        "eligible_kst_days": int(days),
        "complete": False,
        "authority_hash": authority_hash,
        "strategy_instance_id": strategy_instance_id,
        "participation_policy_hash": participation_policy_hash,
        "pair": pair,
        "interval": interval,
        "source_backtest_pnl": None,
        "live_observed_pnl": None,
    }
    metrics = {field: 0 for field in H74_OBSERVATION_REPORT_FIELDS if field not in payload}
    metrics["exit_delay_seconds_p50"] = 0.0
    metrics["exit_delay_seconds_max"] = 0.0
    metrics["fee_total_krw"] = 0.0
    metrics["observed_fee_bps"] = 0.0
    metrics["slippage_bps_avg"] = 0.0
    if conn is not None:
        metrics.update(
            _sqlite_metrics(
                conn,
                start_ts=int(start.timestamp() * 1000),
                end_ts=int(end.timestamp() * 1000),
                authority_hash=authority_hash,
                strategy_instance_id=strategy_instance_id,
                pair=pair,
                participation_policy_hash=participation_policy_hash,
                required_kst_days=required_kst_days,
            )
        )
    payload.update(metrics)
    elapsed_seconds = (end - start).total_seconds()
    covered_days = set(payload.get("covered_kst_days") or [])
    duplicate_days = set(payload.get("duplicate_buy_kst_days") or [])
    authority_scope_applied = bool(authority_hash or strategy_instance_id)
    payload["required_kst_days"] = required_kst_days
    payload["required_kst_day_count"] = len(required_kst_days)
    payload["authority_scope_applied"] = authority_scope_applied
    payload["complete"] = bool(
        start <= current
        and end <= current
        and elapsed_seconds >= 7 * 86400
        and len(required_kst_days) == 7
        and covered_days == set(required_kst_days)
        and not duplicate_days
        and authority_scope_applied
    )
    return payload


def _required_kst_days(start: datetime, end: datetime) -> list[str]:
    start_kst = start.astimezone(KST)
    end_kst = end.astimezone(KST)
    first = start_kst.date()
    last_exclusive = end_kst.date()
    if end_kst.time() != time(0, 0):
        last_exclusive = last_exclusive + timedelta(days=1)
    days: list[str] = []
    cursor = first
    while cursor < last_exclusive:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    return column in {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _time_filter(conn: sqlite3.Connection, table: str, column: str, start_ts: int, end_ts: int) -> tuple[str, list[Any]]:
    if _column_exists(conn, table, column):
        return f" AND {column} >= ? AND {column} < ?", [start_ts, end_ts]
    return "", []


def _order_scope_filter(
    conn: sqlite3.Connection,
    *,
    start_ts: int,
    end_ts: int,
    authority_hash: str | None,
    strategy_instance_id: str | None,
    pair: str,
) -> tuple[str, list[Any]]:
    clauses = ["strategy_name='daily_participation_sma'"]
    params: list[Any] = []
    if _column_exists(conn, "orders", "created_ts"):
        clauses.append("created_ts >= ? AND created_ts < ?")
        params.extend([start_ts, end_ts])
    if _column_exists(conn, "orders", "pair"):
        clauses.append("pair=?")
        params.append(pair)
    if strategy_instance_id and _column_exists(conn, "orders", "strategy_instance_id"):
        clauses.append("strategy_instance_id=?")
        params.append(strategy_instance_id)
    if authority_hash and _column_exists(conn, "orders", "authority_hash"):
        clauses.append("authority_hash=?")
        params.append(authority_hash)
    elif authority_hash and _column_exists(conn, "orders", "submit_truth_source_fields"):
        clauses.append("COALESCE(submit_truth_source_fields,'') LIKE ?")
        params.append(f"%{authority_hash}%")
    return " AND ".join(clauses), params


def _claim_scope_filter(
    conn: sqlite3.Connection,
    *,
    start_ts: int,
    end_ts: int,
    required_kst_days: list[str],
    authority_hash: str | None,
    strategy_instance_id: str | None,
    pair: str,
    participation_policy_hash: str | None,
) -> tuple[str, list[Any]]:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(daily_participation_claims)").fetchall()}
    required = {"strategy_instance_id", "pair", "kst_day", "participation_policy_hash", "status"}
    if not required.issubset(columns):
        return "0", []
    clauses = ["pair=?", "kst_day IN (" + ",".join("?" for _ in required_kst_days) + ")"]
    params: list[Any] = [pair, *required_kst_days]
    if strategy_instance_id:
        clauses.append("strategy_instance_id=?")
        params.append(strategy_instance_id)
    if participation_policy_hash:
        clauses.append("participation_policy_hash=?")
        params.append(participation_policy_hash)
    if "created_ts" in columns:
        clauses.append("created_ts >= ? AND created_ts < ?")
        params.extend([start_ts, end_ts])
    elif "updated_ts" in columns:
        clauses.append("updated_ts >= ? AND updated_ts < ?")
        params.extend([start_ts, end_ts])
    if authority_hash and "authority_hash" in columns:
        clauses.append("authority_hash=?")
        params.append(authority_hash)
    return " AND ".join(clauses), params


def _sqlite_metrics(
    conn: sqlite3.Connection,
    *,
    start_ts: int,
    end_ts: int,
    authority_hash: str | None,
    strategy_instance_id: str | None,
    pair: str,
    participation_policy_hash: str | None,
    required_kst_days: list[str],
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    if _table_exists(conn, "orders"):
        scope_sql, scope_params = _order_scope_filter(
            conn,
            start_ts=start_ts,
            end_ts=end_ts,
            authority_hash=authority_hash,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
        )
        metrics["daily_buy_intent_count"] = int(conn.execute(
            f"SELECT COUNT(*) FROM orders WHERE {scope_sql} AND UPPER(side)='BUY'",
            scope_params,
        ).fetchone()[0])
        metrics["daily_buy_submitted_count"] = int(conn.execute(
            f"SELECT COUNT(*) FROM orders WHERE {scope_sql} AND UPPER(side)='BUY' AND status NOT IN ('FAILED','CANCELED','CANCELLED')",
            scope_params,
        ).fetchone()[0])
        metrics["max_holding_exit_due_count"] = int(conn.execute(
            f"SELECT COUNT(*) FROM orders WHERE {scope_sql} AND UPPER(side)='SELL' AND COALESCE(exit_rule_name,'')='max_holding_time'",
            scope_params,
        ).fetchone()[0])
        metrics["max_holding_exit_filled_count"] = int(conn.execute(
            f"SELECT COUNT(*) FROM orders WHERE {scope_sql} AND UPPER(side)='SELL' AND COALESCE(exit_rule_name,'')='max_holding_time' AND status IN ('FILLED','ACCOUNTING_PENDING')",
            scope_params,
        ).fetchone()[0])
        metrics["manual_intervention_count"] = int(conn.execute(
            f"SELECT COUNT(*) FROM orders WHERE {scope_sql} AND COALESCE(decision_reason,'') IN ('manual_flatten','operator_closeout')",
            scope_params,
        ).fetchone()[0])
        metrics["broker_local_mismatch_count"] = int(conn.execute(
            f"SELECT COUNT(*) FROM orders WHERE {scope_sql} AND COALESCE(last_error,'') LIKE '%mismatch%'",
            scope_params,
        ).fetchone()[0])
        day_expr = (
            "date(datetime(created_ts / 1000, 'unixepoch', '+9 hours'))"
            if _column_exists(conn, "orders", "created_ts")
            else "'unknown'"
        )
        duplicate_rows = conn.execute(
            f"""
            SELECT {day_expr} AS kst_day, COUNT(*) AS cnt
            FROM orders
            WHERE {scope_sql} AND UPPER(side)='BUY' AND status IN ('FILLED','ACCOUNTING_PENDING')
            GROUP BY {day_expr}
            """,
            scope_params,
        ).fetchall()
        duplicate_days = [str(row[0]) for row in duplicate_rows if int(row[1]) > 1]
        metrics["duplicate_entry_block_count"] = sum(max(0, int(row[1]) - 1) for row in duplicate_rows)
        metrics["duplicate_buy_kst_days"] = duplicate_days
        coverage_rows = conn.execute(
            f"""
            SELECT {day_expr} AS kst_day, COUNT(*) AS cnt
            FROM orders
            WHERE {scope_sql}
              AND UPPER(side)='BUY'
              AND status IN ('SUBMITTED','NEW','PARTIAL','FILLED','ACCOUNTING_PENDING')
            GROUP BY {day_expr}
            """,
            scope_params,
        ).fetchall()
        metrics["covered_kst_days"] = [str(row[0]) for row in coverage_rows if str(row[0]) in set(required_kst_days)]
    if _table_exists(conn, "fills"):
        time_sql, time_params = _time_filter(conn, "fills", "fill_ts", start_ts, end_ts)
        scope_sql, scope_params = _order_scope_filter(
            conn,
            start_ts=start_ts,
            end_ts=end_ts,
            authority_hash=authority_hash,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
        )
        metrics["daily_buy_filled_count"] = int(conn.execute(
            f"""
            SELECT COUNT(*) FROM fills f JOIN orders o ON o.client_order_id=f.client_order_id
            WHERE {scope_sql} AND UPPER(o.side)='BUY'{time_sql.replace('fill_ts', 'f.fill_ts')}
            """,
            scope_params + time_params,
        ).fetchone()[0])
        price_expr = "COALESCE(f.price,0.0)" if _column_exists(conn, "fills", "price") else "0.0"
        qty_expr = "COALESCE(f.qty,0.0)" if _column_exists(conn, "fills", "qty") else "0.0"
        reference_expr = (
            "COALESCE(f.reference_price,0.0)" if _column_exists(conn, "fills", "reference_price") else "0.0"
        )
        slippage_expr = "f.slippage_bps" if _column_exists(conn, "fills", "slippage_bps") else "NULL"
        fill_ts_expr = "f.fill_ts" if _column_exists(conn, "fills", "fill_ts") else "0"
        created_ts_expr = "o.created_ts" if _column_exists(conn, "orders", "created_ts") else "0"
        fill_rows = conn.execute(
            f"""
            SELECT COALESCE(f.fee,0.0) AS fee, {price_expr} AS price, {qty_expr} AS qty,
                   {reference_expr} AS reference_price,
                   {slippage_expr} AS slippage_bps,
                   {fill_ts_expr} AS fill_ts,
                   {created_ts_expr} AS created_ts
            FROM fills f JOIN orders o ON o.client_order_id=f.client_order_id
            WHERE {scope_sql}{time_sql.replace('fill_ts', 'f.fill_ts')}
            """,
            scope_params + time_params,
        ).fetchall()
        metrics["fee_total_krw"] = float(sum(float(row[0] or 0.0) for row in fill_rows))
        notional = sum(float(row[1] or 0.0) * float(row[2] or 0.0) for row in fill_rows)
        metrics["observed_fee_bps"] = (float(metrics["fee_total_krw"]) / notional * 10_000.0) if notional > 0 else 0.0
        slippages: list[float] = []
        delays: list[float] = []
        for row in fill_rows:
            raw_slippage = row[4]
            if raw_slippage is not None:
                slippages.append(float(raw_slippage))
            elif float(row[3] or 0.0) > 0 and float(row[1] or 0.0) > 0:
                slippages.append(((float(row[1]) - float(row[3])) / float(row[3])) * 10_000.0)
            if _column_exists(conn, "orders", "created_ts") and _column_exists(conn, "fills", "fill_ts"):
                delays.append(max(0.0, (float(row[5] or 0) - float(row[6] or 0)) / 1000.0))
        metrics["slippage_bps_avg"] = sum(slippages) / len(slippages) if slippages else 0.0
        if delays:
            ordered = sorted(delays)
            metrics["exit_delay_seconds_p50"] = ordered[len(ordered) // 2]
            metrics["exit_delay_seconds_max"] = max(ordered)
    if _table_exists(conn, "daily_participation_claims"):
        claim_sql, claim_params = _claim_scope_filter(
            conn,
            start_ts=start_ts,
            end_ts=end_ts,
            required_kst_days=required_kst_days,
            authority_hash=authority_hash,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
            participation_policy_hash=participation_policy_hash,
        )
        rows = conn.execute(
            f"SELECT status, COUNT(*) FROM daily_participation_claims WHERE {claim_sql} GROUP BY status",
            claim_params,
        ).fetchall()
        status_counts = {str(row[0]): int(row[1]) for row in rows}
        metrics["claim_pending_count"] = status_counts.get("claim_pending", 0) + status_counts.get("submitted", 0)
        metrics["claim_fulfilled_count"] = status_counts.get("fulfilled", 0)
        metrics["claim_terminal_failed_count"] = status_counts.get("terminal_failed", 0)
    metrics.setdefault("duplicate_entry_block_count", 0)
    metrics.setdefault("covered_kst_days", [])
    metrics.setdefault("duplicate_buy_kst_days", [])
    return metrics


def _load_authority_scope(authority_path: str | None) -> dict[str, object]:
    if not authority_path:
        return {}
    with Path(authority_path).expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("h74_observation_report_authority_payload_not_object")
    bound = dict(payload.get("hash_bound_parameters") or {})
    return {
        "authority_hash": str(payload.get("authority_content_hash") or sha256_prefixed(payload)),
        "pair": str(bound.get("market") or payload.get("pair") or ""),
        "interval": str(bound.get("interval") or payload.get("interval") or ""),
        "strategy_instance_id": str(payload.get("strategy_instance_id") or bound.get("strategy_instance_id") or ""),
        "participation_policy_hash": str(
            payload.get("participation_policy_hash") or bound.get("participation_policy_hash") or ""
        ),
    }


def _parse_kst_date(value: str) -> date:
    return date.fromisoformat(str(value).strip())


def _kst_window(from_date: str | None, to_date: str | None) -> tuple[datetime | None, datetime | None]:
    if not from_date and not to_date:
        return None, None
    if not from_date or not to_date:
        raise ValueError("h74_observation_report_requires_from_and_to")
    start = datetime.combine(_parse_kst_date(from_date), time(0, 0), tzinfo=KST).astimezone(timezone.utc)
    end = datetime.combine(_parse_kst_date(to_date), time(0, 0), tzinfo=KST).astimezone(timezone.utc)
    if end <= start:
        raise ValueError("h74_observation_report_to_must_be_after_from")
    return start, end


def cmd_h74_observation_report(
    *,
    db_path: str | None = None,
    days: int = 7,
    as_json: bool = False,
    authority: str | None = None,
    authority_hash: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    strategy_instance_id: str | None = None,
    pair: str = "KRW-BTC",
    interval: str = "1m",
    participation_policy_hash: str | None = None,
) -> int:
    authority_scope = _load_authority_scope(authority)
    resolved_authority_hash = authority_hash or str(authority_scope.get("authority_hash") or "") or None
    resolved_strategy_instance_id = strategy_instance_id or str(authority_scope.get("strategy_instance_id") or "") or None
    resolved_pair = str(pair or authority_scope.get("pair") or "KRW-BTC")
    resolved_interval = str(interval or authority_scope.get("interval") or "1m")
    resolved_policy_hash = participation_policy_hash or str(authority_scope.get("participation_policy_hash") or "") or None
    observation_start, observation_end = _kst_window(from_date, to_date)
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True) if db_path else None
    try:
        report = build_h74_observation_report(
            conn=conn,
            days=days,
            observation_start=observation_start,
            observation_end=observation_end,
            authority_hash=resolved_authority_hash,
            strategy_instance_id=resolved_strategy_instance_id,
            participation_policy_hash=resolved_policy_hash,
            pair=resolved_pair,
            interval=resolved_interval,
        )
    finally:
        if conn is not None:
            conn.close()
    if as_json:
        print(json.dumps(report, sort_keys=True, ensure_ascii=False))
    else:
        print(f"h74_observation_report complete={report['complete']} days={days}")
    return 0
