from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .live_trade_classification import classify_h74_live_trade
from .h74_cycle_classification import classify_h74_cycle
from .h74_pnl_attribution import build_pnl_attribution, build_terminal_residual
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
        "terminal_residual": build_terminal_residual(
            residual_qty=0.0,
            residual_mark_price=0.0,
            origin_cycle_id="",
            allow_true_dust_next_cycle=True,
        ),
        "pnl_attribution": build_pnl_attribution(
            backtest_expected_entry_price=0.0,
            live_entry_avg_price=0.0,
            backtest_expected_exit_price=0.0,
            live_exit_avg_price=0.0,
            qty=0.0,
        ),
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
                interval=interval,
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
    payload["interval_scope_applied"] = bool(payload.get("interval_scope_applied", False))
    payload["interval_scope_unavailable"] = list(payload.get("interval_scope_unavailable") or [])
    payload["complete"] = bool(
        start <= current
        and end <= current
        and elapsed_seconds >= 7 * 86400
        and len(required_kst_days) == 7
        and covered_days == set(required_kst_days)
        and not duplicate_days
        and authority_scope_applied
        and payload["interval_scope_applied"]
        and not payload["interval_scope_unavailable"]
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
    interval: str,
) -> tuple[str, list[Any]]:
    clauses = ["strategy_name='daily_participation_sma'"]
    params: list[Any] = []
    if _column_exists(conn, "orders", "created_ts"):
        clauses.append("created_ts >= ? AND created_ts < ?")
        params.extend([start_ts, end_ts])
    if _column_exists(conn, "orders", "pair"):
        clauses.append("pair=?")
        params.append(pair)
    if _column_exists(conn, "orders", "interval"):
        clauses.append("interval=?")
        params.append(interval)
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
    interval: str,
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
    if "interval" in columns:
        clauses.append("interval=?")
        params.append(interval)
    elif (
        "client_order_id" in columns
        and _column_exists(conn, "orders", "client_order_id")
        and _column_exists(conn, "orders", "interval")
    ):
        order_scope_sql, order_scope_params = _order_scope_filter(
            conn,
            start_ts=start_ts,
            end_ts=end_ts,
            authority_hash=authority_hash,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
            interval=interval,
        )
        clauses.append(
            "EXISTS ("
            "SELECT 1 FROM orders o "
            "WHERE o.client_order_id=daily_participation_claims.client_order_id "
            f"AND {order_scope_sql}"
            ")"
        )
        params.extend(order_scope_params)
    return " AND ".join(clauses), params


def _claim_interval_scope_available(conn: sqlite3.Connection) -> bool:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(daily_participation_claims)").fetchall()}
    return "interval" in columns or (
        "client_order_id" in columns
        and _column_exists(conn, "orders", "client_order_id")
        and _column_exists(conn, "orders", "interval")
    )


def _sqlite_metrics(
    conn: sqlite3.Connection,
    *,
    start_ts: int,
    end_ts: int,
    authority_hash: str | None,
    strategy_instance_id: str | None,
    pair: str,
    interval: str,
    participation_policy_hash: str | None,
    required_kst_days: list[str],
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    interval_scope_unavailable: list[str] = []
    orders_interval_scoped = _table_exists(conn, "orders") and _column_exists(conn, "orders", "interval")
    if not orders_interval_scoped:
        interval_scope_unavailable.extend(["orders", "fills_via_orders"])
    if _table_exists(conn, "orders"):
        scope_sql, scope_params = _order_scope_filter(
            conn,
            start_ts=start_ts,
            end_ts=end_ts,
            authority_hash=authority_hash,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
            interval=interval,
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
            interval=interval,
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
        decision_reason_expr = (
            "COALESCE(o.decision_reason_code, o.decision_reason, '')"
            if _column_exists(conn, "orders", "decision_reason_code")
            else "COALESCE(o.decision_reason, '')"
            if _column_exists(conn, "orders", "decision_reason")
            else "''"
        )
        intent_type_expr = "COALESCE(o.intent_type, '')" if _column_exists(conn, "orders", "intent_type") else "''"
        authority_source_expr = (
            "COALESCE(o.authority_source, '')" if _column_exists(conn, "orders", "authority_source") else "''"
        )
        entry_authority_source_expr = (
            "COALESCE(o.entry_authority_source, '')"
            if _column_exists(conn, "orders", "entry_authority_source")
            else "''"
        )
        entry_authority_status_expr = (
            "COALESCE(o.entry_authority_status, '')"
            if _column_exists(conn, "orders", "entry_authority_status")
            else "''"
        )
        decision_kst_hour_expr = (
            "o.decision_kst_hour"
            if _column_exists(conn, "orders", "decision_kst_hour")
            else "CAST(strftime('%H', datetime(o.created_ts / 1000, 'unixepoch', '+9 hours')) AS INTEGER)"
            if _column_exists(conn, "orders", "created_ts")
            else "NULL"
        )
        exchange_order_id_expr = (
            "COALESCE(o.exchange_order_id, '')"
            if _column_exists(conn, "orders", "exchange_order_id")
            else "''"
        )
        fill_rows = conn.execute(
            f"""
            SELECT COALESCE(f.fee,0.0) AS fee, {price_expr} AS price, {qty_expr} AS qty,
                   {reference_expr} AS reference_price,
                   {slippage_expr} AS slippage_bps,
                   {fill_ts_expr} AS fill_ts,
                   {created_ts_expr} AS created_ts,
                   COALESCE(o.client_order_id, '') AS client_order_id,
                   COALESCE(o.side, '') AS side,
                   {decision_reason_expr} AS decision_reason_code,
                   {intent_type_expr} AS intent_type,
                   {authority_source_expr} AS authority_source,
                   {entry_authority_source_expr} AS entry_authority_source,
                   {entry_authority_status_expr} AS entry_authority_status,
                   {decision_kst_hour_expr} AS decision_kst_hour,
                   {exchange_order_id_expr} AS exchange_order_id
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
        classified_rows: list[dict[str, Any]] = []
        classification_error_count = 0
        for row in fill_rows:
            base = {
                "client_order_id": str(row[7] or ""),
                "side": str(row[8] or ""),
                "filled": True,
                "exchange_order_id": str(row[15] or ""),
                "decision_reason_code": str(row[9] or ""),
                "intent_type": str(row[10] or ""),
                "authority_source": str(row[11] or ""),
                "entry_authority_source": str(row[12] or ""),
                "entry_authority_status": str(row[13] or ""),
                "decision_kst_hour": row[14],
            }
            try:
                classification = classify_h74_live_trade(base)
            except ValueError as exc:
                classification_error_count += 1
                classification = {
                    "live_plumbing_success": True,
                    "h74_backtest_validation_sample": False,
                    "incident_type": "classification_error",
                    "entry_authority_source": "",
                    "classification_error": str(exc),
                }
            classified_rows.append({**base, **classification})
        metrics["h74_live_trade_classifications"] = classified_rows
        cycle_metrics = _h74_cycle_metrics(
            conn,
            start_ts=start_ts,
            end_ts=end_ts,
            authority_hash=authority_hash,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
            interval=interval,
        )
        if cycle_metrics:
            metrics.update(cycle_metrics)
        else:
            metrics["h74_backtest_validation_sample_count"] = sum(
                1 for row in classified_rows if bool(row.get("h74_backtest_validation_sample"))
            )
            metrics["entry_path_sample_count"] = sum(
                1 for row in classified_rows if bool(row.get("h74_entry_path_sample"))
            )
            metrics["cycle_validation_success_count"] = sum(
                1 for row in classified_rows if bool(row.get("h74_cycle_validation_success"))
            )
        incident_counts: dict[str, int] = {}
        for row in classified_rows:
            incident_type = str(row.get("incident_type") or "none")
            if incident_type != "none":
                incident_counts[incident_type] = incident_counts.get(incident_type, 0) + 1
        metrics["h74_incident_counts"] = incident_counts
        metrics["h74_trade_classification_error_count"] = classification_error_count
    if _table_exists(conn, "daily_participation_claims"):
        if not _claim_interval_scope_available(conn):
            interval_scope_unavailable.append("daily_participation_claims")
        claim_sql, claim_params = _claim_scope_filter(
            conn,
            start_ts=start_ts,
            end_ts=end_ts,
            required_kst_days=required_kst_days,
            authority_hash=authority_hash,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
            interval=interval,
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
    metrics["interval_scope_applied"] = not interval_scope_unavailable
    metrics["interval_scope_unavailable"] = interval_scope_unavailable
    return metrics


def _rows_as_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    columns = [str(item[0]) for item in (cursor.description or ())]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _select_existing_columns(conn: sqlite3.Connection, table: str, columns: list[str]) -> list[str]:
    if not _table_exists(conn, table):
        return []
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    return [column for column in columns if column in existing]


def _h74_cycle_metrics(
    conn: sqlite3.Connection,
    *,
    start_ts: int,
    end_ts: int,
    authority_hash: str | None,
    strategy_instance_id: str | None,
    pair: str,
    interval: str,
) -> dict[str, Any]:
    if not (_table_exists(conn, "orders") and _table_exists(conn, "fills")):
        return {}
    if not _column_exists(conn, "orders", "cycle_id"):
        return {}
    order_columns = _select_existing_columns(
        conn,
        "orders",
        [
            "client_order_id",
            "strategy_name",
            "strategy_instance_id",
            "pair",
            "interval",
            "side",
            "status",
            "exit_rule_name",
            "decision_reason",
            "decision_reason_code",
            "entry_authority_source",
            "authority_source",
            "created_ts",
            "cycle_id",
            "authority_hash",
        ],
    )
    if not {"client_order_id", "side", "cycle_id"}.issubset(set(order_columns)):
        return {}
    scope_sql, scope_params = _order_scope_filter(
        conn,
        start_ts=start_ts,
        end_ts=end_ts,
        authority_hash=authority_hash,
        strategy_instance_id=strategy_instance_id,
        pair=pair,
        interval=interval,
    )
    order_sql = (
        "SELECT "
        + ", ".join(order_columns)
        + f" FROM orders WHERE {scope_sql} AND COALESCE(cycle_id,'')<>''"
    )
    orders = _rows_as_dicts(conn.execute(order_sql, scope_params))
    if not orders:
        return {}
    fill_columns = _select_existing_columns(
        conn,
        "fills",
        ["client_order_id", "fill_ts", "price", "qty", "fee", "reference_price", "slippage_bps"],
    )
    fills: list[dict[str, Any]] = []
    if "client_order_id" in fill_columns:
        fills = _rows_as_dicts(
            conn.execute(
                "SELECT " + ", ".join(fill_columns) + " FROM fills",
            )
        )
    state_rows = []
    if _table_exists(conn, "h74_cycle_state"):
        state_rows = _rows_as_dicts(
            conn.execute(
                """
                SELECT cycle_id, authority_hash, strategy_instance_id, pair, state,
                       entry_client_order_id, exit_client_order_id, acquired_qty,
                       sold_qty, locked_exit_qty
                FROM h74_cycle_state
                """
            )
        )
    fills_by_order: dict[str, list[dict[str, Any]]] = {}
    for fill in fills:
        fills_by_order.setdefault(str(fill.get("client_order_id") or ""), []).append(fill)
    state_by_cycle = {str(row.get("cycle_id") or ""): row for row in state_rows}
    orders_by_cycle: dict[str, list[dict[str, Any]]] = {}
    for order in orders:
        orders_by_cycle.setdefault(str(order.get("cycle_id") or ""), []).append(order)

    cycle_reports: list[dict[str, Any]] = []
    for cycle_id, cycle_orders in sorted(orders_by_cycle.items()):
        if not cycle_id:
            continue
        entry_order = next(
            (order for order in cycle_orders if str(order.get("side") or "").upper() == "BUY"),
            None,
        )
        exit_order = next(
            (
                order
                for order in cycle_orders
                if str(order.get("side") or "").upper() == "SELL"
                and str(order.get("exit_rule_name") or "") == "max_holding_time"
            ),
            None,
        )
        entry = _cycle_fill_payload(entry_order, fills_by_order, side="BUY")
        exit = _cycle_fill_payload(exit_order, fills_by_order, side="SELL")
        state = state_by_cycle.get(cycle_id, {})
        acquired_qty = _float_value(state.get("acquired_qty"))
        sold_qty = _float_value(state.get("sold_qty"))
        locked_exit_qty = _float_value(state.get("locked_exit_qty"))
        remaining_cycle_qty = max(0.0, acquired_qty - sold_qty - locked_exit_qty)
        mark_price = _cycle_mark_price(entry, exit)
        terminal_residual = build_terminal_residual(
            residual_qty=remaining_cycle_qty,
            residual_mark_price=mark_price,
            origin_cycle_id=cycle_id,
            allow_true_dust_next_cycle=True,
        )
        terminal = {
            "terminal_executable_qty": remaining_cycle_qty
            if bool(terminal_residual.get("exchange_sellable"))
            else 0.0,
            "broker_local_converged": True,
            "executable_residual_qty": remaining_cycle_qty
            if bool(terminal_residual.get("exchange_sellable"))
            else 0.0,
        }
        classification = classify_h74_cycle(
            entry=entry,
            exit=exit,
            terminal=terminal,
            orders=cycle_orders,
        )
        pnl_attribution = _cycle_pnl_attribution(entry=entry, exit=exit, residual=terminal_residual)
        success = bool(classification.h74_cycle_validation_success)
        if not bool(terminal_residual.get("next_cycle_allowed")):
            success = False
        if not bool(pnl_attribution.get("passes")):
            success = False
        cycle_report = {
            "cycle_id": cycle_id,
            **classification.as_dict(),
            "h74_cycle_validation_success": success,
            "h74_backtest_validation_sample": success,
            "terminal_residual": terminal_residual,
            "pnl_attribution": pnl_attribution,
        }
        cycle_reports.append(cycle_report)
    if not cycle_reports:
        return {}
    latest = cycle_reports[-1]
    return {
        "h74_cycle_classifications": cycle_reports,
        "entry_path_sample_count": sum(
            1 for row in cycle_reports if bool(row.get("h74_entry_path_sample"))
        ),
        "cycle_validation_success_count": sum(
            1 for row in cycle_reports if bool(row.get("h74_cycle_validation_success"))
        ),
        "h74_backtest_validation_sample_count": sum(
            1 for row in cycle_reports if bool(row.get("h74_backtest_validation_sample"))
        ),
        "unauthorized_intermediate_order_count": sum(
            int(row.get("unauthorized_intermediate_order_count") or 0)
            for row in cycle_reports
        ),
        "unauthorized_order_ids": [
            order_id
            for row in cycle_reports
            for order_id in list(row.get("unauthorized_order_ids") or [])
        ],
        "terminal_residual": latest["terminal_residual"],
        "pnl_attribution": latest["pnl_attribution"],
    }


def _cycle_fill_payload(
    order: dict[str, Any] | None,
    fills_by_order: dict[str, list[dict[str, Any]]],
    *,
    side: str,
) -> dict[str, Any] | None:
    if not order:
        return None
    client_order_id = str(order.get("client_order_id") or "")
    fills = fills_by_order.get(client_order_id, [])
    fill_qty = sum(_float_value(fill.get("qty")) for fill in fills)
    fill_notional = sum(_float_value(fill.get("qty")) * _float_value(fill.get("price")) for fill in fills)
    avg_price = fill_notional / fill_qty if fill_qty > 0 else 0.0
    fill_ts_values = [
        int(_float_value(fill.get("fill_ts")))
        for fill in fills
        if _float_value(fill.get("fill_ts")) > 0
    ]
    payload = {
        **order,
        "client_order_id": client_order_id,
        "side": side,
        "fill_ts": min(fill_ts_values) if fill_ts_values else order.get("created_ts"),
        "qty": fill_qty,
        "avg_price": avg_price,
        "fee": sum(_float_value(fill.get("fee")) for fill in fills),
        "reference_price": _float_value(fills[0].get("reference_price")) if fills else avg_price,
        "authority_source": order.get("entry_authority_source")
        or order.get("authority_source")
        or "daily_participation_entry",
    }
    return payload


def _cycle_mark_price(entry: dict[str, Any] | None, exit: dict[str, Any] | None) -> float:
    for payload in (exit, entry):
        if payload and _float_value(payload.get("avg_price")) > 0:
            return _float_value(payload.get("avg_price"))
    return 0.0


def _cycle_pnl_attribution(
    *,
    entry: dict[str, Any] | None,
    exit: dict[str, Any] | None,
    residual: dict[str, Any],
) -> dict[str, Any]:
    entry_price = _float_value((entry or {}).get("avg_price"))
    exit_price = _float_value((exit or {}).get("avg_price"))
    qty = min(_float_value((entry or {}).get("qty")), _float_value((exit or {}).get("qty")))
    fee_delta = _float_value((entry or {}).get("fee")) + _float_value((exit or {}).get("fee"))
    attribution = build_pnl_attribution(
        backtest_expected_entry_price=_float_value((entry or {}).get("reference_price")) or entry_price,
        live_entry_avg_price=entry_price,
        backtest_expected_exit_price=_float_value((exit or {}).get("reference_price")) or exit_price,
        live_exit_avg_price=exit_price,
        qty=qty,
        fee_delta_krw=fee_delta,
        residual_mark_to_market_krw=_float_value(residual.get("residual_notional_krw")),
    )
    from .h74_pnl_attribution import pnl_attribution_passes

    attribution["passes"] = pnl_attribution_passes(attribution)
    return attribution


def _float_value(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


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
