from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from . import runtime_state
from .broker.balance_source import fetch_balance_snapshot
from .config import settings
from .db_core import (
    get_portfolio,
    get_portfolio_breakdown,
    init_portfolio,
    portfolio_asset_total,
    portfolio_cash_total,
)
from .lifecycle import summarize_position_lots
from .observability import format_log_kv
from .oms import evaluate_unresolved_order_gate
from .recovery import CASH_SPLIT_ABS_TOL

KST = timezone(timedelta(hours=9))
POSITION_EPSILON = 1e-12
RISK_STATE_MISMATCH = "RISK_STATE_MISMATCH"
DAILY_LOSS_LIMIT_REASON_CODE = "DAILY_LOSS_LIMIT"
_DEFAULT_ASSET_ABS_TOL = 1e-12
RISK_LOG = logging.getLogger("bithumb_bot.run")


@dataclass(frozen=True)
class DailyLossEvaluation:
    blocked: bool
    reason: str
    reason_code: str
    decision: str
    evaluation_ts_ms: int
    day_kst: str
    max_daily_loss_krw: float
    start_equity: float | None
    current_equity: float | None
    loss_today: float | None
    current_cash_krw: float | None
    current_asset_qty: float | None
    mark_price: float
    mark_price_source: str
    details: dict[str, Any]


def _day_kst(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=KST)
    return dt.strftime("%Y-%m-%d")


def _add_column_if_missing(conn: sqlite3.Connection, table: str, name: str, ddl: str) -> None:
    columns = {
        str(row["name"]) if hasattr(row, "keys") else str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if name in columns:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _ensure_daily_risk_tables(conn: sqlite3.Connection) -> None:
    had_tx = conn.in_transaction
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_risk (
            day_kst TEXT PRIMARY KEY,
            start_equity REAL NOT NULL
        )
        """
    )
    for name, ddl in (
        ("baseline_cash_krw", "baseline_cash_krw REAL"),
        ("baseline_asset_qty", "baseline_asset_qty REAL"),
        ("baseline_mark_price", "baseline_mark_price REAL"),
        ("baseline_mark_price_source", "baseline_mark_price_source TEXT"),
        ("baseline_origin", "baseline_origin TEXT"),
        ("baseline_balance_source", "baseline_balance_source TEXT"),
        ("baseline_balance_observed_ts_ms", "baseline_balance_observed_ts_ms INTEGER"),
        ("baseline_reconcile_epoch_sec", "baseline_reconcile_epoch_sec REAL"),
        ("baseline_reconcile_reason_code", "baseline_reconcile_reason_code TEXT"),
        ("baseline_context", "baseline_context TEXT"),
        ("created_ts_ms", "created_ts_ms INTEGER"),
    ):
        _add_column_if_missing(conn, "daily_risk", name, ddl)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evaluation_ts_ms INTEGER NOT NULL,
            day_kst TEXT NOT NULL,
            evaluation_origin TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            decision TEXT NOT NULL,
            max_daily_loss_krw REAL NOT NULL,
            start_equity REAL,
            current_equity REAL,
            loss_today REAL,
            current_cash_krw REAL,
            current_asset_qty REAL,
            mark_price REAL NOT NULL,
            mark_price_source TEXT NOT NULL,
            baseline_cash_krw REAL,
            baseline_asset_qty REAL,
            baseline_mark_price REAL,
            baseline_origin TEXT,
            baseline_balance_source TEXT,
            baseline_balance_observed_ts_ms INTEGER,
            current_source TEXT,
            current_balance_source TEXT,
            current_balance_observed_ts_ms INTEGER,
            current_reconcile_epoch_sec REAL,
            current_reconcile_reason_code TEXT,
            local_cash_krw REAL,
            local_asset_qty REAL,
            broker_cash_krw REAL,
            broker_asset_qty REAL,
            mismatch_summary TEXT,
            details_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_evaluations_day_eval_ts
        ON risk_evaluations(day_kst, evaluation_ts_ms, id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_evaluations_reason_code
        ON risk_evaluations(reason_code, evaluation_ts_ms, id)
        """
    )
    if not had_tx:
        conn.commit()


def _asset_abs_tolerance(conn: sqlite3.Connection) -> float:
    rules_min_qty = 0.0
    try:
        lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR)
        lot_definition = getattr(lot_snapshot, "lot_definition", None)
        if lot_definition is not None and lot_definition.min_qty is not None:
            rules_min_qty = float(lot_definition.min_qty)
    except Exception:
        rules_min_qty = 0.0
    return max(_DEFAULT_ASSET_ABS_TOL, min(max(rules_min_qty / 10.0, 0.0), 1e-4))


def _portfolio_snapshot(conn: sqlite3.Connection) -> dict[str, float]:
    init_portfolio(conn)
    cash_total, asset_total = get_portfolio(conn)
    cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
    return {
        "cash_total": float(cash_total),
        "asset_total": float(asset_total),
        "cash_available": float(cash_available),
        "cash_locked": float(cash_locked),
        "asset_available": float(asset_available),
        "asset_locked": float(asset_locked),
    }


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _load_daily_risk_row(conn: sqlite3.Connection, day_kst: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
            day_kst,
            start_equity,
            baseline_cash_krw,
            baseline_asset_qty,
            baseline_mark_price,
            baseline_mark_price_source,
            baseline_origin,
            baseline_balance_source,
            baseline_balance_observed_ts_ms,
            baseline_reconcile_epoch_sec,
            baseline_reconcile_reason_code,
            baseline_context,
            created_ts_ms
        FROM daily_risk
        WHERE day_kst=?
        """,
        (day_kst,),
    ).fetchone()


def _seed_daily_risk_baseline(
    conn: sqlite3.Connection,
    *,
    day_kst: str,
    equity: float,
    current_snapshot: dict[str, Any],
    evaluation_ts_ms: int,
) -> sqlite3.Row:
    had_tx = conn.in_transaction
    baseline_context = {
        "current_source": current_snapshot["current_source"],
        "current_balance_source": current_snapshot.get("current_balance_source"),
        "current_balance_observed_ts_ms": current_snapshot.get("current_balance_observed_ts_ms"),
        "current_reconcile_epoch_sec": current_snapshot.get("current_reconcile_epoch_sec"),
        "current_reconcile_reason_code": current_snapshot.get("current_reconcile_reason_code"),
        "local_cash_krw": current_snapshot.get("local_cash_krw"),
        "local_asset_qty": current_snapshot.get("local_asset_qty"),
        "broker_cash_krw": current_snapshot.get("broker_cash_krw"),
        "broker_asset_qty": current_snapshot.get("broker_asset_qty"),
    }
    conn.execute(
        """
        INSERT INTO daily_risk(
            day_kst,
            start_equity,
            baseline_cash_krw,
            baseline_asset_qty,
            baseline_mark_price,
            baseline_mark_price_source,
            baseline_origin,
            baseline_balance_source,
            baseline_balance_observed_ts_ms,
            baseline_reconcile_epoch_sec,
            baseline_reconcile_reason_code,
            baseline_context,
            created_ts_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            day_kst,
            float(equity),
            float(current_snapshot["cash_krw"]),
            float(current_snapshot["asset_qty"]),
            float(current_snapshot["mark_price"]),
            str(current_snapshot["mark_price_source"]),
            "seeded_on_first_verified_eval",
            current_snapshot.get("current_balance_source"),
            current_snapshot.get("current_balance_observed_ts_ms"),
            current_snapshot.get("current_reconcile_epoch_sec"),
            current_snapshot.get("current_reconcile_reason_code"),
            _json_dumps(baseline_context),
            int(evaluation_ts_ms),
        ),
    )
    if not had_tx:
        conn.commit()
    row = _load_daily_risk_row(conn, day_kst)
    if row is None:
        raise RuntimeError(f"failed to seed daily_risk baseline for {day_kst}")
    return row


def _record_risk_evaluation(conn: sqlite3.Connection, evaluation: DailyLossEvaluation) -> None:
    had_tx = conn.in_transaction
    details = dict(evaluation.details)
    baseline = details.get("baseline") if isinstance(details.get("baseline"), dict) else {}
    conn.execute(
        """
        INSERT INTO risk_evaluations(
            evaluation_ts_ms,
            day_kst,
            evaluation_origin,
            reason_code,
            decision,
            max_daily_loss_krw,
            start_equity,
            current_equity,
            loss_today,
            current_cash_krw,
            current_asset_qty,
            mark_price,
            mark_price_source,
            baseline_cash_krw,
            baseline_asset_qty,
            baseline_mark_price,
            baseline_origin,
            baseline_balance_source,
            baseline_balance_observed_ts_ms,
            current_source,
            current_balance_source,
            current_balance_observed_ts_ms,
            current_reconcile_epoch_sec,
            current_reconcile_reason_code,
            local_cash_krw,
            local_asset_qty,
            broker_cash_krw,
            broker_asset_qty,
            mismatch_summary,
            details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(evaluation.evaluation_ts_ms),
            str(evaluation.day_kst),
            str(details.get("evaluation_origin") or "risk_eval"),
            str(evaluation.reason_code),
            str(evaluation.decision),
            float(evaluation.max_daily_loss_krw),
            (float(evaluation.start_equity) if evaluation.start_equity is not None else None),
            (float(evaluation.current_equity) if evaluation.current_equity is not None else None),
            (float(evaluation.loss_today) if evaluation.loss_today is not None else None),
            (float(evaluation.current_cash_krw) if evaluation.current_cash_krw is not None else None),
            (float(evaluation.current_asset_qty) if evaluation.current_asset_qty is not None else None),
            float(evaluation.mark_price),
            str(evaluation.mark_price_source),
            (
                float(baseline.get("baseline_cash_krw"))
                if baseline.get("baseline_cash_krw") is not None
                else None
            ),
            (
                float(baseline.get("baseline_asset_qty"))
                if baseline.get("baseline_asset_qty") is not None
                else None
            ),
            (
                float(baseline.get("baseline_mark_price"))
                if baseline.get("baseline_mark_price") is not None
                else None
            ),
            str(baseline.get("baseline_origin")) if baseline.get("baseline_origin") is not None else None,
            str(baseline.get("baseline_balance_source"))
            if baseline.get("baseline_balance_source") is not None
            else None,
            (
                int(baseline.get("baseline_balance_observed_ts_ms"))
                if baseline.get("baseline_balance_observed_ts_ms") is not None
                else None
            ),
            str(details.get("current_source")) if details.get("current_source") is not None else None,
            (
                str(details.get("current_balance_source"))
                if details.get("current_balance_source") is not None
                else None
            ),
            (
                int(details.get("current_balance_observed_ts_ms"))
                if details.get("current_balance_observed_ts_ms") is not None
                else None
            ),
            (
                float(details.get("current_reconcile_epoch_sec"))
                if details.get("current_reconcile_epoch_sec") is not None
                else None
            ),
            (
                str(details.get("current_reconcile_reason_code"))
                if details.get("current_reconcile_reason_code") is not None
                else None
            ),
            (float(details.get("local_cash_krw")) if details.get("local_cash_krw") is not None else None),
            (float(details.get("local_asset_qty")) if details.get("local_asset_qty") is not None else None),
            (float(details.get("broker_cash_krw")) if details.get("broker_cash_krw") is not None else None),
            (float(details.get("broker_asset_qty")) if details.get("broker_asset_qty") is not None else None),
            str(details.get("mismatch_summary")) if details.get("mismatch_summary") is not None else None,
            _json_dumps(details),
        ),
    )
    if not had_tx:
        conn.commit()


def _mismatch_evaluation(
    *,
    evaluation_ts_ms: int,
    day_kst: str,
    max_daily_loss_krw: float,
    mark_price: float,
    mark_price_source: str,
    reason_detail: str,
    details: dict[str, Any],
) -> DailyLossEvaluation:
    reason = f"risk state mismatch ({reason_detail})"
    evaluation = DailyLossEvaluation(
        blocked=True,
        reason=reason,
        reason_code=RISK_STATE_MISMATCH,
        decision="unverified",
        evaluation_ts_ms=int(evaluation_ts_ms),
        day_kst=day_kst,
        max_daily_loss_krw=float(max_daily_loss_krw),
        start_equity=None,
        current_equity=None,
        loss_today=None,
        current_cash_krw=None,
        current_asset_qty=None,
        mark_price=float(mark_price),
        mark_price_source=str(mark_price_source),
        details=details,
    )
    RISK_LOG.warning(
        format_log_kv(
            "[RISK] daily_loss_unverified",
            day_kst=day_kst,
            evaluation_origin=details.get("evaluation_origin"),
            reason_code=RISK_STATE_MISMATCH,
            reason=reason,
            current_source=details.get("current_source"),
            current_balance_source=details.get("current_balance_source"),
            current_balance_observed_ts_ms=details.get("current_balance_observed_ts_ms"),
            current_reconcile_epoch_sec=details.get("current_reconcile_epoch_sec"),
            current_reconcile_reason_code=details.get("current_reconcile_reason_code"),
            mismatch_summary=details.get("mismatch_summary"),
        )
    )
    return evaluation


def _build_current_snapshot(
    conn: sqlite3.Connection,
    *,
    broker: object | None,
    ts_ms: int,
    mark_price: float,
    mark_price_source: str,
    evaluation_origin: str,
) -> dict[str, Any] | DailyLossEvaluation:
    local_snapshot = _portfolio_snapshot(conn)
    state = runtime_state.snapshot()
    last_reconcile_epoch_sec = getattr(state, "last_reconcile_epoch_sec", None)
    last_reconcile_reason_code = getattr(state, "last_reconcile_reason_code", None)
    last_reconcile_status = getattr(state, "last_reconcile_status", None)
    details: dict[str, Any] = {
        "evaluation_origin": evaluation_origin,
        "current_source": "local_portfolio",
        "local_cash_krw": float(local_snapshot["cash_total"]),
        "local_asset_qty": float(local_snapshot["asset_total"]),
        "current_reconcile_epoch_sec": last_reconcile_epoch_sec,
        "current_reconcile_reason_code": last_reconcile_reason_code,
        "last_reconcile_status": last_reconcile_status,
        "mark_price": float(mark_price),
        "mark_price_source": str(mark_price_source),
    }

    if settings.MODE == "live":
        if str(last_reconcile_status or "").lower() != "ok" or last_reconcile_epoch_sec is None:
            details["mismatch_summary"] = "latest reconcile state is not ok"
            return _mismatch_evaluation(
                evaluation_ts_ms=ts_ms,
                day_kst=_day_kst(ts_ms),
                max_daily_loss_krw=float(settings.MAX_DAILY_LOSS_KRW),
                mark_price=float(mark_price),
                mark_price_source=mark_price_source,
                reason_detail=(
                    "latest reconcile is not verified "
                    f"(status={last_reconcile_status or 'none'} reason_code={last_reconcile_reason_code or 'none'})"
                ),
                details=details,
            )
        if broker is None:
            details["mismatch_summary"] = "live risk evaluation requires current broker balance snapshot"
            return _mismatch_evaluation(
                evaluation_ts_ms=ts_ms,
                day_kst=_day_kst(ts_ms),
                max_daily_loss_krw=float(settings.MAX_DAILY_LOSS_KRW),
                mark_price=float(mark_price),
                mark_price_source=mark_price_source,
                reason_detail="current broker balance snapshot unavailable in live risk path",
                details=details,
            )
        try:
            balance_snapshot = fetch_balance_snapshot(broker)
        except Exception as exc:
            details["mismatch_summary"] = f"balance snapshot unavailable: {type(exc).__name__}: {exc}"
            return _mismatch_evaluation(
                evaluation_ts_ms=ts_ms,
                day_kst=_day_kst(ts_ms),
                max_daily_loss_krw=float(settings.MAX_DAILY_LOSS_KRW),
                mark_price=float(mark_price),
                mark_price_source=mark_price_source,
                reason_detail=f"broker balance snapshot unavailable ({type(exc).__name__}: {exc})",
                details=details,
            )
        broker_balance = balance_snapshot.balance
        broker_cash_total = portfolio_cash_total(
            cash_available=float(broker_balance.cash_available),
            cash_locked=float(broker_balance.cash_locked),
        )
        broker_asset_total = portfolio_asset_total(
            asset_available=float(broker_balance.asset_available),
            asset_locked=float(broker_balance.asset_locked),
        )
        details.update(
            {
                "current_source": "broker_balance_snapshot",
                "current_balance_source": str(balance_snapshot.source_id or "unknown"),
                "current_balance_observed_ts_ms": int(balance_snapshot.observed_ts_ms),
                "broker_cash_krw": float(broker_cash_total),
                "broker_asset_qty": float(broker_asset_total),
            }
        )

        asset_tol = _asset_abs_tolerance(conn)
        cash_delta = float(local_snapshot["cash_total"]) - float(broker_cash_total)
        asset_delta = float(local_snapshot["asset_total"]) - float(broker_asset_total)
        if abs(cash_delta) > CASH_SPLIT_ABS_TOL or abs(asset_delta) > asset_tol:
            details["mismatch_summary"] = (
                "broker/local portfolio mismatch "
                f"(cash_delta={cash_delta:.6f} asset_delta={asset_delta:.12f})"
            )
            return _mismatch_evaluation(
                evaluation_ts_ms=ts_ms,
                day_kst=_day_kst(ts_ms),
                max_daily_loss_krw=float(settings.MAX_DAILY_LOSS_KRW),
                mark_price=float(mark_price),
                mark_price_source=mark_price_source,
                reason_detail=(
                    "broker snapshot does not match persisted portfolio "
                    f"(cash_delta={cash_delta:.6f} asset_delta={asset_delta:.12f})"
                ),
                details=details,
            )

        return {
            "cash_krw": float(broker_cash_total),
            "asset_qty": float(broker_asset_total),
            "mark_price": float(mark_price),
            "mark_price_source": str(mark_price_source),
            "current_source": "broker_balance_snapshot",
            "current_balance_source": str(balance_snapshot.source_id or "unknown"),
            "current_balance_observed_ts_ms": int(balance_snapshot.observed_ts_ms),
            "current_reconcile_epoch_sec": last_reconcile_epoch_sec,
            "current_reconcile_reason_code": last_reconcile_reason_code,
            "local_cash_krw": float(local_snapshot["cash_total"]),
            "local_asset_qty": float(local_snapshot["asset_total"]),
            "broker_cash_krw": float(broker_cash_total),
            "broker_asset_qty": float(broker_asset_total),
            "evaluation_origin": evaluation_origin,
        }

    return {
        "cash_krw": float(local_snapshot["cash_total"]),
        "asset_qty": float(local_snapshot["asset_total"]),
        "mark_price": float(mark_price),
        "mark_price_source": str(mark_price_source),
        "current_source": "local_portfolio",
        "current_balance_source": "local_portfolio",
        "current_balance_observed_ts_ms": int(ts_ms),
        "current_reconcile_epoch_sec": last_reconcile_epoch_sec,
        "current_reconcile_reason_code": last_reconcile_reason_code,
        "local_cash_krw": float(local_snapshot["cash_total"]),
        "local_asset_qty": float(local_snapshot["asset_total"]),
        "broker_cash_krw": None,
        "broker_asset_qty": None,
        "evaluation_origin": evaluation_origin,
    }


def _validated_baseline_or_mismatch(
    conn: sqlite3.Connection,
    *,
    day_kst: str,
    current_snapshot: dict[str, Any],
    current_equity: float,
    evaluation_ts_ms: int,
) -> sqlite3.Row | DailyLossEvaluation:
    row = _load_daily_risk_row(conn, day_kst)
    if row is None:
        return _seed_daily_risk_baseline(
            conn,
            day_kst=day_kst,
            equity=current_equity,
            current_snapshot=current_snapshot,
            evaluation_ts_ms=evaluation_ts_ms,
        )

    if settings.MODE == "live":
        required_columns = {
            "baseline_cash_krw": row["baseline_cash_krw"],
            "baseline_asset_qty": row["baseline_asset_qty"],
            "baseline_mark_price": row["baseline_mark_price"],
            "baseline_origin": row["baseline_origin"],
            "baseline_balance_source": row["baseline_balance_source"],
            "created_ts_ms": row["created_ts_ms"],
        }
        missing = [key for key, value in required_columns.items() if value is None]
        if missing:
            details = {
                "evaluation_origin": current_snapshot["evaluation_origin"],
                "current_source": current_snapshot["current_source"],
                "current_balance_source": current_snapshot.get("current_balance_source"),
                "current_balance_observed_ts_ms": current_snapshot.get("current_balance_observed_ts_ms"),
                "current_reconcile_epoch_sec": current_snapshot.get("current_reconcile_epoch_sec"),
                "current_reconcile_reason_code": current_snapshot.get("current_reconcile_reason_code"),
                "local_cash_krw": current_snapshot.get("local_cash_krw"),
                "local_asset_qty": current_snapshot.get("local_asset_qty"),
                "broker_cash_krw": current_snapshot.get("broker_cash_krw"),
                "broker_asset_qty": current_snapshot.get("broker_asset_qty"),
                "mismatch_summary": f"baseline provenance missing columns={','.join(missing)}",
                "baseline": {
                    "day_kst": day_kst,
                    "start_equity": float(row["start_equity"]),
                    "baseline_cash_krw": row["baseline_cash_krw"],
                    "baseline_asset_qty": row["baseline_asset_qty"],
                    "baseline_mark_price": row["baseline_mark_price"],
                    "baseline_origin": row["baseline_origin"],
                    "baseline_balance_source": row["baseline_balance_source"],
                    "baseline_balance_observed_ts_ms": row["baseline_balance_observed_ts_ms"],
                },
            }
            return _mismatch_evaluation(
                evaluation_ts_ms=evaluation_ts_ms,
                day_kst=day_kst,
                max_daily_loss_krw=float(settings.MAX_DAILY_LOSS_KRW),
                mark_price=float(current_snapshot["mark_price"]),
                mark_price_source=str(current_snapshot["mark_price_source"]),
                reason_detail=f"daily baseline is not reproducible ({','.join(missing)})",
                details=details,
            )
    return row


def evaluate_daily_loss_state(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    price: float,
    broker: object | None = None,
    mark_price_source: str = "market_price",
    evaluation_origin: str = "daily_loss_eval",
) -> DailyLossEvaluation:
    _ensure_daily_risk_tables(conn)
    max_daily_loss_krw = float(settings.MAX_DAILY_LOSS_KRW)
    day_kst = _day_kst(ts_ms)
    if max_daily_loss_krw <= 0:
        evaluation = DailyLossEvaluation(
            blocked=False,
            reason="ok",
            reason_code="DISABLED",
            decision="allow",
            evaluation_ts_ms=int(ts_ms),
            day_kst=day_kst,
            max_daily_loss_krw=max_daily_loss_krw,
            start_equity=None,
            current_equity=None,
            loss_today=None,
            current_cash_krw=None,
            current_asset_qty=None,
            mark_price=float(price),
            mark_price_source=str(mark_price_source),
            details={"evaluation_origin": evaluation_origin},
        )
        _record_risk_evaluation(conn, evaluation)
        return evaluation

    current_snapshot = _build_current_snapshot(
        conn,
        broker=broker,
        ts_ms=ts_ms,
        mark_price=float(price),
        mark_price_source=mark_price_source,
        evaluation_origin=evaluation_origin,
    )
    if isinstance(current_snapshot, DailyLossEvaluation):
        _record_risk_evaluation(conn, current_snapshot)
        return current_snapshot

    current_equity = float(current_snapshot["cash_krw"]) + float(current_snapshot["asset_qty"]) * float(price)
    baseline_row = _validated_baseline_or_mismatch(
        conn,
        day_kst=day_kst,
        current_snapshot=current_snapshot,
        current_equity=current_equity,
        evaluation_ts_ms=ts_ms,
    )
    if isinstance(baseline_row, DailyLossEvaluation):
        _record_risk_evaluation(conn, baseline_row)
        return baseline_row

    start_equity = float(baseline_row["start_equity"])
    loss_today = max(0.0, start_equity - current_equity)
    blocked = loss_today >= max_daily_loss_krw
    reason = "ok"
    reason_code = "OK"
    decision = "allow"
    if blocked:
        reason = f"daily loss limit exceeded ({loss_today:,.0f}/{max_daily_loss_krw:,.0f} KRW)"
        reason_code = DAILY_LOSS_LIMIT_REASON_CODE
        decision = "block"

    details = {
        "evaluation_origin": evaluation_origin,
        "current_source": current_snapshot["current_source"],
        "current_balance_source": current_snapshot.get("current_balance_source"),
        "current_balance_observed_ts_ms": current_snapshot.get("current_balance_observed_ts_ms"),
        "current_reconcile_epoch_sec": current_snapshot.get("current_reconcile_epoch_sec"),
        "current_reconcile_reason_code": current_snapshot.get("current_reconcile_reason_code"),
        "local_cash_krw": current_snapshot.get("local_cash_krw"),
        "local_asset_qty": current_snapshot.get("local_asset_qty"),
        "broker_cash_krw": current_snapshot.get("broker_cash_krw"),
        "broker_asset_qty": current_snapshot.get("broker_asset_qty"),
        "mismatch_summary": None,
        "baseline": {
            "day_kst": str(baseline_row["day_kst"]),
            "start_equity": float(baseline_row["start_equity"]),
            "baseline_cash_krw": (
                float(baseline_row["baseline_cash_krw"])
                if baseline_row["baseline_cash_krw"] is not None
                else None
            ),
            "baseline_asset_qty": (
                float(baseline_row["baseline_asset_qty"])
                if baseline_row["baseline_asset_qty"] is not None
                else None
            ),
            "baseline_mark_price": (
                float(baseline_row["baseline_mark_price"])
                if baseline_row["baseline_mark_price"] is not None
                else None
            ),
            "baseline_mark_price_source": baseline_row["baseline_mark_price_source"],
            "baseline_origin": baseline_row["baseline_origin"],
            "baseline_balance_source": baseline_row["baseline_balance_source"],
            "baseline_balance_observed_ts_ms": baseline_row["baseline_balance_observed_ts_ms"],
            "baseline_reconcile_epoch_sec": baseline_row["baseline_reconcile_epoch_sec"],
            "baseline_reconcile_reason_code": baseline_row["baseline_reconcile_reason_code"],
            "created_ts_ms": baseline_row["created_ts_ms"],
        },
    }
    evaluation = DailyLossEvaluation(
        blocked=blocked,
        reason=reason,
        reason_code=reason_code,
        decision=decision,
        evaluation_ts_ms=int(ts_ms),
        day_kst=day_kst,
        max_daily_loss_krw=max_daily_loss_krw,
        start_equity=start_equity,
        current_equity=current_equity,
        loss_today=loss_today,
        current_cash_krw=float(current_snapshot["cash_krw"]),
        current_asset_qty=float(current_snapshot["asset_qty"]),
        mark_price=float(price),
        mark_price_source=str(mark_price_source),
        details=details,
    )
    _record_risk_evaluation(conn, evaluation)
    if blocked:
        RISK_LOG.warning(
            format_log_kv(
                "[RISK] daily_loss_limit",
                day_kst=day_kst,
                evaluation_origin=evaluation_origin,
                reason_code=DAILY_LOSS_LIMIT_REASON_CODE,
                start_equity=start_equity,
                current_equity=current_equity,
                loss_today=loss_today,
                max_daily_loss_krw=max_daily_loss_krw,
                current_source=current_snapshot["current_source"],
                current_balance_source=current_snapshot.get("current_balance_source"),
                current_balance_observed_ts_ms=current_snapshot.get("current_balance_observed_ts_ms"),
            )
        )
    return evaluation


def evaluate_buy_guardrails(
    conn: sqlite3.Connection,
    ts_ms: int,
    cash: float,
    qty: float,
    price: float,
    *,
    broker: object | None = None,
    mark_price_source: str = "market_price",
    evaluation_origin: str = "buy_guardrails",
) -> tuple[bool, str]:
    """
    Returns (blocked, reason)
    - Kill switch
    - Max open position (single-position model)
    - Daily loss limit (optional)
    - Daily order count limit (optional)
    """
    del cash
    if settings.KILL_SWITCH:
        return True, "KILL_SWITCH=ON"

    if settings.MAX_OPEN_POSITIONS <= 1 and qty > POSITION_EPSILON:
        return True, "duplicate entry blocked"

    if settings.MAX_DAILY_ORDER_COUNT > 0:
        today_orders = _count_orders_today(conn, ts_ms)
        if today_orders >= settings.MAX_DAILY_ORDER_COUNT:
            return True, f"daily order count limit exceeded ({today_orders}/{settings.MAX_DAILY_ORDER_COUNT})"

    evaluation = evaluate_daily_loss_state(
        conn,
        ts_ms=ts_ms,
        price=price,
        broker=broker,
        mark_price_source=mark_price_source,
        evaluation_origin=evaluation_origin,
    )
    if evaluation.blocked:
        return True, evaluation.reason

    return False, "ok"


def evaluate_daily_loss_breach(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    cash: float,
    qty: float,
    price: float,
    broker: object | None = None,
    mark_price_source: str = "market_price",
    evaluation_origin: str = "daily_loss_breach",
) -> tuple[bool, str]:
    """Returns whether current portfolio equity already breached the daily loss limit."""
    del cash
    evaluation = evaluate_daily_loss_state(
        conn,
        ts_ms=ts_ms,
        price=price,
        broker=broker,
        mark_price_source=mark_price_source,
        evaluation_origin=evaluation_origin,
    )
    return evaluation.blocked, evaluation.reason


def daily_loss_reason_code_from_reason(reason: str) -> str:
    reason_text = str(reason or "")
    if reason_text.startswith("risk state mismatch"):
        return RISK_STATE_MISMATCH
    if "daily loss limit exceeded" in reason_text:
        return DAILY_LOSS_LIMIT_REASON_CODE
    return "UNKNOWN"


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


def _latest_position_entry_price(conn: sqlite3.Connection) -> float | None:
    row = conn.execute(
        """
        SELECT price
        FROM trades
        WHERE side='BUY'
        ORDER BY ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    v = row["price"] if hasattr(row, "keys") else row[0]
    if v is None:
        return None
    price = float(v)
    if not price > 0:
        return None
    return price


def evaluate_position_loss_breach(
    conn: sqlite3.Connection,
    *,
    qty: float,
    price: float,
) -> tuple[bool, str]:
    if settings.MAX_POSITION_LOSS_PCT <= 0:
        return False, "ok"
    if float(qty) <= POSITION_EPSILON:
        return False, "ok"
    if float(price) <= 0:
        return False, "ok"

    entry_price = _latest_position_entry_price(conn)
    if entry_price is None:
        return False, "ok"

    loss_pct = max(0.0, ((entry_price - float(price)) / entry_price) * 100.0)
    threshold_pct = float(settings.MAX_POSITION_LOSS_PCT)
    if loss_pct >= threshold_pct:
        return (
            True,
            "position loss threshold breached "
            f"({loss_pct:.2f}%/{threshold_pct:.2f}%, entry={entry_price:,.0f}, mark={float(price):,.0f})",
        )

    return False, "ok"


def evaluate_order_submission_halt(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    now_ms: int,
    cash: float,
    qty: float,
    price: float,
    broker: object | None = None,
    mark_price_source: str = "market_price",
    evaluation_origin: str = "submission_halt",
) -> tuple[bool, str]:
    """Shared hard-stop checks before placing any new order."""
    del cash
    if settings.KILL_SWITCH:
        return True, "KILL_SWITCH=ON"

    blocked, reason = evaluate_daily_loss_breach(
        conn,
        ts_ms=ts_ms,
        cash=0.0,
        qty=0.0,
        price=price,
        broker=broker,
        mark_price_source=mark_price_source,
        evaluation_origin=evaluation_origin,
    )
    if blocked:
        return True, reason

    blocked, reason = evaluate_position_loss_breach(
        conn,
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


def fetch_daily_risk_baseline(conn: sqlite3.Connection, *, day_kst: str | None = None) -> dict[str, Any] | None:
    _ensure_daily_risk_tables(conn)
    effective_day = str(day_kst or _day_kst(int(time.time() * 1000)))
    row = _load_daily_risk_row(conn, effective_day)
    if row is None:
        return None
    payload = dict(row)
    raw_context = payload.get("baseline_context")
    if raw_context:
        try:
            payload["baseline_context"] = json.loads(str(raw_context))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload["baseline_context"] = raw_context
    return payload


def fetch_recent_risk_evaluations(conn: sqlite3.Connection, *, limit: int = 20) -> list[dict[str, Any]]:
    _ensure_daily_risk_tables(conn)
    rows = conn.execute(
        """
        SELECT
            evaluation_ts_ms,
            day_kst,
            evaluation_origin,
            reason_code,
            decision,
            max_daily_loss_krw,
            start_equity,
            current_equity,
            loss_today,
            current_cash_krw,
            current_asset_qty,
            mark_price,
            mark_price_source,
            baseline_cash_krw,
            baseline_asset_qty,
            baseline_mark_price,
            baseline_origin,
            baseline_balance_source,
            baseline_balance_observed_ts_ms,
            current_source,
            current_balance_source,
            current_balance_observed_ts_ms,
            current_reconcile_epoch_sec,
            current_reconcile_reason_code,
            local_cash_krw,
            local_asset_qty,
            broker_cash_krw,
            broker_asset_qty,
            mismatch_summary,
            details_json
        FROM risk_evaluations
        ORDER BY evaluation_ts_ms DESC, id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        raw_details = payload.get("details_json")
        if raw_details:
            try:
                payload["details"] = json.loads(str(raw_details))
            except (TypeError, ValueError, json.JSONDecodeError):
                payload["details"] = raw_details
        payload.pop("details_json", None)
        results.append(payload)
    return results
