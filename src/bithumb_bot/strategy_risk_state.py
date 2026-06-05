from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .canonical_decision import canonical_payload_hash
from .oms import collect_risky_order_state
from .risk import (
    daily_loss_reason_code_from_reason,
    evaluate_daily_loss_state,
)
from .risk_contract import RiskPolicy, RiskSnapshot
from .runtime_risk_engine import _classify_unresolved_state


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {
        str(row["name"]) if hasattr(row, "keys") else str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }


def _strategy_decision_ids_for_instance(
    conn: sqlite3.Connection,
    *,
    strategy_instance_id: str,
    pair: str,
    interval: str,
    as_of_ts_ms: int,
) -> tuple[int, ...] | None:
    if not _table_exists(conn, "strategy_decisions"):
        return None
    columns = _table_columns(conn, "strategy_decisions")
    if not {"id", "context_json"}.issubset(columns):
        return None
    rows = conn.execute(
        """
        SELECT id, context_json
        FROM strategy_decisions
        WHERE decision_ts <= ?
        ORDER BY id
        """,
        (int(as_of_ts_ms),),
    ).fetchall()
    ids: list[int] = []
    for row in rows:
        raw_context = row["context_json"] if hasattr(row, "keys") else row[1]
        try:
            context = json.loads(str(raw_context or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(context, dict):
            continue
        if str(context.get("strategy_instance_id") or "").strip() != str(strategy_instance_id):
            continue
        context_pair = str(context.get("pair") or context.get("market") or "").strip()
        context_interval = str(context.get("interval") or "").strip()
        if context_pair and context_pair != str(pair):
            continue
        if context_interval and context_interval != str(interval):
            continue
        ids.append(int(row["id"] if hasattr(row, "keys") else row[0]))
    return tuple(ids)


def _placeholders(values: tuple[int, ...]) -> str:
    return ",".join("?" for _ in values)


def _count_orders_today(
    conn: sqlite3.Connection,
    ts_ms: int,
    *,
    pair: str,
    strategy_decision_ids: tuple[int, ...] | None,
) -> int | None:
    if strategy_decision_ids is None or not _table_exists(conn, "orders"):
        return None
    columns = _table_columns(conn, "orders")
    if not {"created_ts", "entry_decision_id", "exit_decision_id"}.issubset(columns):
        return None
    day_start_ms = int(ts_ms) - (int(ts_ms) % 86_400_000)
    params: list[object] = [day_start_ms, int(ts_ms)]
    if "pair" in columns:
        pair_clause = "AND COALESCE(pair, '') = COALESCE(?, COALESCE(pair, ''))"
        params.append(str(pair))
    else:
        pair_clause = ""
    if not strategy_decision_ids:
        return 0
    placeholders = _placeholders(strategy_decision_ids)
    params.extend(strategy_decision_ids)
    params.extend(strategy_decision_ids)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM orders
        WHERE created_ts >= ? AND created_ts <= ?
          {pair_clause}
          AND (
            entry_decision_id IN ({placeholders})
            OR exit_decision_id IN ({placeholders})
          )
        """,
        tuple(params),
    ).fetchone()
    return int(row["count"] if hasattr(row, "keys") else row[0])


def _count_trades_today(
    conn: sqlite3.Connection,
    ts_ms: int,
    *,
    pair: str,
    strategy_decision_ids: tuple[int, ...] | None,
) -> int | None:
    if strategy_decision_ids is None:
        return None
    if not _table_exists(conn, "trades"):
        return None
    columns = _table_columns(conn, "trades")
    if not {"ts", "pair", "entry_decision_id", "exit_decision_id"}.issubset(columns):
        return None
    day_start_ms = int(ts_ms) - (int(ts_ms) % 86_400_000)
    if not strategy_decision_ids:
        return 0
    placeholders = _placeholders(strategy_decision_ids)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM trades
        WHERE ts >= ? AND ts <= ? AND COALESCE(pair, '') = COALESCE(?, COALESCE(pair, ''))
          AND (
            entry_decision_id IN ({placeholders})
            OR exit_decision_id IN ({placeholders})
          )
        """,
        (day_start_ms, int(ts_ms), str(pair), *strategy_decision_ids, *strategy_decision_ids),
    ).fetchone()
    return int(row["count"] if hasattr(row, "keys") else row[0])


def _portfolio_asset_qty(conn: sqlite3.Connection) -> float | None:
    if not _table_exists(conn, "portfolio"):
        return None
    row = conn.execute(
        "SELECT asset_available, asset_locked, asset_qty FROM portfolio ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    getter = row.__getitem__
    available = getter("asset_available") if hasattr(row, "keys") else row[0]
    locked = getter("asset_locked") if hasattr(row, "keys") else row[1]
    qty = getter("asset_qty") if hasattr(row, "keys") else row[2]
    if available is not None or locked is not None:
        return float(available or 0.0) + float(locked or 0.0)
    if qty is not None:
        return float(qty or 0.0)
    return None


def _open_exposure_qty(
    conn: sqlite3.Connection,
    *,
    pair: str,
    strategy_decision_ids: tuple[int, ...] | None,
) -> float | None:
    if strategy_decision_ids is None:
        return None
    if not _table_exists(conn, "open_position_lots"):
        return None
    columns = _table_columns(conn, "open_position_lots")
    if not {"pair", "position_state", "qty_open", "entry_decision_id"}.issubset(columns):
        return None
    if not strategy_decision_ids:
        return 0.0
    placeholders = _placeholders(strategy_decision_ids)
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(qty_open), 0.0) AS qty
        FROM open_position_lots
        WHERE pair=? AND position_state='open_exposure'
          AND entry_decision_id IN ({placeholders})
        """,
        (str(pair), *strategy_decision_ids),
    ).fetchone()
    return float(row["qty"] if hasattr(row, "keys") else row[0])


def _position_entry_price_for_instance(
    conn: sqlite3.Connection,
    *,
    pair: str,
    strategy_decision_ids: tuple[int, ...] | None,
) -> float | None:
    if strategy_decision_ids is None:
        return None
    if not _table_exists(conn, "open_position_lots"):
        return None
    columns = _table_columns(conn, "open_position_lots")
    if not {"pair", "position_state", "qty_open", "entry_decision_id"}.issubset(columns):
        return None
    if "entry_price" not in columns:
        return None
    if not strategy_decision_ids:
        return None
    placeholders = _placeholders(strategy_decision_ids)
    row = conn.execute(
        f"""
        SELECT
            COALESCE(SUM(qty_open * entry_price), 0.0) AS weighted_entry,
            COALESCE(SUM(qty_open), 0.0) AS qty
        FROM open_position_lots
        WHERE pair=? AND position_state='open_exposure'
          AND entry_decision_id IN ({placeholders})
          AND entry_price IS NOT NULL
          AND entry_price > 0
        """,
        (str(pair), *strategy_decision_ids),
    ).fetchone()
    weighted = float(row["weighted_entry"] if hasattr(row, "keys") else row[0])
    qty = float(row["qty"] if hasattr(row, "keys") else row[1])
    if qty <= 0.0:
        return None
    return weighted / qty


def _loss_today_for_instance(
    conn: sqlite3.Connection,
    ts_ms: int,
    *,
    pair: str,
    strategy_instance_id: str,
) -> float | None:
    if not _table_exists(conn, "trade_lifecycles"):
        return None
    columns = _table_columns(conn, "trade_lifecycles")
    if not {"exit_ts", "pair", "strategy_instance_id", "net_pnl"}.issubset(columns):
        return None
    day_start_ms = int(ts_ms) - (int(ts_ms) % 86_400_000)
    row = conn.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN net_pnl < 0 THEN -net_pnl ELSE 0 END), 0.0) AS loss
        FROM trade_lifecycles
        WHERE exit_ts >= ? AND exit_ts <= ?
          AND pair=?
          AND strategy_instance_id=?
        """,
        (day_start_ms, int(ts_ms), str(pair), str(strategy_instance_id)),
    ).fetchone()
    return float(row["loss"] if hasattr(row, "keys") else row[0])


def _minutes_since_last_loss_for_instance(
    conn: sqlite3.Connection,
    ts_ms: int,
    *,
    pair: str,
    strategy_instance_id: str,
) -> float | None:
    if not _table_exists(conn, "trade_lifecycles"):
        return None
    columns = _table_columns(conn, "trade_lifecycles")
    if not {"exit_ts", "pair", "strategy_instance_id", "net_pnl"}.issubset(columns):
        return None
    row = conn.execute(
        """
        SELECT MAX(exit_ts) AS last_loss_ts
        FROM trade_lifecycles
        WHERE exit_ts <= ?
          AND pair=?
          AND strategy_instance_id=?
          AND net_pnl < 0
        """,
        (int(ts_ms), str(pair), str(strategy_instance_id)),
    ).fetchone()
    last_loss_ts = row["last_loss_ts"] if hasattr(row, "keys") else row[0]
    if last_loss_ts is None:
        return None
    return max(0.0, (int(ts_ms) - int(last_loss_ts)) / 60_000.0)


def _current_drawdown_pct_for_instance(
    conn: sqlite3.Connection,
    ts_ms: int,
    *,
    pair: str,
    strategy_instance_id: str,
) -> float | None:
    if not _table_exists(conn, "trade_lifecycles"):
        return None
    columns = _table_columns(conn, "trade_lifecycles")
    if not {"exit_ts", "pair", "strategy_instance_id", "net_pnl"}.issubset(columns):
        return None
    rows = conn.execute(
        """
        SELECT exit_ts, net_pnl
        FROM trade_lifecycles
        WHERE exit_ts <= ?
          AND pair=?
          AND strategy_instance_id=?
        ORDER BY exit_ts, id
        """,
        (int(ts_ms), str(pair), str(strategy_instance_id)),
    ).fetchall()
    if not rows:
        return 0.0
    equity = 0.0
    peak = 0.0
    max_drawdown_abs = 0.0
    for row in rows:
        pnl = float(row["net_pnl"] if hasattr(row, "keys") else row[1] or 0.0)
        equity += pnl
        peak = max(peak, equity)
        max_drawdown_abs = max(max_drawdown_abs, peak - equity)
    if peak <= 0.0:
        return 100.0 if max_drawdown_abs > 0.0 else 0.0
    return max(0.0, (max_drawdown_abs / peak) * 100.0)


def _missing_required_state(policy: RiskPolicy, snapshot: RiskSnapshot) -> tuple[str, ...]:
    missing: list[str] = []
    if float(policy.max_daily_loss_krw) > 0.0 and snapshot.loss_today is None:
        missing.append("loss_today")
    if float(policy.max_position_loss_pct) > 0.0 and (
        snapshot.current_asset_qty is None or snapshot.position_entry_price is None
    ):
        missing.append("position_loss_state")
    if int(policy.max_daily_order_count) > 0 and snapshot.daily_order_count is None:
        missing.append("daily_order_count")
    if int(policy.max_trade_count_per_day) > 0 and snapshot.daily_trade_count is None:
        missing.append("daily_trade_count")
    if float(policy.max_drawdown_pct) > 0.0 and snapshot.current_drawdown_pct is None:
        missing.append("current_drawdown_pct")
    if int(policy.cooldown_after_loss_min) > 0 and snapshot.minutes_since_last_loss is None:
        missing.append("minutes_since_last_loss")
    return tuple(dict.fromkeys(missing))


@dataclass(frozen=True)
class StrategyRiskStateProvider:
    conn: sqlite3.Connection
    max_open_order_age_sec: int = 300

    def snapshot(
        self,
        *,
        strategy_instance_id: str,
        strategy_name: str,
        pair: str,
        interval: str,
        as_of_ts_ms: int,
        mark_price: float,
        policy: RiskPolicy | None = None,
        broker: object | None = None,
        mark_price_source: str = "market_price",
        enforced: bool = False,
    ) -> RiskSnapshot:
        daily = evaluate_daily_loss_state(
            self.conn,
            ts_ms=int(as_of_ts_ms),
            price=float(mark_price),
            broker=broker,
            mark_price_source=mark_price_source,
            evaluation_origin="strategy_risk_state_provider",
        )
        mismatch = daily.reason_code == "RISK_STATE_MISMATCH" or daily_loss_reason_code_from_reason(
            daily.reason
        ) == "RISK_STATE_MISMATCH"
        strategy_decision_ids = _strategy_decision_ids_for_instance(
            self.conn,
            strategy_instance_id=strategy_instance_id,
            pair=pair,
            interval=interval,
            as_of_ts_ms=int(as_of_ts_ms),
        )
        unresolved_state: dict[str, Any] = dict(
            collect_risky_order_state(
                self.conn,
                now_ms=int(as_of_ts_ms),
                max_open_order_age_sec=int(self.max_open_order_age_sec),
            )
        )
        unresolved_blocked, unresolved_reason_code, unresolved_reason = _classify_unresolved_state(
            unresolved_state,
            max_open_order_age_sec=int(self.max_open_order_age_sec),
        )
        asset_qty = _open_exposure_qty(
            self.conn,
            pair=pair,
            strategy_decision_ids=strategy_decision_ids,
        )
        loss_today = _loss_today_for_instance(
            self.conn,
            int(as_of_ts_ms),
            pair=pair,
            strategy_instance_id=strategy_instance_id,
        )
        daily_order_count = _count_orders_today(
            self.conn,
            int(as_of_ts_ms),
            pair=pair,
            strategy_decision_ids=strategy_decision_ids,
        )
        daily_trade_count = _count_trades_today(
            self.conn,
            int(as_of_ts_ms),
            pair=pair,
            strategy_decision_ids=strategy_decision_ids,
        )
        minutes_since_last_loss = _minutes_since_last_loss_for_instance(
            self.conn,
            int(as_of_ts_ms),
            pair=pair,
            strategy_instance_id=strategy_instance_id,
        )
        position_entry_price = _position_entry_price_for_instance(
            self.conn,
            pair=pair,
            strategy_decision_ids=strategy_decision_ids,
        )
        current_drawdown_pct = _current_drawdown_pct_for_instance(
            self.conn,
            int(as_of_ts_ms),
            pair=pair,
            strategy_instance_id=strategy_instance_id,
        )
        evidence = {
            "strategy_instance_id": str(strategy_instance_id),
            "strategy_name": str(strategy_name),
            "pair": str(pair),
            "interval": str(interval),
            "as_of_ts_ms": int(as_of_ts_ms),
            "mark_price": float(mark_price),
            "mark_price_source": str(mark_price_source),
            "scope": "strategy_instance",
            "strategy_instance_scope": {
                "source_table": "strategy_decisions",
                "source_columns": ["id", "context_json", "decision_ts"],
                "filters": {
                    "strategy_instance_id": str(strategy_instance_id),
                    "pair": str(pair),
                    "interval": str(interval),
                    "decision_ts_lte": int(as_of_ts_ms),
                },
                "decision_ids": (
                    None if strategy_decision_ids is None else list(strategy_decision_ids)
                ),
                "missing_state_behavior": "fail_closed" if enforced else "telemetry",
            },
            "state_tables": {
                "portfolio": _table_exists(self.conn, "portfolio"),
                "orders": _table_exists(self.conn, "orders"),
                "trades": _table_exists(self.conn, "trades"),
                "open_position_lots": _table_exists(self.conn, "open_position_lots"),
                "trade_lifecycles": _table_exists(self.conn, "trade_lifecycles"),
                "strategy_decisions": _table_exists(self.conn, "strategy_decisions"),
            },
            "state_derivation": {
                "loss_today": {
                    "scope": "strategy_instance",
                    "table": "trade_lifecycles",
                    "columns": ["exit_ts", "pair", "strategy_instance_id", "net_pnl"],
                    "filters": {
                        "strategy_instance_id": str(strategy_instance_id),
                        "pair": str(pair),
                        "day_start_lte_exit_ts_lte_as_of": True,
                    },
                    "value_available": loss_today is not None,
                },
                "daily_order_count": {
                    "scope": "strategy_instance",
                    "table": "orders",
                    "columns": ["created_ts", "entry_decision_id", "exit_decision_id"],
                    "filters": {
                        "entry_or_exit_decision_id_in_strategy_decision_ids": True,
                        "day_start_lte_created_ts_lte_as_of": True,
                    },
                    "value_available": daily_order_count is not None,
                },
                "daily_trade_count": {
                    "scope": "strategy_instance",
                    "table": "trades",
                    "columns": ["ts", "pair", "entry_decision_id", "exit_decision_id"],
                    "filters": {
                        "pair": str(pair),
                        "entry_or_exit_decision_id_in_strategy_decision_ids": True,
                        "day_start_lte_ts_lte_as_of": True,
                    },
                    "value_available": daily_trade_count is not None,
                },
                "current_asset_qty": {
                    "scope": "strategy_instance",
                    "table": "open_position_lots",
                    "columns": ["pair", "position_state", "qty_open", "entry_decision_id"],
                    "filters": {
                        "pair": str(pair),
                        "position_state": "open_exposure",
                        "entry_decision_id_in_strategy_decision_ids": True,
                    },
                    "value_available": asset_qty is not None,
                },
                "position_entry_price": {
                    "scope": "strategy_instance",
                    "table": "open_position_lots",
                    "columns": ["pair", "position_state", "qty_open", "entry_price", "entry_decision_id"],
                    "filters": {
                        "pair": str(pair),
                        "position_state": "open_exposure",
                        "entry_decision_id_in_strategy_decision_ids": True,
                    },
                    "value_available": position_entry_price is not None,
                },
                "current_drawdown_pct": {
                    "scope": "strategy_instance",
                    "table": "trade_lifecycles",
                    "columns": ["exit_ts", "pair", "strategy_instance_id", "net_pnl"],
                    "filters": {
                        "strategy_instance_id": str(strategy_instance_id),
                        "pair": str(pair),
                        "exit_ts_lte_as_of": True,
                    },
                    "value_available": current_drawdown_pct is not None,
                },
                "minutes_since_last_loss": {
                    "scope": "strategy_instance",
                    "table": "trade_lifecycles",
                    "columns": ["exit_ts", "pair", "strategy_instance_id", "net_pnl"],
                    "filters": {
                        "strategy_instance_id": str(strategy_instance_id),
                        "pair": str(pair),
                        "net_pnl_lt_zero": True,
                        "exit_ts_lte_as_of": True,
                    },
                    "value_available": minutes_since_last_loss is not None,
                },
                "unresolved_order_evidence": {
                    "scope": "account_global",
                    "source": "oms.collect_risky_order_state",
                    "global_scope_reason": "unresolved submit/accounting/recovery orders are account-level safety gates",
                },
            },
            "daily_loss_evaluation": {
                "reason_code": daily.reason_code,
                "decision": daily.decision,
                "day_kst": daily.day_kst,
                "mark_price_source": daily.mark_price_source,
                "scope": "account_global_observability",
            },
            "unresolved_order_gate": {
                "blocked": bool(unresolved_blocked),
                "reason_code": str(unresolved_reason_code),
                "reason": str(unresolved_reason),
                "state": unresolved_state,
                "evaluated_once": True,
            },
        }
        snapshot = RiskSnapshot(
            evaluation_ts_ms=int(as_of_ts_ms),
            mark_price=float(mark_price),
            current_equity=daily.current_equity,
            baseline_equity=daily.start_equity,
            loss_today=loss_today,
            current_cash_krw=daily.current_cash_krw,
            current_asset_qty=asset_qty,
            position_entry_price=position_entry_price,
            broker_local_mismatch=bool(mismatch),
            recovery_risk_mismatch_reason=daily.reason if mismatch else None,
            duplicate_entry=bool(asset_qty is not None and float(asset_qty) > 1e-12),
            daily_order_count=daily_order_count,
            daily_trade_count=daily_trade_count,
            current_drawdown_pct=current_drawdown_pct,
            minutes_since_last_loss=minutes_since_last_loss,
            unresolved_order_blocked=bool(unresolved_blocked),
            unresolved_order_reason_code=str(unresolved_reason_code),
            unresolved_order_reason=str(unresolved_reason),
            state_source="runtime_db_strategy_instance_ledger",
            evidence=evidence,
        )
        missing = _missing_required_state(policy, snapshot) if policy is not None else ()
        if missing:
            evidence = {
                **evidence,
                "missing_required_risk_state": list(missing),
                "missing_required_risk_state_behavior": (
                    "fail_closed" if enforced else "telemetry"
                ),
            }
            snapshot = RiskSnapshot(
                **{**snapshot.as_dict(), "evidence": evidence}  # type: ignore[arg-type]
            )
        evidence = {
            **dict(snapshot.evidence),
            "risk_state_evidence_hash": canonical_payload_hash(snapshot.evidence),
        }
        return RiskSnapshot(**{**snapshot.as_dict(), "evidence": evidence})  # type: ignore[arg-type]


def missing_required_risk_state(policy: RiskPolicy, snapshot: RiskSnapshot) -> tuple[str, ...]:
    return _missing_required_state(policy, snapshot)
