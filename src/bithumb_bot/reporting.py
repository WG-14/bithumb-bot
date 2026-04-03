from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from statistics import median
from typing import Any

from .analytics_context import _load_context_json, normalize_analysis_context_from_lifecycle_row
from .config import PATH_MANAGER, settings
from .broker.order_rules import get_effective_order_rules, rule_source_for
from .db_core import ensure_db
from .markets import canonical_market_with_raw
from .storage_io import write_json_atomic
from .utils_time import kst_str, parse_interval_sec
from .broker.bithumb import BithumbBroker


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
    realized_gross_pnl: float
    realized_net_pnl: float
    expectancy_per_trade: float
    fee_total: float
    holding_time_avg_sec: float | None
    holding_time_min_sec: float | None
    holding_time_max_sec: float | None
    entry_reason_linked_count: int
    exit_reason_linked_count: int
    entry_reason_sample: str | None
    exit_reason_sample: str | None


@dataclass
class LifecycleCloseStat:
    entry_rule_name: str
    exit_rule_name: str
    exit_reason_bucket: str
    trade_count: int
    win_rate: float
    realized_net_pnl: float
    avg_hold_time_sec: float | None


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


@dataclass
class DecisionTelemetrySummary:
    decision_type: str
    strategy_name: str
    pair: str
    interval: str
    block_reason: str
    count: int


@dataclass
class FilterObservationSummary:
    observation_window_bars: int
    observed_count: int
    insufficient_sample: bool
    sample_threshold: int
    avg_return_bps: float | None
    median_return_bps: float | None
    avoided_loss_count: int
    opportunity_missed_count: int
    flat_or_unknown_count: int


@dataclass
class FilterEffectivenessSummary:
    total_entry_candidates: int
    executed_entry_count: int
    blocked_entry_count: int
    hold_decision_count: int
    blocked_by_filter: dict[str, int]
    multi_filter_blocked_count: int
    observation: FilterObservationSummary
    notes: list[str]


@dataclass
class ExperimentBucketStat:
    bucket: str
    trade_count: int
    trade_count_share: float
    win_rate: float
    realized_net_pnl: float
    realized_net_pnl_share: float
    absolute_pnl_concentration: float
    profitable_pnl_concentration: float
    loss_pnl_concentration: float
    expectancy_per_trade: float


@dataclass
class ExperimentReportSummary:
    realized_net_pnl: float
    trade_count: int
    win_rate: float
    expectancy_per_trade: float
    max_drawdown: float
    top_n_concentration: float
    top_n: int
    longest_losing_streak: int
    sample_threshold: int
    sample_insufficient: bool
    regime_skew_ratio: float
    regime_pnl_skew_ratio: float
    warnings: list[str]
    time_bucket_rows: list[ExperimentBucketStat]
    regime_bucket_rows: list[ExperimentBucketStat]


@dataclass
class AttributionQualitySummary:
    total_trade_count: int
    unattributed_trade_count: int
    ambiguous_linkage_count: int
    recovery_derived_attribution_count: int
    unattributed_trade_ratio: float
    ambiguous_linkage_ratio: float
    recovery_derived_attribution_ratio: float
    reason_buckets: dict[str, int]
    warnings: list[str]


@dataclass
class RecoveryAttributionSignalSummary:
    recent_recovery_derived_trade_count: int
    unresolved_attribution_count: int
    ambiguous_linkage_after_recent_reconcile: bool | None
    last_reconcile_epoch_sec: float | None


def fetch_recovery_attribution_signal_summary(
    conn: sqlite3.Connection,
    *,
    strategy_name: str | None = None,
    pair: str | None = None,
    last_reconcile_epoch_sec: float | None = None,
) -> RecoveryAttributionSignalSummary:
    if last_reconcile_epoch_sec is None:
        row = conn.execute(
            "SELECT last_reconcile_epoch_sec FROM bot_health WHERE id=1"
        ).fetchone()
        if row is not None and row["last_reconcile_epoch_sec"] is not None:
            last_reconcile_epoch_sec = float(row["last_reconcile_epoch_sec"])

    filters: list[str] = []
    params: list[object] = []
    if strategy_name:
        filters.append("COALESCE(tl.strategy_name, '<unknown>') = ?")
        params.append(str(strategy_name))
    if pair:
        filters.append("COALESCE(tl.pair, '<unknown>') = ?")
        params.append(str(pair))

    unresolved_where = ""
    if filters:
        unresolved_where = f"WHERE {' AND '.join(filters)}"

    unresolved_row = conn.execute(
        f"""
        SELECT
            COALESCE(
                SUM(
                    CASE
                        WHEN tl.entry_decision_id IS NULL
                             OR COALESCE(tl.entry_decision_linkage, '') = 'ambiguous_multi_candidate'
                             OR COALESCE(tl.entry_decision_linkage, '') LIKE 'degraded_recovery_%'
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS unresolved_attribution_count
        FROM trade_lifecycles tl
        {unresolved_where}
        """,
        tuple(params),
    ).fetchone()
    unresolved_attribution_count = int(unresolved_row["unresolved_attribution_count"] or 0) if unresolved_row else 0

    recent_recovery_derived_trade_count = 0
    ambiguous_after_recent_reconcile: bool | None = None
    if last_reconcile_epoch_sec is not None:
        recent_cutoff_ts_ms = int(float(last_reconcile_epoch_sec) * 1000)
        recent_filters = list(filters)
        recent_params = [*params, recent_cutoff_ts_ms]
        recent_filters.append("tl.exit_ts >= ?")
        recent_where = f"WHERE {' AND '.join(recent_filters)}"
        recent_row = conn.execute(
            f"""
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(tl.entry_decision_linkage, '') LIKE 'degraded_recovery_%'
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS recent_recovery_derived_trade_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(tl.entry_decision_linkage, '') = 'ambiguous_multi_candidate'
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                ) AS recent_ambiguous_linkage_count
            FROM trade_lifecycles tl
            {recent_where}
            """,
            tuple(recent_params),
        ).fetchone()
        recent_recovery_derived_trade_count = (
            int(recent_row["recent_recovery_derived_trade_count"] or 0) if recent_row else 0
        )
        ambiguous_after_recent_reconcile = bool(int(recent_row["recent_ambiguous_linkage_count"] or 0)) if recent_row else False

    return RecoveryAttributionSignalSummary(
        recent_recovery_derived_trade_count=recent_recovery_derived_trade_count,
        unresolved_attribution_count=unresolved_attribution_count,
        ambiguous_linkage_after_recent_reconcile=ambiguous_after_recent_reconcile,
        last_reconcile_epoch_sec=last_reconcile_epoch_sec,
    )


def fetch_attribution_quality_summary(
    conn: sqlite3.Connection,
    *,
    strategy_name: str | None = None,
    pair: str | None = None,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
) -> AttributionQualitySummary:
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

    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_trade_count,
            COALESCE(SUM(CASE WHEN tl.entry_decision_id IS NULL THEN 1 ELSE 0 END), 0) AS unattributed_trade_count,
            COALESCE(
                SUM(
                    CASE
                        WHEN COALESCE(tl.entry_decision_linkage, '') = 'ambiguous_multi_candidate' THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS ambiguous_linkage_count,
            COALESCE(
                SUM(
                    CASE
                        WHEN COALESCE(tl.entry_decision_linkage, '') LIKE 'degraded_recovery_%' THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS recovery_derived_attribution_count,
            COALESCE(
                SUM(
                    CASE
                        WHEN tl.entry_decision_id IS NULL
                             AND COALESCE(tl.entry_decision_linkage, '') IN (
                                 'unattributed',
                                 'unattributed_missing_strategy',
                                 'unattributed_no_strict_match',
                                 'unattributed_unknown_entry'
                             )
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS reason_missing_decision_id,
            COALESCE(
                SUM(
                    CASE
                        WHEN COALESCE(tl.entry_decision_linkage, '') = 'ambiguous_multi_candidate' THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS reason_multiple_candidate_decisions,
            COALESCE(
                SUM(
                    CASE
                        WHEN tl.entry_decision_id IS NULL
                             AND TRIM(COALESCE(tl.entry_decision_linkage, '')) = ''
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS reason_legacy_incomplete_row,
            COALESCE(
                SUM(
                    CASE
                        WHEN COALESCE(tl.entry_decision_linkage, '') LIKE 'degraded_recovery_%' THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS reason_recovery_unresolved_linkage
        FROM trade_lifecycles tl
        {where_clause}
        """,
        tuple(params),
    ).fetchone()
    total_trade_count = int(row["total_trade_count"] or 0) if row is not None else 0
    unattributed_trade_count = int(row["unattributed_trade_count"] or 0) if row is not None else 0
    ambiguous_linkage_count = int(row["ambiguous_linkage_count"] or 0) if row is not None else 0
    recovery_derived_count = int(row["recovery_derived_attribution_count"] or 0) if row is not None else 0
    denominator = total_trade_count if total_trade_count > 0 else 1
    reason_buckets = {
        "missing_decision_id": int(row["reason_missing_decision_id"] or 0) if row is not None else 0,
        "multiple_candidate_decisions": (
            int(row["reason_multiple_candidate_decisions"] or 0) if row is not None else 0
        ),
        "legacy_incomplete_row": int(row["reason_legacy_incomplete_row"] or 0) if row is not None else 0,
        "recovery_unresolved_linkage": (
            int(row["reason_recovery_unresolved_linkage"] or 0) if row is not None else 0
        ),
    }
    warnings: list[str] = []
    if total_trade_count <= 0:
        warnings.append("no trade_lifecycles rows matched the filter window; attribution quality unavailable.")
    if unattributed_trade_count > 0:
        warnings.append(
            f"unattributed trades present: {unattributed_trade_count}/{total_trade_count} "
            f"({(unattributed_trade_count / denominator):.2%})."
        )
    if ambiguous_linkage_count > 0:
        warnings.append(
            f"ambiguous decision linkage present: {ambiguous_linkage_count}/{total_trade_count} "
            f"({(ambiguous_linkage_count / denominator):.2%})."
        )
    if recovery_derived_count > 0:
        warnings.append(
            "recovery-derived attribution present: "
            f"{recovery_derived_count}/{total_trade_count} ({(recovery_derived_count / denominator):.2%})."
        )
    return AttributionQualitySummary(
        total_trade_count=total_trade_count,
        unattributed_trade_count=unattributed_trade_count,
        ambiguous_linkage_count=ambiguous_linkage_count,
        recovery_derived_attribution_count=recovery_derived_count,
        unattributed_trade_ratio=unattributed_trade_count / denominator,
        ambiguous_linkage_ratio=ambiguous_linkage_count / denominator,
        recovery_derived_attribution_ratio=recovery_derived_count / denominator,
        reason_buckets=reason_buckets,
        warnings=warnings,
    )


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


def fetch_decision_telemetry_summary(
    conn: sqlite3.Connection,
    *,
    limit: int = 200,
) -> list[DecisionTelemetrySummary]:
    rows = conn.execute(
        """
        SELECT
            COALESCE(json_extract(context_json, '$.decision_type'), signal) AS decision_type,
            COALESCE(json_extract(context_json, '$.strategy_name'), strategy_name, '<unknown>') AS strategy_name,
            COALESCE(json_extract(context_json, '$.pair'), '<unknown>') AS pair,
            COALESCE(json_extract(context_json, '$.interval'), '<unknown>') AS interval,
            COALESCE(
                json_extract(context_json, '$.block_reason'),
                json_extract(context_json, '$.entry_reason'),
                reason
            ) AS block_reason,
            COUNT(*) AS decision_count
        FROM (
            SELECT *
            FROM strategy_decisions
            ORDER BY decision_ts DESC, id DESC
            LIMIT ?
        ) recent
        GROUP BY decision_type, strategy_name, pair, interval, block_reason
        ORDER BY decision_count DESC, decision_type ASC, strategy_name ASC, pair ASC, interval ASC
        """,
        (int(max(1, limit)),),
    ).fetchall()
    return [
        DecisionTelemetrySummary(
            decision_type=str(row["decision_type"]),
            strategy_name=str(row["strategy_name"]),
            pair=str(row["pair"]),
            interval=str(row["interval"]),
            block_reason=str(row["block_reason"]),
            count=int(row["decision_count"] or 0),
        )
        for row in rows
    ]


def _extract_blocked_filters(context_json: str | None) -> list[str]:
    context = _load_context_json(context_json)
    raw_filters = context.get("blocked_filters")
    if not isinstance(raw_filters, list):
        return []
    normalized: list[str] = []
    for item in raw_filters:
        text = str(item).strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _extract_decision_type(context_json: str | None, fallback_signal: str) -> str:
    context = _load_context_json(context_json)
    decision_type = str(context.get("decision_type") or "").strip()
    if decision_type:
        return decision_type
    return str(fallback_signal or "").strip().upper() or "UNKNOWN"


def fetch_filter_effectiveness_summary(
    conn: sqlite3.Connection,
    *,
    strategy_name: str | None = None,
    pair: str | None = None,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    observation_window_bars: int = 5,
    min_observation_sample: int = 10,
) -> FilterEffectivenessSummary:
    filters: list[str] = []
    params: list[object] = []
    if strategy_name:
        filters.append("COALESCE(sd.strategy_name, '<unknown>') = ?")
        params.append(str(strategy_name))
    if pair:
        filters.append("COALESCE(json_extract(sd.context_json, '$.pair'), '<unknown>') = ?")
        params.append(str(pair))
    if from_ts_ms is not None:
        filters.append("sd.decision_ts >= ?")
        params.append(int(from_ts_ms))
    if to_ts_ms is not None:
        filters.append("sd.decision_ts <= ?")
        params.append(int(to_ts_ms))

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    decision_rows = conn.execute(
        f"""
        SELECT
            sd.id,
            sd.decision_ts,
            sd.signal,
            sd.strategy_name,
            sd.candle_ts,
            sd.market_price,
            sd.context_json
        FROM strategy_decisions sd
        {where_clause}
        ORDER BY sd.decision_ts ASC, sd.id ASC
        """,
        tuple(params),
    ).fetchall()

    total_entry_candidates = 0
    hold_decision_count = 0
    blocked_entry_count = 0
    multi_filter_blocked_count = 0
    blocked_by_filter: dict[str, int] = {}
    blocked_rows: list[sqlite3.Row] = []

    for row in decision_rows:
        context = _load_context_json(row["context_json"])
        base_signal = str(context.get("base_signal") or "").strip().upper()
        decision_type = _extract_decision_type(row["context_json"], str(row["signal"] or ""))
        if base_signal == "BUY":
            total_entry_candidates += 1
        if decision_type == "HOLD":
            hold_decision_count += 1
        if decision_type == "BLOCKED_ENTRY":
            blocked_entry_count += 1
            blocked_rows.append(row)
            blocked_filters = _extract_blocked_filters(row["context_json"])
            if len(blocked_filters) >= 2:
                multi_filter_blocked_count += 1
            for blocked_filter in blocked_filters:
                blocked_by_filter[blocked_filter] = blocked_by_filter.get(blocked_filter, 0) + 1

    executed_entry_count = int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM trade_lifecycles tl
            LEFT JOIN strategy_decisions sd ON sd.id = tl.entry_decision_id
            {where_clause}
            """,
            tuple(params),
        ).fetchone()[0]
        or 0
    )

    candle_rows = conn.execute("SELECT ts, close FROM candles ORDER BY ts ASC").fetchall()
    close_by_ts: dict[int, float] = {}
    for candle in candle_rows:
        ts_raw = candle["ts"]
        close_raw = candle["close"]
        if ts_raw is None or close_raw is None:
            continue
        close_by_ts[int(ts_raw)] = float(close_raw)

    observation_bars = max(1, int(observation_window_bars))
    default_interval_ms = parse_interval_sec(settings.INTERVAL) * 1000
    observed_returns_bps: list[float] = []
    avoided_loss_count = 0
    opportunity_missed_count = 0
    flat_or_unknown_count = 0

    for row in blocked_rows:
        context = _load_context_json(row["context_json"])
        interval_text = str(context.get("interval") or settings.INTERVAL)
        try:
            interval_ms = parse_interval_sec(interval_text) * 1000
        except ValueError:
            interval_ms = default_interval_ms

        decision_price = float(row["market_price"]) if row["market_price"] is not None else None
        decision_candle_ts = int(row["candle_ts"]) if row["candle_ts"] is not None else None
        if decision_price is None or decision_price <= 0 or decision_candle_ts is None:
            flat_or_unknown_count += 1
            continue

        target_ts = decision_candle_ts + (interval_ms * observation_bars)
        observed_price = close_by_ts.get(target_ts)
        if observed_price is None or observed_price <= 0:
            flat_or_unknown_count += 1
            continue

        return_bps = ((observed_price - decision_price) / decision_price) * 10000.0
        observed_returns_bps.append(return_bps)
        if return_bps < 0:
            avoided_loss_count += 1
        elif return_bps > 0:
            opportunity_missed_count += 1
        else:
            flat_or_unknown_count += 1

    avg_return_bps = (
        float(sum(observed_returns_bps) / len(observed_returns_bps)) if observed_returns_bps else None
    )
    median_return_bps = median(observed_returns_bps) if observed_returns_bps else None
    sample_threshold = max(1, int(min_observation_sample))
    insufficient_sample = len(observed_returns_bps) < sample_threshold

    notes: list[str] = []
    if total_entry_candidates <= 0:
        notes.append("no BUY entry candidates found in strategy_decisions window")
    if blocked_entry_count <= 0:
        notes.append("no BLOCKED_ENTRY decisions found in strategy_decisions window")
    if insufficient_sample:
        notes.append(
            "insufficient sample for blocked-entry observation window "
            f"(observed={len(observed_returns_bps)}, threshold={sample_threshold})"
        )
    notes.append(
        "observation metric is descriptive only; blocked candidates are not counterfactual realized pnl"
    )

    return FilterEffectivenessSummary(
        total_entry_candidates=total_entry_candidates,
        executed_entry_count=executed_entry_count,
        blocked_entry_count=blocked_entry_count,
        hold_decision_count=hold_decision_count,
        blocked_by_filter=dict(sorted(blocked_by_filter.items(), key=lambda item: (-item[1], item[0]))),
        multi_filter_blocked_count=multi_filter_blocked_count,
        observation=FilterObservationSummary(
            observation_window_bars=observation_bars,
            observed_count=len(observed_returns_bps),
            insufficient_sample=insufficient_sample,
            sample_threshold=sample_threshold,
            avg_return_bps=avg_return_bps,
            median_return_bps=median_return_bps,
            avoided_loss_count=avoided_loss_count,
            opportunity_missed_count=opportunity_missed_count,
            flat_or_unknown_count=flat_or_unknown_count,
        ),
        notes=notes,
    )


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
    market, raw_symbol = canonical_market_with_raw(settings.PAIR)
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
        "market": market,
        "raw_symbol": raw_symbol,
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
        f"mode={settings.MODE} market={market} "
        f"{f'raw_symbol={raw_symbol} ' if raw_symbol else ''}db_path={settings.DB_PATH} "
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
    lifecycle_cols = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(trade_lifecycles)").fetchall()
    }
    required_cols = {
        "pair",
        "strategy_name",
        "exit_ts",
        "gross_pnl",
        "fee_total",
        "net_pnl",
        "holding_time_sec",
    }
    missing_cols = sorted(required_cols - lifecycle_cols)
    if missing_cols:
        raise RuntimeError(
            "trade_lifecycles schema missing required realized-pnl columns: "
            + ", ".join(missing_cols)
        )

    group_axes = _normalize_group_by(group_by)

    lifecycle_base = """
        SELECT
            tl.id,
            COALESCE(tl.strategy_name, '<unknown>') AS strategy_name,
            COALESCE(tl.pair, '<unknown>') AS pair,
            tl.exit_ts,
            tl.gross_pnl,
            tl.net_pnl,
            tl.fee_total,
            tl.holding_time_sec,
            CASE
                WHEN TRIM(COALESCE(json_extract(esd.context_json, '$.entry_reason'), '')) != ''
                    THEN TRIM(json_extract(esd.context_json, '$.entry_reason'))
                ELSE NULL
            END AS entry_reason,
            CASE
                WHEN TRIM(COALESCE(tl.exit_reason, '')) != '' THEN TRIM(tl.exit_reason)
                WHEN TRIM(COALESCE(json_extract(xsd.context_json, '$.exit.reason'), '')) != ''
                    THEN TRIM(json_extract(xsd.context_json, '$.exit.reason'))
                ELSE NULL
            END AS exit_reason,
            COALESCE(
                tl.exit_rule_name,
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
        LEFT JOIN strategy_decisions esd ON esd.id = tl.entry_decision_id
        LEFT JOIN strategy_decisions xsd ON xsd.id = tl.exit_decision_id
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
            COALESCE(SUM(gross_pnl), 0.0) AS realized_gross_pnl,
            COALESCE(SUM(net_pnl), 0.0) AS realized_net_pnl,
            COALESCE(SUM(fee_total), 0.0) AS fee_total,
            AVG(holding_time_sec) AS holding_time_avg_sec,
            MIN(holding_time_sec) AS holding_time_min_sec,
            MAX(holding_time_sec) AS holding_time_max_sec,
            COALESCE(SUM(CASE WHEN entry_reason IS NOT NULL THEN 1 ELSE 0 END), 0) AS entry_reason_linked_count,
            COALESCE(SUM(CASE WHEN exit_reason IS NOT NULL THEN 1 ELSE 0 END), 0) AS exit_reason_linked_count,
            MIN(CASE WHEN entry_reason IS NOT NULL THEN entry_reason ELSE NULL END) AS entry_reason_sample,
            MIN(CASE WHEN exit_reason IS NOT NULL THEN exit_reason ELSE NULL END) AS exit_reason_sample
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
                realized_gross_pnl=float(row["realized_gross_pnl"] or 0.0),
                realized_net_pnl=float(row["realized_net_pnl"] or 0.0),
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
                entry_reason_linked_count=int(row["entry_reason_linked_count"] or 0),
                exit_reason_linked_count=int(row["exit_reason_linked_count"] or 0),
                entry_reason_sample=(
                    None if row["entry_reason_sample"] is None else str(row["entry_reason_sample"])
                ),
                exit_reason_sample=(
                    None if row["exit_reason_sample"] is None else str(row["exit_reason_sample"])
                ),
            )
        )
    return stats


def fetch_lifecycle_close_summary(
    conn: sqlite3.Connection,
    *,
    strategy_name: str | None = None,
    exit_rule_name: str | None = None,
    pair: str | None = None,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    min_sample_size: int = 3,
    entry_exit_combo_limit: int = 20,
) -> tuple[list[LifecycleCloseStat], list[LifecycleCloseStat], list[str]]:
    lifecycle_base = """
        SELECT
            tl.id,
            COALESCE(tl.strategy_name, '<unknown>') AS strategy_name,
            COALESCE(tl.pair, '<unknown>') AS pair,
            tl.exit_ts,
            tl.net_pnl,
            tl.holding_time_sec,
            COALESCE(
                NULLIF(TRIM(COALESCE(tl.exit_rule_name, '')), ''),
                NULLIF(TRIM(COALESCE(json_extract(xsd.context_json, '$.exit.rule'), '')), ''),
                '<unknown_exit_rule>'
            ) AS exit_rule_name,
            COALESCE(
                NULLIF(TRIM(COALESCE(tl.exit_reason, '')), ''),
                NULLIF(TRIM(COALESCE(json_extract(xsd.context_json, '$.exit.reason'), '')), ''),
                '<legacy_missing_exit_reason>'
            ) AS exit_reason_bucket,
            COALESCE(
                NULLIF(TRIM(COALESCE(json_extract(esd.context_json, '$.entry.rule'), '')), ''),
                NULLIF(TRIM(COALESCE(json_extract(esd.context_json, '$.entry_reason'), '')), ''),
                '<unknown_entry_rule>'
            ) AS entry_rule_name
        FROM trade_lifecycles tl
        LEFT JOIN strategy_decisions esd ON esd.id = tl.entry_decision_id
        LEFT JOIN strategy_decisions xsd ON xsd.id = tl.exit_decision_id
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

    def _map_rows(rows: list[sqlite3.Row]) -> list[LifecycleCloseStat]:
        mapped: list[LifecycleCloseStat] = []
        for row in rows:
            trade_count = int(row["trade_count"] or 0)
            win_count = int(row["win_count"] or 0)
            mapped.append(
                LifecycleCloseStat(
                    entry_rule_name=str(row["entry_rule_name"]),
                    exit_rule_name=str(row["exit_rule_name"]),
                    exit_reason_bucket=str(row["exit_reason_bucket"]),
                    trade_count=trade_count,
                    win_rate=(win_count / trade_count) if trade_count > 0 else 0.0,
                    realized_net_pnl=float(row["realized_net_pnl"] or 0.0),
                    avg_hold_time_sec=(
                        None if row["avg_hold_time_sec"] is None else float(row["avg_hold_time_sec"])
                    ),
                )
            )
        return mapped

    by_exit_rule_rows = conn.execute(
        f"""
        {cte}
        SELECT
            '<all>' AS entry_rule_name,
            exit_rule_name,
            exit_reason_bucket,
            COUNT(*) AS trade_count,
            COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0) AS win_count,
            COALESCE(SUM(net_pnl), 0.0) AS realized_net_pnl,
            AVG(holding_time_sec) AS avg_hold_time_sec
        FROM lifecycle_base
        {post_where}
        GROUP BY exit_rule_name, exit_reason_bucket
        ORDER BY trade_count DESC, realized_net_pnl DESC, exit_rule_name ASC
        """,
        tuple(params),
    ).fetchall()

    by_entry_exit_rows = conn.execute(
        f"""
        {cte}
        SELECT
            entry_rule_name,
            exit_rule_name,
            exit_reason_bucket,
            COUNT(*) AS trade_count,
            COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0) AS win_count,
            COALESCE(SUM(net_pnl), 0.0) AS realized_net_pnl,
            AVG(holding_time_sec) AS avg_hold_time_sec
        FROM lifecycle_base
        {post_where}
        GROUP BY entry_rule_name, exit_rule_name, exit_reason_bucket
        ORDER BY trade_count DESC, realized_net_pnl DESC, entry_rule_name ASC, exit_rule_name ASC
        LIMIT ?
        """,
        (*params, max(1, int(entry_exit_combo_limit))),
    ).fetchall()

    by_exit_rule = _map_rows(by_exit_rule_rows)
    by_entry_exit = _map_rows(by_entry_exit_rows)

    notes: list[str] = []
    threshold = max(1, int(min_sample_size))
    low_sample_rows = [row for row in by_exit_rule if row.trade_count < threshold]
    if low_sample_rows:
        notes.append(
            "low-sample exit buckets present (trade_count < "
            f"{threshold}): "
            + ", ".join(f"{row.exit_rule_name}/{row.exit_reason_bucket}" for row in low_sample_rows[:5])
        )
    return by_exit_rule, by_entry_exit, notes


def cmd_strategy_report(
    *,
    strategy_name: str | None,
    exit_rule_name: str | None,
    pair: str | None,
    from_ts_ms: int | None,
    to_ts_ms: int | None,
    group_by: tuple[str, ...] | list[str] | None,
    observation_window_bars: int = 5,
    min_observation_sample: int = 10,
    as_json: bool = False,
) -> None:
    conn = ensure_db()
    try:
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
            close_by_exit_rule, close_by_entry_exit, close_notes = fetch_lifecycle_close_summary(
                conn,
                strategy_name=strategy_name,
                exit_rule_name=exit_rule_name,
                pair=pair,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
            filter_effectiveness = fetch_filter_effectiveness_summary(
                conn,
                strategy_name=strategy_name,
                pair=pair,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
                observation_window_bars=observation_window_bars,
                min_observation_sample=min_observation_sample,
            )
            attribution_quality = fetch_attribution_quality_summary(
                conn,
                strategy_name=strategy_name,
                pair=pair,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
        except RuntimeError as exc:
            print("[STRATEGY-PERFORMANCE-REPORT]")
            print(f"  schema_error={exc}")
            print("  tip: 최신 스키마로 마이그레이션하거나 최신 DB를 사용하세요.")
            return
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
                "realized_gross_pnl": stat.realized_gross_pnl,
                "fee_total": stat.fee_total,
                "realized_net_pnl": stat.realized_net_pnl,
                "expectancy_per_trade": stat.expectancy_per_trade,
                "net_pnl": stat.realized_net_pnl,
                "holding_time": {
                    "avg_sec": stat.holding_time_avg_sec,
                    "min_sec": stat.holding_time_min_sec,
                    "max_sec": stat.holding_time_max_sec,
                },
                "reason_summary": {
                    "entry_reason_linked_count": stat.entry_reason_linked_count,
                    "exit_reason_linked_count": stat.exit_reason_linked_count,
                    "entry_reason_sample": stat.entry_reason_sample,
                    "exit_reason_sample": stat.exit_reason_sample,
                },
            }
            for stat in stats
        ],
        "lifecycle_close_summary": {
            "low_sample_threshold": 3,
            "by_exit_rule": [
                {
                    "entry_rule_name": row.entry_rule_name,
                    "exit_rule_name": row.exit_rule_name,
                    "exit_reason_bucket": row.exit_reason_bucket,
                    "trade_count": row.trade_count,
                    "win_rate": row.win_rate,
                    "realized_net_pnl": row.realized_net_pnl,
                    "avg_hold_time_sec": row.avg_hold_time_sec,
                }
                for row in close_by_exit_rule
            ],
            "entry_exit_combinations": [
                {
                    "entry_rule_name": row.entry_rule_name,
                    "exit_rule_name": row.exit_rule_name,
                    "exit_reason_bucket": row.exit_reason_bucket,
                    "trade_count": row.trade_count,
                    "win_rate": row.win_rate,
                    "realized_net_pnl": row.realized_net_pnl,
                    "avg_hold_time_sec": row.avg_hold_time_sec,
                }
                for row in close_by_entry_exit
            ],
            "notes": close_notes,
        },
        "filter_effectiveness": {
            "entry_candidate_summary": {
                "total_entry_candidates": filter_effectiveness.total_entry_candidates,
                "executed_entry_count": filter_effectiveness.executed_entry_count,
                "blocked_entry_count": filter_effectiveness.blocked_entry_count,
                "hold_decision_count": filter_effectiveness.hold_decision_count,
                "multi_filter_blocked_count": filter_effectiveness.multi_filter_blocked_count,
                "blocked_by_filter": filter_effectiveness.blocked_by_filter,
            },
            "blocked_observation_window": {
                "window_bars": filter_effectiveness.observation.observation_window_bars,
                "observed_count": filter_effectiveness.observation.observed_count,
                "insufficient_sample": filter_effectiveness.observation.insufficient_sample,
                "sample_threshold": filter_effectiveness.observation.sample_threshold,
                "avg_return_bps": filter_effectiveness.observation.avg_return_bps,
                "median_return_bps": filter_effectiveness.observation.median_return_bps,
                "avoided_loss_count": filter_effectiveness.observation.avoided_loss_count,
                "opportunity_missed_count": filter_effectiveness.observation.opportunity_missed_count,
                "flat_or_unknown_count": filter_effectiveness.observation.flat_or_unknown_count,
            },
            "notes": filter_effectiveness.notes,
        },
        "attribution_quality": {
            "total_trade_count": attribution_quality.total_trade_count,
            "unattributed_trade_count": attribution_quality.unattributed_trade_count,
            "ambiguous_linkage_count": attribution_quality.ambiguous_linkage_count,
            "recovery_derived_attribution_count": attribution_quality.recovery_derived_attribution_count,
            "unattributed_trade_ratio": attribution_quality.unattributed_trade_ratio,
            "ambiguous_linkage_ratio": attribution_quality.ambiguous_linkage_ratio,
            "recovery_derived_attribution_ratio": attribution_quality.recovery_derived_attribution_ratio,
            "reason_buckets": attribution_quality.reason_buckets,
            "warnings": attribution_quality.warnings,
        },
        "notes": (
            ([] if stats else ["no trade_lifecycles rows matched the given filters"])
            + close_notes
            + filter_effectiveness.notes
            + attribution_quality.warnings
        ),
    }
    write_json_atomic(PATH_MANAGER.strategy_validation_report_path(), payload)

    if as_json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return

    print("[STRATEGY-PERFORMANCE-REPORT (REALIZED PNL BASIS)]")
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
    else:
        print(
            "  "
            "strategy_name,exit_rule_name,pair,trade_count,win_rate,average_gain,average_loss,"
            "realized_gross_pnl,fee_total,realized_net_pnl,expectancy_per_trade,holding_avg_sec,"
            "holding_min_sec,holding_max_sec,entry_reason_linked_count,exit_reason_linked_count,"
            "entry_reason_sample,exit_reason_sample"
        )
        for stat in stats:
            holding_avg = "-" if stat.holding_time_avg_sec is None else f"{stat.holding_time_avg_sec:.2f}"
            holding_min = "-" if stat.holding_time_min_sec is None else f"{stat.holding_time_min_sec:.2f}"
            holding_max = "-" if stat.holding_time_max_sec is None else f"{stat.holding_time_max_sec:.2f}"
            entry_reason_sample = stat.entry_reason_sample or "-"
            exit_reason_sample = stat.exit_reason_sample or "-"
            print(
                "  "
                f"{stat.strategy_name},{stat.exit_rule_name},{stat.pair},{stat.trade_count},"
                f"{stat.win_rate:.4f},{stat.avg_gain:.2f},{stat.avg_loss:.2f},{stat.realized_gross_pnl:.2f},"
                f"{stat.fee_total:.2f},{stat.realized_net_pnl:.2f},{stat.expectancy_per_trade:.2f},"
                f"{holding_avg},{holding_min},{holding_max},"
                f"{stat.entry_reason_linked_count},{stat.exit_reason_linked_count},"
                f"{entry_reason_sample},{exit_reason_sample}"
            )

    print("  [lifecycle_close_summary: by_exit_rule]")
    print("  exit_rule_name,exit_reason_bucket,trade_count,win_rate,realized_net_pnl,avg_hold_time_sec")
    for row in close_by_exit_rule[:10]:
        hold_avg = "-" if row.avg_hold_time_sec is None else f"{row.avg_hold_time_sec:.2f}"
        print(
            "  "
            f"{row.exit_rule_name},{row.exit_reason_bucket},{row.trade_count},{row.win_rate:.4f},"
            f"{row.realized_net_pnl:.2f},{hold_avg}"
        )

    if close_by_entry_exit:
        print("  [lifecycle_close_summary: entry_rule x exit_rule]")
        print(
            "  "
            "entry_rule_name,exit_rule_name,exit_reason_bucket,trade_count,win_rate,realized_net_pnl,avg_hold_time_sec"
        )
        for row in close_by_entry_exit[:10]:
            hold_avg = "-" if row.avg_hold_time_sec is None else f"{row.avg_hold_time_sec:.2f}"
            print(
                "  "
                f"{row.entry_rule_name},{row.exit_rule_name},{row.exit_reason_bucket},{row.trade_count},"
                f"{row.win_rate:.4f},{row.realized_net_pnl:.2f},{hold_avg}"
            )

    for note in close_notes:
        print(f"  note: {note}")
    print("  [filter_effectiveness]")
    print(
        "  "
        f"entry_candidates={filter_effectiveness.total_entry_candidates} "
        f"executed_entries={filter_effectiveness.executed_entry_count} "
        f"blocked_entries={filter_effectiveness.blocked_entry_count} "
        f"hold_decisions={filter_effectiveness.hold_decision_count} "
        f"multi_filter_blocked={filter_effectiveness.multi_filter_blocked_count}"
    )
    print("  filter,blocked_count")
    if not filter_effectiveness.blocked_by_filter:
        print("  -,-")
    else:
        for filter_name, blocked_count in filter_effectiveness.blocked_by_filter.items():
            print(f"  {filter_name},{blocked_count}")
    print(
        "  "
        f"blocked_window_bars={filter_effectiveness.observation.observation_window_bars} "
        f"observed_count={filter_effectiveness.observation.observed_count} "
        f"insufficient_sample={1 if filter_effectiveness.observation.insufficient_sample else 0} "
        f"sample_threshold={filter_effectiveness.observation.sample_threshold} "
        f"avg_return_bps={_fmt_rate(filter_effectiveness.observation.avg_return_bps, as_bps=True)} "
        f"median_return_bps={_fmt_rate(filter_effectiveness.observation.median_return_bps, as_bps=True)}"
    )
    print(
        "  "
        f"blocked_window_outcome="
        f"avoided_loss:{filter_effectiveness.observation.avoided_loss_count},"
        f"opportunity_missed:{filter_effectiveness.observation.opportunity_missed_count},"
        f"flat_or_unknown:{filter_effectiveness.observation.flat_or_unknown_count}"
    )
    for note in filter_effectiveness.notes:
        print(f"  note: {note}")
    print("  [attribution_quality]")
    print(
        "  "
        f"trade_count={attribution_quality.total_trade_count} "
        f"unattributed_trade_count={attribution_quality.unattributed_trade_count} "
        f"ambiguous_linkage_count={attribution_quality.ambiguous_linkage_count} "
        f"recovery_derived_attribution_count={attribution_quality.recovery_derived_attribution_count}"
    )
    print(
        "  "
        f"ratios="
        f"unattributed:{attribution_quality.unattributed_trade_ratio:.2%},"
        f"ambiguous:{attribution_quality.ambiguous_linkage_ratio:.2%},"
        f"recovery_derived:{attribution_quality.recovery_derived_attribution_ratio:.2%}"
    )
    print(
        "  "
        "reason_buckets="
        f"missing_decision_id:{attribution_quality.reason_buckets.get('missing_decision_id', 0)},"
        f"multiple_candidate_decisions:{attribution_quality.reason_buckets.get('multiple_candidate_decisions', 0)},"
        f"legacy_incomplete_row:{attribution_quality.reason_buckets.get('legacy_incomplete_row', 0)},"
        f"recovery_unresolved_linkage:{attribution_quality.reason_buckets.get('recovery_unresolved_linkage', 0)}"
    )
    for warning in attribution_quality.warnings:
        print(f"  warning: {warning}")


def _max_drawdown_from_trade_sequence(net_pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in net_pnls:
        equity += float(pnl)
        peak = max(peak, equity)
        drawdown = peak - equity
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd


def _longest_losing_streak(net_pnls: list[float]) -> int:
    streak = 0
    best = 0
    for pnl in net_pnls:
        if float(pnl) < 0.0:
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def _build_bucket_rows(
    stats: dict[str, dict[str, float]],
    *,
    total_trade_count: int,
    total_realized_net_pnl: float,
    total_abs_pnl: float,
    total_profit_pnl: float,
    total_loss_pnl_abs: float,
) -> list[ExperimentBucketStat]:
    rows: list[ExperimentBucketStat] = []
    for bucket, agg in stats.items():
        count = int(agg.get("trade_count", 0))
        wins = int(agg.get("wins", 0))
        net = float(agg.get("realized_net_pnl", 0.0))
        abs_pnl = float(agg.get("absolute_pnl", 0.0))
        profit_pnl = float(agg.get("profit_pnl", 0.0))
        loss_pnl_abs = float(agg.get("loss_pnl_abs", 0.0))
        rows.append(
            ExperimentBucketStat(
                bucket=bucket,
                trade_count=count,
                trade_count_share=(count / total_trade_count) if total_trade_count > 0 else 0.0,
                win_rate=(wins / count) if count > 0 else 0.0,
                realized_net_pnl=net,
                realized_net_pnl_share=(net / total_realized_net_pnl) if total_realized_net_pnl != 0.0 else 0.0,
                absolute_pnl_concentration=(abs_pnl / total_abs_pnl) if total_abs_pnl > 0.0 else 0.0,
                profitable_pnl_concentration=(profit_pnl / total_profit_pnl) if total_profit_pnl > 0.0 else 0.0,
                loss_pnl_concentration=(loss_pnl_abs / total_loss_pnl_abs) if total_loss_pnl_abs > 0.0 else 0.0,
                expectancy_per_trade=(net / count) if count > 0 else 0.0,
            )
        )
    rows.sort(key=lambda row: (-row.trade_count, row.bucket))
    return rows


def _classify_regime_bucket(analysis: dict[str, Any]) -> str:
    buckets = analysis.get("buckets") if isinstance(analysis.get("buckets"), dict) else {}
    volatility = str(buckets.get("volatility") or "unknown")
    extension = str(buckets.get("overextension") or "unknown")
    if volatility == "unknown" and extension == "unknown":
        return "unknown"
    return f"vol={volatility}|ext={extension}"


def fetch_experiment_report_summary(
    conn: sqlite3.Connection,
    *,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
    strategy_name: str | None = None,
    pair: str | None = None,
    top_n: int = 3,
    sample_threshold: int = 30,
    concentration_warn_threshold: float = 0.6,
    regime_skew_warn_threshold: float = 0.7,
    regime_pnl_skew_warn_threshold: float = 0.7,
) -> ExperimentReportSummary:
    filters: list[str] = []
    params: list[object] = []
    if from_ts_ms is not None:
        filters.append("tl.exit_ts >= ?")
        params.append(int(from_ts_ms))
    if to_ts_ms is not None:
        filters.append("tl.exit_ts <= ?")
        params.append(int(to_ts_ms))
    if strategy_name:
        filters.append("COALESCE(tl.strategy_name, '<unknown>') = ?")
        params.append(str(strategy_name))
    if pair:
        filters.append("COALESCE(tl.pair, '<unknown>') = ?")
        params.append(str(pair))

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""
        SELECT
            tl.id,
            tl.entry_ts,
            tl.exit_ts,
            tl.net_pnl,
            esd.context_json AS entry_context_json,
            xsd.context_json AS exit_context_json
        FROM trade_lifecycles tl
        LEFT JOIN strategy_decisions esd ON esd.id = tl.entry_decision_id
        LEFT JOIN strategy_decisions xsd ON xsd.id = tl.exit_decision_id
        {where_clause}
        ORDER BY tl.exit_ts ASC, tl.id ASC
        """,
        tuple(params),
    ).fetchall()

    net_pnls = [float(row["net_pnl"] or 0.0) for row in rows]
    trade_count = len(net_pnls)
    wins = sum(1 for pnl in net_pnls if pnl > 0.0)
    realized_net_pnl = float(sum(net_pnls))
    expectancy = (realized_net_pnl / trade_count) if trade_count > 0 else 0.0
    max_drawdown = _max_drawdown_from_trade_sequence(net_pnls)
    longest_streak = _longest_losing_streak(net_pnls)

    abs_total = float(sum(abs(pnl) for pnl in net_pnls))
    top_sorted = sorted((abs(pnl) for pnl in net_pnls), reverse=True)
    top_n_value = max(1, int(top_n))
    top_n_total = float(sum(top_sorted[:top_n_value]))
    top_n_concentration = (top_n_total / abs_total) if abs_total > 0.0 else 0.0

    time_stats: dict[str, dict[str, float]] = {}
    regime_stats: dict[str, dict[str, float]] = {}
    for row in rows:
        pnl = float(row["net_pnl"] or 0.0)
        analysis = normalize_analysis_context_from_lifecycle_row(
            row,
            entry_context_json=row["entry_context_json"],
            exit_context_json=row["exit_context_json"],
        )
        buckets = analysis.get("buckets") if isinstance(analysis.get("buckets"), dict) else {}
        time_bucket = str(buckets.get("time_of_day") or "unknown")
        regime_bucket = _classify_regime_bucket(analysis)
        for bucket, target in ((time_bucket, time_stats), (regime_bucket, regime_stats)):
            agg = target.setdefault(
                bucket,
                {
                    "trade_count": 0.0,
                    "wins": 0.0,
                    "realized_net_pnl": 0.0,
                    "absolute_pnl": 0.0,
                    "profit_pnl": 0.0,
                    "loss_pnl_abs": 0.0,
                },
            )
            agg["trade_count"] += 1.0
            if pnl > 0.0:
                agg["wins"] += 1.0
            agg["realized_net_pnl"] += pnl
            agg["absolute_pnl"] += abs(pnl)
            if pnl > 0.0:
                agg["profit_pnl"] += pnl
            elif pnl < 0.0:
                agg["loss_pnl_abs"] += abs(pnl)

    total_profit_pnl = float(sum(pnl for pnl in net_pnls if pnl > 0.0))
    total_loss_pnl_abs = float(sum(abs(pnl) for pnl in net_pnls if pnl < 0.0))
    time_bucket_rows = _build_bucket_rows(
        time_stats,
        total_trade_count=trade_count,
        total_realized_net_pnl=realized_net_pnl,
        total_abs_pnl=abs_total,
        total_profit_pnl=total_profit_pnl,
        total_loss_pnl_abs=total_loss_pnl_abs,
    )
    regime_bucket_rows = _build_bucket_rows(
        regime_stats,
        total_trade_count=trade_count,
        total_realized_net_pnl=realized_net_pnl,
        total_abs_pnl=abs_total,
        total_profit_pnl=total_profit_pnl,
        total_loss_pnl_abs=total_loss_pnl_abs,
    )
    regime_top_count = max((row.trade_count for row in regime_bucket_rows), default=0)
    regime_skew_ratio = (regime_top_count / trade_count) if trade_count > 0 else 0.0
    regime_pnl_skew_ratio = max((row.absolute_pnl_concentration for row in regime_bucket_rows), default=0.0)

    warnings: list[str] = []
    if trade_count < max(1, int(sample_threshold)):
        warnings.append(
            f"insufficient sample: trade_count={trade_count} < threshold={int(sample_threshold)}; "
            "avoid strong expectancy conclusions."
        )
    if top_n_concentration >= float(concentration_warn_threshold):
        warnings.append(
            f"concentrated pnl: top{top_n_value}_abs_trade_contribution={top_n_concentration:.2%} "
            f"(threshold={float(concentration_warn_threshold):.0%})."
        )
    if regime_skew_ratio >= float(regime_skew_warn_threshold):
        warnings.append(
            f"regime skew: dominant_regime_trade_share={regime_skew_ratio:.2%} "
            f"(threshold={float(regime_skew_warn_threshold):.0%})."
        )
    if regime_pnl_skew_ratio >= float(regime_pnl_skew_warn_threshold):
        warnings.append(
            f"regime pnl skew: dominant_regime_abs_pnl_share={regime_pnl_skew_ratio:.2%} "
            f"(threshold={float(regime_pnl_skew_warn_threshold):.0%})."
        )

    return ExperimentReportSummary(
        realized_net_pnl=realized_net_pnl,
        trade_count=trade_count,
        win_rate=(wins / trade_count) if trade_count > 0 else 0.0,
        expectancy_per_trade=expectancy,
        max_drawdown=max_drawdown,
        top_n_concentration=top_n_concentration,
        top_n=top_n_value,
        longest_losing_streak=longest_streak,
        sample_threshold=max(1, int(sample_threshold)),
        sample_insufficient=trade_count < max(1, int(sample_threshold)),
        regime_skew_ratio=regime_skew_ratio,
        regime_pnl_skew_ratio=regime_pnl_skew_ratio,
        warnings=warnings,
        time_bucket_rows=time_bucket_rows,
        regime_bucket_rows=regime_bucket_rows,
    )


def cmd_experiment_report(
    *,
    strategy_name: str | None,
    pair: str | None,
    from_ts_ms: int | None,
    to_ts_ms: int | None,
    top_n: int = 3,
    sample_threshold: int = 30,
    concentration_warn_threshold: float = 0.6,
    regime_skew_warn_threshold: float = 0.7,
    regime_pnl_skew_warn_threshold: float = 0.7,
    as_json: bool = False,
) -> None:
    conn = ensure_db()
    try:
        summary = fetch_experiment_report_summary(
            conn,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            strategy_name=strategy_name,
            pair=pair,
            top_n=top_n,
            sample_threshold=sample_threshold,
            concentration_warn_threshold=concentration_warn_threshold,
            regime_skew_warn_threshold=regime_skew_warn_threshold,
            regime_pnl_skew_warn_threshold=regime_pnl_skew_warn_threshold,
        )
        attribution_quality = fetch_attribution_quality_summary(
            conn,
            strategy_name=strategy_name,
            pair=pair,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
        )
        recovery_attribution_signals = fetch_recovery_attribution_signal_summary(
            conn,
            strategy_name=strategy_name,
            pair=pair,
        )
    finally:
        conn.close()

    report_warnings = summary.warnings + attribution_quality.warnings
    payload = {
        "mode": settings.MODE,
        "market": settings.PAIR,
        "filters": {
            "strategy_name": strategy_name,
            "pair": pair,
            "from_ts_ms": from_ts_ms,
            "to_ts_ms": to_ts_ms,
            "sample_threshold": summary.sample_threshold,
            "top_n": summary.top_n,
        },
        "operational_stability_boundary": {
            "note": "ops-report/health/recovery 지표와 분리된 실험용 expectancy 검증 리포트입니다."
        },
        "experiment_expectancy_metrics": {
            "realized_net_pnl": summary.realized_net_pnl,
            "trade_count": summary.trade_count,
            "win_rate": summary.win_rate,
            "expectancy_per_trade": summary.expectancy_per_trade,
            "max_drawdown_proxy": summary.max_drawdown,
            "top_n_concentration": summary.top_n_concentration,
            "longest_losing_streak": summary.longest_losing_streak,
            "sample_insufficient": summary.sample_insufficient,
            "regime_skew_ratio": summary.regime_skew_ratio,
            "regime_pnl_skew_ratio": summary.regime_pnl_skew_ratio,
        },
        "time_of_day_bucket_performance": [
            {
                "bucket": row.bucket,
                "trade_count": row.trade_count,
                "trade_count_share": row.trade_count_share,
                "win_rate": row.win_rate,
                "realized_net_pnl": row.realized_net_pnl,
                "realized_net_pnl_share": row.realized_net_pnl_share,
                "absolute_pnl_concentration": row.absolute_pnl_concentration,
                "profitable_pnl_concentration": row.profitable_pnl_concentration,
                "loss_pnl_concentration": row.loss_pnl_concentration,
                "expectancy_per_trade": row.expectancy_per_trade,
            }
            for row in summary.time_bucket_rows
        ],
        "market_regime_bucket_performance": [
            {
                "bucket": row.bucket,
                "trade_count": row.trade_count,
                "trade_count_share": row.trade_count_share,
                "win_rate": row.win_rate,
                "realized_net_pnl": row.realized_net_pnl,
                "realized_net_pnl_share": row.realized_net_pnl_share,
                "absolute_pnl_concentration": row.absolute_pnl_concentration,
                "profitable_pnl_concentration": row.profitable_pnl_concentration,
                "loss_pnl_concentration": row.loss_pnl_concentration,
                "expectancy_per_trade": row.expectancy_per_trade,
            }
            for row in summary.regime_bucket_rows
        ],
        "attribution_quality": {
            "total_trade_count": attribution_quality.total_trade_count,
            "unattributed_trade_count": attribution_quality.unattributed_trade_count,
            "ambiguous_linkage_count": attribution_quality.ambiguous_linkage_count,
            "recovery_derived_attribution_count": attribution_quality.recovery_derived_attribution_count,
            "unattributed_trade_ratio": attribution_quality.unattributed_trade_ratio,
            "ambiguous_linkage_ratio": attribution_quality.ambiguous_linkage_ratio,
            "recovery_derived_attribution_ratio": attribution_quality.recovery_derived_attribution_ratio,
            "reason_buckets": attribution_quality.reason_buckets,
        },
        "recovery_attribution_quality_signals": {
            "recent_recovery_derived_trade_count": (
                recovery_attribution_signals.recent_recovery_derived_trade_count
            ),
            "unresolved_attribution_count": recovery_attribution_signals.unresolved_attribution_count,
            "ambiguous_linkage_after_recent_reconcile": (
                recovery_attribution_signals.ambiguous_linkage_after_recent_reconcile
            ),
            "last_reconcile_epoch_sec": recovery_attribution_signals.last_reconcile_epoch_sec,
        },
        "warnings": report_warnings,
    }
    write_json_atomic(PATH_MANAGER.report_path("experiment_report"), payload)

    if as_json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return

    print("[EXPERIMENT-REPORT]")
    print(
        "  "
        f"strategy_name={strategy_name or '<all>'} "
        f"pair={pair or '<all>'} "
        f"from_ts_ms={from_ts_ms if from_ts_ms is not None else '<none>'} "
        f"to_ts_ms={to_ts_ms if to_ts_ms is not None else '<none>'}"
    )
    print("[BOUNDARY]")
    print("  ops_stability_metrics=separate (use ops-report/health/recovery)")
    print("  expectancy_validation_metrics=below")
    print("[EXPECTANCY]")
    print(f"  realized_net_pnl={summary.realized_net_pnl:,.2f}")
    print(f"  trade_count={summary.trade_count}")
    print(f"  win_rate={summary.win_rate:.2%}")
    print(f"  expectancy_per_trade={summary.expectancy_per_trade:,.2f}")
    print(f"  max_drawdown_proxy={summary.max_drawdown:,.2f}")
    print(f"  top{summary.top_n}_concentration={summary.top_n_concentration:.2%}")
    print(f"  regime_pnl_skew_ratio={summary.regime_pnl_skew_ratio:.2%}")
    print(f"  longest_losing_streak={summary.longest_losing_streak}")
    print("[TIME-OF-DAY-BUCKETS]")
    print(
        "  bucket,trade_count,trade_count_share,win_rate,realized_net_pnl,realized_net_pnl_share,"
        "absolute_pnl_concentration,profitable_pnl_concentration,loss_pnl_concentration,expectancy_per_trade"
    )
    for row in summary.time_bucket_rows:
        print(
            "  "
            f"{row.bucket},{row.trade_count},{row.trade_count_share:.4f},{row.win_rate:.4f},"
            f"{row.realized_net_pnl:.2f},{row.realized_net_pnl_share:.4f},{row.absolute_pnl_concentration:.4f},"
            f"{row.profitable_pnl_concentration:.4f},{row.loss_pnl_concentration:.4f},{row.expectancy_per_trade:.2f}"
        )
    print("[MARKET-REGIME-BUCKETS]")
    print(
        "  bucket,trade_count,trade_count_share,win_rate,realized_net_pnl,realized_net_pnl_share,"
        "absolute_pnl_concentration,profitable_pnl_concentration,loss_pnl_concentration,expectancy_per_trade"
    )
    for row in summary.regime_bucket_rows:
        print(
            "  "
            f"{row.bucket},{row.trade_count},{row.trade_count_share:.4f},{row.win_rate:.4f},"
            f"{row.realized_net_pnl:.2f},{row.realized_net_pnl_share:.4f},{row.absolute_pnl_concentration:.4f},"
            f"{row.profitable_pnl_concentration:.4f},{row.loss_pnl_concentration:.4f},{row.expectancy_per_trade:.2f}"
        )
    print("[ATTRIBUTION-QUALITY]")
    print(
        "  "
        f"trade_count={attribution_quality.total_trade_count} "
        f"unattributed_trade_count={attribution_quality.unattributed_trade_count} "
        f"ambiguous_linkage_count={attribution_quality.ambiguous_linkage_count} "
        f"recovery_derived_attribution_count={attribution_quality.recovery_derived_attribution_count}"
    )
    print(
        "  "
        "reason_buckets="
        f"missing_decision_id:{attribution_quality.reason_buckets.get('missing_decision_id', 0)},"
        f"multiple_candidate_decisions:{attribution_quality.reason_buckets.get('multiple_candidate_decisions', 0)},"
        f"legacy_incomplete_row:{attribution_quality.reason_buckets.get('legacy_incomplete_row', 0)},"
        f"recovery_unresolved_linkage:{attribution_quality.reason_buckets.get('recovery_unresolved_linkage', 0)}"
    )
    print(
        "  "
        f"unresolved_attribution_count={recovery_attribution_signals.unresolved_attribution_count} "
        f"recent_recovery_derived_trade_count={recovery_attribution_signals.recent_recovery_derived_trade_count} "
        "ambiguous_linkage_after_recent_reconcile="
        f"{recovery_attribution_signals.ambiguous_linkage_after_recent_reconcile}"
    )
    if report_warnings:
        print("[WARNINGS]")
        for warning in report_warnings:
            print(f"  - {warning}")


def cmd_ops_report(*, limit: int = 20) -> None:
    market, raw_symbol = canonical_market_with_raw(settings.PAIR)
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
        recovery_attribution_signals = fetch_recovery_attribution_signal_summary(conn)
    finally:
        conn.close()

    order_rule_snapshot: dict[str, object]
    try:
        resolved_rules = get_effective_order_rules(settings.PAIR)
        rules = resolved_rules.rules
        source = resolved_rules.source or {}
        order_rule_snapshot = {
            "min_qty": {"value": rules.min_qty, "source": rule_source_for("min_qty", source)},
            "qty_step": {"value": rules.qty_step, "source": rule_source_for("qty_step", source)},
            "min_notional_krw": {
                "value": rules.min_notional_krw,
                "source": rule_source_for("min_notional_krw", source),
            },
            "max_qty_decimals": {
                "value": rules.max_qty_decimals,
                "source": rule_source_for("max_qty_decimals", source),
            },
            "buy": {
                "min_total_krw": {
                    "value": rules.bid_min_total_krw,
                    "source": rule_source_for("bid_min_total_krw", source),
                },
                "price_unit": {
                    "value": rules.bid_price_unit,
                    "source": rule_source_for("bid_price_unit", source),
                },
            },
            "sell": {
                "min_total_krw": {
                    "value": rules.ask_min_total_krw,
                    "source": rule_source_for("ask_min_total_krw", source),
                },
                "price_unit": {
                    "value": rules.ask_price_unit,
                    "source": rule_source_for("ask_price_unit", source),
                },
            },
        }
    except Exception as exc:
        order_rule_snapshot = {
            "error": f"{type(exc).__name__}: {exc}",
        }

    payload = {
        "mode": settings.MODE,
        "market": market,
        "raw_symbol": raw_symbol,
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
                "pnl_proxy_deprecated": stat.pnl_proxy,
            }
            for stat in strategy_stats
        ],
        "recent_flow": [dict(row) for row in recent_flow],
        "recent_trades": [dict(row) for row in recent_trades],
        "order_rule_snapshot": order_rule_snapshot,
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
        "recovery_attribution_quality_signals": {
            "recent_recovery_derived_trade_count": (
                recovery_attribution_signals.recent_recovery_derived_trade_count
            ),
            "unresolved_attribution_count": recovery_attribution_signals.unresolved_attribution_count,
            "ambiguous_linkage_after_recent_reconcile": (
                recovery_attribution_signals.ambiguous_linkage_after_recent_reconcile
            ),
            "last_reconcile_epoch_sec": recovery_attribution_signals.last_reconcile_epoch_sec,
        },
    }
    balance_source_diag: dict[str, object] = {
        "source": "unavailable",
        "reason": "not_checked",
        "failure_category": "none",
        "last_success_ts_ms": None,
        "last_observed_ts_ms": None,
        "last_asset_ts_ms": None,
        "stale": None,
    }
    try:
        broker = BithumbBroker()
        try:
            broker.get_balance_snapshot()
        except Exception:
            pass
        raw_diag = broker.get_accounts_validation_diagnostics()
        if isinstance(raw_diag, dict):
            balance_source_diag.update(raw_diag)
    except Exception as exc:
        balance_source_diag["reason"] = f"diagnostic_probe_failed: {type(exc).__name__}"
    payload["balance_source_diagnostics"] = balance_source_diag
    write_json_atomic(PATH_MANAGER.ops_report_path(), payload)

    print("[OPS-REPORT]")
    raw_symbol_info = f" raw_symbol={raw_symbol}" if raw_symbol else ""
    print(
        f"  mode={settings.MODE} market={market}{raw_symbol_info} interval={settings.INTERVAL} db_path={settings.DB_PATH}"
    )
    print(
        "  "
        f"balance_source={balance_source_diag.get('source') or '-'} "
        f"reason={balance_source_diag.get('reason') or '-'} "
        f"category={balance_source_diag.get('failure_category') or '-'} "
        f"stale={balance_source_diag.get('stale')}"
    )
    print(
        "  "
        f"unresolved_attribution_count={recovery_attribution_signals.unresolved_attribution_count} "
        f"recent_recovery_derived_trade_count={recovery_attribution_signals.recent_recovery_derived_trade_count} "
        "ambiguous_linkage_after_recent_reconcile="
        f"{recovery_attribution_signals.ambiguous_linkage_after_recent_reconcile}"
    )
    print("\n[ORDER-RULE-SNAPSHOT]")
    if "error" in order_rule_snapshot:
        print(f"  failed_to_load={order_rule_snapshot['error']}")
    else:
        print(
            "  "
            f"min_qty={order_rule_snapshot['min_qty']['value']} (source={order_rule_snapshot['min_qty']['source']}) "
            f"qty_step={order_rule_snapshot['qty_step']['value']} (source={order_rule_snapshot['qty_step']['source']}) "
            f"min_notional_krw={order_rule_snapshot['min_notional_krw']['value']} (source={order_rule_snapshot['min_notional_krw']['source']}) "
            f"max_qty_decimals={order_rule_snapshot['max_qty_decimals']['value']} (source={order_rule_snapshot['max_qty_decimals']['source']})"
        )
        print(
            "  "
            f"BUY(min_total_krw={order_rule_snapshot['buy']['min_total_krw']['value']} (source={order_rule_snapshot['buy']['min_total_krw']['source']}), "
            f"price_unit={order_rule_snapshot['buy']['price_unit']['value']} (source={order_rule_snapshot['buy']['price_unit']['source']})) "
            f"SELL(min_total_krw={order_rule_snapshot['sell']['min_total_krw']['value']} (source={order_rule_snapshot['sell']['min_total_krw']['source']}), "
            f"price_unit={order_rule_snapshot['sell']['price_unit']['value']} (source={order_rule_snapshot['sell']['price_unit']['source']}))"
        )

    print("\n[STRATEGY-SUMMARY]")
    if not strategy_stats:
        print("  no strategy_context rows in order_intent_dedup")
        print("  tip: strategy_context 기반 집계는 주문 intent dedup 데이터가 있어야 계산됩니다.")
    else:
        print("  strategy_context,order_count,fill_count,buy_notional,sell_notional,fee_total,pnl_proxy_deprecated")
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
    print("  - strategy-report는 trade_lifecycles 기반 realized gross/fee/net pnl 집계를 우선 사용하세요.")
    print("  - ops-report의 strategy_summary는 intent/fill 기반 참고용이며 pnl_proxy_deprecated를 포함합니다.")
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


def cmd_decision_telemetry(*, limit: int = 200) -> None:
    conn = ensure_db()
    try:
        rows = fetch_decision_telemetry_summary(conn, limit=max(1, int(limit)))
    finally:
        conn.close()

    print("[DECISION-TELEMETRY]")
    print(
        f"  mode={settings.MODE} pair={settings.PAIR} interval={settings.INTERVAL} "
        f"strategy={settings.STRATEGY_NAME} window={max(1, int(limit))}"
    )
    if not rows:
        print("  no strategy_decisions rows")
        return
    print("  decision_type,strategy_name,pair,interval,block_reason,count")
    for row in rows:
        print(
            "  "
            f"{row.decision_type},{row.strategy_name},{row.pair},{row.interval},"
            f"{row.block_reason},{row.count}"
        )
