from __future__ import annotations

import math
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from statistics import median
from typing import Any
from pathlib import Path

from .analytics_context import (
    _load_context_json,
    normalize_analysis_context_from_decision_row,
    normalize_analysis_context_from_lifecycle_row,
)
from .config import PATH_MANAGER, settings
from .decision_context import resolve_canonical_position_exposure_snapshot
from .fee_authority import resolve_fee_authority_snapshot
from .broker.order_rules import get_effective_order_rules, rule_source_for
from .reason_codes import (
    DUST_RESIDUAL_SUPPRESSED,
    DUST_RESIDUAL_UNSELLABLE,
    RISKY_ORDER_BLOCK,
    classify_sell_failure_category,
    SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
    SELL_FAILURE_CATEGORY_DUST_RESIDUAL_UNSELLABLE,
    SELL_FAILURE_CATEGORY_DUST_SUPPRESSION,
    SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
    SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD,
    SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
    SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH,
    SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
    SELL_FAILURE_CATEGORY_UNKNOWN,
    sell_failure_detail_from_category,
)
from .db_core import (
    compute_accounting_replay,
    ensure_db,
    get_external_cash_adjustment_summary,
    init_portfolio,
    normalize_asset_qty,
    normalize_cash_amount,
    portfolio_asset_total,
    portfolio_cash_total,
)
from .dust import build_dust_display_context, build_position_state_model, format_flat_start_reason_with_dust
from .lifecycle import summarize_reserved_exit_qty
from .markets import canonical_market_with_raw
from .risk import fetch_daily_risk_baseline, fetch_recent_risk_evaluations
from .storage_io import write_json_atomic
from .utils_time import kst_str, parse_interval_sec
from .run_lock import read_run_lock_status
from .broker.bithumb import BithumbBroker
from .runtime_readiness import compute_runtime_readiness_snapshot


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


def _parse_fee_validation_checks(raw: object) -> dict[str, object]:
    if isinstance(raw, dict):
        return dict(raw)
    if raw in (None, ""):
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def build_fee_rate_drift_diagnostics(
    conn: sqlite3.Connection,
    *,
    observation_limit: int = 100,
) -> dict[str, object]:
    configured_fee_rate = float(settings.LIVE_FEE_RATE_ESTIMATE)
    material_threshold = float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW)
    empty_result = {
        "configured_fee_rate": configured_fee_rate,
        "configured_fee_rate_estimate": configured_fee_rate,
        "configured_fee_bps": configured_fee_rate * 10000.0,
        "observed_fee_bps_median": None,
        "observed_fee_sample_count": 0,
        "observed_material_fee_sample_count": 0,
        "observation_window_count": 0,
        "configured_minus_observed_bps": None,
        "fee_rate_deviation_pct": None,
        "expected_fee_rate_warning_count": 0,
        "recent_expected_fee_rate_mismatch_count": 0,
        "fee_pending_count": 0,
        "fee_pending_count_semantics": "historical_observation_count_deprecated",
        "historical_fee_pending_observation_count": 0,
        "recent_fee_pending_observation_count": 0,
        "active_unresolved_fee_pending_count": 0,
        "repaired_fee_pending_incident_count": 0,
        "fee_pending_accounting_repair_count": 0,
        "unresolved_fee_state": False,
        "fill_accounting_active_issue_count": 0,
        "broker_fill_latest_unresolved_fee_pending_count": 0,
        "position_authority_repair_count": 0,
        "material_notional_threshold_krw": material_threshold,
        "startup_impact": "no_recent_fee_rate_drift_detected",
        "diagnostic_only_vs_startup_blocking": "none",
        "operator_action": "none",
        "recommended_command": "none",
    }
    if not hasattr(conn, "execute"):
        return empty_result
    try:
        rows = conn.execute(
            """
            SELECT fee, price, qty, accounting_status, fee_validation_reason, fee_validation_checks
            FROM broker_fill_observations
            ORDER BY event_ts DESC, id DESC
            LIMIT ?
            """,
            (max(1, int(observation_limit)),),
        ).fetchall()
    except sqlite3.Error:
        return empty_result
    recent_fee_bps: list[float] = []
    mismatch_count = 0
    recent_fee_pending_count = 0
    for row in rows:
        accounting_status = str(row["accounting_status"] or "")
        if accounting_status == "fee_pending":
            recent_fee_pending_count += 1
        checks = _parse_fee_validation_checks(row["fee_validation_checks"])
        if checks.get("expected_fee_rate_match") is False:
            mismatch_count += 1
        elif str(row["fee_validation_reason"] or "") == "expected_fee_rate_mismatch":
            mismatch_count += 1
        fee = row["fee"]
        if fee is None:
            continue
        try:
            fee_value = float(fee)
            price_value = float(row["price"] or 0.0)
            qty_value = float(row["qty"] or 0.0)
        except (TypeError, ValueError):
            continue
        notional = max(0.0, price_value) * max(0.0, qty_value)
        if fee_value < 0.0 or notional < material_threshold or notional <= 0.0:
            continue
        recent_fee_bps.append((fee_value / notional) * 10000.0)

    try:
        accounting_replay = compute_accounting_replay(conn)
    except (sqlite3.Error, RuntimeError):
        accounting_replay = {}
    try:
        repair_row = conn.execute("SELECT COUNT(*) AS repair_count FROM fee_pending_accounting_repairs").fetchone()
        position_repair_row = conn.execute("SELECT COUNT(*) AS repair_count FROM position_authority_repairs").fetchone()
    except sqlite3.Error:
        repair_row = None
        position_repair_row = None

    historical_fee_pending_count = int(accounting_replay.get("broker_fill_fee_pending_count") or 0)
    unresolved_fee_state = bool(accounting_replay.get("unresolved_fee_state"))
    fill_accounting_active_issue_count = int(accounting_replay.get("fill_accounting_active_issue_count") or 0)
    latest_unresolved_fee_pending_count = int(
        accounting_replay.get("broker_fill_latest_unresolved_fee_pending_count") or 0
    )
    repaired_fee_pending_incident_count = int(accounting_replay.get("fill_accounting_repaired_incident_count") or 0)
    fee_pending_accounting_repair_count = int(repair_row["repair_count"] or 0) if repair_row else 0
    active_unresolved_fee_pending_count = max(
        latest_unresolved_fee_pending_count,
        fill_accounting_active_issue_count,
        1 if unresolved_fee_state else 0,
    )
    observed_fee_bps_median = median(recent_fee_bps) if recent_fee_bps else None
    configured_fee_bps = configured_fee_rate * 10000.0
    deviation_bps = (
        configured_fee_bps - float(observed_fee_bps_median)
        if observed_fee_bps_median is not None
        else None
    )
    deviation_pct = (
        ((configured_fee_bps - float(observed_fee_bps_median)) / float(observed_fee_bps_median)) * 100.0
        if observed_fee_bps_median is not None and abs(float(observed_fee_bps_median)) > 1e-12
        else None
    )
    has_diagnostic_history = any(
        (
            mismatch_count > 0,
            recent_fee_pending_count > 0,
            historical_fee_pending_count > 0,
            repaired_fee_pending_incident_count > 0,
            fee_pending_accounting_repair_count > 0,
        )
    )
    if active_unresolved_fee_pending_count > 0:
        startup_impact = "active_fee_pending_blocks_resume"
        impact_class = "startup_blocking_due_to_active_fee_pending"
        operator_action = "resolve_fee_pending_before_resume"
        recommended_command = "uv run python bot.py recovery-report"
    elif has_diagnostic_history:
        startup_impact = "diagnostic_only_without_active_fee_pending"
        impact_class = "diagnostic_only"
        operator_action = "review_fee_diagnostics"
        recommended_command = "uv run python bot.py fee-diagnostics"
    else:
        startup_impact = "no_recent_fee_rate_drift_detected"
        impact_class = "none"
        operator_action = "none"
        recommended_command = "none"
    return {
        "configured_fee_rate": configured_fee_rate,
        "configured_fee_rate_estimate": configured_fee_rate,
        "configured_fee_bps": configured_fee_bps,
        "observed_fee_bps_median": observed_fee_bps_median,
        "observed_fee_sample_count": len(recent_fee_bps),
        "observed_material_fee_sample_count": len(recent_fee_bps),
        "observation_window_count": len(rows),
        "configured_minus_observed_bps": deviation_bps,
        "fee_rate_deviation_pct": deviation_pct,
        "expected_fee_rate_warning_count": mismatch_count,
        "recent_expected_fee_rate_mismatch_count": mismatch_count,
        "fee_pending_count": historical_fee_pending_count,
        "fee_pending_count_semantics": "historical_observation_count_deprecated",
        "historical_fee_pending_observation_count": historical_fee_pending_count,
        "recent_fee_pending_observation_count": recent_fee_pending_count,
        "active_unresolved_fee_pending_count": active_unresolved_fee_pending_count,
        "repaired_fee_pending_incident_count": repaired_fee_pending_incident_count,
        "fee_pending_accounting_repair_count": fee_pending_accounting_repair_count,
        "unresolved_fee_state": unresolved_fee_state,
        "fill_accounting_active_issue_count": fill_accounting_active_issue_count,
        "broker_fill_latest_unresolved_fee_pending_count": latest_unresolved_fee_pending_count,
        "position_authority_repair_count": int(position_repair_row["repair_count"] or 0)
        if position_repair_row
        else 0,
        "material_notional_threshold_krw": material_threshold,
        "startup_impact": startup_impact,
        "diagnostic_only_vs_startup_blocking": impact_class,
        "operator_action": operator_action,
        "recommended_command": recommended_command,
    }


@dataclass
class DecisionTelemetrySummary:
    """Operator-facing telemetry summary; qty fields are diagnostic, not authority."""

    base_signal: str
    decision_type: str
    raw_signal: str
    final_signal: str
    final_action: str
    submit_expected: bool
    pre_submit_proof_status: str
    execution_block_reason: str
    target_exposure_krw: float | None
    current_effective_exposure_krw: float | None
    tracked_residual_exposure_krw: float | None
    buy_delta_krw: float | None
    residual_live_sell_mode: str
    residual_buy_sizing_mode: str
    target_delta_side: str
    target_would_submit: bool
    target_submit_qty: float | None
    target_delta_notional_krw: float | None
    target_block_reason: str
    target_position_truth_state: str
    buy_flow_state: str
    entry_blocked: bool
    entry_allowed: bool
    block_reason: str
    dust_classification: str
    effective_flat: bool
    raw_qty_open: float
    raw_total_asset_qty: float
    position_qty: float
    submit_payload_qty: float
    normalized_exposure_active: bool
    normalized_exposure_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    sell_open_exposure_qty: float
    sell_dust_tracking_qty: float
    sell_qty_basis_qty: float
    sell_qty_boundary_kind: str
    sell_submit_lot_count: int
    sell_normalized_exposure_qty: float
    sell_failure_category: str
    sell_failure_detail: str
    strategy_name: str
    pair: str
    interval: str
    count: int


def _format_external_cash_adjustment_summary(summary: dict[str, object] | None) -> str:
    if not summary or int(summary.get("adjustment_count") or 0) <= 0:
        return "none"
    last_event_ts = summary.get("last_event_ts")
    last_event = kst_str(int(last_event_ts)) if last_event_ts is not None else "none"
    last_delta = summary.get("last_delta_amount")
    last_delta_text = (
        f"{float(last_delta):.3f}" if isinstance(last_delta, (int, float)) else "-"
    )
    return (
        f"count={int(summary.get('adjustment_count') or 0)} "
        f"total={float(summary.get('adjustment_total') or 0.0):.3f} "
        f"last_delta={last_delta_text} "
        f"last_event={last_event} "
        f"present=1 "
        f"key={summary.get('last_adjustment_key') or '-'} "
        f"source={summary.get('last_source') or '-'} "
        f"reason={summary.get('last_reason') or '-'}"
    )


def _target_shadow_from_context(context: dict[str, object]) -> dict[str, object]:
    execution_decision = context.get("execution_decision")
    nested = (
        execution_decision.get("target_shadow_decision")
        if isinstance(execution_decision, dict)
        and isinstance(execution_decision.get("target_shadow_decision"), dict)
        else None
    )
    source = dict(nested or {})
    for key in (
        "target_delta_side",
        "target_would_submit",
        "target_submit_qty",
        "target_delta_notional_krw",
        "target_block_reason",
        "target_position_truth_state",
    ):
        if key not in source and key in context:
            source[key] = context[key]
    return source


def _summarize_external_cash_adjustment_json(raw: str | None, *, keys: list[str]) -> str:
    if raw is None:
        return "-"
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return str(raw)
    if not isinstance(parsed, dict):
        return str(raw)
    pieces: list[str] = []
    for key in keys:
        if key not in parsed:
            continue
        value = parsed.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            pieces.append(f"{key}={1 if value else 0}")
        elif isinstance(value, (int, float)):
            pieces.append(f"{key}={float(value):.3f}")
        else:
            pieces.append(f"{key}={value}")
    return " ".join(pieces) if pieces else str(raw)


def _summarize_external_cash_adjustment_basis(raw: str | None) -> str:
    return _summarize_external_cash_adjustment_json(
        raw,
        keys=[
            "balance_source",
            "observed_ts_ms",
            "asset_ts_ms",
            "broker_cash_available",
            "broker_cash_locked",
            "broker_cash_total",
            "local_cash_available",
            "local_cash_locked",
            "local_cash_total",
            "cash_delta",
            "reconcile_reason_code",
        ],
    )


def _summarize_external_cash_adjustment_correlation(raw: str | None) -> str:
    return _summarize_external_cash_adjustment_json(
        raw,
        keys=[
            "remote_open_order_found",
            "recent_fill_applied",
            "invalid_fill_price_blocked",
            "unresolved_open_order_count",
            "submit_unknown_count",
            "recovery_required_count",
        ],
    )


def _replay_trade_only_cash(conn: sqlite3.Connection) -> dict[str, float | int]:
    init_portfolio(conn)
    cash = normalize_cash_amount(settings.START_CASH_KRW)
    qty = normalize_asset_qty(0.0)
    dup_fill_count = 0
    seen_fill_keys: set[tuple[str, int, float, float]] = set()

    fills = conn.execute(
        """
        SELECT f.client_order_id, f.fill_ts, f.price, f.qty, f.fee, o.side
        FROM fills f
        JOIN orders o ON o.client_order_id = f.client_order_id
        ORDER BY f.fill_ts ASC, f.id ASC
        """
    ).fetchall()

    for row in fills:
        key = (
            str(row["client_order_id"]),
            int(row["fill_ts"]),
            float(row["price"]),
            float(row["qty"]),
        )
        if key in seen_fill_keys:
            dup_fill_count += 1
        seen_fill_keys.add(key)

        fill_price = normalize_cash_amount(row["price"])
        fill_qty = normalize_asset_qty(row["qty"])
        fee = normalize_cash_amount(row["fee"])
        side = str(row["side"])
        if side == "BUY":
            cash = normalize_cash_amount(cash - ((fill_price * fill_qty) + fee))
            qty = normalize_asset_qty(qty + fill_qty)
        elif side == "SELL":
            cash = normalize_cash_amount(cash + ((fill_price * fill_qty) - fee))
            qty = normalize_asset_qty(qty - fill_qty)

    return {
        "trade_cash_krw": cash,
        "trade_asset_qty": qty,
        "dup_fill_count": dup_fill_count,
    }


def _fetch_recent_external_cash_adjustments(
    conn: sqlite3.Connection,
    *,
    limit: int,
) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT adjustment_key, event_ts, delta_amount, source, reason, broker_snapshot_basis, correlation_metadata, note
        FROM external_cash_adjustments
        ORDER BY event_ts DESC, id DESC
        LIMIT ?
        """,
        (max(0, int(limit)),),
    ).fetchall()
    return [
        {
            "adjustment_key": str(row["adjustment_key"]),
            "event_ts": int(row["event_ts"]),
            "delta_amount": float(row["delta_amount"]),
            "source": str(row["source"]),
            "reason": str(row["reason"]),
            "broker_snapshot_basis": str(row["broker_snapshot_basis"]),
            "broker_snapshot_basis_summary": _summarize_external_cash_adjustment_basis(
                str(row["broker_snapshot_basis"])
            ),
            "correlation_metadata": (
                str(row["correlation_metadata"]) if row["correlation_metadata"] is not None else None
            ),
            "correlation_metadata_summary": _summarize_external_cash_adjustment_correlation(
                str(row["correlation_metadata"]) if row["correlation_metadata"] is not None else None
            ),
            "note": str(row["note"]) if row["note"] is not None else None,
        }
        for row in rows
    ]


def _broker_cash_snapshot() -> dict[str, object]:
    snapshot: dict[str, object] = {
        "source": None,
        "observed_ts_ms": None,
        "asset_ts_ms": None,
        "cash_available": None,
        "cash_locked": None,
        "cash_krw": None,
        "error": None,
    }
    try:
        broker = BithumbBroker()
        get_snapshot = getattr(broker, "get_balance_snapshot", None)
        if callable(get_snapshot):
            balance_snapshot = get_snapshot()
            balance = balance_snapshot.balance
            snapshot.update(
                {
                    "source": str(getattr(balance_snapshot, "source_id", None) or "-"),
                    "observed_ts_ms": int(getattr(balance_snapshot, "observed_ts_ms", 0) or 0),
                    "asset_ts_ms": int(getattr(balance_snapshot, "asset_ts_ms", 0) or 0),
                    "cash_available": float(balance.cash_available),
                    "cash_locked": float(balance.cash_locked),
                    "cash_krw": portfolio_cash_total(
                        cash_available=float(balance.cash_available),
                        cash_locked=float(balance.cash_locked),
                    ),
                }
            )
            return snapshot

        get_balance = getattr(broker, "get_balance", None)
        if callable(get_balance):
            balance = get_balance()
            snapshot.update(
                {
                    "source": "legacy_balance_api",
                    "observed_ts_ms": 0,
                    "asset_ts_ms": 0,
                    "cash_available": float(balance.cash_available),
                    "cash_locked": float(balance.cash_locked),
                    "cash_krw": portfolio_cash_total(
                        cash_available=float(balance.cash_available),
                        cash_locked=float(balance.cash_locked),
                    ),
                }
            )
            return snapshot

        raise AttributeError("broker does not provide get_balance/get_balance_snapshot")
    except Exception as exc:
        snapshot["error"] = f"{type(exc).__name__}: {exc}"
        return snapshot


def fetch_cash_drift_report(
    conn: sqlite3.Connection,
    *,
    recent_limit: int = 5,
) -> dict[str, object]:
    generated_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    trade_replay = _replay_trade_only_cash(conn)
    accounting_projection = compute_accounting_replay(conn)
    portfolio_row = conn.execute(
        """
        SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
        FROM portfolio
        WHERE id=1
        """
    ).fetchone()
    if portfolio_row is None:
        raise RuntimeError("portfolio row missing while building cash drift report")

    local_cash_krw = portfolio_cash_total(
        cash_available=float(portfolio_row["cash_available"]),
        cash_locked=float(portfolio_row["cash_locked"]),
    )
    local_asset_qty = portfolio_asset_total(
        asset_available=float(portfolio_row["asset_available"]),
        asset_locked=float(portfolio_row["asset_locked"]),
    )
    trade_cash_krw = float(trade_replay["trade_cash_krw"])
    projection_cash_krw = float(accounting_projection["replay_cash"])
    projection_asset_qty = float(accounting_projection["replay_qty"])
    adjustment_summary = get_external_cash_adjustment_summary(conn)
    recent_adjustments = _fetch_recent_external_cash_adjustments(conn, limit=recent_limit)
    external_cash_adjustment_total_krw = float(adjustment_summary["adjustment_total"])
    explained_local_cash_krw = normalize_cash_amount(trade_cash_krw + external_cash_adjustment_total_krw)
    explained_delta_krw = normalize_cash_amount(local_cash_krw - trade_cash_krw)
    projection_delta_krw = normalize_cash_amount(local_cash_krw - projection_cash_krw)
    projection_asset_delta_qty = normalize_asset_qty(local_asset_qty - projection_asset_qty)
    local_ledger_consistent = math.isclose(local_cash_krw, projection_cash_krw, abs_tol=1e-6)
    local_asset_consistent = math.isclose(
        local_asset_qty,
        projection_asset_qty,
        abs_tol=1e-12,
    )
    broker = _broker_cash_snapshot()
    broker_cash_krw = broker.get("cash_krw")
    broker_local_delta_krw = (
        normalize_cash_amount(float(broker_cash_krw) - local_cash_krw)
        if broker_cash_krw is not None
        else None
    )
    broker_vs_explained_delta_krw = (
        normalize_cash_amount(float(broker_cash_krw) - projection_cash_krw)
        if broker_cash_krw is not None
        else None
    )

    return {
        "mode": settings.MODE,
        "db_path": settings.DB_PATH,
        "generated_at_epoch_ms": generated_at_ms,
        "generated_at_utc": datetime.fromtimestamp(generated_at_ms / 1000, tz=timezone.utc).isoformat(timespec="seconds"),
        "report_path": str(PATH_MANAGER.cash_drift_report_path()),
        "broker": broker,
        "local": {
            "cash_krw": local_cash_krw,
            "asset_qty": local_asset_qty,
            "cash_without_external_adjustments_krw": trade_cash_krw,
            "asset_without_external_adjustments_qty": float(trade_replay["trade_asset_qty"]),
            "consistent": bool(local_ledger_consistent and local_asset_consistent),
            "dup_fill_count": int(trade_replay["dup_fill_count"]),
        },
        "authoritative_projection": {
            "projection_model": str(accounting_projection["projection_model"]),
            "projection_kind": str(accounting_projection["projection_kind"]),
            "cash_krw": projection_cash_krw,
            "asset_qty": projection_asset_qty,
            "cash_available": float(accounting_projection["replay_cash_available"]),
            "cash_locked": float(accounting_projection["replay_cash_locked"]),
            "asset_available": float(accounting_projection["replay_asset_available"]),
            "asset_locked": float(accounting_projection["replay_asset_locked"]),
            "portfolio_cash_delta_krw": projection_delta_krw,
            "portfolio_asset_delta_qty": projection_asset_delta_qty,
            "included_event_families": list(accounting_projection["included_event_families"]),
            "diagnostic_event_families": list(accounting_projection["diagnostic_event_families"]),
            "omitted_event_families": list(accounting_projection["omitted_event_families"]),
            "unresolved_fee_state": bool(accounting_projection["unresolved_fee_state"]),
            "broker_fill_latest_unresolved_fee_pending_count": int(
                accounting_projection["broker_fill_latest_unresolved_fee_pending_count"]
            ),
            "fill_accounting_incident_projection": accounting_projection["fill_accounting_incident_projection"],
            "fill_accounting_active_issue_count": int(accounting_projection["fill_accounting_active_issue_count"]),
            "fill_accounting_already_accounted_observation_stale_count": int(
                accounting_projection["fill_accounting_already_accounted_observation_stale_count"]
            ),
            "fill_accounting_repaired_incident_count": int(
                accounting_projection["fill_accounting_repaired_incident_count"]
            ),
            "fee_gap_accounting_repair_count": int(accounting_projection["fee_gap_accounting_repair_count"]),
            "fee_pending_accounting_repair_count": int(accounting_projection["fee_pending_accounting_repair_count"]),
            "position_authority_repair_count": int(accounting_projection["position_authority_repair_count"]),
        },
        "diagnostic_execution_snapshot": {
            "cash_krw": trade_cash_krw,
            "asset_qty": float(trade_replay["trade_asset_qty"]),
            "cash_delta_to_portfolio_krw": explained_delta_krw,
            "external_cash_adjustment_only_explained_cash_krw": explained_local_cash_krw,
        },
        "cash_drift": {
            "explained_delta_krw": explained_delta_krw,
            "explained_local_cash_krw": projection_cash_krw,
            "projection_delta_krw": projection_delta_krw,
            "external_cash_adjustment_total_krw": external_cash_adjustment_total_krw,
            "external_cash_adjustment_count": int(adjustment_summary["adjustment_count"]),
            "broker_local_delta_krw": broker_local_delta_krw,
            "unexplained_residual_delta_krw": broker_vs_explained_delta_krw,
        },
        "recent_adjustments": recent_adjustments,
    }


def cmd_cash_drift_report(*, recent_limit: int = 5, as_json: bool = False) -> None:
    conn = ensure_db()
    try:
        report = fetch_cash_drift_report(conn, recent_limit=max(1, int(recent_limit)))
    finally:
        conn.close()

    write_json_atomic(PATH_MANAGER.cash_drift_report_path(), report)

    if as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return

    broker = report.get("broker") or {}
    local = report.get("local") or {}
    projection = report.get("authoritative_projection") or {}
    cash_drift = report.get("cash_drift") or {}
    recent_adjustments = report.get("recent_adjustments") or []
    broker_error = broker.get("error")
    broker_cash = broker.get("cash_krw")
    broker_local_delta = cash_drift.get("broker_local_delta_krw")
    unexplained_residual_delta = cash_drift.get("unexplained_residual_delta_krw")

    print("[CASH-DRIFT-REPORT]")
    broker_cash_line = (
        f"  broker_cash={float(broker_cash):,.3f}"
        if broker_cash is not None
        else "  broker_cash=none"
    )
    local_cash_line = (
        f"  local_cash={float(local.get('cash_krw') or 0.0):,.3f} "
        f"ledger_cash_without_adjustments={float(local.get('cash_without_external_adjustments_krw') or 0.0):,.3f}"
    )
    print(f"{broker_cash_line} {local_cash_line}")
    print(
        "  "
        f"projection_model={projection.get('projection_model') or 'unknown'} "
        f"included_event_families={','.join(projection.get('included_event_families') or [])} "
        f"diagnostic_event_families={','.join(projection.get('diagnostic_event_families') or [])} "
        f"unresolved_fee_state={1 if bool(projection.get('unresolved_fee_state')) else 0}"
    )
    if broker_error:
        print(f"  broker_error={broker_error}")
    broker_local_delta_text = (
        f"{float(broker_local_delta):,.3f}" if broker_local_delta is not None else "none"
    )
    unexplained_residual_delta_text = (
        f"{float(unexplained_residual_delta):,.3f}"
        if unexplained_residual_delta is not None
        else "none"
    )
    print(
        "  "
        f"external_cash_adjustment_total={float(cash_drift.get('external_cash_adjustment_total_krw') or 0.0):,.3f} "
        f"explained_delta={float(cash_drift.get('explained_delta_krw') or 0.0):,.3f} "
        f"broker_local_delta={broker_local_delta_text} "
        f"unexplained_residual_delta={unexplained_residual_delta_text}"
    )
    print(
        "  "
        f"local_ledger_consistent={1 if bool(local.get('consistent')) else 0} "
        f"dup_fill_count={int(local.get('dup_fill_count') or 0)} "
        f"recent_adjustment_count={int(cash_drift.get('external_cash_adjustment_count') or 0)}"
    )
    if not recent_adjustments:
        print("  recent_adjustments=none")
    else:
        print("  recent_adjustments:")
        for item in recent_adjustments:
            event_ts = item.get("event_ts")
            event_label = kst_str(int(event_ts)) if event_ts is not None else "none"
            note = item.get("note") or "-"
            correlation_metadata = item.get("correlation_metadata_summary") or item.get("correlation_metadata") or "-"
            basis_summary = item.get("broker_snapshot_basis_summary") or item.get("broker_snapshot_basis") or "-"
            print(
                "    "
                f"event_ts={event_label} adjustment_key={item.get('adjustment_key') or '-'} "
                f"delta={float(item.get('delta_amount') or 0.0):,.3f} "
                f"source={item.get('source') or '-'} reason={item.get('reason') or '-'} note={note} "
                f"basis={basis_summary} correlation_metadata={correlation_metadata}"
            )
    raw_qty_open: float
    raw_total_asset_qty: float
    position_qty: float
    submit_payload_qty: float
    normalized_exposure_active: bool
    normalized_exposure_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    sell_open_exposure_qty: float
    sell_dust_tracking_qty: float
    submit_qty_source: str
    sell_submit_qty_source: str
    sell_submit_lot_source: str
    sell_submit_lot_count: int
    sell_submit_lot_source_truth_source: str | None
    sell_submit_lot_count_truth_source: str | None
    sell_qty_basis_qty: float
    sell_qty_basis_source: str
    sell_qty_boundary_kind: str
    sell_normalized_exposure_qty: float
    sell_failure_category: str
    sell_failure_detail: str
    position_state_source: str
    raw_qty_open_truth_source: str
    raw_total_asset_qty_truth_source: str
    position_qty_truth_source: str
    submit_payload_qty_truth_source: str
    normalized_exposure_active_truth_source: str
    normalized_exposure_qty_truth_source: str
    open_exposure_qty_truth_source: str
    dust_tracking_qty_truth_source: str
    submit_qty_source_truth_source: str
    position_state_source_truth_source: str
    entry_allowed_truth_source: str
    effective_flat_truth_source: str
    strategy_name: str
    pair: str
    interval: str
    count: int


@dataclass
class RecentDecisionFlowSummary:
    """Recent decision/reporting view; sell qty fields remain non-authoritative diagnostics."""

    decision_id: int
    decision_ts: int
    strategy_name: str
    decision_type: str
    base_signal: str
    raw_signal: str
    final_signal: str
    final_action: str
    submit_expected: bool
    pre_submit_proof_status: str
    execution_block_reason: str
    target_exposure_krw: float | None
    current_effective_exposure_krw: float | None
    tracked_residual_exposure_krw: float | None
    buy_delta_krw: float | None
    residual_live_sell_mode: str
    residual_buy_sizing_mode: str
    target_delta_side: str
    target_would_submit: bool
    target_submit_qty: float | None
    target_delta_notional_krw: float | None
    target_block_reason: str
    target_position_truth_state: str
    buy_flow_state: str
    entry_blocked: bool
    entry_allowed: bool
    effective_flat: bool
    raw_qty_open: float
    raw_total_asset_qty: float
    position_qty: float
    submit_payload_qty: float
    normalized_exposure_active: bool
    normalized_exposure_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    sell_open_exposure_qty: float
    sell_dust_tracking_qty: float
    sell_submit_lot_count: int
    sell_qty_basis_qty: float
    sell_qty_boundary_kind: str
    sell_normalized_exposure_qty: float
    sell_failure_category: str
    sell_failure_detail: str
    block_reason: str
    reason: str


@dataclass
class SellSuppressionSummary:
    """Suppression report surface; qty snapshots document the event but do not grant authority."""

    event_ts: int
    strategy_name: str
    signal: str
    side: str
    reason_code: str
    suppression_category: str
    sell_submit_lot_count: int
    sell_qty_basis_qty: float | None
    sell_qty_boundary_kind: str | None
    requested_qty: float | None
    normalized_qty: float | None
    market_price: float | None
    open_exposure_qty: float | None
    dust_tracking_qty: float | None
    sell_failure_detail: str | None
    dust_state: str | None
    dust_action: str | None
    operator_action: str | None
    summary: str | None


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
    return_distribution_bps: dict[str, float | None]
    blocked_outcome_by_filter: dict[str, dict[str, float | int | bool | None]]
    blocked_outcome_by_signal_strength: dict[str, dict[str, float | int | bool | None]]
    blocked_outcome_by_market_bucket: dict[str, dict[str, float | int | bool | None]]


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
            oe.qty AS order_events_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.order_qty'),
                json_extract(oe.submit_evidence, '$.normalized_qty'),
                oe.qty
            ) AS submit_payload_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.normalized_qty'),
                oe.qty
            ) AS normalized_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.raw_total_asset_qty'),
                json_extract(oe.submit_evidence, '$.raw_qty_open'),
                0.0
            ) AS raw_total_asset_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.observed_position_qty'),
                json_extract(oe.submit_evidence, '$.position_qty'),
                oe.qty,
                0.0
            ) AS position_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.observed_submit_payload_qty'),
                json_extract(oe.submit_evidence, '$.submit_payload_qty'),
                oe.qty,
                0.0
            ) AS submit_payload_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.open_exposure_qty'),
                0.0
            ) AS open_exposure_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.dust_tracking_qty'),
                0.0
            ) AS dust_tracking_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.sell_open_exposure_qty'),
                json_extract(oe.submit_evidence, '$.open_exposure_qty'),
                0.0
            ) AS sell_open_exposure_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.sell_dust_tracking_qty'),
                json_extract(oe.submit_evidence, '$.dust_tracking_qty'),
                0.0
            ) AS sell_dust_tracking_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.observed_sell_qty_basis_qty'),
                json_extract(oe.submit_evidence, '$.sell_qty_basis_qty'),
                0.0
            ) AS sell_qty_basis_qty,
            COALESCE(
                json_extract(oe.submit_evidence, '$.sell_qty_boundary_kind'),
                'none'
            ) AS sell_qty_boundary_kind,
            COALESCE(
                json_extract(oe.submit_evidence, '$.sell_failure_detail'),
                '-'
            ) AS sell_failure_detail,
            COALESCE(
                json_extract(oe.submit_evidence, '$.sell_failure_category'),
                json_extract(oe.submit_evidence, '$.decision_summary.sell_failure_category'),
                '-'
            ) AS sell_failure_category,
            oe.submission_reason_code,
            oe.message,
            oe.submit_evidence,
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


def _fetch_recent_sell_suppressions(conn: sqlite3.Connection, *, limit: int) -> list[SellSuppressionSummary]:
    rows = conn.execute(
        """
        SELECT
            event_ts,
            strategy_name,
            signal,
            side,
            reason_code,
            COALESCE(
                json_extract(context_json, '$.sell_failure_category'),
                json_extract(context_json, '$.sell_failure_detail'),
                '-'
            ) AS sell_failure_category,
            COALESCE(
                CAST(json_extract(context_json, '$.sell_submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.sellable_executable_lot_count') AS INTEGER),
                0
            ) AS sell_submit_lot_count,
            COALESCE(
                json_extract(context_json, '$.observed_sell_qty_basis_qty'),
                json_extract(context_json, '$.sell_qty_basis_qty'),
                json_extract(context_json, '$.open_exposure_qty'),
                json_extract(context_json, '$.sell_open_exposure_qty'),
                0.0
            ) AS sell_qty_basis_qty,
            COALESCE(
                json_extract(context_json, '$.sell_qty_boundary_kind'),
                'none'
            ) AS sell_qty_boundary_kind,
            requested_qty,
            normalized_qty,
            market_price,
            COALESCE(
                json_extract(context_json, '$.open_exposure_qty'),
                json_extract(context_json, '$.sell_open_exposure_qty'),
                0.0
            ) AS open_exposure_qty,
            COALESCE(
                json_extract(context_json, '$.dust_tracking_qty'),
                json_extract(context_json, '$.sell_dust_tracking_qty'),
                0.0
            ) AS dust_tracking_qty,
            COALESCE(
                json_extract(context_json, '$.sell_failure_detail'),
                json_extract(context_json, '$.sell_failure_category'),
                reason
            ) AS sell_failure_detail,
            COALESCE(
                json_extract(context_json, '$.operator_action'),
                json_extract(context_json, '$.dust_action'),
                json_extract(context_json, '$.dust_operator_action'),
                '-'
            ) AS operator_action,
            dust_state,
            dust_action,
            summary
        FROM order_suppressions
        WHERE side='SELL'
        ORDER BY event_ts DESC, updated_ts DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    summaries: list[SellSuppressionSummary] = []
    for row in rows:
        summaries.append(
            SellSuppressionSummary(
                event_ts=int(row["event_ts"] or 0),
                strategy_name=str(row["strategy_name"] or "<unknown>"),
                signal=str(row["signal"] or "-"),
                side=str(row["side"] or "SELL"),
                reason_code=str(row["reason_code"] or "-"),
                suppression_category=_sell_failure_category_from_observability(
                    submission_reason_code=str(row["reason_code"] or ""),
                    message=str(row["summary"] or ""),
                    submit_evidence=None,
                    dust_details={
                        "sell_failure_category": row["sell_failure_category"],
                        "sell_qty_boundary_kind": str(row["sell_qty_boundary_kind"] or ""),
                        "sell_qty_basis_qty": row["sell_qty_basis_qty"],
                        "sell_failure_detail": row["sell_failure_detail"],
                        "reason_code": row["reason_code"],
                        "summary": row["summary"],
                    },
                ),
                sell_submit_lot_count=int(row["sell_submit_lot_count"] or 0),
                sell_qty_basis_qty=(float(row["sell_qty_basis_qty"]) if row["sell_qty_basis_qty"] is not None else None),
                sell_qty_boundary_kind=(str(row["sell_qty_boundary_kind"]) if row["sell_qty_boundary_kind"] is not None else None),
                requested_qty=(float(row["requested_qty"]) if row["requested_qty"] is not None else None),
                normalized_qty=(float(row["normalized_qty"]) if row["normalized_qty"] is not None else None),
                market_price=(float(row["market_price"]) if row["market_price"] is not None else None),
                open_exposure_qty=(float(row["open_exposure_qty"]) if row["open_exposure_qty"] is not None else None),
                dust_tracking_qty=(float(row["dust_tracking_qty"]) if row["dust_tracking_qty"] is not None else None),
                sell_failure_detail=str(row["sell_failure_detail"] or "-"),
                operator_action=(str(row["operator_action"]) if row["operator_action"] is not None else None),
                dust_state=(str(row["dust_state"]) if row["dust_state"] is not None else None),
                dust_action=(str(row["dust_action"]) if row["dust_action"] is not None else None),
                summary=(str(row["summary"]) if row["summary"] is not None else None),
            )
        )
    return summaries


def _derive_buy_flow_state(*, raw_signal: str, final_signal: str, entry_blocked: bool) -> str:
    raw = str(raw_signal or "").strip().upper()
    final = str(final_signal or "").strip().upper()
    if raw == "BUY":
        if final == "BUY":
            return "BUY_BLOCKED" if entry_blocked else "BUY_SUBMIT"
        if final == "HOLD":
            return "BUY_BLOCKED" if entry_blocked else "BUY_NO_OP"
        return f"BUY_{final or 'UNKNOWN'}"
    if raw == "SELL":
        if final == "SELL":
            return "SELL_SUPPRESSED" if entry_blocked else "SELL_SUBMIT"
        if final == "HOLD":
            return "SELL_SUPPRESSED" if entry_blocked else "SELL_NO_OP"
        return f"SELL_{final or 'UNKNOWN'}"
    return final or "UNKNOWN"


def _load_json_dict(raw_json: str | None) -> dict[str, object]:
    if not raw_json:
        return {}
    try:
        value = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _sell_failure_category_from_observability(
    *,
    submission_reason_code: str | None,
    message: str | None,
    submit_evidence: str | None,
    dust_details: dict[str, object] | None = None,
) -> str:
    evidence = _load_json_dict(submit_evidence)
    detail_text = " ".join(
        part
        for part in (
            str(submission_reason_code or "").strip(),
            str(message or "").strip(),
            str(evidence.get("reason") or "").strip(),
            str(evidence.get("summary") or "").strip(),
            str(evidence.get("sell_failure_category") or "").strip(),
            str(evidence.get("sell_failure_detail") or "").strip(),
        )
        if part
    ).lower()
    if str(submission_reason_code or "").strip() == RISKY_ORDER_BLOCK:
        if "unresolved_risk_gate" in detail_text or "reason_detail_code=open_order_timeout" in detail_text:
            return SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE
        if "submission_halt" in detail_text or "runtime halted" in detail_text:
            return SELL_FAILURE_CATEGORY_SUBMISSION_HALT
    return classify_sell_failure_category(
        reason_code=submission_reason_code,
        reason=message,
        error_summary=str(evidence.get("error_summary") or None) if evidence else None,
        dust_details={
            **evidence,
            **(dust_details or {}),
        },
    )

def fetch_recent_decision_flow(
    conn: sqlite3.Connection,
    *,
    limit: int,
) -> list[RecentDecisionFlowSummary]:
    rows = conn.execute(
        """
        SELECT
            id AS decision_id,
            decision_ts,
            strategy_name,
            context_json,
            COALESCE(json_extract(context_json, '$.decision_type'), signal) AS decision_type,
            COALESCE(json_extract(context_json, '$.base_signal'), json_extract(context_json, '$.raw_signal'), signal) AS base_signal,
            COALESCE(json_extract(context_json, '$.raw_signal'), json_extract(context_json, '$.base_signal'), signal) AS raw_signal,
            COALESCE(json_extract(context_json, '$.final_signal'), signal) AS final_signal,
            COALESCE(json_extract(context_json, '$.final_action'), json_extract(context_json, '$.decision_summary.final_action'), 'STRATEGY_HOLD') AS final_action,
            COALESCE(
                CAST(json_extract(context_json, '$.submit_expected') AS INTEGER),
                CAST(json_extract(context_json, '$.decision_summary.submit_expected') AS INTEGER),
                0
            ) AS submit_expected,
            COALESCE(json_extract(context_json, '$.pre_submit_proof_status'), json_extract(context_json, '$.decision_summary.pre_submit_proof_status'), 'not_required') AS pre_submit_proof_status,
            COALESCE(
                NULLIF(json_extract(context_json, '$.execution_block_reason'), ''),
                NULLIF(json_extract(context_json, '$.decision_summary.execution_block_reason'), ''),
                NULLIF(json_extract(context_json, '$.execution_decision.block_reason'), ''),
                'none'
            ) AS execution_block_reason,
            json_extract(context_json, '$.execution_decision.target_exposure_krw') AS target_exposure_krw,
            json_extract(context_json, '$.execution_decision.current_effective_exposure_krw') AS current_effective_exposure_krw,
            json_extract(context_json, '$.execution_decision.tracked_residual_exposure_krw') AS tracked_residual_exposure_krw,
            json_extract(context_json, '$.execution_decision.buy_delta_krw') AS buy_delta_krw,
            COALESCE(json_extract(context_json, '$.residual_live_sell_mode'), json_extract(context_json, '$.execution_decision.residual_live_sell_mode'), '-') AS residual_live_sell_mode,
            COALESCE(json_extract(context_json, '$.residual_buy_sizing_mode'), json_extract(context_json, '$.execution_decision.residual_buy_sizing_mode'), '-') AS residual_buy_sizing_mode,
            COALESCE(
                CAST(json_extract(context_json, '$.entry_blocked') AS INTEGER),
                CASE
                    WHEN COALESCE(json_extract(context_json, '$.raw_signal'), json_extract(context_json, '$.base_signal'), signal) IN ('BUY', 'SELL')
                     AND COALESCE(json_extract(context_json, '$.final_signal'), signal) != COALESCE(json_extract(context_json, '$.raw_signal'), json_extract(context_json, '$.base_signal'), signal)
                    THEN 1
                    ELSE 0
                END
            ) AS entry_blocked,
            COALESCE(
                CAST(json_extract(context_json, '$.entry_allowed') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.normalized_exposure.entry_allowed') AS INTEGER),
                CAST(json_extract(context_json, '$.position_gate.entry_allowed') AS INTEGER),
                CAST(json_extract(context_json, '$.position_gate.effective_flat_due_to_harmless_dust') AS INTEGER),
                0
            ) AS entry_allowed,
            COALESCE(
                NULLIF(json_extract(context_json, '$.decision_entry_block_reason'), ''),
                NULLIF(json_extract(context_json, '$.entry_block_reason'), ''),
                NULLIF(json_extract(context_json, '$.block_reason'), ''),
                NULLIF(json_extract(context_json, '$.entry_reason'), ''),
                NULLIF(json_extract(context_json, '$.reason'), ''),
                reason
            ) AS block_reason,
            COALESCE(
                json_extract(context_json, '$.dust_classification'),
                json_extract(context_json, '$.position_gate.dust_classification'),
                json_extract(context_json, '$.position_gate.dust_state'),
                ''
            ) AS dust_classification,
            COALESCE(
                CAST(json_extract(context_json, '$.effective_flat') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.effective_flat') AS INTEGER),
                CAST(json_extract(context_json, '$.position_gate.effective_flat_due_to_harmless_dust') AS INTEGER),
                0
            ) AS effective_flat,
            COALESCE(
                json_extract(context_json, '$.position_state.normalized_exposure.raw_qty_open'),
                json_extract(context_json, '$.position_state.raw_qty_open'),
                json_extract(context_json, '$.raw_qty_open'),
                json_extract(context_json, '$.position_gate.raw_qty_open'),
                0.0
            ) AS raw_qty_open,
            COALESCE(
                json_extract(context_json, '$.position_state.normalized_exposure.raw_total_asset_qty'),
                json_extract(context_json, '$.position_state.raw_total_asset_qty'),
                json_extract(context_json, '$.raw_total_asset_qty'),
                json_extract(context_json, '$.position_gate.raw_total_asset_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.raw_qty_open'),
                json_extract(context_json, '$.raw_qty_open'),
                0.0
            ) AS raw_total_asset_qty,
            COALESCE(
                json_extract(context_json, '$.observed_position_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.open_exposure_qty'),
                json_extract(context_json, '$.position_state.open_exposure_qty'),
                json_extract(context_json, '$.open_exposure_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.position_qty'),
                json_extract(context_json, '$.position_gate.open_exposure_qty'),
                json_extract(context_json, '$.position_qty'),
                0.0
            ) AS position_qty,
            COALESCE(
                json_extract(context_json, '$.observed_submit_payload_qty'),
                json_extract(context_json, '$.submit_payload_qty'),
                json_extract(context_json, '$.position_state.submit_payload_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.submit_payload_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.sell_normalized_exposure_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.open_exposure_qty'),
                json_extract(context_json, '$.open_exposure_qty'),
                json_extract(context_json, '$.normalized_exposure_qty'),
                0.0
            ) AS submit_payload_qty,
            COALESCE(
                CAST(json_extract(context_json, '$.normalized_exposure_active') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.normalized_exposure_active') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.normalized_exposure.normalized_exposure_active') AS INTEGER),
                CAST(json_extract(context_json, '$.position_gate.normalized_exposure_active') AS INTEGER),
                CASE
                    WHEN COALESCE(
                        json_extract(context_json, '$.open_exposure_qty'),
                        json_extract(context_json, '$.position_state.open_exposure_qty'),
                        json_extract(context_json, '$.position_state.normalized_exposure.open_exposure_qty'),
                        json_extract(context_json, '$.position_state.normalized_exposure.position_qty'),
                        json_extract(context_json, '$.position_gate.open_exposure_qty'),
                        0.0
                    ) > 0
                    AND COALESCE(
                        CAST(json_extract(context_json, '$.effective_flat') AS INTEGER),
                        CAST(json_extract(context_json, '$.position_gate.effective_flat_due_to_harmless_dust') AS INTEGER),
                        0
                    ) = 0
                    THEN 1
                    ELSE 0
                END
            ) AS normalized_exposure_active,
            COALESCE(
                json_extract(context_json, '$.position_state.normalized_exposure.normalized_exposure_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure_qty'),
                json_extract(context_json, '$.normalized_exposure_qty'),
                json_extract(context_json, '$.position_gate.normalized_exposure_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.open_exposure_qty'),
                CASE
                    WHEN COALESCE(
                        CAST(json_extract(context_json, '$.normalized_exposure_active') AS INTEGER),
                        CAST(json_extract(context_json, '$.position_state.normalized_exposure_active') AS INTEGER),
                        CAST(json_extract(context_json, '$.position_state.normalized_exposure.normalized_exposure_active') AS INTEGER),
                        CAST(json_extract(context_json, '$.position_gate.normalized_exposure_active') AS INTEGER),
                        CASE
                            WHEN COALESCE(
                                json_extract(context_json, '$.position_state.normalized_exposure.open_exposure_qty'),
                                json_extract(context_json, '$.position_state.open_exposure_qty'),
                                json_extract(context_json, '$.open_exposure_qty'),
                                json_extract(context_json, '$.position_state.normalized_exposure.position_qty'),
                                json_extract(context_json, '$.position_gate.open_exposure_qty'),
                                0.0
                            ) > 0
                            AND COALESCE(
                                CAST(json_extract(context_json, '$.effective_flat') AS INTEGER),
                                CAST(json_extract(context_json, '$.position_gate.effective_flat_due_to_harmless_dust') AS INTEGER),
                                0
                            ) = 0
                            THEN 1
                            ELSE 0
                        END
                    ) = 1
                    THEN COALESCE(
                        json_extract(context_json, '$.position_state.normalized_exposure.open_exposure_qty'),
                        json_extract(context_json, '$.position_state.open_exposure_qty'),
                        json_extract(context_json, '$.open_exposure_qty'),
                        json_extract(context_json, '$.position_state.normalized_exposure.position_qty'),
                        json_extract(context_json, '$.position_gate.open_exposure_qty'),
                        0.0
                    )
                    ELSE 0.0
                END
            ) AS normalized_exposure_qty,
            COALESCE(
                json_extract(context_json, '$.position_state.normalized_exposure.open_exposure_qty'),
                json_extract(context_json, '$.position_state.open_exposure_qty'),
                json_extract(context_json, '$.open_exposure_qty'),
                json_extract(context_json, '$.position_gate.open_exposure_qty'),
                0.0
            ) AS open_exposure_qty,
            COALESCE(
                json_extract(context_json, '$.position_state.normalized_exposure.dust_tracking_qty'),
                json_extract(context_json, '$.position_state.dust_tracking_qty'),
                json_extract(context_json, '$.dust_tracking_qty'),
                json_extract(context_json, '$.position_gate.dust_tracking_qty'),
                0.0
            ) AS dust_tracking_qty,
            COALESCE(
                json_extract(context_json, '$.sell_open_exposure_qty'),
                json_extract(context_json, '$.position_state.sell_open_exposure_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.sell_open_exposure_qty'),
                json_extract(context_json, '$.open_exposure_qty'),
                0.0
            ) AS sell_open_exposure_qty,
            COALESCE(
                json_extract(context_json, '$.sell_dust_tracking_qty'),
                json_extract(context_json, '$.position_state.sell_dust_tracking_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.sell_dust_tracking_qty'),
                json_extract(context_json, '$.dust_tracking_qty'),
                0.0
            ) AS sell_dust_tracking_qty,
            COALESCE(
                json_extract(context_json, '$.observed_sell_qty_basis_qty'),
                json_extract(context_json, '$.sell_qty_basis_qty'),
                json_extract(context_json, '$.position_state.sell_qty_basis_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_basis_qty'),
                0.0
            ) AS sell_qty_basis_qty,
            COALESCE(
                json_extract(context_json, '$.sell_qty_boundary_kind'),
                json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind'),
                'none'
            ) AS sell_qty_boundary_kind,
            COALESCE(
                CAST(json_extract(context_json, '$.sell_submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.sell_submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.normalized_exposure.sell_submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.position_state.normalized_exposure.submit_lot_count') AS INTEGER),
                CAST(json_extract(context_json, '$.position_gate.submit_lot_count') AS INTEGER),
                0
            ) AS sell_submit_lot_count,
            COALESCE(
                json_extract(context_json, '$.sell_normalized_exposure_qty'),
                json_extract(context_json, '$.position_state.sell_normalized_exposure_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.sell_normalized_exposure_qty'),
                json_extract(context_json, '$.position_state.normalized_exposure.normalized_exposure_qty'),
                json_extract(context_json, '$.position_gate.normalized_exposure_qty'),
                json_extract(context_json, '$.open_exposure_qty'),
                json_extract(context_json, '$.normalized_exposure_qty'),
                0.0
            ) AS sell_normalized_exposure_qty,
            COALESCE(
                NULLIF(json_extract(context_json, '$.sell_failure_category'), 'none'),
                NULLIF(json_extract(context_json, '$.decision_summary.sell_failure_category'), 'none'),
                CASE
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'remainder_after_sell' THEN 'remainder_dust_guard'
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'min_qty' THEN 'boundary_below_min'
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'qty_step' THEN 'qty_step_mismatch'
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'dust_mismatch' THEN 'unsafe_dust_mismatch_dust'
                    ELSE NULL
                END,
                'none'
            ) AS sell_failure_category,
            COALESCE(
                NULLIF(json_extract(context_json, '$.sell_failure_detail'), 'none'),
                NULLIF(json_extract(context_json, '$.decision_summary.sell_failure_detail'), 'none'),
                NULLIF(json_extract(context_json, '$.sell_failure_category'), 'none'),
                CASE
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'remainder_after_sell' THEN 'remainder_dust_guard'
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'min_qty' THEN 'boundary_below_min'
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'qty_step' THEN 'qty_step_mismatch'
                    WHEN COALESCE(
                        json_extract(context_json, '$.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.sell_qty_boundary_kind'),
                        json_extract(context_json, '$.position_state.normalized_exposure.sell_qty_boundary_kind')
                    ) = 'dust_mismatch' THEN 'unsafe_dust_mismatch_dust'
                    ELSE NULL
                END,
                'none'
            ) AS sell_failure_detail
        FROM (
            SELECT *
            FROM strategy_decisions
            ORDER BY decision_ts DESC, id DESC
            LIMIT ?
        ) recent
        ORDER BY decision_ts DESC, decision_id DESC
        """,
        (int(max(1, limit)),),
    ).fetchall()
    summaries: list[RecentDecisionFlowSummary] = []
    for row in rows:
        context = _load_json_dict(str(row["context_json"] or ""))
        exposure = resolve_canonical_position_exposure_snapshot(context)
        target_shadow = _target_shadow_from_context(context)
        summaries.append(
            RecentDecisionFlowSummary(
                decision_id=int(row["decision_id"]),
                decision_ts=int(row["decision_ts"] or 0),
                strategy_name=str(row["strategy_name"]),
                decision_type=str(row["decision_type"]),
                base_signal=str(row["base_signal"]),
                raw_signal=str(row["raw_signal"]),
                final_signal=str(row["final_signal"]),
                final_action=str(row["final_action"]),
                submit_expected=bool(row["submit_expected"]),
                pre_submit_proof_status=str(row["pre_submit_proof_status"]),
                execution_block_reason=str(row["execution_block_reason"]),
                target_exposure_krw=(
                    None if row["target_exposure_krw"] is None else float(row["target_exposure_krw"])
                ),
                current_effective_exposure_krw=(
                    None
                    if row["current_effective_exposure_krw"] is None
                    else float(row["current_effective_exposure_krw"])
                ),
                tracked_residual_exposure_krw=(
                    None
                    if row["tracked_residual_exposure_krw"] is None
                    else float(row["tracked_residual_exposure_krw"])
                ),
                buy_delta_krw=(None if row["buy_delta_krw"] is None else float(row["buy_delta_krw"])),
                residual_live_sell_mode=str(row["residual_live_sell_mode"]),
                residual_buy_sizing_mode=str(row["residual_buy_sizing_mode"]),
                target_delta_side=str(target_shadow.get("target_delta_side") or "-"),
                target_would_submit=bool(target_shadow.get("target_would_submit")),
                target_submit_qty=(
                    None
                    if target_shadow.get("target_submit_qty") is None
                    else float(target_shadow.get("target_submit_qty") or 0.0)
                ),
                target_delta_notional_krw=(
                    None
                    if target_shadow.get("target_delta_notional_krw") is None
                    else float(target_shadow.get("target_delta_notional_krw") or 0.0)
                ),
                target_block_reason=str(target_shadow.get("target_block_reason") or "-"),
                target_position_truth_state=str(target_shadow.get("target_position_truth_state") or "-"),
                buy_flow_state=_derive_buy_flow_state(
                    raw_signal=str(row["raw_signal"]),
                    final_signal=str(row["final_signal"]),
                    entry_blocked=bool(row["entry_blocked"]),
                ),
                entry_blocked=bool(row["entry_blocked"]),
                entry_allowed=bool(row["entry_allowed"]),
                effective_flat=bool(row["effective_flat"]),
                raw_qty_open=float(exposure.raw_qty_open),
                raw_total_asset_qty=float(exposure.raw_total_asset_qty),
                position_qty=float(exposure.position_qty),
                submit_payload_qty=float(exposure.submit_payload_qty),
                normalized_exposure_active=bool(exposure.normalized_exposure_active),
                normalized_exposure_qty=float(exposure.normalized_exposure_qty),
                open_exposure_qty=float(exposure.open_exposure_qty),
                dust_tracking_qty=float(exposure.dust_tracking_qty),
                sell_open_exposure_qty=float(exposure.sell_open_exposure_qty),
                sell_dust_tracking_qty=float(exposure.sell_dust_tracking_qty),
                sell_qty_basis_qty=float(exposure.sell_qty_basis_qty),
                sell_qty_boundary_kind=str(exposure.sell_qty_boundary_kind),
                sell_submit_lot_count=int(exposure.sell_submit_lot_count),
                sell_normalized_exposure_qty=float(exposure.sell_normalized_exposure_qty),
                sell_failure_category=str(row["sell_failure_category"]),
                sell_failure_detail=str(row["sell_failure_detail"]),
                block_reason=str(row["block_reason"]),
                reason=str(row["block_reason"]),
            )
        )
    return summaries


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
        SELECT signal, reason, strategy_name, context_json
        FROM strategy_decisions
        ORDER BY decision_ts DESC, id DESC
        LIMIT ?
        """,
        (int(max(1, limit)),),
    ).fetchall()

    def _derived_sell_failure(kind: str) -> str | None:
        if kind == "remainder_after_sell":
            return SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD
        if kind == "min_qty":
            return SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN
        if kind == "qty_step":
            return SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH
        if kind == "dust_mismatch":
            return SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH
        return None

    def _decision_reason(*values: object) -> str:
        for value in values:
            text = str(value or "").strip()
            if text and text.lower() != "none":
                return text
        return "none"

    grouped: dict[tuple[object, ...], int] = {}
    for row in rows:
        try:
            context = json.loads(str(row["context_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            context = {}
        if not isinstance(context, dict):
            context = {}

        exposure = resolve_canonical_position_exposure_snapshot(context)
        base_signal = str(context.get("base_signal") or context.get("raw_signal") or row["signal"])
        decision_type = str(context.get("decision_type") or row["signal"])
        raw_signal = str(context.get("raw_signal") or context.get("base_signal") or row["signal"])
        final_signal = str(context.get("final_signal") or row["signal"])
        final_action = str(
            context.get("final_action")
            or context.get("decision_summary", {}).get("final_action")
            or "STRATEGY_HOLD"
        )
        submit_expected = bool(
            context.get("submit_expected")
            if "submit_expected" in context
            else context.get("decision_summary", {}).get("submit_expected", False)
        )
        pre_submit_proof_status = str(
            context.get("pre_submit_proof_status")
            or context.get("decision_summary", {}).get("pre_submit_proof_status")
            or "not_required"
        )
        execution_block_reason = str(
            context.get("execution_block_reason")
            or context.get("decision_summary", {}).get("execution_block_reason")
            or context.get("execution_decision", {}).get("block_reason")
            or "none"
        )
        entry_blocked = bool(
            context.get("entry_blocked")
            if "entry_blocked" in context
            else raw_signal in {"BUY", "SELL"} and final_signal != raw_signal
        )
        block_reason = _decision_reason(
            context.get("decision_entry_block_reason"),
            context.get("entry_block_reason"),
            context.get("block_reason"),
            context.get("entry_reason"),
            context.get("reason"),
            row["reason"],
        )
        strategy_name = str(context.get("strategy_name") or row["strategy_name"] or "<unknown>")
        pair = str(context.get("pair") or "<unknown>")
        interval = str(context.get("interval") or "<unknown>")
        sell_failure_category = str(
            context.get("sell_failure_category")
            or context.get("decision_summary", {}).get("sell_failure_category")
            or _derived_sell_failure(exposure.sell_qty_boundary_kind)
            or "none"
        )
        sell_failure_detail = str(
            context.get("sell_failure_detail")
            or context.get("decision_summary", {}).get("sell_failure_detail")
            or (
                sell_failure_category
                if sell_failure_category != "none"
                else _derived_sell_failure(exposure.sell_qty_boundary_kind) or "none"
            )
        )
        execution_decision = (
            context.get("execution_decision") if isinstance(context.get("execution_decision"), dict) else {}
        )
        target_shadow = _target_shadow_from_context(context)
        target_exposure_krw = execution_decision.get("target_exposure_krw")
        current_effective_exposure_krw = execution_decision.get("current_effective_exposure_krw")
        tracked_residual_exposure_krw = execution_decision.get("tracked_residual_exposure_krw")
        buy_delta_krw = execution_decision.get("buy_delta_krw")
        residual_live_sell_mode = str(
            context.get("residual_live_sell_mode")
            or execution_decision.get("residual_live_sell_mode")
            or "-"
        )
        residual_buy_sizing_mode = str(
            context.get("residual_buy_sizing_mode")
            or execution_decision.get("residual_buy_sizing_mode")
            or "-"
        )

        key = (
            base_signal,
            decision_type,
            raw_signal,
            final_signal,
            final_action,
            submit_expected,
            pre_submit_proof_status,
            execution_block_reason,
            entry_blocked,
            exposure.entry_allowed,
            strategy_name,
            pair,
            interval,
            block_reason,
            exposure.dust_classification,
            exposure.effective_flat,
            exposure.raw_qty_open,
            exposure.raw_total_asset_qty,
            exposure.position_qty,
            exposure.submit_payload_qty,
            exposure.normalized_exposure_active,
            exposure.normalized_exposure_qty,
            exposure.open_exposure_qty,
            exposure.dust_tracking_qty,
            exposure.sell_open_exposure_qty,
            exposure.sell_dust_tracking_qty,
            exposure.sell_qty_basis_qty,
            exposure.sell_qty_boundary_kind,
            sell_failure_category,
            sell_failure_detail,
            exposure.sell_submit_lot_count,
            exposure.sell_normalized_exposure_qty,
            target_exposure_krw,
            current_effective_exposure_krw,
            tracked_residual_exposure_krw,
            buy_delta_krw,
            residual_live_sell_mode,
            residual_buy_sizing_mode,
            target_shadow.get("target_delta_side"),
            bool(target_shadow.get("target_would_submit")),
            target_shadow.get("target_submit_qty"),
            target_shadow.get("target_delta_notional_krw"),
            target_shadow.get("target_block_reason"),
            target_shadow.get("target_position_truth_state"),
        )
        grouped[key] = grouped.get(key, 0) + 1

    summaries = [
            DecisionTelemetrySummary(
                base_signal=str(key[0]),
                decision_type=str(key[1]),
                raw_signal=str(key[2]),
                final_signal=str(key[3]),
                final_action=str(key[4]),
                submit_expected=bool(key[5]),
                pre_submit_proof_status=str(key[6]),
                execution_block_reason=str(key[7]),
                target_exposure_krw=(None if key[32] is None else float(key[32])),
                current_effective_exposure_krw=(None if key[33] is None else float(key[33])),
                tracked_residual_exposure_krw=(None if key[34] is None else float(key[34])),
                buy_delta_krw=(None if key[35] is None else float(key[35])),
                residual_live_sell_mode=str(key[36]),
                residual_buy_sizing_mode=str(key[37]),
                target_delta_side=str(key[38] or "-"),
                target_would_submit=bool(key[39]),
                target_submit_qty=(None if key[40] is None else float(key[40])),
                target_delta_notional_krw=(None if key[41] is None else float(key[41])),
                target_block_reason=str(key[42] or "-"),
                target_position_truth_state=str(key[43] or "-"),
                buy_flow_state=_derive_buy_flow_state(
                    raw_signal=str(key[2]),
                    final_signal=str(key[3]),
                    entry_blocked=bool(key[8]),
                ),
                entry_blocked=bool(key[8]),
                entry_allowed=bool(key[9]),
                block_reason=str(key[13]),
                dust_classification=str(key[14]),
                effective_flat=bool(key[15]),
                raw_qty_open=float(key[16]),
                raw_total_asset_qty=float(key[17]),
                position_qty=float(key[18]),
                submit_payload_qty=float(key[19]),
                normalized_exposure_active=bool(key[20]),
                normalized_exposure_qty=float(key[21]),
                open_exposure_qty=float(key[22]),
                dust_tracking_qty=float(key[23]),
                sell_open_exposure_qty=float(key[24]),
                sell_dust_tracking_qty=float(key[25]),
                sell_qty_basis_qty=float(key[26]),
                sell_qty_boundary_kind=str(key[27]),
                sell_submit_lot_count=int(key[30]),
                sell_normalized_exposure_qty=float(key[31]),
                sell_failure_category=str(key[28]),
                sell_failure_detail=str(key[29]),
                strategy_name=str(key[10]),
                pair=str(key[11]),
                interval=str(key[12]),
                count=count,
            )
        for key, count in grouped.items()
    ]
    summaries.sort(
        key=lambda item: (
            -item.count,
            item.decision_type,
            item.base_signal,
            item.raw_signal,
            item.final_signal,
            item.strategy_name,
            item.pair,
            item.interval,
        )
    )
    return summaries


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


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    ratio = min(1.0, max(0.0, float(q)))
    index = int(round((len(ordered) - 1) * ratio))
    return float(ordered[index])


def _build_return_distribution(values: list[float]) -> dict[str, float | None]:
    return {
        "min_bps": float(min(values)) if values else None,
        "p10_bps": _percentile(values, 0.10),
        "median_bps": median(values) if values else None,
        "p90_bps": _percentile(values, 0.90),
        "max_bps": float(max(values)) if values else None,
        "avg_bps": float(sum(values) / len(values)) if values else None,
    }


def _empty_outcome_bucket() -> dict[str, float | int | bool | None]:
    return {
        "blocked_count": 0,
        "observed_count": 0,
        "avoided_loss_count": 0,
        "opportunity_missed_count": 0,
        "flat_or_unknown_count": 0,
        "avoided_loss_ratio": None,
        "opportunity_missed_ratio": None,
        "flat_or_unknown_ratio": None,
        "avg_return_bps": None,
        "median_return_bps": None,
        "insufficient_sample": True,
    }


def _finalize_outcome_breakdown(
    source: dict[str, dict[str, float | int | bool | None | list[float]]],
    *,
    sample_threshold: int,
) -> dict[str, dict[str, float | int | bool | None]]:
    finalized: dict[str, dict[str, float | int | bool | None]] = {}
    for key in sorted(source):
        bucket = dict(source[key])
        blocked_count = int(bucket.get("blocked_count") or 0)
        observed_count = int(bucket.get("observed_count") or 0)
        avoided_count = int(bucket.get("avoided_loss_count") or 0)
        missed_count = int(bucket.get("opportunity_missed_count") or 0)
        flat_count = int(bucket.get("flat_or_unknown_count") or 0)
        returns = list(bucket.get("_returns") or [])
        denominator = blocked_count if blocked_count > 0 else 1
        bucket["avoided_loss_ratio"] = avoided_count / denominator
        bucket["opportunity_missed_ratio"] = missed_count / denominator
        bucket["flat_or_unknown_ratio"] = flat_count / denominator
        bucket["avg_return_bps"] = float(sum(returns) / len(returns)) if returns else None
        bucket["median_return_bps"] = median(returns) if returns else None
        bucket["insufficient_sample"] = observed_count < sample_threshold
        bucket.pop("_returns", None)
        finalized[key] = bucket
    return finalized


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
    sample_threshold = max(1, int(min_observation_sample))
    observed_returns_bps: list[float] = []
    avoided_loss_count = 0
    opportunity_missed_count = 0
    flat_or_unknown_count = 0
    blocked_returns_by_filter: dict[str, list[float]] = {}
    blocked_outcome_by_filter: dict[str, dict[str, float | int | bool | None | list[float]]] = {}
    blocked_outcome_by_signal_strength: dict[str, dict[str, float | int | bool | None | list[float]]] = {}
    blocked_outcome_by_market_bucket: dict[str, dict[str, float | int | bool | None | list[float]]] = {}

    for row in blocked_rows:
        context = _load_context_json(row["context_json"])
        interval_text = str(context.get("interval") or settings.INTERVAL)
        try:
            interval_ms = parse_interval_sec(interval_text) * 1000
        except ValueError:
            interval_ms = default_interval_ms

        blocked_filters = _extract_blocked_filters(row["context_json"])
        if not blocked_filters:
            blocked_filters = ["<unspecified>"]
        signal_strength_label = str(context.get("signal_strength_label") or "unknown").strip().lower() or "unknown"
        analysis = normalize_analysis_context_from_decision_row(row)
        market_bucket = str(analysis.get("buckets", {}).get("volatility") or "unknown")
        decision_price = float(row["market_price"]) if row["market_price"] is not None else None
        decision_candle_ts = int(row["candle_ts"]) if row["candle_ts"] is not None else None
        observed_return_bps: float | None = None
        outcome_label = "flat_or_unknown"
        if decision_price is None or decision_price <= 0 or decision_candle_ts is None:
            flat_or_unknown_count += 1
        else:
            target_ts = decision_candle_ts + (interval_ms * observation_bars)
            observed_price = close_by_ts.get(target_ts)
            if observed_price is None or observed_price <= 0:
                flat_or_unknown_count += 1
            else:
                observed_return_bps = ((observed_price - decision_price) / decision_price) * 10000.0
                observed_returns_bps.append(observed_return_bps)
                if observed_return_bps < 0:
                    avoided_loss_count += 1
                    outcome_label = "avoided_loss"
                elif observed_return_bps > 0:
                    opportunity_missed_count += 1
                    outcome_label = "opportunity_missed"
                else:
                    flat_or_unknown_count += 1

        for filter_name in blocked_filters:
            filter_bucket = blocked_outcome_by_filter.setdefault(filter_name, _empty_outcome_bucket())
            filter_bucket["blocked_count"] = int(filter_bucket["blocked_count"]) + 1
            filter_bucket[outcome_label + "_count"] = int(filter_bucket[outcome_label + "_count"]) + 1
            if observed_return_bps is not None:
                filter_bucket["observed_count"] = int(filter_bucket["observed_count"]) + 1
                filter_bucket.setdefault("_returns", []).append(observed_return_bps)
                blocked_returns_by_filter.setdefault(filter_name, []).append(observed_return_bps)

        for label, bucket_source in (
            (signal_strength_label, blocked_outcome_by_signal_strength),
            (market_bucket, blocked_outcome_by_market_bucket),
        ):
            target_bucket = bucket_source.setdefault(label, _empty_outcome_bucket())
            target_bucket["blocked_count"] = int(target_bucket["blocked_count"]) + 1
            target_bucket[outcome_label + "_count"] = int(target_bucket[outcome_label + "_count"]) + 1
            if observed_return_bps is not None:
                target_bucket["observed_count"] = int(target_bucket["observed_count"]) + 1
                target_bucket.setdefault("_returns", []).append(observed_return_bps)

    avg_return_bps = (
        float(sum(observed_returns_bps) / len(observed_returns_bps)) if observed_returns_bps else None
    )
    median_return_bps = median(observed_returns_bps) if observed_returns_bps else None
    insufficient_sample = len(observed_returns_bps) < sample_threshold
    return_distribution_bps = _build_return_distribution(observed_returns_bps)
    blocked_return_distribution_by_filter = {
        key: _build_return_distribution(values) for key, values in sorted(blocked_returns_by_filter.items())
    }
    finalized_blocked_by_filter = _finalize_outcome_breakdown(
        blocked_outcome_by_filter,
        sample_threshold=sample_threshold,
    )
    for key, distribution in blocked_return_distribution_by_filter.items():
        if key in finalized_blocked_by_filter:
            finalized_blocked_by_filter[key]["return_distribution_bps"] = distribution
    finalized_by_signal_strength = _finalize_outcome_breakdown(
        blocked_outcome_by_signal_strength,
        sample_threshold=sample_threshold,
    )
    finalized_by_market_bucket = _finalize_outcome_breakdown(
        blocked_outcome_by_market_bucket,
        sample_threshold=sample_threshold,
    )

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
    notes.append("blocked outcome breakdowns are explanatory observations, not execution or realized-pnl claims")

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
            return_distribution_bps=return_distribution_bps,
            blocked_outcome_by_filter=finalized_blocked_by_filter,
            blocked_outcome_by_signal_strength=finalized_by_signal_strength,
            blocked_outcome_by_market_bucket=finalized_by_market_bucket,
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
    fee_authority_payload: dict[str, object] | None = None
    if estimated_fee_rate is None and settings.MODE == "live":
        try:
            fee_authority = resolve_fee_authority_snapshot(settings.PAIR)
            estimate = float(fee_authority.taker_roundtrip_fee_rate / 2)
            fee_authority_payload = fee_authority.as_dict()
        except Exception as exc:
            estimate = settings.LIVE_FEE_RATE_ESTIMATE
            fee_authority_payload = {
                "unavailable": True,
                "fee_source": "config_estimate_degraded",
                "error": f"{type(exc).__name__}: {exc}",
            }
    else:
        estimate = (
            settings.PAPER_FEE_RATE
            if estimated_fee_rate is None
            else float(estimated_fee_rate)
        )
    estimate = (
        float(estimate)
    )
    conn = ensure_db()
    try:
        summary = fetch_fee_diagnostics(
            conn,
            fill_limit=fill_limit,
            roundtrip_limit=roundtrip_limit,
            estimated_fee_rate=estimate,
        )
        fee_rate_drift = build_fee_rate_drift_diagnostics(conn)
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
        "fee_rate_drift": fee_rate_drift,
        "roundtrip": {
            "total_fee": summary.roundtrip_fee_total,
            "pnl_before_fee": summary.pnl_before_fee_total,
            "pnl_after_fee": summary.pnl_after_fee_total,
            "pnl_fee_drag": summary.pnl_fee_drag_total,
        },
        "notes": summary.notes,
    }
    fee_model_validation_source = str(
        (fee_authority_payload or {}).get("fee_source")
        or ("manual_override" if estimated_fee_rate is not None else ("paper_default" if settings.MODE != "live" else "manual_or_paper"))
    )
    fee_model_validation: dict[str, object] = {
        "configured_fee_rate": float(fee_rate_drift.get("configured_fee_rate") or 0.0),
        "configured_fee_bps": float(fee_rate_drift.get("configured_fee_bps") or 0.0),
        "estimated_fee_rate": summary.estimated_fee_rate,
        "estimated_fee_rate_source": fee_model_validation_source,
        "estimated_fee_rate_semantics": (
            "exchange_chance_doc_diagnostic_not_configured_live_fee"
            if fee_model_validation_source == "chance_doc"
            else "diagnostic_estimate_not_settlement_authority"
        ),
        "estimated_minus_actual_bps": summary.estimated_minus_actual_bps,
        "fee_model_validation_source": fee_model_validation_source,
        "settlement_authority": "exchange_paid_fee_when_coherent",
        "operator_note": (
            "chance_doc_fee_rate_is_diagnostic_not_settlement_authority"
            if fee_model_validation_source == "chance_doc"
            else "fee_model_estimate_is_diagnostic_not_settlement_authority"
        ),
        "fee_authority": fee_authority_payload,
    }
    if fee_model_validation_source == "chance_doc":
        fee_model_validation["exchange_chance_doc_fee_rate"] = summary.estimated_fee_rate
        fee_model_validation["exchange_chance_doc_fee_bps"] = summary.estimated_fee_rate * 10000.0
    payload["fee_model_validation"] = fee_model_validation

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
        f"configured_fee_rate={float(fee_model_validation.get('configured_fee_rate') or 0.0):.6f} "
        f"configured_fee_bps={float(fee_model_validation.get('configured_fee_bps') or 0.0):.3f} "
        f"estimated_fee_rate={summary.estimated_fee_rate:.6f} "
        f"estimated_fee_rate_source={fee_model_validation.get('estimated_fee_rate_source') or 'unknown'} "
        f"estimated_fee_rate_semantics={fee_model_validation.get('estimated_fee_rate_semantics') or 'unknown'} "
        f"estimated_minus_actual_bps={_fmt_rate(summary.estimated_minus_actual_bps, as_bps=True)} "
        f"fee_authority_source={fee_model_validation.get('fee_model_validation_source') or 'unknown'}"
    )
    if fee_model_validation.get("exchange_chance_doc_fee_rate") is not None:
        print(
            "  "
            f"exchange_chance_doc_fee_rate={float(fee_model_validation.get('exchange_chance_doc_fee_rate') or 0.0):.6f} "
            f"exchange_chance_doc_fee_bps={float(fee_model_validation.get('exchange_chance_doc_fee_bps') or 0.0):.3f} "
            f"settlement_authority={fee_model_validation.get('settlement_authority') or 'unknown'} "
            f"operator_note={fee_model_validation.get('operator_note') or 'none'}"
        )
    observed_fee_bps_text = (
        "-"
        if fee_rate_drift.get("observed_fee_bps_median") is None
        else f"{float(fee_rate_drift.get('observed_fee_bps_median')):.3f} bps"
    )
    deviation_pct_text = (
        "-"
        if fee_rate_drift.get("fee_rate_deviation_pct") is None
        else f"{float(fee_rate_drift.get('fee_rate_deviation_pct')):.2f}%"
    )
    print("\n[FEE-RATE-DRIFT]")
    print(
        "  "
        f"configured_fee_rate={float(fee_rate_drift.get('configured_fee_rate') or 0.0):.6f} "
        f"configured_fee_bps={float(fee_rate_drift.get('configured_fee_bps') or 0.0):.3f} "
        f"observed_fee_bps_median={observed_fee_bps_text} "
        f"observed_fee_sample_count={int(fee_rate_drift.get('observed_fee_sample_count') or 0)} "
        f"fee_rate_deviation_pct={deviation_pct_text}"
    )
    print(
        "  "
        f"expected_fee_rate_warning_count={int(fee_rate_drift.get('expected_fee_rate_warning_count') or 0)} "
        f"active_unresolved_fee_pending_count={int(fee_rate_drift.get('active_unresolved_fee_pending_count') or 0)} "
        f"historical_fee_pending_observation_count={int(fee_rate_drift.get('historical_fee_pending_observation_count') or 0)} "
        f"recent_fee_pending_observation_count={int(fee_rate_drift.get('recent_fee_pending_observation_count') or 0)} "
        f"repaired_fee_pending_incident_count={int(fee_rate_drift.get('repaired_fee_pending_incident_count') or 0)} "
        f"broker_fill_latest_unresolved_fee_pending_count={int(fee_rate_drift.get('broker_fill_latest_unresolved_fee_pending_count') or 0)} "
        f"fill_accounting_active_issue_count={int(fee_rate_drift.get('fill_accounting_active_issue_count') or 0)} "
        f"unresolved_fee_state={1 if bool(fee_rate_drift.get('unresolved_fee_state')) else 0} "
        f"fee_pending_count={int(fee_rate_drift.get('fee_pending_count') or 0)} "
        f"fee_pending_count_semantics={fee_rate_drift.get('fee_pending_count_semantics') or 'unknown'} "
        f"fee_pending_accounting_repair_count={int(fee_rate_drift.get('fee_pending_accounting_repair_count') or 0)} "
        f"position_authority_repair_count={int(fee_rate_drift.get('position_authority_repair_count') or 0)} "
        f"diagnostic_only_vs_startup_blocking={fee_rate_drift.get('diagnostic_only_vs_startup_blocking') or 'unknown'} "
        f"startup_impact={fee_rate_drift.get('startup_impact') or 'unknown'} "
        f"operator_action={fee_rate_drift.get('operator_action') or 'unknown'} "
        f"recommended_command={fee_rate_drift.get('recommended_command') or 'none'}"
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
            print("  tip: ?????됰Ŧ???????????????????????뀀??????곌퇈猷??????????????쇰뮛?????산뭣???????됰Ŧ???????????????????????????????轅붽틓?????????????μ떜媛?걫?疫뀀툙????????????????됰Ŧ???????????DB???????????븐뼐??????????????ㅼ굣????")
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
                "return_distribution_bps": filter_effectiveness.observation.return_distribution_bps,
                "avoided_loss_count": filter_effectiveness.observation.avoided_loss_count,
                "opportunity_missed_count": filter_effectiveness.observation.opportunity_missed_count,
                "flat_or_unknown_count": filter_effectiveness.observation.flat_or_unknown_count,
            },
            "blocked_outcome_by_filter": filter_effectiveness.observation.blocked_outcome_by_filter,
            "blocked_outcome_by_signal_strength": filter_effectiveness.observation.blocked_outcome_by_signal_strength,
            "blocked_outcome_by_market_bucket": filter_effectiveness.observation.blocked_outcome_by_market_bucket,
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
        print("  tip: ???????? ??????????????ш끽維뽳쭩?뱀땡???얩맪????????????????????????????????????????????????????????lifecycle ????????????????獄쏅챶留덌┼?????????????????釉먮폁????????????????븐뼐??????????????ㅼ굣????")
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
    print(
        "  "
        f"blocked_return_distribution_bps="
        f"min:{_fmt_rate(filter_effectiveness.observation.return_distribution_bps.get('min_bps'), as_bps=True)},"
        f"p10:{_fmt_rate(filter_effectiveness.observation.return_distribution_bps.get('p10_bps'), as_bps=True)},"
        f"median:{_fmt_rate(filter_effectiveness.observation.return_distribution_bps.get('median_bps'), as_bps=True)},"
        f"p90:{_fmt_rate(filter_effectiveness.observation.return_distribution_bps.get('p90_bps'), as_bps=True)},"
        f"max:{_fmt_rate(filter_effectiveness.observation.return_distribution_bps.get('max_bps'), as_bps=True)}"
    )
    print("  [blocked_outcome_by_signal_strength]")
    print(
        "  signal_strength,blocked_count,observed_count,avoided_loss_ratio,"
        "opportunity_missed_ratio,flat_or_unknown_ratio"
    )
    if not filter_effectiveness.observation.blocked_outcome_by_signal_strength:
        print("  -,-,-,-,-,-")
    else:
        for bucket, stats in filter_effectiveness.observation.blocked_outcome_by_signal_strength.items():
            print(
                "  "
                f"{bucket},{stats['blocked_count']},{stats['observed_count']},"
                f"{stats['avoided_loss_ratio']:.4f},{stats['opportunity_missed_ratio']:.4f},"
                f"{stats['flat_or_unknown_ratio']:.4f}"
            )
    print("  [blocked_outcome_by_market_bucket]")
    print(
        "  market_bucket,blocked_count,observed_count,avoided_loss_ratio,"
        "opportunity_missed_ratio,flat_or_unknown_ratio"
    )
    if not filter_effectiveness.observation.blocked_outcome_by_market_bucket:
        print("  -,-,-,-,-,-")
    else:
        for bucket, stats in filter_effectiveness.observation.blocked_outcome_by_market_bucket.items():
            print(
                "  "
                f"{bucket},{stats['blocked_count']},{stats['observed_count']},"
                f"{stats['avoided_loss_ratio']:.4f},{stats['opportunity_missed_ratio']:.4f},"
                f"{stats['flat_or_unknown_ratio']:.4f}"
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
            "note": "ops-report/health/recovery ?????됰Ŧ?????????? ??????????????????????????깅♥???expectancy ????留⑶뜮????????????????ル탛????耀붿빓??????????釉먮폁???????????????щⅨ????????????쇨덫櫻?"
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
    reserved_exit_qty = 0.0
    conn = ensure_db()
    try:
        strategy_stats = _fetch_strategy_stats(conn)
        recent_flow = _fetch_recent_flow(conn, limit=max(1, int(limit)))
        recent_sell_suppressions = _fetch_recent_sell_suppressions(conn, limit=max(1, int(limit)))
        recent_decision_flow = fetch_recent_decision_flow(conn, limit=max(1, int(limit)))
        recent_trades = _fetch_recent_trade_ops(conn, limit=max(1, int(limit)))
        fee_summary = fetch_fee_diagnostics(
            conn,
            fill_limit=max(1, int(limit)),
            roundtrip_limit=max(1, int(limit)),
            estimated_fee_rate=float(settings.FEE_RATE),
        )
        recovery_attribution_signals = fetch_recovery_attribution_signal_summary(conn)
        recent_external_cash_adjustment = get_external_cash_adjustment_summary(conn)
        health_row = conn.execute(
            """
            SELECT
                unresolved_open_order_count,
                oldest_unresolved_order_age_sec,
                recovery_required_count,
                last_reconcile_epoch_sec,
                last_reconcile_status,
                last_reconcile_reason_code,
                last_reconcile_metadata,
                last_disable_reason,
                halt_reason_code,
                halt_state_unresolved
            FROM bot_health
            WHERE id=1
            """
        ).fetchone()
        readiness_snapshot = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    run_lock_status = read_run_lock_status(Path(settings.RUN_LOCK_PATH))

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
        "recent_sell_suppressions": [
            {
                "event_ts": row.event_ts,
                "strategy_name": row.strategy_name,
                "signal": row.signal,
                "side": row.side,
                "reason_code": row.reason_code,
                "suppression_category": row.suppression_category,
                "requested_qty": row.requested_qty,
                "normalized_qty": row.normalized_qty,
                "market_price": row.market_price,
                "observed_sell_qty_basis_qty": row.sell_qty_basis_qty,
                "sell_qty_boundary_kind": row.sell_qty_boundary_kind,
                "sell_submit_lot_count": row.sell_submit_lot_count,
                "dust_state": row.dust_state,
                "dust_action": row.dust_action,
                "operator_action": row.operator_action,
                "summary": row.summary,
            }
            for row in recent_sell_suppressions
        ],
        "recent_decision_flow": [
            {
                "decision_id": row.decision_id,
                "decision_ts": row.decision_ts,
                "strategy_name": row.strategy_name,
                "decision_type": row.decision_type,
                "base_signal": row.base_signal,
                "raw_signal": row.raw_signal,
                "final_signal": row.final_signal,
                "buy_flow_state": row.buy_flow_state,
                "entry_blocked": row.entry_blocked,
                "entry_allowed": row.entry_allowed,
                "effective_flat": row.effective_flat,
                "raw_qty_open": row.raw_qty_open,
                "raw_total_asset_qty": row.raw_total_asset_qty,
                "observed_position_qty": row.position_qty,
                "position_qty": row.position_qty,
                "observed_submit_payload_qty": row.submit_payload_qty,
                "submit_payload_qty": row.submit_payload_qty,
                "normalized_exposure_active": row.normalized_exposure_active,
                "normalized_exposure_qty": row.normalized_exposure_qty,
                "open_exposure_qty": row.open_exposure_qty,
                "dust_tracking_qty": row.dust_tracking_qty,
                "sell_open_exposure_qty": row.sell_open_exposure_qty,
                "sell_dust_tracking_qty": row.sell_dust_tracking_qty,
                "observed_sell_qty_basis_qty": row.sell_qty_basis_qty,
                "sell_qty_boundary_kind": row.sell_qty_boundary_kind,
                "sell_submit_lot_count": row.sell_submit_lot_count,
                "sell_normalized_exposure_qty": row.sell_normalized_exposure_qty,
                "block_reason": row.block_reason,
                "reason": row.reason,
            }
            for row in recent_decision_flow
        ],
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
        "recent_external_cash_adjustment": recent_external_cash_adjustment,
        "runtime_state_snapshot": {
            "unresolved_open_order_count": int(health_row["unresolved_open_order_count"] or 0) if health_row else 0,
            "oldest_unresolved_order_age_sec": (
                float(health_row["oldest_unresolved_order_age_sec"]) if health_row and health_row["oldest_unresolved_order_age_sec"] is not None else None
            ),
            "recovery_required_present": bool(int(health_row["recovery_required_count"] or 0)) if health_row else False,
            "last_reconcile_epoch_sec": (
                float(health_row["last_reconcile_epoch_sec"]) if health_row and health_row["last_reconcile_epoch_sec"] is not None else None
            ),
            "last_reconcile_status": (str(health_row["last_reconcile_status"]) if health_row and health_row["last_reconcile_status"] is not None else None),
            "last_reconcile_reason_code": (str(health_row["last_reconcile_reason_code"]) if health_row and health_row["last_reconcile_reason_code"] is not None else None),
            "last_disable_reason": (str(health_row["last_disable_reason"]) if health_row and health_row["last_disable_reason"] is not None else None),
            "halt_reason_code": (str(health_row["halt_reason_code"]) if health_row and health_row["halt_reason_code"] is not None else None),
            "halt_state_unresolved": bool(int(health_row["halt_state_unresolved"])) if health_row else False,
        },
        "run_lock": run_lock_status.as_dict(),
    }
    reconcile_metadata_raw = health_row["last_reconcile_metadata"] if health_row else None
    dust_context = build_dust_display_context(reconcile_metadata_raw)
    position_metadata_raw: str | dict[str, object] | None = reconcile_metadata_raw
    if isinstance(reconcile_metadata_raw, str):
        try:
            parsed_metadata = json.loads(reconcile_metadata_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed_metadata = None
        if isinstance(parsed_metadata, dict):
            position_metadata_raw = parsed_metadata
    if isinstance(position_metadata_raw, dict):
        position_metadata_raw = {
            **position_metadata_raw,
            "unresolved_open_order_count": int(health_row["unresolved_open_order_count"] or 0) if health_row else 0,
            "recovery_required_count": int(health_row["recovery_required_count"] or 0) if health_row else 0,
        }
    position_state = readiness_snapshot.position_state
    dust_view = position_state.operator_diagnostics
    recent_buy_block_count = sum(
        1
        for row in recent_decision_flow
        if str(row.raw_signal) == "BUY" and (bool(row.entry_blocked) or str(row.final_signal) != "BUY")
    )
    recent_sell_suppression_count = len(recent_sell_suppressions)
    payload["operator_recovery_summary"] = {
        "recovery_stage": readiness_snapshot.recovery_stage,
        "recovery_blocker_categories": list(readiness_snapshot.blocker_categories),
        "canonical_next_action": readiness_snapshot.operator_next_action,
        "fee_gap_incident": readiness_snapshot.fee_gap_incident.as_dict(),
        "fee_gap_incident_kind": readiness_snapshot.fee_gap_incident.incident_kind,
        "fee_gap_incident_scope": readiness_snapshot.fee_gap_incident.incident_scope,
        "fee_gap_resolution_state": readiness_snapshot.fee_gap_incident.resolution_state,
        "fee_gap_active_issue": bool(readiness_snapshot.fee_gap_incident.active_issue),
        "canonical_state": readiness_snapshot.canonical_state,
        "residual_class": readiness_snapshot.residual_class,
        "run_loop_allowed": bool(readiness_snapshot.run_loop_allowed),
        "new_entry_allowed": bool(readiness_snapshot.new_entry_allowed),
        "closeout_allowed": bool(readiness_snapshot.closeout_allowed),
        "execution_flat": bool(readiness_snapshot.execution_flat),
        "accounting_flat": bool(readiness_snapshot.accounting_flat),
        "effective_flat": bool(readiness_snapshot.effective_flat),
        "operator_action_required": bool(readiness_snapshot.operator_action_required),
        "why_not": readiness_snapshot.why_not,
        "recent_buy_block_count": int(recent_buy_block_count),
        "recent_sell_suppression_count": int(recent_sell_suppression_count),
        "tradeability": readiness_snapshot.tradeability.as_dict(),
        "unresolved_open_order_count": int(health_row["unresolved_open_order_count"] or 0) if health_row else 0,
        "recovery_required_count": int(health_row["recovery_required_count"] or 0) if health_row else 0,
        "position_authority_summary": position_state.normalized_exposure.position_authority_summary,
        "authority_truth_model": dict(readiness_snapshot.authority_truth_model),
        "structured_blockers": [dict(item) for item in readiness_snapshot.structured_blockers],
        "inspect_only_mode": bool(readiness_snapshot.inspect_only_mode),
        **dust_context.fields,
        **readiness_snapshot.tradeability_operator_fields,
        "raw_holdings": position_state.raw_holdings.as_dict(),
        "normalized_exposure": position_state.normalized_exposure.as_dict(),
        "state_interpretation": position_state.state_interpretation.as_dict(),
        "operator_diagnostics": {
            "state": position_state.operator_diagnostics.state,
            "state_label": position_state.operator_diagnostics.state_label,
            "operator_action": position_state.operator_diagnostics.operator_action,
            "operator_message": position_state.operator_diagnostics.operator_message,
            "broker_local_match": bool(position_state.operator_diagnostics.broker_local_match),
            "new_orders_allowed": bool(position_state.operator_diagnostics.new_orders_allowed),
            "resume_allowed": bool(position_state.operator_diagnostics.resume_allowed),
            "treat_as_flat": bool(position_state.operator_diagnostics.treat_as_flat),
        },
    }
    payload.update(
        {
            "raw_total_asset_qty": float(position_state.normalized_exposure.raw_total_asset_qty),
            "open_exposure_qty": float(position_state.normalized_exposure.open_exposure_qty),
            "dust_tracking_qty": float(position_state.normalized_exposure.dust_tracking_qty),
            "open_lot_count": int(position_state.normalized_exposure.open_lot_count),
            "dust_tracking_lot_count": int(position_state.normalized_exposure.dust_tracking_lot_count),
            "reserved_exit_lot_count": int(position_state.normalized_exposure.reserved_exit_lot_count),
            "sellable_executable_lot_count": int(position_state.normalized_exposure.sellable_executable_lot_count),
            "sellable_executable_qty": float(position_state.normalized_exposure.sellable_executable_qty),
            "internal_lot_size": float(position_state.normalized_exposure.internal_lot_size),
            "terminal_state": str(position_state.normalized_exposure.terminal_state),
            "exit_block_reason": str(position_state.normalized_exposure.exit_block_reason),
        }
    )
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
    if dust_context.classification.present and dust_view.resume_allowed and dust_view.treat_as_flat:
        balance_source_diag["flat_start_allowed"] = True
    balance_source_diag["flat_start_reason"] = format_flat_start_reason_with_dust(
        balance_source_diag.get("flat_start_reason"),
        dust_context,
    )
    payload["balance_source_diagnostics"] = balance_source_diag
    write_json_atomic(PATH_MANAGER.ops_report_path(), payload)

    print("[OPS-REPORT]")
    raw_symbol_info = f" raw_symbol={raw_symbol}" if raw_symbol else ""
    print(
        f"  mode={settings.MODE} market={market}{raw_symbol_info} interval={settings.INTERVAL} db_path={settings.DB_PATH}"
    )
    operator_recovery = payload["operator_recovery_summary"]
    print(
        "  "
        f"recovery_stage={operator_recovery['recovery_stage']} "
        "recovery_blocker_categories="
        f"{','.join(str(x) for x in operator_recovery['recovery_blocker_categories']) or 'none'} "
        f"canonical_next_action={operator_recovery['canonical_next_action']} "
        f"fee_gap_incident_kind={operator_recovery['fee_gap_incident_kind']} "
        f"fee_gap_incident_scope={operator_recovery['fee_gap_incident_scope']} "
        f"fee_gap_resolution_state={operator_recovery['fee_gap_resolution_state']} "
        f"fee_gap_active_issue={1 if operator_recovery['fee_gap_active_issue'] else 0} "
        f"canonical_state={operator_recovery['canonical_state']} "
        f"residual_class={operator_recovery['residual_class']} "
        f"strategy_tradeability_state={operator_recovery['strategy_tradeability_state']} "
        f"run_loop_allowed={1 if operator_recovery['run_loop_allowed'] else 0} "
        f"new_entry_allowed={1 if operator_recovery['new_entry_allowed'] else 0} "
        f"closeout_allowed={1 if operator_recovery['closeout_allowed'] else 0} "
        f"recent_buy_block_count={operator_recovery['recent_buy_block_count']} "
        f"recent_sell_suppression_count={operator_recovery['recent_sell_suppression_count']} "
        f"unresolved_open_order_count={operator_recovery['unresolved_open_order_count']} "
        f"recovery_required_count={operator_recovery['recovery_required_count']} "
        f"dust_state={operator_recovery['dust_state']} "
        f"dust_display_scope={operator_recovery['dust_display_scope']} "
        f"residue_policy_state={operator_recovery['residue_policy_state']} "
        f"dust_tradeability_consistent={1 if operator_recovery['dust_tradeability_consistent'] else 0} "
        f"dust_action={operator_recovery['dust_operator_action']} "
        f"dust_new_orders_allowed={1 if operator_recovery['dust_new_orders_allowed'] else 0} "
        f"dust_resume_allowed={1 if operator_recovery['dust_resume_allowed_by_policy'] else 0} "
        f"dust_treat_as_flat={1 if operator_recovery['dust_treat_as_flat'] else 0}"
    )
    print(f"  position_authority_summary={operator_recovery['position_authority_summary']}")
    print(
        "  "
        f"raw_holdings_state={position_state.raw_holdings.state} "
        f"raw_holdings_match={1 if position_state.raw_holdings.broker_local_match else 0} "
        f"entry_allowed={1 if position_state.normalized_exposure.entry_allowed else 0} "
        f"entry_block_reason={position_state.normalized_exposure.entry_block_reason} "
        f"exit_allowed={1 if position_state.normalized_exposure.exit_allowed else 0} "
        f"exit_block_reason={position_state.normalized_exposure.exit_block_reason} "
        f"normalized_exposure_active={1 if position_state.normalized_exposure.normalized_exposure_active else 0} "
        f"has_executable_exposure={1 if position_state.normalized_exposure.has_executable_exposure else 0} "
        f"has_dust_only_remainder={1 if position_state.normalized_exposure.has_dust_only_remainder else 0} "
        f"normalized_exposure_qty={position_state.normalized_exposure.normalized_exposure_qty:.8f}"
    )
    print(
        "  "
        f"total_holdings_qty={float(position_state.normalized_exposure.raw_total_asset_qty):.8f} "
        f"executable_exposure_qty={float(position_state.normalized_exposure.open_exposure_qty):.8f} "
        f"tracked_dust_qty={float(position_state.normalized_exposure.dust_tracking_qty):.8f} "
        f"reserved_exit_qty={float(position_state.normalized_exposure.reserved_exit_qty):.8f} "
        f"sellable_executable_qty={float(position_state.normalized_exposure.sellable_executable_qty):.8f} "
        f"internal_lot_size={float(position_state.normalized_exposure.internal_lot_size):.8f} "
        f"terminal_state={position_state.normalized_exposure.terminal_state}"
    )
    print(
        "  "
        f"state_outcome={position_state.state_interpretation.operator_outcome} "
        f"exit_submit_expected={1 if position_state.state_interpretation.exit_submit_expected else 0} "
        f"state_message={position_state.state_interpretation.operator_message}"
    )
    print(
        "  "
        f"tradeability=canonical_state={readiness_snapshot.canonical_state} "
        f"residual_class={readiness_snapshot.residual_class} "
        f"strategy_tradeability_state={operator_recovery['strategy_tradeability_state']} "
        f"run_loop_allowed={1 if readiness_snapshot.run_loop_allowed else 0} "
        f"new_entry_allowed={1 if readiness_snapshot.new_entry_allowed else 0} "
        f"closeout_allowed={1 if readiness_snapshot.closeout_allowed else 0} "
        f"execution_flat={1 if readiness_snapshot.execution_flat else 0} "
        f"accounting_flat={1 if readiness_snapshot.accounting_flat else 0} "
        f"effective_flat={1 if readiness_snapshot.effective_flat else 0} "
        f"operator_action_required={1 if readiness_snapshot.operator_action_required else 0} "
        f"why_not={readiness_snapshot.why_not}"
    )
    print(f"  tradeability_operator_message={operator_recovery['tradeability_operator_message']}")
    print(
        "  "
        f"dust_broker_qty={float(position_state.raw_holdings.broker_qty):.8f} "
        f"dust_local_qty={float(position_state.raw_holdings.local_qty):.8f} "
        f"dust_delta_qty={float(position_state.raw_holdings.delta_qty):.8f} "
        f"dust_min_qty={float(position_state.raw_holdings.min_qty):.8f} "
        f"dust_min_notional_krw={float(position_state.raw_holdings.min_notional_krw):.1f} "
        f"dust_broker_local_match={1 if position_state.raw_holdings.broker_local_match else 0} "
        f"dust_threshold_basis={dust_context.fields['dust_threshold_basis']} "
        f"dust_qty_below_min={dust_context.qty_below_min_summary} "
        f"dust_notional_below_min={dust_context.notional_below_min_summary}"
    )
    print(
        "  "
        f"balance_source={balance_source_diag.get('source') or '-'} "
        f"reason={balance_source_diag.get('reason') or '-'} "
        f"category={balance_source_diag.get('failure_category') or '-'} "
        f"stale={balance_source_diag.get('stale')} "
        f"execution_mode={balance_source_diag.get('execution_mode') or '-'} "
        f"quote_currency={balance_source_diag.get('quote_currency') or '-'} "
        f"base_currency={balance_source_diag.get('base_currency') or '-'} "
        f"base_missing_policy={balance_source_diag.get('base_currency_missing_policy') or '-'} "
        f"preflight_outcome={balance_source_diag.get('preflight_outcome') or '-'} "
        f"accounts_flat_start_allowed={balance_source_diag.get('flat_start_allowed')} "
        f"accounts_flat_start_reason={balance_source_diag.get('flat_start_reason') or '-'}"
    )
    print(
        "  recent_external_cash_adjustment="
        f"{_format_external_cash_adjustment_summary(recent_external_cash_adjustment)}"
    )
    print(
        "  "
        f"operator_action={position_state.operator_diagnostics.operator_action} "
        f"resume_allowed={1 if position_state.operator_diagnostics.resume_allowed else 0} "
        f"treat_as_flat={1 if position_state.operator_diagnostics.treat_as_flat else 0} "
        f"unresolved_attribution_count={recovery_attribution_signals.unresolved_attribution_count} "
        f"recent_recovery_derived_trade_count={recovery_attribution_signals.recent_recovery_derived_trade_count} "
        "ambiguous_linkage_after_recent_reconcile="
        f"{recovery_attribution_signals.ambiguous_linkage_after_recent_reconcile}"
    )
    runtime_state_snapshot = payload.get("runtime_state_snapshot") or {}
    print(
        "  "
        f"runtime_state_snapshot=recovery_required_present={1 if runtime_state_snapshot.get('recovery_required_present') else 0} "
        f"unresolved_open_order_count={runtime_state_snapshot.get('unresolved_open_order_count', 0)} "
        f"oldest_unresolved_order_age_sec={runtime_state_snapshot.get('oldest_unresolved_order_age_sec') or '-'} "
        f"last_reconcile_epoch_sec={runtime_state_snapshot.get('last_reconcile_epoch_sec') or '-'} "
        f"last_disable_reason={runtime_state_snapshot.get('last_disable_reason') or '-'} "
        f"halt_reason_code={runtime_state_snapshot.get('halt_reason_code') or '-'} "
        f"halt_state_unresolved={1 if runtime_state_snapshot.get('halt_state_unresolved') else 0}"
    )
    run_lock = payload.get("run_lock") or {}
    print(f"  run_lock={run_lock.get('human_text') or '-'}")
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
        print("  tip: strategy_context ????????????????????됰Ŧ?????????棺堉?뤃?????????????袁⑸즴筌????遺얘턁?????????ㅼ굡獒??intent dedup ?????????? ??????????????????????????????븐뼐?????????")
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
            submit_evidence = str(row["submit_evidence"] or "")
            evidence_payload = _load_json_dict(submit_evidence)
            decision_summary_payload = (
                evidence_payload.get("decision_summary")
                if isinstance(evidence_payload.get("decision_summary"), dict)
                else {}
            )
            sell_failure_category = _sell_failure_category_from_observability(
                submission_reason_code=str(row["submission_reason_code"] or ""),
                message=message,
                submit_evidence=submit_evidence,
            )
            if not sell_failure_category or sell_failure_category in {"-", "none", "null", "unknown"}:
                sell_failure_category = str(evidence_payload.get("sell_failure_category") or "").strip()
            if not sell_failure_category or sell_failure_category in {"-", "none", "null", "unknown"}:
                sell_failure_category = str(decision_summary_payload.get("sell_failure_category") or "").strip()
            if not sell_failure_category or sell_failure_category in {"-", "none", "null", "unknown"}:
                sell_failure_category = str(row["sell_failure_category"] or "").strip()
            if not sell_failure_category or sell_failure_category in {"-", "none", "null", "unknown"}:
                sell_failure_category = "unknown"
            sell_failure_detail = str(evidence_payload.get("sell_failure_detail") or "").strip()
            if not sell_failure_detail or sell_failure_detail in {"-", "none", "null", "unknown"}:
                sell_failure_detail = str(decision_summary_payload.get("sell_failure_detail") or "").strip()
            if not sell_failure_detail or sell_failure_detail in {"-", "none", "null", "unknown"}:
                sell_failure_detail = str(row["sell_failure_detail"] or "").strip()
            if not sell_failure_detail or sell_failure_detail in {"-", "none", "null", "unknown"}:
                sell_failure_detail = sell_failure_category
            operator_action = str(
                evidence_payload.get("operator_action")
                or evidence_payload.get("dust_action")
                or evidence_payload.get("dust_operator_action")
                or "-"
            )
            sell_normalized_exposure_qty = float(
                evidence_payload.get("sell_normalized_exposure_qty")
                or evidence_payload.get("normalized_qty")
                or row["normalized_qty"]
                or 0.0
            )
            if len(message) > 80:
                message = f"{message[:77]}..."
            print(
                "  "
                f"{ts} strategy={strategy_context} cid={row['client_order_id']} "
                f"event={row['event_type']} status={row['order_status'] or '-'} side={row['side'] or '-'} "
                f"observed_position_qty={_fmt_float(float(row['position_qty'] or 0.0), 8)} "
                f"observed_submit_payload_qty={_fmt_float(float(row['submit_payload_qty'] or 0.0), 8)} "
                f"order_events_qty={_fmt_float(float(row['order_events_qty'] or 0.0), 8)} "
                f"normalized_qty={_fmt_float(float(row['normalized_qty'] or 0.0), 8)} "
                f"observed_sell_qty_basis_qty={_fmt_float(float(row['sell_qty_basis_qty'] or 0.0), 8)} "
                f"sell_qty_boundary_kind={row['sell_qty_boundary_kind'] or '-'} "
                f"sell_failure_category={sell_failure_category} "
                f"sell_failure_detail={sell_failure_detail} "
                f"operator_action={operator_action} "
                f"sell_normalized_exposure_qty={_fmt_float(float(sell_normalized_exposure_qty), 8)} "
                f"raw_total_asset_qty={_fmt_float(float(row['raw_total_asset_qty'] or 0.0), 8)} "
                f"open_exposure_qty={_fmt_float(float(row['open_exposure_qty'] or 0.0), 8)} "
                f"dust_tracking_qty={_fmt_float(float(row['dust_tracking_qty'] or 0.0), 8)} "
                f"sell_open_exposure_qty={_fmt_float(float(row['sell_open_exposure_qty'] or 0.0), 8)} "
                f"sell_dust_tracking_qty={_fmt_float(float(row['sell_dust_tracking_qty'] or 0.0), 8)} "
                f"price={_fmt_float(float(row['price'] or 0.0), 0)} "
                f"reason={row['submission_reason_code'] or '-'} note={message or '-'}"
            )

    print("\n[RECENT-SELL-SUPPRESSIONS]")
    if not recent_sell_suppressions:
        print("  no order_suppressions rows")
    else:
        for row in reversed(recent_sell_suppressions):
            print(
                "  "
                f"{kst_str(int(row.event_ts))} strategy={row.strategy_name} signal={row.signal} side={row.side} "
                f"reason={row.reason_code} sell_failure_category={row.suppression_category} "
                f"sell_failure_detail={row.sell_failure_detail or '-'} "
                f"sell_submit_lot_count={row.sell_submit_lot_count} "
                f"observed_sell_qty_basis_qty={_fmt_float(float(row.sell_qty_basis_qty or 0.0), 8)} "
                f"sell_qty_boundary_kind={row.sell_qty_boundary_kind or '-'} "
                f"requested_qty={_fmt_float(float(row.requested_qty or 0.0), 8)} "
                f"normalized_qty={_fmt_float(float(row.normalized_qty or 0.0), 8)} "
                f"open_exposure_qty={_fmt_float(float(row.open_exposure_qty or 0.0), 8)} "
                f"dust_tracking_qty={_fmt_float(float(row.dust_tracking_qty or 0.0), 8)} "
                f"market_price={_fmt_float(float(row.market_price or 0.0), 0)} "
                f"dust_state={row.dust_state or '-'} dust_action={row.dust_action or '-'} "
                f"operator_action={row.operator_action or '-'} "
                f"summary={row.summary or '-'}"
            )

    print("\n[RECENT-STRATEGY-DECISION-FLOW]")
    if not recent_decision_flow:
        print("  no strategy_decisions rows")
    else:
        for row in recent_decision_flow:
            print(
                "  "
                f"{kst_str(int(row.decision_ts))} decision_id={row.decision_id} strategy={row.strategy_name} "
                f"base={row.base_signal} raw={row.raw_signal} final={row.final_signal} "
                f"final_action={row.final_action} submit_expected={1 if row.submit_expected else 0} "
                f"pre_submit_proof={row.pre_submit_proof_status} execution_block_reason={row.execution_block_reason} "
                f"residual_live_sell_mode={row.residual_live_sell_mode} "
                f"residual_buy_sizing_mode={row.residual_buy_sizing_mode} "
                f"target_exposure_krw={_fmt_float(float(row.target_exposure_krw or 0.0), 0)} "
                f"current_effective_exposure_krw={_fmt_float(float(row.current_effective_exposure_krw or 0.0), 0)} "
                f"buy_delta_krw={_fmt_float(float(row.buy_delta_krw or 0.0), 0)} "
                f"target_delta_side={row.target_delta_side} "
                f"target_would_submit={1 if row.target_would_submit else 0} "
                f"target_submit_qty={_fmt_float(float(row.target_submit_qty or 0.0), 8)} "
                f"target_delta_notional_krw={_fmt_float(float(row.target_delta_notional_krw or 0.0), 0)} "
                f"target_block_reason={row.target_block_reason} "
                f"target_position_truth_state={row.target_position_truth_state} "
                f"flow={row.buy_flow_state} entry_blocked={1 if row.entry_blocked else 0} "
                f"entry_allowed={1 if row.entry_allowed else 0} effective_flat={1 if row.effective_flat else 0} "
                f"normalized_exposure_active={1 if row.normalized_exposure_active else 0} "
                f"has_executable_exposure={1 if bool(getattr(row, 'has_executable_exposure', False)) else 0} "
                f"has_dust_only_remainder={1 if bool(getattr(row, 'has_dust_only_remainder', False)) else 0} "
                f"observed_position_qty={_fmt_float(float(row.position_qty), 8)} "
                f"observed_submit_payload_qty={_fmt_float(float(row.submit_payload_qty), 8)} "
                f"sell_submit_lot_count={row.sell_submit_lot_count} "
                f"observed_sell_qty_basis_qty={_fmt_float(float(row.sell_qty_basis_qty), 8)} "
                f"sell_qty_boundary_kind={row.sell_qty_boundary_kind} "
                f"sell_normalized_exposure_qty={_fmt_float(float(row.sell_normalized_exposure_qty), 8)} "
                f"sell_failure_category={row.sell_failure_category} "
                f"sell_failure_detail={row.sell_failure_detail} "
                f"normalized_exposure_qty={_fmt_float(float(row.normalized_exposure_qty), 8)} "
                f"raw_qty_open={_fmt_float(float(row.raw_qty_open), 8)} "
                f"raw_total_asset_qty={_fmt_float(float(row.raw_total_asset_qty), 8)} "
                f"open_exposure_qty={_fmt_float(float(row.open_exposure_qty), 8)} "
                f"dust_tracking_qty={_fmt_float(float(row.dust_tracking_qty), 8)} "
                f"reason={row.reason}"
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
    print("  - strategy-report??trade_lifecycles ???????????????realized gross/fee/net pnl ?????됰Ŧ?????????棺堉?뤃????????筌띯뫔???????????????? ?????????븐뼐??????????????ㅼ굣????")
    print("  - ops-report??strategy_summary??intent/fill ????????????????????됰Ŧ???????????怨뺤른?????????????????먃??곌램鍮???pnl_proxy_deprecated???????????븐뼐?????????")
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


def cmd_risk_report(*, limit: int = 20, as_json: bool = False) -> None:
    conn = ensure_db()
    try:
        baseline = fetch_daily_risk_baseline(conn)
        evaluations = fetch_recent_risk_evaluations(conn, limit=max(1, int(limit)))
        health_row = conn.execute(
            """
            SELECT
                last_reconcile_epoch_sec,
                last_reconcile_status,
                last_reconcile_reason_code,
                last_disable_reason,
                halt_reason_code
            FROM bot_health
            WHERE id=1
            """
        ).fetchone()
    finally:
        conn.close()

    payload = {
        "mode": settings.MODE,
        "pair": settings.PAIR,
        "max_daily_loss_krw": float(settings.MAX_DAILY_LOSS_KRW),
        "today_baseline": baseline,
        "recent_evaluations": evaluations,
        "runtime_state": {
            "last_reconcile_epoch_sec": (
                float(health_row["last_reconcile_epoch_sec"])
                if health_row and health_row["last_reconcile_epoch_sec"] is not None
                else None
            ),
            "last_reconcile_status": (
                str(health_row["last_reconcile_status"])
                if health_row and health_row["last_reconcile_status"] is not None
                else None
            ),
            "last_reconcile_reason_code": (
                str(health_row["last_reconcile_reason_code"])
                if health_row and health_row["last_reconcile_reason_code"] is not None
                else None
            ),
            "halt_reason_code": (
                str(health_row["halt_reason_code"])
                if health_row and health_row["halt_reason_code"] is not None
                else None
            ),
            "last_disable_reason": (
                str(health_row["last_disable_reason"])
                if health_row and health_row["last_disable_reason"] is not None
                else None
            ),
        },
    }
    write_json_atomic(PATH_MANAGER.report_path("risk_report"), payload)

    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return

    print("[RISK-REPORT]")
    print(f"  mode={settings.MODE} pair={settings.PAIR} max_daily_loss_krw={float(settings.MAX_DAILY_LOSS_KRW):,.3f}")
    if baseline is None:
        print("  baseline=none")
    else:
        print(
            "  "
            f"baseline_day={baseline.get('day_kst')} "
            f"start_equity={float(baseline.get('start_equity') or 0.0):,.3f} "
            f"baseline_cash_krw={float(baseline.get('baseline_cash_krw') or 0.0):,.3f} "
            f"baseline_asset_qty={float(baseline.get('baseline_asset_qty') or 0.0):.10f} "
            f"baseline_mark_price={float(baseline.get('baseline_mark_price') or 0.0):,.3f} "
            f"baseline_origin={baseline.get('baseline_origin') or '-'} "
            f"baseline_balance_source={baseline.get('baseline_balance_source') or '-'} "
            f"baseline_balance_observed_ts_ms={baseline.get('baseline_balance_observed_ts_ms') or '-'}"
        )
    runtime_state_snapshot = payload["runtime_state"]
    print(
        "  "
        f"last_reconcile_status={runtime_state_snapshot['last_reconcile_status'] or '-'} "
        f"last_reconcile_reason_code={runtime_state_snapshot['last_reconcile_reason_code'] or '-'} "
        f"halt_reason_code={runtime_state_snapshot['halt_reason_code'] or '-'}"
    )
    print(f"  report_path={PATH_MANAGER.report_path('risk_report')}")
    print(f"[P1] recent_evaluations(top {len(evaluations)}):")
    if not evaluations:
        print("  none")
        return
    for row in evaluations:
        print(
            "  "
            f"evaluation_ts={kst_str(int(row['evaluation_ts_ms']))} "
            f"origin={row['evaluation_origin']} "
            f"decision={row['decision']} "
            f"reason_code={row['reason_code']} "
            f"loss_today={float(row['loss_today'] or 0.0):,.3f} "
            f"start_equity={float(row['start_equity'] or 0.0):,.3f} "
            f"current_equity={float(row['current_equity'] or 0.0):,.3f} "
            f"current_cash_krw={float(row['current_cash_krw'] or 0.0):,.3f} "
            f"current_asset_qty={float(row['current_asset_qty'] or 0.0):.10f} "
            f"mark_price={float(row['mark_price'] or 0.0):,.3f} "
            f"current_source={row['current_source'] or '-'} "
            f"balance_source={row['current_balance_source'] or '-'} "
            f"mismatch_summary={row['mismatch_summary'] or '-'}"
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
    print(
        "  base_signal,decision_type,raw_signal,final_signal,buy_flow_state,entry_blocked,"
        "entry_allowed,block_reason,dust_classification,effective_flat,raw_qty_open,"
        "final_action,submit_expected,pre_submit_proof_status,execution_block_reason,"
        "residual_live_sell_mode,residual_buy_sizing_mode,target_exposure_krw,"
        "current_effective_exposure_krw,tracked_residual_exposure_krw,buy_delta_krw,"
        "target_delta_side,target_would_submit,target_submit_qty,target_delta_notional_krw,"
        "target_block_reason,target_position_truth_state,"
        "raw_total_asset_qty,observed_position_qty,observed_submit_payload_qty,normalized_exposure_active,"
        "normalized_exposure_qty,open_exposure_qty,dust_tracking_qty,sell_open_exposure_qty,sell_dust_tracking_qty,"
        "observed_sell_qty_basis_qty,sell_qty_boundary_kind,sell_submit_lot_count,"
        "sell_normalized_exposure_qty,sell_failure_category,sell_failure_detail,"
        "strategy_name,pair,interval,count"
    )
    for row in rows:
        print(
            "  "
            f"{row.base_signal},{row.decision_type},{row.raw_signal},{row.final_signal},{row.buy_flow_state},"
            f"{1 if row.entry_blocked else 0},{1 if row.entry_allowed else 0},{row.block_reason},"
            f"{row.dust_classification},{1 if row.effective_flat else 0},{row.raw_qty_open:.8f},"
            f"{row.final_action},{1 if row.submit_expected else 0},{row.pre_submit_proof_status},"
            f"{row.execution_block_reason},{row.residual_live_sell_mode},{row.residual_buy_sizing_mode},"
            f"{float(row.target_exposure_krw or 0.0):.2f},{float(row.current_effective_exposure_krw or 0.0):.2f},"
            f"{float(row.tracked_residual_exposure_krw or 0.0):.2f},{float(row.buy_delta_krw or 0.0):.2f},"
            f"{row.target_delta_side},{1 if row.target_would_submit else 0},"
            f"{float(row.target_submit_qty or 0.0):.8f},{float(row.target_delta_notional_krw or 0.0):.2f},"
            f"{row.target_block_reason},{row.target_position_truth_state},"
            f"{row.raw_total_asset_qty:.8f},{row.position_qty:.8f},"
            f"{row.submit_payload_qty:.8f},{1 if row.normalized_exposure_active else 0},"
            f"{row.normalized_exposure_qty:.8f},{row.open_exposure_qty:.8f},"
            f"{row.dust_tracking_qty:.8f},{row.sell_open_exposure_qty:.8f},{row.sell_dust_tracking_qty:.8f},"
            f"{row.sell_qty_basis_qty:.8f},{row.sell_qty_boundary_kind},{row.sell_submit_lot_count},"
            f"{row.sell_normalized_exposure_qty:.8f},{row.sell_failure_category},{row.sell_failure_detail},"
            f"{row.strategy_name},{row.pair},{row.interval},{row.count}"
        )
