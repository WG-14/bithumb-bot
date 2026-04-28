from __future__ import annotations

import sqlite3
import time
from typing import Any

from .config import settings
from .db_core import (
    compute_accounting_replay,
    normalize_asset_qty,
    record_external_position_adjustment,
    record_position_authority_projection_publication,
    record_position_authority_repair,
    summarize_fill_accounting_incident_projection,
)
from .lifecycle import (
    DUST_TRACKING_STATE,
    ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED,
    LOT_SEMANTIC_VERSION_V1,
    OPEN_POSITION_STATE,
    apply_fill_lifecycle,
    apply_portfolio_anchored_projection_repair_basis,
    rebuild_lifecycle_projections_from_trades,
    summarize_position_lots,
)
from .lot_model import lot_count_to_qty
from .position_authority_incidents import PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON
from .position_authority_state import (
    HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT,
    PARTIAL_CLOSE_RESIDUAL_REPAIR_REASON,
    build_lot_projection_convergence,
    build_position_authority_assessment,
)
from .runtime_readiness import build_broker_position_evidence, compute_runtime_readiness_snapshot


_EPS = 1e-12
FULL_PROJECTION_REBUILD_REASON = "full_projection_materialized_rebuild"
FLAT_STALE_LOT_PROJECTION_REPAIR_REASON = "flat_stale_lot_projection_repair"
FLAT_STALE_LOT_PROJECTION_REPAIR_COMMAND = (
    "uv run bithumb-bot rebuild-position-authority --flat-stale-projection-repair --apply --yes"
)


def _fetch_anchor_buy_row(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
            t.id AS trade_id,
            t.client_order_id,
            t.ts AS fill_ts,
            t.price,
            t.qty,
            t.fee,
            t.strategy_name,
            t.entry_decision_id,
            f.fill_id,
            COALESCE(f.internal_lot_size, o.internal_lot_size) AS internal_lot_size,
            COALESCE(o.effective_min_trade_qty, f.internal_lot_size, o.internal_lot_size) AS effective_min_trade_qty,
            COALESCE(o.qty_step, f.internal_lot_size, o.internal_lot_size) AS qty_step,
            COALESCE(o.min_notional_krw, 0.0) AS min_notional_krw
        FROM trades t
        LEFT JOIN fills f
          ON f.client_order_id=t.client_order_id
         AND f.fill_ts=t.ts
         AND ABS(f.price-t.price) < 1e-12
         AND ABS(f.qty-t.qty) < 1e-12
        LEFT JOIN orders o
          ON o.client_order_id=t.client_order_id
        WHERE t.pair=? AND t.side='BUY'
        ORDER BY t.ts DESC, t.id DESC
        LIMIT 1
        """,
        (settings.PAIR,),
    ).fetchone()


def _build_full_projection_rebuild_gate_report(
    conn: sqlite3.Connection,
    *,
    snapshot,
    authority_assessment: dict[str, Any],
    portfolio_qty: float,
) -> dict[str, Any]:
    broker_evidence = build_broker_position_evidence(snapshot.reconcile_metadata, pair=settings.PAIR)
    broker_qty = float(broker_evidence.get("broker_qty") or 0.0)
    broker_qty_known = bool(broker_evidence.get("broker_qty_known"))
    broker_portfolio_converged = bool(
        broker_qty_known and abs(normalize_asset_qty(broker_qty) - normalize_asset_qty(portfolio_qty)) <= _EPS
    )
    remote_open_order_count = int(snapshot.reconcile_metadata.get("remote_open_order_found", 0) or 0)
    unresolved_open_order_count = int(snapshot.open_order_count)
    recovery_required_count = int(snapshot.recovery_required_count)
    pending_submit_count = int(
        (
            conn.execute("SELECT COUNT(*) AS cnt FROM orders WHERE status='PENDING_SUBMIT'").fetchone()
            or {"cnt": 0}
        )["cnt"]
    )
    submit_unknown_count = int(
        (
            conn.execute("SELECT COUNT(*) AS cnt FROM orders WHERE status='SUBMIT_UNKNOWN'").fetchone()
            or {"cnt": 0}
        )["cnt"]
    )
    accounting_replay = compute_accounting_replay(conn)
    replay_qty = float(accounting_replay.get("replay_qty") or 0.0)
    accounting_projection_ok = bool(abs(normalize_asset_qty(replay_qty) - normalize_asset_qty(portfolio_qty)) <= _EPS)
    unresolved_fee_pending = bool(int(snapshot.fee_pending_count) > 0)
    reasons: list[str] = []
    if not bool(broker_evidence.get("balance_snapshot_available_for_position_rebuild")):
        blockers = list(broker_evidence.get("position_rebuild_blockers") or [])
        if broker_qty_known and blockers:
            reasons.extend(blockers)
        else:
            reasons.append("broker_position_qty_evidence_missing")
    elif not broker_qty_known:
        reasons.append("broker_position_qty_evidence_missing")
    elif not broker_portfolio_converged:
        reasons.append(
            "broker_portfolio_qty_mismatch="
            f"broker_qty={broker_qty:.12f},portfolio_qty={portfolio_qty:.12f}"
        )
    if remote_open_order_count > 0:
        reasons.append(f"remote_open_orders={remote_open_order_count}")
    if unresolved_open_order_count > 0:
        reasons.append(f"unresolved_open_orders={unresolved_open_order_count}")
    if recovery_required_count > 0:
        reasons.append(f"recovery_required_orders={recovery_required_count}")
    if pending_submit_count > 0:
        reasons.append(f"pending_submit={pending_submit_count}")
    if submit_unknown_count > 0:
        reasons.append(f"submit_unknown={submit_unknown_count}")
    if unresolved_fee_pending:
        reasons.append(f"fee_pending_count={int(snapshot.fee_pending_count)}")
    if not accounting_projection_ok:
        reasons.append(
            "accounting_projection_mismatch="
            f"replay_qty={replay_qty:.12f},"
            f"portfolio_qty={portfolio_qty:.12f}"
        )
    if not bool(authority_assessment.get("needs_full_projection_rebuild")):
        reasons.append("full_projection_rebuild_not_required")
    return {
        "broker_qty": broker_qty,
        "broker_qty_known": broker_qty_known,
        "broker_qty_value_source": broker_evidence.get("broker_qty_value_source"),
        "broker_qty_evidence_source": broker_evidence.get("broker_qty_evidence_source"),
        "broker_qty_evidence_observed_ts_ms": broker_evidence.get("broker_qty_evidence_observed_ts_ms"),
        "balance_source": broker_evidence.get("balance_source"),
        "balance_source_stale": broker_evidence.get("balance_source_stale"),
        "balance_snapshot_available_for_health": broker_evidence.get("balance_snapshot_available_for_health"),
        "balance_snapshot_available_for_position_rebuild": broker_evidence.get(
            "balance_snapshot_available_for_position_rebuild"
        ),
        "missing_evidence_fields": list(broker_evidence.get("missing_evidence_fields") or []),
        "position_rebuild_blockers": list(broker_evidence.get("position_rebuild_blockers") or []),
        "base_currency": broker_evidence.get("base_currency"),
        "quote_currency": broker_evidence.get("quote_currency"),
        "asset_available": broker_evidence.get("asset_available"),
        "asset_locked": broker_evidence.get("asset_locked"),
        "cash_available": broker_evidence.get("cash_available"),
        "cash_locked": broker_evidence.get("cash_locked"),
        "broker_portfolio_converged": broker_portfolio_converged,
        "remote_open_order_count": remote_open_order_count,
        "unresolved_open_order_count": unresolved_open_order_count,
        "recovery_required_count": recovery_required_count,
        "pending_submit_count": pending_submit_count,
        "submit_unknown_count": submit_unknown_count,
        "unresolved_fee_pending": unresolved_fee_pending,
        "accounting_projection_ok": accounting_projection_ok,
        "accounting_replay": accounting_replay,
        "needs_full_projection_rebuild": bool(authority_assessment.get("needs_full_projection_rebuild")),
        "projection_converged": bool(
            (authority_assessment.get("projection_convergence") or {}).get("converged")
        ),
        "projected_total_qty": float(authority_assessment.get("projected_total_qty") or 0.0),
        "projected_qty_excess": float(authority_assessment.get("projected_qty_excess") or 0.0),
        "lot_row_count": int(
            ((authority_assessment.get("projection_convergence") or {}).get("lot_row_count") or 0)
        ),
        "other_active_qty": float(authority_assessment.get("other_active_qty") or 0.0),
        "portfolio_projection_publication_present": bool(
            authority_assessment.get("portfolio_projection_publication_present")
        ),
        "portfolio_projection_repair_event_status": str(
            authority_assessment.get("portfolio_projection_repair_event_status") or "none"
        ),
        "reasons": reasons,
        "safe_to_apply": not reasons,
    }


def _source_modes_for_pair(conn: sqlite3.Connection, *, pair: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT lot_rule_source_mode
        FROM open_position_lots
        WHERE pair=? AND qty_open > 1e-12
        ORDER BY lot_rule_source_mode ASC
        """,
        (str(pair),),
    ).fetchall()
    return [str(row["lot_rule_source_mode"] or "").strip() for row in rows if str(row["lot_rule_source_mode"] or "").strip()]


def _query_runtime_gate_counts(conn: sqlite3.Connection) -> dict[str, int]:
    open_order_row = conn.execute(
        """
        SELECT
            COUNT(*) AS open_order_count,
            COALESCE(SUM(CASE WHEN status='RECOVERY_REQUIRED' THEN 1 ELSE 0 END), 0) AS recovery_required_count,
            COALESCE(SUM(CASE WHEN status='PENDING_SUBMIT' THEN 1 ELSE 0 END), 0) AS pending_submit_count,
            COALESCE(SUM(CASE WHEN status='SUBMIT_UNKNOWN' THEN 1 ELSE 0 END), 0) AS submit_unknown_count
        FROM orders
        WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING',
                         'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
        """
    ).fetchone()
    return {
        "unresolved_open_order_count": int((open_order_row or {"open_order_count": 0})["open_order_count"] or 0),
        "recovery_required_count": int((open_order_row or {"recovery_required_count": 0})["recovery_required_count"] or 0),
        "pending_submit_count": int((open_order_row or {"pending_submit_count": 0})["pending_submit_count"] or 0),
        "submit_unknown_count": int((open_order_row or {"submit_unknown_count": 0})["submit_unknown_count"] or 0),
    }


def _portfolio_asset_qty(conn: sqlite3.Connection) -> float:
    row = conn.execute(
        "SELECT asset_qty, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    if row is None:
        return 0.0
    try:
        keys = row.keys()
    except AttributeError:
        keys = ()
    if "asset_available" in keys:
        return normalize_asset_qty(float(row["asset_available"] or 0.0) + float(row["asset_locked"] or 0.0))
    return normalize_asset_qty(float(row["asset_qty"] or 0.0))


def _load_latest_terminal_sell_evidence(conn: sqlite3.Connection) -> dict[str, Any]:
    latest_order = conn.execute(
        """
        SELECT id, client_order_id, exchange_order_id, status, side, qty_filled, qty_req, created_ts, updated_ts
        FROM orders
        ORDER BY updated_ts DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_trade = conn.execute(
        """
        SELECT id, ts, client_order_id, side, qty, asset_after
        FROM trades
        WHERE pair=?
        ORDER BY ts DESC, id DESC
        LIMIT 1
        """,
        (settings.PAIR,),
    ).fetchone()
    order_dict = dict(latest_order) if latest_order is not None else None
    trade_dict = dict(latest_trade) if latest_trade is not None else None
    return {
        "latest_terminal_order": order_dict,
        "latest_trade": trade_dict,
        "latest_sell_client_order_id": (
            str(latest_order["client_order_id"]) if latest_order is not None else None
        ),
        "latest_sell_exchange_order_id": (
            str(latest_order["exchange_order_id"]) if latest_order is not None and latest_order["exchange_order_id"] else None
        ),
        "latest_sell_qty": (
            normalize_asset_qty(float(latest_order["qty_filled"] or latest_order["qty_req"] or 0.0))
            if latest_order is not None
            else 0.0
        ),
        "latest_trade_id": int(latest_trade["id"]) if latest_trade is not None else None,
        "latest_trade_asset_after": (
            normalize_asset_qty(float(latest_trade["asset_after"] or 0.0)) if latest_trade is not None else None
        ),
        "latest_trade_qty": (
            normalize_asset_qty(float(latest_trade["qty"] or 0.0)) if latest_trade is not None else 0.0
        ),
    }


def build_flat_stale_lot_projection_repair_preview(conn: sqlite3.Connection) -> dict[str, Any]:
    snapshot = compute_runtime_readiness_snapshot(conn)
    readiness = snapshot.as_dict()
    projection = build_lot_projection_convergence(conn, pair=settings.PAIR)
    broker_evidence = build_broker_position_evidence(snapshot.reconcile_metadata, pair=settings.PAIR)
    broker_qty = normalize_asset_qty(float(broker_evidence.get("broker_qty") or 0.0))
    broker_qty_known = bool(broker_evidence.get("broker_qty_known"))
    portfolio_qty = _portfolio_asset_qty(conn)
    projected_total_qty = normalize_asset_qty(float(projection.get("projected_total_qty") or 0.0))
    stale_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT id, pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                   qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size,
                   lot_min_qty, lot_qty_step, lot_min_notional_krw, lot_max_qty_decimals,
                   lot_rule_source_mode, position_semantic_basis, position_state, entry_fee_total,
                   strategy_name, entry_decision_id, entry_decision_linkage
            FROM open_position_lots
            WHERE pair=? AND qty_open > 1e-12
            ORDER BY id ASC
            """,
            (settings.PAIR,),
        ).fetchall()
    ]
    stale_total_qty = normalize_asset_qty(sum(float(row.get("qty_open") or 0.0) for row in stale_rows))
    latest_sell = _load_latest_terminal_sell_evidence(conn)
    gate_counts = _query_runtime_gate_counts(conn)
    remote_open_order_count = int(snapshot.reconcile_metadata.get("remote_open_order_found", 0) or 0)
    try:
        accounting_replay = compute_accounting_replay(conn)
        replay_qty = normalize_asset_qty(float(accounting_replay.get("replay_qty") or 0.0))
        accounting_projection_ok = bool(abs(replay_qty - portfolio_qty) <= _EPS)
    except RuntimeError as exc:
        accounting_replay = {"error": str(exc)}
        accounting_projection_ok = False

    blockers: list[str] = []
    if not broker_qty_known:
        blockers.append("broker_qty_unknown")
    if abs(broker_qty) > _EPS:
        blockers.append("broker_not_flat")
    if abs(portfolio_qty) > _EPS:
        blockers.append("portfolio_not_flat")
    broker_portfolio_converged = bool(broker_qty_known and abs(broker_qty - portfolio_qty) <= _EPS)
    if not broker_portfolio_converged:
        blockers.append("broker_portfolio_not_converged")
    if int(gate_counts["unresolved_open_order_count"]) > 0:
        blockers.append("open_orders_present")
    if remote_open_order_count > 0:
        blockers.append("remote_open_orders_present")
    if int(gate_counts["pending_submit_count"]) > 0:
        blockers.append("pending_submit_present")
    if int(gate_counts["submit_unknown_count"]) > 0:
        blockers.append("submit_unknown_present")
    if int(gate_counts["recovery_required_count"]) > 0:
        blockers.append("recovery_required_orders_present")
    if not accounting_projection_ok:
        blockers.append("accounting_projection_mismatch")

    latest_order = latest_sell.get("latest_terminal_order") or {}
    latest_trade = latest_sell.get("latest_trade") or {}
    terminal_sell_ok = bool(
        latest_order
        and str(latest_order.get("side") or "").upper() == "SELL"
        and str(latest_order.get("status") or "").upper() == "FILLED"
        and latest_trade
        and str(latest_trade.get("side") or "").upper() == "SELL"
        and str(latest_trade.get("client_order_id") or "") == str(latest_order.get("client_order_id") or "")
        and abs(normalize_asset_qty(float(latest_trade.get("asset_after") or 0.0))) <= _EPS
    )
    if not terminal_sell_ok:
        blockers.append("missing_terminal_flat_sell_evidence")

    if projected_total_qty <= _EPS:
        blockers.append("stale_lot_projection_not_present")
    if not stale_rows:
        blockers.append("stale_lot_rows_missing")
    if any(str(row.get("position_state") or "") != DUST_TRACKING_STATE for row in stale_rows):
        blockers.append("non_dust_tracking_lot_rows_present")
    if any(int(row.get("executable_lot_count") or 0) != 0 for row in stale_rows):
        blockers.append("executable_lot_rows_present")
    if abs(stale_total_qty - projected_total_qty) > _EPS:
        blockers.append("stale_lot_total_projection_mismatch")
    if terminal_sell_ok and abs(stale_total_qty - normalize_asset_qty(float(latest_trade.get("qty") or 0.0))) > _EPS:
        blockers.append("stale_lot_qty_latest_sell_qty_mismatch")

    blockers = list(dict.fromkeys(blockers))
    needed = bool(projected_total_qty > _EPS and abs(portfolio_qty) <= _EPS and stale_rows)
    safe = bool(needed and not blockers)
    return {
        "needed": needed,
        "safe_to_apply": safe,
        "final_safe_to_apply": safe,
        "repair_mode": "flat_stale_projection_repair",
        "reason": "flat_stale_lot_projection_detected" if needed else "flat_stale_lot_projection_not_present",
        "blockers": blockers,
        "why_unsafe": blockers,
        "broker_qty": broker_qty,
        "broker_qty_known": broker_qty_known,
        "portfolio_qty": portfolio_qty,
        "broker_portfolio_converged": broker_portfolio_converged,
        "remote_open_order_count": remote_open_order_count,
        **gate_counts,
        "accounting_projection_ok": accounting_projection_ok,
        "accounting_replay": accounting_replay,
        "latest_sell_client_order_id": latest_sell.get("latest_sell_client_order_id"),
        "latest_sell_exchange_order_id": latest_sell.get("latest_sell_exchange_order_id"),
        "latest_sell_qty": latest_sell.get("latest_sell_qty"),
        "latest_trade_id": latest_sell.get("latest_trade_id"),
        "latest_sell_trade_id": latest_sell.get("latest_trade_id"),
        "latest_trade_asset_after": latest_sell.get("latest_trade_asset_after"),
        "latest_trade": latest_sell.get("latest_trade"),
        "latest_terminal_order": latest_sell.get("latest_terminal_order"),
        "stale_lot_row_count": len(stale_rows),
        "stale_lot_qty_total": stale_total_qty,
        "stale_lot_rows": stale_rows,
        "projected_total_qty_before": projected_total_qty,
        "projected_total_qty_after_preview": 0.0 if safe else projected_total_qty,
        "expected_post_projection_converged": bool(safe),
        "startup_gate_blockers": list(readiness.get("resume_blockers") or []),
        "readiness_recovery_stage": readiness.get("recovery_stage"),
        "recommended_command": FLAT_STALE_LOT_PROJECTION_REPAIR_COMMAND if safe else None,
        "preview_command": "uv run bithumb-bot rebuild-position-authority --flat-stale-projection-repair",
        "touched_tables": [
            "open_position_lots",
            "position_authority_repairs",
            "position_authority_projection_publications",
        ],
        "expected_after": "open_position_lots projection converges to broker/portfolio flat state",
        "preconditions": "broker_qty=0, portfolio_qty=0, latest SELL filled, stale dust_tracking projection present",
    }


def _build_postcondition_gate_report(
    conn: sqlite3.Connection,
    *,
    repair_mode: str,
    require_broker_portfolio_convergence: bool,
    broker_qty: float | None = None,
    broker_qty_known: bool | None = None,
    remote_open_order_count: int | None = None,
) -> dict[str, Any]:
    post_projection_convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
    post_assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
    portfolio_qty = float(post_projection_convergence.get("portfolio_qty") or 0.0)
    projected_total_qty = float(post_projection_convergence.get("projected_total_qty") or 0.0)
    gate_counts = _query_runtime_gate_counts(conn)
    fee_pending_count = int(summarize_fill_accounting_incident_projection(conn).get("active_issue_count") or 0)
    accounting_replay = compute_accounting_replay(conn)
    replay_qty = float(accounting_replay.get("replay_qty") or 0.0)
    accounting_projection_ok = bool(abs(normalize_asset_qty(replay_qty) - normalize_asset_qty(portfolio_qty)) <= _EPS)
    final_gate_failures: list[str] = []

    if require_broker_portfolio_convergence:
        if not bool(broker_qty_known):
            final_gate_failures.append("broker_position_qty_evidence_missing")
        elif abs(normalize_asset_qty(float(broker_qty or 0.0)) - normalize_asset_qty(portfolio_qty)) > _EPS:
            final_gate_failures.append(
                "broker_portfolio_qty_mismatch="
                f"broker_qty={float(broker_qty or 0.0):.12f},portfolio_qty={portfolio_qty:.12f}"
            )

    if not accounting_projection_ok:
        final_gate_failures.append(
            "accounting_projection_mismatch="
            f"replay_qty={replay_qty:.12f},portfolio_qty={portfolio_qty:.12f}"
        )
    if not bool(post_projection_convergence.get("converged")):
        final_gate_failures.append(
            "post_repair_projection_converged=0:"
            f"{post_projection_convergence.get('reason') or 'projection_non_converged'}"
        )
    if abs(projected_total_qty - portfolio_qty) > _EPS:
        final_gate_failures.append(
            "post_repair_projected_total_qty_mismatch="
            f"projected_total_qty={projected_total_qty:.12f},portfolio_qty={portfolio_qty:.12f}"
        )
    if require_broker_portfolio_convergence and bool(broker_qty_known):
        if abs(projected_total_qty - float(broker_qty or 0.0)) > _EPS:
            final_gate_failures.append(
                "post_repair_projected_total_qty_broker_mismatch="
                f"projected_total_qty={projected_total_qty:.12f},broker_qty={float(broker_qty or 0.0):.12f}"
            )

    if fee_pending_count > 0:
        final_gate_failures.append(f"fee_pending_count={fee_pending_count}")
    if int(gate_counts["unresolved_open_order_count"]) > 0:
        final_gate_failures.append(f"unresolved_open_order_count={gate_counts['unresolved_open_order_count']}")
    if int(gate_counts["recovery_required_count"]) > 0:
        final_gate_failures.append(f"recovery_required_count={gate_counts['recovery_required_count']}")
    if int(remote_open_order_count or 0) > 0:
        final_gate_failures.append(f"remote_open_order_count={int(remote_open_order_count or 0)}")
    if int(gate_counts["pending_submit_count"]) > 0:
        final_gate_failures.append(f"pending_submit_count={gate_counts['pending_submit_count']}")
    if int(gate_counts["submit_unknown_count"]) > 0:
        final_gate_failures.append(f"submit_unknown_count={gate_counts['submit_unknown_count']}")
    if not bool(post_assessment.get("semantic_contract_check_passed")):
        final_gate_failures.append(
            "semantic_contract_not_satisfied="
            f"{post_assessment.get('semantic_contract_check_state') or 'unknown'}"
        )
    if any(
        bool(post_assessment.get(key))
        for key in (
            "needs_full_projection_rebuild",
            "needs_correction",
            "needs_portfolio_projection_repair",
            "needs_residual_normalization",
        )
    ):
        final_gate_failures.append(
            "post_assessment_blockers="
            f"{'|'.join(str(item) for item in (post_assessment.get('blockers') or [])) or 'none'}"
        )

    return {
        "repair_mode": repair_mode,
        "post_assessment": post_assessment,
        "post_projection_convergence": post_projection_convergence,
        "accounting_projection_ok": accounting_projection_ok,
        "fee_pending_count": fee_pending_count,
        "broker_qty": broker_qty,
        "broker_qty_known": broker_qty_known,
        "remote_open_order_count": int(remote_open_order_count or 0),
        **gate_counts,
        "final_gate_failures": final_gate_failures,
        "why_safe": (
            "final post-state is converged and semantically valid"
            if not final_gate_failures
            else None
        ),
        "why_unsafe": list(final_gate_failures),
        "final_safe_to_apply": bool(not final_gate_failures),
    }


def _build_full_projection_rebuild_post_state_preview(
    conn: sqlite3.Connection,
    *,
    preview: dict[str, Any],
    gate_report: dict[str, Any],
) -> dict[str, Any]:
    portfolio_qty = float(preview.get("portfolio_qty") or 0.0)
    broker_qty = float(preview.get("broker_qty") or 0.0)
    pre_gate_passed = bool(not gate_report.get("reasons"))
    pre_convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
    savepoint = "position_authority_full_projection_rebuild_preview"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        projection_replay = rebuild_lifecycle_projections_from_trades(
            conn,
            pair=settings.PAIR,
            allow_entry_decision_fallback=False,
        )
        replay_convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
        anchor_summary = _replace_with_portfolio_anchored_projection(
            conn,
            portfolio_qty=portfolio_qty,
            broker_qty=broker_qty,
        )
        post_replace_convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
        publish_basis = {
            "event_type": "full_projection_materialized_rebuild_preview",
            "preview_only": True,
            "portfolio_qty": portfolio_qty,
            "broker_qty": broker_qty,
            "projection_replay": projection_replay.as_dict(),
            "portfolio_anchor_projection": anchor_summary,
        }
        record_position_authority_projection_publication(
            conn,
            event_ts=0,
            pair=settings.PAIR,
            target_trade_id=int(anchor_summary["anchor_trade_id"]),
            source="preview_full_projection_rebuild_publish",
            publish_basis=publish_basis,
            note="preview only; rolled back",
        )
        post_publish_convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
        post_publish_assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
        source_modes_of_new_rows = _source_modes_for_pair(conn, pair=settings.PAIR)
        final_gate_failures: list[str] = []
        if not bool(gate_report.get("broker_portfolio_converged")):
            final_gate_failures.append("broker_portfolio_converged=0")
        if not bool(gate_report.get("accounting_projection_ok")):
            final_gate_failures.append("accounting_projection_ok=0")
        if not bool(post_publish_convergence.get("converged")):
            final_gate_failures.append(
                "post_rebuild_projection_converged=0:"
                f"{post_publish_convergence.get('reason') or 'projection_non_converged'}"
            )
        if abs(float(post_publish_convergence.get("projected_total_qty") or 0.0) - portfolio_qty) > _EPS:
            final_gate_failures.append(
                "post_rebuild_projected_total_qty_mismatch="
                f"projected_total_qty={float(post_publish_convergence.get('projected_total_qty') or 0.0):.12f},"
                f"portfolio_qty={portfolio_qty:.12f}"
            )
        if bool(gate_report.get("unresolved_fee_pending")):
            final_gate_failures.append("fee_pending_count>0")
        if int(gate_report.get("unresolved_open_order_count") or 0) > 0:
            final_gate_failures.append(
                f"unresolved_open_order_count={int(gate_report.get('unresolved_open_order_count') or 0)}"
            )
        if int(gate_report.get("remote_open_order_count") or 0) > 0:
            final_gate_failures.append(
                f"remote_open_order_count={int(gate_report.get('remote_open_order_count') or 0)}"
            )
        if int(gate_report.get("submit_unknown_count") or 0) > 0:
            final_gate_failures.append(f"submit_unknown_count={int(gate_report.get('submit_unknown_count') or 0)}")
        if not bool(post_publish_assessment.get("semantic_contract_check_passed")):
            final_gate_failures.append(
                "semantic_contract_not_satisfied="
                f"{post_publish_assessment.get('semantic_contract_check_state') or 'unknown'}"
            )
        if any(
            bool(post_publish_assessment.get(key))
            for key in (
                "needs_full_projection_rebuild",
                "needs_correction",
                "needs_portfolio_projection_repair",
                "needs_residual_normalization",
            )
        ):
            final_gate_failures.append(
                "post_publish_authority_blockers="
                f"{'|'.join(str(item) for item in (post_publish_assessment.get('blockers') or [])) or 'none'}"
            )
        return {
            "repair_kind": "full_projection_rebuild",
            "truth_source": "broker_portfolio_anchor",
            "pre_gate_passed": pre_gate_passed,
            "pre_projected_total_qty": float(pre_convergence.get("projected_total_qty") or 0.0),
            "replay_projected_total_qty": float(replay_convergence.get("projected_total_qty") or 0.0),
            "post_replace_projected_total_qty": float(post_replace_convergence.get("projected_total_qty") or 0.0),
            "post_publish_projected_total_qty": float(post_publish_convergence.get("projected_total_qty") or 0.0),
            "portfolio_qty": portfolio_qty,
            "broker_qty": broker_qty,
            "projection_converged_before": bool(pre_convergence.get("converged")),
            "projection_converged_after_replay": bool(replay_convergence.get("converged")),
            "projection_converged_after_replace": bool(post_replace_convergence.get("converged")),
            "projection_converged_after_publish": bool(post_publish_convergence.get("converged")),
            "replay_projection_converged": bool(replay_convergence.get("converged")),
            "post_publish_projection_converged": bool(post_publish_convergence.get("converged")),
            "source_mode_of_new_rows": source_modes_of_new_rows,
            "target_lot_provenance_kind": str(post_publish_assessment.get("target_lot_provenance_kind") or "unknown"),
            "fill_qty_invariant_applies": bool(
                post_publish_assessment.get("target_lot_fill_qty_invariant_applies")
            ),
            "semantic_contract_check_applicable": bool(
                post_publish_assessment.get("semantic_contract_check_applicable")
            ),
            "semantic_contract_check_skipped_reason": post_publish_assessment.get(
                "semantic_contract_check_skipped_reason"
            ),
            "semantic_contract_check_passed": bool(post_publish_assessment.get("semantic_contract_check_passed")),
            "post_publish_assessment": post_publish_assessment,
            "projection_replay": projection_replay.as_dict(),
            "pre_projection_convergence": pre_convergence,
            "replay_projection_convergence": replay_convergence,
            "post_replace_projection_convergence": post_replace_convergence,
            "post_publish_projection_convergence": post_publish_convergence,
            "portfolio_anchor_projection": anchor_summary,
            "final_gate_failures": final_gate_failures,
            "why_safe": (
                "broker, portfolio, and rebuilt projection converge after portfolio-anchor publication"
                if pre_gate_passed and not final_gate_failures
                else None
            ),
            "why_unsafe": list(final_gate_failures),
            "final_safe_to_apply": bool(pre_gate_passed and not final_gate_failures),
            "rollback_path": "preview savepoint rollback; no DB changes committed",
        }
    finally:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")


def _simulate_non_full_position_authority_repair(
    conn: sqlite3.Connection,
    *,
    preview: dict[str, Any],
    note: str | None = None,
) -> dict[str, Any]:
    repair_mode = str(preview.get("repair_mode") or "rebuild")
    assessment = dict(preview.get("position_authority_assessment") or {})
    target_trade_id = int(assessment.get("target_trade_id") or 0)
    before = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    event_ts = int(time.time() * 1000)

    if repair_mode in {"correction", "residual_normalization", "portfolio_projection_repair"}:
        row = conn.execute(
            """
            SELECT
                t.id AS trade_id,
                t.client_order_id,
                t.ts AS fill_ts,
                t.price,
                t.qty,
                t.fee,
                t.strategy_name,
                t.entry_decision_id,
                t.exit_decision_id,
                t.exit_reason,
                t.exit_rule_name,
                f.fill_id
            FROM trades t
            LEFT JOIN fills f
              ON f.client_order_id=t.client_order_id
             AND f.fill_ts=t.ts
             AND ABS(f.price-t.price) < 1e-12
             AND ABS(f.qty-t.qty) < 1e-12
            WHERE t.id=? AND t.pair=? AND t.side='BUY'
            """,
            (target_trade_id, settings.PAIR),
        ).fetchone()
        if row is None:
            raise RuntimeError("position authority correction target BUY evidence disappeared")

        before_rows = [
            dict(item)
            for item in conn.execute(
                """
                SELECT id, pair, entry_trade_id, entry_client_order_id, entry_fill_id, qty_open,
                       executable_lot_count, dust_tracking_lot_count, internal_lot_size,
                       position_state, position_semantic_basis
                FROM open_position_lots
                WHERE pair=? AND entry_trade_id=?
                ORDER BY id ASC
                """,
                (settings.PAIR, target_trade_id),
            ).fetchall()
        ]
        sell_rows: list[dict[str, Any]] = []
        sell_trade_ids = [int(value) for value in assessment.get("sell_trade_ids") or []]
        if repair_mode == "residual_normalization":
            if not sell_trade_ids:
                raise RuntimeError("partial-close residual normalization target SELL evidence disappeared")
            placeholders = ",".join("?" for _ in sell_trade_ids)
            sell_rows = [
                dict(item)
                for item in conn.execute(
                    f"""
                    SELECT
                        t.id AS trade_id,
                        t.client_order_id,
                        t.ts AS fill_ts,
                        t.price,
                        t.qty,
                        t.fee,
                        t.strategy_name,
                        t.entry_decision_id,
                        t.exit_decision_id,
                        t.exit_reason,
                        t.exit_rule_name,
                        f.fill_id
                    FROM trades t
                    LEFT JOIN fills f
                      ON f.client_order_id=t.client_order_id
                     AND f.fill_ts=t.ts
                     AND ABS(f.price-t.price) < 1e-12
                     AND ABS(f.qty-t.qty) < 1e-12
                    WHERE t.id IN ({placeholders}) AND t.pair=? AND t.side='SELL'
                    ORDER BY t.ts ASC, t.id ASC
                    """,
                    (*sell_trade_ids, settings.PAIR),
                ).fetchall()
            ]
            if [int(item["trade_id"]) for item in sell_rows] != sell_trade_ids:
                raise RuntimeError("partial-close residual normalization target SELL evidence changed")

        before_lifecycles = [
            dict(item)
            for item in conn.execute(
                """
                SELECT *
                FROM trade_lifecycles
                WHERE pair=?
                  AND (
                        entry_trade_id=?
                        OR exit_trade_id IN (
                            SELECT id FROM trades
                            WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))
                        )
                      )
                ORDER BY id ASC
                """,
                (settings.PAIR, target_trade_id, settings.PAIR, int(row["fill_ts"]), int(row["fill_ts"]), target_trade_id),
            ).fetchall()
        ]

        if repair_mode == "portfolio_projection_repair":
            repair_basis = {
                "event_type": "portfolio_anchored_authority_projection_repair",
                "preview": preview,
                "target_trade_id": target_trade_id,
                "target_client_order_id": assessment.get("target_client_order_id"),
                "target_fill_id": assessment.get("target_fill_id"),
                "target_fill_ts": assessment.get("target_fill_ts"),
                "target_price": assessment.get("target_price"),
                "target_qty": assessment.get("target_qty"),
                "portfolio_qty": preview.get("portfolio_qty"),
                "broker_qty": preview.get("broker_qty"),
                "other_active_qty": assessment.get("other_active_qty"),
                "projected_total_qty": assessment.get("projected_total_qty"),
                "projected_qty_excess": assessment.get("projected_qty_excess"),
                "target_remainder_qty": assessment.get("portfolio_target_remainder_qty"),
                "canonical_internal_lot_size": assessment.get("canonical_internal_lot_size"),
                "canonical_executable_lot_count": assessment.get("canonical_executable_lot_count"),
                "canonical_executable_qty": assessment.get("canonical_executable_qty"),
                "old_lot_rows": before_rows,
                "old_trade_lifecycle_rows": before_lifecycles,
                "lot_snapshot_before": before,
            }
            apply_portfolio_anchored_projection_repair_basis(
                conn,
                pair=settings.PAIR,
                repair_basis=repair_basis,
            )
            after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
            repair_basis["lot_snapshot_after"] = after
            portfolio_row = conn.execute(
                """
                SELECT cash_available, cash_locked, asset_available, asset_locked
                FROM portfolio
                WHERE id=1
                """
            ).fetchone()
            replay = compute_accounting_replay(conn)
            portfolio_cash = 0.0
            portfolio_qty = 0.0
            if portfolio_row is not None:
                portfolio_cash = float(portfolio_row["cash_available"] or 0.0) + float(portfolio_row["cash_locked"] or 0.0)
                portfolio_qty = float(portfolio_row["asset_available"] or 0.0) + float(portfolio_row["asset_locked"] or 0.0)
            accounting_preview = {
                "replay_cash": float(replay.get("replay_cash") or 0.0),
                "replay_qty": float(replay.get("replay_qty") or 0.0),
                "portfolio_cash": float(portfolio_cash),
                "portfolio_qty": float(portfolio_qty),
                "cash_delta": float(portfolio_cash) - float(replay.get("replay_cash") or 0.0),
                "asset_qty_delta": float(portfolio_qty) - float(replay.get("replay_qty") or 0.0),
                "safe_to_apply": True,
                "needs_repair": bool(
                    abs(float(portfolio_cash) - float(replay.get("replay_cash") or 0.0)) > 1e-8
                    or abs(float(portfolio_qty) - float(replay.get("replay_qty") or 0.0)) > 1e-12
                ),
            }
            repair_basis["external_position_accounting_preview"] = accounting_preview
            adjustment = None
            if bool(accounting_preview.get("needs_repair")):
                adjustment_basis = {
                    "event_type": "external_position_adjustment",
                    "source_event_type": "portfolio_anchored_authority_projection_repair",
                    "target_trade_id": target_trade_id,
                    "target_client_order_id": assessment.get("target_client_order_id"),
                    "target_fill_id": assessment.get("target_fill_id"),
                    "position_authority_preview": preview,
                    "accounting_preview": accounting_preview,
                }
                adjustment = record_external_position_adjustment(
                    conn,
                    event_ts=event_ts,
                    asset_qty_delta=float(accounting_preview.get("asset_qty_delta") or 0.0),
                    cash_delta=float(accounting_preview.get("cash_delta") or 0.0),
                    source="manual_portfolio_anchored_authority_projection_repair",
                    reason="portfolio_projection_external_position_adjustment",
                    adjustment_basis=adjustment_basis,
                    note=note,
                )
            repair_basis["external_position_adjustment"] = adjustment
            publication = record_position_authority_projection_publication(
                conn,
                event_ts=event_ts,
                pair=settings.PAIR,
                target_trade_id=target_trade_id,
                source="manual_portfolio_anchored_authority_projection_publish",
                publish_basis=repair_basis,
                note=note,
            )
            repair_basis["projection_publication"] = publication
            repair = record_position_authority_repair(
                conn,
                event_ts=event_ts,
                source="manual_portfolio_anchored_authority_projection_repair",
                reason=PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON,
                repair_basis=repair_basis,
                note=note,
            )
            return {
                "repair_mode": repair_mode,
                "repair_basis": repair_basis,
                "repair": repair,
                "projection_publication": publication,
                "external_position_adjustment": adjustment,
                "lot_snapshot_before": before,
                "lot_snapshot_after": after,
            }

        conn.execute(
            "DELETE FROM open_position_lots WHERE pair=? AND entry_trade_id=?",
            (settings.PAIR, target_trade_id),
        )
        conn.execute(
            """
            DELETE FROM trade_lifecycles
            WHERE pair=?
              AND (
                    entry_trade_id=?
                    OR exit_trade_id IN (
                        SELECT id FROM trades
                        WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))
                    )
                  )
            """,
            (settings.PAIR, target_trade_id, settings.PAIR, int(row["fill_ts"]), int(row["fill_ts"]), target_trade_id),
        )
        apply_fill_lifecycle(
            conn,
            side="BUY",
            pair=settings.PAIR,
            trade_id=int(row["trade_id"]),
            client_order_id=str(row["client_order_id"]),
            fill_id=(str(row["fill_id"]) if row["fill_id"] is not None else None),
            fill_ts=int(row["fill_ts"]),
            price=float(row["price"]),
            qty=float(row["qty"]),
            fee=float(row["fee"] or 0.0),
            strategy_name=(str(row["strategy_name"]) if row["strategy_name"] is not None else None),
            entry_decision_id=(int(row["entry_decision_id"]) if row["entry_decision_id"] is not None else None),
            allow_entry_decision_fallback=False,
        )
        for sell in sell_rows:
            apply_fill_lifecycle(
                conn,
                side="SELL",
                pair=settings.PAIR,
                trade_id=int(sell["trade_id"]),
                client_order_id=str(sell["client_order_id"]),
                fill_id=(str(sell["fill_id"]) if sell["fill_id"] is not None else None),
                fill_ts=int(sell["fill_ts"]),
                price=float(sell["price"]),
                qty=float(sell["qty"]),
                fee=float(sell["fee"] or 0.0),
                strategy_name=(str(sell["strategy_name"]) if sell["strategy_name"] is not None else None),
                entry_decision_id=(int(sell["entry_decision_id"]) if sell["entry_decision_id"] is not None else None),
                exit_decision_id=(int(sell["exit_decision_id"]) if sell["exit_decision_id"] is not None else None),
                exit_reason=(str(sell["exit_reason"]) if sell["exit_reason"] is not None else None),
                exit_rule_name=(str(sell["exit_rule_name"]) if sell["exit_rule_name"] is not None else None),
                allow_entry_decision_fallback=False,
            )
        after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
        repair_basis = {
            "event_type": (
                "partial_close_residual_authority_normalization"
                if repair_mode == "residual_normalization"
                else "position_authority_correction"
            ),
            "preview": preview,
            "target_trade_id": target_trade_id,
            "target_client_order_id": assessment.get("target_client_order_id"),
            "sell_trade_ids": sell_trade_ids,
            "expected_residual_qty": assessment.get("expected_residual_qty"),
            "sell_after_target_buy_qty": assessment.get("sell_after_target_buy_qty"),
            "target_lifecycle_matched_qty": assessment.get("target_lifecycle_matched_qty"),
            "effective_closed_qty": assessment.get("effective_closed_qty"),
            "lifecycle_matched_qty_acceptance_reason": assessment.get("lifecycle_matched_qty_acceptance_reason"),
            "canonical_executable_qty": assessment.get("canonical_executable_qty"),
            "old_lot_rows": before_rows,
            "old_trade_lifecycle_rows": before_lifecycles,
            "lot_snapshot_before": before,
            "lot_snapshot_after": after,
        }
        repair = record_position_authority_repair(
            conn,
            event_ts=event_ts,
            source=(
                "manual_partial_close_residual_authority_normalization"
                if repair_mode == "residual_normalization"
                else "manual_position_authority_correction"
            ),
            reason=(
                PARTIAL_CLOSE_RESIDUAL_REPAIR_REASON
                if repair_mode == "residual_normalization"
                else "accounted_buy_fill_authority_correction"
            ),
            repair_basis=repair_basis,
            note=note,
        )
        return {
            "repair_mode": repair_mode,
            "repair_basis": repair_basis,
            "repair": repair,
            "lot_snapshot_before": before,
            "lot_snapshot_after": after,
        }

    projection_replay = rebuild_lifecycle_projections_from_trades(
        conn,
        pair=settings.PAIR,
        allow_entry_decision_fallback=False,
    )
    after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    repair_basis = {
        "event_type": "position_authority_rebuild",
        "preview": preview,
        "lot_snapshot_before": before,
        "lot_snapshot_after": after,
        "projection_replay": projection_replay.as_dict(),
    }
    repair = record_position_authority_repair(
        conn,
        event_ts=event_ts,
        source="manual_position_authority_rebuild",
        reason="accounted_buy_fill_authority_rebuild",
        repair_basis=repair_basis,
        note=note,
    )
    return {
        "repair_mode": repair_mode,
        "repair_basis": repair_basis,
        "repair": repair,
        "lot_snapshot_before": before,
        "lot_snapshot_after": after,
        "projection_replay": projection_replay.as_dict(),
    }


def build_position_authority_rebuild_preview(
    conn,
    *,
    full_projection_rebuild: bool = False,
    flat_stale_projection_repair: bool = False,
) -> dict[str, Any]:
    if flat_stale_projection_repair:
        flat_preview = build_flat_stale_lot_projection_repair_preview(conn)
        return {
            "needs_rebuild": bool(flat_preview.get("needed")),
            "safe_to_apply": bool(flat_preview.get("safe_to_apply")),
            "final_safe_to_apply": bool(flat_preview.get("final_safe_to_apply")),
            "pre_gate_passed": bool(flat_preview.get("safe_to_apply")),
            "action_state": (
                "safe_to_apply_now" if bool(flat_preview.get("safe_to_apply")) else "blocked_pending_evidence"
            ),
            "eligibility_reason": (
                "flat stale lot projection repair applicable"
                if bool(flat_preview.get("safe_to_apply"))
                else ", ".join(str(item) for item in flat_preview.get("blockers") or [])
                or str(flat_preview.get("reason") or "flat stale lot projection repair not applicable")
            ),
            "recovery_stage": str(flat_preview.get("readiness_recovery_stage") or "UNKNOWN"),
            "repair_mode": "flat_stale_projection_repair",
            "next_required_action": (
                "apply_flat_stale_lot_projection_repair"
                if bool(flat_preview.get("safe_to_apply"))
                else "review_position_authority_evidence"
            ),
            "operator_next_action": (
                "apply_flat_stale_lot_projection_repair"
                if bool(flat_preview.get("safe_to_apply"))
                else "review_position_authority_evidence"
            ),
            "preview_command": str(flat_preview.get("preview_command") or ""),
            "recommended_command": flat_preview.get("recommended_command"),
            "position_authority_assessment": build_position_authority_assessment(conn, pair=settings.PAIR),
            "portfolio_qty": float(flat_preview.get("portfolio_qty") or 0.0),
            "accounted_buy_qty": 0.0,
            "accounted_sell_qty": float(flat_preview.get("latest_sell_qty") or 0.0),
            "accounted_net_qty": 0.0,
            "accounted_buy_fill_count": 0,
            "sell_trade_count": 1 if flat_preview.get("latest_sell_trade_id") is not None else 0,
            "existing_lot_rows": int(flat_preview.get("stale_lot_row_count") or 0),
            "open_lot_count": 0,
            "dust_tracking_lot_count": int(flat_preview.get("stale_lot_row_count") or 0),
            "authority_gap_reason": str(flat_preview.get("reason") or "none"),
            "projection_converged": False,
            "projected_total_qty": float(flat_preview.get("projected_total_qty_before") or 0.0),
            "projected_qty_excess": float(flat_preview.get("projected_total_qty_before") or 0.0),
            "lot_row_count": int(flat_preview.get("stale_lot_row_count") or 0),
            "other_active_qty": 0.0,
            "portfolio_projection_publication_present": False,
            "portfolio_projection_repair_event_status": "none",
            "target_lot_provenance_kind": "stale_dust_tracking_projection",
            "target_lot_source_modes": sorted(
                {
                    str(row.get("lot_rule_source_mode") or "")
                    for row in flat_preview.get("stale_lot_rows") or []
                    if str(row.get("lot_rule_source_mode") or "")
                }
            ),
            "target_lot_fill_qty_invariant_applies": False,
            "semantic_contract_check_applicable": False,
            "semantic_contract_check_skipped_reason": "verified_flat_stale_projection_reset",
            "semantic_contract_check_passed": bool(flat_preview.get("safe_to_apply")),
            "portfolio_anchor_missing_evidence": [],
            "manual_projection_missing_evidence": [],
            "manual_db_update_unsafe": bool(not flat_preview.get("safe_to_apply")),
            "sell_after_target_buy_qty": 0.0,
            "target_lifecycle_matched_qty": 0.0,
            "effective_closed_qty": float(flat_preview.get("latest_sell_qty") or 0.0),
            "expected_residual_qty": 0.0,
            "target_residual_qty_delta": 0.0,
            "residual_qty_tolerance": _EPS,
            "sell_after_qty_authority_mode": "terminal_flat_sell_evidence",
            "lifecycle_matched_qty_accepted": False,
            "lifecycle_matched_qty_acceptance_reason": "not_required_for_verified_flat_reset",
            "needs_full_projection_rebuild": False,
            "broker_qty": float(flat_preview.get("broker_qty") or 0.0),
            "broker_qty_known": bool(flat_preview.get("broker_qty_known")),
            "broker_qty_value_source": None,
            "broker_qty_evidence_source": None,
            "broker_qty_evidence_observed_ts_ms": None,
            "balance_source": None,
            "balance_source_stale": None,
            "balance_snapshot_available_for_health": None,
            "balance_snapshot_available_for_position_rebuild": None,
            "missing_evidence_fields": [],
            "position_rebuild_blockers": [],
            "base_currency": None,
            "quote_currency": None,
            "asset_available": None,
            "asset_locked": None,
            "cash_available": None,
            "cash_locked": None,
            "remote_open_order_count": int(flat_preview.get("remote_open_order_count") or 0),
            "broker_portfolio_converged": bool(flat_preview.get("broker_portfolio_converged")),
            "accounting_projection_ok": bool(flat_preview.get("accounting_projection_ok")),
            "unresolved_open_order_count": int(flat_preview.get("unresolved_open_order_count") or 0),
            "pending_submit_count": int(flat_preview.get("pending_submit_count") or 0),
            "submit_unknown_count": int(flat_preview.get("submit_unknown_count") or 0),
            "unresolved_fee_pending": False,
            "full_projection_rebuild_gate_report": None,
            "repair_kind": "flat_stale_lot_projection_repair",
            "truth_source": "broker_portfolio_terminal_sell_flat_evidence",
            "pre_projected_total_qty": float(flat_preview.get("projected_total_qty_before") or 0.0),
            "replay_projected_total_qty": None,
            "post_publish_projected_total_qty": float(flat_preview.get("projected_total_qty_after_preview") or 0.0),
            "projection_converged_before": False,
            "projection_converged_after_replay": None,
            "projection_converged_after_publish": bool(flat_preview.get("expected_post_projection_converged")),
            "replay_projection_converged": None,
            "post_publish_projection_converged": bool(flat_preview.get("expected_post_projection_converged")),
            "source_mode_of_new_rows": [],
            "rollback_path": "preview only; no DB changes committed",
            "why_safe": (
                "broker, portfolio, accounting replay, terminal SELL evidence, and dust-only stale rows prove flat reset"
                if bool(flat_preview.get("safe_to_apply"))
                else None
            ),
            "why_unsafe": list(flat_preview.get("blockers") or []),
            "flat_stale_projection_repair_preview": flat_preview,
        }
    snapshot = compute_runtime_readiness_snapshot(conn)
    authority_assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
    lot_snapshot = snapshot.lot_snapshot
    position = snapshot.position_state.normalized_exposure
    portfolio_qty = float(position.raw_qty_open)

    rows = conn.execute(
        """
        SELECT
            t.id AS trade_id,
            t.client_order_id,
            t.ts AS fill_ts,
            t.price,
            t.qty,
            t.fee,
            t.strategy_name,
            t.entry_decision_id,
            f.fill_id
        FROM trades t
        LEFT JOIN fills f
          ON f.client_order_id=t.client_order_id
         AND f.fill_ts=t.ts
         AND ABS(f.price-t.price) < 1e-12
         AND ABS(f.qty-t.qty) < 1e-12
        WHERE t.pair=? AND t.side='BUY'
        ORDER BY t.ts ASC, t.id ASC
        """,
        (settings.PAIR,),
    ).fetchall()
    sell_row = conn.execute(
        "SELECT COUNT(*) AS cnt, COALESCE(SUM(qty), 0.0) AS qty FROM trades WHERE pair=? AND side='SELL'",
        (settings.PAIR,),
    ).fetchone()
    existing_lot_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM open_position_lots WHERE pair=? AND qty_open > 1e-12",
        (settings.PAIR,),
    ).fetchone()

    buy_qty = normalize_asset_qty(sum(float(row["qty"] or 0.0) for row in rows))
    sell_count = int(sell_row["cnt"] if sell_row else 0)
    sell_qty = normalize_asset_qty(float(sell_row["qty"] or 0.0) if sell_row else 0.0)
    net_accounted_qty = normalize_asset_qty(max(0.0, buy_qty - sell_qty))
    existing_lot_count = int(existing_lot_row["cnt"] if existing_lot_row else 0)
    reasons: list[str] = []
    authority_action_state = str(authority_assessment.get("repair_action_state") or "not_applicable")
    full_projection_gate_report: dict[str, Any] | None = None
    full_projection_post_state_preview: dict[str, Any] | None = None
    post_state_preview: dict[str, Any] | None = None
    projection_convergence = dict(authority_assessment.get("projection_convergence") or {})
    flat_preview = build_flat_stale_lot_projection_repair_preview(conn)
    if not full_projection_rebuild and bool(flat_preview.get("needed")):
        return build_position_authority_rebuild_preview(conn, flat_stale_projection_repair=True)

    if full_projection_rebuild or bool(authority_assessment.get("needs_full_projection_rebuild")):
        repair_mode = "full_projection_rebuild"
    elif bool(authority_assessment.get("needs_portfolio_projection_repair")):
        repair_mode = "portfolio_projection_repair"
    elif bool(authority_assessment.get("needs_residual_normalization")):
        repair_mode = "residual_normalization"
    elif bool(authority_assessment.get("needs_correction")):
        repair_mode = "correction"
    else:
        repair_mode = "rebuild"
    if repair_mode in {"correction", "residual_normalization", "portfolio_projection_repair"}:
        reasons.extend(str(item) for item in authority_assessment.get("blockers") or [])
    elif snapshot.recovery_stage != "AUTHORITY_REBUILD_PENDING":
        reasons.append(f"recovery_stage={snapshot.recovery_stage}")
    if snapshot.open_order_count > 0:
        reasons.append(f"open_or_unresolved_orders={snapshot.open_order_count}")
    if snapshot.recovery_required_count > 0:
        reasons.append(f"recovery_required_orders={snapshot.recovery_required_count}")
    broker_evidence = build_broker_position_evidence(snapshot.reconcile_metadata, pair=settings.PAIR)
    broker_qty = float(broker_evidence.get("broker_qty") or 0.0)
    broker_qty_known = bool(broker_evidence.get("broker_qty_known"))
    remote_open_order_count = int(snapshot.reconcile_metadata.get("remote_open_order_found", 0) or 0)
    if repair_mode == "full_projection_rebuild":
        full_projection_gate_report = _build_full_projection_rebuild_gate_report(
            conn,
            snapshot=snapshot,
            authority_assessment=authority_assessment,
            portfolio_qty=portfolio_qty,
        )
        reasons = list(full_projection_gate_report["reasons"])
        if not reasons:
            full_projection_post_state_preview = _build_full_projection_rebuild_post_state_preview(
                conn,
                preview={
                    "portfolio_qty": portfolio_qty,
                    "broker_qty": broker_qty,
                },
                gate_report=full_projection_gate_report,
            )
            if not bool(full_projection_post_state_preview.get("final_safe_to_apply")):
                reasons = list(full_projection_post_state_preview.get("final_gate_failures") or [])
    elif repair_mode in {"correction", "residual_normalization", "portfolio_projection_repair", "rebuild"} and not reasons:
        savepoint = "position_authority_preview_postcheck"
        conn.execute(f"SAVEPOINT {savepoint}")
        try:
            simulated = _simulate_non_full_position_authority_repair(
                conn,
                preview={
                    "repair_mode": repair_mode,
                    "position_authority_assessment": authority_assessment,
                    "portfolio_qty": portfolio_qty,
                    "broker_qty": broker_qty,
                    "broker_qty_known": broker_qty_known,
                },
            )
            post_state_preview = {
                **simulated,
                **_build_postcondition_gate_report(
                    conn,
                    repair_mode=repair_mode,
                    require_broker_portfolio_convergence=False,
                    broker_qty=broker_qty,
                    broker_qty_known=broker_qty_known,
                    remote_open_order_count=remote_open_order_count,
                ),
                "source_mode_of_new_rows": _source_modes_for_pair(conn, pair=settings.PAIR),
                "rollback_path": "preview savepoint rollback; no DB changes committed",
            }
        finally:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        if not bool(post_state_preview.get("final_safe_to_apply")):
            reasons = list(post_state_preview.get("final_gate_failures") or [])
    if repair_mode == "portfolio_projection_repair":
        portfolio_remainder_qty = normalize_asset_qty(
            float(authority_assessment.get("portfolio_target_remainder_qty") or 0.0)
        )
        canonical_lot_size = float(authority_assessment.get("canonical_internal_lot_size") or 0.0)
        if remote_open_order_count > 0:
            reasons.append(f"remote_open_orders={remote_open_order_count}")
        if not broker_qty_known:
            reasons.append("broker_position_qty_evidence_missing")
        elif abs(normalize_asset_qty(broker_qty) - normalize_asset_qty(portfolio_qty)) > 1e-12:
            reasons.append(
                "broker_portfolio_qty_mismatch="
                f"broker_qty={broker_qty:.12f},portfolio_qty={portfolio_qty:.12f}"
            )
        if canonical_lot_size <= 1e-12:
            reasons.append("canonical_internal_lot_size_missing")
        elif portfolio_remainder_qty >= canonical_lot_size - 1e-12:
            reasons.append(
                "portfolio_remainder_still_executable="
                f"remainder_qty={portfolio_remainder_qty:.12f},lot_size={canonical_lot_size:.12f}"
            )
        if not bool(authority_assessment.get("projection_repair_covers_excess")):
            reasons.append(
                "projection_excess_outside_target="
                f"projected_qty_excess={float(authority_assessment.get('projected_qty_excess') or 0.0):.12f},"
                f"repair_removable_qty={float(authority_assessment.get('projection_repair_removable_qty') or 0.0):.12f}"
            )
    if repair_mode == "rebuild":
        if existing_lot_count > 0:
            reasons.append(f"existing_lot_rows={existing_lot_count}")
        if not rows:
            reasons.append("accounted_buy_fill_evidence_missing")
        if portfolio_qty <= 1e-12:
            reasons.append("portfolio_asset_qty_not_positive")
        if abs(net_accounted_qty - normalize_asset_qty(portfolio_qty)) > 1e-12:
            reasons.append(
                "accounted_net_qty_portfolio_mismatch="
                f"buy_qty={buy_qty:.12f},sell_qty={sell_qty:.12f},"
                f"net_qty={net_accounted_qty:.12f},portfolio_qty={portfolio_qty:.12f}"
            )

    effective_action_state = authority_action_state
    if repair_mode == "rebuild" and not reasons:
        effective_action_state = "safe_to_apply_now"
    elif repair_mode == "full_projection_rebuild" and not reasons:
        effective_action_state = "safe_to_apply_now"
    elif repair_mode == "full_projection_rebuild":
        effective_action_state = (
            "inspect_only"
            if full_projection_post_state_preview is not None
            and list(full_projection_post_state_preview.get("final_gate_failures") or [])
            else "blocked_pending_evidence"
        )
    elif repair_mode == "rebuild" and authority_action_state == "not_applicable":
        effective_action_state = "blocked_pending_evidence"

    safe_to_apply = bool(not reasons and effective_action_state == "safe_to_apply_now")
    final_safe_to_apply = (
        bool(full_projection_post_state_preview.get("final_safe_to_apply"))
        if full_projection_post_state_preview is not None
        else (
            bool(not reasons and post_state_preview.get("final_safe_to_apply"))
            if post_state_preview is not None
            else safe_to_apply
        )
    )
    if repair_mode in {"full_projection_rebuild", "correction", "residual_normalization", "portfolio_projection_repair", "rebuild"}:
        safe_to_apply = final_safe_to_apply
    next_required_action = (
        "review_rebuild_replay"
        if repair_mode == "full_projection_rebuild"
        and full_projection_post_state_preview is not None
        and not final_safe_to_apply
        else (
            "review_position_authority_evidence"
            if post_state_preview is not None and not final_safe_to_apply
            else ("apply_rebuild_position_authority" if safe_to_apply else snapshot.operator_next_action)
        )
    )
    preview_command = (
        "uv run python bot.py rebuild-position-authority --full-projection-rebuild"
        if repair_mode == "full_projection_rebuild"
        else "uv run python bot.py rebuild-position-authority"
    )
    recommended_command = (
        "uv run python bot.py rebuild-position-authority --full-projection-rebuild --apply --yes"
        if repair_mode == "full_projection_rebuild" and final_safe_to_apply
        else (
            "uv run python bot.py rebuild-position-authority --apply --yes"
            if repair_mode != "full_projection_rebuild" and safe_to_apply
            else None
        )
    )
    return {
        "needs_rebuild": snapshot.recovery_stage in {
            "AUTHORITY_REBUILD_PENDING",
            "AUTHORITY_CORRECTION_PENDING",
            "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING",
            "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING",
            "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING",
        },
        "safe_to_apply": safe_to_apply,
        "final_safe_to_apply": final_safe_to_apply,
        "pre_gate_passed": (
            bool(full_projection_post_state_preview.get("pre_gate_passed"))
            if full_projection_post_state_preview is not None
            else bool(not reasons)
        ),
        "action_state": effective_action_state,
        "eligibility_reason": (
            "partial-close residual authority normalization applicable"
            if safe_to_apply and repair_mode == "residual_normalization"
            else (
                "portfolio-anchored projection repair applicable"
                if safe_to_apply and repair_mode == "portfolio_projection_repair"
                else (
                "full projection rebuild applicable"
                if safe_to_apply and repair_mode == "full_projection_rebuild"
                else (
                "position authority correction applicable"
                if safe_to_apply and repair_mode == "correction"
                else ("position authority rebuild applicable" if safe_to_apply else ", ".join(dict.fromkeys(reasons)))
                )
                )
            )
        ),
        "recovery_stage": snapshot.recovery_stage,
        "repair_mode": repair_mode,
        "next_required_action": next_required_action,
        "operator_next_action": next_required_action,
        "preview_command": preview_command,
        "recommended_command": recommended_command,
        "post_assessment_blockers": (
            list((full_projection_post_state_preview or {}).get("post_publish_assessment", {}).get("blockers") or [])
            or list((post_state_preview or {}).get("post_assessment", {}).get("blockers") or [])
        ),
        "position_authority_assessment": authority_assessment,
        "portfolio_qty": portfolio_qty,
        "accounted_buy_qty": buy_qty,
        "accounted_sell_qty": sell_qty,
        "accounted_net_qty": net_accounted_qty,
        "accounted_buy_fill_count": len(rows),
        "sell_trade_count": sell_count,
        "existing_lot_rows": existing_lot_count,
        "open_lot_count": int(lot_snapshot.open_lot_count),
        "dust_tracking_lot_count": int(lot_snapshot.dust_tracking_lot_count),
        "authority_gap_reason": position.authority_gap_reason,
        "projection_converged": bool(projection_convergence.get("converged")),
        "projected_total_qty": float(authority_assessment.get("projected_total_qty") or 0.0),
        "projected_qty_excess": float(authority_assessment.get("projected_qty_excess") or 0.0),
        "lot_row_count": int(projection_convergence.get("lot_row_count") or 0),
        "other_active_qty": float(authority_assessment.get("other_active_qty") or 0.0),
        "portfolio_projection_publication_present": bool(
            authority_assessment.get("portfolio_projection_publication_present")
        ),
        "portfolio_projection_repair_event_status": str(
            authority_assessment.get("portfolio_projection_repair_event_status") or "none"
        ),
        "target_lot_provenance_kind": (
            str(
                (
                    (full_projection_post_state_preview or {})
                    .get("post_publish_assessment", {})
                    .get("target_lot_provenance_kind")
                )
                or (post_state_preview or {}).get("post_assessment", {}).get("target_lot_provenance_kind")
                or authority_assessment.get("target_lot_provenance_kind")
                or "unknown"
            )
        ),
        "target_lot_source_modes": (
            list(
                (
                    (full_projection_post_state_preview or {})
                    .get("post_publish_assessment", {})
                    .get("target_lot_source_modes")
                )
                or (post_state_preview or {}).get("post_assessment", {}).get("target_lot_source_modes")
                or authority_assessment.get("target_lot_source_modes")
                or []
            )
        ),
        "target_lot_fill_qty_invariant_applies": bool(
            (
                (full_projection_post_state_preview or {})
                .get("post_publish_assessment", {})
                .get("target_lot_fill_qty_invariant_applies")
            )
            if full_projection_post_state_preview is not None
            else (
                (post_state_preview or {}).get("post_assessment", {}).get("target_lot_fill_qty_invariant_applies")
                if post_state_preview is not None
                else authority_assessment.get("target_lot_fill_qty_invariant_applies")
            )
        ),
        "semantic_contract_check_applicable": bool(
            (
                (full_projection_post_state_preview or {})
                .get("post_publish_assessment", {})
                .get("semantic_contract_check_applicable")
            )
            if full_projection_post_state_preview is not None
            else (
                (post_state_preview or {}).get("post_assessment", {}).get("semantic_contract_check_applicable")
                if post_state_preview is not None
                else authority_assessment.get("semantic_contract_check_applicable")
            )
        ),
        "semantic_contract_check_skipped_reason": (
            (full_projection_post_state_preview or {})
            .get("post_publish_assessment", {})
            .get("semantic_contract_check_skipped_reason")
            if full_projection_post_state_preview is not None
            else (
                (post_state_preview or {}).get("post_assessment", {}).get("semantic_contract_check_skipped_reason")
                if post_state_preview is not None
                else authority_assessment.get("semantic_contract_check_skipped_reason")
            )
        ),
        "semantic_contract_check_passed": bool(
            (
                (full_projection_post_state_preview or {})
                .get("post_publish_assessment", {})
                .get("semantic_contract_check_passed")
            )
            if full_projection_post_state_preview is not None
            else (
                (post_state_preview or {}).get("post_assessment", {}).get("semantic_contract_check_passed")
                if post_state_preview is not None
                else authority_assessment.get("semantic_contract_check_passed")
            )
        ),
        "portfolio_anchor_missing_evidence": list(authority_assessment.get("portfolio_anchor_missing_evidence") or []),
        "manual_projection_missing_evidence": list(authority_assessment.get("manual_projection_missing_evidence") or []),
        "manual_db_update_unsafe": bool(not safe_to_apply),
        "sell_after_target_buy_qty": float(authority_assessment.get("sell_after_target_buy_qty") or 0.0),
        "target_lifecycle_matched_qty": float(authority_assessment.get("target_lifecycle_matched_qty") or 0.0),
        "effective_closed_qty": float(authority_assessment.get("effective_closed_qty") or 0.0),
        "expected_residual_qty": float(authority_assessment.get("expected_residual_qty") or 0.0),
        "target_residual_qty_delta": float(authority_assessment.get("target_residual_qty_delta") or 0.0),
        "residual_qty_tolerance": float(authority_assessment.get("residual_qty_tolerance") or 0.0),
        "sell_after_qty_authority_mode": str(
            authority_assessment.get("sell_after_qty_authority_mode") or "not_applicable"
        ),
        "lifecycle_matched_qty_accepted": bool(authority_assessment.get("lifecycle_matched_qty_accepted")),
        "lifecycle_matched_qty_acceptance_reason": str(
            authority_assessment.get("lifecycle_matched_qty_acceptance_reason") or "none"
        ),
        "needs_full_projection_rebuild": bool(authority_assessment.get("needs_full_projection_rebuild")),
        "broker_qty": broker_qty,
        "broker_qty_known": broker_qty_known,
        "broker_qty_value_source": broker_evidence.get("broker_qty_value_source"),
        "broker_qty_evidence_source": broker_evidence.get("broker_qty_evidence_source"),
        "broker_qty_evidence_observed_ts_ms": broker_evidence.get("broker_qty_evidence_observed_ts_ms"),
        "balance_source": broker_evidence.get("balance_source"),
        "balance_source_stale": broker_evidence.get("balance_source_stale"),
        "balance_snapshot_available_for_health": broker_evidence.get("balance_snapshot_available_for_health"),
        "balance_snapshot_available_for_position_rebuild": broker_evidence.get(
            "balance_snapshot_available_for_position_rebuild"
        ),
        "missing_evidence_fields": list(broker_evidence.get("missing_evidence_fields") or []),
        "position_rebuild_blockers": list(broker_evidence.get("position_rebuild_blockers") or []),
        "base_currency": broker_evidence.get("base_currency"),
        "quote_currency": broker_evidence.get("quote_currency"),
        "asset_available": broker_evidence.get("asset_available"),
        "asset_locked": broker_evidence.get("asset_locked"),
        "cash_available": broker_evidence.get("cash_available"),
        "cash_locked": broker_evidence.get("cash_locked"),
        "remote_open_order_count": remote_open_order_count,
        "broker_portfolio_converged": (
            bool(full_projection_gate_report["broker_portfolio_converged"])
            if full_projection_gate_report is not None
            else (post_state_preview.get("broker_qty_known") if post_state_preview is not None else None)
        ),
        "accounting_projection_ok": (
            bool(full_projection_gate_report["accounting_projection_ok"])
            if full_projection_gate_report is not None
            else ((post_state_preview or {}).get("accounting_projection_ok") if post_state_preview is not None else None)
        ),
        "unresolved_open_order_count": (
            int(full_projection_gate_report["unresolved_open_order_count"])
            if full_projection_gate_report is not None
            else ((post_state_preview or {}).get("unresolved_open_order_count") if post_state_preview is not None else None)
        ),
        "pending_submit_count": (
            int(full_projection_gate_report["pending_submit_count"])
            if full_projection_gate_report is not None
            else ((post_state_preview or {}).get("pending_submit_count") if post_state_preview is not None else None)
        ),
        "submit_unknown_count": (
            int(full_projection_gate_report["submit_unknown_count"])
            if full_projection_gate_report is not None
            else ((post_state_preview or {}).get("submit_unknown_count") if post_state_preview is not None else None)
        ),
        "unresolved_fee_pending": (
            bool(full_projection_gate_report["unresolved_fee_pending"])
            if full_projection_gate_report is not None
            else (int((post_state_preview or {}).get("fee_pending_count") or 0) > 0 if post_state_preview is not None else None)
        ),
        "full_projection_rebuild_gate_report": full_projection_gate_report,
        "repair_kind": (
            "full_projection_rebuild" if repair_mode == "full_projection_rebuild" else str(repair_mode)
        ),
        "truth_source": (
            "broker_portfolio_anchor" if repair_mode == "full_projection_rebuild" else "position_authority_assessment"
        ),
        "pre_projected_total_qty": (
            float(full_projection_post_state_preview.get("pre_projected_total_qty") or 0.0)
            if full_projection_post_state_preview is not None
            else float(authority_assessment.get("projected_total_qty") or 0.0)
        ),
        "replay_projected_total_qty": (
            float(full_projection_post_state_preview.get("replay_projected_total_qty") or 0.0)
            if full_projection_post_state_preview is not None
            else None
        ),
        "post_publish_projected_total_qty": (
            float(full_projection_post_state_preview.get("post_publish_projected_total_qty") or 0.0)
            if full_projection_post_state_preview is not None
            else float((post_state_preview or {}).get("post_projection_convergence", {}).get("projected_total_qty") or 0.0)
            if post_state_preview is not None
            else None
        ),
        "projection_converged_before": (
            bool(full_projection_post_state_preview.get("projection_converged_before"))
            if full_projection_post_state_preview is not None
            else bool(projection_convergence.get("converged"))
        ),
        "projection_converged_after_replay": (
            None
            if full_projection_post_state_preview is None
            else bool(full_projection_post_state_preview.get("projection_converged_after_replay"))
        ),
        "projection_converged_after_publish": (
            bool(full_projection_post_state_preview.get("projection_converged_after_publish"))
            if full_projection_post_state_preview is not None
            else bool((post_state_preview or {}).get("post_projection_convergence", {}).get("converged"))
            if post_state_preview is not None
            else None
        ),
        "replay_projection_converged": (
            None
            if full_projection_post_state_preview is None
            else bool(full_projection_post_state_preview.get("replay_projection_converged"))
        ),
        "post_publish_projection_converged": (
            bool(full_projection_post_state_preview.get("post_publish_projection_converged"))
            if full_projection_post_state_preview is not None
            else bool((post_state_preview or {}).get("post_projection_convergence", {}).get("converged"))
            if post_state_preview is not None
            else None
        ),
        "source_mode_of_new_rows": (
            list(full_projection_post_state_preview.get("source_mode_of_new_rows") or [])
            if full_projection_post_state_preview is not None
            else list((post_state_preview or {}).get("source_mode_of_new_rows") or [])
        ),
        "rollback_path": (
            str(
                (full_projection_post_state_preview or {}).get("rollback_path")
                or (post_state_preview or {}).get("rollback_path")
                or ""
            ) or None
        ),
        "why_safe": (
            (full_projection_post_state_preview or {}).get("why_safe")
            or (post_state_preview or {}).get("why_safe")
        ),
        "why_unsafe": (
            list((full_projection_post_state_preview or {}).get("why_unsafe") or [])
            or list((post_state_preview or {}).get("why_unsafe") or [])
        ),
        "full_projection_rebuild_post_state_preview": full_projection_post_state_preview,
        "post_state_preview": post_state_preview,
    }


def _assert_post_repair_projection_converged(
    conn,
    *,
    repair_basis: dict[str, Any],
    repair_mode: str,
) -> dict[str, Any]:
    convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
    repair_basis["post_repair_projection_convergence"] = convergence
    if not bool(convergence.get("converged")):
        raise RuntimeError(
            "position authority repair postcondition failed: "
            f"repair_mode={repair_mode}; projection_converged=0; "
            f"reason={convergence.get('reason')}; "
            f"projected_total_qty={float(convergence.get('projected_total_qty') or 0.0):.12f}; "
            f"portfolio_qty={float(convergence.get('portfolio_qty') or 0.0):.12f}"
        )
    return convergence


def apply_flat_stale_lot_projection_repair(
    conn: sqlite3.Connection,
    *,
    note: str | None = None,
) -> dict[str, Any]:
    preview = build_flat_stale_lot_projection_repair_preview(conn)
    if not bool(preview.get("safe_to_apply")):
        if (
            not bool(preview.get("needed"))
            and float(preview.get("projected_total_qty_before") or 0.0) <= _EPS
            and int(preview.get("stale_lot_row_count") or 0) == 0
        ):
            return {
                "preview": preview,
                "noop": True,
                "lot_snapshot_before": summarize_position_lots(conn, pair=settings.PAIR).as_dict(),
                "lot_snapshot_after": summarize_position_lots(conn, pair=settings.PAIR).as_dict(),
                "post_repair_projection_convergence": build_lot_projection_convergence(conn, pair=settings.PAIR),
            }
        raise RuntimeError(
            "flat stale lot projection repair is not safe to apply: "
            f"{'|'.join(str(item) for item in preview.get('blockers') or []) or preview.get('reason') or 'unknown'}"
        )

    savepoint = "flat_stale_lot_projection_repair"
    before = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    convergence_before = build_lot_projection_convergence(conn, pair=settings.PAIR)
    readiness_before = compute_runtime_readiness_snapshot(conn).as_dict()
    stale_rows = list(preview.get("stale_lot_rows") or [])
    event_ts = int(time.time() * 1000)
    repair_basis: dict[str, Any] = {
        "event_type": FLAT_STALE_LOT_PROJECTION_REPAIR_REASON,
        "repair_mode": "flat_stale_projection_repair",
        "operator_command": FLAT_STALE_LOT_PROJECTION_REPAIR_COMMAND,
        "operator_timestamp_ms": event_ts,
        "preview": preview,
        "broker_qty": float(preview.get("broker_qty") or 0.0),
        "portfolio_qty": float(preview.get("portfolio_qty") or 0.0),
        "latest_sell_client_order_id": preview.get("latest_sell_client_order_id"),
        "latest_sell_exchange_order_id": preview.get("latest_sell_exchange_order_id"),
        "latest_sell_qty": preview.get("latest_sell_qty"),
        "latest_sell_trade_id": preview.get("latest_sell_trade_id"),
        "latest_trade_id": preview.get("latest_trade_id"),
        "latest_trade_asset_after": preview.get("latest_trade_asset_after"),
        "latest_terminal_order": preview.get("latest_terminal_order"),
        "latest_trade": preview.get("latest_trade"),
        "open_position_lots_before_repair": stale_rows,
        "projected_total_qty_before": preview.get("projected_total_qty_before"),
        "startup_gate_blockers": list(preview.get("startup_gate_blockers") or []),
        "readiness_recovery_stage": preview.get("readiness_recovery_stage"),
        "lot_snapshot_before": before,
        "projection_convergence_before": convergence_before,
        "runtime_readiness_before": readiness_before,
        "accounting_replay": preview.get("accounting_replay"),
    }
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        ids = [int(row["id"]) for row in stale_rows]
        placeholders = ",".join("?" for _ in ids)
        deleted = conn.execute(
            f"DELETE FROM open_position_lots WHERE pair=? AND id IN ({placeholders})",
            (settings.PAIR, *ids),
        ).rowcount
        if int(deleted or 0) != len(ids):
            raise RuntimeError("flat stale lot projection repair postcondition failed: stale row set changed")
        after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
        repair_basis["lot_snapshot_after"] = after
        convergence_after = _assert_post_repair_projection_converged(
            conn,
            repair_basis=repair_basis,
            repair_mode="flat_stale_projection_repair",
        )
        if (
            abs(float(convergence_after.get("projected_total_qty") or 0.0)) > _EPS
            or abs(float(convergence_after.get("portfolio_qty") or 0.0)) > _EPS
            or abs(float(preview.get("broker_qty") or 0.0)) > _EPS
        ):
            raise RuntimeError(
                "flat stale lot projection repair postcondition failed: "
                f"projected_total_qty={float(convergence_after.get('projected_total_qty') or 0.0):.12f},"
                f"portfolio_qty={float(convergence_after.get('portfolio_qty') or 0.0):.12f},"
                f"broker_qty={float(preview.get('broker_qty') or 0.0):.12f}"
            )
        post_assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
        repair_basis["position_authority_assessment_after"] = post_assessment
        if any(
            bool(post_assessment.get(key))
            for key in (
                "needs_full_projection_rebuild",
                "needs_correction",
                "needs_portfolio_projection_repair",
                "needs_residual_normalization",
            )
        ):
            raise RuntimeError(
                "flat stale lot projection repair postcondition failed: "
                f"blockers={'|'.join(str(item) for item in post_assessment.get('blockers') or []) or 'none'}"
            )
        publication = record_position_authority_projection_publication(
            conn,
            event_ts=event_ts,
            pair=settings.PAIR,
            target_trade_id=int(preview.get("latest_sell_trade_id") or 0),
            source="manual_flat_stale_lot_projection_repair_publish",
            publish_basis=repair_basis,
            note=note,
        )
        repair_basis["projection_publication"] = publication
        repair = record_position_authority_repair(
            conn,
            event_ts=event_ts,
            source="manual_flat_stale_lot_projection_repair",
            reason=FLAT_STALE_LOT_PROJECTION_REPAIR_REASON,
            repair_basis=repair_basis,
            note=note,
        )
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise
    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    return {
        "preview": preview,
        "repair": repair,
        "projection_publication": publication,
        "repair_basis": repair_basis,
        "lot_snapshot_before": before,
        "lot_snapshot_after": after,
        "post_repair_projection_convergence": convergence_after,
        "post_repair_assessment": post_assessment,
    }


def _load_position_authority_history_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    repair_summary = conn.execute(
        """
        SELECT COUNT(*) AS repair_count, MAX(event_ts) AS last_event_ts
        FROM position_authority_repairs
        """
    ).fetchone()
    publication_summary = conn.execute(
        """
        SELECT COUNT(*) AS publication_count, MAX(event_ts) AS last_event_ts
        FROM position_authority_projection_publications
        WHERE pair=?
        """,
        (settings.PAIR,),
    ).fetchone()
    return {
        "repair_count": int(repair_summary["repair_count"] or 0) if repair_summary is not None else 0,
        "repair_last_event_ts": (
            int(repair_summary["last_event_ts"]) if repair_summary is not None and repair_summary["last_event_ts"] is not None else None
        ),
        "publication_count": (
            int(publication_summary["publication_count"] or 0) if publication_summary is not None else 0
        ),
        "publication_last_event_ts": (
            int(publication_summary["last_event_ts"])
            if publication_summary is not None and publication_summary["last_event_ts"] is not None
            else None
        ),
    }


def _replace_with_portfolio_anchored_projection(
    conn: sqlite3.Connection,
    *,
    portfolio_qty: float,
    broker_qty: float,
) -> dict[str, Any]:
    anchor_row = _fetch_anchor_buy_row(conn)
    if anchor_row is None:
        raise RuntimeError("full projection rebuild requires accounted BUY fill evidence")
    lot_size = float(anchor_row["internal_lot_size"] or 0.0)
    if lot_size <= _EPS:
        raise RuntimeError("full projection rebuild requires authoritative internal lot size")

    normalized_portfolio_qty = normalize_asset_qty(portfolio_qty)
    executable_lot_count = int(max(0, normalized_portfolio_qty / lot_size + _EPS))
    executable_qty = normalize_asset_qty(lot_count_to_qty(lot_count=executable_lot_count, lot_size=lot_size))
    dust_qty = normalize_asset_qty(max(0.0, normalized_portfolio_qty - executable_qty))

    conn.execute("DELETE FROM open_position_lots WHERE pair=?", (settings.PAIR,))
    if executable_qty > _EPS:
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total, strategy_name, entry_decision_id, entry_decision_linkage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                int(anchor_row["trade_id"]),
                str(anchor_row["client_order_id"]),
                (str(anchor_row["fill_id"]) if anchor_row["fill_id"] is not None else None),
                int(anchor_row["fill_ts"]),
                float(anchor_row["price"]),
                executable_qty,
                executable_lot_count,
                0,
                LOT_SEMANTIC_VERSION_V1,
                lot_size,
                float(anchor_row["effective_min_trade_qty"] or lot_size),
                float(anchor_row["qty_step"] or lot_size),
                float(anchor_row["min_notional_krw"] or 0.0),
                int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
                "full_projection_rebuild_portfolio_anchor",
                "lot-native",
                OPEN_POSITION_STATE,
                0.0,
                (str(anchor_row["strategy_name"]) if anchor_row["strategy_name"] is not None else None),
                (int(anchor_row["entry_decision_id"]) if anchor_row["entry_decision_id"] is not None else None),
                ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED,
            ),
        )
    if dust_qty > _EPS:
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total, strategy_name, entry_decision_id, entry_decision_linkage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                int(anchor_row["trade_id"]),
                str(anchor_row["client_order_id"]),
                (str(anchor_row["fill_id"]) if anchor_row["fill_id"] is not None else None),
                int(anchor_row["fill_ts"]),
                float(anchor_row["price"]),
                dust_qty,
                0,
                1,
                LOT_SEMANTIC_VERSION_V1,
                lot_size,
                float(anchor_row["effective_min_trade_qty"] or lot_size),
                float(anchor_row["qty_step"] or lot_size),
                float(anchor_row["min_notional_krw"] or 0.0),
                int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
                "full_projection_rebuild_portfolio_anchor",
                "lot-native",
                DUST_TRACKING_STATE,
                0.0,
                (str(anchor_row["strategy_name"]) if anchor_row["strategy_name"] is not None else None),
                (int(anchor_row["entry_decision_id"]) if anchor_row["entry_decision_id"] is not None else None),
                ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED,
            ),
        )
    return {
        "anchor_trade_id": int(anchor_row["trade_id"]),
        "anchor_client_order_id": str(anchor_row["client_order_id"]),
        "anchor_fill_id": (str(anchor_row["fill_id"]) if anchor_row["fill_id"] is not None else None),
        "anchor_fill_ts": int(anchor_row["fill_ts"]),
        "internal_lot_size": lot_size,
        "portfolio_qty": normalized_portfolio_qty,
        "broker_qty": normalize_asset_qty(broker_qty),
        "executable_lot_count": executable_lot_count,
        "executable_qty": executable_qty,
        "dust_qty": dust_qty,
    }


def _apply_full_projection_rebuild(conn: sqlite3.Connection, *, note: str | None = None) -> dict[str, Any]:
    preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    convergence_before = build_lot_projection_convergence(conn, pair=settings.PAIR)
    if not bool(preview["safe_to_apply"]):
        if bool(
            bool(convergence_before.get("converged"))
            and (
                str(preview.get("repair_mode") or "") != "full_projection_rebuild"
                or "full_projection_rebuild_not_required" in str(preview.get("eligibility_reason") or "")
            )
        ):
            return {
                "preview": preview,
                "noop": True,
                "lot_snapshot_before": summarize_position_lots(conn, pair=settings.PAIR).as_dict(),
                "lot_snapshot_after": summarize_position_lots(conn, pair=settings.PAIR).as_dict(),
                "post_repair_projection_convergence": convergence_before,
            }
        raise RuntimeError(f"position authority rebuild is not safe to apply: {preview['eligibility_reason']}")

    before = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    before_lot_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM open_position_lots
            WHERE pair=?
            ORDER BY entry_ts ASC, id ASC
            """,
            (settings.PAIR,),
        ).fetchall()
    ]
    before_lifecycles = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM trade_lifecycles
            WHERE pair=?
            ORDER BY id ASC
            """,
            (settings.PAIR,),
        ).fetchall()
    ]
    repair_basis: dict[str, Any] = {
        "event_type": "full_projection_materialized_rebuild",
        "preview": preview,
        "lot_snapshot_before": before,
        "projection_convergence_before": convergence_before,
        "runtime_readiness_before": compute_runtime_readiness_snapshot(conn).as_dict(),
        "broker_portfolio_evidence": {
            "portfolio_qty": float(preview.get("portfolio_qty") or 0.0),
            "broker_qty": float(preview.get("broker_qty") or 0.0),
            "broker_qty_known": bool(preview.get("broker_qty_known")),
            "broker_portfolio_converged": bool(preview.get("broker_portfolio_converged")),
        },
        "gate_report": dict(preview.get("full_projection_rebuild_gate_report") or {}),
        "position_authority_history_summary": _load_position_authority_history_summary(conn),
        "old_lot_rows": before_lot_rows,
        "old_trade_lifecycle_rows": before_lifecycles,
    }
    savepoint = "position_authority_full_projection_rebuild"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        projection_replay = rebuild_lifecycle_projections_from_trades(
            conn,
            pair=settings.PAIR,
            allow_entry_decision_fallback=False,
        )
        repair_basis["projection_replay"] = projection_replay.as_dict()
        anchor_summary = _replace_with_portfolio_anchored_projection(
            conn,
            portfolio_qty=float(preview.get("portfolio_qty") or 0.0),
            broker_qty=float(preview.get("broker_qty") or 0.0),
        )
        repair_basis["portfolio_anchor_projection"] = anchor_summary
        after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
        repair_basis["lot_snapshot_after"] = after
        convergence = _assert_post_repair_projection_converged(
            conn,
            repair_basis=repair_basis,
            repair_mode="full_projection_rebuild",
        )
        event_ts = int(time.time() * 1000)
        publication = record_position_authority_projection_publication(
            conn,
            event_ts=event_ts,
            pair=settings.PAIR,
            target_trade_id=int(anchor_summary["anchor_trade_id"]),
            source="manual_full_projection_rebuild_publish",
            publish_basis=repair_basis,
            note=note,
        )
        repair_basis["projection_publication"] = publication
        post_assessment = build_position_authority_assessment(conn, pair=settings.PAIR)
        repair_basis["position_authority_assessment_after"] = post_assessment
        if (
            bool(post_assessment.get("needs_full_projection_rebuild"))
            or bool(post_assessment.get("needs_correction"))
            or bool(post_assessment.get("needs_portfolio_projection_repair"))
            or bool(post_assessment.get("needs_residual_normalization"))
        ):
            raise RuntimeError(
                "position authority full projection rebuild postcondition failed: "
                f"reason={post_assessment.get('reason') or 'unknown'}; "
                f"provenance={post_assessment.get('target_lot_provenance_kind') or 'unknown'}; "
                f"blockers={'|'.join(str(item) for item in post_assessment.get('blockers') or []) or 'none'}"
            )
        repair = record_position_authority_repair(
            conn,
            event_ts=event_ts,
            source="manual_full_projection_rebuild",
            reason=FULL_PROJECTION_REBUILD_REASON,
            repair_basis=repair_basis,
            note=note,
        )
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise
    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    return {
        "preview": preview,
        "repair": repair,
        "projection_publication": publication,
        "lot_snapshot_before": before,
        "lot_snapshot_after": after,
        "post_repair_projection_convergence": convergence,
    }


def apply_position_authority_rebuild(
    conn,
    *,
    note: str | None = None,
    full_projection_rebuild: bool = False,
    flat_stale_projection_repair: bool = False,
) -> dict[str, Any]:
    if flat_stale_projection_repair:
        return apply_flat_stale_lot_projection_repair(conn, note=note)
    preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=full_projection_rebuild)
    if full_projection_rebuild:
        return _apply_full_projection_rebuild(conn, note=note)
    if str(preview.get("repair_mode") or "") == "full_projection_rebuild":
        raise RuntimeError(
            "position authority rebuild requires explicit full projection rebuild mode: "
            "re-run with --full-projection-rebuild"
        )
    if not bool(preview["safe_to_apply"]):
        raise RuntimeError(f"position authority rebuild is not safe to apply: {preview['eligibility_reason']}")

    before = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    if str(preview.get("repair_mode") or "rebuild") in {
        "correction",
        "residual_normalization",
        "portfolio_projection_repair",
        "rebuild",
    }:
        simulated = _simulate_non_full_position_authority_repair(conn, preview=preview, note=note)
        gate_report = _build_postcondition_gate_report(
            conn,
            repair_mode=str(preview.get("repair_mode") or "rebuild"),
            require_broker_portfolio_convergence=False,
            broker_qty=float(preview.get("broker_qty") or 0.0),
            broker_qty_known=bool(preview.get("broker_qty_known")),
            remote_open_order_count=int(preview.get("remote_open_order_count") or 0),
        )
        repair_basis = dict(simulated.get("repair_basis") or {})
        repair_basis["post_repair_projection_convergence"] = gate_report["post_projection_convergence"]
        repair_basis["position_authority_assessment_after"] = gate_report["post_assessment"]
        repair_basis["final_gate_failures"] = list(gate_report["final_gate_failures"])
        if list(gate_report["final_gate_failures"]):
            raise RuntimeError(
                "position authority repair postcondition failed: "
                f"repair_mode={preview.get('repair_mode') or 'rebuild'}; "
                f"blockers={'|'.join(str(item) for item in gate_report['final_gate_failures']) or 'none'}"
            )
        return {
            "preview": preview,
            **simulated,
            "lot_snapshot_before": before,
            "post_repair_projection_convergence": gate_report["post_projection_convergence"],
            "post_repair_assessment": gate_report["post_assessment"],
            "final_gate_failures": gate_report["final_gate_failures"],
        }

    if str(preview.get("repair_mode") or "rebuild") in {
        "correction",
        "residual_normalization",
        "portfolio_projection_repair",
    }:
        repair_mode = str(preview.get("repair_mode") or "correction")
        assessment = dict(preview.get("position_authority_assessment") or {})
        target_trade_id = int(assessment.get("target_trade_id") or 0)
        row = conn.execute(
            """
            SELECT
                t.id AS trade_id,
                t.client_order_id,
                t.ts AS fill_ts,
                t.price,
                t.qty,
                t.fee,
                t.strategy_name,
                t.entry_decision_id,
                f.fill_id
            FROM trades t
            LEFT JOIN fills f
              ON f.client_order_id=t.client_order_id
             AND f.fill_ts=t.ts
             AND ABS(f.price-t.price) < 1e-12
             AND ABS(f.qty-t.qty) < 1e-12
            WHERE t.id=? AND t.pair=? AND t.side='BUY'
            """,
            (target_trade_id, settings.PAIR),
        ).fetchone()
        if row is None:
            raise RuntimeError("position authority correction target BUY evidence disappeared")
        before_rows = [
            dict(item)
            for item in conn.execute(
                """
                SELECT id, pair, entry_trade_id, entry_client_order_id, entry_fill_id, qty_open,
                       executable_lot_count, dust_tracking_lot_count, internal_lot_size,
                       position_state, position_semantic_basis
                FROM open_position_lots
                WHERE pair=? AND entry_trade_id=?
                ORDER BY id ASC
                """,
                (settings.PAIR, target_trade_id),
            ).fetchall()
        ]
        sell_rows = []
        sell_trade_ids = [int(value) for value in assessment.get("sell_trade_ids") or []]
        if repair_mode == "residual_normalization":
            if not sell_trade_ids:
                raise RuntimeError("partial-close residual normalization target SELL evidence disappeared")
            placeholders = ",".join("?" for _ in sell_trade_ids)
            sell_rows = [
                dict(item)
                for item in conn.execute(
                    f"""
                    SELECT
                        t.id AS trade_id,
                        t.client_order_id,
                        t.ts AS fill_ts,
                        t.price,
                        t.qty,
                        t.fee,
                        t.strategy_name,
                        t.entry_decision_id,
                        t.exit_decision_id,
                        t.exit_reason,
                        t.exit_rule_name,
                        f.fill_id
                    FROM trades t
                    LEFT JOIN fills f
                      ON f.client_order_id=t.client_order_id
                     AND f.fill_ts=t.ts
                     AND ABS(f.price-t.price) < 1e-12
                     AND ABS(f.qty-t.qty) < 1e-12
                    WHERE t.id IN ({placeholders}) AND t.pair=? AND t.side='SELL'
                    ORDER BY t.ts ASC, t.id ASC
                    """,
                    (*sell_trade_ids, settings.PAIR),
                ).fetchall()
            ]
            if [int(item["trade_id"]) for item in sell_rows] != sell_trade_ids:
                raise RuntimeError("partial-close residual normalization target SELL evidence changed")
        before_lifecycles = [
            dict(item)
            for item in conn.execute(
                """
                SELECT *
                FROM trade_lifecycles
                WHERE pair=?
                  AND (
                        entry_trade_id=?
                        OR exit_trade_id IN (
                            SELECT id FROM trades
                            WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))
                        )
                      )
                ORDER BY id ASC
                """,
                (settings.PAIR, target_trade_id, settings.PAIR, int(row["fill_ts"]), int(row["fill_ts"]), target_trade_id),
            ).fetchall()
        ]
        if repair_mode == "portfolio_projection_repair":
            repair_basis = {
                "event_type": "portfolio_anchored_authority_projection_repair",
                "preview": preview,
                "target_trade_id": target_trade_id,
                "target_client_order_id": assessment.get("target_client_order_id"),
                "target_fill_id": assessment.get("target_fill_id"),
                "target_fill_ts": assessment.get("target_fill_ts"),
                "target_price": assessment.get("target_price"),
                "target_qty": assessment.get("target_qty"),
                "portfolio_qty": preview.get("portfolio_qty"),
                "broker_qty": preview.get("broker_qty"),
                "other_active_qty": assessment.get("other_active_qty"),
                "projected_total_qty": assessment.get("projected_total_qty"),
                "projected_qty_excess": assessment.get("projected_qty_excess"),
                "target_remainder_qty": assessment.get("portfolio_target_remainder_qty"),
                "canonical_internal_lot_size": assessment.get("canonical_internal_lot_size"),
                "canonical_executable_lot_count": assessment.get("canonical_executable_lot_count"),
                "canonical_executable_qty": assessment.get("canonical_executable_qty"),
                "old_lot_rows": before_rows,
                "old_trade_lifecycle_rows": before_lifecycles,
                "lot_snapshot_before": before,
            }
            apply_portfolio_anchored_projection_repair_basis(
                conn,
                pair=settings.PAIR,
                repair_basis=repair_basis,
            )
            after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
            repair_basis["lot_snapshot_after"] = after
            convergence = _assert_post_repair_projection_converged(
                conn,
                repair_basis=repair_basis,
                repair_mode=repair_mode,
            )
            portfolio_row = conn.execute(
                """
                SELECT cash_available, cash_locked, asset_available, asset_locked
                FROM portfolio
                WHERE id=1
                """
            ).fetchone()
            replay = compute_accounting_replay(conn)
            portfolio_cash = 0.0
            portfolio_qty = 0.0
            if portfolio_row is not None:
                portfolio_cash = float(portfolio_row["cash_available"] or 0.0) + float(portfolio_row["cash_locked"] or 0.0)
                portfolio_qty = float(portfolio_row["asset_available"] or 0.0) + float(portfolio_row["asset_locked"] or 0.0)
            accounting_preview = {
                "replay_cash": float(replay.get("replay_cash") or 0.0),
                "replay_qty": float(replay.get("replay_qty") or 0.0),
                "portfolio_cash": float(portfolio_cash),
                "portfolio_qty": float(portfolio_qty),
                "cash_delta": float(portfolio_cash) - float(replay.get("replay_cash") or 0.0),
                "asset_qty_delta": float(portfolio_qty) - float(replay.get("replay_qty") or 0.0),
                "safe_to_apply": True,
                "needs_repair": bool(
                    abs(float(portfolio_cash) - float(replay.get("replay_cash") or 0.0)) > 1e-8
                    or abs(float(portfolio_qty) - float(replay.get("replay_qty") or 0.0)) > 1e-12
                ),
            }
            repair_basis["external_position_accounting_preview"] = accounting_preview
            adjustment = None
            if bool(accounting_preview.get("needs_repair")):
                adjustment_basis = {
                    "event_type": "external_position_adjustment",
                    "source_event_type": "portfolio_anchored_authority_projection_repair",
                    "target_trade_id": target_trade_id,
                    "target_client_order_id": assessment.get("target_client_order_id"),
                    "target_fill_id": assessment.get("target_fill_id"),
                    "position_authority_preview": preview,
                    "accounting_preview": accounting_preview,
                }
                adjustment = record_external_position_adjustment(
                    conn,
                    event_ts=int(time.time() * 1000),
                    asset_qty_delta=float(accounting_preview.get("asset_qty_delta") or 0.0),
                    cash_delta=float(accounting_preview.get("cash_delta") or 0.0),
                    source="manual_portfolio_anchored_authority_projection_repair",
                    reason="portfolio_projection_external_position_adjustment",
                    adjustment_basis=adjustment_basis,
                    note=note,
                )
            repair_basis["external_position_adjustment"] = adjustment
            publication = record_position_authority_projection_publication(
                conn,
                event_ts=int(time.time() * 1000),
                pair=settings.PAIR,
                target_trade_id=target_trade_id,
                source="manual_portfolio_anchored_authority_projection_publish",
                publish_basis=repair_basis,
                note=note,
            )
            repair_basis["projection_publication"] = publication
            repair = record_position_authority_repair(
                conn,
                event_ts=int(time.time() * 1000),
                source="manual_portfolio_anchored_authority_projection_repair",
                reason=PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON,
                repair_basis=repair_basis,
                note=note,
            )
            return {
                "preview": preview,
                "repair": repair,
                "projection_publication": publication,
                "external_position_adjustment": adjustment,
                "lot_snapshot_before": before,
                "lot_snapshot_after": after,
                "post_repair_projection_convergence": convergence,
            }
        conn.execute(
            "DELETE FROM open_position_lots WHERE pair=? AND entry_trade_id=?",
            (settings.PAIR, target_trade_id),
        )
        conn.execute(
            """
            DELETE FROM trade_lifecycles
            WHERE pair=?
              AND (
                    entry_trade_id=?
                    OR exit_trade_id IN (
                        SELECT id FROM trades
                        WHERE pair=? AND side='SELL' AND (ts > ? OR (ts=? AND id>?))
                    )
                  )
            """,
            (settings.PAIR, target_trade_id, settings.PAIR, int(row["fill_ts"]), int(row["fill_ts"]), target_trade_id),
        )
        apply_fill_lifecycle(
            conn,
            side="BUY",
            pair=settings.PAIR,
            trade_id=int(row["trade_id"]),
            client_order_id=str(row["client_order_id"]),
            fill_id=(str(row["fill_id"]) if row["fill_id"] is not None else None),
            fill_ts=int(row["fill_ts"]),
            price=float(row["price"]),
            qty=float(row["qty"]),
            fee=float(row["fee"] or 0.0),
            strategy_name=(str(row["strategy_name"]) if row["strategy_name"] is not None else None),
            entry_decision_id=(int(row["entry_decision_id"]) if row["entry_decision_id"] is not None else None),
            allow_entry_decision_fallback=False,
        )
        for sell in sell_rows:
            apply_fill_lifecycle(
                conn,
                side="SELL",
                pair=settings.PAIR,
                trade_id=int(sell["trade_id"]),
                client_order_id=str(sell["client_order_id"]),
                fill_id=(str(sell["fill_id"]) if sell["fill_id"] is not None else None),
                fill_ts=int(sell["fill_ts"]),
                price=float(sell["price"]),
                qty=float(sell["qty"]),
                fee=float(sell["fee"] or 0.0),
                strategy_name=(str(sell["strategy_name"]) if sell["strategy_name"] is not None else None),
                entry_decision_id=(int(sell["entry_decision_id"]) if sell["entry_decision_id"] is not None else None),
                exit_decision_id=(int(sell["exit_decision_id"]) if sell["exit_decision_id"] is not None else None),
                exit_reason=(str(sell["exit_reason"]) if sell["exit_reason"] is not None else None),
                exit_rule_name=(str(sell["exit_rule_name"]) if sell["exit_rule_name"] is not None else None),
                allow_entry_decision_fallback=False,
            )
        after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
        repair_basis = {
            "event_type": (
                "partial_close_residual_authority_normalization"
                if repair_mode == "residual_normalization"
                else "position_authority_correction"
            ),
            "preview": preview,
            "target_trade_id": target_trade_id,
            "target_client_order_id": assessment.get("target_client_order_id"),
            "sell_trade_ids": sell_trade_ids,
            "expected_residual_qty": assessment.get("expected_residual_qty"),
            "sell_after_target_buy_qty": assessment.get("sell_after_target_buy_qty"),
            "target_lifecycle_matched_qty": assessment.get("target_lifecycle_matched_qty"),
            "effective_closed_qty": assessment.get("effective_closed_qty"),
            "lifecycle_matched_qty_acceptance_reason": assessment.get("lifecycle_matched_qty_acceptance_reason"),
            "canonical_executable_qty": assessment.get("canonical_executable_qty"),
            "old_lot_rows": before_rows,
            "old_trade_lifecycle_rows": before_lifecycles,
            "lot_snapshot_before": before,
            "lot_snapshot_after": after,
        }
        convergence = _assert_post_repair_projection_converged(
            conn,
            repair_basis=repair_basis,
            repair_mode=repair_mode,
        )
        repair = record_position_authority_repair(
            conn,
            event_ts=int(time.time() * 1000),
            source=(
                "manual_partial_close_residual_authority_normalization"
                if repair_mode == "residual_normalization"
                else "manual_position_authority_correction"
            ),
            reason=(
                PARTIAL_CLOSE_RESIDUAL_REPAIR_REASON
                if repair_mode == "residual_normalization"
                else "accounted_buy_fill_authority_correction"
            ),
            repair_basis=repair_basis,
            note=note,
        )
        return {
            "preview": preview,
            "repair": repair,
            "lot_snapshot_before": before,
            "lot_snapshot_after": after,
            "post_repair_projection_convergence": convergence,
        }

    projection_replay = rebuild_lifecycle_projections_from_trades(
        conn,
        pair=settings.PAIR,
        allow_entry_decision_fallback=False,
    )
    after = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    repair_basis = {
        "event_type": "position_authority_rebuild",
        "preview": preview,
        "lot_snapshot_before": before,
        "lot_snapshot_after": after,
        "projection_replay": projection_replay.as_dict(),
    }
    convergence = _assert_post_repair_projection_converged(
        conn,
        repair_basis=repair_basis,
        repair_mode="rebuild",
    )
    repair = record_position_authority_repair(
        conn,
        event_ts=int(time.time() * 1000),
        source="manual_position_authority_rebuild",
        reason="accounted_buy_fill_authority_rebuild",
        repair_basis=repair_basis,
        note=note,
    )
    return {
        "preview": preview,
        "repair": repair,
        "lot_snapshot_before": before,
        "lot_snapshot_after": after,
        "post_repair_projection_convergence": convergence,
    }
