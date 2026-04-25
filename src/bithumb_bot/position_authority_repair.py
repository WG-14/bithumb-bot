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
from .runtime_readiness import compute_runtime_readiness_snapshot


_EPS = 1e-12
FULL_PROJECTION_REBUILD_REASON = "full_projection_materialized_rebuild"


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
    broker_qty = float(snapshot.position_state.normalized_exposure.raw_holdings.broker_qty)
    broker_qty_known = bool(int(snapshot.reconcile_metadata.get("balance_observed_ts_ms", 0) or 0) > 0)
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
    if not broker_qty_known:
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


def build_position_authority_rebuild_preview(conn, *, full_projection_rebuild: bool = False) -> dict[str, Any]:
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
    projection_convergence = dict(authority_assessment.get("projection_convergence") or {})

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
    broker_qty = float(snapshot.position_state.normalized_exposure.raw_holdings.broker_qty)
    broker_qty_known = bool(int(snapshot.reconcile_metadata.get("balance_observed_ts_ms", 0) or 0) > 0)
    remote_open_order_count = int(snapshot.reconcile_metadata.get("remote_open_order_found", 0) or 0)
    if repair_mode == "full_projection_rebuild":
        full_projection_gate_report = _build_full_projection_rebuild_gate_report(
            conn,
            snapshot=snapshot,
            authority_assessment=authority_assessment,
            portfolio_qty=portfolio_qty,
        )
        reasons = list(full_projection_gate_report["reasons"])
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
        effective_action_state = "blocked_pending_evidence"
    elif repair_mode == "rebuild" and authority_action_state == "not_applicable":
        effective_action_state = "blocked_pending_evidence"

    safe_to_apply = bool(not reasons and effective_action_state == "safe_to_apply_now")
    return {
        "needs_rebuild": snapshot.recovery_stage in {
            "AUTHORITY_REBUILD_PENDING",
            "AUTHORITY_CORRECTION_PENDING",
            "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING",
            "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING",
            "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING",
        },
        "safe_to_apply": safe_to_apply,
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
        "next_required_action": "apply_rebuild_position_authority" if safe_to_apply else snapshot.operator_next_action,
        "recommended_command": (
            (
                "uv run python bot.py rebuild-position-authority --full-projection-rebuild --apply --yes"
                if repair_mode == "full_projection_rebuild"
                else "uv run python bot.py rebuild-position-authority --apply --yes"
            )
            if safe_to_apply
            else (
                "uv run python bot.py rebuild-position-authority --full-projection-rebuild"
                if repair_mode == "full_projection_rebuild"
                else snapshot.recommended_command
            )
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
        "target_lot_provenance_kind": str(authority_assessment.get("target_lot_provenance_kind") or "unknown"),
        "target_lot_source_modes": list(authority_assessment.get("target_lot_source_modes") or []),
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
        "remote_open_order_count": remote_open_order_count,
        "broker_portfolio_converged": (
            None if full_projection_gate_report is None else bool(full_projection_gate_report["broker_portfolio_converged"])
        ),
        "accounting_projection_ok": (
            None if full_projection_gate_report is None else bool(full_projection_gate_report["accounting_projection_ok"])
        ),
        "unresolved_open_order_count": (
            None if full_projection_gate_report is None else int(full_projection_gate_report["unresolved_open_order_count"])
        ),
        "pending_submit_count": (
            None if full_projection_gate_report is None else int(full_projection_gate_report["pending_submit_count"])
        ),
        "submit_unknown_count": (
            None if full_projection_gate_report is None else int(full_projection_gate_report["submit_unknown_count"])
        ),
        "unresolved_fee_pending": (
            None if full_projection_gate_report is None else bool(full_projection_gate_report["unresolved_fee_pending"])
        ),
        "full_projection_rebuild_gate_report": full_projection_gate_report,
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
) -> dict[str, Any]:
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
