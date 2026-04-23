from __future__ import annotations

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
    apply_fill_lifecycle,
    apply_portfolio_anchored_projection_repair_basis,
    rebuild_lifecycle_projections_from_trades,
    summarize_position_lots,
)
from .position_authority_incidents import PORTFOLIO_ANCHORED_PROJECTION_REPAIR_REASON
from .position_authority_state import (
    PARTIAL_CLOSE_RESIDUAL_REPAIR_REASON,
    build_lot_projection_convergence,
    build_position_authority_assessment,
)
from .runtime_readiness import compute_runtime_readiness_snapshot


def build_position_authority_rebuild_preview(conn) -> dict[str, Any]:
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

    if bool(authority_assessment.get("needs_portfolio_projection_repair")):
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
    elif repair_mode == "rebuild" and authority_action_state == "not_applicable":
        effective_action_state = "blocked_pending_evidence"

    safe_to_apply = bool(not reasons and effective_action_state == "safe_to_apply_now")
    return {
        "needs_rebuild": snapshot.recovery_stage in {
            "AUTHORITY_REBUILD_PENDING",
            "AUTHORITY_CORRECTION_PENDING",
            "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING",
            "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING",
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
                "position authority correction applicable"
                if safe_to_apply and repair_mode == "correction"
                else ("position authority rebuild applicable" if safe_to_apply else ", ".join(dict.fromkeys(reasons)))
                )
            )
        ),
        "recovery_stage": snapshot.recovery_stage,
        "repair_mode": repair_mode,
        "next_required_action": "apply_rebuild_position_authority" if safe_to_apply else snapshot.operator_next_action,
        "recommended_command": (
            "uv run python bot.py rebuild-position-authority --apply --yes"
            if safe_to_apply
            else snapshot.recommended_command
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
        "broker_qty": broker_qty,
        "broker_qty_known": broker_qty_known,
        "remote_open_order_count": remote_open_order_count,
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


def apply_position_authority_rebuild(conn, *, note: str | None = None) -> dict[str, Any]:
    preview = build_position_authority_rebuild_preview(conn)
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
