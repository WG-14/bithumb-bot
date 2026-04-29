from __future__ import annotations

import hashlib
import json
from typing import Any


def _truthy(value: object) -> bool:
    return bool(value)


def _float(value: object, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return default


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def build_recovery_policy_from_report(report: dict[str, Any]) -> dict[str, Any]:
    runtime_readiness = dict(report.get("runtime_readiness") or {})
    normalized_exposure = dict(runtime_readiness.get("normalized_exposure") or {})
    residual_inventory = dict(runtime_readiness.get("residual_inventory") or {})
    fill_projection = dict(report.get("fill_accounting_incident_projection") or {})
    fill_root = dict(report.get("fill_accounting_root_cause") or {})
    fee_gap_preview = dict(report.get("fee_gap_accounting_repair_preview") or {})
    manual_flat_preview = dict(report.get("manual_flat_accounting_repair_preview") or {})
    external_position_preview = dict(report.get("external_position_accounting_repair_preview") or {})
    position_preview = dict(report.get("position_authority_rebuild_preview") or {})
    fee_rate_drift = dict(report.get("fee_rate_drift_diagnostics") or {})

    active_fill_issue_count = _int(fill_projection.get("active_issue_count"))
    projection_drift = (
        not _truthy(report.get("lot_projection_converged"))
        or _truthy(position_preview.get("needs_rebuild"))
    )
    fee_gap_active_issue = _truthy(fee_gap_preview.get("active_issue")) or (
        _truthy(fee_gap_preview.get("needs_repair"))
        and _truthy(fee_gap_preview.get("resume_blocking"))
    )
    residual_inventory_tracked = str(runtime_readiness.get("residual_class") or "") == "RESIDUAL_INVENTORY_TRACKED"
    residual_only_holdings = bool(
        "NON_EXECUTABLE_RESIDUAL_HOLDINGS" in (runtime_readiness.get("resume_blockers") or [])
        or str(runtime_readiness.get("residual_class") or "") == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    )
    manual_flat_active_issue = bool(
        _truthy(manual_flat_preview.get("needs_repair")) and not residual_only_holdings
    )
    external_position_active_issue = bool(
        _truthy(external_position_preview.get("needs_repair")) and not residual_only_holdings
    )
    accounting_root_cause_unresolved = any(
        (
            active_fill_issue_count > 0,
            fee_gap_active_issue,
            manual_flat_active_issue,
            external_position_active_issue,
            projection_drift,
        )
    )
    accounting_evidence_reliable = bool(
        not accounting_root_cause_unresolved
        and _truthy(report.get("accounting_projection_ok"))
        and _truthy(report.get("broker_portfolio_converged"))
        and _truthy(report.get("lot_projection_converged"))
    )
    actual_executable_exposure = bool(normalized_exposure.get("has_executable_exposure"))
    run_loop_allowed = bool(runtime_readiness.get("run_loop_allowed"))
    position_management_allowed = bool(runtime_readiness.get("position_management_allowed"))
    new_entry_allowed = bool(runtime_readiness.get("new_entry_allowed"))
    closeout_allowed = bool(runtime_readiness.get("closeout_allowed"))
    recommended_mode = "recovery"
    primary_incident_class = "RECOVERY_READINESS"
    additional_orders_allowed = False
    flatten_primary_recommendation = False
    recommended_action = str(report.get("operator_next_action") or "investigate_blockers")
    recommended_command = str(report.get("recommended_command") or "uv run python bot.py recovery-report")

    if residual_inventory_tracked:
        recommended_mode = "residual_inventory_tracked"
        primary_incident_class = "RESIDUAL_INVENTORY"
        additional_orders_allowed = bool(new_entry_allowed)
        recommended_action = "run_with_residual_inventory_tracking"
        recommended_command = str(runtime_readiness.get("recommended_command") or "uv run python bot.py resume")
    elif accounting_root_cause_unresolved:
        recommended_mode = "forensic_accounting"
        primary_incident_class = "ACCOUNTING_ROOT_CAUSE"
        recommended_action = "collect_broker_fill_evidence_and_build_repair_plan"
        recommended_command = "uv run python bot.py repair-plan"
    elif residual_only_holdings:
        recommended_mode = "residual_policy_review"
        primary_incident_class = "TRADEABILITY_POLICY"
        recommended_action = "residual_policy_review"
        recommended_command = str(
            runtime_readiness.get("recommended_command") or "uv run bithumb-bot residual-closeout-plan"
        )
    elif actual_executable_exposure and accounting_evidence_reliable and run_loop_allowed and position_management_allowed:
        recommended_mode = "position_management"
        primary_incident_class = "CANONICAL_OPEN_POSITION"
        additional_orders_allowed = bool(new_entry_allowed)
        recommended_action = "resume_position_management"
        recommended_command = "uv run python bot.py resume"

    incident_reasons: list[str] = []
    if active_fill_issue_count > 0:
        incident_reasons.append("fill_accounting_incident_active")
    if fee_gap_active_issue:
        incident_reasons.append("fee_gap_recovery_active")
    if manual_flat_active_issue:
        incident_reasons.append("manual_flat_accounting_repair_required")
    if external_position_active_issue:
        incident_reasons.append("external_position_accounting_repair_required")
    if projection_drift:
        incident_reasons.append("open_position_lots_projection_drift")
    if residual_only_holdings:
        incident_reasons.append("non_executable_residual_holdings")
    if residual_inventory_tracked:
        incident_reasons.append("residual_inventory_tracked")
    if _int(fee_rate_drift.get("recent_expected_fee_rate_mismatch_count")) > 0:
        incident_reasons.append("fee_rate_drift_visible")

    return {
        "primary_incident_class": primary_incident_class,
        "recommended_mode": recommended_mode,
        "accounting_root_cause_unresolved": bool(accounting_root_cause_unresolved),
        "accounting_evidence_reliable": bool(accounting_evidence_reliable),
        "actual_executable_exposure": bool(actual_executable_exposure),
        "position_management_allowed": bool(position_management_allowed),
        "additional_orders_allowed": bool(additional_orders_allowed),
        "flatten_primary_recommendation": bool(flatten_primary_recommendation),
        "flatten_not_primary": bool(not flatten_primary_recommendation),
        "recommended_action": recommended_action,
        "recommended_command": recommended_command,
        "residual_qty": _float(residual_inventory.get("residual_qty")),
        "residual_classes": list(residual_inventory.get("residual_classes") or []),
        "fill_root_cause": str(fill_root.get("root") or "none"),
        "incident_reasons": incident_reasons,
    }


def _backup_guidance(mode: str) -> str:
    return f"backup/{mode}/db/ via scripts/backup_sqlite.sh before apply"


def _fee_pending_accounting_repair_candidate(
    *,
    report: dict[str, Any],
    fill_projection: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    diagnostics = dict(
        (report.get("broker_fill_observation_summary") or {}).get("fee_evidence_diagnostics") or {}
    )
    needed = bool(
        _int(fill_projection.get("active_fee_pending_count")) > 0
        or _int(fill_projection.get("fee_validation_blocked_count")) > 0
        or _int(fill_projection.get("unapplied_principal_pending_count")) > 0
    )
    active_issue = bool(_int(fill_projection.get("active_issue_count")) > 0)
    safe_to_apply = bool(diagnostics.get("safe_to_repair"))
    why_not_safe = list(diagnostics.get("why_not_safe") or [])
    preconditions = (
        "deterministic order-level paid_fee allocation evidence passed repair invariants"
        if safe_to_apply
        else (
            "; ".join(str(item) for item in why_not_safe)
            or "identify the exact pending fill and provide client_order_id, fill_id, fee, and fee_provenance"
        )
    )
    return {
        "name": "fee-pending-accounting-repair",
        "needed": needed,
        "active_issue": active_issue,
        "safe_to_apply": safe_to_apply,
        "final_safe_to_apply": safe_to_apply,
        "primary_blocker": diagnostics.get("primary_blocker"),
        "client_order_id": diagnostics.get("affected_client_order_id"),
        "exchange_order_id": diagnostics.get("affected_exchange_order_id"),
        "fill_id": diagnostics.get("affected_fill_id"),
        "fill_ids": list(diagnostics.get("affected_fill_ids") or []),
        "order_paid_fee": diagnostics.get("order_paid_fee"),
        "sum_observed_or_applied_fill_fee": diagnostics.get("sum_observed_or_applied_fill_fee"),
        "fee_delta": diagnostics.get("fee_delta"),
        "fee": diagnostics.get("proposed_repair_fee"),
        "fee_provenance": diagnostics.get("fee_provenance"),
        "complete_fill_set_available": bool(diagnostics.get("complete_fill_set_available")),
        "deterministic_allocation_available": bool(diagnostics.get("deterministic_allocation_available")),
        "operator_confirmation_required": bool(diagnostics.get("operator_confirmation_required")),
        "preconditions": preconditions,
        "touched_tables": [
            "broker_fill_observations",
            "fills",
            "trades",
            "fee_pending_accounting_repairs",
            "portfolio",
        ],
        "expected_after": (
            diagnostics.get("expected_after")
            or "fee finalization recorded through normal accounting path; fee_pending incident count decreases"
        ),
        "idempotency_key": (
            diagnostics.get("idempotency_key")
            or "fee_pending_accounting_repair:<client_order_id>:<fill_id>:<fee>"
        ),
        "rollback_or_backup": diagnostics.get("rollback_or_backup") or _backup_guidance(mode),
        "why_safe": "; ".join(str(item) for item in (diagnostics.get("why_safe") or []))
        if safe_to_apply
        else "blocked until deterministic fee repair invariants pass",
        "why_not_safe": why_not_safe,
        "recommended_command": (
            diagnostics.get("recommended_command")
            or "uv run python bot.py fee-pending-accounting-repair --client-order-id <id> --fill-id <fill_id> --fee <fee> --fee-provenance <source>"
        ),
        "post_apply_verification_commands": list(diagnostics.get("post_apply_verification_commands") or []),
    }


def _fee_pending_cash_effect_adjustment_candidate(
    *,
    report: dict[str, Any],
    fill_projection: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    accounting_projection = dict(report.get("accounting_projection") or {})
    fee_gap_preview = dict(report.get("fee_gap_accounting_repair_preview") or {})
    repair_summary = dict(report.get("fee_pending_accounting_repair_summary") or {})
    runtime_readiness = dict(report.get("runtime_readiness") or {})
    broker_cash_available = _float(report.get("cash_available"))
    broker_cash_locked = _float(report.get("cash_locked"))
    local_cash_available_source = (
        accounting_projection.get("portfolio_cash_available")
        if accounting_projection.get("portfolio_cash_available") is not None
        else fee_gap_preview.get("cash_available")
        if fee_gap_preview.get("cash_available") is not None
        else fee_gap_preview.get("portfolio_cash")
    )
    local_cash_locked_source = (
        accounting_projection.get("portfolio_cash_locked")
        if accounting_projection.get("portfolio_cash_locked") is not None
        else fee_gap_preview.get("cash_locked")
    )
    local_cash_available = _float(local_cash_available_source)
    local_cash_locked = _float(local_cash_locked_source)
    broker_cash_total = broker_cash_available + broker_cash_locked
    local_cash_total_source = (
        accounting_projection.get("portfolio_cash")
        if accounting_projection.get("portfolio_cash") is not None
        else fee_gap_preview.get("portfolio_cash")
    )
    local_cash_total = (
        _float(local_cash_total_source)
        if local_cash_total_source is not None
        else local_cash_available + local_cash_locked
    )
    cash_delta = broker_cash_total - local_cash_total
    repaired_fee = _float(repair_summary.get("last_fee"))
    active_issue = bool(_int(fill_projection.get("active_issue_count")) > 0)
    unresolved_fee_state = bool((accounting_projection.get("unresolved_fee_state")))
    repaired_incident_count = _int(
        accounting_projection.get(
            "fill_accounting_repaired_incident_count",
            fill_projection.get("repaired_count"),
        )
    )
    repair_count = _int(repair_summary.get("repair_count"))
    broker_local_cash_only_mismatch = bool(
        abs(cash_delta) > 1e-8
        and abs(broker_cash_locked - local_cash_locked) <= 1e-8
    )
    delta_matches_repaired_fee = bool(repaired_fee > 0.0 and abs(abs(cash_delta) - repaired_fee) <= 1e-8)
    cash_effect_direction_ok = bool(cash_delta < 0.0)
    needed = bool(
        repair_count > 0
        and repaired_incident_count > 0
        and not active_issue
        and not unresolved_fee_state
        and broker_local_cash_only_mismatch
        and delta_matches_repaired_fee
        and cash_effect_direction_ok
    )
    blockers: list[str] = []
    if repair_count <= 0:
        blockers.append("fee_pending_accounting_repair_count=0")
    if repaired_incident_count <= 0:
        blockers.append("repaired_fee_pending_incident_count=0")
    if active_issue:
        blockers.append("active_fee_pending_issue_present")
    if unresolved_fee_state:
        blockers.append("unresolved_fee_state=1")
    if not broker_local_cash_only_mismatch:
        blockers.append("broker_local_cash_only_mismatch_not_detected")
    if not delta_matches_repaired_fee:
        blockers.append(
            f"cash_delta_does_not_match_repaired_fee:delta={cash_delta:.8f},fee={repaired_fee:.8f}"
        )
    if not cash_effect_direction_ok:
        blockers.append(f"cash_delta_not_fee_decrease:delta={cash_delta:.8f}")

    last_event_ts = _int(repair_summary.get("last_event_ts"))
    last_repair_key = str(repair_summary.get("last_repair_key") or "unknown")
    adjustment_key = (
        "fee_pending_cash_effect:"
        f"{last_repair_key}:"
        f"{cash_delta:.8f}:"
        f"{broker_cash_total:.8f}:"
        f"{local_cash_total:.8f}"
    )
    broker_snapshot_basis = {
        "source": "fee_pending_cash_effect_repair_plan",
        "fee_pending_accounting_repair_key": last_repair_key,
        "fee_pending_accounting_repair_event_ts": last_event_ts,
        "broker_cash_available": broker_cash_available,
        "broker_cash_locked": broker_cash_locked,
        "broker_cash_total": broker_cash_total,
        "local_cash_available": local_cash_available,
        "local_cash_locked": local_cash_locked,
        "local_cash_total": local_cash_total,
        "cash_delta": cash_delta,
        "repaired_fee": repaired_fee,
    }
    correlation_metadata = {
        "fee_pending_accounting_repair_count": repair_count,
        "fill_accounting_repaired_incident_count": repaired_incident_count,
        "fill_accounting_active_issue_count": _int(fill_projection.get("active_issue_count")),
        "resume_blockers": list(runtime_readiness.get("resume_blockers") or []),
    }
    basis_arg = json.dumps(broker_snapshot_basis, sort_keys=True, separators=(",", ":"))
    correlation_arg = json.dumps(correlation_metadata, sort_keys=True, separators=(",", ":"))
    recommended_command = (
        "uv run python bot.py record-external-cash-adjustment "
        f"--event-ts {last_event_ts or '<event_ts>'} "
        f"--delta-amount {cash_delta:.8f} "
        "--source fee_pending_accounting_repair "
        "--reason fee_pending_cash_effect_repair "
        f"--broker-snapshot-basis '{basis_arg}' "
        f"--correlation-metadata '{correlation_arg}' "
        f"--adjustment-key '{adjustment_key}' "
        "--yes"
    )
    return {
        "name": "fee-pending-cash-effect-adjustment",
        "needed": needed,
        "active_issue": False,
        "safe_to_apply": needed,
        "final_safe_to_apply": needed,
        "preconditions": (
            "fee-pending incident is resolved and the only remaining cash delta equals the repaired fee"
            if needed
            else "; ".join(blockers or ["fee-pending cash-effect adjustment not applicable"])
        ),
        "touched_tables": [
            "external_cash_adjustments",
            "portfolio",
            "fee_pending_accounting_repairs",
        ],
        "expected_after": "portfolio cash decreases by the repaired fee delta through an audited external cash adjustment event",
        "idempotency_key": adjustment_key,
        "rollback_or_backup": _backup_guidance(mode),
        "why_safe": (
            "bounded to a resolved fee-pending repair where broker/local cash delta exactly matches the repaired fee"
            if needed
            else "blocked until resolved repair evidence and exact broker/local fee-delta match are present"
        ),
        "why_not_safe": blockers,
        "cash_delta": cash_delta,
        "repaired_fee": repaired_fee,
        "broker_cash_total": broker_cash_total,
        "local_cash_total": local_cash_total,
        "recommended_command": recommended_command,
        "post_apply_verification_commands": [
            "uv run python bot.py reconcile",
            "uv run python bot.py health",
            "uv run python bot.py recovery-report",
        ],
    }


def _candidate_repairs_from_report(report: dict[str, Any], policy: dict[str, Any]) -> list[dict[str, Any]]:
    mode = str(report.get("mode") or "paper")
    fill_projection = dict(report.get("fill_accounting_incident_projection") or {})
    fee_gap_preview = dict(report.get("fee_gap_accounting_repair_preview") or {})
    manual_flat_preview = dict(report.get("manual_flat_accounting_repair_preview") or {})
    external_position_preview = dict(report.get("external_position_accounting_repair_preview") or {})
    position_preview = dict(report.get("position_authority_rebuild_preview") or {})
    flat_stale_preview = dict(position_preview.get("flat_stale_projection_repair_preview") or {})

    candidates = [
        _fee_pending_accounting_repair_candidate(report=report, fill_projection=fill_projection, mode=mode),
        _fee_pending_cash_effect_adjustment_candidate(report=report, fill_projection=fill_projection, mode=mode),
        {
            "name": "fee-gap-accounting-repair",
            "needed": bool(_truthy(fee_gap_preview.get("needs_repair")) or _truthy(fee_gap_preview.get("active_issue"))),
            "active_issue": bool(_truthy(fee_gap_preview.get("active_issue"))),
            "safe_to_apply": bool(fee_gap_preview.get("safe_to_apply")),
            "preconditions": str(fee_gap_preview.get("eligibility_reason") or "review fee-gap repair preview"),
            "touched_tables": ["fee_gap_accounting_repairs", "portfolio"],
            "expected_after": "historical fee-gap debt becomes explicit repair evidence and replay-consistent cash drift is eliminated",
            "idempotency_key": "fee_gap_accounting_repair:<adjustment_count>:<total_krw>",
            "rollback_or_backup": _backup_guidance(mode),
            "why_safe": "bounded explicit repair event with existing resume and flatness gates",
            "recommended_command": str(fee_gap_preview.get("recommended_command") or "uv run python bot.py fee-gap-accounting-repair"),
        },
        {
            "name": "manual-flat-accounting-repair",
            "needed": bool(manual_flat_preview.get("needs_repair")),
            "active_issue": bool(manual_flat_preview.get("needs_repair")),
            "safe_to_apply": bool(manual_flat_preview.get("safe_to_apply")),
            "preconditions": str(manual_flat_preview.get("eligibility_reason") or "review manual-flat accounting repair preview"),
            "touched_tables": ["manual_flat_accounting_repairs", "portfolio"],
            "expected_after": "portfolio and accounting replay converge at verified flat state without inventing synthetic fills",
            "idempotency_key": "manual_flat_accounting_repair:<cash_delta>:<asset_qty_delta>",
            "rollback_or_backup": _backup_guidance(mode),
            "why_safe": "only applies when portfolio is flat, open orders are clear, and lot residue is resolved",
            "recommended_command": str(manual_flat_preview.get("recommended_command") or "uv run python bot.py manual-flat-accounting-repair"),
        },
        {
            "name": "external-position-accounting-repair",
            "needed": bool(external_position_preview.get("needs_repair")),
            "active_issue": bool(external_position_preview.get("needs_repair")),
            "safe_to_apply": bool(external_position_preview.get("safe_to_apply")),
            "preconditions": str(external_position_preview.get("eligibility_reason") or "review external-position accounting repair preview"),
            "touched_tables": ["external_position_adjustments", "portfolio"],
            "expected_after": "portfolio and canonical accounting replay converge with explicit external-position evidence",
            "idempotency_key": "external_position_accounting_repair:<cash_delta>:<asset_qty_delta>",
            "rollback_or_backup": _backup_guidance(mode),
            "why_safe": "records replay-compatible external adjustment evidence instead of mutating canonical fills or trades",
            "recommended_command": str(
                external_position_preview.get("recommended_command")
                or "uv run python bot.py external-position-accounting-repair"
            ),
        },
        {
            "name": "flat-stale-lot-projection-repair",
            "needed": bool(
                position_preview.get("repair_mode") == "flat_stale_projection_repair"
                and (flat_stale_preview.get("needed") or position_preview.get("needs_rebuild"))
            ),
            "active_issue": bool(position_preview.get("repair_mode") == "flat_stale_projection_repair"),
            "safe_to_apply": bool(flat_stale_preview.get("safe_to_apply") or position_preview.get("safe_to_apply")),
            "pre_gate_passed": bool(position_preview.get("pre_gate_passed")),
            "final_safe_to_apply": bool(position_preview.get("final_safe_to_apply")),
            "preconditions": str(
                flat_stale_preview.get("preconditions")
                or "broker_qty=0, portfolio_qty=0, latest SELL filled, stale dust_tracking projection present"
            ),
            "touched_tables": [
                "open_position_lots",
                "position_authority_repairs",
                "position_authority_projection_publications",
            ],
            "expected_after": str(
                flat_stale_preview.get("expected_after")
                or "open_position_lots projection converges to broker/portfolio flat state"
            ),
            "idempotency_key": "flat_stale_lot_projection_repair:<latest_sell_trade_id>:<stale_lot_qty_total>",
            "rollback_or_backup": _backup_guidance(mode),
            "repair_kind": "flat_stale_lot_projection_repair",
            "truth_source": "broker_portfolio_terminal_sell_flat_evidence",
            "broker_qty": flat_stale_preview.get("broker_qty", position_preview.get("broker_qty")),
            "portfolio_qty": flat_stale_preview.get("portfolio_qty", position_preview.get("portfolio_qty")),
            "stale_lot_row_count": flat_stale_preview.get("stale_lot_row_count"),
            "stale_lot_qty_total": flat_stale_preview.get("stale_lot_qty_total"),
            "latest_sell_client_order_id": flat_stale_preview.get("latest_sell_client_order_id"),
            "latest_sell_trade_id": flat_stale_preview.get("latest_sell_trade_id"),
            "latest_sell_qty": flat_stale_preview.get("latest_sell_qty"),
            "projected_total_qty_before": flat_stale_preview.get("projected_total_qty_before"),
            "projected_total_qty_after_preview": flat_stale_preview.get("projected_total_qty_after_preview"),
            "projection_converged_after_publish": flat_stale_preview.get("expected_post_projection_converged"),
            "operator_next_action": position_preview.get("operator_next_action"),
            "preview_command": flat_stale_preview.get("preview_command") or position_preview.get("preview_command"),
            "why_safe": position_preview.get("why_safe"),
            "why_unsafe": list(flat_stale_preview.get("blockers") or position_preview.get("why_unsafe") or []),
            "recommended_command": str(
                flat_stale_preview.get("recommended_command")
                or position_preview.get("recommended_command")
                or ""
            ),
        },
        {
            "name": "rebuild-position-authority",
            "needed": bool(position_preview.get("needs_rebuild") or not report.get("lot_projection_converged", True)),
            "active_issue": bool(policy.get("accounting_root_cause_unresolved")),
            "safe_to_apply": bool(position_preview.get("safe_to_apply")),
            "pre_gate_passed": bool(position_preview.get("pre_gate_passed")),
            "final_safe_to_apply": bool(position_preview.get("final_safe_to_apply")),
            "preconditions": str(position_preview.get("eligibility_reason") or "review position-authority rebuild preview"),
            "touched_tables": [
                "open_position_lots",
                "position_authority_repairs",
                "position_authority_projection_publications",
                "external_position_adjustments",
            ],
            "expected_after": "open_position_lots projection is rebuilt or repaired so projected lots converge to canonical holdings",
            "idempotency_key": f"position_authority_repair:{position_preview.get('repair_mode') or 'unknown'}",
            "rollback_or_backup": _backup_guidance(mode),
            "repair_kind": position_preview.get("repair_kind"),
            "truth_source": position_preview.get("truth_source"),
            "pre_projected_total_qty": position_preview.get("pre_projected_total_qty"),
            "replay_projected_total_qty": position_preview.get("replay_projected_total_qty"),
            "post_publish_projected_total_qty": position_preview.get("post_publish_projected_total_qty"),
            "portfolio_qty": position_preview.get("portfolio_qty"),
            "broker_qty": position_preview.get("broker_qty"),
            "projection_converged_before": position_preview.get("projection_converged_before"),
            "projection_converged_after_replay": position_preview.get("projection_converged_after_replay"),
            "projection_converged_after_publish": position_preview.get("projection_converged_after_publish"),
            "replay_projection_converged": position_preview.get("replay_projection_converged"),
            "post_publish_projection_converged": position_preview.get("post_publish_projection_converged"),
            "source_mode_of_new_rows": list(position_preview.get("source_mode_of_new_rows") or []),
            "target_lot_provenance_kind": position_preview.get("target_lot_provenance_kind"),
            "fill_qty_invariant_applies": position_preview.get("target_lot_fill_qty_invariant_applies"),
            "semantic_contract_check_applicable": position_preview.get("semantic_contract_check_applicable"),
            "semantic_contract_check_skipped_reason": position_preview.get("semantic_contract_check_skipped_reason"),
            "rollback_path": position_preview.get("rollback_path"),
            "operator_next_action": position_preview.get("operator_next_action"),
            "preview_command": position_preview.get("preview_command"),
            "why_safe": position_preview.get("why_safe"),
            "why_unsafe": list(position_preview.get("why_unsafe") or []),
            "recommended_command": str(
                position_preview.get("recommended_command") or ""
            ),
        },
    ]
    inactive_reason = (
        "current broker/portfolio/projection are converged; residual-only tradeability policy applies"
        if str(policy.get("recommended_mode") or "") == "residual_policy_review"
        else "current report does not indicate this repair is needed"
    )
    for candidate in candidates:
        needed = bool(candidate.get("needed"))
        if not needed:
            candidate["recommended_command"] = None
            candidate["command_applicable"] = False
            candidate["not_recommended_reason"] = inactive_reason
            continue
        candidate["command_applicable"] = bool(candidate.get("recommended_command"))
        candidate["not_recommended_reason"] = None
    return candidates


def build_repair_plan_preview_from_report(report: dict[str, Any]) -> dict[str, Any]:
    policy = build_recovery_policy_from_report(report)
    position_preview = dict(report.get("position_authority_rebuild_preview") or {})
    projection_reason = "converged" if _truthy(report.get("lot_projection_converged")) else (
        str(
            ((report.get("runtime_readiness") or {}).get("projection_convergence") or {}).get("reason")
            or "projection_non_converged"
        )
    )
    runtime_readiness = dict(report.get("runtime_readiness") or {})
    tradeability_reason = str(
        report.get("tradeability_reason")
        or runtime_readiness.get("tradeability_reason")
        or runtime_readiness.get("residual_class")
        or "none"
    )
    primary_reason = str(
        report.get("primary_reason")
        or runtime_readiness.get("primary_reason")
        or tradeability_reason
        or projection_reason
    )
    candidate_repairs = _candidate_repairs_from_report(report, policy)
    safe_recommended_candidate = next(
        (
            item
            for item in candidate_repairs
            if bool(item.get("needed"))
            and bool(item.get("safe_to_apply"))
            and item.get("recommended_command")
        ),
        None,
    )
    recommended_command = (
        str(safe_recommended_candidate["recommended_command"])
        if safe_recommended_candidate is not None
        else str(policy["recommended_command"])
    )
    recommended_action = (
        f"apply_{safe_recommended_candidate['name']}"
        if safe_recommended_candidate is not None
        else str(policy["recommended_action"])
    )
    payload = {
        "mode": str(report.get("mode") or "paper"),
        "primary_incident_class": str(policy["primary_incident_class"]),
        "recommended_mode": str(policy["recommended_mode"]),
        "accounting_root_cause_unresolved": bool(policy["accounting_root_cause_unresolved"]),
        "accounting_evidence_reliable": bool(policy["accounting_evidence_reliable"]),
        "actual_executable_exposure": bool(policy["actual_executable_exposure"]),
        "position_management_allowed": bool(policy["position_management_allowed"]),
        "additional_orders_allowed": bool(policy["additional_orders_allowed"]),
        "flatten_primary_recommendation": bool(policy["flatten_primary_recommendation"]),
        "flatten_not_primary": bool(policy["flatten_not_primary"]),
        "recommended_action": recommended_action,
        "recommended_command": recommended_command,
        "incident_reasons": list(policy["incident_reasons"]),
        "canonical_portfolio_qty": _float(report.get("portfolio_qty")),
        "broker_qty": _float(report.get("broker_qty")),
        "broker_qty_known": _truthy(report.get("broker_qty_known")),
        "broker_qty_value_source": report.get("broker_qty_value_source"),
        "broker_qty_evidence_source": report.get("broker_qty_evidence_source"),
        "broker_qty_evidence_observed_ts_ms": report.get("broker_qty_evidence_observed_ts_ms"),
        "balance_source": report.get("balance_source"),
        "balance_source_stale": report.get("balance_source_stale"),
        "balance_snapshot_available_for_health": _truthy(report.get("balance_snapshot_available_for_health")),
        "balance_snapshot_available_for_position_rebuild": _truthy(
            report.get("balance_snapshot_available_for_position_rebuild")
        ),
        "missing_evidence_fields": list(report.get("missing_evidence_fields") or []),
        "position_rebuild_blockers": list(report.get("position_rebuild_blockers") or []),
        "base_currency": report.get("base_currency"),
        "quote_currency": report.get("quote_currency"),
        "asset_available": report.get("asset_available"),
        "asset_locked": report.get("asset_locked"),
        "cash_available": report.get("cash_available"),
        "cash_locked": report.get("cash_locked"),
        "open_position_lots_projected_qty": _float(
            ((report.get("runtime_readiness") or {}).get("projection_convergence") or {}).get("projected_total_qty")
        ),
        "broker_portfolio_converged": bool(report.get("broker_portfolio_converged")),
        "projection_converged": bool(report.get("lot_projection_converged")),
        "source_of_truth": "fills+trades+fee_adjustments+external_adjustments+repair_events",
        "projection_kind": "open_position_lots",
        "rebuildable": True,
        "safe_to_rebuild": bool((report.get("position_authority_rebuild_preview") or {}).get("safe_to_apply")),
        "pre_gate_passed": bool((report.get("position_authority_rebuild_preview") or {}).get("pre_gate_passed")),
        "final_safe_to_rebuild": bool((report.get("position_authority_rebuild_preview") or {}).get("final_safe_to_apply")),
        "reason": primary_reason,
        "primary_reason": primary_reason,
        "projection_reason": projection_reason,
        "tradeability_reason": tradeability_reason,
        "non_mutating_preview": True,
        "repair_kind": position_preview.get("repair_kind"),
        "truth_source": position_preview.get("truth_source"),
        "pre_projected_total_qty": position_preview.get("pre_projected_total_qty"),
        "replay_projected_total_qty": position_preview.get("replay_projected_total_qty"),
        "post_publish_projected_total_qty": position_preview.get("post_publish_projected_total_qty"),
        "projection_converged_before": position_preview.get("projection_converged_before"),
        "projection_converged_after_replay": position_preview.get("projection_converged_after_replay"),
        "projection_converged_after_publish": position_preview.get("projection_converged_after_publish"),
        "replay_projection_converged": position_preview.get("replay_projection_converged"),
        "post_publish_projection_converged": position_preview.get("post_publish_projection_converged"),
        "source_mode_of_new_rows": list(position_preview.get("source_mode_of_new_rows") or []),
        "target_lot_provenance_kind": position_preview.get("target_lot_provenance_kind"),
        "fill_qty_invariant_applies": position_preview.get("target_lot_fill_qty_invariant_applies"),
        "semantic_contract_check_applicable": position_preview.get("semantic_contract_check_applicable"),
        "semantic_contract_check_skipped_reason": position_preview.get("semantic_contract_check_skipped_reason"),
        "rollback_path": position_preview.get("rollback_path"),
        "operator_next_action": position_preview.get("operator_next_action"),
        "preview_command": position_preview.get("preview_command"),
        "why_safe": position_preview.get("why_safe"),
        "why_unsafe": list(position_preview.get("why_unsafe") or []),
        "candidate_repairs": candidate_repairs,
    }
    plan_basis = {
        "mode": payload["mode"],
        "primary_incident_class": payload["primary_incident_class"],
        "recommended_mode": payload["recommended_mode"],
        "candidate_repairs": [
            {
                "name": item["name"],
                "needed": item["needed"],
                "safe_to_apply": item["safe_to_apply"],
            }
            for item in payload["candidate_repairs"]
        ],
    }
    payload["plan_id"] = hashlib.sha256(
        json.dumps(plan_basis, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return payload
