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
    accounting_root_cause_unresolved = any(
        (
            active_fill_issue_count > 0,
            fee_gap_active_issue,
            _truthy(manual_flat_preview.get("needs_repair")),
            _truthy(external_position_preview.get("needs_repair")),
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
    closeout_allowed = bool(runtime_readiness.get("closeout_allowed"))

    recommended_mode = "recovery"
    primary_incident_class = "RECOVERY_READINESS"
    additional_orders_allowed = False
    flatten_primary_recommendation = False
    recommended_action = str(report.get("operator_next_action") or "investigate_blockers")
    recommended_command = str(report.get("recommended_command") or "uv run python bot.py recovery-report")

    if accounting_root_cause_unresolved:
        recommended_mode = "forensic_accounting"
        primary_incident_class = "ACCOUNTING_ROOT_CAUSE"
        recommended_action = "collect_broker_fill_evidence_and_build_repair_plan"
        recommended_command = "uv run python bot.py repair-plan"
    elif actual_executable_exposure and accounting_evidence_reliable and closeout_allowed:
        recommended_mode = "market_risk"
        primary_incident_class = "MARKET_RISK_EXPOSURE"
        additional_orders_allowed = True
        flatten_primary_recommendation = True
        recommended_action = "review_executable_exposure_and_consider_flatten"
        recommended_command = "uv run python bot.py flatten-position"

    incident_reasons: list[str] = []
    if active_fill_issue_count > 0:
        incident_reasons.append("fill_accounting_incident_active")
    if fee_gap_active_issue:
        incident_reasons.append("fee_gap_recovery_active")
    if _truthy(manual_flat_preview.get("needs_repair")):
        incident_reasons.append("manual_flat_accounting_repair_required")
    if _truthy(external_position_preview.get("needs_repair")):
        incident_reasons.append("external_position_accounting_repair_required")
    if projection_drift:
        incident_reasons.append("open_position_lots_projection_drift")
    if _int(fee_rate_drift.get("recent_expected_fee_rate_mismatch_count")) > 0:
        incident_reasons.append("fee_rate_drift_visible")

    return {
        "primary_incident_class": primary_incident_class,
        "recommended_mode": recommended_mode,
        "accounting_root_cause_unresolved": bool(accounting_root_cause_unresolved),
        "accounting_evidence_reliable": bool(accounting_evidence_reliable),
        "actual_executable_exposure": bool(actual_executable_exposure),
        "additional_orders_allowed": bool(additional_orders_allowed),
        "flatten_primary_recommendation": bool(flatten_primary_recommendation),
        "flatten_not_primary": bool(not flatten_primary_recommendation),
        "recommended_action": recommended_action,
        "recommended_command": recommended_command,
        "fill_root_cause": str(fill_root.get("root") or "none"),
        "incident_reasons": incident_reasons,
    }


def _backup_guidance(mode: str) -> str:
    return f"backup/{mode}/db/ via scripts/backup_sqlite.sh before apply"


def _candidate_repairs_from_report(report: dict[str, Any], policy: dict[str, Any]) -> list[dict[str, Any]]:
    mode = str(report.get("mode") or "paper")
    fill_projection = dict(report.get("fill_accounting_incident_projection") or {})
    fee_gap_preview = dict(report.get("fee_gap_accounting_repair_preview") or {})
    manual_flat_preview = dict(report.get("manual_flat_accounting_repair_preview") or {})
    external_position_preview = dict(report.get("external_position_accounting_repair_preview") or {})
    position_preview = dict(report.get("position_authority_rebuild_preview") or {})

    candidates = [
        {
            "name": "fee-pending-accounting-repair",
            "needed": bool(
                _int(fill_projection.get("active_fee_pending_count")) > 0
                or _int(fill_projection.get("fee_validation_blocked_count")) > 0
                or _int(fill_projection.get("unapplied_principal_pending_count")) > 0
            ),
            "active_issue": bool(_int(fill_projection.get("active_issue_count")) > 0),
            "safe_to_apply": False,
            "preconditions": (
                "identify the exact pending fill and provide client_order_id, fill_id, fee, and fee_provenance"
            ),
            "touched_tables": [
                "broker_fill_observations",
                "fills",
                "trades",
                "fee_pending_accounting_repairs",
                "portfolio",
            ],
            "expected_after": "fee finalization recorded through normal accounting path; fee_pending incident count decreases",
            "idempotency_key": "fee_pending_accounting_repair:<client_order_id>:<fill_id>:<fee>",
            "rollback_or_backup": _backup_guidance(mode),
            "why_safe": "requires explicit operator fee evidence and replays through canonical accounting instead of manual row edits",
            "recommended_command": "uv run python bot.py fee-pending-accounting-repair --client-order-id <id> --fill-id <fill_id> --fee <fee> --fee-provenance <source>",
        },
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
            "name": "rebuild-position-authority",
            "needed": bool(position_preview.get("needs_rebuild") or not report.get("lot_projection_converged", True)),
            "active_issue": bool(policy.get("accounting_root_cause_unresolved")),
            "safe_to_apply": bool(position_preview.get("safe_to_apply")),
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
            "why_safe": "treats open_position_lots as rebuildable projection and requires broker, portfolio, and replay gates before mutation",
            "recommended_command": str(
                position_preview.get("recommended_command") or "uv run python bot.py rebuild-position-authority"
            ),
        },
    ]
    return candidates


def build_repair_plan_preview_from_report(report: dict[str, Any]) -> dict[str, Any]:
    policy = build_recovery_policy_from_report(report)
    projection_reason = "converged" if _truthy(report.get("lot_projection_converged")) else (
        str(
            ((report.get("runtime_readiness") or {}).get("projection_convergence") or {}).get("reason")
            or "projection_non_converged"
        )
    )
    payload = {
        "mode": str(report.get("mode") or "paper"),
        "primary_incident_class": str(policy["primary_incident_class"]),
        "recommended_mode": str(policy["recommended_mode"]),
        "accounting_root_cause_unresolved": bool(policy["accounting_root_cause_unresolved"]),
        "accounting_evidence_reliable": bool(policy["accounting_evidence_reliable"]),
        "actual_executable_exposure": bool(policy["actual_executable_exposure"]),
        "additional_orders_allowed": bool(policy["additional_orders_allowed"]),
        "flatten_primary_recommendation": bool(policy["flatten_primary_recommendation"]),
        "flatten_not_primary": bool(policy["flatten_not_primary"]),
        "recommended_action": str(policy["recommended_action"]),
        "recommended_command": str(policy["recommended_command"]),
        "incident_reasons": list(policy["incident_reasons"]),
        "canonical_portfolio_qty": _float(report.get("portfolio_qty")),
        "broker_qty": _float(report.get("broker_qty")),
        "broker_qty_known": _truthy(report.get("broker_qty_known")),
        "broker_qty_evidence_source": report.get("broker_qty_evidence_source"),
        "broker_qty_evidence_observed_ts_ms": report.get("broker_qty_evidence_observed_ts_ms"),
        "balance_source": report.get("balance_source"),
        "balance_source_stale": report.get("balance_source_stale"),
        "balance_snapshot_available_for_health": _truthy(report.get("balance_snapshot_available_for_health")),
        "balance_snapshot_available_for_position_rebuild": _truthy(
            report.get("balance_snapshot_available_for_position_rebuild")
        ),
        "missing_evidence_fields": list(report.get("missing_evidence_fields") or []),
        "open_position_lots_projected_qty": _float(
            ((report.get("runtime_readiness") or {}).get("projection_convergence") or {}).get("projected_total_qty")
        ),
        "broker_portfolio_converged": bool(report.get("broker_portfolio_converged")),
        "projection_converged": bool(report.get("lot_projection_converged")),
        "source_of_truth": "fills+trades+fee_adjustments+external_adjustments+repair_events",
        "projection_kind": "open_position_lots",
        "rebuildable": True,
        "safe_to_rebuild": bool((report.get("position_authority_rebuild_preview") or {}).get("safe_to_apply")),
        "reason": projection_reason,
        "non_mutating_preview": True,
        "candidate_repairs": _candidate_repairs_from_report(report, policy),
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
