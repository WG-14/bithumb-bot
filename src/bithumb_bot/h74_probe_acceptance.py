from __future__ import annotations

from collections.abc import Mapping


REQUIRED_REPORT_FIELDS = (
    "buy_order_filled",
    "h74_cycle_ownership_created",
    "h74_cycle_id",
    "h74_remaining_cycle_qty_before_sell",
    "sell_order_submitted",
    "sell_order_filled",
    "h74_cycle_state_closed",
    "portfolio_flat",
    "accounting_flat",
    "manual_intervention",
    "buy_decision_id",
    "buy_execution_plan_id",
    "buy_order_id",
    "buy_client_order_id",
    "buy_fill_id",
    "buy_order_h74_entry_plan_client_order_id",
    "buy_order_h74_position_ownership_contract",
    "cycle_h74_entry_plan_client_order_id",
    "open_lot_id",
    "sell_decision_id",
    "sell_execution_plan_id",
    "sell_order_id",
    "sell_client_order_id",
    "sell_fill_id",
    "lifecycle_id",
)


def _nested_missing(report: Mapping[str, object]) -> list[str]:
    missing: list[str] = []
    buy_leg = report.get("buy_leg")
    sell_leg = report.get("sell_leg")
    if not isinstance(buy_leg, Mapping):
        missing.append("buy_leg")
    else:
        for key in (
            "decision_id",
            "execution_plan_id",
            "order_id",
            "client_order_id",
            "fill_id",
            "open_lot_id",
        ):
            if not buy_leg.get(key):
                missing.append(f"buy_leg.{key}")
    if not isinstance(sell_leg, Mapping):
        missing.append("sell_leg")
    else:
        for key in (
            "decision_id",
            "execution_plan_id",
            "order_id",
            "client_order_id",
            "fill_id",
            "lifecycle_id",
        ):
            if not sell_leg.get(key):
                missing.append(f"sell_leg.{key}")
    return missing


def evaluate_h74_execution_path_probe_acceptance(report: Mapping[str, object]) -> dict[str, object]:
    if str(report.get("artifact_type") or "") != "h74_execution_path_probe_report":
        missing = ["h74_execution_path_probe_report_schema"]
    else:
        missing = []
        for key in REQUIRED_REPORT_FIELDS:
            if key == "manual_intervention":
                if key not in report:
                    missing.append(key)
                continue
            if not report.get(key):
                missing.append(key)
        missing.extend(_nested_missing(report))
    accounting = report.get("accounting")
    if not isinstance(accounting, Mapping) or not bool(accounting.get("validated")):
        missing.append("accounting.validated")
    if not bool(report.get("final_flat_or_documented_dust")):
        missing.append("final_flat_or_documented_dust")
    buy_entry_id = str(report.get("buy_order_h74_entry_plan_client_order_id") or "").strip()
    cycle_entry_id = str(report.get("cycle_h74_entry_plan_client_order_id") or "").strip()
    if buy_entry_id and cycle_entry_id and buy_entry_id != cycle_entry_id:
        missing.append("h74_entry_plan_identity_match")
    manual_intervention = bool(
        report.get("manual_intervention")
        or report.get("manual_sell")
        or report.get("operator_closeout")
    )
    if manual_intervention:
        missing.append("automated_sell_required")
    report_status = str(report.get("execution_path_probe_status") or "")
    if report_status != "PASS":
        missing.append("execution_path_probe_status")

    status = "PASS" if not missing else ("PARTIAL_PASS" if bool(report.get("buy_order_filled")) else "INCOMPLETE")
    return {
        "artifact_type": "h74_execution_path_probe_acceptance",
        "acceptance_track": "execution_path_probe",
        "probe_run_id": str(report.get("probe_run_id") or ""),
        "execution_path_probe_status": status,
        "source_execution_path_probe_status": report_status,
        "buy_order_filled": bool(report.get("buy_order_filled")),
        "buy_order_h74_entry_plan_client_order_id": buy_entry_id,
        "cycle_h74_entry_plan_client_order_id": cycle_entry_id,
        "h74_cycle_ownership_created": bool(report.get("h74_cycle_ownership_created")),
        "h74_cycle_id": str(report.get("h74_cycle_id") or ""),
        "h74_remaining_cycle_qty_before_sell": float(report.get("h74_remaining_cycle_qty_before_sell") or 0.0),
        "sell_order_submitted": bool(report.get("sell_order_submitted")),
        "sell_order_filled": bool(report.get("sell_order_filled")),
        "h74_cycle_state_closed": bool(report.get("h74_cycle_state_closed")),
        "portfolio_flat": bool(report.get("portfolio_flat")),
        "accounting_flat": bool(report.get("accounting_flat")),
        "manual_intervention": manual_intervention,
        "h74_exit_authority_ready": int(bool(report.get("h74_exit_authority_ready"))),
        "h74_remaining_cycle_qty": float(report.get("h74_remaining_cycle_qty") or 0.0),
        "h74_cycle_contract_hash": str(report.get("h74_cycle_contract_hash") or ""),
        "h74_exit_authority_not_ready_reason": str(
            report.get("h74_exit_authority_not_ready_reason") or ""
        ),
        "missing_evidence": missing,
        "research_equivalence": False,
        "research_equivalence_status": "NOT_APPLICABLE",
        "production_approval": False,
        "promotion_grade": False,
    }
