from __future__ import annotations

from bithumb_bot.h74_probe_acceptance import evaluate_h74_execution_path_probe_acceptance


def _pass_report() -> dict[str, object]:
    return {
        "artifact_type": "h74_execution_path_probe_report",
        "probe_run_id": "probe-1",
        "execution_path_probe_status": "PASS",
        "buy_order_filled": True,
        "h74_cycle_ownership_created": True,
        "h74_cycle_id": "cycle-1",
        "h74_remaining_cycle_qty_before_sell": 0.0008,
        "sell_order_submitted": True,
        "sell_order_filled": True,
        "h74_cycle_state_closed": True,
        "portfolio_flat": True,
        "accounting_flat": True,
        "manual_intervention": False,
        "h74_exit_authority_ready": 1,
        "h74_remaining_cycle_qty": 0.0008,
        "h74_cycle_contract_hash": "sha256:contract",
        "h74_exit_authority_not_ready_reason": "none",
        "buy_decision_id": 1,
        "buy_execution_plan_id": 2,
        "buy_order_id": 3,
        "buy_client_order_id": "buy-1",
        "buy_fill_id": 4,
        "open_lot_id": 5,
        "sell_decision_id": 6,
        "sell_execution_plan_id": 7,
        "sell_order_id": 8,
        "sell_client_order_id": "sell-1",
        "sell_fill_id": 9,
        "lifecycle_id": 10,
        "buy_leg": {
            "decision_id": 1,
            "execution_plan_id": 2,
            "order_id": 3,
            "client_order_id": "buy-1",
            "fill_id": 4,
            "open_lot_id": 5,
        },
        "sell_leg": {
            "decision_id": 6,
            "execution_plan_id": 7,
            "order_id": 8,
            "client_order_id": "sell-1",
            "fill_id": 9,
            "lifecycle_id": 10,
        },
        "accounting": {"validated": True},
        "final_flat_or_documented_dust": True,
        "research_equivalence": False,
        "research_equivalence_status": "NOT_APPLICABLE",
        "production_approval": False,
    }


def test_acceptance_consumes_probe_report_schema() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(_pass_report())
    assert result["execution_path_probe_status"] == "PASS"
    assert result["acceptance_track"] == "execution_path_probe"
    assert result["sell_order_filled"] is True
    assert result["h74_cycle_state_closed"] is True
    assert result["portfolio_flat"] is True
    assert result["accounting_flat"] is True


def test_acceptance_rejects_report_without_lifecycle_id() -> None:
    report = _pass_report()
    report["lifecycle_id"] = None
    report["sell_leg"]["lifecycle_id"] = None
    result = evaluate_h74_execution_path_probe_acceptance(report)
    assert result["execution_path_probe_status"] != "PASS"
    assert "lifecycle_id" in result["missing_evidence"]


def test_acceptance_artifact_never_enables_research_or_production() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(_pass_report())
    assert result["research_equivalence"] is False
    assert result["research_equivalence_status"] == "NOT_APPLICABLE"
    assert result["production_approval"] is False
    assert result["promotion_grade"] is False


def test_h74_buy_only_does_not_pass_roundtrip_acceptance() -> None:
    report = _pass_report()
    report["sell_order_submitted"] = False
    report["sell_order_filled"] = False
    report["sell_order_id"] = None
    report["sell_leg"]["order_id"] = None

    result = evaluate_h74_execution_path_probe_acceptance(report)

    assert result["execution_path_probe_status"] != "PASS"
    assert "sell_order_submitted" in result["missing_evidence"]
    assert "sell_order_filled" in result["missing_evidence"]


def test_h74_manual_sell_does_not_count_as_automated_sell_success() -> None:
    report = _pass_report()
    report["manual_sell"] = True

    result = evaluate_h74_execution_path_probe_acceptance(report)

    assert result["execution_path_probe_status"] != "PASS"
    assert "automated_sell_required" in result["missing_evidence"]
