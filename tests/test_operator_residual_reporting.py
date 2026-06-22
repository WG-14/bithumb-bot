from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.residual_disposition import build_residual_disposition


def _payload(*, mismatch: bool = False) -> dict[str, object]:
    qty = 0.00009996
    verdict = build_residual_disposition(
        residual_inventory=SimpleNamespace(residual_qty=qty, exchange_sellable=False),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        residual_policy_allows_run=True,
        residual_policy_allows_buy=True,
        residual_policy_allows_sell=False,
        position_state=SimpleNamespace(
            normalized_exposure=SimpleNamespace(
                has_executable_exposure=False,
                sellable_executable_lot_count=0,
            )
        ),
        authority_assessment={},
        projection_convergence={
            "converged": not mismatch,
            "portfolio_qty": qty,
            "projected_total_qty": qty if not mismatch else qty + 0.0001,
        },
        broker_position_evidence={"broker_qty_known": True, "broker_qty": qty},
        lot_definition=SimpleNamespace(
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=8,
            source_mode="ledger",
        ),
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
    )
    return {
        "operator_action_required": verdict.operator_action_required,
        "recommended_action": verdict.recommended_action,
        "manual_exchange_action_required": verdict.manual_exchange_action_required,
        "residual_disposition": verdict.disposition,
        "residual_reason_code": verdict.reason_codes[0],
        "quantity_rule_authority": verdict.quantity_rule_authority,
        "broker_local_projection_state": verdict.broker_local_projection_state,
    }


def test_reports_do_not_recommend_manual_app_sell_for_tracked_sub_min_residual():
    payload = _payload()

    assert payload["residual_disposition"] == "TRACKED_NON_EXECUTABLE"
    assert payload["operator_action_required"] is False
    assert payload["manual_exchange_action_required"] is False
    assert payload["recommended_action"] == "none"


def test_reports_recommend_review_for_projection_mismatch():
    payload = _payload(mismatch=True)

    assert payload["residual_disposition"] == "BLOCKING_INCONSISTENT"
    assert payload["recommended_action"] == "review_recovery_report"
    assert payload["manual_exchange_action_required"] is False


def test_reports_include_quantity_rule_authority():
    payload = _payload()

    assert payload["quantity_rule_authority"] == "persisted_exchange_snapshot"
    assert payload["residual_reason_code"] == "sub_min_qty_residual_tracked"
