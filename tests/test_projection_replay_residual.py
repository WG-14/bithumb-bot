from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.residual_disposition import build_residual_disposition


def _verdict(*, projected_delta: float = 0.0):
    qty = 0.00009996
    return build_residual_disposition(
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
            "converged": projected_delta == 0.0,
            "portfolio_qty": qty,
            "projected_total_qty": qty + projected_delta,
            "expected_residual_qty": qty,
            "projection_convergence": "converged" if projected_delta == 0.0 else "mismatch",
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


def test_partial_close_residual_converged_does_not_require_authority_correction():
    verdict = _verdict()

    assert verdict.disposition == "TRACKED_NON_EXECUTABLE"
    assert verdict.repair_required is False
    assert verdict.run_allowed is True


def test_projection_mismatch_requires_authority_repair_not_manual_closeout():
    verdict = _verdict(projected_delta=0.0001)

    assert verdict.disposition == "BLOCKING_INCONSISTENT"
    assert verdict.repair_required is True
    assert verdict.recommended_action == "review_recovery_report"
    assert verdict.manual_exchange_action_required is False


def test_rebuild_projection_is_deterministic_for_incident_fixture():
    first = _verdict().as_dict()
    second = _verdict().as_dict()

    assert second == first
