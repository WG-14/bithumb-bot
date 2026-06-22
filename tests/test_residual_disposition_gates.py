from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.residual_disposition import build_residual_disposition


def _verdict(*, run_allowed: bool = True, mismatch: bool = False):
    qty = 0.00009996
    return build_residual_disposition(
        residual_inventory=SimpleNamespace(residual_qty=qty, exchange_sellable=False),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        residual_policy_allows_run=run_allowed,
        residual_policy_allows_buy=run_allowed,
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


def test_startup_resume_and_reconcile_halt_agree_for_tracked_sub_min_residual():
    verdict = _verdict()

    assert verdict.disposition == "TRACKED_NON_EXECUTABLE"
    assert verdict.run_allowed is True
    assert verdict.buy_allowed is True
    assert verdict.flatten_allowed is False


def test_resume_does_not_block_when_residual_disposition_allows_run():
    verdict = _verdict()

    assert verdict.run_allowed is True
    assert verdict.reason_codes == ("sub_min_qty_residual_tracked",)


def test_reconcile_halt_clears_only_when_residual_disposition_allows_run():
    allowed = _verdict()
    blocked = _verdict(mismatch=True)

    assert allowed.run_allowed is True
    assert blocked.disposition == "BLOCKING_INCONSISTENT"
    assert blocked.run_allowed is False
