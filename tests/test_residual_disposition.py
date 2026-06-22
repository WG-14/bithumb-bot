from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.residual_disposition import build_residual_disposition


def _position_state(*, executable: bool = False, sellable_lots: int = 0):
    return SimpleNamespace(
        normalized_exposure=SimpleNamespace(
            has_executable_exposure=executable,
            sellable_executable_lot_count=sellable_lots,
        )
    )


def _residual(*, exchange_sellable: bool = False):
    return SimpleNamespace(
        residual_qty=0.00009996,
        residual_notional_krw=9665.0,
        residual_classes=("LEDGER_SPLIT_RESIDUAL", "TRUE_DUST"),
        exchange_sellable=exchange_sellable,
    )


def _lot_definition():
    return SimpleNamespace(
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
        source_mode="ledger",
    )


def _converged():
    return {
        "converged": True,
        "portfolio_qty": 0.00009996,
        "projected_total_qty": 0.00009996,
    }


def _broker():
    return {
        "broker_qty_known": True,
        "broker_qty": 0.00009996,
    }


def test_single_verdict_for_sub_min_high_notional_tracked_residual():
    verdict = build_residual_disposition(
        residual_inventory=_residual(exchange_sellable=False),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        residual_policy_allows_run=True,
        residual_policy_allows_buy=True,
        residual_policy_allows_sell=False,
        position_state=_position_state(),
        authority_assessment={},
        projection_convergence=_converged(),
        broker_position_evidence=_broker(),
        lot_definition=_lot_definition(),
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
    )

    assert verdict.disposition == "TRACKED_NON_EXECUTABLE"
    assert verdict.exchange_sellable is False
    assert verdict.reason_codes == ("sub_min_qty_residual_tracked",)
    assert verdict.run_allowed is True
    assert verdict.flatten_allowed is False
    assert verdict.manual_exchange_action_required is False


def test_startup_resume_flatten_use_same_residual_disposition():
    verdict = build_residual_disposition(
        residual_inventory=_residual(exchange_sellable=False),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        residual_policy_allows_run=True,
        residual_policy_allows_buy=True,
        residual_policy_allows_sell=False,
        position_state=_position_state(),
        authority_assessment={},
        projection_convergence=_converged(),
        broker_position_evidence=_broker(),
        lot_definition=_lot_definition(),
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
    )

    payload = verdict.as_dict()
    assert payload["run_allowed"] is True
    assert payload["buy_allowed"] is True
    assert payload["sell_allowed"] is False
    assert payload["flatten_allowed"] is False
    assert payload["recommended_action"] == "none"


def test_authority_repair_required_overrides_tracked_residual():
    verdict = build_residual_disposition(
        residual_inventory=_residual(exchange_sellable=False),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        residual_policy_allows_run=True,
        residual_policy_allows_buy=True,
        residual_policy_allows_sell=False,
        position_state=_position_state(),
        authority_assessment={"needs_portfolio_projection_repair": True},
        projection_convergence=_converged(),
        broker_position_evidence=_broker(),
        lot_definition=_lot_definition(),
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
    )

    assert verdict.disposition == "AUTHORITY_REPAIR_REQUIRED"
    assert verdict.run_allowed is False
    assert verdict.repair_required is True
    assert verdict.reason_codes == ("authority_repair_required",)
