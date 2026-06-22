from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.quantity_contract import ExchangeQuantityContract
from bithumb_bot.residual_disposition import build_residual_disposition


def test_residual_verdict_includes_quantity_rule_sources():
    verdict = build_residual_disposition(
        residual_inventory=SimpleNamespace(residual_qty=0.00009996, exchange_sellable=False),
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
            "converged": True,
            "portfolio_qty": 0.00009996,
            "projected_total_qty": 0.00009996,
        },
        broker_position_evidence={"broker_qty_known": True, "broker_qty": 0.00009996},
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

    payload = verdict.as_dict()
    assert payload["min_qty"] == 0.0001
    assert payload["min_qty_source"] == "persisted_exchange_snapshot"
    assert payload["qty_step"] == 0.0001
    assert payload["qty_step_authority_level"] == "persisted_exchange_snapshot"
    assert payload["quantity_contract_complete"] is True


def test_local_fallback_qty_min_blocks_as_local_policy_not_exchange_reject():
    contract = ExchangeQuantityContract.local_fallback(
        market="BTC_KRW",
        min_qty=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
        configured_qty_step=0.0001,
    )

    payload = contract.as_dict()
    assert payload["qty_step_authority_level"] == "local_fallback"
    assert payload["quantity_rule_source_mode"] == "local_fallback"
    assert contract.quantity_contract_recommended_action == "review_local_quantity_fallback_before_real_submit"


def test_unknown_qty_authority_blocks_with_refresh_rules_action():
    resolution = SimpleNamespace(
        rules=SimpleNamespace(
            market_id="BTC_KRW",
            min_qty=0.0001,
            min_notional_krw=5000.0,
            qty_step=0.0001,
            max_qty_decimals=8,
        ),
        source={},
        source_mode="",
        snapshot_persisted=False,
    )
    contract = ExchangeQuantityContract.from_rule_resolution(resolution, market="BTC_KRW")

    assert contract.qty_step_authority_level == "unknown"
    assert contract.quantity_contract_complete is False
    assert contract.quantity_contract_recommended_action == "refresh_order_rules_or_review_quantity_settings"
