from __future__ import annotations

from bithumb_bot.quantity_kernel import OrderRuleSnapshot, plan_buy_notional, plan_sell_qty
from bithumb_bot.order_sizing import build_target_delta_execution_sizing
from bithumb_bot.research.backtest_kernel import _research_execution_plan_bundle
from tests.test_research_execution_submit_plan_bridge import _typed_decision


def _rules(**overrides) -> OrderRuleSnapshot:
    payload = {"min_qty": 0.0001, "qty_step": 0.0001, "max_qty_decimals": 8, "min_notional_krw": 5000.0}
    payload.update(overrides)
    return OrderRuleSnapshot.from_mapping(payload)


def test_buy_100000_krw_same_quantity_contract_in_research_and_live() -> None:
    research = plan_buy_notional(requested_notional_krw=100_000.0, reference_price=123_456_789.0, rules=_rules())
    live = plan_buy_notional(requested_notional_krw=100_000.0, reference_price=123_456_789.0, rules=_rules())

    assert research.submitted_qty == live.submitted_qty
    assert research.quantity_contract_hash == live.quantity_contract_hash


def test_sell_remaining_cycle_qty_same_submitted_volume_in_research_and_live() -> None:
    research = plan_sell_qty(requested_qty=0.00087, reference_price=123_456_789.0, rules=_rules())
    live = plan_sell_qty(requested_qty=0.00087, reference_price=123_456_789.0, rules=_rules())

    assert research.submitted_qty == live.submitted_qty == 0.0008
    assert research.exchange_submit_field == live.exchange_submit_field == "volume"


def test_qty_step_mutation_changes_quantity_contract_hash() -> None:
    baseline = plan_sell_qty(requested_qty=0.00087, reference_price=123_456_789.0, rules=_rules())
    mutated = plan_sell_qty(requested_qty=0.00087, reference_price=123_456_789.0, rules=_rules(qty_step=0.00001))

    assert baseline.quantity_contract_hash != mutated.quantity_contract_hash


def test_max_qty_decimals_mutation_changes_quantity_contract_hash() -> None:
    baseline = plan_buy_notional(requested_notional_krw=100_000.0, reference_price=123_456_789.0, rules=_rules())
    mutated = plan_buy_notional(requested_notional_krw=100_000.0, reference_price=123_456_789.0, rules=_rules(max_qty_decimals=4))

    assert baseline.quantity_contract_hash != mutated.quantity_contract_hash


def test_h74_research_planner_and_live_target_delta_emit_same_quantity_contract_hash() -> None:
    reference_price = 123_456_789.0
    research_bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=1_000_000.0,
        buy_fraction=0.5,
        sellable_qty=0.0,
        reference_price=reference_price,
        policy_decision=_typed_decision(raw_signal="BUY", final_signal="BUY"),
        candle_ts=1_704_046_800_000,
    )
    assert research_bundle.submit_plan is not None
    research_plan = research_bundle.submit_plan.as_dict()
    live_sizing = build_target_delta_execution_sizing(
        pair="KRW-BTC",
        side="BUY",
        desired_qty=float(research_plan["notional_krw"]) / reference_price,
        market_price=reference_price,
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        max_qty_decimals=8,
        authority_source="target_delta.desired_delta",
    )

    assert research_plan["quantity_contract_hash"] == live_sizing.quantity_contract_hash
    assert research_plan["qty"] == live_sizing.final_submitted_qty


def test_h74_live_rehearsal_uses_submit_plan_quantity_contract_hash(tmp_path) -> None:
    from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal
    from tests.test_h74_live_rehearsal import _source_artifact

    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path))
    )
    plan = payload["would_submit_plan"]
    target_sizing = plan["target_sizing"]

    assert payload["quantity_contract_hash"] == target_sizing["quantity_contract_hash"]
    assert payload["quantity_contract_hash"] != payload["order_rule_snapshot_hash"]
