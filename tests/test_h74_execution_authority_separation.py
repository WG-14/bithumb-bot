from __future__ import annotations

import json
import inspect
from types import SimpleNamespace

import pytest

from bithumb_bot.broker import live
from bithumb_bot.broker import order_rules
from bithumb_bot.execution_models import OrderIntent
from bithumb_bot.execution_planner import build_submit_plan
from bithumb_bot.execution_service import ExecutionSubmitPlan, H74SubmitSemantics
from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal
from bithumb_bot.submit_authority_policy import evaluate_submit_authority_policy


pytestmark = pytest.mark.fast_regression


def _rules() -> order_rules.DerivedOrderConstraints:
    return order_rules.DerivedOrderConstraints(
        order_types=("limit", "price", "market"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        bid_min_total_krw=5000.0,
        ask_min_total_krw=5000.0,
        min_notional_krw=5000.0,
        min_qty=0.0001,
        qty_step=0.0001,
        max_qty_decimals=8,
        bid_price_unit=1.0,
        ask_price_unit=1.0,
    )


def _source_artifact(tmp_path) -> str:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {"fee_rate": 0.0004, "slippage_bps": 10},
                "candle_timing": "closed_candle_kst",
                "behavior_contract": {
                    "position_mode": "fixed_fill_qty_until_exit",
                    "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
                    "residual_inventory_mode": "terminal_dust_reported_not_reused_without_authority",
                    "initial_position_policy": "flat_start_required",
                    "partial_fill_policy": "accumulate_cycle_acquired_qty",
                    "fee_application_policy": "repository_observed_fee_fields",
                },
                "entry_submit_semantics": {
                    "schema_version": 1,
                    "entry_order_type": "price",
                    "entry_submit_field": "price",
                    "entry_quote_notional_krw": 100_000,
                    "entry_volume_forbidden": True,
                    "entry_qty_preview_authoritative": False,
                    "entry_fill_qty_authority": "broker_fills",
                },
            }
        ),
        encoding="utf-8",
    )
    return str(source)


def test_h74_source_observation_buy_uses_h74_quote_notional_authority(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))
    plan = payload["would_submit_plan"]

    assert plan["source"] == "h74_source_observation"
    assert plan["authority"] == "h74_fixed_fill_quote_notional_buy"
    assert plan["submit_semantics_authority"] == "h74_fixed_fill_quote_notional_buy"
    assert plan["submit_semantics"] == "quote_notional_market_buy"


def test_general_target_delta_buy_keeps_canonical_target_delta_authority() -> None:
    rules = _rules()
    plan = build_submit_plan(
        intent=OrderIntent(
            client_order_id="general-target-delta",
            market="KRW-BTC",
            side="BUY",
            normalized_side="bid",
            qty=100_000.0 / 100_000_120.0,
            price=None,
            created_ts=1,
            submit_contract=order_rules.build_buy_price_none_submit_contract(
                rules=rules,
                resolution=order_rules.resolve_buy_price_none_resolution(rules=rules),
            ),
            market_price_hint=100_000_120.0,
        ),
        rules=rules,
        fetch_order_rules=lambda _market: type("Resolution", (), {"rules": rules})(),
        fetch_top_of_book=lambda _market: None,
        resolve_best_ask=lambda _quote, _market: 100_000_120.0,
        truncate_volume=lambda qty: qty,
    )

    assert plan.exchange_constrained_qty == pytest.approx(0.0009)
    assert plan.exchange_submit_notional_krw == pytest.approx(90_000.0)
    assert plan.submit_qty_authority == "submit_plan.exchange_constraints"
    assert plan.submit_semantics_authority is None


def test_h74_source_observation_rejects_canonical_target_delta_only_authority(tmp_path, monkeypatch) -> None:
    from bithumb_bot import h74_live_rehearsal

    original = h74_live_rehearsal.evaluate_submit_authority_policy

    def _mutating_policy(plan, *args, **kwargs):
        plan["authority"] = "canonical_target_delta_sizing"
        plan["submit_semantics_authority"] = "canonical_target_delta_sizing"
        return original(plan, *args, **kwargs)

    monkeypatch.setattr(h74_live_rehearsal, "evaluate_submit_authority_policy", _mutating_policy)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["would_submit"] is False
    assert payload["primary_block_gate"] == "submit_semantics"
    assert "h74_quote_notional_authority_missing" in payload["primary_block_reason"]


def test_broker_does_not_branch_on_h74_strategy_name() -> None:
    source = inspect.getsource(live)

    assert 'strategy_name == "daily_participation_sma"' not in source
    assert "strategy_name == 'daily_participation_sma'" not in source


def test_execution_submit_plan_h74_semantics_are_typed_fields_not_extra_payload() -> None:
    plan = ExecutionSubmitPlan(
        side="BUY",
        source="h74_source_observation",
        authority="h74_fixed_fill_quote_notional_buy",
        final_action="REBALANCE_TO_TARGET",
        qty=0.0009,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="h74-plan",
        h74_submit_semantics=H74SubmitSemantics(
            sizing_mode="quote_notional",
            quote_notional_krw=100_000.0,
            submit_semantics="quote_notional_market_buy",
            fill_qty_authority="broker_fill",
            position_mode="fixed_fill_qty_until_exit",
            exchange_order_type="price",
            exchange_submit_field="price",
            exchange_submit_notional_krw=100_000.0,
            exchange_submit_qty=None,
            quote_notional_authority="h74_fixed_fill_quote_notional_buy",
            submit_semantics_authority="h74_fixed_fill_quote_notional_buy",
        ),
        extra_payload={"strategy_name": "daily_participation_sma"},
    )

    payload = plan.as_dict()

    assert plan.h74_submit_semantics is not None
    assert payload["submit_semantics"] == "quote_notional_market_buy"
    assert payload["sizing_mode"] == "quote_notional"
    assert payload["quote_notional_krw"] == pytest.approx(100_000.0)
    assert payload["fill_qty_authority"] == "broker_fill"
    assert "submit_semantics" not in plan.extra_payload
    with pytest.raises(ValueError, match="reserved_h74_semantics"):
        ExecutionSubmitPlan(
            side="BUY",
            source="h74_source_observation",
            authority="h74_fixed_fill_quote_notional_buy",
            final_action="REBALANCE_TO_TARGET",
            qty=0.0009,
            notional_krw=100_000.0,
            target_exposure_krw=100_000.0,
            current_effective_exposure_krw=0.0,
            delta_krw=100_000.0,
            submit_expected=True,
            pre_submit_proof_status="passed",
            block_reason="none",
            idempotency_key="h74-plan",
            extra_payload={"submit_semantics": "quote_notional_market_buy"},
        )


def test_submit_authority_rejects_h74_source_without_typed_semantics() -> None:
    decision = evaluate_submit_authority_policy(
        {
            "side": "BUY",
            "source": "h74_source_observation",
            "authority": "h74_fixed_fill_quote_notional_buy",
            "submit_expected": True,
            "pre_submit_proof_status": "passed",
            "portfolio_target_authoritative": True,
            "portfolio_target_hash": "sha256:portfolio",
            "allocation_decision_hash": "sha256:allocation",
            "strategy_contribution_hash": "sha256:contribution",
        },
        settings_obj=SimpleNamespace(
            MODE="live",
            LIVE_DRY_RUN=False,
            LIVE_REAL_ORDER_ARMED=True,
            EXECUTION_ENGINE="target_delta",
            TARGET_DELTA_LIVE_REAL_ORDER_ENABLED=True,
            H74_SOURCE_OBSERVATION_AUTHORITY_PATH="/runtime/h74-authority.json",
        ),
        plan_kind="target",
        require_final_payload=False,
    )

    assert decision.allowed is False
    assert decision.reason == "h74_source_observation_submit_semantics_missing"


def test_submit_authority_rejects_h74_source_when_typed_fields_missing_even_if_extra_payload_has_strings() -> None:
    decision = evaluate_submit_authority_policy(
        {
            "side": "BUY",
            "source": "h74_source_observation",
            "authority": "h74_fixed_fill_quote_notional_buy",
            "submit_expected": True,
            "pre_submit_proof_status": "passed",
            "portfolio_target_authoritative": True,
            "portfolio_target_hash": "sha256:portfolio",
            "allocation_decision_hash": "sha256:allocation",
            "strategy_contribution_hash": "sha256:contribution",
            "extra_payload": {
                "submit_semantics": "quote_notional_market_buy",
                "sizing_mode": "quote_notional",
                "quote_notional_krw": 100_000.0,
                "fill_qty_authority": "broker_fill",
                "position_mode": "fixed_fill_qty_until_exit",
            },
        },
        settings_obj=SimpleNamespace(
            MODE="live",
            LIVE_DRY_RUN=False,
            LIVE_REAL_ORDER_ARMED=True,
            EXECUTION_ENGINE="target_delta",
            TARGET_DELTA_LIVE_REAL_ORDER_ENABLED=True,
            H74_SOURCE_OBSERVATION_AUTHORITY_PATH="/runtime/h74-authority.json",
        ),
        plan_kind="target",
        require_final_payload=False,
    )

    assert decision.allowed is False
    assert decision.reason == "h74_source_observation_submit_semantics_missing"
