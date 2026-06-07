from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.core.sma_policy import (
    ExitExecutionIntent,
    PositionSnapshot,
    StrategyDecisionV2,
)
from bithumb_bot.research.execution_planning import _execution_plan_evidence, _research_execution_plan_bundle


def _typed_sell_decision() -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name="sma_with_filter",
        raw_signal="SELL",
        raw_reason="typed_raw_sell",
        entry_signal="HOLD",
        entry_reason="position_has_executable_exposure",
        exit_signal="SELL",
        exit_reason="typed_exit",
        final_signal="SELL",
        final_reason="typed_final_sell",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule="unit_exit",
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(
            in_position=True,
            entry_allowed=False,
            exit_allowed=True,
            entry_block_reason="position_has_executable_exposure",
            exit_block_reason="none",
            terminal_state="research_simulated_open_exposure",
            qty_open=0.25,
            raw_qty_open=0.25,
            raw_total_asset_qty=0.25,
            open_lot_count=2500,
            sellable_executable_lot_count=2500,
            dust_state="no_dust",
            effective_flat=False,
            has_executable_exposure=True,
            has_any_position_residue=True,
        ),
        execution_intent=ExitExecutionIntent(
            side="SELL",
            intent="exit",
            pair="KRW-BTC",
            requires_execution_sizing=True,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={},
        policy_hash="sha256:pure",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )


def test_research_exploratory_allows_legacy_event_first_diagnostic_fallback() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=1_000_000.0,
        buy_fraction=0.5,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
        allow_compatibility_fallback=True,
        promotion_grade_required=False,
    )
    evidence = _execution_plan_evidence(bundle)

    assert evidence["compatibility_fallback"] is True
    assert evidence["promotion_grade"] is False
    assert evidence["artifact_grade"] == "diagnostic_only"
    assert evidence["recommended_next_action"] == "regenerate_research_decisions_with_typed_execution_submit_plan"


def test_promotion_mode_rejects_execution_compatibility_fallback() -> None:
    bundle = _research_execution_plan_bundle(
        side="BUY",
        cash=1_000_000.0,
        buy_fraction=0.5,
        sellable_qty=0.0,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
        allow_compatibility_fallback=True,
        promotion_grade_required=True,
    )

    assert bundle.submit_plan is None
    assert bundle.compatibility_fallback is False
    assert bundle.promotion_grade is False
    assert bundle.reason_code == "promotion_requires_typed_execution_submit_plan"


def test_promotion_mode_rejects_typed_sell_compatibility_submit_plan() -> None:
    with pytest.raises(ValueError, match="research_submit_plan_missing|research_typed_submit_plan_missing"):
        _research_execution_plan_bundle(
            side="SELL",
            cash=1_000_000.0,
            buy_fraction=0.5,
            sellable_qty=0.25,
            reference_price=10.0,
            policy_decision=_typed_sell_decision(),
            candle_ts=123,
            allow_compatibility_fallback=True,
            promotion_grade_required=True,
        )

    diagnostic_bundle = _research_execution_plan_bundle(
        side="SELL",
        cash=1_000_000.0,
        buy_fraction=0.5,
        sellable_qty=0.25,
        reference_price=10.0,
        policy_decision=_typed_sell_decision(),
        candle_ts=123,
        allow_compatibility_fallback=True,
        promotion_grade_required=False,
    )
    evidence = _execution_plan_evidence(diagnostic_bundle)

    assert diagnostic_bundle.submit_plan is None
    assert diagnostic_bundle.compatibility_fallback is False
    assert diagnostic_bundle.promotion_grade is False
    assert evidence["execution_plan_status"] == "BLOCKED"
    assert evidence["compatibility_fallback"] is False
    assert evidence["promotion_grade"] is False
    assert evidence["artifact_grade"] == "diagnostic_only"
    assert evidence["promotion_rejection_reason"]
    assert evidence["recommended_next_action"] == "regenerate_research_decisions_with_typed_execution_submit_plan"


def test_promotion_mode_rejects_missing_strategy_decision_v2() -> None:
    source = Path("src/bithumb_bot/research/strategy_evaluator_stage.py").read_text(encoding="utf-8")

    assert "if promotion_grade_policy_required and policy_decision is None" in source
    assert "research_policy_decision_missing_not_comparable" in source


def test_promotion_mode_rejects_strategy_decision_missing_policy_hashes() -> None:
    source = Path("src/bithumb_bot/research/strategy_evaluator_stage.py").read_text(encoding="utf-8")

    for field in (
        "policy_hash",
        "policy_contract_hash",
        "policy_input_hash",
        "policy_decision_hash",
    ):
        assert field in source
    assert "research_strategy_decision_promotion_fields_missing" in source


def test_research_exploratory_fallback_sets_recommended_next_action() -> None:
    bundle = _research_execution_plan_bundle(
        side="SELL",
        cash=1_000_000.0,
        buy_fraction=0.5,
        sellable_qty=0.25,
        reference_price=10.0,
        policy_decision=None,
        candle_ts=123,
        allow_compatibility_fallback=True,
        promotion_grade_required=False,
    )

    assert bundle.promotion_grade is False
    assert bundle.recommended_next_action != "none"
