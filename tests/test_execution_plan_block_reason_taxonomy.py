from __future__ import annotations

from bithumb_bot.runtime.lifecycle_artifacts import final_action_for_primary_block_gate


def test_strategy_risk_block_maps_to_block_strategy_risk() -> None:
    assert final_action_for_primary_block_gate("strategy_risk") == "BLOCK_STRATEGY_RISK"


def test_target_authority_block_remains_target_authority() -> None:
    assert final_action_for_primary_block_gate("target_authority") == "BLOCK_TARGET_AUTHORITY"


def test_submit_authority_block_remains_submit_authority() -> None:
    assert final_action_for_primary_block_gate("submit_authority") == "BLOCK_SUBMIT_AUTHORITY"
