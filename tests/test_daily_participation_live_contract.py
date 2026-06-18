from __future__ import annotations

from dataclasses import replace

from bithumb_bot.config import LiveModeValidationError, settings, validate_live_strategy_selection
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin, strategy_runtime_capability_issues
from bithumb_bot.strategy_plugin_inventory import build_strategy_target_verdict


def test_daily_participation_sma_is_level_3_promotion_grade() -> None:
    plugin = resolve_research_strategy_plugin("daily_participation_sma")
    payload = plugin.contract_payload()

    assert payload["authoring_level"] == "level_3_promotion_grade"
    assert payload["runtime_decision_supported"] is True
    assert payload["live_dry_run_allowed"] is True
    assert payload["approved_profile_required"] is True


def test_daily_participation_sma_runtime_decision_target_allowed() -> None:
    verdict = build_strategy_target_verdict("daily_participation_sma", "runtime_decision")

    assert verdict["allowed"] is True


def test_daily_participation_sma_live_dry_run_target_allowed() -> None:
    verdict = build_strategy_target_verdict("daily_participation_sma", "live_dry_run")

    assert verdict["allowed"] is False
    assert any("approved_profile_required_for_strategy:daily_participation_sma" in item for item in verdict["blocking_reasons"])
    assert verdict["capability_level"] == "live_eligible"


def test_daily_participation_sma_live_real_order_requires_approved_profile() -> None:
    issues = strategy_runtime_capability_issues(
        "daily_participation_sma",
        live_dry_run=True,
        live_real_order_armed=True,
        approved_profile_path="",
        require_promotion_runtime=True,
        require_runtime_replay=True,
        require_runtime_decision_adapter=True,
    )

    assert "approved_profile_required_for_strategy:daily_participation_sma" in issues


def test_live_strategy_selection_blocks_without_approved_profile() -> None:
    cfg = replace(
        settings,
        MODE="live",
        STRATEGY_NAME="daily_participation_sma",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        APPROVED_STRATEGY_PROFILE_PATH="",
    )

    try:
        validate_live_strategy_selection(cfg)
    except LiveModeValidationError as exc:
        assert "approved_profile_required_for_strategy:daily_participation_sma" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected approved profile fail-closed gate")
