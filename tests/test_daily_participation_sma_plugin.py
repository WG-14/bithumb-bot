from __future__ import annotations

import pytest

from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin
from bithumb_bot.research.strategy_spec import SMA_WITH_FILTER_SPEC, strategy_spec_for_name
from bithumb_bot.strategy_plugins.builtin_manifest import iter_builtin_strategy_plugins_from_manifest
from bithumb_bot.strategy_plugins.daily_participation_sma import DAILY_PARTICIPATION_SMA_PLUGIN, DAILY_PARTICIPATION_SMA_SPEC


def _manifest(strategy_name: str = "daily_participation_sma") -> dict[str, object]:
    params = {
        "SMA_SHORT": [2],
        "SMA_LONG": [4],
        "DAILY_PARTICIPATION_ENABLED": [True],
        "DAILY_PARTICIPATION_TIMEZONE": ["Asia/Seoul"],
        "DAILY_PARTICIPATION_COUNT_BASIS": ["filled"],
        "DAILY_PARTICIPATION_WINDOW_START_HOUR_KST": [0],
        "DAILY_PARTICIPATION_WINDOW_END_HOUR_KST": [24],
        "DAILY_PARTICIPATION_BUY_FRACTION": [0.05],
        "DAILY_PARTICIPATION_MAX_ORDER_KRW": [10000.0],
    }
    return {
        "experiment_id": "daily_participation_sma_contract",
        "hypothesis": "Daily participation composes SMA without mutating it.",
        "strategy_name": strategy_name,
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "candles_v1",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
        },
        "parameter_space": params,
        "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 50,
            "min_profit_factor": 1.0,
            "oos_return_must_be_positive": True,
            "parameter_stability_required": False,
        },
    }


def test_daily_participation_sma_registered_in_builtin_manifest() -> None:
    plugins = {
        getattr(plugin, "name", getattr(plugin, "strategy_name", "")): plugin
        for plugin in iter_builtin_strategy_plugins_from_manifest()
    }

    assert "daily_participation_sma" in plugins
    assert resolve_research_strategy_plugin("daily_participation_sma").name == "daily_participation_sma"


def test_daily_participation_sma_has_own_strategy_spec() -> None:
    assert DAILY_PARTICIPATION_SMA_SPEC.strategy_name == "daily_participation_sma"
    assert DAILY_PARTICIPATION_SMA_SPEC is not SMA_WITH_FILTER_SPEC
    assert strategy_spec_for_name("daily_participation_sma") is DAILY_PARTICIPATION_SMA_SPEC


def test_daily_participation_sma_is_promotion_grade_live_eligible() -> None:
    plugin = resolve_research_strategy_plugin("daily_participation_sma")

    assert plugin.runtime_capabilities is not None
    assert plugin.contract_payload()["authoring_level"] == "level_3_promotion_grade"
    assert plugin.runtime_capabilities.live_dry_run_allowed is True
    assert plugin.runtime_capabilities.live_real_order_allowed is True
    assert plugin.runtime_capabilities.approved_profile_required is True
    assert plugin.runtime_capabilities.promotion_runtime_decisions_supported is True
    assert plugin.runtime_capabilities.runtime_replay_supported is True
    assert plugin.runtime_decision_adapter_factory is not None
    assert plugin.policy_assembly_factory is not None


def test_base_sma_spec_is_not_mutated() -> None:
    assert not any(name.startswith("DAILY_") for name in SMA_WITH_FILTER_SPEC.accepted_parameter_names)


def test_daily_participation_sma_accepts_daily_parameters() -> None:
    manifest = parse_manifest(_manifest())
    assert manifest.strategy_name == "daily_participation_sma"


def test_daily_participation_sma_has_research_policy_decision_builder() -> None:
    assert DAILY_PARTICIPATION_SMA_PLUGIN.research_policy_decision_builder is not None


def test_daily_participation_sma_runtime_capability_matches_declared_scope() -> None:
    plugin = resolve_research_strategy_plugin("daily_participation_sma")

    assert plugin.runtime_capabilities.runtime_replay_supported is True
    assert plugin.runtime_capabilities.runtime_decision_supported is True
    assert plugin.runtime_capabilities.live_dry_run_allowed is True
    assert plugin.runtime_capabilities.live_real_order_allowed is True
    assert plugin.runtime_capabilities.fail_closed_reason == "daily_participation_sma_capability_missing"


def test_sma_with_filter_still_rejects_daily_parameters() -> None:
    payload = _manifest("sma_with_filter")
    with pytest.raises(ManifestValidationError, match="unknown strategy parameter"):
        parse_manifest(payload)


def test_daily_participation_sma_rejects_gate_count_basis_mismatch() -> None:
    payload = _manifest()
    payload["acceptance_gate"]["participation_count_basis"] = "intent"  # type: ignore[index]

    with pytest.raises(ManifestValidationError, match="participation_count_basis conflicts"):
        parse_manifest(payload)
