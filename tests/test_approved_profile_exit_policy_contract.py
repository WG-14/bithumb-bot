from __future__ import annotations

import pytest

from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.promotion_gate import PromotionGateError, build_candidate_profile
from bithumb_bot.research.strategy_registry import reload_research_strategy_plugins_for_tests
from tests.fixtures.custom_exit_strategy_plugin import custom_exit_policy_materializer, custom_exit_provider


@pytest.fixture(autouse=True)
def _restore_plugins() -> None:
    reload_research_strategy_plugins_for_tests(providers=(custom_exit_provider,))
    yield
    reload_research_strategy_plugins_for_tests()


def _candidate(*, ratio: float = 0.03) -> dict[str, object]:
    return {
        "strategy_name": "custom_exit_canary",
        "parameter_values": {"TRAILING_STOP_RATIO": ratio},
        "parameter_values_raw": {"TRAILING_STOP_RATIO": ratio},
    }


def test_build_candidate_profile_preserves_custom_exit_policy_without_stop_loss_key() -> None:
    materialized = custom_exit_policy_materializer(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )
    candidate = {
        **_candidate(),
        **materialized,
    }

    profile = build_candidate_profile(candidate)

    assert profile["exit_policy"]["rules"] == ["trailing_stop"]
    assert "stop_loss" not in profile["exit_policy"]
    assert profile["exit_policy_hash"] == materialized["exit_policy_hash"]
    assert profile["exit_policy_source"] == "custom_exit_canary_materializer"


def test_build_candidate_profile_rejects_custom_exit_policy_hash_mismatch() -> None:
    materialized = custom_exit_policy_materializer(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )
    candidate = {
        **_candidate(),
        **materialized,
        "exit_policy_hash": sha256_prefixed({"wrong": True}),
    }

    with pytest.raises(PromotionGateError, match="candidate_exit_policy_hash_mismatch"):
        build_candidate_profile(candidate)


def test_build_candidate_profile_uses_plugin_exit_policy_materializer_when_candidate_policy_missing() -> None:
    profile = build_candidate_profile(_candidate(ratio=0.05))

    assert profile["exit_policy"]["rules"] == ["trailing_stop"]
    assert profile["exit_policy"]["trailing_stop"]["trailing_stop_ratio"] == 0.05
    assert profile["exit_policy_hash"].startswith("sha256:")
    assert profile["exit_policy_config_hash"].startswith("sha256:")


def test_legacy_stop_loss_schema_migration_does_not_run_for_custom_policy_contract() -> None:
    materialized = custom_exit_policy_materializer(
        "custom_exit_canary",
        {"TRAILING_STOP_RATIO": 0.03},
    )
    profile = build_candidate_profile({**_candidate(), **materialized})

    assert profile["exit_policy"]["strategy_rules"] == ["trailing_stop"]
    assert profile["exit_policy_materialization_mode"] == "test_materializer"
