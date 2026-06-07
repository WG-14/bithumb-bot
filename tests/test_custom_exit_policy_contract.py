from __future__ import annotations

import inspect
from dataclasses import replace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.research.promotion_gate import build_candidate_profile
from bithumb_bot.research.strategy_registry import StrategyRuntimeCapabilities, reload_research_strategy_plugins_for_tests
from bithumb_bot.research.strategy_spec import COMMON_EXIT_RULE_NAMES, StrategySpecError, _validate_common_exit_rule_names
from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec
from tests.fixtures.custom_exit_strategy_plugin import (
    CUSTOM_EXIT_PLUGIN,
    custom_exit_policy_materializer,
)


@pytest.fixture(autouse=True)
def _custom_exit_runtime_plugin() -> None:
    reload_research_strategy_plugins_for_tests(providers=(_runtime_custom_exit_provider,))
    yield
    reload_research_strategy_plugins_for_tests()


def _runtime_custom_exit_provider():
    return (
        replace(
            CUSTOM_EXIT_PLUGIN,
            runtime_replay_builder=lambda *_args, **_kwargs: None,
            runtime_decision_adapter_factory=lambda: object(),
            policy_assembly_factory=lambda: object(),
            runtime_capabilities=StrategyRuntimeCapabilities(
                promotion_runtime_decisions_supported=True,
                runtime_replay_supported=True,
                live_dry_run_allowed=False,
                live_real_order_allowed=False,
                approved_profile_required=False,
                fail_closed_reason="custom_exit_canary_runtime_test",
            ),
            authoring_contract_kind="promotion_grade",
            promotion_extension_payload={"schema_version": 1, "test_only": True},
        ),
    )


def _settings(**overrides):
    values = dict(
        MODE="paper",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_NAME="custom_exit_canary",
        STRATEGY_PARAMETERS_JSON="",
        RUNTIME_STRATEGY_SET_JSON="",
        ACTIVE_STRATEGIES="",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
    )
    values.update(overrides)
    return replace(
        settings,
        **values,
    )


def test_custom_exit_materializer_changes_policy_hash_when_threshold_changes() -> None:
    left = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.03})
    right = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.05})

    assert left["exit_policy_hash"] != right["exit_policy_hash"]
    assert left["exit_policy_config_hash"] != right["exit_policy_config_hash"]


def test_custom_exit_profile_preserves_exit_policy_hash() -> None:
    materialized = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.03})
    profile = build_candidate_profile(
        {
            "strategy_name": "custom_exit_canary",
            "parameter_values": {"TRAILING_STOP_RATIO": 0.03},
            "parameter_values_raw": {"TRAILING_STOP_RATIO": 0.03},
            **materialized,
        }
    )

    assert profile["exit_policy_hash"] == materialized["exit_policy_hash"]
    assert profile["exit_policy_config_hash"] == materialized["exit_policy_config_hash"]


def test_custom_exit_runtime_request_contains_exit_policy_hashes() -> None:
    request = RuntimeDecisionRequestBuilder(settings_obj=_settings()).build_for_spec(
        RuntimeStrategySpec(
            strategy_name="custom_exit_canary",
            pair="KRW-BTC",
            interval="1m",
            parameters={"TRAILING_STOP_RATIO": 0.03},
        ),
        through_ts_ms=1,
    )

    fields = request.observability_fields()
    assert fields["exit_policy_hash"] == request.exit_policy_hash
    assert fields["exit_policy_config_hash"] == request.exit_policy_config_hash
    assert fields["exit_policy_contract_hash"] == request.exit_policy_contract_hash


def test_custom_exit_live_like_contract_preserves_exit_policy_hashes(monkeypatch: pytest.MonkeyPatch) -> None:
    from bithumb_bot import runtime_strategy_set

    materialized = custom_exit_policy_materializer("custom_exit_canary", {"TRAILING_STOP_RATIO": 0.03})
    profile = build_candidate_profile(
        {
            "strategy_name": "custom_exit_canary",
            "parameter_values": {"TRAILING_STOP_RATIO": 0.03},
            "parameter_values_raw": {"TRAILING_STOP_RATIO": 0.03},
            **materialized,
        }
    )
    profile["profile_mode"] = "live_dry_run"
    profile["profile_content_hash"] = "sha256:custom-exit-approved"
    profile["strategy_parameters"] = {"TRAILING_STOP_RATIO": 0.03}
    profile["risk_policy"] = {
        "policy_status": "disabled_explicit",
        "missing_policy": "fail_closed_for_live",
        "source": "unit_approved_profile",
    }
    profile["risk_enforcement_mode"] = "telemetry"
    profile["missing_risk_policy_behavior"] = "fail_closed_for_live"

    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda _path: dict(profile))
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        runtime_strategy_set,
        "runtime_contract_from_settings",
        lambda cfg: {
            "schema_version": 1,
            "mode": cfg.MODE,
                "strategy_name": "custom_exit_canary",
                "market": cfg.PAIR,
                "interval": cfg.INTERVAL,
                "live_dry_run": True,
                "live_real_order_armed": False,
                "strategy_parameters": {"TRAILING_STOP_RATIO": 0.03},
            },
        )

    request = RuntimeDecisionRequestBuilder(
        settings_obj=_settings(MODE="live", LIVE_DRY_RUN=True, LIVE_REAL_ORDER_ARMED=False),
        require_spec_bound_approved_profile=True,
    ).build_for_spec(
        RuntimeStrategySpec(
            strategy_name="custom_exit_canary",
            pair="KRW-BTC",
            interval="1m",
            parameters={"TRAILING_STOP_RATIO": 0.03},
            approved_profile_path="/tmp/custom-exit-approved.json",
            approved_profile_hash="sha256:custom-exit-approved",
        ),
        through_ts_ms=1,
    )

    fields = request.observability_fields()
    for field in (
        "exit_policy_hash",
        "exit_policy_config_hash",
        "exit_policy_contract_hash",
    ):
        assert profile[field] == materialized[field]
        assert fields[field] == materialized[field]
        assert getattr(request, field) == materialized[field]


def test_custom_exit_rule_without_materializer_fails_closed() -> None:
    with pytest.raises(StrategySpecError, match="unsupported rule"):
        _validate_common_exit_rule_names("trailing_stop")


def test_custom_exit_does_not_modify_common_exit_rule_whitelist() -> None:
    assert "trailing_stop" not in COMMON_EXIT_RULE_NAMES
    source = inspect.getsource(_validate_common_exit_rule_names)
    assert "trailing_stop" not in source
