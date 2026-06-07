from __future__ import annotations

import json
from dataclasses import replace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.runtime_strategy_decision import get_runtime_decision_adapter, legacy_db_strategy_fallback_allowed
from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec


def test_live_rejects_strategy_parameters_json_fallback() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_PARAMETERS_JSON=json.dumps(
            {
                "CANARY_ORDER_START_INDEX": 0,
                "CANARY_ORDER_SIDE": "BUY",
                "CANARY_ORDER_REASON": "unit",
            }
        ),
    )

    with pytest.raises(RuntimeError, match="strict_runtime_rejects_strategy_parameters_json_fallback"):
        RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
            RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
            through_ts_ms=1,
        )


def test_live_rejects_plugin_from_settings_fallback() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_PARAMETERS_JSON="",
    )

    with pytest.raises(RuntimeError, match="strict_runtime_rejects_plugin_from_settings_fallback"):
        RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
            RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
            through_ts_ms=1,
        )


def test_live_multi_strategy_rejects_global_profile_fallback() -> None:
    cfg = replace(settings, MODE="live", LIVE_DRY_RUN=True, LIVE_REAL_ORDER_ARMED=False)

    with pytest.raises(RuntimeError, match="spec_bound_approved_profile_path_missing_for_runtime_strategy"):
        RuntimeDecisionRequestBuilder(
            settings_obj=cfg,
            require_spec_bound_approved_profile=True,
        ).build_for_spec(
            RuntimeStrategySpec(
                "canary_non_sma",
                pair="KRW-BTC",
                interval="1m",
                approved_profile_hash="sha256:declared",
            ),
            through_ts_ms=1,
        )


def test_promotion_adapter_with_db_bound_decide_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    from bithumb_bot import runtime_strategy_decision
    from bithumb_bot.research import strategy_registry

    class _DbBoundAdapter:
        strategy_name = "db_bound_unit"

        def decide_feature_snapshot(self, request, feature_snapshot):  # noqa: ANN001
            return None

        def decide(self, conn, request):  # noqa: ANN001
            return None

        def typed_authority_required(self) -> bool:
            return True

    plugin = replace(
        strategy_registry.resolve_research_strategy_plugin("canary_non_sma"),
        name="db_bound_unit",
        runtime_decision_adapter_factory=lambda: _DbBoundAdapter(),
    )
    monkeypatch.setattr(strategy_registry, "resolve_research_strategy_plugin", lambda _name: plugin)
    runtime_strategy_decision._DERIVED_RUNTIME_DECISION_ADAPTER_CACHE.clear()

    with pytest.raises(RuntimeError, match="promotion_runtime_adapter_db_bound_decide_forbidden"):
        get_runtime_decision_adapter("db_bound_unit")


def test_production_path_permits_only_profile_or_runtime_spec_parameter_source() -> None:
    cfg = replace(settings, MODE="paper", APPROVED_STRATEGY_PROFILE_PATH="", STRATEGY_APPROVED_PROFILE_PATH="")
    request = RuntimeDecisionRequestBuilder(settings_obj=cfg, authority_scope="promotion").build_for_spec(
        RuntimeStrategySpec(
            "canary_non_sma",
            pair="KRW-BTC",
            interval="1m",
            parameters={
                "CANARY_ORDER_START_INDEX": 0,
                "CANARY_ORDER_SIDE": "BUY",
                "CANARY_ORDER_REASON": "unit",
            },
        ),
        through_ts_ms=1,
    )

    assert request.parameter_source == "runtime_strategy_spec"
    assert legacy_db_strategy_fallback_allowed(selected_strategy_name="canary_non_sma") is False
