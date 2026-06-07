from __future__ import annotations

from typing import Any

from bithumb_bot.research.backtest_types import BacktestRun
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.strategy_registry import (
    ResearchStrategyPlugin,
    StrategyRuntimeCapabilities,
)
from bithumb_bot.research.strategy_spec import (
    StrategyParameterSchema,
    StrategySpec,
)


CUSTOM_EXIT_STRATEGY_NAME = "custom_exit_canary"


CUSTOM_EXIT_SPEC = StrategySpec(
    strategy_name=CUSTOM_EXIT_STRATEGY_NAME,
    strategy_version="custom_exit_canary.test_contract.v1",
    accepted_parameter_names=("TRAILING_STOP_RATIO",),
    required_parameter_names=("TRAILING_STOP_RATIO",),
    behavior_affecting_parameter_names=("TRAILING_STOP_RATIO",),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={},
    decision_contract_version="custom_exit_canary_decision_contract.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": ("trailing_stop",),
        "trailing_stop": {"unit": "unrealized_pnl_ratio"},
    },
    parameter_schema=(
        StrategyParameterSchema(
            "TRAILING_STOP_RATIO",
            "float",
            required=True,
            min_value=0.0,
            unit="unrealized_pnl_ratio",
        ),
    ),
)


def run_custom_exit_backtest(*_args: Any, **_kwargs: Any) -> BacktestRun:
    raise RuntimeError("custom_exit_canary_backtest_not_used_by_contract_tests")


def build_custom_exit_events(**_kwargs: Any) -> tuple[object, ...]:
    return ()


def custom_exit_policy_materializer(
    strategy_name: str,
    parameter_values: dict[str, Any],
) -> dict[str, object]:
    if strategy_name != CUSTOM_EXIT_STRATEGY_NAME:
        raise ValueError(f"custom_exit_strategy_mismatch:{strategy_name}")
    ratio = float(parameter_values["TRAILING_STOP_RATIO"])
    policy = {
        "schema_version": 1,
        "strategy_name": CUSTOM_EXIT_STRATEGY_NAME,
        "rules": ["trailing_stop"],
        "common_rules": [],
        "strategy_rules": ["trailing_stop"],
        "trailing_stop": {
            "enabled": ratio > 0.0,
            "trailing_stop_ratio": ratio,
            "evaluation_price_basis": "closed_candle_mark",
        },
    }
    config = {
        "schema_version": 1,
        "strategy_name": CUSTOM_EXIT_STRATEGY_NAME,
        "rules": ["trailing_stop"],
        "trailing_stop_ratio": ratio,
    }
    return {
        "exit_policy": policy,
        "exit_policy_hash": sha256_prefixed(policy),
        "exit_policy_contract_hash": sha256_prefixed(
            {
                "schema_version": 1,
                "strategy_name": CUSTOM_EXIT_STRATEGY_NAME,
                "materializer": "tests.fixtures.custom_exit_strategy_plugin.custom_exit_policy_materializer",
            }
        ),
        "exit_policy_config": config,
        "exit_policy_config_hash": sha256_prefixed(config),
        "exit_policy_source": "custom_exit_canary_materializer",
        "exit_policy_materialization_mode": "test_materializer",
    }


CUSTOM_EXIT_PLUGIN = ResearchStrategyPlugin(
    name=CUSTOM_EXIT_STRATEGY_NAME,
    version=CUSTOM_EXIT_SPEC.strategy_version,
    spec=CUSTOM_EXIT_SPEC,
    required_data=CUSTOM_EXIT_SPEC.required_data,
    optional_data=CUSTOM_EXIT_SPEC.optional_data,
    runner=run_custom_exit_backtest,
    research_event_builder=build_custom_exit_events,
    runtime_replay_builder=None,
    runtime_parameter_adapter=None,
    decision_contract_version=CUSTOM_EXIT_SPEC.decision_contract_version,
    diagnostics_namespace=CUSTOM_EXIT_STRATEGY_NAME,
    exit_policy_materializer=custom_exit_policy_materializer,
    runtime_capabilities=StrategyRuntimeCapabilities(
        promotion_runtime_decisions_supported=False,
        runtime_replay_supported=False,
        research_only=True,
        baseline_only=False,
        live_dry_run_allowed=False,
        live_real_order_allowed=False,
        approved_profile_required=False,
        fail_closed_reason="custom_exit_canary_test_only",
    ),
    authoring_contract_kind="research_only",
)


def custom_exit_provider() -> tuple[ResearchStrategyPlugin, ...]:
    return (CUSTOM_EXIT_PLUGIN,)
