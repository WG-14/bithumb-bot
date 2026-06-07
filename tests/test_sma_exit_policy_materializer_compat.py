from __future__ import annotations

from bithumb_bot.research.strategy_spec import exit_policy_from_parameters
from bithumb_bot.strategy_plugins.sma_with_filter_assembly import SmaWithFilterPolicyAssembly


def _params() -> dict[str, object]:
    return {
        "SMA_SHORT": 2,
        "SMA_LONG": 4,
        "STRATEGY_EXIT_RULES": "stop_loss,opposite_cross,max_holding_time",
        "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.01,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 10,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.02,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.003,
    }


def test_sma_materializer_matches_legacy_declared_exit_policy_payload() -> None:
    materialized = SmaWithFilterPolicyAssembly().materialize_exit_policy(
        "sma_with_filter",
        _params(),
    )

    assert materialized["exit_policy"] == exit_policy_from_parameters("sma_with_filter", _params())


def test_sma_materializer_keeps_opposite_cross_strategy_owned() -> None:
    policy = exit_policy_from_parameters("sma_with_filter", _params())

    assert policy["strategy_rules"] == ["opposite_cross"]
    assert policy["opposite_cross"]["enabled"] is True
