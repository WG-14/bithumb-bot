from __future__ import annotations

from typing import Any

from bithumb_bot.research.strategy_spec import StrategySpec
from bithumb_bot.strategy_authoring import research_plugin_from_decide_snapshot


THRESHOLD_RESEARCH_ONLY_SPEC = StrategySpec(
    strategy_name="threshold_research_only",
    strategy_version="threshold_research_only.research_contract.v1",
    accepted_parameter_names=("THRESHOLD_CLOSE_ABOVE",),
    required_parameter_names=("THRESHOLD_CLOSE_ABOVE",),
    behavior_affecting_parameter_names=("THRESHOLD_CLOSE_ABOVE",),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={},
    decision_contract_version="research_threshold_research_only_decision_contract.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": (),
        "description": "Research-only threshold template; not promotion-grade without an extension.",
    },
)


def decide_threshold_snapshot(
    *,
    candle: Any,
    candle_index: int,
    dataset: Any,
    parameter_values: dict[str, Any],
) -> dict[str, Any]:
    del dataset
    threshold = float(parameter_values["THRESHOLD_CLOSE_ABOVE"])
    close = float(candle.close)
    signal = "BUY" if close > threshold else "HOLD"
    return {
        "signal": signal,
        "reason": "threshold_close_above" if signal == "BUY" else "threshold_not_met",
        "feature_snapshot": {
            "candle_index": int(candle_index),
            "close": close,
            "threshold_close_above": threshold,
        },
        "strategy_diagnostics": {
            "schema_version": 1,
            "threshold_close_above": threshold,
            "close_above_threshold": close > threshold,
        },
        "order_intent": (
            {
                "side": "BUY",
                "sizing": "portfolio_policy_fractional_cash",
            }
            if signal == "BUY"
            else None
        ),
    }


THRESHOLD_RESEARCH_ONLY_PLUGIN = research_plugin_from_decide_snapshot(
    strategy_name=THRESHOLD_RESEARCH_ONLY_SPEC.strategy_name,
    version=THRESHOLD_RESEARCH_ONLY_SPEC.strategy_version,
    spec=THRESHOLD_RESEARCH_ONLY_SPEC,
    required_data=THRESHOLD_RESEARCH_ONLY_SPEC.required_data,
    decide_snapshot=decide_threshold_snapshot,
    diagnostics_namespace="threshold_research_only",
)
