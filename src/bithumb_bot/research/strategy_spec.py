from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .deployment_policy import is_production_bound_target
from .hashing import sha256_prefixed


class StrategySpecError(ValueError):
    pass


@dataclass(frozen=True)
class StrategySpec:
    strategy_name: str
    strategy_version: str
    accepted_parameter_names: tuple[str, ...]
    required_parameter_names: tuple[str, ...]
    behavior_affecting_parameter_names: tuple[str, ...]
    metadata_only_parameter_names: tuple[str, ...]
    research_only_parameter_names: tuple[str, ...]
    default_parameters: dict[str, Any]
    decision_contract_version: str
    required_data: tuple[str, ...]
    optional_data: tuple[str, ...]
    exit_policy_schema: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "accepted_parameter_names": list(self.accepted_parameter_names),
            "required_parameter_names": list(self.required_parameter_names),
            "behavior_affecting_parameter_names": list(self.behavior_affecting_parameter_names),
            "metadata_only_parameter_names": list(self.metadata_only_parameter_names),
            "research_only_parameter_names": list(self.research_only_parameter_names),
            "default_parameters": dict(self.default_parameters),
            "decision_contract_version": self.decision_contract_version,
            "required_data": list(self.required_data),
            "optional_data": list(self.optional_data),
            "exit_policy_schema": dict(self.exit_policy_schema),
        }

    def spec_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


SMA_WITH_FILTER_SPEC = StrategySpec(
    strategy_name="sma_with_filter",
    strategy_version="sma_with_filter.research_runtime_contract.v1",
    accepted_parameter_names=(
        "SMA_SHORT",
        "SMA_LONG",
        "SMA_FILTER_GAP_MIN_RATIO",
        "SMA_FILTER_VOL_WINDOW",
        "SMA_FILTER_VOL_MIN_RANGE_RATIO",
        "SMA_FILTER_VOLUME_WINDOW",
        "SMA_FILTER_LIQUIDITY_WINDOW",
        "SMA_MARKET_REGIME_ENABLED",
        "SMA_FILTER_OVEREXT_LOOKBACK",
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
        "SMA_COST_EDGE_ENABLED",
        "SMA_COST_EDGE_MIN_RATIO",
        "ENTRY_EDGE_BUFFER_RATIO",
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
        "STRATEGY_ENTRY_SLIPPAGE_BPS",
        "LIVE_FEE_RATE_ESTIMATE",
        "STRATEGY_EXIT_RULES",
        "STRATEGY_EXIT_MAX_HOLDING_MIN",
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
    ),
    required_parameter_names=("SMA_SHORT", "SMA_LONG"),
    behavior_affecting_parameter_names=(
        "SMA_SHORT",
        "SMA_LONG",
        "SMA_FILTER_GAP_MIN_RATIO",
        "SMA_FILTER_VOL_WINDOW",
        "SMA_FILTER_VOL_MIN_RANGE_RATIO",
        "SMA_FILTER_OVEREXT_LOOKBACK",
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
        "SMA_MARKET_REGIME_ENABLED",
        "SMA_COST_EDGE_ENABLED",
        "SMA_COST_EDGE_MIN_RATIO",
        "ENTRY_EDGE_BUFFER_RATIO",
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
        "STRATEGY_ENTRY_SLIPPAGE_BPS",
        "LIVE_FEE_RATE_ESTIMATE",
        "STRATEGY_EXIT_RULES",
        "STRATEGY_EXIT_MAX_HOLDING_MIN",
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
    ),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(
        "SMA_FILTER_VOLUME_WINDOW",
        "SMA_FILTER_LIQUIDITY_WINDOW",
    ),
    default_parameters={
        "SMA_FILTER_GAP_MIN_RATIO": 0.0012,
        "SMA_FILTER_VOL_WINDOW": 10,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.003,
        "SMA_FILTER_VOLUME_WINDOW": 10,
        "SMA_FILTER_LIQUIDITY_WINDOW": 10,
        "SMA_FILTER_OVEREXT_LOOKBACK": 3,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.02,
        "SMA_MARKET_REGIME_ENABLED": True,
        "SMA_COST_EDGE_ENABLED": True,
        "SMA_COST_EDGE_MIN_RATIO": 0.0,
        "ENTRY_EDGE_BUFFER_RATIO": 0.0005,
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": 0.0,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": 0.0,
        "LIVE_FEE_RATE_ESTIMATE": 0.0004,
        "STRATEGY_EXIT_RULES": "opposite_cross,max_holding_time",
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 0,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.0,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.0,
    },
    decision_contract_version="research_sma_decision_contract.v2",
    required_data=("candles",),
    optional_data=("top_of_book",),
    exit_policy_schema={
        "schema_version": 1,
        "rules": ("opposite_cross", "max_holding_time"),
        "max_holding_time": {"unit": "minutes", "disabled_value": 0},
        "opposite_cross": {
            "min_take_profit_ratio": "max(configured, roundtrip_fee)",
            "small_loss_tolerance_ratio": "defer_noise_band",
        },
    },
)


def strategy_spec_for_name(strategy_name: str) -> StrategySpec:
    if strategy_name == SMA_WITH_FILTER_SPEC.strategy_name:
        return SMA_WITH_FILTER_SPEC
    raise StrategySpecError(f"unsupported research strategy: {strategy_name}")


def validate_parameter_space_against_strategy_spec(
    *,
    strategy_name: str,
    parameter_space: dict[str, tuple[object, ...]],
    deployment_tier: str,
) -> StrategySpec:
    spec = strategy_spec_for_name(strategy_name)
    accepted = set(spec.accepted_parameter_names)
    unknown = sorted(key for key in parameter_space if key not in accepted)
    if unknown:
        raise StrategySpecError(f"unknown strategy parameter(s): {','.join(unknown)}")
    missing = sorted(key for key in spec.required_parameter_names if key not in parameter_space)
    if missing:
        raise StrategySpecError(f"missing required strategy parameter(s): {','.join(missing)}")
    metadata = sorted(key for key in parameter_space if key in set(spec.metadata_only_parameter_names))
    if metadata and is_production_bound_target(deployment_tier):
        raise StrategySpecError(
            "metadata-only strategy parameter(s) cannot be optimized for production-bound manifests: "
            + ",".join(metadata)
        )
    research_only = sorted(key for key in parameter_space if key in set(spec.research_only_parameter_names))
    if research_only and is_production_bound_target(deployment_tier):
        raise StrategySpecError(
            "research-only strategy parameter(s) cannot be optimized for production-bound manifests: "
            + ",".join(research_only)
        )
    return spec


def materialize_strategy_parameters(
    strategy_name: str,
    parameter_values: dict[str, Any],
    *,
    fee_rate: float | None = None,
    slippage_bps: float | None = None,
) -> dict[str, Any]:
    spec = strategy_spec_for_name(strategy_name)
    values = {**spec.default_parameters, **dict(parameter_values)}
    if fee_rate is not None and "LIVE_FEE_RATE_ESTIMATE" not in parameter_values:
        values["LIVE_FEE_RATE_ESTIMATE"] = float(fee_rate)
    if slippage_bps is not None and "STRATEGY_ENTRY_SLIPPAGE_BPS" not in parameter_values:
        values["STRATEGY_ENTRY_SLIPPAGE_BPS"] = float(slippage_bps)
    return values


def exit_policy_from_parameters(strategy_name: str, parameter_values: dict[str, Any]) -> dict[str, Any]:
    values = materialize_strategy_parameters(strategy_name, parameter_values)
    rules = _normalize_exit_rule_names(str(values.get("STRATEGY_EXIT_RULES") or ""))
    max_holding_min = int(values.get("STRATEGY_EXIT_MAX_HOLDING_MIN") or 0)
    return {
        "schema_version": 1,
        "strategy_name": strategy_name,
        "rules": list(rules),
        "opposite_cross": {
            "enabled": "opposite_cross" in rules,
            "min_take_profit_ratio": float(values.get("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO") or 0.0),
            "small_loss_tolerance_ratio": float(
                values.get("STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO") or 0.0
            ),
        },
        "max_holding_time": {
            "enabled": "max_holding_time" in rules and max_holding_min > 0,
            "max_holding_min": max_holding_min,
            "disabled_when_zero": True,
        },
    }


def exit_policy_hash(policy: dict[str, Any]) -> str:
    return sha256_prefixed(policy)


def _normalize_exit_rule_names(raw: str) -> tuple[str, ...]:
    return tuple(token.strip().lower() for token in raw.split(",") if token.strip())
