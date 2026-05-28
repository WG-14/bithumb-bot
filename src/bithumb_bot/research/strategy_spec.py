from __future__ import annotations

import math
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
    strategy_version="sma_with_filter.research_runtime_contract.v2",
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
        "STRATEGY_EXIT_STOP_LOSS_RATIO",
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
        "STRATEGY_EXIT_STOP_LOSS_RATIO",
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
        "STRATEGY_EXIT_RULES": "stop_loss,opposite_cross,max_holding_time",
        "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.0,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 0,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.0,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.0,
    },
    decision_contract_version="research_sma_decision_contract.v3_entry_exit_risk_exit",
    required_data=("candles",),
    optional_data=("top_of_book",),
    exit_policy_schema={
        "schema_version": 1,
        "rules": ("stop_loss", "opposite_cross", "max_holding_time"),
        "stop_loss": {
            "unit": "unrealized_pnl_ratio",
            "disabled_value": 0,
            "evaluation_price_basis": "closed_candle_mark",
            "intrabar_stop_modeled": False,
            "limitation_reasons": (
                "intra_candle_path_unavailable",
                "candle_close_stop_may_exit_later_than_real_stop",
            ),
        },
        "max_holding_time": {"unit": "minutes", "disabled_value": 0},
        "opposite_cross": {
            "min_take_profit_ratio": "max(configured, roundtrip_fee)",
            "small_loss_tolerance_ratio": "defer_noise_band",
        },
    },
)


NOOP_BASELINE_SPEC = StrategySpec(
    strategy_name="noop_baseline",
    strategy_version="noop_baseline.research_contract.v1",
    accepted_parameter_names=("NOOP_DECISION_START_INDEX", "NOOP_DECISION_REASON"),
    required_parameter_names=(),
    behavior_affecting_parameter_names=("NOOP_DECISION_START_INDEX", "NOOP_DECISION_REASON"),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={"NOOP_DECISION_START_INDEX": 0, "NOOP_DECISION_REASON": "noop_baseline_hold"},
    decision_contract_version="research_noop_baseline_decision_contract.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": (),
        "description": "No-op baseline never emits executable entry or exit intent.",
    },
)


BUY_AND_HOLD_BASELINE_SPEC = StrategySpec(
    strategy_name="buy_and_hold_baseline",
    strategy_version="buy_and_hold_baseline.research_contract.v1",
    accepted_parameter_names=("BUY_HOLD_BUY_INDEX", "BUY_HOLD_DECISION_REASON"),
    required_parameter_names=("BUY_HOLD_BUY_INDEX",),
    behavior_affecting_parameter_names=("BUY_HOLD_BUY_INDEX", "BUY_HOLD_DECISION_REASON"),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={"BUY_HOLD_DECISION_REASON": "buy_and_hold_architecture_canary"},
    decision_contract_version="research_buy_and_hold_baseline_decision_contract.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": (),
        "description": "Executable canary emits one BUY intent, then HOLD decisions.",
    },
)


def strategy_spec_for_name(strategy_name: str) -> StrategySpec:
    if strategy_name == "__test_top_of_book_required__":
        return SMA_WITH_FILTER_SPEC
    try:
        from .strategy_registry import ResearchStrategyRegistryError, resolve_research_strategy_plugin

        return resolve_research_strategy_plugin(strategy_name).spec
    except ResearchStrategyRegistryError as exc:
        raise StrategySpecError(f"unsupported research strategy: {strategy_name}") from exc


def runtime_bound_behavior_parameter_names(strategy_name: str) -> tuple[str, ...]:
    spec = strategy_spec_for_name(strategy_name)
    research_only = set(spec.research_only_parameter_names)
    return tuple(
        name
        for name in spec.behavior_affecting_parameter_names
        if name not in research_only
    )


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
    if is_production_bound_target(deployment_tier):
        runtime_bound_behavior = sorted(runtime_bound_behavior_parameter_names(strategy_name))
        missing_behavior = [key for key in runtime_bound_behavior if key not in parameter_space]
        if missing_behavior:
            raise StrategySpecError(
                "production-bound manifests must declare every runtime-bound behavior-affecting "
                "strategy parameter: " + ",".join(missing_behavior)
            )
    _validate_exit_policy_parameter_values(parameter_space)
    return spec


def strategy_parameter_source_map(
    strategy_name: str,
    parameter_values: dict[str, Any],
    *,
    fee_rate: float | None = None,
    slippage_bps: float | None = None,
) -> dict[str, str]:
    spec = strategy_spec_for_name(strategy_name)
    raw = dict(parameter_values)
    sources = {key: "strategy_spec_default" for key in spec.default_parameters}
    for key in raw:
        sources[key] = "raw_parameter_values"
    if (
        fee_rate is not None
        and "LIVE_FEE_RATE_ESTIMATE" in spec.accepted_parameter_names
        and "LIVE_FEE_RATE_ESTIMATE" not in raw
    ):
        sources["LIVE_FEE_RATE_ESTIMATE"] = "cost_model_fee_rate"
    if (
        slippage_bps is not None
        and "STRATEGY_ENTRY_SLIPPAGE_BPS" in spec.accepted_parameter_names
        and "STRATEGY_ENTRY_SLIPPAGE_BPS" not in raw
    ):
        sources["STRATEGY_ENTRY_SLIPPAGE_BPS"] = "cost_model_slippage_bps"
    return sources


def materialize_strategy_parameters(
    strategy_name: str,
    parameter_values: dict[str, Any],
    *,
    fee_rate: float | None = None,
    slippage_bps: float | None = None,
) -> dict[str, Any]:
    spec = strategy_spec_for_name(strategy_name)
    values = {**spec.default_parameters, **dict(parameter_values)}
    if (
        fee_rate is not None
        and "LIVE_FEE_RATE_ESTIMATE" in spec.accepted_parameter_names
        and "LIVE_FEE_RATE_ESTIMATE" not in parameter_values
    ):
        values["LIVE_FEE_RATE_ESTIMATE"] = float(fee_rate)
    if (
        slippage_bps is not None
        and "STRATEGY_ENTRY_SLIPPAGE_BPS" in spec.accepted_parameter_names
        and "STRATEGY_ENTRY_SLIPPAGE_BPS" not in parameter_values
    ):
        values["STRATEGY_ENTRY_SLIPPAGE_BPS"] = float(slippage_bps)
    _validate_exit_policy_materialized_values(values)
    return values


def materialized_strategy_parameters_hash(parameter_values: dict[str, Any]) -> str:
    return sha256_prefixed(dict(parameter_values))


def exit_policy_from_parameters(strategy_name: str, parameter_values: dict[str, Any]) -> dict[str, Any]:
    spec = strategy_spec_for_name(strategy_name)
    if not spec.exit_policy_schema.get("rules"):
        return {
            "schema_version": 1,
            "strategy_name": strategy_name,
            "rules": [],
            "common_rules": [],
            "strategy_rules": [],
            "entry_exit_policy": "strategy_emits_no_exit_intent",
            "stop_loss": {"enabled": False, "disabled_when_zero": True},
            "max_holding_time": {"enabled": False, "disabled_when_zero": True},
        }
    values = materialize_strategy_parameters(strategy_name, parameter_values)
    rules = _normalize_exit_rule_names(str(values.get("STRATEGY_EXIT_RULES") or ""))
    common_rules = tuple(rule for rule in rules if rule in {"stop_loss", "max_holding_time"})
    strategy_rules = tuple(rule for rule in rules if rule not in set(common_rules))
    stop_loss_ratio = float(values.get("STRATEGY_EXIT_STOP_LOSS_RATIO") or 0.0)
    max_holding_min = int(values.get("STRATEGY_EXIT_MAX_HOLDING_MIN") or 0)
    strategy_specific_exit_policy = {
        "enabled": "opposite_cross" in rules,
        "min_take_profit_ratio": float(values.get("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO") or 0.0),
        "small_loss_tolerance_ratio": float(
            values.get("STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO") or 0.0
        ),
    }
    return {
        "schema_version": 1,
        "strategy_name": strategy_name,
        "rules": list(rules),
        "common_rules": list(common_rules),
        "strategy_rules": list(strategy_rules),
        "stop_loss": {
            "enabled": "stop_loss" in rules and stop_loss_ratio > 0.0,
            "stop_loss_ratio": stop_loss_ratio,
            "disabled_when_zero": True,
            "evaluation_price_basis": "closed_candle_mark",
            "intrabar_stop_modeled": False,
            "limitation_reasons": [
                "intra_candle_path_unavailable",
                "candle_close_stop_may_exit_later_than_real_stop",
            ],
        },
        "opposite_cross": strategy_specific_exit_policy,
        "strategy_specific": strategy_specific_exit_policy,
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


def _validate_exit_policy_parameter_values(parameter_space: dict[str, tuple[object, ...]]) -> None:
    rules_values = parameter_space.get("STRATEGY_EXIT_RULES")
    ratio_values = parameter_space.get("STRATEGY_EXIT_STOP_LOSS_RATIO")
    if ratio_values is None:
        return
    for raw_ratio in ratio_values:
        ratio = _non_negative_float("STRATEGY_EXIT_STOP_LOSS_RATIO", raw_ratio)
        if ratio <= 0.0 or rules_values is None:
            continue
        for raw_rules in rules_values:
            rules = _normalize_exit_rule_names(str(raw_rules or ""))
            if "stop_loss" not in rules:
                raise StrategySpecError(
                    "STRATEGY_EXIT_STOP_LOSS_RATIO is positive but "
                    "STRATEGY_EXIT_RULES does not include stop_loss"
                )


def _validate_exit_policy_materialized_values(values: dict[str, Any]) -> None:
    stop_loss_ratio = _non_negative_float(
        "STRATEGY_EXIT_STOP_LOSS_RATIO",
        values.get("STRATEGY_EXIT_STOP_LOSS_RATIO", 0.0),
    )
    rules = _normalize_exit_rule_names(str(values.get("STRATEGY_EXIT_RULES") or ""))
    if stop_loss_ratio > 0.0 and "stop_loss" not in rules:
        raise StrategySpecError(
            "STRATEGY_EXIT_STOP_LOSS_RATIO is positive but STRATEGY_EXIT_RULES does not include stop_loss"
        )


def _non_negative_float(name: str, value: object) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise StrategySpecError(f"{name} must be a finite value >= 0, got {value!r}") from exc
    if not math.isfinite(resolved) or resolved < 0.0:
        raise StrategySpecError(f"{name} must be a finite value >= 0, got {value!r}")
    return resolved
