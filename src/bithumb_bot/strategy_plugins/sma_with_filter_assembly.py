from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable

from bithumb_bot.core.sma_policy import (
    ExecutionConstraintSnapshot,
    MarketWindow,
    PositionSnapshot,
    SmaPolicyConfig,
    _stable_hash,
)
from bithumb_bot.market_regime.policy import normalize_live_regime_policy
from bithumb_bot.research.strategy_spec import (
    exit_policy_hash,
    materialize_strategy_parameters,
    runtime_bound_behavior_parameter_names,
    strategy_parameter_source_map,
    strategy_spec_for_name,
)

if TYPE_CHECKING:
    from bithumb_bot.strategy.exit_rules import ExitPolicyConfig
    from bithumb_bot.strategy.sma_policy_strategy import SmaWithFilterStrategy


class MaterializationMode(str, Enum):
    RESEARCH_EXPLORATORY = "research_exploratory"
    RESEARCH_PROMOTION = "research_promotion"
    RUNTIME_REPLAY = "runtime_replay"
    LIVE_DRY_RUN = "live_dry_run"
    LIVE_REAL_ORDER = "live_real_order"

    @property
    def strict_runtime_bound(self) -> bool:
        return self is not MaterializationMode.RESEARCH_EXPLORATORY

    @property
    def requires_candidate_regime_policy(self) -> bool:
        return self is not MaterializationMode.RESEARCH_EXPLORATORY

    @property
    def runtime_comparable(self) -> bool:
        return self is not MaterializationMode.RESEARCH_EXPLORATORY


@dataclass(frozen=True)
class CandidateRegimePolicyStatus:
    policy: dict[str, object] | None
    policy_hash: str
    source: str
    required: bool
    loaded: bool
    valid: bool
    equivalence_scope: str
    verification_status: str
    block_reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_regime_policy_hash": self.policy_hash,
            "candidate_regime_policy_source": self.source,
            "candidate_regime_policy_required": bool(self.required),
            "candidate_regime_policy_loaded": bool(self.loaded),
            "candidate_regime_policy_valid": bool(self.valid),
            "candidate_regime_policy_equivalence_scope": self.equivalence_scope,
            "candidate_regime_policy_verification_status": self.verification_status,
            "candidate_regime_policy_block_reason": self.block_reason,
        }


@dataclass(frozen=True)
class MaterializedSmaWithFilterParameters:
    mode: MaterializationMode
    values: dict[str, Any]
    sources: dict[str, str]
    runtime_comparable: bool
    legacy_defaults_used: tuple[str, ...]

    def require(self, name: str) -> Any:
        return self.values[name]

    def materialized_payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "strategy_name": SmaWithFilterPolicyAssembly.strategy_name,
            "materialization_mode": self.mode.value,
            "runtime_comparable": bool(self.runtime_comparable),
            "values": {name: self.values.get(name) for name in sorted(self.values)},
            "sources": dict(self.sources),
            "legacy_defaults_used": list(self.legacy_defaults_used),
            "runtime_bound_behavior_parameter_names": list(
                runtime_bound_behavior_parameter_names(SmaWithFilterPolicyAssembly.strategy_name)
            ),
        }

    def as_strategy_kwargs(self) -> dict[str, object]:
        values = self.values
        return {
            "short_n": int(values["SMA_SHORT"]),
            "long_n": int(values["SMA_LONG"]),
            "min_gap_ratio": float(values["SMA_FILTER_GAP_MIN_RATIO"]),
            "volatility_window": int(values["SMA_FILTER_VOL_WINDOW"]),
            "min_volatility_ratio": float(values["SMA_FILTER_VOL_MIN_RANGE_RATIO"]),
            "overextended_lookback": int(values["SMA_FILTER_OVEREXT_LOOKBACK"]),
            "overextended_max_return_ratio": float(values["SMA_FILTER_OVEREXT_MAX_RETURN_RATIO"]),
            "cost_edge_enabled": _coerce_bool(values["SMA_COST_EDGE_ENABLED"]),
            "cost_edge_min_ratio": float(values["SMA_COST_EDGE_MIN_RATIO"]),
            "strategy_min_expected_edge_ratio": float(values["STRATEGY_MIN_EXPECTED_EDGE_RATIO"]),
            "market_regime_enabled": _coerce_bool(values["SMA_MARKET_REGIME_ENABLED"]),
            "entry_edge_buffer_ratio": float(values["ENTRY_EDGE_BUFFER_RATIO"]),
            "slippage_bps": float(values["STRATEGY_ENTRY_SLIPPAGE_BPS"]),
            "live_fee_rate_estimate": float(values["LIVE_FEE_RATE_ESTIMATE"]),
            "exit_rule_names": _exit_rule_names(values["STRATEGY_EXIT_RULES"]),
            "exit_stop_loss_ratio": float(values["STRATEGY_EXIT_STOP_LOSS_RATIO"]),
            "exit_max_holding_min": int(values["STRATEGY_EXIT_MAX_HOLDING_MIN"]),
            "exit_min_take_profit_ratio": float(values["STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO"]),
            "exit_small_loss_tolerance_ratio": float(
                values["STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO"]
            ),
            "buy_fraction": float(values.get("BUY_FRACTION") or 0.0),
            "max_order_krw": float(values.get("MAX_ORDER_KRW") or 0.0),
        }


class SmaPolicyAssemblyError(ValueError):
    pass


class SmaWithFilterPolicyAssembly:
    strategy_name = "sma_with_filter"
    execution_sizing_defaults = {
        "BUY_FRACTION": 0.99,
        "MAX_ORDER_KRW": 0.0,
    }

    def runtime_parameter_names(self) -> tuple[str, ...]:
        return runtime_bound_behavior_parameter_names(self.strategy_name)

    def materialize_parameters(
        self,
        raw: dict[str, Any],
        mode: MaterializationMode | str,
        *,
        fee_rate: float | None = None,
        slippage_bps: float | None = None,
        profile: dict[str, Any] | None = None,
    ) -> MaterializedSmaWithFilterParameters:
        resolved_mode = _mode(mode)
        raw_values = dict(raw or {})
        profile_params = (
            dict(profile.get("strategy_parameters"))
            if isinstance(profile, dict) and isinstance(profile.get("strategy_parameters"), dict)
            else {}
        )
        merged = {**profile_params, **raw_values}
        execution_sizing_values = {
            name: merged.pop(name)
            for name in tuple(self.execution_sizing_defaults)
            if name in merged
        }
        runtime_bound = set(runtime_bound_behavior_parameter_names(self.strategy_name))
        if resolved_mode.strict_runtime_bound:
            missing = sorted(name for name in runtime_bound if name not in merged)
            if missing:
                raise SmaPolicyAssemblyError(
                    "sma_policy_assembly_runtime_bound_parameter_missing:" + ",".join(missing)
                )
        values = materialize_strategy_parameters(
            self.strategy_name,
            merged,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
        )
        sources = strategy_parameter_source_map(
            self.strategy_name,
            merged,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
        )
        for name, default in self.execution_sizing_defaults.items():
            if name not in values:
                if name in execution_sizing_values:
                    values[name] = execution_sizing_values[name]
                    sources[name] = "sma_policy_assembly_execution_sizing_input"
                else:
                    values[name] = default
                    sources[name] = "sma_policy_assembly_execution_sizing_default"
        legacy_defaults = tuple(
            sorted(name for name in runtime_bound if sources.get(name) == "strategy_spec_default")
        )
        if resolved_mode.strict_runtime_bound and legacy_defaults:
            raise SmaPolicyAssemblyError(
                "sma_policy_assembly_runtime_bound_default_used:" + ",".join(legacy_defaults)
            )
        return MaterializedSmaWithFilterParameters(
            mode=resolved_mode,
            values=values,
            sources=sources,
            runtime_comparable=resolved_mode.runtime_comparable and not legacy_defaults,
            legacy_defaults_used=legacy_defaults,
        )

    def materialize_from_strategy(
        self,
        strategy: "SmaWithFilterStrategy",
        mode: MaterializationMode | str,
    ) -> MaterializedSmaWithFilterParameters:
        return self.materialize_parameters(
            {
                "SMA_SHORT": int(strategy.short_n),
                "SMA_LONG": int(strategy.long_n),
                "SMA_FILTER_GAP_MIN_RATIO": float(strategy.min_gap_ratio),
                "SMA_FILTER_VOL_WINDOW": int(strategy.volatility_window),
                "SMA_FILTER_VOL_MIN_RANGE_RATIO": float(strategy.min_volatility_ratio),
                "SMA_FILTER_OVEREXT_LOOKBACK": int(strategy.overextended_lookback),
                "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": float(
                    strategy.overextended_max_return_ratio
                ),
                "SMA_COST_EDGE_ENABLED": bool(strategy.cost_edge_enabled),
                "SMA_COST_EDGE_MIN_RATIO": float(strategy.cost_edge_min_ratio),
                "SMA_MARKET_REGIME_ENABLED": bool(strategy.market_regime_enabled),
                "ENTRY_EDGE_BUFFER_RATIO": float(strategy.entry_edge_buffer_ratio),
                "STRATEGY_MIN_EXPECTED_EDGE_RATIO": float(
                    getattr(
                        strategy,
                        "strategy_min_expected_edge_ratio",
                        strategy.cost_edge_min_ratio,
                    )
                ),
                "STRATEGY_ENTRY_SLIPPAGE_BPS": float(strategy.slippage_bps),
                "LIVE_FEE_RATE_ESTIMATE": float(strategy.live_fee_rate_estimate),
                "STRATEGY_EXIT_RULES": ",".join(str(item) for item in strategy.exit_rule_names),
                "STRATEGY_EXIT_STOP_LOSS_RATIO": float(strategy.exit_stop_loss_ratio),
                "STRATEGY_EXIT_MAX_HOLDING_MIN": int(strategy.exit_max_holding_min),
                "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": float(
                    strategy.exit_min_take_profit_ratio
                ),
                "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": float(
                    strategy.exit_small_loss_tolerance_ratio
                ),
                "BUY_FRACTION": float(strategy.buy_fraction),
                "MAX_ORDER_KRW": float(strategy.max_order_krw),
            },
            mode,
        )

    def candidate_regime_policy_status(
        self,
        *,
        candidate_regime_policy: dict[str, object] | None,
        mode: MaterializationMode | str,
    ) -> CandidateRegimePolicyStatus:
        resolved_mode = _mode(mode)
        normalized = normalize_live_regime_policy(candidate_regime_policy)
        policy_hash = (
            _stable_hash(dict(candidate_regime_policy))
            if isinstance(candidate_regime_policy, dict)
            else "sha256:missing"
        )
        loaded = bool(normalized.get("regime_policy_present"))
        valid = bool(normalized.get("regime_policy_valid"))
        source = str(normalized.get("regime_policy_source") or "none")
        block_reason = str(normalized.get("regime_block_reason") or "none")
        verification_status = (
            "not_required"
            if not resolved_mode.requires_candidate_regime_policy
            else "verified"
            if loaded and valid
            else "fail_closed_missing"
            if not loaded
            else "fail_closed_invalid"
        )
        status = CandidateRegimePolicyStatus(
            policy=dict(candidate_regime_policy) if isinstance(candidate_regime_policy, dict) else None,
            policy_hash=policy_hash,
            source=source,
            required=resolved_mode.requires_candidate_regime_policy,
            loaded=loaded,
            valid=valid,
            equivalence_scope=(
                "promotion_runtime_live_equivalence"
                if resolved_mode.runtime_comparable
                else "research_exploratory_not_runtime_comparable"
            ),
            verification_status=verification_status,
            block_reason=block_reason,
        )
        return status

    def build_strategy(
        self,
        materialized: MaterializedSmaWithFilterParameters,
        *,
        pair: str,
        interval: str,
        candidate_regime_policy: dict[str, object] | None = None,
    ) -> "SmaWithFilterStrategy":
        from bithumb_bot.strategy.sma_policy_strategy import create_sma_with_filter_strategy

        status = self.candidate_regime_policy_status(
            candidate_regime_policy=candidate_regime_policy,
            mode=materialized.mode,
        )
        return create_sma_with_filter_strategy(
            **materialized.as_strategy_kwargs(),
            pair=pair,
            interval=interval,
            candidate_regime_policy=status.policy,
            legacy_candidate_regime_policy_fallback=False,
        )

    def build_policy_config(
        self,
        materialized: MaterializedSmaWithFilterParameters,
        strategy: "SmaWithFilterStrategy",
        *,
        candidate_regime_policy: dict[str, object] | None = None,
        candidate_regime_policy_enforced: bool | None = None,
    ) -> SmaPolicyConfig:
        status = self.candidate_regime_policy_status(
            candidate_regime_policy=candidate_regime_policy,
            mode=materialized.mode,
        )
        values = materialized.values
        return SmaPolicyConfig(
            strategy_name=strategy.name,
            short_n=int(values["SMA_SHORT"]),
            long_n=int(values["SMA_LONG"]),
            min_gap_ratio=float(values["SMA_FILTER_GAP_MIN_RATIO"]),
            volatility_window=int(values["SMA_FILTER_VOL_WINDOW"]),
            min_volatility_ratio=float(values["SMA_FILTER_VOL_MIN_RANGE_RATIO"]),
            overextended_lookback=int(values["SMA_FILTER_OVEREXT_LOOKBACK"]),
            overextended_max_return_ratio=float(values["SMA_FILTER_OVEREXT_MAX_RETURN_RATIO"]),
            slippage_bps=float(values["STRATEGY_ENTRY_SLIPPAGE_BPS"]),
            live_fee_rate_estimate=float(values["LIVE_FEE_RATE_ESTIMATE"]),
            entry_edge_buffer_ratio=float(values["ENTRY_EDGE_BUFFER_RATIO"]),
            cost_edge_enabled=_coerce_bool(values["SMA_COST_EDGE_ENABLED"]),
            cost_edge_min_ratio=float(values["SMA_COST_EDGE_MIN_RATIO"]),
            strategy_min_expected_edge_ratio=float(values["STRATEGY_MIN_EXPECTED_EDGE_RATIO"]),
            market_regime_enabled=_coerce_bool(values["SMA_MARKET_REGIME_ENABLED"]),
            buy_fraction=float(values.get("BUY_FRACTION") or getattr(strategy, "buy_fraction", 0.0) or 0.0),
            max_order_krw=float(values.get("MAX_ORDER_KRW") or getattr(strategy, "max_order_krw", 0.0) or 0.0),
            candidate_regime_policy=status.policy,
            require_candidate_regime_policy=status.required,
            candidate_regime_policy_enforced=candidate_regime_policy_enforced,
            candidate_regime_policy_status=status.as_dict(),
            runtime_comparable=materialized.runtime_comparable,
            materialization_mode=materialized.mode.value,
        )

    def build_market_snapshot(
        self,
        *,
        pair: str,
        interval: str,
        candle_ts: int,
        closes: Iterable[float],
        prev_s: float,
        prev_l: float,
        curr_s: float,
        curr_l: float,
        through_ts_ms: int | None = None,
        gap_ratio: float | None = None,
        volatility_ratio: float | None = None,
        overextended_ratio: float | None = None,
        market_regime_snapshot: dict[str, object] | None = None,
        previous_cross_state: str | None = None,
        allow_initial_cross: bool = True,
    ) -> MarketWindow:
        return MarketWindow(
            pair=pair,
            interval=interval,
            candle_ts=int(candle_ts),
            closes=tuple(float(value) for value in closes),
            prev_s=float(prev_s),
            prev_l=float(prev_l),
            curr_s=float(curr_s),
            curr_l=float(curr_l),
            through_ts_ms=through_ts_ms,
            gap_ratio=gap_ratio,
            volatility_ratio=volatility_ratio,
            overextended_ratio=overextended_ratio,
            market_regime_snapshot=(
                dict(market_regime_snapshot) if isinstance(market_regime_snapshot, dict) else None
            ),
            previous_cross_state=previous_cross_state,
            allow_initial_cross=bool(allow_initial_cross),
        )

    def build_exit_policy_config(
        self,
        materialized: MaterializedSmaWithFilterParameters,
        *,
        fee_rate_for_decision: float | None = None,
    ) -> "ExitPolicyConfig":
        from bithumb_bot.strategy.exit_rules import ExitPolicyConfig

        values = materialized.values
        return ExitPolicyConfig(
            rule_names=tuple(_exit_rule_names(values["STRATEGY_EXIT_RULES"])),
            stop_loss_ratio=float(values["STRATEGY_EXIT_STOP_LOSS_RATIO"]),
            max_holding_sec=float(values["STRATEGY_EXIT_MAX_HOLDING_MIN"]) * 60.0,
            min_take_profit_ratio=float(values["STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO"]),
            small_loss_tolerance_ratio=float(values["STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO"]),
            live_fee_rate_estimate=(
                float(values["LIVE_FEE_RATE_ESTIMATE"])
                if fee_rate_for_decision is None
                else float(fee_rate_for_decision)
            ),
        )

    def materialize_exit_policy(
        self,
        strategy_name: str,
        parameter_values: dict[str, Any],
        *,
        materialization_mode: str = "research_promotion",
    ) -> dict[str, object]:
        if str(strategy_name or "").strip().lower() != self.strategy_name:
            raise SmaPolicyAssemblyError(f"sma_exit_policy_strategy_mismatch:{strategy_name}")
        values = materialize_strategy_parameters(self.strategy_name, dict(parameter_values))
        rules = tuple(_exit_rule_names(values["STRATEGY_EXIT_RULES"]))
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
        policy = {
            "schema_version": 1,
            "strategy_name": self.strategy_name,
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
        config = {
            "schema_version": 1,
            "strategy_name": self.strategy_name,
            "rules": list(rules),
            "stop_loss_ratio": stop_loss_ratio,
            "max_holding_min": max_holding_min,
            "min_take_profit_ratio": strategy_specific_exit_policy["min_take_profit_ratio"],
            "small_loss_tolerance_ratio": strategy_specific_exit_policy["small_loss_tolerance_ratio"],
            "live_fee_rate_estimate": float(values.get("LIVE_FEE_RATE_ESTIMATE") or 0.0),
        }
        return {
            "exit_policy": policy,
            "exit_policy_hash": exit_policy_hash(policy),
            "exit_policy_config": config,
            "exit_policy_config_hash": _stable_hash(config),
            "exit_policy_source": "sma_with_filter_exit_policy_materializer",
            "exit_policy_materialization_mode": materialization_mode,
        }

    def build_execution_snapshot(
        self,
        materialized: MaterializedSmaWithFilterParameters,
        *,
        pair: str,
        fee_rate_for_decision: float | None = None,
        runtime_fee_authority: bool = False,
    ) -> ExecutionConstraintSnapshot:
        from bithumb_bot.canonical_decision import order_rules_snapshot_payload
        from bithumb_bot.runtime_sma_context import (
            fee_authority_context,
            get_effective_order_rules,
            live_armed_entry_fee_authority_blocks,
            resolve_strategy_fee_authority,
        )

        fee = float(materialized.values["LIVE_FEE_RATE_ESTIMATE"])
        fee_authority = resolve_strategy_fee_authority(pair=pair, config_fallback_fee_rate=fee)
        decision_fee = (
            float(fee_authority.taker_roundtrip_fee_rate / 2)
            if fee_rate_for_decision is None
            else float(fee_rate_for_decision)
        )
        return ExecutionConstraintSnapshot(
            fee_rate_for_decision=decision_fee,
            fee_authority_degraded_blocks_entry=(
                live_armed_entry_fee_authority_blocks(fee_authority)
                if runtime_fee_authority
                else False
            ),
            fee_authority=fee_authority_context(fee_authority),
            order_rules=order_rules_snapshot_payload(get_effective_order_rules(pair), pair=pair),
        )

    def build_execution_snapshot_from_payloads(
        self,
        *,
        fee_rate_for_decision: float,
        fee_authority_degraded_blocks_entry: bool,
        fee_authority: dict[str, object],
        order_rules: dict[str, object],
    ) -> ExecutionConstraintSnapshot:
        return ExecutionConstraintSnapshot(
            fee_rate_for_decision=float(fee_rate_for_decision),
            fee_authority_degraded_blocks_entry=bool(fee_authority_degraded_blocks_entry),
            fee_authority=dict(fee_authority),
            order_rules=dict(order_rules),
        )

    def policy_input_payload(
        self,
        *,
        materialized: MaterializedSmaWithFilterParameters,
        market: MarketWindow,
        position: PositionSnapshot,
        policy_config: SmaPolicyConfig,
        execution_context: ExecutionConstraintSnapshot,
        exit_policy_config: "ExitPolicyConfig",
    ) -> dict[str, object]:
        spec = strategy_spec_for_name(self.strategy_name)
        materialized_payload = materialized.materialized_payload()
        execution_payload = execution_context.policy_input_payload()
        exit_policy_payload = exit_policy_config.policy_input_payload()
        strategy_parameter_values = {
            name: materialized.values[name]
            for name in spec.accepted_parameter_names
            if name in materialized.values
        }
        return {
            "schema_version": 1,
            "strategy_name": self.strategy_name,
            "strategy_version": spec.strategy_version,
            "decision_contract_version": spec.decision_contract_version,
            "materialization_mode": materialized.mode.value,
            "materialization_equivalence_scope": (
                "runtime_comparable"
                if materialized.runtime_comparable
                else "research_exploratory_not_runtime_comparable"
            ),
            "runtime_comparable": bool(materialized.runtime_comparable),
            "materialized_parameters": materialized_payload,
            "materialized_parameters_hash": _stable_hash(materialized_payload),
            "parameters": {name: materialized.values.get(name) for name in sorted(materialized.values)},
            "parameter_sources": dict(materialized.sources),
            "legacy_defaults_used": list(materialized.legacy_defaults_used),
            "policy_config": policy_config.policy_input_payload(),
            "candidate_regime_policy": dict(policy_config.candidate_regime_policy_status),
            "market": market.policy_input_payload(),
            "position": position.policy_input_payload(),
            "execution_constraints": execution_payload,
            "execution_sizing": {
                "buy_fraction": float(policy_config.buy_fraction),
                "max_order_krw": float(policy_config.max_order_krw),
            },
            "exit_policy": exit_policy_payload,
            "exit_policy_hash": _stable_hash(exit_policy_payload),
            "declared_exit_policy": self.materialize_exit_policy(
                self.strategy_name,
                strategy_parameter_values,
                materialization_mode=materialized.mode.value,
            )["exit_policy"],
        }

    def build_replay_fingerprint_payload(
        self,
        *,
        strategy_name: str,
        pair: str,
        interval: str,
        candle_ts: int,
        through_ts_ms: int | None,
        materialized: MaterializedSmaWithFilterParameters,
        thresholds: dict[str, object],
        fee_authority: dict[str, object],
        slippage_bps: float,
        regime_version: str,
        policy_input_payload: dict[str, object],
        policy_input_hash: str,
        exit_policy_hash: str,
    ) -> dict[str, object]:
        payload = build_sma_with_filter_replay_fingerprint(
            strategy_name=strategy_name,
            pair=pair,
            interval=interval,
            candle_ts=int(candle_ts),
            through_ts_ms=None if through_ts_ms is None else int(through_ts_ms),
            short_n=int(materialized.values["SMA_SHORT"]),
            long_n=int(materialized.values["SMA_LONG"]),
            thresholds={
                **dict(thresholds),
                "materialization_mode": materialized.mode.value,
                "runtime_comparable": bool(materialized.runtime_comparable),
            },
            fee_authority=dict(fee_authority),
            slippage_bps=float(slippage_bps),
            regime_version=regime_version,
            order_sizing={
                "buy_fraction": float(materialized.values.get("BUY_FRACTION") or 0.0),
                "max_order_krw": float(materialized.values.get("MAX_ORDER_KRW") or 0.0),
            },
        )
        payload["policy_input_payload_hash"] = _stable_hash(policy_input_payload)
        payload["policy_input_payload"] = policy_input_payload
        payload["policy_input_hash"] = policy_input_hash
        payload["exit_policy_hash"] = exit_policy_hash
        payload["materialized_parameters_hash"] = _stable_hash(materialized.materialized_payload())
        return payload


def build_sma_with_filter_replay_fingerprint(
    *,
    strategy_name: str,
    pair: str,
    interval: str,
    candle_ts: int | None,
    through_ts_ms: int | None,
    short_n: int,
    long_n: int,
    thresholds: dict[str, object],
    fee_authority: dict[str, object],
    slippage_bps: float,
    regime_version: str,
    order_sizing: dict[str, object] | None = None,
) -> dict[str, object]:
    from bithumb_bot.decision_contract import DECISION_CONTRACT_VERSION

    fee_source = str(fee_authority.get("fee_source") or "unknown").strip() or "unknown"
    return {
        "strategy_name": str(strategy_name),
        "strategy_version": "sma_with_filter_v2_entry_exit_channel_split",
        "decision_contract_version": DECISION_CONTRACT_VERSION,
        "pair": str(pair),
        "interval": str(interval),
        "candle_ts": None if candle_ts is None else int(candle_ts),
        "through_ts_ms": None if through_ts_ms is None else int(through_ts_ms),
        "sma_short": int(short_n),
        "sma_long": int(long_n),
        "regime_feature_version": str(regime_version),
        "thresholds": dict(thresholds),
        "fee_authority_source": fee_source,
        "fee_authority_degraded": bool(fee_authority.get("degraded", False)),
        "slippage_bps": float(slippage_bps),
        "order_sizing": dict(order_sizing or {}),
    }


def _mode(mode: MaterializationMode | str) -> MaterializationMode:
    if isinstance(mode, MaterializationMode):
        return mode
    try:
        return MaterializationMode(str(mode))
    except ValueError as exc:
        raise SmaPolicyAssemblyError(f"unknown_sma_policy_materialization_mode:{mode}") from exc


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _exit_rule_names(raw: object) -> list[str]:
    return [token.strip().lower() for token in str(raw or "").split(",") if token.strip()]


__all__ = [
    "CandidateRegimePolicyStatus",
    "MaterializationMode",
    "MaterializedSmaWithFilterParameters",
    "SmaPolicyAssemblyError",
    "SmaWithFilterPolicyAssembly",
]
