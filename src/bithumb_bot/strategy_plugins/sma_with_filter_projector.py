from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.core.sma_policy import _stable_hash
from bithumb_bot.market_regime.thresholds import MarketRegimeThresholds
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot
from bithumb_bot.research.strategy_spec import materialized_strategy_parameters_hash
from bithumb_bot.strategy_decision_input import StrategyDecisionInputBundle
from bithumb_bot.strategy_policy_contract import PositionSnapshot
from bithumb_bot.strategy_plugins import sma_with_filter_events

from .sma_with_filter_assembly import (
    MaterializationMode,
    MaterializedSmaWithFilterParameters,
    SmaWithFilterPolicyAssembly,
)


@dataclass(frozen=True)
class SmaWithFilterProjectedDecisionInput:
    strategy: object
    materialized: MaterializedSmaWithFilterParameters
    bundle: StrategyDecisionInputBundle
    rule_sources: dict[str, str]
    replay_fingerprint: dict[str, object]


@dataclass(frozen=True)
class SmaWithFilterRuntimeProjectionResult:
    """Validated runtime-side projection material before bundle construction.

    Runtime replay may assemble position/fee/order-rule snapshots from DB state,
    but those snapshots are admitted to the canonical bundle only through this
    projector-owned contract. The evidence hashes here are non-authoritative
    observability; the typed snapshots remain the service inputs.
    """

    strategy: object
    materialized: MaterializedSmaWithFilterParameters
    market: object
    position: PositionSnapshot
    config: object
    execution_constraints: object
    exit_policy_config: object
    provenance: dict[str, object]
    source_contract: str = "SmaWithFilterRuntimeProjectionResult.v1"

    def __post_init__(self) -> None:
        if not bool(self.materialized.runtime_comparable):
            raise ValueError("sma_runtime_projection_not_runtime_comparable")
        if self.materialized.mode is not MaterializationMode.RUNTIME_REPLAY:
            raise ValueError("sma_runtime_projection_materialization_mode_invalid")
        if not str(getattr(self.strategy, "name", "") or "").strip():
            raise ValueError("sma_runtime_projection_strategy_missing")
        if not isinstance(self.position, PositionSnapshot):
            raise TypeError("sma_runtime_projection_position_snapshot_invalid")
        policy_payload = getattr(self.config, "policy_input_payload", None)
        if not callable(policy_payload):
            raise TypeError("sma_runtime_projection_policy_config_invalid")
        execution_payload = getattr(self.execution_constraints, "policy_input_payload", None)
        if not callable(execution_payload):
            raise TypeError("sma_runtime_projection_execution_constraints_invalid")
        exit_payload = getattr(self.exit_policy_config, "policy_input_payload", None)
        if not callable(exit_payload):
            raise TypeError("sma_runtime_projection_exit_policy_config_invalid")

    def evidence_payload(self, *, projector_version: str, projector_hash: str) -> dict[str, object]:
        return {
            "source_contract": self.source_contract,
            "snapshot_projector_version": projector_version,
            "snapshot_projector_hash": projector_hash,
            "market_snapshot_hash": _stable_hash(self.market.policy_input_payload()),
            "position_snapshot_hash": _stable_hash(self.position.policy_input_payload()),
            "execution_constraints_hash": _stable_hash(
                self.execution_constraints.policy_input_payload()
            ),
            "policy_config_hash": _stable_hash(self.config.policy_input_payload()),
            "exit_policy_config_hash": _stable_hash(self.exit_policy_config.policy_input_payload()),
            "materialized_parameters_hash": materialized_strategy_parameters_hash(
                dict(self.materialized.values)
            ),
        }


@dataclass(frozen=True)
class PromotionDecisionSeed:
    """Promotion-grade replay seed. It carries timing only, never signal authority."""

    candle_index: int
    candle_ts: int
    decision_ts: int | None = None
    source: str = "research_event_seed"

    @classmethod
    def from_research_event(
        cls,
        *,
        event: Any,
        candle_index: int,
    ) -> "PromotionDecisionSeed":
        return cls(
            candle_index=int(candle_index),
            candle_ts=int(getattr(event, "candle_ts")),
            decision_ts=(
                int(getattr(event, "decision_ts"))
                if getattr(event, "decision_ts", None) is not None
                else None
            ),
        )

    def provenance_payload(self) -> dict[str, object]:
        return {
            "seed_contract": "PromotionDecisionSeed.v1",
            "seed_source": self.source,
            "candle_index": int(self.candle_index),
            "candle_ts": int(self.candle_ts),
            "decision_ts": self.decision_ts,
            "event_signal_authority": "rejected",
            "event_feature_authority": "rejected",
        }


@dataclass(frozen=True)
class SmaWithFilterCanonicalFeatureProjection:
    """Canonical SMA/filter/regime feature material for decision input projection."""

    candle_index: int
    candle_ts: int
    through_ts_ms: int | None
    closes: tuple[float, ...]
    prev_s: float
    prev_l: float
    curr_s: float
    curr_l: float
    gap_ratio: float
    volatility_ratio: float
    overextended_ratio: float
    previous_cross_state: str
    allow_initial_cross: bool
    market_regime_snapshot: dict[str, object]

    def policy_input_payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "candle_ts": int(self.candle_ts),
            "prev_s": float(self.prev_s),
            "prev_l": float(self.prev_l),
            "curr_s": float(self.curr_s),
            "curr_l": float(self.curr_l),
            "gap_ratio": float(self.gap_ratio),
            "volatility_ratio": float(self.volatility_ratio),
            "overextended_ratio": float(self.overextended_ratio),
            "previous_cross_state": str(self.previous_cross_state),
            "allow_initial_cross": bool(self.allow_initial_cross),
            "market_regime_snapshot": dict(self.market_regime_snapshot),
        }

    @property
    def feature_hash(self) -> str:
        return _stable_hash(self.policy_input_payload())

    def diagnostics_payload(self) -> dict[str, object]:
        return {
            "candle_index": self.candle_index,
            "candle_ts": self.candle_ts,
            "through_ts_ms": self.through_ts_ms,
            "prev_s": self.prev_s,
            "prev_l": self.prev_l,
            "curr_s": self.curr_s,
            "curr_l": self.curr_l,
            "gap_ratio": self.gap_ratio,
            "volatility_ratio": self.volatility_ratio,
            "overextended_ratio": self.overextended_ratio,
            "previous_cross_state": self.previous_cross_state,
            "allow_initial_cross": self.allow_initial_cross,
            "market_regime_snapshot": dict(self.market_regime_snapshot),
            "market_feature_hash": self.feature_hash,
            "canonical_feature_projection_hash": self.feature_hash,
            "feature_authority": "SmaWithFilterSnapshotProjector.project_features",
        }


class SmaWithFilterSnapshotProjector:
    """Canonical projector for SMA decision input material."""

    version = "sma_with_filter_snapshot_projector_v1"

    def __init__(self, assembly: SmaWithFilterPolicyAssembly | None = None) -> None:
        self.assembly = assembly or SmaWithFilterPolicyAssembly()

    @property
    def projector_hash(self) -> str:
        return _stable_hash(
            {
                "projector": self.__class__.__name__,
                "version": self.version,
                "authority": "canonical_strategy_decision_input_bundle",
            }
        )

    def project_from_research_event(
        self,
        *,
        event: Any,
        dataset: DatasetSnapshot,
        candle_index: int,
        position: PositionSnapshot,
        parameter_values: dict[str, Any],
        fee_rate: float,
        slippage_bps: float,
        active_exit_policy: dict[str, Any],
        buy_fraction: float,
        materialization_mode: MaterializationMode | str,
        candidate_regime_policy: dict[str, object] | None,
        candidate_regime_policy_enforced: bool | None,
    ) -> SmaWithFilterProjectedDecisionInput | None:
        seed = PromotionDecisionSeed.from_research_event(event=event, candle_index=candle_index)
        materialized = self.assembly.materialize_parameters(
            {**dict(parameter_values), "BUY_FRACTION": buy_fraction},
            materialization_mode,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
        )
        features = self.project_features_from_dataset(
            dataset=dataset,
            candle_index=seed.candle_index,
            materialized=materialized,
            through_ts_ms=seed.candle_ts,
            allow_initial_cross=True,
        )
        if features is None:
            return None
        market = self.assembly.build_market_snapshot(
            pair=dataset.market,
            interval=dataset.interval,
            candle_ts=features.candle_ts,
            closes=features.closes,
            prev_s=features.prev_s,
            prev_l=features.prev_l,
            curr_s=features.curr_s,
            curr_l=features.curr_l,
            gap_ratio=features.gap_ratio,
            volatility_ratio=features.volatility_ratio,
            overextended_ratio=features.overextended_ratio,
            market_regime_snapshot=features.market_regime_snapshot,
            through_ts_ms=features.through_ts_ms,
            previous_cross_state=features.previous_cross_state,
            allow_initial_cross=features.allow_initial_cross,
        )
        strategy = self.assembly.build_strategy(
            materialized,
            pair=dataset.market,
            interval=dataset.interval,
            candidate_regime_policy=candidate_regime_policy,
        )
        config = self.assembly.build_policy_config(
            materialized,
            strategy,
            candidate_regime_policy=candidate_regime_policy,
            candidate_regime_policy_enforced=candidate_regime_policy_enforced,
        )
        fee = float(materialized.values.get("LIVE_FEE_RATE_ESTIMATE") or fee_rate)
        execution = self.assembly.build_execution_snapshot(
            materialized,
            pair=dataset.market,
            fee_rate_for_decision=fee,
        )
        exit_policy_config = self.assembly.build_exit_policy_config(
            materialized,
            fee_rate_for_decision=fee,
        )
        common_exit_rule_names = set(active_exit_policy.get("common_rules") or ())
        strategy_exit_rule_names = set(active_exit_policy.get("strategy_rules") or ())
        rule_names = tuple(str(name).strip().lower() for name in active_exit_policy.get("rules") or () if str(name).strip())
        if not common_exit_rule_names and not strategy_exit_rule_names:
            rule_sources = _default_sma_exit_rule_sources(rule_names)
        else:
            rule_sources = {
                name: (
                    "common_risk_and_plugin"
                    if name in common_exit_rule_names and name in strategy_exit_rule_names
                    else "common_risk"
                    if name in common_exit_rule_names
                    else "plugin"
                    if name in strategy_exit_rule_names
                    else "unknown"
                )
                for name in rule_names
            }
        materialized_hash = materialized_strategy_parameters_hash(dict(materialized.values))
        provenance = {
            "projection_source": "promotion_decision_seed",
            **seed.provenance_payload(),
            "runtime_comparable": bool(materialized.runtime_comparable),
            "policy_materialization_mode": materialized.mode.value,
            "candidate_regime_policy_enforced": candidate_regime_policy_enforced,
                "canonical_feature_projection": features.diagnostics_payload(),
                "market_feature_hash": features.feature_hash,
                "canonical_feature_projection_hash": features.feature_hash,
            }
        bundle = StrategyDecisionInputBundle.build(
            strategy_name=strategy.name,
            market=market,
            position=position,
            config=config,
            execution_constraints=execution,
            exit_policy_config=exit_policy_config,
            materialized_parameters_hash=materialized_hash,
            snapshot_projector_version=self.version,
            snapshot_projector_hash=self.projector_hash,
            provenance=provenance,
        )
        replay_fingerprint = self.build_replay_fingerprint(
            strategy_name=strategy.name,
            pair=dataset.market,
            interval=dataset.interval,
            candle_ts=features.candle_ts,
            through_ts_ms=features.through_ts_ms,
            materialized=materialized,
            bundle=bundle,
            regime_version=str((market.market_regime_snapshot or {}).get("version") or ""),
        )
        return SmaWithFilterProjectedDecisionInput(
            strategy=strategy,
            materialized=materialized,
            bundle=bundle,
            rule_sources=rule_sources,
            replay_fingerprint=replay_fingerprint,
        )

    def project_features_from_dataset(
        self,
        *,
        dataset: DatasetSnapshot,
        candle_index: int,
        materialized: MaterializedSmaWithFilterParameters,
        through_ts_ms: int | None,
        allow_initial_cross: bool,
    ) -> SmaWithFilterCanonicalFeatureProjection | None:
        candles = dataset.candles
        if not candles:
            return None
        return self.project_features_from_arrays(
            pair=dataset.market,
            interval=dataset.interval,
            ts_list=[int(item.ts) for item in candles],
            closes=[float(item.close) for item in candles],
            highs=[float(item.high) for item in candles],
            lows=[float(item.low) for item in candles],
            volumes=[float(item.volume) for item in candles],
            materialized=materialized,
            candle_index=int(candle_index),
            through_ts_ms=through_ts_ms,
            allow_initial_cross=allow_initial_cross,
        )

    def project_features_from_arrays(
        self,
        *,
        pair: str,
        interval: str,
        ts_list: list[int],
        closes: list[float],
        highs: list[float],
        lows: list[float],
        volumes: list[float],
        materialized: MaterializedSmaWithFilterParameters,
        candle_index: int | None = None,
        through_ts_ms: int | None,
        allow_initial_cross: bool,
    ) -> SmaWithFilterCanonicalFeatureProjection | None:
        del pair, interval
        short_n = int(materialized.values["SMA_SHORT"])
        long_n = int(materialized.values["SMA_LONG"])
        if short_n <= 0 or long_n <= 0 or short_n >= long_n:
            raise ValueError("sma_feature_projection_invalid_window")
        if len(ts_list) != len(closes):
            return None
        index = len(closes) - 1 if candle_index is None else int(candle_index)
        if index < long_n or index >= len(closes):
            return None
        end_prev = index
        end_curr = index + 1
        prev_s = _sma(closes, short_n, end_prev)
        prev_l = _sma(closes, long_n, end_prev)
        curr_s = _sma(closes, short_n, end_curr)
        curr_l = _sma(closes, long_n, end_curr)
        if prev_s > prev_l:
            previous_cross_state = "above"
        elif prev_s < prev_l:
            previous_cross_state = "below"
        else:
            previous_cross_state = "unknown"
        volatility_window = max(1, int(materialized.values["SMA_FILTER_VOL_WINDOW"]))
        overextended_lookback = max(1, int(materialized.values["SMA_FILTER_OVEREXT_LOOKBACK"]))
        overextended_max_return_ratio = float(materialized.values["SMA_FILTER_OVEREXT_MAX_RETURN_RATIO"])
        min_gap_ratio = float(materialized.values["SMA_FILTER_GAP_MIN_RATIO"])
        min_volatility_ratio = float(materialized.values["SMA_FILTER_VOL_MIN_RANGE_RATIO"])
        market_regime_snapshot = sma_with_filter_events.classify_market_regime_from_arrays(
            closes=[float(value) for value in closes],
            highs=[float(value) for value in highs],
            lows=[float(value) for value in lows],
            volumes=[float(value) for value in volumes],
            index=index,
            short_sma=float(curr_s),
            long_sma=float(curr_l),
            volatility_window=volatility_window,
            volume_window=max(1, int(materialized.values.get("SMA_FILTER_VOLUME_WINDOW", 10))),
            liquidity_window=max(1, int(materialized.values.get("SMA_FILTER_LIQUIDITY_WINDOW", 10))),
            thresholds=MarketRegimeThresholds(
                min_trend_strength_ratio=max(0.0, min_gap_ratio),
                low_volatility_ratio=max(0.0, min_volatility_ratio),
            ),
            overextended_lookback=overextended_lookback,
            overextended_max_return_ratio=overextended_max_return_ratio,
        ).as_dict()
        return SmaWithFilterCanonicalFeatureProjection(
            candle_index=int(index),
            candle_ts=int(ts_list[index]),
            through_ts_ms=None if through_ts_ms is None else int(through_ts_ms),
            closes=tuple(float(value) for value in closes[:end_curr]),
            prev_s=float(prev_s),
            prev_l=float(prev_l),
            curr_s=float(curr_s),
            curr_l=float(curr_l),
            gap_ratio=abs((float(curr_s) - float(curr_l)) / float(curr_l)) if float(curr_l) != 0.0 else 0.0,
            volatility_ratio=_rolling_close_range_ratio(closes, volatility_window, index),
            overextended_ratio=_overextended_return_ratio(closes, overextended_lookback, index),
            previous_cross_state=previous_cross_state,
            allow_initial_cross=bool(allow_initial_cross and previous_cross_state == "unknown"),
            market_regime_snapshot=market_regime_snapshot,
        )

    def project_from_runtime_projection(
        self,
        *,
        projection: SmaWithFilterRuntimeProjectionResult,
    ) -> StrategyDecisionInputBundle:
        evidence = projection.evidence_payload(
            projector_version=self.version,
            projector_hash=self.projector_hash,
        )
        return StrategyDecisionInputBundle.build(
            strategy_name=str(getattr(projection.strategy, "name", "sma_with_filter")),
            market=projection.market,
            position=projection.position,
            config=projection.config,
            execution_constraints=projection.execution_constraints,
            exit_policy_config=projection.exit_policy_config,
            materialized_parameters_hash=str(evidence["materialized_parameters_hash"]),
            snapshot_projector_version=self.version,
            snapshot_projector_hash=self.projector_hash,
            provenance={
                "projection_source": "validated_runtime_projection",
                "runtime_comparable": bool(projection.materialized.runtime_comparable),
                "policy_materialization_mode": projection.materialized.mode.value,
                "runtime_projection_evidence": evidence,
                **dict(projection.provenance or {}),
            },
        )

    def build_replay_fingerprint(
        self,
        *,
        strategy_name: str,
        pair: str,
        interval: str,
        candle_ts: int,
        through_ts_ms: int | None,
        materialized: MaterializedSmaWithFilterParameters,
        bundle: StrategyDecisionInputBundle,
        regime_version: str,
        policy_input_hash: str | None = None,
        policy_decision_hash: str | None = None,
        policy_contract_hash: str | None = None,
    ) -> dict[str, object]:
        thresholds = {
            "sma_filter_gap_min_ratio": float(materialized.values["SMA_FILTER_GAP_MIN_RATIO"]),
            "sma_filter_vol_window": int(materialized.values["SMA_FILTER_VOL_WINDOW"]),
            "sma_filter_vol_min_range_ratio": float(materialized.values["SMA_FILTER_VOL_MIN_RANGE_RATIO"]),
            "sma_filter_overext_lookback": int(materialized.values["SMA_FILTER_OVEREXT_LOOKBACK"]),
            "sma_filter_overext_max_return_ratio": float(
                materialized.values["SMA_FILTER_OVEREXT_MAX_RETURN_RATIO"]
            ),
            "sma_cost_edge_enabled": _coerce_bool(materialized.values["SMA_COST_EDGE_ENABLED"]),
            "sma_cost_edge_min_ratio": float(materialized.values["SMA_COST_EDGE_MIN_RATIO"]),
            "strategy_min_expected_edge_ratio": float(
                materialized.values["STRATEGY_MIN_EXPECTED_EDGE_RATIO"]
            ),
            "entry_edge_buffer_ratio": float(materialized.values["ENTRY_EDGE_BUFFER_RATIO"]),
            "market_regime_enabled": _coerce_bool(materialized.values["SMA_MARKET_REGIME_ENABLED"]),
            "materialization_mode": materialized.mode.value,
            "runtime_comparable": bool(materialized.runtime_comparable),
        }
        payload = self.assembly.build_replay_fingerprint_payload(
            strategy_name=strategy_name,
            pair=pair,
            interval=interval,
            candle_ts=int(candle_ts),
            through_ts_ms=None if through_ts_ms is None else int(through_ts_ms),
            materialized=materialized,
            thresholds=thresholds,
            fee_authority=bundle.execution_constraints.policy_input_payload().get("fee_authority", {}),
            slippage_bps=float(materialized.values["STRATEGY_ENTRY_SLIPPAGE_BPS"]),
            regime_version=regime_version,
            policy_input_payload=bundle.payload(),
            policy_input_hash=policy_input_hash or bundle.decision_input_bundle_hash,
            exit_policy_hash=bundle.exit_policy_config_hash,
        )
        payload.update(
            {
                "decision_input_bundle_hash": bundle.decision_input_bundle_hash,
                "decision_input_contract_hash": bundle.decision_input_contract_hash,
                "decision_input_bundle_payload_hash": bundle.decision_input_bundle_payload_hash,
                "snapshot_projector_version": bundle.snapshot_projector_version,
                "snapshot_projector_hash": bundle.snapshot_projector_hash,
                "market_snapshot_hash": bundle.market_snapshot_hash,
                "market_feature_hash": bundle.market_feature_hash,
                "canonical_feature_projection_hash": bundle.market_feature_hash,
                "position_snapshot_hash": bundle.position_snapshot_hash,
                "execution_constraints_hash": bundle.execution_constraints_hash,
                "policy_config_hash": bundle.policy_config_hash,
                "exit_policy_config_hash": bundle.exit_policy_config_hash,
                "policy_decision_hash": policy_decision_hash or "",
                "policy_contract_hash": policy_contract_hash or "",
            }
        )
        return payload


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _default_sma_exit_rule_sources(rule_names: tuple[str, ...]) -> dict[str, str]:
    return {
        name: "common_risk" if name in {"stop_loss", "max_holding_time"} else "plugin"
        for name in rule_names
    }


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(float(value) for value in values[end - n : end]) / n


def _rolling_close_range_ratio(values: list[float], window: int, index: int) -> float:
    window = max(1, int(window))
    start = max(0, int(index) - window + 1)
    subset = [float(value) for value in values[start : int(index) + 1]]
    if not subset:
        return 0.0
    mean = sum(subset) / len(subset)
    return ((max(subset) - min(subset)) / mean) if mean != 0.0 else 0.0


def _overextended_return_ratio(values: list[float], lookback: int, index: int) -> float:
    lookback = max(1, int(lookback))
    if int(index) < lookback:
        return 0.0
    base = float(values[int(index) - lookback])
    return abs((float(values[int(index)]) - base) / base) if base != 0.0 else 0.0


__all__ = [
    "PromotionDecisionSeed",
    "SmaWithFilterCanonicalFeatureProjection",
    "SmaWithFilterProjectedDecisionInput",
    "SmaWithFilterRuntimeProjectionResult",
    "SmaWithFilterSnapshotProjector",
]
