from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.config import settings
from bithumb_bot.runtime_position_state_normalizer import PositionStateNormalizer
from bithumb_bot.runtime_sma_snapshot import decide_sma_with_filter_runtime_snapshot_from_db
from bithumb_bot.runtime_sma_snapshot_builder import (
    RuntimeSmaDecisionResult,
    _latest_signal_close,
    _resolve_signal_through_ts_ms,
)
from bithumb_bot.runtime_strategy_decision import RuntimeStrategyDecisionResult
from bithumb_bot.strategy_plugins.sma_with_filter_assembly import (
    MaterializationMode,
    MaterializedSmaWithFilterParameters,
    SmaWithFilterPolicyAssembly,
)
from bithumb_bot.strategy.sma_policy_strategy import SmaWithFilterStrategy


def _normalization_boundary_label() -> str:
    return "runtime_adapters.sma_with_filter.normalize_position_state_before_strategy_decision"


def normalize_position_state_before_strategy_decision(
    conn,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    normalizer: PositionStateNormalizer | None = None,
) -> int:
    signal_through_ts_ms = _resolve_signal_through_ts_ms(
        interval=strategy.interval,
        through_ts_ms=through_ts_ms,
    )
    if signal_through_ts_ms is None:
        return 0
    market_price = _latest_signal_close(
        conn,
        pair=strategy.pair,
        interval=strategy.interval,
        through_ts_ms=signal_through_ts_ms,
    )
    if market_price is None:
        return 0
    return (normalizer or PositionStateNormalizer()).normalize_and_persist(
        conn,
        pair=strategy.pair,
        market_price=float(market_price),
        slippage_bps=float(strategy.slippage_bps),
        entry_edge_buffer_ratio=float(strategy.entry_edge_buffer_ratio),
    )


def normalize_position_state_for_runtime_decision(
    conn,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    normalizer: PositionStateNormalizer | None = None,
) -> dict[str, object]:
    updated_count = normalize_position_state_before_strategy_decision(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        normalizer=normalizer,
    )
    return {
        "normalization_boundary": _normalization_boundary_label(),
        "normalization_updated_count": int(updated_count),
        "decision_boundary_phase": "pre_decision_normalization_complete",
    }


def build_read_only_strategy_decision_snapshot(
    conn,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    result = decide_sma_with_filter_runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
    )
    if result is not None and boundary_telemetry:
        boundary = {**dict(result.boundary), **dict(boundary_telemetry)}
        boundary["decision_boundary_phase"] = "post_normalization_decision"
        result.base_context.update(boundary)
        object.__setattr__(result, "boundary", boundary)
    return result


def compute_strategy_decision_after_normalization(
    conn,
    strategy: SmaWithFilterStrategy,
    *,
    through_ts_ms: int | None = None,
    boundary_telemetry: dict[str, object] | None = None,
) -> RuntimeSmaDecisionResult | None:
    return build_read_only_strategy_decision_snapshot(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        boundary_telemetry=boundary_telemetry,
    )


@dataclass(frozen=True)
class SmaWithFilterRuntimeConfig:
    pair: str
    interval: str
    materialized: MaterializedSmaWithFilterParameters

    @classmethod
    def from_runtime_request(cls, request) -> "SmaWithFilterRuntimeConfig":
        pair = str(getattr(request, "pair", "") or "").strip()
        interval = str(getattr(request, "interval", "") or "").strip()
        if not pair:
            raise RuntimeError("sma_runtime_request_pair_missing")
        if not interval:
            raise RuntimeError("sma_runtime_request_interval_missing")
        raw_params = dict(request.parameters or {})
        return cls.from_parameter_payload(pair=pair, interval=interval, parameters=raw_params)

    @classmethod
    def from_profile(cls, profile: dict[str, object]) -> "SmaWithFilterRuntimeConfig":
        pair = str(profile.get("market") or "").strip()
        interval = str(profile.get("interval") or "").strip()
        if not pair:
            raise RuntimeError("sma_runtime_profile_market_missing")
        if not interval:
            raise RuntimeError("sma_runtime_profile_interval_missing")
        params = profile.get("strategy_parameters") if isinstance(profile.get("strategy_parameters"), dict) else {}
        return cls.from_parameter_payload(pair=pair, interval=interval, parameters=dict(params))

    @classmethod
    def from_parameter_payload(
        cls,
        *,
        pair: str,
        interval: str,
        parameters: dict[str, object],
    ) -> "SmaWithFilterRuntimeConfig":
        raw_params = dict(parameters or {})
        try:
            materialized = SmaWithFilterPolicyAssembly().materialize_parameters(
                raw_params,
                MaterializationMode.RUNTIME_REPLAY,
            )
        except Exception as exc:
            message = str(exc)
            if "runtime_bound_parameter_missing:" in message:
                missing = message.split("runtime_bound_parameter_missing:", 1)[1]
                raise RuntimeError(
                    "sma_runtime_request_behavior_parameter_missing:" + missing
                ) from exc
            raise RuntimeError(str(exc)) from exc
        return cls(pair=pair, interval=interval, materialized=materialized)

    @staticmethod
    def runtime_parameter_names() -> tuple[str, ...]:
        return SmaWithFilterPolicyAssembly().runtime_parameter_names()

    def build_strategy(
        self,
        *,
        candidate_regime_policy: dict[str, object] | None = None,
    ) -> SmaWithFilterStrategy:
        assembly = SmaWithFilterPolicyAssembly()
        return assembly.build_strategy(
            self.materialized,
            pair=self.pair,
            interval=self.interval,
            candidate_regime_policy=candidate_regime_policy,
        )


@dataclass(frozen=True)
class SmaWithFilterRuntimeDecisionAdapter:
    strategy_name: str = "sma_with_filter"

    def decide(
        self,
        conn,
        request,
    ) -> RuntimeStrategyDecisionResult | None:
        strategy = SmaWithFilterRuntimeConfig.from_runtime_request(request).build_strategy()
        if not isinstance(strategy, SmaWithFilterStrategy):
            raise RuntimeError(f"strategy_policy_invalid:{self.strategy_name}")
        boundary_telemetry = normalize_position_state_for_runtime_decision(
            conn,
            strategy,
            through_ts_ms=request.through_ts_ms,
        )
        return compute_strategy_decision_after_normalization(
            conn,
            strategy,
            through_ts_ms=request.through_ts_ms,
            boundary_telemetry=boundary_telemetry,
        )

    def typed_authority_required(self) -> bool:
        mode = str(settings.MODE or "").strip().lower()
        if mode == "live":
            return True
        if str(getattr(settings, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip():
            return True
        return True


def compute_sma_with_filter_signal(
    conn,
    short_n: int | None = None,
    long_n: int | None = None,
    *,
    through_ts_ms: int | None = None,
) -> dict[str, object] | None:
    from bithumb_bot.runtime_strategy_decision import _attach_runtime_request_metadata
    from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec
    from bithumb_bot.research.strategy_registry import runtime_strategy_parameters_from_settings

    parameters = runtime_strategy_parameters_from_settings("sma_with_filter", settings)
    parameters["SMA_SHORT"] = int(settings.SMA_SHORT if short_n is None else short_n)
    parameters["SMA_LONG"] = int(settings.SMA_LONG if long_n is None else long_n)

    request = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            strategy_name="sma_with_filter",
            parameters=parameters,
            parameter_source="sma_diagnostic_arguments",
        ),
        through_ts_ms=through_ts_ms,
    )
    result = SmaWithFilterRuntimeDecisionAdapter().decide(conn, request)
    if result is None:
        return None
    _attach_runtime_request_metadata(result, request)
    payload = result.as_legacy_dict()
    payload.setdefault("strategy", result.decision.strategy_name)
    return payload
