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
from bithumb_bot.research.strategy_spec import materialize_strategy_parameters, runtime_bound_behavior_parameter_names
from bithumb_bot.strategy.sma_policy_strategy import SmaWithFilterStrategy, create_sma_with_filter_strategy


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
    short_n: int
    long_n: int
    min_gap_ratio: float
    volatility_window: int
    min_volatility_ratio: float
    overextended_lookback: int
    overextended_max_return_ratio: float
    cost_edge_enabled: bool
    cost_edge_min_ratio: float
    market_regime_enabled: bool
    entry_edge_buffer_ratio: float
    slippage_bps: float
    live_fee_rate_estimate: float
    exit_rule_names: tuple[str, ...]
    exit_stop_loss_ratio: float
    exit_max_holding_min: int
    exit_min_take_profit_ratio: float
    exit_small_loss_tolerance_ratio: float

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
        runtime_bound = runtime_bound_behavior_parameter_names("sma_with_filter")
        missing = tuple(name for name in runtime_bound if name not in raw_params)
        if missing:
            raise RuntimeError(
                "sma_runtime_request_behavior_parameter_missing:" + ",".join(sorted(missing))
            )
        params = materialize_strategy_parameters("sma_with_filter", raw_params)
        config = cls(
            pair=pair,
            interval=interval,
            short_n=int(params["SMA_SHORT"]),
            long_n=int(params["SMA_LONG"]),
            min_gap_ratio=float(params["SMA_FILTER_GAP_MIN_RATIO"]),
            volatility_window=int(params["SMA_FILTER_VOL_WINDOW"]),
            min_volatility_ratio=float(params["SMA_FILTER_VOL_MIN_RANGE_RATIO"]),
            overextended_lookback=int(params["SMA_FILTER_OVEREXT_LOOKBACK"]),
            overextended_max_return_ratio=float(params["SMA_FILTER_OVEREXT_MAX_RETURN_RATIO"]),
            cost_edge_enabled=_coerce_bool(params["SMA_COST_EDGE_ENABLED"]),
            cost_edge_min_ratio=float(params["SMA_COST_EDGE_MIN_RATIO"]),
            market_regime_enabled=_coerce_bool(params["SMA_MARKET_REGIME_ENABLED"]),
            entry_edge_buffer_ratio=float(params["ENTRY_EDGE_BUFFER_RATIO"]),
            slippage_bps=float(params["STRATEGY_ENTRY_SLIPPAGE_BPS"]),
            live_fee_rate_estimate=float(params["LIVE_FEE_RATE_ESTIMATE"]),
            exit_rule_names=tuple(
                token.strip()
                for token in str(params["STRATEGY_EXIT_RULES"]).split(",")
                if token.strip()
            ),
            exit_stop_loss_ratio=float(params["STRATEGY_EXIT_STOP_LOSS_RATIO"]),
            exit_max_holding_min=int(params["STRATEGY_EXIT_MAX_HOLDING_MIN"]),
            exit_min_take_profit_ratio=float(params["STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO"]),
            exit_small_loss_tolerance_ratio=float(params["STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO"]),
        )
        config_keys = set(config.runtime_parameter_names())
        missing_from_config = sorted(set(runtime_bound) - config_keys)
        if missing_from_config:
            raise RuntimeError(
                "sma_runtime_config_unmapped_behavior_parameter:" + ",".join(missing_from_config)
            )
        return config

    @staticmethod
    def runtime_parameter_names() -> tuple[str, ...]:
        return (
            "SMA_SHORT",
            "SMA_LONG",
            "SMA_FILTER_GAP_MIN_RATIO",
            "SMA_FILTER_VOL_WINDOW",
            "SMA_FILTER_VOL_MIN_RANGE_RATIO",
            "SMA_FILTER_OVEREXT_LOOKBACK",
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
            "SMA_COST_EDGE_ENABLED",
            "SMA_COST_EDGE_MIN_RATIO",
            "SMA_MARKET_REGIME_ENABLED",
            "ENTRY_EDGE_BUFFER_RATIO",
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
            "STRATEGY_ENTRY_SLIPPAGE_BPS",
            "LIVE_FEE_RATE_ESTIMATE",
            "STRATEGY_EXIT_RULES",
            "STRATEGY_EXIT_STOP_LOSS_RATIO",
            "STRATEGY_EXIT_MAX_HOLDING_MIN",
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
        )

    def build_strategy(
        self,
        *,
        candidate_regime_policy: dict[str, object] | None = None,
    ) -> SmaWithFilterStrategy:
        return create_sma_with_filter_strategy(
            short_n=self.short_n,
            long_n=self.long_n,
            pair=self.pair,
            interval=self.interval,
            min_gap_ratio=self.min_gap_ratio,
            volatility_window=self.volatility_window,
            min_volatility_ratio=self.min_volatility_ratio,
            overextended_lookback=self.overextended_lookback,
            overextended_max_return_ratio=self.overextended_max_return_ratio,
            cost_edge_enabled=self.cost_edge_enabled,
            cost_edge_min_ratio=self.cost_edge_min_ratio,
            market_regime_enabled=self.market_regime_enabled,
            entry_edge_buffer_ratio=self.entry_edge_buffer_ratio,
            slippage_bps=self.slippage_bps,
            live_fee_rate_estimate=self.live_fee_rate_estimate,
            exit_rule_names=list(self.exit_rule_names),
            exit_stop_loss_ratio=self.exit_stop_loss_ratio,
            exit_max_holding_min=self.exit_max_holding_min,
            exit_min_take_profit_ratio=self.exit_min_take_profit_ratio,
            exit_small_loss_tolerance_ratio=self.exit_small_loss_tolerance_ratio,
            candidate_regime_policy=candidate_regime_policy,
            legacy_candidate_regime_policy_fallback=False,
        )


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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
