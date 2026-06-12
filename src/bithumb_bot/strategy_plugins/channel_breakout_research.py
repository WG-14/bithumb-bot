from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean
from time import gmtime, strftime
from collections.abc import Iterator
from typing import Any

from bithumb_bot.market_regime import classify_market_regime_from_arrays
from bithumb_bot.research.backtest_types import BacktestRunContext
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.execution_timing import candle_close_ts
from bithumb_bot.research.experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy
from bithumb_bot.research.prepared_candles import PreparedCandleArrays, prepare_candle_arrays
from bithumb_bot.research.strategy_registry import ResearchStrategyPlugin
from bithumb_bot.research.strategy_spec import (
    StrategyParameterSchema,
    StrategySpec,
    StrategySpecError,
    materialize_strategy_parameters,
)
from bithumb_bot.strategy_authoring import research_plugin_from_event_builder


CHANNEL_BREAKOUT_STRATEGY_NAME = "channel_breakout_with_regime_filter"
_SUPPORTED_ENTRY_MODES = frozenset({"immediate_breakout", "delayed_confirmation"})

CHANNEL_BREAKOUT_COMPLEXITY_METADATA = {
    "schema_version": 1,
    "complexity_class": "linear_precomputed_ohlcv",
    "expected_us_per_candle": 25,
    "precompute_required": True,
    "precompute_path": "prepare_channel_breakout_context",
}


def estimate_channel_breakout_complexity(
    *,
    strategy_name: str,
    parameter_space: dict[str, Any] | None = None,
    report_detail: str = "summary",
    diagnostic_mode: str = "exploratory",
    audit_trail: Any | None = None,
    expected_candle_count: int | None = None,
) -> dict[str, Any]:
    modes = _parameter_values_for_key(parameter_space or {}, "ENTRY_MODE")
    unsupported_modes = sorted(str(mode) for mode in modes if str(mode) not in _SUPPORTED_ENTRY_MODES)
    includes_delayed = "delayed_confirmation" in {str(mode) for mode in modes}
    full_observability = str(report_detail or "").lower() == "full" or bool(
        getattr(audit_trail, "complete_external", False)
    )
    expected_us = int(CHANNEL_BREAKOUT_COMPLEXITY_METADATA["expected_us_per_candle"])
    decision_payload_bytes = 384
    feature_snapshot_bytes = 512
    reasons = ["linear_precomputed_ohlcv"]
    if includes_delayed:
        expected_us += 15
        decision_payload_bytes += 256
        feature_snapshot_bytes += 256
        reasons.append("delayed_confirmation_pending_state")
    if full_observability:
        decision_payload_bytes *= 3
        feature_snapshot_bytes *= 2
        reasons.append("full_observability_payloads")
    if unsupported_modes:
        reasons.append("unsupported_entry_mode:" + ",".join(unsupported_modes))
    return {
        "schema_version": 1,
        "strategy_name": strategy_name,
        "expected_candle_count": expected_candle_count,
        "expected_us_per_candle": expected_us,
        "expected_feature_snapshot_bytes_per_event": feature_snapshot_bytes,
        "expected_decision_payload_bytes_per_event": decision_payload_bytes,
        "complexity_reasons": tuple(reasons),
        "unsupported_parameter_values": {"ENTRY_MODE": tuple(unsupported_modes)} if unsupported_modes else {},
    }


def _parameter_values_for_key(parameter_space: dict[str, Any], key: str) -> tuple[Any, ...]:
    if key not in parameter_space:
        return (CHANNEL_BREAKOUT_SPEC.default_parameters.get(key),)
    raw = parameter_space.get(key)
    if isinstance(raw, (list, tuple, set, frozenset)):
        return tuple(raw)
    return (raw,)

CHANNEL_BREAKOUT_SPEC = StrategySpec(
    strategy_name=CHANNEL_BREAKOUT_STRATEGY_NAME,
    strategy_version="channel_breakout_with_regime_filter.research_contract.v1",
    accepted_parameter_names=(
        "CHANNEL_BREAKOUT_LOOKBACK",
        "CHANNEL_BREAKOUT_RANGE_WINDOW",
        "CHANNEL_BREAKOUT_RANGE_RATIO_MIN",
        "CHANNEL_BREAKOUT_VOLUME_WINDOW",
        "CHANNEL_BREAKOUT_VOLUME_RATIO_MIN",
        "CHANNEL_BREAKOUT_REGIME_FILTER_ENABLED",
        "ENTRY_MODE",
        "CONFIRMATION_WINDOW_MIN",
        "PULLBACK_RATIO",
        "COOLDOWN_MIN",
        "MAX_TRADES_PER_DAY",
        "STRATEGY_EXIT_RULES",
        "STRATEGY_EXIT_STOP_LOSS_RATIO",
        "STRATEGY_EXIT_MAX_HOLDING_MIN",
        "TAKE_PROFIT_RATIO",
        "TRAILING_STOP_RATIO",
        "BREAK_EVEN_STOP_ENABLED",
        "OPPOSITE_SIGNAL_EXIT_ENABLED",
        "REGIME_CHANGE_EXIT_ENABLED",
    ),
    required_parameter_names=(
        "CHANNEL_BREAKOUT_LOOKBACK",
        "CHANNEL_BREAKOUT_RANGE_WINDOW",
        "CHANNEL_BREAKOUT_VOLUME_WINDOW",
    ),
    behavior_affecting_parameter_names=(
        "CHANNEL_BREAKOUT_LOOKBACK",
        "CHANNEL_BREAKOUT_RANGE_WINDOW",
        "CHANNEL_BREAKOUT_RANGE_RATIO_MIN",
        "CHANNEL_BREAKOUT_VOLUME_WINDOW",
        "CHANNEL_BREAKOUT_VOLUME_RATIO_MIN",
        "CHANNEL_BREAKOUT_REGIME_FILTER_ENABLED",
        "ENTRY_MODE",
        "CONFIRMATION_WINDOW_MIN",
        "PULLBACK_RATIO",
        "COOLDOWN_MIN",
        "MAX_TRADES_PER_DAY",
        "STRATEGY_EXIT_RULES",
        "STRATEGY_EXIT_STOP_LOSS_RATIO",
        "STRATEGY_EXIT_MAX_HOLDING_MIN",
        "TAKE_PROFIT_RATIO",
        "TRAILING_STOP_RATIO",
        "BREAK_EVEN_STOP_ENABLED",
        "OPPOSITE_SIGNAL_EXIT_ENABLED",
        "REGIME_CHANGE_EXIT_ENABLED",
    ),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={
        "CHANNEL_BREAKOUT_RANGE_RATIO_MIN": 1.2,
        "CHANNEL_BREAKOUT_VOLUME_RATIO_MIN": 1.1,
        "CHANNEL_BREAKOUT_REGIME_FILTER_ENABLED": True,
        "ENTRY_MODE": "immediate_breakout",
        "CONFIRMATION_WINDOW_MIN": 0,
        "PULLBACK_RATIO": 0.0,
        "COOLDOWN_MIN": 0,
        "MAX_TRADES_PER_DAY": 0,
        "STRATEGY_EXIT_RULES": "stop_loss,max_holding_time",
        "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.01,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 30,
        "TAKE_PROFIT_RATIO": 0.0,
        "TRAILING_STOP_RATIO": 0.0,
        "BREAK_EVEN_STOP_ENABLED": False,
        "OPPOSITE_SIGNAL_EXIT_ENABLED": False,
        "REGIME_CHANGE_EXIT_ENABLED": False,
    },
    parameter_schema=(
        StrategyParameterSchema("CHANNEL_BREAKOUT_LOOKBACK", "int", required=True, min_value=2, unit="candles"),
        StrategyParameterSchema("CHANNEL_BREAKOUT_RANGE_WINDOW", "int", required=True, min_value=2, unit="candles"),
        StrategyParameterSchema("CHANNEL_BREAKOUT_RANGE_RATIO_MIN", "float", min_value=0.0, unit="range_ratio"),
        StrategyParameterSchema("CHANNEL_BREAKOUT_VOLUME_WINDOW", "int", required=True, min_value=2, unit="candles"),
        StrategyParameterSchema("CHANNEL_BREAKOUT_VOLUME_RATIO_MIN", "float", min_value=0.0, unit="volume_ratio"),
        StrategyParameterSchema("CHANNEL_BREAKOUT_REGIME_FILTER_ENABLED", "bool", unit="enabled_flag"),
        StrategyParameterSchema(
            "ENTRY_MODE",
            "str",
            enum=(
                "immediate_breakout",
                "pullback_after_breakout",
                "delayed_confirmation",
                "contrarian_after_exhaustion",
            ),
            unit="entry_hypothesis",
        ),
        StrategyParameterSchema("CONFIRMATION_WINDOW_MIN", "int", min_value=0, unit="minutes"),
        StrategyParameterSchema("PULLBACK_RATIO", "float", min_value=0.0, unit="price_ratio"),
        StrategyParameterSchema("COOLDOWN_MIN", "int", min_value=0, unit="minutes"),
        StrategyParameterSchema("MAX_TRADES_PER_DAY", "int", min_value=0, unit="count"),
        StrategyParameterSchema("STRATEGY_EXIT_RULES", "str", unit="comma_separated_exit_rule_names"),
        StrategyParameterSchema("STRATEGY_EXIT_STOP_LOSS_RATIO", "float", min_value=0.0, unit="unrealized_pnl_ratio"),
        StrategyParameterSchema("STRATEGY_EXIT_MAX_HOLDING_MIN", "int", min_value=0, unit="minutes"),
        StrategyParameterSchema("TAKE_PROFIT_RATIO", "float", min_value=0.0, unit="unrealized_pnl_ratio"),
        StrategyParameterSchema("TRAILING_STOP_RATIO", "float", min_value=0.0, unit="unrealized_pnl_ratio"),
        StrategyParameterSchema("BREAK_EVEN_STOP_ENABLED", "bool", unit="enabled_flag"),
        StrategyParameterSchema("OPPOSITE_SIGNAL_EXIT_ENABLED", "bool", unit="enabled_flag"),
        StrategyParameterSchema("REGIME_CHANGE_EXIT_ENABLED", "bool", unit="enabled_flag"),
    ),
    decision_contract_version="research_channel_breakout_decision_contract.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": ("stop_loss", "take_profit", "max_holding_time"),
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
    },
)


@dataclass
class BreakoutPendingState:
    active: bool = False
    breakout_index: int = -1
    breakout_level: float = 0.0
    breakout_close: float = 0.0
    expires_at_index: int = -1


def materialize_channel_breakout_parameters(
    *,
    plugin: ResearchStrategyPlugin,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    context: BacktestRunContext | None = None,
) -> dict[str, Any]:
    del context
    values = materialize_strategy_parameters(
        plugin.name,
        parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
    )
    for name in (
        "CHANNEL_BREAKOUT_LOOKBACK",
        "CHANNEL_BREAKOUT_RANGE_WINDOW",
        "CHANNEL_BREAKOUT_VOLUME_WINDOW",
    ):
        if int(values[name]) < 2:
            raise StrategySpecError(f"{name} must be >= 2")
    rules = _normalize_exit_rules(values.get("STRATEGY_EXIT_RULES") or "")
    unsupported = sorted(set(rules) - {"stop_loss", "take_profit", "max_holding_time"})
    if unsupported:
        raise StrategySpecError(
            "STRATEGY_EXIT_RULES contains unsupported rule(s): " + ",".join(unsupported)
        )
    _validate_supported_entry_mode(values.get("ENTRY_MODE"))
    return values


def decide_channel_breakout_snapshot(
    *,
    candle: Candle,
    candle_index: int,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    candles: tuple[Candle, ...] | None = None,
    closes: tuple[float, ...] | None = None,
    highs: tuple[float, ...] | None = None,
    lows: tuple[float, ...] | None = None,
    volumes: tuple[float, ...] | None = None,
) -> dict[str, Any]:
    if candles is None or closes is None or highs is None or lows is None or volumes is None:
        prepared = prepare_channel_breakout_context(dataset)
        candles = prepared.candles
        closes = prepared.closes
        highs = prepared.highs
        lows = prepared.lows
        volumes = prepared.volumes
    lookback = int(parameter_values["CHANNEL_BREAKOUT_LOOKBACK"])
    range_window = int(parameter_values["CHANNEL_BREAKOUT_RANGE_WINDOW"])
    volume_window = int(parameter_values["CHANNEL_BREAKOUT_VOLUME_WINDOW"])
    min_required_prior = max(lookback, range_window, volume_window)
    close = float(candle.close)
    volume = float(candle.volume)

    if candle_index < min_required_prior:
        regime = classify_market_regime_from_arrays(
            closes=closes,
            highs=highs,
            lows=lows,
            volumes=volumes,
            index=int(candle_index),
            volatility_window=range_window,
            volume_window=volume_window,
            liquidity_window=volume_window,
        )
        feature_snapshot = {
            "schema_version": 1,
            "candle_index": int(candle_index),
            "close": close,
            "rolling_high": 0.0,
            "breakout_distance": 0.0,
            "current_range": float(candle.high) - float(candle.low),
            "avg_range": 0.0,
            "range_ratio": 0.0,
            "volume": volume,
            "avg_volume": 0.0,
            "volume_ratio": 0.0,
            "price_regime": regime.price_regime,
            "volatility_bucket": regime.volatility_bucket,
            "volume_bucket": regime.volume_bucket,
            "liquidity_bucket": regime.liquidity_bucket,
            "composite_regime": regime.composite_regime,
            "blocked_filters": (),
            "required_prior_candles": int(min_required_prior),
        }
        return {
            "signal": "HOLD",
            "reason": "not_enough_lookback",
            "feature_snapshot": feature_snapshot,
            "strategy_diagnostics": {
                "schema_version": 1,
                "blocked_filters": (),
                "regime_filter_enabled": bool(parameter_values["CHANNEL_BREAKOUT_REGIME_FILTER_ENABLED"]),
                "entry_mode": str(parameter_values.get("ENTRY_MODE") or "immediate_breakout"),
            },
        }

    prior_lookback = candles[candle_index - lookback : candle_index]
    prior_range = candles[candle_index - range_window : candle_index]
    prior_volume = candles[candle_index - volume_window : candle_index]
    rolling_high = max(float(item.high) for item in prior_lookback)
    current_range = float(candle.high) - float(candle.low)
    avg_range = fmean(float(item.high) - float(item.low) for item in prior_range)
    range_ratio = _safe_ratio(current_range, avg_range)
    avg_volume = fmean(float(item.volume) for item in prior_volume)
    volume_ratio = _safe_ratio(volume, avg_volume)
    breakout_distance = _safe_ratio(close - rolling_high, rolling_high)

    regime = classify_market_regime_from_arrays(
        closes=closes,
        highs=highs,
        lows=lows,
        volumes=volumes,
        index=int(candle_index),
        volatility_window=range_window,
        volume_window=volume_window,
        liquidity_window=volume_window,
    )

    blocked_filters: list[str] = []
    if close <= rolling_high:
        blocked_filters.append("close_not_above_rolling_high")
    if range_ratio < float(parameter_values["CHANNEL_BREAKOUT_RANGE_RATIO_MIN"]):
        blocked_filters.append("range_ratio_below_min")
    if volume_ratio < float(parameter_values["CHANNEL_BREAKOUT_VOLUME_RATIO_MIN"]):
        blocked_filters.append("volume_ratio_below_min")
    regime_filter_enabled = bool(parameter_values["CHANNEL_BREAKOUT_REGIME_FILTER_ENABLED"])
    if regime_filter_enabled:
        if regime.price_regime == "downtrend":
            blocked_filters.append("downtrend_regime")
        if regime.legacy_regime == "chop" or regime.price_regime == "sideways":
            blocked_filters.append("chop_regime")

    entry_mode = _validate_supported_entry_mode(parameter_values.get("ENTRY_MODE"))
    blocked = tuple(blocked_filters)
    signal = "BUY" if entry_mode == "immediate_breakout" and not blocked else "HOLD"
    confirmation_status = "not_applicable"
    reason = "channel_breakout_confirmed" if signal == "BUY" else "channel_breakout_blocked"
    if entry_mode == "delayed_confirmation":
        confirmation_status = "candidate" if not blocked else "blocked"
        reason = "breakout_pending_confirmation" if not blocked else "channel_breakout_blocked"
    feature_snapshot = {
        "schema_version": 1,
        "candle_index": int(candle_index),
        "close": close,
        "rolling_high": float(rolling_high),
        "breakout_distance": float(breakout_distance),
        "current_range": float(current_range),
        "avg_range": float(avg_range),
        "range_ratio": float(range_ratio),
        "volume": volume,
        "avg_volume": float(avg_volume),
        "volume_ratio": float(volume_ratio),
        "price_regime": regime.price_regime,
        "volatility_bucket": regime.volatility_bucket,
        "volume_bucket": regime.volume_bucket,
        "liquidity_bucket": regime.liquidity_bucket,
        "composite_regime": regime.composite_regime,
        "blocked_filters": blocked,
    }
    if entry_mode == "delayed_confirmation":
        feature_snapshot.update(
            {
                "entry_mode": "delayed_confirmation",
                "breakout_candidate": not blocked,
                "breakout_pending": not blocked,
                "breakout_level": float(rolling_high) if not blocked else 0.0,
                "breakout_index": int(candle_index) if not blocked else -1,
                "confirmation_window_min": int(parameter_values["CONFIRMATION_WINDOW_MIN"]),
                "pending_expires_at_index": (
                    int(candle_index) + int(parameter_values["CONFIRMATION_WINDOW_MIN"])
                    if not blocked
                    else -1
                ),
                "confirmation_status": confirmation_status,
            }
        )
    decision = {
        "signal": signal,
        "reason": reason,
        "feature_snapshot": feature_snapshot,
        "strategy_diagnostics": {
            "schema_version": 1,
            "blocked_filters": blocked,
            "regime_filter_enabled": regime_filter_enabled,
            "entry_mode": entry_mode,
            "confirmation_status": confirmation_status,
        },
    }
    if signal == "BUY":
        decision["order_intent"] = {
            "side": "BUY",
            "sizing": "portfolio_policy_fractional_cash",
        }
    return decision


def prepare_channel_breakout_context(dataset: DatasetSnapshot) -> PreparedCandleArrays:
    return prepare_candle_arrays(dataset)


def build_channel_breakout_research_events(
    *,
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    execution_timing_policy: ExecutionTimingPolicy,
    portfolio_policy: PortfolioPolicy,
    context: Any | None = None,
) -> Iterator[ResearchDecisionEvent]:
    del fee_rate, slippage_bps, portfolio_policy, context
    prepared = prepare_channel_breakout_context(dataset)
    candles = prepared.candles
    entry_mode = _validate_supported_entry_mode(parameter_values.get("ENTRY_MODE"))
    pending = BreakoutPendingState()
    last_buy_index: int | None = None
    trade_count_by_day: dict[str, int] = {}
    for candle_index, candle in enumerate(candles):
        decision = decide_channel_breakout_snapshot(
            candle=candle,
            candle_index=candle_index,
            dataset=dataset,
            parameter_values=parameter_values,
            candles=candles,
            closes=prepared.closes,
            highs=prepared.highs,
            lows=prepared.lows,
            volumes=prepared.volumes,
        )
        if entry_mode == "delayed_confirmation":
            decision = _apply_delayed_confirmation_state(
                decision=decision,
                candle=candle,
                candle_index=candle_index,
                parameter_values=parameter_values,
                pending=pending,
            )
        decision = _apply_buy_limits(
            decision=decision,
            candle=candle,
            candle_index=candle_index,
            parameter_values=parameter_values,
            last_buy_index=last_buy_index,
            trade_count_by_day=trade_count_by_day,
        )
        signal = str(decision.get("signal") or "HOLD").upper()
        feature_snapshot = dict(decision.get("feature_snapshot") or {})
        blocked_filters = tuple(str(item) for item in feature_snapshot.get("blocked_filters") or ())
        decision_ts = candle_close_ts(candle, interval=dataset.interval) + int(
            execution_timing_policy.decision_guard_ms
        )
        yield ResearchDecisionEvent(
            candle_ts=int(candle.ts),
            decision_ts=int(decision_ts),
            strategy_name=CHANNEL_BREAKOUT_SPEC.strategy_name,
            strategy_version=CHANNEL_BREAKOUT_SPEC.strategy_version,
            raw_signal=signal,
            final_signal=signal,
            reason=str(decision.get("reason") or "channel_breakout_research_decision"),
            feature_snapshot=feature_snapshot,
            strategy_diagnostics=dict(decision.get("strategy_diagnostics") or {}),
            entry_signal=signal if signal == "BUY" else "HOLD",
            exit_signal="HOLD",
            blocked_filters=blocked_filters,
            order_intent=(
                dict(decision["order_intent"])
                if isinstance(decision.get("order_intent"), dict)
                else None
            ),
            exit_intent={
                "mode": "evaluate_exit_policy",
                "base_signal": "HOLD",
                "base_reason": "common_exit_policy_only",
            },
            extra_payload={"strategy_family": "channel_breakout", "research_only": True},
        )
        if signal == "BUY":
            last_buy_index = int(candle_index)
            day_key = _candle_utc_day_key(candle)
            trade_count_by_day[day_key] = trade_count_by_day.get(day_key, 0) + 1


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return float(numerator) / float(denominator)


def _normalize_exit_rules(raw: object) -> tuple[str, ...]:
    if not isinstance(raw, str):
        raise StrategySpecError("STRATEGY_EXIT_RULES must be str")
    return tuple(token.strip().lower() for token in raw.split(",") if token.strip())


def _validate_supported_entry_mode(raw: object) -> str:
    entry_mode = str(raw or "immediate_breakout").strip()
    if entry_mode not in _SUPPORTED_ENTRY_MODES:
        raise StrategySpecError(
            "ENTRY_MODE unsupported for channel_breakout_with_regime_filter: "
            f"{entry_mode}; supported entry modes: {','.join(sorted(_SUPPORTED_ENTRY_MODES))}"
        )
    return entry_mode


def _apply_delayed_confirmation_state(
    *,
    decision: dict[str, Any],
    candle: Candle,
    candle_index: int,
    parameter_values: dict[str, Any],
    pending: BreakoutPendingState,
) -> dict[str, Any]:
    if pending.active and int(candle_index) > pending.breakout_index:
        return _evaluate_pending_confirmation(
            decision=decision,
            candle=candle,
            candle_index=candle_index,
            parameter_values=parameter_values,
            pending=pending,
        )
    feature_snapshot = dict(decision.get("feature_snapshot") or {})
    if bool(feature_snapshot.get("breakout_candidate")):
        pending.active = True
        pending.breakout_index = int(candle_index)
        pending.breakout_level = float(feature_snapshot["breakout_level"])
        pending.breakout_close = float(feature_snapshot["close"])
        pending.expires_at_index = int(feature_snapshot["pending_expires_at_index"])
    return decision


def _evaluate_pending_confirmation(
    *,
    decision: dict[str, Any],
    candle: Candle,
    candle_index: int,
    parameter_values: dict[str, Any],
    pending: BreakoutPendingState,
) -> dict[str, Any]:
    if int(candle_index) > int(pending.expires_at_index):
        return _delayed_confirmation_decision(
            base_decision=decision,
            pending=pending,
            signal="HOLD",
            reason="breakout_confirmation_expired",
            confirmation_status="expired",
            clear_pending=True,
        )
    close = float(candle.close)
    low = float(candle.low)
    pullback_ratio = float(parameter_values["PULLBACK_RATIO"])
    if close <= pending.breakout_level:
        return _delayed_confirmation_decision(
            base_decision=decision,
            pending=pending,
            signal="HOLD",
            reason="breakout_confirmation_failed_close_below_level",
            confirmation_status="failed_close_below_level",
            clear_pending=True,
        )
    if low < pending.breakout_level * (1.0 - pullback_ratio):
        return _delayed_confirmation_decision(
            base_decision=decision,
            pending=pending,
            signal="HOLD",
            reason="breakout_confirmation_failed_deep_retest",
            confirmation_status="failed_deep_retest",
            clear_pending=True,
        )
    blocked_filters = tuple(str(item) for item in (decision.get("feature_snapshot") or {}).get("blocked_filters") or ())
    if "downtrend_regime" in blocked_filters or "chop_regime" in blocked_filters:
        return _delayed_confirmation_decision(
            base_decision=decision,
            pending=pending,
            signal="HOLD",
            reason="breakout_confirmation_failed_regime",
            confirmation_status="failed_regime",
            clear_pending=True,
        )
    return _delayed_confirmation_decision(
        base_decision=decision,
        pending=pending,
        signal="BUY",
        reason="delayed_breakout_confirmed",
        confirmation_status="confirmed",
        clear_pending=True,
    )


def _delayed_confirmation_decision(
    *,
    base_decision: dict[str, Any],
    pending: BreakoutPendingState,
    signal: str,
    reason: str,
    confirmation_status: str,
    clear_pending: bool,
) -> dict[str, Any]:
    decision = dict(base_decision)
    feature_snapshot = dict(decision.get("feature_snapshot") or {})
    feature_snapshot.update(
        {
            "entry_mode": "delayed_confirmation",
            "breakout_candidate": False,
            "breakout_pending": pending.active and not clear_pending,
            "breakout_level": float(pending.breakout_level),
            "breakout_index": int(pending.breakout_index),
            "confirmation_window_min": int(
                feature_snapshot.get("confirmation_window_min")
                if feature_snapshot.get("confirmation_window_min") is not None
                else max(0, int(pending.expires_at_index) - int(pending.breakout_index))
            ),
            "pending_expires_at_index": int(pending.expires_at_index),
            "confirmation_status": confirmation_status,
        }
    )
    diagnostics = dict(decision.get("strategy_diagnostics") or {})
    diagnostics["entry_mode"] = "delayed_confirmation"
    diagnostics["confirmation_status"] = confirmation_status
    decision["signal"] = signal
    decision["reason"] = reason
    decision["feature_snapshot"] = feature_snapshot
    decision["strategy_diagnostics"] = diagnostics
    if signal == "BUY":
        decision["order_intent"] = {
            "side": "BUY",
            "sizing": "portfolio_policy_fractional_cash",
        }
    else:
        decision.pop("order_intent", None)
    if clear_pending:
        pending.active = False
        pending.breakout_index = -1
        pending.breakout_level = 0.0
        pending.breakout_close = 0.0
        pending.expires_at_index = -1
    return decision


def _apply_buy_limits(
    *,
    decision: dict[str, Any],
    candle: Candle,
    candle_index: int,
    parameter_values: dict[str, Any],
    last_buy_index: int | None,
    trade_count_by_day: dict[str, int],
) -> dict[str, Any]:
    if str(decision.get("signal") or "HOLD").upper() != "BUY":
        return decision
    cooldown_min = int(parameter_values["COOLDOWN_MIN"])
    if last_buy_index is not None and cooldown_min > 0 and int(candle_index) - int(last_buy_index) < cooldown_min:
        return _blocked_buy_limit_decision(decision=decision, reason="cooldown_active")
    max_trades_per_day = int(parameter_values["MAX_TRADES_PER_DAY"])
    day_key = _candle_utc_day_key(candle)
    if max_trades_per_day > 0 and trade_count_by_day.get(day_key, 0) >= max_trades_per_day:
        return _blocked_buy_limit_decision(decision=decision, reason="max_trades_per_day_reached")
    return decision


def _blocked_buy_limit_decision(*, decision: dict[str, Any], reason: str) -> dict[str, Any]:
    blocked = tuple(str(item) for item in (decision.get("feature_snapshot") or {}).get("blocked_filters") or ())
    blocked = (*blocked, reason)
    limited = dict(decision)
    feature_snapshot = dict(limited.get("feature_snapshot") or {})
    feature_snapshot["blocked_filters"] = blocked
    diagnostics = dict(limited.get("strategy_diagnostics") or {})
    diagnostics["blocked_filters"] = blocked
    limited["signal"] = "HOLD"
    limited["reason"] = "channel_breakout_blocked"
    limited["feature_snapshot"] = feature_snapshot
    limited["strategy_diagnostics"] = diagnostics
    limited.pop("order_intent", None)
    return limited


def _candle_utc_day_key(candle: Candle) -> str:
    return strftime("%Y-%m-%d", gmtime(int(candle.ts) // 1000))


CHANNEL_BREAKOUT_WITH_REGIME_FILTER_PLUGIN = research_plugin_from_event_builder(
    strategy_name=CHANNEL_BREAKOUT_SPEC.strategy_name,
    version=CHANNEL_BREAKOUT_SPEC.strategy_version,
    spec=CHANNEL_BREAKOUT_SPEC,
    required_data=CHANNEL_BREAKOUT_SPEC.required_data,
    optional_data=CHANNEL_BREAKOUT_SPEC.optional_data,
    build_research_events=build_channel_breakout_research_events,
    diagnostics_namespace=CHANNEL_BREAKOUT_SPEC.strategy_name,
    research_parameter_materializer=materialize_channel_breakout_parameters,
).to_research_strategy_plugin()

object.__setattr__(
    CHANNEL_BREAKOUT_WITH_REGIME_FILTER_PLUGIN,
    "complexity_metadata",
    CHANNEL_BREAKOUT_COMPLEXITY_METADATA,
)
object.__setattr__(
    CHANNEL_BREAKOUT_WITH_REGIME_FILTER_PLUGIN,
    "estimate_complexity",
    estimate_channel_breakout_complexity,
)
