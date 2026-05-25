from __future__ import annotations

from dataclasses import dataclass, field

from ..config import settings
from ..core.sma_policy import (
    ExecutionConstraintSnapshot,
    MarketWindow,
    PositionSnapshot,
    SmaPolicyConfig,
    StrategyDecisionV2,
    evaluate_sma_policy,
)
from ..strategy_config import normalize_exit_rule_names, sma_strategy_config_from_settings


@dataclass(frozen=True)
class SmaWithFilterStrategy:
    """Promotion-grade snapshot SMA strategy.

    This class intentionally exposes only the typed snapshot policy API. The
    DB-bound ``decide(conn)`` compatibility path is isolated in
    ``sma_legacy_adapter``.
    """

    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL
    min_gap_ratio: float = settings.SMA_FILTER_GAP_MIN_RATIO
    volatility_window: int = settings.SMA_FILTER_VOL_WINDOW
    min_volatility_ratio: float = settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
    overextended_lookback: int = settings.SMA_FILTER_OVEREXT_LOOKBACK
    overextended_max_return_ratio: float = settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO
    slippage_bps: float = settings.STRATEGY_ENTRY_SLIPPAGE_BPS
    live_fee_rate_estimate: float = settings.LIVE_FEE_RATE_ESTIMATE
    entry_edge_buffer_ratio: float = settings.ENTRY_EDGE_BUFFER_RATIO
    cost_edge_enabled: bool = settings.SMA_COST_EDGE_ENABLED
    cost_edge_min_ratio: float = settings.SMA_COST_EDGE_MIN_RATIO
    market_regime_enabled: bool = settings.SMA_MARKET_REGIME_ENABLED
    exit_rule_names: list[str] = field(
        default_factory=lambda: list(normalize_exit_rule_names(settings.STRATEGY_EXIT_RULES))
    )
    exit_stop_loss_ratio: float = settings.STRATEGY_EXIT_STOP_LOSS_RATIO
    exit_max_holding_min: int = settings.STRATEGY_EXIT_MAX_HOLDING_MIN
    exit_min_take_profit_ratio: float = settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
    exit_small_loss_tolerance_ratio: float = settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
    buy_fraction: float = settings.BUY_FRACTION
    max_order_krw: float = settings.MAX_ORDER_KRW
    candidate_regime_policy: dict[str, object] | None = None

    name: str = "sma_with_filter"

    def decide_snapshot(
        self,
        *,
        market: MarketWindow,
        position: PositionSnapshot,
        config: SmaPolicyConfig,
        execution_context: ExecutionConstraintSnapshot,
    ) -> StrategyDecisionV2:
        return evaluate_sma_policy(
            market=market,
            position=position,
            config=config,
            execution_context=execution_context,
        )


def create_sma_with_filter_strategy(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
    min_gap_ratio: float | None = None,
    volatility_window: int | None = None,
    min_volatility_ratio: float | None = None,
    overextended_lookback: int | None = None,
    overextended_max_return_ratio: float | None = None,
    slippage_bps: float | None = None,
    live_fee_rate_estimate: float | None = None,
    entry_edge_buffer_ratio: float | None = None,
    strategy_min_expected_edge_ratio: float | None = None,
    cost_edge_enabled: bool | None = None,
    cost_edge_min_ratio: float | None = None,
    market_regime_enabled: bool | None = None,
    candidate_regime_policy: dict[str, object] | None = None,
    exit_rule_names: list[str] | None = None,
    exit_stop_loss_ratio: float | None = None,
    exit_max_holding_min: int | None = None,
    exit_min_take_profit_ratio: float | None = None,
    exit_small_loss_tolerance_ratio: float | None = None,
) -> SmaWithFilterStrategy:
    settings_config = sma_strategy_config_from_settings(short_n=short_n, long_n=long_n)
    return SmaWithFilterStrategy(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings.PAIR if pair is None else str(pair),
        interval=settings.INTERVAL if interval is None else str(interval),
        min_gap_ratio=float(
            settings.SMA_FILTER_GAP_MIN_RATIO if min_gap_ratio is None else min_gap_ratio
        ),
        volatility_window=int(
            settings.SMA_FILTER_VOL_WINDOW if volatility_window is None else volatility_window
        ),
        min_volatility_ratio=float(
            settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
            if min_volatility_ratio is None
            else min_volatility_ratio
        ),
        overextended_lookback=int(
            settings.SMA_FILTER_OVEREXT_LOOKBACK
            if overextended_lookback is None
            else overextended_lookback
        ),
        overextended_max_return_ratio=float(
            settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO
            if overextended_max_return_ratio is None
            else overextended_max_return_ratio
        ),
        slippage_bps=float(
            settings.STRATEGY_ENTRY_SLIPPAGE_BPS if slippage_bps is None else slippage_bps
        ),
        live_fee_rate_estimate=float(
            settings.LIVE_FEE_RATE_ESTIMATE
            if live_fee_rate_estimate is None
            else live_fee_rate_estimate
        ),
        entry_edge_buffer_ratio=float(
            settings.ENTRY_EDGE_BUFFER_RATIO
            if entry_edge_buffer_ratio is None
            else entry_edge_buffer_ratio
        ),
        cost_edge_enabled=(
            bool(settings.SMA_COST_EDGE_ENABLED) if cost_edge_enabled is None else bool(cost_edge_enabled)
        ),
        cost_edge_min_ratio=float(
            (
                settings.SMA_COST_EDGE_MIN_RATIO
                if cost_edge_min_ratio is None and strategy_min_expected_edge_ratio is None
                else strategy_min_expected_edge_ratio
                if cost_edge_min_ratio is None
                else cost_edge_min_ratio
            )
        ),
        market_regime_enabled=(
            bool(settings.SMA_MARKET_REGIME_ENABLED)
            if market_regime_enabled is None
            else bool(market_regime_enabled)
        ),
        candidate_regime_policy=(
            settings_config.candidate_regime_policy
            if candidate_regime_policy is None
            else candidate_regime_policy
        ),
        exit_rule_names=(
            list(normalize_exit_rule_names(settings.STRATEGY_EXIT_RULES))
            if exit_rule_names is None
            else [str(name).strip().lower() for name in exit_rule_names if str(name).strip()]
        ),
        exit_stop_loss_ratio=float(
            settings.STRATEGY_EXIT_STOP_LOSS_RATIO
            if exit_stop_loss_ratio is None
            else exit_stop_loss_ratio
        ),
        exit_max_holding_min=int(
            settings.STRATEGY_EXIT_MAX_HOLDING_MIN
            if exit_max_holding_min is None
            else exit_max_holding_min
        ),
        exit_min_take_profit_ratio=float(
            settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
            if exit_min_take_profit_ratio is None
            else exit_min_take_profit_ratio
        ),
        exit_small_loss_tolerance_ratio=float(
            settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
            if exit_small_loss_tolerance_ratio is None
            else exit_small_loss_tolerance_ratio
        ),
    )


__all__ = ["SmaWithFilterStrategy", "create_sma_with_filter_strategy"]
