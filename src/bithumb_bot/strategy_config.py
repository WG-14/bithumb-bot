from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .approved_profile import (
    load_profile_or_promotion_regime_policy,
    runtime_contract_from_settings,
    verify_profile_against_runtime,
)
from .config import settings


@dataclass(frozen=True)
class SmaStrategyConfig:
    short_n: int
    long_n: int
    pair: str
    interval: str
    exit_rule_names: tuple[str, ...]
    exit_max_holding_min: int
    exit_min_take_profit_ratio: float
    exit_small_loss_tolerance_ratio: float
    slippage_bps: float
    live_fee_rate_estimate: float
    entry_edge_buffer_ratio: float
    strategy_min_expected_edge_ratio: float
    buy_fraction: float
    max_order_krw: float
    candidate_regime_policy: dict[str, object] | None = None


def normalize_exit_rule_names(raw: str | Iterable[object]) -> tuple[str, ...]:
    if isinstance(raw, str):
        values = raw.split(",")
    else:
        values = raw
    return tuple(str(token).strip().lower() for token in values if str(token).strip())


def sma_strategy_config_from_settings(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
) -> SmaStrategyConfig:
    profile_or_candidate_path = (
        str(settings.APPROVED_STRATEGY_PROFILE_PATH or "").strip()
        or str(settings.STRATEGY_CANDIDATE_PROFILE_PATH or "").strip()
    )
    candidate_regime_policy = _candidate_regime_policy_from_configured_profile(profile_or_candidate_path)
    return SmaStrategyConfig(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=str(settings.PAIR),
        interval=str(settings.INTERVAL),
        exit_rule_names=normalize_exit_rule_names(settings.STRATEGY_EXIT_RULES),
        exit_max_holding_min=int(settings.STRATEGY_EXIT_MAX_HOLDING_MIN),
        exit_min_take_profit_ratio=float(settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO),
        exit_small_loss_tolerance_ratio=float(settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO),
        slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        live_fee_rate_estimate=float(settings.LIVE_FEE_RATE_ESTIMATE),
        entry_edge_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
        strategy_min_expected_edge_ratio=float(settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO),
        buy_fraction=float(settings.BUY_FRACTION),
        max_order_krw=float(settings.MAX_ORDER_KRW),
        candidate_regime_policy=candidate_regime_policy,
    )


def _candidate_regime_policy_from_configured_profile(path: str) -> dict[str, object] | None:
    raw_path = str(path or "").strip()
    if not raw_path:
        return None
    approved_profile_path = str(settings.APPROVED_STRATEGY_PROFILE_PATH or "").strip()
    if raw_path == approved_profile_path:
        result = verify_profile_against_runtime(
            profile_path=raw_path,
            runtime=runtime_contract_from_settings(settings),
            require_profile=True,
            expected_profile_modes=_expected_profile_modes_for_settings(),
            verify_source_promotion=True,
        )
        if not result.ok:
            return {
                "_policy_load_error": result.reason,
                "_policy_source": raw_path,
                **result.audit_fields(),
            }
    return load_profile_or_promotion_regime_policy(raw_path)


def _expected_profile_modes_for_settings() -> set[str] | None:
    mode = str(settings.MODE or "").strip().lower()
    if mode == "paper":
        return {"paper"}
    if mode != "live":
        return None
    if bool(settings.LIVE_DRY_RUN) and not bool(settings.LIVE_REAL_ORDER_ARMED):
        return {"live_dry_run"}
    if not bool(settings.LIVE_DRY_RUN) and bool(settings.LIVE_REAL_ORDER_ARMED):
        return {"small_live"}
    return set()
