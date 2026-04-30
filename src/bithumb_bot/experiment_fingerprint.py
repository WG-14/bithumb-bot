from __future__ import annotations

import hashlib
import json
import subprocess
from functools import lru_cache

from .config import PROJECT_ROOT, settings


@lru_cache(maxsize=1)
def current_code_commit_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(PROJECT_ROOT),
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
    except Exception:
        return "unknown"
    return result.stdout.strip() or "unknown"


def build_experiment_fingerprint_payload(*, strategy_name: str | None = None) -> dict[str, object]:
    return {
        "code_commit_sha": current_code_commit_sha(),
        "strategy_name": str(strategy_name or settings.STRATEGY_NAME),
        "sma_short": int(settings.SMA_SHORT),
        "sma_long": int(settings.SMA_LONG),
        "sma_filter_gap_min_ratio": float(settings.SMA_FILTER_GAP_MIN_RATIO),
        "sma_filter_vol_window": int(settings.SMA_FILTER_VOL_WINDOW),
        "sma_filter_vol_min_range_ratio": float(settings.SMA_FILTER_VOL_MIN_RANGE_RATIO),
        "sma_filter_overext_lookback": int(settings.SMA_FILTER_OVEREXT_LOOKBACK),
        "sma_filter_overext_max_return_ratio": float(settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO),
        "sma_cost_edge_enabled": bool(settings.SMA_COST_EDGE_ENABLED),
        "strategy_min_expected_edge_ratio": float(settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO),
        "entry_edge_buffer_ratio": float(settings.ENTRY_EDGE_BUFFER_RATIO),
        "exit_rules": str(settings.STRATEGY_EXIT_RULES),
        "exit_max_holding_min": int(settings.STRATEGY_EXIT_MAX_HOLDING_MIN),
        "exit_min_take_profit_ratio": float(settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO),
        "exit_small_loss_tolerance_ratio": float(settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO),
        "execution_engine": str(settings.EXECUTION_ENGINE),
        "target_exposure_krw": settings.TARGET_EXPOSURE_KRW,
        "max_order_krw": float(settings.MAX_ORDER_KRW),
        "fee_rate_estimate": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "fee_authority_ref": "settings.LIVE_FEE_RATE_ESTIMATE",
        "slippage_bps": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "market": str(settings.PAIR),
        "interval": str(settings.INTERVAL),
        "max_daily_loss_krw": float(settings.MAX_DAILY_LOSS_KRW),
        "max_position_loss_pct": float(settings.MAX_POSITION_LOSS_PCT),
        "max_daily_order_count": int(settings.MAX_DAILY_ORDER_COUNT),
        "order_sizing_policy_version": "target_delta_exchange_floor_v1",
        "approval_state": {
            "mode": str(settings.MODE),
            "live_dry_run": bool(settings.LIVE_DRY_RUN),
            "live_real_order_armed": bool(settings.LIVE_REAL_ORDER_ARMED),
        },
    }


def experiment_fingerprint(*, strategy_name: str | None = None) -> str:
    payload = build_experiment_fingerprint_payload(strategy_name=strategy_name)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def experiment_context(*, strategy_name: str | None = None) -> dict[str, object]:
    payload = build_experiment_fingerprint_payload(strategy_name=strategy_name)
    return {
        "experiment_id": experiment_fingerprint(strategy_name=strategy_name),
        "experiment_fingerprint": experiment_fingerprint(strategy_name=strategy_name),
        "experiment_fingerprint_version": "experiment_fingerprint_v1",
        "experiment_fingerprint_inputs": payload,
    }
