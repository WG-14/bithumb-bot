from __future__ import annotations

import os
from dataclasses import dataclass


def parse_bool_env(key: str, default: str = "false") -> bool:
    v = os.getenv(key, default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class Settings:
    # runtime
    MODE: str = os.getenv("MODE", "paper")
    PAIR: str = os.getenv("PAIR", "BTC_KRW")
    INTERVAL: str = os.getenv("INTERVAL", "1m")
    EVERY: int = int(os.getenv("EVERY", "60"))  # seconds

    # strategy
    SMA_SHORT: int = int(os.getenv("SMA_SHORT", "7"))
    SMA_LONG: int = int(os.getenv("SMA_LONG", "30"))
    COOLDOWN_MIN: int = int(os.getenv("COOLDOWN_MIN", "1"))
    MIN_GAP: float = float(os.getenv("MIN_GAP", "0.0003"))

    # storage
    DB_PATH: str = os.getenv("DB_PATH", "data/bithumb_1m.sqlite")

    # paper portfolio
    START_CASH_KRW: float = float(os.getenv("START_CASH_KRW", "1000000"))
    BUY_FRACTION: float = float(os.getenv("BUY_FRACTION", "0.99"))
    FEE_RATE: float = float(os.getenv("FEE_RATE", "0.0004"))  # 기본값은 너 코드와 다를 수 있음
    SLIPPAGE_BPS: float = float(os.getenv("SLIPPAGE_BPS", "0"))
    MAX_ORDERBOOK_SPREAD_BPS: float = float(os.getenv("MAX_ORDERBOOK_SPREAD_BPS", "100"))
    MAX_MARKET_SLIPPAGE_BPS: float = float(os.getenv("MAX_MARKET_SLIPPAGE_BPS", "0"))
    MIN_ORDER_NOTIONAL_KRW: float = float(os.getenv("MIN_ORDER_NOTIONAL_KRW", "0"))
    PRETRADE_BALANCE_BUFFER_BPS: float = float(os.getenv("PRETRADE_BALANCE_BUFFER_BPS", "0"))

    # risk
    MAX_ORDER_KRW: float = float(os.getenv("MAX_ORDER_KRW", "0"))
    MAX_DAILY_LOSS_KRW: float = float(os.getenv("MAX_DAILY_LOSS_KRW", "0"))
    MAX_OPEN_POSITIONS: int = int(os.getenv("MAX_OPEN_POSITIONS", "1"))
    KILL_SWITCH: bool = parse_bool_env("KILL_SWITCH", "false")
    KILL_SWITCH_LIQUIDATE: bool = parse_bool_env("KILL_SWITCH_LIQUIDATE", "false")
    MAX_DAILY_ORDER_COUNT: int = int(os.getenv("MAX_DAILY_ORDER_COUNT", "0"))

    # bithumb private api / live
    BITHUMB_API_BASE: str = os.getenv("BITHUMB_API_BASE", "https://api.bithumb.com")
    BITHUMB_API_KEY: str = os.getenv("BITHUMB_API_KEY", "")
    BITHUMB_API_SECRET: str = os.getenv("BITHUMB_API_SECRET", "")
    LIVE_DRY_RUN: bool = parse_bool_env("LIVE_DRY_RUN", "false")
    OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC: int = int(
        os.getenv("OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC", "30")
    )
    MAX_OPEN_ORDER_AGE_SEC: int = int(os.getenv("MAX_OPEN_ORDER_AGE_SEC", "900"))

settings = Settings()


class LiveModeValidationError(ValueError):
    pass


def validate_live_mode_preflight(cfg: Settings) -> None:
    if cfg.MODE != "live":
        return

    issues: list[str] = []
    if cfg.MAX_ORDER_KRW <= 0:
        issues.append("MAX_ORDER_KRW must be > 0")
    if cfg.MAX_DAILY_LOSS_KRW <= 0:
        issues.append("MAX_DAILY_LOSS_KRW must be > 0")
    if cfg.MAX_DAILY_ORDER_COUNT <= 0:
        issues.append("MAX_DAILY_ORDER_COUNT must be > 0")
    if cfg.KILL_SWITCH_LIQUIDATE:
        issues.append(
            "KILL_SWITCH_LIQUIDATE=true is not supported yet; keep KILL_SWITCH_LIQUIDATE=false"
        )

    if not cfg.LIVE_DRY_RUN:
        if not cfg.BITHUMB_API_KEY.strip():
            issues.append("BITHUMB_API_KEY is required when LIVE_DRY_RUN=false")
        if not cfg.BITHUMB_API_SECRET.strip():
            issues.append("BITHUMB_API_SECRET is required when LIVE_DRY_RUN=false")

    if issues:
        raise LiveModeValidationError(
            "live mode preflight validation failed: " + "; ".join(issues)
        )
