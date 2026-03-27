from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

from .notifier import is_configured as notifier_is_configured


DEFAULT_DB_PATH = "data/bithumb_1m.sqlite"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LIVE_DB_PATH_REQUIRED_MSG = (
    "DB_PATH must be explicitly set when MODE=live; live env 파일에 DB_PATH를 명시하라"
)
PAPER_ONLY_ENV_KEYS = (
    "START_CASH_KRW",
    "BUY_FRACTION",
    "FEE_RATE",
    "SLIPPAGE_BPS",
)
ALLOWED_RUNTIME_MODES = ("paper", "live")


def parse_bool_env(key: str, default: str = "false") -> bool:
    v = os.getenv(key, default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def resolve_db_path(path: str) -> str:
    p = Path(path)
    if str(p) == ":memory:" or p.is_absolute():
        return str(p)
    return str((PROJECT_ROOT / p).resolve())


class LiveModeValidationError(ValueError):
    pass


class ModeValidationError(ValueError):
    pass


def resolve_db_path_from_env(mode: str) -> str:
    raw_db_path = os.getenv("DB_PATH")
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode == "live" and (raw_db_path is None or not raw_db_path.strip()):
        raise LiveModeValidationError(LIVE_DB_PATH_REQUIRED_MSG)
    selected_db_path = raw_db_path if raw_db_path and raw_db_path.strip() else DEFAULT_DB_PATH
    return resolve_db_path(selected_db_path)


def default_run_lock_path(mode: str) -> str:
    normalized_mode = (mode or "paper").strip().lower() or "paper"
    return f"data/locks/bithumb-bot-run-{normalized_mode}.lock"


def resolve_run_lock_path(path: str) -> str:
    p = Path(path)
    if p.is_absolute():
        return str(p)
    return str((PROJECT_ROOT / p).resolve())


@dataclass(frozen=True)
class Settings:
    # runtime
    MODE: str = os.getenv("MODE", "paper")
    PAIR: str = os.getenv("PAIR", "BTC_KRW")
    INTERVAL: str = os.getenv("INTERVAL", "1m")
    EVERY: int = int(os.getenv("EVERY", "60"))  # seconds

    # strategy
    STRATEGY_NAME: str = os.getenv("STRATEGY_NAME", "sma_cross")
    SMA_SHORT: int = int(os.getenv("SMA_SHORT", "7"))
    SMA_LONG: int = int(os.getenv("SMA_LONG", "30"))
    COOLDOWN_MIN: int = int(os.getenv("COOLDOWN_MIN", "1"))
    MIN_GAP: float = float(os.getenv("MIN_GAP", "0.0003"))
    SMA_FILTER_GAP_MIN_RATIO: float = float(os.getenv("SMA_FILTER_GAP_MIN_RATIO", "0.0005"))
    SMA_FILTER_VOL_WINDOW: int = int(os.getenv("SMA_FILTER_VOL_WINDOW", "10"))
    SMA_FILTER_VOL_MIN_RANGE_RATIO: float = float(
        os.getenv("SMA_FILTER_VOL_MIN_RANGE_RATIO", "0.002")
    )
    SMA_FILTER_OVEREXT_LOOKBACK: int = int(os.getenv("SMA_FILTER_OVEREXT_LOOKBACK", "3"))
    SMA_FILTER_OVEREXT_MAX_RETURN_RATIO: float = float(
        os.getenv("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", "0.03")
    )
    STRATEGY_EXIT_RULES: str = os.getenv("STRATEGY_EXIT_RULES", "opposite_cross,max_holding_time")
    STRATEGY_EXIT_MAX_HOLDING_MIN: int = int(os.getenv("STRATEGY_EXIT_MAX_HOLDING_MIN", "0"))
    STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO: float = float(
        os.getenv("STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", "0")
    )

    # storage
    DB_PATH: str = resolve_db_path_from_env(os.getenv("MODE", "paper"))
    RUN_LOCK_PATH: str = resolve_run_lock_path(
        os.getenv("RUN_LOCK_PATH", default_run_lock_path(os.getenv("MODE", "paper")))
    )
    DB_BUSY_TIMEOUT_MS: int = int(os.getenv("DB_BUSY_TIMEOUT_MS", "5000"))
    DB_LOCK_RETRY_COUNT: int = int(os.getenv("DB_LOCK_RETRY_COUNT", "2"))
    DB_LOCK_RETRY_BACKOFF_MS: int = int(os.getenv("DB_LOCK_RETRY_BACKOFF_MS", "50"))

    # paper portfolio
    START_CASH_KRW: float = float(os.getenv("START_CASH_KRW", "1000000"))
    BUY_FRACTION: float = float(os.getenv("BUY_FRACTION", "0.99"))
    FEE_RATE: float = float(os.getenv("FEE_RATE", "0.0004"))  # 기본값은 너 코드와 다를 수 있음
    SLIPPAGE_BPS: float = float(os.getenv("SLIPPAGE_BPS", "0"))
    MAX_ORDERBOOK_SPREAD_BPS: float = float(os.getenv("MAX_ORDERBOOK_SPREAD_BPS", "100"))
    MAX_MARKET_SLIPPAGE_BPS: float = float(os.getenv("MAX_MARKET_SLIPPAGE_BPS", "0"))
    LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS: float = float(
        os.getenv("LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", "0")
    )
    LIVE_PRICE_REFERENCE_MAX_AGE_SEC: int = int(os.getenv("LIVE_PRICE_REFERENCE_MAX_AGE_SEC", "0"))
    MIN_ORDER_NOTIONAL_KRW: float = float(os.getenv("MIN_ORDER_NOTIONAL_KRW", "0"))
    PRETRADE_BALANCE_BUFFER_BPS: float = float(os.getenv("PRETRADE_BALANCE_BUFFER_BPS", "0"))
    LIVE_MIN_ORDER_QTY: float = float(os.getenv("LIVE_MIN_ORDER_QTY", "0"))
    LIVE_ORDER_QTY_STEP: float = float(os.getenv("LIVE_ORDER_QTY_STEP", "0"))
    LIVE_ORDER_MAX_QTY_DECIMALS: int = int(os.getenv("LIVE_ORDER_MAX_QTY_DECIMALS", "0"))

    # risk
    MAX_ORDER_KRW: float = float(os.getenv("MAX_ORDER_KRW", "0"))
    MAX_DAILY_LOSS_KRW: float = float(os.getenv("MAX_DAILY_LOSS_KRW", "0"))
    MAX_POSITION_LOSS_PCT: float = float(os.getenv("MAX_POSITION_LOSS_PCT", "0"))
    MAX_OPEN_POSITIONS: int = int(os.getenv("MAX_OPEN_POSITIONS", "1"))
    KILL_SWITCH: bool = parse_bool_env("KILL_SWITCH", "false")
    KILL_SWITCH_LIQUIDATE: bool = parse_bool_env("KILL_SWITCH_LIQUIDATE", "false")
    MAX_DAILY_ORDER_COUNT: int = int(os.getenv("MAX_DAILY_ORDER_COUNT", "0"))

    # bithumb private api / live
    BITHUMB_API_BASE: str = os.getenv("BITHUMB_API_BASE", "https://api.bithumb.com")
    BITHUMB_API_KEY: str = os.getenv("BITHUMB_API_KEY", "")
    BITHUMB_API_SECRET: str = os.getenv("BITHUMB_API_SECRET", "")
    LIVE_DRY_RUN: bool = parse_bool_env("LIVE_DRY_RUN", "false")
    LIVE_REAL_ORDER_ARMED: bool = parse_bool_env("LIVE_REAL_ORDER_ARMED", "false")
    OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC: int = int(
        os.getenv("OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC", "30")
    )
    MAX_OPEN_ORDER_AGE_SEC: int = int(os.getenv("MAX_OPEN_ORDER_AGE_SEC", "900"))

settings = Settings()


def validate_mode_or_raise(mode: str) -> None:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode in ALLOWED_RUNTIME_MODES:
        return
    allowed = ", ".join(ALLOWED_RUNTIME_MODES)
    raise ModeValidationError(
        f"invalid MODE={mode!r}; allowed values: {allowed}"
    )


def validate_live_mode_preflight(cfg: Settings) -> None:
    if cfg.MODE != "live":
        return

    issues: list[str] = []
    db_path_env = os.getenv("DB_PATH")
    if db_path_env is None or not db_path_env.strip():
        issues.append(LIVE_DB_PATH_REQUIRED_MSG)
    else:
        configured_db_path = resolve_db_path(cfg.DB_PATH)
        default_db_path = resolve_db_path(DEFAULT_DB_PATH)
        if configured_db_path == default_db_path:
            issues.append(
                "DB_PATH must not point to the default paper/shared DB path "
                f"({DEFAULT_DB_PATH}) when MODE=live"
            )

    explicitly_set_paper_keys = [
        key for key in PAPER_ONLY_ENV_KEYS if os.getenv(key) not in (None, "")
    ]
    if explicitly_set_paper_keys:
        issues.append(
            "paper/test-like config mixing is not allowed when MODE=live; "
            "unset paper-only env keys: " + ", ".join(explicitly_set_paper_keys)
        )

    if cfg.MAX_ORDER_KRW <= 0:
        issues.append("MAX_ORDER_KRW must be > 0")
    if cfg.MAX_DAILY_LOSS_KRW <= 0:
        issues.append("MAX_DAILY_LOSS_KRW must be > 0")
    if cfg.MAX_DAILY_ORDER_COUNT <= 0:
        issues.append("MAX_DAILY_ORDER_COUNT must be > 0")
    spread_limit_bps = float(cfg.MAX_ORDERBOOK_SPREAD_BPS)
    if not math.isfinite(spread_limit_bps) or spread_limit_bps <= 0:
        issues.append(
            "MAX_ORDERBOOK_SPREAD_BPS must be a finite value > 0 when MODE=live "
            "(spread guard cannot be disabled)"
        )

    market_slippage_bps = float(cfg.MAX_MARKET_SLIPPAGE_BPS)
    if not math.isfinite(market_slippage_bps) or market_slippage_bps <= 0:
        issues.append(
            "MAX_MARKET_SLIPPAGE_BPS must be a finite value > 0 when MODE=live "
            "(market slippage guard cannot be disabled)"
        )

    live_protection_slippage_bps = float(cfg.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS)
    if not math.isfinite(live_protection_slippage_bps) or live_protection_slippage_bps <= 0:
        issues.append(
            "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS must be a finite value > 0 when MODE=live "
            "(live price protection cannot be disabled)"
        )

    if not cfg.LIVE_DRY_RUN:
        if not cfg.LIVE_REAL_ORDER_ARMED:
            issues.append(
                "LIVE_REAL_ORDER_ARMED=true is required to place real live orders "
                "(MODE=live and LIVE_DRY_RUN=false)"
            )
        if not cfg.BITHUMB_API_KEY.strip():
            issues.append("BITHUMB_API_KEY is required when LIVE_DRY_RUN=false")
        if not cfg.BITHUMB_API_SECRET.strip():
            issues.append("BITHUMB_API_SECRET is required when LIVE_DRY_RUN=false")

    if not notifier_is_configured():
        issues.append(
            "notifier must be enabled and configured with at least one delivery target "
            "(NOTIFIER_WEBHOOK_URL, SLACK_WEBHOOK_URL, or TELEGRAM_BOT_TOKEN+TELEGRAM_CHAT_ID) when MODE=live"
        )

    from .broker.order_rules import get_effective_order_rules, required_rule_issues

    try:
        resolved_rules = get_effective_order_rules(cfg.PAIR).rules
        issues.extend(required_rule_issues(resolved_rules))
    except Exception as exc:
        issues.append(f"failed to resolve order rules: {type(exc).__name__}: {exc}")

    if issues:
        raise LiveModeValidationError(
            "live mode preflight validation failed: " + "; ".join(issues)
        )
