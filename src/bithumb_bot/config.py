from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

from .notifier import is_configured as notifier_is_configured
from .paths import PathManager, PathPolicyError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
try:
    PATH_MANAGER = PathManager.from_env(PROJECT_ROOT)
except PathPolicyError as exc:
    raise ValueError(str(exc)) from exc
LIVE_DB_PATH_REQUIRED_MSG = (
    "DB_PATH must be explicitly set when MODE=live; live env 파일에 DB_PATH를 명시하라"
)
PAPER_ONLY_ENV_KEYS = (
    "START_CASH_KRW",
    "BUY_FRACTION",
    "FEE_RATE",
    "PAPER_FEE_RATE",
    "PAPER_FEE_RATE_ESTIMATE",
    "SLIPPAGE_BPS",
)
ALLOWED_RUNTIME_MODES = ("paper", "live")
DEFAULT_RUNTIME_STRATEGY = "sma_with_filter"


def parse_bool_env(key: str, default: str = "false") -> bool:
    v = os.getenv(key, default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def parse_float_env(key: str, default: str) -> float:
    raw = os.getenv(key)
    candidate = raw if raw is not None and raw.strip() != "" else default
    try:
        return float(candidate)
    except ValueError as exc:
        raise ValueError(f"{key} must be a float-compatible value, got {candidate!r}") from exc


def resolve_db_path(path: str) -> str:
    p = Path(path)
    if str(p) == ":memory:":
        return str(p)
    if p.is_absolute():
        return str(p.resolve())
    raise ValueError(
        f"DB_PATH must be an absolute path (got relative path: {path!r}); "
        "use PathManager-managed absolute DATA_ROOT path"
    )


class LiveModeValidationError(ValueError):
    pass


class ModeValidationError(ValueError):
    pass


def resolve_db_path_from_env(mode: str) -> str:
    raw_db_path = os.getenv("DB_PATH")
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode == "live" and (raw_db_path is None or not raw_db_path.strip()):
        raise LiveModeValidationError(LIVE_DB_PATH_REQUIRED_MSG)
    if raw_db_path and raw_db_path.strip():
        return resolve_db_path(raw_db_path)
    return str(PATH_MANAGER.primary_db_path())


def resolve_strategy_name_from_env() -> str:
    raw = os.getenv("STRATEGY_NAME")
    normalized = str(raw or "").strip().lower()
    return normalized or DEFAULT_RUNTIME_STRATEGY


def default_run_lock_path(mode: str) -> str:
    normalized_mode = (mode or "paper").strip().lower() or "paper"
    return str(PATH_MANAGER.config.run_root / normalized_mode / "bithumb-bot.lock")


def resolve_run_lock_path(path: str) -> str:
    p = Path(path)
    if p.is_absolute():
        return str(p)
    return str((PROJECT_ROOT / p).resolve())


def resolve_run_lock_path_from_env(mode: str) -> str:
    raw = os.getenv("RUN_LOCK_PATH")
    if raw and raw.strip():
        return resolve_run_lock_path(raw)
    return default_run_lock_path(mode)


@dataclass(frozen=True)
class Settings:
    # runtime
    MODE: str = os.getenv("MODE", "paper")
    PAIR: str = os.getenv("PAIR", "BTC_KRW")
    INTERVAL: str = os.getenv("INTERVAL", "1m")
    EVERY: int = int(os.getenv("EVERY", "60"))  # seconds

    # strategy
    # 운영 기본 전략은 필터 포함 sma_with_filter를 권장.
    # STRATEGY_NAME 환경변수로 전략 이름을 명시적으로 선택한다.
    STRATEGY_NAME: str = resolve_strategy_name_from_env()
    SMA_SHORT: int = int(os.getenv("SMA_SHORT", "7"))
    SMA_LONG: int = int(os.getenv("SMA_LONG", "30"))
    COOLDOWN_MIN: int = int(os.getenv("COOLDOWN_MIN", "1"))
    MIN_GAP: float = float(os.getenv("MIN_GAP", "0.0003"))
    # 실거래 수수료/슬리피지 환경에서 과도한 잔진입을 줄이기 위한 보수적 기본 임계값.
    SMA_FILTER_GAP_MIN_RATIO: float = float(os.getenv("SMA_FILTER_GAP_MIN_RATIO", "0.0012"))
    SMA_FILTER_VOL_WINDOW: int = int(os.getenv("SMA_FILTER_VOL_WINDOW", "10"))
    SMA_FILTER_VOL_MIN_RANGE_RATIO: float = float(
        os.getenv("SMA_FILTER_VOL_MIN_RANGE_RATIO", "0.003")
    )
    SMA_FILTER_OVEREXT_LOOKBACK: int = int(os.getenv("SMA_FILTER_OVEREXT_LOOKBACK", "3"))
    SMA_FILTER_OVEREXT_MAX_RETURN_RATIO: float = float(
        os.getenv("SMA_FILTER_OVEREXT_MAX_RETURN_RATIO", "0.02")
    )
    ENTRY_EDGE_BUFFER_RATIO: float = parse_float_env("ENTRY_EDGE_BUFFER_RATIO", "0.0005")
    STRATEGY_MIN_EXPECTED_EDGE_RATIO: float = parse_float_env(
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO", "0"
    )
    STRATEGY_EXIT_RULES: str = os.getenv("STRATEGY_EXIT_RULES", "opposite_cross,max_holding_time")
    STRATEGY_EXIT_MAX_HOLDING_MIN: int = int(os.getenv("STRATEGY_EXIT_MAX_HOLDING_MIN", "0"))
    STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO: float = parse_float_env(
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", "0"
    )
    STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO: float = float(
        os.getenv("STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO", "0")
    )

    # storage
    ENV_ROOT: str = str(PATH_MANAGER.config.env_root)
    RUN_ROOT: str = str(PATH_MANAGER.config.run_root)
    DATA_ROOT: str = str(PATH_MANAGER.config.data_root)
    LOG_ROOT: str = str(PATH_MANAGER.config.log_root)
    BACKUP_ROOT: str = str(PATH_MANAGER.config.backup_root)
    ARCHIVE_ROOT: str = str(PATH_MANAGER.config.archive_root) if PATH_MANAGER.config.archive_root else ""
    DB_PATH: str = resolve_db_path_from_env(os.getenv("MODE", "paper"))
    RUN_LOCK_PATH: str = resolve_run_lock_path_from_env(os.getenv("MODE", "paper"))
    DB_BUSY_TIMEOUT_MS: int = int(os.getenv("DB_BUSY_TIMEOUT_MS", "5000"))
    DB_LOCK_RETRY_COUNT: int = int(os.getenv("DB_LOCK_RETRY_COUNT", "2"))
    DB_LOCK_RETRY_BACKOFF_MS: int = int(os.getenv("DB_LOCK_RETRY_BACKOFF_MS", "50"))

    # paper portfolio
    START_CASH_KRW: float = float(os.getenv("START_CASH_KRW", "1000000"))
    BUY_FRACTION: float = float(os.getenv("BUY_FRACTION", "0.99"))
    # 공통 기본 수수료율. 운영에서는 LIVE/PAPER 수수료율을 각각 명시한다.
    FEE_RATE: float = float(os.getenv("FEE_RATE", "0.0004"))
    # live pretrade 잔고/현금 검증 전용 보수적 추정 수수료율.
    # 우선순위: LIVE_FEE_RATE_ESTIMATE > FEE_RATE > 0.0025(default)
    LIVE_FEE_RATE_ESTIMATE: float = parse_float_env(
        "LIVE_FEE_RATE_ESTIMATE", os.getenv("FEE_RATE", "0.0025")
    )
    # paper 체결/손익 시뮬레이션 전용 수수료율.
    # 우선순위:
    #   PAPER_FEE_RATE > PAPER_FEE_RATE_ESTIMATE > FEE_RATE > LIVE_FEE_RATE_ESTIMATE > 0.0025
    PAPER_FEE_RATE: float = float(
        os.getenv(
            "PAPER_FEE_RATE",
            os.getenv(
                "PAPER_FEE_RATE_ESTIMATE",
                os.getenv("FEE_RATE", os.getenv("LIVE_FEE_RATE_ESTIMATE", "0.0025")),
            ),
        )
    )
    # PAPER_FEE_RATE와 동일 값(기존 키 호환용).
    PAPER_FEE_RATE_ESTIMATE: float = PAPER_FEE_RATE
    SLIPPAGE_BPS: float = float(os.getenv("SLIPPAGE_BPS", "0"))
    # 전략 진입 비용 필터에서 기대 슬리피지를 추정할 때 사용하는 bps.
    # 우선순위:
    #   STRATEGY_ENTRY_SLIPPAGE_BPS > MAX_MARKET_SLIPPAGE_BPS > SLIPPAGE_BPS > 0
    STRATEGY_ENTRY_SLIPPAGE_BPS: float = float(
        os.getenv(
            "STRATEGY_ENTRY_SLIPPAGE_BPS",
            os.getenv("MAX_MARKET_SLIPPAGE_BPS", os.getenv("SLIPPAGE_BPS", "0")),
        )
    )
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
    LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW: float = float(
        os.getenv("LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", "10000")
    )
    LIVE_FILL_FEE_STRICT_MODE: bool = parse_bool_env("LIVE_FILL_FEE_STRICT_MODE", "false")
    LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW: float = float(
        os.getenv("LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", "100000")
    )
    LIVE_FILL_FEE_RATIO_MIN: float = float(os.getenv("LIVE_FILL_FEE_RATIO_MIN", "0.000001"))
    LIVE_FILL_FEE_RATIO_MAX: float = float(os.getenv("LIVE_FILL_FEE_RATIO_MAX", "0.02"))

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
    for root_key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT"):
        root_raw = os.getenv(root_key)
        if root_raw is None or not root_raw.strip():
            issues.append(f"{root_key} must be explicitly set when MODE=live")
            continue
        root_path = Path(root_raw).expanduser()
        if not root_path.is_absolute():
            issues.append(f"{root_key} must be an absolute path when MODE=live")
            continue
        resolved_root = root_path.resolve()
        try:
            resolved_root.relative_to(PROJECT_ROOT.resolve())
            issues.append(f"{root_key} must be outside repository when MODE=live ({resolved_root})")
        except ValueError:
            pass
        if "paper" in {part.lower() for part in resolved_root.parts}:
            issues.append(f"{root_key} must not contain a paper-scoped path segment when MODE=live")

    db_path_env = os.getenv("DB_PATH")
    if db_path_env is None or not db_path_env.strip():
        issues.append(LIVE_DB_PATH_REQUIRED_MSG)
    else:
        configured_db_path = resolve_db_path(cfg.DB_PATH)
        if "/paper/" in configured_db_path.replace("\\", "/"):
            issues.append("DB_PATH must not point to a paper-scoped path when MODE=live")
        try:
            Path(configured_db_path).resolve().relative_to(PROJECT_ROOT.resolve())
            issues.append("DB_PATH must be outside repository when MODE=live")
        except ValueError:
            pass

    lock_path = resolve_run_lock_path_from_env(cfg.MODE)
    if "/paper/" in lock_path.replace("\\", "/"):
        issues.append("RUN_LOCK_PATH must not point to a paper-scoped path when MODE=live")
    try:
        Path(lock_path).resolve().relative_to(PROJECT_ROOT.resolve())
        issues.append("RUN_LOCK_PATH must be outside repository when MODE=live")
    except ValueError:
        pass

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

    strict_min_notional_raw = cfg.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW
    strict_min_notional_value: float | None = None
    try:
        strict_min_notional_value = float(strict_min_notional_raw)
    except (TypeError, ValueError):
        strict_min_notional_value = None

    if bool(cfg.LIVE_FILL_FEE_STRICT_MODE):
        if strict_min_notional_value is None:
            issues.append(
                "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW must be a float-compatible value > 0 "
                "when LIVE_FILL_FEE_STRICT_MODE=true "
                f"(got {strict_min_notional_raw!r})"
            )
        elif not math.isfinite(strict_min_notional_value) or strict_min_notional_value <= 0:
            issues.append(
                "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW must be a finite value > 0 "
                "when LIVE_FILL_FEE_STRICT_MODE=true "
                f"(got {strict_min_notional_raw!r})"
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
