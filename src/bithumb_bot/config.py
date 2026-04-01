from __future__ import annotations

import math
import os
import logging
import re
from dataclasses import dataclass
from pathlib import Path

from .markets import (
    MarketCatalogError,
    MarketRegistry,
    UnsupportedMarketError,
    get_market_registry,
    normalize_market_id,
    validate_exchange_market_id,
)
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
DEFAULT_CANONICAL_MARKET = "KRW-BTC"
LEGACY_V1_ORDER_SCAN_ENV_KEYS = (
    "BITHUMB_V1_ORDER_SCAN_MARKET",
    "BITHUMB_V1_ORDER_SCAN_STATES",
    "BITHUMB_V1_ORDER_SCAN_LIMIT",
)
LOG = logging.getLogger(__name__)
_MARKET_TOKEN_RE = re.compile(r"^[A-Z0-9]+$")


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


class MarketPreflightValidationError(ValueError):
    pass


class AccountsPreflightValidationError(ValueError):
    pass


def _fetch_accounts_payload_for_preflight(*, api_key: str, api_secret: str, base_url: str) -> object:
    from .broker.bithumb import BithumbPrivateAPI

    private_api = BithumbPrivateAPI(
        api_key=api_key,
        api_secret=api_secret,
        base_url=base_url,
        dry_run=False,
    )
    return private_api.request("GET", "/v1/accounts", params={}, retry_safe=True)


def validate_accounts_preflight(cfg: Settings) -> None:
    from .broker.bithumb import BithumbBroker, classify_private_api_error

    canonical_market = normalize_market_id(str(cfg.PAIR or ""))
    quote_currency, base_currency = canonical_market.split("-", 1)

    try:
        response = _fetch_accounts_payload_for_preflight(
            api_key=str(cfg.BITHUMB_API_KEY or ""),
            api_secret=str(cfg.BITHUMB_API_SECRET or ""),
            base_url=str(cfg.BITHUMB_API_BASE or ""),
        )
    except Exception as exc:
        code, summary = classify_private_api_error(exc)
        detail = str(exc)
        if code in {"AUTH_SIGN", "PERMISSION"}:
            raise AccountsPreflightValidationError(
                "/v1/accounts preflight 인증 실패: "
                f"reason=auth failure reason_code=ACCOUNTS_AUTH_FAILED class={code} summary={summary} detail={detail}"
            ) from exc
        raise AccountsPreflightValidationError(
            "/v1/accounts preflight transport 실패: "
            f"reason=transport failure reason_code=ACCOUNTS_TRANSPORT_FAILED class={code} summary={summary} detail={detail}"
        ) from exc

    row_count = len(response) if isinstance(response, list) else 0
    currencies: list[str] = []
    if isinstance(response, list):
        for row in response:
            if not isinstance(row, dict):
                continue
            token = str(row.get("currency") or "").strip().upper()
            if token:
                currencies.append(token)
    duplicate_currencies = sorted({token for token in currencies if currencies.count(token) > 1})

    try:
        accounts = BithumbBroker._parse_accounts_payload(response)
    except Exception as exc:
        detail_lower = str(exc).lower()
        reason = "duplicate currency" if "duplicate currency row" in detail_lower else "schema mismatch"
        reason_code = (
            "ACCOUNTS_DUPLICATE_CURRENCY"
            if reason == "duplicate currency"
            else "ACCOUNTS_SCHEMA_MISMATCH"
        )
        raise AccountsPreflightValidationError(
            "/v1/accounts preflight schema mismatch: "
            f"reason={reason} reason_code={reason_code} row_count={row_count} "
            f"currencies={','.join(sorted(set(currencies)))} duplicate_currencies={','.join(duplicate_currencies)} detail={exc}"
        ) from exc

    missing: list[str] = []
    if quote_currency not in accounts:
        missing.append(quote_currency)
    if base_currency not in accounts:
        missing.append(base_currency)
    if missing:
        raise AccountsPreflightValidationError(
            "/v1/accounts preflight required currency missing: "
            f"reason=required currency missing reason_code=ACCOUNTS_REQUIRED_CURRENCY_MISSING market={canonical_market} "
            f"row_count={row_count} currencies={','.join(sorted(accounts.keys()))} missing={','.join(missing)}"
        )


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


def _normalize_config_market_input(raw_market: str, *, env_key: str) -> str:
    token = str(raw_market or "").strip().upper().replace(" ", "")
    if not token:
        raise ValueError(f"{env_key} must not be empty")

    if "-" in token:
        left, right = token.split("-", 1)
        if not (_MARKET_TOKEN_RE.fullmatch(left or "") and _MARKET_TOKEN_RE.fullmatch(right or "")):
            raise ValueError(
                f"invalid {env_key} format: {raw_market!r}; expected canonical 'KRW-BTC' style token"
            )
        return normalize_market_id(token)

    if "_" in token:
        left, right = token.split("_", 1)
        if not (_MARKET_TOKEN_RE.fullmatch(left or "") and _MARKET_TOKEN_RE.fullmatch(right or "")):
            raise ValueError(
                f"invalid {env_key} format: {raw_market!r}; expected legacy 'BTC_KRW' style token"
            )
        return normalize_market_id(token)

    if not _MARKET_TOKEN_RE.fullmatch(token):
        raise ValueError(
            f"invalid {env_key} format: {raw_market!r}; expected one of KRW-BTC, BTC_KRW, BTC"
        )
    return normalize_market_id(token)


def resolve_market_from_env() -> str:
    raw_market = os.getenv("MARKET")
    raw_pair = os.getenv("PAIR")

    has_market = raw_market is not None and raw_market.strip() != ""
    has_pair = raw_pair is not None and raw_pair.strip() != ""

    if has_market:
        canonical_market = _normalize_config_market_input(raw_market, env_key="MARKET")
    elif has_pair:
        canonical_market = _normalize_config_market_input(raw_pair, env_key="PAIR")
    else:
        canonical_market = DEFAULT_CANONICAL_MARKET

    if has_market and has_pair:
        canonical_pair = _normalize_config_market_input(raw_pair, env_key="PAIR")
        if canonical_pair != canonical_market:
            raise ValueError(
                "MARKET and PAIR resolve to different canonical markets: "
                f"MARKET={raw_market!r}->{canonical_market}, PAIR={raw_pair!r}->{canonical_pair}"
            )

    return canonical_market


def default_run_lock_path(mode: str) -> str:
    normalized_mode = (mode or "paper").strip().lower() or "paper"
    return str(PATH_MANAGER.config.run_root / normalized_mode / "bithumb-bot.lock")


def resolve_run_lock_path(path: str, *, mode: str | None = None) -> str:
    normalized_mode = str(mode or os.getenv("MODE", "paper") or "paper").strip().lower() or "paper"
    resolved = PathManager._resolve_explicit_root(
        "RUN_LOCK_PATH",
        path,
        normalized_mode,
        PROJECT_ROOT,
    )
    return str(resolved)


def resolve_run_lock_path_from_env(mode: str) -> str:
    normalized_mode = str(mode or "paper").strip().lower() or "paper"
    raw = os.getenv("RUN_LOCK_PATH")
    if raw and raw.strip():
        return resolve_run_lock_path(raw, mode=normalized_mode)
    return default_run_lock_path(normalized_mode)


@dataclass(frozen=True)
class Settings:
    # runtime
    MODE: str = os.getenv("MODE", "paper")
    PAIR: str = resolve_market_from_env()
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
    MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR: bool = parse_bool_env(
        "MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR", ""
    )
    MARKET_PREFLIGHT_BLOCK_ON_WARNING: bool = parse_bool_env("MARKET_PREFLIGHT_BLOCK_ON_WARNING", "")
    MARKET_PREFLIGHT_WARNING_STATES: str = os.getenv("MARKET_PREFLIGHT_WARNING_STATES", "CAUTION")
    MARKET_REGISTRY_CACHE_TTL_SEC: float = parse_float_env("MARKET_REGISTRY_CACHE_TTL_SEC", "900")
    MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH: bool = parse_bool_env(
        "MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", ""
    )

settings = Settings()


def validate_mode_or_raise(mode: str) -> None:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode in ALLOWED_RUNTIME_MODES:
        return
    allowed = ", ".join(ALLOWED_RUNTIME_MODES)
    raise ModeValidationError(
        f"invalid MODE={mode!r}; allowed values: {allowed}"
    )


def _fetch_market_registry_for_preflight(
    *,
    refresh: bool,
    ttl_seconds: float,
    is_details: bool,
) -> MarketRegistry:
    return get_market_registry(
        refresh=refresh,
        client=None,
        is_details=is_details,
        ttl_seconds=ttl_seconds,
    )


def _warning_state_set(raw_states: str) -> set[str]:
    states = {token.strip().upper() for token in str(raw_states or "").split(",")}
    cleaned = {token for token in states if token}
    return cleaned or {"CAUTION"}


def validate_market_preflight(cfg: Settings) -> None:
    normalized_mode = str(cfg.MODE or "").strip().lower()
    is_dryrun = normalized_mode == "live" and bool(cfg.LIVE_DRY_RUN)
    is_live_real = normalized_mode == "live" and not is_dryrun
    block_on_catalog_error = (
        bool(cfg.MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR)
        if os.getenv("MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR") not in (None, "")
        else is_live_real
    )
    block_on_warning = (
        bool(cfg.MARKET_PREFLIGHT_BLOCK_ON_WARNING)
        if os.getenv("MARKET_PREFLIGHT_BLOCK_ON_WARNING") not in (None, "")
        # Safety default: block warning states only for armed live-real execution.
        # Dry-run/paper paths keep warning-only behavior unless explicitly overridden.
        else is_live_real
    )

    configured_market = str(cfg.PAIR or "")
    normalized_market_input = normalize_market_id(configured_market)
    warning_block_states = _warning_state_set(cfg.MARKET_PREFLIGHT_WARNING_STATES)
    if cfg.MARKET_REGISTRY_CACHE_TTL_SEC < 0:
        raise MarketPreflightValidationError(
            "MARKET_REGISTRY_CACHE_TTL_SEC must be >= 0"
        )
    force_refresh = (
        bool(cfg.MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH)
        if os.getenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH") not in (None, "")
        else normalized_mode == "live"
    )

    try:
        registry = _fetch_market_registry_for_preflight(
            refresh=force_refresh,
            ttl_seconds=cfg.MARKET_REGISTRY_CACHE_TTL_SEC,
            is_details=True,
        )
    except Exception as exc:
        msg = (
            "market preflight catalog fetch failed: "
            f"pair={configured_market!r} normalized={normalized_market_input} "
            f"error={type(exc).__name__}: {exc}"
        )
        if block_on_catalog_error:
            raise MarketPreflightValidationError(msg) from exc
        LOG.warning("%s; continuing by policy (mode=%s, dry_run=%s)", msg, normalized_mode, is_dryrun)
        return

    try:
        canonical_market = validate_exchange_market_id(normalized_market_input, registry=registry)
    except (UnsupportedMarketError, ValueError) as exc:
        raise MarketPreflightValidationError(
            "market preflight rejected unsupported pair: "
            f"pair={configured_market!r} normalized={normalized_market_input}"
        ) from exc

    market_info = registry.get(canonical_market)
    if market_info is None:
        raise MarketPreflightValidationError(
            "market preflight registry inconsistency: "
            f"pair={configured_market!r} canonical={canonical_market}"
        )

    market_warning = str(market_info.market_warning or "").strip().upper()
    if market_warning and market_warning in warning_block_states:
        msg = (
            "market preflight detected warning state: "
            f"pair={configured_market!r} canonical={canonical_market} market_warning={market_warning}"
        )
        if block_on_warning:
            raise MarketPreflightValidationError(msg)
        LOG.warning("%s; continuing by policy (mode=%s, dry_run=%s)", msg, normalized_mode, is_dryrun)

    try:
        validate_accounts_preflight(cfg)
    except AccountsPreflightValidationError as exc:
        if normalized_mode == "live":
            raise MarketPreflightValidationError(str(exc)) from exc
        LOG.warning(
            "accounts preflight warning (mode=%s): %s",
            normalized_mode,
            exc,
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

    lock_path: str | None = None
    try:
        lock_path = resolve_run_lock_path_from_env(cfg.MODE)
    except ValueError as exc:
        issues.append(str(exc))
    if lock_path:
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

    from .broker.order_rules import (
        get_effective_order_rules,
        required_rule_issues,
        required_rule_source_issues,
    )

    legacy_scan_keys = [
        key for key in LEGACY_V1_ORDER_SCAN_ENV_KEYS if os.getenv(key) not in (None, "")
    ]
    if legacy_scan_keys:
        issues.append(
            "legacy /v1/orders broad-scan env is not allowed; "
            "identifier-based lookup only after /v1/orders transition. "
            "unset keys: " + ", ".join(legacy_scan_keys)
        )

    try:
        resolved = get_effective_order_rules(cfg.PAIR)
        resolved_rules = resolved.rules
        issues.extend(required_rule_issues(resolved_rules))
        rule_source_issues = required_rule_source_issues(resolved.source)
        if rule_source_issues:
            if cfg.LIVE_DRY_RUN:
                LOG.warning(
                    "live dry-run preflight surfaced documented order-rule source gaps: %s",
                    "; ".join(rule_source_issues),
                )
            else:
                issues.extend(rule_source_issues)
    except Exception as exc:
        issues.append(f"failed to resolve order rules: {type(exc).__name__}: {exc}")

    if not issues:
        try:
            validate_market_preflight(cfg)
        except MarketPreflightValidationError as exc:
            issues.append(str(exc))

    if issues:
        raise LiveModeValidationError(
            "live mode preflight validation failed: " + "; ".join(issues)
        )
