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
    evaluate_market_warning_policy,
    UnsupportedMarketError,
    get_market_registry,
    normalize_market_id,
    validate_exchange_market_id,
)
from .market_catalog_snapshot import record_market_catalog_snapshot
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
_CANONICAL_MARKET_RE = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")


def parse_bool_env(key: str, default: str = "false") -> bool:
    v = os.getenv(key, default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def parse_bool_env_strict(key: str, default: str) -> bool:
    raw = os.getenv(key)
    candidate = raw if raw is not None and raw.strip() != "" else default
    normalized = str(candidate).strip().lower()
    if normalized in ("1", "true", "yes", "y", "on"):
        return True
    if normalized in ("0", "false", "no", "n", "off"):
        return False
    raise ValueError(
        f"{key} must be a boolean value (one of: true/false/1/0/yes/no/on/off), got {candidate!r}"
    )


def parse_float_env(key: str, default: str) -> float:
    raw = os.getenv(key)
    candidate = raw if raw is not None and raw.strip() != "" else default
    try:
        return float(candidate)
    except ValueError as exc:
        raise ValueError(f"{key} must be a float-compatible value, got {candidate!r}") from exc


def parse_non_negative_float_env(key: str, default: str) -> float:
    value = parse_float_env(key, default)
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{key} must be a finite value >= 0, got {value!r}")
    return value


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


def _validate_live_db_path_policy(resolved_db_path: str) -> None:
    db_path = Path(resolved_db_path).resolve()
    if PathManager._contains_segment(db_path, "paper"):
        raise LiveModeValidationError("DB_PATH must not point to a paper-scoped path when MODE=live")
    if PathManager._is_within(db_path, PROJECT_ROOT.resolve()):
        raise LiveModeValidationError("DB_PATH must be outside repository when MODE=live")


def resolve_db_path_for_mode(path: str, *, mode: str) -> str:
    resolved = resolve_db_path(path)
    normalized_mode = str(mode or "").strip().lower() or "paper"
    if normalized_mode == "live":
        _validate_live_db_path_policy(resolved)
    return resolved


def resolve_db_path_for_connection(path: str, *, mode: str | None = None) -> str:
    normalized_mode = str(mode or os.getenv("MODE", "paper") or "paper").strip().lower() or "paper"
    return resolve_db_path_for_mode(path, mode=normalized_mode)


def prepare_db_path_for_connection(path: str, *, mode: str | None = None) -> str:
    normalized_mode = str(mode or os.getenv("MODE", "paper") or "paper").strip().lower() or "paper"
    resolved = resolve_db_path_for_mode(path, mode=normalized_mode)
    if resolved != ":memory:":
        PATH_MANAGER.ensure_parent_dir(Path(resolved))
    return resolved


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
    from .broker.accounts_v1 import (
        AccountsRequiredCurrencyMissingError,
        parse_accounts_response,
        select_pair_balances,
    )
    from .broker.bithumb import classify_private_api_error

    canonical_market = normalize_market_id(str(cfg.PAIR or ""))
    quote_currency, base_currency = canonical_market.split("-", 1)
    is_live_mode = bool(str(cfg.MODE or "").strip().lower() == "live")
    is_live_dry_run = bool(is_live_mode and cfg.LIVE_DRY_RUN and not cfg.LIVE_REAL_ORDER_ARMED)
    execution_mode = "live_dry_run_unarmed" if is_live_dry_run else "live_real_order_path"
    flat_start_allowed, flat_start_reason = _flat_start_safety_for_accounts_preflight()
    allow_missing_base = bool(is_live_dry_run or flat_start_allowed)
    if is_live_dry_run:
        base_missing_policy = "allow_zero_position_start_in_dry_run"
    elif allow_missing_base:
        base_missing_policy = "allow_flat_start_when_no_open_or_unresolved_exposure"
    else:
        base_missing_policy = "block_when_base_currency_row_missing"

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
                "/v1/accounts REST snapshot preflight 인증 실패: "
                f"reason=auth failure reason_code=ACCOUNTS_AUTH_FAILED class={code} summary={summary} "
                f"execution_mode={execution_mode} quote_currency={quote_currency} base_currency={base_currency} "
                f"base_currency_missing_policy={base_missing_policy} detail={detail}"
            ) from exc
        raise AccountsPreflightValidationError(
            "/v1/accounts REST snapshot preflight transport 실패: "
            f"reason=transport failure reason_code=ACCOUNTS_TRANSPORT_FAILED class={code} summary={summary} "
            f"execution_mode={execution_mode} quote_currency={quote_currency} base_currency={base_currency} "
            f"base_currency_missing_policy={base_missing_policy} detail={detail}"
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
        parsed_accounts = parse_accounts_response(response)
        select_pair_balances(
            parsed_accounts,
            order_currency=base_currency,
            payment_currency=quote_currency,
            allow_missing_base=allow_missing_base,
        )
        if allow_missing_base and base_currency not in parsed_accounts.balances:
            LOG.warning(
                "/v1/accounts preflight passed with zero-position allowance: "
                "reason=required currency missing reason_code=ACCOUNTS_BASE_ROW_MISSING_ALLOWED "
                "result=pass_no_position_allowed execution_mode=%s quote_currency=%s base_currency=%s "
                "base_currency_missing_policy=%s flat_start_allowed=%s flat_start_reason=%s row_count=%s currencies=%s",
                execution_mode,
                quote_currency,
                base_currency,
                base_missing_policy,
                int(flat_start_allowed),
                flat_start_reason,
                row_count,
                ",".join(sorted(set(currencies))) or "-",
            )
    except Exception as exc:
        detail_lower = str(exc).lower()
        if isinstance(exc, AccountsRequiredCurrencyMissingError):
            reason = "required currency missing"
            reason_code = "ACCOUNTS_REQUIRED_CURRENCY_MISSING"
        else:
            reason = "duplicate currency" if "duplicate currency row" in detail_lower else "schema mismatch"
            reason_code = (
                "ACCOUNTS_DUPLICATE_CURRENCY"
                if reason == "duplicate currency"
                else "ACCOUNTS_SCHEMA_MISMATCH"
            )
        raise AccountsPreflightValidationError(
            "/v1/accounts REST snapshot preflight validation failed: "
            f"reason={reason} reason_code={reason_code} row_count={row_count} "
            f"currencies={','.join(sorted(set(currencies)))} duplicate_currencies={','.join(duplicate_currencies)} "
            f"execution_mode={execution_mode} quote_currency={quote_currency} base_currency={base_currency} "
            f"base_currency_missing_policy={base_missing_policy} flat_start_allowed={1 if flat_start_allowed else 0} "
            f"flat_start_reason={flat_start_reason} result=fail_real_order_blocked detail={exc}"
        ) from exc


def _flat_start_safety_for_accounts_preflight() -> tuple[bool, str]:
    if str(settings.MODE).strip().lower() != "live":
        return False, "non_live_mode"
    if bool(settings.LIVE_DRY_RUN) or not bool(settings.LIVE_REAL_ORDER_ARMED):
        return False, "not_real_order_path"
    from . import runtime_state
    from .dust import DustClassification, DustState
    from .db_core import ensure_db

    conn = ensure_db()
    try:
        unresolved_row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'RECOVERY_REQUIRED')
            """
        ).fetchone()
        unresolved_count = int(unresolved_row["cnt"] if unresolved_row else 0)
        if unresolved_count > 0:
            return False, f"local_unresolved_or_open_orders={unresolved_count}"
        portfolio_row = conn.execute("SELECT asset_qty FROM portfolio WHERE id=1").fetchone()
        asset_qty = float(portfolio_row["asset_qty"] if portfolio_row is not None else 0.0)
        if abs(asset_qty) > 1e-12:
            dust = DustClassification.from_metadata(runtime_state.snapshot().last_reconcile_metadata)
            if dust.classification == DustState.HARMLESS_DUST.value and dust.allow_resume and dust.effective_flat:
                return True, f"flat_start_effective_flat({dust.summary})"
            return False, f"local_position_present={asset_qty:.12f}"
    finally:
        conn.close()
    return True, "flat_start_safe"


def resolve_db_path_from_env(mode: str) -> str:
    raw_db_path = os.getenv("DB_PATH")
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode == "live" and (raw_db_path is None or not raw_db_path.strip()):
        raise LiveModeValidationError(LIVE_DB_PATH_REQUIRED_MSG)
    if raw_db_path and raw_db_path.strip():
        return resolve_db_path_for_mode(raw_db_path, mode=normalized_mode)
    return resolve_db_path_for_mode(str(PATH_MANAGER.primary_db_path()), mode=normalized_mode)


def resolve_strategy_name_from_env() -> str:
    raw = os.getenv("STRATEGY_NAME")
    normalized = str(raw or "").strip().lower()
    return normalized or DEFAULT_RUNTIME_STRATEGY


def _normalize_config_market_input(raw_market: str, *, env_key: str, strict_canonical: bool) -> str:
    token = str(raw_market or "").strip().upper()
    if not token:
        raise ValueError(f"{env_key} must not be empty")

    if " " in token:
        raise ValueError(
            f"invalid {env_key} format: {raw_market!r}; market code must not contain spaces"
        )

    if strict_canonical:
        if not _CANONICAL_MARKET_RE.fullmatch(token):
            raise ValueError(
                f"invalid {env_key} format for MODE=live: {raw_market!r}; "
                "must be canonical QUOTE-BASE token like 'KRW-BTC' "
                "(legacy 'BTC_KRW' and bare 'BTC' are not allowed in live mode)"
            )
        return token

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
    normalized_mode = str(os.getenv("MODE", "paper") or "paper").strip().lower() or "paper"
    strict_canonical = normalized_mode == "live"
    raw_market = os.getenv("MARKET")
    raw_pair = os.getenv("PAIR")

    has_market = raw_market is not None and raw_market.strip() != ""
    has_pair = raw_pair is not None and raw_pair.strip() != ""

    if has_market:
        canonical_market = _normalize_config_market_input(
            raw_market,
            env_key="MARKET",
            strict_canonical=True,
        )
    elif has_pair:
        canonical_market = _normalize_config_market_input(
            raw_pair,
            env_key="PAIR",
            strict_canonical=strict_canonical,
        )
    else:
        canonical_market = DEFAULT_CANONICAL_MARKET

    if has_market and has_pair:
        canonical_pair = _normalize_config_market_input(
            raw_pair,
            env_key="PAIR",
            strict_canonical=strict_canonical,
        )
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
    SMA_COST_EDGE_ENABLED: bool = parse_bool_env_strict("SMA_COST_EDGE_ENABLED", "true")
    SMA_COST_EDGE_MIN_RATIO: float = parse_non_negative_float_env(
        "SMA_COST_EDGE_MIN_RATIO",
        os.getenv("STRATEGY_MIN_EXPECTED_EDGE_RATIO", "0"),
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
    BITHUMB_WS_MYASSET_ENABLED: bool = parse_bool_env("BITHUMB_WS_MYASSET_ENABLED", "false")
    BITHUMB_WS_MYASSET_SUBSCRIBE_TICKET: str = os.getenv("BITHUMB_WS_MYASSET_SUBSCRIBE_TICKET", "")
    BITHUMB_WS_MYASSET_STALE_AFTER_MS: int = int(os.getenv("BITHUMB_WS_MYASSET_STALE_AFTER_MS", "15000"))
    BITHUMB_WS_MYASSET_RECV_TIMEOUT_SEC: float = parse_float_env("BITHUMB_WS_MYASSET_RECV_TIMEOUT_SEC", "5")
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
    MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC: float = parse_float_env(
        "MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC", "900"
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
    if not cleaned:
        return {"CAUTION", "UNKNOWN"}
    cleaned.add("UNKNOWN")
    return cleaned


def _validate_market_registry_contract(
    cfg: Settings,
    *,
    context: str,
    record_snapshot: bool,
    force_refresh: bool,
) -> None:
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
    strict_canonical = normalized_mode == "live"
    try:
        normalized_market_input = _normalize_config_market_input(
            configured_market,
            env_key="PAIR",
            strict_canonical=strict_canonical,
        )
    except ValueError as exc:
        raise MarketPreflightValidationError(
            f"market {context} rejected invalid configured market format: "
            f"pair={configured_market!r} mode={normalized_mode} detail={exc}"
        ) from exc
    warning_block_states = _warning_state_set(cfg.MARKET_PREFLIGHT_WARNING_STATES)
    if cfg.MARKET_REGISTRY_CACHE_TTL_SEC < 0:
        raise MarketPreflightValidationError(
            "MARKET_REGISTRY_CACHE_TTL_SEC must be >= 0"
        )

    try:
        registry = _fetch_market_registry_for_preflight(
            refresh=force_refresh,
            ttl_seconds=cfg.MARKET_REGISTRY_CACHE_TTL_SEC,
            is_details=True,
        )
    except Exception as exc:
        msg = (
            f"market {context} catalog fetch failed: "
            "endpoint=/v1/market/all isDetails=true "
            f"pair={configured_market!r} normalized={normalized_market_input} "
            f"mode={normalized_mode} dry_run={is_dryrun} block_on_catalog_error={block_on_catalog_error} "
            f"schema_drift={isinstance(exc, MarketCatalogError)} "
            f"error={type(exc).__name__}: {exc}"
        )
        if block_on_catalog_error:
            raise MarketPreflightValidationError(msg) from exc
        LOG.warning("%s; continuing by policy", msg)
        return

    try:
        canonical_market = validate_exchange_market_id(normalized_market_input, registry=registry)
    except (UnsupportedMarketError, ValueError) as exc:
        raise MarketPreflightValidationError(
            f"market {context} rejected unsupported pair: "
            f"pair={configured_market!r} normalized={normalized_market_input}"
        ) from exc

    market_info = registry.get(canonical_market)
    if market_info is None:
        raise MarketPreflightValidationError(
            f"market {context} registry inconsistency: "
            f"pair={configured_market!r} canonical={canonical_market}"
        )

    warning_decision = evaluate_market_warning_policy(
        raw_warning=market_info.market_warning,
        warning_block_states=warning_block_states,
    )
    if warning_decision.is_warning_state:
        msg = (
            f"market {context} detected warning state: "
            f"pair={configured_market!r} canonical={canonical_market} "
            f"market_warning={warning_decision.normalized_warning}"
        )
        if warning_decision.should_block and block_on_warning:
            raise MarketPreflightValidationError(msg)
        LOG.warning(
            "%s; continuing by policy (mode=%s, dry_run=%s, block_on_warning=%s, warning_block_states=%s)",
            msg,
            normalized_mode,
            is_dryrun,
            block_on_warning,
            sorted(warning_block_states),
        )

    if record_snapshot:
        try:
            record_market_catalog_snapshot(
                path_manager=PATH_MANAGER,
                mode=normalized_mode,
                source="market_preflight",
                markets=registry.items(),
            )
        except Exception as exc:
            LOG.warning(
                "market catalog snapshot update failed mode=%s source=market_preflight error=%s: %s",
                normalized_mode,
                type(exc).__name__,
                exc,
            )


def validate_market_preflight(cfg: Settings) -> None:
    normalized_mode = str(cfg.MODE or "").strip().lower()
    force_refresh = (
        bool(cfg.MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH)
        if os.getenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH") not in (None, "")
        else normalized_mode == "live"
    )
    _validate_market_registry_contract(
        cfg,
        context="preflight",
        record_snapshot=True,
        force_refresh=force_refresh,
    )

    try:
        validate_accounts_preflight(cfg)
    except AccountsPreflightValidationError as exc:
        if normalized_mode == "live":
            raise MarketPreflightValidationError(str(exc)) from exc
        LOG.warning(
            "accounts REST snapshot preflight warning (mode=%s): %s",
            normalized_mode,
            exc,
        )


def validate_market_runtime(cfg: Settings) -> None:
    _validate_market_registry_contract(
        cfg,
        context="runtime",
        record_snapshot=False,
        force_refresh=True,
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
        try:
            resolve_db_path_for_mode(cfg.DB_PATH, mode="live")
        except ValueError as exc:
            issues.append(str(exc))

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
    if not cfg.SMA_COST_EDGE_ENABLED:
        LOG.warning(
            "live preflight warning: SMA_COST_EDGE_ENABLED=false (cost-edge entry block disabled for sma_with_filter)"
        )

    from .broker.order_rules import (
        get_effective_order_rules,
        optional_rule_source_warnings,
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
        rule_source_issues = required_rule_source_issues(
            resolved.source,
            require_price_unit_sources=False,
        )
        if rule_source_issues:
            if cfg.LIVE_DRY_RUN:
                LOG.warning(
                    "live dry-run preflight surfaced documented order-rule source gaps: %s",
                    "; ".join(rule_source_issues),
                )
            else:
                issues.extend(rule_source_issues)
        source_warnings = optional_rule_source_warnings(resolved.source)
        if source_warnings:
            LOG.warning(
                "live preflight warning: optional order-rule source gaps detected: %s",
                "; ".join(source_warnings),
            )
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
