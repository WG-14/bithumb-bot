from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Literal


CONFIG_SCHEMA_VERSION = "config_spec_v1"

ModeScope = Literal["common", "paper", "live", "internal", "bootstrap"]
SafetyTier = Literal["P0", "P1", "P2", "internal"]
SideEffectClass = Literal["none", "external_notification", "broker_private"]


@dataclass(frozen=True)
class EnvVarSpec:
    name: str
    value_type: str = "string"
    default: str = ""
    default_resolver: str = ""
    description: str = ""
    mode_scope: ModeScope = "common"
    secret: bool = False
    deprecated: bool = False
    ignored: bool = False
    required_in_live: bool = False
    operator_visible: bool = True
    safety_tier: SafetyTier = "P1"
    category: str = "runtime"
    example: str = ""
    docs: str = ""
    validation_kind: str = ""
    min_live_bytes: int | None = None
    side_effect_class: SideEffectClass = "none"

    def payload(self) -> dict[str, object]:
        return asdict(self)


DECLARED_ENV_NAMES: tuple[str, ...] = (
    "ACTIVE_STRATEGIES",
    "APPROVED_STRATEGY_PROFILE_PATH",
    "ARCHIVE_ROOT",
    "BACKUP_DIR",
    "BACKUP_ROOT",
    "BACKUP_RETENTION_COUNT",
    "BACKUP_RETENTION_DAYS",
    "BITHUMB_API_BASE",
    "BITHUMB_API_KEY",
    "BITHUMB_API_SECRET",
    "BITHUMB_AUTH_DIAGNOSTICS",
    "BITHUMB_CANCEL_RETRY_ATTEMPTS",
    "BITHUMB_CANCEL_RETRY_BACKOFF_SEC",
    "BITHUMB_DEPLOY_COMMIT_SHA",
    "BITHUMB_DEPLOY_DIRTY",
    "BITHUMB_ENV_FILE",
    "BITHUMB_ENV_FILE_LIVE",
    "BITHUMB_ENV_FILE_PAPER",
    "BITHUMB_ORDER_RPS_LIMIT",
    "BITHUMB_PYTEST_RUN_ID",
    "BITHUMB_PYTEST_STARTED",
    "BITHUMB_PYTEST_SUITE",
    "BITHUMB_PYTEST_SUMMARY_ON_SUCCESS",
    "BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK",
    "BITHUMB_RESEARCH_MAX_WORKERS",
    "BITHUMB_RESEARCH_MP_START_METHOD",
    "BITHUMB_TOTAL_PROCESS_BUDGET",
    "BITHUMB_PRIVATE_RPS_LIMIT",
    "BITHUMB_WS_MYASSET_ENABLED",
    "BITHUMB_WS_MYASSET_RECV_TIMEOUT_SEC",
    "BITHUMB_WS_MYASSET_STALE_AFTER_MS",
    "BITHUMB_WS_MYASSET_SUBSCRIBE_TICKET",
    "BLOCK_ON_OPEN_ORDER",
    "BLOCK_ON_SUBMIT_UNKNOWN",
    "BUY_FRACTION",
    "BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED",
    "COOLDOWN_MIN",
    "DATA_ROOT",
    "DB_BUSY_TIMEOUT_MS",
    "DB_LOCK_RETRY_BACKOFF_MS",
    "DB_LOCK_RETRY_COUNT",
    "DB_PATH",
    "ENTRY_EDGE_BUFFER_RATIO",
    "ENV_ROOT",
    "EVERY",
    "EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL",
    "EXECUTION_CALIBRATION_ARTIFACT_HASH",
    "EXECUTION_CALIBRATION_REQUIRED",
    "EXECUTION_DECISION_GUARD_MS",
    "EXECUTION_DEPTH_REQUIRED",
    "EXECUTION_ENGINE",
    "EXECUTION_FEE_SOURCE",
    "EXECUTION_FILL_REFERENCE_POLICY",
    "EXECUTION_INTRA_CANDLE_PATH_AVAILABLE",
    "EXECUTION_LATENCY_MODEL_TYPE",
    "EXECUTION_LATENCY_MS",
    "EXECUTION_MARKET_IMPACT_REQUIRED",
    "EXECUTION_MAX_QUOTE_WAIT_MS",
    "EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION",
    "EXECUTION_MISSING_QUOTE_POLICY",
    "EXECUTION_ORDER_FAILURE_MODEL_TYPE",
    "EXECUTION_ORDER_FAILURE_RATE",
    "EXECUTION_PARTIAL_FILL_MODEL_TYPE",
    "EXECUTION_PARTIAL_FILL_RATE",
    "EXECUTION_QUEUE_POSITION_REQUIRED",
    "EXECUTION_QUOTE_AGE_LIMIT_MS",
    "EXECUTION_QUOTE_SOURCE",
    "EXECUTION_REALITY_LEVEL",
    "EXECUTION_SLIPPAGE_SOURCE",
    "EXECUTION_TOP_OF_BOOK_IS_FULL_DEPTH",
    "EXECUTION_TOP_OF_BOOK_REQUIRED",
    "EXECUTION_TRADE_TICK_REQUIRED",
    "FEE_RATE",
    "HEALTH_MAX_CANDLE_AGE_SEC",
    "HEALTH_MAX_ERROR_COUNT",
    "INTERVAL",
    "KILL_SWITCH",
    "KILL_SWITCH_LIQUIDATE",
    "LEGACY_DEFAULT_STRATEGY_COMPAT",
    "LIVE_ALLOW_ORDER_RULE_FALLBACK",
    "LIVE_DRY_RUN",
    "LIVE_EXECUTION_QUALITY_GATE_ENABLED",
    "LIVE_EXECUTION_QUALITY_GATE_MODE",
    "LIVE_EXECUTION_QUALITY_MAX_MODEL_BREACH_RATE",
    "LIVE_EXECUTION_QUALITY_MAX_P90_SLIPPAGE_BPS",
    "LIVE_EXECUTION_QUALITY_MAX_P95_FULL_FILL_LATENCY_MS",
    "LIVE_EXECUTION_QUALITY_MAX_PARTIAL_FILL_RATE",
    "LIVE_EXECUTION_QUALITY_MIN_SAMPLE",
    "LIVE_FEE_RATE_ESTIMATE",
    "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW",
    "LIVE_FILL_FEE_RATIO_MAX",
    "LIVE_FILL_FEE_RATIO_MIN",
    "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW",
    "LIVE_FILL_FEE_STRICT_MODE",
    "LIVE_MIN_ORDER_QTY",
    "LIVE_ORDER_MAX_QTY_DECIMALS",
    "LIVE_ORDER_QTY_STEP",
    "LIVE_ORDER_RULE_FALLBACK_PROFILE",
    "LIVE_PERFORMANCE_GATE_ENABLED",
    "LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO",
    "LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW",
    "LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW",
    "LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR",
    "LIVE_PERFORMANCE_GATE_MIN_SAMPLE",
    "LIVE_PERFORMANCE_GATE_RECENT_LIMIT",
    "LIVE_PERFORMANCE_GATE_SCOPE",
    "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS",
    "LIVE_PRICE_REFERENCE_MAX_AGE_SEC",
    "LIVE_REAL_ORDER_ARMED",
    "LIVE_SUBMIT_CONTRACT_PROFILE",
    "LOG_ROOT",
    "MARKET",
    "MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR",
    "MARKET_PREFLIGHT_BLOCK_ON_WARNING",
    "MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH",
    "MARKET_PREFLIGHT_WARNING_STATES",
    "MARKET_REGISTRY_CACHE_TTL_SEC",
    "MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC",
    "MAX_DAILY_LOSS_KRW",
    "MAX_DAILY_ORDER_COUNT",
    "MAX_MARKET_SLIPPAGE_BPS",
    "MAX_OPEN_ORDER_AGE_SEC",
    "MAX_OPEN_POSITIONS",
    "MAX_ORDERBOOK_SPREAD_BPS",
    "MAX_ORDER_KRW",
    "MAX_POSITION_LOSS_PCT",
    "MIN_GAP",
    "MIN_MARGIN_AFTER_COST_RATIO",
    "MIN_NET_EDGE_KRW",
    "MIN_ORDER_NOTIONAL_KRW",
    "MODE",
    "NOTIFIER_DEDUPE_WINDOW_SEC",
    "NOTIFIER_ENABLED",
    "NOTIFIER_TIMEOUT_SEC",
    "NOTIFIER_WEBHOOK_URL",
    "NTFY_PRIORITY_FAILURE",
    "NTFY_PRIORITY_SUCCESS",
    "NTFY_SERVER",
    "NTFY_TITLE_PREFIX",
    "NTFY_TOPIC",
    "NTFY_URL",
    "OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC",
    "PAIR",
    "PAPER_EXECUTION_LATENCY_MS",
    "PAPER_EXECUTION_MODEL",
    "PAPER_EXECUTION_ORDER_FAILURE_RATE",
    "PAPER_EXECUTION_PARTIAL_FILL_FRACTION",
    "PAPER_EXECUTION_PARTIAL_FILL_RATE",
    "PAPER_EXECUTION_STRESS_SEED",
    "PAPER_FEE_RATE",
    "PAPER_FEE_RATE_ESTIMATE",
    "PRETRADE_BALANCE_BUFFER_BPS",
    "PRE_TRADE_ECONOMICS_BLOCKING_ENABLED",
    "RESEARCH_NOTIFICATION_POLICY",
    "REQUIRE_BROKER_LOCAL_CONVERGENCE",
    "RESIDUAL_BUY_SIZING_MODE",
    "RESIDUAL_INVENTORY_MODE",
    "RESIDUAL_LIVE_SELL_MODE",
    "RUNTIME_STRATEGY_SET_JSON",
    "RUN_LOCK_PATH",
    "RUN_ROOT",
    "SLACK_WEBHOOK_URL",
    "SLIPPAGE_BPS",
    "SMA_COST_EDGE_ENABLED",
    "SMA_COST_EDGE_MIN_RATIO",
    "SMA_FILTER_GAP_MIN_RATIO",
    "SMA_FILTER_OVEREXT_LOOKBACK",
    "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
    "SMA_FILTER_VOL_MIN_RANGE_RATIO",
    "SMA_FILTER_VOL_WINDOW",
    "SMA_LONG",
    "SMA_MARKET_REGIME_ENABLED",
    "SMA_SHORT",
    "SNAPSHOT_ROOT",
    "START_CASH_KRW",
    "STRATEGY_APPROVED_PROFILE_PATH",
    "STRATEGY_CANDIDATE_PROFILE_PATH",
    "STRATEGY_ENTRY_SLIPPAGE_BPS",
    "STRATEGY_EXIT_MAX_HOLDING_MIN",
    "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
    "STRATEGY_EXIT_RULES",
    "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
    "STRATEGY_EXIT_STOP_LOSS_RATIO",
    "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
    "STRATEGY_NAME",
    "STRATEGY_PARAMETERS_JSON",
    "TARGET_EXECUTION_SHADOW",
    "TARGET_EXPOSURE_KRW",
    "TARGET_HOLD_POLICY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "PYTEST_XDIST_WORKER",
    "PYTEST_XDIST_WORKER_COUNT",
    "PYTEST_XDIST_WORKERS",
    "XDG_STATE_HOME",
)

SECRET_KEYS = {
    "BITHUMB_API_KEY",
    "BITHUMB_API_SECRET",
    "NOTIFIER_WEBHOOK_URL",
    "SLACK_WEBHOOK_URL",
    "TELEGRAM_BOT_TOKEN",
}

EXTERNAL_NOTIFICATION_ENV_KEYS = {
    "NTFY_TOPIC",
    "NOTIFIER_WEBHOOK_URL",
    "SLACK_WEBHOOK_URL",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
}

BROKER_PRIVATE_ENV_KEYS = {
    "BITHUMB_API_KEY",
    "BITHUMB_API_SECRET",
}

PYTEST_INHERITANCE_UNSAFE_ENV_KEYS = frozenset(
    sorted(EXTERNAL_NOTIFICATION_ENV_KEYS | BROKER_PRIVATE_ENV_KEYS)
)

JWT_HS256_MIN_SECRET_BYTES = 32
JWT_HS256_SECRET_VALIDATION_KIND = "jwt_hs256_secret"

LIVE_REQUIRED_KEYS = {
    "BITHUMB_API_KEY",
    "BITHUMB_API_SECRET",
    "BACKUP_ROOT",
    "DATA_ROOT",
    "DB_PATH",
    "ENV_ROOT",
    "LOG_ROOT",
    "MAX_DAILY_LOSS_KRW",
    "MAX_DAILY_ORDER_COUNT",
    "MAX_MARKET_SLIPPAGE_BPS",
    "MAX_ORDERBOOK_SPREAD_BPS",
    "MAX_ORDER_KRW",
    "RUN_ROOT",
}

INTERNAL_KEYS = {
    "BITHUMB_DEPLOY_COMMIT_SHA",
    "BITHUMB_DEPLOY_DIRTY",
    "BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK",
    "BITHUMB_RESEARCH_MAX_WORKERS",
    "BITHUMB_RESEARCH_MP_START_METHOD",
    "BITHUMB_TOTAL_PROCESS_BUDGET",
    "BITHUMB_PYTEST_RUN_ID",
    "BITHUMB_PYTEST_STARTED",
    "BITHUMB_PYTEST_SUITE",
    "BITHUMB_PYTEST_SUMMARY_ON_SUCCESS",
    "PYTEST_XDIST_WORKER",
    "PYTEST_XDIST_WORKER_COUNT",
    "PYTEST_XDIST_WORKERS",
    "XDG_STATE_HOME",
}

BOOTSTRAP_KEYS = {
    "BITHUMB_ENV_FILE",
    "BITHUMB_ENV_FILE_LIVE",
    "BITHUMB_ENV_FILE_PAPER",
}

PAPER_KEYS = {
    "BUY_FRACTION",
    "PAPER_EXECUTION_LATENCY_MS",
    "PAPER_EXECUTION_MODEL",
    "PAPER_EXECUTION_ORDER_FAILURE_RATE",
    "PAPER_EXECUTION_PARTIAL_FILL_FRACTION",
    "PAPER_EXECUTION_PARTIAL_FILL_RATE",
    "PAPER_EXECUTION_STRESS_SEED",
    "PAPER_FEE_RATE",
    "PAPER_FEE_RATE_ESTIMATE",
    "SLIPPAGE_BPS",
    "START_CASH_KRW",
}

DEPRECATED_IGNORED_KEYS = {
    "BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED",
    "LIVE_ALLOW_ORDER_RULE_FALLBACK",
}

DEPRECATED_ALIAS_KEYS = {
    "NTFY_URL",
}

EXAMPLE_DEFAULTS = {
    "MODE": "paper",
    "MARKET": "KRW-BTC",
    "PAIR": "KRW-BTC",
    "DB_PATH": "",
    "LIVE_DRY_RUN": "true",
    "LIVE_REAL_ORDER_ARMED": "false",
    "LIVE_SUBMIT_CONTRACT_PROFILE": "live_explicit_submit_plan_v1",
    "LIVE_ORDER_RULE_FALLBACK_PROFILE": "persisted_snapshot_required",
    "NOTIFIER_DEDUPE_WINDOW_SEC": "20",
}

DESCRIPTIONS = {
    "BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED": (
        "Deprecated and ignored compatibility gate; runtime behavior remains fail-closed."
    ),
    "LIVE_ALLOW_ORDER_RULE_FALLBACK": (
        "Deprecated and ignored compatibility gate; use LIVE_ORDER_RULE_FALLBACK_PROFILE."
    ),
    "LIVE_ORDER_RULE_FALLBACK_PROFILE": (
        "Controls order-rule fallback posture; armed live must use persisted_snapshot_required."
    ),
    "NOTIFIER_DEDUPE_WINDOW_SEC": (
        "Notifier duplicate suppression window in seconds. Internal operational setting, documented for reproducibility."
    ),
    "NTFY_SERVER": "Standard ntfy server base URL. NTFY_URL is a deprecated compatibility alias.",
    "NTFY_URL": "Deprecated compatibility alias for NTFY_SERVER; NTFY_SERVER takes priority when both are set.",
    "RESEARCH_NOTIFICATION_POLICY": (
        "Research command notification policy: best_effort, require_delivery, or disabled. Defaults to best_effort."
    ),
    "DB_PATH": "Compatibility override for the mode-specific SQLite trade ledger path.",
    "RUN_LOCK_PATH": "Compatibility override for the mode-specific run lock path.",
    "SNAPSHOT_ROOT": "Compatibility override for snapshot output under the backup bucket.",
}


def _infer_type(name: str) -> str:
    if name in {
        "BITHUMB_PYTEST_STARTED",
        "BITHUMB_PYTEST_SUMMARY_ON_SUCCESS",
        "BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK",
    }:
        return "bool"
    if name in {"BITHUMB_RESEARCH_MAX_WORKERS", "BITHUMB_TOTAL_PROCESS_BUDGET", "PYTEST_XDIST_WORKER_COUNT", "PYTEST_XDIST_WORKERS"}:
        return "number"
    if name.endswith("_ENABLED") or name in {
        "KILL_SWITCH",
        "KILL_SWITCH_LIQUIDATE",
        "LIVE_DRY_RUN",
        "LIVE_REAL_ORDER_ARMED",
        "TARGET_EXECUTION_SHADOW",
    }:
        return "bool"
    if name.endswith("_SEC") or name.endswith("_MS") or name.endswith("_COUNT") or name.endswith("_MIN"):
        return "number"
    if name.endswith("_BPS") or name.endswith("_RATE") or name.endswith("_RATIO") or name.endswith("_KRW"):
        return "number"
    return "string"


def _scope_for(name: str) -> ModeScope:
    if name in INTERNAL_KEYS:
        return "internal"
    if name in BOOTSTRAP_KEYS:
        return "bootstrap"
    if name in PAPER_KEYS:
        return "paper"
    if name.startswith("LIVE_") or name in LIVE_REQUIRED_KEYS:
        return "live"
    return "common"


def _safety_tier_for(name: str) -> SafetyTier:
    if name in LIVE_REQUIRED_KEYS or name.startswith(("LIVE_", "MAX_", "KILL_", "DB_", "RUN_", "BITHUMB_API")):
        return "P0"
    if name.startswith(("NOTIFIER_", "NTFY_", "SLACK_", "TELEGRAM_", "HEALTH_")):
        return "P1"
    if name.startswith(("SMA_", "STRATEGY_", "PAPER_")):
        return "P2"
    if name in INTERNAL_KEYS:
        return "internal"
    return "P1"


def _category_for(name: str) -> str:
    if name.startswith("BITHUMB_RESEARCH_") or name == "BITHUMB_TOTAL_PROCESS_BUDGET":
        return "research"
    if name.startswith("BITHUMB_PYTEST_") or name.startswith("PYTEST_XDIST_"):
        return "test_runtime"
    if name.endswith("_ROOT") or name in {"DB_PATH", "RUN_LOCK_PATH", "SNAPSHOT_ROOT", "BACKUP_DIR"}:
        return "storage"
    if name.startswith(("BITHUMB_", "LIVE_", "MAX_", "KILL_")):
        return "live_safety"
    if name.startswith(("NOTIFIER_", "NTFY_", "SLACK_", "TELEGRAM_", "HEALTH_")):
        return "observability"
    if name.startswith(("SMA_", "STRATEGY_", "ACTIVE_", "RUNTIME_STRATEGY")):
        return "strategy"
    if name.startswith(("PAPER_", "EXECUTION_", "FEE_", "SLIPPAGE_")):
        return "execution"
    return "runtime"


def _side_effect_class_for(name: str) -> SideEffectClass:
    if name in EXTERNAL_NOTIFICATION_ENV_KEYS:
        return "external_notification"
    if name in BROKER_PRIVATE_ENV_KEYS:
        return "broker_private"
    return "none"


def _build_spec(name: str) -> EnvVarSpec:
    scope = _scope_for(name)
    validation_kind = ""
    min_live_bytes: int | None = None
    if name == "BITHUMB_API_SECRET":
        validation_kind = JWT_HS256_SECRET_VALIDATION_KIND
        min_live_bytes = JWT_HS256_MIN_SECRET_BYTES
    return EnvVarSpec(
        name=name,
        value_type=_infer_type(name),
        default=EXAMPLE_DEFAULTS.get(name, ""),
        default_resolver="PathManager" if name.endswith("_ROOT") or name in {"DB_PATH", "RUN_LOCK_PATH"} else "",
        description=DESCRIPTIONS.get(name, f"{name} runtime configuration."),
        mode_scope=scope,
        secret=name in SECRET_KEYS,
        deprecated=name in DEPRECATED_IGNORED_KEYS or name in DEPRECATED_ALIAS_KEYS,
        ignored=name in DEPRECATED_IGNORED_KEYS,
        required_in_live=name in LIVE_REQUIRED_KEYS,
        operator_visible=scope != "internal",
        safety_tier=_safety_tier_for(name),
        category=_category_for(name),
        example=EXAMPLE_DEFAULTS.get(name, ""),
        validation_kind=validation_kind,
        min_live_bytes=min_live_bytes,
        side_effect_class=_side_effect_class_for(name),
    )


ENV_SPECS: tuple[EnvVarSpec, ...] = tuple(_build_spec(name) for name in DECLARED_ENV_NAMES)
SPEC_BY_NAME: dict[str, EnvVarSpec] = {spec.name: spec for spec in ENV_SPECS}


def config_spec_payload() -> dict[str, object]:
    return {
        "config_schema_version": CONFIG_SCHEMA_VERSION,
        "env": [spec.payload() for spec in ENV_SPECS],
    }


def config_spec_hash() -> str:
    encoded = json.dumps(config_spec_payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def documentation_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_hash(path: Path) -> str:
    if not path.exists():
        return "missing"
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def settings_contract_failures(settings_fields: set[str]) -> list[str]:
    declared = set(SPEC_BY_NAME)
    failures: list[str] = []
    missing = sorted(settings_fields - declared)
    if missing:
        failures.append("Settings fields missing from ConfigSpec: " + ", ".join(missing))
    return failures
