from __future__ import annotations

import math
import os
from pathlib import Path

import pytest

from bithumb_bot import config
from bithumb_bot.config import settings
from bithumb_bot.broker import order_rules


@pytest.fixture(autouse=True)
def _restore_settings():
    old_values = {
        "MODE": settings.MODE,
        "DB_PATH": settings.DB_PATH,
        "MAX_ORDER_KRW": settings.MAX_ORDER_KRW,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "MAX_DAILY_ORDER_COUNT": settings.MAX_DAILY_ORDER_COUNT,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "KILL_SWITCH_LIQUIDATE": settings.KILL_SWITCH_LIQUIDATE,
        "BITHUMB_API_KEY": settings.BITHUMB_API_KEY,
        "BITHUMB_API_SECRET": settings.BITHUMB_API_SECRET,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "MAX_ORDERBOOK_SPREAD_BPS": settings.MAX_ORDERBOOK_SPREAD_BPS,
        "MAX_MARKET_SLIPPAGE_BPS": settings.MAX_MARKET_SLIPPAGE_BPS,
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS,
        "LIVE_FILL_FEE_STRICT_MODE": settings.LIVE_FILL_FEE_STRICT_MODE,
        "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW": settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW,
    }
    old_cache = dict(order_rules._cached_rules)
    yield
    for key, value in old_values.items():
        object.__setattr__(settings, key, value)
    order_rules._cached_rules.clear()
    order_rules._cached_rules.update(old_cache)


@pytest.fixture(autouse=True)
def _set_live_roots_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    roots = {
        "ENV_ROOT": tmp_path / "env",
        "RUN_ROOT": tmp_path / "run",
        "DATA_ROOT": tmp_path / "data",
        "LOG_ROOT": tmp_path / "logs",
        "BACKUP_ROOT": tmp_path / "backup",
    }
    for key, value in roots.items():
        monkeypatch.setenv(key, str(value.resolve()))


def _set_valid_live_defaults(
    monkeypatch: pytest.MonkeyPatch,
    *,
    db_path: str | None = None,
) -> None:
    data_root = Path(os.environ["DATA_ROOT"])
    run_root = Path(os.environ["RUN_ROOT"])
    resolved_db_path = str(
        Path(db_path).resolve() if db_path is not None else (data_root / "live" / "trades" / "live.sqlite").resolve()
    )
    monkeypatch.setenv("DB_PATH", resolved_db_path)
    monkeypatch.setenv("RUN_LOCK_PATH", str((run_root / "live" / "bithumb-bot.lock").resolve()))
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    monkeypatch.delenv("START_CASH_KRW", raising=False)
    monkeypatch.delenv("BUY_FRACTION", raising=False)
    monkeypatch.delenv("FEE_RATE", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", resolved_db_path)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 100000.0)



def test_live_preflight_skips_paper_mode() -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_requires_live_risk_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "MAX_ORDER_KRW must be > 0" in msg
    assert "MAX_DAILY_LOSS_KRW must be > 0" in msg
    assert "MAX_DAILY_ORDER_COUNT must be > 0" in msg


def test_live_preflight_requires_credentials_when_not_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "BITHUMB_API_KEY is required when LIVE_DRY_RUN=false" in msg
    assert "BITHUMB_API_SECRET is required when LIVE_DRY_RUN=false" in msg


def test_live_preflight_requires_explicit_arming_for_real_live_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_REAL_ORDER_ARMED=true is required" in str(exc.value)


def test_live_preflight_accepts_real_live_orders_when_explicitly_armed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")

    config.validate_live_mode_preflight(settings)





def test_live_preflight_requires_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS must be a finite value > 0" in str(exc.value)


def test_live_preflight_accepts_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)

    config.validate_live_mode_preflight(settings)

def test_live_preflight_allows_kill_switch_liquidate_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)

    config.validate_live_mode_preflight(settings)

def test_live_preflight_allows_dry_run_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    config.validate_live_mode_preflight(settings)


def test_live_preflight_requires_explicit_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("DB_PATH", raising=False)
    object.__setattr__(settings, "DB_PATH", str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DB_PATH must be explicitly set when MODE=live" in str(exc.value)


def test_live_preflight_rejects_paper_scoped_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    paper_db = str((Path(os.environ["DATA_ROOT"]) / "paper" / "trades" / "paper.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=paper_db)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DB_PATH must not point to a paper-scoped path when MODE=live" in str(exc.value)


@pytest.mark.parametrize(("env_key", "env_value"), [("LOG_ROOT", "logs"), ("BACKUP_ROOT", "backup")])
def test_live_preflight_rejects_relative_log_and_backup_roots(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, env_value)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must be an absolute path when MODE=live" in str(exc.value)


@pytest.mark.parametrize(("env_key", "child"), [("DATA_ROOT", "data"), ("LOG_ROOT", "logs"), ("BACKUP_ROOT", "backup")])
def test_live_preflight_rejects_repo_internal_roots(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    child: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, str((config.PROJECT_ROOT / child).resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must be outside repository when MODE=live" in str(exc.value)


def test_live_preflight_rejects_paper_scoped_root_segments(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("DATA_ROOT", str((Path(os.environ["DATA_ROOT"]).parent / "paper" / "data").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DATA_ROOT must not contain a paper-scoped path segment when MODE=live" in str(exc.value)


@pytest.mark.parametrize("env_key", ["LOG_ROOT", "BACKUP_ROOT"])
def test_live_preflight_rejects_paper_scoped_log_and_backup_segments(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(
        env_key,
        str((Path(os.environ["DATA_ROOT"]).parent / "paper" / env_key.lower()).resolve()),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must not contain a paper-scoped path segment when MODE=live" in str(exc.value)


def test_live_preflight_accepts_non_default_live_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_live_db = str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live-prod.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=custom_live_db)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_accepts_explicit_non_default_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_live_db = str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live_trading.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=custom_live_db)

    config.validate_live_mode_preflight(settings)

@pytest.mark.parametrize(
    ("env_key", "env_value"),
    [
        ("START_CASH_KRW", "1000000"),
        ("BUY_FRACTION", "0.5"),
        ("FEE_RATE", "0.0004"),
        ("SLIPPAGE_BPS", "5"),
    ],
)
def test_live_preflight_rejects_paper_only_env_keys_in_live(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, env_value)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "paper/test-like config mixing is not allowed when MODE=live" in msg
    assert env_key in msg


def test_live_preflight_accepts_clean_live_env_without_paper_only_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("START_CASH_KRW", raising=False)
    monkeypatch.delenv("BUY_FRACTION", raising=False)
    monkeypatch.delenv("FEE_RATE", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)

    config.validate_live_mode_preflight(settings)

def test_live_preflight_requires_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("NOTIFIER_ENABLED", "false")
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "notifier must be enabled and configured" in str(exc.value)


def test_live_preflight_accepts_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")

    config.validate_live_mode_preflight(settings)


def test_live_preflight_paper_mode_notifier_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTIFIER_ENABLED", "false")
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_fails_when_order_rule_sync_fails_and_manual_rules_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "min_qty must be > 0" in msg
    assert "qty_step must be > 0" in msg
    assert "min_notional_krw must be > 0" in msg
    assert "max_qty_decimals must be > 0" in msg


def test_live_preflight_passes_with_valid_auto_synced_order_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.OrderRules(
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=4,
        ),
    )

    config.validate_live_mode_preflight(settings)


def test_live_preflight_allows_non_positive_strict_threshold_when_strict_mode_is_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 0.0)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_accepts_positive_strict_threshold_when_strict_mode_is_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 10000.0)

    config.validate_live_mode_preflight(settings)


@pytest.mark.parametrize(
    "invalid_threshold",
    [0.0, -1.0, math.nan, math.inf, -math.inf, "abc"],
)
def test_live_preflight_rejects_invalid_strict_threshold_when_strict_mode_is_on(
    monkeypatch: pytest.MonkeyPatch,
    invalid_threshold: object,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", invalid_threshold)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW" in msg
    assert "LIVE_FILL_FEE_STRICT_MODE=true" in msg
