from __future__ import annotations

import pytest

from bithumb_bot.config import LiveModeValidationError, settings, validate_live_mode_preflight
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
    }
    old_cache = dict(order_rules._cached_rules)
    yield
    for key, value in old_values.items():
        object.__setattr__(settings, key, value)
    order_rules._cached_rules.clear()
    order_rules._cached_rules.update(old_cache)


def test_live_preflight_skips_paper_mode() -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    validate_live_mode_preflight(settings)


def test_live_preflight_requires_live_risk_limits() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "MAX_ORDER_KRW must be > 0" in msg
    assert "MAX_DAILY_LOSS_KRW must be > 0" in msg
    assert "MAX_DAILY_ORDER_COUNT must be > 0" in msg


def test_live_preflight_requires_credentials_when_not_dry_run() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "BITHUMB_API_KEY is required when LIVE_DRY_RUN=false" in msg
    assert "BITHUMB_API_SECRET is required when LIVE_DRY_RUN=false" in msg


def test_live_preflight_requires_explicit_arming_for_real_live_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    assert "LIVE_REAL_ORDER_ARMED=true is required" in str(exc.value)


def test_live_preflight_accepts_real_live_orders_when_explicitly_armed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)

    validate_live_mode_preflight(settings)





def test_live_preflight_requires_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    assert "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS must be a finite value > 0" in str(exc.value)


def test_live_preflight_accepts_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)

    validate_live_mode_preflight(settings)

def test_live_preflight_allows_kill_switch_liquidate_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)

    validate_live_mode_preflight(settings)

def test_live_preflight_allows_dry_run_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)

    validate_live_mode_preflight(settings)


def test_live_preflight_requires_explicit_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DB_PATH", raising=False)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/bithumb_1m.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    assert "DB_PATH must be explicitly set when MODE=live" in str(exc.value)


def test_live_preflight_rejects_default_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/bithumb_1m.sqlite")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/bithumb_1m.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    assert "DB_PATH must not point to the default paper/shared DB path" in str(exc.value)


def test_live_preflight_rejects_normalized_default_db_path_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    aliased_default = "data/../data/bithumb_1m.sqlite"
    monkeypatch.setenv("DB_PATH", aliased_default)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", aliased_default)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    assert "DB_PATH must not point to the default paper/shared DB path" in str(exc.value)


def test_live_preflight_accepts_non_default_live_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live-prod.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live-prod.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)

    validate_live_mode_preflight(settings)


def test_live_preflight_accepts_explicit_non_default_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live_trading.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live_trading.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)

    validate_live_mode_preflight(settings)

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
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    monkeypatch.setenv(env_key, env_value)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "paper/test-like config mixing is not allowed when MODE=live" in msg
    assert env_key in msg


def test_live_preflight_accepts_clean_live_env_without_paper_only_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    monkeypatch.delenv("START_CASH_KRW", raising=False)
    monkeypatch.delenv("BUY_FRACTION", raising=False)
    monkeypatch.delenv("FEE_RATE", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)

    validate_live_mode_preflight(settings)

def test_live_preflight_requires_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("NOTIFIER_ENABLED", "false")
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    assert "notifier must be enabled and configured" in str(exc.value)


def test_live_preflight_accepts_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)

    validate_live_mode_preflight(settings)


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

    validate_live_mode_preflight(settings)


def test_live_preflight_fails_when_order_rule_sync_fails_and_manual_rules_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
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

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "min_qty must be > 0" in msg
    assert "qty_step must be > 0" in msg
    assert "min_notional_krw must be > 0" in msg
    assert "max_qty_decimals must be > 0" in msg


def test_live_preflight_passes_with_valid_auto_synced_order_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
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

    validate_live_mode_preflight(settings)
