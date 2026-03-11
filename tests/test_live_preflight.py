from __future__ import annotations

import pytest

from bithumb_bot.config import LiveModeValidationError, settings, validate_live_mode_preflight


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
    }
    yield
    for key, value in old_values.items():
        object.__setattr__(settings, key, value)


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

    validate_live_mode_preflight(settings)




def test_live_preflight_rejects_kill_switch_liquidate_mode() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_mode_preflight(settings)

    assert (
        "KILL_SWITCH_LIQUIDATE=true is not supported yet; keep KILL_SWITCH_LIQUIDATE=false"
        in str(exc.value)
    )

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


def test_live_preflight_accepts_explicit_non_default_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", "data/live_trading.sqlite")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", "data/live_trading.sqlite")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

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
