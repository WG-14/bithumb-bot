from __future__ import annotations

import pytest

from bithumb_bot.config import LiveModeValidationError, settings, validate_live_mode_preflight


@pytest.fixture(autouse=True)
def _restore_settings():
    old_values = {
        "MODE": settings.MODE,
        "MAX_ORDER_KRW": settings.MAX_ORDER_KRW,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "MAX_DAILY_ORDER_COUNT": settings.MAX_DAILY_ORDER_COUNT,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
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




def test_live_preflight_rejects_kill_switch_liquidate_mode() -> None:
    object.__setattr__(settings, "MODE", "live")
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

def test_live_preflight_allows_dry_run_without_credentials() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    validate_live_mode_preflight(settings)
