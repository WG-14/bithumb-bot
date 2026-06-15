from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.strategy.daily_participation_policy import (
    DailyParticipationPolicyConfig,
    DailyParticipationStateSnapshot,
    evaluate_daily_participation_policy,
    kst_day,
)


def _config(**overrides):
    values = {
        "enabled": True,
        "timezone": "Asia/Seoul",
        "count_basis": "filled",
        "window_start_hour": 0,
        "window_end_hour": 24,
        "buy_fraction": 0.05,
        "max_order_krw": 10000.0,
    }
    values.update(overrides)
    return DailyParticipationPolicyConfig(**values)


def _state(**overrides):
    values = {
        "decision_ts": 1_704_046_800_000,
        "count_for_kst_day": 0,
        "position_open": False,
        "entry_allowed": True,
        "market_open": True,
        "daily_count_snapshot_hash": "sha256:" + "1" * 64,
    }
    values.update(overrides)
    return DailyParticipationStateSnapshot(**values)


def test_kst_day_boundary_uses_configured_timezone() -> None:
    assert kst_day(1_704_067_200_000, "Asia/Seoul") == "2024-01-01"


def test_fallback_allowed_when_count_zero_flat_and_window_open() -> None:
    result = evaluate_daily_participation_policy(config=_config(), state=_state())

    assert result.allowed is True
    assert result.reason_code == "daily_participation_fallback_allowed"
    assert result.entry_signal_source == "daily_participation_fallback"


def test_fallback_blocked_when_position_open() -> None:
    result = evaluate_daily_participation_policy(config=_config(), state=_state(position_open=True))

    assert result.allowed is False
    assert result.reason_code == "position_open"


def test_fallback_blocked_outside_window() -> None:
    result = evaluate_daily_participation_policy(
        config=_config(window_start_hour=23, window_end_hour=24),
        state=_state(),
    )

    assert result.allowed is False
    assert result.reason_code == "outside_daily_participation_window"


def test_count_basis_is_required_and_recorded() -> None:
    with pytest.raises(ValueError, match="count_basis"):
        _config(count_basis="")
    result = evaluate_daily_participation_policy(config=_config(count_basis="intent"), state=_state())
    assert result.count_basis == "intent"
    assert result.timestamp_field == "decision_ts"


def test_daily_participation_policy_module_has_no_sqlite_dependency() -> None:
    source = Path("src/bithumb_bot/strategy/daily_participation_policy.py").read_text(encoding="utf-8")

    assert "sqlite3" not in source
    assert "conn.execute" not in source
    assert "PRAGMA table_info" not in source
