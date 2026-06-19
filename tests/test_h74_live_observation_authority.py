from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bithumb_bot.h74_observation import (
    H74_OBSERVATION_PARAMETERS,
    H74ObservationAuthorityError,
    build_h74_observation_authority_payload,
    verify_h74_observation_authority,
)
from bithumb_bot.config import LiveModeValidationError, settings, validate_live_strategy_selection
from bithumb_bot.execution_authority import execution_authority_from_payload
from bithumb_bot.research.strategy_spec import runtime_bound_behavior_parameter_names
from dataclasses import replace
import json


def test_h74_observation_authority_hash_binds_50k_parameters() -> None:
    payload = build_h74_observation_authority_payload()

    bound = payload["hash_bound_parameters"]
    assert bound["DAILY_PARTICIPATION_MAX_ORDER_KRW"] == 50_000
    assert payload["authority_parameter_hash"].startswith("sha256:")
    verify_h74_observation_authority(payload, runtime_values=H74_OBSERVATION_PARAMETERS)


def test_h74_authority_binds_all_behavior_affecting_parameters() -> None:
    payload = build_h74_observation_authority_payload()
    bound = set(payload["hash_bound_parameters"])
    required = set(runtime_bound_behavior_parameter_names("daily_participation_sma"))

    assert required - bound == set()
    assert bound - required >= {"strategy_name", "market", "interval", "max_daily_order_count", "max_notional_krw"}


def test_h74_authority_rejects_missing_behavior_affecting_parameter() -> None:
    payload = build_h74_observation_authority_payload()
    payload["hash_bound_parameters"].pop("DAILY_PARTICIPATION_BUY_FRACTION")
    from bithumb_bot.research.hashing import sha256_prefixed

    payload["authority_parameter_hash"] = sha256_prefixed(payload["hash_bound_parameters"])
    payload["authority_content_hash"] = sha256_prefixed(
        {k: v for k, v in payload.items() if k != "authority_content_hash"}
    )

    with pytest.raises(H74ObservationAuthorityError, match="DAILY_PARTICIPATION_BUY_FRACTION"):
        verify_h74_observation_authority(payload, runtime_values=H74_OBSERVATION_PARAMETERS)


def test_h74_authority_rejects_runtime_mismatch_for_each_bound_parameter() -> None:
    payload = build_h74_observation_authority_payload()
    for name in runtime_bound_behavior_parameter_names("daily_participation_sma"):
        runtime = dict(H74_OBSERVATION_PARAMETERS)
        current = runtime[name]
        runtime[name] = (not current) if isinstance(current, bool) else f"{current}_changed"
        with pytest.raises(H74ObservationAuthorityError, match=name):
            verify_h74_observation_authority(payload, runtime_values=runtime)


def test_h74_observation_authority_rejects_100k_runtime_mismatch() -> None:
    payload = build_h74_observation_authority_payload()
    runtime = dict(H74_OBSERVATION_PARAMETERS)
    runtime["DAILY_PARTICIPATION_MAX_ORDER_KRW"] = 100_000

    with pytest.raises(H74ObservationAuthorityError, match="DAILY_PARTICIPATION_MAX_ORDER_KRW"):
        verify_h74_observation_authority(payload, runtime_values=runtime)


def test_h74_observation_authority_expires_after_7_days() -> None:
    payload = build_h74_observation_authority_payload(
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
    )

    with pytest.raises(H74ObservationAuthorityError, match="expired"):
        verify_h74_observation_authority(payload, runtime_values=H74_OBSERVATION_PARAMETERS)


def test_h74_observation_authority_not_accepted_as_promotion_profile() -> None:
    payload = build_h74_observation_authority_payload()

    assert payload["promotion_grade"] is False
    assert payload["research_promotion_evidence"] is False
    assert payload["approved_profile_evidence"] is False


def test_h74_observation_authority_requires_daily_window_09_11() -> None:
    payload = build_h74_observation_authority_payload()
    runtime = dict(H74_OBSERVATION_PARAMETERS)
    runtime["DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"] = 0

    with pytest.raises(H74ObservationAuthorityError, match="DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"):
        verify_h74_observation_authority(payload, runtime_values=runtime)


def test_h74_observation_authority_requires_holding_74() -> None:
    payload = build_h74_observation_authority_payload()
    runtime = dict(H74_OBSERVATION_PARAMETERS)
    runtime["STRATEGY_EXIT_MAX_HOLDING_MIN"] = 75

    with pytest.raises(H74ObservationAuthorityError, match="STRATEGY_EXIT_MAX_HOLDING_MIN"):
        verify_h74_observation_authority(payload, runtime_values=runtime)


def test_live_observation_authority_runtime_hook_rejects_env_mismatch(tmp_path, monkeypatch) -> None:
    authority = build_h74_observation_authority_payload()
    path = tmp_path / "authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("LIVE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_MAX_HOLDING_MIN=75,
        APPROVED_STRATEGY_PROFILE_PATH="",
    )

    with pytest.raises(LiveModeValidationError, match="live_observation_authority_validation_failed"):
        validate_live_strategy_selection(cfg)


def test_h74_observation_authority_does_not_replace_approved_profile(tmp_path, monkeypatch) -> None:
    authority = build_h74_observation_authority_payload()
    path = tmp_path / "authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("LIVE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_DAILY_ORDER_COUNT=1,
        APPROVED_STRATEGY_PROFILE_PATH="",
    )

    with pytest.raises(LiveModeValidationError, match="approved_profile_required_for_strategy:daily_participation_sma"):
        validate_live_strategy_selection(cfg)


def test_live_observation_authority_path_does_not_grant_strategy_run_operation() -> None:
    authority = execution_authority_from_payload(build_h74_observation_authority_payload())

    assert authority.allows("h74_live_observation_50k")
    assert not authority.allows("strategy_run")
    assert authority.risk_authority is False
