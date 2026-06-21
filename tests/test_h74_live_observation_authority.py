from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from bithumb_bot.h74_observation import (
    H74_OBSERVATION_PARAMETERS,
    H74_SOURCE_OBSERVATION_PARAMETERS,
    H74_SOURCE_OBSERVATION_AUTHORITY_ARTIFACT_TYPE,
    H74ObservationAuthorityError,
    build_h74_observation_authority_payload,
    build_h74_source_observation_authority_payload,
    h74_source_observation_risk_policy_hash,
    h74_source_runtime_values_from_settings,
    verify_h74_observation_authority,
    verify_h74_source_observation_authority,
)
from bithumb_bot.config import (
    LiveModeValidationError,
    settings,
    validate_live_strategy_selection,
    validate_runtime_strategy_set_selection,
)
from bithumb_bot.execution_authority import execution_authority_from_payload
from bithumb_bot.research.strategy_spec import runtime_bound_behavior_parameter_names
from bithumb_bot.runtime_strategy_set import (
    ProfileAuthorityContext,
    RuntimeDecisionRequestBuilder,
    RuntimeStrategySet,
    RuntimeStrategySpec,
)
from dataclasses import replace
import json


def _rehash_authority(payload: dict) -> dict:
    from bithumb_bot.research.hashing import sha256_prefixed

    payload["authority_parameter_hash"] = sha256_prefixed(payload["hash_bound_parameters"])
    payload["authority_content_hash"] = sha256_prefixed(
        {k: v for k, v in payload.items() if k != "authority_content_hash"}
    )
    return payload


def _rehash_source_risk_policy(payload: dict) -> dict:
    payload["risk_policy_hash"] = h74_source_observation_risk_policy_hash(payload["risk_policy"])
    payload["hash_bound_parameters"]["risk_policy_hash"] = payload["risk_policy_hash"]
    return _rehash_authority(payload)


def _source_authority() -> dict:
    return build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash="sha256:source-candidate",
        backtest_report_hash="sha256:backtest",
        validation_run_hash="sha256:validation",
        code_commit_sha="test-commit",
    )


def _source_parameters() -> dict[str, object]:
    return {
        name: H74_SOURCE_OBSERVATION_PARAMETERS[name]
        for name in runtime_bound_behavior_parameter_names("daily_participation_sma")
    }


def _h74_source_cfg(authority_path: Path | str, **overrides) -> object:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(authority_path),
        **overrides,
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)
    return cfg


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


def test_h74_source_observation_authority_verifies_100k_exact_params() -> None:
    payload = _source_authority()

    assert payload["artifact_type"] == H74_SOURCE_OBSERVATION_AUTHORITY_ARTIFACT_TYPE
    bound = payload["hash_bound_parameters"]
    assert bound["candidate_id"] == "candidate_9738b8d6"
    assert bound["source_candidate_artifact_hash"] == "sha256:source-candidate"
    assert bound["backtest_report_hash"] == "sha256:backtest"
    assert bound["validation_run_hash"] == "sha256:validation"
    assert bound["SMA_FILTER_GAP_MIN_RATIO"] == 0.0002
    assert bound["STRATEGY_EXIT_RULES"] == "max_holding_time"
    assert bound["DAILY_PARTICIPATION_MAX_ORDER_KRW"] == 100_000
    assert bound["DAILY_PARTICIPATION_BUY_FRACTION"] == 1.0
    assert bound["max_entry_notional_krw"] == 100_000
    assert bound["max_daily_entry_count"] == 1
    assert bound["max_daily_total_order_count"] == 2
    assert bound["observation_window_days"] == 7
    assert bound["code_commit_sha"] == "test-commit"
    assert payload["risk_policy_hash"] == h74_source_observation_risk_policy_hash(payload["risk_policy"])
    assert bound["risk_policy_hash"] == payload["risk_policy_hash"]
    assert payload["risk_profile_source"] == "h74_source_live_observation_authority"
    assert payload["risk_enforcement_mode"] == "enforced"
    verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_missing_risk_policy() -> None:
    payload = _source_authority()
    payload.pop("risk_policy")
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="risk_policy_missing"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_risk_policy_hash_mismatch() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_daily_order_count"] = 1
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="risk_policy_hash_mismatch"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_disabled_risk_policy() -> None:
    payload = _source_authority()
    payload["risk_policy"]["policy_status"] = "disabled"
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="risk_policy_disabled"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_daily_order_count_above_2() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_daily_order_count"] = 3
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="daily_order_count_too_high"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_trade_count_above_2() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_trade_count_per_day"] = 3
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="trade_count_too_high"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_open_positions_not_one() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_open_positions"] = 2
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="open_positions_invalid"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_unresolved_order_policy_not_block() -> None:
    payload = _source_authority()
    payload["risk_policy"]["unresolved_order_policy"] = "allow"
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="unresolved_order_policy_invalid"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_50k_authority() -> None:
    with pytest.raises(H74ObservationAuthorityError, match="artifact_type_invalid"):
        verify_h74_source_observation_authority(
            build_h74_observation_authority_payload(),
            runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS,
        )


def test_h74_source_observation_rejects_gap_mismatch_0012_vs_0002() -> None:
    payload = _source_authority()
    runtime = dict(H74_SOURCE_OBSERVATION_PARAMETERS)
    runtime["SMA_FILTER_GAP_MIN_RATIO"] = 0.0012

    with pytest.raises(H74ObservationAuthorityError, match="SMA_FILTER_GAP_MIN_RATIO"):
        verify_h74_source_observation_authority(payload, runtime_values=runtime)


def test_h74_source_observation_rejects_legacy_exit_rules() -> None:
    payload = _source_authority()
    runtime = dict(H74_SOURCE_OBSERVATION_PARAMETERS)
    runtime["STRATEGY_EXIT_RULES"] = "stop_loss,opposite_cross,max_holding_time"

    with pytest.raises(H74ObservationAuthorityError, match="STRATEGY_EXIT_RULES"):
        verify_h74_source_observation_authority(payload, runtime_values=runtime)


def test_h74_source_observation_rejects_expired_authority() -> None:
    payload = build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash="sha256:source-candidate",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        code_commit_sha="test-commit",
    )

    with pytest.raises(H74ObservationAuthorityError, match="expired"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_max_order_above_100000() -> None:
    payload = _source_authority()
    payload["hash_bound_parameters"]["DAILY_PARTICIPATION_MAX_ORDER_KRW"] = 100_001
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="above_100000"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_window_outside_09_11() -> None:
    payload = _source_authority()
    payload["hash_bound_parameters"]["DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"] = 8
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="window_start"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_missing_authority_does_not_replace_approved_profile() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH="",
    )

    with pytest.raises(LiveModeValidationError, match="approved_profile_required_for_strategy:daily_participation_sma"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_allows_live_dry_run_materialization(tmp_path, monkeypatch) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)

    validate_live_strategy_selection(cfg)
    runtime = h74_source_runtime_values_from_settings(cfg)
    assert runtime["max_daily_total_order_count"] == 2
    assert runtime["exit_closeout_not_blocked_by_entry_cap"] is True


def test_h74_source_observation_runtime_strategy_set_selection_passes_without_approved_profile(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
            "strategies": [
                {
                    "strategy_name": "daily_participation_sma",
                    "strategy_instance_id": "h74-source-observation",
                    "pair": "KRW-BTC",
                    "interval": "1m",
                    "desired_exposure_krw": 100_000,
                    "parameters": _source_parameters(),
                }
            ],
        }
    )
    cfg = _h74_source_cfg(path, RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json)

    validate_runtime_strategy_set_selection(cfg)


def test_h74_source_observation_selection_rejects_expired_authority(
    tmp_path,
    monkeypatch,
) -> None:
    authority = build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash="sha256:source-candidate",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        code_commit_sha="test-commit",
    )
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)

    with pytest.raises(LiveModeValidationError, match="h74_source_observation_authority_expired"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_selection_rejects_runtime_mismatch(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)
    object.__setattr__(cfg, "SMA_FILTER_GAP_MIN_RATIO", 0.0012)

    with pytest.raises(LiveModeValidationError, match="SMA_FILTER_GAP_MIN_RATIO"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_selection_rejects_invalid_risk_policy_authority(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    authority["risk_policy"]["max_daily_order_count"] = 3
    _rehash_source_risk_policy(authority)
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)

    with pytest.raises(LiveModeValidationError, match="daily_order_count_too_high"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_live_dry_run_materializes_risk_profile(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(path),
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)
    strategy_set = RuntimeStrategySet(
        source="RUNTIME_STRATEGY_SET_JSON",
        strategies=(
            RuntimeStrategySpec(
                "daily_participation_sma",
                pair="KRW-BTC",
                interval="1m",
                parameters=_source_parameters(),
            ),
        ),
    )
    context = ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=cfg)

    instance = RuntimeDecisionRequestBuilder(settings_obj=cfg).with_authority_context(
        context
    ).materialize_instance(strategy_set.active_strategies[0])

    assert instance.approved_profile_path is None
    assert instance.approved_profile_hash is None
    assert instance.risk_profile is not None
    assert instance.risk_profile.risk_profile_source == "h74_source_live_observation_authority"
    assert instance.risk_profile.enforcement_mode == "enforced"
    assert instance.risk_profile.policy.policy_status == "enabled"
    assert instance.risk_profile.policy.max_daily_order_count == 2
    assert instance.risk_profile.policy.max_trade_count_per_day == 2
    assert instance.risk_profile.policy.max_open_positions == 1
    assert instance.risk_profile.policy.unresolved_order_policy == "block"
    assert instance.risk_profile.risk_policy_hash == authority["risk_policy_hash"]


def test_h74_source_observation_other_strategy_still_requires_approved_profile(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="sma_with_filter",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(path),
    )

    with pytest.raises(LiveModeValidationError, match="approved_profile_required_for_strategy:sma_with_filter"):
        validate_live_strategy_selection(cfg)


def _write_smoke_success(path) -> None:
    path.write_text(
        json.dumps(
            {
                "status": "passed",
                "execution_mode": "live_pipeline_smoke",
                "orders_expected": 10,
                "orders_submitted": 10,
                "manual_intervention_required": False,
                "final": {
                    "broker_qty": 0.0,
                    "portfolio_qty": 0.0,
                    "projected_total_qty": 0.0,
                    "open_order_count": 0,
                    "submit_unknown_count": 0,
                    "recovery_required_count": 0,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_h74_source_real_order_requires_live_pipeline_smoke_evidence(tmp_path, monkeypatch) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(path),
        H74_SOURCE_OBSERVATION_LIVE_PIPELINE_SMOKE_EVIDENCE_PATH="",
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)

    with pytest.raises(LiveModeValidationError, match="live_pipeline_smoke_evidence_missing"):
        validate_live_strategy_selection(cfg)


def test_h74_source_real_order_accepts_live_pipeline_smoke_success_evidence(tmp_path, monkeypatch) -> None:
    authority = _source_authority()
    authority_path = tmp_path / "source-authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    smoke_path = tmp_path / "smoke-success.json"
    _write_smoke_success(smoke_path)
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(authority_path))
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_LIVE_PIPELINE_SMOKE_EVIDENCE_PATH", str(smoke_path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(authority_path),
        H74_SOURCE_OBSERVATION_LIVE_PIPELINE_SMOKE_EVIDENCE_PATH=str(smoke_path),
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)

    validate_live_strategy_selection(cfg)


def test_h74_source_observation_policy_does_not_set_approved_profile_ok(
    tmp_path,
    monkeypatch,
) -> None:
    from bithumb_bot.strategy_config import sma_strategy_config_from_settings
    from bithumb_bot.config import settings as live_settings

    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    object.__setattr__(live_settings, "MODE", "live")
    object.__setattr__(live_settings, "LIVE_DRY_RUN", True)
    object.__setattr__(live_settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(live_settings, "STRATEGY_NAME", "daily_participation_sma")
    object.__setattr__(live_settings, "PAIR", "KRW-BTC")
    object.__setattr__(live_settings, "INTERVAL", "1m")
    object.__setattr__(live_settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
    object.__setattr__(live_settings, "STRATEGY_APPROVED_PROFILE_PATH", "")
    object.__setattr__(live_settings, "STRATEGY_CANDIDATE_PROFILE_PATH", "")
    object.__setattr__(live_settings, "H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    object.__setattr__(live_settings, "MAX_ORDER_KRW", 100_000)
    object.__setattr__(live_settings, "MAX_DAILY_ORDER_COUNT", 2)
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(live_settings, key, value)

    config = sma_strategy_config_from_settings()

    assert config.candidate_regime_policy is not None
    assert config.candidate_regime_policy["h74_observation_authority_verified"] is True
    assert config.candidate_regime_policy["approved_profile_verification_ok"] is False
    assert config.candidate_regime_policy["approved_profile_block_reason"] == "h74_source_observation_authority_used"
    assert config.candidate_regime_policy["approved_profile_contract_scope"] == "h74_source_live_observation_only"
    assert config.candidate_regime_policy["production_approval"] is False
    assert config.candidate_regime_policy["risk_profile_source"] == "h74_source_live_observation_authority"
    assert config.candidate_regime_policy["risk_enforcement_mode"] == "enforced"


def test_h74_source_observation_exit_closeout_not_blocked_by_entry_cap_after_buy() -> None:
    payload = _source_authority()

    assert payload["hash_bound_parameters"]["max_daily_entry_count"] == 1
    assert payload["hash_bound_parameters"]["max_daily_total_order_count"] == 2
    assert payload["hash_bound_parameters"]["exit_closeout_not_blocked_by_entry_cap"] is True
