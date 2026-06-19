from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect

import pytest

from bithumb_bot.execution_authority import (
    APPROVED_PROFILE_AUTHORITY_TYPE,
    LIVE_OBSERVATION_AUTHORITY_TYPE,
    OPERATOR_SMOKE_AUTHORITY_TYPE,
    execution_authority_from_payload,
    require_authority_operation,
    resolve_execution_authority,
    validate_live_observation_authority_complete_for_runtime,
)
from bithumb_bot.h74_observation import build_h74_observation_authority_payload
from bithumb_bot.operator_smoke import execute_smoke_buy
from bithumb_bot.operator_smoke_authority import build_operator_smoke_authority_payload


def test_operator_smoke_authority_allows_only_operator_smoke_operations() -> None:
    payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )

    authority = execution_authority_from_payload(payload)

    assert authority.authority_type == OPERATOR_SMOKE_AUTHORITY_TYPE
    assert authority.allows("operator_smoke_buy")
    assert not authority.allows("strategy_run")
    assert authority.parameter_authority is False
    assert authority.risk_authority is False


def test_h74_observation_authority_cannot_be_used_as_approved_profile() -> None:
    authority = execution_authority_from_payload(build_h74_observation_authority_payload())

    assert authority.authority_type == LIVE_OBSERVATION_AUTHORITY_TYPE
    assert authority.allows("h74_live_observation_50k")
    assert not authority.allows("strategy_run")
    assert authority.evidence_classification == "live_observation_non_substitutive"


def test_approved_profile_authority_cannot_be_used_as_operator_smoke_without_operator_confirmation() -> None:
    authority = execution_authority_from_payload(
        {
            "artifact_type": "approved_profile",
            "profile_content_hash": "sha256:" + "a" * 64,
            "market": "KRW-BTC",
        }
    )

    assert authority.authority_type == APPROVED_PROFILE_AUTHORITY_TYPE
    assert authority.allows("strategy_run")
    assert not authority.allows("operator_smoke_buy")


def test_live_observation_authority_requires_parameter_exit_and_risk_authority() -> None:
    authority = execution_authority_from_payload(build_h74_observation_authority_payload())

    with pytest.raises(ValueError, match="requires_parameter_exit_and_risk_authority"):
        validate_live_observation_authority_complete_for_runtime(authority)


def test_smoke_buy_resolves_operator_smoke_authority_before_submit() -> None:
    source = inspect.getsource(execute_smoke_buy)

    assert source.index("execution_authority_from_payload") < source.index("authority.verify")
    assert source.index('require_authority_operation(command_authority, "operator_smoke_buy")') < source.index(
        "authority.consume"
    )
    assert source.index('require_authority_operation(command_authority, "operator_smoke_buy")') < source.index(
        "submit_live_order_and_confirm"
    )


def test_h74_observation_authority_is_rejected_for_strategy_run_operation() -> None:
    authority = execution_authority_from_payload(build_h74_observation_authority_payload())

    with pytest.raises(ValueError, match="operation_not_allowed"):
        require_authority_operation(authority, "strategy_run")


def test_command_intent_rejects_wrong_authority_type() -> None:
    smoke_payload = build_operator_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )
    h74_payload = build_h74_observation_authority_payload()
    approved_payload = {
        "artifact_type": "approved_profile",
        "profile_content_hash": "sha256:" + "a" * 64,
        "market": "KRW-BTC",
    }

    assert resolve_execution_authority("smoke-buy", object(), smoke_payload).authority_type == OPERATOR_SMOKE_AUTHORITY_TYPE
    assert resolve_execution_authority("strategy-run", object(), approved_payload).authority_type == APPROVED_PROFILE_AUTHORITY_TYPE
    assert resolve_execution_authority("h74-observation", object(), h74_payload).authority_type == LIVE_OBSERVATION_AUTHORITY_TYPE
    with pytest.raises(ValueError, match="operation_not_allowed"):
        resolve_execution_authority("smoke-buy", object(), approved_payload)
    with pytest.raises(ValueError, match="operation_not_allowed"):
        resolve_execution_authority("strategy-run", object(), h74_payload)
    with pytest.raises(ValueError, match="operation_not_allowed"):
        resolve_execution_authority("strategy-run", object(), smoke_payload)
