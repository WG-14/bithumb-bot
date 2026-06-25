from __future__ import annotations

import pytest

from bithumb_bot.runtime_scope import derive_risk_scope_id, require_risk_scope_reset_authority, strategy_revision_id


def _payload(**overrides: object) -> dict[str, object]:
    payload = {
        "strategy_name": "daily_participation_sma",
        "strategy_instance_id": "old",
        "pair": "KRW-BTC",
        "interval": "1m",
        "runtime_contract_hash": "sha256:" + "1" * 64,
        "approved_profile_hash": "sha256:" + "2" * 64,
        "strategy_parameters_hash": "sha256:" + "3" * 64,
        "risk_policy_hash": "sha256:" + "4" * 64,
        "risk_capital_basis": "fixed_observation_notional",
        "risk_capital_krw": 100_000,
    }
    payload.update(overrides)
    return payload


def test_non_economic_runtime_contract_change_preserves_risk_scope_id() -> None:
    old = _payload(strategy_instance_id="64fb", runtime_contract_hash="sha256:" + "1" * 64)
    new = _payload(strategy_instance_id="cabccc", runtime_contract_hash="sha256:" + "9" * 64)

    assert strategy_revision_id(old) != strategy_revision_id(new)
    assert derive_risk_scope_id(old) == derive_risk_scope_id(new)


def test_risk_scope_reset_requires_explicit_authority() -> None:
    with pytest.raises(ValueError, match="risk_scope_reset_authority_required"):
        require_risk_scope_reset_authority(
            previous=_payload(risk_capital_krw=100_000),
            current=_payload(risk_capital_krw=200_000),
        )


def test_strategy_revision_change_does_not_drop_lifecycle_history() -> None:
    old = _payload(strategy_instance_id="64fb")
    new = _payload(strategy_instance_id="cabccc")

    assert derive_risk_scope_id(old) == derive_risk_scope_id(new)
