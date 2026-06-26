from __future__ import annotations

import pytest

from bithumb_bot.h74_position_ownership import h74_position_ownership_contract_from_payload
from bithumb_bot.h74_submit_identity import H74SubmitIdentity, H74SubmitIdentityError


def _contract(**overrides: object):
    payload = {
        "cycle_id": "cycle-1",
        "h74_cycle_id": "cycle-1",
        "strategy_instance_id": "h74-source-observation",
        "authority_hash": "sha256:a",
        "probe_run_id": "probe-run-1",
        "pair": "KRW-BTC",
        "entry_side": "BUY",
        "entry_plan_id": "h74-entry-plan-1",
        "position_mode": "fixed_fill_qty_until_exit",
        "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
    }
    payload.update(overrides)
    return h74_position_ownership_contract_from_payload(payload)


def _identity_payload(**overrides: object) -> dict[str, object]:
    contract = _contract()
    payload = {
        "cycle_id": contract.cycle_id,
        "h74_cycle_id": contract.h74_cycle_id,
        "strategy_instance_id": contract.strategy_instance_id,
        "authority_hash": contract.authority_hash,
        "probe_run_id": contract.probe_run_id,
        "h74_entry_plan_client_order_id": contract.entry_plan_id,
        "h74_position_ownership_contract_hash": contract.contract_hash,
        "h74_position_ownership_contract": contract.as_dict(),
    }
    payload.update(overrides)
    return payload


def test_h74_submit_identity_requires_cycle_aliases() -> None:
    with pytest.raises(H74SubmitIdentityError, match="cycle_id"):
        H74SubmitIdentity.from_mapping(_identity_payload(cycle_id=""))
    with pytest.raises(H74SubmitIdentityError, match="h74_cycle_id"):
        H74SubmitIdentity.from_mapping(_identity_payload(h74_cycle_id=""))


def test_h74_submit_identity_serializes_cycle_id_and_h74_cycle_id() -> None:
    identity = H74SubmitIdentity.from_mapping(_identity_payload())

    evidence = identity.as_evidence_dict()

    assert evidence["cycle_id"] == "cycle-1"
    assert evidence["h74_cycle_id"] == "cycle-1"


def test_h74_submit_identity_rejects_missing_entry_plan_id() -> None:
    with pytest.raises(H74SubmitIdentityError, match="h74_entry_plan_client_order_id"):
        H74SubmitIdentity.from_mapping(_identity_payload(h74_entry_plan_client_order_id=""))


def test_h74_submit_identity_rejects_contract_hash_mismatch() -> None:
    with pytest.raises(H74SubmitIdentityError, match="contract_hash"):
        H74SubmitIdentity.from_mapping(
            _identity_payload(h74_position_ownership_contract_hash="sha256:mismatch")
        )


def test_h74_submit_identity_rejects_entry_plan_contract_mismatch() -> None:
    payload = _identity_payload(h74_entry_plan_client_order_id="h74-entry-plan-other")

    with pytest.raises(H74SubmitIdentityError, match="h74_entry_plan_client_order_id"):
        H74SubmitIdentity.from_mapping(payload)
