from __future__ import annotations

import pytest

from bithumb_bot.h74_cycle_state import H74CycleInventory
from bithumb_bot.h74_position_ownership import (
    H74PositionOwnershipContract,
    H74PositionOwnershipError,
)


def _contract(**overrides: object) -> H74PositionOwnershipContract:
    values = {
        "cycle_id": "h74-cycle-1",
        "h74_cycle_id": "h74-cycle-1",
        "strategy_instance_id": "h74-source-observation",
        "authority_hash": "sha256:authority",
        "probe_run_id": "probe-run-1",
        "pair": "KRW-BTC",
        "entry_side": "BUY",
        "entry_plan_id": "h74-entry-plan-1",
        "position_mode": "fixed_fill_qty_until_exit",
        "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
    }
    values.update(overrides)
    return H74PositionOwnershipContract(**values)


@pytest.mark.parametrize(
    "field",
    ("cycle_id", "h74_cycle_id", "strategy_instance_id", "authority_hash", "probe_run_id"),
)
def test_h74_position_ownership_contract_requires_cycle_identity_fields(field: str) -> None:
    with pytest.raises(H74PositionOwnershipError, match="h74_cycle_ownership_required_for_entry"):
        _contract(**{field: ""})


def test_h74_position_ownership_contract_hash_is_stable() -> None:
    first = _contract()
    second = _contract()

    assert first.content_hash() == second.content_hash()
    assert first.contract_hash == second.contract_hash


def test_h74_position_ownership_contract_serializes_required_fields() -> None:
    payload = _contract().as_dict()

    for field in (
        "cycle_id",
        "h74_cycle_id",
        "strategy_instance_id",
        "authority_hash",
        "probe_run_id",
        "pair",
        "entry_side",
        "entry_plan_id",
        "position_mode",
        "hold_policy",
        "contract_hash",
    ):
        assert payload[field]


def test_h74_position_ownership_contract_serializes_remaining_cycle_qty_or_declares_inventory_boundary() -> None:
    contract = _contract()
    contract_payload = contract.as_dict()
    inventory = H74CycleInventory(
        cycle_id=contract.cycle_id,
        authority_hash=contract.authority_hash,
        strategy_instance_id=contract.strategy_instance_id,
        acquired_qty=0.0008,
        sold_qty=0.0,
        locked_exit_qty=0.0,
        contract_hash=contract.contract_hash,
    )
    inventory_payload = inventory.as_dict()

    assert "remaining_cycle_qty" not in contract_payload
    assert contract_payload["contract_hash"] == contract.contract_hash
    assert inventory_payload["remaining_cycle_qty"] == pytest.approx(0.0008)
    assert inventory_payload["contract_hash"] == contract.contract_hash
    assert inventory_payload["h74_position_ownership_contract_hash"] == contract.contract_hash


def test_h74_position_ownership_quantity_artifact_matches_cycle_inventory() -> None:
    contract = _contract()
    inventory = H74CycleInventory(
        cycle_id=contract.cycle_id,
        authority_hash=contract.authority_hash,
        strategy_instance_id=contract.strategy_instance_id,
        acquired_qty=0.0010,
        sold_qty=0.0001,
        locked_exit_qty=0.0002,
        contract_hash=contract.contract_hash,
    )
    h74_cycle_state = {
        "cycle_id": inventory.cycle_id,
        "acquired_qty": inventory.acquired_qty,
        "sold_qty": inventory.sold_qty,
        "locked_exit_qty": inventory.locked_exit_qty,
        "contract_hash": inventory.contract_hash,
    }
    expected_remaining = (
        h74_cycle_state["acquired_qty"]
        - h74_cycle_state["sold_qty"]
        - h74_cycle_state["locked_exit_qty"]
    )

    assert contract.cycle_id == inventory.cycle_id
    assert inventory.contract_hash == contract.contract_hash
    assert h74_cycle_state["contract_hash"] == contract.contract_hash
    assert inventory.remaining_cycle_qty == pytest.approx(expected_remaining)
