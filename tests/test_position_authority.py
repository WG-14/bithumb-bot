from __future__ import annotations

from bithumb_bot.position_authority import (
    LOT_NATIVE_RESEARCH_POSITION_MODEL,
    research_lot_native_position_authority_snapshot,
    research_position_authority_snapshot,
    runtime_position_authority_snapshot,
)


def _position_gate(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "raw_total_asset_qty": 0.0002,
        "open_lot_count": 2,
        "dust_tracking_lot_count": 0,
        "reserved_exit_lot_count": 0,
        "sellable_executable_lot_count": 2,
        "open_exposure_qty": 0.0002,
        "dust_tracking_qty": 0.0,
        "reserved_exit_qty": 0.0,
        "sellable_executable_qty": 0.0002,
        "terminal_state": "open_exposure",
        "entry_allowed": False,
        "exit_allowed": True,
        "effective_flat": False,
        "normalized_exposure_active": True,
        "dust_state": "no_dust",
        "has_any_position_residue": True,
        "has_dust_only_remainder": False,
        "recovery_blocked": False,
        "recovery_block_reason": "none",
    }
    payload.update(overrides)
    return payload


def _snapshot(position_gate: dict[str, object]):
    return runtime_position_authority_snapshot(
        position_gate=position_gate,
        order_rules_hash="sha256:order_rules",
        fee_authority_hash="sha256:fee_authority",
        position_state_hash="sha256:position",
    )


def _research_lot_native_fields(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "open_lot_count": 2,
        "sellable_executable_lot_count": 2,
        "reserved_exit_lot_count": 0,
        "dust_tracking_lot_count": 0,
        "open_exposure_qty": 0.0002,
        "sellable_executable_qty": 0.0002,
        "reserved_exit_qty": 0.0,
        "dust_tracking_qty": 0.0,
        "terminal_state": "open_exposure",
        "order_rules_hash": "sha256:order_rules",
        "fee_authority_hash": "sha256:fee_authority",
        "position_state_hash": "sha256:position",
    }
    payload.update(overrides)
    return payload


def _research_lot_native_snapshot(fields: dict[str, object]):
    return research_lot_native_position_authority_snapshot(
        lot_native_fields=fields,
        order_rules_hash="sha256:order_rules",
        fee_authority_hash="sha256:fee_authority",
        position_state_hash="sha256:position",
    )


def test_runtime_open_exposure_with_complete_lot_native_fields_is_supported() -> None:
    snapshot = _snapshot(_position_gate())

    assert snapshot.state_class == "open_exposure"
    assert snapshot.unsupported_reason == ""


def test_runtime_reserved_exit_pending_with_complete_lot_native_fields_remains_scaffolded_fail_closed() -> None:
    snapshot = _snapshot(
        _position_gate(
            reserved_exit_lot_count=2,
            sellable_executable_lot_count=0,
            reserved_exit_qty=0.0002,
            sellable_executable_qty=0.0,
            terminal_state="reserved_exit_pending",
            exit_allowed=False,
        )
    )

    assert snapshot.state_class == "reserved_exit_pending"
    assert snapshot.unsupported_reason == "research_model_lacks_lot_native_authority"
    assert snapshot.research_position_model == ""


def test_runtime_positive_class_with_missing_lot_native_field_fails_closed() -> None:
    position_gate = _position_gate()
    position_gate.pop("sellable_executable_qty")

    snapshot = _snapshot(position_gate)

    assert snapshot.state_class == "open_exposure"
    assert snapshot.unsupported_reason == "research_model_lacks_lot_native_authority"


def test_runtime_positive_class_with_inconsistent_lot_native_fields_fails_closed() -> None:
    snapshot = _snapshot(_position_gate(sellable_executable_lot_count=0))

    assert snapshot.state_class == "open_exposure"
    assert snapshot.unsupported_reason == "research_model_lacks_lot_native_authority"


def test_runtime_dust_and_recovery_states_remain_fail_closed() -> None:
    dust = _snapshot(
        _position_gate(
            open_lot_count=0,
            sellable_executable_lot_count=0,
            open_exposure_qty=0.0,
            sellable_executable_qty=0.0,
            dust_tracking_lot_count=1,
            dust_tracking_qty=0.0001,
            terminal_state="dust_only",
            exit_allowed=False,
            normalized_exposure_active=False,
            dust_state="dust_only",
            has_dust_only_remainder=True,
        )
    )
    recovery = _snapshot(_position_gate(recovery_blocked=True, recovery_block_reason="recovery_required_present"))

    assert dust.state_class == "dust_only"
    assert dust.unsupported_reason == "research_model_lacks_dust_state"
    assert recovery.state_class == "recovery_blocked"
    assert recovery.unsupported_reason == "research_model_lacks_lot_native_authority"


def test_research_flat_no_dust_no_position_remains_positive_equivalence() -> None:
    snapshot = research_position_authority_snapshot(
        qty=0.0,
        sellable_qty=0.0,
        order_rules_hash="sha256:order_rules",
        fee_authority_hash="sha256:fee_authority",
        position_state_hash="sha256:position",
    )

    assert snapshot.state_class == "flat_no_dust_no_position"
    assert snapshot.unsupported_reason == ""


def test_research_open_exposure_with_complete_lot_native_authority_is_supported() -> None:
    snapshot = _research_lot_native_snapshot(_research_lot_native_fields())

    assert snapshot.state_class == "open_exposure"
    assert snapshot.unsupported_reason == ""
    assert snapshot.research_position_model == LOT_NATIVE_RESEARCH_POSITION_MODEL
    assert snapshot.open_lot_count == 2
    assert snapshot.sellable_executable_lot_count == 2


def test_research_open_exposure_without_required_lot_native_fields_fails_closed() -> None:
    fields = _research_lot_native_fields()
    fields.pop("sellable_executable_qty")

    snapshot = _research_lot_native_snapshot(fields)

    assert snapshot.state_class == "open_exposure"
    assert snapshot.unsupported_reason == "research_model_lacks_lot_native_authority"


def test_research_open_exposure_with_hash_mismatch_fails_closed() -> None:
    snapshot = _research_lot_native_snapshot(
        _research_lot_native_fields(order_rules_hash="sha256:different_order_rules")
    )

    assert snapshot.state_class == "open_exposure"
    assert snapshot.unsupported_reason == "research_model_lacks_lot_native_authority"


def test_research_reserved_exit_pending_remains_fail_closed_without_positive_fixture() -> None:
    snapshot = _research_lot_native_snapshot(
        _research_lot_native_fields(
            terminal_state="reserved_exit_pending",
            sellable_executable_lot_count=0,
            reserved_exit_lot_count=2,
            sellable_executable_qty=0.0,
            reserved_exit_qty=0.0002,
        )
    )

    assert snapshot.state_class == "reserved_exit_pending"
    assert snapshot.unsupported_reason == "research_model_lacks_lot_native_authority"


def test_research_dust_non_executable_and_recovery_states_remain_fail_closed() -> None:
    dust = _research_lot_native_snapshot(
        _research_lot_native_fields(
            terminal_state="dust_only",
            open_lot_count=0,
            sellable_executable_lot_count=0,
            dust_tracking_lot_count=1,
            open_exposure_qty=0.0,
            sellable_executable_qty=0.0,
            dust_tracking_qty=0.0001,
        )
    )
    non_executable = _research_lot_native_snapshot(
        _research_lot_native_fields(
            terminal_state="non_executable_position",
            open_lot_count=0,
            sellable_executable_lot_count=0,
            open_exposure_qty=0.0,
            sellable_executable_qty=0.0,
        )
    )
    recovery = _research_lot_native_snapshot(_research_lot_native_fields(recovery_blocked=True))

    assert dust.state_class == "dust_only"
    assert dust.unsupported_reason == "research_model_lacks_dust_state"
    assert non_executable.state_class == "non_executable_position"
    assert non_executable.unsupported_reason == "research_model_lacks_lot_native_authority"
    assert recovery.state_class == "recovery_blocked"
    assert recovery.unsupported_reason == "research_model_lacks_lot_native_authority"
