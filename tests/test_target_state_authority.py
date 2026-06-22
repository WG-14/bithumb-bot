from __future__ import annotations

from tests.test_target_delta_entry_authority import _restore, _set_target_delta, _target_plan


def test_desired_exposure_does_not_activate_target_without_entry_authority() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="HOLD",
            final_reason="outside_daily_participation_window",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert plan["entry_authorized"] is False
    assert plan["active_target_state"] == "inactive"
    assert plan["active_target_exposure_krw"] == 0.0
    assert plan["submit_expected"] is False


def test_entry_authority_activates_nonzero_target_state() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="BUY",
            final_reason="daily_participation_fallback_allowed",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert plan["entry_authorized"] is True
    assert plan["active_target_state"] == "active"
    assert plan["active_target_exposure_krw"] == 100_000.0


def test_hold_flat_state_keeps_target_inactive_after_restart() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="HOLD",
            final_reason="outside_daily_participation_window",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert plan["target_policy_action"] in {"", "initialize_flat_target", "use_existing_target"}
    assert plan["entry_authority_status"] == "BLOCK"
    assert plan["submit_expected"] is False
