from __future__ import annotations

from tests.test_position_management_authority import _sell_plan
from tests.test_target_delta_entry_authority import _restore, _set_target_delta, _target_plan


def test_restart_missing_target_state_hold_flat_does_not_buy() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="HOLD",
            final_reason="outside_daily_participation_window",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert plan["submit_expected"] is False
    assert plan["entry_authority_status"] == "BLOCK"


def test_restart_missing_target_state_kst10_entry_can_buy() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="BUY",
            final_reason="daily_participation_fallback_allowed",
            previous_target_exposure_krw=0.0,
        )
    finally:
        _restore(old)

    assert plan["submit_expected"] is True
    assert plan["entry_authority_status"] == "ALLOW"


def test_restart_preserves_existing_position_management() -> None:
    old = _set_target_delta()
    try:
        plan = _sell_plan()
    finally:
        _restore(old)

    assert plan["target_delta_side"] == "SELL"
    assert plan["submit_expected"] is True
