from __future__ import annotations

from bithumb_bot.config import settings
from bithumb_bot.execution_service import build_execution_decision_summary

from tests.test_target_delta_entry_authority import _readiness, _restore, _set_target_delta


def _sell_plan(*, final_reason: str = "max_holding_exit") -> dict[str, object]:
    summary = build_execution_decision_summary(
        decision_context={
            "raw_signal": "SELL",
            "final_signal": "SELL",
            "signal": "SELL",
            "final_reason": final_reason,
            "market_price": 100_000_000.0,
        },
        readiness_payload=_readiness(broker_qty=0.001),
        raw_signal="SELL",
        final_signal="SELL",
        final_reason=final_reason,
        previous_target_exposure_krw=100_000.0,
    )
    assert summary.target_submit_plan is not None
    return summary.target_submit_plan.as_dict()


def test_entry_block_does_not_block_existing_position_sell() -> None:
    old = _set_target_delta()
    try:
        plan = _sell_plan()
    finally:
        _restore(old)

    assert plan["target_delta_side"] == "SELL"
    assert plan["submit_expected"] is True
    assert plan["entry_authority_status"] == "ALLOW"
    assert plan["position_management_authority_status"] == "ALLOW"


def test_max_holding_exit_allowed_when_entry_authority_blocks() -> None:
    old = _set_target_delta()
    try:
        plan = _sell_plan(final_reason="max_holding_exit")
    finally:
        _restore(old)

    assert plan["submit_expected"] is True
    assert plan["block_reason"] == "none"
    assert plan["position_management_authority_reason_code"] == "target_delta_sell_or_noop_allowed"


def test_closeout_authority_can_sell_without_entry_authority() -> None:
    old = _set_target_delta()
    try:
        summary = build_execution_decision_summary(
            decision_context={
                "raw_signal": "HOLD",
                "final_signal": "HOLD",
                "signal": "HOLD",
                "final_reason": "operator_closeout",
                "market_price": 100_000_000.0,
                "target_closeout_requested": True,
            },
            readiness_payload={**_readiness(broker_qty=0.001), "target_closeout_requested": True},
            raw_signal="HOLD",
            final_signal="HOLD",
            final_reason="operator_closeout",
            previous_target_exposure_krw=0.0,
        )
    finally:
        _restore(old)

    assert summary.target_submit_plan is not None
    plan = summary.target_submit_plan.as_dict()
    assert plan["target_delta_side"] == "SELL"
    assert plan["submit_expected"] is True
    assert plan["closeout_authority_status"] == "ALLOW"
    assert plan["entry_authority_status"] == "ALLOW"
