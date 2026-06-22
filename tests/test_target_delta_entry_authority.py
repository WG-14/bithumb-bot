from __future__ import annotations

from bithumb_bot.config import settings
from bithumb_bot.execution_service import build_execution_decision_summary


def _set_target_delta() -> dict[str, object]:
    old = {
        "MODE": settings.MODE,
        "EXECUTION_ENGINE": settings.EXECUTION_ENGINE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "TARGET_EXPOSURE_KRW": settings.TARGET_EXPOSURE_KRW,
        "MAX_ORDER_KRW": settings.MAX_ORDER_KRW,
    }
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "TARGET_EXPOSURE_KRW", 100_000.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100_000.0)
    return old


def _restore(old: dict[str, object]) -> None:
    for key, value in old.items():
        object.__setattr__(settings, key, value)


def _readiness(*, broker_qty: float = 0.0) -> dict[str, object]:
    return {
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": broker_qty,
            "balance_source_stale": False,
        },
        "projection_converged": True,
        "projection_convergence": {"converged": True},
        "broker_portfolio_converged": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "accounting_projection_ok": True,
        "cash_available": 1_000_000.0,
        "min_qty": 0.0001,
        "qty_step": 0.0001,
        "min_notional_krw": 5_000.0,
    }


def _target_plan(
    *,
    final_signal: str,
    final_reason: str,
    previous_target_exposure_krw: float,
) -> dict[str, object]:
    summary = build_execution_decision_summary(
        decision_context={
            "raw_signal": final_signal,
            "final_signal": final_signal,
            "signal": final_signal,
            "final_reason": final_reason,
            "market_price": 100_000_000.0,
        },
        readiness_payload=_readiness(),
        raw_signal=final_signal,
        final_signal=final_signal,
        final_reason=final_reason,
        previous_target_exposure_krw=previous_target_exposure_krw,
    )
    assert summary.target_submit_plan is not None
    return summary.target_submit_plan.as_dict()


def test_out_of_window_hold_flat_desired_exposure_does_not_create_buy_plan() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="HOLD",
            final_reason="outside_daily_participation_window",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert plan["target_delta_side"] == "BUY"
    assert plan["submit_expected"] is False
    assert plan["block_reason"] == "target_delta_entry_without_strategy_buy_authority"
    assert plan["entry_authority_status"] == "BLOCK"
    assert plan["active_target_state"] == "inactive"
    assert plan["active_target_exposure_krw"] == 0.0


def test_final_signal_buy_allows_target_delta_buy() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="BUY",
            final_reason="sma_cross",
            previous_target_exposure_krw=0.0,
        )
    finally:
        _restore(old)

    assert plan["target_delta_side"] == "BUY"
    assert plan["submit_expected"] is True
    assert plan["entry_authority_status"] == "ALLOW"
    assert plan["entry_authority_reason_code"] == "strategy_final_signal_buy"


def test_daily_participation_fallback_allowed_allows_target_delta_buy() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="HOLD",
            final_reason="daily_participation_fallback_allowed",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert plan["target_delta_side"] == "BUY"
    assert plan["submit_expected"] is True
    assert plan["entry_authority_status"] == "ALLOW"
    assert plan["entry_authority_reason_code"] == "daily_participation_entry"


def test_restart_target_state_daily_participation_fallback_allowed_can_buy() -> None:
    old = _set_target_delta()
    try:
        plan = _target_plan(
            final_signal="HOLD",
            final_reason="daily_participation_fallback_allowed",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert plan["target_delta_side"] == "BUY"
    assert plan["submit_expected"] is True
    assert plan["entry_authority_status"] == "ALLOW"
    assert plan["entry_authority_reason_code"] == "daily_participation_entry"
