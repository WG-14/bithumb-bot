from __future__ import annotations

from bithumb_bot.h74_startup_gate import evaluate_h74_startup_gate
from bithumb_bot.virtual_target_state import StrategyVirtualTargetState


def _readiness() -> dict[str, object]:
    return {
        "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0},
        "open_order_count": 0,
        "submit_unknown_count": 0,
        "recovery_required_count": 0,
    }


def test_risk_blocked_buy_does_not_create_active_virtual_open() -> None:
    state = StrategyVirtualTargetState(
        strategy_instance_id="s",
        strategy_name="daily_participation_sma",
        pair="KRW-BTC",
        interval="1m",
        scope_key_hash="sha256:" + "a" * 64,
        runtime_contract_hash="sha256:" + "b" * 64,
        virtual_target_exposure_krw=100_000.0,
        virtual_target_qty=1.0,
        lifecycle_state="virtual_open",
        last_signal="BUY",
        updated_ts=1,
    )

    assert state.as_dict()["live_submit_authority"] is False


def test_startup_gate_ignores_non_authoritative_virtual_state() -> None:
    result = evaluate_h74_startup_gate(
        readiness_payload=_readiness(),
        target_state={"target_exposure_krw": 100_000.0, "live_submit_authority": False},
    )

    assert result.allowed is True


def test_startup_gate_blocks_authoritative_virtual_state() -> None:
    result = evaluate_h74_startup_gate(
        readiness_payload=_readiness(),
        target_state={"target_exposure_krw": 100_000.0, "live_submit_authority": True},
    )

    assert result.reason_code == "target_state_nonzero"
