from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, load_target_position_state, upsert_target_position_state
from bithumb_bot.execution_service import build_execution_decision_summary
from bithumb_bot.target_position import (
    TARGET_STATE_PERSISTENCE_NOT_PERSISTED,
    TARGET_STATE_PERSISTENCE_MISSING,
    TARGET_STATE_PERSISTENCE_PERSISTED,
    TargetPositionSettings,
    build_target_position_decision,
)


def _readiness(
    *,
    broker_qty: float,
    projection_converged: bool = True,
    broker_portfolio_converged: bool = True,
    open_order_count: int = 0,
    unresolved_open_order_count: int = 0,
    recovery_required_count: int = 0,
    submit_unknown_count: int = 0,
) -> dict[str, object]:
    return {
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": broker_qty,
            "balance_source_stale": False,
        },
        "projection_converged": projection_converged,
        "projection_convergence": {"converged": projection_converged},
        "broker_portfolio_converged": broker_portfolio_converged,
        "open_order_count": open_order_count,
        "unresolved_open_order_count": unresolved_open_order_count,
        "recovery_required_count": recovery_required_count,
        "submit_unknown_count": submit_unknown_count,
        "accounting_projection_ok": True,
        "active_fee_accounting_blocker": False,
    }


def _rules() -> dict[str, object]:
    return {"min_qty": 0.0001, "min_notional_krw": 5000.0}


def _settings(*, target_exposure_krw: float | None = None) -> TargetPositionSettings:
    return TargetPositionSettings(
        execution_engine="lot_native",
        shadow_enabled=True,
        target_exposure_krw=target_exposure_krw,
        max_order_krw=100_000.0,
        hold_policy="maintain_previous_target",
    )


def _target_delta_settings(*, target_exposure_krw: float | None = None) -> TargetPositionSettings:
    return TargetPositionSettings(
        execution_engine="target_delta",
        shadow_enabled=False,
        target_exposure_krw=target_exposure_krw,
        max_order_krw=100_000.0,
        hold_policy="maintain_previous_target",
    )


def test_target_shadow_sell_models_ec2_residual_as_executable_delta() -> None:
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=None,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0004998),
        order_rules=_rules(),
        reference_price=115_000_000.0,
        settings=_settings(),
    )

    assert decision.new_target_exposure_krw == 0.0
    assert decision.delta_side == "SELL"
    assert decision.submit_qty == pytest.approx(0.0004998)
    assert decision.would_submit is True
    assert decision.block_reason == "none"
    assert decision.position_truth_state == "converged"


def test_target_shadow_buy_subtracts_current_position_exposure() -> None:
    decision = build_target_position_decision(
        raw_signal="BUY",
        previous_target_exposure_krw=None,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0004998),
        order_rules=_rules(),
        reference_price=115_000_000.0,
        settings=_settings(target_exposure_krw=100_000.0),
    )

    assert decision.new_target_exposure_krw == 100_000.0
    assert decision.delta_side == "BUY"
    assert decision.current_exposure_krw == pytest.approx(57_477.0)
    assert decision.delta_notional_krw == pytest.approx(42_523.0)
    assert decision.submit_notional_krw == pytest.approx(42_523.0)
    assert decision.would_submit is True


def test_target_shadow_true_dust_is_noop_below_exchange_minimum() -> None:
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=None,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.00000004),
        order_rules=_rules(),
        reference_price=115_000_000.0,
        settings=_settings(),
    )

    assert decision.delta_side == "NONE"
    assert decision.would_submit is False
    assert decision.dust_classification == "true_dust"
    assert decision.block_reason == "delta_below_exchange_min"


def test_target_shadow_blocks_when_broker_local_position_not_converged() -> None:
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=None,
        current_position_snapshot=None,
        readiness_payload=_readiness(
            broker_qty=0.0005,
            projection_converged=False,
            broker_portfolio_converged=False,
        ),
        order_rules=_rules(),
        reference_price=115_000_000.0,
        settings=_settings(),
    )

    assert decision.would_submit is False
    assert decision.block_reason == "broker_local_not_converged"
    assert decision.position_truth_state == "blocked"


def test_target_shadow_hold_is_explicitly_degraded_without_persisted_target_state() -> None:
    decision = build_target_position_decision(
        raw_signal="HOLD",
        previous_target_exposure_krw=None,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0004998),
        order_rules=_rules(),
        reference_price=115_000_000.0,
        settings=_settings(target_exposure_krw=100_000.0),
    )

    assert decision.would_submit is False
    assert decision.new_target_exposure_krw is None
    assert decision.block_reason == "missing_persistent_target_state"
    assert decision.state_persistence == TARGET_STATE_PERSISTENCE_NOT_PERSISTED


def test_target_delta_buy_sell_and_hold_use_persisted_target() -> None:
    buy = build_target_position_decision(
        raw_signal="BUY",
        previous_target_exposure_krw=None,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(target_exposure_krw=None),
    )
    assert buy.engine_mode == "target_delta"
    assert buy.new_target_exposure_krw == pytest.approx(100_000.0)
    assert buy.target_qty == pytest.approx(0.001)
    assert buy.state_persistence == TARGET_STATE_PERSISTENCE_PERSISTED

    sell = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0004998),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(),
    )
    assert sell.new_target_exposure_krw == 0.0
    assert sell.delta_side == "SELL"
    assert sell.submit_qty == pytest.approx(0.0004998)

    hold = build_target_position_decision(
        raw_signal="HOLD",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0004),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(),
    )
    assert hold.new_target_exposure_krw == pytest.approx(100_000.0)
    assert hold.delta_side == "BUY"
    assert hold.submit_qty == pytest.approx(0.0006)


def test_target_delta_hold_without_persisted_target_fails_closed() -> None:
    decision = build_target_position_decision(
        raw_signal="HOLD",
        previous_target_exposure_krw=None,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(),
    )

    assert decision.would_submit is False
    assert decision.block_reason == "missing_persistent_target_state"
    assert decision.state_persistence == TARGET_STATE_PERSISTENCE_MISSING


def test_target_state_persistence_survives_restart_simulation(tmp_path) -> None:
    db_path = str(tmp_path / "target_state.sqlite")
    conn = ensure_db(db_path)
    try:
        upsert_target_position_state(
            conn,
            pair="KRW-BTC",
            target_exposure_krw=0.0,
            target_qty=0.0,
            last_signal="SELL",
            last_decision_id=7,
            last_reference_price=100_000_000.0,
            updated_ts=1234,
        )
        conn.commit()
    finally:
        conn.close()

    restarted = ensure_db(db_path)
    try:
        state = load_target_position_state(restarted, pair="KRW-BTC")
    finally:
        restarted.close()

    assert state is not None
    assert state.target_exposure_krw == 0.0
    assert state.last_signal == "SELL"
    assert state.last_decision_id == 7


def test_target_delta_unsafe_readiness_blocks_submit() -> None:
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=0.0,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0005, open_order_count=1),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(),
    )

    assert decision.would_submit is False
    assert decision.block_reason == "open_order_count_nonzero"
    assert decision.position_truth_state == "blocked"


def test_execution_decision_omits_target_shadow_when_feature_flag_disabled() -> None:
    old_shadow = settings.TARGET_EXECUTION_SHADOW
    try:
        object.__setattr__(settings, "TARGET_EXECUTION_SHADOW", False)
        summary = build_execution_decision_summary(
            decision_context={
                "raw_signal": "SELL",
                "market_price": 115_000_000.0,
                "sellable_executable_lot_count": 0,
            },
            readiness_payload=_readiness(broker_qty=0.0004998) | {
                "residual_proof_min_qty": 0.0001,
                "residual_proof_min_notional_krw": 5000.0,
            },
            raw_signal="SELL",
            final_signal="HOLD",
        ).as_dict()
    finally:
        object.__setattr__(settings, "TARGET_EXECUTION_SHADOW", old_shadow)

    assert summary["target_shadow_decision"] is None


def test_execution_decision_includes_target_shadow_when_feature_flag_enabled() -> None:
    old_shadow = settings.TARGET_EXECUTION_SHADOW
    old_target = settings.TARGET_EXPOSURE_KRW
    try:
        object.__setattr__(settings, "TARGET_EXECUTION_SHADOW", True)
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", 100_000.0)
        summary = build_execution_decision_summary(
            decision_context={
                "raw_signal": "BUY",
                "market_price": 115_000_000.0,
                "sellable_executable_lot_count": 0,
            },
            readiness_payload=_readiness(broker_qty=0.0004998) | {
                "residual_proof_min_qty": 0.0001,
                "residual_proof_min_notional_krw": 5000.0,
            },
            raw_signal="BUY",
            final_signal="BUY",
        ).as_dict()
    finally:
        object.__setattr__(settings, "TARGET_EXECUTION_SHADOW", old_shadow)
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", old_target)

    target = summary["target_shadow_decision"]
    assert isinstance(target, dict)
    assert target["target_delta_side"] == "BUY"
    assert target["target_would_submit"] is True
    assert target["target_submit_notional_krw"] == pytest.approx(42_523.0)


def test_target_delta_execution_bypasses_residual_sell_mode_and_lot_authority() -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_residual_mode = settings.RESIDUAL_LIVE_SELL_MODE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry")
        summary = build_execution_decision_summary(
            decision_context={
                "raw_signal": "SELL",
                "market_price": 115_000_000.0,
                "sellable_executable_lot_count": 0,
                "exit_allowed": False,
                "exit_block_reason": "dust_only_remainder",
            },
            readiness_payload=_readiness(broker_qty=0.0004998) | {
                "residual_proof_min_qty": 0.0001,
                "residual_proof_min_notional_krw": 5000.0,
            },
            raw_signal="SELL",
            final_signal="HOLD",
            previous_target_exposure_krw=0.0,
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", old_residual_mode)

    assert summary.submit_expected is True
    assert summary.block_reason == "none"
    assert summary.strategy_sell_candidate is None
    assert summary.residual_submit_plan is None
    assert summary.target_submit_plan is not None
    assert summary.target_submit_plan["source"] == "target_delta"
    assert summary.target_submit_plan["authority"] == "target_position_delta"
    assert summary.target_submit_plan["side"] == "SELL"
    assert summary.target_submit_plan["qty"] == pytest.approx(0.0004998)


def test_target_delta_buy_sizes_only_missing_delta() -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_target = settings.TARGET_EXPOSURE_KRW
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", 100_000.0)
        summary = build_execution_decision_summary(
            decision_context={"raw_signal": "BUY", "market_price": 100_000_000.0},
            readiness_payload=_readiness(broker_qty=0.0004) | {
                "residual_proof_min_qty": 0.0001,
                "residual_proof_min_notional_krw": 5000.0,
            },
            raw_signal="BUY",
            final_signal="BUY",
            previous_target_exposure_krw=0.0,
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", old_target)

    assert summary.target_submit_plan is not None
    assert summary.target_submit_plan["side"] == "BUY"
    assert summary.target_submit_plan["qty"] == pytest.approx(0.0006)
    assert summary.target_submit_plan["notional_krw"] == pytest.approx(60_000.0)
