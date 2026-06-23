from __future__ import annotations

from bithumb_bot.core.sma_policy import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, load_target_position_state
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.execution_service import build_execution_decision_summary
from bithumb_bot.experiment_execution_contract import POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT
from bithumb_bot.run_loop_execution_planner import (
    ExecutionPlanner,
    _inject_h74_startup_gate,
    resolve_target_position_state_for_run_loop,
)
from bithumb_bot.target_position import TARGET_POLICY_INITIALIZE_FLAT_TARGET
from tests.test_position_management_authority import _sell_plan
from tests.test_run_loop_execution_planner import _Readiness
from tests.test_target_delta_entry_authority import _readiness, _restore, _set_target_delta, _target_plan


def _hold_envelope(*, final_signal: str, final_reason: str) -> DecisionEnvelope:
    decision = StrategyDecisionV2(
        strategy_name="daily_participation_sma",
        raw_signal=final_signal,
        raw_reason=final_reason,
        entry_signal=final_signal,
        entry_reason=final_reason,
        exit_signal="HOLD",
        exit_reason="no_exit",
        final_signal=final_signal,
        final_reason=final_reason,
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=EntryExecutionIntent(
            side="BUY" if final_signal == "BUY" else "NONE",
            intent="enter" if final_signal == "BUY" else "hold",
            pair="KRW-BTC",
            requires_execution_sizing=final_signal == "BUY",
            budget_fraction_of_cash=1.0 if final_signal == "BUY" else 0.0,
            max_budget_krw=100_000.0,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"final_signal": final_signal, "final_reason": final_reason},
        policy_hash="sha256:policy",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )
    return DecisionEnvelope(
        strategy_decision=decision,
        candle_ts=1_782_147_600_000,
        market_price=100_000_000.0,
        base_context={
            "strategy": "daily_participation_sma",
            "final_signal": final_signal,
            "final_reason": final_reason,
            "signal": final_signal,
            "market_price": 100_000_000.0,
            "last_close": 100_000_000.0,
        },
        policy_hashes=None,
        replay_fingerprint={"candle_ts": 1_782_147_600_000},
        boundary={"phase": "target_state_restart_guard"},
    )


def _planner_result(tmp_path, *, final_signal: str, final_reason: str):
    db_path = tmp_path / f"empty-target-{final_signal.lower()}.sqlite"
    conn = ensure_db(str(db_path))
    try:
        assert load_target_position_state(conn, pair="KRW-BTC") is None
        planner = ExecutionPlanner(
            readiness_snapshot_builder=lambda _conn: _Readiness(
                _readiness(broker_qty=0.0)
                | {
                    "residual_proof_min_qty": 0.0001,
                    "residual_proof_min_notional_krw": 5_000.0,
                }
            ),
        )
        return planner.plan_envelope(
            conn,
            _hold_envelope(final_signal=final_signal, final_reason=final_reason),
            updated_ts=1_782_147_600_000,
        )
    finally:
        conn.close()


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


def test_restart_empty_target_state_hold_flat_resolver_initializes_flat(tmp_path) -> None:
    old = _set_target_delta()
    try:
        conn = ensure_db(str(tmp_path / "empty-target-resolver.sqlite"))
        try:
            assert load_target_position_state(conn, pair="KRW-BTC") is None
            resolved = resolve_target_position_state_for_run_loop(
                conn,
                readiness_payload=_readiness(broker_qty=0.0)
                | {
                    "residual_proof_min_qty": 0.0001,
                    "residual_proof_min_notional_krw": 5_000.0,
                },
                reference_price=100_000_000.0,
                raw_signal="HOLD",
                updated_ts=1_782_147_600_000,
                runtime_pair="KRW-BTC",
            )
        finally:
            conn.close()
    finally:
        _restore(old)

    metadata = resolved["target_policy_metadata"]
    assert resolved["previous_target_exposure_krw"] == 0.0
    assert metadata["target_policy_action"] == TARGET_POLICY_INITIALIZE_FLAT_TARGET
    assert metadata["target_state_update_intent"]["target_exposure_krw"] == 0.0
    assert metadata["target_state_update_intent"]["target_qty"] == 0.0


def test_full_planner_empty_target_state_kst18_no_buy(tmp_path) -> None:
    old = _set_target_delta()
    try:
        result = _planner_result(
            tmp_path,
            final_signal="HOLD",
            final_reason="outside_daily_participation_window",
        )
    finally:
        _restore(old)

    assert result.planning_error is None
    assert result.submit_plan is not None
    plan = result.submit_plan.as_dict()
    assert plan["target_delta_side"] == "NONE"
    assert plan["submit_expected"] is False
    assert plan["active_target_state"] == "active"
    assert plan["active_target_exposure_krw"] == 0.0
    assert plan["target_previous_exposure_krw"] == 0.0
    assert plan["target_missing_state_resolution"] == TARGET_POLICY_INITIALIZE_FLAT_TARGET


def test_restart_empty_target_state_kst10_entry_can_buy(tmp_path) -> None:
    old = _set_target_delta()
    try:
        object.__setattr__(settings, "MODE", "paper")
        object.__setattr__(settings, "LIVE_DRY_RUN", True)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
        result = _planner_result(
            tmp_path,
            final_signal="BUY",
            final_reason="daily_participation_fallback_allowed",
        )
    finally:
        _restore(old)

    assert result.planning_error is None
    assert result.submit_plan is not None
    plan = result.submit_plan.as_dict()
    assert plan["target_delta_side"] == "BUY"
    assert plan["submit_expected"] is True
    assert plan["entry_authority_status"] == "ALLOW"
    assert plan["entry_authority_reason_code"] == "strategy_final_signal_buy"
    assert plan["target_previous_exposure_krw"] == 0.0


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


def test_h74_source_authority_nonzero_target_state_blocks_entry() -> None:
    old = _set_target_delta()
    try:
        payload = _inject_h74_startup_gate(
            readiness_payload={
                **_readiness(broker_qty=0.0),
                "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
            },
            target_state={"target_exposure_krw": 100_000.0},
            authority_fields={"residual_inventory_mode": "block_executable_residual"},
        )
        summary = build_execution_decision_summary(
            decision_context={
                **payload,
                "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
                "raw_signal": "BUY",
                "final_signal": "BUY",
                "signal": "BUY",
                "final_reason": "daily_participation_fallback_allowed",
                "market_price": 100_000_000.0,
                "cash_available": 1_000_000.0,
            },
            readiness_payload=payload,
            raw_signal="BUY",
            final_signal="BUY",
            final_reason="daily_participation_fallback_allowed",
            previous_target_exposure_krw=100_000.0,
        )
    finally:
        _restore(old)

    assert summary.target_submit_plan is not None
    plan = summary.target_submit_plan.as_dict()
    assert plan["h74_startup_gate_status"] == "START_BLOCKED"
    assert plan["h74_startup_gate_reason_code"] == "target_state_nonzero"
    assert plan["submit_expected"] is False


def test_restart_preserves_existing_position_management() -> None:
    old = _set_target_delta()
    try:
        plan = _sell_plan()
    finally:
        _restore(old)

    assert plan["target_delta_side"] == "SELL"
    assert plan["submit_expected"] is True
