from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import (
    ensure_db,
    ensure_schema,
    load_target_position_state,
    upsert_target_position_state,
)
from bithumb_bot.engine import (
    _load_previous_target_exposure_for_run_loop,
    _resolve_target_position_state_for_run_loop,
    _persist_target_position_state_for_run_loop,
)
from bithumb_bot.execution_service import build_execution_decision_summary
from bithumb_bot.target_position import (
    TARGET_ORIGIN_ADOPTED_EXISTING_POSITION,
    TARGET_ORIGIN_FLAT_START,
    TARGET_ORIGIN_OPERATOR_CLOSEOUT,
    TARGET_ORIGIN_STRATEGY_BUY,
    TARGET_ORIGIN_STRATEGY_SELL,
    TARGET_ORIGIN_TRUE_DUST_FLAT,
    TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
    TARGET_POLICY_BLOCK_UNSAFE_STATE,
    TARGET_POLICY_INITIALIZE_FLAT_TARGET,
    TARGET_POLICY_INITIALIZE_TRUE_DUST_FLAT,
    TARGET_STATE_PERSISTENCE_NOT_PERSISTED,
    TARGET_STATE_PERSISTENCE_MISSING,
    TARGET_STATE_PERSISTENCE_PERSISTED,
    TargetPositionState,
    TargetPositionSettings,
    build_target_position_decision,
    resolve_startup_target_position_policy,
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


def test_startup_policy_missing_target_adopts_executable_broker_position_without_submit() -> None:
    policy = resolve_startup_target_position_policy(
        existing_target_state=None,
        readiness_payload=_readiness(broker_qty=0.0004998),
        order_rules=_rules(),
        reference_price=114_120_000.0,
        raw_signal="HOLD",
    )

    assert policy.policy_action == TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION
    assert policy.target_origin == TARGET_ORIGIN_ADOPTED_EXISTING_POSITION
    assert policy.target_qty == pytest.approx(0.0004998)
    assert policy.target_exposure_krw == pytest.approx(0.0004998 * 114_120_000.0)
    assert policy.adopted_broker_qty == pytest.approx(0.0004998)
    assert policy.would_submit_on_startup is False
    assert policy.block_reason == "none"

    decision = build_target_position_decision(
        raw_signal="HOLD",
        previous_target_exposure_krw=policy.target_exposure_krw,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0004998) | policy.as_dict(),
        order_rules=_rules(),
        reference_price=114_120_000.0,
        settings=_target_delta_settings(),
    )
    assert decision.target_origin == TARGET_ORIGIN_ADOPTED_EXISTING_POSITION
    assert decision.delta_side == "NONE"
    assert decision.would_submit is False
    assert decision.target_qty == pytest.approx(0.0004998)


def test_run_loop_resolver_missing_target_adopts_and_persists_broker_position(tmp_path) -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_pair = settings.PAIR
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "PAIR", "KRW-BTC")
        conn = ensure_db(str(tmp_path / "target_startup_adoption.sqlite"))
        try:
            readiness = _readiness(broker_qty=0.0004998) | {
                "residual_proof_min_qty": 0.0001,
                "residual_proof_min_notional_krw": 5000.0,
            }
            resolved = _resolve_target_position_state_for_run_loop(
                conn,
                readiness_payload=readiness,
                reference_price=114_120_000.0,
                raw_signal="HOLD",
                updated_ts=123456,
            )
            conn.commit()
            state = load_target_position_state(conn, pair="KRW-BTC")
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "PAIR", old_pair)

    metadata = resolved["target_policy_metadata"]
    expected_exposure = 0.0004998 * 114_120_000.0
    assert metadata["target_policy_action"] == TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION
    assert metadata["target_origin"] == TARGET_ORIGIN_ADOPTED_EXISTING_POSITION
    assert metadata["target_adopted_broker_qty"] == pytest.approx(0.0004998)
    assert resolved["previous_target_exposure_krw"] == pytest.approx(expected_exposure)
    assert state is not None
    assert state.target_exposure_krw == pytest.approx(expected_exposure)
    assert state.target_qty == pytest.approx(0.0004998)
    assert state.target_origin == TARGET_ORIGIN_ADOPTED_EXISTING_POSITION
    assert state.adoption_reason == "safe_converged_executable_broker_position"
    assert state.adopted_broker_qty == pytest.approx(0.0004998)
    assert state.adopted_broker_exposure_krw == pytest.approx(expected_exposure)
    assert state.created_from_signal == "HOLD"


def test_execution_summary_after_startup_adoption_does_not_sell() -> None:
    old_engine = settings.EXECUTION_ENGINE
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        adopted_exposure = 0.0004998 * 114_120_000.0
        summary = build_execution_decision_summary(
            decision_context={"raw_signal": "HOLD", "market_price": 114_120_000.0},
            readiness_payload=_readiness(broker_qty=0.0004998) | {
                "residual_proof_min_qty": 0.0001,
                "residual_proof_min_notional_krw": 5000.0,
                "target_policy_action": TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
                "target_origin": TARGET_ORIGIN_ADOPTED_EXISTING_POSITION,
                "target_adoption_reason": "safe_converged_executable_broker_position",
                "target_adopted_broker_qty": 0.0004998,
                "target_adopted_exposure_krw": adopted_exposure,
                "target_startup_policy_state": "converged",
                "target_existing_state_present": False,
                "target_missing_state_resolution": TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
                "target_would_submit_on_startup": False,
                "target_strategy_signal_source": "HOLD",
            },
            raw_signal="HOLD",
            final_signal="HOLD",
            previous_target_exposure_krw=adopted_exposure,
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)

    assert summary.submit_expected is False
    assert summary.final_action != "REBALANCE_TO_TARGET"
    assert summary.target_submit_plan is not None
    assert summary.target_submit_plan["submit_expected"] is False
    assert summary.target_submit_plan["side"] != "SELL"
    assert summary.target_submit_plan["target_delta_side"] == "NONE"
    assert summary.target_shadow_decision is not None
    assert summary.target_shadow_decision["target_origin"] == TARGET_ORIGIN_ADOPTED_EXISTING_POSITION
    assert summary.target_shadow_decision["target_delta_side"] == "NONE"


def test_startup_policy_missing_target_flat_broker_initializes_flat_without_submit() -> None:
    policy = resolve_startup_target_position_policy(
        existing_target_state=None,
        readiness_payload=_readiness(broker_qty=0.0),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        raw_signal="HOLD",
    )

    assert policy.policy_action == TARGET_POLICY_INITIALIZE_FLAT_TARGET
    assert policy.target_origin == TARGET_ORIGIN_FLAT_START
    assert policy.target_exposure_krw == 0.0
    assert policy.target_qty == 0.0
    assert policy.would_submit_on_startup is False


def test_startup_policy_missing_target_below_min_initializes_true_dust_flat() -> None:
    policy = resolve_startup_target_position_policy(
        existing_target_state=None,
        readiness_payload=_readiness(broker_qty=0.00000004),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        raw_signal="HOLD",
    )

    assert policy.policy_action == TARGET_POLICY_INITIALIZE_TRUE_DUST_FLAT
    assert policy.target_origin == TARGET_ORIGIN_TRUE_DUST_FLAT
    assert policy.dust_classification == "true_dust"
    assert policy.target_exposure_krw == 0.0
    assert policy.would_submit_on_startup is False


def test_startup_policy_existing_target_is_preserved() -> None:
    existing = TargetPositionState(
        pair="KRW-BTC",
        target_exposure_krw=57_816.0,
        target_qty=0.0004998,
        last_signal="HOLD",
        last_decision_id=10,
        last_reference_price=115_680_000.0,
        updated_ts=123,
        target_origin=TARGET_ORIGIN_ADOPTED_EXISTING_POSITION,
        adoption_reason="safe_converged_executable_broker_position",
        adopted_broker_qty=0.0004998,
        adopted_broker_exposure_krw=57_816.0,
        created_from_signal="HOLD",
    )

    policy = resolve_startup_target_position_policy(
        existing_target_state=existing,
        readiness_payload=_readiness(broker_qty=0.001),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        raw_signal="HOLD",
    )

    assert policy.policy_action == "use_existing_target"
    assert policy.target_exposure_krw == pytest.approx(57_816.0)
    assert policy.target_qty == pytest.approx(0.0004998)
    assert policy.target_origin == TARGET_ORIGIN_ADOPTED_EXISTING_POSITION


def test_target_delta_strategy_signals_change_adopted_target_authoritatively() -> None:
    readiness = _readiness(broker_qty=0.0004998)

    sell = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=57_816.0,
        current_position_snapshot=None,
        readiness_payload=readiness
        | {
            "target_origin": TARGET_ORIGIN_ADOPTED_EXISTING_POSITION,
            "target_policy_action": TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
        },
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(target_exposure_krw=70_000.0),
    )
    assert sell.new_target_exposure_krw == 0.0
    assert sell.target_origin == TARGET_ORIGIN_STRATEGY_SELL
    assert sell.delta_side == "SELL"
    assert sell.would_submit is True

    buy = build_target_position_decision(
        raw_signal="BUY",
        previous_target_exposure_krw=57_816.0,
        current_position_snapshot=None,
        readiness_payload=readiness
        | {
            "target_origin": TARGET_ORIGIN_ADOPTED_EXISTING_POSITION,
            "target_policy_action": TARGET_POLICY_ADOPT_EXISTING_BROKER_POSITION,
        },
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(target_exposure_krw=70_000.0),
    )
    assert buy.new_target_exposure_krw == 70_000.0
    assert buy.target_origin == TARGET_ORIGIN_STRATEGY_BUY
    assert buy.delta_side == "BUY"
    assert buy.submit_notional_krw == pytest.approx(20_020.0)


def test_operator_closeout_target_allows_hold_cycle_target_delta_sell() -> None:
    decision = build_target_position_decision(
        raw_signal="HOLD",
        previous_target_exposure_krw=0.0,
        current_position_snapshot=None,
        readiness_payload=_readiness(broker_qty=0.0004998)
        | {
            "target_origin": TARGET_ORIGIN_OPERATOR_CLOSEOUT,
            "target_closeout_requested": True,
            "target_strategy_signal_source": "OPERATOR_CLOSEOUT",
        },
        order_rules=_rules(),
        reference_price=100_000_000.0,
        settings=_target_delta_settings(),
    )

    assert decision.target_origin == TARGET_ORIGIN_OPERATOR_CLOSEOUT
    assert decision.target_closeout_requested is True
    assert decision.new_target_exposure_krw == 0.0
    assert decision.delta_side == "SELL"
    assert decision.would_submit is True


def test_startup_policy_unsafe_readiness_blocks_adoption() -> None:
    policy = resolve_startup_target_position_policy(
        existing_target_state=None,
        readiness_payload=_readiness(broker_qty=0.0004998, open_order_count=1),
        order_rules=_rules(),
        reference_price=100_000_000.0,
        raw_signal="HOLD",
    )

    assert policy.policy_action == TARGET_POLICY_BLOCK_UNSAFE_STATE
    assert policy.target_exposure_krw is None
    assert policy.block_reason == "open_order_count_nonzero"


def test_run_loop_resolver_unsafe_missing_target_blocks_adoption_without_persist(tmp_path) -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_pair = settings.PAIR
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "PAIR", "KRW-BTC")
        conn = ensure_db(str(tmp_path / "target_unsafe_startup.sqlite"))
        try:
            resolved = _resolve_target_position_state_for_run_loop(
                conn,
                readiness_payload=_readiness(broker_qty=0.0004998, open_order_count=1) | {
                    "residual_proof_min_qty": 0.0001,
                    "residual_proof_min_notional_krw": 5000.0,
                },
                reference_price=100_000_000.0,
                raw_signal="HOLD",
                updated_ts=123456,
            )
            state = load_target_position_state(conn, pair="KRW-BTC")
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "PAIR", old_pair)

    metadata = resolved["target_policy_metadata"]
    assert metadata["target_policy_action"] == TARGET_POLICY_BLOCK_UNSAFE_STATE
    assert metadata["target_startup_policy_block_reason"] == "open_order_count_nonzero"
    assert metadata["target_would_submit_on_startup"] is False
    assert resolved["previous_target_exposure_krw"] is None
    assert state is None


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
            target_origin=TARGET_ORIGIN_OPERATOR_CLOSEOUT,
            adoption_reason="explicit_operator_closeout",
            created_from_signal="OPERATOR_CLOSEOUT",
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
    assert state.target_origin == TARGET_ORIGIN_OPERATOR_CLOSEOUT
    assert state.adoption_reason == "explicit_operator_closeout"


def test_target_position_state_schema_migrates_adoption_metadata_columns() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            """
            CREATE TABLE target_position_state (
                pair TEXT PRIMARY KEY,
                target_exposure_krw REAL NOT NULL,
                target_qty REAL NOT NULL,
                last_signal TEXT NOT NULL,
                last_decision_id INTEGER,
                last_reference_price REAL NOT NULL,
                updated_ts INTEGER NOT NULL
            )
            """
        )
        ensure_schema(conn)
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(target_position_state)").fetchall()
        }
        for column in {
            "target_origin",
            "adoption_reason",
            "adopted_broker_qty",
            "adopted_broker_exposure_krw",
            "created_from_signal",
        }:
            assert column in columns
        upsert_target_position_state(
            conn,
            pair="KRW-BTC",
            target_exposure_krw=57_038.976,
            target_qty=0.0004998,
            last_signal="HOLD",
            last_decision_id=None,
            last_reference_price=114_120_000.0,
            updated_ts=123456,
            target_origin=TARGET_ORIGIN_ADOPTED_EXISTING_POSITION,
            adoption_reason="safe_converged_executable_broker_position",
            adopted_broker_qty=0.0004998,
            adopted_broker_exposure_krw=57_038.976,
            created_from_signal="HOLD",
        )
        loaded = load_target_position_state(conn, pair="KRW-BTC")
    finally:
        conn.close()

    assert loaded is not None
    assert loaded.target_origin == TARGET_ORIGIN_ADOPTED_EXISTING_POSITION
    assert loaded.adoption_reason == "safe_converged_executable_broker_position"
    assert loaded.adopted_broker_qty == pytest.approx(0.0004998)
    assert loaded.created_from_signal == "HOLD"


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
    assert summary.buy_submit_plan is None
    assert summary.target_submit_plan is not None
    assert summary.target_submit_plan["source"] == "target_delta"
    assert summary.target_submit_plan["authority"] == "target_position_delta"
    assert summary.target_submit_plan["intent_type"] == "target_delta_rebalance"
    assert summary.target_submit_plan["strategy_context"] == "target_delta"
    assert summary.target_submit_plan["side"] == "SELL"
    assert summary.target_submit_plan["qty"] == pytest.approx(0.0004998)
    assert summary.target_submit_plan["submit_expected"] is True
    assert summary.target_submit_plan["block_reason"] == "none"
    assert summary.target_submit_plan["qty"] == pytest.approx(
        summary.target_shadow_decision["target_submit_qty"]
    )


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


def test_target_delta_ec2_reproduction_uses_settings_rules_when_payload_lacks_min_qty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from bithumb_bot.broker import order_rules

    old_engine = settings.EXECUTION_ENGINE
    old_target = settings.TARGET_EXPOSURE_KRW
    old_max_order = settings.MAX_ORDER_KRW
    old_min_qty = settings.LIVE_MIN_ORDER_QTY
    old_qty_step = settings.LIVE_ORDER_QTY_STEP
    old_min_notional = settings.MIN_ORDER_NOTIONAL_KRW
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", 70_000.0)
        object.__setattr__(settings, "MAX_ORDER_KRW", 70_000.0)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
        monkeypatch.setattr(
            order_rules,
            "get_effective_order_rules",
            lambda _pair: (_ for _ in ()).throw(RuntimeError("exchange unavailable")),
        )

        summary = build_execution_decision_summary(
            decision_context={"raw_signal": "BUY", "market_price": 113_428_000.0},
            readiness_payload=_readiness(broker_qty=0.0),
            raw_signal="BUY",
            final_signal="BUY",
            previous_target_exposure_krw=0.0,
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", old_target)
        object.__setattr__(settings, "MAX_ORDER_KRW", old_max_order)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", old_min_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", old_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", old_min_notional)

    target = summary.target_shadow_decision
    assert target is not None
    assert target["target_order_rule_min_qty"] == pytest.approx(0.0001)
    assert target["target_order_rule_min_notional_krw"] == pytest.approx(5000.0)
    assert target["target_delta_side"] == "BUY"
    assert target["target_submit_qty"] == pytest.approx(70_000.0 / 113_428_000.0)
    assert target["target_would_submit"] is True
    assert target["target_block_reason"] == "none"
    assert target["order_rule_authority_source"] == "settings"
    assert summary.submit_expected is True
    assert summary.block_reason == "none"
    assert summary.target_submit_plan is not None
    assert summary.target_submit_plan["submit_expected"] is True
    assert summary.target_submit_plan["target_order_rule_min_qty"] == pytest.approx(0.0001)


def test_target_delta_fails_closed_without_payload_effective_or_settings_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from bithumb_bot.broker import order_rules

    old_engine = settings.EXECUTION_ENGINE
    old_target = settings.TARGET_EXPOSURE_KRW
    old_min_qty = settings.LIVE_MIN_ORDER_QTY
    old_qty_step = settings.LIVE_ORDER_QTY_STEP
    old_min_notional = settings.MIN_ORDER_NOTIONAL_KRW
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", 70_000.0)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
        monkeypatch.setattr(
            order_rules,
            "get_effective_order_rules",
            lambda _pair: (_ for _ in ()).throw(RuntimeError("exchange unavailable")),
        )

        summary = build_execution_decision_summary(
            decision_context={"raw_signal": "BUY", "market_price": 113_428_000.0},
            readiness_payload=_readiness(broker_qty=0.0),
            raw_signal="BUY",
            final_signal="BUY",
            previous_target_exposure_krw=0.0,
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", old_target)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", old_min_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", old_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", old_min_notional)

    target = summary.target_shadow_decision
    assert target is not None
    assert target["target_order_rule_min_qty"] is None
    assert target["target_would_submit"] is False
    assert target["target_block_reason"] == "missing_order_rule_min_qty"
    assert target["order_rule_authority_source"] == "missing"
    assert summary.submit_expected is False
    assert summary.block_reason == "missing_order_rule_min_qty"


def test_target_delta_audit_prefers_payload_rule_source(monkeypatch: pytest.MonkeyPatch) -> None:
    from bithumb_bot.broker import order_rules

    old_engine = settings.EXECUTION_ENGINE
    old_target = settings.TARGET_EXPOSURE_KRW
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", 70_000.0)
        monkeypatch.setattr(
            order_rules,
            "get_effective_order_rules",
            lambda _pair: (_ for _ in ()).throw(AssertionError("payload rules should win")),
        )

        summary = build_execution_decision_summary(
            decision_context={"raw_signal": "BUY", "market_price": 100_000_000.0},
            readiness_payload=_readiness(broker_qty=0.0)
            | {
                "min_qty": 0.0002,
                "min_notional_krw": 10_000.0,
                "qty_step": 0.0001,
            },
            raw_signal="BUY",
            final_signal="BUY",
            previous_target_exposure_krw=0.0,
        )
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", old_target)

    target = summary.target_shadow_decision
    assert target is not None
    assert target["target_order_rule_min_qty"] == pytest.approx(0.0002)
    assert target["target_order_rule_min_notional_krw"] == pytest.approx(10_000.0)
    assert target["target_order_rule_qty_step"] == pytest.approx(0.0001)
    assert target["order_rule_authority_source"] == "payload"
    assert target["target_order_rule_min_qty_source"] == "payload"


def test_engine_run_loop_target_state_helper_preserves_restart_hold_target(tmp_path) -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_pair = settings.PAIR
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "PAIR", "KRW-BTC")
        db_path = str(tmp_path / "target_restart_hold.sqlite")
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
                updated_ts=1000,
            )
            conn.commit()

            previous_target = _load_previous_target_exposure_for_run_loop(conn)
            summary = build_execution_decision_summary(
                decision_context={"raw_signal": "HOLD", "market_price": 100_000_000.0},
                readiness_payload=_readiness(broker_qty=0.0004998) | {
                    "residual_proof_min_qty": 0.0001,
                    "residual_proof_min_notional_krw": 5000.0,
                },
                raw_signal="HOLD",
                final_signal="HOLD",
                previous_target_exposure_krw=previous_target,
            ).as_dict()

            persisted = _persist_target_position_state_for_run_loop(
                conn,
                execution_decision=summary,
                signal="HOLD",
                decision_id=8,
                updated_ts=2000,
            )
            conn.commit()
            state = load_target_position_state(conn, pair="KRW-BTC")
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "PAIR", old_pair)

    target_plan = summary["target_submit_plan"]
    assert previous_target == 0.0
    assert isinstance(target_plan, dict)
    assert target_plan["side"] == "SELL"
    assert target_plan["qty"] == pytest.approx(0.0004998)
    assert target_plan["target_dust_classification"] == "executable_delta"
    assert persisted is True
    assert state is not None
    assert state.target_exposure_krw == 0.0
    assert state.last_signal == "HOLD"
    assert state.last_decision_id == 8


def test_target_delta_hold_without_startup_policy_fails_closed(tmp_path) -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_pair = settings.PAIR
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "PAIR", "KRW-BTC")
        conn = ensure_db(str(tmp_path / "target_missing_hold.sqlite"))
        try:
            previous_target = _load_previous_target_exposure_for_run_loop(conn)
            summary = build_execution_decision_summary(
                decision_context={"raw_signal": "HOLD", "market_price": 100_000_000.0},
                readiness_payload=_readiness(broker_qty=0.0004998) | {
                    "residual_proof_min_qty": 0.0001,
                    "residual_proof_min_notional_krw": 5000.0,
                },
                raw_signal="HOLD",
                final_signal="HOLD",
                previous_target_exposure_krw=previous_target,
            ).as_dict()
            persisted = _persist_target_position_state_for_run_loop(
                conn,
                execution_decision=summary,
                signal="HOLD",
                decision_id=9,
                updated_ts=3000,
            )
            conn.commit()
            state = load_target_position_state(conn, pair="KRW-BTC")
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "PAIR", old_pair)

    target = summary["target_shadow_decision"]
    target_plan = summary["target_submit_plan"]
    assert previous_target is None
    assert isinstance(target, dict)
    assert target["target_block_reason"] == "missing_persistent_target_state"
    assert target["target_state_persistence"] == TARGET_STATE_PERSISTENCE_MISSING
    assert isinstance(target_plan, dict)
    assert target_plan["submit_expected"] is False
    assert target_plan["block_reason"] == "missing_persistent_target_state"
    assert persisted is False
    assert state is None


def test_engine_run_loop_target_state_helper_true_dust_noops_without_corruption(tmp_path) -> None:
    old_engine = settings.EXECUTION_ENGINE
    old_pair = settings.PAIR
    try:
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "PAIR", "KRW-BTC")
        conn = ensure_db(str(tmp_path / "target_true_dust.sqlite"))
        try:
            upsert_target_position_state(
                conn,
                pair="KRW-BTC",
                target_exposure_krw=0.0,
                target_qty=0.0,
                last_signal="SELL",
                last_decision_id=7,
                last_reference_price=100_000_000.0,
                updated_ts=1000,
            )
            conn.commit()
            previous_target = _load_previous_target_exposure_for_run_loop(conn)
            summary = build_execution_decision_summary(
                decision_context={"raw_signal": "HOLD", "market_price": 100_000_000.0},
                readiness_payload=_readiness(broker_qty=0.00000004) | {
                    "residual_proof_min_qty": 0.0001,
                    "residual_proof_min_notional_krw": 5000.0,
                },
                raw_signal="HOLD",
                final_signal="HOLD",
                previous_target_exposure_krw=previous_target,
            ).as_dict()
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "EXECUTION_ENGINE", old_engine)
        object.__setattr__(settings, "PAIR", old_pair)

    target_plan = summary["target_submit_plan"]
    assert isinstance(target_plan, dict)
    assert target_plan["submit_expected"] is False
    assert target_plan["side"] == "NONE"
    assert target_plan["block_reason"] == "delta_below_exchange_min"
    assert target_plan["target_dust_classification"] == "true_dust"
