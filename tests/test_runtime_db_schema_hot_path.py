from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from bithumb_bot.runtime import cycle_pipeline, decision_coordinator
from bithumb_bot.runtime.cycle_pipeline import RuntimeCyclePipeline
from bithumb_bot.runtime.decision_coordinator import DecisionCoordinator
from bithumb_bot.db_core import ensure_db


def test_runtime_container_uses_schema_ready_db_factory_after_startup() -> None:
    source = Path("src/bithumb_bot/runtime/app_container.py").read_text(encoding="utf-8")
    assert "startup_schema_conn = ensure_db(ensure_schema_ready=True)" in source
    assert "return ensure_db(ensure_schema_ready=False)" in source
    assert "db_factory=ensure_db" not in source


def test_ensure_schema_ready_false_still_applies_pragmas(tmp_path) -> None:
    db_path = tmp_path / "schema-ready-false.sqlite"
    conn = ensure_db(str(db_path), ensure_schema_ready=False)
    try:
        foreign_keys = int(conn.execute("PRAGMA foreign_keys").fetchone()[0])
        busy_timeout = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
    finally:
        conn.close()

    assert foreign_keys == 1
    assert busy_timeout >= 0


class _PreflightProvider:
    def __init__(self, **_kwargs):
        pass

    def evaluate(self, **_kwargs):
        return SimpleNamespace(
            reason_code="ok",
            latest_candle_ts=1,
            latest_close=100.0,
            closed_candle_allowed=True,
            closed_candle_ts=1,
            runtime_data_availability_report_hash=None,
            as_dict=lambda: {"decision_hash": "sha256:preflight"},
        )


class _SafetyUsesDbFactory:
    def evaluate_market_runtime(self, **_kwargs):
        return SimpleNamespace(blocked=False, last_market_runtime_check_at=None)

    def evaluate_runtime_safety(self, **kwargs):
        conn = kwargs["db_factory"]()
        conn.close()
        return SimpleNamespace(blocked=False, last_open_order_reconcile_at=None)


def _successful_decision_result():
    return SimpleNamespace(
        candle_ts=1,
        strategy_name="s",
        signal="HOLD",
        reason="ok",
        decision_id=1,
        decision_context={},
        execution_decision_summary=None,
        execution_plan_bundle=None,
        strategy_decision_hash="sha256:decision",
        execution_plan_bundle_hash=None,
        runtime_strategy_decision_bundle_id=None,
        runtime_strategy_decision_bundle_hash=None,
        portfolio_allocation_decision_id=None,
        portfolio_allocation_decision_hash=None,
        portfolio_target_id=None,
        portfolio_target_hash=None,
        strategy_contribution_hash=None,
        execution_plan_id=None,
        execution_plan_batch_hash=None,
        execution_plan_batch_id=None,
        execution_submit_plan_hash=None,
        strategy_virtual_lifecycle_transition_hashes=(),
        strategy_risk_decision_hash=None,
        strategy_risk_policy_hash=None,
        strategy_risk_input_hash=None,
        strategy_risk_evidence_hash=None,
        strategy_risk_state_source=None,
        strategy_risk_status=None,
        strategy_risk_reason_code=None,
        portfolio_risk_decision_hash=None,
        portfolio_risk_policy_hash=None,
        portfolio_risk_input_hash=None,
        portfolio_risk_evidence_hash=None,
        portfolio_risk_state_source=None,
        portfolio_risk_status=None,
        portfolio_risk_reason_code=None,
        pre_submit_risk_decision_hash=None,
        pre_submit_risk_policy_hash=None,
        pre_submit_risk_input_hash=None,
        pre_submit_risk_evidence_hash=None,
        pre_submit_risk_plan_hash=None,
        pre_submit_risk_state_source=None,
        pre_submit_risk_status=None,
        pre_submit_risk_reason_code=None,
        persistence_status="persisted",
        market_price=100.0,
        exit_rule_name=None,
        failure_phase=None,
        failure_subphase=None,
        failure_reason_code=None,
        failure_detail=None,
        operator_next_action=None,
        failure_evidence_hash=None,
        persistence_failure_metadata=None,
        persistence_retry_count=None,
        persistence_max_retry_count=None,
        db_subphase=None,
        sql_group=None,
        transaction_elapsed_ms=None,
        lock_wait_elapsed_ms=None,
        as_dict=lambda: {"decision_hash": "sha256:decision-cycle"},
    )


def test_run_loop_cycle_db_factory_does_not_call_ensure_schema_after_startup(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "runtime-hot-path.sqlite"
    startup = ensure_db(str(db_path), ensure_schema_ready=True)
    startup.close()
    calls = {"ensure_schema": 0}

    import bithumb_bot.db_core as db_core

    original_ensure_schema = db_core.ensure_schema

    def counted_ensure_schema(conn):
        calls["ensure_schema"] += 1
        return original_ensure_schema(conn)

    monkeypatch.setattr(db_core, "ensure_schema", counted_ensure_schema)
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    execution = SimpleNamespace(called=False)

    def execute_cycle(**_kwargs):
        execution.called = True
        return SimpleNamespace(
            mark_processed_allowed=False,
            submitted=False,
            trade=None,
            halt_transition=None,
            as_dict=lambda: {"decision_hash": "sha256:execution"},
        )

    container = SimpleNamespace(
        interval_parser=lambda _interval: 60,
        settings_obj=SimpleNamespace(INTERVAL="1m", PAIR="KRW-BTC", MODE="paper"),
        clock=lambda: 10.0,
        notification_adapter=SimpleNamespace(send_event=lambda *_args, **_kwargs: None),
        safety_controller=_SafetyUsesDbFactory(),
        validate_market_runtime=lambda **_kwargs: None,
        market_validation_error_type=RuntimeError,
        portfolio_cash_qty_with_position_state=lambda *_args, **_kwargs: None,
        db_factory=lambda: ensure_db(str(db_path), ensure_schema_ready=False),
        open_order_snapshot=lambda *_args, **_kwargs: None,
        mark_open_orders_recovery_required=lambda *_args, **_kwargs: None,
        reconcile_with_broker=lambda *_args, **_kwargs: None,
        decision_coordinator=SimpleNamespace(decide_cycle=lambda **_kwargs: _successful_decision_result()),
        execution_coordinator=SimpleNamespace(execute_cycle=execute_cycle),
        runtime_dependency_manifest_hash=None,
    )
    runner = SimpleNamespace(
        container=container,
        runtime_checkpoint=SimpleNamespace(apply=lambda **_kwargs: None),
        runtime_events=SimpleNamespace(event=lambda name, **fields: {"event_hash": f"sha256:{name}", **fields}),
        runtime_strategy_set=object(),
        broker=None,
        execution_service=None,
        fail_count=0,
        max_fails=3,
        last_market_runtime_check_at=None,
        last_open_order_reconcile_at=None,
        decision_persistence_failure_count=0,
    )

    RuntimeCyclePipeline(runner).run_once()

    assert calls["ensure_schema"] == 0
    assert execution.called is True


def test_decision_coordinator_runtime_db_factory_uses_schema_ready_connection(monkeypatch) -> None:
    calls: list[bool] = []

    class _Conn:
        def close(self):
            pass

        def rollback(self):
            pass

    def fake_ensure_db(*, ensure_schema_ready=True):
        calls.append(bool(ensure_schema_ready))
        return _Conn()

    monkeypatch.setattr(decision_coordinator, "ensure_db", fake_ensure_db)
    coordinator = DecisionCoordinator(
        decision_gateway_factory=lambda: SimpleNamespace(decide_bundle=lambda *_args, **_kwargs: None)
    )

    result = coordinator.decide_cycle(runtime_strategy_set=object(), candle_ts=1, updated_ts=1)

    assert result.persistence_status == "insufficient_signal_history"
    assert calls == [False]
