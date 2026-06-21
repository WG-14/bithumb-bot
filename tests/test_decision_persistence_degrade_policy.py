from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.db_core import ensure_db
from bithumb_bot.runtime import cycle_pipeline
from bithumb_bot.runtime.cycle_pipeline import RuntimeCyclePipeline
from bithumb_bot.runtime.decision_coordinator import DecisionCycleResult
from bithumb_bot.runtime.lifecycle_artifacts import RuntimeCycleArtifact
import bithumb_bot.db_core as db_core


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


class _Safety:
    def evaluate_market_runtime(self, **_kwargs):
        return SimpleNamespace(blocked=False, last_market_runtime_check_at=None)

    def evaluate_runtime_safety(self, **_kwargs):
        return SimpleNamespace(blocked=False, last_open_order_reconcile_at=None)


def _decision_result() -> DecisionCycleResult:
    return DecisionCycleResult(
        candle_ts=1,
        strategy_name="s",
        signal="HOLD",
        reason="decision_persistence_sqlite_lock",
        decision_id=None,
        decision_context=None,
        execution_decision_summary=None,
        execution_plan_bundle=None,
        strategy_decision_hash=None,
        execution_plan_bundle_hash=None,
        persistence_status="failed",
        mark_processed_candidate=False,
        failure_phase="decision persistence",
        failure_reason_code="decision_persistence_sqlite_lock",
        db_subphase="begin_immediate",
        sql_group="decision_persistence_transaction",
        persistence_retry_count=1,
    )


def _success_decision_result() -> DecisionCycleResult:
    return DecisionCycleResult(
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
        persistence_status="persisted",
        mark_processed_candidate=True,
        market_price=100.0,
    )


def _runner(*, threshold: int = 3):
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
        settings_obj=SimpleNamespace(
            INTERVAL="1m",
            PAIR="KRW-BTC",
            MODE="paper",
            DECISION_PERSISTENCE_FAILURE_HALT_THRESHOLD=threshold,
        ),
        clock=lambda: 10.0,
        notification_adapter=SimpleNamespace(send_event=lambda *_args, **_kwargs: None),
        safety_controller=_Safety(),
        validate_market_runtime=lambda **_kwargs: None,
        market_validation_error_type=RuntimeError,
        portfolio_cash_qty_with_position_state=lambda *_args, **_kwargs: None,
        db_factory=lambda: None,
        open_order_snapshot=lambda *_args, **_kwargs: None,
        mark_open_orders_recovery_required=lambda *_args, **_kwargs: None,
        reconcile_with_broker=lambda *_args, **_kwargs: None,
        decision_coordinator=SimpleNamespace(decide_cycle=lambda **_kwargs: _decision_result()),
        execution_coordinator=SimpleNamespace(execute_cycle=execute_cycle),
        runtime_dependency_manifest_hash=None,
    )
    return SimpleNamespace(
        container=container,
        runtime_checkpoint=SimpleNamespace(),
        runtime_events=SimpleNamespace(event=lambda name, **fields: {"event_hash": f"sha256:{name}", **fields}),
        runtime_strategy_set=object(),
        broker=None,
        execution_service=None,
        fail_count=0,
        max_fails=3,
        last_market_runtime_check_at=None,
        last_open_order_reconcile_at=None,
        execution=execution,
    )


def test_single_decision_persistence_failure_records_retryable_skip_without_halt(monkeypatch) -> None:
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    halted = []
    monkeypatch.setattr(cycle_pipeline.runtime_state, "disable_trading_until", lambda *args, **kwargs: halted.append(kwargs))
    runner = _runner(threshold=3)

    artifact = RuntimeCyclePipeline(runner).run_once()

    assert artifact.cycle_id == "skip:decision_persistence_failed_retryable"
    assert halted == []
    assert runner.execution.called is False


def test_consecutive_decision_persistence_failures_enter_halt_after_threshold(monkeypatch) -> None:
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    halted = []
    monkeypatch.setattr(cycle_pipeline.runtime_state, "disable_trading_until", lambda *args, **kwargs: halted.append(kwargs))
    runner = _runner(threshold=2)

    RuntimeCyclePipeline(runner).run_once()
    artifact = RuntimeCyclePipeline(runner).run_once()

    assert artifact.cycle_id == "halt:decision_persistence_blocked"
    assert halted[-1]["reason_code"] == "DECISION_PERSISTENCE_BLOCKED"
    assert halted[-1]["unresolved"] is True
    assert runner.execution.called is False


def test_successful_cycle_resets_decision_persistence_failure_counter(monkeypatch) -> None:
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    halted = []
    monkeypatch.setattr(cycle_pipeline.runtime_state, "disable_trading_until", lambda *args, **kwargs: halted.append(kwargs))
    runner = _runner(threshold=2)

    RuntimeCyclePipeline(runner).run_once()
    assert runner.decision_persistence_failure_count == 1

    runner.container.decision_coordinator = SimpleNamespace(decide_cycle=lambda **_kwargs: _success_decision_result())
    RuntimeCyclePipeline(runner).run_once()
    assert runner.decision_persistence_failure_count == 0

    runner.container.decision_coordinator = SimpleNamespace(decide_cycle=lambda **_kwargs: _decision_result())
    artifact = RuntimeCyclePipeline(runner).run_once()

    assert artifact.cycle_id == "skip:decision_persistence_failed_retryable"
    assert halted == []


def test_halt_after_threshold_persists_bot_health_halt_state(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "halt-policy.sqlite"
    monkeypatch.setattr(db_core, "settings", SimpleNamespace(DB_PATH=str(db_path), MODE="paper"))
    conn = ensure_db(str(db_path))
    conn.close()
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    runner = _runner(threshold=3)

    RuntimeCyclePipeline(runner).run_once()
    RuntimeCyclePipeline(runner).run_once()
    artifact = RuntimeCyclePipeline(runner).run_once()

    conn = ensure_db(str(db_path), ensure_schema_ready=False)
    row = conn.execute(
        "SELECT trading_enabled, halt_reason_code, halt_state_unresolved FROM bot_health WHERE id=1"
    ).fetchone()
    conn.close()

    assert artifact.cycle_id == "halt:decision_persistence_blocked"
    assert row is not None
    assert int(row["trading_enabled"]) == 0
    assert row["halt_reason_code"] == "DECISION_PERSISTENCE_BLOCKED"
    assert int(row["halt_state_unresolved"]) == 1


def test_halt_after_persistence_failures_prevents_execution_call(monkeypatch) -> None:
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    halted = []
    monkeypatch.setattr(cycle_pipeline.runtime_state, "disable_trading_until", lambda *args, **kwargs: halted.append(kwargs))
    runner = _runner(threshold=1)

    artifact = RuntimeCyclePipeline(runner).run_once()

    assert artifact.cycle_id == "halt:decision_persistence_blocked"
    assert runner.execution.called is False
