from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from bithumb_bot.db_core import ensure_db, record_portfolio_allocation_decision
from bithumb_bot.config import settings
from bithumb_bot.execution_service import ExecutionSubmitPlan
from bithumb_bot.run_loop_execution_planner import ExecutionPlanner
from bithumb_bot.runtime import cycle_pipeline
from bithumb_bot.runtime.cycle_pipeline import RuntimeCyclePipeline
from bithumb_bot.runtime.decision_coordinator import DecisionCoordinator
from bithumb_bot.runtime.decision_persistence import DecisionPersistenceUnitOfWork
from bithumb_bot.runtime_strategy_set import RuntimeStrategySet, RuntimeStrategySpec
from bithumb_bot.strategy_policy_contract import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2


STRATEGY_NAME = "canary_non_sma"


def _count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _bundle() -> SimpleNamespace:
    return SimpleNamespace(strategy_set=SimpleNamespace(market_scope=SimpleNamespace(pair="KRW-BTC", interval="1m")))


def _planning_bundle() -> SimpleNamespace:
    return SimpleNamespace(execution_plan_batch=object(), planning_error=None)


def _context() -> dict[str, object]:
    return {
        "ts": 1,
        "last_close": 100.0,
        "execution_decision": {},
        "portfolio_allocation_decision": {"allocation_decision_hash": "alloc-hash"},
    }


def _smoke_uow(**overrides) -> DecisionPersistenceUnitOfWork:
    def bundle_fn(conn, **_kwargs):
        conn.execute(
            """
            INSERT INTO runtime_strategy_decision_bundle(
                candle_ts, pair, interval, strategy_set_manifest_hash,
                bundle_hash, result_count, created_ts
            )
            VALUES (1, 'KRW-BTC', '1m', 'manifest', 'bundle-hash', 1, 1)
            """
        )
        return {
            "runtime_strategy_decision_bundle_id": 1,
            "runtime_strategy_decision_bundle_hash": "bundle-hash",
            "runtime_strategy_set_manifest_hash": "manifest",
        }

    def allocation_fn(conn, **_kwargs):
        conn.execute(
            """
            INSERT INTO portfolio_allocation_decision(
                bundle_id, allocation_decision_hash, allocation_input_hash,
                allocator_config_hash, strategy_contribution_hash, authoritative,
                primary_block_reason, reason, conflict_resolution_json,
                allocation_decision_json
            )
            VALUES (1, 'alloc-hash', 'input', 'config', 'contrib', 1, '', 'ok', '{}', '{}')
            """
        )
        return {
            "portfolio_allocation_decision_id": 1,
            "allocation_decision_hash": "alloc-hash",
            "portfolio_target_id": None,
            "portfolio_target_hash": "",
        }

    def batch_fn(conn, **_kwargs):
        conn.execute(
            """
            INSERT INTO execution_plan_batch(
                batch_hash, batch_id, runtime_strategy_set_manifest_hash,
                allocation_decision_hash, budget_lock_hash, status, batch_json, created_ts
            )
            VALUES ('batch-hash', 'batch-id', 'manifest', 'alloc-hash', 'lock', 'ALLOW', '{}', 1)
            """
        )
        return {"execution_plan_batch_hash": "batch-hash", "execution_plan_batch_id": "batch-id"}

    def execution_fn(conn, **_kwargs):
        conn.execute(
            """
            INSERT INTO execution_plan(
                allocation_id, portfolio_target_hash, execution_plan_bundle_hash,
                execution_submit_plan_hash, submit_expected, final_action,
                block_reason, status, execution_plan_bundle_json
            )
            VALUES (1, '', 'plan-hash', 'submit-hash', 0, 'HOLD', '', 'NOT_REQUIRED', '{}')
            """
        )
        return {
            "execution_plan_id": 1,
            "execution_plan_bundle_hash": "plan-hash",
            "execution_submit_plan_hash": "submit-hash",
        }

    def strategy_fn(conn, **_kwargs):
        conn.execute(
            "INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json) VALUES (1, 's', 'HOLD', 'ok', '{}')"
        )
        return 1

    kwargs = {
        "record_runtime_strategy_decision_bundle_fn": bundle_fn,
        "record_portfolio_allocation_decision_fn": allocation_fn,
        "record_execution_plan_batch_fn": batch_fn,
        "record_execution_plan_fn": execution_fn,
        "record_strategy_decision_fn": strategy_fn,
    }
    kwargs.update(overrides)
    return DecisionPersistenceUnitOfWork(**kwargs)


def _persist(uow: DecisionPersistenceUnitOfWork, conn):
    return uow.persist(
        conn,
        typed_bundle=_bundle(),
        planning_bundle=_planning_bundle(),
        context=_context(),
        strategy_name="s",
        signal="HOLD",
        reason="ok",
        updated_ts=1,
        settings_obj=SimpleNamespace(PAIR="KRW-BTC"),
    )


def _decision(final_signal: str = "BUY") -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name=STRATEGY_NAME,
        raw_signal=final_signal,
        raw_reason=f"raw {final_signal}",
        entry_signal=final_signal,
        entry_reason=f"entry {final_signal}",
        exit_signal=final_signal,
        exit_reason=f"exit {final_signal}",
        final_signal=final_signal,
        final_reason=f"final {final_signal}",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=EntryExecutionIntent(
            side=final_signal,
            intent="enter",
            pair="KRW-BTC",
            requires_execution_sizing=True,
            budget_fraction_of_cash=1.0,
            max_budget_krw=10000.0,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"final_signal": final_signal},
        policy_hash="sha256:policy",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash=f"sha256:decision-{final_signal}",
    )


@dataclass(frozen=True)
class _RuntimeResult:
    decision: StrategyDecisionV2
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    policy_hashes: object | None
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]

    def as_legacy_dict(self) -> dict[str, object]:
        return {
            **self.base_context,
            "strategy": self.decision.strategy_name,
            "signal": self.decision.final_signal,
            "reason": self.decision.final_reason,
            "ts": int(self.candle_ts),
            "last_close": float(self.market_price),
        }


def _runtime_bundle() -> SimpleNamespace:
    spec = RuntimeStrategySpec(
        STRATEGY_NAME,
        strategy_instance_id=STRATEGY_NAME,
        pair="KRW-BTC",
        interval="1m",
        desired_exposure_krw=10000.0,
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "live_like_smoke",
        },
        parameter_source="runtime_strategy_spec",
    )
    strategy_set = RuntimeStrategySet(source="unit", strategies=(spec,))
    decision = _decision("BUY")
    base_context = {
        "strategy": STRATEGY_NAME,
        "signal": "BUY",
        "reason": decision.final_reason,
        "strategy_instance_id": STRATEGY_NAME,
        "runtime_decision_request_hash": "sha256:request",
        "strategy_parameters_hash": "sha256:params",
        "approved_profile_hash": None,
        "runtime_contract_hash": "sha256:runtime-contract",
        "plugin_contract_hash": "sha256:plugin-contract",
        "scope_key_hash": "sha256:scope",
        "through_ts_ms": 1,
    }
    result = _RuntimeResult(
        decision=decision,
        base_context=base_context,
        candle_ts=1,
        market_price=100_000_000.0,
        policy_hashes={
            "policy_contract_hash": decision.policy_contract_hash,
            "policy_input_hash": decision.policy_input_hash,
            "policy_decision_hash": decision.policy_decision_hash,
        },
        replay_fingerprint={"runtime_decision_request_hash": "sha256:request"},
        boundary={"phase": "unit"},
    )
    return SimpleNamespace(
        strategy_set=strategy_set,
        results=(result,),
        candle_ts=1,
        market_price=100_000_000.0,
        as_dict=lambda: {"schema_version": 1, "results": [result.as_legacy_dict()]},
        content_hash=lambda: "sha256:runtime-bundle",
    )


class _Readiness:
    def as_dict(self) -> dict[str, object]:
        return {
            "residual_inventory_qty": 0.0,
            "residual_inventory_notional_krw": 0.0,
            "residual_inventory_state": "flat",
            "residual_inventory_policy_allows_buy": True,
            "residual_inventory_policy_allows_sell": False,
            "residual_inventory_policy_allows_run": True,
            "cash_available": 1_000_000.0,
            "broker_position_evidence": {
                "broker_qty_known": True,
                "broker_qty": 0.0,
                "balance_source_stale": False,
            },
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "broker_portfolio_converged": True,
            "open_order_count": 0,
            "accounting_projection_ok": True,
            "active_fee_accounting_blocker": False,
            "residual_proof_min_qty": 0.0001,
            "residual_proof_min_notional_krw": 5000.0,
        }


class _Summary:
    def __init__(self) -> None:
        self._submit_plan = ExecutionSubmitPlan(
            side="BUY",
            source="test",
            authority="test",
            final_action="BUY",
            qty=0.001,
            notional_krw=10000.0,
            target_exposure_krw=10000.0,
            current_effective_exposure_krw=0.0,
            delta_krw=10000.0,
            submit_expected=True,
            pre_submit_proof_status="not_required",
            block_reason="",
            idempotency_key="live-like-buy",
            pair="KRW-BTC",
        )
        self.submit_expected = True
        self.block_reason = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "final_action": "BUY",
            "submit_expected": True,
            "pre_submit_proof_status": "not_required",
            "block_reason": "",
            "target_submit_plan": {
                **self._submit_plan.as_dict(),
                "submit_plan_hash": self._submit_plan.content_hash(),
            },
        }

    def typed_target_submit_plan(self):
        return self._submit_plan

    def typed_residual_submit_plan(self):
        return None

    def typed_buy_submit_plan(self):
        return None


def _settings() -> SimpleNamespace:
    return replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        MODE="paper",
        EXECUTION_ENGINE="target_delta",
        MAX_ORDER_KRW=10000.0,
        TARGET_EXPOSURE_KRW=10000.0,
        LIVE_REAL_ORDER_ARMED=False,
        LIVE_DRY_RUN=True,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        MAX_OPEN_ORDER_AGE_SEC=300,
        DECISION_PERSISTENCE_FAILURE_HALT_THRESHOLD=3,
    )


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


def _runner(db_path, *, coordinator: DecisionCoordinator):
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

    settings_obj = _settings()
    container = SimpleNamespace(
        interval_parser=lambda _interval: 60,
        settings_obj=settings_obj,
        clock=lambda: 10.0,
        notification_adapter=SimpleNamespace(send_event=lambda *_args, **_kwargs: None),
        safety_controller=_Safety(),
        validate_market_runtime=lambda **_kwargs: None,
        market_validation_error_type=RuntimeError,
        portfolio_cash_qty_with_position_state=lambda *_args, **_kwargs: None,
        db_factory=lambda: ensure_db(str(db_path), ensure_schema_ready=False),
        open_order_snapshot=lambda *_args, **_kwargs: None,
        mark_open_orders_recovery_required=lambda *_args, **_kwargs: None,
        reconcile_with_broker=lambda *_args, **_kwargs: None,
        decision_coordinator=coordinator,
        execution_coordinator=SimpleNamespace(execute_cycle=execute_cycle),
        runtime_dependency_manifest_hash=None,
    )
    return SimpleNamespace(
        container=container,
        runtime_checkpoint=SimpleNamespace(apply=lambda **_kwargs: None),
        runtime_events=SimpleNamespace(event=lambda name, **fields: {"event_hash": f"sha256:{name}", **fields}),
        runtime_strategy_set=_runtime_bundle().strategy_set,
        broker=SimpleNamespace(submit_order=lambda **_kwargs: None),
        execution_service=None,
        fail_count=0,
        max_fails=3,
        last_market_runtime_check_at=None,
        last_open_order_reconcile_at=None,
        decision_persistence_failure_count=0,
        execution=execution,
    )


def _coordinator(db_path, **overrides) -> DecisionCoordinator:
    settings_obj = _settings()
    values = {
        "settings_obj": settings_obj,
        "db_factory": lambda: ensure_db(str(db_path), ensure_schema_ready=False),
        "decision_gateway_factory": lambda: SimpleNamespace(decide_bundle=lambda *_args, **_kwargs: _runtime_bundle()),
        "planner_factory": lambda **_kwargs: ExecutionPlanner(
            settings_obj=settings_obj,
            readiness_snapshot_builder=lambda _conn: _Readiness(),
            summary_builder=lambda **_summary_kwargs: _Summary(),
        ),
        "decision_persistence_uow_factory": DecisionPersistenceUnitOfWork,
    }
    values.update(overrides)
    return DecisionCoordinator(**values)


def test_live_like_cycle_persists_decision_bundle_allocation_execution_plan_and_hands_off_to_execution(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    db_path = tmp_path / "live-like-success.sqlite"
    startup = ensure_db(str(db_path))
    startup.close()
    runner = _runner(db_path, coordinator=_coordinator(db_path))

    artifact = RuntimeCyclePipeline(runner).run_once()

    conn = ensure_db(str(db_path), ensure_schema_ready=False)
    assert _count(conn, "runtime_strategy_decision_bundle") == 1
    assert _count(conn, "portfolio_allocation_decision") == 1
    assert _count(conn, "execution_plan") == 1
    assert _count(conn, "strategy_decisions") == 1
    assert artifact.cycle_id == "checkpoint:processed"
    assert runner.execution.called is True
    conn.close()


def test_live_like_cycle_persistence_failure_does_not_call_execution_and_leaves_no_partial_rows(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    db_path = tmp_path / "live-like-failure.sqlite"
    startup = ensure_db(str(db_path))
    startup.close()
    coordinator = _coordinator(
        db_path,
        record_execution_plan_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    runner = _runner(db_path, coordinator=coordinator)

    artifact = RuntimeCyclePipeline(runner).run_once()

    conn = ensure_db(str(db_path), ensure_schema_ready=False)
    assert artifact.cycle_id == "skip:decision_persistence_failed_retryable"
    assert runner.execution.called is False
    assert _count(conn, "runtime_strategy_decision_bundle") == 0
    assert _count(conn, "portfolio_allocation_decision") == 0
    assert _count(conn, "execution_plan") == 0
    assert _count(conn, "strategy_decisions") == 0
    conn.close()


def test_live_like_cycle_locked_db_reports_planner_or_persistence_subphase(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cycle_pipeline, "RuntimeDataCyclePreflightProvider", _PreflightProvider)
    db_path = tmp_path / "live-like-lock.sqlite"
    startup = ensure_db(str(db_path))
    startup.close()

    def allocation_then_lock(conn, **kwargs):
        refs = record_portfolio_allocation_decision(conn, **kwargs)
        raise sqlite3.OperationalError("database is locked")

    coordinator = _coordinator(
        db_path,
        record_portfolio_allocation_decision_fn=allocation_then_lock,
        decision_persistence_uow_factory=lambda: DecisionPersistenceUnitOfWork(
            retry_count=0,
            retry_backoff_ms=0,
        ),
    )
    runner = _runner(db_path, coordinator=coordinator)

    artifact = RuntimeCyclePipeline(runner).run_once()
    payload = artifact.as_dict()

    assert artifact.cycle_id == "skip:decision_persistence_failed_retryable"
    assert runner.execution.called is False
    assert payload["failure_subphase"] or payload["db_subphase"]
    assert payload["retry_count"] == 0
    assert payload["db_subphase"] == "portfolio_allocation"
