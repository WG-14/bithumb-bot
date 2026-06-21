from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.runtime.cycle_artifact_assembler import RuntimeCycleArtifactAssembler
from bithumb_bot.runtime.decision_coordinator import DecisionCoordinator, DecisionCycleResult
from bithumb_bot.runtime.decision_persistence import DecisionPersistenceError, DecisionPersistenceUnitOfWork


def test_locked_db_failure_artifact_contains_subphase_sql_group_and_retry_count(tmp_path, caplog) -> None:
    db_path = tmp_path / "observability.sqlite"
    conn = ensure_db(str(db_path))
    holder = sqlite3.connect(str(db_path), timeout=0.01)
    holder.execute("BEGIN IMMEDIATE")
    uow = DecisionPersistenceUnitOfWork(retry_count=0, retry_backoff_ms=0)

    with pytest.raises(DecisionPersistenceError) as excinfo:
        uow.persist(
            conn,
            typed_bundle=SimpleNamespace(
                strategy_set=SimpleNamespace(market_scope=SimpleNamespace(pair="KRW-BTC", interval="1m"))
            ),
            planning_bundle=SimpleNamespace(execution_plan_batch=object(), planning_error=None),
            context={"portfolio_allocation_decision": {}, "execution_decision": {}, "ts": 1},
            strategy_name="s",
            signal="HOLD",
            reason="ok",
            updated_ts=1,
            settings_obj=SimpleNamespace(PAIR="KRW-BTC"),
        )

    metadata = dict(excinfo.value.metadata)
    assert metadata["db_subphase"] == "begin_immediate"
    assert metadata["sql_group"] == "decision_persistence_transaction"
    assert metadata["retry_count"] == 0
    assert metadata["transaction_elapsed_ms"] >= 0
    assert "INSERT INTO" not in caplog.text

    result = DecisionCycleResult(
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
        failure_subphase="begin_immediate",
        failure_reason_code="decision_persistence_sqlite_lock",
        failure_detail=str(excinfo.value),
        persistence_failure_metadata=metadata,
        persistence_retry_count=0,
        persistence_max_retry_count=0,
        db_subphase=str(metadata["db_subphase"]),
        sql_group=str(metadata["sql_group"]),
        transaction_elapsed_ms=float(metadata["transaction_elapsed_ms"]),
        lock_wait_elapsed_ms=float(metadata["lock_wait_elapsed_ms"]),
    )
    artifact = RuntimeCycleArtifactAssembler().from_cycle_results(
        cycle_id="skip:decision_persistence_failed_retryable",
        startup_state="READY",
        decision_result=result,
    ).as_dict()

    assert artifact["db_subphase"] == "begin_immediate"
    assert artifact["sql_group"] == "decision_persistence_transaction"
    assert artifact["retry_count"] == 0
    assert artifact["transaction_elapsed_ms"] >= 0
    holder.rollback()
    holder.close()
    conn.close()


def _count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _bundle() -> SimpleNamespace:
    return SimpleNamespace(strategy_set=SimpleNamespace(market_scope=SimpleNamespace(pair="KRW-BTC", interval="1m")))


def _planning_bundle() -> SimpleNamespace:
    return SimpleNamespace(execution_plan_batch=object(), planning_error=None)


def _uow_with_late_lock(*, lock_after: str) -> DecisionPersistenceUnitOfWork:
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
        if lock_after == "runtime_strategy_bundle_insert":
            raise sqlite3.OperationalError("database is locked")
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
        if lock_after == "portfolio_allocation_insert":
            raise sqlite3.OperationalError("database is locked")
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
        if lock_after == "execution_plan_batch_insert":
            raise sqlite3.OperationalError("database is locked")
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

    def target_persister(*_args, **_kwargs):
        if lock_after == "target_state_upsert":
            raise sqlite3.OperationalError("database is locked")

    def budget_persister(*_args, **_kwargs):
        if lock_after == "budget_or_order_lock_insert":
            raise sqlite3.OperationalError("database is locked")
        return {}

    return DecisionPersistenceUnitOfWork(
        record_runtime_strategy_decision_bundle_fn=bundle_fn,
        record_portfolio_allocation_decision_fn=allocation_fn,
        record_execution_plan_batch_fn=batch_fn,
        record_execution_plan_fn=execution_fn,
        record_strategy_decision_fn=strategy_fn,
        target_state_persister=target_persister,
        budget_lock_persister=budget_persister,
        retry_count=0,
        retry_backoff_ms=0,
    )


def _persist_late_lock(conn, *, lock_after: str):
    return _uow_with_late_lock(lock_after=lock_after).persist(
        conn,
        typed_bundle=_bundle(),
        planning_bundle=_planning_bundle(),
        context={
            "ts": 1,
            "last_close": 100.0,
            "execution_decision": {},
            "portfolio_allocation_decision": {"allocation_decision_hash": "alloc-hash"},
            "target_state_update_intent": {
                "pair": "KRW-BTC",
                "target_exposure_krw": 0.0,
                "target_qty": 0.0,
                "last_signal": "HOLD",
                "last_decision_id": None,
                "last_reference_price": 100.0,
                "updated_ts": 1,
            },
            "lock_intents": [
                {
                    "lock_kind": "budget",
                    "pair": "KRW-BTC",
                    "currency": "KRW",
                    "amount": 10000.0,
                    "reason": "unit",
                    "created_ts": 1,
                    "idempotency_key": "budget",
                    "evidence": {},
                }
            ],
        },
        strategy_name="s",
        signal="HOLD",
        reason="ok",
        updated_ts=1,
        settings_obj=SimpleNamespace(PAIR="KRW-BTC"),
    )


def test_locked_db_failure_log_contains_sql_group_without_raw_sql_payload(tmp_path, caplog) -> None:
    caplog.set_level("WARNING", logger="bithumb_bot.run")
    conn = ensure_db(str(tmp_path / "late-log.sqlite"))

    with pytest.raises(DecisionPersistenceError):
        _persist_late_lock(conn, lock_after="portfolio_allocation_insert")

    assert "db_subphase=portfolio_allocation" in caplog.text
    assert "sql_group=portfolio_allocation_insert" in caplog.text
    assert "INSERT INTO" not in caplog.text
    assert _count(conn, "runtime_strategy_decision_bundle") == 0
    conn.close()


def test_decision_cycle_result_contains_transaction_elapsed_ms_on_persistence_failure() -> None:
    metadata = {
        "db_subphase": "execution_plan_batch",
        "sql_group": "execution_plan_batch_insert",
        "retry_count": 0,
        "max_retry_count": 0,
        "transaction_elapsed_ms": 1.5,
        "lock_wait_elapsed_ms": 0.0,
        "last_lock_error": "OperationalError: database is locked",
    }

    class _Uow:
        def persist(self, *_args, **_kwargs):
            raise DecisionPersistenceError("decision_persistence_sqlite_lock_exhausted", metadata)

    coordinator = DecisionCoordinator(
        db_factory=lambda: SimpleNamespace(close=lambda: None, rollback=lambda: None),
        decision_gateway_factory=lambda: SimpleNamespace(
            decide_bundle=lambda *_args, **_kwargs: SimpleNamespace(
                results=[
                    SimpleNamespace(
                        decision=SimpleNamespace(
                            strategy_name="unit",
                            final_signal="HOLD",
                            final_reason="unit",
                        )
                    )
                ],
                strategy_set=SimpleNamespace(multi_strategy_enabled=False),
                candle_ts=1,
                market_price=100.0,
            )
        ),
        planner_factory=lambda **_kwargs: SimpleNamespace(
            plan_runtime_strategy_results=lambda *_args, **_kwargs: SimpleNamespace(
                persistence_context={
                    "portfolio_allocation_decision": {},
                    "execution_decision": {},
                    "ts": 1,
                    "last_close": 100.0,
                },
                execution_plan_batch=object(),
                summary=None,
                planning_error=None,
            )
        ),
        decision_persistence_uow_factory=lambda: _Uow(),
    )

    result = coordinator.decide_cycle(runtime_strategy_set=object(), candle_ts=1, updated_ts=1)

    assert result.transaction_elapsed_ms == 1.5
    assert result.lock_wait_elapsed_ms == 0.0
    assert result.persistence_failure_metadata == metadata


def test_late_locked_db_failure_artifact_contains_precise_sql_group(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "late-artifact.sqlite"))

    with pytest.raises(DecisionPersistenceError) as excinfo:
        _persist_late_lock(conn, lock_after="target_state_upsert")

    metadata = dict(excinfo.value.metadata)
    result = DecisionCycleResult(
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
        failure_subphase=str(metadata["db_subphase"]),
        failure_reason_code="decision_persistence_sqlite_lock",
        failure_detail=str(excinfo.value),
        persistence_failure_metadata=metadata,
        persistence_retry_count=int(metadata["retry_count"]),
        persistence_max_retry_count=int(metadata["max_retry_count"]),
        db_subphase=str(metadata["db_subphase"]),
        sql_group=str(metadata["sql_group"]),
        transaction_elapsed_ms=float(metadata["transaction_elapsed_ms"]),
        lock_wait_elapsed_ms=float(metadata["lock_wait_elapsed_ms"]),
    )
    artifact = RuntimeCycleArtifactAssembler().from_cycle_results(
        cycle_id="skip:decision_persistence_failed_retryable",
        startup_state="READY",
        decision_result=result,
    ).as_dict()

    for payload in (artifact, artifact["persistence_failure_metadata"]):
        assert payload["db_subphase"] == "target_state"
        assert payload["sql_group"] == "target_state_upsert"
        assert payload["retry_count"] == 0
        assert payload["max_retry_count"] == 0
        assert payload["transaction_elapsed_ms"] >= 0
        assert payload["lock_wait_elapsed_ms"] >= 0
        assert payload["last_lock_error"]
    conn.close()
