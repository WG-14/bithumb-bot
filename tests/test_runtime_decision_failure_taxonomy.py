from __future__ import annotations

import inspect
from types import SimpleNamespace

from bithumb_bot.runtime.cycle_artifact_assembler import RuntimeCycleArtifactAssembler
from bithumb_bot.runtime.decision_coordinator import DecisionCoordinator


class _Conn:
    def close(self) -> None:
        pass

    def commit(self) -> None:
        pass


def _typed_bundle() -> SimpleNamespace:
    decision = SimpleNamespace(
        strategy_name="unit",
        final_signal="HOLD",
        final_reason="unit",
    )
    result = SimpleNamespace(decision=decision)
    strategy_set = SimpleNamespace(
        multi_strategy_enabled=False,
        market_scope=SimpleNamespace(pair="KRW-BTC", interval="1m"),
    )
    return SimpleNamespace(
        results=[result],
        strategy_set=strategy_set,
        candle_ts=123,
        market_price=10.0,
    )


def _coordinator(**overrides: object) -> DecisionCoordinator:
    values = dict(
        db_factory=lambda: _Conn(),
        decision_gateway_factory=lambda: SimpleNamespace(decide_bundle=lambda *_args, **_kwargs: _typed_bundle()),
        planner_factory=lambda **_kwargs: SimpleNamespace(
            plan_runtime_strategy_results=lambda *_args, **_kwargs: SimpleNamespace(
                persistence_context={
                    "portfolio_allocation_decision": {"ok": True},
                    "execution_decision": {"target_shadow_decision": {}},
                    "ts": 123,
                    "last_close": 10.0,
                },
                execution_plan_batch=object(),
                summary=object(),
            )
        ),
        record_runtime_strategy_decision_bundle_fn=lambda *_args, **_kwargs: {
            "runtime_strategy_decision_bundle_id": 1,
            "runtime_strategy_decision_bundle_hash": "sha256:bundle",
        },
        record_portfolio_allocation_decision_fn=lambda *_args, **_kwargs: {
            "portfolio_allocation_decision_id": 2,
            "allocation_decision_hash": "sha256:allocation",
            "portfolio_target_id": 3,
            "portfolio_target_hash": "sha256:target",
        },
        record_execution_plan_batch_fn=lambda *_args, **_kwargs: {
            "execution_plan_batch_id": "batch",
            "execution_plan_batch_hash": "sha256:batch",
        },
        record_execution_plan_fn=lambda *_args, **_kwargs: {
            "execution_plan_id": 4,
            "execution_submit_plan_hash": "sha256:submit",
        },
        record_strategy_decision_fn=lambda *_args, **_kwargs: 5,
        target_position_state_persister=lambda *_args, **_kwargs: True,
    )
    values.update(overrides)
    return DecisionCoordinator(**values)


def test_gateway_failure_has_runtime_decision_reason_code() -> None:
    coordinator = _coordinator(
        decision_gateway_factory=lambda: SimpleNamespace(
            decide_bundle=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("gateway"))
        )
    )

    result = coordinator.decide_cycle(runtime_strategy_set=object(), candle_ts=123, updated_ts=456)

    assert result.persistence_status == "failed"
    assert result.failure_phase == "gateway"
    assert result.failure_reason_code == "runtime_decision_gateway_failed"
    assert result.failure_evidence_hash and result.failure_evidence_hash.startswith("sha256:")


def test_planner_failure_has_execution_planning_reason_code() -> None:
    coordinator = _coordinator(
        planner_factory=lambda **_kwargs: SimpleNamespace(
            plan_runtime_strategy_results=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("planner"))
        )
    )

    result = coordinator.decide_cycle(runtime_strategy_set=object(), candle_ts=123, updated_ts=456)

    assert result.persistence_status == "failed"
    assert result.failure_phase == "planner"
    assert result.failure_reason_code == "execution_planning_failed"


def test_record_execution_plan_failure_has_persistence_reason_code() -> None:
    coordinator = _coordinator(
        record_execution_plan_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db"))
    )

    result = coordinator.decide_cycle(runtime_strategy_set=object(), candle_ts=123, updated_ts=456)

    assert result.persistence_status == "failed"
    assert result.failure_phase == "execution plan persistence"
    assert result.failure_reason_code == "execution_plan_persistence_failed"


def test_failure_artifact_contains_operator_next_action() -> None:
    result = _coordinator(
        planner_factory=lambda **_kwargs: SimpleNamespace(
            plan_runtime_strategy_results=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("planner"))
        )
    ).decide_cycle(runtime_strategy_set=object(), candle_ts=123, updated_ts=456)

    artifact = RuntimeCycleArtifactAssembler().from_cycle_results(
        cycle_id="skip:decision_persistence_failed_retryable",
        startup_state="READY",
        decision_result=result,
    )

    payload = artifact.as_dict()
    assert payload["failure_reason_code"] == "execution_planning_failed"
    assert payload["operator_next_action"] == "inspect_execution_planner_inputs"
    assert payload["failure_evidence_hash"] == result.failure_evidence_hash


def test_failed_decision_result_requires_failure_reason_code() -> None:
    result = _coordinator(
        record_execution_plan_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db"))
    ).decide_cycle(runtime_strategy_set=object(), candle_ts=123, updated_ts=456)

    assert result.persistence_status == "failed"
    assert result.failure_reason_code is not None


def test_decision_coordinator_broad_exception_calls_classifier() -> None:
    source = inspect.getsource(DecisionCoordinator.decide_cycle)
    assert "classify_decision_cycle_failure(exc, phase=current_phase)" in source
