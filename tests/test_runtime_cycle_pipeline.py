from __future__ import annotations

import inspect
from types import SimpleNamespace

from bithumb_bot.runtime.cycle_artifact_assembler import RuntimeCycleArtifactAssembler
from bithumb_bot.runtime.cycle_pipeline import RuntimeCyclePipeline
from bithumb_bot.runtime.decision_coordinator import DecisionCoordinator, DecisionCycleResult
from bithumb_bot.runtime.execution_coordinator import ExecutionCycleResult
from bithumb_bot.runtime.lifecycle_artifacts import RuntimeCycleArtifact
from bithumb_bot.runtime.runner import Runner


def _decision_result() -> DecisionCycleResult:
    return DecisionCycleResult(
        candle_ts=1,
        strategy_name="unit",
        signal="HOLD",
        reason="unit",
        decision_id=1,
        decision_context={},
        execution_decision_summary=object(),
        execution_plan_bundle=object(),
        strategy_decision_hash="sha256:strategy",
        execution_plan_bundle_hash="sha256:plan",
        persistence_status="persisted",
        mark_processed_candidate=True,
        pre_submit_risk_decision_hash="sha256:pre-decision",
        pre_submit_risk_policy_hash="sha256:pre-policy",
        pre_submit_risk_input_hash="sha256:pre-input",
        pre_submit_risk_evidence_hash="sha256:pre-evidence",
        pre_submit_risk_plan_hash="sha256:pre-plan",
    )


def test_runtime_cycle_pipeline_runs_stage_order() -> None:
    source = inspect.getsource(RuntimeCyclePipeline.run_once)
    order = [
        "RuntimeDataCyclePreflightProvider(",
        ".evaluate_market_runtime(",
        ".evaluate_runtime_safety(",
        ".decide_cycle(",
        ".execute_cycle(",
    ]
    positions = [source.index(token) for token in order]
    assert positions == sorted(positions)
    assert source.index(".execute_cycle(") < source.rindex("RuntimeCycleArtifactAssembler(")


def test_runtime_cycle_pipeline_returns_runtime_cycle_artifact() -> None:
    artifact = RuntimeCycleArtifactAssembler().from_cycle_results(
        cycle_id="checkpoint:processed",
        startup_state="READY",
        decision_result=_decision_result(),
        execution_result=ExecutionCycleResult(
            candle_ts=1,
            decision_id=1,
            planning_status="submit_blocked",
            submit_expected=False,
            submitted=False,
            post_trade_reconciled=False,
            mark_processed_allowed=True,
        ),
    )
    assert isinstance(artifact, RuntimeCycleArtifact)
    assert artifact.as_dict()["cycle_id"] == "checkpoint:processed"


def test_runner_run_one_cycle_calls_pipeline_once() -> None:
    calls: list[str] = []

    class _Pipeline:
        def run_once(self) -> RuntimeCycleArtifact:
            calls.append("run_once")
            return RuntimeCycleArtifact(cycle_id="unit", candle_ts=None)

    container = SimpleNamespace(runtime_cycle_pipeline_factory=lambda _runner: _Pipeline())
    runner = Runner(container)  # type: ignore[arg-type]
    runner._started = True

    artifact = runner.run_one_cycle()

    assert calls == ["run_once"]
    assert artifact is not None
    assert artifact.cycle_id == "unit"


def test_decision_coordinator_passes_cycle_broker_to_planner_provider() -> None:
    broker = object()
    seen: dict[str, object] = {}
    typed_bundle = SimpleNamespace(
        candle_ts=1,
        market_price=100.0,
        strategy_set=SimpleNamespace(multi_strategy_enabled=False),
        results=(
            SimpleNamespace(
                decision=SimpleNamespace(
                    strategy_name="unit",
                    final_signal="HOLD",
                    final_reason="unit",
                )
            ),
        ),
    )

    class _Gateway:
        def decide_bundle(self, *_args, **_kwargs):
            return typed_bundle

    class _Planner:
        def plan_runtime_strategy_results(self, *_args, **_kwargs):
            return SimpleNamespace(
                summary=None,
                persistence_context={},
                planning_error=None,
                execution_plan_batch=None,
                submit_plan=None,
            )

    def _planner_factory(*, broker_provider, **_kwargs):
        seen["broker"] = broker_provider()
        return _Planner()

    class _Persistence:
        def persist(self, *_args, context, **_kwargs):
            return SimpleNamespace(context=context, decision_id=1, metadata=lambda: {})

    coordinator = DecisionCoordinator(
        db_factory=lambda: SimpleNamespace(close=lambda: None),
        decision_gateway_factory=_Gateway,
        planner_factory=_planner_factory,
        decision_persistence_uow_factory=_Persistence,
    )

    result = coordinator.decide_cycle(
        runtime_strategy_set=object(),
        candle_ts=1,
        updated_ts=1,
        broker=broker,
    )

    assert seen["broker"] is broker
    assert result.persistence_status == "persisted"
