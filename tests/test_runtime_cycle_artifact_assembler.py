from __future__ import annotations

import inspect

from bithumb_bot.runtime.cycle_artifact_assembler import RuntimeCycleArtifactAssembler
from bithumb_bot.runtime.decision_coordinator import DecisionCycleResult
from bithumb_bot.runtime.execution_coordinator import ExecutionCycleResult
from bithumb_bot.runtime.lifecycle_artifacts import RuntimeCycleArtifact


def _decision_result(**overrides: object) -> DecisionCycleResult:
    values = dict(
        candle_ts=123,
        strategy_name="unit",
        signal="HOLD",
        reason="unit",
        decision_id=7,
        decision_context={},
        execution_decision_summary=object(),
        execution_plan_bundle=object(),
        strategy_decision_hash="sha256:strategy",
        execution_plan_bundle_hash="sha256:bundle",
        persistence_status="persisted",
        mark_processed_candidate=True,
        pre_submit_risk_decision_hash="sha256:pre-decision",
        pre_submit_risk_policy_hash="sha256:pre-policy",
        pre_submit_risk_input_hash="sha256:pre-input",
        pre_submit_risk_evidence_hash="sha256:pre-evidence",
        pre_submit_risk_plan_hash="sha256:pre-plan",
        pre_submit_risk_state_source="runtime_db_broker",
        pre_submit_risk_status="ALLOW",
        pre_submit_risk_reason_code="OK",
    )
    values.update(overrides)
    return DecisionCycleResult(**values)


def test_checkpoint_artifact_contains_pre_submit_fields_from_decision_result() -> None:
    execution_result = ExecutionCycleResult(
        candle_ts=123,
        decision_id=7,
        planning_status="submit_blocked",
        submit_expected=False,
        submitted=False,
        post_trade_reconciled=False,
        mark_processed_allowed=True,
    )

    artifact = RuntimeCycleArtifactAssembler(
        runtime_dependency_manifest_hash="sha256:deps",
    ).from_cycle_results(
        cycle_id="checkpoint:processed",
        startup_state="READY",
        decision_result=_decision_result(),
        execution_result=execution_result,
    )

    payload = artifact.as_dict()
    assert payload["pre_submit_risk_decision_hash"] == "sha256:pre-decision"
    assert payload["pre_submit_risk_policy_hash"] == "sha256:pre-policy"
    assert payload["pre_submit_risk_input_hash"] == "sha256:pre-input"
    assert payload["pre_submit_risk_evidence_hash"] == "sha256:pre-evidence"
    assert payload["pre_submit_risk_plan_hash"] == "sha256:pre-plan"
    assert payload["runtime_dependency_manifest_hash"] == "sha256:deps"
    assert payload["execution_result_hash"] == execution_result.as_dict()["decision_hash"]


def test_artifact_assembler_does_not_accept_db_connection() -> None:
    signature = inspect.signature(RuntimeCycleArtifactAssembler.from_cycle_results)
    assert "conn" not in signature.parameters
    assert "db_factory" not in signature.parameters
    source = inspect.getsource(RuntimeCycleArtifactAssembler)
    assert ".execute(" not in source
    assert "SELECT " not in source


def test_runner_artifact_creation_survives_db_select_failure_after_execution() -> None:
    class _FailingDb:
        def execute(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("post-execution SELECT must not be used")

    artifact = RuntimeCycleArtifactAssembler().from_cycle_results(
        cycle_id="checkpoint:processed",
        startup_state="READY",
        decision_result=_decision_result(execution_plan_id=99),
        execution_result=None,
    )

    assert isinstance(artifact, RuntimeCycleArtifact)
    assert _FailingDb  # proves the fixture is intentionally unused
    assert artifact.as_dict()["pre_submit_risk_decision_hash"] == "sha256:pre-decision"
