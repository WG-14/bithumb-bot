from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..decision_equivalence import sha256_prefixed


@dataclass(frozen=True)
class ExecutionCycleResult:
    candle_ts: int
    decision_id: int | None
    planning_status: str
    submit_expected: bool
    submitted: bool
    post_trade_reconciled: bool
    mark_processed_allowed: bool
    halt_transition: Mapping[str, Any] | None = None
    input_hash: str | None = None
    evidence_hash: str | None = None
    decision_hash: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "artifact_type": "execution_cycle_result",
            "schema_version": 1,
            "candle_ts": self.candle_ts,
            "decision_id": self.decision_id,
            "planning_status": self.planning_status,
            "submit_expected": bool(self.submit_expected),
            "submitted": bool(self.submitted),
            "post_trade_reconciled": bool(self.post_trade_reconciled),
            "mark_processed_allowed": bool(self.mark_processed_allowed),
            "halt_transition": dict(self.halt_transition or {}),
            "input_hash": self.input_hash
            or sha256_prefixed({"candle_ts": self.candle_ts, "decision_id": self.decision_id}),
            "evidence_hash": self.evidence_hash
            or sha256_prefixed(
                {
                    "planning_status": self.planning_status,
                    "submit_expected": bool(self.submit_expected),
                    "submitted": bool(self.submitted),
                    "post_trade_reconciled": bool(self.post_trade_reconciled),
                }
            ),
        }
        payload["decision_hash"] = self.decision_hash or sha256_prefixed(payload)
        return payload


@dataclass(frozen=True)
class ExecutionCoordinator:
    execution_engine_name: str

    def resolve_submit_expectation(self, summary: Any) -> TypedExecutionSubmitExpectation:
        return resolve_typed_execution_submit_expectation(
            summary,
            execution_engine_name=self.execution_engine_name,
        )

    def target_delta_submit_expected(self, *, submit_expected: bool) -> bool:
        return self.execution_engine_name.strip().lower() == "target_delta" and bool(submit_expected)

    def execute_cycle(
        self,
        *,
        candle_ts: int,
        decision_id: int | None,
        execution_decision_summary: Any,
        submit_invoker: Any | None = None,
        post_trade_reconcile: Any | None = None,
        input_hash: str | None = None,
        execution_plan_bundle_hash: str | None = None,
    ) -> ExecutionCycleResult:
        if decision_id is None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=None,
                planning_status="decision_persistence_failed",
                submit_expected=False,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=False,
                input_hash=input_hash,
                decision_hash=execution_plan_bundle_hash,
            )
        if execution_decision_summary is None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="execution_summary_missing",
                submit_expected=False,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=False,
                input_hash=input_hash,
                decision_hash=execution_plan_bundle_hash,
            )
        expectation = self.resolve_submit_expectation(execution_decision_summary)
        if not expectation.submit_expected or submit_invoker is None:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="submit_blocked",
                submit_expected=bool(expectation.submit_expected),
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                input_hash=input_hash,
                decision_hash=execution_plan_bundle_hash,
            )
        try:
            submit_invoker()
        except Exception as exc:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="submit_failed",
                submit_expected=True,
                submitted=False,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                halt_transition={"reason_code": "LIVE_EXECUTION_BROKER_ERROR", "error": f"{type(exc).__name__}: {exc}"},
                input_hash=input_hash,
                decision_hash=execution_plan_bundle_hash,
            )
        try:
            if post_trade_reconcile is not None:
                post_trade_reconcile()
        except Exception as exc:
            return ExecutionCycleResult(
                candle_ts=candle_ts,
                decision_id=decision_id,
                planning_status="post_trade_reconcile_failed",
                submit_expected=True,
                submitted=True,
                post_trade_reconciled=False,
                mark_processed_allowed=True,
                halt_transition={"reason_code": "POST_TRADE_RECONCILE_FAILED", "error": f"{type(exc).__name__}: {exc}"},
                input_hash=input_hash,
                decision_hash=execution_plan_bundle_hash,
            )
        return ExecutionCycleResult(
            candle_ts=candle_ts,
            decision_id=decision_id,
            planning_status="submitted",
            submit_expected=True,
            submitted=True,
            post_trade_reconciled=post_trade_reconcile is not None,
            mark_processed_allowed=True,
            input_hash=input_hash,
            decision_hash=execution_plan_bundle_hash,
        )


__all__ = [
    "ExecutionCoordinator",
    "ExecutionCycleResult",
    "TypedExecutionSubmitExpectation",
    "resolve_typed_execution_submit_expectation",
]
@dataclass(frozen=True)
class TypedExecutionSubmitExpectation:
    submit_expected: bool
    plan_source: str | None = None
    block_reason: str | None = None


def resolve_typed_execution_submit_expectation(
    summary: Any,
    *,
    execution_engine_name: str,
) -> TypedExecutionSubmitExpectation:
    if summary is None:
        return TypedExecutionSubmitExpectation(submit_expected=False)
    engine_name = str(execution_engine_name or "lot_native").strip().lower()
    if engine_name != "target_delta":
        return TypedExecutionSubmitExpectation(submit_expected=bool(summary.submit_expected))
    target_plan = summary.typed_target_submit_plan()
    if target_plan is None:
        return TypedExecutionSubmitExpectation(
            submit_expected=False,
            block_reason="missing_typed_target_submit_plan",
        )
    return TypedExecutionSubmitExpectation(
        submit_expected=bool(target_plan.submit_expected)
        and str(target_plan.block_reason or "none") == "none",
        plan_source=target_plan.source,
        block_reason=target_plan.block_reason,
    )

