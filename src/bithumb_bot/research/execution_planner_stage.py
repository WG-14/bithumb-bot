from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .execution_planning import ResearchExecutionPlanBundle
from .execution_planning import _execution_plan_evidence as _default_execution_plan_evidence
from .execution_planning import _research_execution_plan_bundle as _default_research_execution_plan_bundle


@dataclass(frozen=True)
class ExecutionPlanningRequest:
    candle: Any
    event: Any
    ledger: Any
    strategy_name: str
    action: str
    decision_reason: str
    sellable_qty: float
    buy_fraction: float
    promotion_grade_policy_required: bool
    allow_execution_compatibility_fallback: bool
    policy_drives_execution: bool
    policy_decision: Any | None


@dataclass(frozen=True)
class ExecutionPlanningResult:
    plan_bundle: ResearchExecutionPlanBundle
    evidence: dict[str, object] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class DefaultExecutionPlanner:
    """Convert risk-approved strategy authority into a typed research submit plan."""

    def run(self, state: Any) -> Any:
        return state

    def plan(self, request: ExecutionPlanningRequest) -> ExecutionPlanningResult:
        action = str(request.action or "HOLD").upper()
        plan_bundle = _default_research_execution_plan_bundle(
            side=action,
            cash=float(request.ledger.cash),
            buy_fraction=float(request.buy_fraction),
            sellable_qty=float(request.sellable_qty),
            reference_price=float(request.candle.close),
            policy_decision=(
                request.policy_decision if bool(request.policy_drives_execution) else None
            ),
            candle_ts=int(request.candle.ts),
            allow_compatibility_fallback=(
                bool(request.allow_execution_compatibility_fallback)
                or not bool(request.policy_drives_execution)
            ),
            promotion_grade_required=(
                bool(request.policy_drives_execution)
                and bool(request.promotion_grade_policy_required)
                and not bool(request.allow_execution_compatibility_fallback)
            ),
            block_reason=str(request.decision_reason or ""),
        )
        if plan_bundle.submit_plan is None and request.promotion_grade_policy_required:
            raise ValueError("research_submit_plan_missing")
        evidence = _default_execution_plan_evidence(plan_bundle)
        warnings: tuple[str, ...] = ()
        if plan_bundle.submit_plan is None and not request.promotion_grade_policy_required:
            warnings = ("research_submit_plan_missing",)
        return ExecutionPlanningResult(
            plan_bundle=plan_bundle,
            evidence=dict(evidence),
            warnings=warnings,
        )

__all__ = ["DefaultExecutionPlanner", "ExecutionPlanningRequest", "ExecutionPlanningResult"]
