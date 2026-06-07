from __future__ import annotations

from dataclasses import dataclass

from ..decision_equivalence import sha256_prefixed


@dataclass(frozen=True)
class DecisionCycleFailure:
    phase: str
    reason_code: str
    detail: str
    operator_next_action: str
    evidence_hash: str
    persistence_status: str = "failed"


_PHASE_REASON = {
    "gateway": (
        "runtime_decision_gateway_failed",
        "inspect_runtime_data_and_strategy_contract",
    ),
    "planner": (
        "execution_planning_failed",
        "inspect_execution_planner_inputs",
    ),
    "bundle persistence": (
        "runtime_decision_bundle_persistence_failed",
        "inspect_runtime_strategy_decision_bundle_persistence",
    ),
    "allocation persistence": (
        "portfolio_allocation_persistence_failed",
        "inspect_portfolio_allocation_persistence",
    ),
    "execution plan persistence": (
        "execution_plan_persistence_failed",
        "inspect_execution_plan_persistence",
    ),
    "target state persistence": (
        "target_state_persistence_failed",
        "inspect_target_state_persistence",
    ),
    "strategy decision persistence": (
        "strategy_decision_persistence_failed",
        "inspect_strategy_decision_persistence",
    ),
}


def classify_decision_cycle_failure(exc: BaseException, phase: str) -> DecisionCycleFailure:
    normalized_phase = str(phase or "unknown").strip().lower() or "unknown"
    reason_code, next_action = _PHASE_REASON.get(
        normalized_phase,
        ("decision_cycle_failed", "inspect_decision_cycle_failure"),
    )
    detail = f"{type(exc).__name__}: {exc}"
    evidence = {
        "phase": normalized_phase,
        "reason_code": reason_code,
        "exception_type": type(exc).__name__,
        "exception_message": str(exc),
    }
    return DecisionCycleFailure(
        phase=normalized_phase,
        reason_code=reason_code,
        detail=detail,
        operator_next_action=next_action,
        evidence_hash=sha256_prefixed(evidence),
    )


__all__ = ["DecisionCycleFailure", "classify_decision_cycle_failure"]
