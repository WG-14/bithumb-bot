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
    subphase: str | None = None
    metadata: dict[str, object] | None = None


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
    "decision persistence": (
        "decision_persistence_failed",
        "inspect_decision_persistence",
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
    metadata = dict(getattr(exc, "metadata", {}) or {})
    if getattr(exc, "db_subphase", None) is not None:
        metadata.setdefault("db_subphase", str(getattr(exc, "db_subphase")))
    if getattr(exc, "sql_group", None) is not None:
        metadata.setdefault("sql_group", str(getattr(exc, "sql_group")))
    exception_type = str(metadata.get("exception_type") or type(exc).__name__)
    exception_message = str(metadata.get("exception_message") or str(exc))
    subphase = metadata.get("failure_subphase") or metadata.get("db_subphase")
    if normalized_phase == "planner":
        reason_code = str(metadata.get("failure_reason_code") or reason_code)
        subphase = subphase or metadata.get("planner_subphase")
        if "database is locked" in exception_message.lower():
            reason_code = "planner_sqlite_lock"
            next_action = "inspect_execution_planner_db_read"
    contract_error = (
        reason_code == "portfolio_allocation_decision_missing_after_successful_planning"
        or "portfolio_allocation_decision_missing_after_successful_planning" in exception_message
    )
    if contract_error:
        reason_code = "portfolio_allocation_decision_missing_after_successful_planning"
        next_action = "inspect_planner_persistence_contract"
    if metadata.get("last_lock_error") or "database is locked" in exception_message.lower():
        metadata.setdefault("db_subphase", subphase or normalized_phase)
        metadata.setdefault("sql_group", metadata.get("sql_group") or normalized_phase.replace(" ", "_"))
        metadata.setdefault("retry_count", metadata.get("retry_count", 0))
        if normalized_phase == "decision persistence":
            reason_code = "decision_persistence_sqlite_lock"
            next_action = "inspect_decision_persistence_lock_contention"
    elif normalized_phase == "decision persistence" and not contract_error:
        subphase_text = str(subphase or "")
        phase_by_subphase = {
            "runtime_strategy_bundle": "bundle persistence",
            "portfolio_allocation": "allocation persistence",
            "execution_plan": "execution plan persistence",
            "target_state": "target state persistence",
            "strategy_decision": "strategy decision persistence",
        }
        mapped_phase = phase_by_subphase.get(subphase_text)
        if mapped_phase is not None:
            normalized_phase = mapped_phase
            reason_code, next_action = _PHASE_REASON[normalized_phase]
    detail = f"{type(exc).__name__}: {exc}"
    evidence = {
        "phase": normalized_phase,
        "subphase": None if subphase is None else str(subphase),
        "reason_code": reason_code,
        "exception_type": exception_type,
        "exception_message": exception_message,
        **metadata,
    }
    return DecisionCycleFailure(
        phase=normalized_phase,
        reason_code=reason_code,
        detail=detail,
        operator_next_action=next_action,
        evidence_hash=sha256_prefixed(evidence),
        subphase=None if subphase is None else str(subphase),
        metadata=metadata,
    )


__all__ = ["DecisionCycleFailure", "classify_decision_cycle_failure"]
