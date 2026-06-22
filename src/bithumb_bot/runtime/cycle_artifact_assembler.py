from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .lifecycle_artifacts import RuntimeCycleArtifact

if TYPE_CHECKING:
    from .decision_coordinator import DecisionCycleResult
    from .execution_coordinator import ExecutionCycleResult


@dataclass(frozen=True)
class RuntimeCycleArtifactAssembler:
    """Build runtime cycle artifacts from typed stage results only."""

    runtime_dependency_manifest_hash: str | None = None

    def from_cycle_results(
        self,
        *,
        cycle_id: str,
        startup_state: str,
        decision_result: DecisionCycleResult,
        execution_result: ExecutionCycleResult | None = None,
    ) -> RuntimeCycleArtifact:
        def _coalesce_execution_pre_submit(field_name: str):
            if execution_result is not None:
                execution_value = getattr(execution_result, field_name, None)
                if execution_value:
                    return execution_value
            return getattr(decision_result, field_name)

        return RuntimeCycleArtifact(
            cycle_id=cycle_id,
            candle_ts=decision_result.candle_ts,
            startup_state=startup_state,
            strategy_decision_hash=decision_result.strategy_decision_hash,
            runtime_strategy_decision_bundle_id=decision_result.runtime_strategy_decision_bundle_id,
            runtime_strategy_decision_bundle_hash=decision_result.runtime_strategy_decision_bundle_hash,
            portfolio_allocation_decision_id=decision_result.portfolio_allocation_decision_id,
            portfolio_allocation_decision_hash=decision_result.portfolio_allocation_decision_hash,
            portfolio_target_id=decision_result.portfolio_target_id,
            portfolio_target_hash=decision_result.portfolio_target_hash,
            strategy_contribution_hash=decision_result.strategy_contribution_hash,
            execution_plan_id=decision_result.execution_plan_id,
            execution_plan_bundle_hash=decision_result.execution_plan_bundle_hash,
            execution_submit_plan_hash=decision_result.execution_submit_plan_hash,
            strategy_virtual_lifecycle_transition_hashes=decision_result.strategy_virtual_lifecycle_transition_hashes,
            strategy_risk_decision_hash=decision_result.strategy_risk_decision_hash,
            strategy_risk_policy_hash=decision_result.strategy_risk_policy_hash,
            strategy_risk_input_hash=decision_result.strategy_risk_input_hash,
            strategy_risk_evidence_hash=decision_result.strategy_risk_evidence_hash,
            strategy_risk_state_source=decision_result.strategy_risk_state_source,
            strategy_risk_status=decision_result.strategy_risk_status,
            strategy_risk_reason_code=decision_result.strategy_risk_reason_code,
            portfolio_risk_decision_hash=decision_result.portfolio_risk_decision_hash,
            portfolio_risk_policy_hash=decision_result.portfolio_risk_policy_hash,
            portfolio_risk_input_hash=decision_result.portfolio_risk_input_hash,
            portfolio_risk_evidence_hash=decision_result.portfolio_risk_evidence_hash,
            portfolio_risk_state_source=decision_result.portfolio_risk_state_source,
            portfolio_risk_status=decision_result.portfolio_risk_status,
            portfolio_risk_reason_code=decision_result.portfolio_risk_reason_code,
            pre_submit_risk_decision_hash=_coalesce_execution_pre_submit("pre_submit_risk_decision_hash"),
            pre_submit_risk_policy_hash=_coalesce_execution_pre_submit("pre_submit_risk_policy_hash"),
            pre_submit_risk_input_hash=_coalesce_execution_pre_submit("pre_submit_risk_input_hash"),
            pre_submit_risk_evidence_hash=_coalesce_execution_pre_submit("pre_submit_risk_evidence_hash"),
            pre_submit_risk_plan_hash=_coalesce_execution_pre_submit("pre_submit_risk_plan_hash"),
            pre_submit_risk_state_source=_coalesce_execution_pre_submit("pre_submit_risk_state_source"),
            pre_submit_risk_status=_coalesce_execution_pre_submit("pre_submit_risk_status"),
            pre_submit_risk_reason_code=_coalesce_execution_pre_submit("pre_submit_risk_reason_code"),
            execution_result_hash=(
                execution_result.as_dict()["decision_hash"] if execution_result is not None else None
            ),
            runtime_dependency_manifest_hash=self.runtime_dependency_manifest_hash,
            hard_gate_trace_entries=getattr(decision_result, "hard_gate_trace_entries", ()),
            failure_phase=decision_result.failure_phase,
            failure_subphase=decision_result.failure_subphase,
            failure_reason_code=decision_result.failure_reason_code,
            failure_detail=decision_result.failure_detail,
            operator_next_action=decision_result.operator_next_action,
            failure_evidence_hash=decision_result.failure_evidence_hash,
            persistence_failure_metadata=decision_result.persistence_failure_metadata or {},
            db_subphase=decision_result.db_subphase,
            sql_group=decision_result.sql_group,
            retry_count=decision_result.persistence_retry_count,
            max_retry_count=decision_result.persistence_max_retry_count,
            transaction_elapsed_ms=decision_result.transaction_elapsed_ms,
            lock_wait_elapsed_ms=decision_result.lock_wait_elapsed_ms,
            last_lock_error=str((decision_result.persistence_failure_metadata or {}).get("last_lock_error") or "")
            or None,
        )


__all__ = ["RuntimeCycleArtifactAssembler"]
