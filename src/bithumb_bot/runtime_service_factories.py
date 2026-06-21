from __future__ import annotations

from .operator_flatten_service import OperatorFlattenService
from .operator_notification_service import OperatorNotificationService
from .operator_repair_service import OperatorRepairService
from .run_loop_execution_planner import ExecutionPlanner
from .runtime_readiness import compute_runtime_readiness_snapshot
from .strategy_performance import evaluate_strategy_performance_gate
from .execution_service import build_typed_execution_decision_summary


def operator_repair_service() -> OperatorRepairService:
    return OperatorRepairService()


def operator_notification_service() -> OperatorNotificationService:
    return OperatorNotificationService()


def operator_flatten_service() -> OperatorFlattenService:
    return OperatorFlattenService()


def run_loop_execution_planner(
    *,
    target_state_resolver,
    persistence_context_builder,
    broker_provider=None,
) -> ExecutionPlanner:
    return ExecutionPlanner(
        readiness_snapshot_builder=compute_runtime_readiness_snapshot,
        performance_gate_evaluator=evaluate_strategy_performance_gate,
        summary_builder=build_typed_execution_decision_summary,
        target_state_resolver=target_state_resolver,
        persistence_context_builder=persistence_context_builder,
        broker_provider=(broker_provider or (lambda: None)),
    )
