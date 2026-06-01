from __future__ import annotations

from .config import settings
from .runtime.app_container import create_default_runtime_app


def run_loop() -> None:
    # Compatibility boundary: from .runtime.runner import run_loop
    create_default_runtime_app(settings).runner.run_forever()


def compute_strategy_decision_snapshot(*args, **kwargs):
    import bithumb_bot.runtime_decision_service as runtime_decision_service

    return runtime_decision_service.compute_strategy_decision_snapshot(*args, **kwargs)


def __getattr__(name: str):
    if name == "build_execution_decision_summary":
        import bithumb_bot.execution_service as execution_service

        return getattr(execution_service, name)
    raise AttributeError(name)


def prepare_strategy_decision_persistence_context(*args, **kwargs):
    import bithumb_bot.run_loop_execution_planner as run_loop_execution_planner

    return run_loop_execution_planner.prepare_strategy_decision_persistence_context(*args, **kwargs)


def resolve_typed_execution_submit_expectation(summary):
    import bithumb_bot.runtime.execution_coordinator as execution_coordinator

    return execution_coordinator.resolve_typed_execution_submit_expectation(
        summary,
        execution_engine_name=str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native"),
    )

__all__ = [
    "build_execution_decision_summary",
    "compute_strategy_decision_snapshot",
    "prepare_strategy_decision_persistence_context",
    "resolve_typed_execution_submit_expectation",
    "run_loop",
    "settings",
]
