from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .config import settings
from .compat.strategy import create_legacy_db_strategy
from .run_loop_execution_planner import ExecutionPlanner, ExecutionPlanningResult


def _live_real_order_enabled() -> bool:
    return bool(
        str(settings.MODE).strip().lower() == "live"
        and bool(settings.LIVE_REAL_ORDER_ARMED)
        and not bool(settings.LIVE_DRY_RUN)
    )


def legacy_context_planning_allowed_for_compatibility(
    *,
    signal_handoff_fn: object,
    runtime_handoff_fn: object,
) -> bool:
    """Allow dict planning only for patched paper/smoke compatibility callers."""
    if _live_real_order_enabled():
        return False
    return signal_handoff_fn is not runtime_handoff_fn


@dataclass(frozen=True)
class RunLoopCompatibilityPlanner:
    """Non-production bridge for old dict signal handoff tests and smoke callers."""

    planner_factory: Callable[[], ExecutionPlanner]
    runtime_handoff_fn: object

    def plan_legacy_context(
        self,
        conn,
        *,
        decision_context: dict[str, object],
        signal: str,
        reason: str,
        updated_ts: int,
        signal_handoff_fn: object,
    ) -> ExecutionPlanningResult:
        return self.planner_factory().plan_diagnostic_legacy_context(
            conn,
            decision_context=decision_context,
            signal=signal,
            reason=reason,
            updated_ts=updated_ts,
            allow_legacy_context_planning=legacy_context_planning_allowed_for_compatibility(
                signal_handoff_fn=signal_handoff_fn,
                runtime_handoff_fn=self.runtime_handoff_fn,
            ),
        )


@dataclass(frozen=True)
class LegacyDbDecisionCompatibilityRunner:
    """Explicit non-production runner for legacy DB-bound smoke compatibility."""

    strategy_factory: Callable[..., object] = create_legacy_db_strategy

    def decide_snapshot(
        self,
        conn,
        short_n: int,
        long_n: int,
        *,
        through_ts_ms: int | None = None,
        strategy_name: str | None = None,
    ) -> tuple[object, object] | None:
        if _live_real_order_enabled():
            raise RuntimeError("legacy_db_decision_compatibility_live_real_order_disabled")
        selected_strategy_name = str(strategy_name or settings.STRATEGY_NAME).strip().lower()
        strategy = self.strategy_factory(
            selected_strategy_name,
            short_n=short_n,
            long_n=long_n,
            pair=settings.PAIR,
            interval=settings.INTERVAL,
        )
        decision = strategy.decide(conn, through_ts_ms=through_ts_ms)
        return None if decision is None else (decision, strategy)
