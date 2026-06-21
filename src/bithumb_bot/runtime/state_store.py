from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .halt_state_projector import HaltStateProjector
from .lifecycle_artifacts import StateTransitionResult

from .. import runtime_state
from ..db_core import ensure_db


@dataclass(frozen=True)
class RuntimeStateStore:
    snapshot_reader: Callable[[], object]
    halt_projector: HaltStateProjector | None = None

    def snapshot(self) -> object:
        return self.snapshot_reader()

    def persist(self) -> None:
        runtime_state.persist_current_state()

    def apply_transition(self, transition: StateTransitionResult) -> StateTransitionResult:
        if not transition.applied:
            return transition
        if transition.state_to == "HALTED":
            runtime_state.disable_trading_until(
                float("inf"),
                reason=str(transition.evidence.get("reason") or transition.reason_code),
                reason_code=transition.reason_code,
                halt_new_orders_blocked=True,
                unresolved=bool(transition.evidence.get("unresolved", False)),
                attempt_flatten=bool(transition.evidence.get("attempt_flatten", False)),
                halt_projection=self.project_halt_state(),
            )
        elif transition.state_to in {"READY", "DEGRADED_RECOVERY_CONTINUE"}:
            runtime_state.enable_trading()
        return transition

    def project_halt_state(self) -> dict[str, object]:
        projector = self.halt_projector or HaltStateProjector(
            db_factory=lambda: ensure_db(ensure_schema_ready=False)
        )
        state = self.snapshot()
        return projector.project_from_db(metadata_raw=getattr(state, "last_reconcile_metadata", None))

    def pause_until(self, epoch_sec: float, reason: str | None = None) -> None:
        runtime_state.disable_trading_until(epoch_sec, reason=reason)

    def enable(self) -> None:
        runtime_state.enable_trading()

    def set_resume_gate(self, *, blocked: bool, reason: str | None) -> None:
        runtime_state.set_resume_gate(blocked=blocked, reason=reason)

    def enter_halt(
        self,
        *,
        reason_code: str,
        reason: str,
        unresolved: bool,
        attempt_flatten: bool = False,
    ) -> None:
        runtime_state.disable_trading_until(
            float("inf"),
            reason=reason,
            reason_code=reason_code,
            halt_new_orders_blocked=True,
            unresolved=unresolved,
            attempt_flatten=attempt_flatten,
            halt_projection=self.project_halt_state(),
        )


def pause_trading_until(epoch_sec: float, reason: str | None = None) -> None:
    RuntimeStateStore(runtime_state.snapshot).pause_until(epoch_sec, reason=reason)


__all__ = ["RuntimeStateStore", "pause_trading_until"]
