from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from . import runtime_state
from .operator_repair_service import OperatorRepairService
from .runtime_recovery_gate import ResumeBlocker, RuntimeRecoveryGateService
from .runtime_recovery_services import StartupSafetyGateService
from .runtime_resume_services import (
    RestartReadinessService,
    RuntimeResumeService,
    default_ledger_external_cash_adjustment_summary,
    reconcile_balance_split_mismatch_count,
)


@dataclass(frozen=True)
class RuntimeGateApi:
    initial_reconcile_halt_evaluator: Callable[..., object]
    live_execution_broker_halt_evaluator: Callable[..., object]
    risk_state_mismatch_halt_evaluator: Callable[..., object]
    exposure_snapshot: Callable[[int], tuple[bool, bool]]
    emergency_flatten_blocker: Callable[[], str | None] = runtime_state.get_emergency_flatten_blocker
    logger: logging.Logger = logging.getLogger("bithumb_bot.run")

    def startup_safety_gate(self) -> str | None:
        return StartupSafetyGateService(
            state_snapshot=runtime_state.snapshot,
            refresh_open_order_health=runtime_state.refresh_open_order_health,
            emergency_flatten_blocker=self.emergency_flatten_blocker,
            set_startup_gate_reason=runtime_state.set_startup_gate_reason,
            balance_split_mismatch_counter=reconcile_balance_split_mismatch_count,
            logger=self.logger,
        ).evaluate()

    def recovery_gate_service(self) -> RuntimeRecoveryGateService:
        return RuntimeRecoveryGateService(
            startup_gate_evaluator=self.startup_safety_gate,
            initial_reconcile_halt_evaluator=self.initial_reconcile_halt_evaluator,
            live_execution_broker_halt_evaluator=self.live_execution_broker_halt_evaluator,
            risk_state_mismatch_halt_evaluator=self.risk_state_mismatch_halt_evaluator,
            state_snapshot=runtime_state.snapshot,
        )

    def resume_eligibility(self) -> tuple[bool, list[ResumeBlocker]]:
        return RuntimeResumeService(
            recovery_gate_factory=self.recovery_gate_service,
            exposure_snapshot=self.exposure_snapshot,
            ledger_external_cash_adjustment_summary=default_ledger_external_cash_adjustment_summary,
            repair_service=OperatorRepairService(),
        ).evaluate_resume_eligibility()

    def restart_readiness(self) -> list[tuple[str, bool, str]]:
        return RestartReadinessService(
            resume_evaluator=self.resume_eligibility,
            repair_service=OperatorRepairService(),
        ).evaluate_restart_readiness()
