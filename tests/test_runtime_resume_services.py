from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.operator_flatten_service import OperatorFlattenService
from bithumb_bot.operator_notification_service import OperatorNotificationService
from bithumb_bot.operator_repair_service import OperatorRepairService
from bithumb_bot.runtime_recovery_gate import RuntimeRecoveryGateService, resume_blocker
from bithumb_bot.runtime_resume_services import (
    RestartReadinessService,
    RuntimeResumeService,
)


@dataclass
class _State:
    last_reconcile_status: str | None = None
    last_reconcile_reason_code: str | None = None
    last_reconcile_error: str | None = None
    last_reconcile_metadata: str | None = None
    halt_new_orders_blocked: bool = False
    halt_state_unresolved: bool = False
    halt_reason_code: str | None = None
    last_disable_reason: str | None = None
    unresolved_open_order_count: int = 0
    recovery_required_count: int = 0
    emergency_flatten_blocked: bool = False
    emergency_flatten_block_reason: str | None = None
    last_flatten_position_status: str | None = None
    halt_open_orders_present: bool = False
    halt_position_present: bool = False


class _Conn:
    def __init__(self, *, open_count: int = 0, recovery_required_count: int = 0) -> None:
        self.open_count = open_count
        self.recovery_required_count = recovery_required_count
        self.closed = False

    def execute(self, sql: str, *_args: object) -> "_Cursor":
        if "RECOVERY_REQUIRED" in sql:
            return _Cursor({"recovery_required_count": self.recovery_required_count})
        return _Cursor({"open_count": self.open_count})

    def close(self) -> None:
        self.closed = True


class _Cursor:
    def __init__(self, row: dict[str, int]) -> None:
        self.row = row

    def fetchone(self) -> dict[str, int]:
        return self.row


class _NormalizedExposure:
    terminal_state = "flat"
    has_dust_only_remainder = False
    authority_gap_reason = None
    raw_qty_open = 0.0
    has_any_position_residue = False
    has_executable_exposure = False


class _PositionState:
    normalized_exposure = _NormalizedExposure()


class _Readiness:
    position_state = _PositionState()
    reconcile_metadata = None
    resume_ready = True
    recovery_stage = "READY"


def _clear_recovery_gate(state: _State) -> RuntimeRecoveryGateService:
    return RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: None,
        stale_initial_reconcile_halt_clearer=lambda: False,
        stale_live_execution_broker_halt_clearer=lambda **_kwargs: False,
        stale_risk_state_mismatch_halt_clearer=lambda **_kwargs: False,
        state_snapshot=lambda: state,
    )


def test_resume_service_blocks_non_overridable_last_reconcile_failure() -> None:
    state = _State(
        last_reconcile_status="error",
        last_reconcile_reason_code="RECONCILE_FAILED",
        last_reconcile_error="broker timeout",
    )
    gates: list[tuple[bool, str | None]] = []
    service = RuntimeResumeService(
        recovery_gate_factory=lambda: _clear_recovery_gate(state),
        state_snapshot=lambda: state,
        set_resume_gate=lambda *, blocked, reason: gates.append((blocked, reason)),
        db_factory=lambda: _Conn(),
        repair_service=OperatorRepairService(
            manual_flat_preview_builder=lambda _conn: {"safe_to_apply": False},
            fee_gap_preview_builder=lambda _conn: {"needs_repair": False},
        ),
    )

    allowed, blockers = service.evaluate_resume_eligibility()

    assert allowed is False
    assert [blocker.code for blocker in blockers] == ["LAST_RECONCILE_FAILED"]
    assert blockers[0].overridable is False
    assert blockers[0].reason_code == "LAST_RECONCILE_FAILED"
    assert gates[-1][0] is True
    assert "LAST_RECONCILE_FAILED" in str(gates[-1][1])


def test_resume_service_allows_clear_state() -> None:
    state = _State(last_reconcile_status="ok")
    gates: list[tuple[bool, str | None]] = []
    service = RuntimeResumeService(
        recovery_gate_factory=lambda: _clear_recovery_gate(state),
        state_snapshot=lambda: state,
        set_resume_gate=lambda *, blocked, reason: gates.append((blocked, reason)),
        db_factory=lambda: _Conn(),
        repair_service=OperatorRepairService(
            manual_flat_preview_builder=lambda _conn: {"safe_to_apply": False},
            fee_gap_preview_builder=lambda _conn: {"needs_repair": False},
        ),
    )

    allowed, blockers = service.evaluate_resume_eligibility()

    assert allowed is True
    assert blockers == []
    assert gates == [(False, None)]


def test_restart_readiness_service_returns_clear_check_tuples() -> None:
    state = _State(last_reconcile_status="ok")
    service = RestartReadinessService(
        resume_evaluator=lambda: (True, []),
        state_snapshot=lambda: state,
        db_factory=lambda: _Conn(open_count=0, recovery_required_count=0),
        readiness_snapshot_builder=lambda _conn: _Readiness(),
        repair_service=OperatorRepairService(
            manual_flat_preview_builder=lambda _conn: {"safe_to_apply": False, "eligibility_reason": "none"},
            fee_gap_preview_builder=lambda _conn: {"needs_repair": False, "resume_blocking": True},
        ),
    )

    checks = service.evaluate_restart_readiness()

    assert checks[0] == ("unresolved/recovery-required orders", True, "unresolved=0 recovery_required=0")
    assert checks[1] == ("open orders", True, "open_orders=0")
    assert checks[2] == (
        "normalized position state",
        True,
        "terminal_state=flat has_executable_exposure=0 has_dust_only_remainder=0 dust_resume_allowed=0 recovery_stage=READY",
    )
    assert checks[3] == ("halt state", True, "halt_blocked=0 halt_unresolved=0 detail=none")


def test_restart_readiness_service_reports_blocked_orders_and_halt() -> None:
    state = _State(
        last_reconcile_status="ok",
        halt_new_orders_blocked=True,
        halt_state_unresolved=True,
        last_disable_reason="operator review required",
    )
    blocker = resume_blocker(
        code="HALT_STATE_UNRESOLVED",
        detail="halt unresolved",
        overridable=False,
    )
    service = RestartReadinessService(
        resume_evaluator=lambda: (False, [blocker]),
        state_snapshot=lambda: state,
        db_factory=lambda: _Conn(open_count=2, recovery_required_count=1),
        readiness_snapshot_builder=lambda _conn: _Readiness(),
        repair_service=OperatorRepairService(
            manual_flat_preview_builder=lambda _conn: {"safe_to_apply": False},
            fee_gap_preview_builder=lambda _conn: {"needs_repair": False, "resume_blocking": True},
        ),
    )

    checks = service.evaluate_restart_readiness()

    assert checks[0] == ("unresolved/recovery-required orders", False, "unresolved=3 recovery_required=1")
    assert checks[1] == ("open orders", False, "open_orders=2")
    assert checks[3] == (
        "halt state",
        False,
        "halt_blocked=1 halt_unresolved=1 detail=operator review required",
    )


def test_operator_services_delegate_through_stable_boundaries() -> None:
    sent: list[str] = []
    repaired = OperatorRepairService(
        fee_gap_preview_builder=lambda _conn: {"needs_repair": True},
        manual_flat_preview_builder=lambda _conn: {"safe_to_apply": True},
    )
    notified = OperatorNotificationService(
        event_formatter=lambda event, **fields: f"{event}:{fields['status']}",
        message_sender=sent.append,
    )
    flattened = OperatorFlattenService(
        flattener=lambda **kwargs: {"status": "ok", "trigger": kwargs["trigger"]},
    )

    assert repaired.fee_gap_accounting_preview(object()) == {"needs_repair": True}
    assert repaired.manual_flat_accounting_preview(object()) == {"safe_to_apply": True}
    notified.send_event("unit", status="ok")
    notified.send_message("plain")
    assert sent == ["unit:ok", "plain"]
    assert flattened.flatten_position(trigger="unit") == {"status": "ok", "trigger": "unit"}
