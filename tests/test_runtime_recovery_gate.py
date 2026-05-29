from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.runtime_recovery_gate import ResumeBlocker, RuntimeRecoveryGateService
from bithumb_bot.runtime_resume_services import RestartReadinessService, RuntimeResumeService


@dataclass(frozen=True)
class _State:
    last_reconcile_status: str | None = None
    recovery_required_count: int = 0
    last_reconcile_reason_code: str | None = None
    last_reconcile_error: str | None = None
    halt_new_orders_blocked: bool = False
    halt_state_unresolved: bool = False
    halt_open_orders_present: bool = False
    halt_position_present: bool = False
    halt_reason_code: str | None = None
    last_disable_reason: str | None = None
    emergency_flatten_blocked: bool = False
    emergency_flatten_block_reason: str | None = None
    last_flatten_position_status: str | None = None
    unresolved_open_order_count: int = 0
    last_reconcile_metadata: str | None = None


@dataclass(frozen=True)
class _Blocker:
    code: str
    detail: str
    reason_code: str
    summary: str
    overridable: bool


def _blocker_factory(**kwargs) -> _Blocker:
    return _Blocker(**kwargs)


@dataclass(frozen=True)
class _Clearance:
    allowed: bool

    def as_dict(self) -> dict[str, object]:
        return {"allowed": self.allowed, "decision_hash": f"hash:{int(self.allowed)}"}


def _gate_service(**overrides) -> RuntimeRecoveryGateService:
    kwargs = {
        "startup_gate_evaluator": lambda: None,
        "initial_reconcile_halt_evaluator": lambda **_kwargs: _Clearance(False),
        "live_execution_broker_halt_evaluator": lambda **_kwargs: _Clearance(False),
        "risk_state_mismatch_halt_evaluator": lambda **_kwargs: _Clearance(False),
        "state_snapshot": lambda: _State(),
    }
    kwargs.update(overrides)
    return RuntimeRecoveryGateService(**kwargs)


def test_prepare_resume_gate_evaluates_clearance_without_stale_clear_side_effects() -> None:
    calls: list[str] = []

    service = _gate_service(
        startup_gate_evaluator=lambda: calls.append("startup") or "startup safety gate: pending_submit_orders=1",
        initial_reconcile_halt_evaluator=lambda **_kwargs: calls.append("initial_eval") or _Clearance(True),
        live_execution_broker_halt_evaluator=lambda **_kwargs: calls.append("broker_eval") or _Clearance(False),
        risk_state_mismatch_halt_evaluator=lambda **_kwargs: calls.append("risk_eval") or _Clearance(True),
        startup_gate_reason_classifier=lambda _reason, **_kwargs: ("UNUSED", "unused"),
        resume_blocker_factory=_blocker_factory,
    )

    result = service.prepare_resume_gate()

    assert calls == ["startup", "initial_eval", "broker_eval", "risk_eval"]
    assert result.initial_reconcile_halt_cleared is True
    assert result.live_execution_broker_halt_cleared is False
    assert result.risk_state_mismatch_halt_cleared is True
    assert result.startup_gate_reason == "startup safety gate: pending_submit_orders=1"
    assert len(result.clearance_artifacts) == 3


def test_startup_safety_resume_blocker_preserves_classified_reason_code() -> None:
    service = _gate_service(
        state_snapshot=lambda: _State(recovery_required_count=1),
        startup_gate_reason_classifier=lambda reason, **_kwargs: (
            "SUBMIT_UNKNOWN_RECOVERY_REQUIRED",
            f"classified:{reason}",
        ),
        resume_blocker_factory=_blocker_factory,
    )

    blockers = service.startup_safety_resume_blockers(
        "startup safety gate: recovery_required_orders=1"
    )

    assert blockers == [
        _Blocker(
            code="STARTUP_SAFETY_GATE_BLOCKED",
            detail="startup safety gate: recovery_required_orders=1",
            reason_code="SUBMIT_UNKNOWN_RECOVERY_REQUIRED",
            summary="classified:startup safety gate: recovery_required_orders=1",
            overridable=False,
        )
    ]


def test_reconcile_ok_did_not_clear_blocker_skips_fee_gap_recovery() -> None:
    service = _gate_service(
        state_snapshot=lambda: _State(last_reconcile_status="ok"),
        startup_gate_reason_classifier=lambda _reason, **_kwargs: (
            "FEE_GAP_RECOVERY_REQUIRED",
            "fee gap",
        ),
        resume_blocker_factory=_blocker_factory,
    )

    assert service.reconcile_ok_did_not_clear_blockers("startup safety gate: fee_gap") == []


def test_default_blocker_factory_and_classifier_preserve_production_structure() -> None:
    service = _gate_service(
        state_snapshot=lambda: _State(last_reconcile_status="ok", recovery_required_count=1),
    )

    startup_blockers = service.startup_safety_resume_blockers(
        "startup safety gate: recovery_required_orders=1"
    )
    reconcile_blockers = service.reconcile_ok_did_not_clear_blockers(
        "startup safety gate: recovery_required_orders=1"
    )

    assert startup_blockers == [
        ResumeBlocker(
            code="STARTUP_SAFETY_GATE_BLOCKED",
            detail="startup safety gate: recovery_required_orders=1",
            reason_code="SUBMIT_UNKNOWN_RECOVERY_REQUIRED",
            summary="recovery-required orders remain",
            overridable=False,
        )
    ]
    assert reconcile_blockers == [
        ResumeBlocker(
            code="LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS",
            detail="latest reconcile reported ok but startup safety gate still blocks resume",
            reason_code="SUBMIT_UNKNOWN_RECOVERY_REQUIRED",
            summary="recovery-required orders remain",
            overridable=False,
        )
    ]


def test_startup_gate_blocker_classification_variants_are_stable() -> None:
    assert _gate_service(
        state_snapshot=lambda: _State(),
    ).startup_safety_resume_blockers(
        "startup safety gate: position_authority_projection_convergence_required=qty drift"
    ) == [
        ResumeBlocker(
            code="STARTUP_SAFETY_GATE_BLOCKED",
            detail="startup safety gate: position_authority_projection_convergence_required=qty drift",
            reason_code="POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED",
            summary="aggregate lot projection does not converge to canonical holdings",
            overridable=False,
        )
    ]


def test_runtime_resume_service_uses_recovery_gate_contract_and_sets_gate() -> None:
    calls: list[str] = []
    gate_updates: list[dict[str, object]] = []

    class _Gate:
        def prepare_resume_gate(self):
            calls.append("prepare")
            return type(
                "Prep",
                (),
                {
                    "startup_gate_reason": "startup safety gate: recovery_required_orders=1",
                    "initial_reconcile_halt_cleared": False,
                    "live_execution_broker_halt_cleared": False,
                    "risk_state_mismatch_halt_cleared": False,
                },
            )()

        def startup_safety_resume_blockers(self, reason):
            calls.append(f"startup_blockers:{reason}")
            return [
                ResumeBlocker(
                    code="STARTUP_SAFETY_GATE_BLOCKED",
                    detail=str(reason),
                    reason_code="SUBMIT_UNKNOWN_RECOVERY_REQUIRED",
                    summary="recovery-required orders remain",
                    overridable=False,
                )
            ]

        def reconcile_ok_did_not_clear_blockers(self, reason):
            calls.append(f"reconcile_blockers:{reason}")
            return [
                ResumeBlocker(
                    code="LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS",
                    detail="latest reconcile reported ok but startup safety gate still blocks resume",
                    reason_code="SUBMIT_UNKNOWN_RECOVERY_REQUIRED",
                    summary="recovery-required orders remain",
                    overridable=False,
                )
            ]

    service = RuntimeResumeService(
        recovery_gate_factory=lambda: _Gate(),
        state_snapshot=lambda: _State(last_reconcile_status="ok", recovery_required_count=1),
        set_resume_gate=lambda **kwargs: gate_updates.append(dict(kwargs)),
    )

    allowed, blockers = service.evaluate_resume_eligibility()

    assert allowed is False
    assert [blocker.code for blocker in blockers] == [
        "STARTUP_SAFETY_GATE_BLOCKED",
        "LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS",
    ]
    assert calls == [
        "prepare",
        "startup_blockers:startup safety gate: recovery_required_orders=1",
        "reconcile_blockers:startup safety gate: recovery_required_orders=1",
    ]
    assert gate_updates == [
        {
            "blocked": True,
            "reason": (
                "STARTUP_SAFETY_GATE_BLOCKED:startup safety gate: recovery_required_orders=1; "
                "LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS:latest reconcile reported ok but startup safety gate still blocks resume"
            ),
        }
    ]


def test_restart_readiness_service_uses_injected_resume_boundary_before_db_checks() -> None:
    calls: list[str] = []

    class _Conn:
        def execute(self, sql, *_args):
            calls.append("db")
            if "RECOVERY_REQUIRED" in sql:
                return type("Cursor", (), {"fetchone": lambda self: {"recovery_required_count": 0}})()
            return type("Cursor", (), {"fetchone": lambda self: {"open_count": 0}})()

        def close(self):
            calls.append("close")

    class _Readiness:
        resume_ready = True
        recovery_stage = "RESUME_READY"
        reconcile_metadata = None
        position_authority_assessment = {}
        projection_convergence = {}

        class position_state:
            class normalized_exposure:
                terminal_state = "flat"
                has_dust_only_remainder = False
                authority_gap_reason = "none"
                raw_qty_open = 0.0
                has_any_position_residue = False
                has_executable_exposure = False

    blocker = ResumeBlocker(
        code="STARTUP_SAFETY_GATE_BLOCKED",
        detail="startup safety gate: recovery_required_orders=1",
        reason_code="SUBMIT_UNKNOWN_RECOVERY_REQUIRED",
        summary="recovery-required orders remain",
        overridable=False,
    )

    class _RepairService:
        def manual_flat_accounting_preview(self, _conn):
            return {"safe_to_apply": False, "eligibility_reason": "none"}

        def fee_gap_accounting_preview(self, _conn):
            return {
                "needs_repair": False,
                "resume_blocking": False,
                "safe_to_apply": False,
                "eligibility_reason": "none",
            }

    service = RestartReadinessService(
        resume_evaluator=lambda: calls.append("resume") or (False, [blocker]),
        state_snapshot=lambda: _State(),
        db_factory=lambda: _Conn(),
        readiness_snapshot_builder=lambda _conn: calls.append("readiness") or _Readiness(),
        repair_service=_RepairService(),
    )

    checks = service.evaluate_restart_readiness()

    assert calls[:2] == ["resume", "db"]
    assert (
        "halt state",
        False,
        "halt_blocked=0 halt_unresolved=0 detail=none",
    ) in checks
