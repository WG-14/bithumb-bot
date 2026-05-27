from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.runtime_recovery_gate import ResumeBlocker, RuntimeRecoveryGateService


@dataclass(frozen=True)
class _State:
    last_reconcile_status: str | None = None
    recovery_required_count: int = 0


@dataclass(frozen=True)
class _Blocker:
    code: str
    detail: str
    reason_code: str
    summary: str
    overridable: bool


def _blocker_factory(**kwargs) -> _Blocker:
    return _Blocker(**kwargs)


def test_prepare_resume_gate_runs_stale_clearers_around_startup_gate() -> None:
    calls: list[str] = []
    startup_reasons = iter(["startup safety gate: pending_submit_orders=1", None])

    service = RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: calls.append("startup") or next(startup_reasons),
        stale_initial_reconcile_halt_clearer=lambda: calls.append("initial") or True,
        stale_live_execution_broker_halt_clearer=lambda **_kwargs: calls.append("broker") or False,
        stale_risk_state_mismatch_halt_clearer=lambda **_kwargs: calls.append("risk") or True,
        state_snapshot=lambda: _State(),
        startup_gate_reason_classifier=lambda _reason, **_kwargs: ("UNUSED", "unused"),
        resume_blocker_factory=_blocker_factory,
    )

    result = service.prepare_resume_gate()

    assert calls == ["initial", "startup", "broker", "risk", "startup"]
    assert result.initial_reconcile_halt_cleared is True
    assert result.live_execution_broker_halt_cleared is False
    assert result.risk_state_mismatch_halt_cleared is True
    assert result.startup_gate_reason is None


def test_startup_safety_resume_blocker_preserves_classified_reason_code() -> None:
    service = RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: None,
        stale_initial_reconcile_halt_clearer=lambda: False,
        stale_live_execution_broker_halt_clearer=lambda **_kwargs: False,
        stale_risk_state_mismatch_halt_clearer=lambda **_kwargs: False,
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
    service = RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: None,
        stale_initial_reconcile_halt_clearer=lambda: False,
        stale_live_execution_broker_halt_clearer=lambda **_kwargs: False,
        stale_risk_state_mismatch_halt_clearer=lambda **_kwargs: False,
        state_snapshot=lambda: _State(last_reconcile_status="ok"),
        startup_gate_reason_classifier=lambda _reason, **_kwargs: (
            "FEE_GAP_RECOVERY_REQUIRED",
            "fee gap",
        ),
        resume_blocker_factory=_blocker_factory,
    )

    assert service.reconcile_ok_did_not_clear_blockers("startup safety gate: fee_gap") == []


def test_default_blocker_factory_and_classifier_preserve_production_structure() -> None:
    service = RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: None,
        stale_initial_reconcile_halt_clearer=lambda: False,
        stale_live_execution_broker_halt_clearer=lambda **_kwargs: False,
        stale_risk_state_mismatch_halt_clearer=lambda **_kwargs: False,
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
