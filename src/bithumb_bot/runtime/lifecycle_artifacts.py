from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ..decision_equivalence import sha256_prefixed


def _stable_hash(payload: Mapping[str, Any] | Sequence[Any] | None) -> str:
    return sha256_prefixed({} if payload is None else payload)


def _operator_event_hash(event: Mapping[str, Any] | None) -> str | None:
    if not event:
        return None
    event_hash = event.get("event_hash")
    if event_hash:
        return str(event_hash)
    return _stable_hash(event)


@dataclass(frozen=True)
class StateTransitionResult:
    status: str
    reason_code: str
    state_from: str | None = None
    state_to: str | None = None
    applied: bool = False
    evidence: Mapping[str, Any] = field(default_factory=dict)
    input_hash: str | None = None
    evidence_hash: str | None = None
    decision_hash: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "artifact_type": "state_transition_result",
            "schema_version": 1,
            "status": self.status,
            "reason_code": self.reason_code,
            "state_from": self.state_from,
            "state_to": self.state_to,
            "applied": bool(self.applied),
            "evidence": dict(self.evidence),
            "input_hash": self.input_hash or _stable_hash({"reason_code": self.reason_code, "state_from": self.state_from}),
            "evidence_hash": self.evidence_hash or _stable_hash(self.evidence),
        }
        payload["decision_hash"] = self.decision_hash or _stable_hash(payload)
        return payload


@dataclass(frozen=True)
class SafetyDecision:
    action: str
    reason_code: str
    reason: str
    unresolved: bool = False
    attempt_flatten: bool = False
    state_transition: StateTransitionResult | Mapping[str, Any] | None = None
    operator_event: Mapping[str, Any] | None = None
    input_hash: str | None = None
    evidence_hash: str | None = None
    decision_hash: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        transition = (
            self.state_transition.as_dict()
            if isinstance(self.state_transition, StateTransitionResult)
            else dict(self.state_transition or {})
        )
        payload = {
            "artifact_type": "safety_decision",
            "schema_version": 1,
            "action": self.action,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "unresolved": bool(self.unresolved),
            "attempt_flatten": bool(self.attempt_flatten),
            "state_transition": transition,
            "operator_event": dict(self.operator_event or {}),
            "operator_event_hashes": (
                [event_hash] if (event_hash := _operator_event_hash(self.operator_event)) else []
            ),
            "evidence": dict(self.evidence),
            "input_hash": self.input_hash or _stable_hash({"reason_code": self.reason_code, "reason": self.reason}),
            "evidence_hash": self.evidence_hash or _stable_hash(self.evidence),
        }
        payload["decision_hash"] = self.decision_hash or _stable_hash(payload)
        return payload


@dataclass(frozen=True)
class RecoveryClearance:
    status: str
    reason_code: str
    allowed: bool
    state_transition: Mapping[str, Any] | StateTransitionResult | None = None
    operator_event_hashes: Sequence[str] = ()
    input_hash: str | None = None
    evidence_hash: str | None = None
    decision_hash: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        transition = (
            self.state_transition.as_dict()
            if isinstance(self.state_transition, StateTransitionResult)
            else dict(self.state_transition or {})
        )
        payload = {
            "artifact_type": "recovery_clearance",
            "schema_version": 1,
            "status": self.status,
            "reason_code": self.reason_code,
            "allowed": bool(self.allowed),
            "state_transition": transition,
            "operator_event_hashes": list(self.operator_event_hashes),
            "evidence": dict(self.evidence),
            "input_hash": self.input_hash or _stable_hash({"reason_code": self.reason_code, "evidence": self.evidence}),
            "evidence_hash": self.evidence_hash or _stable_hash(self.evidence),
        }
        payload["decision_hash"] = self.decision_hash or _stable_hash(payload)
        return payload


@dataclass(frozen=True)
class StartupResult:
    status: str
    broker: object | None = None
    startup_gate_reason: str | None = None
    reason_code: str | None = None
    operator_event: Mapping[str, Any] | None = None
    halt_transition: Mapping[str, Any] | StateTransitionResult | None = None
    input_hash: str | None = None
    evidence_hash: str | None = None
    decision_hash: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        transition = (
            self.halt_transition.as_dict()
            if isinstance(self.halt_transition, StateTransitionResult)
            else dict(self.halt_transition or {})
        )
        payload = {
            "artifact_type": "startup_result",
            "schema_version": 1,
            "status": self.status,
            "reason_code": self.reason_code,
            "startup_gate_reason": self.startup_gate_reason,
            "broker_present": self.broker is not None,
            "operator_event": dict(self.operator_event or {}),
            "operator_event_hashes": (
                [event_hash] if (event_hash := _operator_event_hash(self.operator_event)) else []
            ),
            "halt_transition": transition,
            "evidence": dict(self.evidence),
            "input_hash": self.input_hash or _stable_hash({"status": self.status, "startup_gate_reason": self.startup_gate_reason}),
            "evidence_hash": self.evidence_hash or _stable_hash(self.evidence),
        }
        payload["decision_hash"] = self.decision_hash or _stable_hash(payload)
        return payload


@dataclass(frozen=True)
class RuntimeDependencyManifest:
    schema_version: int
    source_revision: str | None
    settings_hash: str
    env_summary_hash: str | None
    env_file_source: str | None
    mode: str
    live_dry_run: bool
    live_real_order_armed: bool
    execution_engine: str
    broker_factory_identity: str
    private_api_submit_boundary_identity: str
    decision_gateway_identity: str
    execution_service_identity: str
    notification_service_identity: str
    flatten_service_identity: str
    clock_identity: str
    scheduler_identity: str
    compat_override_enabled: bool
    legacy_patch_points_enabled: bool
    runtime_strategy_set_manifest_hash: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "artifact_type": "runtime_dependency_manifest",
            "schema_version": self.schema_version,
            "source_revision": self.source_revision,
            "settings_hash": self.settings_hash,
            "env_summary_hash": self.env_summary_hash,
            "env_file_source": self.env_file_source,
            "mode": self.mode,
            "LIVE_DRY_RUN": bool(self.live_dry_run),
            "LIVE_REAL_ORDER_ARMED": bool(self.live_real_order_armed),
            "execution_engine": self.execution_engine,
            "broker_factory_identity": self.broker_factory_identity,
            "private_api_submit_boundary_identity": self.private_api_submit_boundary_identity,
            "decision_gateway_identity": self.decision_gateway_identity,
            "execution_service_identity": self.execution_service_identity,
            "notification_service_identity": self.notification_service_identity,
            "flatten_service_identity": self.flatten_service_identity,
            "clock_identity": self.clock_identity,
            "scheduler_identity": self.scheduler_identity,
            "compat_override_enabled": bool(self.compat_override_enabled),
            "legacy_patch_points_enabled": bool(self.legacy_patch_points_enabled),
            "runtime_strategy_set_manifest_hash": self.runtime_strategy_set_manifest_hash,
        }
        payload["runtime_dependency_manifest_hash"] = _stable_hash(payload)
        return payload


@dataclass(frozen=True)
class RuntimeCycleArtifact:
    cycle_id: str
    candle_ts: int | None
    startup_state: str | None = None
    readiness_hash: str | None = None
    strategy_decision_hash: str | None = None
    runtime_strategy_decision_bundle_id: int | None = None
    runtime_strategy_decision_bundle_hash: str | None = None
    portfolio_allocation_decision_id: int | None = None
    portfolio_allocation_decision_hash: str | None = None
    portfolio_target_id: int | None = None
    portfolio_target_hash: str | None = None
    strategy_contribution_hash: str | None = None
    execution_plan_id: int | None = None
    execution_plan_bundle_hash: str | None = None
    execution_submit_plan_hash: str | None = None
    strategy_virtual_lifecycle_transition_hashes: Sequence[str] = ()
    strategy_risk_decision_hash: str | None = None
    strategy_risk_policy_hash: str | None = None
    strategy_risk_input_hash: str | None = None
    strategy_risk_evidence_hash: str | None = None
    strategy_risk_state_source: str | None = None
    strategy_risk_status: str | None = None
    strategy_risk_reason_code: str | None = None
    portfolio_risk_decision_hash: str | None = None
    portfolio_risk_policy_hash: str | None = None
    portfolio_risk_input_hash: str | None = None
    portfolio_risk_evidence_hash: str | None = None
    portfolio_risk_state_source: str | None = None
    portfolio_risk_status: str | None = None
    portfolio_risk_reason_code: str | None = None
    pre_submit_risk_decision_hash: str | None = None
    pre_submit_risk_policy_hash: str | None = None
    pre_submit_risk_input_hash: str | None = None
    pre_submit_risk_evidence_hash: str | None = None
    pre_submit_risk_plan_hash: str | None = None
    pre_submit_risk_state_source: str | None = None
    pre_submit_risk_status: str | None = None
    pre_submit_risk_reason_code: str | None = None
    execution_result_hash: str | None = None
    safety_decision_hash: str | None = None
    recovery_decision_hash: str | None = None
    state_transition_hash: str | None = None
    runtime_dependency_manifest_hash: str | None = None
    notification_event_hashes: Sequence[str] = ()
    failure_phase: str | None = None
    failure_reason_code: str | None = None
    failure_detail: str | None = None
    operator_next_action: str | None = None
    failure_evidence_hash: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "artifact_type": "runtime_cycle_artifact",
            "schema_version": 1,
            "cycle_id": self.cycle_id,
            "candle_ts": self.candle_ts,
            "startup_state": self.startup_state,
            "readiness_hash": self.readiness_hash,
            "strategy_decision_hash": self.strategy_decision_hash,
            "runtime_strategy_decision_bundle_id": self.runtime_strategy_decision_bundle_id,
            "runtime_strategy_decision_bundle_hash": self.runtime_strategy_decision_bundle_hash,
            "portfolio_allocation_decision_id": self.portfolio_allocation_decision_id,
            "portfolio_allocation_decision_hash": self.portfolio_allocation_decision_hash,
            "portfolio_target_id": self.portfolio_target_id,
            "portfolio_target_hash": self.portfolio_target_hash,
            "strategy_contribution_hash": self.strategy_contribution_hash,
            "execution_plan_id": self.execution_plan_id,
            "execution_plan_bundle_hash": self.execution_plan_bundle_hash,
            "execution_submit_plan_hash": self.execution_submit_plan_hash,
            "strategy_virtual_lifecycle_transition_hashes": list(
                self.strategy_virtual_lifecycle_transition_hashes
            ),
            "strategy_risk_decision_hash": self.strategy_risk_decision_hash,
            "strategy_risk_policy_hash": self.strategy_risk_policy_hash,
            "strategy_risk_input_hash": self.strategy_risk_input_hash,
            "strategy_risk_evidence_hash": self.strategy_risk_evidence_hash,
            "strategy_risk_state_source": self.strategy_risk_state_source,
            "strategy_risk_status": self.strategy_risk_status,
            "strategy_risk_reason_code": self.strategy_risk_reason_code,
            "portfolio_risk_decision_hash": self.portfolio_risk_decision_hash,
            "portfolio_risk_policy_hash": self.portfolio_risk_policy_hash,
            "portfolio_risk_input_hash": self.portfolio_risk_input_hash,
            "portfolio_risk_evidence_hash": self.portfolio_risk_evidence_hash,
            "portfolio_risk_state_source": self.portfolio_risk_state_source,
            "portfolio_risk_status": self.portfolio_risk_status,
            "portfolio_risk_reason_code": self.portfolio_risk_reason_code,
            "pre_submit_risk_decision_hash": self.pre_submit_risk_decision_hash,
            "pre_submit_risk_policy_hash": self.pre_submit_risk_policy_hash,
            "pre_submit_risk_input_hash": self.pre_submit_risk_input_hash,
            "pre_submit_risk_evidence_hash": self.pre_submit_risk_evidence_hash,
            "pre_submit_risk_plan_hash": self.pre_submit_risk_plan_hash,
            "pre_submit_risk_state_source": self.pre_submit_risk_state_source,
            "pre_submit_risk_status": self.pre_submit_risk_status,
            "pre_submit_risk_reason_code": self.pre_submit_risk_reason_code,
            "execution_result_hash": self.execution_result_hash,
            "safety_decision_hash": self.safety_decision_hash,
            "recovery_decision_hash": self.recovery_decision_hash,
            "state_transition_hash": self.state_transition_hash,
            "runtime_dependency_manifest_hash": self.runtime_dependency_manifest_hash,
            "notification_event_hashes": list(self.notification_event_hashes),
            "failure_phase": self.failure_phase,
            "failure_reason_code": self.failure_reason_code,
            "failure_detail": self.failure_detail,
            "operator_next_action": self.operator_next_action,
            "failure_evidence_hash": self.failure_evidence_hash,
        }
        payload["input_hash"] = _stable_hash(
            {
                "cycle_id": self.cycle_id,
                "candle_ts": self.candle_ts,
                "startup_state": self.startup_state,
            }
        )
        payload["evidence_hash"] = _stable_hash(
            {
                "readiness_hash": self.readiness_hash,
                "strategy_decision_hash": self.strategy_decision_hash,
                "runtime_strategy_decision_bundle_hash": self.runtime_strategy_decision_bundle_hash,
                "portfolio_allocation_decision_hash": self.portfolio_allocation_decision_hash,
                "portfolio_target_hash": self.portfolio_target_hash,
                "strategy_contribution_hash": self.strategy_contribution_hash,
                "execution_plan_bundle_hash": self.execution_plan_bundle_hash,
                "execution_submit_plan_hash": self.execution_submit_plan_hash,
                "strategy_virtual_lifecycle_transition_hashes": list(
                    self.strategy_virtual_lifecycle_transition_hashes
                ),
                "strategy_risk_decision_hash": self.strategy_risk_decision_hash,
                "strategy_risk_policy_hash": self.strategy_risk_policy_hash,
                "strategy_risk_input_hash": self.strategy_risk_input_hash,
                "strategy_risk_evidence_hash": self.strategy_risk_evidence_hash,
                "strategy_risk_state_source": self.strategy_risk_state_source,
                "strategy_risk_status": self.strategy_risk_status,
                "strategy_risk_reason_code": self.strategy_risk_reason_code,
                "portfolio_risk_decision_hash": self.portfolio_risk_decision_hash,
                "portfolio_risk_policy_hash": self.portfolio_risk_policy_hash,
                "portfolio_risk_input_hash": self.portfolio_risk_input_hash,
                "portfolio_risk_evidence_hash": self.portfolio_risk_evidence_hash,
                "portfolio_risk_state_source": self.portfolio_risk_state_source,
                "portfolio_risk_status": self.portfolio_risk_status,
                "portfolio_risk_reason_code": self.portfolio_risk_reason_code,
                "pre_submit_risk_decision_hash": self.pre_submit_risk_decision_hash,
                "pre_submit_risk_policy_hash": self.pre_submit_risk_policy_hash,
                "pre_submit_risk_input_hash": self.pre_submit_risk_input_hash,
                "pre_submit_risk_evidence_hash": self.pre_submit_risk_evidence_hash,
                "pre_submit_risk_plan_hash": self.pre_submit_risk_plan_hash,
                "pre_submit_risk_state_source": self.pre_submit_risk_state_source,
                "pre_submit_risk_status": self.pre_submit_risk_status,
                "pre_submit_risk_reason_code": self.pre_submit_risk_reason_code,
                "execution_result_hash": self.execution_result_hash,
                "safety_decision_hash": self.safety_decision_hash,
                "recovery_decision_hash": self.recovery_decision_hash,
                "state_transition_hash": self.state_transition_hash,
                "runtime_dependency_manifest_hash": self.runtime_dependency_manifest_hash,
                "notification_event_hashes": list(self.notification_event_hashes),
                "failure_phase": self.failure_phase,
                "failure_reason_code": self.failure_reason_code,
                "operator_next_action": self.operator_next_action,
                "failure_evidence_hash": self.failure_evidence_hash,
            }
        )
        payload["decision_hash"] = _stable_hash(payload)
        return payload


__all__ = [
    "RecoveryClearance",
    "RuntimeCycleArtifact",
    "RuntimeDependencyManifest",
    "SafetyDecision",
    "StartupResult",
    "StateTransitionResult",
]
