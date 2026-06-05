from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, Protocol

from .decision_equivalence import sha256_prefixed
from .strategy_policy_contract import (
    ExecutionConstraintSnapshot,
    PositionSnapshot,
    StrategyDecisionV2,
)
from .strategy_decision_input import StrategyDecisionInputBundle
from .strategy_evidence_contract import DecisionEvidenceContract
from .strategy_evidence_contract import GENERIC_DECISION_EVIDENCE_CONTRACT


class StrategyPolicyLike(Protocol):
    name: str

    def decide_snapshot(
        self,
        *,
        market: object,
        position: PositionSnapshot,
        config: object,
        execution_context: ExecutionConstraintSnapshot,
        exit_policy_config: object | None = None,
        rule_sources: dict[str, str] | None = None,
    ) -> StrategyDecisionV2: ...


@dataclass(frozen=True)
class StrategyEvaluationRequest:
    strategy_name: str
    strategy_instance_id: str | None
    mode: str
    strategy_policy: StrategyPolicyLike
    market_snapshot: object
    position_snapshot: PositionSnapshot
    strategy_config: object
    execution_constraints: ExecutionConstraintSnapshot
    exit_policy_config: object | None
    rule_sources: Mapping[str, str]
    approved_profile_hash: str | None
    runtime_contract_hash: str | None
    plugin_contract_hash: str | None
    request_hash: str | None
    provenance: Mapping[str, object]
    decision_input_bundle: StrategyDecisionInputBundle | None = None
    decision_evidence_contract: DecisionEvidenceContract = GENERIC_DECISION_EVIDENCE_CONTRACT

    def __post_init__(self) -> None:
        name = str(self.strategy_name or "").strip().lower()
        mode = str(self.mode or "").strip().lower()
        if not name:
            raise ValueError("strategy_evaluation_strategy_name_missing")
        if not mode:
            raise ValueError("strategy_evaluation_mode_missing")
        if not hasattr(self.strategy_policy, "decide_snapshot"):
            raise TypeError(f"strategy_evaluation_policy_invalid:{name}:missing_decide_snapshot")
        if not isinstance(self.decision_evidence_contract, DecisionEvidenceContract):
            raise TypeError(f"strategy_evaluation_decision_evidence_contract_invalid:{name}")
        object.__setattr__(self, "strategy_name", name)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(
            self,
            "rule_sources",
            MappingProxyType({str(key): str(value) for key, value in dict(self.rule_sources or {}).items()}),
        )
        object.__setattr__(
            self,
            "provenance",
            MappingProxyType({str(key): value for key, value in dict(self.provenance or {}).items()}),
        )


@dataclass(frozen=True)
class StrategyEvaluationResult:
    decision: StrategyDecisionV2
    policy_input_hash: str
    policy_decision_hash: str
    policy_contract_hash: str
    replay_fingerprint_hash: str
    replay_fingerprint: Mapping[str, object]
    provenance: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.policy_input_hash:
            raise ValueError("strategy_evaluation_policy_input_hash_missing")
        if not self.policy_decision_hash:
            raise ValueError("strategy_evaluation_policy_decision_hash_missing")
        if not self.policy_contract_hash:
            raise ValueError("strategy_evaluation_policy_contract_hash_missing")
        if not self.replay_fingerprint_hash:
            raise ValueError("strategy_evaluation_replay_fingerprint_hash_missing")
        object.__setattr__(self, "replay_fingerprint", MappingProxyType(dict(self.replay_fingerprint or {})))
        object.__setattr__(self, "provenance", MappingProxyType(dict(self.provenance or {})))


class StrategyDecisionService:
    """Canonical production-grade strategy decision evaluation boundary."""

    _PROMOTION_COMPARABLE_MODES = {
        "research_promotion",
        "runtime_replay",
        "paper",
        "paper_dry_run",
        "live",
        "live_dry_run",
        "live_real_order",
    }

    _PROMOTION_PROVENANCE_FIELDS = (
        "strategy_instance_id",
        "strategy_parameters_hash",
        "approved_profile_hash",
        "plugin_contract_hash",
        "runtime_contract_hash",
        "runtime_decision_request_hash",
    )

    def evaluate(self, request: StrategyEvaluationRequest) -> StrategyEvaluationResult:
        policy_name = str(getattr(request.strategy_policy, "name", "") or "").strip().lower()
        if policy_name and policy_name != request.strategy_name:
            raise ValueError(f"strategy_evaluation_policy_strategy_mismatch:{request.strategy_name}:{policy_name}")
        bundle = request.decision_input_bundle
        if (
            request.decision_evidence_contract.requires_decision_input_bundle
            and request.mode in self._PROMOTION_COMPARABLE_MODES
            and bundle is None
        ):
            raise ValueError(f"strategy_evaluation_decision_input_bundle_missing:{request.strategy_name}")
        if bundle is not None:
            if bundle.strategy_name != request.strategy_name:
                raise ValueError(
                    "strategy_evaluation_decision_input_bundle_strategy_mismatch:"
                    f"{request.strategy_name}:{bundle.strategy_name}"
                )
            market_snapshot = bundle.market
            position_snapshot = bundle.position
            strategy_config = bundle.config
            execution_constraints = bundle.execution_constraints
            exit_policy_config = bundle.exit_policy_config
        else:
            market_snapshot = request.market_snapshot
            position_snapshot = request.position_snapshot
            strategy_config = request.strategy_config
            execution_constraints = request.execution_constraints
            exit_policy_config = request.exit_policy_config
        decision = request.strategy_policy.decide_snapshot(
            market=market_snapshot,
            position=position_snapshot,
            config=strategy_config,
            execution_context=execution_constraints,
            exit_policy_config=exit_policy_config,
            rule_sources=dict(request.rule_sources),
        )
        if not isinstance(decision, StrategyDecisionV2):
            raise TypeError(f"strategy_evaluation_decision_invalid:{request.strategy_name}")
        if str(decision.strategy_name or "").strip().lower() != request.strategy_name:
            raise ValueError(
                "strategy_evaluation_decision_strategy_mismatch:"
                f"{request.strategy_name}:{decision.strategy_name}"
            )
        for field_name in ("policy_input_hash", "policy_decision_hash", "policy_contract_hash"):
            if not str(getattr(decision, field_name, "") or "").strip():
                raise ValueError(f"strategy_evaluation_{field_name}_missing:{request.strategy_name}")
        replay_fingerprint = request.provenance.get("replay_fingerprint")
        replay_payload = dict(replay_fingerprint) if isinstance(replay_fingerprint, Mapping) else {}
        if "policy_input_hash" not in replay_payload:
            replay_payload["policy_input_hash"] = decision.policy_input_hash
        if "policy_decision_hash" not in replay_payload:
            replay_payload["policy_decision_hash"] = decision.policy_decision_hash
        if "policy_contract_hash" not in replay_payload:
            replay_payload["policy_contract_hash"] = decision.policy_contract_hash
        if bundle is not None:
            replay_payload.update(bundle.observability_payload())
        decision_trace = decision.as_trace()
        for key in ("final_exit_decision_input_hash",):
            value = decision_trace.get(key)
            if str(value or "").strip():
                replay_payload[key] = value
        replay_hash_payload = {
            key: value for key, value in replay_payload.items() if key != "replay_fingerprint_hash"
        }
        replay_payload["replay_fingerprint_hash"] = sha256_prefixed(replay_hash_payload)
        provenance = {
            **dict(request.provenance),
            "strategy_name": request.strategy_name,
            "strategy_instance_id": request.strategy_instance_id,
            "strategy_parameters_hash": request.provenance.get("strategy_parameters_hash"),
            "strategy_evaluation_mode": request.mode,
            "approved_profile_hash": request.approved_profile_hash,
            "runtime_contract_hash": request.runtime_contract_hash,
            "plugin_contract_hash": request.plugin_contract_hash,
            "runtime_decision_request_hash": request.request_hash,
            "policy_input_hash": decision.policy_input_hash,
            "policy_decision_hash": decision.policy_decision_hash,
            "policy_contract_hash": decision.policy_contract_hash,
            "replay_fingerprint_hash": replay_payload["replay_fingerprint_hash"],
            "decision_boundary": "StrategyDecisionService.evaluate",
        }
        if bundle is not None:
            provenance.update(bundle.observability_payload())
        for key in ("final_exit_decision_input_hash",):
            value = decision_trace.get(key)
            if str(value or "").strip():
                provenance[key] = value
        for key in request.decision_evidence_contract.required_promotion_provenance_fields:
            if key in provenance:
                continue
            value = replay_payload.get(key)
            if str(value or "").strip():
                provenance[key] = value
        for key in request.decision_evidence_contract.required_live_real_order_fields:
            if key in provenance:
                continue
            value = replay_payload.get(key)
            if str(value or "").strip():
                provenance[key] = value
        if request.mode in self._PROMOTION_COMPARABLE_MODES and bool(
            provenance.get("compatibility_fallback")
        ):
            raise ValueError(f"strategy_evaluation_compatibility_fallback_rejected:{request.strategy_name}")
        self._validate_snapshot_projector_contract(request=request, provenance=provenance)
        self._validate_promotion_provenance(request=request, provenance=provenance)
        return StrategyEvaluationResult(
            decision=decision,
            policy_input_hash=decision.policy_input_hash,
            policy_decision_hash=decision.policy_decision_hash,
            policy_contract_hash=decision.policy_contract_hash,
            replay_fingerprint_hash=str(replay_payload["replay_fingerprint_hash"]),
            replay_fingerprint=replay_payload,
            provenance=provenance,
        )

    def _validate_promotion_provenance(
        self,
        *,
        request: StrategyEvaluationRequest,
        provenance: Mapping[str, object],
    ) -> None:
        if request.mode not in self._PROMOTION_COMPARABLE_MODES:
            return
        missing: list[str] = []
        for field_name in self._PROMOTION_PROVENANCE_FIELDS:
            value = provenance.get(field_name)
            if str(value or "").strip():
                continue
            reason_key = f"{field_name}_unavailable_reason"
            if str(provenance.get(reason_key) or "").strip():
                continue
            missing.append(field_name)
        required_decision_fields = [
            "policy_input_hash",
            "policy_decision_hash",
            "policy_contract_hash",
            "replay_fingerprint_hash",
            "strategy_evaluation_mode",
            "decision_boundary",
        ]
        required_decision_fields.extend(
            request.decision_evidence_contract.required_promotion_provenance_fields
        )
        for field_name in required_decision_fields:
            if not str(provenance.get(field_name) or "").strip():
                missing.append(field_name)
        if missing:
            raise ValueError(
                "strategy_evaluation_required_provenance_missing:"
                + request.strategy_name
                + ":"
                + ",".join(sorted(set(missing)))
            )
        if request.mode == "live_real_order":
            self._validate_live_real_order_provenance(request=request, provenance=provenance)

    def _validate_live_real_order_provenance(
        self,
        *,
        request: StrategyEvaluationRequest,
        provenance: Mapping[str, object],
    ) -> None:
        missing: list[str] = []
        for field_name in (
            "policy_input_hash",
            "policy_decision_hash",
            "policy_contract_hash",
            "replay_fingerprint_hash",
            "strategy_parameters_hash",
            "approved_profile_hash",
            "plugin_contract_hash",
            "runtime_contract_hash",
            "runtime_decision_request_hash",
        ):
            if not str(provenance.get(field_name) or "").strip():
                missing.append(field_name)
        for field_name in request.decision_evidence_contract.required_live_real_order_fields:
            if not str(provenance.get(field_name) or "").strip():
                missing.append(field_name)
        if not (
            str(provenance.get("fee_authority_hash") or "").strip()
            or str(provenance.get("fee_authority_payload_hash") or "").strip()
        ):
            missing.append("fee_authority_hash")
        if not (
            str(provenance.get("order_rules_hash") or "").strip()
            or str(provenance.get("order_rules_payload_hash") or "").strip()
        ):
            missing.append("order_rules_hash")
        unavailable = sorted(
            key
            for key, value in provenance.items()
            if str(key).endswith("_unavailable_reason") and str(value or "").strip()
        )
        if unavailable:
            missing.extend(unavailable)
        if missing:
            raise ValueError(
                "strategy_evaluation_live_real_order_provenance_missing:"
                + ",".join(sorted(set(missing)))
            )

    def _validate_snapshot_projector_contract(
        self,
        *,
        request: StrategyEvaluationRequest,
        provenance: Mapping[str, object],
    ) -> None:
        expected = request.decision_evidence_contract.snapshot_projector_contract
        if expected is None or request.mode not in self._PROMOTION_COMPARABLE_MODES:
            return
        observed_contract = str(provenance.get("snapshot_projector_contract") or "").strip()
        observed_version = str(provenance.get("snapshot_projector_version") or "").strip()
        if observed_contract == expected or observed_version == expected:
            return
        raise ValueError(
            "strategy_evaluation_snapshot_projector_contract_mismatch:"
            f"{request.strategy_name}:{expected}:{observed_contract or observed_version or 'missing'}"
        )


__all__ = [
    "StrategyDecisionService",
    "StrategyEvaluationRequest",
    "StrategyEvaluationResult",
]
