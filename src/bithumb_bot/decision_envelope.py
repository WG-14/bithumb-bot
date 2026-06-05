from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .decision_equivalence import sha256_prefixed
from .runtime_decision_contract import RuntimeStrategyPolicyHashes
from .runtime_strategy_decision import RuntimeStrategyDecisionResult
from .strategy_policy_contract import StrategyDecisionV2


def _freeze_value(value: Any) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_value(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_value(item) for item in value)
    return deepcopy(value)


def _frozen_mapping(value: Mapping[str, object] | None) -> Mapping[str, object]:
    return MappingProxyType({str(key): _freeze_value(item) for key, item in dict(value or {}).items()})


def _thaw_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_value(item) for item in value]
    return deepcopy(value)


def _thaw_mapping(value: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _thaw_value(item) for key, item in value.items()}


@dataclass(frozen=True)
class DecisionEnvelope:
    """Typed strategy decision authority plus non-authoritative export context."""

    strategy_decision: StrategyDecisionV2
    candle_ts: int
    market_price: float
    base_context: Mapping[str, object]
    policy_hashes: RuntimeStrategyPolicyHashes | Mapping[str, object] | object | None
    replay_fingerprint: Mapping[str, object]
    boundary: Mapping[str, object]

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_decision, StrategyDecisionV2):
            raise TypeError("strategy_decision_must_be_typed")
        object.__setattr__(self, "candle_ts", int(self.candle_ts))
        object.__setattr__(self, "market_price", float(self.market_price))
        object.__setattr__(self, "base_context", _frozen_mapping(self.base_context))
        object.__setattr__(self, "replay_fingerprint", _frozen_mapping(self.replay_fingerprint))
        object.__setattr__(self, "boundary", _frozen_mapping(self.boundary))

    @classmethod
    def from_runtime_result(cls, result: RuntimeStrategyDecisionResult) -> "DecisionEnvelope":
        if not isinstance(result.decision, StrategyDecisionV2):
            raise TypeError("runtime_result_decision_must_be_typed")
        return cls(
            strategy_decision=result.decision,
            candle_ts=result.candle_ts,
            market_price=result.market_price,
            base_context=result.base_context,
            policy_hashes=result.policy_hashes,
            replay_fingerprint=result.replay_fingerprint,
            boundary=result.boundary,
        )

    def _policy_hashes_as_dict(self) -> dict[str, object]:
        if self.policy_hashes is None:
            return {}
        if isinstance(self.policy_hashes, Mapping):
            return _thaw_mapping(self.policy_hashes)
        if hasattr(self.policy_hashes, "as_dict"):
            value = self.policy_hashes.as_dict()
            if isinstance(value, Mapping):
                return dict(value)
        raise TypeError("policy_hashes_must_be_mapping_or_as_dict")

    def as_persistence_context(self) -> dict[str, object]:
        """Serialize observability material; this dict is not execution authority."""
        decision = self.strategy_decision
        context = _thaw_mapping(self.base_context)
        context.update(
            {
                "ts": int(self.candle_ts),
                "last_close": float(self.market_price),
                "strategy": decision.strategy_name,
                "signal": decision.final_signal,
                "reason": decision.final_reason,
                "raw_signal": decision.raw_signal,
                "raw_reason": decision.raw_reason,
                "final_signal": decision.final_signal,
                "final_reason": decision.final_reason,
                "pure_policy_hash": decision.policy_hash,
                "policy_contract_hash": decision.policy_contract_hash,
                "policy_input_hash": decision.policy_input_hash,
                "policy_decision_hash": decision.policy_decision_hash,
                "pure_policy_trace": decision.as_trace(),
                "replay_fingerprint": _thaw_mapping(self.replay_fingerprint),
                "replay_fingerprint_hash": sha256_prefixed(_thaw_mapping(self.replay_fingerprint)),
                "boundary": _thaw_mapping(self.boundary),
                "decision_authority_source": "DecisionEnvelope.strategy_decision",
                "decision_envelope_present": True,
                "persistence_context_authoritative": 0,
                "non_authoritative_observability_payload": True,
            }
        )
        context.update(self._policy_hashes_as_dict())
        _attach_decision_projection_observability(context, _thaw_mapping(self.replay_fingerprint))
        if decision.execution_intent is not None:
            context["strategy_trace"] = {
                **dict(context.get("strategy_trace") or {}),
                "execution_intent": decision.execution_intent.as_dict(),
                "execution_intent_authority": "non_authoritative_strategy_hint",
            }
        return context

    def observability_fields(self) -> dict[str, object]:
        policy_hashes: dict[str, object] = self._policy_hashes_as_dict()
        return {
            "decision_authority_source": "DecisionEnvelope.strategy_decision",
            "decision_envelope_present": True,
            "persistence_context_authoritative": 0,
            "non_authoritative_observability_payload": True,
            "replay_fingerprint_hash": sha256_prefixed(_thaw_mapping(self.replay_fingerprint)),
            **policy_hashes,
        }


def _attach_decision_projection_observability(
    context: dict[str, object],
    replay_fingerprint: Mapping[str, object],
) -> None:
    for key in (
        "decision_input_bundle_hash",
        "decision_input_contract_hash",
        "decision_input_bundle_payload_hash",
        "snapshot_projector_version",
        "snapshot_projector_hash",
        "market_snapshot_hash",
        "feature_snapshot_hash",
        "market_feature_hash",
        "canonical_feature_projection_hash",
        "final_exit_decision_input_hash",
        "position_snapshot_hash",
        "execution_constraints_hash",
        "policy_config_hash",
        "exit_policy_config_hash",
    ):
        if str(context.get(key) or "").strip():
            continue
        value = replay_fingerprint.get(key)
        if str(value or "").strip():
            context[key] = value
