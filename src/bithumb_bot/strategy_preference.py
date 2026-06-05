from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Mapping

from .canonical_decision import sha256_prefixed
from .strategy_policy_contract import StrategyDecisionV2


def _stable_value(value: object) -> object:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return {str(key): _stable_value(item) for key, item in sorted(value.items())}
    if isinstance(value, tuple | list):
        return [_stable_value(item) for item in value]
    if hasattr(value, "as_dict"):
        return _stable_value(value.as_dict())  # type: ignore[no-any-return]
    if hasattr(value, "__dataclass_fields__"):
        return _stable_value(asdict(value))
    return str(value)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


@dataclass(frozen=True)
class StrategyPreference:
    """Typed non-authoritative strategy preference.

    A strategy preference is an input to allocation, not execution authority.
    ``execution_intent_hint`` is preserved for traceability only; live submit
    authority must come from allocator-derived ``PortfolioTarget`` and the
    typed execution planner.
    """

    strategy_instance_id: str
    strategy_name: str
    pair: str
    signal_direction: str
    reason: str
    raw_signal: str = "HOLD"
    final_signal: str = "HOLD"
    desired_exposure_krw: float | None = None
    desired_weight: float | None = None
    confidence: float | None = None
    horizon: str = ""
    max_target_exposure_krw: float | None = None
    risk_budget_krw: float | None = None
    policy_hash: str = ""
    policy_contract_hash: str = ""
    policy_input_hash: str = ""
    policy_decision_hash: str = ""
    position_snapshot_hash: str = ""
    execution_intent_hint: Mapping[str, object] | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)
    schema_version: int = 1

    def __post_init__(self) -> None:
        direction = str(self.signal_direction or "HOLD").upper()
        if direction not in {"BUY", "SELL", "HOLD"}:
            raise ValueError(f"strategy_preference_invalid_signal:{direction or 'missing'}")
        object.__setattr__(self, "signal_direction", direction)
        object.__setattr__(self, "raw_signal", str(self.raw_signal or "HOLD").upper())
        object.__setattr__(self, "final_signal", str(self.final_signal or direction).upper())
        object.__setattr__(self, "desired_exposure_krw", _optional_float(self.desired_exposure_krw))
        object.__setattr__(self, "desired_weight", _optional_float(self.desired_weight))
        object.__setattr__(self, "confidence", _optional_float(self.confidence))
        max_target_exposure = _optional_float(self.max_target_exposure_krw)
        object.__setattr__(self, "risk_budget_krw", _optional_float(self.risk_budget_krw))
        object.__setattr__(self, "max_target_exposure_krw", max_target_exposure)
        object.__setattr__(
            self,
            "execution_intent_hint",
            None
            if self.execution_intent_hint is None
            else _stable_value(dict(self.execution_intent_hint)),
        )
        object.__setattr__(self, "metadata", _stable_value(dict(self.metadata)))
        object.__setattr__(self, "strategy_instance_id", str(self.strategy_instance_id or self.strategy_name))

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "pair": self.pair,
            "signal_direction": self.signal_direction,
            "raw_signal": self.raw_signal,
            "final_signal": self.final_signal,
            "desired_exposure_krw": self.desired_exposure_krw,
            "desired_weight": self.desired_weight,
            "confidence": self.confidence,
            "horizon": self.horizon,
            "max_target_exposure_krw": self.max_target_exposure_krw,
            "risk_budget_krw": self.risk_budget_krw,
            "risk_budget_semantics": "deprecated_non_authoritative_not_exposure_cap",
            "risk_decision_hash": "deprecated:risk_budget_krw_not_enforced_as_loss_budget",
            "reason": self.reason,
            "policy_hash": self.policy_hash,
            "policy_contract_hash": self.policy_contract_hash,
            "policy_input_hash": self.policy_input_hash,
            "policy_decision_hash": self.policy_decision_hash,
            "position_snapshot_hash": self.position_snapshot_hash,
            "execution_intent_hint": self.execution_intent_hint,
            "execution_intent_authority": "non_authoritative_strategy_hint",
            "metadata": dict(self.metadata),
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


@dataclass(frozen=True)
class StrategyPreferenceSet:
    preferences: tuple[StrategyPreference, ...]
    schema_version: int = 1

    def __post_init__(self) -> None:
        for preference in self.preferences:
            if not isinstance(preference, StrategyPreference):
                raise TypeError("strategy_preference_set_requires_typed_preferences")
        object.__setattr__(
            self,
            "preferences",
            tuple(
                sorted(
                    self.preferences,
                    key=lambda item: (item.pair, item.strategy_instance_id, item.content_hash()),
                )
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "preferences": [preference.as_dict() for preference in self.preferences],
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


def strategy_decision_to_preference(
    decision: StrategyDecisionV2,
    *,
    pair: str,
    strategy_instance_id: str | None = None,
    desired_exposure_krw: float | None = None,
    desired_weight: float | None = None,
    risk_budget_krw: float | None = None,
    max_target_exposure_krw: float | None = None,
    horizon: str = "",
    confidence: float | None = None,
    metadata: Mapping[str, object] | None = None,
) -> StrategyPreference:
    if not isinstance(decision, StrategyDecisionV2):
        raise TypeError("strategy_decision_to_preference_requires_strategy_decision_v2")
    position_payload = _stable_value(decision.position_snapshot)
    execution_intent = decision.execution_intent
    execution_intent_payload = (
        execution_intent.as_dict()
        if execution_intent is not None and hasattr(execution_intent, "as_dict")
        else None
    )
    return StrategyPreference(
        strategy_instance_id=str(strategy_instance_id or decision.strategy_name),
        strategy_name=decision.strategy_name,
        pair=pair,
        signal_direction=str(decision.final_signal or "HOLD").upper(),
        raw_signal=decision.raw_signal,
        final_signal=decision.final_signal,
        reason=decision.final_reason,
        desired_exposure_krw=desired_exposure_krw,
        desired_weight=desired_weight,
        max_target_exposure_krw=max_target_exposure_krw,
        risk_budget_krw=risk_budget_krw,
        horizon=horizon,
        confidence=confidence,
        policy_hash=decision.policy_hash,
        policy_contract_hash=decision.policy_contract_hash,
        policy_input_hash=decision.policy_input_hash,
        policy_decision_hash=decision.policy_decision_hash,
        position_snapshot_hash=sha256_prefixed(position_payload),
        execution_intent_hint=execution_intent_payload,
        metadata={
            "raw_reason": decision.raw_reason,
            "entry_reason": decision.entry_reason,
            "exit_reason": decision.exit_reason,
            "entry_blocked": bool(decision.entry_blocked),
            "entry_block_reason": decision.entry_block_reason,
            "exit_rule": decision.exit_rule,
            "blocked_filters": list(decision.blocked_filters),
            **dict(metadata or {}),
        },
    )
