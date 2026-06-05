from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from bithumb_bot.decision_equivalence import sha256_prefixed
from bithumb_bot.strategy_policy_contract import (
    ExecutionConstraintSnapshot,
    PositionSnapshot,
)


TRANSIENT_PROVENANCE_FIELDS = frozenset(
    {
        "created_at",
        "created_at_ms",
        "created_at_sec",
        "decision_ts",
        "evaluated_at",
        "evaluated_at_ms",
        "generated_at",
        "generated_at_ms",
        "now",
        "now_ms",
        "timestamp",
        "timestamp_ms",
        "wall_clock",
        "wall_clock_ms",
        "candle_index",
        "through_ts_ms",
        "projection_source",
        "seed_contract",
        "seed_source",
        "source_contract",
        "snapshot_builder",
        "event_signal_authority",
        "event_feature_authority",
        "runtime_projection_evidence",
        "canonical_feature_projection",
        "policy_materialization_mode",
        "candidate_regime_policy_enforced",
        "previous_cross_state",
        "canonical_feature_projection_hash",
        "market_feature_hash",
        "feature_snapshot_hash",
        "provider_contract_hash",
        "runtime_data_availability_report_hash",
        "source_schema_hash",
    }
)


@dataclass(frozen=True)
class StrategyDecisionInputBundle:
    """Canonical typed input material for promotion-grade strategy decisions."""

    strategy_name: str
    market: object
    position: PositionSnapshot
    config: object
    execution_constraints: ExecutionConstraintSnapshot
    exit_policy_config: object | None
    materialized_parameters_hash: str
    snapshot_projector_version: str
    snapshot_projector_hash: str
    provenance: Mapping[str, object]
    market_snapshot_hash: str
    market_feature_hash: str
    position_snapshot_hash: str
    config_hash: str
    execution_constraints_hash: str
    exit_policy_config_hash: str
    policy_config_hash: str
    decision_input_contract_hash: str
    decision_input_bundle_payload_hash: str
    decision_input_bundle_hash: str
    component_payloads: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        strategy_name = str(self.strategy_name or "").strip().lower()
        if not strategy_name:
            raise ValueError("strategy_decision_input_bundle_strategy_name_missing")
        if not str(self.materialized_parameters_hash or "").startswith("sha256:"):
            raise ValueError("strategy_decision_input_bundle_materialized_parameters_hash_missing")
        if not str(self.snapshot_projector_version or "").strip():
            raise ValueError("strategy_decision_input_bundle_projector_version_missing")
        if not str(self.snapshot_projector_hash or "").startswith("sha256:"):
            raise ValueError("strategy_decision_input_bundle_projector_hash_missing")
        object.__setattr__(self, "strategy_name", strategy_name)
        object.__setattr__(
            self,
            "provenance",
            MappingProxyType({str(key): value for key, value in dict(self.provenance or {}).items()}),
        )
        object.__setattr__(
            self,
            "component_payloads",
            MappingProxyType(
                {
                    str(key): dict(value) if isinstance(value, Mapping) else value
                    for key, value in dict(self.component_payloads or {}).items()
                }
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        strategy_name: str,
        market: object,
        position: PositionSnapshot,
        config: object,
        execution_constraints: ExecutionConstraintSnapshot,
        exit_policy_config: object | None,
        materialized_parameters_hash: str,
        snapshot_projector_version: str,
        snapshot_projector_hash: str,
        provenance: Mapping[str, object] | None = None,
        component_payloads: Mapping[str, object] | None = None,
    ) -> "StrategyDecisionInputBundle":
        overrides = dict(component_payloads or {})
        market_payload = _payload_override(overrides, "market", _policy_payload(market))
        market_feature_payload = _payload_override(
            overrides,
            "market_feature",
            _generic_feature_payload(market_payload),
        )
        position_payload = _payload_override(overrides, "position", dict(position.policy_input_payload()))
        config_payload = _payload_override(overrides, "config", _policy_payload(config))
        execution_payload = _payload_override(
            overrides,
            "execution_constraints",
            execution_constraints.policy_input_payload(),
        )
        exit_payload = _payload_override(overrides, "exit_policy_config", _policy_payload(exit_policy_config))
        component_hashes = {
            "market_snapshot_hash": _stable_hash(market_payload),
            "market_feature_hash": _stable_hash(market_feature_payload),
            "position_snapshot_hash": _stable_hash(position_payload),
            "config_hash": _stable_hash(config_payload),
            "execution_constraints_hash": _stable_hash(execution_payload),
            "exit_policy_config_hash": _stable_hash(exit_payload),
        }
        component_hashes["policy_config_hash"] = component_hashes["config_hash"]
        payload = {
            "schema_version": 1,
            "strategy_name": str(strategy_name or "").strip().lower(),
            "market": market_payload,
            "position": position_payload,
            "config": config_payload,
            "execution_constraints": execution_payload,
            "exit_policy_config": exit_payload,
            "materialized_parameters_hash": str(materialized_parameters_hash),
            "snapshot_projector_version": str(snapshot_projector_version),
            "snapshot_projector_hash": str(snapshot_projector_hash),
            "decision_input_contract_kind": "generic",
            "component_hashes": component_hashes,
        }
        decision_input_contract_hash = _stable_hash(payload)
        payload_with_provenance = {
            **payload,
            "stable_provenance": _stable_provenance(dict(provenance or {})),
        }
        decision_input_bundle_payload_hash = _stable_hash(payload_with_provenance)
        return cls(
            strategy_name=strategy_name,
            market=market,
            position=position,
            config=config,
            execution_constraints=execution_constraints,
            exit_policy_config=exit_policy_config,
            materialized_parameters_hash=str(materialized_parameters_hash),
            snapshot_projector_version=str(snapshot_projector_version),
            snapshot_projector_hash=str(snapshot_projector_hash),
            provenance=dict(provenance or {}),
            market_snapshot_hash=component_hashes["market_snapshot_hash"],
            market_feature_hash=component_hashes["market_feature_hash"],
            position_snapshot_hash=component_hashes["position_snapshot_hash"],
            config_hash=component_hashes["config_hash"],
            execution_constraints_hash=component_hashes["execution_constraints_hash"],
            exit_policy_config_hash=component_hashes["exit_policy_config_hash"],
            policy_config_hash=component_hashes["config_hash"],
            decision_input_contract_hash=decision_input_contract_hash,
            decision_input_bundle_payload_hash=decision_input_bundle_payload_hash,
            decision_input_bundle_hash=decision_input_contract_hash,
            component_payloads=overrides,
        )

    def payload(self) -> dict[str, object]:
        overrides = dict(self.component_payloads or {})
        market_payload = _payload_override(overrides, "market", _policy_payload(self.market))
        position_payload = _payload_override(overrides, "position", dict(self.position.policy_input_payload()))
        config_payload = _payload_override(overrides, "config", _policy_payload(self.config))
        execution_payload = _payload_override(
            overrides,
            "execution_constraints",
            self.execution_constraints.policy_input_payload(),
        )
        exit_payload = _payload_override(overrides, "exit_policy_config", _policy_payload(self.exit_policy_config))
        return {
            "schema_version": 1,
            "strategy_name": self.strategy_name,
            "market": market_payload,
            "position": position_payload,
            "config": config_payload,
            "execution_constraints": execution_payload,
            "exit_policy_config": exit_payload,
            "materialized_parameters_hash": self.materialized_parameters_hash,
            "snapshot_projector_version": self.snapshot_projector_version,
            "snapshot_projector_hash": self.snapshot_projector_hash,
            "decision_input_contract_kind": "generic",
            "component_hashes": self.component_hashes(),
            "stable_provenance": _stable_provenance(dict(self.provenance)),
        }

    def component_hashes(self) -> dict[str, str]:
        return {
            "market_snapshot_hash": self.market_snapshot_hash,
            "market_feature_hash": self.market_feature_hash,
            "position_snapshot_hash": self.position_snapshot_hash,
            "config_hash": self.config_hash,
            "execution_constraints_hash": self.execution_constraints_hash,
            "exit_policy_config_hash": self.exit_policy_config_hash,
            "policy_config_hash": self.policy_config_hash,
        }

    def observability_payload(self) -> dict[str, object]:
        return {
            "decision_input_contract_hash": self.decision_input_contract_hash,
            "decision_input_bundle_hash": self.decision_input_bundle_hash,
            "decision_input_bundle_payload_hash": self.decision_input_bundle_payload_hash,
            "snapshot_projector_version": self.snapshot_projector_version,
            "snapshot_projector_contract": self.snapshot_projector_version,
            "snapshot_projector_hash": self.snapshot_projector_hash,
            "materialized_parameters_hash": self.materialized_parameters_hash,
            **self.component_hashes(),
        }


def _policy_payload(value: object) -> dict[str, object]:
    if value is None:
        return {}
    payload_method = getattr(value, "policy_input_payload", None)
    if callable(payload_method):
        payload = payload_method()
        if isinstance(payload, dict):
            return dict(payload)
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError(f"strategy_decision_input_bundle_policy_payload_unsupported:{type(value).__name__}")


def _payload_override(
    overrides: dict[str, object],
    key: str,
    default: dict[str, object],
) -> dict[str, object]:
    value = overrides.get(key)
    if value is None:
        return dict(default)
    if not isinstance(value, Mapping):
        raise TypeError(f"strategy_decision_input_bundle_component_payload_invalid:{key}")
    return dict(value)


def _generic_feature_payload(market_payload: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "feature_contract_kind": "generic_market_payload",
        "market": dict(market_payload),
    }


def _stable_hash(payload: object) -> str:
    return sha256_prefixed(payload)


def _stable_provenance(payload: dict[str, object]) -> dict[str, object]:
    return {
        str(key): value
        for key, value in sorted(payload.items())
        if str(key) not in TRANSIENT_PROVENANCE_FIELDS
    }


__all__ = ["StrategyDecisionInputBundle"]
