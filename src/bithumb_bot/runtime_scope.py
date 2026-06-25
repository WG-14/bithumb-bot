from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from .canonical_decision import sha256_prefixed


RUNTIME_SCOPE_KEY_SCHEMA_VERSION = 1


def _clean(value: object) -> str:
    return str(value or "").strip()


def strategy_revision_id(payload: Mapping[str, object]) -> str:
    material = {
        "strategy_name": _clean(payload.get("strategy_name")).lower(),
        "strategy_instance_id": _clean(payload.get("strategy_instance_id")),
        "runtime_contract_hash": _clean(payload.get("runtime_contract_hash")),
        "approved_profile_hash": _clean(payload.get("approved_profile_hash")),
        "strategy_parameters_hash": _clean(payload.get("strategy_parameters_hash")),
    }
    return sha256_prefixed(material)


def derive_risk_scope_id(payload: Mapping[str, object]) -> str:
    material = {
        "strategy_name": _clean(payload.get("strategy_name")).lower(),
        "pair": _clean(payload.get("pair")),
        "interval": _clean(payload.get("interval")),
        "risk_policy_hash": _clean(payload.get("risk_policy_hash")),
        "risk_capital_basis": _clean(payload.get("risk_capital_basis")),
        "risk_capital_krw": _clean(payload.get("risk_capital_krw")),
        "position_ownership_model": _clean(payload.get("position_ownership_model") or "owner_risk_scope_v1"),
    }
    return sha256_prefixed(material)


def require_risk_scope_reset_authority(
    *,
    previous: Mapping[str, object],
    current: Mapping[str, object],
    risk_scope_reset_authority: str | None = None,
) -> str:
    previous_scope = derive_risk_scope_id(previous)
    current_scope = derive_risk_scope_id(current)
    if previous_scope != current_scope and not _clean(risk_scope_reset_authority):
        raise ValueError("risk_scope_reset_authority_required")
    return current_scope


@dataclass(frozen=True)
class RuntimeScopeKey:
    pair: str
    interval: str
    strategy_instance_id: str
    strategy_name: str
    runtime_contract_hash: str
    approved_profile_hash: str
    strategy_parameters_hash: str = ""
    schema_version: int = RUNTIME_SCOPE_KEY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        pair = _clean(self.pair)
        interval = _clean(self.interval)
        instance_id = _clean(self.strategy_instance_id)
        strategy_name = _clean(self.strategy_name).lower()
        runtime_contract_hash = _clean(self.runtime_contract_hash)
        approved_profile_hash = _clean(self.approved_profile_hash)
        strategy_parameters_hash = _clean(self.strategy_parameters_hash)
        missing = [
            name
            for name, value in (
                ("pair", pair),
                ("interval", interval),
                ("strategy_instance_id", instance_id),
                ("strategy_name", strategy_name),
                ("runtime_contract_hash", runtime_contract_hash),
                ("approved_profile_hash", approved_profile_hash),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"runtime_scope_key_missing:{','.join(missing)}")
        object.__setattr__(self, "pair", pair)
        object.__setattr__(self, "interval", interval)
        object.__setattr__(self, "strategy_instance_id", instance_id)
        object.__setattr__(self, "strategy_name", strategy_name)
        object.__setattr__(self, "runtime_contract_hash", runtime_contract_hash)
        object.__setattr__(self, "approved_profile_hash", approved_profile_hash)
        object.__setattr__(self, "strategy_parameters_hash", strategy_parameters_hash)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "pair": self.pair,
            "interval": self.interval,
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "runtime_contract_hash": self.runtime_contract_hash,
            "approved_profile_hash": self.approved_profile_hash,
            "strategy_parameters_hash": self.strategy_parameters_hash,
        }

    def scope_key_hash(self) -> str:
        return sha256_prefixed(self.as_dict())

    def with_hash_payload(self) -> dict[str, object]:
        payload = self.as_dict()
        payload["scope_key_hash"] = self.scope_key_hash()
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "RuntimeScopeKey":
        return cls(
            pair=str(payload.get("pair") or ""),
            interval=str(payload.get("interval") or ""),
            strategy_instance_id=str(payload.get("strategy_instance_id") or ""),
            strategy_name=str(payload.get("strategy_name") or payload.get("strategy") or ""),
            runtime_contract_hash=str(payload.get("runtime_contract_hash") or ""),
            approved_profile_hash=str(payload.get("approved_profile_hash") or ""),
            strategy_parameters_hash=str(payload.get("strategy_parameters_hash") or ""),
        )


def runtime_scope_payload(
    scope_key: RuntimeScopeKey | Mapping[str, object] | None,
) -> dict[str, object]:
    if scope_key is None:
        return {}
    if isinstance(scope_key, RuntimeScopeKey):
        return scope_key.with_hash_payload()
    payload = dict(scope_key)
    if "scope_key_hash" in payload:
        return payload
    key = RuntimeScopeKey.from_mapping(payload)
    return key.with_hash_payload()


def validate_scope_key_hash(payload: Mapping[str, object]) -> dict[str, object]:
    scope_payload = dict(payload.get("runtime_scope_key") or payload)
    expected = _clean(payload.get("scope_key_hash") or scope_payload.get("scope_key_hash"))
    key = RuntimeScopeKey.from_mapping(scope_payload)
    actual = key.scope_key_hash()
    status = "pass" if expected == actual else "fail"
    return {
        "schema_version": 1,
        "layer": "runtime_scope_key",
        "status": status,
        "expected_scope_key_hash": expected,
        "recomputed_scope_key_hash": actual,
        "mismatch_reason": "" if status == "pass" else "scope_key_hash_mismatch",
        "runtime_scope_key": key.as_dict(),
    }


@dataclass(frozen=True)
class ReplayHashChain:
    manifest_hash: str = ""
    scope_key_hash: str = ""
    runtime_data_availability_hash: str = ""
    feature_snapshot_hash: str = ""
    runtime_decision_request_hash: str = ""
    allocation_input_hash: str = ""
    portfolio_target_hash: str = ""
    execution_plan_batch_hash: str = ""
    pair_execution_plan_hash: str = ""
    execution_submit_plan_hash: str = ""
    pre_submit_risk_decision_hash: str = ""
    schema_version: int = 1

    def __post_init__(self) -> None:
        for field in (
            "manifest_hash",
            "scope_key_hash",
            "runtime_data_availability_hash",
            "feature_snapshot_hash",
            "runtime_decision_request_hash",
            "allocation_input_hash",
            "portfolio_target_hash",
            "execution_plan_batch_hash",
            "pair_execution_plan_hash",
            "execution_submit_plan_hash",
            "pre_submit_risk_decision_hash",
        ):
            object.__setattr__(self, field, _clean(getattr(self, field)))

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "manifest_hash": self.manifest_hash,
            "scope_key_hash": self.scope_key_hash,
            "runtime_data_availability_hash": self.runtime_data_availability_hash,
            "feature_snapshot_hash": self.feature_snapshot_hash,
            "runtime_decision_request_hash": self.runtime_decision_request_hash,
            "allocation_input_hash": self.allocation_input_hash,
            "portfolio_target_hash": self.portfolio_target_hash,
            "execution_plan_batch_hash": self.execution_plan_batch_hash,
            "pair_execution_plan_hash": self.pair_execution_plan_hash,
            "execution_submit_plan_hash": self.execution_submit_plan_hash,
            "pre_submit_risk_decision_hash": self.pre_submit_risk_decision_hash,
        }

    def chain_hash(self) -> str:
        return sha256_prefixed(self.as_dict())

    def with_hash_payload(self) -> dict[str, object]:
        payload = self.as_dict()
        payload["replay_hash_chain_hash"] = self.chain_hash()
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "ReplayHashChain":
        return cls(
            manifest_hash=str(payload.get("manifest_hash") or payload.get("runtime_strategy_set_manifest_hash") or ""),
            scope_key_hash=str(payload.get("scope_key_hash") or ""),
            runtime_data_availability_hash=str(
                payload.get("runtime_data_availability_hash")
                or payload.get("runtime_data_availability_report_hash")
                or ""
            ),
            feature_snapshot_hash=str(payload.get("feature_snapshot_hash") or ""),
            runtime_decision_request_hash=str(
                payload.get("runtime_decision_request_hash") or payload.get("request_hash") or ""
            ),
            allocation_input_hash=str(payload.get("allocation_input_hash") or ""),
            portfolio_target_hash=str(
                payload.get("portfolio_target_hash") or payload.get("final_portfolio_target_hash") or ""
            ),
            execution_plan_batch_hash=str(payload.get("execution_plan_batch_hash") or ""),
            pair_execution_plan_hash=str(payload.get("pair_execution_plan_hash") or ""),
            execution_submit_plan_hash=str(
                payload.get("execution_submit_plan_hash") or payload.get("submit_plan_hash") or ""
            ),
            pre_submit_risk_decision_hash=str(payload.get("pre_submit_risk_decision_hash") or ""),
        )


def validate_replay_hash_chain(payload: Mapping[str, object]) -> dict[str, object]:
    chain_payload = dict(payload.get("replay_hash_chain") or payload)
    expected = _clean(payload.get("replay_hash_chain_hash") or chain_payload.get("replay_hash_chain_hash"))
    chain = ReplayHashChain.from_mapping(chain_payload)
    actual = chain.chain_hash()
    missing = [key for key, value in chain.as_dict().items() if key != "schema_version" and not _clean(value)]
    status = "pass" if expected == actual and not missing else "fail"
    mismatch = ""
    if missing:
        mismatch = f"replay_hash_chain_missing:{','.join(missing)}"
    elif expected != actual:
        mismatch = "replay_hash_chain_hash_mismatch"
    return {
        "schema_version": 1,
        "layer": "replay_hash_chain",
        "status": status,
        "expected_replay_hash_chain_hash": expected,
        "recomputed_replay_hash_chain_hash": actual,
        "mismatch_reason": mismatch,
        "missing_layers": missing,
        "replay_hash_chain": chain.as_dict(),
    }


@dataclass(frozen=True)
class RuntimeScopeShard:
    scope_key: RuntimeScopeKey
    data_preflight_hash: str
    decision_bundle_hash: str = ""
    allocation_decision_hash: str = ""
    execution_plan_hash: str = ""
    status: str = "pending"
    evidence: Mapping[str, object] = MappingProxyType({})
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.scope_key, RuntimeScopeKey):
            raise TypeError("runtime_scope_shard_requires_scope_key")
        object.__setattr__(self, "data_preflight_hash", _clean(self.data_preflight_hash))
        object.__setattr__(self, "decision_bundle_hash", _clean(self.decision_bundle_hash))
        object.__setattr__(self, "allocation_decision_hash", _clean(self.allocation_decision_hash))
        object.__setattr__(self, "execution_plan_hash", _clean(self.execution_plan_hash))
        object.__setattr__(self, "status", _clean(self.status) or "pending")
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence or {})))

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "runtime_scope_key": self.scope_key.as_dict(),
            "scope_key_hash": self.scope_key.scope_key_hash(),
            "pair": self.scope_key.pair,
            "interval": self.scope_key.interval,
            "strategy_instance_id": self.scope_key.strategy_instance_id,
            "data_preflight_hash": self.data_preflight_hash,
            "decision_bundle_hash": self.decision_bundle_hash,
            "allocation_decision_hash": self.allocation_decision_hash,
            "execution_plan_hash": self.execution_plan_hash,
            "status": self.status,
            "evidence": dict(self.evidence),
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())
