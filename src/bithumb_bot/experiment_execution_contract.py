from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .decision_equivalence import sha256_prefixed


POSITION_MODE_CONTINUOUS_NOTIONAL_TARGET = "continuous_notional_target"
POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT = "fixed_fill_qty_until_exit"


@dataclass(frozen=True)
class ExperimentExecutionContract:
    source_artifact_hash: str = ""
    authority_hash: str = ""
    code_commit_sha: str = ""
    env_file_hash: str = ""
    strategy_parameter_hash: str = ""
    position_mode: str = POSITION_MODE_CONTINUOUS_NOTIONAL_TARGET
    quantity_contract_hash: str = ""
    order_rule_snapshot_hash: str = ""
    fee_slippage_timing_hash: str = ""
    startup_gate_hash: str = ""
    extra: Mapping[str, Any] = field(default_factory=dict)

    def as_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": 1,
            "contract_type": "experiment_execution_contract",
            "source_artifact_hash": str(self.source_artifact_hash or ""),
            "authority_hash": str(self.authority_hash or ""),
            "code_commit_sha": str(self.code_commit_sha or ""),
            "env_file_hash": str(self.env_file_hash or ""),
            "strategy_parameter_hash": str(self.strategy_parameter_hash or ""),
            "position_mode": str(self.position_mode or POSITION_MODE_CONTINUOUS_NOTIONAL_TARGET),
            "quantity_contract_hash": str(self.quantity_contract_hash or ""),
            "order_rule_snapshot_hash": str(self.order_rule_snapshot_hash or ""),
            "fee_slippage_timing_hash": str(self.fee_slippage_timing_hash or ""),
            "startup_gate_hash": str(self.startup_gate_hash or ""),
        }
        payload.update({str(key): value for key, value in dict(self.extra or {}).items()})
        payload["contract_hash"] = sha256_prefixed(payload)
        return payload

    def contract_hash(self) -> str:
        return str(self.as_payload()["contract_hash"])


def experiment_execution_contract_from_mapping(payload: Mapping[str, Any]) -> ExperimentExecutionContract:
    return ExperimentExecutionContract(
        source_artifact_hash=str(payload.get("source_artifact_hash") or ""),
        authority_hash=str(payload.get("authority_hash") or payload.get("h74_authority_hash") or ""),
        code_commit_sha=str(payload.get("code_commit_sha") or payload.get("commit_sha") or ""),
        env_file_hash=str(payload.get("env_file_hash") or ""),
        strategy_parameter_hash=str(payload.get("strategy_parameter_hash") or payload.get("authority_parameter_hash") or ""),
        position_mode=str(payload.get("position_mode") or POSITION_MODE_CONTINUOUS_NOTIONAL_TARGET),
        quantity_contract_hash=str(payload.get("quantity_contract_hash") or ""),
        order_rule_snapshot_hash=str(payload.get("order_rule_snapshot_hash") or ""),
        fee_slippage_timing_hash=str(payload.get("fee_slippage_timing_hash") or ""),
        startup_gate_hash=str(payload.get("startup_gate_hash") or payload.get("gate_trace_hash") or ""),
    )


def current_h74_experiment_execution_contract_from_payload(
    payload: Mapping[str, Any],
    *,
    code_commit_sha: str,
    env_file_hash: str,
    quantity_contract_hash: str,
    order_rule_snapshot_hash: str,
    fee_slippage_timing_hash: str,
) -> ExperimentExecutionContract:
    source_authority = payload.get("h74_source_authority")
    source_authority_payload = dict(source_authority) if isinstance(source_authority, Mapping) else {}
    bound = dict(source_authority_payload.get("hash_bound_parameters") or {})
    source_artifact_hash = str(
        payload.get("source_artifact_hash")
        or bound.get("source_candidate_artifact_hash")
        or bound.get("source_artifact_hash")
        or ""
    )
    return ExperimentExecutionContract(
        source_artifact_hash=source_artifact_hash,
        authority_hash=str(
            payload.get("authority_hash")
            or payload.get("h74_source_authority_hash")
            or source_authority_payload.get("authority_content_hash")
            or ""
        ),
        code_commit_sha=str(code_commit_sha or ""),
        env_file_hash=str(env_file_hash or ""),
        strategy_parameter_hash=str(
            payload.get("strategy_parameter_hash")
            or payload.get("authority_parameter_hash")
            or source_authority_payload.get("authority_parameter_hash")
            or ""
        ),
        position_mode=str(payload.get("position_mode") or POSITION_MODE_CONTINUOUS_NOTIONAL_TARGET),
        quantity_contract_hash=str(quantity_contract_hash or ""),
        order_rule_snapshot_hash=str(order_rule_snapshot_hash or ""),
        fee_slippage_timing_hash=str(fee_slippage_timing_hash or ""),
        startup_gate_hash=str(payload.get("startup_gate_hash") or ""),
    )


__all__ = [
    "ExperimentExecutionContract",
    "POSITION_MODE_CONTINUOUS_NOTIONAL_TARGET",
    "POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT",
    "current_h74_experiment_execution_contract_from_payload",
    "experiment_execution_contract_from_mapping",
]
