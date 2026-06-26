from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .h74_position_ownership import (
    H74PositionOwnershipContract,
    H74PositionOwnershipError,
    h74_position_ownership_contract_from_payload,
)


class H74SubmitIdentityError(ValueError):
    pass


@dataclass(frozen=True)
class H74SubmitIdentity:
    cycle_id: str
    h74_cycle_id: str
    strategy_instance_id: str
    authority_hash: str
    probe_run_id: str
    h74_entry_plan_client_order_id: str
    h74_position_ownership_contract_hash: str
    h74_position_ownership_contract: dict[str, object]

    def __post_init__(self) -> None:
        self.validate_complete()

    @classmethod
    def from_ownership_contract(cls, contract: H74PositionOwnershipContract) -> "H74SubmitIdentity":
        return cls(
            cycle_id=contract.cycle_id,
            h74_cycle_id=contract.h74_cycle_id,
            strategy_instance_id=contract.strategy_instance_id,
            authority_hash=contract.authority_hash,
            probe_run_id=contract.probe_run_id,
            h74_entry_plan_client_order_id=contract.entry_plan_id,
            h74_position_ownership_contract_hash=contract.contract_hash,
            h74_position_ownership_contract=contract.as_dict(),
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "H74SubmitIdentity":
        contract_payload = payload.get("h74_position_ownership_contract")
        if not isinstance(contract_payload, Mapping):
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_position_ownership_contract")
        cycle_id = str(payload.get("cycle_id") or "").strip()
        h74_cycle_id = str(payload.get("h74_cycle_id") or "").strip()
        if not cycle_id:
            raise H74SubmitIdentityError("h74_submit_identity_missing:cycle_id")
        if not h74_cycle_id:
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_cycle_id")
        if cycle_id != h74_cycle_id:
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:cycle_id")
        contract_hash = str(payload.get("h74_position_ownership_contract_hash") or "").strip()
        if not contract_hash:
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_position_ownership_contract_hash")
        try:
            contract = h74_position_ownership_contract_from_payload(
                {
                    **dict(contract_payload),
                    "cycle_id": cycle_id,
                    "h74_cycle_id": h74_cycle_id,
                    "strategy_instance_id": payload.get("strategy_instance_id"),
                    "authority_hash": payload.get("authority_hash"),
                    "probe_run_id": payload.get("probe_run_id")
                    or payload.get("h74_execution_path_probe_run_id"),
                    "h74_position_ownership_contract_hash": contract_hash,
                }
            )
        except H74PositionOwnershipError as exc:
            raise H74SubmitIdentityError(f"h74_submit_identity_invalid_contract:{exc}") from exc
        entry_plan_id = str(payload.get("h74_entry_plan_client_order_id") or "").strip()
        if not entry_plan_id:
            raise H74SubmitIdentityError("h74_submit_identity_missing:h74_entry_plan_client_order_id")
        if entry_plan_id != contract.entry_plan_id:
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:h74_entry_plan_client_order_id")
        return cls.from_ownership_contract(contract)

    def validate_complete(self) -> None:
        missing = [
            field
            for field, value in (
                ("cycle_id", self.cycle_id),
                ("h74_cycle_id", self.h74_cycle_id),
                ("strategy_instance_id", self.strategy_instance_id),
                ("authority_hash", self.authority_hash),
                ("probe_run_id", self.probe_run_id),
                ("h74_entry_plan_client_order_id", self.h74_entry_plan_client_order_id),
                ("h74_position_ownership_contract_hash", self.h74_position_ownership_contract_hash),
            )
            if not str(value or "").strip()
        ]
        if missing:
            raise H74SubmitIdentityError("h74_submit_identity_missing:" + ",".join(missing))
        if str(self.cycle_id).strip() != str(self.h74_cycle_id).strip():
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:cycle_id")
        contract = h74_position_ownership_contract_from_payload(
            {
                **dict(self.h74_position_ownership_contract),
                "cycle_id": self.cycle_id,
                "h74_cycle_id": self.h74_cycle_id,
                "strategy_instance_id": self.strategy_instance_id,
                "authority_hash": self.authority_hash,
                "probe_run_id": self.probe_run_id,
                "h74_position_ownership_contract_hash": self.h74_position_ownership_contract_hash,
            }
        )
        if contract.contract_hash != str(self.h74_position_ownership_contract_hash).strip():
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:h74_position_ownership_contract_hash")
        if contract.entry_plan_id != str(self.h74_entry_plan_client_order_id).strip():
            raise H74SubmitIdentityError("h74_submit_identity_mismatch:h74_entry_plan_client_order_id")

    def as_evidence_dict(self) -> dict[str, object]:
        return {
            "cycle_id": self.cycle_id,
            "h74_cycle_id": self.h74_cycle_id,
            "strategy_instance_id": self.strategy_instance_id,
            "authority_hash": self.authority_hash,
            "probe_run_id": self.probe_run_id,
            "h74_execution_path_probe_run_id": self.probe_run_id,
            "h74_entry_plan_client_order_id": self.h74_entry_plan_client_order_id,
            "h74_position_ownership_contract_hash": self.h74_position_ownership_contract_hash,
            "h74_position_ownership_contract": dict(self.h74_position_ownership_contract),
        }

    def as_order_metadata(self) -> dict[str, object]:
        return {
            "strategy_instance_id": self.strategy_instance_id,
            "cycle_id": self.cycle_id,
            "authority_hash": self.authority_hash,
            "probe_run_id": self.probe_run_id,
            "h74_cycle_id": self.h74_cycle_id,
            "h74_entry_plan_client_order_id": self.h74_entry_plan_client_order_id,
            "h74_position_ownership_contract_hash": self.h74_position_ownership_contract_hash,
            "h74_position_ownership_contract": dict(self.h74_position_ownership_contract),
        }


__all__ = ["H74SubmitIdentity", "H74SubmitIdentityError"]
