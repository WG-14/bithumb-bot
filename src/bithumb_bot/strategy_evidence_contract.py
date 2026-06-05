from __future__ import annotations

from dataclasses import dataclass

from .decision_equivalence import sha256_prefixed


@dataclass(frozen=True)
class DecisionEvidenceContract:
    schema_version: int = 1
    requires_decision_input_bundle: bool = False
    required_promotion_provenance_fields: tuple[str, ...] = ()
    required_live_real_order_fields: tuple[str, ...] = ()
    snapshot_projector_contract: str | None = None
    decision_input_contract_kind: str = "generic"

    def __post_init__(self) -> None:
        schema_version = int(self.schema_version)
        if schema_version != 1:
            raise ValueError(f"decision_evidence_contract_schema_unsupported:{schema_version}")
        contract_kind = str(self.decision_input_contract_kind or "").strip().lower()
        if not contract_kind:
            raise ValueError("decision_evidence_contract_kind_missing")
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "decision_input_contract_kind", contract_kind)
        object.__setattr__(
            self,
            "required_promotion_provenance_fields",
            _normalize_fields(self.required_promotion_provenance_fields),
        )
        object.__setattr__(
            self,
            "required_live_real_order_fields",
            _normalize_fields(self.required_live_real_order_fields),
        )
        if self.snapshot_projector_contract is not None:
            projector = str(self.snapshot_projector_contract or "").strip()
            if not projector:
                raise ValueError("decision_evidence_snapshot_projector_contract_empty")
            object.__setattr__(self, "snapshot_projector_contract", projector)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "requires_decision_input_bundle": bool(self.requires_decision_input_bundle),
            "required_promotion_provenance_fields": list(self.required_promotion_provenance_fields),
            "required_live_real_order_fields": list(self.required_live_real_order_fields),
            "snapshot_projector_contract": self.snapshot_projector_contract,
            "decision_input_contract_kind": self.decision_input_contract_kind,
            "contract_hash": self.contract_hash(),
        }

    def payload_without_hash(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "requires_decision_input_bundle": bool(self.requires_decision_input_bundle),
            "required_promotion_provenance_fields": list(self.required_promotion_provenance_fields),
            "required_live_real_order_fields": list(self.required_live_real_order_fields),
            "snapshot_projector_contract": self.snapshot_projector_contract,
            "decision_input_contract_kind": self.decision_input_contract_kind,
        }

    def contract_hash(self) -> str:
        return sha256_prefixed(self.payload_without_hash())


def _normalize_fields(fields: tuple[str, ...]) -> tuple[str, ...]:
    normalized = sorted({str(field or "").strip() for field in tuple(fields or ())})
    if any(not field for field in normalized):
        raise ValueError("decision_evidence_required_field_empty")
    return tuple(normalized)


GENERIC_DECISION_EVIDENCE_CONTRACT = DecisionEvidenceContract()


__all__ = ["DecisionEvidenceContract", "GENERIC_DECISION_EVIDENCE_CONTRACT"]
